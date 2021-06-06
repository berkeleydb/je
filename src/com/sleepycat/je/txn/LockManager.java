/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.je.txn;

import static com.sleepycat.je.txn.LockStatDefinition.GROUP_DESC;
import static com.sleepycat.je.txn.LockStatDefinition.GROUP_NAME;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_OWNERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_REQUESTS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_TOTAL;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionTimeoutException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.dbi.RangeRestartException;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchFactory;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TinyHashSet;

/**
 * LockManager manages locks.
 *
 * Note that locks are counted as taking up part of the JE cache.
 */
public abstract class LockManager implements EnvConfigObserver {

    /*
     * The total memory cost for a lock is the Lock object, plus its entry and
     * key in the lock hash table.
     *
     * The addition and removal of Lock objects, and the corresponding cost of
     * their hashmap entry and key are tracked through the LockManager.
     */
    private static final long TOTAL_LOCKIMPL_OVERHEAD =
        MemoryBudget.LOCKIMPL_OVERHEAD +
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.LONG_OVERHEAD;

    static final long TOTAL_THINLOCKIMPL_OVERHEAD =
        MemoryBudget.THINLOCKIMPL_OVERHEAD +
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.LONG_OVERHEAD;

    private static final long REMOVE_TOTAL_LOCKIMPL_OVERHEAD =
        0 - TOTAL_LOCKIMPL_OVERHEAD;

    private static final long REMOVE_TOTAL_THINLOCKIMPL_OVERHEAD =
        0 - TOTAL_THINLOCKIMPL_OVERHEAD;

    private static final long THINLOCK_MUTATE_OVERHEAD =
        MemoryBudget.LOCKIMPL_OVERHEAD -
        MemoryBudget.THINLOCKIMPL_OVERHEAD +
        MemoryBudget.LOCKINFO_OVERHEAD;
    
    private static final List<ThreadLocker> EMPTY_THREAD_LOCKERS =
        Collections.emptyList();

    /* Hook called after a lock is requested. */
    public static TestHook<Void> afterLockHook;
    
    /* 
     * Hook called after waitingFor is set and before doing deadlock
     * detection. This aims to inject some wait-action for the current
     * thread. 
     */
    static TestHook<Void> simulatePartialDeadlockHook;
    
    final int nLockTables;
    final Latch[] lockTableLatches;
    private final Map<Long,Lock>[] lockTables;          // keyed by LSN
    private final EnvironmentImpl envImpl;
    private final MemoryBudget memoryBudget;

    private final StatGroup stats;
    private final LongStat nRequests; /* number of time a request was made. */
    private final LongStat nWaits;    /* number of time a request blocked. */

    private static RangeRestartException rangeRestartException =
        new RangeRestartException();
    private static boolean lockTableDump = false;

    /**
     * Maps a thread to a set of ThreadLockers.  Currently this map is only
     * maintained (non-null) in a replicated environment because it is only
     * needed for determining when to throw LockPreemptedException.
     *
     * Access to the map need not be synchronized because it is a
     * ConcurrentHashMap.  Access to the TinyHashSet stored for each thread
     * need not be synchronized, since it is only accessed by a single thread.
     *
     * A TinyHashSet is used because typically only a single ThreadLocker per
     * thread will be open at one time.
     *
     * @see ThreadLocker#checkPreempted
     * [#16513]
     */
    private final Map<Thread, TinyHashSet<ThreadLocker>> threadLockers;

    /*
     * @SuppressWarnings is used to stifle a type safety complaint about the
     * assignment of lockTables = new Map[nLockTables]. There's no way to
     * specify the type of the array.
     */
    @SuppressWarnings("unchecked")
    public LockManager(final EnvironmentImpl envImpl) {

        final DbConfigManager configMgr = envImpl.getConfigManager();
        nLockTables = configMgr.getInt(EnvironmentParams.N_LOCK_TABLES);
        lockTables = new Map[nLockTables];
        lockTableLatches = new Latch[nLockTables];
        for (int i = 0; i < nLockTables; i++) {
            lockTables[i] = new HashMap<Long,Lock>();
            lockTableLatches[i] = LatchFactory.createExclusiveLatch(
                envImpl, "Lock Table " + i, true /*collectStats*/);
        }
        this.envImpl = envImpl;
        memoryBudget = envImpl.getMemoryBudget();

        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        nRequests = new LongStat(stats, LOCK_REQUESTS);
        nWaits = new LongStat(stats, LOCK_WAITS);

        /* Initialize mutable properties and register for notifications. */
        envConfigUpdate(configMgr, null);
        envImpl.addConfigObserver(this);

        if (envImpl.isReplicated()) {
            threadLockers =
                new ConcurrentHashMap<Thread, TinyHashSet<ThreadLocker>>();
        } else {
            threadLockers = null;
        }
    }

    /**
     * Process notifications of mutable property changes.
     */
    public void envConfigUpdate(final DbConfigManager configMgr,
                                final EnvironmentMutableConfig ignore) {

        LockInfo.setDeadlockStackTrace(configMgr.getBoolean
            (EnvironmentParams.TXN_DEADLOCK_STACK_TRACE));

        setLockTableDump(configMgr.getBoolean
            (EnvironmentParams.TXN_DUMPLOCKS));
    }

    /**
     * Called when the je.txn.dumpLocks property is changed.
     */
    private static void setLockTableDump(boolean enable) {
        lockTableDump = enable;
    }

    int getLockTableIndex(Long lsn) {
        return (((int) lsn.longValue()) & 0x7fffffff) % nLockTables;
    }

    int getLockTableIndex(long lsn) {
        return (((int) lsn) & 0x7fffffff) % nLockTables;
    }

    private static long timeRemain(final long timeout, final long startTime) {
        return (timeout - (System.currentTimeMillis() - startTime));
    }

    /**
     * Attempt to acquire a lock of 'type' on 'lsn'.
     *
     * @param lsn The LSN to lock.
     *
     * @param locker The Locker to lock this on behalf of.
     *
     * @param type The lock type requested.
     *
     * @param timeout milliseconds to time out after if lock couldn't be
     * obtained.  0 means block indefinitely.  Not used if nonBlockingRequest
     * is true.
     *
     * @param nonBlockingRequest if true, means don't block if lock can't be
     * acquired, and ignore the timeout parameter.
     *
     * @param jumpAheadOfWaiters grant the lock before other waiters, if any.
     *
     * @return a LockGrantType indicating whether the request was fulfilled or
     * not.  LockGrantType.NEW means the lock grant was fulfilled and the
     * caller did not previously hold the lock.  PROMOTION means the lock was
     * granted and it was a promotion from READ to WRITE.  EXISTING means the
     * lock was already granted (not a promotion).  DENIED means the lock was
     * not granted when nonBlockingRequest is true.
     *
     * @throws LockConflictException if lock could not be acquired. If
     * nonBlockingRequest is true, a LockConflictException is never thrown.
     * Otherwise, if the lock acquisition would result in a deadlock,
     * DeadlockException is thrown. Otherwise, if the lock timeout interval
     * elapses and no deadlock is detected, LockTimeoutException is thrown.
     */
    public LockGrantType lock(final long lsn,
                              final Locker locker,
                              final LockType type,
                              long timeout,
                              final boolean nonBlockingRequest,
                              final boolean jumpAheadOfWaiters,
                              final DatabaseImpl database)
        throws LockConflictException, DatabaseException {

        assert timeout >= 0;

        /* No lock needed for dirty-read, return as soon as possible. */
        if (type == LockType.NONE) {
            return LockGrantType.NONE_NEEDED;
        }

        final long startTime;
        LockAttemptResult result;
        LockGrantType grant;

        synchronized (locker) {

            /* Attempt to lock without any initial wait. */
            result = attemptLock(
                lsn, locker, type, nonBlockingRequest, jumpAheadOfWaiters);

            grant = result.lockGrant;

            /* If we got the lock or a non-blocking lock was denied, return. */
            if (result.success || grant == LockGrantType.DENIED) {
                assert nonBlockingRequest || result.success;
                if (afterLockHook != null) {
                    afterLockHook.doHook();
                }
                return grant;
            }

            if (LatchSupport.TRACK_LATCHES) {
                if (!nonBlockingRequest) {
                    LatchSupport.expectBtreeLatchesHeld(0);
                }
            }

            /*
             * We must have gotten WAIT_* from the lock request. We know that
             * this is a blocking request, because if it wasn't, Lock.lock
             * would have returned DENIED. Go wait!
             */
            assert !nonBlockingRequest;

            locker.setWaitingFor(lsn, type);

            /* currentTimeMillis is expensive. Only call it if we will wait. */
            startTime = System.currentTimeMillis();

            /*
             * If there is a txn timeout, and the txn time remaining is less
             * than the lock timeout, then use the txn time remaining instead.
             */
            final long txnTimeout = locker.getTxnTimeout();
            if (txnTimeout > 0) {
                final long txnTimeRemaining =
                    timeRemain(txnTimeout, locker.getTxnStartMillis());

                if (timeout == 0 || txnTimeRemaining < timeout) {
                    timeout = Math.max(1, txnTimeRemaining);
                }
            }

            /*
             * After the deadlock detection delay, if this locker is the owner,
             * we're done.
             */
            if (performDeadlockDetectionDelay(
                lsn, locker, type, grant, timeout, startTime)) {
                return grant;
            }
        }

        DeadlockChecker lastDC = null;

        /*
         * Repeatedly try waiting for the lock. If a deadlock is detected,
         * notify the victim. When this locker becomes the owner, we're done.
         *
         * If there is a deadlock and this locker is the victim, waitForLock
         * will throw DeadlockException. If there is a deadlock and this locker
         * is not the victim, we call notifyVictim. If the deadlock cannot be
         * broken by notifying the victim, notifyVictim will throw
         * DeadlockException.
         *
         * If a timeout occurs and a deadlock was detected, DeadlockException
         * will be thrown by waitForLock or notifyVictim. If a timeout occurs
         * and no deadlock was detected, a timeout exception will be thrown by
         * waitForLock.
         */
        while (true) {
            final WaitForLockResult waitResult;

            synchronized (locker) {
                waitResult = waitForLock(
                    result, lsn, locker, type, lastDC, timeout, startTime,
                    database);
            }

            result = waitResult.getResult();
            grant = result.lockGrant;
            lastDC = waitResult.getDeadLockChecker();
            final Locker victim = waitResult.getVictim();
            
            if (victim == null) {
                /* The locker owns the lock and no deadlock was detected. */
                return grant;
            }
            
            /*
             * A deadlock is detected and this locker is not the victim.
             * Notify the victim.
             */
            final Pair<Boolean, DeadlockChecker> nvResult = notifyVictim(
                victim, locker, lsn, type, lastDC, timeout, startTime,
                database);

            if (nvResult.first()) {

                /*
                 * The deadlock was broken and this locker is now the owner.
                 * finishLock will clear locker.waitingFor, which must be
                 * protected by the locker mutex.
                 */
                synchronized (locker) {
                    finishLock(locker, lsn, type, grant);
                }
                return grant;
            }
            
            /*
             * A deadlock is no longer present, or a deadlock with a different
             * victim was detected. And the timeout has not expired. Retry.
             */
            lastDC = nvResult.second();
        }
    }

    /**
     * Waits for a lock that was previously requested. Handles deadlocks and
     * timeouts. However, does not notify the victim when a deadlock is
     * detected and the current locker is not the victim; in that case the
     * caller must notify the victim. This method cannot notify the victim
     * because it is synchronized on the current locker, and we can synchronize
     * on only one locker at a time.
     *
     * This method must be called while synchronized on the locker, for several
     * reasons:
     * 1. The locker will be modified if the lock is acquired by stealLock
     * and when finishLock is called.
     * 2. It must be synchronized when performing the lock delay.
     *
     * @return the lock result and a null victim if locking was successful, or
     * a non-null victim if a deadlock is detected and the current locker is
     * not the victim.
     *
     * @throws DeadlockException when a deadlock is detected and the current
     * locker is the victim.
     *
     * @throws LockTimeoutException when the timeout elapses and no deadlock is
     * detected.
     *
     * @throws TransactionTimeoutException when the transaction time limit was
     * exceeded.
     */
    private WaitForLockResult waitForLock(
        LockAttemptResult result,
        final Long lsn,
        final Locker locker,
        final LockType type,
        DeadlockChecker lastDC,
        final long timeout,
        final long startTime,
        final DatabaseImpl database) {

        final boolean isImportunate = locker.getImportunate();
        final boolean waitForever = (timeout == 0);
        Locker victim = null;

        if (simulatePartialDeadlockHook != null) {
            simulatePartialDeadlockHook.doHook();
        }
        
        /*
         * There are two reasons for the loop below.
         *
         * 1. When another thread detects a deadlock and notifies this thread,
         * it will wakeup before the timeout interval has expired. We must loop
         * again to perform deadlock detection. Normally, if the deadlock
         * detected by the other thread is still present, this locker will be
         * selected as the victim and we will throw DeadlockException below.
         *
         * 2. Even when another JE locking thread does not notify this thread,
         * this thread may wake up before the timeout has elapsed due to
         * "spurious wakeups" -- see Object.wait().
         */
        while (true) {

            /*
             * Perform deadlock detection before waiting.
             *
             * When the locker is a ReplayTxn (isImportunate is true), we
             * must steal the lock even if when we encounter a deadlock.
             * 
             * There are two reasons that we do not check for deadlocks
             * here for a ReplayTxn:
             *
             * 1. If we were to detect a deadlock here, we could not
             * control which locker will be chosen as the victim. The
             * ReplayTxn might be chosen as the victim and then it would be
             * aborted by throwing DeadlockException. ReplayTxns may not be
             * aborted and a LockConflictException should not be thrown.
             *
             * 2. If the ReplayTxn deadlocks with other txns, the other
             * txns will detect deadlock.
             *  + If another txn is chosen as the victim, then ReplayTxn
             *    will acquire the lock.
             *  + If ReplayTxn is chosen as the victim, ReplayTxn will be
             *    notified and will wake up and steal the lock. This is
             *    efficient, since a long wait will not be needed.
             */
            if (envImpl.getDeadlockDetection() && !isImportunate) {

                /* Do deadlock detect */
                final DeadlockResult dlr = checkAndHandleDeadlock(
                    locker, lsn, type, timeout, database);

                if (dlr.isOwner) {
                    break;
                }

                if (dlr.trueDeadlock) {
                    victim = dlr.victim;
                    lastDC = dlr.dc;
                    break;
                }

                /*
                 * Else do nothing. We did not detect a true deadlock and
                 * this locker does not own the lock, so wait again with the
                 * time remaining.
                 */
            } else {
                /*
                 * Check ownership before waiting, since we release the locker
                 * mutex between calling attemptLock and waitForLock. This
                 * check prevents a missed notification:
                 *    locker1                        locker2
                 *    wait for a lock
                 *                        release the lock and notify locker1
                 *    locker1.wait()
                 */
                if (isOwner(lsn, locker, type)) {
                    break;
                }
            }
                                            
            try {
                if (waitForever) {
                    locker.wait(0);
                } else {
                    locker.wait(Math.max(1, timeRemain(timeout, startTime)));
                } 
            } catch (InterruptedException IE) {
                throw new ThreadInterruptedException(envImpl, IE);
            }

            final boolean lockerTimedOut = locker.isTimedOut();
            final long now = System.currentTimeMillis();

            final boolean thisLockTimedOut =
                (!waitForever && (now - startTime) >= timeout);

            final boolean isRestart =
                (result.lockGrant == LockGrantType.WAIT_RESTART);
            
            /*
             * Try to get accurate owners and waiters of requested
             * lock when deciding to possibly throw LockTimeoutException
             */
            final Set<LockInfo> owners = new HashSet<>();
            final List<LockInfo> waiters = new ArrayList<>();

            final boolean getOwnersAndWaiters =
                (lockerTimedOut || thisLockTimedOut) && !isImportunate;

            /*
             * Only flush the waiters if isRestart && !isImportunate. If
             * lockerTimedOut or thisLockTimedOut, we will flush the waiters
             * before throwing an exception further below.
             */
            final boolean flushFromWaiters = isRestart && !isImportunate;

            if (validateOwnership(
                lsn, locker, type, getOwnersAndWaiters, flushFromWaiters,
                owners, waiters)) {

                break;
            }

            if (isImportunate) {
                result = stealLock(lsn, locker, type);
                if (result.success) {
                    break;
                }
                /* Lock holder is non-preemptable, wait again. */
                continue;
            }

            /* After a restart conflict the lock will not be held. */
            if (isRestart) {
                throw rangeRestartException;
            }

            if (!thisLockTimedOut && !lockerTimedOut) {
                continue;
            }

            final DeadlockResult dlr =
                checkAndHandleDeadlock(locker, lsn, type, timeout, database);

            if (dlr.isOwner) {
                break;
            }

            if (dlr.trueDeadlock) {
                lastDC = dlr.dc;
            }

            /* Flush lock from waiters before throwing exception. */
            if (validateOwnership(
                lsn, locker, type,
                false /*getOwnersAndWaiters*/,
                true /*flushFromWaiters*/, null, null)) {

                break;
            }

            if (lastDC != null) {
                throw makeDeadlockException(
                    lastDC, locker, timeout, false /*isVictim*/, database);
            }

            /*
             * When both types of timeouts occur, throw TransactionTimeout.
             * Otherwise TransactionTimeout may never be thrown, because when
             * the txn times out, the lock probably also times out.
             */
            if (lockerTimedOut) {
                throw makeTimeoutException(
                    false /*isLockNotTxnTimeout*/, locker, lsn, type,
                    result.lockGrant, result.useLock,
                    locker.getTxnTimeout(), locker.getTxnStartMillis(),
                    now, database, owners, waiters);
            } else {
                throw makeTimeoutException(
                    true /*isLockNotTxnTimeout*/, locker, lsn, type,
                    result.lockGrant, result.useLock,
                    timeout, startTime,
                    now, database, owners, waiters);
            }
        }
        
        if (victim == null) {
            assert isOwner(lsn, locker, type);
            finishLock(locker, lsn, type, result.lockGrant);
        }

        return new WaitForLockResult(victim, lastDC, result);
    }
    
    private void finishLock(
        final Locker locker,
        final Long nid,
        final LockType type,
        final LockGrantType grant) {

        locker.clearWaitingFor();

        assert EnvironmentImpl.maybeForceYield();

        locker.addLock(nid, type, grant);

        if (afterLockHook != null) {
            afterLockHook.doHook();
        }
    }

    /**
     * Returns the Lockers that own a lock on the given LSN.  Note that when
     * this method returns, there is nothing to prevent these lockers from
     * releasing the lock or being closed.
     */
    public abstract Set<LockInfo> getOwners(Long lsn);

    Set<LockInfo> getOwnersInternal(final Long lsn, final int lockTableIndex) {
        /* Get the target lock. */
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return null;
        }
        return useLock.getOwnersClone();
    }
    
    /**
     * Returns the Lockers that wait on a lock on the given LSN.  
     */
    public abstract List<LockInfo> getWaiters(Long lsn);

    List<LockInfo> getWaitersInternal(final Long lsn,
                                      final int lockTableIndex) {
        /* Get the target lock. */
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return null;
        }
        return useLock.getWaitersListClone();
    }

    /**
     * Returns the LockType if the given locker owns a lock on the given node,
     * or null if the lock is not owned.
     */
    public abstract LockType getOwnedLockType(Long lsn, Locker locker);

    LockType getOwnedLockTypeInternal(final Long lsn,
                                      final Locker locker,
                                      final int lockTableIndex) {
        /* Get the target lock. */
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return null;
        }
        return useLock.getOwnedLockType(locker);
    }

    public abstract boolean isLockUncontended(Long lsn);

    boolean isLockUncontendedInternal(final Long lsn,
                                      final int lockTableIndex) {
        /* Get the target lock. */
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return true;
        }
        return useLock.nWaiters() == 0 &&
               useLock.nOwners() == 0;
    }

    public abstract boolean ownsOrSharesLock(Locker locker, Long lsn);

    boolean ownsOrSharesLockInternal(final Locker locker,
                                     final Long lsn,
                                     final int lockTableIndex) {
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return false;
        }
        for (final LockInfo info : getOwnersInternal(lsn, lockTableIndex)) {
            final Locker owner = info.getLocker();
            if (owner == locker ||
                owner.sharesLocksWith(locker) ||
                locker.sharesLocksWith(owner)) {
                return true;
            }
        }
        return false;
    }

    abstract Lock lookupLock(Long lsn)
        throws DatabaseException;

    Lock lookupLockInternal(final Long lsn, final int lockTableIndex) {
        /* Get the target lock. */
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        return lockTable.get(lsn);
    }

    abstract LockAttemptResult attemptLock(Long lsn,
                                           Locker locker,
                                           LockType type,
                                           boolean nonBlockingRequest,
                                           boolean jumpAheadOfWaiters)
        throws DatabaseException;

    LockAttemptResult attemptLockInternal(final Long lsn,
                                          final Locker locker,
                                          final LockType type,
                                          final boolean nonBlockingRequest,
                                          final boolean jumpAheadOfWaiters,
                                          final int lockTableIndex)
        throws DatabaseException {

        nRequests.increment();

        /* Get the target lock. */
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            useLock = new ThinLockImpl();
            lockTable.put(lsn, useLock);
            memoryBudget.updateLockMemoryUsage(
                TOTAL_THINLOCKIMPL_OVERHEAD, lockTableIndex);
        }

        /*
         * Attempt to lock.  Possible return values are NEW, PROMOTION, DENIED,
         * EXISTING, WAIT_NEW, WAIT_PROMOTION, WAIT_RESTART.
         */
        final LockAttemptResult lar = useLock.lock
            (type, locker, nonBlockingRequest, jumpAheadOfWaiters,
             memoryBudget, lockTableIndex);
        if (lar.useLock != useLock) {
            /* The lock mutated from ThinLockImpl to LockImpl. */
            useLock = lar.useLock;
            lockTable.put(lsn, useLock);
            /* We still have the overhead of the hashtable (locktable). */
            memoryBudget.updateLockMemoryUsage
                (THINLOCK_MUTATE_OVERHEAD, lockTableIndex);
        }
        final LockGrantType lockGrant = lar.lockGrant;
        boolean success = false;

        /* Was the attempt successful? */
        if ((lockGrant == LockGrantType.NEW) ||
            (lockGrant == LockGrantType.PROMOTION)) {
            locker.addLock(lsn, type, lockGrant);
            success = true;
        } else if (lockGrant == LockGrantType.EXISTING) {
            success = true;
        } else if (lockGrant == LockGrantType.DENIED) {
            /* Locker.lock will throw LockNotAvailableException. */
        } else {
            nWaits.increment();
        }
        return new LockAttemptResult(useLock, lockGrant, success);
    }

    /**
     * Performs the deadlock detection delay, if needed.
     *
     * This method must be called while synchronized on the locker because it
     * calls locker.wait and finishLock.
     *
     * @return true if the locker is the owner after the delay.
     */
    private boolean performDeadlockDetectionDelay(
        final Long lsn,
        final Locker locker,
        final LockType type,
        final LockGrantType grant,
        final long timeout,
        final long startTime) {

        /*
         * If dd is enabled, and there is a dd delay, and the locker is not
         * importunate, perform the delay here. See waitForLock for a
         * discussion of importunate lockers and dd.
         */
        long ddDelay = envImpl.getDeadlockDetectionDelay();

        if (!envImpl.getDeadlockDetection() ||
            locker.getImportunate() ||
            ddDelay == 0) {

            return false;
        }

        final boolean waitForever = (timeout == 0);

        if (!waitForever) {
            ddDelay = Math.min(
                ddDelay, timeRemain(timeout, startTime));
        }

        try {
            locker.wait(Math.max(1, ddDelay));
        } catch (InterruptedException IE) {
            throw new ThreadInterruptedException(envImpl, IE);
        }

        /* If the locker now owns the lock, we're done. */
        if (isOwner(lsn, locker, type)) {
            finishLock(locker, lsn, type, grant);
            return true;
        }

        /* Otherwise, we'll do deadlock detection in waitForLock. */
        return false;
    }

    /*
     * @return DeadlockHandleResult dlhr, where 
     *        dlhr.isOwner is true if the current locker owns the lock;
     *        dlhr.trueDeadlock is true if a true deadlock is detected and
     *        the current locker does not own the lock;
     *        dlhr.victim is not null if a victim is chosen and the victim
     *        is not the current locker;
     *        dlhr.dc will never be null, for simplicity, but it is useful only
     *        when a true deadlock is detected.
     * @throws DeadlockException if the victim is the current locker
     */
    private DeadlockResult checkAndHandleDeadlock(
        final Locker locker,
        final Long lsn,
        final LockType type,
        final long timeout,
        DatabaseImpl database) {

        boolean isOwner = false;
        boolean hasTrueDeadlock = false;
        Locker targetedVictim = null;
        DeadlockChecker dc;
        for (int round = 0;; round++) {
            dc = new DeadlockChecker(locker, lsn, type);

            if (dc.hasCycle()){
                if (dc.hasTrueDeadlock()) {

                    targetedVictim = dc.chooseTargetedLocker();
                    
                    if (targetedVictim != locker) {
                        /*
                         * There is a window where after we chose a victim,
                         * another thread notifies the same victim and the
                         * deadlock is handled.
                         * 
                         * So if now the current locker owns the lock, we do
                         * no longer need to notify the victim.
                         * 
                         * If the current locker does not own the lock, we will
                         * return the victim. The outer caller will notify the
                         * victim. Even if, when the outer caller notifies the
                         * victim, and the current locker owns the lock, this
                         * does not impact correctness. We just do one more
                         * redundant step to notify the victim, which has been
                         * already notified.
                         */
                        if (isOwner(lsn, locker, type)) {
                            isOwner = true;
                            targetedVictim = null;
                            break;
                        }
                        
                        hasTrueDeadlock = true;
                        break; 
                    }
                    
                    /*
                     * The targeted victim is this locker, so we will throw
                     * DeadlockException. But first we must call
                     * validateOwnership() to flush the locker from waiter
                     * list.
                     * 
                     * Normally, validateOwnership() will first check whether
                     * the current locker owns the lock. But here there is no
                     * possibility that the current locker can own the lock.
                     */
                    if (validateOwnership(
                        lsn, locker, type,
                        false /*getOwnersAndWaiters*/,
                        true /*flushFromWaiters*/, null, null)) {

                        isOwner = true;
                        targetedVictim = null;
                        break;
                    }

                    throw makeDeadlockException(
                        dc, locker, timeout, true /*isVictim*/, database);

                } else {
                    if (isOwner(lsn, locker, type)) {
                        isOwner = true;
                        break;
                    }
                    
                    if (round >= 10) {
                        break;
                    }
                }
            } else {
                if (isOwner(lsn, locker, type)) {
                    isOwner = true;
                }
                break;
            }
        }

        return new DeadlockResult(
            isOwner, hasTrueDeadlock, targetedVictim, dc);
    }
    
    /*
     * Notify the targetedVictim to cause it to abort, and wait for the
     * deadlock to be broken.
     *
     * @return {done, lastDC}. done is true if the deadlock has been broken and
     * currentLocker is now the owner. done is false in 2 cases:
     * 1. a deadlock is no longer present;
     * 2. a deadlock with a different victim is detected;
     * When done is false, the caller should retry.
     *
     * @throws DeadlockException if the original deadlock (or a deadlock with
     * the same victim) is not broken, and the timeout is exceeded.
     */
    private Pair<Boolean, DeadlockChecker> notifyVictim(
        final Locker targetedVictim,
        final Locker currentLocker,
        final Long lsn,
        final LockType type,
        DeadlockChecker lastDC,
        final long timeout,
        final long startTime,
        DatabaseImpl database) {

        final boolean waitForever = (timeout == 0);

        while (true) {
            /*
             * Check for a timeout first, to guarantee that we do not "live
             * lock" when deadlocks are repeatedly created and resolved in
             * other threads.
             */
            if (!waitForever && timeRemain(timeout, startTime) <= 0) {

                /*
                 * The original timeout was exceeded. Flush the current locker
                 * from the waiters list, and throw DeadlockException using the
                 * last known deadlock info.
                 */
                if (validateOwnership(
                    lsn, currentLocker, type,
                    false /*getOwnersAndWaiters*/,
                    true /*flushFromWaiters*/, null, null)) {

                    /* The currentLocker unexpectedly became the owner. */
                    return new Pair<>(true, lastDC);
                }

                throw makeDeadlockException(
                    lastDC, currentLocker, timeout, false /*isVictim*/,
                    database);
            }

            /*
             * Notify the victim and sleep for 1ms to allow the victim to
             * wakeup and abort.
             */
            synchronized (targetedVictim) {
                targetedVictim.notify();
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(envImpl, e);
            }
            
            /* If currentLocker is the owner, the deadlock was broken. */
            if (isOwner(lsn, currentLocker, type)) {
                return new Pair<>(true, lastDC);
            }

            final DeadlockChecker dc =
                new DeadlockChecker(currentLocker, lsn, type);

            /*
             * If the deadlock was broken, but currentLocker is not the owner,
             * then let the caller retry.
             */
            if (!(dc.hasCycle() && dc.hasTrueDeadlock())) {
                return new Pair<>(false, lastDC);
            }

            /* We found a true deadlock. */
            lastDC = dc;

            /*
             * If the victim is different, then the original deadlock was
             * broken but there is now a new deadlock. Let the caller handle
             * the new deadlock.
             */
            if (dc.chooseTargetedLocker() != targetedVictim) {
                return new Pair<>(false, lastDC);
            }

            /*
             * The victim is the same, so for simplicity we assume it is the
             * same deadlock. Retry.
             */
        }
    }
    
    private LockConflictException makeDeadlockException(
        final DeadlockChecker dc,
        final Locker locker,
        final long timeout,
        final boolean isVictim,
        final DatabaseImpl database){
        
        StringBuilder msg = new StringBuilder();
        msg.append("Deadlock was detected. ");
        if (isVictim) {
            msg.append("Locker: \"").append(locker);
            msg.append("\" was chosen randomly as the victim.\n");
        } else {
            msg.append("Unable to break deadlock using random victim ");
            msg.append("selection within the timeout interval. ");
            msg.append("Current locker: \"").append(locker);
            msg.append("\" must be aborted.\n");
        }

        if (database != null) {
            msg.append("DB: ").append(database.getDebugName()).append(". ");
        }

        msg.append("Timeout: ");
        if (timeout == 0) {
            msg.append("none.\n");
        } else {
            msg.append(timeout).append("ms.\n");
        }

        msg.append(dc);

        final LockConflictException ex = new DeadlockException(
            locker, msg.toString());

        ex.setOwnerTxnIds(getTxnIds(dc.getOwnersForRootLock()));
        ex.setWaiterTxnIds(getTxnIds(dc.getWaitersForRootLock()));
        ex.setTimeoutMillis(timeout);

        return ex;
    }

    private LockConflictException makeTimeoutException(
        final boolean isLockNotTxnTimeout,
        final Locker locker,
        final long lsn,
        final LockType type,
        final LockGrantType grantType,
        final Lock useLock,
        final long timeout,
        final long start,
        final long now,
        final DatabaseImpl database,
        final Set<LockInfo> owners,
        final List<LockInfo> waiters) {

        /*
         * getTimeoutInfo synchronizes on the lock table. The timeout exception
         * must be created outside that synchronization block because its ctor
         * invalidates the txn, sometimes synchronizing on the buddy locker.
         * The order of mutex acquisition must always be 1) locker, 2) lock
         * table.
         */
        final TimeoutInfo info = getTimeoutInfo(
            isLockNotTxnTimeout, locker, lsn, type, grantType, useLock,
            timeout, start, now, database, owners, waiters);

        final LockConflictException ex =
            isLockNotTxnTimeout ?
            new LockTimeoutException(locker, info.message) :
            new TransactionTimeoutException(locker, info.message);

        ex.setOwnerTxnIds(getTxnIds(info.owners));
        ex.setWaiterTxnIds(getTxnIds(info.waiters));
        ex.setTimeoutMillis(timeout);

        return ex;
    }

    static class TimeoutInfo {
        final String message;
        final Set<LockInfo> owners;
        final List<LockInfo> waiters;

        TimeoutInfo(final String message,
                    final Set<LockInfo> owners,
                    final List<LockInfo> waiters) {
            this.message = message;
            this.owners = owners;
            this.waiters = waiters;
        }
    }

    /**
     * Create a informative lock or txn timeout message.
     */
    abstract TimeoutInfo getTimeoutInfo(
        boolean isLockNotTxnTimeout,
        Locker locker,
        long lsn,
        LockType type,
        LockGrantType grantType,
        Lock useLock,
        long timeout,
        long start,
        long now,
        DatabaseImpl database,
        Set<LockInfo> owners,
        List<LockInfo> waiters);

    /**
     * Do the real work of creating an lock or txn timeout message.
     */
    TimeoutInfo getTimeoutInfoInternal(
        final boolean isLockNotTxnTimeout,
        final Locker locker,
        final long lsn,
        final LockType type,
        final LockGrantType grantType,
        final Lock useLock,
        final long timeout,
        final long start,
        final long now,
        final DatabaseImpl database,
        final Set<LockInfo> owners,
        final List<LockInfo> waiters) {

        /*
         * Because we're accessing parts of the lock, need to have protected
         * access to the lock table because things can be changing out from
         * underneath us.  This is a big hammer to grab for so long while we
         * traverse the graph, but it's only when we have a deadlock and we're
         * creating a debugging message.
         *
         * The alternative would be to handle ConcurrentModificationExceptions
         * and retry until none of them happen.
         */
        if (lockTableDump) {
            System.out.println("++++++++++ begin lock table dump ++++++++++");
            for (int i = 0; i < nLockTables; i++) {
                boolean success = false;
                for (int j = 0; j < 3; j++) {
                    try {
                        final StringBuilder sb = new StringBuilder();
                        dumpToStringNoLatch(sb, i);
                        System.out.println(sb.toString());
                        success = true;
                        break; // for j...
                    } catch (ConcurrentModificationException CME) {
                        // continue
                    }
                }
                if (!success) {
                    System.out.println("Couldn't dump locktable " + i);
                }
            }
            System.out.println("++++++++++ end lock table dump ++++++++++");
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(isLockNotTxnTimeout ? "Lock" : "Transaction");
        sb.append(" expired. Locker ").append(locker);
        sb.append(": waited for lock");

        if (database != null) {
            sb.append(" on database=").append(database.getDebugName());
        }
        sb.append(" LockAddr:").append(System.identityHashCode(useLock));
        sb.append(" LSN=").append(DbLsn.getNoFormatString(lsn));
        sb.append(" type=").append(type);
        sb.append(" grant=").append(grantType);
        sb.append(" timeoutMillis=").append(timeout);
        sb.append(" startTime=").append(start);
        sb.append(" endTime=").append(now);
        sb.append("\nOwners: ").append(owners);
        sb.append("\nWaiters: ").append(waiters).append("\n");
        return new TimeoutInfo(sb.toString(), owners, waiters);
    }

    private long[] getTxnIds(final Collection<LockInfo> c) {
        final long[] ret = new long[c.size()];
        final Iterator<LockInfo> iter = c.iterator();
        int i = 0;
        while (iter.hasNext()) {
            final LockInfo info = iter.next();
            ret[i++] = info.getLocker().getId();
        }

        return ret;
    }

    /**
     * Release a lock and possibly notify any waiters that they have been
     * granted the lock.
     *
     * @param lsn The LSN of the lock to release.
     *
     * @return true if the lock is released successfully, false if
     * the lock is not currently being held.
     */
    public boolean release(final long lsn, final Locker locker)
        throws DatabaseException {

        final Set<Locker> newOwners =
            releaseAndFindNotifyTargets(lsn, locker);

        if (newOwners == null) {
            return false;
        }

        if (newOwners.size() > 0) {

            /*
             * There is a new set of owners and/or there are restart
             * waiters that should be notified.
             */
            for (Locker newOwner : newOwners) {
                /* Use notifyAll to support multiple threads per txn. */
                synchronized (newOwner) {
                    newOwner.notifyAll();
                }

                assert EnvironmentImpl.maybeForceYield();
            }
        }

        return true;
    }

    /**
     * Release the lock, and return the set of new owners to notify, if any.
     *
     * @return
     * null if the lock does not exist or the given locker was not the owner,
     * a non-empty set if owners should be notified after releasing,
     * an empty set if no notification is required.
     */
    abstract Set<Locker> releaseAndFindNotifyTargets(long lsn,
                                                     Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of releaseAndFindNotifyTargets
     */
    Set<Locker> releaseAndFindNotifyTargetsInternal(final long lsn,
                                                    final Locker locker,
                                                    final int lockTableIndex) {
        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock lock = lockTable.get(lsn);
        if (lock == null) {
            lock = lockTable.get(lsn);
        }

        if (lock == null) {
            /* Lock doesn't exist. */
            return null;
        }

        final Set<Locker> newOwners =
            lock.release(locker, memoryBudget, lockTableIndex);

        if (newOwners == null) {
            /* Not owner. */
            return null;
        }

        /* If it's not in use at all, remove it from the lock table. */
        if ((lock.nWaiters() == 0) &&
            (lock.nOwners() == 0)) {

            lockTable.remove(lsn);

            if (lock.isThin()) {
                memoryBudget.updateLockMemoryUsage
                    (REMOVE_TOTAL_THINLOCKIMPL_OVERHEAD, lockTableIndex);
            } else {
                memoryBudget.updateLockMemoryUsage
                    (REMOVE_TOTAL_LOCKIMPL_OVERHEAD, lockTableIndex);
            }
        } else {

            /**
             * In the deadlock detection process, in order to check that a
             * cycle still exists, we need to detect lock release.
             *
             * During release, we will create a new lock object. During
             * deadlock detection, we compare the lock reference in the cycle
             * with the current lock reference; if they are unequal, the lock
             * was released.
             */
            if (lock.isThin()) {
                lock = new ThinLockImpl((ThinLockImpl)lock);
            } else {
                lock = new LockImpl((LockImpl)lock);
            }

            lockTable.put(lsn, lock);
        }

        return newOwners;
    }

    /**
     * Demote a lock from write to read. Call back to the owning locker to
     * move this to its read collection.
     * @param lsn The lock to release.
     */
    abstract void demote(long lsn, Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of demote.
     */
    void demoteInternal(final long lsn,
                        final Locker locker,
                        final int lockTableIndex) {

        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock useLock = lockTable.get(lsn);
        /* Lock may or may not be currently held. */
        if (useLock != null) {
            useLock.demote(locker);
            locker.moveWriteToReadLock(lsn, useLock);
        }
    }

    /**
     * Test the status of the lock on LSN.  If any transaction holds any
     * lock on it, true is returned.  If no transaction holds a lock on it,
     * false is returned.
     *
     * This method is only used by unit tests.
     *
     * @param lsn The LSN to check.
     * @return true if any transaction holds any lock on the LSN. false
     * if no lock is held by any transaction.
     */
    abstract boolean isLocked(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of isLocked.
     */
    boolean isLockedInternal(final Long lsn, final int lockTableIndex) {

        final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
        final Lock entry = lockTable.get(lsn);
        return (entry != null) && entry.nOwners() != 0;

    }

    /**
     * Return true if this locker owns this a lock of this type on given node.
     */
    abstract boolean isOwner(Long lsn, Locker locker, LockType type)
        throws DatabaseException;

    /**
     * Do the real work of isOwner.
     */
    boolean isOwnerInternal(final Long lsn,
                            final Locker locker,
                            final LockType type,
                            final int lockTableIndex) {

        final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
        final Lock entry = lockTable.get(lsn);
        return entry != null && entry.isOwner(locker, type);
    }

    /**
     * Return true if this locker is waiting on this lock.
     *
     * This method is only used by unit tests.
     */
    abstract boolean isWaiter(Long lsn, Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of isWaiter.
     */
    boolean isWaiterInternal(final Long lsn,
                             final Locker locker,
                             final int lockTableIndex) {

        final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
        final Lock entry = lockTable.get(lsn);
        return entry != null && entry.isWaiter(locker);
    }

    /**
     * Return the number of waiters for this lock.
     */
    abstract int nWaiters(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of nWaiters.
     */
    int nWaitersInternal(final Long lsn, final int lockTableIndex) {

        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock entry = lockTable.get(lsn);
        return entry == null ? -1 : entry.nWaiters();
    }

    /**
     * Return the number of owners of this lock.
     */
    abstract int nOwners(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of nWaiters.
     */
    int nOwnersInternal(final Long lsn, final int lockTableIndex) {

        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock entry = lockTable.get(lsn);
        return entry == null ? -1 : entry.nOwners();
    }

    /**
     * @return the transaction that owns the write lock for this
     */
    abstract Locker getWriteOwnerLocker(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of getWriteOwnerLocker.
     */
    Locker getWriteOwnerLockerInternal(final Long lsn,
                                       final int lockTableIndex) {

        final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        final Lock lock = lockTable.get(lsn);
        if (lock == null) {
            return null;
        } else if (lock.nOwners() > 1) {
            /* not a write lock */
            return null;
        } else {
            return lock.getWriteOwnerLocker();
        }
    }

    /*
     * Check if the locker owns the lock. If the locker owns the lock, this
     * function immediately returns true. If the locker cannot get ownership,
     * according to the arguments, this function will choose to do the
     * following:
     *   get the current owners and waiters of the lock
     *   flush this locker from the set of waiters
     *   
     * Note that the ownership checking and the flushing action should be done
     * in a critical section to prevent any orphaning of the
     * lock -- we must be in a critical section between the time that we check
     * ownership and when we flush any waiters (SR #10103)
     * 
     * Concretely, this function is called in the following places:
     *
     * In waitForLock():
     *     After the wait for timeout. Here, only if txn or lock times out, we
     *     get the owners and waiters which will be used in timeout exception.
     *     If Restart is true, we flush the locker from the waiter list.
     *
     *     Before throwing DeadlockEx or TimeoutEx when txn or lock times out.
     *     Here we only need to flush the locker from waiter list. We do NOT
     *     need to get owners and waiters. This is because DeadlockEx does
     *     not need owners/waiters information at all and TimeoutEx uses the
     *     owners/waiters information which is gotten when locker.wait()
     *     wakes up.
     * 
     *  In notifyVictim():
     *      After the victim notification fails due to timeout. Here we only
     *      need to flush the locker from waiter list to prepare for throwing
     *      DeadlockEx. 
     *      
     *  In checkAndHandleDeadlock():
     *      After we choose the current locker itself as the victim, we want
     *      to throw DeadlockEx to abort this locker.  Here we also only
     *      need to flush the locker from waiter list to prepare for throwing
     *      DeadlockEx. 
     * 
     * The real work is done in the following validateOwnershipInternal().
     *
     * @return true if locker is the owner.
     */
    abstract boolean validateOwnership(Long lsn,
                                       Locker locker,
                                       LockType type,
                                       boolean getOwnersAndWaiters,
                                       boolean flushFromWaiters,
                                       Set<LockInfo> owners,
                                       List<LockInfo> waiters)
        throws DatabaseException;

    boolean validateOwnershipInternal(final Long lsn,
                                      final Locker locker,
                                      final LockType type,
                                      final boolean getOwnersAndWaiters,
                                      final boolean flushFromWaiters,
                                      final int lockTableIndex,
                                      final Set<LockInfo> owners,
                                      final List<LockInfo> waiters) {

        if (isOwnerInternal(lsn, locker, type, lockTableIndex)) {
            return true;
        }
        
        if (getOwnersAndWaiters) {
           /*
            * getOwnersInternal/getWaitersInternal may return null when the
            * lock corresponding to the lsn has already not existed.
            */
            if (owners != null) {
                final Set<LockInfo> localOwners =
                    getOwnersInternal(lsn,  lockTableIndex);

                if (localOwners != null) {
                    owners.addAll(localOwners);
                }
            }   
            
            if (waiters != null) {
                final List<LockInfo> localWaiters =
                    getWaitersInternal(lsn,  lockTableIndex);

                if (localWaiters != null) {
                    waiters.addAll(localWaiters);
                }
            }
        }
        
        if (flushFromWaiters) {
            final Lock entry = lockTables[lockTableIndex].get(lsn);
            if (entry != null) {
                entry.flushWaiter(locker, memoryBudget, lockTableIndex);
            }
        }

        return false;
    }

    public abstract LockAttemptResult stealLock(Long lsn,
                                                Locker locker,
                                                LockType lockType)
        throws DatabaseException;

    LockAttemptResult stealLockInternal(final Long lsn,
                                        final Locker locker,
                                        final LockType lockType,
                                        final int lockTableIndex)
        throws DatabaseException {

        final Lock entry = lockTables[lockTableIndex].get(lsn);
        assert entry != null : "Lock " + DbLsn.getNoFormatString(lsn) + 
                " for txn " + locker.getId() + " does not exist"; 

        /*
         * Note that flushWaiter may do nothing, because the lock may have been
         * granted to our locker after the prior call to attemptLock and before
         * the call to this method.
         */
        entry.flushWaiter(locker, memoryBudget, lockTableIndex);

        /* Remove all owners except for our owner. */
        entry.stealLock(locker, memoryBudget, lockTableIndex);

        /*
         * The lock attempt normally succeeds, but can fail if the lock holder
         * is non-preemptable.
         */
        return attemptLockInternal(
            lsn, locker, lockType, false /*nonBlockingRequest*/,
            false /*jumpAheadOfWaiters*/, lockTableIndex);
    }

    /**
     * Called when a ThreadLocker is created.
     */
    void registerThreadLocker(final ThreadLocker locker) {
        if (threadLockers == null) {
            return;
        }
        final Thread thread = Thread.currentThread();
        final TinyHashSet<ThreadLocker> set = threadLockers.get(thread);
        if (set != null) {
            final boolean added = set.add(locker);
            assert added;
        } else {
            threadLockers.put(thread, new TinyHashSet<>(locker));
        }
    }

    /**
     * Called when a ThreadLocker is closed.
     */
    void unregisterThreadLocker(final ThreadLocker locker) {
        if (threadLockers == null) {
            return;
        }
        final Thread thread = Thread.currentThread();
        final TinyHashSet<ThreadLocker> set = threadLockers.get(thread);
        assert set != null;
        final boolean removed = set.remove(locker);
        assert removed;
        if (threadLockers.size() == 0) {
            threadLockers.remove(thread);
        }
    }

    /**
     * Returns an iterator over all thread lockers for the given thread, or
     * an empty iterator if none.
     */
    Iterator<ThreadLocker> getThreadLockers(final Thread thread) {
        if (threadLockers == null) {
            return EMPTY_THREAD_LOCKERS.iterator();
        }
        final TinyHashSet<ThreadLocker> set = threadLockers.get(thread);
        if (set == null) {
            return EMPTY_THREAD_LOCKERS.iterator();
        }
        return set.iterator();
    }

    /**
     * Statistics
     */
    LockStats lockStat(final StatsConfig config)
        throws DatabaseException {

        final StatGroup latchStats = new StatGroup(
            "Locktable latches", "Shows lock table contention");

        for (int i = 0; i < nLockTables; i++) {
            latchStats.addAll(lockTableLatches[i].getStats());
        }

        /* Dump info about the lock table. */
        final StatGroup tableStats = new StatGroup(
            "Locktable", "The types of locks held in the lock table");

        if (!config.getFast()) {
            dumpLockTable(tableStats, false /*clear*/);
        }
        
        return new LockStats(stats.cloneGroup(config.getClear()),
                             latchStats.cloneGroup(config.getClear()),
                             tableStats.cloneGroup(config.getClear()));
    }

    public StatGroup loadStats(final StatsConfig config) {

        final StatGroup copyStats = stats.cloneGroup(config.getClear());

        final StatGroup latchStats = new StatGroup(
            "Locktable latches", "Shows lock table contention");

        for (int i = 0; i < nLockTables; i++) {
            latchStats.addAll(lockTableLatches[i].getStats());
            if (config.getClear()) {
                lockTableLatches[i].clearStats();
            }
        }

        /* Add all the latch stats to the whole stats group. */
        copyStats.addAll(latchStats);

        final StatGroup tableStats = new StatGroup(
            "Locktable", "The types of locks held in the lock table");

        if (!config.getFast()) {
            dumpLockTable(tableStats, config.getClear());
        }

        /* Add all the lock table stats to the whole stats group. */
        copyStats.addAll(tableStats);
        
        return copyStats;
    }

    /**
     * Dump the lock table to the lock stats.
     */
    abstract void dumpLockTable(StatGroup tableStats, boolean clear)
        throws DatabaseException;

    /**
     * Do the real work of dumpLockTableInternal.
     */
    void dumpLockTableInternal(final StatGroup tableStats,
                               final int i,
                               final boolean clear) {

        final StatGroup oneTable = new StatGroup(
            "Single lock table", "Temporary stat group");

        final IntStat totalLocks = new IntStat(oneTable, LOCK_TOTAL);
        final IntStat waiters = new IntStat(oneTable, LOCK_WAITERS);
        final IntStat owners = new IntStat(oneTable, LOCK_OWNERS);
        final IntStat readLocks = new IntStat(oneTable, LOCK_READ_LOCKS);
        final IntStat writeLocks = new IntStat(oneTable, LOCK_WRITE_LOCKS);

        final Map<Long, Lock> lockTable = lockTables[i];
        totalLocks.add(lockTable.size());

        for (final Lock lock : lockTable.values()) {
            waiters.add(lock.nWaiters());
            owners.add(lock.nOwners());

            /* Go through all the owners for a lock. */
            for (final LockInfo info : lock.getOwnersClone()) {
                if (info.getLockType().isWriteLock()) {
                    writeLocks.increment();
                } else {
                    readLocks.increment();
                }
            }
        }

        tableStats.addAll(oneTable);
    }

    /**
     * Debugging
     */
    public void dump()
        throws DatabaseException {

        System.out.println(dumpToString());
    }

    private String dumpToString()
        throws DatabaseException {

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nLockTables; i++) {
            lockTableLatches[i].acquireExclusive();
            try {
                dumpToStringNoLatch(sb, i);
            } finally {
                lockTableLatches[i].release();
            }
        }
        return sb.toString();
    }

    private void dumpToStringNoLatch(final StringBuilder sb,
                                     final int whichTable) {

        final Map<Long,Lock> lockTable = lockTables[whichTable];

        for (final Map.Entry<Long, Lock> entry : lockTable.entrySet()) {
            final Long lsn = entry.getKey();
            final Lock lock = entry.getValue();

            sb.append("---- LSN: ").
                append(DbLsn.getNoFormatString(lsn)).
                append("----\n");

            sb.append(lock);
            sb.append('\n');
        }
    }
    
    /*
     * Internal class for deadlock detection.
     */
    private class DeadlockChecker {
        
        private final Locker rootLocker;
        private final Long lsn;
        private final LockType rootLocktype;
        
        /**
         * The owners and waiters for the root Lock which will be contained in
         * DeadlockException.
         */
        private Set<LockInfo> ownersForRootLock;
        private List<LockInfo> waitersForRootLock;
        
        private List<CycleNode> cycle = new ArrayList<>();
        
        /**
         * Creates an instance of this class.
         *
         * @param locker the locker which is waiting for the lock
         * @param lsn the lock ID which the locker is waiting for
         * @param locktype the request type for the lock
         */
        DeadlockChecker(final Locker locker,
                        final Long lsn,
                        final LockType locktype) {
            this.rootLocker = locker;
            this.lsn = lsn;
            this.rootLocktype = locktype;
        }
        
        Locker chooseTargetedLocker() {
            /* 
             * Java 8 can directly use method ArrayList.sort
             * (Comparator<? super E> c), but Java 7 does not have this method.
             */
            final CycleNode[] cycleNodeArray = cycle.toArray(new CycleNode[0]);
            final CycleNodeComparator cnc = new CycleNodeComparator();
            Arrays.sort(cycleNodeArray, cnc);
            return cycleNodeArray[getTargetedLockerIndex()].getLocker();
        }
        
        /*
         * This method should guarantee that the same deadlock will return the
         * same Locker index that will be targeted for abort.
         */
        int getTargetedLockerIndex() {
            long sum = 0;
            int nLockers = 0;
            for (final CycleNode cn : cycle) {
                /*
                 *  Sum the Lock pointers (System.identityHashCode(lock))
                 *  rather than the locker IDs and LSNs. Since the
                 *  identityHashCode will change when a Lock is released
                 *  and new Lock is allocated, this will ensure that we don't
                 *  always pick the same victim for the same deadlock, if the
                 *  same deadlock (same locks and lockers) happens repeatedly.
                 */
                sum += System.identityHashCode(cn.getLock());
                nLockers++;
            }

            /*
             *  Note that System.identityHashCode may return a negative value
             *  on AIX, so we use Math.abs() below.
             */
            return (int)(Math.abs(sum) % nLockers); 
        }
        
        boolean hasCycle() {
            getOwnerAndWaitersForRootLocker();
            /*
             * The rootLocker may own several locks, so we do not know
             * which one involves in the deadlock cycle. So we just set
             * the type of lock owned by rootLocker to null.
             */
            return hasCycleInternal(rootLocker, lsn, rootLocktype, null);
        }
        
        boolean hasCycleInternal(final Locker checkedLocker,
                                 final Long lsn,
                                 final LockType requestLocktype,
                                 final LockType ownLockType) {

            final Lock checkedLock;
            final Set<LockInfo> ownersForCheckedLock;
            
            /*
             * When entering this function, we think that the checkedLocker is
             * waiting for the lsn(checkedLock), so we want to continue to get
             * the owners(ownersForCheckedLock) of the lsn. 
             * 
             * But we need to first check whether now the checkedLocker owns
             * the lsn(checkedLock). 
             */
            final int lockTableIndex = getLockTableIndex(lsn);
            synchronized(lockTableLatches[lockTableIndex]) {
                if (isOwnerInternal(lsn, checkedLocker,
                                    requestLocktype, lockTableIndex)) {
                    return false;
                }
                final Map<Long,Lock> lockTable = lockTables[lockTableIndex];
                checkedLock = lockTable.get(lsn);
                ownersForCheckedLock = getOwnersInternal(lsn, lockTableIndex);
            }

            if (ownersForCheckedLock == null) {
                return false;
            }

            /*
             * checkedLock may be null. If so, then ownersForCheckedLock must
             * be null. So if ownersForCheckedLock is non-null, then
             * checkedLock must be non-null.
             */
            final CycleNode cn = new CycleNode(
                checkedLocker, lsn, checkedLock, requestLocktype, ownLockType);

            cycle.add(cn);

            for (final LockInfo info : ownersForCheckedLock) {
                final Locker locker = info.getLocker();
                final LockType ownType = info.getLockType();
                final Long waitsFor = locker.getWaitingFor();
                final LockType requestType = locker.getWaitingForType();
                /*
                 * The constraint "locker != checkedLocker" handles the
                 * following scenario:
                 *     locker owns a read lock and now it wants to acquire a
                 *     write lock, but it needs to wait. Then this locker will
                 *     check for a deadlock and it will check the owner(s) of
                 *     the lock. Because this locker itself is also the owner
                 *     of this lock, it will choose itself as the "new owner"
                 *     and then check the owners of the waitingFor of the
                 *     "new owner". Then it will continue to check itself. This
                 *     will cause java.lang.StackOverflowError
                 *       
                 *  Without this constraint, 
                 *  com.sleepycat.je.DatabaseConfigTest.testConfigConflict will
                 *  get java.lang.StackOverflowError.
                 *     
                 *  With this constraint, we STILL can detect the following
                 *  deadlock:
                 *       locker1                           locker2
                 *       
                 *       hold read lock on lsn
                 *       
                 *                                    hold read lock on lsn
                 *       
                 * acquire write lock(wait_promotion)
                 *       
                 *                           acquire write lock(wait_promotion)
                 */
                if (locker != checkedLocker) {
                    if (locker == rootLocker) {
                        /* Found a cycle */
                        return true;
                    }
                    
                    /*
                     * This handles the situation when a partial cycle exists.
                     * If we do not handle a "partial cycle", a locker may
                     * detect a deadlock, but the true deadlock exists in two
                     * other different lockers. Then, this locker will would
                     * infinitely invoke hasCycleInternal().
                     */
                    for (int index = 0; index < cycle.size(); index++) {
                        if (cycle.get(index).getLocker() == locker) {
                            /* Get partial cycle. It is the true deadlock. */
                            cycle.subList(0, index).clear();
                            return true;
                        }
                    }

                    if (waitsFor != null &&
                        requestType != null &&
                        ownType != null) {

                        if (hasCycleInternal(
                            locker, waitsFor, requestType, ownType)) {
                            return true;
                        }
                    }
                }
            }
            
            cycle.remove(cn);
            return false;        
        }
        
        boolean hasTrueDeadlock() {

            for (final CycleNode cn : cycle) {
                final Lock lock = cn.getLock();
                final Long lsn = cn.getLsn();
                final Lock realtimeLock;

                final int lockTableIndex = getLockTableIndex(lsn);
                synchronized (lockTableLatches[lockTableIndex]) {
                    final Map<Long, Lock> lockTable =
                        lockTables[lockTableIndex];
                    realtimeLock = lockTable.get(lsn);
                }

                if (realtimeLock != lock) {
                    return false;
                }
            }
            return true;
        }
        
        /*
         * In order to get consistent owners and waiters, we cannot call
         * getOwners(lsn) and getWaiters(lsn) separately. We get them
         * atomically while synchronized on the lock table.
         */
        void getOwnerAndWaitersForRootLocker() {
            final int lockTableIndex = getLockTableIndex(lsn);
            synchronized (lockTableLatches[lockTableIndex]) {

                final Set<LockInfo> localOwners =
                    getOwnersInternal(lsn,  lockTableIndex);

                if (localOwners != null) {
                    ownersForRootLock = localOwners;
                }

                final List<LockInfo> localWaiters =
                    getWaitersInternal(lsn,  lockTableIndex);

                if (localWaiters != null) {
                    waitersForRootLock = localWaiters;
                }
            }
        }
        
        Set<LockInfo> getOwnersForRootLock() {
            return ownersForRootLock;
        }
        
        List<LockInfo> getWaitersForRootLock() {
            return waitersForRootLock;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
          
            Lock preLock = null;
            Long preLsn = null;

            for (final CycleNode cn : cycle) {

                final Locker locker = cn.getLocker();
                final Lock lock = cn.getLock();
                final Long lsn = cn.getLsn();
                final LockType requestType = cn.getRequestLockType();
                final LockType ownType = cn.getOwnLockType();

                if (preLock != null) {
                    sb.append("Locker: \"");
                    sb.append(locker).append("\" owns lock: ");
                    sb.append(System.identityHashCode(preLock));
                    sb.append("(LSN: ");
                    sb.append(DbLsn.getNoFormatString(preLsn));
                    sb.append(", ownedType: ").append(ownType).append("). ");
                }

                sb.append("Locker: \"");
                sb.append(locker).append("\" waits for lock: ");
                sb.append(System.identityHashCode(lock)).append("(LSN: ");
                sb.append(DbLsn.getNoFormatString(lsn));
                sb.append(", requestType: ").append(requestType).append(").");
                sb.append("\n");

                preLock = lock;
                preLsn = lsn;
            }
            
            return sb.toString();
        }
        
        class CycleNodeComparator implements Comparator {
            @Override
            public int compare (Object obj1, Object obj2) {
                final CycleNode nc1 = (CycleNode) obj1;
                final CycleNode nc2 = (CycleNode) obj2;
                return (int) (nc1.getLocker().getWaiterThreadId() -
                    nc2.getLocker().getWaiterThreadId());
            }
        }

        /**
         * Represents each node in the cycle.
         */
        class CycleNode {
            
            /**
             * The locker which waits on the lock.
             */
            private final Locker locker;
            
            /**
             * The lsn which represents the lock. It will not change if
             * the lock is released.
             */
            private final Long lsn;
            
            /**
             * The Lock instance. Releasing a lock will re-create a new Lock
             * object. By comparing it with the lock gotten by lsn, we can
             * determine whether the lock was released during the deadlock
             * detection process.
             */
            private final Lock lock;
            
            /* The lock request type. */
            private LockType requestLockType;
            
            /*
             * If this locker is involved in a cycle, is must own some lock.
             */
            private LockType ownLockType;

            CycleNode(final Locker locker,
                      final Long lsn,
                      final Lock lock,
                      final LockType requestLockType,
                      final LockType ownLockType) {
                this.locker = locker;
                this.lsn = lsn;
                this.lock = lock;
                this.requestLockType = requestLockType;
                this.ownLockType = ownLockType;
            }
            
            private Locker getLocker() {
                return locker;
            }
            
            private Long getLsn() {
                return lsn;
            }
            
            private Lock getLock() {
                return lock;
            }
            
            private LockType getRequestLockType() {
                return requestLockType;
            }
            
            private LockType getOwnLockType() {
                return ownLockType;
            }
        }
    }

    /**
     * Returned by waitForLock().
     */
    private static class WaitForLockResult {
        private final Locker targetVictim;
        private final DeadlockChecker dc;
        private final LockAttemptResult result;

        WaitForLockResult(final Locker targetVictim,
                          final DeadlockChecker dc,
                          final LockAttemptResult result) {
            this.targetVictim = targetVictim;
            this.dc = dc;
            this.result = result;
        }

        Locker getVictim() {
            return targetVictim;
        }

        DeadlockChecker getDeadLockChecker() {
            return dc;
        }

        LockAttemptResult getResult() {
            return result;
        }
    }

    /**
     * The result of checking for deadlocks, and handling a deadlock if one
     * is found.
     */
    private static class DeadlockResult {
        private final boolean isOwner;
        private final boolean trueDeadlock;
        private final Locker victim;
        private final DeadlockChecker dc;

        DeadlockResult(
            final boolean isOwner,
            final boolean trueDeadlock,
            final Locker victim,
            final DeadlockChecker dc) {

            this.isOwner = isOwner;
            this.trueDeadlock = trueDeadlock;
            this.victim = victim;
            this.dc = dc;
        }
    }
}
