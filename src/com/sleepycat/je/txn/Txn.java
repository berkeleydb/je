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

import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_TOTAL;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;
import static com.sleepycat.je.utilint.DbLsn.NULL_LSN;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogParams;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.log.entry.AbortLogEntry;
import com.sleepycat.je.log.entry.CommitLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.TxnChain.CompareSlot;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TinyHashSet;
import com.sleepycat.je.utilint.VLSN;

/**
 * A Txn is the internal representation of a transaction created by a call to
 * Environment.txnBegin. This class must support multi-threaded use.
 */
public class Txn extends Locker implements VersionedWriteLoggable {

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    /* Use an AtomicInteger to record cursors opened under this txn. */
    private final AtomicInteger cursors = new AtomicInteger();

    /* Internal txn flags. */
    private byte txnFlags;
    /* Set if prepare() has been called on this transaction. */
    private static final byte IS_PREPARED = 1;
    /* Set if xa_end(TMSUSPEND) has been called on this transaction. */
    private static final byte XA_SUSPENDED = 2;
    /* Set if this rollback() has been called on this transaction. */
    private static final byte PAST_ROLLBACK = 4;

    /*
     * Set if this transaction may abort other transactions holding a needed
     * lock.  Note that this bit flag and the setImportunate method could be
     * removed in favor of overriding getImportunate in ReplayTxn.  This was
     * not done, for now, to avoid changing importunate tests that use a Txn
     * and call setImportunate. [#16513]
     */
    private static final byte IMPORTUNATE = 8;

    /* Holds the public Transaction state. */
    private Transaction.State txnState;

    /* Information about why a Txn was made only abortable. */
    private OperationFailureException onlyAbortableCause;

    /*
     * A Txn can be used by multiple threads. Modification to the read and
     * write lock collections is done by synchronizing on the txn.
     */
    private Set<Long> readLocks; // key is LSN
    private Map<Long, WriteLockInfo> writeInfo; // key is LSN

    /*
     * A set of BuddyLockers that have this locker as their buddy.  Currently
     * this set is only maintained (non-null) in a replicated environment
     * because it is only needed for determining when to throw
     * LockPreemptedException.  If null, it can be assumed that no other
     * thread will change it.  If non-null, access should be synchronized on
     * the buddyLockers object.  TinyHashSet is used because it is optimized
     * for 0 to 2 entries, and normally a Txn will have at most two buddy
     * lockers (for read-committed mode).
     */
    private TinyHashSet<BuddyLocker> buddyLockers;

    private static final int READ_LOCK_OVERHEAD =
        MemoryBudget.HASHSET_ENTRY_OVERHEAD;
    private static final int WRITE_LOCK_OVERHEAD =
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.WRITE_LOCKINFO_OVERHEAD;

    /*
     * We have to keep a set of DatabaseCleanupInfo objects so after commit or
     * abort of Environment.truncateDatabase() or Environment.removeDatabase(),
     * we can appropriately purge the unneeded MapLN and DatabaseImpl.
     * Synchronize access to this set on this object.
     */
    protected Set<DatabaseCleanupInfo> deletedDatabases;

    /*
     * We need a map of the latest databaseImpl objects to drive the undo
     * during an abort, because it's too hard to look up the database object in
     * the mapping tree. (The normal code paths want to take locks, add
     * cursors, etc.
     */
    protected Map<DatabaseId, DatabaseImpl> undoDatabases;

    /**
     * @see #addOpenedDatabase
     * @see HandleLocker
     */
    protected Set<Database> openedDatabaseHandles;

    /*
     * First LSN logged for this transaction -- used for keeping track of the
     * first active LSN point, for checkpointing. This field is not persistent.
     *
     * [#16861] This field is volatile to avoid making getFirstActiveLsn
     * synchronized, which causes a deadlock in HA.
     */
    protected volatile long firstLoggedLsn = NULL_LSN;

    /*
     * Last LSN logged for this transaction. Serves as the handle onto the
     * chained log entries belonging to this transaction. Is persistent.
     */
    protected long lastLoggedLsn = NULL_LSN;

    /*
     * The LSN used to commit the transaction. One of commitLSN or abortLSN
     * must be set after a commit() or abort() operation. Note that a commit()
     * may set abortLSN, if the commit failed, and the transaction had to be
     * aborted.
     */
    protected long commitLsn = NULL_LSN;

    /* The LSN used to record the abort of the transaction. */
    long abortLsn = NULL_LSN;

    /* The configured durability at the time the transaction was created. */
    private Durability defaultDurability;

    /* The durability used for the actual commit. */
    private Durability commitDurability;

    /* Whether to use Serializable isolation (prevent phantoms). */
    private boolean serializableIsolation;

    /* Whether to use Read-Committed isolation. */
    private boolean readCommittedIsolation;

    /*
     * In-memory size, in bytes. A Txn tracks the memory needed for itself and
     * the readlock, writeInfo, undoDatabases, and deletedDatabases
     * collections, including the cost of each collection entry. However, the
     * actual Lock object memory cost is maintained within the Lock class.
     */
    private int inMemorySize;

    /*
     * Accumulated memory budget delta. Once this exceeds ACCUMULATED_LIMIT we
     * inform the MemoryBudget that a change has occurred.
     */
    private int accumulatedDelta = 0;

    /*
     * The set of databases for which triggers were invoked during the
     * course of this transaction. It's null if no triggers were invoked.
     */
    private Set<DatabaseImpl> triggerDbs = null;

    /*
     * The user Transaction handle associated with this Txn. It's null if there
     * isn't one, e.g. it's an internal transaction.
     */
    private Transaction transaction;

    /*
     * Max allowable accumulation of memory budget changes before MemoryBudget
     * should be updated. This allows for consolidating multiple calls to
     * updateXXXMemoryBudget() into one call. Not declared final so that unit
     * tests can modify this. See SR 12273.
     */
    public static int ACCUMULATED_LIMIT = 10000;

    /*
     * Each Txn instance has a handle on a ReplicationContext instance for use
     * in logging a TxnCommit or TxnAbort log entries.
     */
    protected ReplicationContext repContext;

    /*
     * Used to track mixed mode (sync/durability) transaction API usage. When
     * the sync based api is removed, these tracking ivs can be as well.
     */
    private boolean explicitSyncConfigured = false;
    private boolean explicitDurabilityConfigured = false;

    /* Determines whether the transaction is auto-commit */
    private boolean isAutoCommit = false;

    private boolean readOnly;

    /**
     * Constructor for reading from log.
     */
    public Txn() {
        lastLoggedLsn = NULL_LSN;
    }

    protected Txn(EnvironmentImpl envImpl,
                  TransactionConfig config,
                  ReplicationContext repContext) {
        this(envImpl, config, repContext, 0L /*mandatedId */ );
    }

    /**
     * A non-zero mandatedId is specified only by subtypes which arbitrarily
     * impose a transaction id value onto the transaction. This is done by
     * implementing a version of Locker.generateId() which uses the proposed
     * id.
     */
    protected Txn(EnvironmentImpl envImpl,
                  TransactionConfig config,
                  ReplicationContext repContext,
                  long mandatedId)
        throws DatabaseException {

        /*
         * Initialize using the config but don't hold a reference to it, since
         * it has not been cloned.
         */
        super(envImpl, config.getReadUncommitted(), config.getNoWait(),
              mandatedId);
        initTxn(config);
        this.repContext = repContext;
    }

    public static Txn createLocalTxn(EnvironmentImpl envImpl,
                                     TransactionConfig config) {
        return new Txn(envImpl, config, ReplicationContext.NO_REPLICATE);
    }

    public static Txn createLocalAutoTxn(EnvironmentImpl envImpl,
                                         TransactionConfig config) {
        Txn txn = createLocalTxn(envImpl, config);
        txn.isAutoCommit = true;
        return txn;
    }

    /*
     * Make a transaction for a user instigated transaction. Whether the
     * environment is replicated or not determines whether a MasterTxn or
     * a plain local Txn is returned.
     */
    static Txn createUserTxn(EnvironmentImpl envImpl,
                             TransactionConfig config) {

        Txn ret = null;
        try {
            ret = envImpl.isReplicated() ?
                  envImpl.createRepUserTxn(config) :
                  createLocalTxn(envImpl, config);
        } catch (DatabaseException DE) {
            if (ret != null) {
                ret.close(false);
            }
            throw DE;
        }
        return ret;
    }

    static Txn createAutoTxn(EnvironmentImpl envImpl,
                             TransactionConfig config,
                             ReplicationContext repContext)
        throws DatabaseException {

        Txn ret = null;
        try {
            if (envImpl.isReplicated() && repContext.inReplicationStream()) {
                ret = envImpl.createRepUserTxn(config);
            } else {
                ret = new Txn(envImpl, config, repContext);
            }

            ret.isAutoCommit = true;
        } catch (DatabaseException DE) {
            if (ret != null) {
                ret.close(false);
            }
            throw DE;
        }
        return ret;
    }

    @SuppressWarnings("deprecation")
    private void initTxn(TransactionConfig config)
        throws DatabaseException {

        serializableIsolation = config.getSerializableIsolation();
        readCommittedIsolation = config.getReadCommitted();
        defaultDurability = config.getDurability();
        if (defaultDurability == null) {
            explicitDurabilityConfigured = false;
            defaultDurability = config.getDurabilityFromSync(envImpl);
        } else {
            explicitDurabilityConfigured = true;
        }
        explicitSyncConfigured =
            config.getSync() || config.getNoSync() || config.getWriteNoSync();

        assert (!(explicitDurabilityConfigured && explicitSyncConfigured));

        readOnly = config.getReadOnly();

        lastLoggedLsn = NULL_LSN;
        firstLoggedLsn = NULL_LSN;

        txnFlags = 0;
        setState(Transaction.State.OPEN);

        if (envImpl.isReplicated()) {
            buddyLockers = new TinyHashSet<BuddyLocker>();
        }

        txnBeginHook(config);

        /*
         * Note: readLocks, writeInfo, undoDatabases, deleteDatabases are
         * initialized lazily in order to conserve memory. WriteInfo and
         * undoDatabases are treated as a package deal, because they are both
         * only needed if a transaction does writes.
         *
         * When a lock is added to this transaction, we add the collection
         * entry overhead to the memory cost, but don't add the lock
         * itself. That's taken care of by the Lock class.
         */
        updateMemoryUsage(MemoryBudget.TXN_OVERHEAD);

        if (registerImmediately()) {
            this.envImpl.getTxnManager().registerTxn(this);
        }
    }

    /**
     * True if this transaction should be registered with the transaction
     * manager immediately at startup. True for all transactions except for
     * those ReplayTxns which were created as transformed master transactions.
     */
    protected boolean registerImmediately() {
        return true;
    }

    @Override
    void addBuddy(BuddyLocker buddy) {
        if (buddyLockers != null) {
            synchronized (buddyLockers) {
                buddyLockers.add(buddy);
            }
        }
    }

    @Override
    void removeBuddy(BuddyLocker buddy) {
        if (buddyLockers != null) {
            synchronized (buddyLockers) {
                buddyLockers.remove(buddy);
            }
        }
    }

    /**
     * UserTxns get a new unique id for each instance.
     */
    @Override
    @SuppressWarnings("unused")
    protected long generateId(TxnManager txnManager,
                              long ignore /* mandatedId */) {
        return txnManager.getNextTxnId();
    }

    /**
     * Access to last LSN.
     */
    public long getLastLsn() {
        return lastLoggedLsn;
    }

    /**
     *
     * Returns the durability used for the commit operation. It's only
     * available after a commit operation has been initiated.
     *
     * @return the durability associated with the commit, or null if the
     * commit has not yet been initiated.
     */
    public Durability getCommitDurability() {
        return commitDurability;
    }

    /**
     * Returns the durability associated the transaction at the time it's first
     * created.
     *
     * @return the durability associated with the transaction at creation.
     */
    public Durability getDefaultDurability() {
        return defaultDurability;
    }

    public boolean getPrepared() {
        return (txnFlags & IS_PREPARED) != 0;
    }

    public void setPrepared(boolean prepared) {
        if (prepared) {
            txnFlags |= IS_PREPARED;
        } else {
            txnFlags &= ~IS_PREPARED;
        }
    }

    public void setSuspended(boolean suspended) {
        if (suspended) {
            txnFlags |= XA_SUSPENDED;
        } else {
            txnFlags &= ~XA_SUSPENDED;
        }
   }

    public boolean isSuspended() {
        return (txnFlags & XA_SUSPENDED) != 0;
    }

    protected void setRollback() {
        txnFlags |= PAST_ROLLBACK;
    }

    /**
     * @return if this transaction has ever executed a rollback.
     * A Rollback is an undo of the transaction that can return either to the
     * original pre-txn state, or to an intermediate intra-txn state. An abort
     * always returns the txn to the pre-txn state.
     */
    @Override
    public boolean isRolledBack() {
        return (txnFlags & PAST_ROLLBACK) != 0;
    }

    /**
     * Gets a lock on this LSN and, if it is a write lock, saves an abort
     * LSN. Caller will set the abortLsn later, after the write lock has been
     * obtained.
     *
     * @throws IllegalStateException via API read/write methods if the txn is
     * closed, in theory.  However, this should not occur from a user API call,
     * because the API methods first call Transaction.getLocker, which will
     * throw IllegalStateException if the txn is closed.  It might occur,
     * however, if the transaction ends in the window between the call to
     * getLocker and the lock attempt.
     *
     * @throws OperationFailureException via API read/write methods if an
     * OperationFailureException occurred earlier and set the txn to
     * abort-only.
     *
     * @see Locker#lockInternal
     * @Override
     */
    @Override
    protected LockResult lockInternal(long lsn,
                                      LockType lockType,
                                      boolean noWait,
                                      boolean jumpAheadOfWaiters,
                                      DatabaseImpl database)
        throws DatabaseException {

        long timeout = 0;
        boolean useNoWait = noWait || defaultNoWait;
        synchronized (this) {
            checkState(false);
            if (!useNoWait) {
                timeout = getLockTimeout();
            }
        }

        /* Ask for the lock. */
        LockGrantType grant = lockManager.lock
            (lsn, this, lockType, timeout, useNoWait, jumpAheadOfWaiters,
             database);

        WriteLockInfo info = null;
        if (writeInfo != null) {
            if (grant != LockGrantType.DENIED && lockType.isWriteLock()) {
                synchronized (this) {
                    info = writeInfo.get(Long.valueOf(lsn));
                    /* Save the latest version of this database for undoing. */
                    undoDatabases.put(database.getId(), database);
                }
            }
        }

        return new LockResult(grant, info);
    }

    /**
     * Prepare to undo in the (very unlikely) event that logging succeeds but
     * locking fails. Subclasses should call super.preLogWithoutLock. [#22875]
     */
    @Override
    public synchronized void preLogWithoutLock(DatabaseImpl database) {
        ensureWriteInfo();
        undoDatabases.put(database.getId(), database);
    }

    /**
     * @throws IllegalStateException via XAResource
     */
    public synchronized int prepare(Xid xid)
        throws DatabaseException {

        if ((txnFlags & IS_PREPARED) != 0) {
            throw new IllegalStateException
                ("prepare() has already been called for Transaction " +
                 id + ".");
        }

        checkState(false);
        if (checkCursorsForClose()) {
            throw new IllegalStateException
                ("Transaction " + id +
                 " prepare failed because there were open cursors.");
        }

        setPrepared(true);
        envImpl.getTxnManager().notePrepare();
        if (writeInfo == null) {
            return XAResource.XA_RDONLY;
        }

        SingleItemEntry<TxnPrepare> prepareEntry =
            SingleItemEntry.create(LogEntryType.LOG_TXN_PREPARE,
                                   new TxnPrepare(id,xid));
        /* Flush required. */
        LogManager logManager = envImpl.getLogManager();
        logManager.logForceFlush(prepareEntry,
                                 true,  // fsyncrequired
                                 ReplicationContext.NO_REPLICATE);

        return XAResource.XA_OK;
    }

    public void commit(Xid xid)
        throws DatabaseException {

        commit(Durability.COMMIT_SYNC);
        envImpl.getTxnManager().unRegisterXATxn(xid, true);
        return;
    }

    public void abort(Xid xid)
        throws DatabaseException {

        abort(true /* forceFlush */);
        envImpl.getTxnManager().unRegisterXATxn(xid, false);
        return;
    }

    /**
     * Call commit() with the default sync configuration property.
     */
    public long commit()
        throws DatabaseException {

        return commit(defaultDurability);
    }

    /**
     * Commit this transaction; it involves the following logical steps:
     *
     * 1. Run pre-commit hook.
     *
     * 2. Release read locks.
     *
     * 3. Log a txn commit record and flush the log as indicated by the
     * durability policy.
     *
     * 4. Run the post-commit hook.
     *
     * 5. Add deleted LN info to IN compressor queue.
     *
     * 6. Release all write locks
     *
     * If this transaction has not made any changes to the database, that is,
     * it is a read-only transaction, no entry is made to the log. Otherwise,
     * a concerted effort is made to log a commit entry, or an abort entry,
     * but NOT both. If exceptions are encountered and neither entry can be
     * logged, a EnvironmentFailureException is thrown.
     *
     * Error conditions (in contrast to Exceptions) always result in the
     * environment being invalidated and the Error being propagated back to the
     * application.  In addition, if the environment is made invalid in another
     * thread, or the transaction is closed by another thread, then we
     * propagate the exception and we do not attempt to abort.  This special
     * handling is prior to the pre-commit stage.
     *
     * From an exception handling viewpoint the commit goes through two stages:
     * a pre-commit stage spanning steps 1-3, and a post-commit stage
     * spanning steps 4-5. The post-commit stage is entered only after a commit
     * entry has been successfully logged.
     *
     * Any exceptions detected during the pre-commit stage results in an
     * attempt to log an abort entry. A NULL commitLsn (and abortLsn)
     * indicates that we are in the pre-commit stage. Note in particular, that
     * if the log of the commit entry (step 3) fails due to an IOException,
     * then the lower levels are responsible for wrapping it in a
     * EnvironmentFailureException which is propagated directly to the
     * application.
     *
     * Exceptions thrown in the post-commit stage are examined to see if they
     * are expected and must be propagated back to the caller after completing
     * any pending cleanup; some replication exceptions fall into this
     * category. If the exception was unexpected, the environment is
     * invalidated and a EnvironmentFailureException is thrown instead. The
     * current implementation only allows propagation of exceptions from the
     * post-commit hook, since we do not expect exceptions from any of the
     * other post-commit operations.
     *
     * When there are multiple failures in commit(), we want the caller to
     * receive the first exception, to make the problem manifest. So an effort
     * is made to preserve that primary exception and propagate it instead of
     * any following, secondary exceptions. The secondary exception is always
     * logged in such a circumstance.
     *
     * @throws IllegalStateException via Transaction.commit if cursors are
     * open.
     *
     * @throws OperationFailureException via Transaction.commit if an
     * OperationFailureException occurred earlier and set the txn to
     * abort-only.
     *
     * Note that IllegalStateException should never be thrown by
     * Transaction.commit because of a closed txn, since Transaction.commit and
     * abort set the Transaction.txn to null and disallow subsequent method
     * calls (other than abort).  So in a sense the call to checkState(true) in
     * this method is unnecessary, although perhaps a good safeguard.
     */
    public long commit(Durability durability)
        throws DatabaseException {

        /*
         * If frozen, throw the appropriate exception, but don't attempt to
         * make any changes to cleanup the exception.
         */
        checkIfFrozen(true /* isCommit */);

        /*
         * A post commit exception that needs to be propagated back to the
         * caller. Its throw is delayed until the post commit cleanup has been
         * completed.
         */
        DatabaseException queuedPostCommitException = null;

        this.commitDurability = durability;

        try {

            synchronized (this) {
                checkState(false);
                if (checkCursorsForClose()) {
                    throw new IllegalStateException
                        ("Transaction " + id +
                         " commit failed because there were open cursors.");
                }

                /*
                 * Do the pre-commit hook before executing any commit related
                 * actions like releasing locks.
                 */
                if (updateLoggedForTxn()) {
                    preLogCommitHook();
                }

                /*
                 * Release all read locks, clear lock collection. Optimize for
                 * the case where there are no read locks.
                 */
                int numReadLocks = clearReadLocks();

                /*
                 * Log the commit if we ever logged any modifications for this
                 * txn. Refraining from logging empty commits is more efficient
                 * and makes for fewer edge cases for HA. Note that this is not
                 * the same as the question of whether we have held any write
                 * locks. Various scenarios, like RMW txns and
                 * Cursor.putNoOverwrite can take write locks without having
                 * actually made any modifications.
                 *
                 * If we have outstanding write locks, we must release them
                 * even if we won't log a commit.  TODO: This may have been
                 * true in the past because of dbhandle write locks that were
                 * transferred away, but is probably no longer true.
                 */
                int numWriteLocks = 0;
                Collection<WriteLockInfo> obsoleteLsns = null;
                if (writeInfo != null) {
                    numWriteLocks = writeInfo.size();
                    obsoleteLsns = getObsoleteLsnInfo();
                }

                /*
                 * If nothing was written to log for this txn, no need to log a
                 * commit.
                 */
                if (updateLoggedForTxn()) {
                    final LogItem commitItem =
                        logCommitEntry(durability.getLocalSync(),
                                       obsoleteLsns);
                    commitLsn = commitItem.lsn;

                    try {
                        postLogCommitHook(commitItem);
                    } catch (DatabaseException hookException) {
                        if (txnState == Transaction.State.MUST_ABORT) {
                            throw EnvironmentFailureException.
                                unexpectedException
                                ("postLogCommitHook may not set MUST_ABORT",
                                 hookException);
                        }
                        if (!propagatePostCommitException(hookException)) {
                            throw hookException;
                        }
                        queuedPostCommitException = hookException;
                    }
                }

                /*
                 * Set database state for deletes before releasing any write
                 * locks.
                 */
                setDeletedDatabaseState(true);

                /* Release all write locks, clear lock collection. */
                if (numWriteLocks > 0) {
                    releaseWriteLocks();
                }
                writeInfo = null;

                /* Unload delete info, but don't wake up the compressor. */
                if ((deleteInfo != null) && deleteInfo.size() > 0) {
                    envImpl.addToCompressorQueue(deleteInfo.values());
                    deleteInfo.clear();
                }
                traceCommit(numWriteLocks, numReadLocks);
            }

            /*
             * Purge any databaseImpls not needed as a result of the commit. Be
             * sure to do this outside the synchronization block, to avoid
             * conflict w/ checkpointer.
             */
            cleanupDatabaseImpls(true);

            /*
             * Unregister this txn. Be sure to do this outside the
             * synchronization block, to avoid conflict w/ checkpointer.
             */
            close(true);

            if (queuedPostCommitException == null) {
                TriggerManager.runCommitTriggers(this);
                return commitLsn;
            }
        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;
        } catch (RuntimeException commitException) {
            if (!envImpl.isValid()) {
                /* Env is invalid, propagate exception. */
                throw commitException;
            }
            if (commitLsn != NULL_LSN) {
                /* An unfiltered post commit exception */
                throw new EnvironmentFailureException
                    (envImpl,
                     EnvironmentFailureReason.LOG_INCOMPLETE,
                     "Failed after commiting transaction " +
                     id +
                     " during post transaction cleanup." +
                     "Original exception = " +
                     commitException.getMessage(),
                     commitException);
            }

            /*
             * If this transaction is frozen, just bail out, and don't try
             * to clean up with an abort.
             */
            checkIfFrozen(true);
            throwPreCommitException(durability, commitException);
        } finally {

            /*
             * Final catch-all to ensure state is set, in case close(boolean)
             * is not called.
             */
            if (txnState == Transaction.State.OPEN) {
                setState(Transaction.State.COMMITTED);
            }
        }
        throw queuedPostCommitException;
    }

    /**
     * Releases all write locks, nulls the lock collection.
     */
    protected void releaseWriteLocks() throws DatabaseException {
        if (writeInfo == null) {
            return;
        }
        for (Long lsn : writeInfo.keySet()) {
            lockManager.release(lsn, this);
        }
        writeInfo = null;
    }

    /**
     * Aborts the current transaction and throws the pre-commit Exception,
     * wrapped in a Database exception if it isn't already a DatabaseException.
     *
     * If the attempt at writing the abort entry fails, that is, if neither an
     * abort entry, nor a commit entry was successfully written to the log, the
     * environment is invalidated and a EnvironmentFailureException is thrown.
     * Note that for HA, it's necessary that either a commit or abort entry be
     * made in the log, so that it can be replayed to the replicas and the
     * transaction is not left in limbo at the other nodes.
     *
     * @param durability used to determine whether the abort record should be
     * flushed to the log.
     * @param preCommitException the exception being handled.
     * @throws DatabaseException this is the normal return for the method.
     */
    private void throwPreCommitException(Durability durability,
                                         RuntimeException preCommitException) {

        try {
            abortInternal(durability.getLocalSync() == SyncPolicy.SYNC);
            LoggerUtils.traceAndLogException(envImpl, "Txn", "commit",
                                             "Commit of transaction " + id +
                                             " failed", preCommitException);
        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;
        } catch (RuntimeException abortT2) {
            if (!envImpl.isValid()) {
                /* Env already invalid, propagate exception. */
                throw abortT2;
            }
            String message = "Failed while attempting to commit transaction " +
                    id + ". The attempt to abort also failed. " +
                    "The original exception seen from commit = " +
                    preCommitException.getMessage() +
                    " The exception from the cleanup = " +
                    abortT2.getMessage();
            if ((writeInfo != null) && (abortLsn == NULL_LSN)) {
                /* Failed to log an abort or commit entry */
                throw new EnvironmentFailureException
                    (envImpl,
                     EnvironmentFailureReason.LOG_INCOMPLETE,
                     message, preCommitException);
            }

            /*
             * An abort entry has been written, so we can proceed. Log the
             * secondary exception, but throw the more meaningful original
             * exception.
             */
            LoggerUtils.envLogMsg(Level.WARNING, envImpl, message);
            /* The preCommitException exception will be thrown below. */
        }
        postLogCommitAbortHook();

        /*
         * Abort entry was written, wrap the exception if necessary and throw
         * it.  An IllegalStateException is thrown by commit() when cursors are
         * open.
         */
        if (preCommitException instanceof DatabaseException ||
            preCommitException instanceof IllegalStateException) {
            throw preCommitException;
        }

        /* Now throw an exception that shows the commit problem. */
        throw EnvironmentFailureException.unexpectedException
            ("Failed while attempting to commit transaction " +
              id + ", aborted instead. Original exception = " +
              preCommitException.getMessage(),
              preCommitException);
    }

    /**
     * Creates and logs the txn commit entry, enforcing the flush/Sync
     * behavior.
     *
     * @param flushSyncBehavior the local durability requirements
     *
     * @return the committed log item
     *
     * @throws DatabaseException
     */
    private LogItem logCommitEntry(SyncPolicy flushSyncBehavior,
                                   Collection<WriteLockInfo> obsoleteLsns)
        throws DatabaseException {

        LogManager logManager = envImpl.getLogManager();
        assert checkForValidReplicatorNodeId();

        final CommitLogEntry commitEntry =
            new CommitLogEntry(new TxnCommit(id,
                                             lastLoggedLsn,
                                             getReplicatorNodeId(),
                                             getDTVLSN()));

        LogParams params = new LogParams();
        params.entry = commitEntry;
        params.provisional = Provisional.NO;
        params.repContext = repContext;

        params.obsoleteWriteLockInfo = obsoleteLsns;

        switch (flushSyncBehavior) {

            case SYNC:
                params.flushRequired = true;
                params.fsyncRequired = true;
                break;

            case WRITE_NO_SYNC:
                params.flushRequired = true;
                params.fsyncRequired = false;
                break;

            default:
                params.flushRequired = false;
                params.fsyncRequired = false;
                break;
        }

        /*
         * Do a final pre-log check just before the logging call, to minimize
         * the window where the POSSIBLY_COMMITTED state may be set. [#21264]
         */
        preLogCommitCheck();

        /* Log the commit with requested durability. */
        boolean logSuccess = false;
        try {
            LogItem item = logManager.log(params);
            logSuccess = true;
            return item;
        } catch (RuntimeException e) {

            /*
             * Exceptions thrown during logging are expected to be fatal.
             * Ensure that the environment is invalidated when a non-fatal
             * exception is unexpectedly thrown, since the commit durability is
             * unknown [#21264].
             */
            if (envImpl.isValid()) {
                throw EnvironmentFailureException.unexpectedException
                    (envImpl,
                     "Unexpected non-fatal exception while logging commit",
                     e);
            }
            throw e;
        } catch (Error e) {
            /* Ensure that the environment is invalidated. [#21264] */
            envImpl.invalidate(e);
            throw e;
        } finally {

            /*
             * If logging fails, there is still a possibility that the commit
             * is durable. [#21264]
             */
            if (!logSuccess) {
                setState(Transaction.State.POSSIBLY_COMMITTED);
            }
        }
    }

    /**
     * Pre-log check for an invalid environment or interrupted thread (this
     * thread may have been interrupted but we haven't found out yet, because
     * we haven't done a wait or an I/O) to narrow the time window where a
     * commit could become partially durable.  See getPartialDurability.
     * [#21264]
     */
    private void preLogCommitCheck() {
        if (Thread.interrupted()) {
            throw new ThreadInterruptedException
                (envImpl, "Thread interrupted prior to logging the commit");
        }
        envImpl.checkIfInvalid();
    }

    /*
     * A replicated txn must know the node of the master which issued it.
     */
    private boolean checkForValidReplicatorNodeId() {
        if (isReplicated()) {
            if (getReplicatorNodeId() == 0) {
                return false;
            }

            /*
            return (repContext.getClientVLSN() != null) &&
                   (!repContext.getClientVLSN().isNull());
                   */
        }
        return true;
    }

    /**
     * Extract obsolete LSN info from writeInfo. Do not add a WriteInfo if a
     * slot with a deleted LN was reused (abortKnownDeleted), to avoid double
     * counting. And count each abortLSN only once.
     */
    private Collection<WriteLockInfo> getObsoleteLsnInfo() {

        /*
         * A Map is used to prevent double counting abortLNS if there is more
         * then one WriteLockInfo with the same abortLSN in this txn, which can
         * occur when the txn has performed more than one CUD ops on the same
         * record.
         */
        Map<Long, WriteLockInfo> map = new HashMap<Long, WriteLockInfo>();

        for (WriteLockInfo info : writeInfo.values()) {
            maybeCountObsoleteLSN(map, info);
        }

        return map.values();
    }

    private void maybeCountObsoleteLSN(
        Map<Long, WriteLockInfo> obsoleteLsnSet,
        WriteLockInfo info) {

        if (info.getAbortLsn() == DbLsn.NULL_LSN ||
            info.getAbortKnownDeleted()) {
            return;
        }

        if ((info.getDb() != null) &&
            info.getDb().isLNImmediatelyObsolete()) {
            /* Was already counted obsolete during logging. */
            return;
        }

        if (info.getAbortData() != null) {
            /* Was already counted obsolete during logging. */
            return;
        }

        final Long longLsn = Long.valueOf(info.getAbortLsn());

        if (!obsoleteLsnSet.containsKey(longLsn)) {
            obsoleteLsnSet.put(longLsn, info);
        }
    }

    /**
     * Abort this transaction. This flavor does not return an LSN, nor does it
     * require the logging of a durable abort record.
     */
    public void abort()
        throws DatabaseException {

        if (isClosed()) {
            return;
        }
        abort(false /* forceFlush */);
    }

    /**
     * Abort this transaction. Steps are:
     * 1. Release LN read locks.
     * 2. Write a txn abort entry to the log. This is used for log file
     *    cleaning optimization and replication, and there's no need to
     *    guarantee a flush to disk.
     * 3. Find the last LN log entry written for this txn, and use that
     *    to traverse the log looking for nodes to undo. For each node,
     *    use the same undo logic as recovery to undo the transaction. Note
     *    that we walk the log in order to undo in reverse order of the
     *    actual operations. For example, suppose the txn did this:
     *       delete K1/D1 (in LN 10)
     *       create K1/D1 (in LN 20)
     *    If we process LN10 before LN 20, we'd inadvertently create a
     *    duplicate tree of "K1", which would be fatal for the mapping tree.
     * 4. Release the write lock for this LN.
     *
     * An abort differs from a rollback in that the former always undoes every
     * operation, and returns it to the pre-txn state. A rollback may return
     * the txn to an intermediate state, or to the pre-txn state.
     */
    public long abort(boolean forceFlush)
        throws DatabaseException {

        return abortInternal(forceFlush);
    }

    /**
     * @throws IllegalStateException via Transaction.abort if cursors are open.
     *
     * Note that IllegalStateException should never be thrown by
     * Transaction.abort because of a closed txn, since Transaction.commit and
     * abort set the Transaction.txn to null and disallow subsequent method
     * calls (other than abort).  So in a sense the call to checkState(true) in
     * this method is unnecessary, although perhaps a good safeguard.
     */
    private long abortInternal(boolean forceFlush)
        throws DatabaseException {

        /*
         * If frozen, throw the appropriate exception, but don't attempt to
         * make any changes to cleanup the exception.
         */
        boolean hooked = false;
        checkIfFrozen(false);

        try {
            try {
                synchronized (this) {
                    checkState(true);

                    /*
                     * State is set to ABORTED before undo, so that other
                     * threads cannot access this txn in the middle of undo.
                     * [#19321]
                     */
                    setState(Transaction.State.ABORTED);

                    /* Log the abort. */
                    if (updateLoggedForTxn()) {
                        preLogAbortHook();
                        hooked = true;
                        assert checkForValidReplicatorNodeId();
                        assert (commitLsn == NULL_LSN) &&
                               (abortLsn == NULL_LSN);
                        final AbortLogEntry abortEntry =
                            new AbortLogEntry(
                                new TxnAbort(id, lastLoggedLsn,
                                             getReplicatorNodeId(),
                                             getDTVLSN()));
                        abortLsn = forceFlush ?
                            envImpl.getLogManager().
                            logForceFlush(abortEntry,
                                          true /* fsyncRequired */,
                                          repContext) :
                            envImpl.getLogManager().log(abortEntry,
                                                        repContext);
                    }
                }
            } finally {
                if (hooked) {
                    postLogAbortHook();
                    hooked = false;
                }

                /*
                 * undo must be called outside the synchronization block to
                 * preserve locking order: For non-blocking locks, the BIN
                 * is latched before synchronizing on the Txn.  If we were
                 * to synchronize while calling undo, this order would be
                 * reversed.
                 */
                undo();
            }

            /*
             * Purge any databaseImpls not needed as a result of the abort. Be
             * sure to do this outside the synchronization block, to avoid
             * conflict w/ checkpointer.
             */
            cleanupDatabaseImpls(false);

            synchronized (this) {
                boolean openCursors = checkCursorsForClose();
                Logger logger = envImpl.getLogger();
                if (logger.isLoggable(Level.FINE)) {
                    LoggerUtils.fine(logger, envImpl,
                                     "Abort: id = " + id + " openCursors= " +
                                     openCursors);
                }

                /* Invalidate any Db handles protected by this txn. */
                if (openedDatabaseHandles != null) {
                    for (Database handle : openedDatabaseHandles) {
                        DbInternal.invalidate(handle);
                    }
                }
                /* Delay the exception until cleanup is complete. */
                if (openCursors) {
                    envImpl.checkIfInvalid();
                    throw new IllegalStateException
                            ("Transaction " + id +
                             " detected open cursors while aborting");
                }
            }
        } finally {

            /*
             * The close method, which unregisters the txn, and must be called
             * after undo and cleanupDatabaseImpls.  A transaction must remain
             * registered until all actions that modify/dirty INs are complete;
             * see Checkpointer class comments for details.  [#19321]
             *
             * close must be called, even though the state has already been set
             * to ABORTED above, for two reasons: 1) To unregister the txn, and
             * 2) to allow subclasses to override the close method.
             *
             * close must be called outside the synchronization block to avoid
             * conflict w/ checkpointer.
             */
            close(false);

            if (abortLsn != NULL_LSN) {
                TriggerManager.runAbortTriggers(this);
            }
        }

        return abortLsn;
    }

    /**
     * Undo write operations and release all resources held by the transaction.
     */
    protected void undo()
        throws DatabaseException {

        /*
         * We need to undo, or reverse the effect of any applied operations on
         * the in-memory btree. We also need to make the latest version of any
         * record modified by the transaction obsolete.
         */
        Set<Long> alreadyUndoneLsns = new HashSet<Long>();
        Set<CompareSlot> alreadyUndoneSlots = new TreeSet<CompareSlot>();
        TreeLocation location = new TreeLocation();
        long undoLsn = lastLoggedLsn;

        try {
            while (undoLsn != NULL_LSN) {
                UndoReader undo =
                    UndoReader.create(envImpl, undoLsn, undoDatabases);
                /*
                 * Only undo the first instance we see of any node. All log
                 * entries for a given node have the same abortLsn, so we don't
                 * need to undo it multiple times.
                 */
                if (firstInstance(
                    alreadyUndoneLsns, alreadyUndoneSlots, undo)) {

                    RecoveryManager.abortUndo(
                        envImpl.getLogger(), Level.FINER, location,
                        undo.db, undo.logEntry, undoLsn);

                    countObsoleteExact(undoLsn, undo, isRolledBack());
                }

                /* Move on to the previous log entry for this txn. */
                undoLsn = undo.logEntry.getUserTxn().getLastLsn();
            }
        } catch (DatabaseException e) {
            String lsnMsg = "LSN=" + DbLsn.getNoFormatString(undoLsn);
            LoggerUtils.traceAndLogException(envImpl, "Txn", "undo",
                                             lsnMsg, e);
            e.addErrorMessage(lsnMsg);
            throw e;
        } catch (RuntimeException e) {
            throw EnvironmentFailureException.unexpectedException
                ("Txn undo for LSN=" + DbLsn.getNoFormatString(undoLsn), e);
        }

        /*
         * Release all read locks after the undo (since the undo may need to
         * read in mapLNs).
         */
        if (readLocks != null) {
            clearReadLocks();
        }

        /* Set database state for deletes before releasing any write locks. */
        setDeletedDatabaseState(false);

        /* Throw away write lock collection, don't retain any locks. */
        Set<Long> empty = Collections.emptySet();
        clearWriteLocks(empty);

        /*
         * Let the delete related info (binreferences and dbs) get gc'ed. Don't
         * explicitly iterate and clear -- that's far less efficient, gives GC
         * wrong input.
         */
        deleteInfo = null;
    }

    /**
     * For an explanation of obsoleteDupsAllowed, see ReplayTxn.rollback.
     */
    private void countObsoleteExact(long undoLsn, UndoReader undo,
                                    boolean obsoleteDupsAllowed) {
        /*
         * "Immediately obsolete" LNs are counted as obsolete when they are
         * logged, so no need to repeat here.
         */
        if (undo.logEntry.isImmediatelyObsolete(undo.db)) {
            return;
        }

        LogManager logManager = envImpl.getLogManager();

        if (obsoleteDupsAllowed) {
            logManager.countObsoleteNodeDupsAllowed
                (undoLsn,
                 null, // type
                 undo.logEntrySize,
                 undo.db);
        } else {
            logManager.countObsoleteNode(undoLsn,
                                         null,  // type
                                         undo.logEntrySize,
                                         undo.db,
                                         true); // countExact
        }
    }

    /**
     * Release any write locks that are not in the retainedNodes set.
     */
    protected void clearWriteLocks(Set<Long> retainedNodes)
        throws DatabaseException {

        if (writeInfo == null) {
            return;
        }

        /* Release all write locks, clear lock collection. */
        Iterator<Map.Entry<Long, WriteLockInfo>> iter =
            writeInfo.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, WriteLockInfo> entry = iter.next();
            Long lsn = entry.getKey();

            /* Release any write locks not in the retained set. */
            if (!retainedNodes.contains(lsn)) {
                lockManager.release(lsn, this);
                iter.remove();
            }
        }

        if (writeInfo.size() == 0) {
            writeInfo = null;
        }
    }

    protected int clearReadLocks()
        throws DatabaseException {

        int numReadLocks = 0;
        if (readLocks != null) {
            numReadLocks = readLocks.size();
            Iterator<Long> iter = readLocks.iterator();
            while (iter.hasNext()) {
                Long rLockNid = iter.next();
                lockManager.release(rLockNid, this);
            }
            readLocks = null;
        }
        return numReadLocks;
    }

    /**
     * Called by LNLogEntry.postLogWork() via the LogManager (while still under
     * the LWL) after a transactional LN is logged. Also called by the recovery
     * manager when logging a transaction aware object.
     *
     * This method is synchronized by the caller, by being called within the
     * log latch. Record the last LSN for this transaction, to create the
     * transaction chain, and also record the LSN in the write info for abort
     * logic.
     */
    public synchronized void addLogInfo(long lastLsn) {
        /* Save the last LSN for maintaining the transaction LSN chain. */
        lastLoggedLsn = lastLsn;

        /*
         * Save handle to LSN for aborts.
         *
         * If this is the first LSN, save it for calculating the first LSN
         * of any active txn, for checkpointing.
         */
        if (firstLoggedLsn == NULL_LSN) {
            firstLoggedLsn = lastLsn;
        }
    }

    /**
     * [#16861] The firstLoggedLsn field is volatile to avoid making
     * getFirstActiveLsn synchronized, which causes a deadlock in HA.
     *
     * @return first logged LSN, to aid recovery undo
     */
    public long getFirstActiveLsn() {
        return firstLoggedLsn;
    }

    /**
     * @return true if this txn has logged any log entries.
     */
    protected boolean updateLoggedForTxn() {
        return (lastLoggedLsn != DbLsn.NULL_LSN);
    }

    /**
     * @param dbImpl databaseImpl to remove
     * @param deleteAtCommit true if this databaseImpl should be cleaned on
     * commit, false if it should be cleaned on abort.
     */
    @Override
    public synchronized void markDeleteAtTxnEnd(DatabaseImpl dbImpl,
                                                boolean deleteAtCommit) {
        int delta = 0;
        if (deletedDatabases == null) {
            deletedDatabases = new HashSet<DatabaseCleanupInfo>();
            delta += MemoryBudget.HASHSET_OVERHEAD;
        }

        deletedDatabases.add(new DatabaseCleanupInfo(dbImpl,
                                                     deleteAtCommit));
        delta += MemoryBudget.HASHSET_ENTRY_OVERHEAD +
            MemoryBudget.OBJECT_OVERHEAD;
        updateMemoryUsage(delta);

        /* releaseDb will be called by cleanupDatabaseImpls. */
    }

    public Set<DatabaseCleanupInfo> getDeletedDatabases() {
        return deletedDatabases;
    }

    /*
     * Leftover databaseImpls that are a by-product of database operations like
     * removeDatabase(), truncateDatabase() will be deleted after the write
     * locks are released. However, do set the database state appropriately
     * before the locks are released.
     */
    protected void setDeletedDatabaseState(boolean isCommit) {
        if (deletedDatabases != null) {
            Iterator<DatabaseCleanupInfo> iter = deletedDatabases.iterator();
            while (iter.hasNext()) {
                DatabaseCleanupInfo info = iter.next();
                if (info.deleteAtCommit == isCommit) {
                    info.dbImpl.startDeleteProcessing();
                }
            }
        }
    }

    /**
     * Cleanup leftover databaseImpls that are a by-product of database
     * operations like removeDatabase(), truncateDatabase().
     *
     * This method must be called outside the synchronization on this txn,
     * because it calls finishDeleteProcessing, which gets the TxnManager's
     * allTxns latch. The checkpointer also gets the allTxns latch, and within
     * that latch, needs to synchronize on individual txns, so we must avoid a
     * latching hiearchy conflict.
     *
     * [#16861] FUTURE: Perhaps this special handling is no longer needed, now
     * that firstLoggedLsn is volatile and getFirstActiveLsn is not
     * synchronized.
     */
    protected void cleanupDatabaseImpls(boolean isCommit)
        throws DatabaseException {

        if (deletedDatabases != null) {
            /* Make a copy of the deleted databases while synchronized. */
            DatabaseCleanupInfo[] infoArray;
            synchronized (this) {
                infoArray = new DatabaseCleanupInfo[deletedDatabases.size()];
                deletedDatabases.toArray(infoArray);
            }
            for (DatabaseCleanupInfo info : infoArray) {
                if (info.deleteAtCommit == isCommit) {

                    /*
                     * If deletedDatabases contains same databases with
                     * different deleteAtCommit, firstly release the database,
                     * then delete it. [#19636]
                     */
                    if (checkRepeatedDeletedDB(infoArray, info)) {
                        envImpl.getDbTree().releaseDb(info.dbImpl);
                    }
                    /* releaseDb will be called by finishDeleteProcessing. */
                    info.dbImpl.finishDeleteProcessing();
                } else if(!checkRepeatedDeletedDB(infoArray, info)){

                    /*
                     * If deletedDatabases contains same databases with
                     * different deleteAtCommit, do nothing. [#19636]
                     */
                    envImpl.getDbTree().releaseDb(info.dbImpl);
                }
            }
            deletedDatabases = null;
        }
    }

    private boolean checkRepeatedDeletedDB(DatabaseCleanupInfo[] infoArray,
                                           DatabaseCleanupInfo info) {
        for (DatabaseCleanupInfo element : infoArray) {
            if (element.dbImpl.getId().equals(info.dbImpl.getId()) &&
                element.deleteAtCommit != info.deleteAtCommit){
                    return true;
            }
        }
        return false;
    }

    private synchronized void ensureWriteInfo() {
        if (writeInfo == null) {
            writeInfo = new HashMap<Long, WriteLockInfo>();
            undoDatabases = new HashMap<DatabaseId, DatabaseImpl>();
            updateMemoryUsage(MemoryBudget.TWOHASHMAPS_OVERHEAD);
        }
    }

    /**
     * Add lock to the appropriate queue.
     */
    @Override
    protected synchronized void addLock(Long lsn,
                                        LockType type,
                                        LockGrantType grantStatus) {
        if (type.isWriteLock()) {

            ensureWriteInfo();
            writeInfo.put(lsn, new WriteLockInfo());

            int delta = WRITE_LOCK_OVERHEAD;

            if ((grantStatus == LockGrantType.PROMOTION) ||
                (grantStatus == LockGrantType.WAIT_PROMOTION)) {
                readLocks.remove(lsn);
                delta -= READ_LOCK_OVERHEAD;
            }
            updateMemoryUsage(delta);
        } else {
            addReadLock(lsn);
        }
    }

    private void addReadLock(Long lsn) {
        int delta = 0;
        if (readLocks == null) {
            readLocks = new HashSet<Long>();
            delta = MemoryBudget.HASHSET_OVERHEAD;
        }

        readLocks.add(lsn);
        delta += READ_LOCK_OVERHEAD;
        updateMemoryUsage(delta);
    }

    /**
     * Remove the lock from the set owned by this transaction. If specified to
     * LockManager.release, the lock manager will call this when its releasing
     * a lock. Usually done because the transaction doesn't need to really keep
     * the lock, i.e for a deleted record.
     */
    @Override
    protected
    synchronized void removeLock(long lsn) {

        /*
         * We could optimize by passing the lock type so we know which
         * collection to look in. Be careful of demoted locks, which have
         * shifted collection.
         *
         * Don't bother updating memory utilization here -- we'll update at
         * transaction end.
         */
        if ((readLocks != null) &&
            readLocks.remove(lsn)) {
            updateMemoryUsage(0 - READ_LOCK_OVERHEAD);
        } else if ((writeInfo != null) &&
                   (writeInfo.remove(lsn) != null)) {
            updateMemoryUsage(0 - WRITE_LOCK_OVERHEAD);
        }
    }

    /**
     * A lock is being demoted. Move it from the write collection into the read
     * collection.
     */
    @Override
    @SuppressWarnings("unused")
    synchronized void moveWriteToReadLock(long lsn, Lock lock) {

        boolean found = false;
        if ((writeInfo != null) &&
            (writeInfo.remove(lsn) != null)) {
            found = true;
            updateMemoryUsage(0 - WRITE_LOCK_OVERHEAD);
        }

        assert found : "Couldn't find lock for Node " + lsn +
            " in writeInfo Map.";
        addReadLock(lsn);
    }

    private void updateMemoryUsage(int delta) {
        inMemorySize += delta;
        accumulatedDelta += delta;
        if (accumulatedDelta > ACCUMULATED_LIMIT ||
            accumulatedDelta < -ACCUMULATED_LIMIT) {
            envImpl.getMemoryBudget().updateTxnMemoryUsage(accumulatedDelta);
            accumulatedDelta = 0;
        }
    }

    /**
     * Returns the amount of memory currently budgeted for this transaction.
     */
    int getBudgetedMemorySize() {
        return inMemorySize - accumulatedDelta;
    }

    /**
     * @return the WriteLockInfo for this node.
     */
    @Override
    public WriteLockInfo getWriteLockInfo(long lsn) {
        WriteLockInfo wli = null;
        synchronized (this) {
            if (writeInfo != null) {
                wli = writeInfo.get(lsn);
            }
        }

        if (wli == null) {
            throw EnvironmentFailureException.unexpectedState
                ("writeInfo is null in Txn.getWriteLockInfo");
        }
        return wli;
    }

    /**
     * Is always transactional.
     */
    @Override
    public boolean isTransactional() {
        return true;
    }

    /**
     * Determines whether this is an auto transaction.
     */
    public boolean isAutoTxn() {
        return isAutoCommit;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Is serializable isolation if so configured.
     */
    @Override
    public boolean isSerializableIsolation() {
        return serializableIsolation;
    }

    /**
     * Is read-committed isolation if so configured.
     */
    @Override
    public boolean isReadCommittedIsolation() {
        return readCommittedIsolation;
    }

    /**
     * Returns true if the sync api was used for configuration
     */
    public boolean getExplicitSyncConfigured() {
        return explicitSyncConfigured;
    }

    /**
     * Returns true if the durability api was used for configuration.
     */
    public boolean getExplicitDurabilityConfigured() {
        return explicitDurabilityConfigured;
    }

    /**
     * This is a transactional locker.
     */
    @Override
    public Txn getTxnLocker() {
        return this;
    }

    /**
     * Returns 'this', since this locker holds no non-transactional locks.
     * Since this is returned, sharing of locks is obviously supported.
     */
    @Override
    public Locker newNonTxnLocker() {
        return this;
    }

    /**
     * This locker holds no non-transactional locks.
     */
    @Override
    public void releaseNonTxnLocks() {
    }

    /**
     * Created transactions do nothing at the end of the operation.
     */
    @Override
    public void nonTxnOperationEnd() {
    }

    /*
     * @see com.sleepycat.je.txn.Locker#operationEnd(boolean)
     */
    @Override
    public void operationEnd(boolean operationOK)
        throws DatabaseException {

        if (!isAutoCommit) {
            /* Created transactions do nothing at the end of the operation. */
            return;
        }

        if (operationOK) {
            commit();
        } else {
            abort(false); // no sync required
        }
    }

    /**
     * Called at the end of a database open operation to add the database
     * handle to a user txn.  When a user txn aborts, handles opened using that
     * txn are invalidated.
     *
     * A non-txnal locker or auto-commit txn does not retain the handle,
     * because the open database operation will succeed or fail atomically and
     * no database invalidation is needed at a later time.
     *
     * @see HandleLocker
     */
    @Override
    public synchronized void addOpenedDatabase(Database dbHandle) {
        if (isAutoCommit) {
            return;
        }
        if (openedDatabaseHandles == null) {
            openedDatabaseHandles = new HashSet<Database>();
        }
        openedDatabaseHandles.add(dbHandle);
    }

    /**
     * Increase the counter if a new Cursor is opened under this transaction.
     */
    @Override
    @SuppressWarnings("unused")
    public void registerCursor(CursorImpl cursor) {
        cursors.getAndIncrement();
    }

    /**
     * Decrease the counter if a Cursor is closed under this transaction.
     */
    @Override
    @SuppressWarnings("unused")
    public void unRegisterCursor(CursorImpl cursor) {
        cursors.getAndDecrement();
    }

    /*
     * Txns always require locking.
     */
    @Override
    public boolean lockingRequired() {
        return true;
    }

    /**
     * Check if all cursors associated with the txn are closed. If not, those
     * open cursors will be forcibly closed.
     * @return true if open cursors exist
     */
    private boolean checkCursorsForClose() {
      return (cursors.get() != 0);
    }

    /**
     * stats
     */
    @Override
    public StatGroup collectStats() {
        StatGroup stats =
            new StatGroup("Transaction lock counts" ,
                          "Read and write locks held by transaction " + id);

        IntStat statReadLocks = new IntStat(stats, LOCK_READ_LOCKS);
        IntStat statWriteLocks = new IntStat(stats, LOCK_WRITE_LOCKS);
        IntStat statTotalLocks = new IntStat(stats, LOCK_TOTAL);

        synchronized (this) {
            int nReadLocks = (readLocks == null) ? 0 : readLocks.size();
            statReadLocks.add(nReadLocks);
            int nWriteLocks = (writeInfo == null) ? 0 : writeInfo.size();
            statWriteLocks.add(nWriteLocks);
            statTotalLocks.add(nReadLocks + nWriteLocks);
        }

        return stats;
    }

    /**
     * Set the state of a transaction to abort-only.  Should ONLY be called
     * by OperationFailureException.
     */
    @Override
    public void setOnlyAbortable(OperationFailureException cause) {
        assert cause != null;
        setState(Transaction.State.MUST_ABORT);
        onlyAbortableCause = cause;
    }

    /**
     * Set the state of a transaction's IMPORTUNATE bit.
     */
    @Override
    public void setImportunate(boolean importunate) {
        if (importunate) {
            txnFlags |= IMPORTUNATE;
        } else {
            txnFlags &= ~IMPORTUNATE;
        }
    }

    /**
     * Get the state of a transaction's IMPORTUNATE bit.
     */
    @Override
    public boolean getImportunate() {
        return (txnFlags & IMPORTUNATE) != 0;
    }

    /**
     * Checks for preemption in this locker and all its child buddies.  Does
     * NOT call checkPreempted on its child buddies, since this would cause an
     * infinite recursion.
     */
    @Override
    public void checkPreempted(final Locker allowPreemptedLocker)
        throws OperationFailureException {

        /* First check this locker. */
        throwIfPreempted(allowPreemptedLocker);

        /*
         * Then check our buddy lockers. It's OK to call throwIfPreempted while
         * synchronized on buddyLockers, since it takes no locks.
         */
        if (buddyLockers != null) {
            synchronized (buddyLockers) {
                for (BuddyLocker buddy : buddyLockers) {
                    buddy.throwIfPreempted(allowPreemptedLocker);
                }
            }
        }
    }

    /**
     * Throw an exception if the transaction is not open.
     *
     * If calledByAbort is true, it means we're being called from abort(). But
     * once closed, a Transaction never calls abort(). See comment at the top
     * of abortInternal.
     *
     * Caller must invoke with "this" synchronized.
     */
    @Override
    public void checkState(boolean calledByAbort)
        throws DatabaseException {

        switch (txnState) {

            case OPEN:
                return;

            case MUST_ABORT:

                /* Don't complain if the user is doing what we asked. */
                if (calledByAbort) {
                    return;
                }

                /*
                 * Throw the original exception that caused the txn to be set
                 * to abort-only, wrapped in a new exception of the same class.
                 * That way, both stack traces are available and the user can
                 * specify a meaningful class in their catch statement.
                 *
                 * It's ok for FindBugs to whine about id not being
                 * synchronized.
                 */
                throw onlyAbortableCause.wrapSelf
                    ("Transaction " + id  +
                     " must be aborted, caused by: " + onlyAbortableCause);

            default:
                /* All other states are equivalent to closed. */

                /*
                 * It's ok for FindBugs to whine about id not being
                 * synchronized.
                 */
                throw new IllegalStateException
                    ("Transaction " + id + " has been closed.");
        }
    }

    /**
     * Close and unregister this txn.
     */
    public void close(boolean isCommit) {

        if (isCommit) {
            /* Set final state to COMMITTED, if not set earlier. */
            if (txnState == Transaction.State.OPEN) {
                setState(Transaction.State.COMMITTED);
            }
        } else {
            /* This was set earlier by abort, but here also for safety. */
            setState(Transaction.State.ABORTED);
        }

        /*
         * UnregisterTxn must be called outside the synchronization on this
         * txn, because it gets the TxnManager's allTxns latch. The
         * checkpointer also gets the allTxns latch, and within that latch,
         * needs to synchronize on individual txns, so we must avoid a latching
         * hierarchy conflict.
         *
         * [#16861] FUTURE: Perhaps this special handling is no longer needed,
         * now that firstLoggedLsn is volatile and getFirstActiveLsn is not
         * synchronized.
         */
        envImpl.getTxnManager().unRegisterTxn(this, isCommit);

        /* Set the superclass Locker state to closed. */
        close();
    }

    private synchronized void setState(Transaction.State state) {
        txnState = state;
    }

    public Transaction.State getState() {
        return txnState;
    }

    @Override
    public boolean isValid() {
        return txnState == Transaction.State.OPEN;
    }

    public boolean isClosed() {
        return txnState != Transaction.State.OPEN &&
               txnState != Transaction.State.MUST_ABORT;
    }

    public boolean isOnlyAbortable() {
        return txnState == Transaction.State.MUST_ABORT;
    }

    /**
     * This method is overridden by HA txn subclasses and returns the node id
     * of the master node that committed or aborted the txn.
     */
    protected int getReplicatorNodeId() {
        /* Non replicated txns don't use a node ID. */
        return 0;
    }

    /**
     * This method is overridden by replication txn subclasses and returns the
     * DTVLSN associated with the Txn.
     */
    protected long getDTVLSN() {
       /*  Non replicated txns don't use VLSNs. */
        return VLSN.UNINITIALIZED_VLSN_SEQUENCE;
    }

    /*
     * Log support
     */

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Collections.emptyList();
    }

    @Override
    public int getLogSize() {
        return getLogSize(LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer) {
        writeToLog(
            logBuffer, LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return LogUtils.getPackedLongLogSize(id) +
               LogUtils.getPackedLongLogSize(
                   forReplication ? DbLsn.NULL_LSN : lastLoggedLsn);
    }

    /**
     * It's ok for FindBugs to whine about id not being synchronized.
     */
    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        LogUtils.writePackedLong(logBuffer, id);
        LogUtils.writePackedLong(logBuffer,
            forReplication ? DbLsn.NULL_LSN : lastLoggedLsn);
    }

    /**
     * It's ok for FindBugs to whine about id not being synchronized.
     */
    @Override
    public void readFromLog(ByteBuffer logBuffer, int entryVersion) {
        id = LogUtils.readLong(logBuffer, (entryVersion < 6));
        lastLoggedLsn = LogUtils.readLong(logBuffer, (entryVersion < 6));
    }

    @Override
    public boolean hasReplicationFormat() {
        return false;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return false;
    }

    @Override
    @SuppressWarnings("unused")
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<txn id=\"");
        sb.append(getId());
        sb.append("\">");
        sb.append(DbLsn.toString(lastLoggedLsn));
        sb.append("</txn>");
    }

    @Override
    public long getTransactionId() {
        return getId();
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof Txn)) {
            return false;
        }

        return id == ((Txn) other).id;
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled. The string
     * construction can be numerous enough to show up on a performance profile.
     */
    private void traceCommit(int numWriteLocks, int numReadLocks) {
        Logger logger = envImpl.getLogger();
        if (logger.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder();
            sb.append(" Commit: id = ").append(id);
            sb.append(" numWriteLocks=").append(numWriteLocks);
            sb.append(" numReadLocks = ").append(numReadLocks);
            LoggerUtils.fine(logger, envImpl, sb.toString());
        }
    }

    /**
     * Store information about a DatabaseImpl that will have to be
     * purged at transaction commit or abort. This handles cleanup after
     * operations like Environment.truncateDatabase,
     * Environment.removeDatabase. Cleanup like this is done outside the
     * usual transaction commit or node undo processing, because
     * the mapping tree is always auto Txn'ed to avoid deadlock and is
     * essentially  non-transactional.
     */
    public static class DatabaseCleanupInfo {
        DatabaseImpl dbImpl;

        /* if true, clean on commit. If false, clean on abort. */
        boolean deleteAtCommit;

        DatabaseCleanupInfo(DatabaseImpl dbImpl,
                            boolean deleteAtCommit) {
            this.dbImpl = dbImpl;
            this.deleteAtCommit = deleteAtCommit;
        }

        /**
         * Make sure that a set of DatabaseCleanupInfo only has one entry
         * per databaseImpl/deleteAtCommit tuple.
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DatabaseCleanupInfo)) {
                return false;
            }

            DatabaseCleanupInfo other = (DatabaseCleanupInfo) obj;
            return (dbImpl.equals(other.dbImpl)) &&
                (deleteAtCommit == other.deleteAtCommit);
        }

        @Override
        public int hashCode() {
            return dbImpl.hashCode();
        }
    }

    /* Transaction hooks used for replication support. */

    /**
     * A replicated environment introduces some new considerations when
     * entering a transaction scope via an Environment.transactionBegin()
     * operation.
     *
     * On a Replica, the transactionBegin() operation must wait until the
     * Replica has synched up to where it satisfies the ConsistencyPolicy that
     * is in effect.
     *
     * On a Master, the transactionBegin() must wait until the Feeder has
     * sufficient connections to ensure that it can satisfy the
     * ReplicaAckPolicy, since if it does not, it will fail at commit() and the
     * work done in the transaction will need to be undone.
     *
     * This hook provides the mechanism for implementing the above support for
     * replicated transactions. It ignores all non-replicated transactions.
     *
     * The hook throws ReplicaStateException, if a Master switches to a Replica
     * state while waiting for its Replicas connections. Changes from a Replica
     * to a Master are handled transparently to the application. Exceptions
     * manifest themselves as DatabaseException at the interface to minimize
     * use of Replication based exceptions in core JE.
     *
     * @param config the transaction config that applies to the txn
     *
     * @throws DatabaseException if there is a failure
     */
    protected void txnBeginHook(TransactionConfig config)
        throws DatabaseException {

        /* Overridden by Txn subclasses when appropriate */
    }

    /**
     * This hook is invoked before the commit of a transaction that made
     * changes to a replicated environment. It's invoked for transactions
     * executed on the master or replica, but is only relevant to transactions
     * being done on the master. When invoked for a transaction on a replica
     * the implementation just returns.
     *
     * The hook is invoked at a very specific point in the normal commit
     * sequence: immediately before the commit log entry is written to the log.
     * It represents the last chance to abort the transaction and provides an
     * opportunity to make some final checks before allowing the commit can go
     * ahead. Note that it should be possible to abort the transaction at the
     * time the hook is invoked.
     *
     * After invocation of the "pre" hook one of the "post" hooks:
     * postLogCommitHook or postLogAbortHook must always be invoked.
     *
     * Exceptions thrown by this hook result in the transaction being aborted
     * and the exception being propagated back to the application.
     *
     * @throws DatabaseException if there was a problem and that the
     * transaction should be aborted.
     */
    protected void preLogCommitHook()
        throws DatabaseException {

        /* Overridden by Txn subclasses when appropriate */
    }

    /**
     * This hook is invoked after the commit record has been written to the
     * log, but before write locks have been released, so that other
     * application cannot see the changes made by the transaction. At this
     * point the transaction has been committed by the Master.
     *
     * Exceptions thrown by this hook result in the transaction being completed
     * on the Master, that is, locks are released, etc. and the exception is
     * propagated back to the application.
     *
     * @param commitItem the commit item that was just logged
     *
     * @throws DatabaseException to indicate that there was a replication
     * related problem that needs to be communicated back to the application.
     */
    protected void postLogCommitHook(LogItem commitItem)
        throws DatabaseException {

        /* Overridden by Txn subclasses when appropriate */
    }

    protected void preLogAbortHook()
        throws DatabaseException {

        /* Override by Txn subclasses when appropriate */
    }

    /**
     * Invoked if the transaction associated with the preLogCommitHook was
     * subsequently aborted, for example due to a lack of disk space. This
     * method is responsible for any cleanup that may need to be done as a
     * result of the abort.
     *
     * Note that only one of the "post" hooks (commit or abort) is invoked
     * following the invocation of the "pre" hook.
     */
    protected void postLogCommitAbortHook() {
        /* Overridden by Txn subclasses when appropriate */
    }

    protected void postLogAbortHook() {
        /* Overridden by Txn subclasses when appropriate */
    }

    /**
     * Returns the CommitToken associated with a successful replicated commit.
     *
     * @see com.sleepycat.je.Transaction#getCommitToken
     */
    public CommitToken getCommitToken() {
        return null;
    }

    /**
     * Identifies exceptions that may be propagated back to the caller during
     * the postCommit phase of a transaction commit.
     *
     * @param postCommitException the exception being evaluated
     *
     * @return true if the exception must be propagated back to the caller,
     * false if the exception indicates there is a serious problem with the
     * commit operation and the environment should be invalidated.
     */
    protected boolean
        propagatePostCommitException(DatabaseException postCommitException) {
        return false;
    }

    /**
     * Use the marker Sets to record whether this is the first time we've see
     * this logical node.
     */
    private boolean firstInstance(Set<Long> seenLsns,
                                  Set<CompareSlot> seenSlots,
                                  UndoReader undo) {
        final LNLogEntry<?> undoEntry = undo.logEntry;
        final long abortLsn1 = undoEntry.getAbortLsn();
        if (abortLsn1 != DbLsn.NULL_LSN) {
            return seenLsns.add(abortLsn1);
        }
        final CompareSlot slot = new CompareSlot(undo.db, undoEntry);
        return seenSlots.add(slot);
    }

    /**
     * Accumulates the set of databases for which transaction commit/abort
     * triggers must be run.
     *
     * @param dbImpl the database that associated with the trigger
     */
    public void noteTriggerDb(DatabaseImpl dbImpl) {
        if (triggerDbs == null) {
            triggerDbs =
                Collections.synchronizedSet(new HashSet<DatabaseImpl>());
        }
        triggerDbs.add(dbImpl);
    }

    /**
     * Returns the set of databases for which transaction commit/abort
     * triggers must be run. Returns Null if no triggers need to be run.
     */
    public Set<DatabaseImpl> getTriggerDbs() {
        return triggerDbs;
    }

    /** Get the set of lock ids owned by this transaction */
    public Set<Long> getWriteLockIds() {
        if (writeInfo == null) {
            Set<Long> empty = Collections.emptySet();
            return empty;
        }

        return writeInfo.keySet();
    }

    /* For unit tests. */
    public Set<Long> getReadLockIds() {
        if (readLocks == null) {
            return new HashSet<Long>();
        }
        return new HashSet<Long>(readLocks);
    }

    public EnvironmentImpl getEnvironmentImpl() {
        return envImpl;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Transaction getTransaction() {
        return (transaction != null) ?
                transaction :
                (transaction = new AutoTransaction(this));
    }

    private static class AutoTransaction extends Transaction {

        protected AutoTransaction(Txn txn) {
            /* AutoTransactions do not have a convenient environment handle. */
            super(txn.getEnvironmentImpl().getInternalEnvHandle(), txn);
        }

        @Override
        public synchronized void commit()
            throws DatabaseException {

            EnvironmentFailureException.unexpectedState
                ("commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void commit
            (@SuppressWarnings("unused") Durability durability) {
            EnvironmentFailureException.unexpectedState
            ("commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void commitNoSync()
            throws DatabaseException {

            EnvironmentFailureException.unexpectedState
                ("commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void commitWriteNoSync()
            throws DatabaseException {

            EnvironmentFailureException.unexpectedState
                ("commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void abort()
            throws DatabaseException {

            EnvironmentFailureException.unexpectedState
                ("abort() not permitted on an auto transaction");
        }
    }

    public Map<DatabaseId, DatabaseImpl> getUndoDatabases() {
        return undoDatabases;
    }

    /**
     * Txn freezing is used to prevent changes to transaction lock contents.  A
     * frozen transaction should ignore any transaction commit/abort
     * requests. This is used only by MasterTxns, as a way of holding a
     * transaction stable while cloning it to serve as a ReplayTxn during
     * master->replica transitions.
     * @param isCommit true if called by commit.
     */
    protected void checkIfFrozen(boolean isCommit)
        throws DatabaseException {
        return;
    }

    /*
     * Used when creating a subset of MasterTxns. Using an explicit method
     * like this rather than checking class types insulates us from any
     * assumptions about the class hierarchy.
     */
    public boolean isMasterTxn() {
        return false;
    }
}
