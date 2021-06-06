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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Extends BasicLocker to share locks with another specific locker.
 *
 * <p>In general, a BuddyLocker can be used whenever the primary (API) locker
 * is in use, and we need to lock a node and release that lock before the
 * primary locker transaction ends.  In other words, for this particular lock
 * we don't want to use two-phase locking.  To accomplish that we use a
 * separate BuddyLocker instance to hold the lock, while sharing locks with the
 * primary locker.  The BuddyLocker can be closed to release this particular
 * lock, without releasing the other locks held by the primary locker.</p>
 *
 * <p>In particular, a ReadCommittedLocker extends BuddyLocker. The
 * ReadCommittedLocker keeps track of read locks, while its buddy Txn keeps
 * track of write locks. The two lockers must share locks to prevent
 * conflicts.</p>
 *
 * <p>In addition, a BuddyLocker is used when acquiring a RANGE_INSERT lock.
 * RANGE_INSERT only needs to be held until the point we have inserted the new
 * node into the BIN.  A separate locker is therefore used so we can release
 * that lock separately when the insertion into the BIN is complete.  But the
 * RANGE_INSERT lock must not conflict with locks held by the primary locker.
 * So a BuddyLocker is used that shares locks with the primary locker.</p>
 */
public class BuddyLocker extends BasicLocker {

    private final Locker buddy;

    /**
     * Creates a BuddyLocker.
     */
    protected BuddyLocker(EnvironmentImpl env, Locker buddy) {
        super(env, buddy.getDefaultNoWait());
        this.buddy = buddy;
        buddy.addBuddy(this);
    }

    public static BuddyLocker createBuddyLocker(EnvironmentImpl env,
                                                Locker buddy)
        throws DatabaseException {

        return new BuddyLocker(env, buddy);
    }

    @Override
    void close() {
        super.close();
        buddy.removeBuddy(this);
    }

    /**
     * Returns the buddy locker.
     */
    @Override
    Locker getBuddy() {
        return buddy;
    }

    /**
     * Forwards this call to the buddy locker.  This object itself is never
     * transactional but the buddy may be.
     */
    @Override
    public Txn getTxnLocker() {
        return buddy.getTxnLocker();
    }

    /**
     * Forwards this call to the buddy locker.  This object itself is never
     * transactional but the buddy may be.
     */
    @Override
    public Transaction getTransaction() {
        return buddy.getTransaction();
    }

    /**
     * Forwards this call to the base class and to the buddy locker.
     */
    @Override
    public void releaseNonTxnLocks()
        throws DatabaseException {

        super.releaseNonTxnLocks();
        buddy.releaseNonTxnLocks();
    }

    /**
     * Returns whether this locker can share locks with the given locker.
     */
    @Override
    public boolean sharesLocksWith(Locker other) {

        if (super.sharesLocksWith(other)) {
            return true;
        } else {
            return (buddy == other ||
                    other.getBuddy() == this ||
                    buddy == other.getBuddy());
        }
    }

    /**
     * Returns the lock timeout of the buddy locker, since this locker has no
     * independent timeout.
     */
    @Override
    public long getLockTimeout() {
        return buddy.getLockTimeout();
    }

    /**
     * Returns the transaction timeout of the buddy locker, since this locker
     * has no independent timeout.
     */
    @Override
    public long getTxnTimeout() {
        return buddy.getTxnTimeout();
    }

    /**
     * Sets the lock timeout of the buddy locker, since this locker has no
     * independent timeout.
     */
    @Override
    public void setLockTimeout(long timeout) {
        buddy.setLockTimeout(timeout);
    }

    /**
     * Sets the transaction timeout of the buddy locker, since this locker has
     * no independent timeout.
     */
    @Override
    public void setTxnTimeout(long timeout) {
        buddy.setTxnTimeout(timeout);
    }

    /**
     * Returns whether the buddy locker is timed out, since this locker has no
     * independent timeout.
     */
    @Override
    public boolean isTimedOut() {
        return buddy.isTimedOut();
    }

    /**
     * Returns the buddy locker's start time, since this locker has no
     * independent timeout.
     */
    @Override
    public long getTxnStartMillis() {
        return buddy.getTxnStartMillis();
    }

    /**
     * Forwards to the buddy locker, since the buddy may be transactional.
     */
    @Override
    public void setOnlyAbortable(OperationFailureException cause) {
        buddy.setOnlyAbortable(cause);
    }

    /**
     * Forwards to the parent buddy locker, so the buddy can check itself and
     * all of its child buddies.
     */
    @Override
    public void checkPreempted(final Locker allowPreemptedLocker) 
        throws OperationFailureException {

        buddy.checkPreempted(allowPreemptedLocker);
    }

    /**
     * Consider this locker replicated if its buddy (Txn) is replicated.
     */
    @Override
    public boolean isReplicated() {
        return buddy.isReplicated();
    }

    /**
     * Consider this locker local-write if its buddy is local-write.
     */
    @Override
    public boolean isLocalWrite() {
        return buddy.isLocalWrite();
    }
}
