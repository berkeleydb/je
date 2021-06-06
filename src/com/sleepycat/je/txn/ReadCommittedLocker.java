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
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;

/**
 * Extends BuddyLocker to acquire write locks using the buddy locker (the
 * transaction locker).  This is used for ReadCommitted (Degree 2) isolation.
 */
public class ReadCommittedLocker extends BuddyLocker {

    /**
     * Creates a ReadCommittedLocker.
     * @param buddy is a transactional locker that will be used for acquiring
     * write locks.
     */
    private ReadCommittedLocker(EnvironmentImpl env, Locker buddy) {

        /*
         * If the buddy param is a read-committed locker, reach in to get its
         * transactional buddy locker.
         */
        super(env,
              (buddy instanceof ReadCommittedLocker) ?
              ((ReadCommittedLocker) buddy).getBuddy() : buddy);

        assert(getBuddy() instanceof Txn);
    }

    public static ReadCommittedLocker createReadCommittedLocker(
        EnvironmentImpl env,
        Locker buddy)
        throws DatabaseException {

        return new ReadCommittedLocker(env, buddy);
    }

    /**
     * Returns a new ReadCommittedLocker that shares locks with this locker by
     * virtue of both lockers only holding READ locks.  The buddy locker
     * underlying both ReadCommittedLocker lockers is the same transactional
     * locker, so WRITE locks are also shared.
     */
    @Override
    public Locker newNonTxnLocker()
        throws DatabaseException {

        /*
         * getBuddy().newNonTxnLocker() will return the transactional buddy
         * locker itself (same as getBuddy), but we call newNonTxnLocker for
         * consistency.
         */
        return ReadCommittedLocker.createReadCommittedLocker
            (envImpl, getBuddy().newNonTxnLocker());
    }

    /**
     * Forwards write locks to the buddy locker (the transaction locker).
     *
     * @see Locker#lockInternal
     */
    @Override
    protected LockResult lockInternal(long lsn,
                                      LockType lockType,
                                      boolean noWait,
                                      boolean jumpAheadOfWaiters,
                                      DatabaseImpl database)
        throws DatabaseException {

        if (lockType.isWriteLock()) {
            return getBuddy().lockInternal
                (lsn, lockType, noWait, jumpAheadOfWaiters, database);
        } else {
            return super.lockInternal
                (lsn, lockType, noWait, jumpAheadOfWaiters, database);
        }
    }

    /**
     * Releases the lock from this locker, or if not owned by this locker then
     * releases it from the buddy locker.
     */
    @Override
    public boolean releaseLock(long lsn)
        throws DatabaseException {

        boolean ret = true;
        if (!lockManager.release(lsn, this)) {
            ret = lockManager.release(lsn, getBuddy());
        }
        removeLock(lsn);
        return ret;
    }

    /**
     * @return the WriteLockInfo for this node.
     */
    @Override
    public WriteLockInfo getWriteLockInfo(long lsn) {
        return getBuddy().getWriteLockInfo(lsn);
    }

    /**
     * Write operations are handled by the buddy Txn.
     */
    @Override
    public void markDeleteAtTxnEnd(DatabaseImpl db, boolean deleteAtCommit) {
        getBuddy().markDeleteAtTxnEnd(db, deleteAtCommit);
    }

    /**
     * Forwards this method to the transactional buddy.  The buddy handles
     * write locks and therefore handles delete information.
     */
    @Override
    public void addDeleteInfo(BIN bin) {
        getBuddy().addDeleteInfo(bin);
    }

    /**
     * Forwards this method to the transactional buddy.  The buddy Txn tracks
     * cursors.
     */
    @Override
    public void registerCursor(CursorImpl cursor) {
        getBuddy().registerCursor(cursor);
    }

    /**
     * Forwards this method to the transactional buddy.  The buddy Txn tracks
     * cursors.
     */
    @Override
    public void unRegisterCursor(CursorImpl cursor) {
        getBuddy().unRegisterCursor(cursor);
    }

    /**
     * ReadCommittedLockers always require locking.
     */
    @Override
    public boolean lockingRequired() {
        return true;
    }

    /**
     * Is always transactional because the buddy locker is transactional.
     */
    @Override
    public boolean isTransactional() {
        return true;
    }

    /**
     * Is always read-committed isolation.
     */
    @Override
    public boolean isReadCommittedIsolation() {
        return true;
    }
}
