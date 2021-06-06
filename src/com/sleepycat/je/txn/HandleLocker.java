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
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Extends BasicLocker to share locks with another Locker that is being used to
 * open a database.  HandleLocker holds a read lock on the NameLN for the
 * Database object, to prevent rename, removal or truncation of a database
 * while it is open.  The HandleLocker and the Locker used to open the database
 * both hold a NameLN lock at the same time, so they must share locks to avoid
 * conflicts.
 *
 * Accounts for the fact that the Txn may end before this locker ends by only
 * keeping the Txn ID rather than a reference to the Txn object.  A reference
 * to a non-tnxl Locker is OK, OTOH, because it is short lived.
 *
 * Handle Locking Overview
 * ------------------------
 * Environment.openDatabase calls Environment.setupDatabase, which calls
 * Database.initHandleLocker to create the HandleLocker. setupDatabase ensures
 * that the HandleLocker is passed to DbTree.getDb and createDb.  These latter
 * methods acquire a read lock on the NameLN for the HandleLocker, in addition
 * to acquiring a read or write lock for the openDatabase locker.
 *
 * If setupDatabase is not successful, it ensures that locks are released via
 * HandleLocker.endOperation.  If setupDatabase is successful, the handle is
 * returned by openDatabase, and Database.close must be called to release the
 * lock.  The handle lock is released by calling HandleLocker.endOperation.
 *
 * A special case is when a user txn is passed to openDatabase.  If the txn
 * aborts, the Database handle must be invalidated.  When setupDatabase
 * succeeds it passes the handle to Txn.addOpenedDatabase, which remembers the
 * handle.  Txn.abort invalidates the handle.
 * 
 * NameLN Migration and LSN Changes [#20617]
 * -----------------------------------------
 * When the log cleaner migrates a NameLN, its LSN changes and the new LSN is
 * locked on behalf of all existing lockers by CursorImpl.lockAfterLsnChange.
 * lockAfterLsnChange is also used when a dirty deferred-write LN is logged by
 * BIN.logDirtyLN, as part of flushing a BIN during a checkpoint or eviction.
 *
 * Because handle lockers are legitimately very long lived, it is important
 * that lockAfterLsnChange releases the locks on the old LSN, to avoid a steady
 * accumulation of locks in a HandleLocker.  Therefore, lockAfterLsnChange will
 * release the lock on the old LSN, for HandleLockers only.  Although it may be
 * desirable to release the old LSN lock on other long lived lockers, it is too
 * risky.  In an experiment, this caused problems with demotion and upgrade,
 * when a lock being demoted or upgraded was released.
 *
 * Because LSNs can change, it is also important that we don't rely on a single
 * NameLN locker ID (LSN) as a data structure key for handle locks.  This was
 * acceptable when a stable Node ID was used as a lock ID, but is no longer
 * appropriate now that mutable LSNs are used as lock IDs.
 */
public class HandleLocker extends BasicLocker {

    private final long shareWithTxnId;
    private final Locker shareWithNonTxnlLocker;

    /**
     * Creates a HandleLocker.
     */
    protected HandleLocker(EnvironmentImpl env, Locker buddy) {
        super(env);
        shareWithTxnId =
            buddy.isTransactional() ? buddy.getId() : TxnManager.NULL_TXN_ID;
        shareWithNonTxnlLocker = 
            buddy.isTransactional() ? null : buddy;
    }

    public static HandleLocker createHandleLocker(EnvironmentImpl env,
                                                  Locker buddy)
        throws DatabaseException {

        return new HandleLocker(env, buddy);
    }

    /**
     * Returns whether this locker can share locks with the given locker.
     */
    @Override
    public boolean sharesLocksWith(Locker other) {

        if (super.sharesLocksWith(other)) {
            return true;
        }
        if (shareWithTxnId != TxnManager.NULL_TXN_ID &&
            shareWithTxnId == other.getId()) {
            return true;
        }
        if (shareWithNonTxnlLocker != null &&
            shareWithNonTxnlLocker == other) {
            return true;
        }
        return false;
    }

    /**
     * Because handle lockers are legitimately very long lived, it is important
     * that lockAfterLsnChange releases the locks on the old LSN, to avoid a
     * steady accumulation of locks in a HandleLocker
     */
    @Override
    public boolean allowReleaseLockAfterLsnChange() {
        return true;
    }
}
