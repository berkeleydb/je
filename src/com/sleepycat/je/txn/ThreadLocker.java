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

import java.util.Iterator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Extends BasicLocker to share locks among all lockers for the same thread.
 * This locker is used when a JE entry point is called with a null transaction
 * parameter.
 */
public class ThreadLocker extends BasicLocker {

    /**
     * Set to allow this locker to be used by multiple threads.  This mode
     * should only be set temporarily, for example, while locking in
     * lockAfterLsnChange.
     */
    private boolean allowMultithreadedAccess;

    /**
     * Creates a ThreadLocker.
     */
    public ThreadLocker(EnvironmentImpl env) {
        super(env);
        lockManager.registerThreadLocker(this);
    }

    public static ThreadLocker createThreadLocker(EnvironmentImpl env,
                                                  boolean replicated)
        throws DatabaseException {

        return (env.isReplicated() && replicated) ?
               env.createRepThreadLocker() :
               new ThreadLocker(env);
    }

    @Override
    void close() {
        super.close();
        lockManager.unregisterThreadLocker(this);
    }

    /**
     * Checks for preemption in all thread lockers for this thread.
     */
    @Override
    public void checkPreempted(final Locker allowPreemptedLocker) 
        throws OperationFailureException {

        final Iterator<ThreadLocker> iter =
            lockManager.getThreadLockers(thread);
        while (iter.hasNext()) {
            final ThreadLocker locker = iter.next();
            locker.throwIfPreempted(allowPreemptedLocker);
        }
    }

    /**
     * Set the allowMultithreadedAccess mode during execution of this method
     * because a ThreadLocker is not normally allowed to perform locking from
     * more than one thread.
     */
    @Override
    public synchronized void lockAfterLsnChange(long oldLsn,
                                                long newLsn,
                                                DatabaseImpl dbImpl) {
        final boolean oldVal = allowMultithreadedAccess;
        allowMultithreadedAccess = true;
        try {
            super.lockAfterLsnChange(oldLsn, newLsn, dbImpl);
        } finally {
            allowMultithreadedAccess = oldVal;
        }
    }

    /**
     * Check that this locker is not used in the wrong thread.
     *
     * @throws IllegalStateException via all Cursor methods that use a
     * non-transactional locker.
     */
    @Override
    protected synchronized void checkState(boolean ignoreCalledByAbort) {
        if (!allowMultithreadedAccess && thread != Thread.currentThread()) {
            throw new IllegalStateException
                ("Non-transactional Cursors may not be used in multiple " +
                 "threads; Cursor was created in " + thread +
                 " but used in " + Thread.currentThread());
        }
    }

    /**
     * Returns a new non-transactional locker that shares locks with this
     * locker by virtue of being a ThreadLocker for the same thread.
     */
    @Override
    public Locker newNonTxnLocker()
        throws DatabaseException {

        checkState(false);
        return newEmptyThreadLockerClone();
    }

    public ThreadLocker newEmptyThreadLockerClone() {
        return new ThreadLocker(envImpl);
    }

    /**
     * Returns whether this locker can share locks with the given locker.
     * Locks are shared when both lockers are ThreadLocker instances for the
     * same thread.
     */
    @Override
    public boolean sharesLocksWith(Locker other) {

        if (super.sharesLocksWith(other)) {
            return true;
        } else if (other instanceof ThreadLocker) {
            return thread == other.thread;
        } else {
            return false;
        }
    }
}
