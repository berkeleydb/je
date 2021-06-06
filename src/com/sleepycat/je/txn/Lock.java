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

import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.MemoryBudget;

/**
 * A Lock embodies the lock state of an LSN.  It includes a set of owners and
 * a list of waiters.
 */
interface Lock {

    /**
     * Get a list of waiters for debugging and error messages.
     */
    public List<LockInfo> getWaitersListClone();

    /**
     * Remove this locker from the waiter list.
     */
    public void flushWaiter(Locker locker,
                            MemoryBudget mb,
                            int lockTableIndex);

    /**
     * Get a new Set of the owners.
     */
    public Set<LockInfo> getOwnersClone();

    /**
     * Return true if locker is an owner of this Lock for lockType,
     * false otherwise.
     *
     * This method is only used by unit tests.
     */
    public boolean isOwner(Locker locker, LockType lockType);

    /**
     * Return true if locker is an owner of this Lock and this is a write
     * lock.
     */
    public boolean isOwnedWriteLock(Locker locker);

    /**
     * Returns the LockType if the given locker owns this lock, or null if the
     * lock is not owned.
     */
    public LockType getOwnedLockType(Locker locker);

    /**
     * Return true if locker is a waiter on this Lock.
     *
     * This method is only used by unit tests.
     */
    public boolean isWaiter(Locker locker);

    public int nWaiters();

    public int nOwners();

    /**
     * Attempts to acquire the lock and returns the LockGrantType.
     *
     * Assumes we hold the lockTableLatch when entering this method.
     */
    public LockAttemptResult lock(LockType requestType,
                                  Locker locker,
                                  boolean nonBlockingRequest,
                                  boolean jumpAheadOfWaiters,
                                  MemoryBudget mb,
                                  int lockTableIndex)
        throws DatabaseException;

    /**
     * Releases a lock and moves the next waiter(s) to the owners.
     * @return
     *  - null if we were not the owner,
     *  - a non-empty set if owners should be notified after releasing,
     *  - an empty set if no notification is required.
     */
    public Set<Locker> release(Locker locker, 
                               MemoryBudget mb, 
                               int lockTableIndex);

    /**
     * Removes all owners except for the given owner, and sets the Preempted
     * property on the removed owners.
     */
    public void stealLock(Locker locker, MemoryBudget mb, int lockTableIndex)
        throws DatabaseException;

    /**
     * Downgrade a write lock to a read lock.
     */
    public void demote(Locker locker);

    /**
     * Return the locker that has a write ownership on this lock. If no
     * write owner exists, return null.
     */
    public Locker getWriteOwnerLocker();

    public boolean isThin();

    /**
     * Debug dumper.
     */
    public String toString();
}
