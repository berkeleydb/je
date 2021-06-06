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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.MemoryBudget;

/**
 * Implements a lightweight Lock with no waiters and only a single Owner.  If,
 * during an operation (lock) more than one owner or waiter is required, then
 * this will mutate to a LockImpl, perform the requested operation, and return
 * the new LockImpl to the caller.
 *
 * public for Sizeof.
 */
public class ThinLockImpl extends LockInfo implements Lock {

    /**
     * Create a Lock.  Public for Sizeof.
     */
    public ThinLockImpl() {
        super(null, null);
    }

    /* Used when releasing lock */
    ThinLockImpl(ThinLockImpl tl) {
        super(tl.getLocker(), tl.getLockType());
    }

    public List<LockInfo> getWaitersListClone() {
        return Collections.emptyList();
    }

    public void flushWaiter(Locker locker,
                            MemoryBudget mb,
                            int lockTableIndex) {

        /* Do nothing. */
        return;
    }

    public Set<LockInfo> getOwnersClone() {

        Set<LockInfo> ret = new HashSet<LockInfo>();
        if (locker != null) {
            ret.add(this);
        }
        return ret;
    }

    public boolean isOwner(Locker locker, LockType lockType) {
        return locker == this.locker && lockType == this.lockType;
    }

    public boolean isOwnedWriteLock(Locker locker) {

        if (locker != this.locker) {
            return false;
        }

        if (this.lockType != null) {
            return this.lockType.isWriteLock();
        } else {
            return false;
        }
    }

    public LockType getOwnedLockType(Locker locker) {
        if (locker != this.locker) {
            return null;
        }
        return this.lockType;
    }

    public boolean isWaiter(Locker locker) {

        /* There can never be waiters on Thin Locks. */
        return false;
    }

    public int nWaiters() {
        return 0;
    }

    public int nOwners() {
        return (locker == null ? 0 : 1);
    }

    public LockAttemptResult lock(LockType requestType,
                                  Locker locker,
                                  boolean nonBlockingRequest,
                                  boolean jumpAheadOfWaiters,
                                  MemoryBudget mb,
                                  int lockTableIndex)
        throws DatabaseException {

        if (this.locker != null &&
            this.locker != locker) {
            /* Lock is already held by someone else so mutate. */
            Lock newLock = new LockImpl(new LockInfo(this));
            return newLock.lock(requestType, locker, nonBlockingRequest,
                                jumpAheadOfWaiters, mb, lockTableIndex);
        }

        LockGrantType grant = null;
        if (this.locker == null) {
            this.locker = locker;
            this.lockType = requestType;
            grant = LockGrantType.NEW;
        } else {

            /* The requestor holds this lock.  Check for upgrades. */
            LockUpgrade upgrade = lockType.getUpgrade(requestType);
            if (upgrade.getUpgrade() == null) {
                grant = LockGrantType.EXISTING;
            } else {
                LockType upgradeType = upgrade.getUpgrade();
                assert upgradeType != null;
                this.lockType = upgradeType;
                grant = (upgrade.getPromotion() ?
                         LockGrantType.PROMOTION :
                         LockGrantType.EXISTING);
            }
        }
        return new LockAttemptResult(this, grant, false);
    }

    public Set<Locker> release(Locker locker,
                               MemoryBudget mb,
                               int lockTableIndex) {

        if (locker == this.locker) {
            this.locker = null;
            this.lockType = null;
            return Collections.emptySet();
        } else {
            return null;
        }
    }

    public void stealLock(Locker locker, MemoryBudget mb, int lockTableIndex) {
        if (this.locker != locker &&
            this.locker.getPreemptable()) {
            this.locker.setPreempted();
            this.locker = null;
        }
    }

    public void demote(Locker locker) {

        if (this.lockType.isWriteLock()) {
            this.lockType = (lockType == LockType.RANGE_WRITE) ?
                LockType.RANGE_READ : LockType.READ;
        }
    }

    public Locker getWriteOwnerLocker() {

        if (lockType != null &&
            lockType.isWriteLock()) {
            return locker;
        } else {
            return null;
        }
    }

    public boolean isThin() {
        return true;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(" ThinLockAddr:").append(System.identityHashCode(this));
        sb.append(" Owner:");
        if (nOwners() == 0) {
            sb.append(" (none)");
        } else {
            sb.append(locker);
        }

        sb.append(" Waiters: (none)");
        return sb.toString();
    }
}
