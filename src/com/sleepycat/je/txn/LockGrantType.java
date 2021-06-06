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

/**
 * LockGrantType is an enumeration of the possible results of a lock attempt.
 */
public enum LockGrantType {

    /**
     * The locker did not previously own a lock on the node, and a new lock has
     * been granted.
     */
    NEW,

    /**
     * The locker did not previously own a lock on the node, and must wait for
     * a new lock because a conflicting lock is held by another locker.
     */
    WAIT_NEW,

    /**
     * The locker previously owned a read lock on the node, and a write lock
     * has been granted by upgrading the lock from read to write.
     */
    PROMOTION,

    /**
     * The locker previously owned a read lock on the node, and must wait for a
     * lock upgrade because a conflicting lock is held by another locker.
     */
    WAIT_PROMOTION,

    /**
     * The locker already owns the requested lock, and no new lock or upgrade
     * is needed.
     */
    EXISTING,

    /**
     * The lock request was a non-blocking one and the lock has not been 
     * granted because a conflicting lock is held by another locker.
     */
    DENIED,

    /**
     * The lock has not been granted because a conflicting lock is held by
     * another locker, and a RangeRestartException must be thrown.
     */
    WAIT_RESTART,

    /**
     * No lock has been granted because LockType.NONE was requested.
     */
    NONE_NEEDED,
}
