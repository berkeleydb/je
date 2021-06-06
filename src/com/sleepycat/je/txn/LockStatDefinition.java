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

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE lock statistics.
 */
public class LockStatDefinition {

    public static final String GROUP_NAME = "Locks";
    public static final String GROUP_DESC =
        "Record locking is used to provide transactional capabilities.";

    public static final String LOCK_READ_LOCKS_NAME =
        "nReadLocks";
    public static final String LOCK_READ_LOCKS_DESC =
        "Number of read locks currently held.";
    public static final StatDefinition LOCK_READ_LOCKS =
        new StatDefinition(
            LOCK_READ_LOCKS_NAME,
            LOCK_READ_LOCKS_DESC,
            StatType.CUMULATIVE);

    public static final String LOCK_WRITE_LOCKS_NAME =
        "nWriteLocks";
    public static final String LOCK_WRITE_LOCKS_DESC =
        "Number of write locks currently held.";
    public static final StatDefinition LOCK_WRITE_LOCKS =
        new StatDefinition(
            LOCK_WRITE_LOCKS_NAME,
            LOCK_WRITE_LOCKS_DESC,
            StatType.CUMULATIVE);

    public static final String LOCK_OWNERS_NAME =
        "nOwners";
    public static final String LOCK_OWNERS_DESC =
        "Number of lock owners in lock table.";
    public static final StatDefinition LOCK_OWNERS =
        new StatDefinition(
            LOCK_OWNERS_NAME,
            LOCK_OWNERS_DESC,
            StatType.CUMULATIVE);

    public static final String LOCK_REQUESTS_NAME =
        "nRequests";
    public static final String LOCK_REQUESTS_DESC =
        "Number of times a lock request was made.";
    public static final StatDefinition LOCK_REQUESTS =
        new StatDefinition(
            LOCK_REQUESTS_NAME,
            LOCK_REQUESTS_DESC);

    public static final String LOCK_TOTAL_NAME =
        "nTotalLocks";
    public static final String LOCK_TOTAL_DESC =
        "Number of locks current in lock table.";
    public static final StatDefinition LOCK_TOTAL =
        new StatDefinition(
            LOCK_TOTAL_NAME,
            LOCK_TOTAL_DESC,
            StatType.CUMULATIVE);

    public static final String LOCK_WAITS_NAME =
        "nWaits";
    public static final String LOCK_WAITS_DESC =
        "Number of times a lock request blocked.";
    public static final StatDefinition LOCK_WAITS =
        new StatDefinition(
            LOCK_WAITS_NAME,
            LOCK_WAITS_DESC);

    public static final String LOCK_WAITERS_NAME =
        "nWaiters";
    public static final String LOCK_WAITERS_DESC =
        "Number of transactions waiting for a lock.";
    public static final StatDefinition LOCK_WAITERS =
        new StatDefinition(
            LOCK_WAITERS_NAME,
            LOCK_WAITERS_DESC,
            StatType.CUMULATIVE);
}
