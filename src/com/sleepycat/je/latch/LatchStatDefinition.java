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

package com.sleepycat.je.latch;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for JE latch statistics.
 */
public class LatchStatDefinition {

    public static final String GROUP_NAME = "Latch";
    public static final String GROUP_DESC = "Latch characteristics";

    public static final String LATCH_NO_WAITERS_NAME =
        "nLatchAcquiresNoWaiters";
    public static final String LATCH_NO_WAITERS_DESC =
        "Number of times the latch was acquired without contention.";
    public static final StatDefinition LATCH_NO_WAITERS =
        new StatDefinition(
            LATCH_NO_WAITERS_NAME,
            LATCH_NO_WAITERS_DESC);

    public static final String LATCH_SELF_OWNED_NAME =
        "nLatchAcquiresSelfOwned";
    public static final String LATCH_SELF_OWNED_DESC =
        "Number of times the latch was acquired it was already owned by the " +
            "caller.";
    public static final StatDefinition LATCH_SELF_OWNED =
        new StatDefinition(
            LATCH_SELF_OWNED_NAME,
            LATCH_SELF_OWNED_DESC);

    public static final String LATCH_CONTENTION_NAME =
        "nLatchAcquiresWithContention";
    public static final String LATCH_CONTENTION_DESC =
        "Number of times the latch was acquired when it was already owned by " +
            "another thread.";
    public static final StatDefinition LATCH_CONTENTION =
        new StatDefinition(
            LATCH_CONTENTION_NAME,
            LATCH_CONTENTION_DESC);

    public static final String LATCH_NOWAIT_SUCCESS_NAME =
        "nLatchAcquiresNoWaitSuccessful";
    public static final String LATCH_NOWAIT_SUCCESS_DESC =
        "Number of successful no-wait acquires of the lock table latch.";
    public static final StatDefinition LATCH_NOWAIT_SUCCESS =
        new StatDefinition(
            LATCH_NOWAIT_SUCCESS_NAME,
            LATCH_NOWAIT_SUCCESS_DESC);

    public static final String LATCH_NOWAIT_UNSUCCESS_NAME =
        "nLatchAcquireNoWaitUnsuccessful";
    public static final String LATCH_NOWAIT_UNSUCCESS_DESC =
        "Number of unsuccessful no-wait acquires of the lock table latch.";
    public static final StatDefinition LATCH_NOWAIT_UNSUCCESS =
        new StatDefinition(
            LATCH_NOWAIT_UNSUCCESS_NAME,
            LATCH_NOWAIT_UNSUCCESS_DESC);

    public static final String LATCH_RELEASES_NAME =
        "nLatchReleases";
    public static final String LATCH_RELEASES_DESC =
        "Number of latch releases.";
    public static final StatDefinition LATCH_RELEASES =
        new StatDefinition(
            LATCH_RELEASES_NAME,
            LATCH_RELEASES_DESC);
}
