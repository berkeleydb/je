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

package com.sleepycat.je.rep.impl.node;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Replica statistics.
 */
public class ReplicaStatDefinition {

    public static final String GROUP_NAME = "ConsistencyTracker";
    public static final String GROUP_DESC = "Statistics on the delays " +
        "experienced by read requests at the replica in order to conform " +
        "to the specified ReplicaConsistencyPolicy.";

    public static final String N_LAG_CONSISTENCY_WAITS_NAME =
        "nLagConsistencyWaits";
    public static final String N_LAG_CONSISTENCY_WAITS_DESC =
        "Number of Transaction waits while the replica catches up in order to" +
            " meet a transaction's consistency requirement.";
    public static final StatDefinition N_LAG_CONSISTENCY_WAITS =
        new StatDefinition(
            N_LAG_CONSISTENCY_WAITS_NAME,
            N_LAG_CONSISTENCY_WAITS_DESC);

    public static final String N_LAG_CONSISTENCY_WAIT_MS_NAME =
        "nLagConsistencyWaitMS";
    public static final String N_LAG_CONSISTENCY_WAIT_MS_DESC =
        "Number of msec waited while the replica catches up in order to meet " +
            "a transaction's consistency requirement.";
    public static final StatDefinition N_LAG_CONSISTENCY_WAIT_MS =
        new StatDefinition(
            N_LAG_CONSISTENCY_WAIT_MS_NAME,
            N_LAG_CONSISTENCY_WAIT_MS_DESC);

    public static final String N_VLSN_CONSISTENCY_WAITS_NAME =
        "nVLSNConsistencyWaits";
    public static final String N_VLSN_CONSISTENCY_WAITS_DESC =
        "Number of Transaction waits while the replica catches up in order to" +
            " receive a VLSN.";
    public static final StatDefinition N_VLSN_CONSISTENCY_WAITS =
        new StatDefinition(
            N_VLSN_CONSISTENCY_WAITS_NAME,
            N_VLSN_CONSISTENCY_WAITS_DESC);

    public static final String N_VLSN_CONSISTENCY_WAIT_MS_NAME =
        "nVLSNConsistencyWaitMS";
    public static final String N_VLSN_CONSISTENCY_WAIT_MS_DESC =
        "Number of msec waited while the replica catches up in order to " +
            "receive a VLSN.";
    public static final StatDefinition N_VLSN_CONSISTENCY_WAIT_MS =
        new StatDefinition(
            N_VLSN_CONSISTENCY_WAIT_MS_NAME,
            N_VLSN_CONSISTENCY_WAIT_MS_DESC);
}
