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

import static com.sleepycat.je.utilint.StatDefinition.StatType.CUMULATIVE;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Replay statistics.
 */
public class FeederManagerStatDefinition {

    public static final String GROUP_NAME = "FeederManager";
    public static final String GROUP_DESC =
        "A feeder is a replication stream connection between a master and " +
            "replica nodes.";

    public static final String N_FEEDERS_CREATED_NAME =
        "nFeedersCreated";
    public static final String N_FEEDERS_CREATED_DESC =
        "Number of Feeder threads since this node was started.";
    public static final StatDefinition N_FEEDERS_CREATED =
        new StatDefinition(
            N_FEEDERS_CREATED_NAME,
            N_FEEDERS_CREATED_DESC);

    public static final String N_FEEDERS_SHUTDOWN_NAME =
        "nFeedersShutdown";
    public static final String N_FEEDERS_SHUTDOWN_DESC =
        "Number of Feeder threads that were shut down, either because this " +
            "node, or the Replica terminated the connection.";
    public static final StatDefinition N_FEEDERS_SHUTDOWN =
        new StatDefinition(
            N_FEEDERS_SHUTDOWN_NAME,
            N_FEEDERS_SHUTDOWN_DESC);

    /* Naming conflict -- use SNAME suffix in this one case. */
    public static final String N_MAX_REPLICA_LAG_SNAME =
        "nMaxReplicaLag";
    public static final String N_MAX_REPLICA_LAG_DESC =
        "The maximum number of VLSNs by which a replica is lagging.";
    public static final StatDefinition N_MAX_REPLICA_LAG =
        new StatDefinition(
            N_MAX_REPLICA_LAG_SNAME,
            N_MAX_REPLICA_LAG_DESC);

    public static final String N_MAX_REPLICA_LAG_NAME_NAME =
        "nMaxReplicaLagName";
    public static final String N_MAX_REPLICA_LAG_NAME_DESC =
        "The name of the replica with the maximal lag.";
    public static final StatDefinition N_MAX_REPLICA_LAG_NAME =
        new StatDefinition(
            N_MAX_REPLICA_LAG_NAME_NAME,
            N_MAX_REPLICA_LAG_NAME_DESC);

    public static final String REPLICA_DELAY_MAP_NAME =
        "replicaDelayMap";
    public static final String REPLICA_DELAY_MAP_DESC =
        "A map from replica node name to the delay, in milliseconds, between " +
            "when a transaction was committed on the master and when the " +
            "master learned that the change was processed on the replica, if " +
            "known. Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_DELAY_MAP =
        new StatDefinition(
            REPLICA_DELAY_MAP_NAME,
            REPLICA_DELAY_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_LAST_COMMIT_TIMESTAMP_MAP_NAME =
        "replicaLastCommitTimestampMap";
    public static final String REPLICA_LAST_COMMIT_TIMESTAMP_MAP_DESC =
        "A map from replica node name to the commit timestamp of the last " +
            "committed transaction that was processed on the replica, if " +
            "known. Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_LAST_COMMIT_TIMESTAMP_MAP =
        new StatDefinition(
            REPLICA_LAST_COMMIT_TIMESTAMP_MAP_NAME,
            REPLICA_LAST_COMMIT_TIMESTAMP_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_LAST_COMMIT_VLSN_MAP_NAME =
        "replicaLastCommitVLSNMap";
    public static final String REPLICA_LAST_COMMIT_VLSN_MAP_DESC =
        "A map from replica node name to the VLSN of the last committed " +
            "transaction that was processed on the replica, if known. Returns" +
            " an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_LAST_COMMIT_VLSN_MAP =
        new StatDefinition(
            REPLICA_LAST_COMMIT_VLSN_MAP_NAME,
            REPLICA_LAST_COMMIT_VLSN_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_VLSN_LAG_MAP_NAME =
        "replicaVLSNLagMap";
    public static final String REPLICA_VLSN_LAG_MAP_DESC =
        "A map from replica node name to the lag, in VLSNs, between the " +
            "replication state of the replica and the master, if known. " +
            "Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_VLSN_LAG_MAP =
        new StatDefinition(
            REPLICA_VLSN_LAG_MAP_NAME,
            REPLICA_VLSN_LAG_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_VLSN_RATE_MAP_NAME =
        "replicaVLSNRateMap";
    public static final String REPLICA_VLSN_RATE_MAP_DESC =
        "A map from replica node name to a moving average of the rate, in " +
            "VLSNs per minute, that the replica is processing replication " +
            "data, if known. Returns an empty map if this node is not the " +
            "master.";
    public static final StatDefinition REPLICA_VLSN_RATE_MAP =
        new StatDefinition(
            REPLICA_VLSN_RATE_MAP_NAME,
            REPLICA_VLSN_RATE_MAP_DESC);
}

