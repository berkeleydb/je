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
 * Per-stat Metadata for HA Replay statistics.
 */
public class ReplayStatDefinition {

    public static final String GROUP_NAME = "Replay";
    public static final String GROUP_DESC = "The Replay unit applies the " +
        "incoming replication stream at a Replica. These stats show the " +
        "load the Replica incurs when processing updates.";

    public static final String N_COMMITS_NAME =
        "nCommits";
    public static final String N_COMMITS_DESC =
        "Number of Commits replayed by the Replica.";
    public static final StatDefinition N_COMMITS =
        new StatDefinition(
            N_COMMITS_NAME,
            N_COMMITS_DESC);

    public static final String N_GROUP_COMMIT_TIMEOUTS_NAME =
        "nGroupCommitTimeouts";
    public static final String N_GROUP_COMMIT_TIMEOUTS_DESC =
        "Number of group commits that were initiated due to the group timeout" +
            " interval(ReplicationConfig.REPLICA_GROUP_COMMIT_INTERVAL) being" +
            " exceeded.";
    public static final StatDefinition N_GROUP_COMMIT_TIMEOUTS =
        new StatDefinition(
            N_GROUP_COMMIT_TIMEOUTS_NAME,
            N_GROUP_COMMIT_TIMEOUTS_DESC);

    public static final String N_GROUP_COMMIT_MAX_EXCEEDED_NAME =
        "nGroupCommitMaxExceeded";
    public static final String N_GROUP_COMMIT_MAX_EXCEEDED_DESC =
        "Number of group commits that were initiated due to the max group " +
            "size(ReplicationConfig.REPLICA_MAX_GROUP_COMMIT)  being exceeded.";
    public static final StatDefinition N_GROUP_COMMIT_MAX_EXCEEDED =
        new StatDefinition(
            N_GROUP_COMMIT_MAX_EXCEEDED_NAME,
            N_GROUP_COMMIT_MAX_EXCEEDED_DESC);

    public static final String N_GROUP_COMMIT_TXNS_NAME =
        "nGroupCommitTxns";
    public static final String N_GROUP_COMMIT_TXNS_DESC =
        "Number of replay transaction commits that were part of a group " +
            "commit operation.";
    public static final StatDefinition N_GROUP_COMMIT_TXNS =
        new StatDefinition(
            N_GROUP_COMMIT_TXNS_NAME,
            N_GROUP_COMMIT_TXNS_DESC);

    public static final String N_GROUP_COMMITS_NAME =
        "nGroupCommits";
    public static final String N_GROUP_COMMITS_DESC =
        "Number of group commit operations.";
    public static final StatDefinition N_GROUP_COMMITS =
        new StatDefinition(
            N_GROUP_COMMITS_NAME,
            N_GROUP_COMMITS_DESC);

    public static final String N_COMMIT_ACKS_NAME =
        "nCommitAcks";
    public static final String N_COMMIT_ACKS_DESC =
        "Number of commits for which the Master requested an ack.";
    public static final StatDefinition N_COMMIT_ACKS =
        new StatDefinition(
            N_COMMIT_ACKS_NAME,
            N_COMMIT_ACKS_DESC);

    public static final String N_COMMIT_SYNCS_NAME =
        "nCommitSyncs";
    public static final String N_COMMIT_SYNCS_DESC =
        "Number of CommitSyncs used to satisfy ack requests. Note that user " +
            "level commit sync requests may be optimized into CommitNoSync " +
            "requests as part of a group commit.";
    public static final StatDefinition N_COMMIT_SYNCS =
        new StatDefinition(
            N_COMMIT_SYNCS_NAME,
            N_COMMIT_SYNCS_DESC);

    public static final String N_COMMIT_NO_SYNCS_NAME =
        "nCommitNoSyncs";
    public static final String N_COMMIT_NO_SYNCS_DESC =
        "Number of CommitNoSyncs used to satisfy ack requests.";
    public static final StatDefinition N_COMMIT_NO_SYNCS =
        new StatDefinition(
            N_COMMIT_NO_SYNCS_NAME,
            N_COMMIT_NO_SYNCS_DESC);

    public static final String N_COMMIT_WRITE_NO_SYNCS_NAME =
        "nCommitWriteNoSyncs";
    public static final String N_COMMIT_WRITE_NO_SYNCS_DESC =
        "Number of CommitWriteNoSyncs used to satisfy ack requests.";
    public static final StatDefinition N_COMMIT_WRITE_NO_SYNCS =
        new StatDefinition(
            N_COMMIT_WRITE_NO_SYNCS_NAME,
            N_COMMIT_WRITE_NO_SYNCS_DESC);

    public static final String N_ABORTS_NAME =
        "nAborts";
    public static final String N_ABORTS_DESC =
        "Number of Aborts replayed by the Replica.";
    public static final StatDefinition N_ABORTS =
        new StatDefinition(
            N_ABORTS_NAME,
            N_ABORTS_DESC);

    public static final String N_LNS_NAME =
        "nLNs";
    public static final String N_LNS_DESC =
        "Number of LNs.";
    public static final StatDefinition N_LNS =
        new StatDefinition(
            N_LNS_NAME,
            N_LNS_DESC);

    public static final String N_NAME_LNS_NAME =
        "nNameLNs";
    public static final String N_NAME_LNS_DESC =
        "Number of Name LNs.";
    public static final StatDefinition N_NAME_LNS =
        new StatDefinition(
            N_NAME_LNS_NAME,
            N_NAME_LNS_DESC);

    public static final String N_ELAPSED_TXN_TIME_NAME =
        "nElapsedTxnTime";
    public static final String N_ELAPSED_TXN_TIME_DESC =
        "The elapsed time in ms, spent replaying all transactions.";
    public static final StatDefinition N_ELAPSED_TXN_TIME =
        new StatDefinition(
            N_ELAPSED_TXN_TIME_NAME,
            N_ELAPSED_TXN_TIME_DESC);

    public static final String N_MESSAGE_QUEUE_OVERFLOWS_NAME =
        "nMessageQueueOverflows";
    public static final String N_MESSAGE_QUEUE_OVERFLOWS_DESC =
        "Number of failed attempts to place an entry in the replica message " +
            "queue due to the queue being full.";
    public static final StatDefinition N_MESSAGE_QUEUE_OVERFLOWS =
        new StatDefinition(
            N_MESSAGE_QUEUE_OVERFLOWS_NAME,
            N_MESSAGE_QUEUE_OVERFLOWS_DESC);

    public static final String MIN_COMMIT_PROCESSING_NANOS_NAME =
        "minCommitProcessingNanos";
    public static final String MIN_COMMIT_PROCESSING_NANOS_DESC =
        "Minimum nanosecs for commit processing";
    public static final StatDefinition MIN_COMMIT_PROCESSING_NANOS =
        new StatDefinition(
            MIN_COMMIT_PROCESSING_NANOS_NAME,
            MIN_COMMIT_PROCESSING_NANOS_DESC);

    public static final String MAX_COMMIT_PROCESSING_NANOS_NAME =
        "maxCommitProcessingNanos";
    public static final String MAX_COMMIT_PROCESSING_NANOS_DESC =
        "Maximum nanosecs for commit processing";
    public static final StatDefinition MAX_COMMIT_PROCESSING_NANOS =
        new StatDefinition(
            MAX_COMMIT_PROCESSING_NANOS_NAME,
            MAX_COMMIT_PROCESSING_NANOS_DESC);

    public static final String TOTAL_COMMIT_PROCESSING_NANOS_NAME =
        "totalCommitProcessingNanos";
    public static final String TOTAL_COMMIT_PROCESSING_NANOS_DESC =
        "Total nanosecs for commit processing";
    public static final StatDefinition TOTAL_COMMIT_PROCESSING_NANOS =
        new StatDefinition(
            TOTAL_COMMIT_PROCESSING_NANOS_NAME,
            TOTAL_COMMIT_PROCESSING_NANOS_DESC);

    public static final String TOTAL_COMMIT_LAG_MS_NAME =
        "totalCommitLagMs";
    public static final String TOTAL_COMMIT_LAG_MS_DESC =
        "Sum of time periods, in msec, between when update operations commit " +
            "on the master and then subsequently commit on the replica. This " +
            "value is affected by any clock skew between the master and the " +
            "replica.";
    public static final StatDefinition TOTAL_COMMIT_LAG_MS =
        new StatDefinition(
            TOTAL_COMMIT_LAG_MS_NAME,
            TOTAL_COMMIT_LAG_MS_DESC);

    public static final String LATEST_COMMIT_LAG_MS_NAME =
        "latestCommitLagMs";
    public static final String LATEST_COMMIT_LAG_MS_DESC =
        "Time in msec between when the latest update operation committed on " +
            "the master and then subsequently committed on the replica. This " +
            "value is affected by any clock skew between the master and the " +
            "replica.";
    public static final StatDefinition LATEST_COMMIT_LAG_MS =
        new StatDefinition(
            LATEST_COMMIT_LAG_MS_NAME,
            LATEST_COMMIT_LAG_MS_DESC);
}
