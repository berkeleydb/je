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

package com.sleepycat.je.rep;

import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_FEEDERS_CREATED;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_FEEDERS_SHUTDOWN;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_MAX_REPLICA_LAG;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_MAX_REPLICA_LAG_NAME;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_DELAY_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LAST_COMMIT_TIMESTAMP_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LAST_COMMIT_VLSN_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_VLSN_LAG_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_VLSN_RATE_MAP;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.LATEST_COMMIT_LAG_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.MAX_COMMIT_PROCESSING_NANOS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.MIN_COMMIT_PROCESSING_NANOS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_ABORTS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMITS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_ACKS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_NO_SYNCS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_SYNCS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_WRITE_NO_SYNCS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_ELAPSED_TXN_TIME;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMITS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMIT_MAX_EXCEEDED;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMIT_TIMEOUTS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMIT_TXNS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_LNS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_NAME_LNS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.TOTAL_COMMIT_LAG_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.TOTAL_COMMIT_PROCESSING_NANOS;
import static com.sleepycat.je.rep.impl.node.ReplicaStatDefinition.N_LAG_CONSISTENCY_WAITS;
import static com.sleepycat.je.rep.impl.node.ReplicaStatDefinition.N_LAG_CONSISTENCY_WAIT_MS;
import static com.sleepycat.je.rep.impl.node.ReplicaStatDefinition.N_VLSN_CONSISTENCY_WAITS;
import static com.sleepycat.je.rep.impl.node.ReplicaStatDefinition.N_VLSN_CONSISTENCY_WAIT_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.ACK_WAIT_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LAST_COMMIT_TIMESTAMP;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LAST_COMMIT_VLSN;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TOTAL_TXN_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_NOT_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.VLSN_RATE;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.BYTES_READ_RATE;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.BYTES_WRITE_RATE;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.MESSAGE_READ_RATE;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.MESSAGE_WRITE_RATE;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_BYTES_READ;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_BYTES_WRITTEN;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_ENTRIES_WRITTEN_OLD_VERSION;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_BATCHED;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_READ;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_WRITTEN;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGE_BATCHES;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_READ_NANOS;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_WRITE_NANOS;
import static com.sleepycat.je.utilint.CollectionUtils.emptySortedMap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.ReplayStatDefinition;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.rep.impl.node.ReplicaStatDefinition;
import com.sleepycat.je.rep.stream.FeederTxnStatDefinition;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.rep.vlsn.VLSNIndexStatDefinition;
import com.sleepycat.je.utilint.AtomicLongMapStat;
import com.sleepycat.je.utilint.IntegralLongAvgStat;
import com.sleepycat.je.utilint.LongAvgRateMapStat;
import com.sleepycat.je.utilint.LongAvgRateStat;
import com.sleepycat.je.utilint.LongDiffMapStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Statistics for a replicated environment.
 * <p>
 * The statistics are logically grouped into four categories. Viewing the
 * statistics through {@link ReplicatedEnvironmentStats#toString()} displays
 * the values in these categories, as does viewing the stats through the {@link
 * <a href="{@docRoot}/../jconsole/JConsole-plugin.html">RepJEMonitor
 * mbean</a>}.  Viewing the stats with {@link
 * ReplicatedEnvironmentStats#toStringVerbose()} will provide more detailed
 * descriptions of the stats and stat categories.
 * <p>
 * The current categories are:
 * <ul>
 *  <li><b>FeederManager</b>: A feed is the {@link <a
 * href="{@docRoot}/../ReplicationGuide/introduction.html#replicationstreams">replication
 * stream</a>} between a master and replica. The current number of feeders
 * gives a sense of the connectivity of the replication group.
 *  </li>
 *  <li><b>BinaryProtocol</b>: These statistics center on the network traffic
 *   engendered by the replication stream, and provide a sense of the network
 *   bandwidth seen by the replication group.
 *  </li>
 *  <li><b>Replay</b>: The act of receiving and applying the replication stream
 *  at the Replica node is called Replay. These stats give a sense of how much
 *  load the replica node is experiencing when processing the traffic from the
 *  replication group.
 *  </li>
 *  <li><b>ConsistencyTracker</b>: The tracker is invoked when consistency
 *  policies are used at a replica node. This provides a measure of delays
 *  experienced by read requests at a replica, in order to conform with the
 *  consistency specified by the application.
 *  </li>
 * </ul>
 *
 * @see <a href="{@docRoot}/../jconsole/JConsole-plugin.html">Viewing
 * Statistics with JConsole</a>
 */
public class ReplicatedEnvironmentStats implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The "impossible" return value used by stats accessors to indicate the
     * statistic is not available in this instance of
     * ReplicatedEnvironmentStats, because it represents an earlier
     * de-serialized instance in which this statistic was unavailable.
     */
    private static final int VALUE_UNAVAILABLE = -1;

    private StatGroup feederManagerStats;
    private StatGroup feederTxnStats;
    private StatGroup replayStats;
    private StatGroup trackerStats;
    private StatGroup protocolStats;
    private StatGroup vlsnIndexStats;

    private final Map<String, String> tipsMap = new HashMap<String, String>();

    ReplicatedEnvironmentStats(RepImpl repImpl, StatsConfig config) {
        final RepNode repNode = repImpl.getRepNode();
        final FeederManager feederManager = repNode.feederManager();

        feederManagerStats = feederManager.getFeederManagerStats(config);
        feederTxnStats = repNode.getFeederTxns().getStats(config);

        final Replica replica = repNode.getReplica();
        replayStats = replica.getReplayStats(config);
        trackerStats = replica.getTrackerStats(config);
        protocolStats = feederManager.getProtocolStats(config);
        vlsnIndexStats = repImpl.getVLSNIndex().getStats(config);

        protocolStats.addAll(replica.getProtocolStats(config));
        addMessageRateStats();
        addBytesRateStats();
    }

    /**
     * @hidden
     * Internal use only.
     */
    public ReplicatedEnvironmentStats() {
    }

    /**
     * @hidden
     * Internal use only.
     */
    public Collection<StatGroup> getStatGroups() {
        return (feederTxnStats != null) ?
            Arrays.asList(feederManagerStats,
                          feederTxnStats,
                          replayStats,
                          trackerStats,
                          protocolStats,
                          vlsnIndexStats) :
            Arrays.asList(feederManagerStats,
                          replayStats,
                          trackerStats,
                          protocolStats,
                          vlsnIndexStats);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public Map<String, StatGroup> getStatGroupsMap() {
        HashMap<String, StatGroup> statmap = new HashMap<String, StatGroup>();
        statmap.put(feederManagerStats.getName(), feederManagerStats);
        statmap.put(replayStats.getName(), replayStats);
        statmap.put(trackerStats.getName(), trackerStats);
        statmap.put(protocolStats.getName(), protocolStats);
        statmap.put(vlsnIndexStats.getName(), vlsnIndexStats);
        if (feederTxnStats != null) {
            statmap.put(feederTxnStats.getName(), feederTxnStats);
        }
        return statmap;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setStatGroup(StatGroup sg) {

        if (sg.getName().equals(FeederManagerStatDefinition.GROUP_NAME)) {
            feederManagerStats = sg;
        } else if (sg.getName().equals(ReplayStatDefinition.GROUP_NAME)) {
            replayStats = sg;
        } else if (sg.getName().equals(ReplicaStatDefinition.GROUP_NAME)) {
            trackerStats = sg;
        } else if (sg.getName().equals
                       (BinaryProtocolStatDefinition.GROUP_NAME)) {
            protocolStats = sg;
        } else if (sg.getName().equals(VLSNIndexStatDefinition.GROUP_NAME)) {
           vlsnIndexStats = sg;
        } else if (sg.getName().equals(FeederTxnStatDefinition.GROUP_NAME)) {
            feederTxnStats = sg;
        } else {
            throw EnvironmentFailureException.unexpectedState
                ("Internal error stat context is not registered");
        }
    }

    /**
     * @hidden
     * Internal use only
     *
     * For JConsole plugin support.
     */
    public static String[] getStatGroupTitles() {
        return new String[] {
            FeederManagerStatDefinition.GROUP_NAME,
            FeederTxnStatDefinition.GROUP_NAME,
            BinaryProtocolStatDefinition.GROUP_NAME,
            ReplayStatDefinition.GROUP_NAME,
            ReplicaStatDefinition.GROUP_NAME,
            VLSNIndexStatDefinition.GROUP_NAME};
    }

    private void addMessageRateStats() {
        long numerator;
        long denominator;

        numerator = (protocolStats.getLongStat(N_MESSAGES_READ) == null) ?
                     0 : protocolStats.getLongStat(N_MESSAGES_READ).get();
        denominator = (protocolStats.getLongStat(N_READ_NANOS) == null) ?
                0 : protocolStats.getLongStat(N_READ_NANOS).get();
        @SuppressWarnings("unused")
        IntegralLongAvgStat msgReadRate =
            new IntegralLongAvgStat
                (protocolStats,
                 MESSAGE_READ_RATE,
                 numerator,
                 denominator,
                 1000000000);

        numerator = (protocolStats.getLongStat(N_MESSAGES_WRITTEN) == null) ?
                0 : protocolStats.getLongStat(N_MESSAGES_WRITTEN).get();
        denominator = (protocolStats.getLongStat(N_WRITE_NANOS) == null) ?
           0 : protocolStats.getLongStat(N_WRITE_NANOS).get();
        @SuppressWarnings("unused")
        IntegralLongAvgStat msgWriteRate =
            new IntegralLongAvgStat
                (protocolStats,
                 MESSAGE_WRITE_RATE,
                 numerator,
                 denominator,
                 1000000000);
    }

    private void addBytesRateStats() {
        long numerator;
        long denominator;

        numerator = (protocolStats.getLongStat(N_BYTES_READ) == null) ?
                     0 : protocolStats.getLongStat(N_BYTES_READ).get();
        denominator = (protocolStats.getLongStat(N_READ_NANOS) == null) ?
                0 : protocolStats.getLongStat(N_READ_NANOS).get();
        @SuppressWarnings("unused")
        IntegralLongAvgStat bytesReadRate =
            new IntegralLongAvgStat
                (protocolStats,
                 BYTES_READ_RATE,
                 numerator,
                 denominator,
                 1000000000);

        numerator = (protocolStats.getLongStat(N_BYTES_WRITTEN) == null) ?
                0 : protocolStats.getLongStat(N_BYTES_WRITTEN).get();
        denominator = (protocolStats.getLongStat(N_WRITE_NANOS) == null) ?
           0 : protocolStats.getLongStat(N_WRITE_NANOS).get();
        @SuppressWarnings("unused")
        IntegralLongAvgStat bytesWriteRate =
            new IntegralLongAvgStat
                (protocolStats,
                 BYTES_WRITE_RATE,
                 numerator,
                 denominator,
                 1000000000);
    }

    /* Feeder Stats. */

    /**
     * The number of Feeder threads since this node was started. A Master
     * supplies the Replication Stream to a Replica via a Feeder thread. The
     * Feeder thread is created when a Replica connects to the node and is
     * shutdown when the connection is terminated.
     */
    public int getNFeedersCreated() {
        return feederManagerStats.getInt(N_FEEDERS_CREATED);
    }

    /**
     * The number of Feeder threads that were shut down, either because this
     * node, or the Replica terminated the connection.
     *
     * @see #getNFeedersCreated()
     */
    public int getNFeedersShutdown() {
        return feederManagerStats.getInt(N_FEEDERS_SHUTDOWN);
    }

    /**
     * The lag (in VLSNs) associated with the replica that's farthest behind in
     * replaying the replication stream.
     */
    public long getNMaxReplicaLag() {

        /* TODO: Implement using REPLICA_VLSN_LAG_MAP */
        return feederManagerStats.getLong(N_MAX_REPLICA_LAG);
    }

    /**
     * The name of the replica that's farthest behind in replaying the
     * replication stream.
     */
    public String getNMaxReplicaLagName() {

        /* TODO: Implement using REPLICA_VLSN_LAG_MAP */
        return feederManagerStats.getString(N_MAX_REPLICA_LAG_NAME);
    }

    /**
     * Returns a map from replica node name to the delay, in milliseconds,
     * between when a transaction was committed on the master and when the
     * master learned that the transaction was processed on the replica, if
     * known.  Returns an empty map if this node is not the master.
     *
     * @since 6.3.0
     */
    public SortedMap<String, Long> getReplicaDelayMap() {
        final LongDiffMapStat stat =
            (LongDiffMapStat) feederManagerStats.getStat(REPLICA_DELAY_MAP);
        if (stat == null) {
            return emptySortedMap();
        }
        return stat.getMap();
    }

    /**
     * Returns a map from replica node name to the commit timestamp of the last
     * committed transaction that was processed on the replica, if known.
     * Returns an empty map if this node is not the master.
     *
     * @since 6.3.0
     */
    public SortedMap<String, Long> getReplicaLastCommitTimestampMap() {
        final AtomicLongMapStat stat = (AtomicLongMapStat)
            feederManagerStats.getStat(REPLICA_LAST_COMMIT_TIMESTAMP_MAP);
        if (stat == null) {
            return emptySortedMap();
        }
        return stat.getMap();
    }

    /**
     * Returns a map from replica node name to the VLSN of the last committed
     * transaction that was processed on the replica, if known.  Returns an
     * empty map if this node is not the master.
     *
     * @since 6.3.0
     */
    public SortedMap<String, Long> getReplicaLastCommitVLSNMap() {
        final AtomicLongMapStat stat = (AtomicLongMapStat)
            feederManagerStats.getStat(REPLICA_LAST_COMMIT_VLSN_MAP);
        if (stat == null) {
            return emptySortedMap();
        }
        return stat.getMap();
    }

    /**
     * Returns a map from replica node name to the lag, in VLSNs, between the
     * replication state of the replica and the master, if known.  Returns an
     * empty map if this node is not the master.
     *
     * @since 6.3.0
     */
    public SortedMap<String, Long> getReplicaVLSNLagMap() {
        final LongDiffMapStat stat =
            (LongDiffMapStat) feederManagerStats.getStat(REPLICA_VLSN_LAG_MAP);
        if (stat == null) {
            return emptySortedMap();
        }
        return stat.getMap();
    }

    /**
     * Returns a map from replica node name to a moving average of the rate, in
     * VLSNs per minute, that the replica is processing replication data, if
     * known.  Returns an empty map if this node is not the master.
     *
     * @since 6.3.0
     */
    public SortedMap<String, Long> getReplicaVLSNRateMap() {
        final LongAvgRateMapStat stat = (LongAvgRateMapStat)
            feederManagerStats.getStat(REPLICA_VLSN_RATE_MAP);
        if (stat == null) {
            return emptySortedMap();
        }
        return stat.getMap();
    }

    /* Master transaction commit acknowledgment statistics. */

    /**
     * The number of transactions that were successfully acknowledged based
     * upon the {@link ReplicaAckPolicy} policy associated with the
     * transaction commit.
     */
    public long getNTxnsAcked() {
        return (feederTxnStats == null) ?
               VALUE_UNAVAILABLE :
               feederTxnStats.getAtomicLong(TXNS_ACKED);
    }

    /**
     * The number of transactions that were not acknowledged as required by the
     * {@link ReplicaAckPolicy} policy associated with the transaction commit.
     * These transactions resulted in {@link InsufficientReplicasException} or
     * {@link InsufficientAcksException}.
     */
    public long getNTxnsNotAcked() {
        return (feederTxnStats == null) ?
               VALUE_UNAVAILABLE :
               feederTxnStats.getAtomicLong(TXNS_NOT_ACKED);
    }

    /**
     * The total time in milliseconds spent in replicated transactions. This
     * represents the time from the start of the transaction until its
     * successful commit and acknowledgment. It includes the time spent
     * waiting for transaction commit acknowledgments, as determined by
     * {@link #getAckWaitMs()}.
     */
    public long getTotalTxnMs() {
        return (feederTxnStats == null) ?
                VALUE_UNAVAILABLE :
                feederTxnStats.getAtomicLong(TOTAL_TXN_MS);
    }

    /**
     * The total time in milliseconds that the master spent waiting for the
     * {@link ReplicaAckPolicy} to be satisfied during successful transaction
     * commits.
     *
     * @see #getTotalTxnMs()
     */
    public long getAckWaitMs() {
        return (feederTxnStats == null) ?
                VALUE_UNAVAILABLE :
                feederTxnStats.getAtomicLong(ACK_WAIT_MS);
    }

    /**
     * The VLSN of the last committed transaction on the master, or 0 if not
     * known or this node is not the master.
     *
     * @since 6.3.0
     */
    public long getLastCommitVLSN() {
        return (feederTxnStats == null) ?
            VALUE_UNAVAILABLE :
            feederTxnStats.getAtomicLong(LAST_COMMIT_VLSN);
    }

    /**
     * The commit timestamp of the last committed transaction on the master, or
     * 0 if not known or this node is not the master.
     *
     * @since 6.3.0
     */
    public long getLastCommitTimestamp() {
        return (feederTxnStats == null) ?
            VALUE_UNAVAILABLE :
            feederTxnStats.getAtomicLong(LAST_COMMIT_TIMESTAMP);
    }

    /**
     * A moving average of the rate replication data is being generated by the
     * master, in VLSNs per minute, or 0 if not known or this node is not the
     * master.
     *
     * @since 6.3.0
     */
    public long getVLSNRate() {
        if (feederTxnStats == null) {
            return VALUE_UNAVAILABLE;
        }
        final LongAvgRateStat stat =
            (LongAvgRateStat) feederTxnStats.getStat(VLSN_RATE);
        return (stat != null) ? stat.get() : 0;
    }

    /* Replay Stats. */

    /**
     * The number of commit log records that were replayed by this node when
     * it was a Replica. There is one commit record record for each actual
     * commit on the Master.
     */
    public long getNReplayCommits() {
        return replayStats.getLong(N_COMMITS);
    }

    /**
     * The number of commit log records that needed to be acknowledged to the
     * Master by this node when it was a Replica. The rate of change of this
     * statistic, will show a strong correlation with that of
     * <code>NReplayCommits</code> statistic, if the <code>Durability</code>
     * policy used by transactions on the master calls for transaction commit
     * acknowledgments and the Replica is current with respect to the Master.
     */
    public long getNReplayCommitAcks() {
        return replayStats.getLong(N_COMMIT_ACKS);
    }

    /**
     * The number of commitSync() calls executed when satisfying transaction
     * commit acknowledgment requests from the Master.
     */
    public long getNReplayCommitSyncs() {
        return replayStats.getLong(N_COMMIT_SYNCS);
    }

    /**
     * The number of commitNoSync() calls executed when satisfying transaction
     * commit acknowledgment requests from the Master.
     */
    public long getNReplayCommitNoSyncs() {
        return replayStats.getLong(N_COMMIT_NO_SYNCS);
    }

    /**
     * The number of commitNoSync() calls executed when satisfying transaction
     * commit acknowledgment requests from the Master.
     */
    public long getNReplayCommitWriteNoSyncs() {
        return replayStats.getLong(N_COMMIT_WRITE_NO_SYNCS);
    }

    /**
     * The number of abort records which were replayed while the node was in
     * the Replica state.
     */
    public long getNReplayAborts() {
        return replayStats.getLong(N_ABORTS);
    }

    /**
     * The number of NameLN records which were replayed while the node was in
     * the Replica state.
     */
    public long getNReplayNameLNs() {
        return replayStats.getLong(N_NAME_LNS);
    }

    /**
     * The number of data records (creation, update, deletion) which were
     * replayed while the node was in the Replica state.
     */
    public long getNReplayLNs() {
        return replayStats.getLong(N_LNS);
    }

    /**
     * The total elapsed time in milliseconds spent replaying committed and
     * aborted transactions.
     */
    public long getReplayElapsedTxnTime() {
        return replayStats.getLong(N_ELAPSED_TXN_TIME);
    }

    /**
     * The number of group commits that were initiated due to the
     * {@link ReplicationConfig#REPLICA_GROUP_COMMIT_INTERVAL group timeout
     * interval} being exceeded.
     *
     * @since 5.0.76
     */
    public long getNReplayGroupCommitTimeouts() {
        return replayStats.getLong(N_GROUP_COMMIT_TIMEOUTS);
    }

    /**
     * The number of group commits that were initiated due the
     * {@link ReplicationConfig#REPLICA_MAX_GROUP_COMMIT max group size} being
     * exceeded.
     *
     * @since 5.0.76
     */
     public long getNReplayGroupCommitMaxExceeded() {
         return replayStats.getLong(N_GROUP_COMMIT_MAX_EXCEEDED);
     }

    /**
     * The number of replay transaction commits that were part of a group
     * commit operation.
     *
     * @since 5.0.76
     */
     public long getNReplayGroupCommitTxns() {
         return replayStats.getLong(N_GROUP_COMMIT_TXNS);
     }

     /**
      * The number of group commit operations.
      *
      * @since 5.0.76
      */
     public long getNReplayGroupCommits() {
         return replayStats.getLong(N_GROUP_COMMITS);
     }

    /**
     * The minimum time taken to replay a transaction commit operation.
     */
    public long getReplayMinCommitProcessingNanos() {
        return replayStats.getLong(MIN_COMMIT_PROCESSING_NANOS);
    }

    /**
     * The maximum time taken to replay a transaction commit operation.
     */
    public long getReplayMaxCommitProcessingNanos() {
        return replayStats.getLong(MAX_COMMIT_PROCESSING_NANOS);
    }

    /**
     * The total time spent to replay all commit operations.
     */
    public long getReplayTotalCommitProcessingNanos() {
        return replayStats.getLong(TOTAL_COMMIT_PROCESSING_NANOS);
    }

    /**
     * @hidden
     * TODO: Make visible after experimenting with this new stat
     *
     * The sum of time periods, measured in milliseconds, between when update
     * operations commit on the master and then subsequently commit on the
     * replica.  Divide this value by the total number of commit operations,
     * available by calling {@link #getNReplayCommits}, to find the average
     * commit lag for a single operation.
     *
     * <p>Note that each lag is computed on the replica by comparing the time
     * of the master commit, as measured by the master, and time on the replica
     * when it commits locally.  As a result, the return value will be affected
     * by any clock skew between the master and the replica.
     */
    public long getReplayTotalCommitLagMs() {
        return replayStats.getLong(TOTAL_COMMIT_LAG_MS);
    }

    /**
     * @hidden
     * TODO: Make visible after experimenting with this new stat
     *
     * The time in milliseconds between when the latest update operation
     * committed on the master and then subsequently committed on the replica.
     *
     * <p>Note that the lag is computed on the replica by comparing the time of
     * the master commit, as measured by the master, and time on the replica
     * when it commits locally.  As a result, the return value will be affected
     * by any clock skew between the master and the replica.
     */
    public long getReplayLatestCommitLagMs() {
        return replayStats.getLong(LATEST_COMMIT_LAG_MS);
    }

    /* Protocol Stats. */

    /**
     * The number of bytes of Replication Stream read over the network. It does
     * not include the TCP/IP overhead.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the sum total of all Feeder related
     * network activity, as well as Replica network activity.
     */
    public long getNProtocolBytesRead() {
        return protocolStats.getLong(N_BYTES_READ);
    }

    /**
     * The number of Replication Stream messages read over the network.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the sum total of all Feeder related
     * network activity, as well as Replica network activity.
     */
    public long getNProtocolMessagesRead() {
        return protocolStats.getLong(N_MESSAGES_READ);
    }

    /**
     * The number of Replication Stream bytes written over the network.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the sum total of all Feeder related
     * network activity, as well as Replica network activity.
     */
    public long getNProtocolBytesWritten() {
        return protocolStats.getLong(N_BYTES_WRITTEN);
    }

    /**
     * The number of Replication Stream messages that were written as part
     * of a message batch instead of being written  individually.
     *
     * It represents a subset of the messages returned by
     * {@link #getNProtocolMessagesWritten()}
     *
     * @see #getNProtocolMessageBatches
     *
     * @since 6.2.7
     */
    public long getNProtocolMessagesBatched() {
        return protocolStats.getLong(N_MESSAGES_BATCHED);
    }

    /**
     * The number of Replication Stream message batches written to the network.
     *
     * @see #getNProtocolMessagesBatched
     *
     * @since 6.2.7
     */
    public long getNProtocolMessageBatches() {
        return protocolStats.getLong(N_MESSAGE_BATCHES);
    }

    /**
     * The total number of Replication Stream messages written over the
     * network.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the sum total of all Feeder related
     * network activity, as well as Replica network activity.
     */
    public long getNProtocolMessagesWritten() {
        return protocolStats.getLong(N_MESSAGES_WRITTEN);
    }

    /**
     * The number of nanoseconds spent reading from the network channel.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the sum total of all Feeder related
     * network activity, as well as Replica network activity.
     */
    public long getProtocolReadNanos() {
        return protocolStats.getLong(N_READ_NANOS);
    }

    /**
     * The number of nanoseconds spent writing to the network channel.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the sum total of all Feeder related
     * network activity, as well as Replica network activity.
     */
    public long getProtocolWriteNanos() {
        return protocolStats.getLong(N_WRITE_NANOS);
    }

    /**
     * Incoming replication message throughput, in terms of messages received
     * from the replication network channels per second.
     * <p> If the node has served as both a Replica and Master since
     * it was first started, the number represents the message reading rate
     * over all Feeder related network activity, as well as Replica network
     * activity.
     */
    public long getProtocolMessageReadRate() {
        IntegralLongAvgStat rstat =
            protocolStats.getIntegralLongAvgStat(MESSAGE_READ_RATE);
        return (rstat != null) ? rstat.get().longValue() : 0;
    }

    /**
     * Outgoing message throughput, in terms of message written to the
     * replication network channels per second.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the message writing rate over all Feeder
     * related network activity, as well as Replica network activity.
     */
    public long getProtocolMessageWriteRate() {
        IntegralLongAvgStat rstat =
            protocolStats.getIntegralLongAvgStat(MESSAGE_WRITE_RATE);
        return (rstat != null) ? rstat.get().longValue() : 0;
    }

    /**
     * Bytes read throughput, in terms of bytes received from the replication
     * network channels per second.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the bytes reading rate over all Feeder
     * related network activity, as well as Replica network activity.
     */
    public long getProtocolBytesReadRate() {
        IntegralLongAvgStat rstat =
            protocolStats.getIntegralLongAvgStat(BYTES_READ_RATE);
        return (rstat != null) ? rstat.get().longValue() : 0;
    }

    /**
     * Bytes written throughput, in terms of bytes written to the replication
     * network channels per second.
     * <p>
     * If the node has served as both a Replica and Master since it was first
     * started, the number represents the bytes writing rate over all Feeder
     * related network activity, as well as Replica network activity.
     */
    public long getProtocolBytesWriteRate() {
        IntegralLongAvgStat rstat =
            protocolStats.getIntegralLongAvgStat(BYTES_WRITE_RATE);
        return (rstat != null) ? rstat.get().longValue() : 0;
    }

    /**
     * Returns the number of messages containing log entries that were written
     * to the replication stream using the previous log format to support
     * replication to a replica running an earlier version during an upgrade.
     */
    public long getNProtocolEntriesWrittenOldVersion() {
        return protocolStats.getLong(N_ENTRIES_WRITTEN_OLD_VERSION);
    }

    /* ConsistencyTracker Stats. */

    /**
     * The number of times a Replica held back a
     * {@link Environment#beginTransaction(Transaction,TransactionConfig)}
     * operation to satisfy the {@link TimeConsistencyPolicy}.
     */
    public long getTrackerLagConsistencyWaits() {
        return trackerStats.getLong(N_LAG_CONSISTENCY_WAITS);
    }

    /**
     * The total time (in msec) for which a Replica held back a
     * {@link Environment#beginTransaction(Transaction,TransactionConfig)}
     * operation to satisfy the {@link TimeConsistencyPolicy}.
     */
    public long getTrackerLagConsistencyWaitMs() {
        return trackerStats.getLong(N_LAG_CONSISTENCY_WAIT_MS);
    }

    /**
     * The number of times a Replica held back a
     * {@link Environment#beginTransaction(Transaction,TransactionConfig)}
     * operation to satisfy the {@link CommitPointConsistencyPolicy}.
     */
    public long getTrackerVLSNConsistencyWaits() {
        return trackerStats.getLong(N_VLSN_CONSISTENCY_WAITS);
    }

    /**
     * The total time (in msec) for which a Replica held back a
     * {@link Environment#beginTransaction(Transaction,TransactionConfig)}
     * operation to satisfy the {@link CommitPointConsistencyPolicy}.
     */
    public long getTrackerVLSNConsistencyWaitMs() {
        return trackerStats.getLong(N_VLSN_CONSISTENCY_WAIT_MS);
    }

    /**
     * Returns a string representation of the statistics.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (StatGroup group : getStatGroups()) {
            sb.append(group.toString());
        }

        return sb.toString();
    }

    public String toStringVerbose() {
        StringBuilder sb = new StringBuilder();

        for (StatGroup group : getStatGroups()) {
            sb.append(group.toStringVerbose());
        }

        return sb.toString();
    }

    public Map<String, String> getTips() {
        /* Add FeederManager stats definition. */

        for (StatGroup group : getStatGroups()) {
            tipsMap.put(group.getName(), group.getDescription());
            for (StatDefinition def : group.getStats().keySet()) {
                tipsMap.put(def.getName(), def.getDescription());
            }
        }

        return tipsMap;
    }
}
