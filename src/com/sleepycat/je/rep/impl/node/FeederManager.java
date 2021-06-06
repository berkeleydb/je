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

import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_FEEDERS_CREATED;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_FEEDERS_SHUTDOWN;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_MAX_REPLICA_LAG;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_MAX_REPLICA_LAG_NAME;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_DELAY_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LAST_COMMIT_TIMESTAMP_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LAST_COMMIT_VLSN_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_VLSN_LAG_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_VLSN_RATE_MAP;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNTracker;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.MasterStatus.MasterSyncException;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.rep.utilint.IntRunningTotalStat;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.SizeAwaitMap;
import com.sleepycat.je.rep.utilint.SizeAwaitMap.Predicate;
import com.sleepycat.je.utilint.AtomicLongMapStat;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvgRateMapStat;
import com.sleepycat.je.utilint.LongDiffMapStat;
import com.sleepycat.je.utilint.LongMaxZeroStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StringStat;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * FeedManager is responsible for the creation and management of the Feeders
 * used to respond to connections initiated by a Replica. runfeeders() is the
 * central loop that listens for replica connections and manages the lifecycle
 * of individual Feeders. It's re-entered each time the node becomes a Master
 * and is exited when its status changes.
 *
 * There is a single instance of FeederManager that is created for a
 * replication node. There are many instances of Feeders per FeederManager.
 * Each Feeder instance represents an instance of a connection between the node
 * serving as the feeder and the replica.
 *
 * Note that the FeederManager and the Replica currently reuse the Replication
 * node's thread of control. When we implement r2r we will need to revisit the
 * thread management to provide for concurrent operation of the FeederManger
 * and the Replica.
 */
final public class FeederManager {

    private final RepNode repNode;

    /*
     * The queue into which the ServiceDispatcher queues socket channels for
     * new Feeder instances.
     */
    private final BlockingQueue<DataChannel> channelQueue =
        new LinkedBlockingQueue<DataChannel>();

    /*
     * Feeders are stored in either nascentFeeders or activeFeeders, and not
     * both.  To avoid deadlock, if locking both collections, lock
     * nascentFeeders first and then activeFeeders.
     */

    /*
     * Nascent feeders that are starting up and are not yet active. They have
     * network connections but have not synched up or completed handshakes.
     * They are moved into the active feeder map, once they become active.
     */
    private final Set<Feeder> nascentFeeders =
        Collections.synchronizedSet(new HashSet<Feeder>());

    /*
     * The collection of active feeders currently feeding replicas. The map is
     * indexed by the Replica's node name. Access to this map must be
     * synchronized, since it's updated concurrently by the Feeders that share
     * it.
     *
     * A feeder is considered to be active after it has completed the handshake
     * sequence with its associated Replica.
     *
     * Note that the SizeAwaitMap will only wait for feeders that are connected
     * to electable replicas, since those are the only ones participating in
     * durability decisions.
     */
    private final SizeAwaitMap<String, Feeder> activeFeeders;

    /*
     * The number of active ack feeders feeding electable, i.e. acking, nodes.
     */
    private final AtomicInteger ackFeeders = new AtomicInteger(0);

    /*
     * The number of arbiter feeders; there can only be one currently. It's
     * Atomic for consistency with ackFeeders.
     */
    private final AtomicInteger arbiterFeeders = new AtomicInteger(0);
    private String arbiterFeederName;

    /*
     * A test delay introduced in the feeder loop to simulate a loaded master.
     * The feeder waits this amount of time after each message is sent.
     */
    private int testDelayMs = 0;

    /* Set to true to force a shutdown of the FeederManager. */
    AtomicBoolean shutdown = new AtomicBoolean(false);

    /*
     * Non null if the replication node must be shutdown as well. This is
     * typically the result of an unexpected exception in the feeder.
     */
    private RuntimeException repNodeShutdownException;

    /**
     * Used to manage the flushing of the DTVLSN via a null TXN commit
     * when appropriate.
     */
    private final DTVLSNFlusher dtvlsnFlusher;

    private final Logger logger;

    /* FeederManager statistics. */
    private final StatGroup stats;
    private final IntStat nFeedersCreated;
    private final IntStat nFeedersShutdown;
    private final LongDiffMapStat replicaDelayMap;
    private final AtomicLongMapStat replicaLastCommitTimestampMap;
    private final AtomicLongMapStat replicaLastCommitVLSNMap;
    private final LongDiffMapStat replicaVLSNLagMap;
    private final LongAvgRateMapStat replicaVLSNRateMap;

    /*
     * The maximum lag across all replicas. Atomic values or synchronization
     * are not used for the shared statistic to minimize overheads and the
     * resulting occasional inaccuracy in the statics is an acceptable
     * tradeoff.
     */
    private final LongMaxZeroStat nMaxReplicaLag;
    private final StringStat nMaxReplicaLagName;

    /* The poll timeout used when accepting feeder connections. */
    public final long pollTimeoutMs ;

    /* Identifies the Feeder Service. */
    public static final String FEEDER_SERVICE = "Feeder";

    /** The moving average period in milliseconds */
    private static final long MOVING_AVG_PERIOD_MILLIS = 10000;

    /**
     * A test hook, parameterized by feeder's Name/ID pair, that delays CBVLSN
     * updates if it throws an IllegalStateException.
     */
    private static volatile TestHook<NameIdPair> delayCBVLSNUpdateHook;

    FeederManager(RepNode repNode) {
        this.repNode = repNode;
        activeFeeders = new SizeAwaitMap<String, Feeder>(
            repNode.getRepImpl(), new MatchElectableFeeders());
        logger = LoggerUtils.getLogger(getClass());
        stats = new StatGroup(FeederManagerStatDefinition.GROUP_NAME,
                              FeederManagerStatDefinition.GROUP_DESC);
        nFeedersCreated = new IntRunningTotalStat(stats, N_FEEDERS_CREATED);
        nFeedersShutdown = new IntRunningTotalStat(stats, N_FEEDERS_SHUTDOWN);
        nMaxReplicaLag = new LongMaxZeroStat(stats, N_MAX_REPLICA_LAG);
        nMaxReplicaLagName = new StringStat(stats, N_MAX_REPLICA_LAG_NAME);

        /*
         * Treat delays and lags as valid for twice the heartbeat interval, to
         * allow for minor networking delays when receiving heartbeats
         */
        final long validityMillis = 2 * repNode.getHeartbeatInterval();
        replicaDelayMap =
            new LongDiffMapStat(stats, REPLICA_DELAY_MAP, validityMillis);
        replicaLastCommitTimestampMap =
            new AtomicLongMapStat(stats, REPLICA_LAST_COMMIT_TIMESTAMP_MAP);
        replicaLastCommitVLSNMap =
            new AtomicLongMapStat(stats, REPLICA_LAST_COMMIT_VLSN_MAP);
        replicaVLSNLagMap =
            new LongDiffMapStat(stats, REPLICA_VLSN_LAG_MAP, validityMillis);
        replicaVLSNRateMap = new LongAvgRateMapStat(
            stats, REPLICA_VLSN_RATE_MAP, MOVING_AVG_PERIOD_MILLIS, MINUTES);

        pollTimeoutMs = repNode.getConfigManager().
            getDuration(RepParams.FEEDER_MANAGER_POLL_TIMEOUT);
        dtvlsnFlusher = new DTVLSNFlusher();
    }

    /**
     * A SizeAwaitMap predicate that matches feeders connected to electable
     * replicas.
     */
    private class MatchElectableFeeders implements Predicate<Feeder> {
        @Override
        public boolean match(final Feeder value) {

            /* The replica node might be null during unit testing */
            final RepNodeImpl replica = value.getReplicaNode();
            return (replica != null) &&
                repNode.getDurabilityQuorum().replicaAcksQualify(replica);
        }
    }

    /**
     * Returns the statistics associated with the FeederManager.
     *
     * @return the statistics
     */
    public StatGroup getFeederManagerStats(StatsConfig config) {

        synchronized (stats) {
            return stats.cloneGroup(config.getClear());
        }
    }

    /* Get the protocol stats for this FeederManager. */
    public StatGroup getProtocolStats(StatsConfig config) {
        /* Aggregate stats that have not yet been aggregated. */
        StatGroup protocolStats =
            new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                          BinaryProtocolStatDefinition.GROUP_DESC);
        synchronized (activeFeeders) {
            for (Feeder feeder : activeFeeders.values()) {
                protocolStats.addAll(feeder.getProtocolStats(config));
            }
        }

        return protocolStats;
    }

    /* Reset the feeders' stats of this FeederManager. */
    public void resetStats() {
        synchronized (stats) {
            stats.clear();
        }
        synchronized (activeFeeders) {
            for (Feeder feeder : activeFeeders.values()) {
                feeder.resetStats();
            }
        }
    }

    /**
     * Accumulates statistics from a terminating feeder.
     * @param feederStats  stats of feeder
     */
    void incStats(StatGroup feederStats) {
        synchronized (stats) {
            stats.addAll(feederStats);
        }
    }

    public int getTestDelayMs() {
        return testDelayMs;
    }

    public void setTestDelayMs(int testDelayMs) {
        this.testDelayMs = testDelayMs;
    }

    /**
     * Returns the RepNode associated with the FeederManager
     * @return
     */
    RepNode repNode() {
        return repNode;
    }

    /**
     * Returns the Feeder associated with the node, if such a feeder is
     * currently active.
     */
    public Feeder getFeeder(String nodeName) {
        return activeFeeders.get(nodeName);
    }

    public Feeder getArbiterFeeder() {
        synchronized (activeFeeders) {
            return activeFeeders.get(arbiterFeederName);
        }
    }

    /*
     * For test use only.
     */
    public Feeder putFeeder(String nodeName, Feeder feeder) {
        /*
         * Can't check for an electable node since the feeder object can be
         * mocked for testing so it does not have a rep node.
         */
        ackFeeders.incrementAndGet();
        return activeFeeders.put(nodeName, feeder);
    }

    public LongMaxZeroStat getnMaxReplicaLag() {
        return nMaxReplicaLag;
    }

    public StringStat getnMaxReplicaLagName() {
        return nMaxReplicaLagName;
    }

    LongDiffMapStat getReplicaDelayMap() {
        return replicaDelayMap;
    }

    AtomicLongMapStat getReplicaLastCommitTimestampMap() {
        return replicaLastCommitTimestampMap;
    }

    AtomicLongMapStat getReplicaLastCommitVLSNMap() {
        return replicaLastCommitVLSNMap;
    }

    LongDiffMapStat getReplicaVLSNLagMap() {
        return replicaVLSNLagMap;
    }

    LongAvgRateMapStat getReplicaVLSNRateMap() {
        return replicaVLSNRateMap;
    }

    void setRepNodeShutdownException(RuntimeException rNSE) {
        this.repNodeShutdownException = rNSE;
    }

    /**
     * The numbers of Replicas currently "active" with this feeder. Active
     * currently means they are connected. It does not make any guarantees
     * about where they are in the replication stream. They may, for example,
     * be too far behind to participate in timely acks.
     *
     * @return the active replica count
     */
    public int activeReplicaCount() {
        return activeFeeders.size();
    }

    public int activeAckReplicaCount() {
        return ackFeeders.get();
    }

    public int activeAckArbiterCount() {
        return arbiterFeeders.get();
    }

    /**
     * Returns the set of Replicas that are currently active with this feeder.
     * A replica is active if it has completed the handshake sequence.
     *
     * @return the set of replica node names
     */
    public Set<String> activeReplicas() {
        synchronized (activeFeeders) {

            /*
             * Create a copy to avoid inadvertent concurrency conflicts,
             * since the keySet is a view of the underlying map.
             */
            return new HashSet<>(activeFeeders.keySet());
        }
    }

    /**
     * Returns the set of active replicas and arbiters, that are currently
     * active with this feeder and are supplying acknowledgments. A replica is
     * active if it has completed the handshake sequence. An Arbiter is only
     * returned if it's in active arbitration.
     *
     * @param includeArbiters include active arbiters in the list of returned
     * node names if true; exclude arbiters otherwise.
     *
     * @return the set of replica and if includeArbiters active arbiter node names
     */
    public  Set<String> activeAckReplicas(boolean includeArbiters) {
        final Set<String> nodeNames = new HashSet<String>();
        synchronized (activeFeeders) {
            for (final Entry<String, Feeder> entry :
                activeFeeders.entrySet()) {
                final Feeder feeder = entry.getValue();

                /* The replica node should be non-null for an active feeder */
                final RepNodeImpl replica = feeder.getReplicaNode();
                if (!replica.getType().isElectable()) {
                    continue;
                }

                if (replica.getType().isArbiter()) {
                    if (!includeArbiters ||
                        !feeder.getRepNode().getArbiter().isActive()) {
                        /* Skip the arbiter. */
                        continue;
                    }
                }

                nodeNames.add(entry.getKey());
            }
        }
        return nodeNames;
    }

    public Map<String, Feeder> activeReplicasMap() {
        synchronized (activeFeeders){
            return new HashMap<String, Feeder>(activeFeeders);
        }
    }

    /**
     * Transitions a Feeder to being active, so that it can be used in
     * considerations relating to commit acknowledgments and decisions about
     * choosing feeders related to system load.
     *
     * @param feeder the feeder being transitioned.
     */
    void activateFeeder(Feeder feeder) {
        synchronized (nascentFeeders) {
            synchronized (activeFeeders) {
                boolean removed = nascentFeeders.remove(feeder);
                if (feeder.isShutdown()) {
                    return;
                }
                assert(removed);
                String replicaName = feeder.getReplicaNameIdPair().getName();
                assert(!feeder.getReplicaNameIdPair().equals(NameIdPair.NULL));
                Feeder dup = activeFeeders.get(replicaName);
                if ((dup != null) && !dup.isShutdown()) {
                    throw EnvironmentFailureException.
                        unexpectedState(repNode.getRepImpl(),
                                        feeder.getReplicaNameIdPair() +
                                        " is present in both nascent and " +
                                        "active feeder sets");
                }
                activeFeeders.put(replicaName, feeder);
                if (feeder.getReplicaNode().getType().isArbiter()) {
                    assert(arbiterFeeders.get() == 0);
                    arbiterFeeders.incrementAndGet();
                    arbiterFeederName = replicaName;

                } else if (feeder.getReplicaNode().getType().isElectable()) {
                    ackFeeders.incrementAndGet();
                }

                MasterTransfer xfr = repNode.getActiveTransfer();
                if (xfr != null) {
                    xfr.addFeeder(feeder);
                }
            }
        }
    }

    /**
     * Remove the feeder from the sets used to track it. Invoked when a feeder
     * is shutdown.
     *
     * @param feeder
     */
    void removeFeeder(Feeder feeder) {
        assert(feeder.isShutdown());
        final String replicaName = feeder.getReplicaNameIdPair().getName();
        synchronized (nascentFeeders) {
            synchronized (activeFeeders) {
                nascentFeeders.remove(feeder);
                if (activeFeeders.remove(replicaName) != null) {
                    if (arbiterFeederName != null &&
                        arbiterFeederName.equals(replicaName)) {
                        arbiterFeeders.decrementAndGet();
                        arbiterFeederName = null;
                    } else if (feeder.getReplicaNode().getType().isElectable()) {
                        ackFeeders.decrementAndGet();
                    }
                }
            }
        }

        final RepNodeImpl node = feeder.getReplicaNode();
        if ((node != null) && node.getType().hasTransientId()) {
            repNode.removeTransientNode(node);
        }
    }

    /**
     * Clears and shuts down the runFeeders by inserting a special EOF marker
     * value into the queue.
     */
    void shutdownQueue() {
        if (!repNode.isShutdown()) {
            throw EnvironmentFailureException.unexpectedState
                ("Rep node is still active");
        }
        channelQueue.clear();
        /* Add special entry so that the channelQueue.poll operation exits. */
        channelQueue.add(RepUtils.CHANNEL_EOF_MARKER);
    }

    /**
     * The core feeder listener loop that is run either in a Master node, or in
     * a Replica that is serving as a Feeder to other Replica nodes. The core
     * loop accepts connections from Replicas as they come in and establishes a
     * Feeder on that connection.
     *
     * The loop can be terminated for one of the following reasons:
     *
     *  1) A change in Masters.
     *
     *  2) A forced shutdown, via a thread interrupt.
     *
     *  3) A server socket level exception.
     *
     * The timeout on the accept is used to ensure that the check is done at
     * least once per timeout period.
     */
    void runFeeders()
        throws DatabaseException {

        if (shutdown.get()) {
            throw EnvironmentFailureException.unexpectedState
                ("Feeder manager was shutdown");
        }
        Exception feederShutdownException = null;
        LoggerUtils.info(logger, repNode.getRepImpl(),
                         "Feeder manager accepting requests.");

        /* Init GlobalCBVLSN using minJEVersion in the rep group DB. */
        repNode.globalCBVLSN.init(repNode, repNode.getMinJEVersion());

        /* This updater represents the masters's local cbvlsn, which the master
           updates directly. */
        final LocalCBVLSNUpdater updater = new LocalCBVLSNUpdater(
            repNode.getNameIdPair(), repNode.getNodeType(), repNode);
        final LocalCBVLSNTracker tracker = repNode.getCBVLSNTracker();

        try {

            /*
             * Ensure that the Global CBVLSN is initialized for the master when
             * it first comes up; it's subsequently maintained in the loop
             * below.
             */
            updater.updateForMaster(tracker);

            repNode.getServiceDispatcher().
                register(FEEDER_SERVICE, channelQueue);

            /*
             * The Feeder is ready for business, indicate that the node is
             * ready by counting down the latch and releasing any waiters.
             */
            repNode.getReadyLatch().countDown();

            while (true) {
                final DataChannel feederReplicaChannel =
                    channelQueue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);

                if (feederReplicaChannel == RepUtils.CHANNEL_EOF_MARKER) {
                    LoggerUtils.info(logger, repNode.getRepImpl(),
                                     "Feeder manager soft shutdown.");
                    return;
                }

                repNode.getMasterStatus().assertSync();
                if (feederReplicaChannel == null) {
                    if (repNode.isShutdownOrInvalid()) {
                        /* Timeout and shutdown request */
                        LoggerUtils.info(logger, repNode.getRepImpl(),
                                         "Feeder manager forced shutdown.");
                        return;
                    }

                    /*
                     * Simulate extending the polling period for channel input
                     * by delay updating the CBVLSN, to allow tests to control
                     * the timing.
                     */
                    try {
                        assert TestHookExecute.doHookIfSet(
                            delayCBVLSNUpdateHook, repNode.getNameIdPair());
                    } catch (IllegalStateException e) {
                        continue;
                    }

                    /*
                     * Take this opportunity to update this node's CBVLSN The
                     * replicas are sending in their CBVLSNs through the
                     * heartbeat responses, but a master does not send any
                     * heartbeat responses, and needs a different path to
                     * update its local CBVLSN.
                     */
                    updater.updateForMaster(tracker);

                    /* Flush the DTVLSN if it's warranted. */
                    dtvlsnFlusher.flush();

                    /*
                     * Opportunistically attempt to update minJEVersion.
                     * This must be done while the feeder is active.
                     */
                    repNode.globalCBVLSN.setDefunctJEVersion(repNode);

                    continue;
                }

                nFeedersCreated.increment();
                try {
                    Feeder feeder = new Feeder(this, feederReplicaChannel);
                    nascentFeeders.add(feeder);
                    feeder.startFeederThreads();
                } catch (IOException e) {

                    /*
                     * Indicates a feeder socket level exception.
                     */
                    LoggerUtils.fine
                        (logger, repNode.getRepImpl(),
                         "Feeder I/O exception: " + e.getMessage());
                    try {
                        feederReplicaChannel.close();
                    } catch (IOException e1) {
                        LoggerUtils.fine
                            (logger, repNode.getRepImpl(),
                             "Exception during cleanup." + e.getMessage());
                    }
                    continue;
                }
            }
        } catch (MasterSyncException e) {
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Master change: " + e.getMessage());

            feederShutdownException = new UnknownMasterException("Node " +
                                repNode.getRepImpl().getName() +
                                " is not a master anymore");
        } catch (InterruptedException e) {
            if (this.repNodeShutdownException != null) {

                /*
                 * The interrupt was issued to propagate an exception from one
                 * of the Feeder threads. It's not a normal exit.
                 */
                LoggerUtils.warning(logger, repNode.getRepImpl(),
                                    "Feeder manager unexpected interrupt");
                throw repNodeShutdownException; /* Terminate the rep node */
            }
            if (repNode.isShutdown()) {
                LoggerUtils.info(logger, repNode.getRepImpl(),
                                 "Feeder manager interrupted for shutdown");
                return;
            }
            feederShutdownException = e;
            LoggerUtils.warning(logger, repNode.getRepImpl(),
                                "Feeder manager unexpected interrupt");
        } finally {
            repNode.resetReadyLatch(feederShutdownException);
            repNode.getServiceDispatcher().cancel(FEEDER_SERVICE);
            shutdownFeeders(feederShutdownException);
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Feeder manager exited. CurrentTxnEnd VLSN: " +
                             repNode.getCurrentTxnEndVLSN());
        }
    }

    /**
     * Shuts down all the feeders managed by the FeederManager
     *
     * @param feederShutdownException the exception provoking the shutdown.
     */
    private void shutdownFeeders(Exception feederShutdownException) {
        boolean changed = shutdown.compareAndSet(false, true);
        if (!changed) {
            return;
        }

        try {
            /* Copy sets for safe iteration in the presence of deletes.*/
            final Set<Feeder> feederSet;
            synchronized (nascentFeeders) {
                synchronized (activeFeeders) {
                    feederSet = new HashSet<Feeder>(activeFeeders.values());
                    feederSet.addAll(nascentFeeders);
                }
            }

            for (Feeder feeder : feederSet) {
                nFeedersShutdown.increment();
                feeder.shutdown(feederShutdownException);
            }
        } finally {
            if (feederShutdownException == null) {
                feederShutdownException =
                    new IllegalStateException("FeederManager shutdown");
                /*
                 * Release any threads that may have been waiting, but
                 * don't throw any exception
                 */
                activeFeeders.clear(null);
            } else {
                activeFeeders.clear(feederShutdownException);
            }
            nascentFeeders.clear();
        }
    }

    /**
     * Shuts down a specific feeder. It's typically done in response to the
     * removal of a member from the group.
     */
    public void shutdownFeeder(RepNodeImpl node) {
        Feeder feeder = activeFeeders.get(node.getName());
        if (feeder == null) {
            return;
        }
        nFeedersShutdown.increment();
        feeder.shutdown(null);
    }

    /**
     * Block until the required number of electable feeders/replica connections
     * are established. Used for establishing durability quorums. Since this is
     * counting feeder/replica connections, requiredReplicaCount does not
     * include the master.
     *
     * In the future this could be improved by also taking account the position
     * of each feeder. If a feeder is lagging far behind the master and this
     * is likely to prevent commit, we may want to reject the transaction at
     * the outset to reduce the number of wasted txns/aborts. A special case
     * is when the replica is in out-of-disk mode and not acking at all.
     */
    boolean awaitFeederReplicaConnections(
        int requiredReplicaCount, long insufficientReplicasTimeout)
        throws InterruptedException {
        return activeFeeders.sizeAwait(requiredReplicaCount,
                                       insufficientReplicasTimeout,
                                       TimeUnit.MILLISECONDS);
    }

    /*
     * For debugging help, and for expanded exception messages, dump feeder
     * related state.  If acksOnly is true, only include information about
     * feeders for replicas that supply acknowledgments.
     */
    public String dumpState(final boolean acksOnly) {
        StringBuilder sb = new StringBuilder();
        synchronized (activeFeeders) {
            Set<Map.Entry<String, Feeder>> feeds = activeFeeders.entrySet();
            if (feeds.size() == 0) {
                sb.append("No feeders.");
            } else {
                sb.append("Current feeds:");
                for (Map.Entry<String, Feeder> feedEntry : feeds) {
                    final Feeder feeder = feedEntry.getValue();

                    /*
                     * Ignore secondary and external nodes if only want nodes
                     * that provide acknowledgments
                     */
                    if (acksOnly) {
                        final NodeType nodeType =
                            feeder.getReplicaNode().getType();
                        if (nodeType.isSecondary() || nodeType.isExternal()) {
                            continue;
                        }
                    }
                    sb.append("\n ").append(feedEntry.getKey()).append(": ");
                    sb.append(feeder.dumpState());
                }
            }
        }
        return sb.toString();
    }

    /**
     * Returns a count of the number of feeders whose replicas are counted in
     * durability decisions and have acknowledged txn-end VLSNs >= the
     * commitVLSN argument.
     *
     * @param commitVLSN the commitVLSN being checked
     */
    public int getNumCurrentAckFeeders(VLSN commitVLSN) {
        final DurabilityQuorum durabilityQuorum =
            repNode.getDurabilityQuorum();
        int count = 0;
        synchronized (activeFeeders) {
            for (Feeder feeder : activeFeeders.values()) {
                if ((commitVLSN.compareTo(feeder.getReplicaTxnEndVLSN()) <= 0)
                    && durabilityQuorum.replicaAcksQualify(
                        feeder.getReplicaNode())) {
                    count++;
                }
            }
            return count;
        }
    }

    /**
     * Update the Master's DTVLSN if we can conclude based upon the state of
     * the replicas that the DTVLSN needs to be advanced.
     *
     * This method is invoked when a replica heartbeat reports a more recent
     * txn VLSN. This (sometimes) redundant form of DTVLS update is useful in
     * circumstances when the value could not be maintained via the usual ack
     * response processing:
     *
     * 1) The application is using no ack transactions explicitly.
     *
     * 2) There were ack transaction timeouts due to network problems and the
     * acks were never received or were received after the timeout had expired.
     */
    public void updateDTVLSN(long heartbeatVLSN) {

        final long currDTVLSN = repNode.getDTVLSN();
        if (heartbeatVLSN <= currDTVLSN) {
            /* Nothing to update, a lagging replica that's catching up */
            return;
        }

        final DurabilityQuorum durabilityQuorum = repNode.getDurabilityQuorum();
        final int durableAckCount = durabilityQuorum.
            getCurrentRequiredAckCount(ReplicaAckPolicy.SIMPLE_MAJORITY);

        long min = Long.MAX_VALUE;

        synchronized (activeFeeders) {

            int ackCount = 0;
            for (Feeder feeder : activeFeeders.values()) {

                if (!durabilityQuorum.
                    replicaAcksQualify(feeder.getReplicaNode())) {
                    continue;
                }

                final long replicaTxnVLSN =
                    feeder.getReplicaTxnEndVLSN().getSequence();

                if (replicaTxnVLSN <= currDTVLSN) {
                    continue;
                }

                if (replicaTxnVLSN < min) {
                    min = replicaTxnVLSN;
                }

                if (++ackCount >= durableAckCount) {
                    /*
                     * If a majority of replicas have vlsns >= durable txn
                     * vlsn, advance the DTVLSN.
                     */
                    repNode.updateDTVLSN(min);
                    return;
                }
            }

            /* DTVLSN unchanged. */
            return;
        }
    }

    /**
     * Set a test hook, parameterized by feeder's Name/ID pair, that delays
     * CBVLSN updates if it throws an IllegalStateException.
     */
    public static void setDelayCBVLSNUpdateHook(TestHook<NameIdPair> hook) {
        delayCBVLSNUpdateHook = hook;
    }

    /**
     * Convenience constant used by the DTVLSN flusher when committing the null
     * transaction.
     */
    private static TransactionConfig NULL_TXN_CONFIG = new TransactionConfig();
    static {
       NULL_TXN_CONFIG.setDurability(new Durability(SyncPolicy.WRITE_NO_SYNC,
                                                    SyncPolicy.WRITE_NO_SYNC,
                                                    ReplicaAckPolicy.NONE));
    }

    /**
     * Writes a null (no modifications) commit record when it detects that the
     * DTVLSN is ahead of the persistent DTVLSN and needs to be updated.
     *
     * Note that without this mechanism, the in-memory DTVLSN would always be
     * ahead of the persisted VLSN, since in general DTVLSN(vlsn) < vlsn. That
     * is, the commit or abort log record containing the DTVLSN always has a
     * more recent VLSN than the one it contains.
     */
    private class DTVLSNFlusher {

        /**
         * The number of feeder ticks for which the in-memory DTVLSN must be
         * stable before it's written to the log as a null TXN. We are using
         * this "tick" indirection to avoid yet another call to the clock. A
         * "tick" in this context is the FEEDER_MANAGER_POLL_TIMEOUT.
         */
        final int targetStableTicks;

        /**
         * The number of ticks for which the DTVLSN has been stable.
         */
        private int stableTicks = 0;

        public DTVLSNFlusher() {
            final int heartbeatMs = repNode.getConfigManager().
                getInt(RepParams.HEARTBEAT_INTERVAL);
            targetStableTicks =
                (int) Math.max(1, (2 * heartbeatMs) / pollTimeoutMs);
        }

        /**
         * Used to track whether the DTVLSN has been stable enough to write
         * out. While it's changing application commits and aborts are writing
         * it out, so no need to write it here.
         */
        private long stableDTVLSN = VLSN.NULL_VLSN_SEQUENCE;

        /**
         * Update each time we actually persist the DTVLSN via a null txn. It
         * represents the DTVLSN that's been written out.
         */
        private long persistedDTVLSN = VLSN.NULL_VLSN_SEQUENCE;

        /* Identifies the Txn that was used to persist the DTVLSN. */
        private long nullTxnVLSN = VLSN.NULL_VLSN_SEQUENCE;

        /**
         * Persists the DTVLSN if necessary. The DTVLSN is persisted if the
         * version in memory is more current than the version on disk and has
         * not changed for targetStableTicks.
         */
        void flush() {
            final long dtvlsn = repNode.getDTVLSN();

            if (dtvlsn == nullTxnVLSN) {
                /* Don't save VLSN from null transaction as DTVLSN */
                return;
            }

            if (dtvlsn > stableDTVLSN) {
                stableTicks = 0;
                stableDTVLSN = dtvlsn;

                /* The durable DTVLSN is being actively updated. */
                return;
            }

            if (dtvlsn < stableDTVLSN) {
                /* Enforce the invariant that the DTVLSN cannot decrease. */
                throw new IllegalStateException("The DTVLSN sequence cannot decrease" +
                                                "current DTVLSN:" + dtvlsn +
                                                " previous DTVLSN:" + stableDTVLSN);
            }

            /* DTVLSN == stableDTVLSN */
            if (++stableTicks <= targetStableTicks) {
                /*
                 * Increase the stable tick counter. it has not been stable
                 * long enough.
                 */
                return;
            }

            stableTicks = 0;

            /* dtvlsn has been stable */
            if (stableDTVLSN > persistedDTVLSN) {
                if (repNode.getActiveTransfer() != null) {
                    /*
                     * Don't attempt writing a transaction. while a transfer is
                     * in progress and txns will be blocked.
                     */
                    LoggerUtils.info(logger, repNode.getRepImpl(),
                                     "Skipped null txn updating DTVLSN: " +
                                     dtvlsn + " Master transfer in progress");
                    return;
                }
                final RepImpl repImpl = repNode.getRepImpl();
                final MasterTxn nullTxn =
                    MasterTxn.createNullTxn(repImpl, NULL_TXN_CONFIG,
                                            repImpl.getNameIdPair());
                /*
                 * We don't want to wait for any reason, if the txn fails,
                 * we can try later.
                 */
                nullTxn.setTxnTimeout(1);
                try {
                    nullTxn.commit();
                    LoggerUtils.fine(logger, repNode.getRepImpl(),
                                     "Persist DTVLSN: " + dtvlsn +
                                     " at VLSN: " + nullTxn.getCommitVLSN() +
                                     " via null transaction:" + nullTxn.getId());
                    nullTxnVLSN = nullTxn.getCommitVLSN().getSequence();
                    persistedDTVLSN = dtvlsn;
                    stableDTVLSN = persistedDTVLSN;
                } catch (Exception e) {
                    nullTxn.abort();
                    LoggerUtils.warning(logger, repNode.getRepImpl(),
                               "Failed to write null txn updating DTVLSN; " +
                               e.getMessage());
                }
            }
        }
    }

}
