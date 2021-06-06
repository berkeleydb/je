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

import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;
import static com.sleepycat.util.test.GreaterThan.greaterThan;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.TestHookAdapter;

/** Test statistics that are used to measure replication rates. */
public class ReplicationRateStatsTest extends RepTestBase {

    private UpdateThread updateThread;
    private RepEnvInfo masterInfo;

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();

        /* Add a secondary node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);

        /*
         * Use a shorter interval for collecting statistics, to better test any
         * anomalies that might introduce and to make it easier to audit the
         * output
         */
        for (RepEnvInfo i : repEnvInfo) {
            i.getEnvConfig().setConfigParam(
                EnvironmentConfig.STATS_COLLECT_INTERVAL, "5 s");
        }
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        Replica.setInitialReplayHook(null);
        if (updateThread != null) {
            updateThread.shutdown();
        }
        super.tearDown();
    }

    /** The list of all test stages. */
    final List<Stage> stages = new ArrayList<>();

    /* Define test stages */

    /**
     * Reach a steady master VLSN rate of 100 VLSNs/second.  Note that even
     * though we are generating VLSNs at this rate in the application, the rate
     * will sometimes be higher because of meta data VLSNs.
     */
    final Stage STARTUP = new Stage("STARTUP") {
        @Override
        void init() {
            createGroup();
            assertEquals(State.MASTER, repEnvInfo[0].getEnv().getState());
            masterInfo = findMaster(repEnvInfo);
            assertSame(repEnvInfo[0], masterInfo);
            updateThread = new UpdateThread(masterInfo.getEnv());
            updateThread.start();
        }

        /** Wait for node 2's VLSN rate to reach 100 VLSNs/second. */
        @Override
        boolean nextIsLast(int i) {
            ReplicatedEnvironmentStats stats =
                masterInfo.getEnv().getRepStats(StatsConfig.DEFAULT);
            Long node2VLSNRate = stats.getReplicaVLSNRateMap().get("Node2");
            return (node2VLSNRate != null) &&
                (node2VLSNRate > 80*60) &&
                (node2VLSNRate < 120*60);
        }
    };

    /** Run at the 100 VLSNs/second rate. */
    final Stage ALL = new Stage("ALL") {
        @Override
        boolean nextIsLast(int i) { return i == 4; }
    };

    /** Close Node 2. */
    final Stage NODE2_CLOSED = new Stage("NODE2_CLOSED") {
        @Override
        void init() { repEnvInfo[1].closeEnv(); }
        @Override
        boolean nextIsLast(int i) { return i == 4; }
    };

    /** Open Node 2, inserting a delay, and let it catch up. */
    final Stage NODE2_CATCHUP = new Stage("NODE2_CATCHUP") {

        /** Reopen node 2 with a delay */
        @Override
        void init() {
            Replica.setInitialReplayHook(new DelayHook(100, 100));
            repEnvInfo[1].openEnv(NO_CONSISTENCY);
        }

        /** Wait for node 2's VLSN rate to reach 100 VLSNs/second */
        @Override
        boolean nextIsLast(int i) {
            ReplicatedEnvironmentStats stats =
                masterInfo.getEnv().getRepStats(StatsConfig.DEFAULT);
            Long node2VLSNRate = stats.getReplicaVLSNRateMap().get("Node2");
            return (node2VLSNRate != null) &&
                (node2VLSNRate > 80*60) &&
                (node2VLSNRate < 120*60);
        }
    };

    /** Increase Node 2's delay. */
    final Stage NODE2_SLOWDOWN = new Stage("NODE2_SLOWDOWN") {
        @Override
        void init() {
            Replica.setInitialReplayHook(new DelayHook(1000, 100));
        }
    };

    /** Reduce Node 2's delay and let it catch up. */
    final Stage NODE2_CATCHUP2 = new Stage("NODE2_CATCHUP2") {
        @Override
        void init() {
            Replica.setInitialReplayHook(new DelayHook(100, 100));
        }

        /** Wait for Node 2's VLSN rate to reach 100 VLSNs/second. */
        @Override
        boolean nextIsLast(int i) {
            ReplicatedEnvironmentStats stats =
                masterInfo.getEnv().getRepStats(StatsConfig.DEFAULT);
            Long node2VLSNRate = stats.getReplicaVLSNRateMap().get("Node2");
            return (node2VLSNRate != null) &&
                (node2VLSNRate > 80*60) &&
                (node2VLSNRate < 120*60);
        }
    };

    /**
     * Stop updates, and clear stats after shutting down the update thread, to
     * make sure that the VLSN rate will be zero when measured next.
     */
    final Stage NO_UPDATES = new Stage("NO_UPDATES") {
        @Override
        void init() throws InterruptedException {
            updateThread.shutdown();
            masterInfo.getEnv().getRepStats(StatsConfig.CLEAR);
        }
    };

    /** A test stage */
    class Stage {
        private final String name;
        Stage(String name) {
            this.name = name;
            stages.add(this);
        }

        /** Run the test */
        void run() throws InterruptedException {
            logger.fine(this + ": Start");
            init();
            boolean nextIsLast = false;
            int i;

            /* Give up after 60 iterations or seconds */
            for (i = 1; i <= 60; i++) {
                boolean last = nextIsLast;
                Thread.sleep(1000);
                if (runIter(i, last)) {
                    nextIsLast = true;
                }
                if (last) {
                    logger.info(this + ": Complete after " + i +
                                " iterations");
                    return;
                }
            }
            fail(this + " was not complete after " + i + " iterations");
        }

        /** Before running the test */
        void init() throws InterruptedException { }

        /**
         * Run an iteration of the test and return if the next iteration should
         * be the last one.
         */
        boolean runIter(int i, boolean last) {
            for (RepEnvInfo info : repEnvInfo) {
                ReplicatedEnvironment env = info.getEnv();
                if (env == null) {
                    continue;
                }
                ReplicatedEnvironmentStats stats = env.getRepStats(
                    last ? StatsConfig.CLEAR : StatsConfig.DEFAULT);
                ReplicationRateStatsTest.this.checkStats(
                    this, env, stats, i);
            }
            return last || nextIsLast(i);
        }

        /** Should the next iteration be the last one? */
        boolean nextIsLast(int i) {
            return i == 9;
        }

        @Override
        public String toString() {
            return "Stage " + name;
        }
    }

    /** Run the various test stages and check statistics. */
    @Test
    public void testStats()
        throws Exception {

        for (Stage stage : stages) {
            stage.run();
        }
    }

    private static class DelayHook extends TestHookAdapter<Message> {
        private final long delay;
        private final int every;
        private int count;
        DelayHook(long delay, int every) {
            this.delay = delay;
            this.every = every;
        }
        @Override
        public void doHook(Message m) {
            if (count > 0) {
                count--;
                return;
            }
            count = every;
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    /** Check statistics for the specified stage. */
    private void checkStats(Stage stage,
                            ReplicatedEnvironment env,
                            ReplicatedEnvironmentStats stats,
                            int i) {

        long lastCommitVLSN = stats.getLastCommitVLSN();
        long lastCommitTimestamp = stats.getLastCommitTimestamp();
        long vlsnRate = stats.getVLSNRate();
        Map<String, Long> replicaDelayMap = stats.getReplicaDelayMap();
        Map<String, Long> replicaLastCommitTimestampMap =
            stats.getReplicaLastCommitTimestampMap();
        Map<String, Long> replicaLastCommitVLSNMap =
            stats.getReplicaLastCommitVLSNMap();
        Map<String, Long> replicaVLSNLagMap = stats.getReplicaVLSNLagMap();
        Map<String, Long> replicaVLSNRateMap =
            stats.getReplicaVLSNRateMap();

        String iter = stage + "(iter=" + i + ")";

        /* None of these stats should have contents for a non-master */
        if (!env.getState().isMaster()) {
            assertEquals("lastCommitVLSN", 0, lastCommitVLSN, 0);
            assertEquals("lastCommitTimestamp", 0, lastCommitTimestamp, 0);
            assertEquals("vlsnRate", 0, vlsnRate, 0);
            assertIsEmpty("replicaDelayMap", replicaDelayMap);
            assertIsEmpty("replicaLastCommitTimestampMap",
                          replicaLastCommitTimestampMap);
            assertIsEmpty("replicaLastCommitVLSNMap",
                          replicaLastCommitVLSNMap);
            assertIsEmpty("replicaVLSNLagMap", replicaVLSNLagMap);
            assertIsEmpty("replicaVLSNRateMap", replicaVLSNRateMap);
            return;
        }

        logger.fine(
            String.format(
                "Replication stats for master: %s\n" +
                "  lastCommitVLSN: %d\n" +
                "  lastCommitTimestamp: %d\n" +
                "  vlsnRate: %d\n" +
                "  replicaDelayMap: %s\n" +
                "  replicaLastCommitTimestampMap: %s\n" +
                "  replicaLastCommitVLSNMap: %s\n" +
                "  replicaVLSNLagMap: %s\n" +
                "  replicaVLSNRateMap: %s",
                iter,
                lastCommitVLSN,
                lastCommitTimestamp,
                vlsnRate,
                formatLongStats(replicaDelayMap),
                formatLongStats(replicaLastCommitTimestampMap),
                formatLongStats(replicaLastCommitVLSNMap),
                formatLongStats(replicaVLSNLagMap),
                formatLongStats(replicaVLSNRateMap)));

        assertThat(iter + ": lastCommitVLSN", lastCommitVLSN, greaterThan(0));

        assertThat(iter + ": lastCommitTimestamp", lastCommitTimestamp,
                   greaterThan(0));

        if (stage == NO_UPDATES) {
            checkEquals(iter + ": vlsnRate", 0, vlsnRate, 0);
        } else if (stage != STARTUP) {
            checkEquals(iter + ": vlsnRate", 100*60, vlsnRate, 20*60);
        }

        if (stage == NODE2_CLOSED) {

            /* No entries for Node 2 when it is closed */
            assertFalse(iter + ": replicaDelayMap contains entry for" +
                        " closed Node2",
                        replicaDelayMap.containsKey("Node2"));
            assertFalse(iter + ": replicaLastCommitTimestampMap contains" +
                        " entry for closed Node2",
                        replicaLastCommitTimestampMap.containsKey("Node2"));
            assertFalse(iter + ": replicaLastCommitVLSNMap contains entry" +
                        " for closed Node2",
                        replicaLastCommitVLSNMap.containsKey("Node2"));
            assertFalse(iter + ": replicaVLSNLagMap contains entry for" +
                        " closed Node2",
                        replicaVLSNLagMap.containsKey("Node2"));
            assertFalse(iter + ": replicaVLSNRateMap contains entry for" +
                        " closed Node2",
                        replicaVLSNRateMap.containsKey("Node2"));
        }

        assertThatAllValues(iter + ": replicaLastCommitVLSNMap",
                            replicaLastCommitVLSNMap,
                            greaterThan(0));

        /*
         * Note that the master stats are collected after the replica ones
         * so the order of collection should not result in the master VLSNs
         * or timestamps being earlier than the replica ones
         */
        assertThatAllValues(iter + ": replicaLastCommitVLSNMap",
                            replicaLastCommitVLSNMap,
                            not(greaterThan(lastCommitVLSN)));
        assertThatAllValues(iter + ": replicaLastCommitTimestampMap",
                            replicaLastCommitTimestampMap, greaterThan(0));
        assertThatAllValues(iter + ": replicaLastCommitTimestampMap",
                            replicaLastCommitTimestampMap,
                            not(greaterThan(lastCommitTimestamp)));

        if (stage == NO_UPDATES) {
            for (Entry<String, Long> e : replicaVLSNRateMap.entrySet()) {
                checkEquals(iter + ": replicaVLSNRateMap " + e.getKey(),
                            0, e.getValue(), 20*60);
            }
        } else if ((stage != NODE2_CATCHUP) &&
                   (stage != NODE2_CATCHUP2)) {
            for (Entry<String, Long> e : replicaVLSNRateMap.entrySet()) {
                checkEquals(iter + ": replicaVLSNRateMap " + e.getKey(),
                            100*60, e.getValue(), 20*60);
            }
        }
    }

    private static void assertIsEmpty(String msg, Map map) {
        if (!map.isEmpty()) {
            fail(msg + " not empty: " + map);
        }
    }

    private static <T> void assertThatAllValues(
        String mapName, Map<String, T> map, Matcher<T> matcher) {
        for (Entry<String, T> entry : map.entrySet()) {
            assertThat(mapName + ", key " + entry.getKey(),
                       entry.getValue(), matcher);
        }
    }

    /**
     * Like the assertion check, but only logs if the value was not expected.
     * Use this for checks for typical values where the check is not dependable
     * enough to be used as a success criterion.
     */
    private void checkEquals(String msg,
                             double expected,
                             double actual,
                             double delta) {
        if ((actual < (expected - delta)) || (actual > (expected + delta))) {
            logger.warning(msg + " expected:<" + expected + ">, was:<" +
                           actual + ">");
        }
    }

    private static String formatLongStats(Map<String, Long> map) {
        if (map.isEmpty()) {
            return "empty";
        }
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final Entry<String, Long> entry : map.entrySet()) {
            if (!first) {
                sb.append(", ");
            } else {
                first = false;
            }
            sb.append(entry);
        }
        return sb.toString();
    }

    /** Create 100 VLSNs per second */
    private class UpdateThread extends Thread {
        private final long period = 100;
        private final Environment env;
        private volatile boolean shutdown;
        volatile Throwable exception;

        UpdateThread(Environment env) {
            this.env = env;
        }

        void shutdown()
            throws InterruptedException {

            shutdown = true;
            join(1000);
            if (exception != null) {
                throw new RuntimeException("Unexpected exception: " + exception,
                                           exception);
            }
            assertFalse("isAlive", isAlive());
        }

        public void run() {
            final Database db;
            Transaction txn1 = env.beginTransaction(null, null);
            try {
                db = env.openDatabase(txn1, TEST_DB_NAME, dbconfig);
                txn1.commit();
                txn1 = null;
            } finally {
                if (txn1 != null) {
                    txn1.abort();
                }
            }
            try {
                TransactionConfig tc = new TransactionConfig().setDurability(
                    Durability.COMMIT_NO_SYNC);
                long next = System.currentTimeMillis() + period;
                while (!shutdown) {
                    int k = 0;
                    for (int i = 0; i < 5; i++) {
                        Transaction txn = env.beginTransaction(null, tc);
                        try {
                            for (int j = 0; j < 1; j++) {
                                IntegerBinding.intToEntry(k, key);
                                LongBinding.longToEntry(k, data);
                                db.put(txn, key, data);
                                k++;
                            }
                            txn.commit();
                            txn = null;
                        } finally {
                            if (txn != null) {
                                txn.abort();
                            }
                        }
                    }
                    final long wait = next - System.currentTimeMillis();
                    next += period;
                    if (wait > 0) {
                        Thread.sleep(wait);
                    }
                }
            } catch (Throwable t) {
                exception = t;
            } finally {
                db.close();
            }
        }
    }
}
