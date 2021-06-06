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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.REPLICA;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.UNKNOWN;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.PreloadStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Test using the {@link Environment#preload} method with a replicated
 * environment.  Because preload acquires an exclusive latch on the btree for
 * every database, it prevents the replay operation on the replica from making
 * progress, causing the heartbeat to be delayed and, depending on the length
 * of the delay, resulting in the replica being dropped from the replication
 * group.
 */
public class RepPreloadTest extends RepTestBase {

    /**
     * The number of entries in the database.  Use a larger value to produce a
     * preload time longer than the feeder timeout, to test that the preload
     * disrupts the feeder in that case.  The value 400000 works for this
     * purpose on some platforms.
     */
    private static final int NUM_ENTRIES =
        Integer.getInteger("test.num_entries", 100000);

    /**
     * The feeder timeout.  Use a shorter value to permit the preload time to
     * be longer than the feeder timeout.  2000 is the minimum, and is a good
     * choice for this purpose.
     */
    private static final long FEEDER_TIMEOUT_MS =
        Long.getLong("test.feeder_timeout_ms", 10000);

    /**
     * The preload timeout.  Increase this time to allow the preload time to
     * exceed the feeder timeout.
     */
    private static final long PRELOAD_TIMEOUT_MS =
        Long.getLong("test.preload_timeout_ms", 5000);

    /** The timeout for the replica to sync up. */
    private static final long REPLICA_SYNCUP_MS =
        Long.getLong("test.replica_syncup_ms", 120000);

    /** The timeout to wait for events. */
    private static final long EVENT_TIMEOUT_MS =
        Long.getLong("test.event_timeout_ms", 10000);

    /** The timeout to wait for replication to begin. */
    private static final long REPLICATION_START_TIMEOUT_MS =
        Long.getLong("test.replication_start_timeout_ms", 60000);

    private static final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(RepPreloadTest.class, "Test");

    /**
     * Test that preloading on a replica causes the feeder to timeout.
     */
    @Test
    public void testPreloadEnvironmentReplica() throws Exception {

        logger.info("Parameters:" +
                    "\n  test.num_entries=" + NUM_ENTRIES +
                    "\n  test.feeder_timeout_ms=" + FEEDER_TIMEOUT_MS +
                    "\n  test.preload_timeout_ms=" + PRELOAD_TIMEOUT_MS +
                    "\n  test.replica_syncup_ms=" + REPLICA_SYNCUP_MS +
                    "\n  test.event_timeout_ms=" + EVENT_TIMEOUT_MS +
                    "\n  test.replication_start_timeout_ms=" +
                    REPLICATION_START_TIMEOUT_MS);

        logger.info("Setting feeder timeout");

        for (final RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(
                ReplicationConfig.FEEDER_TIMEOUT,
                FEEDER_TIMEOUT_MS + " ms");
        }

        createGroup(3);

        final RepEnvInfo master = repEnvInfo[0];
        final RepEnvInfo replica = repEnvInfo[2];

        logger.info("Populating database");
        CommitToken populateToken = null;

        for (int i = 0; i < NUM_ENTRIES; i += 1000) {
            populateToken = populateDB(master.getEnv(), TEST_DB_NAME, i, 1000);
        }

        logger.info("Wait for replica to sync up");

        Transaction txn = replica.getEnv().beginTransaction(
            null,
            new TransactionConfig().setConsistencyPolicy(
                new CommitPointConsistencyPolicy(
                    populateToken, REPLICA_SYNCUP_MS, MILLISECONDS)));

        txn.commit();

        /* Close the replica so that we can preload a fresh environment */
        repEnvInfo[2].closeEnv();
        logger.info("Closed replica");

        /*
         * Update existing entries in the database.  This creates a replication
         * stream, but also means that the preload will have plenty of data to
         * load from the existing entries.
         */
        final AtomicBoolean done = new AtomicBoolean(false);

        final Thread updateThread = new Thread() {
            @Override
            public void run() {
                final Database db = master.getEnv().openDatabase(
                    null, TEST_DB_NAME, dbconfig);
                try {
                    int i = 0;
                    logger.info("Starting updates");
                    while (!done.get()) {
                        final Transaction txn2 =
                            master.getEnv().beginTransaction(
                                null, RepTestUtils.SYNC_SYNC_NONE_TC);
                        IntegerBinding.intToEntry(i % NUM_ENTRIES, key);
                        LongBinding.longToEntry(i, data);
                        db.put(txn2, key, data);
                        txn2.commit();
                    }
                } finally {
                    db.close();
                }
            }
        };

        updateThread.start();

        /*
         * Don't wait for consistency, to make sure the preload has work to do.
         */
        final ReplicatedEnvironment replicaEnv =
            replica.openEnv(NoConsistencyRequiredPolicy.NO_CONSISTENCY);
        logger.info("Opened replica");

        /* Track node state changes */
        final TestStateChangeListener[] listeners =
            new TestStateChangeListener[3];

        for (int i = 0; i < 3; i++) {
            final RepEnvInfo info = repEnvInfo[i];
            final TestStateChangeListener listener =
                new TestStateChangeListener(info.getEnv().getNodeName());
            listeners[i] = listener;
            info.getEnv().setStateChangeListener(listener);
        }

        /* Clear initial events */
        listeners[0].awaitState(MASTER, EVENT_TIMEOUT_MS, MILLISECONDS);
        listeners[1].awaitState(REPLICA, EVENT_TIMEOUT_MS, MILLISECONDS);
        listeners[2].awaitState(REPLICA, EVENT_TIMEOUT_MS, MILLISECONDS);

        /* Track replication */
        final CountDownLatch started = new CountDownLatch(1);

        final StatsConfig statsConfig =
            new StatsConfig().setFast(true).setClear(true);

        final Thread statsThread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        final ReplicatedEnvironmentStats stats =
                            replicaEnv.getRepStats(statsConfig);
                        if (stats == null) {
                            break;
                        }
                        final long nReplayLNs = stats.getNReplayLNs();
                        if (nReplayLNs > 0 && started.getCount() > 0) {
                            started.countDown();
                        }
                        logger.fine("nReplayLNs: " + stats.getNReplayLNs());
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    } catch (IllegalStateException e) {
                        break;
                    }
                }
            }
        };
        statsThread.setDaemon(true);
        statsThread.start();

        Database db = null;
        try {
            logger.info("Opening replica DB");
            txn = replicaEnv.beginTransaction(
                null,
                /*
                 * Don't wait for consistency, to make sure the preload has
                 * work to do
                 */
                new TransactionConfig()
                .setConsistencyPolicy(
                    NoConsistencyRequiredPolicy.NO_CONSISTENCY));
            db = replicaEnv.openDatabase(txn, TEST_DB_NAME, dbconfig);
            txn.commit();

            started.await(REPLICATION_START_TIMEOUT_MS, MILLISECONDS);
            logger.info("Replication started");

            logger.info("Starting preload");
            final PreloadConfig preloadConfig =
                new PreloadConfig()
                /* Loading the LNs takes more time -- good! */
                .setLoadLNs(true)
                /* Specify timeout */
                .setMaxMillisecs(PRELOAD_TIMEOUT_MS);
            final long startPreload = System.currentTimeMillis();
            final PreloadStats preloadStats =
                replicaEnv.preload(new Database[] { db }, preloadConfig);
            final long preloadTime = System.currentTimeMillis() - startPreload;

            assertEquals(PreloadStatus.SUCCESS, preloadStats.getStatus());

            EnvironmentImpl envImpl =
                DbInternal.getNonNullEnvImpl(master.getEnv());
            boolean embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

            if (embeddedLNs) {
                assertTrue("nEmbeddedLNs > " + NUM_ENTRIES/2,
                           preloadStats.getNEmbeddedLNs() > NUM_ENTRIES/2);
            } else {
                assertTrue("nLNsLoaded > " + NUM_ENTRIES/2,
                           preloadStats.getNLNsLoaded() > NUM_ENTRIES/2);
            }

            logger.info("Finished preload in " + preloadTime + " ms: " +
                        preloadStats);

            /*
             * If the preload took longer than the feeder timeout, then the
             * preload should cause the feeder to timeout and disconnect the
             * replica.  Otherwise, there should be no disruption to the
             * feeder.
             */
            if (preloadTime > FEEDER_TIMEOUT_MS) {
                listeners[2].awaitState(
                    UNKNOWN, EVENT_TIMEOUT_MS, MILLISECONDS);
                logger.info("Received expected event with state " + UNKNOWN);
            } else {
                listeners[2].awaitNoEvent(EVENT_TIMEOUT_MS, MILLISECONDS);
                logger.info("Received no unexpected events");
            }

        } finally {
            done.set(true);
            updateThread.join();
            if (db != null) {
                db.close();
            }
        }
    }

    /** A listener that queues and prints events. */
    private static class TestStateChangeListener
            implements StateChangeListener {

        private final String node;

        final BlockingQueue<StateChangeEvent> events =
            new LinkedBlockingQueue<StateChangeEvent>();

        TestStateChangeListener(final String node) {
            this.node = node;
        }

        @Override
        public void stateChange(final StateChangeEvent stateChangeEvent) {
            events.add(stateChangeEvent);
            String master;
            try {
                master = stateChangeEvent.getMasterNodeName();
            } catch (IllegalStateException e) {
                master = "(none)";
            }
            logger.info("State change event: " +
                        "node:" + node +
                        ", master:" + master +
                        ", state:" + stateChangeEvent.getState());
        }

        /** Wait for an event with the specified state. */
        void awaitState(final ReplicatedEnvironment.State state,
                        final long time,
                        final TimeUnit timeUnit)
            throws InterruptedException {

            final StateChangeEvent event = events.poll(time, timeUnit);
            assertNotNull("Expected state " + state + ", but got no event",
                          event);
            assertEquals(state, event.getState());
        }

        /** Wait to confirm that no event is delivered. */
        void awaitNoEvent(final long time, final TimeUnit timeUnit)
            throws InterruptedException {

            final StateChangeEvent event = events.poll(time, timeUnit);
            assertNull("Expected no event", event);
        }
    }
}
