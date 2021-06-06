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

package com.sleepycat.je.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Checks that the cleaner wakes up at certain times even when there is no
 * logging:
 *  - startup
 *  - change to minUtilization
 *  - DB remove/truncate
 * Also checks that checkpointing and file deletion occur when writing stops.
 */
public class WakeupTest extends TestBase {

    private static final int FILE_SIZE = 1000000;
    private static final String DB_NAME = "WakeupTest";

    private final File envHome;
    private Environment env;
    private Database db;
    
    public WakeupTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    private void open(final boolean runCleaner) {

        /*
         * Use a cleaner/checkpointer byte interval that is much larger than
         * the amount we will write.
         */
        final String veryLargeWriteSize = String.valueOf(Long.MAX_VALUE);

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX, Integer.toString(FILE_SIZE));
        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_MIN_UTILIZATION, "50");
        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "0");
        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_WAKEUP_INTERVAL, "1 s");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, runCleaner ? "true" : "false");
        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_BYTES_INTERVAL, veryLargeWriteSize);
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, veryLargeWriteSize);
        env = new Environment(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    private void close() {
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (env != null) {
            try {
                env.close();
            } finally {
                env = null;
            }
        }
    }

    @Test
    public void testCleanAtStartup() {

        open(false /*runCleaner*/);
        writeFiles(0 /*nActive*/, 10 /*nObsolete*/);
        close();

        open(true /*runCleaner*/);
        expectBackgroundCleaning();
        close();
    }

    @Test
    public void testCleanAfterMinUtilizationChange() {

        open(true /*runCleaner*/);
        writeFiles(4 /*nActive*/, 3 /*nObsolete*/);
        expectNothingToClean();

        final EnvironmentConfig envConfig = env.getConfig();
        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_MIN_UTILIZATION, "90");
        env.setMutableConfig(envConfig);

        expectBackgroundCleaning();
        close();
    }

    /**
     * Tests cleaner wakeup after writing stops.
     *
     * Only a small amount is logged by removeDatabase, which is not enough to
     * motivate cleaning. As of JE 7.1 the cleaner wakes up periodically and
     * cleans writing has stopped but there was at least some writing since the
     * last cleaner activation.
     */
    @Test
    public void testCleanAfterWritingStops() {

        open(true /*runCleaner*/);
        writeFiles(5 /*nActive*/, 0 /*nObsolete*/);
        expectNothingToClean();
        db.close();
        db = null;

        env.removeDatabase(null, DB_NAME);
        expectBackgroundCleaning();
        close();
    }

    /**
     * Tests cleaner wakeup and file deletion, which requires a checkpoint,
     * after writing stops.
     *
     * Only a small amount is logged by truncateDatabase, which is not enough
     * to motivate cleaning. Plus, . As of JE 7.1 the cleaner wakes up periodically and
     * cleans writing has stopped but there was at least some writing since the
     * last cleaner activation.
     */
    @Test
    public void testFileDeletionAfterWritingStops() {
        open(true /*runCleaner*/);
        writeFiles(5 /*nActive*/, 0 /*nObsolete*/);
        expectNothingToClean();
        db.close();
        db = null;

        /* Clear nCheckpoints stat. */
        env.getStats(StatsConfig.CLEAR);

        final EnvironmentConfig envConfig = env.getConfig();
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "true");
        env.setMutableConfig(envConfig);

        env.truncateDatabase(null, DB_NAME, false);
        expectBackgroundCleaning();
        expectBackgroundCheckpointAndFileDeletion();
        close();
    }

    private void expectNothingToClean() {
        env.cleanLog();
        final EnvironmentStats stats = env.getStats(null);
        final String msg = String.format("%d probes, %d non-probes",
            stats.getNCleanerProbeRuns(), stats.getNCleanerRuns());
        assertEquals(msg, 0,
            stats.getNCleanerRuns() - stats.getNCleanerProbeRuns());
    }

    private void expectBackgroundCleaning() {
        final long endTime = System.currentTimeMillis() + (30 * 1000);
        while (System.currentTimeMillis() < endTime) {
            final EnvironmentStats stats = env.getStats(null);
            if (stats.getNCleanerRuns() > 0) {
                return;
            }
        }
        close();
        fail("Cleaner did not run");
    }

    private void expectBackgroundCheckpointAndFileDeletion() {
        final long endTime = System.currentTimeMillis() + (30 * 1000);
        EnvironmentStats stats = null;
        while (System.currentTimeMillis() < endTime) {
            stats = env.getStats(null);
            if (stats.getNCheckpoints() > 0 &&
                stats.getNCleanerDeletions() > 0) {
                return;
            }
        }
        close();
        fail("Checkpointer did not run or no files were deleted: " + stats);
    }

    private void writeFiles(final int nActive, final int nObsolete) {
        int key = 0;
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry(new byte[FILE_SIZE]);
        for (int i = 0; i < nActive; i += 1) {
            IntegerBinding.intToEntry(key, keyEntry);
            db.put(null, keyEntry, dataEntry);
            key += 1;
        }
        IntegerBinding.intToEntry(key, keyEntry);
        for (int i = 0; i <= nObsolete; i += 1) {
            db.put(null, keyEntry, dataEntry);
        }
        env.checkpoint(new CheckpointConfig().setForce(true));
    }
}
