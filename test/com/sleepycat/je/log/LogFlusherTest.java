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

package com.sleepycat.je.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.TimerTask;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Test that the LogFlusher works as we expect.
 */
public class LogFlusherTest extends TestBase {

    private static final String DB_NAME = "testDb";
    private static final String DATA_VALUE = "herococo";

    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;
    private Environment env;

    public LogFlusherTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Tests the basic configuration of LogFlusher, using deprecated HA params.
     *
     * This is an older test case, added prior to moving the LogFlusher into
     * standalone JE. It continues to use the deprecated params for additional
     * compatibility testing.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testHAConfigOld() throws IOException {

        /*
         * Open env with flushSync interval set to 30s using deprecated
         * LOG_FLUSH_TASK_INTERVAL. The flushNoSync interval will have its
         * default value.
         */
        EnvironmentConfig envConfig =
            RepTestUtils.createEnvConfig(Durability.COMMIT_NO_SYNC);

        ReplicationConfig repConfig = new ReplicationConfig();

        repConfig.setConfigParam(
            ReplicationMutableConfig.LOG_FLUSH_TASK_INTERVAL, "30 s");

        repEnvInfo =
            RepTestUtils.setupEnvInfos(envRoot, 3, envConfig, repConfig);

        RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(repEnvInfo[0].isMaster());

        TimerTask[] oldSyncTasks = new TimerTask[repEnvInfo.length];
        TimerTask[] oldNoSyncTasks = new TimerTask[repEnvInfo.length];

        for (int i = 0; i < repEnvInfo.length; i++) {

            LogFlusher flusher = repEnvInfo[i].getRepImpl().getLogFlusher();
            assertNotNull(flusher);

            assertNotNull(flusher.getFlushSyncTask());
            assertNotNull(flusher.getFlushNoSyncTask());
            assertEquals(30000, flusher.getFlushSyncInterval());
            assertEquals(5000, flusher.getFlushNoSyncInterval());

            oldSyncTasks[i] = flusher.getFlushSyncTask();
            oldNoSyncTasks[i] = flusher.getFlushNoSyncTask();
        }

        /*
         * Mutate flushSync interval to 50s using LOG_FLUSH_TASK_INTERVAL.
         */
        repConfig.setConfigParam(
            ReplicationMutableConfig.LOG_FLUSH_TASK_INTERVAL, "50 s");

        for (RepEnvInfo element : repEnvInfo) {
            element.getEnv().setRepMutableConfig(repConfig);
        }

        for (int i = 0; i < repEnvInfo.length; i++) {

            LogFlusher flusher = repEnvInfo[i].getRepImpl().getLogFlusher();
            assertNotNull(flusher);

            assertNotNull(flusher.getFlushSyncTask());
            assertNotNull(flusher.getFlushNoSyncTask());
            assertNotSame(flusher.getFlushSyncTask(), oldSyncTasks[i]);
            assertNotSame(flusher.getFlushNoSyncTask(), oldNoSyncTasks[i]);
            assertEquals(50000, flusher.getFlushSyncInterval());
            assertEquals(5000, flusher.getFlushNoSyncInterval());
        }

        /*
         * Disable both intervals using deprecated RUN_LOG_FLUSH_TASK.
         */
        repConfig.setConfigParam(
            ReplicationMutableConfig.RUN_LOG_FLUSH_TASK, "false");

        for (RepEnvInfo element : repEnvInfo) {
            element.getEnv().setRepMutableConfig(repConfig);
        }

        for (RepEnvInfo info : repEnvInfo) {

            LogFlusher flusher = info.getRepImpl().getLogFlusher();
            assertNotNull(flusher);

            assertNull(flusher.getFlushSyncTask());
            assertNull(flusher.getFlushNoSyncTask());
            assertEquals(0, flusher.getFlushSyncInterval());
            assertEquals(0, flusher.getFlushNoSyncInterval());
        }

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
        RepTestUtils.removeRepEnvironments(envRoot);

        /*
         * Open new env using deprecated RUN_LOG_FLUSH_TASK.
         */
        repConfig.setConfigParam(
            ReplicationConfig.RUN_LOG_FLUSH_TASK, "false");

        repEnvInfo =
            RepTestUtils.setupEnvInfos(envRoot, 3, envConfig, repConfig);

        RepTestUtils.joinGroup(repEnvInfo);

        for (RepEnvInfo info : repEnvInfo) {

            LogFlusher flusher = info.getRepImpl().getLogFlusher();
            assertNotNull(flusher);

            assertNull(flusher.getFlushSyncTask());
            assertNull(flusher.getFlushNoSyncTask());
            assertEquals(0, flusher.getFlushSyncInterval());
            assertEquals(0, flusher.getFlushNoSyncInterval());
        }

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    /**
     * Tests config via EnvironmentConfig, not using old HA params.
     */
    @Test
    public void testConfig() throws IOException {

        ReplicationConfig repConfig = new ReplicationConfig();

        checkConfig(null, null, repConfig, 20 * 1000, 5 * 1000);

        checkConfig("7 ms", null, repConfig, 7, 5 * 1000);

        checkConfig(null, "7 ms", repConfig, 20 * 1000, 7);

        checkConfig("3 ms", "4 ms", repConfig, 3, 4);
    }

    /**
     * Tests use of old HA config params, and combination with new params.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testConfigCompatibility() throws IOException {

        /*
         * HA flush interval param is used, but is illegal when the standalone
         * param is also specified.
         */
        ReplicationConfig repConfig =
            new ReplicationConfig().setConfigParam(
                ReplicationMutableConfig.LOG_FLUSH_TASK_INTERVAL, "7 s");

        EnvironmentConfig envConfig =
            RepTestUtils.createEnvConfig(Durability.COMMIT_NO_SYNC);

        checkHAConfig(envConfig, repConfig, 7 * 1000, 5 * 1000);

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FLUSH_SYNC_INTERVAL, "8 ms");

        try {
            checkHAConfig(envConfig, repConfig, 0, 0);
            fail();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        /*
         * HA flushing param may be set to false and will disable flushing, but
         * is illegal if a standalone param is also specified.
         */
        repConfig = new ReplicationConfig().setConfigParam(
            ReplicationMutableConfig.RUN_LOG_FLUSH_TASK, "false");

        envConfig = RepTestUtils.createEnvConfig(Durability.COMMIT_NO_SYNC);

        checkHAConfig(envConfig, repConfig, 0, 0);

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL, "8 ms");

        try {
            checkHAConfig(envConfig, repConfig, 0, 0);
            fail();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }

        envConfig = RepTestUtils.createEnvConfig(Durability.COMMIT_NO_SYNC);

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL, "8 ms");

        try {
            checkHAConfig(envConfig, repConfig, 0, 0);
            fail();
        } catch (IllegalArgumentException e) {
            /* Expected */
        }
    }

    private void checkConfig(
        String syncParam,
        String noSyncParam,
        ReplicationConfig repConfig,
        int flushSyncInterval,
        int flushNoSyncInterval)
        throws IOException {

        EnvironmentConfig envConfig =
            RepTestUtils.createEnvConfig(Durability.COMMIT_NO_SYNC);

        if (syncParam != null) {
            envConfig.setConfigParam(
                EnvironmentConfig.LOG_FLUSH_SYNC_INTERVAL, syncParam);
        }

        if (noSyncParam != null) {
            envConfig.setConfigParam(
                EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL, noSyncParam);
        }

        checkStandaloneConfig(
            envConfig, flushSyncInterval, flushNoSyncInterval);

        checkHAConfig(
            envConfig, repConfig, flushSyncInterval, flushNoSyncInterval);
    }

    private void checkStandaloneConfig(
        EnvironmentConfig envConfig,
        int flushSyncInterval,
        int flushNoSyncInterval)
        throws IOException {

        env = new Environment(envRoot, envConfig);

        expectFlushIntervals(env, flushSyncInterval, flushNoSyncInterval);

        env.close();
        env = null;
    }

    private void checkHAConfig(
        EnvironmentConfig envConfig,
        ReplicationConfig repConfig,
        int flushSyncInterval,
        int flushNoSyncInterval)
        throws IOException {

        SharedTestUtils.cleanUpTestDir(envRoot);
        RepTestUtils.removeRepEnvironments(envRoot);

        repEnvInfo =
            RepTestUtils.setupEnvInfos(envRoot, 3, envConfig, repConfig);

        RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(repEnvInfo[0].isMaster());

        for (RepEnvInfo info : repEnvInfo) {

            expectFlushIntervals(
                info.getEnv(), flushSyncInterval, flushNoSyncInterval);
        }

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    private void expectFlushIntervals(
        Environment env,
        int flushSyncInterval,
        int flushNoSyncInterval) {

        LogFlusher flusher =
            DbInternal.getEnvironmentImpl(env).getLogFlusher();

        assertNotNull(flusher);

        assertEquals(
            flushSyncInterval, flusher.getFlushSyncInterval());

        assertEquals(
            flushNoSyncInterval, flusher.getFlushNoSyncInterval());
    }

    @Test
    public void testFlushSync()
        throws IOException, InterruptedException {

        checkLogFlush(true /*fsync*/, true /*expectFlush*/);
    }

    @Test
    public void testNoFlushSync()
        throws IOException, InterruptedException {

        checkLogFlush(true /*fsync*/, false /*expectFlush*/);
    }

    @Test
    public void testFlushNoSync()
        throws IOException, InterruptedException {

        checkLogFlush(false /*fsync*/, true /*expectFlush*/);
    }

    @Test
    public void testNoFlushNoSync()
        throws IOException, InterruptedException {

        checkLogFlush(false /*fsync*/, false /*expectFlush*/);
    }

    /**
     * @param fsync is true to test the flushSync task, or false to test the
     * flushNoSync task.
     *
     * @param expectFlush If true, checks that the LogFlushTask does flush the
     * dirty data to the log, and it can be read after crash. If false, checks
     * that the LogFlushTask does not flush the updates before the crash; no
     * data may be written to the disk.
     */
    @SuppressWarnings("null")
    private void checkLogFlush(
        boolean fsync,
        boolean expectFlush)
        throws IOException, InterruptedException {

        /*
         * When we expect a flush, use a small value for the sync (or noSync)
         * interval, and disable the noSync (or sync) interval. Use a large
         * interval when we will not expect a flush.
         */
        createRepEnvInfo(expectFlush ? "5 s" : "20 s", fsync);

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        long startTime = System.currentTimeMillis();

        StatsConfig stConfig = new StatsConfig();
        stConfig.setClear(true);

        /* Flush the existed dirty data before we do writes. */
        for (RepEnvInfo element : repEnvInfo) {
            element.getEnv().sync();
            element.getEnv().getStats(stConfig);
        }

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        Database db = master.openDatabase(null, DB_NAME, dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= 100; i++) {
            IntegerBinding.intToEntry(i, key);
            StringBinding.stringToEntry(DATA_VALUE, data);
            db.put(null, key, data);
        }

        assertTrue(System.currentTimeMillis() - startTime < 5000);

        Thread.sleep(8000); // Add 3s to ensure timer fires

        long endTime = System.currentTimeMillis();

        for (RepEnvInfo element : repEnvInfo) {

            final long fsyncCount =
                element.getEnv().getStats(null).getNLogFSyncs();

            final LogFlusher flusher = element.getRepImpl().getLogFlusher();

            final LogFlusher.FlushTask task = fsync ?
                flusher.getFlushSyncTask() : flusher.getFlushNoSyncTask();

            final int flushCount = task.getFlushCount();
            final long execTime = task.scheduledExecutionTime();

            if (expectFlush) {
                assertTrue(flushCount > 0);
                assertTrue(execTime > startTime);
                assertTrue(execTime < endTime);
                if (fsync) {
                    assertTrue(fsyncCount > 0);
                } else {
                    assertEquals(0, fsyncCount);
                }
            } else {
                assertEquals(0, flushCount);
                assertTrue(execTime < startTime);
                assertEquals(0, fsyncCount);
            }
        }

        /* Close the replicas without doing a checkpoint. */
        File[] envHomes = new File[3];
        for (int i = 0; i < repEnvInfo.length; i++) {
            envHomes[i] = repEnvInfo[i].getEnvHome();
            repEnvInfo[i].getRepImpl().abnormalClose();
        }

        /*
         * Open a read only standalone Environment on each node to see whether
         * the data has been flushed to the disk.
         */
        EnvironmentConfig newConfig = new EnvironmentConfig();
        newConfig.setAllowCreate(false);
        newConfig.setReadOnly(true);
        newConfig.setTransactional(true);

        for (int i = 0; i < repEnvInfo.length; i++) {
            Environment env = new Environment(envHomes[i], newConfig);

            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);

            db = null;
            try {
                db = env.openDatabase(null, DB_NAME, dbConfig);
            } catch (DatabaseNotFoundException e) {

                /*
                 * If the system crashes before the flush, the database is
                 * not synced to the disk, so this database can't be found
                 * at all, it's expected.
                 */
                assertFalse(expectFlush);
            }

            if (expectFlush) {
                assertEquals(100l, db.count());
                for (int index = 1; index <= 100; index++) {
                    IntegerBinding.intToEntry(index, key);
                    OperationStatus status = db.get(null, key, data, null);
                    if (expectFlush) {
                        assertSame(OperationStatus.SUCCESS, status);
                        assertEquals(DATA_VALUE,
                                     StringBinding.entryToString(data));
                    }
                }
            }

            if (db != null) {
                db.close();
            }
            env.close();
        }
    }

    /**
     * Uses the given interval as the flush interval for the Sync (or NoSync if
     * fsync is false) timer, and disable the NoSync (or Sync) timer.
     */
    private void createRepEnvInfo(String interval, boolean fsync)
        throws IOException {

        /*
         * Set a large buffer size and disable checkpointing, so the data in
         * the buffer will only be flushed by the LogFlushTask.
         */
        EnvironmentConfig envConfig =
            RepTestUtils.createEnvConfig(Durability.COMMIT_NO_SYNC);

        envConfig.setConfigParam(
            EnvironmentConfig.MAX_MEMORY, "20000000");

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_TOTAL_BUFFER_BYTES, "120000000");

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_NUM_BUFFERS, "4");

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");

        /* Configure the log flush task. */

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FLUSH_SYNC_INTERVAL,
            fsync ? interval : "0");

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL,
            fsync ? "0" : interval);

        repEnvInfo =
            RepTestUtils.setupEnvInfos(envRoot, 3, envConfig, null);
    }
}
