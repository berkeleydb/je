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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.StatCaptureRepDefinitions;
import com.sleepycat.je.statcap.EnvStatsLogger;
import com.sleepycat.je.statcap.StatCapture;
import com.sleepycat.je.statcap.StatCaptureDefinitions;
import com.sleepycat.je.statcap.StatFile;
import com.sleepycat.je.statcap.StatManager;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.LongMaxStat;
import com.sleepycat.je.utilint.LongMinStat;
import com.sleepycat.je.utilint.Stat;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

public class StatCaptureTest extends TestBase {

    private final File envHome;
    private static final String DB_NAME = "foo";

    public StatCaptureTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Custom Statistics.
     */
    @Test
    public void testCustomStats() throws Exception {

        long start;
        final int DATACOUNT = 1025;
        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        Custom customStats = new Custom();
        envConfig.setCustomStats(customStats);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(
             EnvironmentParams.STATS_COLLECT_INTERVAL.getName(), "10 s");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        Environment env = null;
        Database db = null;
        try {
            env = new Environment(envHome, envConfig);

            env.close();
            env = null;

            /* Try to open and close again, now that the environment exists */
            envConfig.setAllowCreate(false);
            env = new Environment(envHome, envConfig);

            /* Open a database and insert some data. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = env.openDatabase(null, DB_NAME, dbConfig);
            for (int i = 0; i < DATACOUNT; i++) {
                byte[] val = Integer.valueOf(i).toString().getBytes();
                start = System.currentTimeMillis();
                db.put(null,
                       new DatabaseEntry(val),
                       new DatabaseEntry(val));
                customStats.setPutLatency(System.currentTimeMillis() - start);
            }
        } finally {
            if (db != null) {
                db.close();
            }
            if (env != null) {
                env.close();
            }
        }

        File statcsv =
            new File(envHome.getAbsolutePath() + File.separator +
                    "je.stat.csv");
        Map<String, Long> values = StatFile.sumItUp(statcsv);
        Long putCount = values.get("Op:priInsert");
        Long customPutLatency = values.get("Custom:putLatency");
        assertEquals(putCount.longValue(), DATACOUNT);
        assertTrue(customPutLatency > 0);
    }

    /**
     * Basic Statistics Capture.
     */
    @Test
    public void testStatsCapture() throws Exception {

        final int DATACOUNT = 9999;

        long envCreationTime = 0;
        SortedMap<String, Long> statmap;
        File envStatFile = new File(envHome.getAbsolutePath() +
            File.separator + EnvStatsLogger.STATFILENAME +
            "." + EnvStatsLogger.STATFILEEXT);

        /* remove any existing stats files. */
        File envHome = SharedTestUtils.getTestDir();
        FindFile ff = new FindFile(StatCapture.STATFILENAME);
        File[] files = envHome.listFiles(ff);
        for (File f : files) {
            f.delete();
        }

        FindFile envff = new FindFile(EnvStatsLogger.STATFILENAME);
        files = envHome.listFiles(envff);
        for (File f : files) {
            f.delete();
        }

        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        Environment env = new Environment(envHome, envConfig);
        env.close();
        env = null;
        assertEquals("Number of rows in env stat file not expected.",
                     getRowCount(envStatFile),
                     2);

        /* Try to open and close again, now that the environment exists */
        envConfig.setAllowCreate(false);
//        envConfig.setTxnNoSync(true);
        envConfig.setConfigParam(EnvironmentParams.
                                 MAX_MEMORY.getName(),
                                 "100000");
        env = new Environment(envHome, envConfig);
        assertEquals("Number of rows in env stat file not expected.",
                     getRowCount(envStatFile),
                     3);

        /* Open a database and insert some data. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, DB_NAME, dbConfig);
        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < DATACOUNT; i++) {
            byte[] key = Integer.valueOf(i).toString().getBytes();
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus os =
                db.get(null,
                       new DatabaseEntry(key),
                       value,
                       LockMode.DEFAULT);
            assertSame(os, OperationStatus.SUCCESS);
        }

        DatabaseEntry value = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();

        Cursor corsair = db.openCursor(null, CursorConfig.DEFAULT);

        assertSame(corsair.getPrev(key, value, null),
                   OperationStatus.SUCCESS);
        assertSame(corsair.getFirst(key, value, null),
                   OperationStatus.SUCCESS);
        corsair.close();
        corsair = null;

        EnvironmentStats es = env.getStats(TestUtils.FAST_STATS);
        statmap = StatFile.getMap(es.getStatGroups());
        envCreationTime = es.getEnvironmentCreationTime();

        db.close();
        env.close();

        File statcsv =
            new File(envHome.getAbsolutePath() + File.separator +
                     "je.stat.csv");
        Map<String, Long> values = StatFile.sumItUp(statcsv);

        Long putCount = values.get("Op:priInsert");
        Long getCount = values.get("Op:priSearch");
        Long posCount = values.get("Op:priPosition");
        assertEquals(DATACOUNT, putCount.longValue());
        assertEquals(DATACOUNT, getCount.longValue());
        assertEquals(2, posCount.longValue());
        assertEquals(Long.valueOf(envCreationTime),
                     values.get("Environment:environmentCreationTime"));

        verify(values, "Stat File", statmap, "public getStats API");
    }

    /**
     * Statistics Capture configuration test
     */
    @Test
    public void testChangeStatConfig() throws Exception {

        final int DATACOUNT = 30;
        int filecount = 0;
        int prevrowcount = 0;
        int rowcount = 0;
        File currentStatFile;
        File currentConfigStatFile;
        EnvironmentMutableConfig mc;

        /* remove any existing stats files. */
        File envHome = SharedTestUtils.getTestDir();
        FindFile ff = new FindFile(StatCapture.STATFILENAME);
        File[] files = envHome.listFiles(ff);
        for (File f : files) {
            f.delete();
        }
        currentStatFile =
            new File(envHome.getAbsolutePath() + File.separator +
                    StatCapture.STATFILENAME + "." +
                    StatCapture.STATFILEEXT);
        currentConfigStatFile =
                new File(envHome.getAbsolutePath() + File.separator +
                        EnvStatsLogger.STATFILENAME + "." +
                        EnvStatsLogger.STATFILEEXT);

        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(
            EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getName(),
            "1000000000");
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        Environment env = null;
        Database db = null;
        try {
            env = new Environment(envHome, envConfig);
            env.close();
            env = null;

            filecount = envHome.listFiles(ff).length;
            assertSame("Number of stat files was expected to be zero.",
                       filecount,
                       0);

            /* Try to open and close again, now that the environment exists */
            envConfig.setAllowCreate(false);

            envConfig.setConfigParam(
                EnvironmentParams.STATS_COLLECT_INTERVAL.getName(),
                "1 s");
            env = new Environment(envHome, envConfig);

            /* change config param to collect stats */
            mc = env.getMutableConfig();
            mc.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                              "true");
            env.setMutableConfig(mc);

            /* Open a database and insert some data. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = env.openDatabase(null, DB_NAME, dbConfig);
            /* populate */
            for (int i = 0; i < DATACOUNT; i++) {
                byte[] val = Integer.valueOf(i).toString().getBytes();
                db.put(null,
                       new DatabaseEntry(val),
                       new DatabaseEntry(val));
                /* wait a second */
                Thread.sleep(1000);
                if (i == 0) {
                    /* change configuration */
                    Thread.sleep(1000);
                    mc = env.getMutableConfig();
                    mc.setConfigParam(
                        EnvironmentParams.STATS_COLLECT_INTERVAL.getName(),
                        "10 s");
                    env.setMutableConfig(mc);
                    Thread.sleep(5000);
                    /* check there was at least one file */
                    filecount = envHome.listFiles(ff).length;
                    assertSame("No stats files found. ", filecount, 1);
                    rowcount = getRowCount(currentStatFile);
                }
            }
            prevrowcount = rowcount;
            rowcount = getRowCount(currentStatFile);
            assertSame(
                "Expected number of rows in stat file is " +
                "incorrect expected " +
                (DATACOUNT / 10) + " have " + (rowcount - prevrowcount),
                (rowcount - prevrowcount),
                (DATACOUNT / 10));

            /* turn stat capture off, then on to insure that works */
            mc = env.getMutableConfig();
            mc.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                              "false");
            env.setMutableConfig(mc);

            filecount = envHome.listFiles(ff).length;

            // turn back capture back on
            mc = env.getMutableConfig();
            mc.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                              "true");
            env.setMutableConfig(mc);

            mc = env.getMutableConfig();
            mc.setConfigParam(
                EnvironmentParams.STATS_COLLECT_INTERVAL.getName(),
                "1 s");
            mc.setConfigParam(
                EnvironmentParams.STATS_FILE_ROW_COUNT.getName(),
                Integer.toString(DATACOUNT / 4));
            env.setMutableConfig(mc);

            for (int i = 0; i < DATACOUNT; i++) {
                byte[] key = Integer.valueOf(i).toString().getBytes();
                DatabaseEntry value = new DatabaseEntry();
                OperationStatus os =
                    db.get(null,
                           new DatabaseEntry(key),
                           value,
                           LockMode.DEFAULT);
                assertSame(os, OperationStatus.SUCCESS);
                Thread.sleep(1000);
            }

            assertTrue("Number of stat files did not increase " +
                       filecount + " have " + envHome.listFiles(ff).length,
                       filecount < envHome.listFiles(ff).length );

            // check the environment config log
            rowcount = getRowCount(currentConfigStatFile);
            assertEquals("Number of rows in "+
                       currentConfigStatFile.getAbsolutePath() +
                       "not expected.",
                       6, rowcount);
            db.close();
            db = null;
            env.close();
            env = null;

            /* Test changing a non mutable parameter is logged */
            envConfig.setConfigParam(
                EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getName(),
                "4000000");
            envConfig.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                "true");
            env = new Environment(envHome, envConfig);

            // check the environment config log
            rowcount = getRowCount(currentConfigStatFile);
            assertEquals("Number of rows in "+
                         currentConfigStatFile.getAbsolutePath() +
                         "not expected.",
                         7, rowcount);
        } finally {
            if (db != null) {
                db.close();
            }
            if (env != null) {
                env.close();
            }
        }

        Map<String, Long> values =
            StatFile.sumItUp(envHome, StatCapture.STATFILENAME);

        Long putCount = values.get("Op:priInsert");
        Long getCount = values.get("Op:priSearch");
        assertEquals(DATACOUNT, putCount.longValue());
        assertEquals(DATACOUNT, getCount.longValue());
    }


    /**
     * Statistics Capture test that moves the statistics
     * file while stat capture is ongoing.
     */
    @Test
    public void testFileErrors() throws Exception {

        final int DATACOUNT = 10;
        File jeLogFile;
        EnvironmentMutableConfig mc;

        /* remove any existing stats files. */
        File envHome = SharedTestUtils.getTestDir();
        FindFile ff = new FindFile(StatCapture.STATFILENAME);
        File[] files = envHome.listFiles(ff);
        for (File f : files) {
            f.delete();
        }
        File statFileDir =
            new File(envHome.getAbsolutePath() + File.separator +
                "statsdir");
        jeLogFile =
            new File(envHome.getAbsolutePath() + File.separator +
                    "je.info.0");

        jeLogFile.delete();
        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                "true");
        envConfig.setConfigParam(
            EnvironmentParams.STATS_COLLECT_INTERVAL.getName(),
            "1 s");
        envConfig.setConfigParam(EnvironmentParams.STATS_FILE_DIRECTORY.getName(),
            statFileDir.getAbsolutePath());
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        Environment env = null;
        Database db = null;
        try {
            env = new Environment(envHome, envConfig);

            /* Open a database and insert some data. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = env.openDatabase(null, DB_NAME, dbConfig);
            /* populate */
            for (int i = 0; i < DATACOUNT; i++) {
                byte[] val = Integer.valueOf(i).toString().getBytes();
                db.put(null,
                       new DatabaseEntry(val),
                       new DatabaseEntry(val));
                /* wait a second */
                Thread.sleep(1000);
                statFileDir.renameTo(
                    new File(statFileDir.getAbsolutePath() + "1"));
            }
        } finally {
            if (db != null) {
                db.close();
            }
            if (env != null) {
                env.close();
            }
        }
        assertTrue("Number of rows in env stat file not expected.",
                getRowCount(jeLogFile) >= 1);
    }


    /**
     * Test to check if added statistics are projected to the stat file.
     * A manual change to the StatCaptureDefinitions class is required if
     * a new statistic is added and is to be projected.
     */
    @Test
    public void testForMissingStats() throws Exception {;

        StatCaptureDefinitions sd = new StatCaptureDefinitions();
        Map<String, Stat<?>> stats;
        final int DATACOUNT = 10;

        final StatsConfig fastconfig = new StatsConfig();
        fastconfig.setFast(true);
        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        Environment env = null;
        Database db = null;
        try {
            env = new Environment(envHome, envConfig);
            env.close();
            env = null;

            /* Try to open and close again, now that the environment exists */
            envConfig.setAllowCreate(false);
            env = new Environment(envHome, envConfig);

            /* Open a database and insert some data. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = env.openDatabase(null, DB_NAME, dbConfig);
            for (int i = 0; i < DATACOUNT; i++) {
                byte[] val = Integer.valueOf(i).toString().getBytes();
                db.put(null,
                       new DatabaseEntry(val),
                       new DatabaseEntry(val));
            }
            stats =
                StatFile.getNameValueMap(
                    env.getStats(fastconfig).getStatGroups());
            for (Map.Entry<String, Stat<?>> entry : stats.entrySet()) {
                String name = entry.getKey();
                assertTrue("Statistic " + name +
                           " returned but not captured.",
                           sd.getDefinition(name) != null);

                if (entry.getValue() instanceof LongMinStat) {
                    assertTrue("Statistic " + name +
                               " returned but not defined as a minStat.",
                                findDef(name, StatCaptureDefinitions.minStats));
                } else if (entry.getValue() instanceof LongMaxStat) {
                    assertTrue("Statistic " + name +
                               " returned but not defined as a maxStat.",
                                findDef(name, StatCaptureDefinitions.maxStats));
                }
            }
        } finally {
            if (db != null) {
                db.close();
            }
            if (env != null) {
                env.close();
            }
        }
    }

    @Test
    public void testRepEnvStats() throws Exception {

        final int DATACOUNT = 17;
        RepEnvInfo[] repEnvInfo = null;
        File [] repEnvHome = null;
        FindFile ff = new FindFile(StatCapture.STATFILENAME);
        FindFile envff = new FindFile(EnvStatsLogger.STATFILENAME);
        Database db = null;
        try {
            EnvironmentConfig envConfig = RepTestUtils.createEnvConfig(
                new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                 Durability.SyncPolicy.WRITE_NO_SYNC,
                 Durability.ReplicaAckPolicy.ALL));
            envConfig.setConfigParam(
                EnvironmentParams.STATS_COLLECT_INTERVAL.getName(), "1 s");
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
            repEnvInfo = RepTestUtils.setupEnvInfos(
                envHome, 2, envConfig, new ReplicationConfig());
            repEnvHome = new File[repEnvInfo.length];
            for (int i = 0; i < repEnvInfo.length; i++) {
                repEnvHome[i] = repEnvInfo[i].getEnvHome();
            }

            /* remove any existing stats files. */
            for (RepEnvInfo ri : repEnvInfo) {
                File[] files = ri.getEnvHome().listFiles(ff);
                for (File f : files) {
                    f.delete();
                }
                files = ri.getEnvHome().listFiles(envff);
                for (File f : files) {
                    f.delete();
                }
            }
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = master.openDatabase(null, DB_NAME, dbConfig);
            ReplicatedEnvironment replica = repEnvInfo[1].getEnv();
            final StatsConfig fastconfig = new StatsConfig();
            fastconfig.setFast(true);
            fastconfig.setClear(true);

            /*
             * sleep to allow at least one row to be captured in stat file
             * before doing work.
             */
            Thread.sleep(1000);
            ReplicatedEnvironmentStats rs = replica.getRepStats(fastconfig);
            for (int i = 0; i < DATACOUNT; i++) {
                byte[] val = Integer.valueOf(i).toString().getBytes();
                db.put(null,
                       new DatabaseEntry(val),
                       new DatabaseEntry(val));
            }
            Thread.sleep(250);
            rs = replica.getRepStats(null);
            assertTrue("value not expected ",
                       rs.getReplayMaxCommitProcessingNanos() > 0);
            final long replayTotalCommitLagMs = rs.getReplayTotalCommitLagMs();
            assertTrue("replayTotalCommitLagMs should be greater than zero," +
                       " was " + replayTotalCommitLagMs,
                       replayTotalCommitLagMs > 0);
        } finally {
            if (db != null) {
                db.close();
            }
            Thread.sleep(2000);
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
        /* Check master statistics */
        File statcsv =
            new File(repEnvHome[0].getAbsolutePath() + File.separator +
                    "je.stat.csv");
        Map<String, Long> values = StatFile.sumItUp(statcsv);
        Long putCount = values.get("Op:priInsert");
        assertEquals(putCount.longValue(), DATACOUNT);
        Long writtenMessages = values.get("BinaryProtocol:nMessagesWritten");
        assertTrue("BinaryProtocol:nMessagesWritten value not " +
                   "greater than zero.",
                    writtenMessages > 0);

        /* Check slave statistics. */
        statcsv =
            new File(repEnvHome[1].getAbsolutePath() + File.separator +
                    "je.stat.csv");
        values = StatFile.sumItUp(statcsv);
        Long readMessages = values.get("BinaryProtocol:nMessagesRead");
        assertTrue("BinaryProtocol:nMessagesRead value not greater than zero.",
                    readMessages > 0);
    }

    @Test
    public void testNoJoinRepEnvStats() throws Exception {

        RepEnvInfo[] repEnvInfo = null;
        try {
            File currentFile =
                new File(envHome.getAbsolutePath() +
                          File.separator + EnvStatsLogger.STATFILENAME +
                          "." + EnvStatsLogger.STATFILEEXT);
            currentFile.delete();

            repEnvInfo = RepTestUtils.setupEnvInfos(envHome, 2);
            ReplicatedEnvironment master =
                new ReplicatedEnvironment(envHome,
                                          repEnvInfo[0].getRepConfig(),
                                          repEnvInfo[0].getEnvConfig());
            master.close();
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    @Test
    public void testMissingRepEnvStats() throws Exception {

        final int DATACOUNT = 17;
        Map<String, Stat<?>> stats;
        RepEnvInfo[] repEnvInfo = null;
        Database db = null;
        File [] repEnvHome = null;
        StatCaptureRepDefinitions rsd = new StatCaptureRepDefinitions();
        final StatsConfig fastconfig = new StatsConfig();
        fastconfig.setFast(true);
        FindFile ff = new FindFile(StatCapture.STATFILENAME);
        FindFile envff = new FindFile(EnvStatsLogger.STATFILENAME);
        try {
            EnvironmentConfig envConfig = RepTestUtils.createEnvConfig(
                new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                 Durability.SyncPolicy.WRITE_NO_SYNC,
                 Durability.ReplicaAckPolicy.ALL));
            envConfig.setConfigParam(
                EnvironmentParams.STATS_COLLECT_INTERVAL.getName(), "1 s");
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
            repEnvInfo = RepTestUtils.setupEnvInfos(
                envHome, 2, envConfig, new ReplicationConfig());
            repEnvHome = new File[repEnvInfo.length];
            for (int i = 0; i < repEnvInfo.length; i++) {
                repEnvHome[i] = repEnvInfo[i].getEnvHome();
            }

            /* remove any existing stats files. */
            for (RepEnvInfo ri : repEnvInfo) {
                File[] files = ri.getEnvHome().listFiles(ff);
                for (File f : files) {
                    f.delete();
                }
                files = ri.getEnvHome().listFiles(envff);
                for (File f : files) {
                    f.delete();
                }
            }
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = master.openDatabase(null, DB_NAME, dbConfig);
            for (int i = 0; i < DATACOUNT; i++) {
                byte[] val = Integer.valueOf(i).toString().getBytes();
                db.put(null,
                       new DatabaseEntry(val),
                       new DatabaseEntry(val));
            }
            stats =
                StatFile.getNameValueMap(
                   master.getRepStats(fastconfig).getStatGroups());

            for (Map.Entry<String, Stat<?>> entry : stats.entrySet()) {
                String name = entry.getKey();

                assertTrue("Statistic " + name +
                           " returned but not captured from master.",
                           rsd.getDefinition(name) != null);

                if (entry.getValue() instanceof LongMinStat) {
                    assertTrue("Statistic " + name +
                               "returned but not defined as a minStat.",
                                findDef(name,
                                        StatCaptureRepDefinitions.minStats));
                } else if (entry.getValue() instanceof LongMaxStat) {
                    assertTrue("Statistic " + name +
                               "returned but not defined as a maxStat.",
                                findDef(name,
                                        StatCaptureRepDefinitions.maxStats));
                }
            }

            ReplicatedEnvironment replica = repEnvInfo[1].getEnv();
            stats =
                StatFile.getNameValueMap(
                    replica.getRepStats(fastconfig).getStatGroups());


            for (Map.Entry<String, Stat<?>> entry : stats.entrySet()) {
                String name = entry.getKey();

                assertTrue("Statistic " + name +
                           " returned but not captured from replica.",
                           rsd.getDefinition(name) != null);

                if (entry.getValue() instanceof LongMinStat) {
                    assertTrue("Statistic " + name +
                               "returned but not defined as a minStat.",
                                findDef(name,
                                        StatCaptureRepDefinitions.minStats));
                } else if (entry.getValue() instanceof LongMaxStat) {
                    assertTrue("Statistic " + name +
                               "returned but not defined as a maxStat.",
                                findDef(name,
                                        StatCaptureRepDefinitions.maxStats));
                }
            }

            db.close();
        } finally {
            if (db != null) {
                db.close();
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    private void verify(Map<String, Long>m1,
                        String m1desc,
                        Map<String, Long> m2,
                        String m2desc) {

        final int ERROR_PERCENT = 5;
        String[] statsToVerify = {
            "Cache:adminBytes",
            "Cache:dataBytes",
            "Cache:nBINsFetch",
            "Cache:nBINsFetchMiss",
            "Cache:nCachedBINs",
            "Cache:nINCompactKey",
            "Cache:nINNoTarget",
            "Cache:nINSparseTarget",
            "Cache:nLNsFetch",
            "Cache:nLNsFetchMiss",
            "Cache:nUpperINsFetch",
            "Environment:btreeRelatchesRequired",
            "Environment:environmentCreationTime",
            "I/O:bufferBytes",
            "I/O:nCacheMiss",
            "I/O:nFSyncRequests",
            "I/O:nFSyncs",
            "I/O:nLogBuffers",
            "I/O:nLogFSyncs",
            "I/O:nNotResident",
            "I/O:nRandomReadBytes",
            "I/O:nRandomReads",
            "I/O:nSequentialReadBytes",
            "I/O:nSequentialReads",
            "I/O:nSequentialWriteBytes",
            "I/O:nSequentialWrites",
            "Op:priSearch",
            "Op:priInsert",
        };

        for (String statname : statsToVerify) {
            long v1 = m1.get(statname).longValue();
            long v2 = m2.get(statname).longValue();
            long av1 = Math.abs(v1);
            long av2 = Math.abs(v2);
            assertTrue(statname + " " +
                    m1desc + " [" + v1 + "] not equal to " +
                    m2desc + " [" + v2 + "]",
                    (av1 - (av1 * ERROR_PERCENT * .01) <= av2) &&
                    (av2 <= (av1 + (av1 * ERROR_PERCENT * .01))));
        }
    }

    private boolean findDef(String name, StatManager.SDef[] definitions) {
        String[] namePart = name.split(":");
        for (StatManager.SDef def : definitions) {
            if (namePart[0].equals(def.getGroupName()) &&
                namePart[1].equals(def.getDefinition().getName())) {
                return true;
            }
        }
        return false;
    }

    private class Custom implements CustomStats {

        private final AtomicLong putLatency = new AtomicLong();
        private final AtomicLong putCount = new AtomicLong();

        public Custom() {
            putLatency.set(0);
            putCount.set(0);
        }

        public void setPutLatency(long val) {
            if (val == 0){
                val = 1;
            }
            putCount.incrementAndGet();
            putLatency.addAndGet(val);
        }

        @Override
        public String[] getFieldNames() {
            return new String[]{"putLatency"};
        }

        @Override
        public String[] getFieldValues() {
            String[] retval = new String[1];
            if (putCount.get() != 0) {
                long val = putLatency.get() / putCount.get();
                retval[0] =
                        Long.valueOf(val).toString();
            } else {
                retval[0] = "0";
            }
            return retval;
        }
    }

    private int getRowCount(File file) throws Exception {

        BufferedReader fr = null;
        int currentRowCount = 0;
        try {
            fr = new BufferedReader(new FileReader(file));
            while (fr.readLine() != null) {
                currentRowCount++;
            }
        }
        finally {
            if (fr != null) {
                fr.close();
            }
        }
        return currentRowCount;
    }

    class FindFile implements FileFilter {

        String fileprefix;
        FindFile(String fileprefix) {
            this.fileprefix = fileprefix;
        }

        @Override
        public boolean accept(File f) {
            return f.getName().startsWith(fileprefix);
        }
    }
}
