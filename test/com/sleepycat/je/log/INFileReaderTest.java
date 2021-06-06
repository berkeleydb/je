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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.cleaner.RecoveryUtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class INFileReaderTest extends TestBase {

    static private final boolean DEBUG = false;

    private final File envHome;
    private Environment env;
    
    /*
     * Need a handle onto the true environment in order to create
     * a reader
     */
    private EnvironmentImpl envImpl;
    private Database db;
    private long maxNodeId;
    private List<CheckInfo> checkList;

    public INFileReaderTest() {
        super();
        envHome = SharedTestUtils.getTestDir();
        Key.DUMP_TYPE = DumpType.BINARY;
    }

    @Before
    public void setUp()
        throws Exception {

        /*
         * Note that we use the official Environment class to make the
         * environment, so that everything is set up, but we then go a
         * backdoor route to get to the underlying EnvironmentImpl class
         * so that we don't require that the Environment.getDbEnvironment
         * method be unnecessarily public.
         */
        super.setUp();

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam
            (EnvironmentParams.BIN_DELTA_PERCENT.getName(), "75");
        envConfig.setAllowCreate(true);

        /* Disable noisy cleaner database usage. */
        DbInternal.setCreateEP(envConfig, false);
        DbInternal.setCreateUP(envConfig, false);
        DbInternal.setCheckpointUP(envConfig, false);
        /* Don't run the cleaner without a UtilizationProfile. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");

        env = new Environment(envHome, envConfig);

        envImpl =DbInternal.getNonNullEnvImpl(env);

    }

    @After
    public void tearDown()
        throws DatabaseException {

        envImpl = null;
        env.close();
    }

    /**
     * Test no log file
     */
    @Test
    public void testNoFile()
        throws DatabaseException {

        /* Make a log file with a valid header, but no data. */
        INFileReader reader = new INFileReader
            (envImpl, 1000, DbLsn.NULL_LSN, DbLsn.NULL_LSN, false,
             DbLsn.NULL_LSN, DbLsn.NULL_LSN, null);
        reader.addTargetType(LogEntryType.LOG_IN);
        reader.addTargetType(LogEntryType.LOG_BIN);
        reader.addTargetType(LogEntryType.LOG_IN_DELETE_INFO);

        int count = 0;
        while (reader.readNextEntry()) {
            count += 1;
        }
        assertEquals("Empty file should not have entries", 0, count);
    }

    /**
     * Run with an empty file
     */
    @Test
    public void testEmpty()
        throws IOException, DatabaseException {

        /* Make a log file with a valid header, but no data. */
        FileManager fileManager = envImpl.getFileManager();
        FileManagerTestUtils.bumpLsn(fileManager, 1000000);
        FileManagerTestUtils.createLogFile(fileManager, envImpl, 10000);
        fileManager.clear();

        INFileReader reader = new INFileReader
            (envImpl, 1000, DbLsn.NULL_LSN, DbLsn.NULL_LSN, false,
             DbLsn.NULL_LSN, DbLsn.NULL_LSN, null);
        reader.addTargetType(LogEntryType.LOG_IN);
        reader.addTargetType(LogEntryType.LOG_BIN);
        reader.addTargetType(LogEntryType.LOG_IN_DELETE_INFO);

        int count = 0;
        while (reader.readNextEntry()) {
            count += 1;
        }
        assertEquals("Empty file should not have entries", 0, count);
    }

    /**
     * Run with defaults, read whole log
     */
    @Test
    public void testBasic()
        throws IOException, DatabaseException {

        DbConfigManager cm = envImpl.getConfigManager();
        doTest(50,
               cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE),
               0,
               false);
    }

    /**
     * Run with very small buffers and track IDs
     */
    @Test
    public void testTracking()
        throws IOException, DatabaseException {

        doTest(50, // num iterations
               10, // tiny buffer
               0, // start lsn index
               true); // track ids
    }

    /**
     * Start in the middle of the file
     */
    @Test
    public void testMiddleStart()
        throws IOException, DatabaseException {

        doTest(50, 100, 40, true);
    }

    private void doTest(int numIters,
                        int bufferSize,
                        int startLsnIndex,
                        boolean trackIds)
        throws IOException, DatabaseException {

        /* Fill up a fake log file. */
        createLogFile(numIters);

        /* Decide where to start. */
        long startLsn = DbLsn.NULL_LSN;
        int checkIndex = 0;
        if (startLsnIndex >= 0) {
            startLsn = checkList.get(startLsnIndex).lsn;
            checkIndex = startLsnIndex;
        }

        /* Use an empty utilization map for testing tracking. */
        RecoveryUtilizationTracker tracker = trackIds ?
            (new RecoveryUtilizationTracker(envImpl)) : null;

        INFileReader reader =
            new INFileReader(envImpl, bufferSize, startLsn, DbLsn.NULL_LSN,
                             trackIds, DbLsn.NULL_LSN,
                             DbLsn.NULL_LSN, tracker);
        reader.addTargetType(LogEntryType.LOG_IN);
        reader.addTargetType(LogEntryType.LOG_BIN);
        reader.addTargetType(LogEntryType.LOG_BIN_DELTA);
        reader.addTargetType(LogEntryType.LOG_IN_DELETE_INFO);

        /* Read. */
        checkLogFile(reader, checkIndex, trackIds);
    }

    /**
     * Write a logfile of entries, then read the end
     */
    private void createLogFile(int numIters)
        throws IOException, DatabaseException {

        /*
         * Create a log file full of INs, BIN-deltas and Debug Records
         */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "foo", dbConfig);
        LogManager logManager = envImpl.getLogManager();
        maxNodeId = 0;

        checkList = new ArrayList<CheckInfo>();

        for (int i = 0; i < numIters; i++) {
            /* Add a debug record. */
            Trace rec = new Trace("Hello there, rec " + (i + 1));
            rec.trace(envImpl, rec);

            /* Create, log, and save an IN. */
            byte[] data = new byte[i + 1];
            Arrays.fill(data, (byte) (i + 1));

            byte[] key = new byte[i + 1];
            Arrays.fill(key, (byte) (i + 1));

            IN in = new IN(DbInternal.getDbImpl(db), key, 5, 10);
            in.latch(CacheMode.UNCHANGED);
            long lsn = in.log();
            checkList.add(new CheckInfo(lsn, in));

            if (DEBUG) {
                System.out.println("LSN " + i + " = " + lsn);
                System.out.println("IN " + i + " = " + in.getNodeId());
            }

            /* Add other types of INs. */
            BIN bin = new BIN(DbInternal.getDbImpl(db), key, 2, 1);
            bin.latch(CacheMode.UNCHANGED);
            lsn = bin.log();
            checkList.add(new CheckInfo(lsn, bin));

            /* Add provisional entries, which should get ignored. */
            bin.log(
                false /*allowDeltas*/, true /*isProvisional*/,
                false /*backgroundIO*/, in);

            bin.releaseLatch();

            /* Add a LN, to stress the node tracking. */
            LN ln = LN.makeLN(envImpl, data);
            ln.log(
                envImpl, DbInternal.getDbImpl(db),
                null /*locker*/, null /*writeLockInfo*/,
                false /*newEmbeddedLN*/, key,
                0 /*newExpiration*/, false /*newExpirationInHours*/,
                false /*currEmbeddedLN*/, DbLsn.NULL_LSN /*currLsn*/,
                0 /*currSize*/, true /*isInsertion*/,
                false, ReplicationContext.NO_REPLICATE);

            /*
             * Add an BIN-delta. Generate it by making the first, full version
             * provisional so the test doesn't pick it up, and then log a
             * delta.
             */
            BIN binDeltaBin =
                new BIN(DbInternal.getDbImpl(db), key, 10, 1);
            maxNodeId = binDeltaBin.getNodeId();
            binDeltaBin.latch();

            assertTrue(binDeltaBin.insertEntry(null, key, DbLsn.makeLsn(0, 0)));

            binDeltaBin.log(
                false /*allowDeltas*/, true /*isProvisional*/,
                false /*backgroundIO*/, in);

            /* Modify the bin with one entry so there can be a delta. */

            byte[] keyBuf2 = new byte[2];
            Arrays.fill(keyBuf2, (byte) (i + 2));

            assertTrue(binDeltaBin.insertEntry(
                null, keyBuf2, DbLsn.makeLsn(100, 101)));

            binDeltaBin.log(
                true /*allowDeltas*/, false /*isProvisional*/,
                false /*backgroundIO*/, in);
            lsn = binDeltaBin.getLastLoggedLsn();
            if (DEBUG) {
                System.out.println("delta =" + binDeltaBin.getNodeId() +
                                   " at LSN " + lsn);
            }
            checkList.add(new CheckInfo(lsn, binDeltaBin));

            binDeltaBin.releaseLatch();

            in.releaseLatch();
        }

        /* Flush the log, files. */
        logManager.flushSync();
        envImpl.getFileManager().clear();
    }

    private void checkLogFile(INFileReader reader,
                              int checkIndex,
                              boolean checkMaxNodeId)
        throws DatabaseException {

        try {
            /* Read all the INs. */
            int i = checkIndex;

            while (reader.readNextEntry()) {
                if (DEBUG) {
                    System.out.println("i = "
                                       + i
                                       + " reader.isDeleteInfo="
                                       + reader.isDeleteInfo()
                                       + " LSN = "
                                       + reader.getLastLsn());
                }

                CheckInfo check = checkList.get(i);

                /*
                 * When comparing the check data against the data from the
                 * log, make the dirty bits match so that they compare
                 * equal.
                 */
                IN inFromLog = reader.getIN(DbInternal.getDbImpl(db));

                inFromLog.setDatabase(DbInternal.getDbImpl(db));

                inFromLog.latch(CacheMode.UNCHANGED);

                if (inFromLog.isBINDelta()) {
                    inFromLog.mutateToFullBIN(false /*leaveFreeSlot*/);
                }

                inFromLog.setDirty(true);
                inFromLog.releaseLatch();

                IN testIN = check.in;
                testIN.latch(CacheMode.UNCHANGED);
                testIN.setDirty(true);
                testIN.releaseLatch();

                /*
                 * Only check the INs we created in the test. (The others
                 * are from the map db.
                 */
                if (reader.getDatabaseId().
                    equals(DbInternal.getDbImpl(db).getId())) {
                    // The IN should match
                    String inFromLogString = inFromLog.toString();
                    String testINString = testIN.toString();
                    if (DEBUG) {
                        System.out.println("testIN=" + testINString);
                        System.out.println("inFromLog=" + inFromLogString);
                    }

                    assertEquals("IN "
                                 + inFromLog.getNodeId()
                                 + " at index "
                                 + i
                                 + " should match.\nTestIN=" +
                                 testIN +
                                 "\nLogIN=" +
                                 inFromLog,
                                 testINString,
                                 inFromLogString);
                }

                /* The LSN should match. */
                assertEquals
                    ("LSN " + i + " should match",
                     check.lsn,
                     reader.getLastLsn());

                i++;
            }
            assertEquals(i, checkList.size());
            if (checkMaxNodeId) {
                assertEquals(maxNodeId, reader.getMaxNodeId());
            }
        } finally {
            db.close();
        }
    }

    private class CheckInfo {
        long lsn;
        IN in;

        CheckInfo(long lsn, IN in) {
            this.lsn = lsn;
            this.in = in;
        }
    }
}
