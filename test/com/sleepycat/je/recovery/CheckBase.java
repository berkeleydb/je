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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.cleaner.VerifyUtils;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.recovery.stepwise.EntryTrackerReader;
import com.sleepycat.je.recovery.stepwise.LogEntryInfo;
import com.sleepycat.je.recovery.stepwise.TestData;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class CheckBase extends TestBase {

    private static final boolean DEBUG = false;
    private HashSet<TestData> expected;
    private Set<TestData> found;

    File envHome;
    Environment env;

    private List<LogEntryInfo> logDescription;
    private long stepwiseStartLsn;

    private boolean checkLsns = true;

    public CheckBase() {
        envHome =  SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        if (env != null) {
            try {
                env.close();
                env = null;
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Create an environment, generate data, record the expected values.
     * Then close the environment and recover, and check that the expected
     * values are there.
     */
    protected void testOneCase(String dbName,
                               EnvironmentConfig startEnvConfig,
                               DatabaseConfig startDbConfig,
                               TestGenerator testGen,
                               EnvironmentConfig validateEnvConfig,
                               DatabaseConfig validateDbConfig)
        throws Throwable {

        try {
            /* Create an environment. */
            env = new Environment(envHome, startEnvConfig);
            Database db = env.openDatabase(null, dbName, startDbConfig);

            /* Generate test data. */
            testGen.generateData(db);

            /* Scan the database to save what values we should have. */
            loadExpectedData(db);

            /* Check for overlap between the tree and utilization profile. */
            if (checkLsns) {
                VerifyUtils.checkLsns(db);
            }

            /* Close w/out checkpointing. */
            db.close();
            DbInternal.getNonNullEnvImpl(env).close(false);
            env = null;

            if (testGen.generateLogDescription) {
                makeLogDescription();
            }

            tryRecovery(validateEnvConfig,
                        validateDbConfig,
                        dbName,
                        expected);
        } catch (Throwable t) {
            /* Dump stack trace before trying to tear down. */
            t.printStackTrace();
            throw t;
        }
    }

    /* Run recovery and validation twice. */
    private void tryRecovery(EnvironmentConfig validateEnvConfig,
                             DatabaseConfig validateDbConfig,
                             String dbName,
                             HashSet<TestData> useExpected)
        throws DatabaseException {
        /* Recover and load what's in the database post-recovery. */
        recoverAndLoadData(validateEnvConfig,
                           validateDbConfig,
                           dbName);

        /* Check the pre and post recovery data. */
        if (useExpected == null) {
            useExpected = expected;
        }
        validate(useExpected);

        /* Repeat the recovery and validation. */
        recoverAndLoadData(validateEnvConfig,
                           validateDbConfig,
                           dbName);

        validate(useExpected);
    }

    void setCheckLsns(boolean checkLsns) {
        this.checkLsns = checkLsns;
    }

    /**
     * Call this method to set the start of the stepwise loop. The stepwise
     * testing will begin at this point in the log.
     */
    void setStepwiseStart() {

        /*
         * Put a tracing message both for debugging assistance, and also
         * to force the truncation to start at this log entry, since we're
         * getting the last used LSN.
         */
        Trace.trace(DbInternal.getNonNullEnvImpl(env), "StepwiseStart");
        FileManager fileManager =
            DbInternal.getNonNullEnvImpl(env).getFileManager();
        stepwiseStartLsn = fileManager.getLastUsedLsn();
    }

    void stepwiseLoop(String dbName,
                      EnvironmentConfig validateEnvConfig,
                      DatabaseConfig validateDbConfig,
                      HashSet<TestData> useExpected,
                      int startingIteration)
        throws DatabaseException, IOException {

        assertTrue(logDescription.size() > 0);
        saveLogFiles(envHome);

        /* txnId -> LogEntryInfo */
        Map<Long, Set<TestData>> newUncommittedRecords = new HashMap<Long, Set<TestData>>();
        Map<Long, Set<TestData>> deletedUncommittedRecords = new HashMap<Long, Set<TestData>>();

        /* Now run recovery repeatedly, truncating at different locations. */
        String status = null;
        try {

            /*
             * Some tests are not working with starting at 0. As a workaround,
             * start at another iteration.
             */
            for (int i = startingIteration; i < logDescription.size(); i++ ) {

                /* Find out where to truncate. */
                LogEntryInfo info = logDescription.get(i);
                long lsn = info.getLsn();

                if (lsn == 0) {
                    continue;
                }

                status = "Iteration " + i + " out of " +
                    logDescription.size() + " truncate at 0x" +
                    DbLsn.getNoFormatString(lsn);

                if (DEBUG) {
                    System.out.println(status);
                }

                /* copy files back. */
                resetLogFiles(envHome);

                /* truncate */
                truncateAtOffset(envHome, lsn);

                /* recover */
                tryRecovery(validateEnvConfig, validateDbConfig,
                            dbName, useExpected);

                /* Adjust the set of expected records for the next iteration.*/
                info.updateExpectedSet(useExpected, newUncommittedRecords,
                                       deletedUncommittedRecords);
            }
        } catch (Error e) {
            System.err.println("Failure at step: " + status);
            throw e;
        }
    }

    protected void turnOffEnvDaemons(EnvironmentConfig envConfig) {
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_CLEANER.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_CHECKPOINTER.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_EVICTOR.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_INCOMPRESSOR.getName(),
                                 "false");
    }

    /**
     * Re-open the environment and load all data present, to compare to the
     * data set of expected values.
     */
    protected void recoverAndLoadData(EnvironmentConfig envConfig,
                                      DatabaseConfig dbConfig,
                                      String dbName)
        throws DatabaseException {

        env = new Environment(envHome, envConfig);
        Database db = env.openDatabase(null, dbName, dbConfig);

        /* Check for overlap between the tree and utilization profile. */
        if (checkLsns) {
            VerifyUtils.checkLsns(db);
        }

        found = new HashSet<TestData>();

        Cursor cursor = db.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        try {
            while (cursor.getNext(key, data, null) ==
                   OperationStatus.SUCCESS) {
                TestData t = new TestData(key, data);
                if (DEBUG) {
                    System.out.println("found k=" +
                                       IntegerBinding.entryToInt(key) +
                                       " d=" +
                                       IntegerBinding.entryToInt(data));
                }
                found.add(t);
            }
        }
        finally {
            cursor.close();
        }

        db.close();

        assertTrue(env.verify(new VerifyConfig(), System.out));
        env.close();
    }

    /*
     * The found and expected data sets need to match exactly after recovery.
     */
    @SuppressWarnings("unchecked") // clone() returns Object
    void validate(HashSet<TestData> expected) {
        Set<TestData> useExpected = (Set<TestData>) expected.clone();

        if (useExpected.size() != found.size()) {
            System.err.println("expected---------");
            dumpHashSet(useExpected);
            System.err.println("actual---------");
            dumpHashSet(found);
            assertEquals("expected and found set sizes don't match",
                useExpected.size(), found.size());
        }

        Iterator<TestData> foundIter = found.iterator();
        while (foundIter.hasNext()) {
            TestData t = foundIter.next();
            assertTrue("Missing " + t + "from the expected set",
                       useExpected.remove(t));
        }

        if (useExpected.size() != 0) {
            System.err.println("remaining---------");
            dumpHashSet(useExpected);
            assertEquals(
                "Expected has " + useExpected.size() + " items remaining",
                0, useExpected.size());
        }
    }

    protected void putTestData(Database db,
                             DatabaseEntry key,
                             DatabaseEntry data)
        throws DatabaseException {

        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
    }

    private void loadExpectedData(Database db)
        throws DatabaseException {

        expected = new HashSet<TestData>();

        Cursor cursor = db.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        try {
            while (cursor.getNext(key, data, null) ==
                   OperationStatus.SUCCESS) {
                if (DEBUG) {
                    System.out.println("expect k=" +
                                       IntegerBinding.entryToInt(key) +
                                       " d=" +
                                       IntegerBinding.entryToInt(data));
                }
                TestData t = new TestData(key, data);
                expected.add(t);
            }
        }
        finally {
            cursor.close();
        }
    }

    void dumpData(Database db)
        throws DatabaseException {

        Cursor cursor = db.openCursor(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        int i = 0;
        try {
            while (cursor.getNext(key, data, null) ==
                   OperationStatus.SUCCESS) {
                TestData t = new TestData(key, data);
                System.out.println(t);
                i++;
            }
        }
        finally {
            cursor.close();
        }
        System.out.println("scanned=" + i);
    }

    private void dumpHashSet(Set<TestData> expected) {
        Iterator<TestData> iter =
            new TreeSet<TestData>(expected).iterator();
        System.err.println("size=" + expected.size());
        while (iter.hasNext()) {
            System.err.println(iter.next());
        }
    }

    private void makeLogDescription()
        throws DatabaseException {

        EnvironmentImpl cmdEnvImpl =
            CmdUtil.makeUtilityEnvironment(envHome, false);
        logDescription = new ArrayList<LogEntryInfo>();

        try {
            EntryTrackerReader reader =
                new EntryTrackerReader(cmdEnvImpl,
                                       stepwiseStartLsn,
                                       logDescription);
            while (reader.readNextEntry()) {
            }
        } finally {
            cmdEnvImpl.close();
        }

        if (DEBUG) {
            Iterator<LogEntryInfo> iter = logDescription.iterator();
            while (iter.hasNext()) {
                Object o = iter.next();
                LogEntryInfo entryInfo =(LogEntryInfo) o;
                System.out.println(entryInfo);
            }
        }
    }

    /**
     * Truncate the log at the specified offset.
     */
    private void truncateAtOffset(File envHome, long lsn)
        throws DatabaseException, IOException {

        EnvironmentImpl cmdEnvImpl =
            CmdUtil.makeUtilityEnvironment(envHome, false);

        cmdEnvImpl.getFileManager().truncateLog(DbLsn.getFileNumber(lsn),
                                                DbLsn.getFileOffset(lsn));

        cmdEnvImpl.close();
    }

    /* Copy all .jdb files to .jdb_save for stepwise processing. */
    private void saveLogFiles(File envHome)
        throws IOException {

        String[] suffix = new String[] {".jdb"};
        String[] fileNames = FileManager.listFiles(envHome, suffix, false);

        for (int i = 0; i < fileNames.length; i++) {
            File src = new File(envHome, fileNames[i]);
            File dst = new File(envHome, fileNames[i]+ "_save");
            copy(src, dst);
        }
    }

    /* Copy all .jdb_save file back to ._jdb */
    private void resetLogFiles(File envHome)
        throws IOException {
        String[] suffix = new String[] {".jdb_save"};
        String[] fileNames = FileManager.listFiles(envHome, suffix, false);

        for (int i = 0; i < fileNames.length; i++) {
            String srcName = fileNames[i];
            int end = srcName.indexOf("_save");
            String dstName = srcName.substring(0, end);
            copy(new File(envHome, srcName), new File(envHome, dstName));
        }
    }

    private void copy(File src, File dst)
        throws IOException {

        InputStream in = new FileInputStream(src);
        OutputStream out = new FileOutputStream(dst);

        // Transfer bytes from in to out
        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
        out.close();
    }

    /*
     * Each unit test overrides the generateData method. Don't make this
     * abstract, because we may want different unit tests to call different
     * flavors of generateData(), and we want a default implementation for each
     * flavor.
     *
     * A unit test may also specify an implementation for truncateLog. When
     * that happens, the truncation is done before the first recovery.
     */
    protected class TestGenerator {

        /* If true, generate a log description to use in stepwise testing. */
        boolean generateLogDescription;

        public TestGenerator() {
        }

        public TestGenerator(boolean generateLogDescription) {
            this.generateLogDescription = generateLogDescription;
        }

        /**
         * @throws Exception in subclasses.
         */
        void generateData(Database db)
            throws Exception {
        }
    }
}
