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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.utilint.StringUtils;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CleanerTest extends CleanerTestBase {

    private static final int N_KEYS = 300;
    private static final int N_KEY_BYTES = 10;

    /*
     * Make the log file size small enough to allow cleaning, but large enough
     * not to generate a lot of fsyncing at the log file boundaries.
     */
    private static final int FILE_SIZE = 10000;

    protected Database db = null;

    private Database exampleDb;

    private boolean embeddedLNs = false;

    private static final CheckpointConfig FORCE_CONFIG = 
        new CheckpointConfig();
    static {
        FORCE_CONFIG.setForce(true);
    }

    private JUnitThread junitThread;
    private volatile int synchronizer;

    public CleanerTest(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }

    private void initEnv(boolean createDb, boolean allowDups)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 Integer.toString(FILE_SIZE));
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_CLEANER.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.CLEANER_REMOVE.getName(),
                                 "false");
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam(EnvironmentParams.BIN_DELTA_PERCENT.getName(),
                                 "75");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        String databaseName = "cleanerDb";
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(createDb);
        dbConfig.setSortedDuplicates(allowDups);
        exampleDb = env.openDatabase(null, databaseName, dbConfig);
    }

    @After
    public void tearDown() 
        throws Exception {

        if (junitThread != null) {
            junitThread.shutdown();
            junitThread = null;
        }

        super.tearDown();
        exampleDb = null;
    }

    private void closeEnv()
        throws DatabaseException {

        if (exampleDb != null) {
            exampleDb.close();
            exampleDb = null;
        }

        if (env != null) {
            env.close();
            env = null;
        }
    }

    @Test
    public void testCleanerNoDupes()
        throws Throwable {

        initEnv(true, false);
        try {
            doCleanerTest(N_KEYS, 1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testCleanerWithDupes()
        throws Throwable {

        initEnv(true, true);
        try {
            doCleanerTest(2, 500);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void doCleanerTest(int nKeys, int nDupsPerKey)
        throws DatabaseException {

        EnvironmentImpl environment =
            DbInternal.getNonNullEnvImpl(env);
        FileManager fileManager = environment.getFileManager();
        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, nKeys, nDupsPerKey, true);
        Long lastNum = fileManager.getLastFileNum();

        /* Read the data back. */
        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();

        Cursor cursor = exampleDb.openCursor(null, null);

        while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
               OperationStatus.SUCCESS) {
        }

        env.checkpoint(FORCE_CONFIG);

        for (int i = 0; i < (int) lastNum.longValue(); i++) {

            /*
             * Force clean one file.  Utilization-based cleaning won't
             * work here, since utilization is over 90%.
             */
            DbInternal.getNonNullEnvImpl(env).
                getCleaner().
                doClean(false, // cleanMultipleFiles
                        true); // forceCleaning
        }

        EnvironmentStats stats = env.getStats(TestUtils.FAST_STATS);
        assertTrue(stats.getNINsCleaned() > 0);

        cursor.close();
        closeEnv();

        initEnv(false, (nDupsPerKey > 1));

        checkData(expectedMap);
        assertTrue(fileManager.getLastFileNum().longValue() >
                   lastNum.longValue());

        closeEnv();
    }

    /**
     * Ensure that INs are cleaned.
     */
    @Test
    public void testCleanInternalNodes()
        throws DatabaseException {

        initEnv(true, true);
        int nKeys = 200;

        EnvironmentImpl environment =
            DbInternal.getNonNullEnvImpl(env);
        FileManager fileManager = environment.getFileManager();
        /* Insert a lot of keys. ExpectedMap holds the expected data */
        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, nKeys, 1, true);

        /* Modify every other piece of data. */
        modifyData(expectedMap, 10, true);
        checkData(expectedMap);

        /* Checkpoint */
        env.checkpoint(FORCE_CONFIG);
        checkData(expectedMap);

        /* Modify every other piece of data. */
        modifyData(expectedMap, 10, true);
        checkData(expectedMap);

        /* Checkpoint -- this should obsolete INs. */
        env.checkpoint(FORCE_CONFIG);
        checkData(expectedMap);

        /* Clean */
        Long lastNum = fileManager.getLastFileNum();
        env.cleanLog();

        /* Validate after cleaning. */
        checkData(expectedMap);
        EnvironmentStats stats = env.getStats(TestUtils.FAST_STATS);

        /* Make sure we really cleaned something.*/
        assertTrue(stats.getNINsCleaned() > 0);
        assertTrue(stats.getNLNsCleaned() > 0);

        closeEnv();
        initEnv(false, true);
        checkData(expectedMap);
        assertTrue(fileManager.getLastFileNum().longValue() >
                   lastNum.longValue());

        closeEnv();
    }

    /**
     * See if we can clean in the middle of the file set.
     */
    @Test
    public void testCleanFileHole()
        throws Throwable {

        initEnv(true, true);

        int nKeys = 20; // test ends up inserting 2*nKeys
        int nDupsPerKey = 30;

        EnvironmentImpl environment =
            DbInternal.getNonNullEnvImpl(env);
        FileManager fileManager = environment.getFileManager();

        /* Insert some non dup data, modify, insert dup data. */
        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, nKeys, 1, true);
        modifyData(expectedMap, 10, true);
        doLargePut(expectedMap, nKeys, nDupsPerKey, true);
        checkData(expectedMap);

        /*
         * Delete all the data, but abort. (Try to fill up the log
         * with entries we don't need.
         */
        deleteData(expectedMap, false, false);
        checkData(expectedMap);

        /* Do some more insertions, but abort them. */
        doLargePut(expectedMap, nKeys, nDupsPerKey, false);
        checkData(expectedMap);

        /* Do some more insertions and commit them. */
        doLargePut(expectedMap, nKeys, nDupsPerKey, true);
        checkData(expectedMap);

        /* Checkpoint */
        env.checkpoint(FORCE_CONFIG);
        checkData(expectedMap);

        /* Clean */
        Long lastNum = fileManager.getLastFileNum();
        env.cleanLog();

        /* Validate after cleaning. */
        checkData(expectedMap);
        EnvironmentStats stats = env.getStats(TestUtils.FAST_STATS);

        /* Make sure we really cleaned something.*/
        assertTrue(stats.getNINsCleaned() > 0);
        assertTrue(stats.getNLNsCleaned() > 0);

        closeEnv();
        initEnv(false, true);
        checkData(expectedMap);
        assertTrue(fileManager.getLastFileNum().longValue() >
                   lastNum.longValue());

        closeEnv();
    }

    /**
     * Test for SR13191.  This SR shows a problem where a MapLN is initialized
     * with a DatabaseImpl that has a null EnvironmentImpl.  When the Database
     * gets used, a NullPointerException occurs in the Cursor code which
     * expects there to be an EnvironmentImpl present.  The MapLN gets init'd
     * by the Cleaner reading through a log file and encountering a MapLN which
     * is not presently in the DbTree.  As an efficiency, the Cleaner calls
     * updateEntry on the BIN to try to insert the MapLN into the BIN so that
     * it won't have to fetch it when it migrates the BIN.  But this is bad
     * since the MapLN has not been init'd properly.  The fix was to ensure
     * that the MapLN is init'd correctly by calling postFetchInit on it just
     * prior to inserting it into the BIN.
     *
     * This test first creates an environment and two databases.  The first
     * database it just adds to the tree with no data.  This will be the MapLN
     * that eventually gets instantiated by the cleaner.  The second database
     * is used just to create a bunch of data that will get deleted so as to
     * create a low utilization for one of the log files.  Once the data for
     * db2 is created, the log is flipped (so file 0 is the one with the MapLN
     * for db1 in it), and the environment is closed and reopened.  We insert
     * more data into db2 until we have enough .jdb files that file 0 is
     * attractive to the cleaner.  Call the cleaner to have it instantiate the
     * MapLN and then use the MapLN in a Database.get() call.
     */
    @Test
    public void testSR13191()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        FileManager fileManager =
            DbInternal.getNonNullEnvImpl(env).getFileManager();

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db1 =
            env.openDatabase(null, "db1", dbConfig);

        Database db2 =
            env.openDatabase(null, "db2", dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(1, key);
        data.setData(new byte[100000]);
        for (int i = 0; i < 50; i++) {
            assertEquals(OperationStatus.SUCCESS, db2.put(null, key, data));
        }
        db1.close();
        db2.close();
        assertEquals("Should have 0 as current file", 0L,
                     fileManager.getCurrentFileNum());
        envImpl.forceLogFileFlip();
        env.close();

        env = new Environment(envHome, envConfig);
        fileManager = DbInternal.getNonNullEnvImpl(env).getFileManager();
        assertEquals("Should have 1 as current file", 1L,
                     fileManager.getCurrentFileNum());

        db2 = env.openDatabase(null, "db2", dbConfig);

        for (int i = 0; i < 250; i++) {
            assertEquals(OperationStatus.SUCCESS, db2.put(null, key, data));
        }

        db2.close();
        env.cleanLog();
        db1 = env.openDatabase(null, "db1", dbConfig);
        db1.get(null, key, data, null);
        db1.close();
        env.close();
    }

    /**
     * Tests that setting je.env.runCleaner=false stops the cleaner from
     * processing more files even if the target minUtilization is not met
     * [#15158].
     */
    @Test
    public void testCleanerStop()
        throws Throwable {

        final int fileSize = 1000000;
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             Integer.toString(fileSize));
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, "CleanerStop", dbConfig);

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        DatabaseEntry data = new DatabaseEntry(new byte[fileSize]);
        for (int i = 0; i <= 10; i += 1) {
            db.put(null, key, data);
        }
        env.checkpoint(FORCE_CONFIG);

        EnvironmentStats stats = env.getStats(null);
        assertEquals(0, stats.getNCleanerRuns());

        envConfig = env.getConfig();
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "true");
        env.setMutableConfig(envConfig);

        int iter = 0;
        while (stats.getNCleanerRuns() < 10) {
            iter += 1;
            if (iter == 20) {

                /*
                 * At one time the DaemonThread did not wakeup immediately in
                 * this test.  A workaround was to add an item to the job queue
                 * in FileProcessor.wakeup.  Later the job queue was removed
                 * and the DaemonThread.run() was fixed to wakeup immediately.
                 * This test verifies that the cleanup of the run() method
                 * works properly [#15267].
                 */
                fail("Cleaner did not run after " + iter + " tries");
            }
            Thread.yield();
            Thread.sleep(1000);
            stats = env.getStats(null);
        }

        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        env.setMutableConfig(envConfig);

        long prevNFiles = stats.getNCleanerRuns();

        /* Do multiple updates to create obsolete records. */
        for (int i = 0; i <= 10; i++) {
            db.put(null, key, data);
        }

        /* Wait a while to see if cleaner starts to work. */
        Thread.sleep(1000);

        stats = env.getStats(null);
        long currNFiles = stats.getNCleanerRuns();
        assertEquals("Expected no files cleaned, prevNFiles=" + prevNFiles + 
                     ", currNFiles=" + currNFiles, 
                     prevNFiles, currNFiles);

        db.close();
        env.close();
    }

    /**
     * Tests that the FileSelector memory budget is subtracted when the
     * environment is closed.  Before the fix in SR [#16368], it was not.
     */
    @Test
    public void testFileSelectorMemBudget()
        throws Throwable {

        final int fileSize = 1000000;
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             Integer.toString(fileSize));
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, "foo", dbConfig);

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        DatabaseEntry data = new DatabaseEntry(new byte[fileSize]);
        for (int i = 0; i <= 10; i += 1) {
            db.put(null, key, data);
        }
        env.checkpoint(FORCE_CONFIG);

        int nFiles = env.cleanLog();
        assertTrue(nFiles > 0);

        db.close();

        /*
         * To force the memory leak to be detected we have to close without a
         * checkpoint.  The checkpoint will finish processing all cleaned files
         * and subtract them from the budget.  But this should happen during
         * close, even without a checkpoint.
         */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        envImpl.close(false /*doCheckpoint*/);
    }

    /**
     * Tests that the cleanLog cannot be called in a read-only environment.
     * [#16368]
     */
    @Test
    public void testCleanLogReadOnly()
        throws Throwable {

        /* Open read-write. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);
        env.close();
        env = null;

        /* Open read-only. */
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        env = new Environment(envHome, envConfig);

        /* Try cleanLog in a read-only env. */
        try {
            env.cleanLog();
            fail();
        } catch (UnsupportedOperationException e) {
            assertEquals
                ("Log cleaning not allowed in a read-only or memory-only " +
                 "environment", e.getMessage());

        }
    }

    /**
     * Tests that when a file being cleaned is deleted, we ignore the error and
     * don't repeatedly try to clean it.  This is happening when we mistakenly
     * clean a file after it has been queued for deletion.  The workaround is
     * to catch LogFileNotFoundException in the cleaner and ignore the error.
     * We're testing the workaround here by forcing cleaning of deleted files.
     * [#15528]
     */
    @Test
    public void testUnexpectedFileDeletion()
        throws DatabaseException {

        initEnv(true, false);
        EnvironmentMutableConfig config = env.getMutableConfig();
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        env.setMutableConfig(config);

        final EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);
        final Cleaner cleaner = envImpl.getCleaner();
        final FileSelector fileSelector = cleaner.getFileSelector();

        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, 1000, 1, true);
        checkData(expectedMap);

        final long file1 = 0;
        final long file2 = 1;

        for (int i = 0; i < 100; i += 1) {
            modifyData(expectedMap, 1, true);
            checkData(expectedMap);
            fileSelector.injectFileForCleaning(new Long(file1));
            fileSelector.injectFileForCleaning(new Long(file2));
            assertTrue(fileSelector.getToBeCleanedFiles().contains(file1));
            assertTrue(fileSelector.getToBeCleanedFiles().contains(file2));
            while (env.cleanLog() > 0) {}
            assertTrue(!fileSelector.getToBeCleanedFiles().contains(file1));
            assertTrue(!fileSelector.getToBeCleanedFiles().contains(file2));
            env.checkpoint(FORCE_CONFIG);
            Map<Long,FileSummary> allFiles = envImpl.getUtilizationProfile().
                getFileSummaryMap(true /*includeTrackedFiles*/);
            assertTrue(!allFiles.containsKey(file1));
            assertTrue(!allFiles.containsKey(file2));
        }
        checkData(expectedMap);

        closeEnv();
    }

    /**
     * Helper routine. Generates keys with random alpha values while data
     * is numbered numerically.
     */
    private void doLargePut(Map<String, Set<String>> expectedMap,
                            int nKeys,
                            int nDupsPerKey,
                            boolean commit)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        for (int i = 0; i < nKeys; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            TestUtils.generateRandomAlphaBytes(key);
            String keyString = StringUtils.fromUTF8(key);

            /*
             * The data map is keyed by key value, and holds a hash
             * map of all data values.
             */
            Set<String> dataVals = new HashSet<String>();
            if (commit) {
                expectedMap.put(keyString, dataVals);
            }
            for (int j = 0; j < nDupsPerKey; j++) {
                String dataString = Integer.toString(j);
                exampleDb.put(txn,
                              new StringDbt(keyString),
                              new StringDbt(dataString));
                dataVals.add(dataString);
            }
        }
        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
    }

    /**
     * Increment each data value.
     */
    private void modifyData(Map<String, Set<String>> expectedMap,
                            int increment,
                            boolean commit)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);

        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();

        Cursor cursor = exampleDb.openCursor(txn, null);
        OperationStatus status = cursor.getFirst(foundKey, foundData,
                                                 LockMode.DEFAULT);

        boolean toggle = true;
        while (status == OperationStatus.SUCCESS) {
            if (toggle) {

                String foundKeyString = foundKey.getString();
                String foundDataString = foundData.getString();
                int newValue = Integer.parseInt(foundDataString) + increment;
                String newDataString = Integer.toString(newValue);

                /* If committing, adjust the expected map. */
                if (commit) {

                    Set<String> dataVals = expectedMap.get(foundKeyString);
                    if (dataVals == null) {
                        fail("Couldn't find " +
                             foundKeyString + "/" + foundDataString);
                    } else if (dataVals.contains(foundDataString)) {
                        dataVals.remove(foundDataString);
                        dataVals.add(newDataString);
                    } else {
                        fail("Couldn't find " +
                             foundKeyString + "/" + foundDataString);
                    }
                }

                assertEquals(OperationStatus.SUCCESS,
                             cursor.delete());
                assertEquals(OperationStatus.SUCCESS,
                             cursor.put(foundKey,
                                        new StringDbt(newDataString)));
                toggle = false;
            } else {
                toggle = true;
            }

            status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
        }

        cursor.close();
        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
    }

    /**
     * Delete data.
     */
    private void deleteData(Map<String, Set<String>> expectedMap,
                            boolean everyOther,
                            boolean commit)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);

        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();

        Cursor cursor = exampleDb.openCursor(txn, null);
        OperationStatus status = cursor.getFirst(foundKey, foundData,
                                                 LockMode.DEFAULT);

        boolean toggle = true;
        while (status == OperationStatus.SUCCESS) {
            if (toggle) {

                String foundKeyString = foundKey.getString();
                String foundDataString = foundData.getString();

                /* If committing, adjust the expected map */
                if (commit) {

                    Set dataVals = expectedMap.get(foundKeyString);
                    if (dataVals == null) {
                        fail("Couldn't find " +
                             foundKeyString + "/" + foundDataString);
                    } else if (dataVals.contains(foundDataString)) {
                        dataVals.remove(foundDataString);
                        if (dataVals.size() == 0) {
                            expectedMap.remove(foundKeyString);
                        }
                    } else {
                        fail("Couldn't find " +
                             foundKeyString + "/" + foundDataString);
                    }
                }

                assertEquals(OperationStatus.SUCCESS, cursor.delete());
            }

            if (everyOther) {
                toggle = toggle? false: true;
            }

            status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
        }

        cursor.close();
        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
    }

    /**
     * Check what's in the database against what's in the expected map.
     */
    private void checkData(Map<String, Set<String>> expectedMap)
        throws DatabaseException {

        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();
        Cursor cursor = exampleDb.openCursor(null, null);
        OperationStatus status = cursor.getFirst(foundKey, foundData,
                                                 LockMode.DEFAULT);

        /*
         * Make a copy of expectedMap so that we're free to delete out
         * of the set of expected results when we verify.
         * Also make a set of counts for each key value, to test count.
         */

        Map<String, Set<String>> checkMap = new HashMap<String, Set<String>>();
        Map<String, Integer>countMap = new HashMap<String, Integer>();
        Iterator<Map.Entry<String, Set<String>>> iter =
                        expectedMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Set<String>> entry = iter.next();
            Set<String> copySet = new HashSet<String>();
            copySet.addAll(entry.getValue());
            checkMap.put(entry.getKey(), copySet);
            countMap.put(entry.getKey(), new Integer(copySet.size()));
        }

        while (status == OperationStatus.SUCCESS) {
            String foundKeyString = foundKey.getString();
            String foundDataString = foundData.getString();

            /* Check that the current value is in the check values map */
            Set dataVals = checkMap.get(foundKeyString);
            if (dataVals == null) {
                fail("Couldn't find " +
                     foundKeyString + "/" + foundDataString);
            } else if (dataVals.contains(foundDataString)) {
                dataVals.remove(foundDataString);
                if (dataVals.size() == 0) {
                    checkMap.remove(foundKeyString);
                }
            } else {
                fail("Couldn't find " +
                     foundKeyString + "/" +
                     foundDataString +
                     " in data vals");
            }

            /* Check that the count is right. */
            int count = cursor.count();
            assertEquals(countMap.get(foundKeyString).intValue(),
                         count);

            status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
        }

        cursor.close();

        if (checkMap.size() != 0) {
            dumpExpected(checkMap);
            fail("checkMapSize = " + checkMap.size());

        }
        assertEquals(0, checkMap.size());
    }

    private void dumpExpected(Map expectedMap) {
        Iterator iter = expectedMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            Iterator dataIter = ((Set) entry.getValue()).iterator();
            while (dataIter.hasNext()) {
                System.out.println("key=" + key +
                                   " data=" + (String) dataIter.next());
            }
        }
    }

    /**
     * Tests that cleaner mutable configuration parameters can be changed and
     * that the changes actually take effect.
     */
    @Test
    public void testMutableConfig()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);
        envConfig = env.getConfig();
        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);
        Cleaner cleaner = envImpl.getCleaner();
        MemoryBudget budget = envImpl.getMemoryBudget();
        String name;
        String val;

        /* je.cleaner.minUtilization */
        name = EnvironmentParams.CLEANER_MIN_UTILIZATION.getName();
        setParam(name, "33");
        assertEquals(33, cleaner.minUtilization);

        /* je.cleaner.minFileUtilization */
        name = EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION.getName();
        setParam(name, "7");
        assertEquals(7, cleaner.minFileUtilization);

        /* je.cleaner.bytesInterval */
        name = EnvironmentParams.CLEANER_BYTES_INTERVAL.getName();
        setParam(name, "1000");
        assertEquals(1000, cleaner.cleanerBytesInterval);

        /* je.cleaner.deadlockRetry */
        name = EnvironmentParams.CLEANER_DEADLOCK_RETRY.getName();
        setParam(name, "7");
        assertEquals(7, cleaner.nDeadlockRetries);

        /* je.cleaner.lockTimeout */
        name = EnvironmentParams.CLEANER_LOCK_TIMEOUT.getName();
        setParam(name, "7000");
        assertEquals(7, cleaner.lockTimeout);

        /* je.cleaner.expunge */
        name = EnvironmentParams.CLEANER_REMOVE.getName();
        val = "false".equals(envConfig.getConfigParam(name)) ?
            "true" : "false";
        setParam(name, val);
        assertEquals(val.equals("true"), cleaner.expunge);

        /* je.cleaner.minAge */
        name = EnvironmentParams.CLEANER_MIN_AGE.getName();
        setParam(name, "7");
        assertEquals(7, cleaner.minAge);

        /* je.cleaner.readSize */
        name = EnvironmentParams.CLEANER_READ_SIZE.getName();
        setParam(name, "7777");
        assertEquals(7777, cleaner.readBufferSize);

        /* je.cleaner.detailMaxMemoryPercentage */
        name = EnvironmentParams.CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE.
            getName();
        setParam(name, "7");
        assertEquals((budget.getMaxMemory() * 7) / 100,
                     budget.getTrackerBudget());

        /* je.cleaner.threads */
        name = EnvironmentParams.CLEANER_THREADS.getName();
        setParam(name, "7");
        assertEquals((envImpl.isNoLocking() ? 0 : 7),
                     countCleanerThreads());

        env.close();
        env = null;
    }

    /**
     * Sets a mutable config param, checking that the given value is not
     * already set and that it actually changes.
     */
    private void setParam(String name, String val)
        throws DatabaseException {

        EnvironmentMutableConfig config = env.getMutableConfig();
        String myVal = config.getConfigParam(name);
        assertTrue(!val.equals(myVal));

        config.setConfigParam(name, val);
        env.setMutableConfig(config);

        config = env.getMutableConfig();
        myVal = config.getConfigParam(name);
        assertTrue(val.equals(myVal));
    }

    /**
     * Count the number of threads with the name "Cleaner#".
     */
    private int countCleanerThreads() {

        Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);

        int count = 0;
        for (int i = 0; i < threads.length; i += 1) {
            if (threads[i] != null &&
                threads[i].getName().startsWith("Cleaner")) {
                count += 1;
            }
        }

        return count;
    }

    /**
     * Checks that the memory budget is updated properly by the
     * UtilizationTracker.  Prior to a bug fix [#15505] amounts were added to
     * the budget but not subtracted when two TrackedFileSummary objects were
     * merged.  Merging occurs when a local tracker is added to the global
     * tracker.  Local trackers are used during recovery, checkpoints, lazy
     * compression, and reverse splits.
     */
    @Test
    public void testTrackerMemoryBudget()
        throws DatabaseException {

        /* Open environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        /* Open database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        exampleDb = env.openDatabase(null, "foo", dbConfig);

        /* Insert data. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= 200; i += 1) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            exampleDb.put(null, key, data);
        }

        /* Sav the admin budget baseline. */
        flushTrackedFiles();
        long admin = env.getStats(null).getAdminBytes();

        /*
         * Nothing becomes obsolete when inserting and no INs are logged, so
         * the budget does not increase. In fact, if the new LN is embedded,
         * it is recorded as immediately obsolete, but its offset is not
         * tracked.
         */
        IntegerBinding.intToEntry(201, key);
        exampleDb.put(null, key, data);
        assertEquals(admin, env.getStats(null).getAdminBytes());
        flushTrackedFiles();
        assertEquals(admin, env.getStats(null).getAdminBytes());

        /*
         * Update a record and expect the budget to increase because the old
         * LN becomes obsolete. With embedded LNs, no increase occurs because
         * the old LN has already been counted.
         */
        exampleDb.put(null, key, data);

        if (embeddedLNs) {
            assertEquals(admin, env.getStats(null).getAdminBytes());
        } else {
            assertTrue(admin < env.getStats(null).getAdminBytes());
        }

        flushTrackedFiles();
        assertEquals(admin, env.getStats(null).getAdminBytes());

        /*
         * Delete all records and expect the budget to increase because LNs
         * become obsolete.
         */
        for (int i = 1; i <= 201; i += 1) {
            IntegerBinding.intToEntry(i, key);
            exampleDb.delete(null, key);
        }

        if (embeddedLNs) {
            assertEquals(admin, env.getStats(null).getAdminBytes());
        } else {
            assertTrue(admin < env.getStats(null).getAdminBytes());
        }

        flushTrackedFiles();
        assertEquals(admin, env.getStats(null).getAdminBytes());

        /*
         * Compress and expect no change to the budget.  Prior to the fix for
         * [#15505] the assertion below failed because the baseline admin
         * budget was not restored.
         */
        env.compress();
        flushTrackedFiles();
        assertEquals(admin, env.getStats(null).getAdminBytes());

        closeEnv();
    }

    /**
     * Flushes all tracked files to subtract tracked info from the admin memory
     * budget.
     */
    private void flushTrackedFiles()
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        UtilizationTracker tracker = envImpl.getUtilizationTracker();
        UtilizationProfile profile = envImpl.getUtilizationProfile();

        for (TrackedFileSummary summary : tracker.getTrackedFiles()) {
            profile.flushFileSummary(summary);
        }
    }

    /**
     * Tests that memory is budgeted correctly for FileSummaryLNs that are
     * inserted and deleted after calling setTrackedSummary.  The size of the
     * FileSummaryLN changes during logging when setTrackedSummary is called,
     * and this is accounted for specially in CursorImpl.finishInsert. [#15831]
     */
    @Test
    public void testFileSummaryLNMemoryUsage()
        throws DatabaseException {

        /* Open environment, prevent concurrent access by daemons. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        UtilizationProfile up = envImpl.getUtilizationProfile();
        DatabaseImpl fileSummaryDb = up.getFileSummaryDb();
        MemoryBudget memBudget = envImpl.getMemoryBudget();

        BasicLocker locker = null;
        CursorImpl cursor = null;
        try {
            locker = BasicLocker.createBasicLocker(envImpl);
            cursor = new CursorImpl(fileSummaryDb, locker);

            /* Get parent BIN.  There should be only one BIN in the tree. */
            IN root =
                fileSummaryDb.getTree().getRootIN(CacheMode.DEFAULT);
            root.releaseLatch();
            assertEquals(1, root.getNEntries());
            BIN parent = (BIN) root.getTarget(0);

            /* Use an artificial FileSummaryLN with a tracked summary. */
            FileSummaryLN ln = new FileSummaryLN(new FileSummary());
            TrackedFileSummary tfs = new TrackedFileSummary
                (envImpl.getUtilizationTracker(), 0 /*fileNum*/,
                 true /*trackDetail*/);
            tfs.trackObsolete(0, true /*checkDupOffsets*/);
            byte[] keyBytes =
                FileSummaryLN.makeFullKey(0 /*fileNum*/, 123 /*sequence*/);
            int keySize = MemoryBudget.byteArraySize(keyBytes.length);

            /* Perform insert after calling setTrackedSummary. */
            long oldSize = ln.getMemorySizeIncludedByParent();
            long oldParentSize = getAdjustedMemSize(parent, memBudget);
            ln.setTrackedSummary(tfs);
            boolean inserted = cursor.insertRecord(
                keyBytes, ln, false, fileSummaryDb.getRepContext());
            assertTrue(inserted);

            cursor.latchBIN();
            assertTrue(cursor.isOnBIN(parent));
            ln.addExtraMarshaledMemorySize(parent);
            cursor.releaseBIN();

            long newSize = ln.getMemorySizeIncludedByParent();
            long newParentSize = getAdjustedMemSize(parent, memBudget);

            /* The size of the LN increases during logging. */
            assertEquals(newSize,
                         oldSize +
                         ln.getObsoleteOffsets().getExtraMemorySize());

            /* The correct size is accounted for by the parent BIN. */
            assertEquals(newSize + keySize, newParentSize - oldParentSize);

            /* Correct size is subtracted during eviction. */
            oldParentSize = newParentSize;
            cursor.evictLN();
            newParentSize = getAdjustedMemSize(parent, memBudget);
            assertEquals(oldParentSize - newSize, newParentSize);

            /* Fetch a fresh FileSummaryLN before deleting it. */
            oldParentSize = newParentSize;
            ln = (FileSummaryLN) cursor.lockAndGetCurrentLN(LockType.READ);
            newSize = ln.getMemorySizeIncludedByParent();
            newParentSize = getAdjustedMemSize(parent, memBudget);
            assertEquals(newSize, newParentSize - oldParentSize);

            /* Perform delete. */
            oldSize = newSize;
            oldParentSize = newParentSize;
            OperationResult result = cursor.deleteCurrentRecord(
                fileSummaryDb.getRepContext());
            assertNotNull(result);
            newSize = ln.getMemorySizeIncludedByParent();
            newParentSize = getAdjustedMemSize(parent, memBudget);

            /* Size changes during delete also, which performs eviction. */
            assertTrue(newSize < oldSize);
            assertTrue(oldSize - newSize >
                       ln.getObsoleteOffsets().getExtraMemorySize());
            assertEquals(0 - oldSize, newParentSize - oldParentSize);
        } finally {
            if (cursor != null) {
                cursor.releaseBIN();
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }

        TestUtils.validateNodeMemUsage(envImpl, true /*assertOnError*/);

        /* Insert again, this time using the UtilizationProfile method. */
        FileSummaryLN ln = new FileSummaryLN(new FileSummary());
        TrackedFileSummary tfs = new TrackedFileSummary
            (envImpl.getUtilizationTracker(), 0 /*fileNum*/,
             true /*trackDetail*/);
        tfs.trackObsolete(0, true/*checkDupOffsets*/);
        ln.setTrackedSummary(tfs);
        assertTrue(up.insertFileSummary(ln, 0 /*fileNum*/, 123 /*sequence*/));
        TestUtils.validateNodeMemUsage(envImpl, true /*assertOnError*/);

        closeEnv();
    }

    /**
     * Checks that log utilization is updated incrementally during the
     * checkpoint rather than only when the highest dirty level in the Btree is
     * flushed.  This feature (incremental update) was added so that log
     * cleaning is not delayed until the end of the checkpoint. [#16037]
     */
    @Test
    public void testUtilizationDuringCheckpoint()
        throws DatabaseException {

        /*
         * Use Database.sync of a deferred-write database to perform this test
         * rather than a checkpoint, because the hook is called at a
         * predictable place when only a single database is flushed.  The
         * implementation of Checkpointer.flushDirtyNodes is shared for
         * Database.sync and checkpoint, so this tests both cases.
         */
        final int FANOUT = 25;
        final int N_KEYS = FANOUT * FANOUT * FANOUT;

        /* Open environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        /* Open ordinary non-transactional database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setNodeMaxEntries(FANOUT);
        exampleDb = env.openDatabase(null, "foo", dbConfig);

        /* Clear stats. */
        StatsConfig statsConfig = new StatsConfig();
        statsConfig.setClear(true);
        env.getStats(statsConfig);

        /* Write to database to create a 3 level Btree. */
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry(new byte[4]);
        for (int i = 0; i < N_KEYS; i += 1) {
            LongBinding.longToEntry(i, keyEntry);
            assertSame(OperationStatus.SUCCESS,
                       exampleDb.put(null, keyEntry, dataEntry));
            EnvironmentStats stats = env.getStats(statsConfig);
            if (stats.getNEvictPasses() > 0) {
                break;
            }
        }

        /*
         * Sync and write an LN in each BIN to create a bunch of dirty INs
         * that, when flushed again, will cause the prior versions to be
         * obsolete.
         */
        env.sync();
        for (int i = 0; i < N_KEYS; i += FANOUT) {
            LongBinding.longToEntry(i, keyEntry);
            assertSame(OperationStatus.SUCCESS,
                       exampleDb.put(null, keyEntry, dataEntry));
        }

        /*
         * Close and re-open as a deferred-write DB so that we can call sync.
         * The INs will remain dirty.
         */
        exampleDb.close();
        dbConfig = new DatabaseConfig();
        dbConfig.setDeferredWrite(true);
        exampleDb = env.openDatabase(null, "foo", dbConfig);

        /*
         * The test hook is called just before writing the highest dirty level
         * in the Btree.  At that point, utilization should be reduced if the
         * incremental utilization update feature is working properly.  Before
         * adding this feature, utilization was not changed at this point.
         */
        final int oldUtilization = getUtilization();
        final StringBuilder hookCalledFlag = new StringBuilder();

        Checkpointer.setMaxFlushLevelHook(
            new TestHook() {

                public void doHook() {

                    hookCalledFlag.append(1);

                    final int newUtilization = getUtilization();

                    int diff = (int)
                        ((100.0 * (oldUtilization - newUtilization)) /
                         oldUtilization);

                    String msg = "oldUtilization=" + oldUtilization +
                        " newUtilization=" + newUtilization +
                        " diff = " + diff + "%";

                    if (embeddedLNs) {
                        assertTrue(msg, diff >= 6);
                    } else {
                        assertTrue(msg, diff >= 10);
                    }

                    /* Don't call the test hook repeatedly. */
                    Checkpointer.setMaxFlushLevelHook(null);
                }
                public Object getHookValue() {
                    throw new UnsupportedOperationException();
                }
                public void doIOHook() {
                    throw new UnsupportedOperationException();
                }
                public void hookSetup() {
                    throw new UnsupportedOperationException();
                }
                public void doHook(Object obj) {
                    throw new UnsupportedOperationException();                
                }
            }
        );
        exampleDb.sync();
        assertTrue(hookCalledFlag.length() > 0);

        /* While we're here, do a quick check of getCurrentMinUtilization. */
        final int lastKnownUtilization =
            env.getStats(null).getCurrentMinUtilization();
        assertTrue(lastKnownUtilization > 0);

        closeEnv();
    }

    private int getUtilization() {
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Map<Long,FileSummary> map =
            envImpl.getUtilizationProfile().getFileSummaryMap(true);
        FileSummary totals = new FileSummary();
        for (FileSummary summary : map.values()) {
            totals.add(summary);
        }
        return FileSummary.utilization(totals.getObsoleteSize(),
                                       totals.totalSize);
    }

    /**
     * Returns the memory size taken by the given IN, not including the target
     * rep, which changes during eviction.
     */
    private long getAdjustedMemSize(IN in, MemoryBudget memBudget) {
        return getMemSize(in, memBudget) -
               in.getTargets().calculateMemorySize();
    }

    /**
     * Returns the memory size taken by the given IN and the tree memory usage.
     */
    private long getMemSize(IN in, MemoryBudget memBudget) {
        return memBudget.getTreeMemoryUsage() +
               in.getInMemorySize() -
               in.getBudgetedMemorySize();
    }

    /**
     * Tests that dirtiness is logged upwards during a checkpoint, even if a
     * node is evicted and refetched after being added to the checkpointer's
     * dirty map, and before that entry in the dirty map is processed by the
     * checkpointer.  [#16523]
     *
     *  Root INa
     *      /  \
     *     INb  ...
     *    /
     *   INc
     *  /
     * BINd
     *
     * The scenario that causes the bug is:
     *
     * 1) Prior to the final checkpoint, the cleaner processes a log file
     * containing BINd.  The cleaner marks BINd dirty so that it will be
     * flushed prior to the end of the next checkpoint, at which point the file
     * containing BINd will be deleted.  The cleaner also calls
     * setProhibitNextDelta on BINd to ensure that a full version will be
     * logged.
     *
     * 2) At checkpoint start, BINd is added to the checkpoiner's dirty map.
     * It so happens that INa is also dirty, perhaps as the result of a split,
     * and added to the dirty map.  The checkpointer's max flush level is 4.
     *
     * 3) The evictor flushes BINd and then its parent INc.  Both are logged
     * provisionally, since their level is less than 4, the checkpointer's max
     * flush level.  INb, the parent of INc, is dirty.
     *
     * 4) INc, along with BINd, is loaded back into the Btree as the result of
     * reading an LN in BINd.  INc and BINd are both non-dirty.  INb, the
     * parent of INc, is still dirty.
     *
     * 5) The checkpointer processes its reference to BINd in the dirty map.
     * It finds that BINd is not dirty, so does not need to be logged.  It
     * attempts to add the parent, INc, to the dirty map in order to propagate
     * changes upward.  However, because INc is not dirty, it is not added to
     * the dirty map -- this was the bug, it should be added even if not dirty.
     * So as the result of this step, the checkpointer does no logging and does
     * not add anything to the dirty map.
     *
     * 6) The checkpointer logs INa (it was dirty at the start of the
     * checkpoint) and the checkpoint finishes.  It deletes the cleaned log
     * file that contains the original version of BINd.
     *
     * The key thing is that INb is now dirty and was not logged.  It should
     * have been logged as the result of being an ancestor of BINd, which was
     * in the dirty map.  Its parent INa was logged, but does not refer to the
     * latest version of INb/INc/BINd.
     *
     * 7) Now we recover.  INc and BINd, which were evicted during step (3),
     * are not replayed because they are provisional -- they are lost.  When a
     * search for an LN in BINd is performed, we traverse down to the old
     * version of BINd, which causes LogFileNotFound.
     *
     * The fix is to add INc to the dirty map at step (5), even though it is
     * not dirty.  When the reference to INc in the dirty map is processed we
     * will not log INc, but we will add its parent INb to the dirty map.  Then
     * when the reference to INb is processed, it will be logged because it is
     * dirty.  Then INa is logged and refers to the latest version of
     * INb/INc/BINd.
     *
     * This problem could only occur with a Btree of depth 4 or greater.
     */
    @Test
    public void testEvictionDuringCheckpoint()
        throws DatabaseException {

        /* Use small fanout to create a deep tree. */
        final int FANOUT = 6;
        final int N_KEYS = FANOUT * FANOUT * FANOUT;

        /* Open environment without interference of daemon threads. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        /*
         * We should disable BtreeVerifier for this test case. Otherwise,
         * it will encounter IN-exist-exception when calling cursor.getNext
         * in BtreeVerifier. cursor.getNext may call Tree.getNextBin, when
         * the next-BIN wants to add to INList, it will throw exception.
         *
         * The reason is as follows.
         * We have the following in-memory tree. The exception happens when
         * we wants to get BINe. simulateEviction can cause BINd and INc to
         * be removed from INList, but it did not remove BINe from the INList.
         * So after simulating evict BINd and INc, through cursor.get/getNext,
         * we will re-fetch INc and BINd. Now it is OK, because BINd and INc
         * are not in the INList. If we wants to continue to access BINe,
         * then now the slot target in INc is null and we need to fetch BINe
         * and then attach it to INc. For IN, for its hashCode, equals,
         * compareTo, we all use the nodeId to identify a IN. So when we want
         * to insert new fetch BINe to ConcurrentMap INList.ins, it will
         * think that the IN has already exist on the INList.
         *  Root INa
         *      /  \
         *     INb  ...
         *    /
         *   INc
         *  /   \
         * BINd  BINe
         */
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);
        final EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);

        /* Open database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setNodeMaxEntries(FANOUT);
        exampleDb = env.openDatabase(null, "foo", dbConfig);
        DatabaseImpl dbImpl = DbInternal.getDbImpl(exampleDb);

        /* Write to database to create a 4 level Btree. */
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry(new byte[0]);
        int nRecords;
        for (nRecords = 1;; nRecords += 1) {
            LongBinding.longToEntry(nRecords, keyEntry);
            assertSame(OperationStatus.SUCCESS,
                       exampleDb.put(null, keyEntry, dataEntry));
            if (nRecords % 10 == 0) {
                int level = envImpl.getDbTree().getHighestLevel(dbImpl);
                if ((level & IN.LEVEL_MASK) >= 4) {
                    break;
                }
            }
        }

        /* Flush all dirty nodes. */
        env.sync();

        /* Get BINd and its ancestors.  Mark BINd and INa dirty. */
        final IN nodeINa = dbImpl.getTree().getRootIN(CacheMode.DEFAULT);
        nodeINa.releaseLatch();
        final IN nodeINb = (IN) nodeINa.getTarget(0);
        final IN nodeINc = (IN) nodeINb.getTarget(0);
        final BIN nodeBINd = (BIN) nodeINc.getTarget(0);
        assertNotNull(nodeBINd);
        nodeINa.setDirty(true);
        nodeBINd.setDirty(true);

        /*
         * The test hook is called after creating the checkpoint dirty map and
         * just before flushing dirty nodes.
         */
        final StringBuilder hookCalledFlag = new StringBuilder();

        Checkpointer.setBeforeFlushHook(new TestHook() {
            public void doHook() {
                hookCalledFlag.append(1);
                /* Don't call the test hook repeatedly. */
                Checkpointer.setBeforeFlushHook(null);
                try {
                    /* Evict BINd and INc. */
                    simulateEviction(env, envImpl, nodeBINd, nodeINc);
                    simulateEviction(env, envImpl, nodeINc, nodeINb);

                    /*
                     * Force BINd and INc to be loaded into cache by fetching
                     * the left-most record.
                     *
                     * Note that nodeINc and nodeBINd are different instances
                     * and are no longer in the Btree but we don't change these
                     * variables because they are final.  They should not be
                     * used past this point.
                     */
                    LongBinding.longToEntry(1, keyEntry);
                    assertSame(OperationStatus.SUCCESS,
                               exampleDb.get(null, keyEntry, dataEntry, null));
                } catch (DatabaseException e) {
                    throw new RuntimeException(e);
                }
            }
            public Object getHookValue() {
                throw new UnsupportedOperationException();
            }
            public void doIOHook() {
                throw new UnsupportedOperationException();
            }
            public void hookSetup() {
                throw new UnsupportedOperationException();
            }
            public void doHook(Object obj) {
                throw new UnsupportedOperationException();
            }
        });
        env.checkpoint(FORCE_CONFIG);
        assertTrue(hookCalledFlag.length() > 0);
        assertTrue(!nodeINa.getDirty());
        assertTrue(!nodeINb.getDirty()); /* This failed before the bug fix. */

        closeEnv();
    }

    /**
     * Simulate eviction by logging this node, updating the LSN in its
     * parent slot, setting the Node to null in the parent slot, and
     * removing the IN from the INList.  Logging is provisional.  The
     * parent is dirtied.  May not be called unless this node is dirty and
     * none of its children are dirty.  Children may be resident.
     */
    private void simulateEviction(Environment env,
                                  EnvironmentImpl envImpl,
                                  IN nodeToEvict,
                                  IN parentNode)
        throws DatabaseException {

        assertTrue("not dirty " + nodeToEvict.getNodeId(),
                   nodeToEvict.getDirty());
        assertTrue(!hasDirtyChildren(nodeToEvict));

        parentNode.latch();

        long lsn = TestUtils.logIN(
            env, nodeToEvict, false /*allowDeltas*/, true /*provisional*/,
            parentNode);

        int index;
        for (index = 0;; index += 1) {
            if (index >= parentNode.getNEntries()) {
                fail();
            }
            if (parentNode.getTarget(index) == nodeToEvict) {
                break;
            }
        }

        nodeToEvict.latch();

        envImpl.getEvictor().remove(nodeToEvict);
        envImpl.getInMemoryINs().remove(nodeToEvict);
        parentNode.recoverIN(index, null /*node*/, lsn, 0 /*lastLoggedSize*/);

        nodeToEvict.releaseLatch();
        parentNode.releaseLatch();
    }

    private boolean hasDirtyChildren(IN parent) {
        for (int i = 0; i < parent.getNEntries(); i += 1) {
            Node child = parent.getTarget(i);
            if (child instanceof IN) {
                IN in = (IN) child;
                if (in.getDirty()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    public void testMultiCleaningBug()
        throws DatabaseException {

        initEnv(true, false);

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final Cleaner cleaner = envImpl.getCleaner();

        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, 1000, 1, true);
        modifyData(expectedMap, 1, true);
        checkData(expectedMap);

        final TestHook hook = new TestHook() {
            public void doHook() {
                /* Signal that hook was called. */
                if (synchronizer != 99) {
                    synchronizer = 1;
                }
                /* Wait for signal to proceed with cleaning. */
                while (synchronizer != 2 &&
                       synchronizer != 99 &&
                       !Thread.interrupted()) {
                    Thread.yield();
                }
            }
            public Object getHookValue() {
                throw new UnsupportedOperationException();
            }
            public void doIOHook() throws IOException {
                throw new UnsupportedOperationException();
            }
            public void hookSetup() {
                throw new UnsupportedOperationException();
            }
            public void doHook(Object obj) {
                throw new UnsupportedOperationException();                
            }
        };

        junitThread = new JUnitThread("TestMultiCleaningBug") {
            public void testBody()
                throws DatabaseException {

                try {
                    while (synchronizer != 99) {
                        /* Wait for initial state. */
                        while (synchronizer != 0 &&
                               synchronizer != 99 &&
                               !Thread.interrupted()) {
                            Thread.yield();
                        }
                        /* Clean with hook set, hook is called next. */
                        cleaner.setFileChosenHook(hook);
                        env.cleanLog();
                        /* Signal that cleaning is done. */
                        if (synchronizer != 99) {
                            synchronizer = 3;
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        };

        /* Kick off thread above. */
        synchronizer = 0;
        junitThread.start();

        for (int i = 0; i < 100 && junitThread.isAlive(); i += 1) {
            /* Wait for hook to be called when a file is chosen. */
            while (synchronizer != 1 && junitThread.isAlive()) {
                Thread.yield();
            }
            /* Allow the thread to clean the chosen file. */
            synchronizer = 2;
            /* But immediately clean here, which could select the same file. */
            cleaner.setFileChosenHook(null);
            env.cleanLog();
            /* Wait for both cleaner runs to finish. */
            while (synchronizer != 3 && junitThread.isAlive()) {
                Thread.yield();
            }
            /* Make more waste to be cleaned. */
            modifyData(expectedMap, 1, true);
            synchronizer = 0;
        }

        synchronizer = 99;

        try {
            junitThread.finishTest();
            junitThread = null;
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.toString());
        }

        closeEnv();
    }

    /**
     * Ensures that LN migration is immediate.  Lazy migration is no longer
     * used, even if configured.
     */
    @SuppressWarnings("deprecation")
    public void testCleanerLazyMigrationConfig()
        throws DatabaseException {

        /* Open environment without interference of daemon threads. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             Integer.toString(FILE_SIZE));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");

        /* Configure immediate migration, even though it's deprecated. */
        envConfig.setConfigParam
            (EnvironmentConfig.CLEANER_LAZY_MIGRATION, "true");

        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);
        final EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);

        /* Open database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        exampleDb = env.openDatabase(null, "foo", dbConfig);

        /* Clear stats. */
        StatsConfig clearStats = new StatsConfig();
        clearStats.setClear(true);
        env.getStats(clearStats);

        /* Insert and update data. */
        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, 1000, 1, true);
        modifyData(expectedMap, 1, true);
        checkData(expectedMap);

        /* Clean. */
        while (true) {
            long files = env.cleanLog();
            if (files == 0) {
                break;
            }
        }

        /* There should be no checkpoint or eviction. */
        EnvironmentStats stats = env.getStats(null);
        assertEquals(0, stats.getNEvictPasses());
        assertEquals(0, stats.getNCheckpoints());
        assertTrue(stats.getNCleanerRuns() > 0);

        /* Clear stats. */
        env.getStats(clearStats);

        /* Flush all dirty nodes. */
        env.sync();

        stats = env.getStats(null);
        assertEquals(0, stats.getNLNsMigrated());

        closeEnv();
    }

    /**
     * Checks that no fetch misses occur when deleting FileSummaryLNs.
     */
    @Test
    public void testOptimizedFileSummaryLNDeletion() {

        /* Open environment without interference of daemon threads. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 Integer.toString(FILE_SIZE));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);
        final EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);

        /* Open database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        exampleDb = env.openDatabase(null, "foo", dbConfig);

        /* Insert and update data. */
        Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, 1000, 1, true);
        modifyData(expectedMap, 1, false);
        checkData(expectedMap);
        deleteData(expectedMap, false, true);
        checkData(expectedMap);

        /* Clear stats. */
        StatsConfig clearStats = new StatsConfig();
        clearStats.setClear(true);
        env.getStats(clearStats);

        /* Clean. */
        while (true) {
            long files = env.cleanLog();
            if (files == 0) {
                break;
            }
        }

        /* There should be cleaning but no checkpoint or eviction. */
        EnvironmentStats stats = env.getStats(clearStats);
        assertTrue(stats.getNCleanerRuns() > 0);
        assertEquals(0, stats.getNEvictPasses());
        assertEquals(0, stats.getNCheckpoints());

        /*
         * Flush all dirty nodes, which should delete the cleaned log files and
         * their FileSummaryLNs and also update the related MapLNs.
         */
        env.sync();

        /*
         * Before optimization of FileSummaryLN deletion, there were 16 cache
         * misses.  Without the optimization the cache misses occur because
         * FileSummaryLNs are always evicted after reading or writing them, and
         * then were fetched before deleting them.  Now that we can delete
         * FileSummaryLNs without fetching, there should be no misses.
         */
        stats = env.getStats(clearStats);
        assertEquals(0, stats.getNLNsFetchMiss());

        closeEnv();
    }

    /**
     * Ensure that LN migration does not cause the representation of INs to
     * change when migration places an LN in the slot and then evicts it
     * afterwards.  The representation should remain "no target" (empty), if
     * that was the BIN representation before the LN migration.  Before the bug
     * fix [#21734] we neglected to call BIN.compactMemory after the eviction.
     * The fix is for the cleaner to call BIN.evictLN, rather then doing the
     * eviction separately.
     */
    @Test
    public void testCompactBINAfterMigrateLN() {

        /* Open environment without interference of daemon threads. */
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam
            (EnvironmentParams.TREE_MAX_EMBEDDED_LN.getName(), "0");
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             Integer.toString(FILE_SIZE));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);
        final EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);

        /* Open database with CacheMode.EVICT_LN. */
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);
        exampleDb = env.openDatabase(null, "foo", dbConfig);

        /* Clear stats. */
        final StatsConfig clearStats = new StatsConfig();
        clearStats.setClear(true);
        env.getStats(clearStats);

        /* Insert and update data. */
        final Map<String, Set<String>> expectedMap =
            new HashMap<String, Set<String>>();
        doLargePut(expectedMap, 1000, 1, true);
        modifyData(expectedMap, 1, true);
        checkData(expectedMap);

        EnvironmentStats stats = env.getStats(clearStats);

        /* There should be no checkpoint or eviction. */
        assertEquals(0, stats.getNEvictPasses());
        assertEquals(0, stats.getNCheckpoints());

        /*
         * Due to using EVICT_LN mode, the representation of most INs should be
         * "no target" (empty).
         */
        final long nNoTarget = stats.getNINNoTarget();
        assertTrue(stats.toString(), nNoTarget > stats.getNINSparseTarget());

        /* Clean. */
        while (true) {
            final long files = env.cleanLog();
            if (files == 0) {
                break;
            }
        }

        stats = env.getStats(null);

        /* There should be no checkpoint or eviction. */
        assertEquals(0, stats.getNEvictPasses());
        assertEquals(0, stats.getNCheckpoints());

        /* A bunch of LNs should have been migrated. */
        assertTrue(stats.getNCleanerRuns() > 0);
        assertTrue(stats.getNLNsMigrated() > 100);

        /*
         * Most importantly, LN migration should not cause the representation
         * of INs to change -- most should still be "no target" (empty).
         * [#21734]
         *
         * The reason that nNoTarget is reduced by one (one is subtracted from
         * nNoTarget below) is apparently because a FileSummaryLN DB BIN has
         * changed.
         */
        final long nNoTarget2 = stats.getNINNoTarget();
        assertTrue("nNoTarget=" + nNoTarget + " nNoTarget2=" + nNoTarget2,
                   nNoTarget2 >= nNoTarget - 1);

        closeEnv();
    }
}
