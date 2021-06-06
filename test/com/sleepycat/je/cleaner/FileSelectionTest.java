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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DbTestProxy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FileSelectionTest extends TestBase {

    private static final String DBNAME = "cleanerFileSelection";
    private static final int DATA_SIZE = 140;
    private static final int FILE_SIZE = 4096 * 10;
    private static final int INITIAL_FILES = 5;
    private static final int INITIAL_FILES_TEMP = 1;
    private static final int INITIAL_KEYS = 2000;
    private static final int INITIAL_KEYS_DUPS = 5000;
    private static final int INITIAL_KEYS_DUPS_TEMP = 10000;
    private static final byte[] MAIN_KEY_FOR_DUPS = {0, 1, 2, 3, 4, 5};

    private static final EnvironmentConfig envConfig = initConfig();
    private static final EnvironmentConfig highUtilizationConfig =
                                                                initConfig();
    private static final EnvironmentConfig steadyStateAutoConfig =
                                                                initConfig();
    private static final EnvironmentConfig noLogFileDeleteDetectConfig =
                                                                initConfig();
    static {
        highUtilizationConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(),
             String.valueOf(90));

        steadyStateAutoConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "true");

        noLogFileDeleteDetectConfig.setConfigParam
            (EnvironmentParams.LOG_DETECT_FILE_DELETE.getName(), "false");
    }

    static EnvironmentConfig initConfig() {
        EnvironmentConfig config = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(config);
        config.setTransactional(true);
        config.setAllowCreate(true);
        config.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        config.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                              Integer.toString(FILE_SIZE));
        config.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                              "false");
        config.setConfigParam(EnvironmentParams.ENV_RUN_CLEANER.getName(),
                              "false");
        config.setConfigParam(EnvironmentParams.CLEANER_REMOVE.getName(),
                              "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        config.setConfigParam
        (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.CLEANER_LOCK_TIMEOUT.getName(), "1");
        config.setConfigParam
            (EnvironmentParams.CLEANER_MAX_BATCH_FILES.getName(), "1");
        return config;
    }

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private File envHome;
    private Environment env;
    private EnvironmentImpl envImpl;
    private Database db;
    private JUnitThread junitThread;
    private volatile int synchronizer;
    private boolean dups;
    private boolean deferredWrite;
    private boolean temporary;

    /* The index is the file number, the value is the first key in the file. */
    private List<Integer> firstKeysInFiles;

    /* Set of keys that should exist. */
    private Set existingKeys;

    @Parameters
    public static List<Object[]> genParams() {
        return Arrays.asList(
            new Object[][] {{false, false}, {true, false}, {false,true}});
    }

    public FileSelectionTest(boolean deferredWrite, boolean temporary) {
        envHome = SharedTestUtils.getTestDir();
        this.deferredWrite = deferredWrite;
        this.temporary = temporary;
        customName = deferredWrite ? ":deferredWrite" :
                    (temporary ? ":temporary" : ":txnl");
    }

    @After
    public void tearDown() {

        if (junitThread != null) {
            junitThread.shutdown();
            junitThread = null;
        }

        try {
            if (env != null) {
                env.close();
            }
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }

        db = null;
        env = null;
        envImpl = null;
        envHome = null;
        existingKeys = null;
        firstKeysInFiles = null;
    }

    private void openEnv()
        throws DatabaseException {

        openEnv(envConfig);
    }

    private void openEnv(EnvironmentConfig config)
        throws DatabaseException {

        env = new Environment(envHome, config);
        envImpl = DbInternal.getNonNullEnvImpl(env);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(!isDeferredWriteMode());
        dbConfig.setDeferredWrite(deferredWrite);
        dbConfig.setTemporary(temporary);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        db = env.openDatabase(null, DBNAME, dbConfig);
    }

    private void closeEnv()
        throws DatabaseException {

        if (temporary) {
            existingKeys.clear();
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    /**
     * Tests that the test utilities work.
     */
    @Test
    public void testBaseline()
        throws DatabaseException {

        int nCleaned;

        openEnv();
        writeData();
        verifyData();
        nCleaned = cleanRoutine();
        if (dups) {
            /*
             * For dup DBs, all LNs are immediately obsolete so we can't expect
             * the same sort of behavior as we do otherwise.  Most files are
             * already obsolete because all LNs are.
             */
            assertTrue(String.valueOf(nCleaned),
                nCleaned >= INITIAL_FILES_TEMP / 2);
        } else {
            /* One file may be cleaned after writing, if a checkpoint occurs. */
            assertTrue(String.valueOf(nCleaned), nCleaned <= 1);
            env.checkpoint(forceConfig);
            nCleaned = cleanRoutine();
            /* One file may be cleaned after cleaning and checkpointing. */
            assertTrue(String.valueOf(nCleaned), nCleaned <= 1);
        }
        closeEnv();
        openEnv();
        verifyData();
        nCleaned = cleanRoutine();
        if (dups) {
            /*
             * For non-temporary DBs, one file may be cleaned due to the
             * checkpoint with no migrated LNs.  For temporary DBs, everything
             * was cleaned in the first phase above.
             */
            assertTrue(String.valueOf(nCleaned), nCleaned <= 1);
        } else if (temporary) {
            /* Temp DBs are automatically deleted and cleaned. */
            assertTrue(String.valueOf(nCleaned),
                       nCleaned >= INITIAL_FILES_TEMP);
        } else {
            /* No files should be cleaned when no writing occurs. */
            assertEquals(0, nCleaned);
        }
        closeEnv();
    }

    @Test
    public void testBaselineDups()
        throws DatabaseException {

        dups = true;
        testBaseline();
    }

    /**
     * Tests that the expected files are selected for cleaning.
     */
    @Test
    public void testBasic()
        throws DatabaseException {

        /* Test assumes that keys are written in order. */
        if (isDeferredWriteMode()) {
            return;
        }

        openEnv();
        writeData();
        verifyDeletedFiles(null);

        /*
         * The first file should be the first to be cleaned because it has
         * relatively few LNs.
         */
        forceCleanOne();
        verifyDeletedFiles(new int[] {0});
        verifyData();

        /*
         * The rest of this test doesn't apply to dup DBs, since the LNs are
         * immediately obsolete and we can't predict which files will contain
         * the BINs for a given key range.
         */
        if (dups) {
            closeEnv();
            return;
        }

        /*
         * Delete most of the LNs in two middle files.  They should be the next
         * two files cleaned.
         */
        int fileNum = INITIAL_FILES / 2;
        int firstKey = firstKeysInFiles.get(fileNum);
        int nextKey = firstKeysInFiles.get(fileNum + 1);
        int count = nextKey - firstKey - 4;
        deleteData(firstKey, count);

        fileNum += 1;
        firstKey = firstKeysInFiles.get(fileNum);
        nextKey = firstKeysInFiles.get(fileNum + 1);
        count = nextKey - firstKey - 4;
        deleteData(firstKey, count);

        forceCleanOne();
        forceCleanOne();
        verifyDeletedFiles(new int[] {0, fileNum - 1, fileNum});
        verifyData();

        closeEnv();
    }

    @Test
    public void testBasicDups()
        throws DatabaseException {

        dups = true;
        testBasic();
    }

    /*
     * testCleaningMode, testTruncateDatabase, and testRemoveDatabase and are
     * not tested with dups=true because with duplicates the total utilization
     * after calling writeData() is 47%, so cleaning will occur and the tests
     * don't expect that.
     */

    /**
     * Tests that routine cleaning does not clean when it should not.
     */
    @Test
    public void testCleaningMode()
        throws DatabaseException {

        int nextFile = -1;
        int nCleaned;

        /*
         * Nothing is cleaned with routine cleaning, even after reopening the
         * environment.
         */
        openEnv();
        writeData();

        nCleaned = cleanRoutine();
        assertEquals(0, nCleaned);
        nextFile = getNextDeletedFile(nextFile);
        assertTrue(nextFile == -1);

        verifyData();
        closeEnv();
        openEnv();
        verifyData();

        nCleaned = cleanRoutine();
        if (temporary) {
            assertTrue(String.valueOf(nCleaned),
                       nCleaned >= INITIAL_FILES_TEMP);
        } else {
            assertEquals(0, nCleaned);
            nextFile = getNextDeletedFile(nextFile);
            assertTrue(nextFile == -1);
        }

        verifyData();

        closeEnv();
    }

    /**
     * Test retries after cleaning fails because an LN was write-locked.
     */
    @Test
    public void testRetry()
        throws DatabaseException {

        /* Test assumes that keys are written in order. */
        if (isDeferredWriteMode()) {
            return;
        }

        /*
         * This test doesn't apply to dup DBs, since the LNs are immediately
         * obsolete and locking the record has no impact on cleaning.
         */
        if (dups) {
            return;
        }

        openEnv(highUtilizationConfig);
        writeData();
        verifyData();

        /*
         * The first file is full of LNs.  Delete all but the last record to
         * cause it to be selected next for cleaning.
         */
        int firstKey = firstKeysInFiles.get(1);
        int nextKey = firstKeysInFiles.get(2);
        int count = nextKey - firstKey - 1;
        deleteData(firstKey, count);
        verifyData();

        /* Write-lock the last record to cause cleaning to fail. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;
        if (dups) {
            key.setData(MAIN_KEY_FOR_DUPS);
            data.setData(TestUtils.getTestArray(nextKey - 1));
            status = cursor.getSearchBoth(key, data, LockMode.RMW);
        } else {
            key.setData(TestUtils.getTestArray(nextKey - 1));
            status = cursor.getSearchKey(key, data, LockMode.RMW);
        }
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);

        /* Cleaning should fail. */
        forceCleanOne();
        verifyDeletedFiles(null);
        forceCleanOne();
        verifyDeletedFiles(null);

        /* Release the write-lock. */
        cursor.close();
        txn.abort();
        verifyData();

        /* Cleaning should succeed, with file 1 (possibly more) deleted. */
        forceCleanOne();
        assertEquals(1, getNextDeletedFile(0));
        verifyData();

        closeEnv();
    }

    /**
     * Tests that the je.cleaner.minFileUtilization property works as expected.
     */
    @Test
    public void testMinFileUtilization()
        throws DatabaseException {

        /* Test assumes that keys are written in order. */
        if (isDeferredWriteMode()) {
            return;
        }

        /* Open with minUtilization=10 and minFileUtilization=0. */
        EnvironmentConfig myConfig = initConfig();
        myConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(),
             String.valueOf(10));
        myConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION.getName(),
             String.valueOf(0));
        openEnv(myConfig);

        /* Write data and delete two thirds of the LNs in the middle file. */
        writeData();
        verifyDeletedFiles(null);
        int fileNum = INITIAL_FILES / 2;
        int firstKey = firstKeysInFiles.get(fileNum);
        int nextKey = firstKeysInFiles.get(fileNum + 1);
        int count = ((nextKey - firstKey) * 2) / 3;
        deleteData(firstKey, count);

        /* The file should not be deleted. */
        env.cleanLog();
        env.checkpoint(forceConfig);
        verifyDeletedFiles(null);

        /* Change minFileUtilization=50 */
        myConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION.getName(),
             String.valueOf(50));
        env.setMutableConfig(myConfig);

        /* The file should now be deleted. */
        env.cleanLog();
        env.checkpoint(forceConfig);
        verifyDeletedFiles(new int[] {fileNum});
        verifyData();

        closeEnv();
    }

    private void printFiles(String msg) {
        System.out.print(msg);
        Long lastNum = envImpl.getFileManager().getLastFileNum();
        for (int i = 0; i <= (int) lastNum.longValue(); i += 1) {
            String name = envImpl.getFileManager().
                getFullFileName(i, FileManager.JE_SUFFIX);
            if (new File(name).exists()) {
                System.out.print(" " + i);
            }
        }
        System.out.println("");
    }

    @Test
    public void testRetryDups()
        throws DatabaseException {

        dups = true;
        testRetry();
    }

    /**
     * Steady state should occur with normal (50% utilization) configuration
     * and automatic checkpointing and cleaning.
     */
    @Test
    public void testSteadyStateAutomatic()
        throws DatabaseException {

        doSteadyState(steadyStateAutoConfig, false, 13);
    }

    @Test
    public void testSteadyStateAutomaticDups()
        throws DatabaseException {

        dups = true;
        testSteadyStateAutomatic();
    }

    /**
     * Steady state utilization with manual checkpointing and cleaning.
     */
    @Test
    public void testSteadyStateManual()
        throws DatabaseException {

        doSteadyState(envConfig, true,
            (deferredWrite | temporary) ? 20 : 13);
    }

    @Test
    public void testSteadyStateManualDups()
        throws DatabaseException {

        dups = true;
        testSteadyStateManual();
    }

    /**
     * Steady state should occur when utilization is at the maximum.
     */
    @Test
    public void testSteadyStateHighUtilization()
        throws DatabaseException {

        doSteadyState(highUtilizationConfig, true,
                      (deferredWrite | temporary) ? 12 : 9);
    }

    @Test
    public void testSteadyStateHighUtilizationDups()
        throws DatabaseException {

        dups = true;
        testSteadyStateHighUtilization();
    }

    /**
     * Tests that we quickly reach a steady state of disk usage when updates
     * are made but no net increase in data occurs.
     *
     * @param manualCleaning is whether to run cleaning manually every
     * iteration, or to rely on the cleaner thread.
     *
     * @param maxFileCount the maximum number of files allowed for this test.
     */
    private void doSteadyState(EnvironmentConfig config,
                               boolean manualCleaning,
                               int maxFileCount)
        throws DatabaseException {

        openEnv(config);
        writeData();
        verifyData();

        final int iterations = 100;

        for (int i = 0; i < iterations; i += 1) {
            /* updateData flushes temp and deferredWrite DBs. */
            updateData(100, 100);
            int cleaned = -1;
            if (manualCleaning) {
                cleaned = cleanRoutine();
            } else {
                /* Need to delay a bit for the cleaner to keep up. */
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {}
            }

            /*
             * Checkpoints need to occur often for the cleaner to keep up.
             * and to delete files that were cleaned.
             *
             * Note that temp DBs are not checkpointed and we rely on eviction
             * to flush obsolete information and cause cleaning.  [#16928]
             */
            env.checkpoint(forceConfig);
            verifyData();
            int fileCount =
                envImpl.getFileManager().getAllFileNumbers().length;
            assertTrue(customName +
                       " fileCount=" + fileCount +
                       " maxFileCount=" + maxFileCount +
                       " iteration=" + i,
                       fileCount <= maxFileCount);
            if (false) {
                System.out.println("fileCount=" + fileCount +
                                   " cleaned=" + cleaned);
            }
        }
        closeEnv();
    }

    /**
     * Tests basic file protection.
     */
    @Test
    public void testProtectedFileRange()
        throws DatabaseException {

        /* Test assumes that keys are written in order. */
        if (isDeferredWriteMode()) {
            return;
        }

        openEnv();
        writeData();
        verifyDeletedFiles(null);

        /* Delete all records so that all INITIAL_FILES will be cleaned. */
        int lastFile = firstKeysInFiles.size() - 1;
        int lastKey = firstKeysInFiles.get(lastFile);
        deleteData(0, lastKey);

        /* Protect 4 ranges: {0-N}, {1-N}, {2-N}, {2-N}. */
        FileProtector fileProtector = envImpl.getFileProtector();
        FileProtector.ProtectedFileSet pfs0 =
            fileProtector.protectFileRange("test-0", 0);
        FileProtector.ProtectedFileSet pfs1 =
            fileProtector.protectFileRange("test-1", 1);
        FileProtector.ProtectedFileSet pfs2a =
            fileProtector.protectFileRange("test-2a", 2);
        FileProtector.ProtectedFileSet pfs2b =
            fileProtector.protectFileRange("test-2b", 2);

        /* No files should be deleted. */
        forceClean(lastFile);
        verifyDeletedFiles(null);
        verifyData();
        assertEquals(lastFile, fileProtector.getNReservedFiles());

        /*
         * Removing {1-N} will not cause any deletions, because {0-N} is still
         * in effect.
         */
        fileProtector.removeFileProtection(pfs1);
        forceClean(lastFile);
        verifyDeletedFiles(null);
        verifyData();
        assertEquals(lastFile, fileProtector.getNReservedFiles());

        /* Removing {0-N} will cause 0 and 1 to be deleted. */
        fileProtector.removeFileProtection(pfs0);
        forceClean(lastFile);
        verifyDeletedFiles(new int[] {0, 1});
        verifyData();
        assertEquals(lastFile - 2, fileProtector.getNReservedFiles());

        /*
         * Removing {2-N} will not cause more deletions because another {2-N}
         * range is still in effect.
         */
        fileProtector.removeFileProtection(pfs2a);
        forceClean(lastFile);
        verifyDeletedFiles(new int[] {0, 1});
        verifyData();
        assertEquals(lastFile - 2, fileProtector.getNReservedFiles());

        /* Removing the 2nd {2-N} range causes all files to be deleted. */
        fileProtector.removeFileProtection(pfs2b);
        forceClean(lastFile);
        int[] allFiles = new int[lastFile];
        for (int i = 0; i < lastFile; i += 1) {
            allFiles[i] = i;
        }
        verifyDeletedFiles(allFiles);
        verifyData();
        assertEquals(0, fileProtector.getNReservedFiles());

        closeEnv();
    }

    /**
     * Tests that truncate causes cleaning.
     */
    @Test
    public void testTruncateDatabase()
        throws DatabaseException {

        int nCleaned;

        openEnv();
        writeData();

        nCleaned = cleanRoutine();
        assertEquals(0, nCleaned);
        db.close();
        db = null;

        /*
         * Temporary databases are removed when the database is closed, so
         * don't call truncate explicitly.
         */
        if (!temporary) {
            env.truncateDatabase(null, DBNAME, false /* returnCount */);
        }

        nCleaned = cleanRoutine();
        if (temporary) {
            assertTrue(String.valueOf(nCleaned),
                       nCleaned >= INITIAL_FILES_TEMP - 1);
        } else {
            assertTrue(String.valueOf(nCleaned),
                       nCleaned >= INITIAL_FILES - 1);
        }

        closeEnv();
    }

    /**
     * Tests that remove causes cleaning.
     */
    @Test
    public void testRemoveDatabase()
        throws DatabaseException {

        int nCleaned;

        openEnv();
        writeData();

        String dbName = db.getDatabaseName();
        db.close();
        db = null;

        nCleaned = cleanRoutine();
        if (temporary) {
            assertTrue(String.valueOf(nCleaned),
                       nCleaned >= INITIAL_FILES_TEMP - 1);
            assertTrue(!env.getDatabaseNames().contains(dbName));
        } else {
            assertEquals(0, nCleaned);

            env.removeDatabase(null, dbName);
            nCleaned = cleanRoutine();
            assertTrue(String.valueOf(nCleaned),
                       nCleaned >= INITIAL_FILES - 1);
        }

        closeEnv();
    }

    @Test
    public void testForceCleanFiles()
        throws DatabaseException {

        /* When the temp DB is closed many files will be cleaned. */
        if (temporary) {
            return;
        }

        /* No files force cleaned. */
        EnvironmentConfig myConfig = initConfig();
        openEnv(myConfig);
        writeData();
        verifyData();
        env.cleanLog();
        env.checkpoint(forceConfig);
        verifyDeletedFiles(null);

        EnvironmentMutableConfig mutableConfig = env.getMutableConfig();

        /* Force cleaning: 3 */
        mutableConfig.setConfigParam(
            EnvironmentConfig.CLEANER_FORCE_CLEAN_FILES, "3");
        env.setMutableConfig(mutableConfig);
        forceCleanOne();
        verifyDeletedFiles(new int[] {3});

        /* Force cleaning: 0 - 1 */
        mutableConfig.setConfigParam(
            EnvironmentConfig.CLEANER_FORCE_CLEAN_FILES, "0-1");
        env.setMutableConfig(mutableConfig);
        forceCleanOne();
        forceCleanOne();
        verifyDeletedFiles(new int[] {0, 1, 3});

        /*
         * Clean file 2 using public cleanLog method -- forcing cleaning with
         * an internal API should not be necessary. File is not deleted,
         * however, because we don't do a checkpoint.
         */
        mutableConfig.setConfigParam(
            EnvironmentConfig.CLEANER_FORCE_CLEAN_FILES, "2");
        env.setMutableConfig(mutableConfig);
        int files = env.cleanLog();
        assertEquals(1, files);
        verifyDeletedFiles(new int[] {0, 1, 3});

        /*
         * Try cleaning 2 again, should not get exception.
         * Before a bug fix [#26326], an assertion fired in FileSelector.
         */
        mutableConfig.setConfigParam(
            EnvironmentConfig.CLEANER_FORCE_CLEAN_FILES, "2");
        env.setMutableConfig(mutableConfig);
        files = env.cleanLog();
        assertEquals(0, files);
        verifyDeletedFiles(new int[] {0, 1, 3});

        /* Finally perform a checkpoint and file 2 should be deleted. */
        env.checkpoint(forceConfig);
        verifyDeletedFiles(new int[] {0, 1, 2, 3});

        closeEnv();
    }

    /**
     * Checks that old version log files are upgraded when
     * je.cleaner.upgradeToLogVersion is set.  The version 5 log files to be
     * upgraded in this test were created with MakeMigrationLogFiles.
     */
    @Test
    public void testLogVersionUpgrade()
        throws DatabaseException, IOException {

        if (temporary) {
            /* This test is not applicable. */
            return;
        }

        /* Copy pre-created files 0 and 1, which are log verion 5. */
        TestUtils.loadLog
            (getClass(), "migrate_f0.jdb", envHome, "00000000.jdb");
        TestUtils.loadLog
            (getClass(), "migrate_f1.jdb", envHome, "00000001.jdb");

        /*
         * Write several more files which are log version 6 or greater.  To
         * check whether these files are cleaned below we need to write more
         * than 2 files (2 is the minimum age for cleaning).
         */
        env = MakeMigrationLogFiles.openEnv(envHome, false /*allowCreate*/);
        MakeMigrationLogFiles.makeMigrationLogFiles(env);
        env.checkpoint(forceConfig);
        MakeMigrationLogFiles.makeMigrationLogFiles(env);
        env.checkpoint(forceConfig);
        closeEnv();

        /* With upgradeToLogVersion=0 no files should be cleaned. */
        openEnvWithUpgradeToLogVersion(0);
        int nFiles = env.cleanLog();
        assertEquals(0, nFiles);
        closeEnv();

        /* With upgradeToLogVersion=5 no files should be cleaned. */
        openEnvWithUpgradeToLogVersion(5);
        nFiles = env.cleanLog();
        assertEquals(0, nFiles);
        closeEnv();

        /* Upgrade log files to the current version, which is 6 or greater. */
        openEnvWithUpgradeToLogVersion(-1); // -1 means current version

        /*
         * Clean one log file at a time so we can check that the backlog is
         * not impacted by log file migration.
         */
        for (int i = 0; i < 2; i += 1) {
            boolean cleaned = env.cleanLogFile();
            assertTrue(cleaned);
            EnvironmentStats stats = env.getStats(null);
            assertEquals(0, stats.getCleanerBacklog());
        }
        env.checkpoint(forceConfig);
        verifyDeletedFiles(new int[] {0, 1});

        /* No more files should be cleaned. */
        nFiles = env.cleanLog();
        assertEquals(0, nFiles);
        closeEnv();

        /*
         * Force clean file 2 to ensure that it was not cleaned above because
         * of its log version and not some other factor.
         */
        EnvironmentConfig myConfig = initConfig();
        myConfig.setConfigParam
            (EnvironmentParams.CLEANER_FORCE_CLEAN_FILES.getName(), "2");
        openEnv(myConfig);
        nFiles = env.cleanLog();
        assertEquals(1, nFiles);
        env.checkpoint(forceConfig);
        verifyDeletedFiles(new int[] {0, 1, 2});

        closeEnv();
    }

    private void openEnvWithUpgradeToLogVersion(int upgradeToLogVersion)
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_UPGRADE_TO_LOG_VERSION.getName(),
             String.valueOf(upgradeToLogVersion));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
        (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);
    }

    /**
     * Tests that when cleaned files are deleted during a compression, the
     * flushing of the local tracker does not transfer tracker information
     * for the deleted files. [#15528]
     *
     * This test also checks that tracker information is not transfered to the
     * MapLN's per-DB utilization information in DbFileSummaryMap.  This was
     * occuring in JE 3.3.74 and earlier, under the same circumstances as
     * tested here (IN compression).  [#16610]
     */
    @Test
    public void testCompressionBug()
        throws DatabaseException {

        /*
         * We need to compress deleted keys and count their utilization under
         * an explicit compress() call.  With deferred write, no utilization
         * counting occurs until eviction/sync, and that would also do
         * compression.
         */
        if (isDeferredWriteMode()) {
            return;
        }

        EnvironmentConfig envConfig = initConfig();
        /* Disable compressor so we can compress explicitly. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        /* Ensure that we check for resurrected file leaks. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_CHECK_LEAKS.getName(), "true");
        openEnv(envConfig);

        /* Write and then delete all data. */
        writeData();
        for (Iterator i = existingKeys.iterator(); i.hasNext();) {
            int nextKey = ((Integer) i.next()).intValue();
            DatabaseEntry key =
                new DatabaseEntry(TestUtils.getTestArray(nextKey));
            OperationStatus status = db.delete(null, key);
            assertSame(OperationStatus.SUCCESS, status);
        }

        synchronizer = 0;

        /* Create thread that will do the compression. */
        junitThread = new JUnitThread("TestCompress") {
            @Override
            public void testBody() {
                try {
                    /* compress() will call the test hook below. */
                    env.compress();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        };

        /*
         * Set a hook that is called by the INCompressor before it calls
         * UtilizationProfile.flushLocalTracker.
         */
        envImpl.getINCompressor().setBeforeFlushTrackerHook(new TestHook() {
            public void doHook() {
                synchronizer = 1;
                /* Wait for log cleaning to complete. */
                while (synchronizer < 2 && !Thread.interrupted()) {
                    Thread.yield();
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

        /* Kick off test in thread above. */
        junitThread.start();
        /* Wait for hook to be called at the end of compression. */
        while (synchronizer < 1) Thread.yield();
        /* Clean and checkpoint to delete cleaned files. */
        while (env.cleanLog() > 0) { }
        env.checkpoint(forceConfig);
        /* Allow test hook to return, so that flushLocalTracker is called. */
        synchronizer = 2;

        /*
         * Before the fix [#15528], an assertion fired in
         * BaseUtilizationTracker.getFileSummary when flushLocalTracker was
         * called.  This assertion fires if the file being tracked does not
         * exist.  The fix was to check for valid files in flushLocalTracker.
         */
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
     * Checks that DB utilization is repaired when damaged by JE 3.3.74 or
     * earlier. Go to some trouble to create a DatabaseImpl with the repair
     * done flag not set, and with a DB file summary for a deleted file.
     * [#16610]
     */
    @Test
    public void testDbUtilizationRepair()
        throws DatabaseException, IOException {

        openEnv(noLogFileDeleteDetectConfig);
        writeData();
        forceCleanOne();
        verifyDeletedFiles(new int[] {0});

        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        DbFileSummaryMap summaries = dbImpl.getDbFileSummaries();

        /* New version DB does not need repair. */
        assertTrue(dbImpl.getUtilizationRepairDone());

        /* Deleted file is absent from summary map. */
        assertNull(summaries.get(0L /*fileNum*/, true /*adjustMemBudget*/,
                                 true /*checkResurrected*/,
                                 envImpl.getFileManager()));

        /*
         * Force addition of deleted file to summary map by creating a dummy
         * file to prevent assertions from firing.
         */
        File dummyFile = new File(env.getHome(), "00000000.jdb");
        assertTrue(dummyFile.createNewFile());
        assertNotNull(summaries.get(0L /*fileNum*/, true /*adjustMemBudget*/,
                                    false /*checkResurrected*/,
                                    envImpl.getFileManager()));
        assertTrue(dummyFile.delete());

        /* Now an entry in the summary map is there for a deleted file.. */
        assertNotNull(summaries.get(0L /*fileNum*/, true /*adjustMemBudget*/,
                                    true /*checkResurrected*/,
                                    envImpl.getFileManager()));

        /* Force the MapLN with the bad entry to be flushed. */
        dbImpl.setDirty();
        env.checkpoint(forceConfig);
        closeEnv();

        /* If the DB is temporary, we can't test it further. */
        if (temporary) {
            return;
        }

        /*
         * When the DB is opened, the repair should not take place, because we
         * did not clear the repair done flag above.
         */
        openEnv();
        dbImpl = DbInternal.getDbImpl(db);
        summaries = dbImpl.getDbFileSummaries();
        assertTrue(dbImpl.getUtilizationRepairDone());
        assertNotNull(summaries.get(0L /*fileNum*/, true /*adjustMemBudget*/,
                                    true /*checkResurrected*/,
                                    envImpl.getFileManager()));

        /* Clear the repair done flag and force the MapLN to be flushed. */
        dbImpl.clearUtilizationRepairDone();
        dbImpl.setDirty();
        env.checkpoint(forceConfig);
        closeEnv();

        /*
         * Since the repair done flag was cleared above, when the DB is opened,
         * the repair should take place.
         */
        openEnv();
        dbImpl = DbInternal.getDbImpl(db);
        summaries = dbImpl.getDbFileSummaries();
        assertTrue(dbImpl.getUtilizationRepairDone());
        assertNull(summaries.get(0L /*fileNum*/, true /*adjustMemBudget*/,
                                 true /*checkResurrected*/,
                                 envImpl.getFileManager()));
        closeEnv();
    }

    /**
     * Force cleaning of N files.
     */
    private void forceClean(int nFiles)
        throws DatabaseException {

        for (int i = 0; i < nFiles; i += 1) {
            envImpl.getCleaner().doClean(false, // cleanMultipleFiles
                                         true); // forceCleaning
        }
        /* To force file deletion a checkpoint is necessary. */
        env.checkpoint(forceConfig);
    }

    /**
     * Force cleaning of one file.
     */
    private void forceCleanOne()
        throws DatabaseException {

        envImpl.getCleaner().doClean(false, // cleanMultipleFiles
                                     true); // forceCleaning
        /* To force file deletion a checkpoint is necessary. */
        env.checkpoint(forceConfig);
    }

    /**
     * Do routine cleaning just as normally done via the cleaner daemon, and
     * return the number of files cleaned.
     */
    private int cleanRoutine()
        throws DatabaseException {

        return env.cleanLog();
    }

    /**
     * Use transactions when not testing deferred write or temporary DBs.
     */
    private boolean isDeferredWriteMode() {
        return deferredWrite || temporary;
    }

    /**
     * Forces eviction when a temporary database is used, since otherwise data
     * will not be flushed.
     */
    private void forceEvictionIfTemporary()
        throws DatabaseException {

        if (temporary) {
            EnvironmentMutableConfig config = env.getMutableConfig();
            long saveSize = config.getCacheSize();
            config.setCacheSize(MemoryBudget.MIN_MAX_MEMORY_SIZE * 2);
            env.setMutableConfig(config);
            env.evictMemory();
            config.setCacheSize(saveSize);
            env.setMutableConfig(config);
        }
    }

    /**
     * Writes data to create INITIAL_FILES number of files, storing the first
     * key for each file in the firstKeysInFiles list.  One extra file is
     * actually created, to ensure that the firstActiveLSN is not in any of
     * INITIAL_FILES files.
     */
    private void writeData()
        throws DatabaseException {

        int firstFile =
            (int) envImpl.getFileManager().getLastFileNum().longValue();
        assertEquals(0, firstFile);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[DATA_SIZE]);
        existingKeys = new HashSet();

        if (isDeferredWriteMode()) {
            firstKeysInFiles = null;

            Cursor cursor = db.openCursor(null, null);

            int maxKey = dups ?
                (temporary ? INITIAL_KEYS_DUPS_TEMP : INITIAL_KEYS_DUPS) :
                INITIAL_KEYS;
            for (int nextKey = 0; nextKey < maxKey; nextKey += 1) {

                OperationStatus status;
                if (dups) {
                    key.setData(MAIN_KEY_FOR_DUPS);
                    data.setData(TestUtils.getTestArray(nextKey));
                    status = cursor.putNoDupData(key, data);
                } else {
                    key.setData(TestUtils.getTestArray(nextKey));
                    data.setData(new byte[DATA_SIZE]);
                    status = cursor.putNoOverwrite(key, data);
                }

                assertEquals(OperationStatus.SUCCESS, status);
                existingKeys.add(new Integer(nextKey));
            }

            cursor.close();
        } else {
            firstKeysInFiles = new ArrayList<Integer>();

            Transaction txn = env.beginTransaction(null, null);
            Cursor cursor = db.openCursor(txn, null);
            int fileNum = -1;

            for (int nextKey = 0; fileNum < INITIAL_FILES; nextKey += 1) {

                OperationStatus status;
                if (dups) {
                    key.setData(MAIN_KEY_FOR_DUPS);
                    data.setData(TestUtils.getTestArray(nextKey));
                    status = cursor.putNoDupData(key, data);
                } else {
                    key.setData(TestUtils.getTestArray(nextKey));
                    data.setData(new byte[DATA_SIZE]);
                    status = cursor.putNoOverwrite(key, data);
                }

                assertEquals(OperationStatus.SUCCESS, status);
                existingKeys.add(new Integer(nextKey));

                long lsn = getLsn(cursor);
                if (DbLsn.getFileNumber(lsn) != fileNum) {
                    assertTrue(fileNum < DbLsn.getFileNumber(lsn));
                    fileNum = (int) DbLsn.getFileNumber(lsn);
                    assertEquals(fileNum, firstKeysInFiles.size());
                    firstKeysInFiles.add(nextKey);
                }
            }
            //System.out.println("Num keys: " + existingKeys.size());

            cursor.close();
            txn.commit();
        }

        forceEvictionIfTemporary();
        env.checkpoint(forceConfig);

        int lastFile =
            (int) envImpl.getFileManager().getLastFileNum().longValue();
        if (temporary) {
            assertTrue(String.valueOf(lastFile),
                       lastFile >= INITIAL_FILES_TEMP);
        } else {
            assertTrue(String.valueOf(lastFile),
                       lastFile >= INITIAL_FILES);
        }
        //System.out.println("last file " + lastFile);
    }

    /**
     * Deletes the specified keys.
     */
    private void deleteData(int firstKey, int keyCount)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Transaction txn = !isDeferredWriteMode() ?
            env.beginTransaction(null, null) : null;
        Cursor cursor = db.openCursor(txn, null);

        for (int i = 0; i < keyCount; i += 1) {
            int nextKey = firstKey + i;
            OperationStatus status;
            if (dups) {
                key.setData(MAIN_KEY_FOR_DUPS);
                data.setData(TestUtils.getTestArray(nextKey));
                status = cursor.getSearchBoth(key, data, null);
            } else {
                key.setData(TestUtils.getTestArray(nextKey));
                status = cursor.getSearchKey(key, data, null);
            }
            assertEquals(OperationStatus.SUCCESS, status);
            status = cursor.delete();
            assertEquals(OperationStatus.SUCCESS, status);
            existingKeys.remove(new Integer(nextKey));
        }

        cursor.close();
        if (txn != null) {
            txn.commit();
        }
        forceEvictionIfTemporary();
    }

    /**
     * Updates the specified keys.
     */
    private void updateData(int firstKey, int keyCount)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Transaction txn = !isDeferredWriteMode() ?
            env.beginTransaction(null, null) : null;
        Cursor cursor = db.openCursor(txn, null);

        for (int i = 0; i < keyCount; i += 1) {
            int nextKey = firstKey + i;
            OperationStatus status;
            if (dups) {
                key.setData(MAIN_KEY_FOR_DUPS);
                data.setData(TestUtils.getTestArray(nextKey));
                status = cursor.getSearchBoth(key, data, null);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(MAIN_KEY_FOR_DUPS.length, key.getSize());
                assertEquals(nextKey, TestUtils.getTestVal(data.getData()));
            } else {
                key.setData(TestUtils.getTestArray(nextKey));
                status = cursor.getSearchKey(key, data, null);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(nextKey, TestUtils.getTestVal(key.getData()));
                assertEquals(DATA_SIZE, data.getSize());
            }
            status = cursor.putCurrent(data);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        cursor.close();
        if (txn != null) {
            txn.commit();
        }

        /*
         * For deferred write and temp DBs, flush them to produce a situation
         * comparable to other modes.
         */
        forceEvictionIfTemporary();
        if (deferredWrite) {
            db.sync();
        }
    }

    /**
     * Verifies that the data written by writeData can be read.
     */
    private void verifyData()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Transaction txn = !isDeferredWriteMode() ?
            env.beginTransaction(null, null) : null;
        Cursor cursor = db.openCursor(txn, null);

        for (Iterator i = existingKeys.iterator(); i.hasNext();) {
            int nextKey = ((Integer) i.next()).intValue();
            OperationStatus status;
            if (dups) {
                key.setData(MAIN_KEY_FOR_DUPS);
                data.setData(TestUtils.getTestArray(nextKey));
                status = cursor.getSearchBoth(key, data, null);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(MAIN_KEY_FOR_DUPS.length, key.getSize());
                assertEquals(nextKey, TestUtils.getTestVal(data.getData()));
            } else {
                key.setData(TestUtils.getTestArray(nextKey));
                status = cursor.getSearchKey(key, data, null);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(nextKey, TestUtils.getTestVal(key.getData()));
                assertEquals(DATA_SIZE, data.getSize());
            }
        }

        cursor.close();
        if (txn != null) {
            txn.commit();
        }
    }

    /**
     * Checks that all log files exist except those specified.
     */
    private void verifyDeletedFiles(int[] shouldNotExist) {
        Long lastNum = envImpl.getFileManager().getLastFileNum();
        for (int i = 0; i <= (int) lastNum.longValue(); i += 1) {
            boolean shouldExist = true;
            if (shouldNotExist != null) {
                for (int j = 0; j < shouldNotExist.length; j += 1) {
                    if (i == shouldNotExist[j]) {
                        shouldExist = false;
                        break;
                    }
                }
            }
            String name = envImpl.getFileManager().
                getFullFileName(i, FileManager.JE_SUFFIX);
            if (shouldExist != new File(name).exists()) {
                fail("file=" + i + " shouldExist=" + shouldExist +
                    " filesThatShouldNotExist=" +
                    Arrays.toString(shouldNotExist) +
                    " filesThatDoExist=" + Arrays.toString(
                    envImpl.getFileManager().getAllFileNumbers()));
            }
        }
    }

    /**
     * Returns the first deleted file number or -1 if none.
     */
    private int getNextDeletedFile(int afterFile) {
        Long lastNum = envImpl.getFileManager().getLastFileNum();
        for (int i = afterFile + 1; i <= (int) lastNum.longValue(); i += 1) {
            String name = envImpl.getFileManager().
                getFullFileName(i, FileManager.JE_SUFFIX);
            if (!(new File(name).exists())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Gets the LSN at the cursor position, using internal methods.
     */
    private long getLsn(Cursor cursor) {
        CursorImpl impl = DbTestProxy.dbcGetCursorImpl(cursor);
        BIN bin = impl.getBIN();
        int index = impl.getIndex();
        assertNotNull(bin);
        assertTrue(index >= 0);
        long lsn = bin.getLsn(index);
        assertTrue(lsn != DbLsn.NULL_LSN);
        return lsn;
    }
}
