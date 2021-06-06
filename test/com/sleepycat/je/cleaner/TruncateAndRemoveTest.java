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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.log.DumpFileReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test cleaning and utilization counting for database truncate and remove.
 */
@RunWith(Parameterized.class)
public class TruncateAndRemoveTest extends CleanerTestBase {

    private static final String DB_NAME1 = "foo";
    private static final String DB_NAME2 = "bar";
    private static final int RECORD_COUNT = 100;

    private static final CheckpointConfig FORCE_CHECKPOINT =
        new CheckpointConfig();
    static {
        FORCE_CHECKPOINT.setForce(true);
    }

    private static final boolean DEBUG = false;

    private EnvironmentImpl envImpl;
    private Database db;
    private DatabaseImpl dbImpl;
    private JUnitThread junitThread;
    private boolean fetchObsoleteSize;
    private boolean truncateOrRemoveDone;
    private boolean dbEviction;

    private boolean embeddedLNs = false;

    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }
    
    public TruncateAndRemoveTest (boolean envMultiDir) {
        envMultiSubDir = envMultiDir;
        customName = (envMultiSubDir) ? "multi-sub-dir" : null;
    }

    @After
    public void tearDown() 
        throws Exception {

        if (junitThread != null) {
            junitThread.shutdown();
            junitThread = null;
        }
        super.tearDown();
        db = null;
        dbImpl = null;
        envImpl = null;
    }

    /**
     * Opens the environment.
     */
    private void openEnv(boolean transactional)
        throws DatabaseException {

        EnvironmentConfig config = TestUtils.initEnvConfig();
        config.setTransactional(transactional);
        config.setAllowCreate(true);
        /* Do not run the daemons since they interfere with LN counting. */
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        config.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        /* Use small nodes to test the post-txn scanning. */
        config.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), "10");
        config.setConfigParam
            (EnvironmentParams.NODE_MAX_DUPTREE.getName(), "10");
        if (envMultiSubDir) {
            config.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES, 
                                  DATA_DIRS + "");
        }

        /* Use small files to ensure that there is cleaning. */
        config.setConfigParam("je.cleaner.minUtilization", "80");
        DbInternal.disableParameterValidation(config);
        config.setConfigParam("je.log.fileMax", "4000");
        /* With tiny files we can't log expiration profile records. */
        DbInternal.setCreateEP(config, false);

        /* Obsolete LN size counting is optional per test. */
        if (fetchObsoleteSize) {
            config.setConfigParam
                (EnvironmentParams.CLEANER_FETCH_OBSOLETE_SIZE.getName(),
                 "true");
        }

        env = new Environment(envHome, config);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        config = env.getConfig();
        dbEviction = config.getConfigParam
            (EnvironmentParams.ENV_DB_EVICTION.getName()).equals("true");
    }

    /**
     * Opens that database.
     */
    private void openDb(Transaction useTxn, String dbName)
        throws DatabaseException {

        openDb(useTxn, dbName, null /*cacheMode*/);
    }

    private void openDb(Transaction useTxn, String dbName, CacheMode cacheMode)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig envConfig = env.getConfig();
        dbConfig.setTransactional(envConfig.getTransactional());
        dbConfig.setAllowCreate(true);
        dbConfig.setCacheMode(cacheMode);
        db = env.openDatabase(useTxn, dbName, dbConfig);
        dbImpl = DbInternal.getDbImpl(db);
    }

    /**
     * Closes the database.
     */
    private void closeDb()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
            dbImpl = null;
        }
    }

    /**
     * Closes the environment and database.
     */
    private void closeEnv()
        throws DatabaseException {

        closeDb();

        if (env != null) {
            env.close();
            env = null;
            envImpl = null;
        }
    }

    @Test
    public void testTruncate()
        throws Exception {

        doTestTruncate(false /*simulateCrash*/);
    }

    @Test
    public void testTruncateRecover()
        throws Exception {

        doTestTruncate(true /*simulateCrash*/);
    }

    /**
     * Test that truncate generates the right number of obsolete LNs.
     */
    private void doTestTruncate(boolean simulateCrash)
        throws Exception {

        openEnv(true);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);
        DatabaseImpl saveDb = dbImpl;
        DatabaseId saveId = dbImpl.getId();
        closeDb();

        Transaction txn = env.beginTransaction(null, null);

        truncate(txn, true);

        ObsoleteCounts beforeCommit = getObsoleteCounts();

       /*
        * The commit of the truncation creates 3 additional obsolete logrecs:
        * prev MapLN + deleted MapLN + prev NameLN
        */
        txn.commit();
        truncateOrRemoveDone = true;

        /* Make sure use count is decremented when we commit. */
        assertDbInUse(saveDb, false);
        openDb(null, DB_NAME1);
        saveDb = dbImpl;
        closeDb();
        assertDbInUse(saveDb, false);

        if (simulateCrash) {
            envImpl.abnormalClose();
            envImpl = null;
            env = null;
            openEnv(true);
            /* After recovery, expect that the record LNs are obsolete. */
            ObsoleteCounts afterCrash = getObsoleteCounts();

            int obsolete = afterCrash.obsoleteLNs - beforeCommit.obsoleteLNs;

            if (embeddedLNs) {
                assertEquals(3, obsolete);
            } else {
                assertTrue("obsolete=" + obsolete + " expected=" + RECORD_COUNT,
                           obsolete >= RECORD_COUNT);
            }
        } else {
            int expectedObsLNs = (embeddedLNs ? 3 : RECORD_COUNT + 3);
            verifyUtilization(beforeCommit,
                              expectedObsLNs,
                              15); // 1 root, 2 INs, 12 BINs
        }

        closeEnv();
        batchCleanAndVerify(saveId);
    }

    /**
     * Test that aborting truncate generates the right number of obsolete LNs.
     */
    @Test
    public void testTruncateAbort()
        throws Exception {

        openEnv(true);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);
        DatabaseImpl saveDb = dbImpl;
        closeDb();

        Transaction txn = env.beginTransaction(null, null);
        truncate(txn, true);
        ObsoleteCounts beforeAbort = getObsoleteCounts();
        txn.abort();

        /* Make sure use count is decremented when we abort. */
        assertDbInUse(saveDb, false);
        openDb(null, DB_NAME1);
        saveDb = dbImpl;
        closeDb();
        assertDbInUse(saveDb, false);

        /*
         * The obsolete count should include the records inserted after
         * the truncate.
         */
        verifyUtilization(beforeAbort,
                          /* 1 new nameLN, 2 copies of MapLN for new db */
                           3,
                           0);

        /* Reopen, db should be populated. */
        openDb(null, DB_NAME1);
        assertEquals(RECORD_COUNT, countRecords(null));
        closeEnv();
    }

    /**
     * Test that aborting truncate generates the right number of obsolete LNs.
     */
    @Test
    public void testTruncateRepopulateAbort()
        throws Exception {

        openEnv(true);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);

        closeDb();

        Transaction txn = env.beginTransaction(null, null);

        /*
         * Truncation creates 2 additional logrecs: new MapLN and nameLN logrecs
         * for the new db.
         */
        truncate(txn, true);

        /* populate the database with some more records. */
        openDb(txn, DB_NAME1);

        writeAndCountRecords(txn, RECORD_COUNT/4);

        DatabaseImpl saveDb = dbImpl;
        DatabaseId saveId = dbImpl.getId();
        closeDb();

        ObsoleteCounts beforeAbort = getObsoleteCounts();

       /*
         * The abort generates one more MapLN logrec: for the deletion of the
         * new DB. This logrec, together with the 2 logrecs generated by the
         * truncate call above are additional obsolete logecs, not counted in
         * beforeAbort.
         */
        txn.abort();

        /*
         * We set truncateOrRemoveDone to true (meaning that per-DB utilization
         * will not be verified) even though the txn was aborted because the
         * discarded new DatabaseImpl will not be counted yet includes INs and
         * LNs from the operations above.
         */
        truncateOrRemoveDone = true;

        /* Make sure use count is decremented when we abort. */
        assertDbInUse(saveDb, false);
        openDb(null, DB_NAME1);
        saveDb = dbImpl;
        closeDb();
        assertDbInUse(saveDb, false);

        /*
         * The obsolete count should include the records inserted after
         * the truncate.
         */
        int expectedObsLNs = (embeddedLNs ? 3 : RECORD_COUNT/4 + 3);

        verifyUtilization(beforeAbort, expectedObsLNs, 5);

        /* Reopen, db should be populated. */
        openDb(null, DB_NAME1);
        assertEquals(RECORD_COUNT, countRecords(null));

        closeEnv();
        batchCleanAndVerify(saveId);
    }

    @Test
    public void testRemove()
        throws Exception {

        doTestRemove(false /*simulateCrash*/);
    }

    @Test
    public void testRemoveRecover()
        throws Exception {

        doTestRemove(true /*simulateCrash*/);
    }

    /**
     * Test that remove generates the right number of obsolete LNs.
     */
    private void doTestRemove(boolean simulateCrash)
        throws Exception {

        openEnv(true);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);

        DatabaseImpl saveDb = dbImpl;
        DatabaseId saveId = dbImpl.getId();
        closeDb();

        Transaction txn = env.beginTransaction(null, null);

        env.removeDatabase(txn, DB_NAME1);

        ObsoleteCounts beforeCommit = getObsoleteCounts();

        txn.commit();
        truncateOrRemoveDone = true;

        /* Make sure use count is decremented when we commit. */
        assertDbInUse(saveDb, false);

        if (simulateCrash) {
            envImpl.abnormalClose();
            envImpl = null;
            env = null;
            openEnv(true);

            /* After recovery, expect that the record LNs are obsolete. */
            ObsoleteCounts afterCrash = getObsoleteCounts();

            int obsolete = afterCrash.obsoleteLNs - beforeCommit.obsoleteLNs;

            if (embeddedLNs) {
                assertEquals(3, obsolete);
            } else {
                assertTrue("obsolete=" + obsolete +
                           " expected=" + RECORD_COUNT,
                           obsolete >= RECORD_COUNT);
            }
        } else {

            /* LNs + old NameLN, old MapLN, delete MapLN */
            int expectedObsLNs = (embeddedLNs ? 3 : RECORD_COUNT + 3);

            verifyUtilization(beforeCommit, expectedObsLNs, 15);
        }

        openDb(null, DB_NAME1);
        assertEquals(0, countRecords(null));

        closeEnv();
        batchCleanAndVerify(saveId);
    }

    /**
     * Test that remove generates the right number of obsolete LNs.
     */
    @Test
    public void testNonTxnalRemove()
        throws Exception {

        openEnv(false);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);
        DatabaseImpl saveDb = dbImpl;
        DatabaseId saveId = dbImpl.getId();
        closeDb();
        ObsoleteCounts beforeOperation = getObsoleteCounts();
        env.removeDatabase(null, DB_NAME1);
        truncateOrRemoveDone = true;

        /* Make sure use count is decremented. */
        assertDbInUse(saveDb, false);

        /* LNs + new NameLN, old NameLN, old MapLN, delete MapLN */
        int expectedObsLNs = (embeddedLNs ? 4 : RECORD_COUNT + 4);

        verifyUtilization(beforeOperation, expectedObsLNs, 15);

        openDb(null, DB_NAME1);
        assertEquals(0, countRecords(null));

        closeEnv();
        batchCleanAndVerify(saveId);
    }

    /**
     * Test that aborting remove generates the right number of obsolete LNs.
     */
    @Test
    public void testRemoveAbort()
        throws Exception {

        /* Create database, populate, remove, abort the remove. */
        openEnv(true);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);
        DatabaseImpl saveDb = dbImpl;
        closeDb();
        Transaction txn = env.beginTransaction(null, null);
        env.removeDatabase(txn, DB_NAME1);
        ObsoleteCounts beforeAbort = getObsoleteCounts();
        txn.abort();

        /* Make sure use count is decremented when we abort. */
        assertDbInUse(saveDb, false);

        verifyUtilization(beforeAbort, 0, 0);

        /* All records should be there. */
        openDb(null, DB_NAME1);
        assertEquals(RECORD_COUNT, countRecords(null));

        closeEnv();

        /*
         * Batch clean and then check the record count again, just to make sure
         * we don't lose any valid data.
         */
        openEnv(true);
        while (env.cleanLog() > 0) {
        }
        CheckpointConfig force = new CheckpointConfig();
        force.setForce(true);
        env.checkpoint(force);
        closeEnv();

        openEnv(true);
        openDb(null, DB_NAME1);
        assertEquals(RECORD_COUNT, countRecords(null));
        closeEnv();
    }

    /**
     * The same as testRemoveNotResident but forces fetching of obsolets LNs
     * in order to count their sizes accurately.
     */
    @Test
    public void testRemoveNotResidentFetchObsoleteSize()
        throws Exception {

        fetchObsoleteSize = true;
        testRemoveNotResident();
    }

    /**
     * Test that we can properly account for a non-resident database.
     */
    @Test
    public void testRemoveNotResident()
        throws Exception {

        /* Create a database, populate. */
        openEnv(true);

        /* Use EVICT_LN so that updates do not count obsolete size. */
        openDb(null, DB_NAME1, CacheMode.EVICT_LN);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);
        /* Updates will not count obsolete size. */
        writeAndCountRecords(null, RECORD_COUNT);
        DatabaseId saveId = DbInternal.getDbImpl(db).getId();
        closeEnv();

        /*
         * Open the environment and remove the database. The
         * database is not resident at all.
         */
        openEnv(true);
        Transaction txn = env.beginTransaction(null, null);
        env.removeDatabase(txn, DB_NAME1);
        ObsoleteCounts beforeCommit = getObsoleteCounts();
        txn.commit();
        truncateOrRemoveDone = true;

        /* LNs + old NameLN, old MapLN, delete MapLN */
        int expectedObsLNs = (embeddedLNs ? 3 : RECORD_COUNT + 3);

        verifyUtilization(beforeCommit,
                          expectedObsLNs,
                          /*
                           * 15 INs for data tree, plus 2 for FileSummaryDB
                           * split during tree walk.
                           */
                          DatabaseImpl.forceTreeWalkForTruncateAndRemove ?
                          17 : 15,
                          /* Records re-written + deleted + aborted LN. */
                          RECORD_COUNT + 2,
                          /* Records write twice. */
                          RECORD_COUNT * 2,
                          true /*expectAccurateObsoleteLNCount*/);

        /* check record count. */
        openDb(null, DB_NAME1);
        assertEquals(0, countRecords(null));

        closeEnv();
        batchCleanAndVerify(saveId);
    }

    /**
     * The same as testRemovePartialResident but forces fetching of obsolets
     * LNs in order to count their sizes accurately.
     */
    @Test
    public void testRemovePartialResidentFetchObsoleteSize()
        throws Exception {

        fetchObsoleteSize = true;
        testRemovePartialResident();
    }

    /**
     * Test that we can properly account for partially resident tree.
     */
    @Test
    public void testRemovePartialResident()
        throws Exception {

        /* Create a database, populate. */
        openEnv(true);
        /* Use EVICT_LN so that updates do not count obsolete size. */
        openDb(null, DB_NAME1, CacheMode.EVICT_LN);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndCountRecords(null, RECORD_COUNT);
        /* Updates will not count obsolete size. */
        writeAndCountRecords(null, RECORD_COUNT);
        DatabaseId saveId = DbInternal.getDbImpl(db).getId();
        closeEnv();

        /*
         * Open the environment and remove the database. Pull 1 BIN in.
         */
        openEnv(true);
        openDb(null, DB_NAME1);
        Cursor c = db.openCursor(null, null);
        assertEquals(OperationStatus.SUCCESS,
                     c.getFirst(new DatabaseEntry(), new DatabaseEntry(),
                                LockMode.DEFAULT));
        c.close();
        DatabaseImpl saveDb = dbImpl;
        closeDb();

        Transaction txn = env.beginTransaction(null, null);
        env.removeDatabase(txn, DB_NAME1);
        ObsoleteCounts beforeCommit = getObsoleteCounts();
        txn.commit();
        truncateOrRemoveDone = true;

        /* Make sure use count is decremented when we commit. */
        assertDbInUse(saveDb, false);

        /* LNs + old NameLN, old MapLN, delete MapLN */
        int expectedObsLNs = (embeddedLNs ? 3 : RECORD_COUNT + 3);

        verifyUtilization(beforeCommit,
                          expectedObsLNs,
                          /*
                           * 15 INs for data tree, plus 2 for FileSummaryDB
                           * split during tree walk.
                           */
                          DatabaseImpl.forceTreeWalkForTruncateAndRemove ?
                          17 : 15,
                          /* Records re-written + deleted + aborted LN. */
                          RECORD_COUNT + 2,
                          /* Records write twice. */
                          RECORD_COUNT * 2,
                          true /*expectAccurateObsoleteLNCount*/);

        /* check record count. */
        openDb(null, DB_NAME1);
        assertEquals(0, countRecords(null));

        closeEnv();
        batchCleanAndVerify(saveId);
    }

    /**
     * Tests that a log file is not deleted by the cleaner when it contains
     * entries in a database that is pending deletion.
     */
    @Test
    public void testDBPendingDeletion()
        throws DatabaseException, InterruptedException {

        doDBPendingTest(RECORD_COUNT + 30, false /*deleteAll*/, 5);
    }

    /**
     * Like testDBPendingDeletion but creates a scenario where only a single
     * log file is cleaned, and that log file contains only known obsolete
     * log entries.  This reproduced a bug where we neglected to add pending
     * deleted DBs to the cleaner's pending DB set if all entries in the log
     * file were known obsoleted. [#13333]
     */
    @Test
    public void testObsoleteLogFile()
        throws DatabaseException, InterruptedException {

        doDBPendingTest(70, true /*deleteAll*/, 1);
    }

    private void doDBPendingTest(int recordCount,
                                 boolean deleteAll,
                                 int expectFilesCleaned)
        throws DatabaseException, InterruptedException {

        /* Create a database, populate, close. */
        Set logFiles = new HashSet();
        openEnv(true);
        openDb(null, DB_NAME1);

        if (embeddedLNs && DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            closeEnv();
            return;
        }

        writeAndMakeWaste(recordCount, logFiles, deleteAll);
        int remainingRecordCount = deleteAll ? 0 : recordCount;
        env.checkpoint(FORCE_CHECKPOINT);
        ObsoleteCounts obsoleteCounts = getObsoleteCounts();
        DatabaseImpl saveDb = dbImpl;
        closeDb();
        assertTrue(!saveDb.isDeleteFinished());
        assertTrue(!saveDb.isDeleted());
        assertDbInUse(saveDb, false);

        /* Make sure that we wrote a full file's worth of LNs. */
        assertTrue(logFiles.size() >= 2);
        assertTrue(logFilesExist(logFiles));

        /* Remove the database but do not commit yet. */
        final Transaction txn = env.beginTransaction(null, null);
        env.removeDatabase(txn, DB_NAME1);

        /*
         * The obsolete count should be <= 1 (for the NameLN).
         *
         * The reason for passing false for expectAccurateObsoleteLNCount is
         * that the NameLN deletion is not committed.  It is not yet counted
         * obsolete by live utilization counting, but will be counted obsolete
         * by the utilization recalculation utility, which assumes that
         * transactions will commit. [#22208]
         */
        obsoleteCounts = verifyUtilization
            (obsoleteCounts, 1, 0, 0, 0,
             false /*expectAccurateObsoleteLNCount*/);
        truncateOrRemoveDone = true;

        junitThread = new JUnitThread("Committer") {
            @Override
            public void testBody() {
                try {
                    txn.commit();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        };

        /*
         * Set a hook to cause the commit to block.  The commit is done in a
         * separate thread.  The commit will set the DB state to pendingDeleted
         * and will then wait for the hook to return.
         */
        final Object lock = new Object();

        saveDb.setPendingDeletedHook(new TestHook() {
            public void doHook() {
                synchronized (lock) {
                    try {
                        lock.notify();
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e.toString());
                    }
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

        /* Start the committer thread; expect the pending deleted state. */
        synchronized (lock) {
            junitThread.start();
            lock.wait();
        }
        assertTrue(!saveDb.isDeleteFinished());
        assertTrue(saveDb.isDeleted());
        assertDbInUse(saveDb, true);

        /* Expect obsolete LNs: NameLN */
        obsoleteCounts = verifyUtilization(obsoleteCounts, 1, 0);

        /* The DB deletion is pending; the log file should still exist. */
        int filesCleaned = env.cleanLog();
        assertEquals(expectFilesCleaned, filesCleaned);
        assertTrue(filesCleaned > 0);
        env.checkpoint(FORCE_CHECKPOINT);
        env.checkpoint(FORCE_CHECKPOINT);
        assertTrue(logFilesExist(logFiles));

        /*
         * When the committer thread finishes, the DB deletion will be
         * complete and the DB state will change to deleted.
         */
        synchronized (lock) {
            lock.notify();
        }
        try {
            junitThread.finishTest();
            junitThread = null;
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.toString());
        }
        assertTrue(saveDb.isDeleteFinished());
        assertTrue(saveDb.isDeleted());
        assertDbInUse(saveDb, false);

        /* Expect obsolete LNs: recordCount + MapLN + FSLNs (apprx). */
        int expectedObsLNs = (embeddedLNs ? 0 : remainingRecordCount) + 8;
        verifyUtilization(obsoleteCounts, expectedObsLNs, 0);

        /* The DB deletion is complete; the log file should be deleted. */
        env.checkpoint(FORCE_CHECKPOINT);
        env.checkpoint(FORCE_CHECKPOINT);
        assertTrue(!logFilesExist(logFiles));
    }

    /*
     * The xxxForceTreeWalk tests set the DatabaseImpl
     * forceTreeWalkForTruncateAndRemove field to true, which will force a walk
     * of the tree to count utilization during truncate/remove, rather than
     * using the per-database info.  This is used to test the "old technique"
     * for counting utilization, which is now used only if the database was
     * created prior to log version 6.
     */
    @Test
    public void testTruncateForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testTruncate();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testTruncateAbortForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testTruncateAbort();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testTruncateRepopulateAbortForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testTruncateRepopulateAbort();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testRemoveForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testRemove();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testNonTxnalRemoveForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testNonTxnalRemove();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testRemoveAbortForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testRemoveAbort();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testRemoveNotResidentForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testRemoveNotResident();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testRemovePartialResidentForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testRemovePartialResident();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testDBPendingDeletionForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testDBPendingDeletion();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testObsoleteLogFileForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testObsoleteLogFile();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    /**
     * Tickles a bug that caused NPE during recovery during the sequence:
     * delete record, trucate DB, crash (close without checkpoint), and
     * recover. [#16515]
     */
    @Test
    public void testDeleteTruncateRecover()
        throws DatabaseException {

        /* Delete a record. */
        openEnv(true);
        openDb(null, DB_NAME1);
        writeAndCountRecords(null, 1);
        closeDb();

        /* Truncate DB. */
        Transaction txn = env.beginTransaction(null, null);
        truncate(txn, false);
        txn.commit();

        /* Close without checkpoint. */
        envImpl.close(false /*doCheckpoint*/);
        envImpl = null;
        env = null;

        /* Recover -- the bug cause NPE here. */
        openEnv(true);
        closeEnv();
    }

    private void writeAndCountRecords(Transaction txn, long count)
        throws DatabaseException {

        for (int i = 1; i <= count; i += 1) {
            DatabaseEntry entry = new DatabaseEntry(TestUtils.getTestArray(i));

            db.put(txn, entry, entry);
        }

        /* Insert and delete some records, insert and abort some records. */
        DatabaseEntry entry =
            new DatabaseEntry(TestUtils.getTestArray((int)count+1));
        db.put(txn, entry, entry);
        db.delete(txn, entry);

        EnvironmentConfig envConfig = env.getConfig();
        if (envConfig.getTransactional()) {
            entry = new DatabaseEntry(TestUtils.getTestArray(0));
            Transaction txn2 = env.beginTransaction(null, null);
            db.put(txn2, entry, entry);
            txn2.abort();
            txn2 = null;
        }

        assertEquals(count, countRecords(txn));
    }

    /**
     * Writes the specified number of records to db.  Check the number of
     * records, and return the number of obsolete records.  Returns a set of
     * the file numbers that are written to.
     *
     * Makes waste (obsolete records):  If doDelete=true, deletes records as
     * they are added; otherwise does updates to produce obsolete records
     * interleaved with non-obsolete records.
     */
    private void writeAndMakeWaste(long count,
                                   Set logFilesWritten,
                                   boolean doDelete)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        for (int i = 0; i < count; i += 1) {
            DatabaseEntry entry = new DatabaseEntry(TestUtils.getTestArray(i));
            cursor.put(entry, entry);
            /* Add log file written. */
            long file = CleanerTestUtils.getLogFile(cursor);
            logFilesWritten.add(new Long(file));
            /* Make waste. */
            if (!doDelete) {
                cursor.put(entry, entry);
                cursor.put(entry, entry);
            }
        }
        if (doDelete) {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus status;
            for (status = cursor.getFirst(key, data, null);
                 status == OperationStatus.SUCCESS;
                 status = cursor.getNext(key, data, null)) {
                /* Make waste. */
                cursor.delete();
                /* Add log file written. */
                long file = CleanerTestUtils.getLogFile(cursor);
                logFilesWritten.add(new Long(file));
            }
        }
        cursor.close();
        txn.commit();
        assertEquals(doDelete ? 0 : count, countRecords(null));
    }

    /* Truncate database and check the count. */
    private void truncate(Transaction useTxn, boolean getCount)
        throws DatabaseException {

        long nTruncated = env.truncateDatabase(useTxn, DB_NAME1, getCount);

        if (getCount) {
            assertEquals(RECORD_COUNT, nTruncated);
        }

        assertEquals(0, countRecords(useTxn));
    }

    /**
     * Returns how many records are in the database.
     */
    private int countRecords(Transaction useTxn)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        boolean opened = false;
        if (db == null) {
            openDb(useTxn, DB_NAME1);
            opened = true;
        }
        Cursor cursor = db.openCursor(useTxn, null);
        int count = 0;
        try {
            OperationStatus status = cursor.getFirst(key, data, null);
            while (status == OperationStatus.SUCCESS) {
                count += 1;
                status = cursor.getNext(key, data, null);
            }
        } finally {
            cursor.close();
        }
        if (opened) {
            closeDb();
        }
        return count;
    }

    /**
     * Return the total number of obsolete node counts according to the
     * UtilizationProfile and UtilizationTracker.
     */
    private ObsoleteCounts getObsoleteCounts() {
        FileSummary[] files = envImpl.getUtilizationProfile()
               .getFileSummaryMap(true)
               .values().toArray(new FileSummary[0]);
        int lnCount = 0;
        int inCount = 0;
        int lnSize = 0;
        int lnSizeCounted = 0;
        for (int i = 0; i < files.length; i += 1) {
            lnCount += files[i].obsoleteLNCount;
            inCount += files[i].obsoleteINCount;
            lnSize += files[i].obsoleteLNSize;
            lnSizeCounted += files[i].obsoleteLNSizeCounted;
        }

        return new ObsoleteCounts(lnCount, inCount, lnSize, lnSizeCounted);
    }

    private class ObsoleteCounts {
        int obsoleteLNs;
        int obsoleteINs;
        int obsoleteLNSize;
        int obsoleteLNSizeCounted;

        ObsoleteCounts(int obsoleteLNs,
                       int obsoleteINs,
                       int obsoleteLNSize,
                       int obsoleteLNSizeCounted) {
            this.obsoleteLNs = obsoleteLNs;
            this.obsoleteINs = obsoleteINs;
            this.obsoleteLNSize = obsoleteLNSize;
            this.obsoleteLNSizeCounted = obsoleteLNSizeCounted;
        }

        @Override
        public String toString() {
            return "lns=" + obsoleteLNs + " ins=" + obsoleteINs +
                   " lnSize=" + obsoleteLNSize +
                   " lnSizeCounted=" + obsoleteLNSizeCounted;
        }
    }

    private ObsoleteCounts verifyUtilization(ObsoleteCounts prev,
                                             int expectedLNs,
                                             int expectedINs)
        throws DatabaseException {

        return verifyUtilization(prev, expectedLNs, expectedINs, 0, 0,
                                 true /*expectAccurateObsoleteLNCount*/);
    }

    /*
     * Check obsolete counts. If the expected IN count is zero, don't
     * check the obsolete IN count.  Always check the obsolete LN count.
     */
    private ObsoleteCounts verifyUtilization(ObsoleteCounts prev,
                                             int expectedLNs,
                                             int expectedINs,
                                             int expectLNsSizeNotCounted,
                                             int minTotalLNsObsolete,
                                         boolean expectAccurateObsoleteLNCount)
        throws DatabaseException {

        /*
         * If we are not forcing a tree walk or we have explicitly configured
         * fetchObsoleteSize, then the size of every LN should have been
         * counted.
         */
        boolean expectAccurateObsoleteLNSize =
            !DatabaseImpl.forceTreeWalkForTruncateAndRemove ||
            fetchObsoleteSize;

        /*
         * Unless we are forcing the tree walk and not not fetching to get
         * obsolete size, the obsolete size is always counted.
         */
        if (fetchObsoleteSize ||
            !DatabaseImpl.forceTreeWalkForTruncateAndRemove) {
            expectLNsSizeNotCounted = 0;
        }

        ObsoleteCounts now = getObsoleteCounts();
        String beforeAndAfter = "before: " + prev + " now: " + now;

        final int newObsolete = now.obsoleteLNs - prev.obsoleteLNs;

        assertEquals(beforeAndAfter, expectedLNs, newObsolete);

        if (expectAccurateObsoleteLNSize) {
            assertEquals(beforeAndAfter,
                         newObsolete + expectLNsSizeNotCounted,
                         now.obsoleteLNSizeCounted -
                         prev.obsoleteLNSizeCounted);
            final int expectMinSize = minTotalLNsObsolete * 6 /*average*/;
            assertTrue("expect min = " + expectMinSize +
                       " total size = " + now.obsoleteLNSize,
                       now.obsoleteLNSize > expectMinSize);
        }

        if (expectedINs > 0) {
            assertEquals(beforeAndAfter, expectedINs,
                         now.obsoleteINs - prev.obsoleteINs);
        }

        /*
         * We pass expectAccurateDbUtilization as false when
         * truncateOrRemoveDone, because the database utilization info for that
         * database is now gone.
         */
        VerifyUtils.verifyUtilization
            (envImpl,
             expectAccurateObsoleteLNCount,
             expectAccurateObsoleteLNSize,
             !truncateOrRemoveDone); // expectAccurateDbUtilization

        return now;
    }

    /**
     * Checks whether a given DB has a non-zero use count.  Does nothing if
     * je.dbEviction is not enabled, since reference counts are only maintained
     * if that config parameter is enabled.
     */
    private void assertDbInUse(DatabaseImpl db, boolean inUse) {
        if (dbEviction) {
            assertEquals(inUse, db.isInUse());
        }
    }

    /**
     * Returns true if all files exist, or false if any file is deleted.
     */
    private boolean logFilesExist(Set fileNumbers) {

        Iterator iter = fileNumbers.iterator();
        while (iter.hasNext()) {
            long fileNum = ((Long) iter.next()).longValue();
            File file = new File(envImpl.getFileManager().getFullFileName
                                 (fileNum, FileManager.JE_SUFFIX));
            if (!file.exists()) {
                return false;
            }
        }
        return true;
    }

    /*
     * Run batch cleaning and verify that there are no files with these
     * log entries.
     */
    private void batchCleanAndVerify(DatabaseId dbId)
        throws Exception {

        /*
         * Open the environment, flip the log files to reduce mixing of new
         * records and old records and add more records to force the
         * utilization level of the removed records down.
         */
        openEnv(true);
        openDb(null, DB_NAME2);
        long lsn = envImpl.forceLogFileFlip();
        CheckpointConfig force = new CheckpointConfig();
        force.setForce(true);
        env.checkpoint(force);

        writeAndCountRecords(null, RECORD_COUNT * 3);
        env.checkpoint(force);

        closeDb();

        /* Check log files, there should be entries with this database. */
        CheckReader checker = new CheckReader(envImpl, dbId, true);
        while (checker.readNextEntry()) {
        }

        if (DEBUG) {
            System.out.println("entries for this db =" + checker.getCount());
        }

        assertTrue(checker.getCount() > 0);

        /* batch clean. */
        boolean anyCleaned = false;
        while (env.cleanLog() > 0) {
            anyCleaned = true;
        }

        assertTrue(anyCleaned);

        if (anyCleaned) {
            env.checkpoint(force);
        }

        /* Check log files, there should be no entries with this database. */
        checker = new CheckReader(envImpl, dbId, false);
        while (checker.readNextEntry()) {
        }

        closeEnv();
    }

    class CheckReader extends DumpFileReader{

        private final DatabaseId dbId;
        private final boolean expectEntries;
        private int count;

        /*
         * @param databaseId we're looking for log entries for this database.
         * @param expectEntries if false, there should be no log entries
         * with this database id. If true, the log should have entries
         * with this database id.
         */
        CheckReader(EnvironmentImpl envImpl,
                    DatabaseId dbId,
                    boolean expectEntries)
            throws DatabaseException {

            super(envImpl, 1000, DbLsn.NULL_LSN, DbLsn.NULL_LSN, 
                  DbLsn.NULL_LSN, null, null, null, false, false, true);
            this.dbId = dbId;
            this.expectEntries = expectEntries;
        }

        @Override
        protected boolean processEntry(ByteBuffer entryBuffer)
            throws DatabaseException {

            /* Figure out what kind of log entry this is */
            byte type = currentEntryHeader.getType();
            LogEntryType lastEntryType = LogEntryType.findType(type);
            boolean isNode = lastEntryType.isNodeType();

            /* Read the entry. */
            LogEntry entry = lastEntryType.getSharedLogEntry();
            entry.readEntry(envImpl, currentEntryHeader, entryBuffer);

            long lsn = getLastLsn();
            if (isNode) {
                boolean found = false;
                if (entry instanceof INLogEntry) {
                    INLogEntry<?> inEntry = (INLogEntry<?>) entry;
                    found = dbId.equals(inEntry.getDbId());
                } else {
                    LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
                    found = dbId.equals(lnEntry.getDbId());
                }
                if (found) {
                    if (expectEntries) {
                        count++;
                    } else {
                        StringBuilder sb = new StringBuilder();
                        entry.dumpEntry(sb, false);
                        fail("lsn=" + DbLsn.getNoFormatString(lsn) +
                             " dbId = " + dbId +
                             " entry= " + sb.toString());
                    }
                }
            }

            return true;
        }

        /* Num entries with this database id seen by reader. */
        int getCount() {
            return count;
        }
    }
}
