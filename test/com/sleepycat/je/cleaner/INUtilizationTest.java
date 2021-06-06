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

import java.io.IOException;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.SearchFileReader;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;

import org.junit.After;
import org.junit.Test;

/**
 * Test utilization counting of INs.
 */
public class INUtilizationTest extends CleanerTestBase {

    private static final String DB_NAME = "foo";

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private EnvironmentImpl envImpl;
    private Database db;
    private DatabaseImpl dbImpl;
    private Transaction txn;
    private Cursor cursor;
    private boolean dups = false;
    private DatabaseEntry keyEntry = new DatabaseEntry();
    private DatabaseEntry dataEntry = new DatabaseEntry();
    private boolean truncateOrRemoveDone;

    private boolean embeddedLNs = false;

    public INUtilizationTest() {
        envMultiSubDir = false;
    }

    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        envImpl = null;
        db = null;
        dbImpl = null;
        txn = null;
        cursor = null;
        keyEntry = null;
        dataEntry = null;
    }

    /**
     * Opens the environment and database.
     */
    @SuppressWarnings("deprecation")
    private void openEnv()
        throws DatabaseException {

        EnvironmentConfig config = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(config);
        config.setTransactional(true);
        config.setTxnNoSync(true);
        config.setAllowCreate(true);
        /* Do not run the daemons. */
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        /* Use a tiny log file size to write one node per file. */
        config.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                              Integer.toString(64));
        /* With tiny files we can't log expiration profile records. */
        DbInternal.setCreateEP(config, false);
        if (envMultiSubDir) {
            config.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                  DATA_DIRS + "");
        }
        env = new Environment(envHome, config);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        /* Speed up test that uses lots of very small files. */
        envImpl.getFileManager().setSyncAtFileEnd(false);

        openDb();

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);
    }

    /**
     * Opens the database.
     */
    private void openDb()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        db = env.openDatabase(null, DB_NAME, dbConfig);
        dbImpl = DbInternal.getDbImpl(db);
    }

    private void closeEnv(boolean doCheckpoint)
        throws DatabaseException {

        closeEnv(doCheckpoint,
                 true,  // expectAccurateObsoleteLNCount
                 true); // expectAccurateObsoleteLNSize
    }

    private void closeEnv(boolean doCheckpoint,
                          boolean expectAccurateObsoleteLNCount)
        throws DatabaseException {

        closeEnv(doCheckpoint,
                 expectAccurateObsoleteLNCount,
                 expectAccurateObsoleteLNCount);
    }

    /**
     * Closes the environment and database.
     *
     * @param expectAccurateObsoleteLNCount should be false when a deleted LN
     * is not counted properly by recovery because its parent INs were flushed
     * and the obsolete LN was not found in the tree.
     *
     * @param expectAccurateObsoleteLNSize should be false when a tree walk is
     * performed for truncate/remove or an abortLsn is counted by recovery.
     */
    private void closeEnv(boolean doCheckpoint,
                          boolean expectAccurateObsoleteLNCount,
                          boolean expectAccurateObsoleteLNSize)
        throws DatabaseException {

        /*
         * We pass expectAccurateDbUtilization as false when
         * truncateOrRemoveDone, because the database utilization info for that
         * database is now gone.
         */
        VerifyUtils.verifyUtilization
            (envImpl, expectAccurateObsoleteLNCount,
             expectAccurateObsoleteLNSize,
             !truncateOrRemoveDone); // expectAccurateDbUtilization

        if (db != null) {
            db.close();
            db = null;
            dbImpl = null;
        }
        if (envImpl != null) {
            envImpl.close(doCheckpoint);
            envImpl = null;
            env = null;
        }
    }

    /**
     * Initial setup for all tests -- open env, put one record (or two for
     * dups) and sync.
     */
    private void openAndWriteDatabase()
        throws DatabaseException {

        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Put one record. */
        IntegerBinding.intToEntry(0, keyEntry);
        IntegerBinding.intToEntry(0, dataEntry);
        cursor.put(keyEntry, dataEntry);

        /* Add a duplicate. */
        if (dups) {
            IntegerBinding.intToEntry(1, dataEntry);
            cursor.put(keyEntry, dataEntry);
        }

        /* Commit the txn to avoid crossing the checkpoint boundary. */
        cursor.close();
        txn.commit();

        /* Checkpoint to the root so nothing is dirty. */
        env.sync();

        /* Open a txn and cursor for use by the test case. */
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* If we added a duplicate, move cursor back to the first record. */
        cursor.getFirst(keyEntry, dataEntry, null);

        /* Expect that BIN and parent IN files are not obsolete. */
        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);
    }

    /**
     * Tests that BIN and IN utilization counting works.
     */
    @Test
    public void testBasic()
        throws DatabaseException {

        openAndWriteDatabase();

        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);

        /* Update to make BIN dirty. */
        cursor.put(keyEntry, dataEntry);

        /* Checkpoint */
        env.checkpoint(forceConfig);

        /* After checkpoint, expect BIN and IN are obsolete. */
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);
        assertTrue(binFile != getBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        /* After second checkpoint, no changes. */
        env.checkpoint(forceConfig);

        /* Both BIN and IN are obsolete. */
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);
        assertTrue(binFile != getBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        /* Expect that new files are not obsolete. */
        long binFile2 = getBINFile(cursor);
        long inFile2 = getINFile(cursor);
        expectObsolete(binFile2, false);
        expectObsolete(inFile2, false);

        cursor.close();
        txn.commit();
        closeEnv(true);
    }

    /**
     * Performs testBasic with duplicates.
     */
    @Test
    public void testBasicDup()
        throws DatabaseException {

        dups = true;
        testBasic();
    }

    /**
     * Tests that BIN-delta utilization counting works.
     */
    @Test
    public void testBINDeltas()
        throws DatabaseException {

        /*
         * Insert 4 additional records so there will be 5 slots total. Then one
         * modified slot (less than 25% of the total) will cause a delta to be
         * logged.  Two modified slots (more than 25% of the total) will cause
         * a full version to be logged.
         */
        openAndWriteDatabase();
        for (int i = 1; i <= 4; i += 1) {
            IntegerBinding.intToEntry(i, keyEntry);
            IntegerBinding.intToEntry(0, dataEntry);
            db.put(txn, keyEntry, dataEntry);
        }
        env.sync();
        long fullBinFile = getFullBINFile(cursor);
        long deltaBinFile = getDeltaBINFile(cursor);
        long inFile = getINFile(cursor);
        assertEquals(-1, deltaBinFile);

        /* Update first record to make one BIN slot dirty. */
        IntegerBinding.intToEntry(0, keyEntry);
        cursor.put(keyEntry, dataEntry);

        /* Checkpoint, write BIN-delta. */
        env.checkpoint(forceConfig);

        /* After checkpoint, expect only IN is obsolete. */
        expectObsolete(fullBinFile, false);
        expectObsolete(inFile, true);
        assertTrue(fullBinFile == getFullBINFile(cursor));
        assertTrue(deltaBinFile != getDeltaBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        /* After second checkpoint, no changes. */
        env.checkpoint(forceConfig);
        expectObsolete(fullBinFile, false);
        expectObsolete(inFile, true);
        assertTrue(fullBinFile == getFullBINFile(cursor));
        assertTrue(deltaBinFile != getDeltaBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        fullBinFile = getFullBINFile(cursor);
        deltaBinFile = getDeltaBINFile(cursor);
        inFile = getINFile(cursor);
        assertTrue(deltaBinFile != -1);

        /* Update first record again, checkpoint to write another delta. */
        IntegerBinding.intToEntry(0, keyEntry);
        cursor.put(keyEntry, dataEntry);

        /* After checkpoint, expect IN and first delta are obsolete. */
        env.checkpoint(forceConfig);
        expectObsolete(fullBinFile, false);
        expectObsolete(deltaBinFile, true);
        expectObsolete(inFile, true);
        assertTrue(fullBinFile == getFullBINFile(cursor));
        assertTrue(deltaBinFile != getDeltaBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        fullBinFile = getFullBINFile(cursor);
        deltaBinFile = getDeltaBINFile(cursor);
        inFile = getINFile(cursor);
        assertTrue(deltaBinFile != -1);

        /* Update two records, checkpoint to write a full BIN version. */
        IntegerBinding.intToEntry(0, keyEntry);
        cursor.put(keyEntry, dataEntry);
        IntegerBinding.intToEntry(1, keyEntry);
        cursor.put(keyEntry, dataEntry);

        /* After checkpoint, expect IN, full BIN, last delta are obsolete. */
        env.checkpoint(forceConfig);
        expectObsolete(fullBinFile, true);
        expectObsolete(deltaBinFile, true);
        expectObsolete(inFile, true);
        assertTrue(fullBinFile != getFullBINFile(cursor));
        assertTrue(deltaBinFile != getDeltaBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));
        assertEquals(-1, getDeltaBINFile(cursor));

        /* Expect that new files are not obsolete. */
        long binFile2 = getBINFile(cursor);
        long inFile2 = getINFile(cursor);
        expectObsolete(binFile2, false);
        expectObsolete(inFile2, false);

        cursor.close();
        txn.commit();
        closeEnv(true);
    }

    /**
     * Performs testBINDeltas with duplicates.
     */
    @Test
    public void testBINDeltasDup()
        throws DatabaseException {

        dups = true;
        testBINDeltas();
    }

    /**
     * Similar to testBasic, but logs INs explicitly and performs recovery to
     * ensure utilization recovery works.
     */
    @Test
    public void testRecovery()
        throws DatabaseException {

        openAndWriteDatabase();
        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);

        /* Close normally and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(true);
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect BIN and IN files have not changed. */
        assertEquals(binFile, getBINFile(cursor));
        assertEquals(inFile, getINFile(cursor));
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);

        /*
         * Log explicitly since we have no way to do a partial checkpoint.
         * The BIN is logged provisionally and the IN non-provisionally.
         */
        TestUtils.logBINAndIN(env, cursor);

        /* Expect to obsolete the BIN and IN. */
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);
        assertTrue(binFile != getBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        /* Save current BIN and IN files. */
        long binFile2 = getBINFile(cursor);
        long inFile2 = getINFile(cursor);
        expectObsolete(binFile2, false);
        expectObsolete(inFile2, false);

        /* Shutdown without a checkpoint and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(false);
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Sync to make all INs non-dirty. */
        env.sync();

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect that recovery counts BIN and IN as obsolete. */
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);
        assertTrue(binFile != getBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));

        /*
         * Even though it is provisional, expect that current BIN is not
         * obsolete because it is not part of partial checkpoint.  This is
         * similar to what happens with a split.  The current IN is not
         * obsolete either (nor is it provisional).
         */
        assertTrue(binFile2 == getBINFile(cursor));
        assertTrue(inFile2 == getINFile(cursor));
        expectObsolete(binFile2, false);
        expectObsolete(inFile2, false);

        /* Update to make BIN dirty. */
        cursor.put(keyEntry, dataEntry);

        /* Check current BIN and IN files. */
        assertTrue(binFile2 == getBINFile(cursor));
        assertTrue(inFile2 == getINFile(cursor));
        expectObsolete(binFile2, false);
        expectObsolete(inFile2, false);

        /* Close normally and reopen to cause checkpoint of dirty BIN/IN. */
        cursor.close();
        txn.commit();
        closeEnv(true);
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect BIN and IN were checkpointed during close. */
        assertTrue(binFile2 != getBINFile(cursor));
        assertTrue(inFile2 != getINFile(cursor));
        expectObsolete(binFile2, true);
        expectObsolete(inFile2, true);

        /* After second checkpoint, no change. */
        env.checkpoint(forceConfig);

        /* Both BIN and IN are obsolete. */
        assertTrue(binFile2 != getBINFile(cursor));
        assertTrue(inFile2 != getINFile(cursor));
        expectObsolete(binFile2, true);
        expectObsolete(inFile2, true);

        cursor.close();
        txn.commit();
        closeEnv(true);
    }

    /**
     * Performs testRecovery with duplicates.
     */
    @Test
    public void testRecoveryDup()
        throws DatabaseException {

        dups = true;
        testRecovery();
    }

    /**
     * Similar to testRecovery, but tests BIN-deltas.
     */
    @Test
    public void testBINDeltaRecovery()
        throws DatabaseException {

        /*
         * Insert 4 additional records so there will be 5 slots total. Then one
         * modified slot (less than 25% of the total) will cause a delta to be
         * logged.  Two modified slots (more than 25% of the total) will cause
         * a full version to be logged.
         */
        openAndWriteDatabase();
        for (int i = 1; i <= 4; i += 1) {
            IntegerBinding.intToEntry(i, keyEntry);
            IntegerBinding.intToEntry(0, dataEntry);
            db.put(txn, keyEntry, dataEntry);
        }
        env.sync();
        long fullBinFile = getFullBINFile(cursor);
        long deltaBinFile = getDeltaBINFile(cursor);
        long inFile = getINFile(cursor);
        assertEquals(-1, deltaBinFile);

        /* Close normally and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(true);
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect BIN and IN files have not changed. */
        assertEquals(fullBinFile, getFullBINFile(cursor));
        assertEquals(deltaBinFile, getDeltaBINFile(cursor));
        assertEquals(inFile, getINFile(cursor));
        expectObsolete(fullBinFile, false);
        expectObsolete(inFile, false);

        /* Update first record to make one BIN slot dirty. */
        IntegerBinding.intToEntry(0, keyEntry);
        cursor.put(keyEntry, dataEntry);

        /*
         * Log explicitly since we have no way to do a partial checkpoint.
         * The BIN-delta is logged provisionally and the IN non-provisionally.
         */
        TestUtils.logBINAndIN(env, cursor, true /*allowDeltas*/);

        /* Expect to obsolete the IN but not the full BIN. */
        expectObsolete(fullBinFile, false);
        expectObsolete(inFile, true);
        assertEquals(fullBinFile, getFullBINFile(cursor));

        /* Save current BIN-delta and IN files. */
        long deltaBinFile2 = getDeltaBINFile(cursor);
        long inFile2 = getINFile(cursor);
        assertTrue(deltaBinFile != deltaBinFile2);
        assertTrue(inFile != inFile2);
        expectObsolete(deltaBinFile2, false);
        expectObsolete(inFile2, false);

        /* Shutdown without a checkpoint and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(false,  // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 false); // expectAccurateObsoleteLNSize
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Sync to make all INs non-dirty. */
        env.sync();

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect that recovery counts only IN as obsolete. */
        expectObsolete(inFile, true);
        expectObsolete(fullBinFile, false);
        expectObsolete(deltaBinFile2, false);
        expectObsolete(inFile2, false);
        assertEquals(fullBinFile, getFullBINFile(cursor));
        assertEquals(deltaBinFile2, getDeltaBINFile(cursor));
        assertEquals(inFile2, getINFile(cursor));

        /* Reset variables to current versions. */
        deltaBinFile = deltaBinFile2;
        inFile = inFile2;

        /* Update same slot and write another delta. */
        IntegerBinding.intToEntry(0, keyEntry);
        cursor.put(keyEntry, dataEntry);
        TestUtils.logBINAndIN(env, cursor, true /*allowDeltas*/);

        /* Expect to obsolete the BIN-delta and IN, but not the full BIN. */
        expectObsolete(fullBinFile, false);
        expectObsolete(deltaBinFile, true);
        expectObsolete(inFile, true);
        assertEquals(fullBinFile, getFullBINFile(cursor));

        /* Save current BIN-delta and IN files. */
        deltaBinFile2 = getDeltaBINFile(cursor);
        inFile2 = getINFile(cursor);
        assertTrue(deltaBinFile != deltaBinFile2);
        assertTrue(inFile != inFile2);
        expectObsolete(deltaBinFile2, false);
        expectObsolete(inFile2, false);

        /* Shutdown without a checkpoint and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(false,  // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 false); // expectAccurateObsoleteLNSize
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Sync to make all INs non-dirty. */
        env.sync();

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect that recovery counts only BIN-delta and IN as obsolete. */
        expectObsolete(fullBinFile, false);
        expectObsolete(deltaBinFile, true);
        expectObsolete(inFile, true);
        expectObsolete(deltaBinFile2, false);
        expectObsolete(inFile2, false);
        assertEquals(fullBinFile, getFullBINFile(cursor));
        assertEquals(deltaBinFile2, getDeltaBINFile(cursor));
        assertEquals(inFile2, getINFile(cursor));

        /* Reset variables to current versions. */
        deltaBinFile = deltaBinFile2;
        inFile = inFile2;

        /* Update two records and write a full BIN version. */
        IntegerBinding.intToEntry(0, keyEntry);
        cursor.put(keyEntry, dataEntry);
        IntegerBinding.intToEntry(1, keyEntry);
        cursor.put(keyEntry, dataEntry);
        TestUtils.logBINAndIN(env, cursor, true /*allowDeltas*/);

        /* Expect to obsolete the full BIN, the BIN-delta and the IN. */
        expectObsolete(fullBinFile, true);
        expectObsolete(deltaBinFile, true);
        expectObsolete(inFile, true);

        /* Save current BIN, BIN-delta and IN files. */
        long fullBinFile2 = getFullBINFile(cursor);
        deltaBinFile2 = getDeltaBINFile(cursor);
        inFile2 = getINFile(cursor);
        assertTrue(fullBinFile != fullBinFile2);
        assertTrue(deltaBinFile != deltaBinFile2);
        assertTrue(inFile != inFile2);
        assertEquals(DbLsn.NULL_LSN, deltaBinFile2);
        expectObsolete(fullBinFile2, false);
        expectObsolete(inFile2, false);

        /* Shutdown without a checkpoint and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(false,  // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 false); // expectAccurateObsoleteLNSize
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Sync to make all INs non-dirty. */
        env.sync();

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect that recovery counts BIN, BIN-delta and IN as obsolete. */
        expectObsolete(fullBinFile, true);
        expectObsolete(deltaBinFile, true);
        expectObsolete(inFile, true);
        expectObsolete(fullBinFile2, false);
        assertEquals(DbLsn.NULL_LSN, deltaBinFile2);
        expectObsolete(inFile2, false);
        assertEquals(deltaBinFile2, getDeltaBINFile(cursor));
        assertEquals(inFile2, getINFile(cursor));

        cursor.close();
        txn.commit();
        closeEnv(false,  // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 false); // expectAccurateObsoleteLNSize
    }

    /**
     * Performs testRecovery with duplicates.
     */
    @Test
    public void testBINDeltaRecoveryDup()
        throws DatabaseException {

        dups = true;
        testBINDeltaRecovery();
    }

    /**
     * Tests that in a partial checkpoint (CkptStart with no CkptEnd) all
     * provisional INs are counted as obsolete.
     */
    @Test
    public void testPartialCheckpoint()
        throws DatabaseException, IOException {

        openAndWriteDatabase();
        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);

        /* Close with partial checkpoint and reopen. */
        cursor.close();
        txn.commit();
        performPartialCheckpoint(true); // truncateUtilizationInfo

        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect BIN and IN files have not changed. */
        assertEquals(binFile, getBINFile(cursor));
        assertEquals(inFile, getINFile(cursor));
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);

        /* Update to make BIN dirty. */
        cursor.put(keyEntry, dataEntry);

        /* Force IN dirty so that BIN is logged provisionally. */
        TestUtils.getIN(TestUtils.getBIN(cursor)).setDirty(true);

        /* Check current BIN and IN files. */
        assertTrue(binFile == getBINFile(cursor));
        assertTrue(inFile == getINFile(cursor));
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);

        /* Close with partial checkpoint and reopen. */
        cursor.close();
        txn.commit();
        performPartialCheckpoint(true);  // truncateUtilizationInfo
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect BIN and IN files are obsolete. */
        assertTrue(binFile != getBINFile(cursor));
        assertTrue(inFile != getINFile(cursor));
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);

        /*
         * Expect that the current BIN is obsolete because it was provisional,
         * and provisional nodes following CkptStart are counted obsolete
         * even if that is sometimes incorrect.  The parent IN file is not
         * obsolete because it is not provisonal.
         */
        long binFile2 = getBINFile(cursor);
        long inFile2 = getINFile(cursor);
        expectObsolete(binFile2, true);
        expectObsolete(inFile2, false);

        /*
         * Now repeat the test above but do not truncate the FileSummaryLNs.
         * The counting will be accurate because the FileSummaryLNs override
         * what is counted manually during recovery.
         */

        /* Update to make BIN dirty. */
        cursor.put(keyEntry, dataEntry);

        /* Close with partial checkpoint and reopen. */
        cursor.close();
        txn.commit();
        performPartialCheckpoint(false,  // truncateUtilizationInfo
                                 true,   // expectAccurateObsoleteLNCount
                                 false); // expectAccurateObsoleteLNSize

        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* The prior BIN file is now double-counted as obsolete. */
        assertTrue(binFile2 != getBINFile(cursor));
        assertTrue(inFile2 != getINFile(cursor));
        expectObsolete(binFile2, 2);
        expectObsolete(inFile2, 1);

        /* Expect current BIN and IN files are not obsolete. */
        binFile2 = getBINFile(cursor);
        inFile2 = getINFile(cursor);
        expectObsolete(binFile2, false);
        expectObsolete(inFile2, false);

        cursor.close();
        txn.commit();
        closeEnv(true,   // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 false); // expectAccurateObsoleteLNSize
    }

    /**
     * Performs testPartialCheckpoint with duplicates.
     */
    @Test
    public void testPartialCheckpointDup()
        throws DatabaseException, IOException {

        dups = true;
        testPartialCheckpoint();
    }

    /**
     * Tests that deleting a subtree (by deleting the last LN in a BIN) is
     * counted correctly.
     */
    @Test
    public void testDelete()
        throws DatabaseException, IOException {

        openAndWriteDatabase();
        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);

        /* Close normally and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(true);
        openEnv();
        txn = env.beginTransaction(null, null);
        cursor = db.openCursor(txn, null);

        /* Position cursor to load BIN and IN. */
        cursor.getSearchKey(keyEntry, dataEntry, null);

        /* Expect BIN and IN are still not obsolete. */
        assertEquals(binFile, getBINFile(cursor));
        assertEquals(inFile, getINFile(cursor));
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);

        /*
         * Add records until we move to the next BIN, so that the compressor
         * would not need to delete the root in order to delete the BIN.
         */
        if (dups) {
            int dataVal = 1;
            while (binFile == getBINFile(cursor)) {
                dataVal += 1;
                IntegerBinding.intToEntry(dataVal, dataEntry);
                cursor.put(keyEntry, dataEntry);
            }
        } else {
            int keyVal = 0;
            while (binFile == getBINFile(cursor)) {
                keyVal += 1;
                IntegerBinding.intToEntry(keyVal, keyEntry);
                cursor.put(keyEntry, dataEntry);
            }
        }
        binFile = getBINFile(cursor);
        inFile = getINFile(cursor);

        /* Delete all records in the last BIN. */
        while (binFile == getBINFile(cursor)) {
            cursor.delete();
            cursor.getLast(keyEntry, dataEntry, null);
        }

        /* Compressor daemon is not running -- they're not obsolete yet. */
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);

        /* Close cursor and compress. */
        cursor.close();
        txn.commit();
        env.compress();

        /* Without a checkpoint, the deleted nodes are not obsolete yet. */
        expectObsolete(binFile, false);
        expectObsolete(inFile, false);

        /*
         * Checkpoint to flush dirty INs and count pending obsolete LSNs.
         * Then expect BIN and IN to be obsolete.
         */
        env.checkpoint(forceConfig);
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);

        /* Close with partial checkpoint and reopen. */
        performPartialCheckpoint(true); // truncateUtilizationInfo
        openEnv();

        /*
         * Expect both files to be obsolete after recovery, because the
         * FileSummaryLN and MapLN were written prior to the checkpoint during
         * compression.
         */
        expectObsolete(binFile, true);
        expectObsolete(inFile, true);

        /*
         * expectAccurateObsoleteLNCount is false because the deleted LN is not
         * counted obsolete correctly as described in RecoveryManager
         * redoUtilizationInfo.
         */
        closeEnv(true,   // doCheckpoint
                 false); // expectAccurateObsoleteLNCount
    }

    /**
     * Performs testDelete with duplicates.
     */
    @Test
    public void testDeleteDup()
        throws DatabaseException, IOException {

        dups = true;
        testDelete();
    }

    /**
     * Tests that truncating a database is counted correctly.
     * Tests recovery also.
     */
    @Test
    public void testTruncate()
        throws DatabaseException, IOException {

        /* Expect inaccurate LN sizes only if we force a tree walk. */
        final boolean expectAccurateObsoleteLNSize =
            !DatabaseImpl.forceTreeWalkForTruncateAndRemove;

        openAndWriteDatabase();
        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);

        /* Close normally and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(true,   // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 expectAccurateObsoleteLNSize);
        openEnv();
        db.close();
        db = null;
        /* Truncate. */
        txn = env.beginTransaction(null, null);
        env.truncateDatabase(txn, DB_NAME, false /* returnCount */);
        truncateOrRemoveDone = true;
        txn.commit();

        /*
         * Expect BIN and IN are obsolete.  Do not check DbFileSummary when we
         * truncate/remove, since the old DatabaseImpl is gone.
         */
        expectObsolete(binFile, true, false /*checkDbFileSummary*/);
        expectObsolete(inFile, true, false /*checkDbFileSummary*/);

        /* Close with partial checkpoint and reopen. */
        performPartialCheckpoint(true,   // truncateUtilizationInfo
                                 true,   // expectAccurateObsoleteLNCount
                                 expectAccurateObsoleteLNSize);
        openEnv();

        /* Expect BIN and IN are counted obsolete during recovery. */
        expectObsolete(binFile, true, false /*checkDbFileSummary*/);
        expectObsolete(inFile, true, false /*checkDbFileSummary*/);

        /*
         * expectAccurateObsoleteLNSize is false because the size of the
         * deleted NameLN is not counted during recovery, as with other
         * abortLsns as described in RecoveryManager redoUtilizationInfo.
         */
        closeEnv(true,   // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 false); // expectAccurateObsoleteLNSize
    }

    /**
     * Tests that truncating a database is counted correctly.
     * Tests recovery also.
     */
    @Test
    public void testRemove()
        throws DatabaseException, IOException {

        /* Expect inaccurate LN sizes only if we force a tree walk. */
        final boolean expectAccurateObsoleteLNSize =
            !DatabaseImpl.forceTreeWalkForTruncateAndRemove;

        openAndWriteDatabase();
        long binFile = getBINFile(cursor);
        long inFile = getINFile(cursor);

        /* Close normally and reopen. */
        cursor.close();
        txn.commit();
        closeEnv(true,   // doCheckpoint
                 true,   // expectAccurateObsoleteLNCount
                 expectAccurateObsoleteLNSize);
        openEnv();

        /* Remove. */
        db.close();
        db = null;
        txn = env.beginTransaction(null, null);
        env.removeDatabase(txn, DB_NAME);
        truncateOrRemoveDone = true;
        txn.commit();

        /*
         * Expect BIN and IN are obsolete.  Do not check DbFileSummary when we
         * truncate/remove, since the old DatabaseImpl is gone.
         */
        expectObsolete(binFile, true, false /*checkDbFileSummary*/);
        expectObsolete(inFile, true, false /*checkDbFileSummary*/);

        /* Close with partial checkpoint and reopen. */
        performPartialCheckpoint(true,   // truncateUtilizationInfo
                                 true,   // expectAccurateObsoleteLNCount
                                 expectAccurateObsoleteLNSize);
        openEnv();

        /* Expect BIN and IN are counted obsolete during recovery. */
        expectObsolete(binFile, true, false /*checkDbFileSummary*/);
        expectObsolete(inFile, true, false /*checkDbFileSummary*/);

        /*
         * expectAccurateObsoleteLNCount is false because the deleted NameLN is
         * not counted obsolete correctly as described in RecoveryManager
         * redoUtilizationInfo.
         */
        closeEnv(true,   // doCheckpoint
                 false); // expectAccurateObsoleteLNCount
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

        /*
         * We cannot use forceTreeWalkForTruncateAndRemove with embedded LNs
         * because we don't have the lastLoggedFile info in the BIN slots.
         * This info is required by the SortedLSNTreeWalker used in
         * DatabaseImpl.finishDeleteProcessing().
         */
        if (embeddedLNs) {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        }

        try {
            testTruncate();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    @Test
    public void testRemoveForceTreeWalk()
        throws Exception {

        /*
         * We cannot use forceTreeWalkForTruncateAndRemove with embedded LNs
         * because we don't have the lastLoggedFile info in the BIN slots.
         * This info is required by the SortedLSNTreeWalker used in
         * DatabaseImpl.finishDeleteProcessing().
         */
        if (embeddedLNs) {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        }

        try {
            testRemove();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    private void expectObsolete(long file, boolean obsolete) {
        expectObsolete(file, obsolete, true /*checkDbFileSummary*/);
    }

    private void expectObsolete(long file,
                                boolean obsolete,
                                boolean checkDbFileSummary) {
        FileSummary fileSummary = getFileSummary(file);
        assertEquals("totalINCount",
                     1, fileSummary.totalINCount);
        assertEquals("obsoleteINCount",
                     obsolete ? 1 : 0, fileSummary.obsoleteINCount);

        if (checkDbFileSummary) {
            DbFileSummary dbFileSummary = getDbFileSummary(file);
            assertEquals("db totalINCount",
                         1, dbFileSummary.totalINCount);
            assertEquals("db obsoleteINCount",
                         obsolete ? 1 : 0, dbFileSummary.obsoleteINCount);
        }
    }

    private void expectObsolete(long file, int obsoleteCount) {
        FileSummary fileSummary = getFileSummary(file);
        assertEquals("totalINCount",
                     1, fileSummary.totalINCount);
        assertEquals("obsoleteINCount",
                     obsoleteCount, fileSummary.obsoleteINCount);

        DbFileSummary dbFileSummary = getDbFileSummary(file);
        assertEquals("db totalINCount",
                     1, dbFileSummary.totalINCount);
        assertEquals("db obsoleteINCount",
                     obsoleteCount, dbFileSummary.obsoleteINCount);
    }

    private long getINFile(Cursor cursor)
        throws DatabaseException {

        IN in = TestUtils.getIN(TestUtils.getBIN(cursor));
        long lsn = in.getLastLoggedLsn();
        assertTrue(lsn != DbLsn.NULL_LSN);
        return DbLsn.getFileNumber(lsn);
    }

    private long getBINFile(Cursor cursor) {
        long lsn = TestUtils.getBIN(cursor).getLastLoggedLsn();
        assertTrue(lsn != DbLsn.NULL_LSN);
        return DbLsn.getFileNumber(lsn);
    }

    private long getFullBINFile(Cursor cursor) {
        long lsn = TestUtils.getBIN(cursor).getLastFullLsn();
        assertTrue(lsn != DbLsn.NULL_LSN);
        return DbLsn.getFileNumber(lsn);
    }

    private long getDeltaBINFile(Cursor cursor) {
        long lsn = TestUtils.getBIN(cursor).getLastDeltaLsn();
        if (lsn == DbLsn.NULL_LSN) {
            return -1;
        }
        return DbLsn.getFileNumber(lsn);
    }

    /**
     * Returns the utilization summary for a given log file.
     */
    private FileSummary getFileSummary(long file) {
        return envImpl.getUtilizationProfile()
                                    .getFileSummaryMap(true)
                                    .get(new Long(file));
    }

    /**
     * Returns the per-database utilization summary for a given log file.
     */
    private DbFileSummary getDbFileSummary(long file) {
        return dbImpl.getDbFileSummary
            (new Long(file), false /*willModify*/);
    }

    private void performPartialCheckpoint(boolean truncateUtilizationInfo)
        throws DatabaseException, IOException {

        performPartialCheckpoint(truncateUtilizationInfo,
                                 true,  // expectAccurateObsoleteLNCount
                                 true); // expectAccurateObsoleteLNSize
    }

    private void performPartialCheckpoint(boolean truncateUtilizationInfo,
                                          boolean
                                          expectAccurateObsoleteLNCount)
        throws DatabaseException, IOException {

        performPartialCheckpoint(truncateUtilizationInfo,
                                 expectAccurateObsoleteLNCount,
                                 expectAccurateObsoleteLNCount);
    }

    /**
     * Performs a checkpoint and truncates the log before the last CkptEnd.  If
     * truncateUtilizationInfo is true, truncates before the FileSummaryLNs
     * that appear at the end of the checkpoint.  The environment should be
     * open when this method is called, and it will be closed when it returns.
     */
    private void performPartialCheckpoint
                    (boolean truncateUtilizationInfo,
                     boolean expectAccurateObsoleteLNCount,
                     boolean expectAccurateObsoleteLNSize)
        throws DatabaseException, IOException {

        /* Do a normal checkpoint. */
        env.checkpoint(forceConfig);
        long eofLsn = envImpl.getFileManager().getNextLsn();
        long lastLsn = envImpl.getFileManager().getLastUsedLsn();

        /* Searching backward from end, find last CkptEnd. */
        SearchFileReader searcher =
            new SearchFileReader(envImpl, 1000, false, lastLsn, eofLsn,
                                 LogEntryType.LOG_CKPT_END);
        assertTrue(searcher.readNextEntry());
        long ckptEnd = searcher.getLastLsn();
        long truncateLsn = ckptEnd;

        if (truncateUtilizationInfo) {

            /* Searching backward from CkptEnd, find last CkptStart. */
            searcher =
                new SearchFileReader(envImpl, 1000, false, ckptEnd, eofLsn,
                                     LogEntryType.LOG_CKPT_START);
            assertTrue(searcher.readNextEntry());
            long ckptStart = searcher.getLastLsn();

            /*
             * Searching forward from CkptStart, find first MapLN for a user 
             * database (ID GT 2), or if none, the last MapLN for a non-user
             * database.  MapLNs are written after writing root INs and before
             * all FileSummaryLNs.  This will find the position at which to
             * truncate all MapLNs and FileSummaryLNs, but not INs below the
             * mapping tree.
             */
            searcher = new SearchFileReader(envImpl, 1000, true,
                                            ckptStart, eofLsn,
                                            LogEntryType.LOG_MAPLN);
            while (searcher.readNextEntry()) {
                MapLN mapLN = (MapLN) searcher.getLastObject();
                truncateLsn = searcher.getLastLsn();
                if (mapLN.getDatabase().getId().getId() > 2) {
                    break;
                }
            }
        }

        /*
         * Close without another checkpoint, although it doesn't matter since
         * we would truncate before it.
         */
        closeEnv(false, // doCheckpoint
                 expectAccurateObsoleteLNCount,
                 expectAccurateObsoleteLNSize);

        /* Truncate the log. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        EnvironmentImpl cmdEnv =
            DbInternal.getNonNullEnvImpl(new Environment(envHome, envConfig));
        cmdEnv.getFileManager().truncateLog(DbLsn.getFileNumber(truncateLsn),
                                            DbLsn.getFileOffset(truncateLsn));
        cmdEnv.abnormalClose();
    }
}
