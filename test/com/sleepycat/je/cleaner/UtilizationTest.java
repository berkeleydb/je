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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogSource;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Test utilization counting of LNs.
 */
@RunWith(Parameterized.class)
public class UtilizationTest extends CleanerTestBase {

    private static final String DB_NAME = "foo";

    private static final String OP_NONE = "op-none";
    private static final String OP_CHECKPOINT = "op-checkpoint";
    private static final String OP_RECOVER = "op-recover";
    //private static final String[] OPERATIONS = { OP_NONE, };
    //*
    private static final String[] OPERATIONS = { OP_NONE,
                                                 OP_CHECKPOINT,
                                                 OP_RECOVER,
                                                 OP_RECOVER };
    //*/

    /*
     * Set fetchObsoleteSize=true only for the second OP_RECOVER test.
     * We check that OP_RECOVER works with without fetching, but with fetching
     * we check that all LN sizes are counted.
     */
    private static final boolean[] FETCH_OBSOLETE_SIZE = { false,
                                                           false,
                                                           false,
                                                           true };

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private EnvironmentImpl envImpl;
    private Database db;
    private DatabaseImpl dbImpl;
    private boolean dups = false;
    private DatabaseEntry keyEntry = new DatabaseEntry();
    private DatabaseEntry dataEntry = new DatabaseEntry();
    private final String operation;
    private long lastFileSeen;
    private final boolean fetchObsoleteSize;
    private boolean truncateOrRemoveDone;

    private boolean embeddedLNs = false;

    public UtilizationTest (String op, boolean fetch) {
        this.operation = op;
        this.fetchObsoleteSize = fetch;
        customName = this.operation + (fetchObsoleteSize ? "fetch" : "");
    }

    @Parameters
    public static List<Object[]> genParams() {
        int i = 0;
        List<Object[]> list = new ArrayList<Object[]>();
        for (String operation : OPERATIONS) {
            list.add(new Object[] {operation, FETCH_OBSOLETE_SIZE[i]});
            i++;
        }
        return list;
    }

    @After
    public void tearDown() 
        throws Exception {

        super.tearDown();
        db = null;
        dbImpl = null;
        envImpl = null;
        keyEntry = null;
        dataEntry = null;
    }

    /**
     * Opens the environment and database.
     */
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
        /* Use a tiny log file size to write one LN per file. */
        config.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                              Integer.toString(64));
        /* With tiny files we can't log expiration profile records. */
        DbInternal.setCreateEP(config, false);

        /* Obsolete LN size counting is optional per test. */
        if (fetchObsoleteSize) {
            config.setConfigParam
                (EnvironmentParams.CLEANER_FETCH_OBSOLETE_SIZE.getName(),
                 "true");
        }

        if (envMultiSubDir) {
            config.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, DATA_DIRS + "");
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

    /**
     * Closes the environment and database.
     */
    private void closeEnv(boolean doCheckpoint)
        throws DatabaseException {

        /*
         * We pass expectAccurateDbUtilization as false when
         * truncateOrRemoveDone, because the database utilization info for that
         * database is now gone.
         */
        VerifyUtils.verifyUtilization
            (envImpl,
             true, // expectAccurateObsoleteLNCount
             expectObsoleteLNSizeCounted(),
             !truncateOrRemoveDone); // expectAccurateDbUtilization

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            envImpl.close(doCheckpoint);
            env = null;
        }
    }

    @Test
    public void testReuseSlotAfterDelete()
        throws DatabaseException {

        openEnv();

        /* Insert and delete without compress to create a knownDeleted slot. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        long file1 = doDelete(0, txn);
        txn.commit();

        /* Insert key 0 to reuse the knownDeleted slot. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, txn);
        /* Delete and insert to reuse deleted slot in same txn. */
        long file3 = doDelete(0, txn);
        long file4 = doPut(0, txn);
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);
        expectObsolete(file4, embeddedLNs);

        closeEnv(true);
    }

    @Test
    public void testReuseKnownDeletedSlot()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and abort to create a knownDeleted slot.  */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        txn.abort();

        /* Insert key 0 to reuse the knownDeleted slot. */
        txn = env.beginTransaction(null, null);
        long file1 = doPut(0, txn);
        txn.commit();
        performRecoveryOperation();

        /* Verify that file0 is still obsolete. */
        expectObsolete(file0, true);
        expectObsolete(file1, embeddedLNs);

        closeEnv(true);
    }

    @Test
    public void testReuseKnownDeletedSlotAbort()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and abort to create a knownDeleted slot.  */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        txn.abort();

        /* Insert key 0 to reuse the knownDeleted slot, and abort. */
        txn = env.beginTransaction(null, null);
        long file1 = doPut(0, txn);
        txn.abort();
        performRecoveryOperation();

        /* Verify that file0 is still obsolete. */
        expectObsolete(file0, true);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testReuseKnownDeletedSlotDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Insert {0, 2} and abort to create a knownDeleted slot. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, 2, txn); // 3rd LN
        txn.abort();

        /* Insert {0, 2} to reuse the knownDeleted slot. */
        txn = env.beginTransaction(null, null);
        long file3 = doPut(0, 2, txn); // 4th LN
        txn.commit();
        performRecoveryOperation();

        /* Verify that file2 is still obsolete and only counted once. */
        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testReuseKnownDeletedSlotDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Insert {0, 2} and abort to create a knownDeleted slot. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, 2, txn); // 3rd LN
        txn.abort();

        /* Insert {0, 2} to reuse the knownDeleted slot, then abort. */
        txn = env.beginTransaction(null, null);
        long file3 = doPut(0, 2, txn); // 4th LN
        txn.abort();
        performRecoveryOperation();

        /* Verify that file2 is still obsolete and only counted once. */
        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testInsert()
        throws DatabaseException {

        openEnv();

        /* Insert key 0. */
        long file0 = doPut(0, true);
        performRecoveryOperation();

        /* Expect that LN is not obsolete. */
        FileSummary fileSummary = getFileSummary(file0);

        assertEquals(1, fileSummary.totalLNCount);

        if (embeddedLNs) {
            assertEquals(1, fileSummary.obsoleteLNCount);
        } else {
            assertEquals(0, fileSummary.obsoleteLNCount);
        }

        DbFileSummary dbFileSummary = getDbFileSummary(file0);

        assertEquals(1, dbFileSummary.totalLNCount);

        if (embeddedLNs) {
            assertEquals(1, fileSummary.obsoleteLNCount);
        } else {
            assertEquals(0, dbFileSummary.obsoleteLNCount);
        }

        closeEnv(true);
    }

    @Test
    public void testInsertAbort()
        throws DatabaseException {

        openEnv();

        /* Insert key 0. */
        long file0 = doPut(0, false);
        performRecoveryOperation();

        /* Expect that LN is obsolete. */
        FileSummary fileSummary = getFileSummary(file0);
        assertEquals(1, fileSummary.totalLNCount);
        assertEquals(1, fileSummary.obsoleteLNCount);

        DbFileSummary dbFileSummary = getDbFileSummary(file0);
        assertEquals(1, dbFileSummary.totalLNCount);
        assertEquals(1, dbFileSummary.obsoleteLNCount);

        closeEnv(true);
    }

    @Test
    public void testInsertDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert key 0 and a dup. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn);
        long file1 = doPut(0, 1, txn);
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true); // 1st LN
        expectObsolete(file1, true); // 2nd LN

        closeEnv(true);
    }

    @Test
    public void testInsertDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert key 0 and a dup. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn);
        long file2 = doPut(0, 1, txn);
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);  // 1st LN
        expectObsolete(file2, true);  // 2nd LN

        closeEnv(true);
    }

    @Test
    public void testUpdate()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Update key 0. */
        long file1 = doPut(0, true);
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, embeddedLNs);

        closeEnv(true);
    }

    @Test
    public void testUpdateAbort()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Update key 0 and abort. */
        long file1 = doPut(0, false);
        performRecoveryOperation();

        expectObsolete(file0, embeddedLNs);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Update {0, 0}. */
        txn = env.beginTransaction(null, null);
        long file3 = doUpdate(0, 0, txn); // 3rd LN
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Update {0, 0}. */
        txn = env.beginTransaction(null, null);
        long file3 = doUpdate(0, 0, txn); // 3rd LN
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testDelete()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Delete key 0. */
        long file1 = doDelete(0, true);
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testDeleteAbort()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Delete key 0 and abort. */
        long file1 = doDelete(0, false);
        performRecoveryOperation();

        expectObsolete(file0, embeddedLNs);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testDeleteDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Delete {0, 0} and abort. */
        txn = env.beginTransaction(null, null);
        long file2 = doDelete(0, 0, txn); // 3rd LN
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);

        closeEnv(true);
    }

    @Test
    public void testDeleteDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Delete {0, 0} and abort. */
        txn = env.beginTransaction(null, null);
        long file2 = doDelete(0, 0, txn); // 3rd LN
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);

        closeEnv(true);
    }

    @Test
    public void testInsertUpdate()
        throws DatabaseException {

        openEnv();

        /* Insert and update key 0. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        long file1 = doPut(0, txn);
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, embeddedLNs);

        closeEnv(true);
    }

    @Test
    public void testInsertUpdateAbort()
        throws DatabaseException {

        openEnv();

        /* Insert and update key 0. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        long file1 = doPut(0, txn);
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testInsertUpdateDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Insert and update {0, 2}. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, 2, txn);    // 3rd LN
        long file3 = doUpdate(0, 2, txn); // 4rd LN
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testInsertUpdateDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Insert and update {0, 2}. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, 2, txn);    // 3rd LN
        long file3 = doUpdate(0, 2, txn); // 4rd LN
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testInsertDelete()
        throws DatabaseException {

        openEnv();

        /* Insert and update key 0. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        long file1 = doDelete(0, txn);
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testInsertDeleteAbort()
        throws DatabaseException {

        openEnv();

        /* Insert and update key 0. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        long file1 = doDelete(0, txn);
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);

        closeEnv(true);
    }

    @Test
    public void testInsertDeleteDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Insert and delete {0, 2}. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, 2, txn);    // 3rd LN
        long file3 = doDelete(0, 2, txn); // 4rd LN
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testInsertDeleteDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Insert and delete {0, 2} and abort. */
        txn = env.beginTransaction(null, null);
        long file2 = doPut(0, 2, txn);    // 3rd LN
        long file3 = doDelete(0, 2, txn); // 4rd LN
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateUpdate()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Update key 0 twice. */
        Transaction txn = env.beginTransaction(null, null);
        long file1 = doPut(0, txn);
        long file2 = doPut(0, txn);
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, embeddedLNs);

        closeEnv(true);
    }

    @Test
    public void testUpdateUpdateAbort()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Update key 0 twice and abort. */
        Transaction txn = env.beginTransaction(null, null);
        long file1 = doPut(0, txn);
        long file2 = doPut(0, txn);
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, embeddedLNs);
        expectObsolete(file1, true);
        expectObsolete(file2, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateUpdateDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Update {0, 1} twice. */
        txn = env.beginTransaction(null, null);
        long file2 = doUpdate(0, 1, txn); // 3rd LN
        long file3 = doUpdate(0, 1, txn); // 4rd LN
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateUpdateDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Update {0, 1} twice and abort. */
        txn = env.beginTransaction(null, null);
        long file2 = doUpdate(0, 1, txn); // 3rd LN
        long file3 = doUpdate(0, 1, txn); // 4rd LN
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateDelete()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Update and delete key 0. */
        Transaction txn = env.beginTransaction(null, null);
        long file1 = doPut(0, txn);
        long file2 = doDelete(0, txn);
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateDeleteAbort()
        throws DatabaseException {

        openEnv();

        /* Insert key 0 and checkpoint. */
        long file0 = doPut(0, true);
        env.checkpoint(forceConfig);

        /* Update and delete key 0 and abort. */
        Transaction txn = env.beginTransaction(null, null);
        long file1 = doPut(0, txn);
        long file2 = doDelete(0, txn);
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, embeddedLNs);
        expectObsolete(file1, true);
        expectObsolete(file2, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateDeleteDup()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Update and delete {0, 1}. */
        txn = env.beginTransaction(null, null);
        long file2 = doUpdate(0, 1, txn); // 3rd LN
        long file3 = doDelete(0, 1, txn); // 4rd LN
        txn.commit();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testUpdateDeleteDupAbort()
        throws DatabaseException {

        dups = true;
        openEnv();

        /* Insert two key 0 dups and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, 0, txn); // 1st LN
        long file1 = doPut(0, 1, txn); // 2nd LN
        txn.commit();
        env.checkpoint(forceConfig);

        /* Update and delete {0, 1} and abort. */
        txn = env.beginTransaction(null, null);
        long file2 = doUpdate(0, 1, txn); // 3rd LN
        long file3 = doDelete(0, 1, txn); // 4rd LN
        txn.abort();
        performRecoveryOperation();

        expectObsolete(file0, true);
        expectObsolete(file1, true);
        expectObsolete(file2, true);
        expectObsolete(file3, true);

        closeEnv(true);
    }

    @Test
    public void testTruncate()
        throws DatabaseException {

        truncateOrRemove(true, true);
    }

    @Test
    public void testTruncateAbort()
        throws DatabaseException {

        truncateOrRemove(true, false);
    }

    @Test
    public void testRemove()
        throws DatabaseException {

        truncateOrRemove(false, true);
    }

    @Test
    public void testRemoveAbort()
        throws DatabaseException {

        truncateOrRemove(false, false);
    }

    /**
     */
    private void truncateOrRemove(boolean truncate, boolean commit)
        throws DatabaseException {

        openEnv();

        /*
         * We cannot use forceTreeWalkForTruncateAndRemove with embedded LNs
         * because we don't have the lastLoggedFile info in the BIN slots.
         * This info is required by the SortedLSNTreeWalker used in
         * DatabaseImpl.finishDeleteProcessing().
         */
        if (embeddedLNs) {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }

        /* Insert 3 keys and checkpoint. */
        Transaction txn = env.beginTransaction(null, null);
        long file0 = doPut(0, txn);
        long file1 = doPut(1, txn);
        long file2 = doPut(2, txn);
        txn.commit();
        env.checkpoint(forceConfig);

        /* Truncate. */
        txn = env.beginTransaction(null, null);
        if (truncate) {
            db.close();
            db = null;
            long count = env.truncateDatabase(txn, DB_NAME,
                                              true /* returnCount */);
            assertEquals(3, count);
        } else {
            db.close();
            db = null;
            env.removeDatabase(txn, DB_NAME);
        }
        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
        truncateOrRemoveDone = true;
        performRecoveryOperation();

        /*
         * Do not check DbFileSummary when we truncate/remove, since the old
         * DatabaseImpl is gone.
         */
        expectObsolete(file0,
                       commit || embeddedLNs,
                       !commit /*checkDbFileSummary*/);

        expectObsolete(file1,
                       commit || embeddedLNs,
                       !commit /*checkDbFileSummary*/);

        expectObsolete(file2,
                       commit || embeddedLNs,
                       !commit /*checkDbFileSummary*/);

        closeEnv(true);
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
    public void testRemoveAbortForceTreeWalk()
        throws Exception {

        DatabaseImpl.forceTreeWalkForTruncateAndRemove = true;
        try {
            testRemoveAbort();
        } finally {
            DatabaseImpl.forceTreeWalkForTruncateAndRemove = false;
        }
    }

    private void expectObsolete(long file, boolean obsolete)
        throws DatabaseException {

        expectObsolete(file, obsolete, true /*checkDbFileSummary*/);
    }

    private void expectObsolete(long file,
                                boolean obsolete,
                                boolean checkDbFileSummary)
        throws DatabaseException {

        FileSummary fileSummary = getFileSummary(file);
        assertEquals("totalLNCount",
                     1, fileSummary.totalLNCount);
        assertEquals("obsoleteLNCount",
                     obsolete ? 1 : 0, fileSummary.obsoleteLNCount);

        DbFileSummary dbFileSummary = getDbFileSummary(file);
        if (checkDbFileSummary) {
            assertEquals("db totalLNCount",
                         1, dbFileSummary.totalLNCount);
            assertEquals("db obsoleteLNCount",
                         obsolete ? 1 : 0, dbFileSummary.obsoleteLNCount);
        }

        if (obsolete) {
            if (expectObsoleteLNSizeCounted()) {
                assertTrue(fileSummary.obsoleteLNSize > 0);
                assertEquals(1, fileSummary.obsoleteLNSizeCounted);
                if (checkDbFileSummary) {
                    assertTrue(dbFileSummary.obsoleteLNSize > 0);
                    assertEquals(1, dbFileSummary.obsoleteLNSizeCounted);
                }
            }
            /* If we counted the size, make sure it is the actual LN size. */
            if (expectObsoleteLNSizeCounted() &&
                fileSummary.obsoleteLNSize > 0) {
                assertEquals(getLNSize(file), fileSummary.obsoleteLNSize);
            }
            if (checkDbFileSummary) {
                if (expectObsoleteLNSizeCounted() &&
                    dbFileSummary.obsoleteLNSize > 0) {
                    assertEquals(getLNSize(file), dbFileSummary.obsoleteLNSize);
                }
                assertEquals(fileSummary.obsoleteLNSize > 0,
                             dbFileSummary.obsoleteLNSize > 0);
            }
        } else {
            assertEquals(0, fileSummary.obsoleteLNSize);
            assertEquals(0, fileSummary.obsoleteLNSizeCounted);
            if (checkDbFileSummary) {
                assertEquals(0, dbFileSummary.obsoleteLNSize);
                assertEquals(0, dbFileSummary.obsoleteLNSizeCounted);
            }
        }
    }

    /**
     * If an LN is obsolete, expect the size to be counted unless we ran
     * recovery and we did NOT configure fetchObsoleteSize=true.  In that
     * case, the size may or may not be counted depending on how the redo
     * or undo was processed during recovery.
     */
    private boolean expectObsoleteLNSizeCounted() {
        return fetchObsoleteSize || !OP_RECOVER.equals(operation);
    }

    private long doPut(int key, boolean commit)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        long file = doPut(key, txn);
        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
        return file;
    }

    private long doPut(int key, Transaction txn)
        throws DatabaseException {

        return doPut(key, key, txn);
    }

    private long doPut(int key, int data, Transaction txn)
        throws DatabaseException {

        Cursor cursor = db.openCursor(txn, null);
        IntegerBinding.intToEntry(key, keyEntry);
        IntegerBinding.intToEntry(data, dataEntry);
        cursor.put(keyEntry, dataEntry);
        long file = getFile(cursor);
        cursor.close();
        return file;
    }

    private long doUpdate(int key, int data, Transaction txn)
        throws DatabaseException {

        Cursor cursor = db.openCursor(txn, null);
        IntegerBinding.intToEntry(key, keyEntry);
        IntegerBinding.intToEntry(data, dataEntry);
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchBoth(keyEntry, dataEntry, null));
        cursor.putCurrent(dataEntry);
        long file = getFile(cursor);
        cursor.close();
        return file;
    }

    private long doDelete(int key, boolean commit)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        long file = doDelete(key, txn);
        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
        return file;
    }

    private long doDelete(int key, Transaction txn)
        throws DatabaseException {

        Cursor cursor = db.openCursor(txn, null);
        IntegerBinding.intToEntry(key, keyEntry);
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchKey(keyEntry, dataEntry, null));
        cursor.delete();
        long file = getFile(cursor);
        cursor.close();
        return file;
    }

    private long doDelete(int key, int data, Transaction txn)
        throws DatabaseException {

        Cursor cursor = db.openCursor(txn, null);
        IntegerBinding.intToEntry(key, keyEntry);
        IntegerBinding.intToEntry(data, dataEntry);
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchBoth(keyEntry, dataEntry, null));
        cursor.delete();
        long file = getFile(cursor);
        cursor.close();
        return file;
    }

    /**
     * Checkpoint, recover, or do neither, depending on the configured
     * operation for this test.  Always compress to count deleted LNs.
     */
    private void performRecoveryOperation()
        throws DatabaseException {

        if (OP_NONE.equals(operation)) {
            /* Compress to count deleted LNs. */
            env.compress();
        } else if (OP_CHECKPOINT.equals(operation)) {
            /* Compress before checkpointing to count deleted LNs. */
            env.compress();
            env.checkpoint(forceConfig);
        } else if (OP_RECOVER.equals(operation)) {
            closeEnv(false);
            openEnv();
            /* Compress after recovery to count deleted LNs. */
            env.compress();
        } else {
            assert false : operation;
        }
    }

    /**
     * Gets the file of the LSN at the cursor position, using internal methods.
     * Also check that the file number is greater than the last file returned,
     * to ensure that we're filling a file every time we write.
     */
    private long getFile(Cursor cursor) {
        long file = CleanerTestUtils.getLogFile(cursor);
        assert file > lastFileSeen;
        lastFileSeen = file;
        return file;
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

    /**
     * Peek into the file to get the total size of the first entry past the
     * file header, which is known to be the LN log entry.
     */
    private int getLNSize(long file)
        throws DatabaseException {

        try {
            long offset = FileManager.firstLogEntryOffset();
            long lsn = DbLsn.makeLsn(file, offset);
            LogManager lm = envImpl.getLogManager();
            LogSource src = lm.getLogSource(lsn);
            ByteBuffer buf = src.getBytes(offset);
            LogEntryHeader header =
                new LogEntryHeader(buf, LogEntryType.LOG_VERSION, lsn);
            int size = header.getItemSize();
            src.release();
            return size + header.getSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ChecksumException e) {
            throw new RuntimeException(e);
        }
    }
}
