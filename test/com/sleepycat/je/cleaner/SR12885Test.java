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

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.je.CheckpointConfig;
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
import com.sleepycat.je.util.TestUtils;

/**
 * Because LNs no longer have node IDs and we now lock the LSN, the specific
 * bug this test was checking is no longer applicable.  However, a similar
 * situation could occur now, because the LSN changes not only during slot
 * reuse but every time we log the LN.  So we continue to run this test.
 *
 * Original description
 * --------------------
 * Reproduces a problem found in SR12885 where we failed to migrate a pending
 * LN if the slot was reused by an active transaction and that transaction was
 * later aborted.
 *
 * This bug can manifest as a LogNotFoundException.  However, there was another
 * bug that caused this bug to manifest sometimes as a NOTFOUND return value.
 * This secondary problem -- more sloppyness than a real bug -- was that the
 * PendingDeleted flag was not cleared during an abort.  If the PendingDeleted
 * flag is set, the low level fetch method will return null rather than
 * throwing a LogFileNotFoundException.  This caused a NOTFOUND in some cases.
 *
 * The sequence that causes the bug is:
 *
 * 1) The cleaner processes a file containing LN-A (node A) for key X.  Key X
 * is a non-deleted LN.
 *
 * 2) The cleaner sets the migrate flag on the BIN entry for LN-A.
 *
 * 3) In transaction T-1, LN-A is deleted and replaced by LN-B with key X,
 * reusing the same slot but assigning a new node ID.  At this point both node
 * IDs (LN-A and LN-B) are locked.
 *
 * 4) The cleaner (via a checkpoint or eviction that logs the BIN) tries to
 * migrate LN-B, the current LN in the BIN, but finds it locked.  It adds LN-B
 * to the pending LN list.
 *
 * 5) T-1 aborts, putting the LSN of LN-A back into the BIN slot.
 *
 * 6) In transaction T-2, LN-A is deleted and replaced by LN-C with key X,
 * reusing the same slot but assigning a new node ID.  At this point both node
 * IDs (LN-A and LN-C) are locked.
 *
 * 7) The cleaner (via a checkpoint or wakeup) processes the pending LN-B.  It
 * first gets a lock on node B, then does the tree lookup.  It finds LN-C in
 * the tree, but it doesn't notice that it has a different node ID than the
 * node it locked.
 *
 * 8) The cleaner sees that LN-C is deleted, and therefore no migration is
 * necessary -- this is incorrect.  It removes LN-B from the pending list,
 * allowing the cleaned file to be deleted.
 *
 * 9) T-2 aborts, putting the LSN of LN-A back into the BIN slot.
 *
 * 10) A fetch of key X will fail, since the file containing the LSN for LN-A
 * has been deleted.  If we didn't clear the PendingDeleted flag, this will
 * cause a NOTFOUND error instead of a LogFileNotFoundException.
 */
@RunWith(Parameterized.class)
public class SR12885Test extends CleanerTestBase {

    private static final String DB_NAME = "foo";

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private Database db;

    public SR12885Test(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }
    
    /**
     * Opens the environment and database.
     */
    private void openEnv()
        throws DatabaseException {

        EnvironmentConfig config = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(config);
        config.setTransactional(true);
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
        /* Use a small log file size to make cleaning more frequent. */
        config.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                              Integer.toString(1024));
        if (envMultiSubDir) {
            config.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                  DATA_DIRS + "");
        }
        env = new Environment(envHome, config);

        openDb();
    }

    /**
     * Opens that database.
     */
    private void openDb()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    /**
     * Closes the environment and database.
     */
    private void closeEnv()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    @Test
    public void testSR12885()
        throws DatabaseException {

        openEnv();

        final int COUNT = 10;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(TestUtils.getTestArray(0));
        OperationStatus status;

        /* Add some records, enough to fill a log file. */
        for (int i = 0; i < COUNT; i += 1) {
            key.setData(TestUtils.getTestArray(i));
            status = db.putNoOverwrite(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /*
         * Delete all but key 0, so the first file can be cleaned but key 0
         * will need to be migrated.
         */
        for (int i = 1; i < COUNT; i += 1) {
            key.setData(TestUtils.getTestArray(i));
            status = db.delete(null, key);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /*
         * Checkpoint and clean to set the migrate flag for key 0.  This must
         * be done when key 0 is not locked, so that it will not be put onto
         * the pending list yet.  Below we cause it to be put onto the pending
         * list with a different node ID.
         */
        env.checkpoint(forceConfig);
        int cleaned = env.cleanLog();
        assertTrue("cleaned=" + cleaned, cleaned > 0);

        /*
         * Using a transaction, delete then insert key 0, reusing the slot.
         * The insertion assigns a new node ID.  Don't abort the transaction
         * until after the cleaner migration is finished.
         */
        Transaction txn = env.beginTransaction(null, null);
        key.setData(TestUtils.getTestArray(0));
        status = db.delete(txn, key);
        assertEquals(OperationStatus.SUCCESS, status);
        status = db.putNoOverwrite(txn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);

        /*
         * Checkpoint again to perform LN migration.  LN migration will not
         * migrate key 0 because it is locked -- it will be put onto the
         * pending list.  But the LN put on the pending list will be the newly
         * inserted node, which has a different node ID than the LN that needs
         * to be migrated -- this is the first condition for the bug.
         */
        env.checkpoint(forceConfig);

        /*
         * Abort the transaction to revert to the original node ID for key 0.
         * Then perform a delete with a new transaction.  This makes the
         * current LN for key 0 deleted.
         */
        txn.abort();
        txn = env.beginTransaction(null, null);
        key.setData(TestUtils.getTestArray(0));
        status = db.delete(txn, key);
        assertEquals(OperationStatus.SUCCESS, status);

        /*
         * The current state of key 0 is that the BIN contains a deleted LN,
         * and that LN has a node ID that is different than the one in the
         * pending LN list.  This node is the one that needs to be migrated.
         *
         * Perform a checkpoint to cause pending LNs to be processed and then
         * delete the cleaned file.  When we process the pending LN, we'll lock
         * the pending LN's node ID (the one we inserted and aborted), which is
         * the wrong node ID.  We'll then examine the current LN, find it
         * deleted, and neglect to migrate the LN that needs to be migrated.
         * The error is that we don't lock the node ID of the current LN.
         *
         * Then abort the delete transaction.  That will revert the BIN entry
         * to the node we failed to migrate.  If we then try to fetch key 0,
         * we'll get LogNotFoundException.
         */
        env.checkpoint(forceConfig);
        txn.abort();
        status = db.get(null, key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);

        /* If we get this far without LogNotFoundException, it's fixed. */

        closeEnv();
    }
}
