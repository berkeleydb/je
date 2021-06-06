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

package com.sleepycat.je.tree;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Because LNs no longer have node IDs, the bug this test was checking is no
 * longer applicable.  However, the test is generic and should run.
 *
 * Original description
 * --------------------
 * Reproduce a bug where fetchEntry rather than fetchEntryIgnoreKnownDeleted
 * was being called when searching the duplicate tree by LN node ID during
 * recovery.
 *
 * The trick is to create a DBIN with a KnownDeleted flag set on an entry.  And
 * to cause recovery to search that DBIN by node ID during redo of a deleted
 * LN.  This deleted LN log entry must not have any data -- it must have been
 * deleted before creation of the dup tree as in SR 8984.
 *
 * In addition, the deleted LN must appear after the entries with KnownDeleted
 * set in the BIN, otherwise the search by node ID will find the LN before
 * it encounters a KnownDeleted entry.

 * The sequence in the test is as follows.  I'm not positive this was the same
 * sequence as seen by the user, since the user did not send their logs, but
 * I believe the bug fix is general enough to cover similar cases.
 *
 * 1) Insert {A, C} (LN with key A, data C) in T1.
 * 2) Delete {A, C} in T1.  The LN log entry will not have any data.
 * 3) Commit T1 so these log entries will be replayed during recovery redo.
 * 4) Insert {A, A} and {A, B} in T2.
 * 5) Abort T2 so that the KnownDeleted flag will be set on these DBIN entries
 * during recovery.
 * 6) Close without a checkpoint and recover.  When replaying the deleted LN
 * {A, C}, we don't have a dup key because it was deleted before the dup tree
 * was created.  So we search the dup tree by LN node ID.  Calling fetchEntry
 * on {A, A} (or {A, B}) throws an exception because KnownDeleted is set.  We
 * neglected to check KnownDeleted.
 */
public class SR13034Test extends DualTestCase {

    private final File envHome;
    private Environment env;
    private Database db;

    public SR13034Test() {
        envHome = SharedTestUtils.getTestDir();
    }

    private void open()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        /* Do not run the daemons to avoid timing considerations. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        env = create(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void close()
        throws DatabaseException {

        db.close();
        db = null;

        close(env);
        env = null;
    }

    @Test
    public void testSR13034()
        throws DatabaseException {

        open();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;
        Transaction txn;

        /*
         * Insert {A, C}, then delete it.  No dup tree has been created, so
         * this logs a deleted LN with no data.
         */
        txn = env.beginTransaction(null, null);
        StringBinding.stringToEntry("A", key);
        StringBinding.stringToEntry("C", data);
        status = db.putNoOverwrite(txn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);
        status = db.delete(txn, key);
        assertEquals(OperationStatus.SUCCESS, status);
        txn.commit();

        /*
         * Insert {A, A}, {A, B}, which creates a dup tree.  Then abort to set
         * KnownDeleted on these entries.
         */
        txn = env.beginTransaction(null, null);
        StringBinding.stringToEntry("A", key);
        StringBinding.stringToEntry("A", data);
        status = db.putNoDupData(txn, key, data);
        StringBinding.stringToEntry("A", key);
        StringBinding.stringToEntry("B", data);
        status = db.putNoDupData(txn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);
        txn.abort();

        /*
         * Close without a checkpoint and recover.  Before the bug fix, the
         * recovery would throw DatabaseException "attempt to fetch a deleted
         * entry".
         */
        db.close();
        closeNoCheckpoint(env);
        open();

        close();
    }
}
