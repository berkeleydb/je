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

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Reproduces a bug [#18937] where the abortLsn is set incorrectly after a slot
 * is reused, and verifies the fix.
 *
 * Problem scenario is as follows.  LN-X-Y is used to mean an LN with key X and
 * data Y.
 *
 *  1/100 LN-A-1, inserted, txn 1
 *  1/200 Commit, txn 1
 *  2/100 LN-A-1, deleted, txn 2
 *  2/200 LN-A-2, inserted, txn 2
 *  2/300 LN-A-3, inserted, txn 2
 *  2/400 Abort, txn 2
 *  2/500 DBIN with one entry, mainKey:A, dbinKey:2, data:1, lsn:1/100
 *
 * The DBIN's key is wrong.  It is 2 and should be 1.  The symptoms are:
 *
 *  + A user Btree lookup (e.g., getSearchBoth) on A-2 will find the record,
 *    but should not; the data returned is 1.
 *
 *  + A lookup on A-1 will return NOTFOUND, but should return the record.
 *
 *  + When log file 1 is cleaned, the cleaner will not migrate 1/100 because it
 *    can't find A-1 in the Btree and assumes it is obsolete.  After the file
 *    is deleted, the user will get LogFileNotFound when accessing the record
 *    in the DBIN.
 *
 * In the user's log there is no FileSummaryLN indicating that LSN 1/100 is
 * obsolete, implying that the cleaner did a Btree lookup and did not find A-1.
 *
 * Cause: The insertion of LN-A-2 incorrectly reuses the BIN slot for LN-A-1,
 * because we don't check the data (we only check the key) when reusing a slot
 * in a BIN, even though the database is configured for duplicates.
 *
 * After 1/100:
 *   BIN
 *    |
 *  LN-A-1
 *
 * After 2/100:
 *   BIN
 *    |
 *  LN-A-1 (deleted)
 *
 * After 2/200:
 *   BIN
 *    |
 *  LN-A-2
 *
 * After 2/300:
 *      BIN
 *       |
 *      DIN
 *       |
 *     DBIN
 *     /   \
 * LN-A-2  LN-A-3
 *
 * The problem in the last two pictures is that the DBIN slot for LN-A-2 has
 * the wrong abortLsn: 1/100.
 *
 * After 2/400:
 *      BIN
 *       |
 *      DIN
 *       |
 *     DBIN
 *       |
 *    LN-A-1 (with wrong key: LN-A-2)
 *
 * Now we have the situation that causes the symptoms listed above.
 *
 * The problem only occurs if:
 * + the prior committed version of the record is not deleted,
 * + a txn deletes the sole member of a dup set, then adds at least two more
 *   members of that dup set with different data, then aborts.
 *
 * The fix is to create the dup tree at 2/300, rather than reusing the slot.
 * The new rule is, in a duplicates database, a slot cannot be reused if the
 * new data and old data are not equal.  Unequal data means logically different
 * records, and slot reuse isn't appropriate.
 *
 * With the fix after 2/200:
 *           BIN
 *            |
 *           DIN
 *            |
 *          DBIN
 *    /              \
 * LN-A-1 (deleted)   LN-A-2
 *
 * With the fix after 2/300:
 *           BIN
 *            |
 *           DIN
 *            |
 *          DBIN
 *    /              \       \
 * LN-A-1 (deleted)   LN-A-2  LN-A-3
 *
 * With the fix after 2/400:
 *      BIN
 *       |
 *      DIN
 *       |
 *     DBIN
 *       |
 *    LN-A-1 (with correct key)
 *
 * I don't believe a problem occurs when the txn commits rather than aborts.
 * The abortLsn will be wrong (with the bug), but that won't cause harm.  The
 * abortLsn is counted obsolete during commit, but that happens to be correct.
 *
 * And I don't believe a problem occurs when the prior version of the record is
 * deleted, as below.
 *
 *  1/100 LN-A-1, deleted, txn 1
 *  1/200 Commit, txn 1
 *  2/200 LN-A-2, inserted, txn 2
 *  2/300 LN-A-3, inserted, txn 2
 *  2/400 Abort, txn 2
 *  2/500 DBIN with one entry, mainKey:A, dbinKey:2, lsn:1/100, KD:true
 *
 * With the bug, the end result is a slot with a DBIN key and LSN that should
 * not go together.  But since the KD flag will be set on the slot, we'll never
 * try to use the LSN for a user operation.  So I think it's safe.
 *
 * This last fact is leveraged in the bug fix. See testDiffTxnAbort, which
 * tests the case above and references the relevant part of the bug fix.
 */
public class DupSlotReuseTest extends TestBase {

    private static final String DB_NAME = "foo";

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private File envHome;
    private Environment env;
    private Database db;

    public DupSlotReuseTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }

        db = null;
        env = null;
        envHome = null;
    }

    private void openEnv() {

        EnvironmentConfig config = TestUtils.initEnvConfig();
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
        env = new Environment(envHome, config);

        openDb();
    }

    private void openDb() {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    private void closeEnv() {

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
     * Tests the failure reported by the user in [#18937].
     */
    @Test
    public void testSameTxnAbort() {

        openEnv();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        OperationStatus status;

        /* Put {1,1}, auto-commit. */
        IntegerBinding.intToEntry(1, key);
        IntegerBinding.intToEntry(1, data);
        status = db.putNoOverwrite(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);

        /* Flip a couple files so file 0 (containing {1,1}) can be cleaned. */
        envImpl.forceLogFileFlip();
        envImpl.forceLogFileFlip();

        /*
         * Delete {1,1}, put {1,2} and {1,3}, then abort.  With the bug, {1,2}
         * will reuse the slot for {1,1}.  This is incorrect.  When the txn
         * aborts, the undo of {1,2} will set the LSN in that slot back to the
         * LSN of {1,1}.  But the key in the DBIN will incorrectly be {2},
         * while it should be {1}.
         */
        Transaction txn = env.beginTransaction(null, null);
        status = db.delete(txn, key);
        assertSame(OperationStatus.SUCCESS, status);
        IntegerBinding.intToEntry(2, data);
        status = db.putNoDupData(txn, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        IntegerBinding.intToEntry(3, data);
        status = db.putNoDupData(txn, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        txn.abort();

        /*
         * Get first dup record for key {1}.  In spite of the bug, the first
         * dup for key {1} is returned correctly as {1,1}.  With the bug,
         * although the DBIN key is incorrectly {2}, the data returned is {1}.
         */
        status = db.get(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(data));

        /*
         * Get dup record with key/data of {1,1}.  The bug causes the following
         * operation to return NOTFOUND, because it looks up {1,1} and does not
         * find a DBIN key of {1}.
         */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getSearchBoth(key, data, null);
        /* Comment out assertions below to see LogFileNotFound. */
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(1, cursor.count());
        cursor.close();

        /*
         * If the above assertions are commented out, the bug will cause a
         * LogFileNotFound when file 0 is cleaned and deleted, and then we
         * attempt to fetch {1,1}.  With the bug, the cleaner will lookup {1,1}
         * in the Btree (just as getSearchBoth does above) and it will not be
         * found, so the LN will not be migrated.  After file 0 is deleted,
         * when we evict and explicitly fetch the LN with a cursor, the
         * getCurrent call below will cause LogFileNotFound.
         */
        envImpl.getCleaner().
                getFileSelector().
                injectFileForCleaning(new Long(0));
        long nCleaned = env.cleanLog();
        assertTrue(nCleaned > 0);
        env.checkpoint(forceConfig);
        cursor = db.openCursor(null, null);
        status = cursor.getSearchKey(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(data));
        DbInternal.getCursorImpl(cursor).evictLN();
        status = cursor.getCurrent(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        cursor.close();

        closeEnv();
    }

    /**
     * Checks that the failure reported in [#18937] does not occur when the
     * prior committed version of the record is deleted.  This did not fail,
     * even before the bug fix.  This confirms what the comment in Tree.insert
     * says:
     *--------------------
     * 2. The last committed version of the record is deleted.
     *    In this case it may be impossible to get the data
     *    (the prior version may be cleaned), so no comparison
     *    is possible.  Fortunately, reusing a slot when the
     *    prior committed version is deleted won't cause a
     *    problem if the txn aborts.  Even though the abortLsn
     *    may belong to a different dup key, the residual slot
     *    will have knownDeleted set, i.e., will be ignored.
     *--------------------
     */
    @Test
    public void testDiffTxnAbort() {

        openEnv();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        OperationStatus status;

        /* Put and delete {1,1}, auto-commit. */
        IntegerBinding.intToEntry(1, key);
        IntegerBinding.intToEntry(1, data);
        status = db.putNoOverwrite(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        status = db.delete(null, key);
        assertSame(OperationStatus.SUCCESS, status);

        /* Flip a couple files so file 0 (containing {1,1}) can be cleaned. */
        envImpl.forceLogFileFlip();
        envImpl.forceLogFileFlip();

        /* See testSameTxnAbort. */
        Transaction txn = env.beginTransaction(null, null);
        IntegerBinding.intToEntry(2, data);
        status = db.putNoDupData(txn, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        IntegerBinding.intToEntry(3, data);
        status = db.putNoDupData(txn, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        txn.abort();

        /* Confirm that we roll back to the deleted record. */
        status = db.get(null, key, data, null);
        assertSame(OperationStatus.NOTFOUND, status);

        /*
         * Confirm that the file containing the deleted record can be cleaned
         * and deleted.
         */
        envImpl.getCleaner().
                getFileSelector().
                injectFileForCleaning(new Long(0));
        long nCleaned = env.cleanLog();
        assertTrue(nCleaned > 0);
        env.checkpoint(forceConfig);
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getSearchKey(key, data, null);
        assertSame(OperationStatus.NOTFOUND, status);
        DbInternal.getCursorImpl(cursor).evictLN();
        status = cursor.getNext(key, data, null);
        assertSame(OperationStatus.NOTFOUND, status);
        cursor.close();

        closeEnv();
    }
}
