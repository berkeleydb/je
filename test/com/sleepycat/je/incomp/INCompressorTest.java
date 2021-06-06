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

package com.sleepycat.je.incomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Test;

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
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Test that BIN compression occurs in the various ways it is supposed to.
 * <p>These are:</p>
 * <ul>
 * <li>transactional and non-transactional delete,</li>
 * <li>delete duplicates and non-duplicates,</li>
 * <li>removal of empty sub-trees (duplicates and non-duplicates),</li>
 * <li>compression of BIN for deleted DIN subtree.</li>
 * <li>removal of empty BIN after deleting a DIN subtree.</li>
 * <li>undo causes compression of inserted LN during abort and recovery,</li>
 * <li>redo causes compression of deleted LN during recovery,</li>
 * </ul>
 *
 * <p>Also test that compression retries occur after we attempt to compress but
 * cannot because:</p>
 * <ul>
 * <li>cursors are open on the BIN when the compressor dequeues them,</li>
 * <li>cursors are open when attempting to delete a sub-tree (dup and non-dup
 * are two separate code paths).</li>
 * <li>a deleted key is locked during compression (NOT TESTED - this is very
 * difficult to reproduce),</li>
 * </ul>
 *
 * <p>Possible problem:  When we attempt to delete a subtree because the BIN is
 * empty, we give up when NodeNotEmptyException is thrown by the search.
 * However, this is thrown not only when entries have been added but also when
 * there are cursors on the BIN; it seems like we should retry in the latter
 * case.  Or is it impossible to have a cursor on an empty BIN?</p>
 *
 * <p>We do not test here the last ditch effort to compress to make room in
 * IN.insertEntry1; that should never happen in theory, so I don't think it
 * is worthwhile to try to reproduce it.</p>
 *
 * <p>Note that when this test is run in replicated mode, for some reason
 * there are deleted FileSummaryDB LNs, causing BINs in the compressor queue,
 * and this throws off the test assertions. The brute force workaround here is
 * to call env.compress() one extra time in replicated mode.</p>
 */
public class INCompressorTest extends DualTestCase {
    
    private static final CheckpointConfig forceConfig;
    static {
        forceConfig = new CheckpointConfig();
        forceConfig.setForce(true);
    }
    private final File envHome;
    private Environment env;
    private Database db;
    private IN in;
    private BIN bin;
    /* Use high keys since we fill the first BIN with low keys. */
    private DatabaseEntry entry0 = new DatabaseEntry(new byte[] {0});
    private DatabaseEntry entry1 = new DatabaseEntry(new byte[] {1});
    private DatabaseEntry entry2 = new DatabaseEntry(new byte[] {2});
    private DatabaseEntry keyFound = new DatabaseEntry();
    private DatabaseEntry dataFound = new DatabaseEntry();

    public INCompressorTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown()
        throws Exception {

        super.tearDown();

        if (env != null) {
            env.close();
        }
    }

    @Test
    public void testDeleteTransactional()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys. */
        openAndInit(true, false);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Closing the cursor without commit does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Commit without calling compress does not compress. */
        txn.commit();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 1, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testDeleteTransactionalWithBinDeltas()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys, binDeltas. */
        openAndInit(true, false, true);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /*
         * Sync so that next operation dirties one slot only, which should be
         * logged as a delta.
         */
        env.sync();
        if (isReplicatedTest(getClass())) {
            env.compress();
        }
        checkINCompQueueSize(0);

        /* Compression does not occur when a cursor or txn is open. */
        status = cursor.delete();
        checkINCompQueueSize(0);
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);
        checkINCompQueueSize(0);

        /* Even with txn closed, compression is not queued. */
        txn.commit();
        checkBinEntriesAndCursors(bin, 2, 0);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);
        checkINCompQueueSize(0);

        /* Lazy compression finally occurs during a full sync (no delta). */
        bin.setProhibitNextDelta(true);
        env.sync();
        checkBinEntriesAndCursors(bin, 1, 0);
        checkINCompQueueSize(0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    /**
     * Ensures that deleted slots are re-inserted into a BIN when applying a
     * delta.  [#20737]
     */
    @Test
    public void testDeleteEvictFetchWithBinDeltas()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys, binDeltas. */
        openAndInit(true, false, true);
        OperationStatus status;

        /*
         * Sync so that next operation dirties one slot only, which should be
         * logged as a delta.
         */
        env.sync();
        if (isReplicatedTest(getClass())) {
            env.compress();
        }
        checkINCompQueueSize(0);

        /* Delete and hold lock with txn. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        status = cursor.putNoOverwrite(entry2, entry0);
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        cursor.close();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Evict BIN which should log delta. */
        bin.latch(CacheMode.UNCHANGED);
        DbInternal.getNonNullEnvImpl(env).getEvictor().doTestEvict
            (bin, EvictionSource.CACHEMODE);

        /* Fetch BIN which should not delete slot when applying delta. */
        cursor = db.openCursor(txn, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(entry0, keyFound);
        cursor.close();
        initInternalNodes();
        checkBinEntriesAndCursors(bin, 3, 0);

        txn.commit();

        closeEnv();
    }

    @Test
    public void testDeleteNonTransactional()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, false);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 1, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testDeleteNonTransactionalWithBinDeltas()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys, binDeltas. */
        openAndInit(false, false, true);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /*
         * Sync so that next operation dirties one slot only, which should be
         * logged as a delta.
         */
        env.sync();
        checkINCompQueueSize(0);

        /* Compression does not occur when a cursor is open. */
        status = cursor.delete();
        checkINCompQueueSize(0);
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);
        checkINCompQueueSize(0);

        /* Even with cursor closed, compression is not queued. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 2, 0);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);
        checkINCompQueueSize(0);

        /* Lazy compression finally occurs during a full sync (no delta). */
        bin.setProhibitNextDelta(true);
        env.sync();
        checkBinEntriesAndCursors(bin, 1, 0);
        checkINCompQueueSize(0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testDeleteDuplicate()
        throws DatabaseException {

        /* Non-transactional dups, 3 two-part keys. */
        openAndInit(false, true);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());
        checkBinEntriesAndCursors(bin, 2, 0);

        closeEnv();
    }

    @Test
    public void testRemoveEmptyBIN()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, false);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.getNext(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 0, 0);

        /* BIN is empty so parent entry should be gone also. */
        assertEquals(1, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testRemoveEmptyBINWithBinDeltas()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys, binDeltas. */
        openAndInit(false, false, true);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /*
         * Sync so that next operation dirties one slot only, which should be
         * logged as a delta.
         */
        env.sync();
        checkINCompQueueSize(0);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        checkINCompQueueSize(0);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);
        checkINCompQueueSize(0);

        /* Move to next. */
        status = cursor.getNext(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);

        /*
         * A second deletion will be queued, since we should not log a delta.
         * But it is not queued until cursor moves/closes, i.e., releases its
         * locks.  The same thing would apply to a txn commit.
         */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        checkINCompQueueSize(0);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);
        checkINCompQueueSize(0);

        /* Closing the cursor will queue the compression. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 2, 0);
        checkINCompQueueSize(1);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 0, 0);
        checkINCompQueueSize(0);

        /* BIN is empty so parent entry should be gone also. */
        assertEquals(1, in.getNEntries());

        closeEnv();
    }

    /**
     * DBINs are no longer used, but this test is retained for good measure.
     */
    @Test
    public void testRemoveEmptyDBIN()
        throws DatabaseException {

        /* Non-transactional dups, 3 two-part keys. */
        openAndInit(false, true);
        OperationStatus status;

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.getNext(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 1, 0);
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    /**
     * DBINs are no longer used, but this test is retained for good measure.
     */
    @Test
    public void testRemoveEmptyDBINandBIN()
        throws DatabaseException {

        /* Non-transactional dups, 3 two-part keys. */
        openAndInit(false, true);
        OperationStatus status;

        /* Delete key 1, cursor appears on BIN, no compression yet. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getSearchKey(entry1, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Move cursor to 1st dup, cursor moves to BIN, no compresion yet. */
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Delete the duplicates for key 0, no compression yet. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.getNext(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Finally compress can compress. */
        env.compress();

        checkBinEntriesAndCursors(bin, 0, 0);
        checkBinEntriesAndCursors(bin, 0, 0);

        /* BIN is empty so parent entry should be gone also. */
        assertEquals(1, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testAbortInsert()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys. */
        openAndInit(true, false);

        /* Add key 2, cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        cursor.put(entry2, entry0);
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Closing the cursor without abort does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Abort without calling compress does not compress. */
        txn.abort();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testAbortInsertWithBinDeltas()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys, binDeltas. */
        openAndInit(true, false, true);

        /*
         * Sync so that next operation dirties one slot only, which should be
         * logged as a delta.
         */
        env.sync();
        if (isReplicatedTest(getClass())) {
            env.compress();
        }
        checkINCompQueueSize(0);

        /* Add key 2, cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        cursor.put(entry2, entry0);
        checkBinEntriesAndCursors(bin, 3, 1);
        checkINCompQueueSize(0);

        /* Closing the cursor without abort does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);
        checkINCompQueueSize(0);

        /*
         * Undo will not queue compression because BIN-delta should be logged.
         */
        txn.abort();
        checkBinEntriesAndCursors(bin, 3, 0);
        checkINCompQueueSize(0);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);
        checkINCompQueueSize(0);

        /* Lazy compression finally occurs during a full sync (no delta). */
        bin.setProhibitNextDelta(true);
        env.sync();
        checkBinEntriesAndCursors(bin, 2, 0);
        checkINCompQueueSize(0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    /**
     * DBINs are no longer used, but this test is retained for good measure.
     */
    @Test
    public void testAbortInsertDuplicate()
        throws DatabaseException {

        /* Transactional dups, 3 two-part keys. */
        openAndInit(true, true);

        /* Add datum 2 for key 0, cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        cursor.put(entry0, entry2);
        checkBinEntriesAndCursors(bin, 4, 1);

        /* Closing the cursor without abort does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 4, 0);

        /* Abort without calling compress does not compress. */
        txn.abort();
        checkBinEntriesAndCursors(bin, 4, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testRollBackInsert()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys. */
        openAndInit(true, false);

        /* Add key 2, cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        cursor.put(entry2, entry0);
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Closing the cursor without abort does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Checkpoint to preserve internal nodes through recovery. */
        env.checkpoint(forceConfig);

        /* Abort without calling compress does not compress. */
        txn.abort();
        checkBinEntriesAndCursors(bin, 3, 0);

        /*
         * Shutdown and reopen to run recovery. The checkpoint will compress.
         */
        db.close();
        closeNoCheckpoint(env);
        env = null;
        openEnv(true, false);
        initInternalNodes();

        /*
         * In replicated tests, we expect 64 BINs. In non-replicated tests, 
         * there should be 2. 
         */
        if (isReplicatedTest(getClass())) {
            checkBinEntriesAndCursors(bin, 64, 0);
        } else {
            checkBinEntriesAndCursors(bin, 3, 0);
        }

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);

        closeEnv();
    }

    @Test
    public void testRollBackInsertWithBinDeltas()
        throws DatabaseException {

        /* Transactional no-dups, 2 keys, binDeltas. */
        openAndInit(true, false, true);

        /*
         * Sync so that next operation dirties one slot only, which should be
         * logged as a delta.
         */
        env.sync();
        if (isReplicatedTest(getClass())) {
            env.compress();
        }
        checkINCompQueueSize(0);

        /* Add key 2, cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        cursor.put(entry2, entry0);
        checkBinEntriesAndCursors(bin, 3, 1);
        checkINCompQueueSize(0);

        /* Closing the cursor without abort does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);
        checkINCompQueueSize(0);

        /* Checkpoint to preserve internal nodes through recovery. */
        env.checkpoint(forceConfig);

        /*
         * Undo will not queue compression because BIN-delta should be logged.
         */
        txn.abort();
        checkBinEntriesAndCursors(bin, 3, 0);
        checkINCompQueueSize(0);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 0);
        checkINCompQueueSize(0);

        /*
         * Shutdown and reopen to run recovery. The checkpoint will not
         * compress because deltas are logged.
         */
        db.close();
        closeNoCheckpoint(env);
        env = null;
        openEnv(true, false, true);
        initInternalNodes();

        if (isReplicatedTest(getClass())) {
            checkBinEntriesAndCursors(bin, 64, 0);
        } else {
            checkBinEntriesAndCursors(bin, 3, 0);
            checkINCompQueueSize(0);
        }

        /* Lazy compression finally occurs during a full sync (no delta). */
        bin.setDirty(true);
        bin.setProhibitNextDelta(true);
        env.sync();
        checkBinEntriesAndCursors(bin, 2, 0);
        if (!isReplicatedTest(getClass())) {
            checkINCompQueueSize(0);
        }

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    /**
     * DBINs are no longer used, but this test is retained for good measure.
     */
    @Test
    public void testRollBackInsertDuplicate()
        throws DatabaseException {

        /* Transactional dups, 3 two-keys. */
        openAndInit(true, true);

        /* Add datum 2 for key 0, cursor appears on BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        cursor.put(entry0, entry2);
        checkBinEntriesAndCursors(bin, 4, 1);

        /* Closing the cursor without abort does not compress. */
        cursor.close();
        env.compress();
        checkBinEntriesAndCursors(bin, 4, 0);

        /* Checkpoint to preserve internal nodes through recovery. */
        env.checkpoint(forceConfig);

        /* Abort without calling compress does not compress. */
        txn.abort();
        checkBinEntriesAndCursors(bin, 4, 0);

        /*
         * Shutdown and reopen to run recovery. The checkpoint will not
         * compress because deltas are logged.
         */
        db.close();
        closeNoCheckpoint(env);
        env = null;
        openEnv(true, true);
        initInternalNodes();

        /* 
         * In replicated tests, we expect 64 BINs. In non-replicated tests, 
         * there should be 2.
         */
        if (isReplicatedTest(getClass())) {
            checkBinEntriesAndCursors(bin, 64, 0);
        } else {
            checkBinEntriesAndCursors(bin, 4, 0);
        }

        /* Lazy compression finally occurs during a full sync (no delta). */
        bin.setDirty(true);
        bin.setProhibitNextDelta(true);
        env.sync();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    @Test
    public void testRollForwardDelete()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, false);
        OperationStatus status;

        /* Checkpoint to preserve internal nodes through recovery. */
        env.checkpoint(forceConfig);

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 2, 0);

        /*
         * Shutdown and reopen to run recovery. The checkpoint will compress.
         */
        db.close();
        closeNoCheckpoint(env);
        openEnv(false, false);
        initInternalNodes();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 1, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());

        closeEnv();
    }

    /**
     * DBINs are no longer used, but this test is retained for good measure.
     */
    @Test
    public void testRollForwardDeleteDuplicate()
        throws DatabaseException {

        /* Non-transactional dups, 3 two-part keys. */
        openAndInit(false, true);
        OperationStatus status;

        /* Checkpoint to preserve internal nodes through recovery. */
        env.checkpoint(forceConfig);

        /* Cursor appears on BIN. */
        Cursor cursor = db.openCursor(null, null);
        status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Delete without closing the cursor does not compress. */
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        env.compress();
        checkBinEntriesAndCursors(bin, 3, 1);

        /* Closing the cursor without calling compress does not compress. */
        cursor.close();
        checkBinEntriesAndCursors(bin, 3, 0);

        /*
         * Shutdown and reopen to run recovery. The checkpoint will compress.
         */
        db.close();
        closeNoCheckpoint(env);
        openEnv(false, true);
        initInternalNodes();
        checkBinEntriesAndCursors(bin, 3, 0);

        /* Finally compress can compress. */
        env.compress();
        checkBinEntriesAndCursors(bin, 2, 0);

        /* Should be no change in parent nodes. */
        assertEquals(2, in.getNEntries());
        checkBinEntriesAndCursors(bin, 2, 0);

        closeEnv();
    }

    /**
     * Test that we can handle cases where lazy compression runs first, but the
     * daemon handles pruning.  Testing against BINs.
     */
    @Test
    public void testLazyPruning()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, false);

        deleteAndLazyCompress(false);

        /* Now compress, empty BIN should disappear. */
        env.compress();
        checkINCompQueueSize(0);
        assertEquals(1, in.getNEntries());

        closeEnv();
    }

    /**
     * Test that we can handle cases where lazy compression runs first, but the
     * daemon handles pruning.  Testing against DBINs.  [#11778]
     * DBINs are no longer used, but this test is retained for good measure.
     */
    @Test
    public void testLazyPruningDups()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, true);

        deleteAndLazyCompress(true);

        /* Now compress, empty BIN should disappear. */
        env.compress();
        /* Compress again. Empty BIN should disappear. */
        env.compress();
        checkINCompQueueSize(0);
        assertEquals(1, in.getNEntries());

        closeEnv();
    }

    /**
     * Scan over an empty DBIN.  [#11778]
     *
     * WARNING: This test no longer tests the situation it originally intended,
     * since DBINs and DBINs are obsolete, but it is left intact for posterity.
     */
    @Test
    public void testEmptyInitialDBINScan()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, true);

        deleteAndLazyCompress(true);

        /*
         * Have IN with two entries, first entry is BIN with 1 entry.  That
         * entry is DIN with 1 entry.  That entry is a DBIN with 0 entries.
         * Position the cursor at the first entry so that we move over that
         * zero-entry DBIN.
         */
        Cursor cursor = db.openCursor(null, null);
        OperationStatus status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertTrue(keyFound.getData()[0] == 64);
        cursor.close();
        closeEnv();
    }

    /**
     * Scan over an empty BIN.  This looks very similar to
     * com.sleepycat.je.test.SR11297Test. [#11778]
     */
    @Test
    public void testEmptyInitialBINScan()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, false);

        deleteAndLazyCompress(false);

        /*
         * Have IN with two entries, first entry is BIN with 0 entries.
         * Position the cursor at the first entry so that we move over that
         * zero-entry BIN.
         */
        Cursor cursor = db.openCursor(null, null);
        OperationStatus status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertTrue(keyFound.getData()[0] == 64);
        cursor.close();
        closeEnv();
    }

    /**
     * Test that we can handle cases where lazy compression runs first, but the
     * daemon handles pruning.
     */
    @Test
    public void testNodeNotEmpty()
        throws DatabaseException {

        /* Non-transactional no-dups, 2 keys. */
        openAndInit(false, false);

        deleteAndLazyCompress(false);

        /*
         * We now have an entry on the compressor queue, but let's re-insert a
         * value to make pruning hit the NodeNotEmptyException case.
         */
        assertEquals(OperationStatus.SUCCESS, db.put(null, entry0, entry0));
        checkBinEntriesAndCursors(bin, 1, 0);

        env.compress();
        assertEquals(2, in.getNEntries());
        checkINCompQueueSize(0);

        closeEnv();
    }

    /* Todo: Check cursor movement across an empty bin. */

    /* Delete all records from the first bin and invoke lazy compression. */
    private void deleteAndLazyCompress(boolean doDups)
        throws DatabaseException {

        /* Position the cursor at the first BIN and delete both keys. */
        Cursor cursor = db.openCursor(null, null);
        OperationStatus status = cursor.getFirst(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        checkBinEntriesAndCursors(bin, doDups ? 3 : 2, 1);

        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.getNext(keyFound, dataFound, null);
        assertEquals(OperationStatus.SUCCESS, status);
        status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);
        if (doDups) {
            status = cursor.getNext(keyFound, dataFound, null);
            assertEquals(OperationStatus.SUCCESS, status);
            status = cursor.delete();
            assertEquals(OperationStatus.SUCCESS, status);
        }
        cursor.close();

        /*
         * Do lazy compression, leaving behind an empty BIN.
         */
        checkINCompQueueSize(1);
        env.checkpoint(forceConfig);
        checkBinEntriesAndCursors(bin, 0, 0);

        /* BIN is empty but tree pruning hasn't happened. */
        assertEquals(2, in.getNEntries());
        checkINCompQueueSize(1);
    }

    /**
     * Checks for expected entry and cursor counts on the given BIN.
     */
    private void checkBinEntriesAndCursors(BIN checkBin,
                                           int nEntries,
                                           int nCursors) {
        assertEquals("nEntries", nEntries, checkBin.getNEntries());
        assertEquals("nCursors", nCursors, checkBin.nCursors());
    }

    /**
     * Check expected size of the INCompressor queue.
     */
    private void checkINCompQueueSize(int expected) {
        assertEquals(expected,
           DbInternal.getNonNullEnvImpl(env).getINCompressorQueueSize());
    }

    private void openAndInit(boolean transactional, boolean dups) {
        openAndInit(transactional, dups, false /*binDeltas*/);
    }

    /**
     * Opens the environment and db and writes 2 records (3 if dups are used).
     *
     * <p>Without dups: {0,0}, {1,0}. This gives two LNs in the BIN.</p>
     *
     * <p>With dups: {0,0}, {0,1}, {1,0}. This gives three LNs in the BIN.</p>
     */
    private void openAndInit(boolean transactional,
                             boolean dups,
                             boolean binDeltas)
        throws DatabaseException {

        openEnv(transactional, dups, binDeltas);

        /*
         * We need at least 2 BINs, otherwise empty BINs won't be deleted.  So
         * we add keys until the BIN splits, then delete everything in the
         * first BIN except the first two keys.  Those are the keys we'll use
         * for testing, and are key values 0 and 1.
         */
        BIN firstBin = null;
        OperationStatus status;

        for (int i = 0;; i += 1) {
            DatabaseEntry key = new DatabaseEntry(new byte[] { (byte) i });
            status = db.put(null, key, entry0);
            assertEquals(OperationStatus.SUCCESS, status);

            Cursor cursor = db.openCursor(null, null);

            status = cursor.getLast(keyFound, dataFound, null);
            assertEquals(OperationStatus.SUCCESS, status);

            BIN b = DbInternal.getCursorImpl(cursor).getBIN();
            cursor.close();
            if (firstBin == null) {
                firstBin = b;
            } else if (firstBin != b) {
                /* Now delete all but the first two keys in the first BIN. */
                while (firstBin.getNEntries() > 2) {
                    cursor = db.openCursor(null, null);
                    keyFound.setData(entry2.getData());
                    status =
                        cursor.getSearchKeyRange(keyFound, dataFound, null);
                    assertEquals(OperationStatus.SUCCESS, status);
                    cursor.close();
                    status = db.delete(null, keyFound);
                    assertEquals(OperationStatus.SUCCESS, status);
                    env.compress();
                }
                break;
            }
        }

        /* Write dup records. */
        if (dups) {
            status = db.put(null, entry0, entry1);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /* Set in, bin. */
        initInternalNodes();
        assertSame(bin, firstBin);

        /* Check that all tree nodes are populated. */
        assertEquals(2, in.getNEntries());
        checkBinEntriesAndCursors(bin, dups ? 3 : 2, 0);
    }

    /**
     * Initialize IN, BIN.
     */
    private void initInternalNodes()
        throws DatabaseException {

        /* Find the BIN. */
        Cursor cursor = db.openCursor(null, null);
        OperationStatus status =
            cursor.getFirst(keyFound, dataFound, LockMode.READ_UNCOMMITTED);
        assertEquals(OperationStatus.SUCCESS, status);
        bin = DbInternal.getCursorImpl(cursor).getBIN();
        cursor.close();

        /* Find the IN parent of the BIN. */
        bin.latch();
        in = DbInternal.getDbImpl(db).getTree().getParentINForChildIN(
            bin, false, /*useTargetLevel*/
            true, /*doFetch*/ CacheMode.DEFAULT).parent;
        assertNotNull(in);
        in.releaseLatch();
    }

    private void openEnv(boolean transactional, boolean dups) {
        openEnv(transactional, dups, false /*binDeltas*/);
    }

    /**
     * Opens the environment and db.
     */
    private void openEnv(boolean transactional,
                         boolean dups,
                         boolean binDeltas)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(transactional);
        envConfig.setConfigParam
            (EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        if (binDeltas) {
            /* Enable deltas when only 1/2 records are modified. */
            envConfig.setConfigParam
                (EnvironmentConfig.TREE_BIN_DELTA, "60");
        }
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = create(envHome, envConfig);

        /* Make a db and open it. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(transactional);
        dbConfig.setSortedDuplicates(dups);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    /**
     * Closes the db and environment.
     */
    private void closeEnv()
        throws DatabaseException {

        db.close();
        db = null;
        close(env);
        env = null;
    }
}
