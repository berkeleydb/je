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

package com.sleepycat.je.dbi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

import org.junit.Test;

public class BINDeltaOperationTest extends DualTestCase {

    /*
     * N_RECORDS is set to 110 and NODE_MAX_ENTRIES to 100. This results in
     * a tree with 2 BIN, the first of which has 40 entries.
     */
    private static final int N_RECORDS = 110;

    private final File envHome;
    private Environment env;
    private Database db;
    private boolean runBtreeVerifier = true;

    public BINDeltaOperationTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    private void open() {
        open(false);
    }

    private void open(boolean deferredWrite) {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "100");
        if (!runBtreeVerifier) {
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        }
        env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(!deferredWrite);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);
        dbConfig.setDeferredWrite(deferredWrite);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    private void close() {
        db.close();
        db = null;
        env.close();
        env = null;
    }

    @Test
    public void testEviction() {

        runBtreeVerifier = false;
        open();

        /* Insert N_RECORDS records into 2 BINs */
        writeData();
        checkData(null);

        /* Flush BINs (checkpoint). */
        env.checkpoint(new CheckpointConfig().setForce(true));
        checkData(null);

        /*
         * Update only enough records in the 1st BIN to make it a delta
         * when it gets selected for eviction.
         */
        writeDeltaFraction();

        env.checkpoint(new CheckpointConfig().setForce(true));


        BIN bin = getFirstBIN();
        assertFalse(bin.isBINDelta(false));
 
        EnvironmentStats stats = env.getStats(StatsConfig.CLEAR);
        final long initialBINs = stats.getNCachedBINs();
        final long initialDeltas = stats.getNCachedBINDeltas();
        assertEquals(0, initialDeltas);

        /*
         * Test partial eviction to mutate full BIN to BIN delta.
         */
        mutateToDelta(bin);
        stats = env.getStats(null);
        assertEquals(initialBINs, stats.getNCachedBINs());
        assertEquals(initialDeltas + 1, stats.getNCachedBINDeltas());

        Transaction txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        Cursor cursor1 = db.openCursor(txn, null);
        Cursor cursor2 = db.openCursor(txn, null);

        /*
         * Read 2 existing keys directly from the bin delta, using 2 different
         * cursors; one doing an exact search and the other a range search.
         */
        searchExistingKey(cursor1, 6, true);
        searchExistingKey(cursor2, 10, false);

        assertTrue(bin.isBINDelta(false));

        /*
         * Update the record where cursor1 is positioned at and make sure the
         * bin is still a delta.
         */
        updateCurrentRecord(cursor1, 20);

        assertTrue(bin.isBINDelta(false));

        /*
         * Now, read all records (checkData). This will mutate the delta to a
         * the full BIN, but only one fetch will be needed (nNotResident == 1).
         */
        checkData(txn);
        assertTrue(!bin.isBINDelta(false));
        assertSame(bin, getFirstBIN());
        stats = env.getStats(StatsConfig.CLEAR);
        assertEquals(initialBINs, stats.getNCachedBINs());
        assertEquals(initialDeltas, stats.getNCachedBINDeltas());
        assertEquals(1, stats.getNFullBINsMiss());
        assertEquals(0, stats.getNBINDeltasFetchMiss());
        assertEquals(1, stats.getNNotResident());

        /*
         * Make sure that cursors 1 and 2 were adjusted correctly when the
         * delta got mutated.
         */
        confirmCurrentKey(cursor1, 6);
        confirmCurrentKey(cursor2, 10);

        cursor1.close();
        cursor2.close();
        txn.commit();

        /*
         * Call evict() to mutate the BIN to a delta.
         */
        mutateToDelta(bin);
        assertTrue(bin.getInListResident());
        stats = env.getStats(null);
        assertEquals(initialBINs, stats.getNCachedBINs());
        assertEquals(initialDeltas + 1, stats.getNCachedBINDeltas());

        /*
         * Delete a record from the bin delta
         */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);

        searchExistingKey(cursor1, 6, true);
        assertTrue(bin.isBINDelta(false));
        assertEquals(OperationStatus.SUCCESS, cursor1.delete());

        cursor1.close();
        txn.commit();

        assertTrue(bin.isBINDelta(false));

        /*
         * Call evict(true) to evict the BIN completely (without explicitly
         * forcing the eviction, the BIN will be put in the dirty LRU). Then
         * reading the entries will require two fetches.
         */
        evict(bin, true);
        assertFalse(bin.getInListResident());
        stats = env.getStats(null);
        assertEquals(initialBINs - 1, stats.getNCachedBINs());
        assertEquals(initialDeltas, stats.getNCachedBINDeltas());

        BIN prevBin = bin;
        bin = getFirstBIN();
        assertNotSame(prevBin, bin);
        assertFalse(bin.isBINDelta(false));
        stats = env.getStats(StatsConfig.CLEAR);
        assertEquals(initialBINs, stats.getNCachedBINs());
        assertEquals(initialDeltas, stats.getNCachedBINDeltas());
        if (stats.getOffHeapBINsLoaded() > 0) {
            assertEquals(1, stats.getOffHeapBINsLoaded());
        } else {
            assertEquals(1, stats.getNBINsFetchMiss());
            assertEquals(1, stats.getNBINDeltasFetchMiss());
            assertEquals(1, stats.getNFullBINsMiss());
            assertEquals(2, stats.getNNotResident());
        }

        /*
         * Put back the record deleted above, so that checkData won't complain.
         */
        inserRecord(6, 10);

        checkData(null);
        stats = env.getStats(StatsConfig.CLEAR);
        assertEquals(0, stats.getNBINsFetchMiss());
        assertEquals(0, stats.getNBINDeltasFetchMiss());
        assertEquals(0, stats.getNNotResident());

        close();
    }

    @Test
    public void testDbCount() {

        open();

        writeData(0, 5000);
        assertEquals(5000, db.count());
        checkData(null, 5000);
        close();

        open();
        assertEquals(5000, db.count());
        checkData(null, 5000);
        close();
    }

    @Test
    public void testTransitionToDeferredWrite() {

        /*
         * Open in normal mode (not deferred-write) and write a BIN-delta. Then
         * close the env in order to start the next step with an empty cache.
         */
        open(false /*deferredWrite*/);

        writeData();
        checkData(null);

        env.checkpoint(new CheckpointConfig().setForce(true));
        checkData(null);

        writeDeltaFraction();
        env.checkpoint(new CheckpointConfig().setForce(true));

        close();

        /*
         * Open in deferred-write mode and search for an existing key. Prior to
         * the fix for [#25999], an assertion would fire during the cursor
         * search, when fetching the BIN-delta, saying that BIN-deltas aren't
         * allowed with deferred-write.
         */
        open(true /*deferredWrite*/);

        Cursor cursor = db.openCursor(null, null);
        searchExistingKey(cursor, 6, true);

        BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        assertFalse(bin.isBINDelta(false));

        cursor.close();
        close();
    }

    private void writeData() {
        writeData(0, N_RECORDS);
    }

    private void writeDeltaFraction() {
        /* Update records in slots 5 to 15 (inclusive) of the 1st BIN */
        writeData(5, N_RECORDS / 10);
    }

    private void writeData(int startRecord, int nRecords) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        for (int i = startRecord; i < nRecords + startRecord; i += 1) {

            key.setData(TestUtils.getTestArray(i));
            data.setData(TestUtils.getTestArray(i));

            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
    }

    private void inserRecord(int keyVal, int dataVal) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));
        data.setData(TestUtils.getTestArray(dataVal));

        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
    }


    private void checkData(final Transaction txn) {
        checkData(txn, N_RECORDS);
    }

    /**
     * Reads all keys, but does not read data to avoid changing the
     * nNotResident stat.
     */
    private void checkData(final Transaction txn, final int nRecords) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(true);

        final Cursor cursor = db.openCursor(txn, null);

        for (int i = 0; i < nRecords; i += 1) {

            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, null));

            assertEquals(i, TestUtils.getTestVal(key.getData()));
        }

        assertEquals(OperationStatus.NOTFOUND,
            cursor.getNext(key, data, null));

        cursor.close();
    }

    private void searchExistingKey(
        final Cursor cursor,
        final int keyVal,
        boolean exactSearch) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        key.setData(TestUtils.getTestArray(keyVal));

        data.setPartial(true);
        
        if (exactSearch) {
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchKey(key, data, null));
        } else {
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchKeyRange(key, data, null));
        }
        
        assertEquals(keyVal, TestUtils.getTestVal(key.getData()));
    }

    private Cursor confirmCurrentKey(final Cursor cursor, int keyVal) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(true);

        assertEquals(OperationStatus.SUCCESS,
            cursor.getCurrent(key, data, null));

        assertEquals(keyVal, TestUtils.getTestVal(key.getData()));

        return cursor;
    }

    private void updateCurrentRecord(final Cursor cursor, int dataVal) {

        final DatabaseEntry data = new DatabaseEntry();
        data.setData(TestUtils.getTestArray(dataVal));

        assertEquals(OperationStatus.SUCCESS, cursor.putCurrent(data));

        final DatabaseEntry key = new DatabaseEntry();
        data.setData(null);

        assertEquals(OperationStatus.SUCCESS,
            cursor.getCurrent(key, data, null));

        assertEquals(dataVal, TestUtils.getTestVal(data.getData()));
    }

    /**
     * Reads first key and returns its BIN. Does not read data to avoid
     * changing the nNotResident stat.
     */
    private BIN getFirstBIN() {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(true);

        final Cursor cursor = db.openCursor(null, null);

        assertEquals(OperationStatus.SUCCESS,
            cursor.getFirst(key, data, null));

        final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        cursor.close();
        assertNotNull(bin);
        return bin;
    }

    private void mutateToDelta(BIN bin) {

        OffHeapCache ohCache =
            DbInternal.getNonNullEnvImpl(env).getOffHeapCache();

        if (!ohCache.isEnabled()) {
            evict(bin, false);
            assertEquals(1, env.getStats(null).getNNodesMutated());
        } else {
            bin.latchNoUpdateLRU();
            bin.mutateToBINDelta();
            bin.releaseLatch();
        }

        assertTrue(bin.isBINDelta(false));
        assertTrue(bin.getInListResident());
    }

    /**
     * Simulated eviction of the BIN, if it were selected.  This may only do
     * partial eviction, if LNs are present or it can be mutated to a delta.
     * We expect that some memory will be reclaimed.
     */
    private void evict(BIN bin, boolean force) {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Evictor evictor = envImpl.getEvictor();

        final long memBefore = TestUtils.validateNodeMemUsage(envImpl, true);

        if (force) {
            /* CACHEMODE eviction will not evict a dirty BIN. */
            env.sync();
        }

        bin.latch(CacheMode.UNCHANGED);

        if (force) {
            evictor.doTestEvict(bin, Evictor.EvictionSource.CACHEMODE);
        } else {
            evictor.doTestEvict(bin, Evictor.EvictionSource.MANUAL);
        }

        final long memAfter = TestUtils.validateNodeMemUsage(envImpl, true);

        assertTrue(memAfter < memBefore);
    }
}
