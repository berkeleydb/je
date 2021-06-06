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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

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
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

public class EmbeddedOpsTest extends DualTestCase {

    /*
     * N_RECORDS is set to 110 and NODE_MAX_ENTRIES to 100. This results in
     * a tree with 2 BINs, the first of which has 50 entries.
     */
    private static final int N_RECORDS = 110;

    private final File envHome;
    private Environment env;
    private Database db;
    private boolean dups;

    EnvironmentConfig envConfig;
    DatabaseConfig dbConfig;

    private boolean debug = true;

    public EmbeddedOpsTest() {

        envHome = SharedTestUtils.getTestDir();

        envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);

        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
            "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "100");

        /*
         * evict() method needs to force MANUAL eviction, but this is defeated
         * in dual/HA mode due to the min tree memory check when a shared cache
         * is used, so we disable the shared cache here as a workaround.
         */
        envConfig.setSharedCache(false);

        envConfig.setConfigParam(EnvironmentConfig.TREE_MAX_EMBEDDED_LN, "20");

        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);
    }

    private boolean open(boolean dups) {

        env = create(envHome, envConfig);

        if (!dups) {
            db = env.openDatabase(null, "testDB", dbConfig);
        } else {
            dbConfig.setSortedDuplicates(true);
            db = env.openDatabase(null, "testDupsDB", dbConfig);
        }

        this.dups = dups;

        /* TREE_MAX_EMBEDDED_LN may be overridden in a je.properties file. */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        if (envImpl.getMaxEmbeddedLN() != 20) {
            System.out.println(
                "Skipping EmbeddedOpsTest because TREE_MAX_EMBEDDED_LN has " +
                "been overridden.");
            close();
            return false;
        }

        return true;
    }

    private void close() {
        db.close();
        db = null;
        close(env);
        env = null;
    }

    @Test
    public void testNoDups() {

        Transaction txn;
        Cursor cursor1;
        Cursor cursor2;

        if (!open(false)) {
            return; // embedded LNs are disabled
        }

        writeData();

        BIN bin1 = getFirstBIN();
        BIN bin2 = getLastBIN();

        assertEquals(50, bin1.getNumEmbeddedLNs());
        assertEquals(60, bin2.getNumEmbeddedLNs());

        /*
         * The BIN split causes full version of both BINs to be logged. After
         * the split is done, 10 more records are inserted in the 2nd BIN, so
         * that BIN has 10 entries marked dirty. During the checkpoint below,
         * a delta will be logged for the 2nd BIN, so although after the
         * checkpoint the BIN will be clean, it will still have 10 dirty
         * entries.
         */
        if (debug) {
            System.out.println(
                "1. BIN-1 has " + bin1.getNEntries() + " entries");
            System.out.println(
                "1. BIN-1 has " + bin1.getNDeltas() + " dirty entries");
            System.out.println(
                "1. BIN-2 has " + bin2.getNEntries() + " entries");
            System.out.println(
                "1. BIN-2 has " + bin2.getNDeltas() + " dirty entries");
        }

        /* Flush BINs (checkpoint) */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Update only enough records in the 1st BIN to make it a delta
         * when it gets selected for eviction. Specifically, update records
         * in slots 5 to 15 (inclusive).
         */
        writeData(5, N_RECORDS / 10);

        if (debug) {
            System.out.println(
                "2. BIN-1 has " + bin1.getNDeltas() + " dirty entries");
            System.out.println(
                "2. BIN-2 has " + bin2.getNDeltas() + " dirty entries");
        }

        assertTrue(!bin1.isBINDelta(false));
        assertTrue(!bin2.isBINDelta(false));

        /*
         * Mutate bin1 to a delta. Make sure all slots have embedded data
         * and use the compact key rep.
         */
        mutateToDelta(bin1);
        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());
        assertTrue(bin1.getKeyVals().accountsForKeyByteMemUsage());

        /*
         * Read keys 12 and 20 directly from the bin delta, using 2 cursors:
         * one doing an exact search and the other a range search. Make sure no
         * mutation back to full bin occurs and the embedded data is correct.
         */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        cursor2 = db.openCursor(txn, null);

        searchKey(cursor1, 12, true/*exact*/, true/*exist*/);
        searchKey(cursor2, 20, false/*range*/, true/*exists*/);

        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());

        confirmCurrentData(cursor1, 12);
        confirmCurrentData(cursor2, 20);

        /*
         * Delete record 12-12.
         */
        assertEquals(OperationStatus.SUCCESS, cursor1.delete());

        /*
         * Update record 20-20 to 20-21. The op keeps the data embedded.
         */
        putRecord(cursor2, 20, 21);
        confirmCurrentData(cursor2, 21);
        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());

        /*
         * Re-insert record 12-14 (slot reuse). The op keeps the data embedded.
         */
        putRecord(cursor1, 12, 14);
        confirmCurrentData(cursor1, 14);
        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());

        /*
         * Update record 12-14 to 12-25. The op causes the key rep to switch
         * from compact to default.
         */
        putRecord(cursor1, 12, 25);
        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());
        assertTrue(!bin1.getKeyVals().accountsForKeyByteMemUsage());
        confirmCurrentData(cursor1, 25);

        /*
         * Update record 120 in a way that makes it non-embedded. Then update
         * it again into a zero-data record.
         */
        putLargeRecord(cursor2, 120, 120);
        assertEquals(bin2.getNEntries()-1, bin2.getNumEmbeddedLNs());

        env.checkpoint(new CheckpointConfig().setForce(true));

        putZeroRecord(cursor2, 120);
        assertEquals(bin2.getNEntries(), bin2.getNumEmbeddedLNs());
        confirmCurrentKey(cursor2, 120);

        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Abort the txn and make sure everything went back to pre-txn state.
         */
        cursor1.close();
        cursor2.close();
        txn.abort();

        cursor1 = db.openCursor(null, null);

        searchKey(cursor1, 12, true/*exact*/, true/*exist*/);
        confirmCurrentData(cursor1, 12);

        searchKey(cursor1, 20, true/*exact*/, true/*exist*/);
        confirmCurrentData(cursor1, 20);

        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());
        assertEquals(bin2.getNEntries(), bin2.getNumEmbeddedLNs());
        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());

        checkKeys(0, 50);
        checkKeys(100, 50);

        cursor1.close();


        /*
         * Cause mutation of bin1 to full bin by searching for an existing
         * key that is not in the delta. Make sure all entries stay embedded
         * and readable.
         */
        cursor1 = db.openCursor(null, null);

        searchKey(cursor1, 0, true/*exactSearch*/, true/*exists*/);

        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());
        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());

        checkKeys(0, 50);

        cursor1.close();

        /*
         * Crash and recover.
         */
        abnormalClose(env);
        open(false);

        checkKeys(0, 50);
        checkKeys(100, 50);

        bin1 = getFirstBIN();
        bin2 = getLastBIN();
        assertEquals(bin1.getNEntries(), bin1.getNumEmbeddedLNs());
        assertEquals(bin2.getNEntries(), bin2.getNumEmbeddedLNs());

        close();
    }

    private void writeData() {
        writeData(0, N_RECORDS);
    }

    private void writeData(int startRecord, int nRecords) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        for (int i = startRecord; i < nRecords + startRecord; i += 1) {

            key.setData(TestUtils.getTestArray(2*i, false));
            data.setData(TestUtils.getTestArray(2*i, false));

            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
    }

    private void putRecord(Cursor cursor, int keyVal, int dataVal) {

        final DatabaseEntry key = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));
        final DatabaseEntry data = new DatabaseEntry();
        data.setData(TestUtils.getTestArray(dataVal));

        assertEquals(OperationStatus.SUCCESS, cursor.put(key, data));
    }

    private void putLargeRecord(Cursor cursor, int keyVal, int dataVal) {

        byte[] dataBytes = new byte[60];
        byte[] tmp = TestUtils.getTestArray(dataVal);
        System.arraycopy(tmp, 0, dataBytes, 0, tmp.length);

        final DatabaseEntry data = new DatabaseEntry();
        data.setData(dataBytes);
        final DatabaseEntry key = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));

        assertEquals(OperationStatus.SUCCESS, cursor.put(key, data));
    }

    private void putZeroRecord(Cursor cursor, int keyVal) {

        final DatabaseEntry key = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));
        final DatabaseEntry data = new DatabaseEntry();
        data.setData(new byte[0]);

        assertEquals(OperationStatus.SUCCESS, cursor.put(key, data));
    }

    private void insertRecord(
        Cursor cursor,
        int keyVal,
        int dataVal,
        boolean large) {

        OperationStatus status = OperationStatus.SUCCESS;

        final DatabaseEntry key = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));

        byte[] dataBytes;

        if (large) {
            dataBytes = new byte[60];
            byte[] tmp = TestUtils.getTestArray(dataVal);
            System.arraycopy(tmp, 0, dataBytes, 0, tmp.length);
        } else {
            dataBytes = TestUtils.getTestArray(dataVal);
        }

        final DatabaseEntry data = new DatabaseEntry();
        data.setData(dataBytes);

        assertEquals(status, cursor.putNoOverwrite(key, data));
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

    private void searchKey(
        final Cursor cursor,
        final int keyVal,
        boolean exactSearch,
        boolean exists) {

        OperationStatus status = OperationStatus.SUCCESS;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        key.setData(TestUtils.getTestArray(keyVal));
        data.setPartial(true);

        if (exactSearch) {
            if (!exists) {
                status = OperationStatus.NOTFOUND;
            }
            assertEquals(status, cursor.getSearchKey(key, data, null));
        } else {
            assertEquals(status, cursor.getSearchKeyRange(key, data, null));
        }
    }

    private void searchRecord(
        Cursor cursor,
        int keyVal,
        int dataVal,
        boolean exactSearch,
        boolean exists) {

        OperationStatus status = OperationStatus.SUCCESS;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        key.setData(TestUtils.getTestArray(keyVal));
        data.setData(TestUtils.getTestArray(dataVal));
 
        if (exactSearch) {
            if (!exists) {
                status = OperationStatus.NOTFOUND;
            }
            assertEquals(status, cursor.getSearchBoth(key, data, null));
        } else {
            assertEquals(status, cursor.getSearchBothRange(key, data, null));
        }
    }

    private void getNext(Cursor cursor, boolean skipDups) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
 
        if (skipDups) {
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNextNoDup(key, data, null));
        } else {
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, null));
        }
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

    private Cursor confirmCurrentData(final Cursor cursor, int dataVal) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
            cursor.getCurrent(key, data, null));

        assertEquals(dataVal, TestUtils.getTestVal(data.getData()));

        return cursor;
    }

    private void checkKeys(final int startKeyVal, final int nRecords) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(true);

        final Cursor cursor = db.openCursor(null, null);

        searchKey(cursor, startKeyVal, true, true);

        for (int i = 1; i < nRecords; i += 1) {

            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, null));

            assertEquals(2 * i + startKeyVal,
                         TestUtils.getTestVal(key.getData()));
        }

        cursor.close();
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

    /**
     * Reads last key and returns its BIN. Does not read data to avoid
     * changing the nNotResident stat.
     */
    private BIN getLastBIN() {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(true);

        final Cursor cursor = db.openCursor(null, null);

        assertEquals(OperationStatus.SUCCESS,
            cursor.getLast(key, data, null));

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

        bin.latch(CacheMode.UNCHANGED);

        if (force) {
            evictor.doTestEvict(bin, Evictor.EvictionSource.CACHEMODE);
        } else {
            evictor.doTestEvict(bin, Evictor.EvictionSource.MANUAL);
        }

        bin.updateMemoryBudget();

        final long memAfter = TestUtils.validateNodeMemUsage(envImpl, true);

        assertTrue(memAfter < memBefore);
    }
}
