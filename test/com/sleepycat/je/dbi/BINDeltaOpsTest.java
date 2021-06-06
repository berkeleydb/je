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
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINDeltaBloomFilter;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

import org.junit.Test;

public class BINDeltaOpsTest extends DualTestCase {

    /*
     * N_RECORDS is set to 110 and NODE_MAX_ENTRIES to 100. This results in
     * a tree with 2 BIN, the first of which has 50 entries.
     */
    private static final int N_RECORDS = 110;

    private final File envHome;
    private Environment env;
    private Database db;
    private boolean dups;

    private boolean debug = false;

    public BINDeltaOpsTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    private void open(boolean dups) {

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
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);

        if (!dups) {
            db = env.openDatabase(null, "testDB", dbConfig);
        } else {
            dbConfig.setSortedDuplicates(true);
            db = env.openDatabase(null, "testDupsDB", dbConfig);
        }

        this.dups = dups;
    }

    private void close() {
        db.close();
        db = null;
        env.close();
        env = null;
    }

    /*
     * This is a test to make sure that when a full BIN is mutated to a delta,
     * the delta is smaller than the full version (there used to be a bug
     * whare key prefixing was used in the full BIN, but not in the delta, and
     * as a result, the delta could be larger than the full BIN).
     */
    @Test
    public void testMemory() {

        open(true);

        /*
         * Create a long key to be used for all the records inserted in the
         * dups db.
         */
        byte[] keyBytes = new byte[50];
        for (int i = 0; i < 50; ++i) {
            keyBytes[i] = 65;
        }

        /*
         * Do the record insertions. All records have the same, 50-byte-long
         * key and a 1-byte-long data portion. Key prefixing is done, resulting
         * in records that take only 3 bytes each.
         */
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        for (int i = 0; i < N_RECORDS; i += 1) {

            key.setData(keyBytes);
            data.setData(new byte[] {(byte)i});

            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        BIN bin1 = getFirstBIN();

        assertTrue(bin1.hasKeyPrefix());

        /* Flush BINs (checkpoint) */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Update the max number of records in the 1st BIN to make it a delta
         * when it gets selected for eviction.
         */
       for (int i = 0; i < 12; i += 1) {

            key.setData(keyBytes);
            data.setData(new byte[] {(byte)i});

            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        assertTrue(!bin1.isBINDelta(false));
 
        /*
         * Mutate bin1 to a delta and make sure memory gets released.
         */
        mutateToDelta(bin1);
        assertTrue(bin1.getInListResident());

        close();
    }

    @Test
    public void testNoDups() {

        Transaction txn;
        Cursor cursor1;
        Cursor cursor2;

        open(false);

        writeData();
        checkData(null);

        BIN bin1 = getFirstBIN();
        BIN bin2 = getLastBIN();

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
         * Mutate bin1 to a delta
         */
        mutateToDelta(bin1);
        assertTrue(bin1.getInListResident());

        /*
         * Read 2 existing keys directly from the bin delta, using 2 cursors:
         * one doing an exact search and the other a range search. Make sure
         * no mutation back to full bin occurs.
         */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        cursor2 = db.openCursor(txn, null);

        searchKey(cursor1, 12, true/*exact*/, true/*exist*/);
        searchKey(cursor2, 20, false/*range*/, true/*exists*/);

        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());
        confirmCurrentKey(cursor1, 12);
        confirmCurrentKey(cursor2, 20);

        /*
         * Delete the record where cursor1 is positioned on and make sure no
         * mutation back to full bin occurs.
         */
        assertEquals(OperationStatus.SUCCESS, cursor1.delete());

        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());

        /*
         * Update a record that in is already in the bin1 delta via a put op
         * and make sure no mutation back to full bin occurs.
         */
        putRecord(cursor1, 14, 40);
        assertTrue(bin1.isBINDelta(false));

        /*
         * Cause mutation to full bin by searching for an existing key that
         * is not in the delta. Make sure that the 2 cursors are adjusted
         * correctly.
         */
        searchKey(cursor1, 2, true, true/*exists*/);

        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());
        confirmCurrentKey(cursor1, 2);
        confirmCurrentKey(cursor2, 20);

        /* Mutation to full BIN should not clear dirty flag. */
        assertTrue(bin1.getDirty());

        cursor1.close();
        cursor2.close();
        txn.commit();

        /*
         * Create a KD slot in bin1 by inserting a new record between two
         * existing records, and then aborting the txn.
         */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);

        putRecord(cursor1, 11, 11);

        CursorImpl cursor1Impl = DbInternal.getCursorImpl(cursor1);
        assertTrue(bin1 == cursor1Impl.getBIN());
        int slotPos = cursor1Impl.getIndex();

        if (debug) {
            System.out.println("Cursor1 is on bin1 and slot " + slotPos);
        }

        cursor1.close();
        txn.abort();

        assertTrue(bin1.isEntryKnownDeleted(slotPos));
        assertTrue(bin1.getNEntries() == 51);

        /* Turn bin1 into a delta */
        mutateToDelta(bin1);
        assertTrue(bin1.getInListResident());
        if (debug) {
            System.out.println("BIN-1D has " + bin1.getNEntries() + " entries");
        }
        assertTrue(bin1.getNEntries() == 12);

        /*
         * Do a range search for key 11. This operation will first position
         * (and register) a CursorImpl on the KD slot, and then getNext() will
         * be called on that cursor. getNext() will mutate the delta to a full
         * BIN, and the position of the cursor must be adjusted correctly.
         */
        cursor1 = db.openCursor(null, null);

        searchKey(cursor1, 11, false, false/*exists*/);

        assertTrue(!bin1.isBINDelta(false));
        cursor1.close();

        /* Turn bin1 into a delta */
        mutateToDelta(bin1);

        /* Mutate bin1 to full bin by doing a put of a non-existing key */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        putRecord(cursor1, 13, 13);
        assertTrue(!bin1.isBINDelta(false));
        cursor1.close();
        txn.commit();

        /*
         * Mutate bin2 to a delta
         */
        mutateToDelta(bin2);
        assertTrue(bin2.getInListResident());

        /*
         * Do a db.count() with a cached BIN delta
         */
        assertTrue(db.count() == N_RECORDS);

        assertTrue(bin2.isBINDelta(false));
        assertTrue(bin2.getInListResident());

        /*
         * 1. Update more records in bin1 so that next logrec will be a full bin.
         * 2. Do a checkpoint to write the full bin1 to the log. Note that bin1
         *    has 2 deleted slots that will be compressed away by logging it.
         * 3. Update 1 record in bin1.
         * 4. Mutate bin1 to a delta with one slot only.
         */
        writeData(15, 10);
        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getNEntries() == 52);
        env.checkpoint(new CheckpointConfig().setForce(true));
        assertTrue(bin1.getNDeltas() == 0);
        assertTrue(bin1.getNEntries() == 50);
        writeData(5, 1);
        mutateToDelta(bin1);
        assertTrue(bin1.getNEntries() == 1);
        assertTrue(bin1.getMaxEntries() == 12);

        /* Do 2 blind puts and 1 blind putNoOverwrite in the bin1 delta */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        putRecord(cursor1, 15, 15);
        putRecord(cursor1, 17, 17);
        insertRecord(cursor1, 21, 21);
        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getNEntries() == 4);
        cursor1.close();
        txn.commit();

        /* Search for a non-existent key in bin1 delta */
        cursor1 = db.openCursor(null, null);
        searchKey(cursor1, 111, true, false/*exists*/);
        assertTrue(bin1.isBINDelta(false));
        cursor1.close();

        /* Mutate bin1 to a full bin by doing an updating put */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        putRecord(cursor1, 16, 16);
        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getNEntries() == 53);
        cursor1.close();
        txn.commit();

        close();
    }

    @Test
    public void testDups() {

        Transaction txn;
        Cursor cursor1;
        Cursor cursor2;
        CursorImpl cursor1Impl;
        CursorImpl cursor2Impl;

        open(true);

        writeData();
        checkData(null);

        BIN bin1 = getFirstBIN();
        BIN bin2 = getLastBIN();

        /* Flush BINs (checkpoint) */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Update only enough records in the 1st BIN to make it a delta
         * when it gets selected for eviction. Specifically, update records
         * in slots 5 to 15 (inclusive).
         */
        writeData(5, N_RECORDS / 10);

        assertTrue(!bin1.isBINDelta(false));
        assertTrue(!bin2.isBINDelta(false));
 
        /*
         * Mutate bin1 to a delta
         */
        mutateToDelta(bin1);

        /*
         * Read 2 existing records directly from the bin delta, using 2 cursors:
         * one doing an exact search and the other a range search. Make sure no
         * mutation back to full bin occurs.
         */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        cursor2 = db.openCursor(txn, null);

        searchRecord(cursor1, 12, 12, true/*exact*/, true/*exists*/);
        searchRecord(cursor2, 20, 20, false/*exact*/, true/*exists*/);

        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());

        /*
         * Delete the record where cursor1 is positioned on and make sure no
         * mutation back to full bin occurs.
         */
        assertEquals(OperationStatus.SUCCESS, cursor1.delete());

        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());

        /*
         * Cause mutation to full bin by searching for a key only.
         */
        searchKey(cursor1, 14, true/*exact*/, true/*exists*/);

        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getInListResident());
        confirmCurrentKey(cursor1, 14);
        confirmCurrentKey(cursor2, 20);

        /*
         * Put a duplicate record in bin1. Note: we are inserting record
         * (20, 10) which compares greater than the existing (20, 20)
         * record.
         */
        putRecord(cursor2, 20, 10);

        cursor1.close();
        cursor2.close();
        txn.commit();

        /*
         * Mutate bin to delta again
         */
        mutateToDelta(bin1);

        /*
         * Cause mutation to full bin via a getNextNoDup() call (which
         * passes a non-null comparator to CursorImpl.searchRange()
         */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        cursor2 = db.openCursor(txn, null);

        searchRecord(cursor1, 20, 10, false/*exact*/, true/*exists*/);
        searchRecord(cursor2, 20, 20, false/*exact*/, true/*exists*/);

        if (debug) {
            cursor1Impl = DbInternal.getCursorImpl(cursor1);
            int slot1Pos = cursor1Impl.getIndex();
            System.out.println("Cursor1 is on bin1 and slot " + slot1Pos);

            cursor2Impl = DbInternal.getCursorImpl(cursor2);
            int slot2Pos = cursor2Impl.getIndex();
            System.out.println("Cursor2 is on bin1 and slot " + slot2Pos);
        }

        assertTrue(bin1.isBINDelta(false));
        confirmCurrentKey(cursor1, 20);

        getNext(cursor1, true/*skipDups*/);

        assertTrue(!bin1.isBINDelta(false));
        confirmCurrentKey(cursor1, 22);

        cursor1.close();
        cursor2.close();
        txn.commit();

        /*
         * Mutate bin1 to delta again. Then do exact record searches for
         * non-existing records and make sure no mutation to full bin.
         */
        mutateToDelta(bin1);
        cursor1 = db.openCursor(null, null);
        searchRecord(cursor1, 21, 10, true/*exact*/, false/*exists*/);
        searchRecord(cursor1, 20, 20000, true/*exact*/, false/*exists*/);
        assertTrue(bin1.isBINDelta(false));

        /*
         * Mutate bin1 to full bin by searching for record that exists in the
         * full bin, but not in the delta.
         */
        searchRecord(cursor1, 32, 32, true/*exact*/, true/*exists*/);
        assertTrue(!bin1.isBINDelta(false));
        cursor1.close();

        /*
         * Mutate bin1 to delta and then back to full bin again by doing an
         * exact search for a key only.
         */
        mutateToDelta(bin1);
        cursor1 = db.openCursor(null, null);
        searchKey(cursor1, 25, true/*exact*/, false/*exists*/);
        assertTrue(!bin1.isBINDelta(false));
        cursor1.close();

        /*
         * 1. Update more records in bin1 so that next logrec will be a full bin.
         * 2. Do a checkpoint to write the full bin1 to the log. Note that bin1
         *    has 2 deleted slots that will be compressed away by logging it.
         * 3. Update 1 record in bin1.
         * 4. Mutate bin1 to a delta with one slot only.
         */
        writeData(15, 10);
        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getNEntries() == 51);
        env.checkpoint(new CheckpointConfig().setForce(true));
        assertTrue(bin1.getNDeltas() == 0);
        assertTrue(bin1.getNEntries() == 50);
        writeData(5, 1);
        mutateToDelta(bin1);
        assertTrue(bin1.getNEntries() == 1);
        assertTrue(bin1.getMaxEntries() == 12);

        /* Do 3 blind puts in the bin1 delta */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        putRecord(cursor1, 15, 15);
        putRecord(cursor1, 17, 17);
        putRecord(cursor1, 21, 21);
        assertTrue(bin1.isBINDelta(false));
        assertTrue(bin1.getNEntries() == 4);
        cursor1.close();
        txn.commit();

        /* Mutate bin1 to a full bin by doing a putNoOverwrite */
        txn = env.beginTransaction(null, TransactionConfig.DEFAULT);
        cursor1 = db.openCursor(txn, null);
        insertRecord(cursor1, 23, 23);
        assertTrue(!bin1.isBINDelta(false));
        assertTrue(bin1.getNEntries() == 54);
        cursor1.close();
        txn.commit();

        close();
    }

    /**
     * Checks that getPrev works when moving backwards across BIN-deltas.
     */
    @Test
    public void testPrevBin() {

        final int nRecs = 1000;

        open(false);
        writeData(0, nRecs);
        env.sync();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        BIN prevBin = null;

        for (int i = 0; i < nRecs; i += 1) {

            assertEquals(
                OperationStatus.SUCCESS,
                cursor.getNext(key, data, null));

            final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
            final BIN bin = cursorImpl.getBIN();

            if (bin != prevBin) {

                assertEquals(
                    OperationStatus.SUCCESS,
                    cursor.putCurrent(data));

                if (prevBin != null) {
                    mutateToDelta(prevBin);
                }

                prevBin = bin;
            }
        }

        assertEquals(
            OperationStatus.NOTFOUND,
            cursor.getNext(key, data, null));

        cursor.close();
        txn.commit();

        cursor = db.openCursor(null, null);

        assertEquals(
            OperationStatus.SUCCESS,
            cursor.getLast(key, data, null));

        for (int i = nRecs - 1;  i >= 0; i -= 1) {

            assertEquals(2 * i, TestUtils.getTestVal(key.getData()));

            assertEquals(
                (i > 0) ? OperationStatus.SUCCESS : OperationStatus.NOTFOUND,
                cursor.getPrev(key, data, null));
        }

        cursor.close();

        close();
    }

    @Test
    public void testLargeBloomFilter() {

        int numKeys = 129;
        int nbytes = BINDeltaBloomFilter.getByteSize(numKeys);

        byte[] bf = new byte[nbytes];

        BINDeltaBloomFilter.HashContext hc =
            new BINDeltaBloomFilter.HashContext();

        for (int i = 0; i < numKeys; ++i) {
            BINDeltaBloomFilter.add(bf, new byte[i], hc);
        }
    }

    private void writeData() {
        writeData(0, N_RECORDS);
    }

    private void writeData(int startRecord, int nRecords) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        for (int i = startRecord; i < nRecords + startRecord; i += 1) {

            key.setData(TestUtils.getTestArray(2*i));
            data.setData(TestUtils.getTestArray(2*i));

            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
    }

    private void putRecord(Cursor cursor, int keyVal, int dataVal) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));
        data.setData(TestUtils.getTestArray(dataVal));

        assertEquals(OperationStatus.SUCCESS, cursor.put(key, data));
    }

    private void insertRecord(Cursor cursor, int keyVal, int dataVal) {

        OperationStatus status = OperationStatus.SUCCESS;
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(keyVal));
        data.setData(TestUtils.getTestArray(dataVal));

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

            assertEquals(2*i, TestUtils.getTestVal(key.getData()));
        }

        assertEquals(OperationStatus.NOTFOUND,
                     cursor.getNext(key, data, null));

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

        bin.latchNoUpdateLRU();
        bin.mutateToBINDelta();
        bin.releaseLatch();

        assertTrue(bin.isBINDelta(false));
        assertTrue(bin.getInListResident());
    }
}
