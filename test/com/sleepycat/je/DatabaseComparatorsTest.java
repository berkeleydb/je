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
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Comparator;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

public class DatabaseComparatorsTest extends DualTestCase {

    private File envHome;
    private Environment env;
    private boolean DEBUG = false;

    public DatabaseComparatorsTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    private void openEnv()
        throws DatabaseException {

        openEnv(false);
    }

    private void openEnv(boolean transactional)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(transactional);
        envConfig.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                                 "true");
        /* Prevent compression. */
        envConfig.setConfigParam("je.env.runINCompressor", "false");
        envConfig.setConfigParam("je.env.runCheckpointer", "false");
        envConfig.setConfigParam("je.env.runEvictor", "false");
        envConfig.setConfigParam("je.env.runCleaner", "false");
        env = create(envHome, envConfig);
    }

    private Database openDb
        (boolean transactional,
         boolean dups,
         Class<? extends Comparator<byte[]>> btreeComparator,
         Class<? extends Comparator<byte[]>> dupComparator)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        dbConfig.setTransactional(transactional);
        if (btreeComparator != null) {
            dbConfig.setBtreeComparator(btreeComparator);
        }
        if (dupComparator != null) {
            dbConfig.setDuplicateComparator(dupComparator);
        }
        return env.openDatabase(null, "testDB", dbConfig);
    }

    @Test
    public void testSR12517()
        throws Exception {

        openEnv();
        Database db = openDb(false /*transactional*/, false /*dups*/,
                             ReverseComparator.class, ReverseComparator.class);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Insert 5 items. */
        for (int i = 0; i < 5; i++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
            /* Add a dup. */
            IntegerBinding.intToEntry(i * 2, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
        read(db);

        db.close();
        close(env);

        openEnv();
        db = openDb(false /*transactional*/, false /*dups*/,
                    ReverseComparator.class, ReverseComparator.class);

        read(db);
        db.close();
        close(env);
        env = null;
    }

    @Test
    public void testDatabaseCompareKeysArgs()
        throws Exception {

        openEnv();
        Database db = openDb(false /*transactional*/, false /*dups*/,
                             null, null);

        DatabaseEntry entry1 = new DatabaseEntry();
        DatabaseEntry entry2 = new DatabaseEntry();

        try {
            db.compareKeys(null, entry2);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareKeys(entry1, null);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareDuplicates(null, entry2);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareDuplicates(entry1, null);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        IntegerBinding.intToEntry(1, entry1);

        try {
            db.compareKeys(entry1, entry2);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareKeys(entry2, entry1);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareDuplicates(entry1, entry2);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareDuplicates(entry2, entry1);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        entry1.setPartial(true);
        IntegerBinding.intToEntry(1, entry2);

        try {
            db.compareKeys(entry1, entry2);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareKeys(entry2, entry1);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareDuplicates(entry1, entry2);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        try {
            db.compareDuplicates(entry2, entry1);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        db.close();

        try {
            db.compareKeys(entry1, entry2);
            fail("should have thrown ISE");
        } catch (IllegalStateException ISE) {
            assertTrue(ISE.getMessage().contains("Database was closed"));
        }

        try {
            db.compareDuplicates(entry1, entry2);
            fail("should have thrown ISE");
        } catch (IllegalStateException ISE) {
            assertTrue(ISE.getMessage().contains("Database was closed"));
        }

        close(env);
        env = null;
    }

    @Test
    public void testSR16816DefaultComparator()
        throws Exception {

        doTestSR16816(null, null, 1);
    }

    @Test
    public void testSR16816ReverseComparator()
        throws Exception {

        doTestSR16816(ReverseComparator.class, ReverseComparator.class, -1);
    }

    private void doTestSR16816(Class btreeComparator,
                               Class dupComparator,
                               int expectedSign)
        throws Exception {

        openEnv();
        Database db = openDb(false /*transactional*/, false /*dups*/,
                             btreeComparator, dupComparator);

        DatabaseEntry entry1 = new DatabaseEntry();
        DatabaseEntry entry2 = new DatabaseEntry();

        IntegerBinding.intToEntry(1, entry1);
        IntegerBinding.intToEntry(2, entry2);

        assertEquals(expectedSign * -1, db.compareKeys(entry1, entry2));
        assertEquals(0, db.compareKeys(entry1, entry1));
        assertEquals(0, db.compareKeys(entry2, entry2));
        assertEquals(expectedSign * 1, db.compareKeys(entry2, entry1));

        assertEquals(expectedSign * -1, db.compareDuplicates(entry1, entry2));
        assertEquals(0, db.compareDuplicates(entry1, entry1));
        assertEquals(0, db.compareDuplicates(entry2, entry2));
        assertEquals(expectedSign * 1, db.compareDuplicates(entry2, entry1));

        db.close();
        close(env);
        env = null;
    }

    private void read(Database db)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Iterate */
        Cursor c = db.openCursor(null, null);
        int expected = 4;
        while (c.getNext(key, data, LockMode.DEFAULT) ==
               OperationStatus.SUCCESS) {
            assertEquals(expected, IntegerBinding.entryToInt(key));
            expected--;
            if (DEBUG) {
                System.out.println("cursor: k=" +
                                   IntegerBinding.entryToInt(key) +
                                   " d=" +
                                   IntegerBinding.entryToInt(data));
            }
        }
        assertEquals(expected, -1);

        c.close();

        /* Retrieve 5 items */
        for (int i = 0; i < 5; i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS,
                         db.get(null, key, data, LockMode.DEFAULT));
            assertEquals(i, IntegerBinding.entryToInt(key));
            assertEquals(i * 2, IntegerBinding.entryToInt(data));
            if (DEBUG) {
                System.out.println("k=" +
                                   IntegerBinding.entryToInt(key) +
                                   " d=" +
                                   IntegerBinding.entryToInt(data));
            }
        }
    }

    public static class ReverseComparator implements Comparator<byte[]> {

        public ReverseComparator() {
        }

        public int compare(byte[] o1, byte[] o2) {

            DatabaseEntry arg1 = new DatabaseEntry(o1);
            DatabaseEntry arg2 = new DatabaseEntry(o2);
            int val1 = IntegerBinding.entryToInt(arg1);
            int val2 = IntegerBinding.entryToInt(arg2);

            if (val1 < val2) {
                return 1;
            } else if (val1 > val2) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    /**
     * Checks that when reusing a slot and then aborting the transaction, the
     * original data is restored, when using a btree comparator. [#15704]
     *
     * When using partial keys to reuse a slot with a different--but equal
     * according to a custom comparator--key, a bug caused corruption of an
     * existing record after an abort.  The sequence for a non-duplicate
     * database and a btree comparator that compares only the first integer in
     * a two integer key is:
     *
     * 100 Insert LN key={0,0} txn 1
     * 110 Commit txn 1
     * 120 Delete LN key={0,0} txn 2
     * 130 Insert LN key={0,1} txn 2
     * 140 Abort txn 2
     *
     * When key {0,1} is inserted at LSN 130, it reuses the slot for {0,0}
     * because these two keys are considered equal by the comparator.  When txn
     * 2 is aborted, it restores LSN 100 in the slot, but the key in the BIN
     * stays {0,1}.  Fetching the record after the abort gives key {0,1}.
     */
    @Test
    public void testReuseSlotAbortPartialKey()
        throws DatabaseException {

        doTestReuseSlotPartialKey(false /*runRecovery*/);
    }

    /**
     * Same as testReuseSlotAbortPartialKey but runs recovery after the abort.
     */
    @Test
    public void testReuseSlotRecoverPartialKey()
        throws DatabaseException {

        doTestReuseSlotPartialKey(true /*runRecovery*/);
    }

    private void doTestReuseSlotPartialKey(boolean runRecovery)
        throws DatabaseException {

        openEnv(true /*transactional*/);
        Database db = openDb
            (true /*transactional*/, false /*dups*/,
             Partial2PartComparator.class /*btreeComparator*/,
             null /*dupComparator*/);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key={0,0}/data={0} using auto-commit. */
        status = db.put(null, entry(0, 0), entry(0));
        assertSame(OperationStatus.SUCCESS, status);
        key = entry(0, 1);
        data = entry(0);
        status = db.getSearchBoth(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0, 0);
        check(data, 0);

        /* Delete, insert key={0,1}/data={1}, abort. */
        Transaction txn = env.beginTransaction(null, null);
        status = db.delete(txn, entry(0, 1));
        assertSame(OperationStatus.SUCCESS, status);
        status = db.get(txn, entry(0, 0), data, null);
        assertSame(OperationStatus.NOTFOUND, status);
        status = db.put(txn, entry(0, 1), entry(1));
        assertSame(OperationStatus.SUCCESS, status);
        key = entry(0, 0);
        data = entry(1);
        status = db.getSearchBoth(txn, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0, 1);
        check(data, 1);
        txn.abort();

        if (runRecovery) {
            db.close();
            close(env);
            env = null;
            openEnv(true /*transactional*/);
            db = openDb
                (true /*transactional*/, false /*dups*/,
                 Partial2PartComparator.class /*btreeComparator*/,
                 null /*dupComparator*/);
        }

        /* Check that we rolled back to key={0,0}/data={0}. */
        key = entry(0, 1);
        data = entry(0);
        status = db.getSearchBoth(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0, 0);
        check(data, 0);

        db.close();
        close(env);
        env = null;
    }

    /**
     * Same as testReuseSlotAbortPartialKey but for reuse of duplicate data
     * slots.  [#15704]
     *
     * The sequence for a duplicate database and a duplicate comparator that
     * compares only the first integer in a two integer data value is:
     *
     * 100 Insert LN key={0}/data={0,0} txn 1
     * 110 Insert LN key={0}/data={1,1} txn 1
     * 120 Commit txn 1
     * 130 Delete LN key={0}/data={0,0} txn 2
     * 140 Insert LN key={0}/data={0,1} txn 2
     * 150 Abort txn 2
     *
     * When data {0,1} is inserted at LSN 140, it reuses the slot for {0,0}
     * because these two data values are considered equal by the comparator.
     * When txn 2 is aborted, it restores LSN 100 in the slot, but the data in
     * the DBIN stays {0,1}.  Fetching the record after the abort gives data
     * {0,1}.
     */
    @Test
    public void testReuseSlotAbortPartialDup()
        throws DatabaseException {

        doTestReuseSlotPartialDup(false /*runRecovery*/);
    }

    /**
     * Same as testReuseSlotAbortPartialDup but runs recovery after the abort.
     */
    @Test
    public void testReuseSlotRecoverPartialDup()
        throws DatabaseException {

        doTestReuseSlotPartialDup(true /*runRecovery*/);
    }

    private void doTestReuseSlotPartialDup(boolean runRecovery)
        throws DatabaseException {

        openEnv(true /*transactional*/);
        Database db = openDb
            (true /*transactional*/, true /*dups*/,
             null /*btreeComparator*/,
             Partial2PartComparator.class /*dupComparator*/);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key={0}/data={0,0} using auto-commit. */
        Transaction txn = env.beginTransaction(null, null);
        status = db.put(txn, entry(0), entry(0, 0));
        assertSame(OperationStatus.SUCCESS, status);
        status = db.put(txn, entry(0), entry(1, 1));
        assertSame(OperationStatus.SUCCESS, status);
        txn.commit();
        key = entry(0);
        data = entry(0, 1);
        status = db.getSearchBoth(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0);
        check(data, 0, 0);

        /* Delete, insert key={0}/data={0,1}, abort. */
        txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        key = entry(0);
        data = entry(0, 1);
        status = cursor.getSearchBoth(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0);
        check(data, 0, 0);
        status = cursor.delete();
        assertSame(OperationStatus.SUCCESS, status);
        status = cursor.put(entry(0), entry(0, 1));
        assertSame(OperationStatus.SUCCESS, status);
        key = entry(0);
        data = entry(0, 1);
        status = cursor.getSearchBoth(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0);
        check(data, 0, 1);
        cursor.close();
        txn.abort();

        if (runRecovery) {
            db.close();
            close(env);
            env = null;
            openEnv(true /*transactional*/);
            db = openDb
                (true /*transactional*/, true /*dups*/,
                 null /*btreeComparator*/,
                 Partial2PartComparator.class /*dupComparator*/);
        }

        /* Check that we rolled back to key={0,0}/data={0}. */
        key = entry(0);
        data = entry(0, 1);
        status = db.getSearchBoth(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0);
        check(data, 0, 0);

        db.close();
        close(env);
        env = null;
    }

    /**
     * In the past, we prohibited the case where dups are configured and the
     * btree comparator does not compare all bytes of the key.  With the old
     * DBIN/DIN dup implementation, to support this would require maintaining
     * the BIN slot and DIN/DBIN.dupKey fields to be transactionally correct.
     * This would have impractical since INs by design are non-transctional.
     * [#15704]
     *
     * But with the two-part key dups implementation, the BIN slot is always
     * transactionally correct, and this test now confirms this. [#19165]
     */
    @Test
    public void testDupsWithPartialComparator()
        throws DatabaseException {

        openEnv(false /*transactional*/);
        Database db = openDb
            (false /*transactional*/, true /*dups*/,
             Partial2PartComparator.class /*btreeComparator*/,
             null /*dupComparator*/);

        OperationStatus status;

        /* Insert key={0,0}/data={0}. */
        status = db.put(null, entry(0, 0), entry(0));
        assertSame(OperationStatus.SUCCESS, status);

        /* Update to key={0,1}/data={0}. */
        status = db.put(null, entry(0, 1), entry(0));
        assertSame(OperationStatus.SUCCESS, status);
        DatabaseEntry key = entry(0, 0);
        DatabaseEntry data = entry(0);
        status = db.get(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        check(key, 0, 1);
        check(data, 0);

        db.close();
        close(env);
        env = null;
    }

    private void check(DatabaseEntry entry, int p1) {
        assertEquals(4, entry.getSize());
        TupleInput input = TupleBase.entryToInput(entry);
        assertEquals(p1, input.readInt());
    }

    private void check(DatabaseEntry entry, int p1, int p2) {
        assertEquals(8, entry.getSize());
        TupleInput input = TupleBase.entryToInput(entry);
        assertEquals(p1, input.readInt());
        assertEquals(p2, input.readInt());
    }

    /*
    private void dump(Database db, Transaction txn)
        throws DatabaseException {

        System.out.println("-- dump --");
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;
        Cursor c = db.openCursor(txn, null);
        while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
            TupleInput keyInput = TupleBase.entryToInput(key);
            int keyP1 = keyInput.readInt();
            int keyP2 = keyInput.readInt();
            int dataVal = IntegerBinding.entryToInt(data);
            System.out.println("keyP1=" + keyP1 +
                               " keyP2=" + keyP2 +
                               " dataVal=" + dataVal);
        }
        c.close();
    }
    */

    private DatabaseEntry entry(int p1) {
        DatabaseEntry entry = new DatabaseEntry();
        TupleOutput output = new TupleOutput();
        output.writeInt(p1);
        TupleBase.outputToEntry(output, entry);
        return entry;
    }

    private DatabaseEntry entry(int p1, int p2) {
        DatabaseEntry entry = new DatabaseEntry();
        TupleOutput output = new TupleOutput();
        output.writeInt(p1);
        output.writeInt(p2);
        TupleBase.outputToEntry(output, entry);
        return entry;
    }

    /**
     * Writes two integers to the byte array.
     */
    private void make2PartEntry(int p1, int p2, DatabaseEntry entry) {
        TupleOutput output = new TupleOutput();
        output.writeInt(p1);
        output.writeInt(p2);
        TupleBase.outputToEntry(output, entry);
    }

    /**
     * Compares only the first integer in the byte arrays.
     */
    public static class Partial2PartComparator
        implements Comparator<byte[]>, PartialComparator {

        public int compare(byte[] o1, byte[] o2) {
            int val1 = new TupleInput(o1).readInt();
            int val2 = new TupleInput(o2).readInt();
            return val1 - val2;
        }
    }
}
