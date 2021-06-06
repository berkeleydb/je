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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.utilint.StringUtils;

import org.junit.Test;

/**
 * Various unit tests for CursorImpl using duplicates.
 */
public class DbCursorDuplicateDeleteTest extends DbCursorTestBase {

    private AtomicInteger sequence;

    public DbCursorDuplicateDeleteTest() {
        super();
    }

    /**
     * Create some simple duplicate data.  Delete it all.  Try to create
     * it again.
     */
    @Test
    public void testSimpleDeleteInsert()
        throws Exception {

        try {
            initEnv(true);
            doSimpleDuplicatePuts();
            DataWalker dw = new DataWalker(null) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        if (prevKey.equals("")) {
                            prevKey = foundKey;
                        }
                        if (!prevKey.equals(foundKey)) {
                            deletedEntries = 0;
                        }
                        prevKey = foundKey;
                        if (cursor.delete() == OperationStatus.SUCCESS) {
                            deletedEntries++;
                        }
                        assertEquals(simpleKeyStrings.length - deletedEntries,
                                     cursor.count());
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            doSimpleDuplicatePuts();

            dw = new DataWalker(null);
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertEquals(simpleKeyStrings.length * simpleKeyStrings.length,
                         dw.nEntries);
            closeEnv();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testCountAfterDelete()
        throws Throwable {
        initEnv(true);
        DatabaseEntry key =
            new DatabaseEntry(new byte[] {(byte) 'n',
                                          (byte) 'o', (byte) 0 });
        DatabaseEntry val1 =
            new DatabaseEntry(new byte[] {(byte) 'k',
                                          (byte) '1', (byte) 0 });
        DatabaseEntry val2 =
            new DatabaseEntry(new byte[] {(byte) 'k',
                                          (byte) '2', (byte) 0 });
        OperationStatus status =
            exampleDb.putNoDupData(null, key, val1);
        if (status != OperationStatus.SUCCESS)
            throw new Exception("status on put 1=" + status);
        status = exampleDb.putNoDupData(null, key, val2);
        if (status != OperationStatus.SUCCESS)
            throw new Exception("status on put 2=" + status);

        Cursor c = exampleDb.openCursor(null, null);
        try {
            status = c.getSearchKey(key, new DatabaseEntry(),
                                    LockMode.DEFAULT);
            if (status != OperationStatus.SUCCESS)
                throw new Exception("status on search=" + status);
            assertEquals(2, c.count());
            status = c.delete();
            if (status != OperationStatus.SUCCESS)
                throw new Exception("err on del 1=" + status);
            status = c.getNext(key, new DatabaseEntry(), LockMode.DEFAULT);
            if (status != OperationStatus.SUCCESS)
                throw new Exception("err on next=" + status);
            status = c.delete();
            if (status != OperationStatus.SUCCESS)
                throw new Exception("err on del 2=" + status);
            assertEquals(0, c.count());
        } finally {
            c.close();
        }

        status = exampleDb.putNoDupData(null, key, val1);
        if (status != OperationStatus.SUCCESS)
            throw new Exception("err on put 3=" + status);

        c = exampleDb.openCursor(null, null);
        try {
            status =
                c.getSearchKey(key, new DatabaseEntry(), LockMode.DEFAULT);
            if (status != OperationStatus.SUCCESS)
                throw new Exception("err on search=" + status);
            assertEquals(1, c.count());
        } finally {
            c.close();
        }
    }

    @Test
    public void testDuplicateDeletionAll()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();
            createRandomDuplicateData(10, 1000, dataMap, false, false);

            DataWalker dw = new DataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        if (ht.get(foundData) != null) {
                            ht.remove(foundData);
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }
                        } else {
                            fail("didn't find " + foundKey + "/" + foundData);
                        }

                        /* Make sure keys are ascending/descending. */
                        assertTrue(foundKey.compareTo(prevKey) >= 0);

                        /*
                         * Make sure duplicate items within key are asc/desc.
                         */
                        if (prevKey.equals(foundKey)) {
                            if (duplicateComparisonFunction == null) {
                                assertTrue(foundData.compareTo(prevData) >= 0);
                            } else {
                                assertTrue
                                    (duplicateComparisonFunction.compare
                                     (StringUtils.toUTF8(foundData),
                                      StringUtils.toUTF8(prevData)) >= 0);
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                        assertTrue(cursor.delete() == OperationStatus.SUCCESS);
                        assertEquals(ht.size(), cursor.count());
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dataMap.size() == 0);

            dw = new DataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        fail("data found after deletion: " +
                             foundKey + "/" + foundData);
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateDeletionAssorted()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();
            Hashtable deletedDataMap = new Hashtable();
            createRandomDuplicateData(10, 1000, dataMap, false, false);

            /* Use the DataWalker.addedData field for a deleted Data Map. */
            DataWalker dw = new DataWalker(dataMap, deletedDataMap) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        /* Make sure keys are ascending/descending. */
                        assertTrue(foundKey.compareTo(prevKey) >= 0);

                        /*
                         * Make sure duplicate items within key are asc/desc.
                         */
                        if (prevKey.equals(foundKey)) {
                            if (duplicateComparisonFunction == null) {
                                assertTrue(foundData.compareTo(prevData) >= 0);
                            } else {
                                assertTrue
                                    (duplicateComparisonFunction.compare
                                     (StringUtils.toUTF8(foundData),
                                      StringUtils.toUTF8(prevData)) >= 0);
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                        if (rnd.nextInt(10) < 8) {
                            Hashtable delht =
                                (Hashtable) addedDataMap.get(foundKey);
                            if (delht == null) {
                                delht = new Hashtable();
                                addedDataMap.put(foundKey, delht);
                            }
                            delht.put(foundData, foundData);
                            assertTrue(cursor.delete() ==
                                       OperationStatus.SUCCESS);

                            if (ht.get(foundData) == null) {
                                fail("didn't find " +
                                     foundKey + "/" + foundData);
                            }
                            ht.remove(foundData);
                            assertEquals(ht.size(), cursor.count());
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();

            dw = new DataWalker(dataMap, deletedDataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable delht =
                            (Hashtable) addedDataMap.get(foundKey);
                        if (delht != null &&
                            delht.get(foundData) != null) {
                            fail("found deleted entry for " +
                                 foundKey + "/" + foundData);
                        }

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("couldn't find hashtable for " + foundKey);
                        }
                        if (ht.get(foundData) == null) {
                            fail("couldn't find entry for " +
                                 foundKey + "/" + foundData);
                        }
                        ht.remove(foundData);
                        if (ht.size() == 0) {
                            dataMap.remove(foundKey);
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateDeletionAssortedSR15375()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();
            Hashtable deletedDataMap = new Hashtable();
            createRandomDuplicateData(10, 1000, dataMap, false, false);

            /* Use the DataWalker.addedData field for a deleted Data Map. */
            DataWalker dw = new DataWalker(dataMap, deletedDataMap) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        /* Make sure keys are ascending/descending. */
                        assertTrue(foundKey.compareTo(prevKey) >= 0);

                        /*
                         * Make sure duplicate items within key are asc/desc.
                         */
                        if (prevKey.equals(foundKey)) {
                            if (duplicateComparisonFunction == null) {
                                assertTrue(foundData.compareTo(prevData) >= 0);
                            } else {
                                assertTrue
                                    (duplicateComparisonFunction.compare
                                     (StringUtils.toUTF8(foundData),
                                      StringUtils.toUTF8(prevData)) >= 0);
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                        if (rnd.nextInt(10) < 8) {
                            Hashtable delht =
                                (Hashtable) addedDataMap.get(foundKey);
                            if (delht == null) {
                                delht = new Hashtable();
                                addedDataMap.put(foundKey, delht);
                            }
                            delht.put(foundData, foundData);
                            assertTrue(cursor.delete() ==
                                       OperationStatus.SUCCESS);

                            if (ht.get(foundData) == null) {
                                fail("didn't find " +
                                     foundKey + "/" + foundData);
                            }
                            ht.remove(foundData);
                            assertEquals(ht.size(), cursor.count());
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }

                            /*
                             * Add back in a duplicate for each one deleted.
                             */
                            String newDupData = foundData + "x";
                            StringDbt newDupDBT =
                                new StringDbt(newDupData);
                            assertTrue
                                (putAndVerifyCursor
                                 (cursor,
                                  new StringDbt(foundKey),
                                  newDupDBT, true) ==
                                 OperationStatus.SUCCESS);
                            ht.put(newDupData, newDupData);
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();

            dw = new DataWalker(dataMap, deletedDataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable delht =
                            (Hashtable) addedDataMap.get(foundKey);
                        if (delht != null &&
                            delht.get(foundData) != null) {
                            fail("found deleted entry for " +
                                 foundKey + "/" + foundData);
                        }

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("couldn't find hashtable for " + foundKey);
                        }
                        if (ht.get(foundData) == null) {
                            fail("couldn't find entry for " +
                                 foundKey + "/" + foundData);
                        }
                        ht.remove(foundData);
                        if (ht.size() == 0) {
                            dataMap.remove(foundKey);
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateDeleteFirst()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();
            Hashtable deletedDataMap = new Hashtable();
            createRandomDuplicateData(-10, 10, dataMap, false, false);

            /* Use the DataWalker.addedData field for a deleted Data Map. */
            DataWalker dw = new DataWalker(dataMap, deletedDataMap) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        /* Make sure keys are ascending/descending. */
                        assertTrue(foundKey.compareTo(prevKey) >= 0);

                        /*
                         * Make sure duplicate items within key are asc/desc.
                         */
                        if (prevKey.equals(foundKey)) {
                            if (duplicateComparisonFunction == null) {
                                assertTrue(foundData.compareTo(prevData) >= 0);
                            } else {
                                assertTrue
                                    (duplicateComparisonFunction.compare
                                     (StringUtils.toUTF8(foundData),
                                      StringUtils.toUTF8(prevData)) >= 0);
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                            if (cursor.count() > 1) {
                                Hashtable delht =
                                    (Hashtable) addedDataMap.get(foundKey);
                                if (delht == null) {
                                    delht = new Hashtable();
                                    addedDataMap.put(foundKey, delht);
                                }
                                delht.put(foundData, foundData);
                                assertTrue(cursor.delete() ==
                                           OperationStatus.SUCCESS);

                                if (ht.get(foundData) == null) {
                                    fail("didn't find " +
                                         foundKey + "/" + foundData);
                                }
                                ht.remove(foundData);
                                assertEquals(ht.size(), cursor.count());
                                if (ht.size() == 0) {
                                    dataMap.remove(foundKey);
                                }
                            }
                        }

                        prevKey = foundKey;
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();

            dw = new DataWalker(dataMap, deletedDataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable delht =
                            (Hashtable) addedDataMap.get(foundKey);
                        if (delht != null &&
                            delht.get(foundData) != null) {
                            fail("found deleted entry for " +
                                 foundKey + "/" + foundData);
                        }

                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("couldn't find hashtable for " + foundKey);
                        }
                        if (ht.get(foundData) == null) {
                            fail("couldn't find entry for " +
                                 foundKey + "/" + foundData);
                        }
                        ht.remove(foundData);
                        if (ht.size() == 0) {
                            dataMap.remove(foundKey);
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Similar to above test, but there was some question about whether
     * this tests new functionality or not.  Insert k1/d1 and d1/k1.
     * Iterate through the data and delete k1/d1.  Reinsert k1/d1 and
     * make sure it inserts ok.
     */
    @Test
    public void testSimpleSingleElementDupTree()
        throws DatabaseException {

        initEnv(true);
        StringDbt key = new StringDbt("k1");
        StringDbt data1 = new StringDbt("d1");
        StringDbt data2 = new StringDbt("d2");

        assertEquals(OperationStatus.SUCCESS,
                     putAndVerifyCursor(cursor, key, data1, true));
        assertEquals(OperationStatus.SUCCESS,
                     putAndVerifyCursor(cursor, key, data2, true));

        DataWalker dw = new DataWalker(null) {
                @Override
        void perData(String foundKey, String foundData)
                    throws DatabaseException {

                    if (foundKey.equals("k1") && deletedEntries == 0) {
                        if (cursor.delete() == OperationStatus.SUCCESS) {
                            deletedEntries++;
                        }
                    }
                }
            };
        dw.setIgnoreDataMap(true);
        dw.walkData();

        dw = new DataWalker(null) {
                @Override
        void perData(String foundKey, String foundData) {
                    deletedEntries++;
                }
            };
        dw.setIgnoreDataMap(true);
        dw.walkData();

        assertEquals(1, dw.deletedEntries);
    }

    @Test
    public void testEmptyNodes()
        throws Throwable {

        initEnv(true);
        synchronized (DbInternal.getNonNullEnvImpl(exampleEnv).
                      getINCompressor()) {
            writeEmptyNodeData();

            exampleEnv.compress();

            Cursor cursor = exampleDb.openCursor(null, null);
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();
            OperationStatus status = cursor.getFirst(foundKey, foundData,
                                                     LockMode.DEFAULT);
            cursor.close();
            assertEquals(OperationStatus.SUCCESS, status);
        }
    }

    @Test
    public void testDeletedReplaySR8984()
        throws DatabaseException {

        initEnvTransactional(true);
        Transaction txn = exampleEnv.beginTransaction(null, null);
        Cursor c = exampleDb.openCursor(txn, null);
        c.put(simpleKeys[0], simpleData[0]);
        c.delete();
        for (int i = 1; i < 3; i++) {
            c.put(simpleKeys[0], simpleData[i]);
        }
        c.close();
        txn.abort();
        txn = exampleEnv.beginTransaction(null, null);
        c = exampleDb.openCursor(txn, null);
        assertEquals(OperationStatus.NOTFOUND,
                     c.getFirst(new DatabaseEntry(),
                                new DatabaseEntry(),
                                LockMode.DEFAULT));
        c.close();
        txn.commit();
    }

    @Test
    public void testDuplicateDeadlockSR9885()
        throws DatabaseException {

        initEnvTransactional(true);
        Transaction txn = exampleEnv.beginTransaction(null, null);
        Cursor c = exampleDb.openCursor(txn, null);
        for (int i = 0; i < simpleKeyStrings.length; i++) {
            c.put(simpleKeys[0], simpleData[i]);
        }
        c.close();
        txn.commit();
        sequence = new AtomicInteger(0);

        JUnitThread tester1 =
            new JUnitThread("testDuplicateDeadlock1") {
                @Override
                public void testBody()
                    throws DatabaseException {

                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry data = new DatabaseEntry();
                    Transaction txn1 = exampleEnv.beginTransaction(null, null);
                    Cursor cursor1 = exampleDb.openCursor(txn1, null);
                    try {
                        cursor1.getFirst(key, data, LockMode.DEFAULT);
                        sequence.incrementAndGet();
                        while (sequence.get() < 2) {
                            Thread.yield();
                        }
                        cursor1.delete();
                        sequence.incrementAndGet();
                        while (sequence.get() < 4) {
                            Thread.yield();
                        }

                    } catch (LockConflictException e) {
                    } finally {
                        cursor1.close();
                        txn1.abort();
                        sequence.set(4);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testDuplicateDeadlock2") {
                @Override
                public void testBody()
                    throws DatabaseException {

                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry data = new DatabaseEntry();
                    Transaction txn2 = exampleEnv.beginTransaction(null, null);
                    Cursor cursor2 = exampleDb.openCursor(txn2, null);
                    try {
                        while (sequence.get() < 1) {
                            Thread.yield();
                        }
                        cursor2.getLast(key, data, LockMode.DEFAULT);
                        sequence.incrementAndGet();
                        //cursor2.put(key,
                        //new DatabaseEntry(StringUtils.toUTF8("d1d1d1")));
                        cursor2.delete();
                        sequence.incrementAndGet();
                        while (sequence.get() < 4) {
                            Thread.yield();
                        }

                    } catch (LockConflictException e) {
                    } finally {
                        cursor2.close();
                        txn2.abort();
                        sequence.set(4);
                    }
                }
            };

        try {
            tester1.start();
            tester2.start();
            tester1.finishTest();
            tester2.finishTest();
            DatabaseImpl dbImpl = DbInternal.getDbImpl(exampleDb);
            dbImpl.verify(new VerifyConfig());
        } catch (Throwable T) {
            fail("testDuplicateDeadlock caught: " + T);
        }
    }

    @Test
    public void testSR9992()
        throws DatabaseException {

        initEnvTransactional(true);
        Transaction txn = exampleEnv.beginTransaction(null, null);
        Cursor c = exampleDb.openCursor(txn, null);
        for (int i = 1; i < simpleKeys.length; i++) {
            c.put(simpleKeys[0], simpleData[i]);
        }
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        c.getCurrent(key, data, LockMode.DEFAULT);
        c.delete();
        assertEquals(OperationStatus.KEYEMPTY,
                     c.putCurrent
                     (new DatabaseEntry(StringUtils.toUTF8("aaaa"))));
        c.close();
        txn.commit();
    }

    @Test
    public void testSR9900()
        throws DatabaseException {

        initEnvTransactional(true);
        Transaction txn = exampleEnv.beginTransaction(null, null);
        Cursor c = exampleDb.openCursor(txn, null);
        c.put(simpleKeys[0], simpleData[0]);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        c.getCurrent(key, data, LockMode.DEFAULT);
        c.delete();
        assertEquals(OperationStatus.KEYEMPTY,
                     c.putCurrent
                     (new DatabaseEntry(StringUtils.toUTF8("aaaa"))));
        c.close();
        txn.commit();
    }

    private void put(int data, int key)
        throws DatabaseException {

        byte[] keyBytes = new byte[1];
        keyBytes[0] = (byte) (key & 0xff);
        DatabaseEntry keyDbt = new DatabaseEntry(keyBytes);

        byte[] dataBytes = new byte[1];
        if (data == -1) {
            dataBytes = new byte[0];
        } else {
            dataBytes[0] = (byte) (data & 0xff);
        }
        DatabaseEntry dataDbt = new DatabaseEntry(dataBytes);

        OperationStatus status = exampleDb.put(null, keyDbt, dataDbt);
        if (status != OperationStatus.SUCCESS) {
            System.out.println("db.put returned " + status +
                               " for key " + key + "/" + data);
        }
    }

    private void del(int key)
        throws DatabaseException {

        byte[] keyBytes = new byte[1];
        keyBytes[0] = (byte) (key & 0xff);
        DatabaseEntry keyDbt = new DatabaseEntry(keyBytes);

        OperationStatus status = exampleDb.delete(null, keyDbt);
        if (status != OperationStatus.SUCCESS) {
            System.out.println("db.del returned " + status +
                               " for key " + key);
        }
    }

    private void delBoth(int key, int data)
        throws DatabaseException {

        byte[] keyBytes = new byte[1];
        keyBytes[0] = (byte) (key & 0xff);
        DatabaseEntry keyDbt = new DatabaseEntry(keyBytes);

        byte[] dataBytes = new byte[1];
        dataBytes[0] = (byte) (data & 0xff);
        DatabaseEntry dataDbt = new DatabaseEntry(dataBytes);

        Cursor cursor = exampleDb.openCursor(null, null);
        OperationStatus status =
            cursor.getSearchBoth(keyDbt, dataDbt, LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) {
            System.out.println("getSearchBoth returned " + status +
                               " for key " + key + "/" + data);
        }

        status = cursor.delete();
        if (status != OperationStatus.SUCCESS) {
            System.out.println("Dbc.delete returned " + status +
                               " for key " + key + "/" + data);
        }
        cursor.close();
    }

    private void writeEmptyNodeData()
        throws DatabaseException {

        put(101, 1);
        put(102, 2);
        put(103, 3);
        put(104, 4);
        put(105, 5);
        put(106, 6);
        del(1);
        del(3);
        del(5);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(3);
        del(5);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(3);
        del(5);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(-1, 2);
        put(-1, 4);
        put(-1, 6);
        put(-1, 1);
        put(-1, 3);
        put(-1, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        del(1);
        del(2);
        del(3);
        del(4);
        del(5);
        del(6);
        put(102, 2);
        put(104, 4);
        put(106, 6);
        put(101, 1);
        put(103, 3);
        put(105, 5);
        put(102, 1);
        put(103, 1);
        put(104, 1);
        put(105, 1);
        delBoth(1, 101);
        delBoth(1, 102);
        delBoth(1, 103);
        delBoth(1, 104);
        delBoth(1, 105);
        put(101, 1);
        put(102, 1);
        put(103, 1);
        put(104, 1);
        put(105, 1);
        delBoth(1, 101);
        delBoth(1, 102);
        delBoth(1, 103);
        delBoth(1, 104);
        delBoth(1, 105);
        put(101, 1);
        put(102, 1);
        put(103, 1);
        put(104, 1);
        put(105, 1);
        delBoth(1, 101);
        delBoth(1, 102);
        delBoth(1, 103);
        delBoth(1, 104);
        delBoth(1, 105);
        put(101, 1);
        put(102, 1);
        put(103, 1);
        put(104, 1);
        put(105, 1);
        delBoth(1, 102);
        delBoth(1, 103);
        delBoth(1, 104);
        delBoth(1, 105);
        put(103, 2);
        put(104, 2);
        put(105, 2);
        put(106, 2);
        delBoth(2, 102);
        delBoth(2, 103);
        delBoth(2, 104);
        delBoth(2, 105);
        delBoth(2, 106);
        put(102, 2);
        put(103, 2);
        put(104, 2);
        put(105, 2);
        put(106, 2);
        delBoth(2, 102);
        delBoth(2, 103);
        delBoth(2, 104);
        delBoth(2, 105);
        delBoth(2, 106);
        put(102, 2);
        put(103, 2);
        put(104, 2);
        put(105, 2);
        put(106, 2);
        delBoth(2, 102);
        delBoth(2, 103);
        delBoth(2, 104);
        delBoth(2, 105);
        delBoth(2, 106);
        put(102, 2);
        put(103, 2);
        put(104, 2);
        put(105, 2);
        put(106, 2);
        delBoth(2, 102);
        delBoth(2, 103);
        delBoth(2, 104);
        delBoth(2, 105);
        delBoth(2, 106);
        put(107, 6);
        put(108, 6);
        put(109, 6);
        put(110, 6);
        delBoth(6, 106);
        delBoth(6, 107);
        delBoth(6, 108);
        delBoth(6, 109);
        delBoth(6, 110);
        put(106, 6);
        put(107, 6);
        put(108, 6);
        put(109, 6);
        put(110, 6);
        delBoth(6, 106);
        delBoth(6, 107);
        delBoth(6, 108);
        delBoth(6, 109);
        delBoth(6, 110);
        put(106, 6);
        put(107, 6);
        put(108, 6);
        put(109, 6);
        put(110, 6);
        delBoth(6, 106);
        delBoth(6, 107);
        delBoth(6, 108);
        delBoth(6, 109);
        delBoth(6, 110);
        put(106, 6);
        put(107, 6);
        put(108, 6);
        put(109, 6);
        put(110, 6);
        delBoth(6, 107);
        delBoth(6, 108);
        delBoth(6, 109);
        delBoth(6, 110);
        put(106, 5);
        put(107, 5);
        put(108, 5);
        put(109, 5);
        delBoth(5, 105);
        delBoth(5, 106);
        delBoth(5, 107);
        delBoth(5, 108);
        delBoth(5, 109);
        put(105, 5);
        put(106, 5);
        put(107, 5);
        put(108, 5);
        put(109, 5);
        delBoth(5, 105);
        delBoth(5, 106);
        delBoth(5, 107);
        delBoth(5, 108);
        delBoth(5, 109);
        put(105, 5);
        put(106, 5);
        put(107, 5);
        put(108, 5);
        put(109, 5);
        delBoth(5, 105);
        delBoth(5, 106);
        delBoth(5, 107);
        delBoth(5, 108);
        delBoth(5, 109);
        put(105, 5);
        put(106, 5);
        put(107, 5);
        put(108, 5);
        put(109, 5);
        delBoth(5, 106);
        delBoth(5, 107);
        delBoth(5, 108);
        delBoth(5, 109);
        delBoth(1, 101);
    }
}
