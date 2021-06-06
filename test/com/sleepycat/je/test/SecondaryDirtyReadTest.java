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

package com.sleepycat.je.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.util.test.SharedTestUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.junit.JUnitMethodThread;
import com.sleepycat.je.util.TestUtils;

/**
 * Tests for multithreading problems when using read-uncommitted with
 * secondaries.  If a primary record is updated while performing a
 * read-uncommitted (in between reading the secondary and the primary), we need
 * to be sure that we don't return inconsistent results to the user.  For
 * example, we should not return a primary data value that no longer contains
 * the secondary key.  We also need to ensure that deleting a primary record in
 * the middle of a secondary read does not appear as a corrupt secondary.  In
 * both of these cases it should appear that the record does not exist, from
 * the viewpoint of an application using a cursor.
 *
 * <p>These tests create two threads, one reading and the other deleting or
 * updating.  The intention is for the reading thread and delete/update thread
 * to race in operating on the same key (nextKey).  If the reading thread reads
 * the secondary, then the other thread deletes the primary, then the reading
 * thread tries to read the primary, we've accomplished our goal.  Prior to
 * when we handled that case in SecondaryCursor, that situation would cause a
 * "secondary corrupt" exception.</p>
 */
@RunWith(Parameterized.class)
public class SecondaryDirtyReadTest extends MultiKeyTxnTestCase {

    private static final int MAX_KEY =
        SharedTestUtils.runLongTests() ? 500000 : 1000;
    private static final int N_DUPS = 3;

    private volatile int nextKey;
    private Database priDb;
    private SecondaryDatabase secDb;
    private final LockMode lockMode;
    private final boolean dups;

    @Parameters
    public static List<Object[]> genParams() {
        
        return paramsHelper(false);
    }
    
    protected static List<Object[]> paramsHelper(boolean rep) {
        final String[] txnTypes = getTxnTypes(null, rep);
        final List<Object[]> newParams = new ArrayList<Object[]>();
        for (final String type : txnTypes) {
            newParams.add(new Object[] {type, true, true, false});
            newParams.add(new Object[] {type, false, true, false});
            newParams.add(new Object[] {type, false, false, false});
            newParams.add(new Object[] {type, true, false, false});
            newParams.add(new Object[] {type, false, true, true});
            newParams.add(new Object[] {type, false, false, true});
        }
        return newParams;
    }
    
    public SecondaryDirtyReadTest(String type, 
                                  boolean multiKey, 
                                  boolean duplicates,
                                  boolean dirtyReadAll){
        initEnvConfig();
        txnType =type;
        useMultiKey = multiKey;
        isTransactional = (txnType != TXN_NULL);
        dups = duplicates;
        lockMode = dirtyReadAll ?
            LockMode.READ_UNCOMMITTED_ALL :
            LockMode.READ_UNCOMMITTED;
        customName = ((useMultiKey) ? "multiKey" : "") +
                     "-" + txnType + "-" + dups;
    }
    
    /**
     * Closes databases, then calls the super.tearDown to close the env.
     */
    @After
    public void tearDown()
        throws Exception {

        if (secDb != null) {
            try {
                secDb.close();
            } catch (Exception e) {}
            secDb = null;
        }
        if (priDb != null) {
            try {
                priDb.close();
            } catch (Exception e) {}
            priDb = null;
        }
        super.tearDown();
    }

    /**
     * Tests that deleting primary records does not cause secondary
     * read-uncommitted to throw a "secondary corrupt" exception.
     */
    @Test
    public void testDeleteWhileReadingByKey()
        throws Throwable {

        doTest("runReadUncommittedByKey", "runPrimaryDelete");
    }

    /**
     * Same as testDeleteWhileReadingByKey but does a scan.  Read-uncommitted
     * for scan and keyed reads are implemented differently, since scanning
     * moves to the next record when a deletion is detected while a keyed read
     * returns NOTFOUND.
     */
    @Test
    public void testDeleteWhileScanning()
        throws Throwable {

        doTest("runReadUncommittedScan", "runPrimaryDelete");
    }

    /**
     * Same as testDeleteWhileScanning but additionally does a join.
     */
    @Test
    public void testDeleteWithJoin()
        throws Throwable {

        doTest("runReadUncommittedJoin", "runPrimaryDelete");
    }

    /**
     * Tests that updating primary records, to cause deletion of the secondary
     * key record, does not cause secondary read-uncommitted to return
     * inconsistent data (a primary datum without a secondary key value).
     */
    @Test
    public void testUpdateWhileReadingByKey()
        throws Throwable {

        doTest("runReadUncommittedByKey", "runPrimaryUpdate");
    }

    /**
     * Same as testUpdateWhileReadingByKey but does a scan.
     */
    @Test
    public void testUpdateWhileScanning()
        throws Throwable {

        doTest("runReadUncommittedScan", "runPrimaryUpdate");
    }

    /**
     * Same as testUpdateWhileScanning but additionally does a join.
     */
    @Test
    public void testUpdateWithJoin()
        throws Throwable {

        doTest("runReadUncommittedJoin", "runPrimaryUpdate");
    }

    @Test
    public void testAlternatingInsertDelete()
        throws Throwable {

        doTest("runReadFirstRecordByKey", "runAlternatingInsertDelete",
               false /*doAddRecords*/);
    }

    private void doTest(String method1, String method2)
        throws Throwable {

        doTest(method1, method2, true /*doAddRecords*/);
    }

    /**
     * Runs two threads for the given method names, after populating the
     * database.
     */
    private void doTest(String method1, String method2, boolean doAddRecords)
        throws Throwable {

        JUnitMethodThread tester1 = new JUnitMethodThread(method1 + "-t1",
                                                          method1, this);
        JUnitMethodThread tester2 = new JUnitMethodThread(method2 + "-t2",
                                                          method2, this);
        priDb = openPrimary("testDB");
        secDb = openSecondary(priDb, "testSecDB");
        if (doAddRecords) {
            addRecords();
        }
        tester1.start();
        tester2.start();
        tester1.finishTest();
        tester2.finishTest();
        secDb.close();
        secDb = null;
        priDb.close();
        priDb = null;
    }

    /**
     * Deletes the key that is being read by the other thread.
     */
    public void runPrimaryDelete()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        while (nextKey < MAX_KEY - 1) {
            Transaction txn = txnBeginCursor();
            key.setData(TestUtils.getTestArray(nextKey));
            /* Alternate use of Cursor.delete and Database.delete. */
            OperationStatus status;
            if ((nextKey & 1) == 0) {
                final Cursor cursor = priDb.openCursor(txn, null);
                status = cursor.getSearchKey(key, new DatabaseEntry(),
                                             LockMode.RMW);
                if (status == OperationStatus.SUCCESS) {
                    status = cursor.delete();
                }
                cursor.close();
            } else {
                status = priDb.delete(txn, key);
            }
            if (status != OperationStatus.SUCCESS) {
                assertEquals(OperationStatus.NOTFOUND, status);
            }
            txnCommit(txn);
        }
    }

    /**
     * Updates the record for the key that is being read by the other thread,
     * changing the datum to -1 so it will cause the secondary key record to
     * be deleted.
     */
    public void runPrimaryUpdate()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        while (nextKey < MAX_KEY - 1) {
            Transaction txn = txnBegin();
            key.setData(TestUtils.getTestArray(nextKey));
            data.setData(TestUtils.getTestArray(-1));
            OperationStatus status = priDb.put(txn, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
            txnCommit(txn);
        }
    }

    /**
     * Does a read-uncommitted by key, retrying until it is deleted by the
     * delete/update thread, then moves to the next key.  We shouldn't get an
     * exception, just a NOTFOUND when it is deleted.
     */
    public void runReadUncommittedByKey()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry pKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        while (nextKey < MAX_KEY - 1) {
            key.setData(TestUtils.getTestArray(nextKey));
            OperationStatus status = secDb.get(null, key, pKey, data,
                                               lockMode);
            if (status != OperationStatus.SUCCESS) {
                assertEquals(OperationStatus.NOTFOUND, status);
                nextKey += 1;
            } else {
                assertEquals(nextKey, TestUtils.getTestVal(key.getData()));
                assertEquals(nextKey, getSecKey(pKey));
                assertEquals(nextKey, getSecKey(data));
                /* For dups, advance next key to the primary key we found. */
                nextKey = TestUtils.getTestVal(pKey.getData());
            }
        }
    }

    /**
     * Does a read-uncommitted scan through the whole key range, but moves
     * forward only after the key is deleted by the delete/update thread.  We
     * shouldn't get an exception or a NOTFOUND, but we may skip values when a
     * key is deleted.
     */
    public void runReadUncommittedScan()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry pKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        SecondaryCursor cursor = secDb.openSecondaryCursor(null, null);
        while (nextKey < MAX_KEY - 1) {
            OperationStatus status = cursor.getNext(key, pKey, data,
                                                    lockMode);
            assertEquals("nextKey=" + nextKey,
                         OperationStatus.SUCCESS, status);
            int keyFound = TestUtils.getTestVal(pKey.getData());
            assertEquals(keyFound, TestUtils.getTestVal(data.getData()));
            assertEquals(getSecKey(keyFound),
                         TestUtils.getTestVal(key.getData()));
            /* Let the delete/update thread catch up. */
            nextKey = keyFound;
            if (nextKey < MAX_KEY - 1) {
                while (status != OperationStatus.KEYEMPTY) {
                    assertEquals(OperationStatus.SUCCESS, status);
                    status = cursor.getCurrent(key, pKey, data,
                                               lockMode);
                }
                nextKey = keyFound + 1;
            }
        }
        cursor.close();
    }

    /**
     * Like runReadUncommittedScan, but also performs a join on each secondary
     * key.
     */
    public void runReadUncommittedJoin()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry pKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry joinPKey = new DatabaseEntry();
        DatabaseEntry joinData = new DatabaseEntry();
        SecondaryCursor cursor = secDb.openSecondaryCursor(null, null);
        while (nextKey < MAX_KEY - 1) {
            OperationStatus status = cursor.getNext(key, pKey, data, lockMode);
            assertEquals("nextKey=" + nextKey,
                         OperationStatus.SUCCESS, status);
            int keyFound = TestUtils.getTestVal(pKey.getData());
            assertEquals(keyFound, TestUtils.getTestVal(data.getData()));
            assertEquals(getSecKey(keyFound),
                         TestUtils.getTestVal(key.getData()));

            /* Do a join on this value.  Use two cursors for the same DB. */
            SecondaryCursor cursor2 = cursor.dup(true /*samePosition*/);
            JoinCursor joinCursor =
                priDb.join(new Cursor[] { cursor, cursor2 }, null);
            int nDups = 0;
            OperationStatus joinStatus =
                joinCursor.getNext(joinPKey, joinData, lockMode);
            while (joinStatus == OperationStatus.SUCCESS) {
                assertEquals(getSecKey(keyFound), getSecKey(joinPKey));
                assertEquals(getSecKey(keyFound), getSecKey(joinData));
                joinStatus = joinCursor.getNext(joinPKey, joinData, lockMode);
            }
            assertTrue("" + nDups, nDups <= N_DUPS);
            assertEquals("nextKey=" + nextKey,
                         OperationStatus.NOTFOUND, joinStatus);
            cursor2.close();
            joinCursor.close();

            /* Let the delete/update thread catch up. */
            nextKey = keyFound;
            if (nextKey < MAX_KEY - 1) {
                while (status != OperationStatus.KEYEMPTY) {
                    assertEquals(OperationStatus.SUCCESS, status);
                    status = cursor.getCurrent(key, pKey, data,
                                               lockMode);
                }
                nextKey = keyFound + 1;
            }
        }
        cursor.close();
    }

    /**
     * Alternate insertion and deletion of key 0.
     */
    public void runAlternatingInsertDelete()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(0));
        data.setData(TestUtils.getTestArray(0));
        while (nextKey == 0) {
            Transaction txn = txnBegin();
            OperationStatus status = priDb.putNoOverwrite(txn, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
            status = priDb.delete(txn, key);
            assertEquals(OperationStatus.SUCCESS, status);
            txnCommit(txn);
        }
    }

    /**
     * Read key 0 while runAlternatingInsertDelete is executing.  The idea is
     * to reproduce a bug that caused SecondaryIntegrityException [#22603].
     */
    public void runReadFirstRecordByKey()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry pKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(0));
        int nDeletions = 0;
        while (nDeletions < MAX_KEY * 10) {
            OperationStatus status = secDb.get(null, key, pKey, data,
                                               lockMode);
            if (status != OperationStatus.SUCCESS) {
                assertEquals(OperationStatus.NOTFOUND, status);
                nDeletions += 1;
            } else {
                assertEquals(0, TestUtils.getTestVal(key.getData()));
                assertEquals(0, getSecKey(pKey));
                assertEquals(0, getSecKey(data));
            }
        }
        nextKey = 1;
    }

    /**
     * Adds records for the entire key range.
     */
    private void addRecords()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = txnBegin();
        for (int i = 0; i < MAX_KEY; i += 1) {
            byte[] val = TestUtils.getTestArray(i);
            key.setData(val);
            data.setData(val);
            OperationStatus status = priDb.putNoOverwrite(txn, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
        }
        txnCommit(txn);
    }

    /**
     * Opens the primary database.
     */
    private Database openPrimary(String name)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        Transaction txn = txnBegin();
        Database priDb;
        try {
            priDb = env.openDatabase(txn, name, dbConfig);
        } finally {
            txnCommit(txn);
        }
        assertNotNull(priDb);
        return priDb;
    }

    /**
     * Opens the secondary database.
     */
    private SecondaryDatabase openSecondary(Database priDb, String dbName)
        throws DatabaseException {

        SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        if (useMultiKey) {
            dbConfig.setMultiKeyCreator
                (new SimpleMultiKeyCreator(new MyKeyCreator()));
        } else {
            dbConfig.setKeyCreator(new MyKeyCreator());
        }
        Transaction txn = txnBegin();
        SecondaryDatabase secDb;
        try {
            secDb = env.openSecondaryDatabase(txn, dbName, priDb, dbConfig);
        } finally {
            txnCommit(txn);
        }
        return secDb;
    }

    /**
     * Creates secondary keys for a primary datum with a non-negative value.
     */
    private class MyKeyCreator implements SecondaryKeyCreator {

        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            int val = getSecKey(data);
            if (val >= 0) {
                result.setData(TestUtils.getTestArray(val));
                return true;
            } else {
                return false;
            }
        }
    }

    private int getSecKey(DatabaseEntry data) {
        int val = TestUtils.getTestVal(data.getData());
        return getSecKey(val);
    }

    /**
     * When dups are configured, the secondary key is truncated to a multiple
     * of N_DUPS.
     */
    private int getSecKey(int val) {
        if (val < 0) {
            return val;
        }
        if (dups) {
            return val - (val % N_DUPS);
        }
        return val;
    }
}
