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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.utilint.StringUtils;

public class RecoveryTest extends RecoveryTestBase {

    /**
     * Basic insert, delete data.
     */
    @Test
    public void testBasic()
        throws Throwable {

        doBasic(true);
    }

    /**
     * Basic insert, delete data with BtreeComparator
     */
    @Test
    public void testBasicRecoveryWithBtreeComparator()
        throws Throwable {

        btreeComparisonFunction = new BtreeComparator(true);
        doBasic(true);
    }

    /**
     * Test that put(OVERWRITE) works correctly with duplicates.
     */
    @Test
    public void testDuplicateOverwrite()
        throws Throwable {

        createEnvAndDbs(1 << 10, false, NUM_DBS);
        try {
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            Transaction txn = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(StringUtils.toUTF8("aaaaa"));
            DatabaseEntry data1 =
                new DatabaseEntry(StringUtils.toUTF8("dddddddddd"));
            DatabaseEntry data2 =
                new DatabaseEntry(StringUtils.toUTF8("eeeeeeeeee"));
            DatabaseEntry data3 =
                new DatabaseEntry(StringUtils.toUTF8("ffffffffff"));
            Database db = dbs[0];
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, key, data1));
            addExpectedData(expectedData, 0, key, data1, true);
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, key, data2));
            addExpectedData(expectedData, 0, key, data2, true);
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, key, data3));
            addExpectedData(expectedData, 0, key, data3, true);
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, key, data3));
            txn.commit();
            closeEnv();

            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Basic insert, delete data.
     */
    @Test
    public void testBasicFewerCheckpoints()
        throws Throwable {

        doBasic(false);
    }

    @Test
    public void testSR8984Part1()
        throws Throwable {

        doTestSR8984Work(true);
    }

    @Test
    public void testSR8984Part2()
        throws Throwable {

        doTestSR8984Work(false);
    }

    private void doTestSR8984Work(boolean sameKey)
        throws DatabaseException {

        final int NUM_EXTRA_DUPS = 150;
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        /* Make an environment and open it */
        envConfig.setTransactional(false);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_CLEANER.getName(),
                                 "false");

        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        Database db = env.openDatabase(null, "testSR8984db", dbConfig);

        DatabaseEntry key = new DatabaseEntry(StringUtils.toUTF8("k1"));
        DatabaseEntry data = new DatabaseEntry(StringUtils.toUTF8("d1"));
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        assertEquals(OperationStatus.SUCCESS, db.delete(null, key));

        if (!sameKey) {
            data.setData(StringUtils.toUTF8("d2"));
        }
        /* Cause a dup tree of some depth to be created. */
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        for (int i = 3; i < NUM_EXTRA_DUPS; i++) {
            data.setData(StringUtils.toUTF8("d" + i));
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        data.setData(StringUtils.toUTF8("d1"));

        Cursor c = db.openCursor(null, null);
        assertEquals(OperationStatus.SUCCESS,
                     c.getFirst(key, data, LockMode.DEFAULT));

        c.close();
        db.close();

        /* Force an abrupt close so there is no checkpoint at the end. */
        closeEnv();
        env = new Environment(envHome, envConfig);
        db = env.openDatabase(null, "testSR8984db", dbConfig);
        c = db.openCursor(null, null);
        assertEquals(OperationStatus.SUCCESS,
                     c.getFirst(key, data, LockMode.DEFAULT));
        assertEquals(NUM_EXTRA_DUPS - 2, c.count());
        c.close();
        db.close();
        env.close();
    }

    /**
     * Insert data, delete data into several dbs.
     */
    public void doBasic(boolean runCheckpointerDaemon)
        throws Throwable {

        createEnvAndDbs(1 << 20, runCheckpointerDaemon, NUM_DBS);
        int numRecs = NUM_RECS;

        try {
            // Set up an repository of expected data
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            // insert all the data
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1, true, NUM_DBS);
            txn.commit();

            // delete all the even records
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, true, NUM_DBS);
            txn.commit();

            // modify all the records
            txn = env.beginTransaction(null, null);
            modifyData(txn, NUM_RECS/2, expectedData, 1, true, NUM_DBS);
            txn.commit();

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            // print stacktrace before trying to clean up files
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Insert data, delete all data into several dbs.
     */
    @Test
    public void testBasicDeleteAll()
        throws Throwable {

        createEnvAndDbs(1024, true, NUM_DBS);
        int numRecs = NUM_RECS;
        try {
            // Set up an repository of expected data
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            // insert all the data
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1, true, NUM_DBS);
            txn.commit();

            // modify half the records
            txn = env.beginTransaction(null, null);
            modifyData(txn, numRecs/2, expectedData, 1, true, NUM_DBS);
            txn.commit();

            // delete all the records
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, true, true, NUM_DBS);
            txn.commit();

            closeEnv();

            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            // print stacktrace before trying to clean up files
            t.printStackTrace();
            throw t;
        }
    }

    protected static class BtreeComparator implements Comparator<byte[]> {
        protected boolean ascendingComparison = true;

        public BtreeComparator() {
        }

        protected BtreeComparator(boolean ascendingComparison) {
            this.ascendingComparison = ascendingComparison;
        }

        public int compare(byte[] o1, byte[] o2) {
            byte[] arg1;
            byte[] arg2;
            if (ascendingComparison) {
                arg1 = (byte[]) o1;
                arg2 = (byte[]) o2;
            } else {
                arg1 = (byte[]) o2;
                arg2 = (byte[]) o1;
            }
            int a1Len = arg1.length;
            int a2Len = arg2.length;

            int limit = Math.min(a1Len, a2Len);

            for (int i = 0; i < limit; i++) {
                byte b1 = arg1[i];
                byte b2 = arg2[i];
                if (b1 == b2) {
                    continue;
                } else {
                    /* Remember, bytes are signed, so convert to shorts so that
                       we effectively do an unsigned byte comparison. */
                    short s1 = (short) (b1 & 0x7F);
                    short s2 = (short) (b2 & 0x7F);
                    if (b1 < 0) {
                        s1 |= 0x80;
                    }
                    if (b2 < 0) {
                        s2 |= 0x80;
                    }
                    return (s1 - s2);
                }
            }

            return (a1Len - a2Len);
        }
    }
}
