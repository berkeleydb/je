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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JoinConfig;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.TestUtils;

@RunWith(Parameterized.class)
public class JoinTest extends MultiKeyTxnTestCase {

    /*
     * DATA sets are pairs of arrays for each record. The first array is
     * the record data and has three values in the 0/1/2 positions for the
     * secondary key values with key IDs 0/1/2.  second array contains a single
     * value which is the primary key.
     *
     * JOIN sets are also pairs of arrays.  The first array in each pair has 3
     * values for setting the input cursors.  Entries 0/1/2 in that array are
     * for secondary keys 0/1/2.  The second array is the set of primary keys
     * that are expected to match in the join operation.
     *
     * A zero value for an index key means "don't index", so zero values are
     * never used for join index keys since we wouldn't be able to successfully
     * position the input cursor.
     *
     * These values are all stored as bytes, not ints, in the actual records,
     * so all values must be within the range of a signed byte.
     */
    private static final int[][][] ALL = {
        /* Data set #1 - single match possible per record. */
        {
            {1, 1, 1}, {11},
            {2, 2, 2}, {12},
            {3, 3, 3}, {13},
        }, {
            {1, 1, 1}, {11},
            {2, 2, 2}, {12},
            {3, 3, 3}, {13},
            {1, 2, 3}, {},
            {1, 1, 2}, {},
            {3, 2, 2}, {},
        },
        /* Data set #2 - no match possible when all indices are not present
         * (when some are zero). */
        {
            {1, 1, 0}, {11},
            {2, 0, 2}, {12},
            {0, 3, 3}, {13},
            {3, 2, 1}, {14},
        }, {
            {1, 1, 1}, {},
            {2, 2, 2}, {},
            {3, 3, 3}, {},
        },
        /* Data set #3 - one match in the presence of non-matching records
         * (with missing/zero index keys). */
        {
            {1, 0, 0}, {11},
            {1, 1, 0}, {12},
            {1, 1, 1}, {13},
            {0, 0, 0}, {14},
        }, {
            {1, 1, 1}, {13},
        },
        /* Data set #4 - one match in the presence of non-matching records
         * (with non-matching but non-zero values). */
        {
            {1, 2, 3}, {11},
            {1, 1, 3}, {12},
            {1, 1, 1}, {13},
            {3, 2, 1}, {14},
        }, {
            {1, 1, 1}, {13},
        },
        /* Data set #5 - two matches in the presence of non-matching records.
         */
        {
            {1, 2, 3}, {11},
            {1, 1, 3}, {12},
            {1, 1, 1}, {13},
            {1, 2, 3}, {14},
        }, {
            {1, 2, 3}, {11, 14},
        },
        /* Data set #6 - three matches in the presence of non-matching records.
         * Also used to verify that cursors are sorted by count: 2, 1, 0 */
        {
            {1, 2, 3}, {11},
            {1, 1, 3}, {12},
            {1, 1, 1}, {13},
            {1, 2, 3}, {14},
            {1, 1, 1}, {15},
            {1, 0, 0}, {16},
            {1, 1, 0}, {17},
            {1, 1, 1}, {18},
            {0, 0, 0}, {19},
            {3, 2, 1}, {20},
        }, {
            {1, 1, 1}, {13, 15, 18},
        },
        /* Data set #7 - three matches by themselves. */
        {
            {1, 2, 3}, {11},
            {1, 2, 3}, {12},
            {1, 2, 3}, {13},
        }, {
            {1, 2, 3}, {11, 12, 13},
        },
    };

    /* Used for testing the cursors are sorted by count. */
    private static final int CURSOR_ORDER_SET = 6;
    private static final int[] CURSOR_ORDER = {2, 1, 0};

    private static EnvironmentConfig envConfig = TestUtils.initEnvConfig();
    static {
        envConfig.setAllowCreate(true);
    }

    private static JoinConfig joinConfigNoSort = new JoinConfig();
    static {
        joinConfigNoSort.setNoSort(true);
    }

    @Parameters
    public static List<Object[]> genParams() {
        return paramsHelper(false);
    }

    protected static List<Object[]> paramsHelper(boolean rep) {
        final String[] txnTypes = getTxnTypes(null, rep);
        final List<Object[]> newParams = new ArrayList<Object[]>();
        for (final String type : txnTypes) {
            newParams.add(new Object[] {type, true});
            newParams.add(new Object[] {type, false});
        }
        return newParams;
    }
    
    public JoinTest(String type, boolean multiKey){
        super.envConfig = envConfig;
        txnType = type;
        useMultiKey = multiKey;
        isTransactional = (txnType != TXN_NULL);
        customName = ((useMultiKey) ? "multiKey" : "") + "-" + txnType;
    }

    @Test
    public void testJoin()
        throws DatabaseException {

        for (CursorConfig config :
             new CursorConfig[] { null, CursorConfig.READ_UNCOMMITTED }) {
            for (boolean withData : new boolean[] { false, true }) {
                for (int i = 0; i < ALL.length; i += 2) {
                    doJoin(ALL[i], ALL[i + 1], (i / 2) + 1, withData, config);
                }
            }
        }
    }

    private void doJoin(int[][] dataSet,
                        int[][] joinSet,
                        int setNum,
                        boolean withData,
                        CursorConfig cursorConfig)
        throws DatabaseException {

        String name = "Set#" + setNum;
        Database priDb = openPrimary("pri");
        SecondaryDatabase secDb0 = openSecondary(priDb, "sec0", true, 0);
        SecondaryDatabase secDb1 = openSecondary(priDb, "sec1", true, 1);
        SecondaryDatabase secDb2 = openSecondary(priDb, "sec2", true, 2);

        OperationStatus status;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn;
        txn = txnBegin();

        for (int i = 0; i < dataSet.length; i += 2) {
            int[] vals = dataSet[i];
            setData(data, vals[0], vals[1], vals[2]);
            setKey(key, dataSet[i + 1][0]);
            status = priDb.put(txn, key, data);
            assertEquals(name, OperationStatus.SUCCESS, status);
        }

        txnCommit(txn);
        txn = txnBeginCursor();

        SecondaryCursor c0 = secDb0.openSecondaryCursor(txn, cursorConfig);
        SecondaryCursor c1 = secDb1.openSecondaryCursor(txn, cursorConfig);
        SecondaryCursor c2 = secDb2.openSecondaryCursor(txn, cursorConfig);
        SecondaryCursor[] cursors = {c0, c1, c2};

        for (int i = 0; i < joinSet.length; i += 2) {
            int[] indexKeys = joinSet[i];
            int[] priKeys = joinSet[i + 1];
            String prefix = name + " row=" + i;
            for (int k = 0; k < 3; k += 1) {
                String msg = prefix + " k=" + k + " ikey=" + indexKeys[k];
                setKey(key, indexKeys[k]);
                status = cursors[k].getSearchKey(key, data,
                                                 LockMode.DEFAULT);
                assertEquals(msg, OperationStatus.SUCCESS, status);
            }
            for (int j = 0; j < 2; j += 1) {
                JoinConfig config = (j == 0) ? null : joinConfigNoSort;
                JoinCursor jc = priDb.join(cursors, config);
                assertSame(priDb, jc.getDatabase());
                for (int k = 0; k < priKeys.length; k += 1) {
                    String msg = prefix + " k=" + k + " pkey=" + priKeys[k];
                    if (withData) {
                        status = jc.getNext(key, data, LockMode.DEFAULT);
                    } else {
                        status = jc.getNext(key, LockMode.DEFAULT);
                    }
                    assertEquals(msg, OperationStatus.SUCCESS, status);
                    assertEquals(msg, priKeys[k], key.getData()[0]);
                    if (withData) {
                        boolean dataFound = false;
                        for (int m = 0; m < dataSet.length; m += 2) {
                            int[] vals = dataSet[m];
                            int priKey = dataSet[m + 1][0];
                            if (priKey == priKeys[k]) {
                                for (int n = 0; n < 3; n += 1) {
                                    assertEquals(msg, vals[n],
                                                 data.getData()[n]);
                                    dataFound = true;
                                }
                            }
                        }
                        assertTrue(msg, dataFound);
                    }
                }
                String msg = prefix + " no more expected";
                if (withData) {
                    status = jc.getNext(key, data, LockMode.DEFAULT);
                } else {
                    status = jc.getNext(key, LockMode.DEFAULT);
                }
                assertEquals(msg, OperationStatus.NOTFOUND, status);

                Cursor[] sorted = DbInternal.getSortedCursors(jc);
                assertEquals(CURSOR_ORDER.length, sorted.length);
                if (config == joinConfigNoSort) {
                    Database db0 = sorted[0].getDatabase();
                    Database db1 = sorted[1].getDatabase();
                    Database db2 = sorted[2].getDatabase();
                    assertSame(db0, secDb0);
                    assertSame(db1, secDb1);
                    assertSame(db2, secDb2);
                } else if (setNum == CURSOR_ORDER_SET) {
                    Database db0 = sorted[CURSOR_ORDER[0]].getDatabase();
                    Database db1 = sorted[CURSOR_ORDER[1]].getDatabase();
                    Database db2 = sorted[CURSOR_ORDER[2]].getDatabase();
                    assertSame(db0, secDb0);
                    assertSame(db1, secDb1);
                    assertSame(db2, secDb2);
                }
                jc.close();
            }
        }

        c0.close();
        c1.close();
        c2.close();
        txnCommit(txn);

        secDb0.close();
        secDb1.close();
        secDb2.close();
        priDb.close();

        /* Remove dbs since we reuse them multiple times in a single case. */
        txn = txnBegin();
        env.removeDatabase(txn, "pri");
        env.removeDatabase(txn, "sec0");
        env.removeDatabase(txn, "sec1");
        env.removeDatabase(txn, "sec2");
        txnCommit(txn);
    }

    /**
     * Checks that a join operation does not block writers from inserting
     * duplicates with the same main key as the search key.  Writers were being
     * blocked before we changed join() to use READ_UNCOMMITTED when getting
     * the duplicate count for each cursor.  [#11833]
     */
    @Test
    public void testWriteDuringJoin()
        throws DatabaseException {

        Database priDb = openPrimary("pri");
        SecondaryDatabase secDb0 = openSecondary(priDb, "sec0", true, 0);
        SecondaryDatabase secDb1 = openSecondary(priDb, "sec1", true, 1);
        SecondaryDatabase secDb2 = openSecondary(priDb, "sec2", true, 2);

        OperationStatus status;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn;
        txn = txnBegin();

        setKey(key, 13);
        setData(data, 1, 1, 1);
        status = priDb.put(txn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);
        setKey(key, 14);
        setData(data, 1, 1, 1);
        status = priDb.put(txn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);

        txnCommit(txn);
        txn = txnBeginCursor();

        SecondaryCursor c0 = secDb0.openSecondaryCursor(txn, null);
        SecondaryCursor c1 = secDb1.openSecondaryCursor(txn, null);
        SecondaryCursor c2 = secDb2.openSecondaryCursor(txn, null);
        SecondaryCursor[] cursors = {c0, c1, c2};

        for (int i = 0; i < 3; i += 1) {
            setKey(key, 1);
            status = cursors[i].getSearchKey(key, data,
                                             LockMode.READ_UNCOMMITTED);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /* join() will get the cursor counts. */
        JoinCursor jc = priDb.join(cursors, null);

        /*
         * After calling join(), try inserting dups for the same main key.
         * Before the fix to use READ_UNCOMMITTED, this would cause a deadlock.
         */
        Transaction writerTxn = txnBegin();
        setKey(key, 12);
        setData(data, 1, 1, 1);
        status = priDb.put(writerTxn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);

        /* The join should retrieve two records, 13 and 14. */
        status = jc.getNext(key, data, LockMode.DEFAULT);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(13, key.getData()[0]);
        status = jc.getNext(key, data, LockMode.DEFAULT);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(14, key.getData()[0]);
        status = jc.getNext(key, data, LockMode.DEFAULT);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Try writing again after calling getNext(). */
        setKey(key, 11);
        setData(data, 1, 1, 1);
        status = priDb.put(writerTxn, key, data);
        assertEquals(OperationStatus.SUCCESS, status);
        txnCommit(writerTxn);

        jc.close();

        c0.close();
        c1.close();
        c2.close();
        txnCommit(txn);

        secDb0.close();
        secDb1.close();
        secDb2.close();
        priDb.close();
    }

    private Database openPrimary(String name)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);

        Transaction txn = txnBegin();
        try {
            return env.openDatabase(txn, name, dbConfig);
        } finally {
            txnCommit(txn);
        }
    }

    private SecondaryDatabase openSecondary(Database priDb, String dbName,
                                            boolean dups, int keyId)
        throws DatabaseException {

        SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        if (useMultiKey) {
            dbConfig.setMultiKeyCreator
                (new SimpleMultiKeyCreator(new MyKeyCreator(keyId)));
        } else {
            dbConfig.setKeyCreator(new MyKeyCreator(keyId));
        }

        Transaction txn = txnBegin();
        try {
            return env.openSecondaryDatabase(txn, dbName, priDb, dbConfig);
        } finally {
            txnCommit(txn);
        }
    }

    private static void setKey(DatabaseEntry key, int priKey) {

        byte[] a = new byte[1];
        a[0] = (byte) priKey;
        key.setData(a);
    }

    private static void setData(DatabaseEntry data,
                                int key1, int key2, int key3) {

        byte[] a = new byte[4];
        a[0] = (byte) key1;
        a[1] = (byte) key2;
        a[2] = (byte) key3;
        data.setData(a);
    }

    private static class MyKeyCreator implements SecondaryKeyCreator {

        private final int keyId;

        MyKeyCreator(int keyId) {

            this.keyId = keyId;
        }

        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            byte val = data.getData()[keyId];
            if (val != 0) {
                result.setData(new byte[] { val });
                return true;
            } else {
                return false;
            }
        }
    }
}
