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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.UniqueConstraintException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.TestUtils;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests basic SecondaryDatabase functionality.  Tests a SecondaryAssociation,
 * but only in a limited mode where there is only one primary DB.
 */
@RunWith(Parameterized.class)
public class SecondaryTest extends MultiKeyTxnTestCase {

    private static final int NUM_RECS = 5;
    private static final int KEY_OFFSET = 100;

    private JUnitThread junitThread;
    private final boolean resetOnFailure;
    private final boolean useCustomAssociation;
    private CustomAssociation customAssociation;

    protected static EnvironmentConfig envConfig = TestUtils.initEnvConfig();
    static {
        envConfig.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "6");
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        /*
         * For testGet, there will be intentional deadlocks, so we will
         * set the lockTimeout to be 1 to speed up this test.
         *
         * But for most other test cases. First, BtreeVerifier will verify
         * the index corruption, so it will first READ_LOCK the secondary
         * record and then try to lock the primary record with nonblocking.
         * Second, in many places of the test cases, it will call put or
         * delete the primary record, e.g. priDb.delete(txn, entry(val)).
         * This will first WRITE_LOCK the primary record and then WRITE_LOCK
         * the secondary record. So when the test case wants to WRITE_LOCK
         * the secondary record, the secondary record may have already been
         * locked by BtreeVerifier. So the test case will first wait. After
         * the BtreeVerifier can not lock the primary record and finish
         * the subsequent work, the BtreeVerifier will release the lock on
         * the secondary record, the test case then can get this lock. If
         * we set the lockTimeout to be 1, then the test case will wake up
         * quickly, think that it is Timeout and then throw LockTimeoutEx.
         * So for these test cases, we just use the default lockTimeout
         * value to simulate the real product environment.
         */
        //envConfig.setLockTimeout(1); // to speed up 
        envConfig.setAllowCreate(true);

        /* Disable daemons so that stats are reliable. */
        envConfig.setConfigParam(EnvironmentConfig.STATS_COLLECT,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                                 "false");
    }

    @Parameters
    public static List<Object[]> genParams() {
       return paramsHelper(false);
    }

    protected static List<Object[]> paramsHelper(boolean rep) {
        final String[] txnTypes = getTxnTypes(null, rep);
        final List<Object[]> newParams = new ArrayList<Object[]>();
//        if (true) {
//            newParams.add(new Object[] {txnTypes[0], false, true, false});
//            return newParams;
//        }
        for (final String type : txnTypes) {
            for (final boolean b1 : new boolean[] {false, true, false}) {
                newParams.add(new Object[] {type, b1, false, true});
                for (final boolean b2 : new boolean[] {false, true}) {
                    newParams.add(new Object[] {type, b1, b2, false});
                }
            }
        }
        
        return newParams;
    }
    
    public SecondaryTest(String type,
                         boolean multiKey,
                         boolean customAssociation,
                         boolean resetOnFailure) {
        super.envConfig = envConfig;
        txnType = type;
        useMultiKey = multiKey;
        useCustomAssociation = customAssociation;
        this.resetOnFailure = resetOnFailure;
        isTransactional = (txnType != TXN_NULL);
        customName = ((useCustomAssociation) ? "customAssoc-" : "") +
                     ((useMultiKey) ? "multiKey-" : "") +
                     ((resetOnFailure) ? "resetOnFailure-" : "") +
                     txnType;
    }
    
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (junitThread != null) {
            junitThread.shutdown();
            junitThread = null;
        }
    }

    @Test
    public void testPutAndDelete() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);

        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        OperationStatus status;
        Transaction txn = txnBegin();

        /* Database.put() */
        status = priDb.put(txn, entry(1), entry(2));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.get(txn, entry(102), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(2), data);

        /* Database.putNoOverwrite() */
        status = priDb.putNoOverwrite(txn, entry(1), entry(1));
        assertSame(OperationStatus.KEYEXIST, status);
        status = secDb.get(txn, entry(102), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(2), data);

        /* Database.put() overwrite */
        status = priDb.put(txn, entry(1), entry(3));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.get(txn, entry(102), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);
        status = secDb.get(txn, entry(103), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(3), data);

        /* Database.delete() */
        status = priDb.delete(txn, entry(1));
        assertSame(OperationStatus.SUCCESS, status);
        status = priDb.delete(txn, entry(1));
        assertSame(OperationStatus.NOTFOUND, status);
        status = secDb.get(txn, entry(103), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);

        /* SecondaryDatabase.delete() */
        status = priDb.put(txn, entry(1), entry(1));
        assertSame(OperationStatus.SUCCESS, status);
        status = priDb.put(txn, entry(2), entry(1));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.get(txn, entry(101), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(1), data);
        status = secDb.delete(txn, entry(101));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.delete(txn, entry(101));
        assertSame(OperationStatus.NOTFOUND, status);
        status = secDb.get(txn, entry(101), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);
        status = priDb.get(txn, entry(1), data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);
        status = priDb.get(txn, entry(2), data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);

        /*
         * Database.putNoDupData() cannot be called since the primary cannot be
         * configured for duplicates.
         */

        /* Primary and secondary are empty now. */

        /* Get a txn for a cursor. */
        txnCommit(txn);
        txn = txnBeginCursor();

        Cursor priCursor = null;
        SecondaryCursor secCursor = null;
        try {
            priCursor = openCursor(priDb, txn);
            secCursor = openCursor(secDb, txn);

            /* Cursor.putNoOverwrite() */
            status = priCursor.putNoOverwrite(entry(1), entry(2));
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(priCursor, 1);
            status = secCursor.getSearchKey(entry(102), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(secCursor, 0);
            assertDataEquals(entry(1), key);
            assertDataEquals(entry(2), data);

            /* Cursor.putCurrent() */
            status = priCursor.putCurrent(entry(3));
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(priCursor, 2);
            status = secCursor.getSearchKey(entry(102), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);
            assertNSecWrites(secCursor, 0);
            status = secCursor.getSearchKey(entry(103), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(secCursor, 0);
            assertDataEquals(entry(1), key);
            assertDataEquals(entry(3), data);

            /* Cursor.delete() */
            status = priCursor.delete();
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(priCursor, 1);
            status = priCursor.delete();
            assertSame(OperationStatus.KEYEMPTY, status);
            status = secCursor.getSearchKey(entry(103), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);
            status = priCursor.getSearchKey(entry(1), data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);

            /* Cursor.put() */
            status = priCursor.put(entry(1), entry(4));
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(priCursor, 1);
            status = secCursor.getSearchKey(entry(104), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(secCursor, 0);
            assertDataEquals(entry(1), key);
            assertDataEquals(entry(4), data);

            /* SecondaryCursor.delete() */
            status = secCursor.delete();
            assertSame(OperationStatus.SUCCESS, status);
            assertNSecWrites(secCursor, 0); // Known deficiency.
            status = secCursor.delete();
            assertSame(OperationStatus.KEYEMPTY, status);
            status = secCursor.getCurrent(new DatabaseEntry(), key, data,
                                          LockMode.DEFAULT);
            assertSame(OperationStatus.KEYEMPTY, status);
            status = secCursor.getSearchKey(entry(104), key, data,
                                            LockMode.DEFAULT);
            assertNSecWrites(secCursor, 0);
            status = priCursor.getSearchKey(entry(1), data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);

            /*
             * Cursor.putNoDupData() cannot be called since the primary cannot
             * be configured for duplicates.
             */

            /* Primary and secondary are empty now. */
        } finally {
            if (secCursor != null) {
                secCursor.close();
            }
            if (priCursor != null) {
                priCursor.close();
            }
        }

        txnCommit(txn);
        secDb.close();
        priDb.close();
    }

    @Test
    public void testPartialDataPut() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);

        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        OperationStatus status;
        Transaction txn = txnBegin();

        /* Database.put() */
        status = priDb.putNoOverwrite(txn, entry(1), partialEntry(0, 1));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.get(txn, entry(101), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(1), data);
        status = priDb.put(txn, entry(1), partialEntry(1, 2));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.get(txn, entry(102), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(2), data);
        status = priDb.put(txn, entry(1), partialEntry(2, 3));
        assertSame(OperationStatus.SUCCESS, status);
        status = secDb.get(txn, entry(102), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);
        status = secDb.get(txn, entry(103), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(1), key);
        assertDataEquals(entry(3), data);

        /* Get a txn for a cursor. */
        txnCommit(txn);
        txn = txnBeginCursor();

        Cursor priCursor = null;
        SecondaryCursor secCursor = null;
        try {
            priCursor = openCursor(priDb, txn);
            secCursor = openCursor(secDb, txn);

            /* Cursor.put() */
            status = priCursor.put(entry(1), partialEntry(3, 2));
            assertSame(OperationStatus.SUCCESS, status);
            status = secCursor.getSearchKey(entry(102), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertDataEquals(entry(1), key);
            assertDataEquals(entry(2), data);

            /* Cursor.putCurrent() */
            status = priCursor.putCurrent(partialEntry(2, 3));
            assertSame(OperationStatus.SUCCESS, status);
            status = secCursor.getSearchKey(entry(102), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);
            status = secCursor.getSearchKey(entry(103), key, data,
                                            LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertDataEquals(entry(1), key);
            assertDataEquals(entry(3), data);
        } finally {
            if (secCursor != null) {
                secCursor.close();
            }
            if (priCursor != null) {
                priCursor.close();
            }
        }

        txnCommit(txn);
        secDb.close();
        priDb.close();
    }

    @Test
    public void testGet() {
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        envImpl.setLockTimeout(1);

        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);

        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry secKey = new DatabaseEntry();
        OperationStatus status;
        Transaction txn = txnBegin();

        /*
         * For parameters that do not require initialization with a non-null
         * data array, we set them to null to make sure this works. [#12121]
         */

        /* Add one record for each key with one data/duplicate. */
        for (int i = 0; i < NUM_RECS; i += 1) {
            status = priDb.put(txn, entry(i), entry(i));
            assertSame(OperationStatus.SUCCESS, status);
        }

        /* SecondaryDatabase.get() */
        for (int i = 0; i < NUM_RECS; i += 1) {

            data.setData(null);
            status = secDb.get(txn, entry(i + KEY_OFFSET), key,
                               data, LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertDataEquals(entry(i), key);
            assertDataEquals(entry(i), data);
        }
        data.setData(null);
        status = secDb.get(txn, entry(NUM_RECS + KEY_OFFSET), key,
                           data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);

        /* SecondaryDatabase.getSearchBoth() */
        for (int i = 0; i < NUM_RECS; i += 1) {
            data.setData(null);
            status = secDb.getSearchBoth(txn, entry(i + KEY_OFFSET), entry(i),
                                         data, LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertDataEquals(entry(i), data);
        }
        data.setData(null);
        status = secDb.getSearchBoth(txn, entry(NUM_RECS + KEY_OFFSET),
                                     entry(NUM_RECS), data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);

        /* Get a cursor txn. */
        txnCommit(txn);
        txn = txnBeginCursor();

        SecondaryCursor cursor = openCursor(secDb, txn);
        try {
            /* SecondaryCursor.getFirst()/getNext() */
            secKey.setData(null);
            key.setData(null);
            data.setData(null);
            status = cursor.getFirst(secKey, key, data, LockMode.DEFAULT);
            for (int i = 0; i < NUM_RECS; i += 1) {
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getNext(secKey, key, data, LockMode.DEFAULT);
            }
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getCurrent() (last) */
            if (!resetOnFailure) {
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getCurrent(
                    secKey, key, data, LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(NUM_RECS - 1 + KEY_OFFSET), secKey);
                assertDataEquals(entry(NUM_RECS - 1), key);
                assertDataEquals(entry(NUM_RECS - 1), data);
                assertPriLocked(priDb, key);
            }

            /* SecondaryCursor.getLast()/getPrev() */
            secKey.setData(null);
            key.setData(null);
            data.setData(null);
            status = cursor.getLast(secKey, key, data, LockMode.DEFAULT);
            for (int i = NUM_RECS - 1; i >= 0; i -= 1) {
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getPrev(secKey, key, data, LockMode.DEFAULT);
            }
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getCurrent() (first) */
            if (!resetOnFailure) {
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getCurrent(secKey, key, data, LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(0 + KEY_OFFSET), secKey);
                assertDataEquals(entry(0), key);
                assertDataEquals(entry(0), data);
                assertPriLocked(priDb, key);
            }

            /* SecondaryCursor.getSearchKey() */
            key.setData(null);
            data.setData(null);
            status = cursor.getSearchKey(entry(KEY_OFFSET - 1), key,
                                         data, LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);
            for (int i = 0; i < NUM_RECS; i += 1) {
                key.setData(null);
                data.setData(null);
                status = cursor.getSearchKey(entry(i + KEY_OFFSET), key,
                                             data, LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key);
            }
            key.setData(null);
            data.setData(null);
            status = cursor.getSearchKey(entry(NUM_RECS + KEY_OFFSET), key,
                                         data, LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getSearchBoth() */
            data.setData(null);
            status = cursor.getSearchKey(entry(KEY_OFFSET - 1), entry(0),
                                         data, LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);
            for (int i = 0; i < NUM_RECS; i += 1) {
                data.setData(null);
                status = cursor.getSearchBoth(entry(i + KEY_OFFSET), entry(i),
                                              data, LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, entry(i));
            }
            data.setData(null);
            status = cursor.getSearchBoth(entry(NUM_RECS + KEY_OFFSET),
                                          entry(NUM_RECS), data,
                                          LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getSearchKeyRange() */
            key.setData(null);
            data.setData(null);
            status = cursor.getSearchKeyRange(entry(KEY_OFFSET - 1), key,
                                              data, LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertDataEquals(entry(0), key);
            assertDataEquals(entry(0), data);
            assertPriLocked(priDb, key);
            for (int i = 0; i < NUM_RECS; i += 1) {
                key.setData(null);
                data.setData(null);
                status = cursor.getSearchKeyRange(entry(i + KEY_OFFSET), key,
                                                  data, LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key);
            }
            key.setData(null);
            data.setData(null);
            status = cursor.getSearchKeyRange(entry(NUM_RECS + KEY_OFFSET),
                                              key, data, LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getSearchBothRange() */
            data.setData(null);
            status = cursor.getSearchBothRange(entry(1 + KEY_OFFSET), entry(1),
                                               data, LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
            assertDataEquals(entry(1), data);
            assertPriLocked(priDb, entry(1));
            for (int i = 0; i < NUM_RECS; i += 1) {
                data.setData(null);
                status = cursor.getSearchBothRange(entry(i + KEY_OFFSET),
                                                   entry(i), data,
                                                   LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, entry(i));
            }
            data.setData(null);
            status = cursor.getSearchBothRange(entry(NUM_RECS + KEY_OFFSET),
                                               entry(NUM_RECS), data,
                                               LockMode.DEFAULT);
            assertSame(OperationStatus.NOTFOUND, status);

            /* Add one duplicate for each key. */
            Cursor priCursor = openCursor(priDb, txn);
            try {
                for (int i = 0; i < NUM_RECS; i += 1) {
                    status = priCursor.put(entry(i + KEY_OFFSET), entry(i));
                    assertSame(OperationStatus.SUCCESS, status);
                }
            } finally {
                priCursor.close();
            }

            /* SecondaryCursor.getNextDup() */
            secKey.setData(null);
            key.setData(null);
            data.setData(null);
            status = cursor.getFirst(secKey, key, data, LockMode.DEFAULT);
            for (int i = 0; i < NUM_RECS; i += 1) {
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key, data);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getNextDup(secKey, key, data,
                                           LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i + KEY_OFFSET), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key, data);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getNextDup(secKey, key, data,
                                           LockMode.DEFAULT);
                assertSame(OperationStatus.NOTFOUND, status);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getNext(secKey, key, data, LockMode.DEFAULT);
            }
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getNextNoDup() */
            secKey.setData(null);
            key.setData(null);
            data.setData(null);
            status = cursor.getFirst(secKey, key, data, LockMode.DEFAULT);
            for (int i = 0; i < NUM_RECS; i += 1) {
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key, data);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getNextNoDup(secKey, key, data,
                                             LockMode.DEFAULT);
            }
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getPrevDup() */
            secKey.setData(null);
            key.setData(null);
            data.setData(null);
            status = cursor.getLast(secKey, key, data, LockMode.DEFAULT);
            for (int i = NUM_RECS - 1; i >= 0; i -= 1) {
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i + KEY_OFFSET), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key, data);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getPrevDup(secKey, key, data,
                                           LockMode.DEFAULT);
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key, data);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getPrevDup(secKey, key, data,
                                           LockMode.DEFAULT);
                assertSame(OperationStatus.NOTFOUND, status);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getPrev(secKey, key, data, LockMode.DEFAULT);
            }
            assertSame(OperationStatus.NOTFOUND, status);

            /* SecondaryCursor.getPrevNoDup() */
            secKey.setData(null);
            key.setData(null);
            data.setData(null);
            status = cursor.getLast(secKey, key, data, LockMode.DEFAULT);
            for (int i = NUM_RECS - 1; i >= 0; i -= 1) {
                assertSame(OperationStatus.SUCCESS, status);
                assertDataEquals(entry(i + KEY_OFFSET), secKey);
                assertDataEquals(entry(i + KEY_OFFSET), key);
                assertDataEquals(entry(i), data);
                assertPriLocked(priDb, key, data);
                secKey.setData(null);
                key.setData(null);
                data.setData(null);
                status = cursor.getPrevNoDup(secKey, key, data,
                                             LockMode.DEFAULT);
            }
            assertSame(OperationStatus.NOTFOUND, status);
        } finally {
            cursor.close();
        }

        txnCommit(txn);
        secDb.close();
        priDb.close();
    }

    @Test
    public void testOpenAndClose() {
        Database priDb = openPrimary(false, "testDB", false);

        /* Open two secondaries as regular databases and as secondaries. */
        Database secDbDetached = openDatabase(true, "testSecDB", false);
        SecondaryDatabase secDb = openSecondary(priDb, true, "testSecDB",
                                                false, false);
        Database secDb2Detached = openDatabase(true, "testSecDB2", false);
        SecondaryDatabase secDb2 = openSecondary(priDb, true, "testSecDB2",
                                                 false, false);
        assertEquals(getSecondaries(priDb), new HashSet(
                     Arrays.asList(new SecondaryDatabase[] {secDb, secDb2})));

        Transaction txn = txnBegin();

        /* Check that primary writes to both secondaries. */
        checkSecondaryUpdate(txn, priDb, 1, secDbDetached, true,
                                            secDb2Detached, true);

        /* New txn before closing database. */
        txnCommit(txn);
        txn = txnBegin();

        /* Close 2nd secondary. */
        closeSecondary(secDb2);
        assertEquals(getSecondaries(priDb), new HashSet(
                     Arrays.asList(new SecondaryDatabase[] {secDb })));

        /* Check that primary writes to 1st secondary only. */
        checkSecondaryUpdate(txn, priDb, 2, secDbDetached, true,
                                             secDb2Detached, false);

        /* New txn before closing database. */
        txnCommit(txn);
        txn = txnBegin();

        /* Close 1st secondary. */
        closeSecondary(secDb);
        assertEquals(0, getSecondaries(priDb).size());

        /* Check that primary writes to no secondaries. */
        checkSecondaryUpdate(txn, priDb, 3, secDbDetached, false,
                                            secDb2Detached, false);

        /* Open the two secondaries again. */
        secDb = openSecondary(priDb, true, "testSecDB", false, false);
        secDb2 = openSecondary(priDb, true, "testSecDB2", false, false);
        assertEquals(getSecondaries(priDb), new HashSet(
                     Arrays.asList(new SecondaryDatabase[] {secDb, secDb2})));

        /* Check that primary writes to both secondaries. */
        checkSecondaryUpdate(txn, priDb, 4, secDbDetached, true,
                                            secDb2Detached, true);

        txnCommit(txn);

        /* Close the primary first to generate exception. */
        try {
            priDb.close();
            if (!useCustomAssociation) {
                fail();
            }
        } catch (IllegalStateException e) {
            if (useCustomAssociation) {
                throw e;
            }
            assertTrue(
                e.getMessage().contains("2 associated SecondaryDatabases"));
        }
        /* Orphaned secondaries can be closed without errors. */
        secDb2.close();
        secDb.close();

        secDb2Detached.close();
        secDbDetached.close();
    }

    /**
     * Check that primary put() writes to each secondary that is open.
     */
    private void checkSecondaryUpdate(Transaction txn,
                                      Database priDb,
                                      int val,
                                      Database secDb,
                                      boolean expectSecDbVal,
                                      Database secDb2,
                                      boolean expectSecDb2Val) {
        OperationStatus status;
        DatabaseEntry data = new DatabaseEntry();
        int secVal = KEY_OFFSET + val;

        status = priDb.put(txn, entry(val), entry(val));
        assertSame(OperationStatus.SUCCESS, status);

        status = secDb.get(txn, entry(secVal), data, LockMode.DEFAULT);
        assertSame(expectSecDbVal ? OperationStatus.SUCCESS
                                  : OperationStatus.NOTFOUND, status);

        status = secDb2.get(txn, entry(secVal), data, LockMode.DEFAULT);
        assertSame(expectSecDb2Val ? OperationStatus.SUCCESS
                                   : OperationStatus.NOTFOUND, status);

        status = priDb.delete(txn, entry(val));
        assertSame(OperationStatus.SUCCESS, status);
    }

    @Test
    public void testReadOnly() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);
        OperationStatus status;
        Transaction txn = txnBegin();

        for (int i = 0; i < NUM_RECS; i += 1) {
            status = priDb.put(txn, entry(i), entry(i));
            assertSame(OperationStatus.SUCCESS, status);
        }

        /*
         * Secondaries can be opened without a key creator if the primary is
         * read only.  openSecondary will specify a null key creator if the
         * readOnly param is false.
         */
        Database readOnlyPriDb = openPrimary(false, "testDB", true);
        SecondaryDatabase readOnlySecDb = openSecondary(readOnlyPriDb,
                                                        true, "testSecDB",
                                                        false, true);
        assertNull(readOnlySecDb.getSecondaryConfig().getKeyCreator());
        verifyRecords(txn, readOnlySecDb, NUM_RECS, true);

        txnCommit(txn);
        readOnlySecDb.close();
        readOnlyPriDb.close();
        secDb.close();
        priDb.close();
    }

    /**
     * Tests population of newly created secondary database, which occurs
     * automatically in openSecondary when AllowPopulate is configured.
     */
    @Test
    public void testAutomaticPopulate() {

        /* Open primary without any secondaries and write data. */
        Database priDb = openPrimary(false, "testDB", false);
        Transaction txn = txnBegin();
        for (int i = 0; i < NUM_RECS; i += 1) {
            assertSame(OperationStatus.SUCCESS,
                       priDb.put(txn, entry(i), entry(i)));
        }
        txnCommit(txn);

        /*
         * Open secondary with allowPopulate option and it will automatically
         * be populated.
         */

        SecondaryDatabase secDb;
        try {
            secDb = openSecondary(
                priDb, true, "testSecDB", true /*allowPopulate*/, false);
            if (useCustomAssociation) {
                fail();
            }
        } catch (IllegalArgumentException e) {
            if (useCustomAssociation) {

                /*
                 * Automatic population not allowed when a user-supplied
                 * SecondaryAssociation is configured.
                 */
                final String msg = e.toString();
                assertTrue(msg, msg.contains("AllowPopulate must be false"));
                priDb.close();
                return;
            }
            throw e;
        }
        txn = txnBegin();
        verifyRecords(txn, secDb, NUM_RECS, true);
        txnCommit(txn);

        /*
         * Clear secondary and perform populate again, to test the case where
         * an existing database is opened, and therefore a write txn will only
         * be created in order to populate it
         */
        Database secDbDetached = openDatabase(true, "testSecDB", false);
        secDb.close();
        txn = txnBegin();
        for (int i = 0; i < NUM_RECS; i += 1) {
            assertSame(OperationStatus.SUCCESS,
                       secDbDetached.delete(txn, entry(i + KEY_OFFSET)));
        }
        verifyRecords(txn, secDbDetached, 0, true);
        txnCommit(txn);
        secDb = openSecondary(priDb, true, "testSecDB", true, false);
        txn = txnBegin();
        verifyRecords(txn, secDb, NUM_RECS, true);
        verifyRecords(txn, secDbDetached, NUM_RECS, true);

        txnCommit(txn);
        secDbDetached.close();
        secDb.close();
        priDb.close();
    }

    /**
     * Tests population of newly created secondary database via explicit calls
     * to incremental indexing methods.  This is a very basic test and not
     * intended to take the place of a stress test that performs concurrent
     * writes.
     */
    @Test
    public void testIncrementalPopulate() {
        /* Open primary without any secondaries and write data. */
        final Database priDb = openPrimary(false, "testDB", false);
        final Transaction txn = txnBegin();
        for (int i = 0; i < NUM_RECS; i += 1) {
            assertSame(OperationStatus.SUCCESS,
                       priDb.put(txn, entry(i), entry(i)));
        }
        txnCommit(txn);

        /*
         * Open secondary without allowPopulate option.  It will initially be
         * empty.
         */
        final SecondaryDatabase secDb = openSecondary(
            priDb, true, "testSecDB", false /*allowPopulate*/, false);
        assertEquals(0, secDb.count());

        /* Enable incremental population.  Secondary reads are not allowed. */
        secDb.startIncrementalPopulation();
        try {
            secDb.get(null, new DatabaseEntry(new byte[0]),
                      new DatabaseEntry(), null);
            fail();
        } catch (IllegalStateException expected) {
        }

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final Cursor cursor = priDb.openCursor(
            null, CursorConfig.READ_COMMITTED);
        OperationResult result;
        while ((result = cursor.get(
                keyEntry, dataEntry, Get.NEXT, null)) != null) {
            priDb.populateSecondaries(
                null, keyEntry, dataEntry, result.getExpirationTime(), null);
        }
        cursor.close();

        /* After disabling incremental population we can read via secondary. */
        secDb.endIncrementalPopulation();
        verifyRecords(null, secDb, NUM_RECS, true);

        secDb.close();
        priDb.close();
    }

    @Test
    public void testTruncate() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);
        Transaction txn = txnBegin();

        for (int i = 0; i < NUM_RECS; i += 1) {
            priDb.put(txn, entry(i), entry(i));
        }
        verifyRecords(txn, priDb, NUM_RECS, false);
        verifyRecords(txn, secDb, NUM_RECS, true);
        txnCommit(txn);
        secDb.close();
        priDb.close();

        txn = txnBegin();
        assertEquals(NUM_RECS, env.truncateDatabase(txn, "testDB", true));
        assertEquals(NUM_RECS, env.truncateDatabase(txn, "testSecDB", true));
        txnCommit(txn);

        secDb = initDb();
        priDb = getPrimaryDatabase(secDb);

        txn = txnBegin();
        verifyRecords(txn, priDb, 0, false);
        verifyRecords(txn, secDb, 0, true);
        txnCommit(txn);

        secDb.close();
        priDb.close();
    }

    private void verifyRecords(Transaction txn,
                               Database db,
                               int numRecs,
                               boolean isSecondary) {
        /* We're only reading, so txn may be null. */
        Cursor cursor = openCursor(db, txn);
        try {
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry key = new DatabaseEntry();
            OperationStatus status;
            int count = 0;
            status = cursor.getFirst(key, data, LockMode.DEFAULT);
            while (status == OperationStatus.SUCCESS) {
                assertDataEquals(entry(count), data);
                if (isSecondary) {
                    assertDataEquals(entry(count + KEY_OFFSET), key);
                } else {
                    assertDataEquals(entry(count), key);
                }
                count += 1;
                status = cursor.getNext(key, data, LockMode.DEFAULT);
            }
            assertEquals(numRecs, count);
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testUniqueSecondaryKey() {
        Database priDb = openPrimary(false, "testDB", false);
        SecondaryDatabase secDb =
            openSecondary(priDb, false, "testSecDB", false, false);
        DatabaseEntry key;
        DatabaseEntry data;
        DatabaseEntry pkey = new DatabaseEntry();
        Transaction txn;

        /* Put {0, 0} */
        txn = txnBegin();
        key = entry(0);
        data = entry(0);
        priDb.put(txn, key, data);
        assertEquals(OperationStatus.SUCCESS,
                     secDb.get(txn, entry(0 + KEY_OFFSET),
                               pkey, data, null));
        assertEquals(0, TestUtils.getTestVal(pkey.getData()));
        assertEquals(0, TestUtils.getTestVal(data.getData()));
        txnCommit(txn);

        /* Put {1, 1} */
        txn = txnBegin();
        key = entry(1);
        data = entry(1);
        priDb.put(txn, key, data);
        assertEquals(OperationStatus.SUCCESS,
                     secDb.get(txn, entry(1 + KEY_OFFSET),
                               pkey, data, null));
        txnCommit(txn);
        assertEquals(1, TestUtils.getTestVal(pkey.getData()));
        assertEquals(1, TestUtils.getTestVal(data.getData()));

        /* Put {2, 0} */
        txn = txnBegin();
        key = entry(2);
        data = entry(0);
        try {
            priDb.put(txn, key, data);
            /* Expect exception because secondary key must be unique. */
            fail();
        } catch (UniqueConstraintException e) {
            txnAbort(txn);
            /* Ensure that primary record was not inserted. */
            assertEquals(OperationStatus.NOTFOUND,
                         secDb.get(null, key, data, null));
            /* Ensure that secondary record has not changed. */
            assertEquals(OperationStatus.SUCCESS,
                         secDb.get(null, entry(0 + KEY_OFFSET),
                                   pkey, data, null));
            assertEquals(0, TestUtils.getTestVal(pkey.getData()));
            assertEquals(0, TestUtils.getTestVal(data.getData()));
        }

        /* Overwrite {1, 1} */
        txn = txnBegin();
        key = entry(1);
        data = entry(1);
        priDb.put(txn, key, data);
        assertEquals(OperationStatus.SUCCESS,
                     secDb.get(txn, entry(1 + KEY_OFFSET),
                               pkey, data, null));
        assertEquals(1, TestUtils.getTestVal(pkey.getData()));
        assertEquals(1, TestUtils.getTestVal(data.getData()));
        txnCommit(txn);

        /* Modify secondary key to {1, 3} */
        txn = txnBegin();
        key = entry(1);
        data = entry(3);
        priDb.put(txn, key, data);
        assertEquals(OperationStatus.SUCCESS,
                     secDb.get(txn, entry(3 + KEY_OFFSET),
                               pkey, data, null));
        assertEquals(1, TestUtils.getTestVal(pkey.getData()));
        assertEquals(3, TestUtils.getTestVal(data.getData()));
        txnCommit(txn);

        secDb.close();
        priDb.close();
    }

    @Test
    public void testOperationsNotAllowed() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);
        Transaction txn = txnBegin();

        /* Open secondary without a key creator. */
        try {
            env.openSecondaryDatabase(txn, "xxx", priDb, null);
            fail();
        } catch (IllegalArgumentException expected) { }
        try {
            env.openSecondaryDatabase(txn, "xxx", priDb,
                                      new SecondaryConfig());
            fail();
        } catch (IllegalArgumentException expected) { }

        /* Open secondary with both single and multi key creators. */
        SecondaryConfig config = new SecondaryConfig();
        config.setKeyCreator(new MyKeyCreator());
        config.setMultiKeyCreator
            (new SimpleMultiKeyCreator(new MyKeyCreator()));
        try {
            env.openSecondaryDatabase(txn, "xxx", priDb, config);
            fail();
        } catch (IllegalArgumentException expected) { }

        /* Open secondary with non-null primaryDb and SecondaryAssociation. */
        config = new SecondaryConfig();
        config.setKeyCreator(new MyKeyCreator());
        config.setSecondaryAssociation(new CustomAssociation());
        try {
            env.openSecondaryDatabase(txn, "xxx", priDb, config);
            fail();
        } catch (IllegalArgumentException expected) {
            final String msg = expected.toString();
            assertTrue(msg, msg.contains(
                "Exactly one must be non-null: " +
                "PrimaryDatabase or SecondaryAssociation"));
        }

        /* Database operations. */

        DatabaseEntry key = entry(1);
        DatabaseEntry data = entry(2);

        try {
            secDb.getSearchBoth(txn, key, data, LockMode.DEFAULT);
            fail();
        } catch (UnsupportedOperationException expected) { }

        try {
            secDb.put(txn, key, data);
            fail();
        } catch (UnsupportedOperationException expected) { }

        try {
            secDb.putNoOverwrite(txn, key, data);
            fail();
        } catch (UnsupportedOperationException expected) { }

        try {
            secDb.putNoDupData(txn, key, data);
            fail();
        } catch (UnsupportedOperationException expected) { }

        try {
            secDb.join(new Cursor[0], null);
            fail();
        } catch (UnsupportedOperationException expected) { }

        /* Cursor operations. */

        txnCommit(txn);
        txn = txnBeginCursor();

        SecondaryCursor cursor = null;
        try {
            cursor = openCursor(secDb, txn);

            try {
                cursor.getSearchBoth(key, data, LockMode.DEFAULT);
                fail();
            } catch (UnsupportedOperationException expected) { }

            try {
                cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
                fail();
            } catch (UnsupportedOperationException expected) { }

            try {
                cursor.putCurrent(data);
                fail();
            } catch (UnsupportedOperationException expected) { }

            try {
                cursor.put(key, data);
                fail();
            } catch (UnsupportedOperationException expected) { }

            try {
                cursor.putNoOverwrite(key, data);
                fail();
            } catch (UnsupportedOperationException expected) { }

            try {
                cursor.putNoDupData(key, data);
                fail();
            } catch (UnsupportedOperationException expected) { }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        txnCommit(txn);
        secDb.close();
        priDb.close();

        /* Primary with duplicates. */
        try {
            priDb = openPrimary(true, "testDBWithDups", false);
            assertTrue(!useCustomAssociation);
            try {
                openSecondary(priDb, true, "testSecDB", false, false);
                fail();
            } catch (IllegalArgumentException expected) {
            }
        } catch (IllegalArgumentException e) {
            if (!useCustomAssociation) {
                throw e;
            }
        }
        priDb.close();

        /* Single secondary with two primaries.*/
        if (!useCustomAssociation) {
            Database pri1 = openPrimary(false, "pri1", false);
            Database pri2 = openPrimary(false, "pri2", false);
            Database sec1 = openSecondary(pri1, false, "sec", false, false);
            try {
                openSecondary(pri2, false, "sec", false, false);
                fail();
            } catch (IllegalArgumentException expected) {}
            sec1.close();
            pri1.close();
            pri2.close();
        }
    }

    /**
     * Test that null can be passed for the LockMode to all get methods.
     */
    @Test
    public void testNullLockMode() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);
        Transaction txn = txnBegin();

        DatabaseEntry key = entry(0);
        DatabaseEntry data = entry(0);
        DatabaseEntry secKey = entry(KEY_OFFSET);
        DatabaseEntry found = new DatabaseEntry();
        DatabaseEntry found2 = new DatabaseEntry();
        DatabaseEntry found3 = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
                     priDb.put(txn, key, data));
        assertEquals(OperationStatus.SUCCESS,
                     priDb.put(txn, entry(1), data));
        assertEquals(OperationStatus.SUCCESS,
                     priDb.put(txn, entry(2), entry(2)));

        /* Database operations. */

        assertEquals(OperationStatus.SUCCESS,
                     priDb.get(txn, key, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     priDb.getSearchBoth(txn, key, data, null));
        assertEquals(OperationStatus.SUCCESS,
                     secDb.get(txn, secKey, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     secDb.get(txn, secKey, found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secDb.getSearchBoth(txn, secKey, key, found, null));

        /* Cursor operations. */

        txnCommit(txn);
        txn = txnBeginCursor();
        Cursor cursor = openCursor(priDb, txn);
        SecondaryCursor secCursor = openCursor(secDb, txn);

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchKey(key, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchBoth(key, data, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchKeyRange(key, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchBothRange(key, data, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getNext(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getPrev(found, found2, null));
        assertEquals(OperationStatus.NOTFOUND,
                     cursor.getNextDup(found, found2, null));
        assertEquals(OperationStatus.NOTFOUND,
                     cursor.getPrevDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getNextNoDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getPrevNoDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getLast(found, found2, null));

        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchKey(secKey, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchKeyRange(secKey, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getFirst(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getNext(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getPrev(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getNextDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getPrevDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getNextNoDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getPrevNoDup(found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getLast(found, found2, null));

        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchKey(secKey, found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchBoth(secKey, data, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchKeyRange(secKey, found, found2, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchBothRange(secKey, data, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getFirst(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getNext(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getPrev(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getNextDup(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getPrevDup(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getNextNoDup(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getPrevNoDup(found, found2, found3, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getLast(found, found2, found3, null));

        secCursor.close();
        cursor.close();
        txnCommit(txn);
        secDb.close();
        priDb.close();
        closeEnv();
        env = null;
    }

    /**
     * Test that an exception is thrown when a cursor is used in the wrong
     * state.  No put or get is allowed in the closed state, and certain gets
     * and puts are not allowed in the uninitialized state.
     */
    @Test
    public void testCursorState() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);
        Transaction txn = txnBegin();

        DatabaseEntry key = entry(0);
        DatabaseEntry data = entry(0);
        DatabaseEntry secKey = entry(KEY_OFFSET);
        DatabaseEntry found = new DatabaseEntry();
        DatabaseEntry found2 = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
                     priDb.put(txn, key, data));

        txnCommit(txn);
        txn = txnBeginCursor();
        Cursor cursor = openCursor(priDb, txn);
        SecondaryCursor secCursor = openCursor(secDb, txn);

        /* Check the uninitialized state for certain operations. */

        try {
            cursor.count();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.delete();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.putCurrent(data);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getCurrent(key, data, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getNextDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getPrevDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}

        try {
            secCursor.count();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.delete();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getCurrent(key, data, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getNextDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getPrevDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}

        /* Cursor.dup works whether initialized or not. */
        {
            Cursor c2 = secCursor.dup(false);
            c2.close();
            c2 = secCursor.dup(true);
            c2.close();
            c2 = secCursor.dup(false);
            c2.close();
            c2 = secCursor.dup(true);
            c2.close();
        }

        /* Initialize, then close, then check all operations. */

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchKey(key, found, null));
        assertEquals(OperationStatus.SUCCESS,
                     secCursor.getSearchKey(secKey, found, null));

        /* Cursor.dup works whether initialized or not. */
        {
            Cursor c2 = cursor.dup(false);
            c2.close();
            c2 = cursor.dup(true);
            c2.close();
            c2 = secCursor.dup(false);
            c2.close();
            c2 = secCursor.dup(true);
            c2.close();
        }

        /* Close, then check all operations. */

        secCursor.close();
        cursor.close();

        try {
            cursor.close();
        } catch (RuntimeException expected) {
            fail("Caught IllegalStateException while re-closing a Cursor.");
        }

        try {
            cursor.count();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.delete();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.put(key, data);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.putNoOverwrite(key, data);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.putNoDupData(key, data);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.putCurrent(data);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getCurrent(key, data, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getSearchKey(key, found, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getSearchBoth(key, data, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getSearchKeyRange(key, found, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getSearchBothRange(key, data, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getFirst(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getNext(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getPrev(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getNextDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getPrevDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getNextNoDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getPrevNoDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            cursor.getLast(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}

        try {
            secCursor.close();
        } catch (RuntimeException e) {
            fail("Caught exception while re-closing a SecondaryCursor.");
        }

        try {
            secCursor.count();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.delete();
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getCurrent(key, data, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getSearchKey(secKey, found, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getSearchKeyRange(secKey, found, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getFirst(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getNext(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getPrev(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getNextDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getPrevDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getNextNoDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getPrevNoDup(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}
        try {
            secCursor.getLast(found, found2, null);
            fail();
        } catch (IllegalStateException expected) {}

        txnCommit(txn);
        secDb.close();
        priDb.close();
        closeEnv();
        env = null;
    }

    /**
     * [#14966]
     */
    @Test
    public void testDirtyReadPartialGet() {
        SecondaryDatabase secDb = initDb();
        Database priDb = getPrimaryDatabase(secDb);

        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry secKey = new DatabaseEntry();
        OperationStatus status;

        /* Put a record */
        Transaction txn = txnBegin();
        status = priDb.put(txn, entry(0), entry(0));
        assertSame(OperationStatus.SUCCESS, status);

        /* Regular get */
        status = secDb.get(txn, entry(0 + KEY_OFFSET), key,
                           data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(0), key);
        assertDataEquals(entry(0), data);

        /* Dirty read returning no data */
        data.setPartial(0, 0, true);
        status = secDb.get(txn, entry(0 + KEY_OFFSET), key,
                           data, LockMode.READ_UNCOMMITTED);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(0), key);
        assertEquals(0, data.getData().length);
        assertEquals(0, data.getSize());

        /* Dirty read returning partial data */
        data.setPartial(0, 1, true);
        status = secDb.get(txn, entry(0 + KEY_OFFSET), key,
                           data, LockMode.READ_UNCOMMITTED);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(0), key);
        assertEquals(1, data.getData().length);
        assertEquals(1, data.getSize());
        txnCommit(txn);

        secDb.close();
        priDb.close();
    }

    /**
     * Tests ImmutableSecondaryKey optimization.
     * 
     * expectFetchOnDelete is true because the data (not just the key) is used
     * by the key creator/extractor.
     */
    @Test
    public void testImmutableSecondaryKey() {
        final SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setImmutableSecondaryKey(true);
        checkUpdateWithNoFetchOfOldData(secConfig,
                                        true /*expectFetchOnDelete*/);
    }

    /**
     * Tests ExtractFromPrimaryKeyOnly optimization.
     * 
     * expectFetchOnDelete is false because only the key is used by the key
     * creator/extractor.
     */
    @Test
    public void testExtractFromPrimaryKeyOnly() {
        final SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setExtractFromPrimaryKeyOnly(true);
        if (useMultiKey) {
            secConfig.setMultiKeyCreator(
                new SimpleMultiKeyCreator(new KeyOnlyKeyCreator()));
        } else {
            secConfig.setKeyCreator(new KeyOnlyKeyCreator());
        }
        checkUpdateWithNoFetchOfOldData(secConfig,
                                        false /*expectFetchOnDelete*/);
    }

    /**
     * Tests ImmutableSecondaryKey and ExtractFromPrimaryKeyOnly optimizations
     * together.
     * 
     * expectFetchOnDelete is false because only the key is used by the key
     * creator/extractor.
     */
    @Test
    public void testImmutableSecondaryKeyAndExtractFromPrimaryKeyOnly() {
        final SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setImmutableSecondaryKey(true);
        secConfig.setExtractFromPrimaryKeyOnly(true);
        if (useMultiKey) {
            secConfig.setMultiKeyCreator(
                new SimpleMultiKeyCreator(new KeyOnlyKeyCreator()));
        } else {
            secConfig.setKeyCreator(new KeyOnlyKeyCreator());
        }
        checkUpdateWithNoFetchOfOldData(secConfig,
                                        false /*expectFetchOnDelete*/);
    }

    /**
     * When ImmutableSecondaryKey or ExtractFromPrimaryKeyOnly is configured,
     * the data should not be fetched when updating the record.  It should also
     * not be fetched during a delete when ExtractFromPrimaryKeyOnly is
     * configured (i.e., when expectFetchOnDelete is true).
     */
    private void checkUpdateWithNoFetchOfOldData(
        final SecondaryConfig secConfig,
        final boolean expectFetchOnDelete) {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        envImpl.getDataVerifier().shutdown();

        boolean embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        final Database priDb = openPrimary(false, "testDB", false);

        secConfig.setSortedDuplicates(true);
        final SecondaryDatabase secDb = openSecondary(
            priDb, "testSecDB", secConfig);

        final StatsConfig clearStats = new StatsConfig().setClear(true);
        final DatabaseEntry data = new DatabaseEntry();
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry secKey = new DatabaseEntry();
        OperationStatus status;

        /* Insert a record */
        env.getStats(clearStats);
        Transaction txn = txnBegin();
        status = priDb.put(txn, entry(0), entry(0));
        assertSame(OperationStatus.SUCCESS, status);
        final EnvironmentStats stats1 = env.getStats(null);
        assertEquals(0, stats1.getNLNsFetch());

        /* Get via secondary. */
        env.getStats(clearStats);
        status = secDb.get(
            txn, entry(0 + KEY_OFFSET), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(0), key);
        assertDataEquals(entry(0), data);
        final EnvironmentStats stats2 = env.getStats(null);
        assertEquals((embeddedLNs ? 0 : 1), stats2.getNLNsFetch());

        /* Update to same value -- should not fetch old data. */
        env.getStats(clearStats);
        status = priDb.put(txn, entry(0), entry(0));
        assertSame(OperationStatus.SUCCESS, status);
        final EnvironmentStats stats3 = env.getStats(null);
        assertEquals(0, stats3.getNLNsFetch());

        /* Get via secondary. */
        env.getStats(clearStats);
        status = secDb.get(
            txn, entry(0 + KEY_OFFSET), key, data, LockMode.DEFAULT);
        assertSame(OperationStatus.SUCCESS, status);
        assertDataEquals(entry(0), key);
        assertDataEquals(entry(0), data);
        final EnvironmentStats stats4 = env.getStats(null);
        assertEquals((embeddedLNs ? 0 : 1), stats4.getNLNsFetch());

        /* Delete -- only fetches old data if expectFetchOnDelete is true. */
        env.getStats(clearStats);
        status = priDb.delete(txn, entry(0));
        assertSame(OperationStatus.SUCCESS, status);
        final EnvironmentStats stats5 = env.getStats(null);
        assertEquals(
            (expectFetchOnDelete && !embeddedLNs ? 1 : 0),
            stats5.getNLNsFetch());

        /* Get via secondary. */
        status = secDb.get(txn, entry(0 + KEY_OFFSET), key,
                           data, LockMode.DEFAULT);
        assertSame(OperationStatus.NOTFOUND, status);

        txnCommit(txn);
        secDb.close();
        priDb.close();
    }

    /**
     * Open environment, primary and secondary db
     */
    private SecondaryDatabase initDb() {
        Database priDb = openPrimary(false, "testDB", false);
        SecondaryDatabase secDb = openSecondary(priDb, true, "testSecDB",
                                                false, false);
        return secDb;
    }

    private Database openPrimary(boolean allowDuplicates, String name,
                                 boolean readOnly) {
        return openDbInternal(allowDuplicates, name, readOnly, true);
    }

    private Database openDatabase(boolean allowDuplicates, String name,
                                  boolean readOnly) {
        return openDbInternal(allowDuplicates, name, readOnly, false);
    }

    private Database openDbInternal(boolean allowDuplicates, String name,
                                    boolean readOnly, boolean isPrimary) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(allowDuplicates);
        dbConfig.setReadOnly(readOnly);
        if (isPrimary && useCustomAssociation) {
            customAssociation = new CustomAssociation();
            dbConfig.setSecondaryAssociation(customAssociation);
        }
        Transaction txn = txnBegin();
        Database priDb;
        try {
            priDb = env.openDatabase(txn, name, dbConfig);
        } finally {
            txnCommit(txn);
        }
        assertNotNull(priDb);
        if (isPrimary && useCustomAssociation) {
            customAssociation.initPrimary(priDb);
        }
        return priDb;
    }

    private SecondaryDatabase openSecondary(Database priDb,
                                            boolean allowDuplicates,
                                            String dbName,
                                            boolean allowPopulate,
                                            boolean readOnly) {
        final SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setSortedDuplicates(allowDuplicates);
        dbConfig.setReadOnly(readOnly);
        dbConfig.setAllowPopulate(allowPopulate);
        return openSecondary(priDb, dbName, dbConfig);
    }



    private SecondaryDatabase openSecondary(Database priDb,
                                            String dbName,
                                            SecondaryConfig dbConfig) {
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        if (!dbConfig.getReadOnly() &&
            dbConfig.getMultiKeyCreator() == null &&
            dbConfig.getKeyCreator() == null) {
            if (useMultiKey) {
                dbConfig.setMultiKeyCreator(
                    new SimpleMultiKeyCreator(new MyKeyCreator()));
            } else {
                dbConfig.setKeyCreator(new MyKeyCreator());
            }
        }
        final Database priDbParam;
        if (useCustomAssociation) {
            priDbParam = null;
            assertSame(priDb, customAssociation.getPrimary(null));
            dbConfig.setSecondaryAssociation(customAssociation);
        } else {
            priDbParam = priDb;
            assertNull(customAssociation);
        }
        final Collection<SecondaryDatabase> secListBefore =
            getSecondaries(priDb);
        final Transaction txn = txnBegin();
        final SecondaryDatabase secDb;
        try {
            secDb = env.openSecondaryDatabase(txn, dbName, priDbParam,
                                              dbConfig);
        } finally {
            txnCommit(txn);
        }
        assertNotNull(secDb);
        if (useCustomAssociation) {
            customAssociation.addSecondary(secDb);
        }

        /* Check configuration. */
        if (useCustomAssociation) {
            assertNull(secDb.getPrimaryDatabase());
        } else {
            assertSame(priDb, secDb.getPrimaryDatabase());
        }
        final SecondaryConfig config2 = secDb.getSecondaryConfig();
        assertEquals(dbConfig.getAllowPopulate(), config2.getAllowPopulate());
        assertEquals(dbConfig.getKeyCreator(), config2.getKeyCreator());
        assertEquals(dbConfig.getMultiKeyCreator(),
                     config2.getMultiKeyCreator());
        assertSame(customAssociation, config2.getSecondaryAssociation());

        /* Make sure the new secondary is added to the primary's list. */
        final Collection<SecondaryDatabase> secListAfter =
            getSecondaries(priDb);
        assertTrue(secListAfter.remove(secDb));
        assertEquals(secListBefore, secListAfter);

        return secDb;
    }

    private void closeSecondary(SecondaryDatabase secDb) {
        if (useCustomAssociation) {
            customAssociation.removeSecondary(secDb);
        }
        secDb.close();
    }

    private Database getPrimaryDatabase(SecondaryDatabase secDb) {
        if (useCustomAssociation) {
            return customAssociation.getPrimary(null);
        }
        return secDb.getPrimaryDatabase();
    }

    /**
     * Returns a copy of the secondaries as a set, so it can be compared to an
     * expected set.
     */
    private Set<SecondaryDatabase> getSecondaries(Database priDb) {
        if (useCustomAssociation) {
            return new HashSet<SecondaryDatabase>(
                customAssociation.getSecondaries(null));
        }
        return new HashSet<SecondaryDatabase>(priDb.getSecondaryDatabases());
    }

    private static class CustomAssociation implements SecondaryAssociation {
        
        private Database priDb = null;
        private final Set<SecondaryDatabase> secondaries =
            new HashSet<SecondaryDatabase>();

        void initPrimary(final Database priDb) {
            this.priDb = priDb;
        }

        public boolean isEmpty() {
            return secondaries.isEmpty();
        }

        public Database getPrimary(@SuppressWarnings("unused")
                                   DatabaseEntry primaryKey) {
            assertNotNull(priDb);
            return priDb;
        }

        public Collection<SecondaryDatabase>
            getSecondaries(@SuppressWarnings("unused")
                           DatabaseEntry primaryKey) {
            return secondaries;
        }

        public void addSecondary(final SecondaryDatabase secDb) {
            secondaries.add(secDb);
        }

        public void removeSecondary(final SecondaryDatabase secDb) {
            secondaries.remove(secDb);
        }
    }

    private Cursor openCursor(Database priDb, Transaction txn) {
        final CursorConfig config =
            resetOnFailure ?
            (new CursorConfig().setNonSticky(true)) :
            null;
        return priDb.openCursor(txn, config);
    }

    private SecondaryCursor openCursor(SecondaryDatabase secDb,
                                       Transaction txn) {
        final CursorConfig config =
            resetOnFailure ?
                (new CursorConfig().setNonSticky(true)) :
                null;
        return secDb.openCursor(txn, config);
    }

    private DatabaseEntry entry(int val) {

        return new DatabaseEntry(TestUtils.getTestArray(val));
    }

    /**
     * Creates a partial entry for changing oldVal to newVal.
     */
    private DatabaseEntry partialEntry(final int oldVal, final int newVal) {

        final DatabaseEntry oldEntry =
            new DatabaseEntry(TestUtils.getTestArray(oldVal));
        final DatabaseEntry newEntry =
            new DatabaseEntry(TestUtils.getTestArray(newVal));

        final int size = oldEntry.getSize();
        assertEquals(size, newEntry.getSize());

        int begOff;
        for (begOff = 0; begOff < size; begOff += 1) {
            if (oldEntry.getData()[begOff] != newEntry.getData()[begOff]) {
                break;
            }
        }

        int endOff;
        for (endOff = size - 1; endOff >= begOff; endOff += 1) {
            if (oldEntry.getData()[endOff] != newEntry.getData()[endOff]) {
                break;
            }
        }

        final int partialSize = endOff - begOff + 1;
        final byte[] newData = new byte[partialSize];
        System.arraycopy(newEntry.getData(), begOff, newData, 0, partialSize);
        newEntry.setData(newData);
        newEntry.setPartial(begOff, partialSize, true);
        return newEntry;
    }

    private void assertDataEquals(DatabaseEntry e1, DatabaseEntry e2) {
        assertTrue(e1.equals(e2));
    }

    private void assertPriLocked(Database priDb, DatabaseEntry key) {
        assertPriLocked(priDb, key, null);
    }

    /**
     * Checks that the given key (or both key and data if data is non-null) is
     * locked in the primary database.  The primary record should be locked
     * whenever a secondary cursor is positioned to point to that primary
     * record. [#15573]
     */
    private void assertPriLocked(final Database priDb,
                                 final DatabaseEntry key,
                                 final DatabaseEntry data) {

        /*
         * Whether the record is locked transactionally or not in the current
         * thread, we should not be able to write lock the record
         * non-transactionally in another thread.
         */
        final StringBuilder error = new StringBuilder();
        junitThread = new JUnitThread("primary-locker") {
            @Override
            public void testBody() {
                Cursor cursor = openCursor(priDb, null);
                try {
                    if (data != null) {
                        cursor.getSearchBoth(key, data, LockMode.RMW);
                    } else {
                        DatabaseEntry myData = new DatabaseEntry();
                        cursor.getSearchKey(key, myData, LockMode.RMW);
                    }
                    error.append("Expected LockConflictException");
                } catch (Exception expected) {
                    assertTrue(
                        expected.toString(),
                        expected instanceof LockConflictException);
                } finally {
                    cursor.close();
                }
            }
        };

        junitThread.start();
        Throwable t = null;
        try {
            junitThread.finishTest();
        } catch (Throwable e) {
            t = e;
        } finally {
            junitThread = null;
        }

        if (t != null) {
            t.printStackTrace();
            fail(t.toString());
        }
        if (error.length() > 0) {
            fail(error.toString());
        }
    }

    private void assertNSecWrites(final Cursor cursor,
                                  final int expectNWrites) {
        assertEquals(
            customName,
            expectNWrites,
            DbInternal.getCursorImpl(cursor).getNSecondaryWrites());
    }

    private static class MyKeyCreator implements SecondaryKeyCreator {

        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            result.setData(
                TestUtils.getTestArray(
                    TestUtils.getTestVal(data.getData()) + KEY_OFFSET));
            return true;
        }
    }

    /**
     * Derives secondary key from primary key alone, to test the
     * ExtractFromPrimaryKeyOnly optimization.  The data param may be null.
     */
    private static class KeyOnlyKeyCreator implements SecondaryKeyCreator {

        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            result.setData(
                TestUtils.getTestArray(
                    TestUtils.getTestVal(key.getData()) + KEY_OFFSET));
            return true;
        }
    }
}
