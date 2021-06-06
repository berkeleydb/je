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

import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BINS_BYLEVEL;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BIN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BIN_ENTRIES_HISTOGRAM;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_DELETED_LN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_INS_BYLEVEL;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_IN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_LN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_MAINTREE_MAXDEPTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.LongArrayStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.util.test.SharedTestUtils;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

/**
 * Basic database operations, excluding configuration testing.
 */
public class DatabaseTest extends DualTestCase {
    private static final boolean DEBUG = false;
    private static final int NUM_RECS = 257;
    private static final int NUM_DUPS = 10;

    private final File envHome;
    private Environment env;

    private boolean runBtreeVerifier = true;
    private boolean runINCompressor = true;

    public DatabaseTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown()
        throws Exception {

        if (env != null) {
            try {
                env.close();
            } finally {
                env = null;
            }
        }
        super.tearDown();
    }

    /**
     * Make sure we can't create a transactional cursor on a non-transactional
     * database.
     */
    @Test
    public void testCursor()
        throws Exception {

        Environment txnalEnv = null;
        Database nonTxnalDb = null;
        Cursor txnalCursor = null;
        Transaction txn = null;

        try {

            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            txnalEnv = new Environment(envHome, envConfig);

            // Make a db and open it
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(false);
            nonTxnalDb = txnalEnv.openDatabase(null, "testDB", dbConfig);

            // We should not be able to open a txnal cursor.
            txn = txnalEnv.beginTransaction(null, null);
            try {
                txnalCursor = nonTxnalDb.openCursor(txn, null);
                fail("Openin a txnal cursor on a nontxnal db is invalid.");
            } catch (IllegalArgumentException e) {
                // expected
            }
        } finally {
            if (txn != null) {
                txn.abort();
            }
            if (txnalCursor != null) {
                txnalCursor.close();
            }
            if (nonTxnalDb != null) {
                nonTxnalDb.close();
            }
            if (txnalEnv != null) {
                txnalEnv.close();
            }

        }
    }

    @Test
    public void testWackyLocalesSR18504()
        throws Throwable {

        Locale currentLocale = Locale.getDefault();
        Database myDb = null;
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            myDb = initEnvAndDb(true, true, false, true, false, null);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            Locale.setDefault(currentLocale);
            myDb.close();
            close(env);
        }
    }

    @Test
    public void testPutExisting()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry getData = new DatabaseEntry();

            Transaction txn = env.beginTransaction(null, null);
            OperationResult r;

            for (int i = NUM_RECS; i > 0; i--) {

                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));

                r = myDb.put(txn, key, data, Put.OVERWRITE, null);
                assertNotNull(r);
                assertFalse(r.isUpdate());

                r = myDb.get(txn, key, getData, Get.SEARCH, null);
                assertNotNull(r);
                assertFalse(r.isUpdate());
                assertTrue(data.equals(getData));

                r = myDb.put(txn, key, data, Put.OVERWRITE, null);
                assertNotNull(r);
                assertTrue(r.isUpdate());

                r = myDb.get(txn, key, getData, Get.SEARCH_BOTH, null);
                assertNotNull(r);
                assertFalse(r.isUpdate());
                assertTrue(data.equals(getData));
            }

            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /*
     * Test that zero length data always returns the same (static) byte[].
     */
    @Test
    public void testZeroLengthData()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry getData = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            byte[] appZLBA = new byte[0];
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(appZLBA);
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.get(txn, key, getData, LockMode.DEFAULT));
                assertFalse(getData.getData() == appZLBA);
                assertTrue(getData.getData() ==
                           LogUtils.ZERO_LENGTH_BYTE_ARRAY);
                assertEquals(0, Key.compareKeys(data.getData(),
                                                getData.getData(), null));
            }
            txn.commit();
            myDb.close();
            close(env);

            /*
             * Read back from the log.
             */

            myDb = initEnvAndDb(true, true, false, true, false, null);
            key = new DatabaseEntry();
            data = new DatabaseEntry();
            getData = new DatabaseEntry();
            txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.get(txn, key, getData, LockMode.DEFAULT));
                assertTrue(getData.getData() ==
                           LogUtils.ZERO_LENGTH_BYTE_ARRAY);
            }
            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDeleteAbort()
        throws Throwable {

        try {
            /* Init the Environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            envConfig.setTxnTimeout(5, TimeUnit.SECONDS);
            envConfig.setLockTimeout(5, TimeUnit.SECONDS);
            envConfig.setTxnSerializableIsolation(false);

            env = create(envHome, envConfig);

            /* Open a database and insert some data. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database myDb = env.openDatabase(null, "testDB", dbConfig);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }
            txn.commit();
            int delkey = NUM_RECS/2;
            DeleteIt deleteIt = new DeleteIt(delkey, env, myDb);
            txn = env.beginTransaction(null, null);
            key.setData(TestUtils.getTestArray(delkey));
            data.setData(TestUtils.getTestArray(delkey));
            assertEquals(OperationStatus.SUCCESS,
                         myDb.delete(txn, key));
            Thread t1 = new Thread(deleteIt);
            t1.start();
            Thread.sleep(1000);
            txn.abort();
            t1.join();
            assertEquals(OperationStatus.SUCCESS,
                         deleteIt.getResult());
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }


    @Test
    public void testDeleteNonDup()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry getData = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }

            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.delete(txn, key));
                OperationStatus status =
                    myDb.get(txn, key, getData, LockMode.DEFAULT);
                if (status != OperationStatus.KEYEMPTY &&
                    status != OperationStatus.NOTFOUND) {
                    fail("invalid Database.get return: " + status);
                }
                assertEquals(OperationStatus.NOTFOUND,
                             myDb.delete(txn, key));
            }
            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDeleteDup()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry getData = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                for (int j = 0; j < NUM_DUPS; j++) {
                    data.setData(TestUtils.getTestArray(i + j));
                    assertEquals(OperationStatus.SUCCESS,
                                 myDb.put(txn, key, data));
                }
            }
            txn.commit();

            txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.delete(txn, key));
                OperationStatus status =
                    myDb.get(txn, key, getData, LockMode.DEFAULT);
                assertEquals(OperationStatus.NOTFOUND, status);
                assertEquals(OperationStatus.NOTFOUND, myDb.delete(txn, key));
            }
            txn.commit();
            myDb.close();
            close(env);

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /* Remove until 14264 is resolved.
    public void XXtestDeleteDupWithData()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry getData = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                for (int j = 0; j < NUM_DUPS; j++) {
                    data.setData(TestUtils.getTestArray(i + j));
                    assertEquals(OperationStatus.SUCCESS,
                                 myDb.put(txn, key, data));
                }
            }
            txn.commit();

            txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                for (int j = 0; j < NUM_DUPS; j++) {
                    data.setData(TestUtils.getTestArray(i + j));
                    assertEquals(OperationStatus.SUCCESS,
                                 myDb.delete(txn, key, data));
                    OperationStatus status =
                        myDb.getSearchBoth(txn, key, data, LockMode.DEFAULT);
                    if (status != OperationStatus.KEYEMPTY &&
                        status != OperationStatus.NOTFOUND) {
                        fail("invalid Database.get return");
                    }
                    assertEquals(OperationStatus.NOTFOUND,
                                 myDb.delete(txn, key, data));
                }
            }
            txn.commit();
            myDb.close();
            env.close();

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    public void XXtestDeleteDupWithSingleRecord()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry getData = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }
            txn.commit();

            txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.delete(txn, key, data));
                OperationStatus status =
                    myDb.getSearchBoth(txn, key, data, LockMode.DEFAULT);
                if (status != OperationStatus.KEYEMPTY &&
                    status != OperationStatus.NOTFOUND) {
                    fail("invalid Database.get return");
                }
                assertEquals(OperationStatus.NOTFOUND,
                             myDb.delete(txn, key, data));
            }
            txn.commit();
            myDb.close();
            env.close();

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
    */
    @Test
    public void testPutDuplicate()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                data.setData(TestUtils.getTestArray(i * 2));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }
            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testPutNoDupData()
        throws Throwable {
        try {
            Database myDb = initEnvAndDb(true, true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoDupData(txn, key, data));
                assertEquals(OperationStatus.KEYEXIST,
                             myDb.putNoDupData(txn, key, data));
                data.setData(TestUtils.getTestArray(i+1));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoDupData(txn, key, data));
            }
            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testPutNoOverwriteInANoDupDb()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoOverwrite(txn, key, data));
                assertEquals(OperationStatus.KEYEXIST,
                             myDb.putNoOverwrite(txn, key, data));
            }
            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testPutNoOverwriteInADupDbTxn()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = NUM_RECS; i > 0; i--) {
                Transaction txn1 = env.beginTransaction(null, null);
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoOverwrite(txn1, key, data));
                assertEquals(OperationStatus.KEYEXIST,
                             myDb.putNoOverwrite(txn1, key, data));
                data.setData(TestUtils.getTestArray(i << 1));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn1, key, data));
                data.setData(TestUtils.getTestArray(i << 2));
                assertEquals(OperationStatus.KEYEXIST,
                    myDb.putNoOverwrite(txn1, key, data));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.delete(txn1, key));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoOverwrite(txn1, key, data));
                txn1.commit();
            }
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testPutNoOverwriteInADupDbNoTxn()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, false, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoOverwrite(null, key, data));
                assertEquals(OperationStatus.KEYEXIST,
                             myDb.putNoOverwrite(null, key, data));
                data.setData(TestUtils.getTestArray(i << 1));
                assertEquals(OperationStatus.SUCCESS,
                    myDb.put(null, key, data));
                data.setData(TestUtils.getTestArray(i << 2));
                assertEquals(OperationStatus.KEYEXIST,
                             myDb.putNoOverwrite(null, key, data));
                assertEquals(OperationStatus.SUCCESS,
                    myDb.delete(null, key));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.putNoOverwrite(null, key, data));
            }
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDatabaseCount()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }

            long count = myDb.count();
            assertEquals(NUM_RECS, count);

            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDeferredWriteDatabaseCount()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, true, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(null, key, data));
            }

            long count = myDb.count();
            assertEquals(NUM_RECS, count);

            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testStat()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }

            BtreeStats stat = (BtreeStats)
                myDb.getStats(TestUtils.FAST_STATS);

            assertEquals(0, stat.getInternalNodeCount());
            assertEquals(0, stat.getBottomInternalNodeCount());
            assertEquals(0, stat.getLeafNodeCount());
            assertEquals(0, stat.getDeletedLeafNodeCount());
            assertEquals(0, stat.getMainTreeMaxDepth());

            stat = (BtreeStats) myDb.getStats(null);

            assertEquals(15, stat.getInternalNodeCount());
            assertEquals(52, stat.getBottomInternalNodeCount());
            assertEquals(NUM_RECS, stat.getLeafNodeCount());
            assertEquals(0, stat.getDeletedLeafNodeCount());
            assertEquals(4, stat.getMainTreeMaxDepth());

            long[] levelsTest = new long[]{ 12, 23, 34, 45, 56,
                                            67, 78, 89, 90, 0 };

            StatGroup group1 = new StatGroup("test1", "test1");
            LongStat stat1 = new LongStat(group1, BTREE_BIN_COUNT, 20);
            new LongStat(group1, BTREE_DELETED_LN_COUNT, 40);
            LongStat stat2 = new LongStat(group1, BTREE_IN_COUNT, 60);
            new LongStat(group1, BTREE_LN_COUNT, 80);
            new IntStat(group1, BTREE_MAINTREE_MAXDEPTH, 5);
            new LongArrayStat(group1, BTREE_INS_BYLEVEL, levelsTest);
            new LongArrayStat(group1, BTREE_BINS_BYLEVEL, levelsTest);
            new LongArrayStat(group1, BTREE_BIN_ENTRIES_HISTOGRAM, levelsTest);

            BtreeStats bts = new BtreeStats();
            bts.setDbImplStats(group1);

            assertEquals(20, bts.getBottomInternalNodeCount());
            assertEquals(40, bts.getDeletedLeafNodeCount());
            assertEquals(60, bts.getInternalNodeCount());
            assertEquals(80, bts.getLeafNodeCount());
            assertEquals(5, bts.getMainTreeMaxDepth());

            for (int i = 0; i < levelsTest.length; i++) {
                assertEquals(levelsTest[i], bts.getINsByLevel()[i]);
            }

            for (int i = 0; i < levelsTest.length; i++) {
                assertEquals(levelsTest[i], bts.getBINsByLevel()[i]);
            }

            for (int i = 0; i < levelsTest.length; i++) {
                assertEquals(levelsTest[i], bts.getBINEntriesHistogram()[i]);
            }

            bts.toString();

            stat1.set(0L);
            stat2.set(0L);

            assertEquals(0, bts.getBottomInternalNodeCount());
            assertEquals(0, bts.getInternalNodeCount());
            bts.toString();

            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDatabaseCountEmptyDB()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);

            long count = myDb.count();
            assertEquals(0, count);

            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDatabaseCountWithDeletedEntries()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, false, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            int deletedCount = 0;
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                if ((i % 5) == 0) {
                    myDb.delete(txn, key);
                    deletedCount++;
                }
            }

            long count = myDb.count();
            assertEquals(NUM_RECS - deletedCount, count);

            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testStatDups()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                for (int j = 0; j < 10; j++) {
                    data.setData(TestUtils.getTestArray(i + j));
                    assertEquals(OperationStatus.SUCCESS,
                                 myDb.put(txn, key, data));
                }
            }

            BtreeStats stat = (BtreeStats)
                myDb.getStats(TestUtils.FAST_STATS);

            assertEquals(0, stat.getInternalNodeCount());
            assertEquals(0, stat.getDuplicateInternalNodeCount());
            assertEquals(0, stat.getBottomInternalNodeCount());
            assertEquals(0, stat.getDuplicateBottomInternalNodeCount());
            assertEquals(0, stat.getLeafNodeCount());
            assertEquals(0, stat.getDeletedLeafNodeCount());
            assertEquals(0, stat.getDupCountLeafNodeCount());
            assertEquals(0, stat.getMainTreeMaxDepth());
            assertEquals(0, stat.getDuplicateTreeMaxDepth());

            stat = (BtreeStats) myDb.getStats(null);

            assertEquals(383, stat.getInternalNodeCount());
            assertEquals(0, stat.getDuplicateInternalNodeCount());
            assertEquals(771, stat.getBottomInternalNodeCount());
            assertEquals(0, stat.getDuplicateBottomInternalNodeCount());
            assertEquals(2570, stat.getLeafNodeCount());
            assertEquals(0, stat.getDeletedLeafNodeCount());
            assertEquals(0, stat.getDupCountLeafNodeCount());
            assertEquals(7, stat.getMainTreeMaxDepth());
            assertEquals(0, stat.getDuplicateTreeMaxDepth());

            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDatabaseCountDups()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, true, false, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            Transaction txn = env.beginTransaction(null, null);
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
                for (int j = 0; j < 10; j++) {
                    data.setData(TestUtils.getTestArray(i + j));
                    assertEquals(OperationStatus.SUCCESS,
                                 myDb.put(txn, key, data));
                }
            }

            long count = myDb.count();

            assertEquals(2570, count);

            txn.commit();
            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDeferredWriteDatabaseCountDups()
        throws Throwable {

        try {
            Database myDb = initEnvAndDb(true, true, true, true, true, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = NUM_RECS; i > 0; i--) {
                key.setData(TestUtils.getTestArray(i));
                data.setData(TestUtils.getTestArray(i));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(null, key, data));
                for (int j = 0; j < 10; j++) {
                    data.setData(TestUtils.getTestArray(i + j));
                    assertEquals(OperationStatus.SUCCESS,
                                 myDb.put(null, key, data));
                }
            }

            long count = myDb.count();

            assertEquals(2570, count);

            myDb.close();
            close(env);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testStatDeletes()
        throws Throwable {

        deleteTestInternal(1, 2, 0, 2);
        tearDown();
        deleteTestInternal(2, 2, 2, 2);
        tearDown();
        deleteTestInternal(10, 2, 10, 10);
        tearDown();
        deleteTestInternal(11, 2, 10, 12);
    }

    private void deleteTestInternal(int numRecs,
                                    int numDupRecs,
                                    int expectedLNs,
                                    int expectedDeletedLNs)
        throws Throwable {

        runINCompressor = false;
        TestUtils.removeLogFiles("Setup", envHome, false);
        Database myDb = initEnvAndDb(true, true, true, true, false, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);
        for (int i = numRecs; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            for (int j = 0; j < numDupRecs; j++) {
                data.setData(TestUtils.getTestArray(i + j));
                assertEquals(OperationStatus.SUCCESS,
                             myDb.put(txn, key, data));
            }
        }

        for (int i = numRecs; i > 0; i -= 2) {
            key.setData(TestUtils.getTestArray(i));
            assertEquals(OperationStatus.SUCCESS,
                         myDb.delete(txn, key));
        }

        txn.commit();

        BtreeStats stat = (BtreeStats) myDb.getStats(null);

        assertEquals(expectedLNs, stat.getLeafNodeCount());
        assertEquals(expectedDeletedLNs, stat.getDeletedLeafNodeCount());
        assertEquals(0, stat.getDupCountLeafNodeCount());

        myDb.close();
        close(env);
    }

    /**
     * Test preload of all records into the main cache.
     */
    @Test
    public void testPreloadAllInCache()
        throws Throwable {

        doPreloadAllInCache(false);
    }

    /**
     * Test preload of all records into cache, using an off-heap cache.
     */
    @Test
    public void testPreloadAllInCacheOffHeap()
        throws Throwable {

        Assume.assumeTrue(!JVMSystemUtils.ZING_JVM);

        System.out.println("testPreloadAllInCacheOffHeap disabled [#25594]");

//        doPreloadAllInCache(true);
    }

    /**
     * Inserts and preloads 100,000 records with key size 100, data size 100,
     * no dups. LNs are not embedded.
     *
     * DbCacheSize requires:
     *  3,158,477 env overhead
     *
     * 1. java DbCacheSize -records 100000 -key 100 -data 100
     *
     * 14,567,896  Internal nodes only
     * 29,584,536  Internal nodes and leaf nodes
     *
     * 2. java DbCacheSize -records 100000 -key 100 -data 100 \
     *                     -offheap -maincache 6558477
     *
     *  6,558,477 = 3,400,000 + 3,158,477
     *
     *  3,400,000  Internal nodes only: MAIN cache
     *  8,828,128  Internal nodes only: OFF-HEAP cache
     *  3,400,000  Internal nodes and leaf nodes: MAIN cache
     * 21,671,816  Internal nodes and leaf nodes: OFF-HEAP cache
     *
     * 3. java -XX:-UseCompressedOops DbCacheSize \  # for Zing
     *       -records 100000 -key 100 -data 100
     *
     * 16,157,464  Internal nodes only
     * 33,368,152  Internal nodes and leaf nodes
     */
    private void doPreloadAllInCache(final boolean useOffHeapCache)
        throws Throwable {

        final int nRecs = 100000;
        final long mainDataSize;
        final long offHeapDataSize;
        final long mainDataSizeNoLNs;
        final long offHeapDataSizeNoLNs;

        if (JVMSystemUtils.ZING_JVM) {
            mainDataSize = 34000000;
            offHeapDataSize = 0;
            mainDataSizeNoLNs = 17000000;
            offHeapDataSizeNoLNs = 0;
        } else if (useOffHeapCache) {
            mainDataSize = 3400000;
            offHeapDataSize = 21800000;
            mainDataSizeNoLNs = 3400000;
            offHeapDataSizeNoLNs = 8900000;
        } else {
            mainDataSize = 30000000;
            offHeapDataSize = 0;
            mainDataSizeNoLNs = 14600000;
            offHeapDataSizeNoLNs = 0;
        }

        /* Use full size cache to do insertions. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setOffHeapCacheSize(offHeapDataSize);
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.STATS_COLLECT, "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        env = new Environment(envHome, envConfig);

        TestUtils.adjustCacheSize(env, mainDataSize);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        Database db = env.openDatabase(null, "foo", dbConfig);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        int nBins = insert100ByteTestData(db, nRecs);
        verify100ByteTestData(db, nRecs);

        /*
         * Re-open env with full cache sizes to hold all nodes.
         */
        db.close();
        close(env);
        env = null;
        envConfig.setAllowCreate(false);
        dbConfig.setAllowCreate(false);
        envConfig.setCacheSize(mainDataSize);
        envConfig.setOffHeapCacheSize(offHeapDataSize);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);
        db = env.openDatabase(null, "foo", dbConfig);

        TestUtils.adjustCacheSize(env, mainDataSize);

        /* Expect very little in cache. */
        EnvironmentStats stats = env.getStats(null);
        assertEquals(0, stats.getOffHeapCachedBINs());
        assertEquals(0, stats.getOffHeapCachedLNs());
        assertTrue(stats.getNCachedBINs() < 10);

        /* Preload with LNs and expect everything in cache. */
        PreloadConfig preConfig = new PreloadConfig();
        preConfig.setLoadLNs(true);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Strip LNs and repeat preload. */
        mutateBINs(db, false /*mutateToDelta*/, true /*stripLNs*/);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Mutate BINs to BIN-deltas and repeat preload. */
        mutateBINs(db, true /*mutateToDelta*/, false /*stripLNs*/);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Both mutate and strip LNs, and repeat preload. */
        mutateBINs(db, true /*mutateToDelta*/, true /*stripLNs*/);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Preload with everything already in cache. */
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Verify final result. */
        verify100ByteTestData(db, nRecs);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /*
         * Re-open env with "internal nodes only" cache sizes.
         */
        db.close();
        close(env);
        env = null;
        envConfig.setAllowCreate(false);
        dbConfig.setAllowCreate(false);
        envConfig.setCacheSize(mainDataSizeNoLNs);
        envConfig.setOffHeapCacheSize(offHeapDataSizeNoLNs);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);
        db = env.openDatabase(null, "foo", dbConfig);

        TestUtils.adjustCacheSize(env, mainDataSizeNoLNs);

        /* Expect very little in cache. */
        stats = env.getStats(StatsConfig.CLEAR);
        assertEquals(0, stats.getOffHeapCachedBINs());
        assertEquals(0, stats.getOffHeapCachedLNs());
        assertTrue(stats.getNCachedBINs() < 10);

        /* Preload without LNs and expect everything except LNs in cache. */
        preConfig.setLoadLNs(false);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, 0, nBins);

        /* Mutate BINs to BIN-deltas and repeat preload. */
        mutateBINs(db, true /*mutateToDelta*/, false /*stripLNs*/);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, 0, nBins);

        /* Preload with BINs already in cache. */
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, 0, nBins);

        /*
         * Mutate config to use full cache sizes to hold all nodes.
         */
        envConfig.setCacheSize(mainDataSize);
        envConfig.setOffHeapCacheSize(offHeapDataSize);
        env.setMutableConfig(envConfig);

        /* Preload with LNs and expect everything in cache. */
        preConfig.setLoadLNs(true);
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Preload with everything already in cache. */
        db.preload(preConfig);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        /* Verify final result. */
        verify100ByteTestData(db, nRecs);
        expectCachedLNsAndBINs(db, nRecs, nBins);

        db.close();
        close(env);
    }

    private int insert100ByteTestData(Database db, int nRecs) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        /*
         * Count BINs as we insert. Using the nCachedBINs stats to count the
         * BINs is unreliable because the LRU will cause some cold level 2
         * INs to be evicted from main, along with their off-heap children.
         */
        final Cursor cursor = db.openCursor(null, null);
        BIN prevBin = null;
        int nBins = 0;
        for (int i = 0; i < nRecs; i += 1) {
            final byte[] array = get100ByteTestArray(i);
            key.setData(array);
            data.setData(array);
            final OperationResult result = cursor.put(
                key, data, Put.NO_OVERWRITE, null);
            assertNotNull(result);
            final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
            if (prevBin != bin) {
                prevBin = bin;
                nBins += 1;
            }
        }
        cursor.close();
        return nBins;
    }

    private void verify100ByteTestData(Database db, int nRecs) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = db.openCursor(null, null);
        for (int i = 0; i < nRecs; i += 1) {
            final OperationResult result =
                cursor.get(key, data, Get.NEXT, null);
            assertNotNull(result);
            final byte[] array = get100ByteTestArray(i);
            assertTrue(Arrays.equals(array, key.getData()));
            assertTrue(Arrays.equals(array, data.getData()));
        }
        cursor.close();
    }

    private byte[] get100ByteTestArray(int i) {
        final byte[] value = TestUtils.getTestArray(i);
        final byte[] array = new byte[100];
        System.arraycopy(value, 0, array, 0, value.length);
        return array;
    }

    private void mutateBINs(Database db,
                            boolean mutateToDelta,
                            boolean stripLNs) {

        final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        final OffHeapCache ohCache = dbImpl.getEnv().getOffHeapCache();

        for (final IN in : dbImpl.getEnv().getInMemoryINs()) {
            if (in.getDatabase() != dbImpl) {
                continue;
            }
            in.latch();
            try {
                if (in.isBIN()) {
                    final BIN bin = (BIN) in;
                    if (stripLNs) {
                        bin.evictLNs();
                    }
                    if (mutateToDelta && bin.canMutateToBINDelta()) {
                        bin.mutateToBINDelta();
                    }
                } else {
                    for (int i = 0; i < in.getNEntries(); i += 1) {
                        if (in.getOffHeapBINId(i) < 0) {
                            continue;
                        }
                        if (stripLNs || mutateToDelta) {
                            ohCache.stripLNs(in, i);
                        }
                        if (mutateToDelta) {
                            ohCache.mutateToBINDelta(in, i);
                        }
                    }
                }
            } finally {
                in.releaseLatch();
            }
        }
    }

    private void expectCachedLNsAndBINs(
        final Database db,
        final int nExpectedLNs,
        final int nExpectedBINs) {

        final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        final EnvironmentImpl envImpl = dbImpl.getEnv();
        final OffHeapCache ohCache = envImpl.getOffHeapCache();

        int nL2INs = 0;
        int nMainLNs = 0;
        int nMainBINs = 0;
        int nMainBINDeltas = 0;
        int nOhLNs = 0;
        int nOhBINs = 0;
        int nOhBINDeltas = 0;

        for (final IN in : dbImpl.getEnv().getInMemoryINs()) {

            /* Be certain that this node is resident. */
            in.latchNoUpdateLRU();
            final boolean resident = in.getInListResident();
            in.releaseLatch();

            if (!resident) {
                System.out.println("Not resident");
                continue;
            }

            /* Ensure there are no off-heap objects in other DBs. */
            if (in.isBIN()) {
                final BIN bin = (BIN) in;
                for (int i = 0; i < bin.getNEntries(); i += 1) {
                    if (bin.getOffHeapLNId(i) != 0) {
                        assertSame(dbImpl, in.getDatabase());
                    }
                }
            } else {
                for (int i = 0; i < in.getNEntries(); i += 1) {
                    final int ohBinId = in.getOffHeapBINId(i);
                    if (ohBinId >= 0) {
                        assertSame(dbImpl, in.getDatabase());
                    }
                }
            }

            if (in.getDatabase() != dbImpl) {
                continue;
            }

            if (in.isBIN()) {
                nMainBINs += 1;
                if (in.isBINDelta(false)) {
                    nMainBINDeltas += 1;
                }
                final BIN bin = (BIN) in;
                for (int i = 0; i < bin.getNEntries(); i += 1) {
                    if (bin.getTarget(i) != null) {
                        nMainLNs += 1;
                    }
                    if (bin.getOffHeapLNId(i) != 0) {
                        nOhLNs += 1;
                    }
                }
            } else {
                if (in.getNormalizedLevel() != 2) {
                    continue;
                }
                nL2INs += 1;
                for (int i = 0; i < in.getNEntries(); i += 1) {
                    final int ohBinId = in.getOffHeapBINId(i);
                    if (ohBinId < 0) {
                        assertNotNull(in.getTarget(i));
                        continue;
                    }
                    assertNull(in.getTarget(i));
                    final BIN ohBin = ohCache.loadBIN(envImpl, ohBinId);
                    assertTrue(ohBin.getLastFullLsn() != DbLsn.NULL_LSN);
                    nOhBINs += 1;
                    if (ohBin.isBINDelta(false)) {
                        nOhBINDeltas += 1;
                    }
                    for (int j = 0; j < ohBin.getNEntries(); j += 1) {
                        if (ohBin.getOffHeapLNId(j) != 0) {
                            nOhLNs += 1;
                        }
                    }
                }
            }
        }

        /* Recalculate using Btree as a double-check. */
        int nL2INs2 = 0;
        int nMainBINs2 = 0;
        int nMainBINDeltas2 = 0;
        int nOhBINs2 = 0;
        int nOhBINDeltas2 = 0;

        final IN rootIN = dbImpl.getTree().getResidentRootIN(false);
        assertTrue(rootIN.getInListResident());
        assertEquals(3, rootIN.getNormalizedLevel());
        for (int i = 0; i < rootIN.getNEntries(); i += 1) {
            final IN in = (IN) rootIN.getTarget(i);
            assertNotNull(in);
            assertTrue(in.getInListResident());
            nL2INs2 += 1;
            for (int j = 0; j < in.getNEntries(); j += 1) {
                final int ohBinId = in.getOffHeapBINId(j);
                final BIN bin = (BIN) in.getTarget(j);
                if (bin != null) {
                    assertTrue(bin.getInListResident());
                    assertEquals(-1, ohBinId);
                    nMainBINs2 += 1;
                    if (bin.isBINDelta(false)) {
                        nMainBINDeltas2 += 1;
                    }
                } else {
                    assertTrue(ohBinId >= 0);
                    final BIN ohBin = ohCache.loadBIN(envImpl, ohBinId);
                    assertTrue(ohBin.getLastFullLsn() != DbLsn.NULL_LSN);
                    nOhBINs2 += 1;
                    if (ohBin.isBINDelta(false)) {
                        nOhBINDeltas2 += 1;
                    }
                }
            }
//            System.out.println(in2.getClass().getName());
        }

        final String msg = "Cache contents" +
            " nL2INs=" + nL2INs +
            " nL2INs2=" + nL2INs2 +
            " mainBINs=" + nMainBINs +
            " mainBINDeltas=" + nMainBINDeltas +
            " mainBINs2=" + nMainBINs2 +
            " mainBINDeltas2=" + nMainBINDeltas2 +
            " offHeapBINs=" + nOhBINs +
            " offHeapBINDeltas=" + nOhBINDeltas +
            " offHeapBINs2=" + nOhBINs2 +
            " offHeapBINDeltas2=" + nOhBINDeltas2 +
            " mainLNs=" + nMainLNs +
            " offHeapLNs=" + nOhLNs;

        EnvironmentStats stats = env.getStats(null);
        try {
            assertEquals(msg, nL2INs, nL2INs2);
            assertEquals(msg, nOhBINs, nOhBINs2);
            assertEquals(msg, nOhBINDeltas, nOhBINDeltas2);
            assertEquals(msg, nMainBINs, nMainBINs2);
            assertEquals(msg, nMainBINDeltas, nMainBINDeltas2);
            assertEquals(msg, nOhBINs, stats.getOffHeapCachedBINs());
            assertEquals(msg, nOhBINDeltas, stats.getOffHeapCachedBINDeltas());
            assertEquals(msg, nOhLNs, stats.getOffHeapCachedLNs());
        } catch (Throwable e) {
            System.out.println(stats);
            throw e;
        }

        assertEquals(msg, 0, nMainBINDeltas);
        assertEquals(msg, 0, nOhBINDeltas);
        assertEquals(msg, nExpectedLNs, nMainLNs + nOhLNs);
        assertEquals(msg, nExpectedBINs, nMainBINs + nOhBINs);
    }

    /**
     * Test the cache memory limit.
     */
    @Test
    public void testPreloadCacheMemoryLimit()
        throws Throwable {

        /* Set up a test db */
        Database myDb = initEnvAndDb(false, true, false, true, false, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);
        for (int i = 2500; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(TestUtils.getTestArray(i));
            assertEquals(OperationStatus.SUCCESS,
                         myDb.put(txn, key, data));
        }

        /* Recover the database, restart w/no evictor. */
        long postCreateMemUsage = env.getMemoryUsage();
        INList inlist = env.getNonNullEnvImpl().getInMemoryINs();
        long postCreateResidentNodes = inlist.getSize();
        txn.commit();
        myDb.close();
        close(env);
        myDb = initEnvAndDb(
            true, true, false, true, false,
            MemoryBudget.MIN_MAX_MEMORY_SIZE_STRING);

        /*
         * Do two evictions, because the first eviction will only strip
         * LNs. We need to actually evict BINS because preload only pulls in
         * IN/BINs
         */
        env.evictMemory(); // first eviction strips LNS.
        env.evictMemory(); // second one will evict BINS

        long postEvictMemUsage = env.getMemoryUsage();
        inlist = env.getNonNullEnvImpl().getInMemoryINs(); // re-get inList
        long postEvictResidentNodes = inlist.getSize();

        /* Now preload, but not up to the full size of the db */
        PreloadConfig conf = new PreloadConfig();
        conf.setMaxBytes(92000);
        PreloadStats stats =
            myDb.preload(conf); /* Cache size is currently 92160. */

        assertEquals(PreloadStatus.FILLED_CACHE, stats.getStatus());

        long postPreloadMemUsage = env.getMemoryUsage();
        long postPreloadResidentNodes = inlist.getSize();

        /* Now iterate to get everything back into memory */
        Cursor cursor = myDb.openCursor(null, null);
        int count = 0;
        OperationStatus status = cursor.getFirst(key, data, LockMode.DEFAULT);
        while (status == OperationStatus.SUCCESS) {
            count++;
            status = cursor.getNext(key, data, LockMode.DEFAULT);
        }
        cursor.close();

        long postIterationMemUsage = env.getMemoryUsage();
        long postIterationResidentNodes = inlist.getSize();

        if (DEBUG) {
            System.out.println("postCreateMemUsage: " + postCreateMemUsage);
            System.out.println("postEvictMemUsage: " + postEvictMemUsage);
            System.out.println("postPreloadMemUsage: " + postPreloadMemUsage);
            System.out.println("postIterationMemUsage: " +
                               postIterationMemUsage);
            System.out.println("postEvictResidentNodes: " +
                               postEvictResidentNodes);
            System.out.println("postPreloadResidentNodes: " +
                               postPreloadResidentNodes);
            System.out.println("postIterationResidentNodes: " +
                               postIterationResidentNodes);
            System.out.println("postCreateResidentNodes: " +
                               postCreateResidentNodes);
        }

        assertTrue(postEvictMemUsage < postCreateMemUsage);
        assertTrue(postEvictMemUsage < postPreloadMemUsage);
        assertTrue("postPreloadMemUsage=" + postPreloadMemUsage +
                " postIterationMemUsage=" + postIterationMemUsage,
            postPreloadMemUsage < postIterationMemUsage);
        assertTrue(postIterationMemUsage <= postCreateMemUsage);
        assertTrue(postEvictResidentNodes < postPreloadResidentNodes);
        //assertEquals(postCreateResidentNodes, postIterationResidentNodes);
        assertTrue(postCreateResidentNodes >= postIterationResidentNodes);

        VerifyConfig vcfg = new VerifyConfig();

        vcfg.setPropagateExceptions(true);
        vcfg.setAggressive(false);
        vcfg.setPrintInfo(true);
        vcfg.setShowProgressStream(System.out);
        vcfg.setShowProgressInterval(5);

        assertEquals(true, vcfg.getPropagateExceptions());
        assertEquals(false, vcfg.getAggressive());
        assertEquals(true, vcfg.getPrintInfo());
        assertEquals(System.out.getClass(),
                     vcfg.getShowProgressStream().getClass());
        assertEquals(5, vcfg.getShowProgressInterval());
        vcfg.toString();

        myDb.close();
        close(env);
    }

    /**
     * Test the internal memory limit.
     */
    @Test
    public void testPreloadInternalMemoryLimit()
        throws Throwable {

        /* Set up a test db */
        Database myDb = initEnvAndDb(false, true, false, true, false, null);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);

        for (int i = 2500; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(TestUtils.getTestArray(i));
            assertEquals(OperationStatus.SUCCESS, myDb.put(txn, key, data));
        }

        /* Recover the database, restart w/no evictor. */
        long postCreateMemUsage = env.getMemoryUsage();
        INList inlist = env.getNonNullEnvImpl().getInMemoryINs();
        long postCreateResidentNodes = inlist.getSize();

        txn.commit();
        myDb.close();

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        boolean embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        close(env);

        /*
         * Don't run the cleaner if embedded LNs. Otherwise, the cleaner
         * admin overhead will tip the mem usage over the cache size, a
         * situation that cannot be rectified by the env.evictMemory() calls
         * below. As a result, the preload will become a noop and the assertion
         * at the end of this test will fail. 
         */
        myDb = initEnvAndDb(
            true, !embeddedLNs /*runCleaner*/, false, true, false,
            MemoryBudget.MIN_MAX_MEMORY_SIZE_STRING);

        long postReopenMemUsage = env.getMemoryUsage();
        inlist = env.getNonNullEnvImpl().getInMemoryINs(); // re-get inList
        long postReopenResidentNodes = inlist.getSize();

        // 26,624
        long minTreeMemUsage =
        env.getNonNullEnvImpl().getMemoryBudget().getMinTreeMemoryUsage();
        long currTreeMemUsage = 
        env.getNonNullEnvImpl().getMemoryBudget().getTreeMemoryUsage();

        /*
         * Do two evictions, because the first eviction will only strip
         * LNs. We need to actually evict BINS because preload only pulls in
         * IN/BINs
         */
        env.evictMemory(); // first eviction strips LNS.
        env.evictMemory(); // second one will evict BINS

        long postEvictMemUsage = env.getMemoryUsage();
        inlist = env.getNonNullEnvImpl().getInMemoryINs(); // re-get inList
        long postEvictResidentNodes = inlist.getSize();

        /* Now preload, but not up to the full size of the db */
        PreloadConfig conf = new PreloadConfig();
        conf.setInternalMemoryLimit(9200);

        PreloadStats stats = myDb.preload(conf); /* Cache size is currently 92160. */

        if (DEBUG) {
            System.out.println();
            System.out.println("postCreateResidentNodes   = " +
                               postCreateResidentNodes);
            System.out.println("postCreateMemUsage        = " +
                               postCreateMemUsage);

            System.out.println();
            System.out.println("postReopenResidentNodes   = " +
                               postReopenResidentNodes);
            System.out.println("postReopenMemUsage        = " +
                               postReopenMemUsage);

            System.out.println();
            System.out.println("postReopenMinTreeMemUsage = " +
                               minTreeMemUsage);
            System.out.println("postReopenTreeMemUsage    = " +
                               currTreeMemUsage);

            System.out.println();
            System.out.println("postEvictResidentNodes    = " +
                               postEvictResidentNodes);
            System.out.println("postEvictMemUsage         = " +
                               postEvictMemUsage);

            System.out.println("NCountMemoryExceeded = " +
                               stats.getNCountMemoryExceeded());
            System.out.println("Stats : " + stats);
        }

        assertTrue(stats.getNCountMemoryExceeded() > 0);

        myDb.close();
        close(env);
    }

    @Test
    public void testPreloadTimeLimit()
        throws Throwable {

        /* Set up a test db */
        Database myDb = initEnvAndDb(false, true, false, true, false, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);
        for (int i = 25000; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(new byte[1]);
            assertEquals(OperationStatus.SUCCESS,
                         myDb.put(txn, key, data));
        }

        /* Recover the database, restart w/no evictor. */
        long postCreateMemUsage = env.getMemoryUsage();
        INList inlist = env.getNonNullEnvImpl().getInMemoryINs();
        long postCreateResidentNodes = inlist.getSize();
        txn.commit();
        myDb.close();
        close(env);
        myDb = initEnvAndDb(true, true, false, true, false, null);

        /*
         * Do two evictions, because the first eviction will only strip
         * LNs. We need to actually evict BINS because preload only pulls in
         * IN/BINs
         */
        env.evictMemory(); // first eviction strips LNS.
        env.evictMemory(); // second one will evict BINS

        long postEvictMemUsage = env.getMemoryUsage();
        inlist = env.getNonNullEnvImpl().getInMemoryINs(); // re-get inList
        long postEvictResidentNodes = inlist.getSize();

        /* Now preload, but not up to the full size of the db */
        PreloadConfig conf = new PreloadConfig();
        conf.setMaxMillisecs(50);
        PreloadStats stats = myDb.preload(conf);
        assertEquals(PreloadStatus.EXCEEDED_TIME, stats.getStatus());

        long postPreloadMemUsage = env.getMemoryUsage();
        long postPreloadResidentNodes = inlist.getSize();

        /* Now iterate to get everything back into memory */
        Cursor cursor = myDb.openCursor(null, null);
        int count = 0;
        OperationStatus status = cursor.getFirst(key, data, LockMode.DEFAULT);
        while (status == OperationStatus.SUCCESS) {
            count++;
            status = cursor.getNext(key, data, LockMode.DEFAULT);
        }
        cursor.close();

        long postIterationMemUsage = env.getMemoryUsage();
        long postIterationResidentNodes = inlist.getSize();

        if (DEBUG) {
            System.out.println("postCreateMemUsage: " + postCreateMemUsage);
            System.out.println("postEvictMemUsage: " + postEvictMemUsage);
            System.out.println("postPreloadMemUsage: " + postPreloadMemUsage);
            System.out.println("postIterationMemUsage: " +
                               postIterationMemUsage);
            System.out.println("postEvictResidentNodes: " +
                               postEvictResidentNodes);
            System.out.println("postPreloadResidentNodes: " +
                               postPreloadResidentNodes);
            System.out.println("postIterationResidentNodes: " +
                               postIterationResidentNodes);
            System.out.println("postCreateResidentNodes: " +
                               postCreateResidentNodes);
        }

        assertTrue(postEvictMemUsage < postCreateMemUsage);
        assertTrue(postEvictMemUsage < postPreloadMemUsage);
        assertTrue("postPreloadMemUsage=" + postPreloadMemUsage +
                   " postIterationMemUsage=" + postIterationMemUsage,
                   postPreloadMemUsage < postIterationMemUsage);
        assertTrue(postIterationMemUsage <= postCreateMemUsage);
        assertTrue(postEvictResidentNodes < postPreloadResidentNodes);
        //assertEquals(postCreateResidentNodes, postIterationResidentNodes);
        assertTrue(postCreateResidentNodes >= postIterationResidentNodes);

        myDb.close();
        close(env);
    }

    @Test
    public void testPreloadMultipleDatabases()
        throws Throwable {

        /* Set up a test db */
        Database myDb1 = initEnvAndDb(false, true, false, true, false, null);

        /* Make a 2nd db and open it. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database myDb2 = env.openDatabase(null, "testDB2", dbConfig);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);
        for (int i = 25000; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(new byte[1]);
            assertEquals(OperationStatus.SUCCESS,
                         myDb1.put(txn, key, data));
            assertEquals(OperationStatus.SUCCESS,
                         myDb2.put(txn, key, data));
        }

        /* Recover the database, restart w/no evictor. */
        long postCreateMemUsage = env.getMemoryUsage();
        INList inlist = env.getNonNullEnvImpl().getInMemoryINs();
        long postCreateResidentNodes = inlist.getSize();
        txn.commit();
        myDb2.close();
        myDb1.close();
        close(env);
        myDb1 = initEnvAndDb(true, true, false, true, false, null);
        /* Make a 2nd db and open it. */
        dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        myDb2 = env.openDatabase(null, "testDB2", dbConfig);

        /*
         * Do two evictions, because the first eviction will only strip
         * LNs. We need to actually evict BINS because preload only pulls in
         * IN/BINs
         */
        env.evictMemory(); // first eviction strips LNS.
        env.evictMemory(); // second one will evict BINS

        long postEvictMemUsage = env.getMemoryUsage();
        inlist = env.getNonNullEnvImpl().getInMemoryINs(); // re-get inList
        long postEvictResidentNodes = inlist.getSize();

        /* Now preload. */
        PreloadConfig conf = new PreloadConfig();
        PreloadStats stats = env.preload(new Database[] { myDb1, myDb2 }, conf);

        long postPreloadMemUsage = env.getMemoryUsage();
        long postPreloadResidentNodes = inlist.getSize();

        env.evictMemory(); // first eviction strips LNS.
        env.evictMemory(); // second one will evict BINS

        /* Now iterate to get everything back into memory */
        Cursor cursor = myDb1.openCursor(null, null);
        int count = 0;
        OperationStatus status = cursor.getFirst(key, data, LockMode.DEFAULT);
        while (status == OperationStatus.SUCCESS) {
            count++;
            status = cursor.getNext(key, data, LockMode.DEFAULT);
        }
        cursor.close();

        cursor = myDb2.openCursor(null, null);
        count = 0;
        status = cursor.getFirst(key, data, LockMode.DEFAULT);
        while (status == OperationStatus.SUCCESS) {
            count++;
            status = cursor.getNext(key, data, LockMode.DEFAULT);
        }
        cursor.close();

        long postIterationMemUsage = env.getMemoryUsage();
        long postIterationResidentNodes = inlist.getSize();

        if (DEBUG) {
            System.out.println("postCreateMemUsage: " + postCreateMemUsage);
            System.out.println("postEvictMemUsage: " + postEvictMemUsage);
            System.out.println("postPreloadMemUsage: " + postPreloadMemUsage);
            System.out.println("postIterationMemUsage: " +
                               postIterationMemUsage);
            System.out.println("postEvictResidentNodes: " +
                               postEvictResidentNodes);
            System.out.println("postPreloadResidentNodes: " +
                               postPreloadResidentNodes);
            System.out.println("postIterationResidentNodes: " +
                               postIterationResidentNodes);
            System.out.println("postCreateResidentNodes: " +
                               postCreateResidentNodes);
        }

        assertTrue(postEvictMemUsage < postCreateMemUsage);
        assertTrue(postEvictMemUsage < postPreloadMemUsage);
        assertTrue("postPreloadMemUsage=" + postPreloadMemUsage +
                   " postIterationMemUsage=" + postIterationMemUsage,
                   postPreloadMemUsage == postIterationMemUsage);
        assertTrue(postIterationMemUsage <= postCreateMemUsage);
        assertTrue(postEvictResidentNodes < postPreloadResidentNodes);
        //assertEquals(postCreateResidentNodes, postIterationResidentNodes);
        assertTrue(postCreateResidentNodes >= postIterationResidentNodes);

        myDb1.close();
        myDb2.close();
        close(env);
    }

    @Test
    public void testPreloadWithProgress()
        throws Throwable {

        /* Set up a test db */
        Database myDb = initEnvAndDb(false, true, false, true, false, null);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);

        for (int i = 2500; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(TestUtils.getTestArray(i));
            assertEquals(OperationStatus.SUCCESS,
                         myDb.put(txn, key, data));
        }

        /* Recover the database, restart w/no evictor. */
        long postCreateMemUsage = env.getMemoryUsage();
        INList inlist = env.getNonNullEnvImpl().getInMemoryINs();
        long postCreateResidentNodes = inlist.getSize();

        txn.commit();
        myDb.close();

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        boolean embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        close(env);

        /*
         * Don't run the cleaner if embedded LNs. Otherwise, the cleaner
         * admin overhead will tip the mem usage over the cache size, a
         * situation that cannot be rectified by the env.evictMemory() calls
         * below. As a result, the preload will become a noop and the assertion
         * at the end of this test will fail. 
         */
        myDb = initEnvAndDb(
            true, !embeddedLNs/*runCleaner*/, false, true, false,
            MemoryBudget.MIN_MAX_MEMORY_SIZE_STRING);

        /*
         * Do two evictions, because the first eviction will only strip
         * LNs. We need to actually evict BINS because preload only pulls in
         * IN/BINs
         */
        env.evictMemory(); // first eviction strips LNS.
        env.evictMemory(); // second one will evict BINS

        long postEvictMemUsage = env.getMemoryUsage();
        inlist = env.getNonNullEnvImpl().getInMemoryINs(); // re-get inList
        long postEvictResidentNodes = inlist.getSize();

        /* Now preload, but not up to the full size of the db */
        PreloadConfig conf = new PreloadConfig();

        conf.setProgressListener(
            new ProgressListener<PreloadConfig.Phases>() {
                public boolean progress(PreloadConfig.Phases operation,
                                        long n,
                                        long total) {
                    if (n % 10 == 0) {
                        throw new RuntimeException("Stop it");
                    }
                    return true;
                }
            });
        PreloadStats stats = null;
        try {
            stats = myDb.preload(conf);
            fail("expected RE");
        } catch (RuntimeException RE) {
            // Expect RuntimeException
        }

        conf.setProgressListener(new ProgressListener<PreloadConfig.Phases>() {
                public boolean progress(PreloadConfig.Phases operation,
                                        long n,
                                        long total) {
                    if (n % 10 == 0) {
                        return false;
                    }
                    return true;
                }
            });
        try {
            stats = myDb.preload(conf);
        } catch (RuntimeException RE) {
            fail("unexpected RE");
        }

        assertEquals(PreloadStatus.USER_HALT_REQUEST, stats.getStatus());

        myDb.close();
        close(env);
    }

    /**
     * Load the entire database with preload.
     */
    @Test
    public void testPreloadEntireDatabase()
        throws Throwable {

        /* Create a test db with one record */
        Database myDb = initEnvAndDb(false, true, false, false, false, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(0));
        data.setData(TestUtils.getTestArray(0));
        assertEquals(OperationStatus.SUCCESS, myDb.put(null, key, data));

        /* Close and reopen. */
        myDb.close();
        close(env);
        myDb = initEnvAndDb(false, true, false, false, false, null);

        /*
         * Preload the entire database.  In JE 2.0.54 this would cause a
         * NullPointerException.
         */
        PreloadConfig conf = new PreloadConfig();
        conf.setMaxBytes(100000);
        myDb.preload(conf);

        myDb.close();
        close(env);
    }

    /**
     * Test preload(N, 0) where N > cache size (throws IllArgException).
     */
    @Test
    public void testPreloadBytesExceedsCache()
        throws Throwable {

        /* Create a test db with one record */
        Database myDb =
            initEnvAndDb(false, true, false, false, false, "100000");
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(0));
        data.setData(TestUtils.getTestArray(0));
        assertEquals(OperationStatus.SUCCESS, myDb.put(null, key, data));

        /* Close and reopen. */
        myDb.close();
        close(env);
        myDb = initEnvAndDb(false, true, false, false, false, "100000");

        /* maxBytes > cache size.  Should throw IllegalArgumentException. */
        try {
            PreloadConfig conf = new PreloadConfig();
            conf.setMaxBytes(Integer.MAX_VALUE);
            myDb.preload(conf);
            fail("should have thrown IAE");
        } catch (IllegalArgumentException IAE) {
        }

        myDb.close();
        close(env);
    }

    @Test
    public void testPreloadNoLNs()
        throws Throwable {

        Database myDb = initEnvAndDb(false, true, false, true, false, null);

        final boolean embeddedLNs =
            (DbInternal.getNonNullEnvImpl(env).getMaxEmbeddedLN() >= 1);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1000; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(new byte[1]);
            assertEquals(OperationStatus.SUCCESS,
                         myDb.put(null, key, data));
        }

        /* Do not load LNs. */
        PreloadConfig conf = new PreloadConfig();
        conf.setLoadLNs(false);
        PreloadStats stats = myDb.preload(conf);
        assertEquals(0, stats.getNLNsLoaded());
        assertEquals(embeddedLNs ? 1000 : 0, stats.getNEmbeddedLNs());

        /* Load LNs. */
        conf.setLoadLNs(true);
        stats = myDb.preload(conf);
        assertEquals(embeddedLNs ? 0 : 1000, stats.getNLNsLoaded());
        assertEquals(embeddedLNs ? 1000 : 0, stats.getNEmbeddedLNs());

        myDb.close();
        close(env);
    }

    /**
     * Test preload with BIN-deltas in cache, for example, when BINs are
     * mutated by the evictor, or after a crash and recovery. The latter is
     * more likely, since preload is normally performed right after opening
     * the Environment as when the bug was discovered [#24565].
     */
    @Test
    public void testPreloadWithBINDeltasInCache()
        throws Throwable {

        /*
         * At first, with log file corruption verification code and btree
         * verification code, this test can pass.
         *
         * After I add some code to sleep some time after checking each file
         * in log file corruption code, "Failed adding new IN node=7 
         * dbIdentity=284751250... Existing IN node=7 dbIdentity=893504292"
         * begin appear. I think this is because the sleep in log file
         * verification delay the execution of BtreeVerifier and cause
         * BtreeVerifier report above issue.
         *
         * Consider this test manually operate the INList and do the preload,
         * I think above issue may happen. So now I just choose to disbale
         * BtreeVerifier for this test.
         */
        runBtreeVerifier = false;
        Database myDb = initEnvAndDb(false, false, false, true, false, null);

        final Random rnd = new Random(123);
        int nRecs = 0;

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        /* Write using random keys to enable logging deltas. */
        for (int i = 0; i < 500; i += 1) {
            key.setData(TestUtils.getTestArray(rnd.nextInt(5000)));
            data.setData(new byte[4]);

            if (OperationStatus.SUCCESS ==
                myDb.putNoOverwrite(null, key, data)) {
                nRecs += 1;
            }
        }

        /* The checkpoint will log some deltas. */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * To create deltas in cache, it is easier to mutate the BINs
         * explicitly than to truncate the log before CkptEnd and recover.
         */
        for (final IN in :
             DbInternal.getNonNullEnvImpl(env).getInMemoryINs()) {

            if (in.isBIN()) {
                final BIN bin = (BIN) in;
                bin.latch();
                if (bin.canMutateToBINDelta()) {
                    bin.mutateToBINDelta();
                }
                bin.releaseLatch();
            }
        }

//        System.out.println(
//            "nDeltas=" + env.getStats(null).getNCachedBINDeltas());

        final PreloadConfig conf = new PreloadConfig();
        conf.setLoadLNs(true);
        final PreloadStats stats = myDb.preload(conf);

        assertEquals(nRecs, stats.getNLNsLoaded() + stats.getNEmbeddedLNs());

//        System.out.println("count " + myDb.count());

        myDb.close();
        close(env);
    }

    /**
     * Tests a fix for a bug that caused eviction of previously cached nodes
     * when scanning with CacheMode.UNCHANGED after a preload. [#24629]
     */
    @Test
    public void testUseUnchangedModeAfterPreload()
        throws Throwable {

        /*
         * This test case check the equality of the total amount of JE cache
         * in use before and after some Read operations with
         * CacheMode.UNCHANGED. But the BtreeVerifier will influence the
         * cache size, such as the traversal of the NameDb, the call of
         * dbTree.getDb and so on, so BtreeVerifier is disabled for this
         * test case. 
         */
        runBtreeVerifier = false;

        /* Create a test db with one record */
        Database myDb = initEnvAndDb(false, false, false, true, false, null);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final int nRecs = 10000;
        data.setData(new byte[1000]);

        final Transaction txn = env.beginTransaction(null, null);

        for (int i = 0; i < nRecs; i += 1) {

            IntegerBinding.intToEntry(i, key);

            assertEquals(
                OperationStatus.SUCCESS,
                myDb.putNoOverwrite(txn, key, data));
        }

        txn.commit();

        /*
         * Test with and without loading LNs.
         */
        for (boolean loadLNs : new boolean[] { false, true }) {

            myDb.close();
            close(env);
            myDb = initEnvAndDb(false, false, false, true, false, null);

            myDb.preload(new PreloadConfig().setLoadLNs(loadLNs));

            long cacheSizeBeforeScan = env.getStats(null).getCacheTotalBytes();

            data.setPartial(!loadLNs);

            try (final Cursor c = myDb.openCursor(null, null)) {
                c.setCacheMode(CacheMode.UNCHANGED);
                while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
                }
            }

            long cacheSizeAfterScan = env.getStats(null).getCacheTotalBytes();

            assertEquals(
                "loadLNs " + loadLNs,
                cacheSizeBeforeScan, cacheSizeAfterScan);
        }

        myDb.close();
        close(env);
    }

    @Test
    public void testDbClose()
        throws Throwable {

        /* Set up a test db */
        Database myDb = initEnvAndDb(false, true, false, true, false, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);
        for (int i = 2500; i > 0; i--) {
            key.setData(TestUtils.getTestArray(i));
            data.setData(TestUtils.getTestArray(i));
            assertEquals(OperationStatus.SUCCESS,
                         myDb.put(txn, key, data));
        }

        /* Create a cursor, use it, then close db without closing cursor. */
        Cursor cursor = myDb.openCursor(txn, null);
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(key, data, LockMode.DEFAULT));

        try {
            myDb.close();
            fail("didn't throw IllegalStateException for unclosed cursor");
        } catch (IllegalStateException e) {
        }

        try {
            txn.commit();
            fail("didn't throw IllegalStateException for uncommitted " +
                 "transaction");
        } catch (IllegalStateException e) {
        }

        close(env);
    }

    /**
     * Checks that a DatabasePreemptedException is thrown after database handle
     * has been forcibly closed by an HA database naming operation (rename,
     * remove, truncate). [#17015]
     */
    @Test
    public void testDbPreempted() {
        doDbPreempted(false /*useTxnForDbOpen*/,
                      false /*accessDbAfterPreempted*/);

        doDbPreempted(false /*useTxnForDbOpen*/,
                      true  /*accessDbAfterPreempted*/);

        doDbPreempted(true  /*useTxnForDbOpen*/,
                      false /*accessDbAfterPreempted*/);

        doDbPreempted(true  /*useTxnForDbOpen*/,
                      true  /*accessDbAfterPreempted*/);
    }

    private void doDbPreempted(boolean useTxnForDbOpen,
                               boolean accessDbAfterPreempted) {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /* DatabasePreemptedException is thrown only if replicated. */
        if (!envImpl.isReplicated()) {
            env.close();
            return;
        }

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        DatabaseEntry data = new DatabaseEntry(new byte[1]);

        /* Create databases and write one record. */
        Database db1 = env.openDatabase(null, "db1", dbConfig);
        OperationStatus status = db1.put(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        db1.close();
        Database db2 = env.openDatabase(null, "db2", dbConfig);
        status = db2.put(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        db2.close();

        /* Open databases for reading. */
        Transaction txn = env.beginTransaction(null, null);
        dbConfig.setAllowCreate(false);
        db1 = env.openDatabase(useTxnForDbOpen ? txn : null, "db1", dbConfig);
        db2 = env.openDatabase(useTxnForDbOpen ? txn : null, "db2", dbConfig);

        Cursor c1 = db1.openCursor(txn, null);
        Cursor c2 = db2.openCursor(txn, null);

        /* Read one record in each. */
        status = c1.getSearchKey(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        status = c2.getSearchKey(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);

        /*
         * Use an importunate txn (also used by the HA replayer) to perform a
         * removeDatabase, which will steal the database handle lock and
         * invalidate the database handle.
         */
        Transaction importunateTxn = env.beginTransaction(null, null);
        DbInternal.getTxn(importunateTxn).setImportunate(true);
        env.removeDatabase(importunateTxn, "db1");
        importunateTxn.commit();

        if (useTxnForDbOpen) {
            try {
                status = c2.getSearchKey(key, data, null);
                fail();
            } catch (DatabasePreemptedException expected) {
                assertSame(db1, expected.getDatabase());
                assertEquals("db1", expected.getDatabaseName());
            }
        } else {
            status = c2.getSearchKey(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);
        }

        if (accessDbAfterPreempted) {
            try {
                status = c1.getSearchKey(key, data, null);
                fail();
            } catch (DatabasePreemptedException expected) {
                assertSame(db1, expected.getDatabase());
                assertEquals("db1", expected.getDatabaseName());
            }
            try {
                c1.dup(true);
                fail();
            } catch (DatabasePreemptedException expected) {
                assertSame(db1, expected.getDatabase());
                assertEquals("db1", expected.getDatabaseName());
            }
            try {
                status = db1.get(txn, key, data, null);
                fail();
            } catch (DatabasePreemptedException expected) {
                assertSame(db1, expected.getDatabase());
                assertEquals("db1", expected.getDatabaseName());
            }
            try {
                db1.openCursor(txn, null);
                fail();
            } catch (DatabasePreemptedException expected) {
                assertSame(db1, expected.getDatabase());
                assertEquals("db1", expected.getDatabaseName());
            }
        }

        c1.close();
        c2.close();

        if (useTxnForDbOpen || accessDbAfterPreempted) {
            try {
                txn.commit();
            } catch (DatabasePreemptedException expected) {
                assertSame(db1, expected.getDatabase());
                assertEquals("db1", expected.getDatabaseName());
            }
            txn.abort();
        } else {
            txn.commit();
        }
        db1.close();
        db2.close();

        env.close();
    }

    /**
     * Ensure that Database.close is allowed (no exception is thrown) after
     * aborting the txn that opened the Database.
     */
    @Test
    public void testDbOpenAbortWithDbClose() {
        doDbOpenAbort(true /*withDbClose*/);
    }

    /**
     * Ensure that Database.close is not required (lack of close does not cause
     * a leak) after aborting the txn that opened the Database.
     */
    @Test
    public void testDbOpenAbortNoDbClose() {
        doDbOpenAbort(false /*withDbClose*/);
    }

    /**
     * Opens (creates) a database with a txn, then aborts that txn.  Optionally
     * closes the database handle after the abort.
     */
    private void doDbOpenAbort(boolean withDbClose) {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        env = new Environment(envHome, envConfig);
        Transaction txn = env.beginTransaction(null, null);
        Database db = env.openDatabase(txn, "testDB", dbConfig);
        Cursor c = db.openCursor(txn, null);
        OperationStatus status = c.put(new DatabaseEntry(new byte[1]),
                                       new DatabaseEntry(new byte[1]));
        assertSame(OperationStatus.SUCCESS, status);

        c.close();
        txn.abort();
        if (withDbClose) {
            db.close();
        }
        env.close();
    }

    @Test
    public void testDbCloseUnopenedDb()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);
        Database myDb = new Database(env);
        try {
            myDb.close();
        } catch (DatabaseException DBE) {
            fail("shouldn't catch DatabaseException for closing unopened db");
        }
        env.close();
    }

    /**
     * Test that open cursor isn't possible on a closed database.
     */
    @Test
    public void testOpenCursor()
        throws DatabaseException {

        Database db = initEnvAndDb(true, true, false, true, false, null);
        Cursor cursor = db.openCursor(null, null);
        cursor.close();
        db.close();
        try {
            db.openCursor(null, null);
            fail("Should throw exception because database is closed");
        } catch (IllegalStateException expected) {
            close(env);
        }
    }

    /**
     * Test that openCursor throws IllegalStateException after invalidating and
     * closing the environment. [#23083]
     */
    @Test
    public void testOpenCursorAfterEnvInvalidation() {

        final Database db = initEnvAndDb(true, true, false, true, false, null);
        final Transaction txn = env.beginTransaction(null, null);
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        envImpl.invalidate(EnvironmentFailureException.unexpectedState(envImpl));
        close(env);
        try {
            db.openCursor(txn, null);
            fail("Should throw exception because env is closed");
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testBufferOverflowingPut()
        throws Throwable {

        try {

            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            //envConfig.setConfigParam("je.log.totalBufferBytes", "5000");
            envConfig.setAllowCreate(true);
            env = new Environment(envHome, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setSortedDuplicates(true);
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            Database myDb = env.openDatabase(null, "testDB", dbConfig);

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry(new byte[10000000]);
            try {
                key.setData(TestUtils.getTestArray(10));
                myDb.put(null, key, data);
            } catch (DatabaseException DE) {
                fail("unexpected DatabaseException");
            }
            myDb.close();
            env.close();
            env = null;
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Check that the handle lock is not left behind when a non-transactional
     * open of a primary DB fails while populating the secondary. [#15558]
     * @throws Exception
     */
    @Test
    public void testFailedNonTxnDbOpen()
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        DatabaseConfig priConfig = new DatabaseConfig();
        priConfig.setAllowCreate(true);
        Database priDb = env.openDatabase(null, "testDB", priConfig);

        priDb.put(null, new DatabaseEntry(new byte[1]),
                        new DatabaseEntry(new byte[2]));

        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(true);
        secConfig.setAllowPopulate(true);
        /* Use priDb as foreign key DB for ease of testing. */
        secConfig.setForeignKeyDatabase(priDb);
        secConfig.setKeyCreator(new SecondaryKeyCreator() {
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData
                    (data.getData(), data.getOffset(), data.getSize());
                return true;
            }
        });
        try {
            env.openSecondaryDatabase(null, "testDB2", priDb, secConfig);
            fail();
        } catch (DatabaseException e) {
            /* Fails because [0,0] does not exist as a key in priDb. */
            assertTrue(e.toString(),
                       e.toString().indexOf("foreign key not allowed") > 0);
        }

        priDb.close();
        env.close();
        env = null;
    }

    EnvironmentConfig getEnvironmentConfig(
        boolean dontRunEvictor,
        boolean runCleaner,
        boolean transactional,
        String memSize)
        throws IllegalArgumentException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(transactional);

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_CHECK_LEAKS, "false");

        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "6");

        envConfig.setConfigParam(
            EnvironmentConfig.NODE_DUP_TREE_MAX_ENTRIES, "6");

        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));

        if (!runCleaner) {
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_CLEANER, "false");
        }

        if (dontRunEvictor) {
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_EVICTOR, "false");


            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");

            /*
             * Don't let critical eviction run or it will interfere with the
             * preload test.
             */
            envConfig.setConfigParam(
                EnvironmentConfig.EVICTOR_CRITICAL_PERCENTAGE, "500");
        }

        if (memSize != null) {
            envConfig.setConfigParam(EnvironmentConfig.MAX_MEMORY, memSize);
        }

        if (!runBtreeVerifier) {
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        }

        if (!runINCompressor) {
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        }

        envConfig.setAllowCreate(true);
        return envConfig;
    }

    /**
     * Set up the environment and db.
     */
    private Database initEnvAndDb(
        boolean dontRunEvictor,
        boolean runCleaner,
        boolean allowDuplicates,
        boolean transactional,
        boolean deferredWrite,
        String memSize)
        throws DatabaseException {

        EnvironmentConfig envConfig = getEnvironmentConfig(dontRunEvictor,
                                                           runCleaner,
                                                           transactional,
                                                           memSize);
        env = create(envHome, envConfig);

        /* Make a db and open it. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(allowDuplicates);
        dbConfig.setAllowCreate(true);
        if (!deferredWrite) {
            dbConfig.setTransactional(transactional);
        }
        dbConfig.setDeferredWrite(deferredWrite);
        Database myDb = env.openDatabase(null, "testDB", dbConfig);
        return myDb;
    }

    /**
     * X'd out because this is expected to be used in the debugger to set
     * specific breakpoints and step through in a synchronous manner.
     */
    private Database pNOCDb;

    public void XXtestPutNoOverwriteConcurrently()
        throws Throwable {

        pNOCDb = initEnvAndDb(true, true, true, true, false, null);
        JUnitThread tester1 =
            new JUnitThread("testNonBlocking1") {
                @Override
                public void testBody() {
                    try {
                        Transaction txn1 = env.beginTransaction(null, null);
                        DatabaseEntry key = new DatabaseEntry();
                        DatabaseEntry data = new DatabaseEntry();
                        key.setData(TestUtils.getTestArray(1));
                        data.setData(TestUtils.getTestArray(1));
                        OperationStatus status =
                            pNOCDb.putNoOverwrite(txn1, key, data);
                        txn1.commit();
                        System.out.println("thread1: " + status);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testNonBlocking2") {
                @Override
                public void testBody() {
                    try {
                        Transaction txn2 = env.beginTransaction(null, null);
                        DatabaseEntry key = new DatabaseEntry();
                        DatabaseEntry data = new DatabaseEntry();
                        key.setData(TestUtils.getTestArray(1));
                        data.setData(TestUtils.getTestArray(2));
                        OperationStatus status =
                            pNOCDb.putNoOverwrite(txn2, key, data);
                        txn2.commit();
                        System.out.println("thread2: " + status);
                    } catch (DatabaseException DBE) {
                        DBE.printStackTrace();
                        fail("caught DatabaseException " + DBE);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester1.finishTest();
        tester2.finishTest();
    }

    /**
     * Ensure that close/abort methods can be called without an exception when
     * the Environment is invalid, if they were closed earlier. [#21264]
     */
    @Test
    public void testCloseWithInvalidEnv() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = create(envHome, envConfig);

        Transaction txn = env.beginTransaction(null, null);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db = env.openDatabase(txn, "testDB", dbConfig);

        Cursor cursor = db.openCursor(txn, null);

        cursor.close();
        db.close();
        txn.commit();

        /* Invalidate env. */
        EnvironmentFailureException.unexpectedState(env.getNonNullEnvImpl());

        try {
            cursor.close();
            db.close();
            txn.abort();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail("Close/abort with invalid/closed env shouldn't fail.");
        }

        /*
         * abnormalClose is used simply to avoid rep (dual) checks that don't
         * work with an invalidated environment.
         */
        abnormalClose(env);
    }

    class DeleteIt implements Runnable {
        int key;
        Database db;
        Environment env;
        OperationStatus retstat = null;
        int waitEventPre;
        int waitEventPost;
        long waittime;

        DeleteIt(int key,
                 Environment env,
                 Database db) {
            this.key = key;
            this.db = db;
            this.env = env;
         }

        public void run() {
            while (!doWork());
        }

        private boolean doWork() {
            boolean done = false;
            DatabaseEntry keye = new DatabaseEntry();
            keye.setData(TestUtils.getTestArray(key));
            while (!done) {
                Transaction xact = env.beginTransaction(null, null);
                try {
                    retstat = db.delete(xact, keye);
                } catch (LockConflictException e) {
                    fail("deadlock occured but not expected.");
                }

                if (xact != null) {
                    xact.commit();
                    done = true;
                }
            }
            return done;
        }

        public OperationStatus getResult() {
            return retstat;
        }
    }
}
