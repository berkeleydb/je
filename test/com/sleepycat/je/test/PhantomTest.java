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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Tests phantom prevention (range locking) added in SR [#10477].
 *
 * <p>We test that with a serializable txn, range locking will prevent phantoms
 * from appearing.  We also test that phantoms *do* appear for non-serializable
 * isolation levels.  These include read-uncommitted, read-committed and
 * repeatable-read now.</p>
 *
 * <p>Test method names have the suffix _Sucess or _NotFound depending on
 * whether they're testing a read operation with a SUCCESS or NOTFOUND outcome.
 * If they're testing duplicates, the _Dup suffix is also added.  Finally, a
 * suffix is added for the isolation level at run time.</p>
 *
 * <p>All tests are for the case where the reader txn locks a range and then
 * the writer txn tries to insert into the locked range.  The reverse (where
 * the writer inserts first) works without range locking because the reader
 * will block on the inserted key, so we don't test that here.</p>
 *
 * <p>We test all read operations with and without duplicates (with duplicates
 * the test name has _Dup appended) except for the following cases which are
 * meaningless without duplicates because get{Next,Prev}Dup always return
 * NOTFOUND when duplicates are not configured:
 * testGetNextDup_Success, testGetNextDup_NotFound,
 * testGetPrevDup_Success, testGetPrevDup_NotFound.</p>
 */
@RunWith(Parameterized.class)
public class PhantomTest extends DualTestCase {

    private static final TransactionConfig READ_UNCOMMITTED_CONFIG
                                           = new TransactionConfig();
    private static final TransactionConfig READ_COMMITTED_CONFIG
                                           = new TransactionConfig();
    private static final TransactionConfig REPEATABLE_READ_CONFIG
                                           = new TransactionConfig();
    private static final TransactionConfig SERIALIZABLE_CONFIG
                                           = new TransactionConfig();
    static {
        READ_UNCOMMITTED_CONFIG.setReadUncommitted(true);
        READ_COMMITTED_CONFIG.setReadCommitted(true);
        SERIALIZABLE_CONFIG.setSerializableIsolation(true);
    }
    private static final TransactionConfig[] TXN_CONFIGS = {
        READ_UNCOMMITTED_CONFIG,
        READ_COMMITTED_CONFIG,
        REPEATABLE_READ_CONFIG,
        SERIALIZABLE_CONFIG,
    };

    private static final String DB_NAME = "PhantomTest";

    private static final int MAX_INSERT_MILLIS = 5000;

    private boolean disableBtreeVerifier = false;

    private File envHome;
    private Environment env;
    private Database db;
    private final TransactionConfig txnConfig;
    private JUnitThread writerThread;
    private final boolean txnSerializable;
    private boolean dups;
    private boolean insertFinished;

    @Parameters
    public static List<Object[]> genParams() {
        List<Object[]> list = new ArrayList<Object[]>();

        for (TransactionConfig txnConfig : TXN_CONFIGS)
            list.add(new Object[]{txnConfig});

        return list;
     }

    public PhantomTest(TransactionConfig txnConfig) {
        envHome = SharedTestUtils.getTestDir();
        this.txnConfig = txnConfig;
        txnSerializable = (txnConfig == SERIALIZABLE_CONFIG);
        String txnType;
        if (txnConfig == SERIALIZABLE_CONFIG) {
            txnType = "-Serializable";
        } else if (txnConfig == REPEATABLE_READ_CONFIG) {
            txnType = "-RepeatableRead";
        } else if (txnConfig == READ_COMMITTED_CONFIG) {
            txnType = "-ReadCommitted";
        } else if (txnConfig == READ_UNCOMMITTED_CONFIG) {
            txnType = "-ReadUncommitted";
        } else {
            throw new IllegalStateException();
        }
        customName = txnType;
    }

    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        envHome = null;
        env = null;
        db = null;

        if (writerThread != null) {
            writerThread.shutdown();
            writerThread = null;
        }
    }

    /**
     * Opens the environment and database.
     */
    private void openEnv(boolean dups)
        throws DatabaseException {

        openEnv(dups, null);
    }

    /**
     * Opens the environment and database.
     */
    private void openEnv(boolean dups, EnvironmentConfig envConfig)
        throws DatabaseException {

        this.dups = dups;
        if (envConfig == null) {
            envConfig = TestUtils.initEnvConfig();
            /* Control over isolation level is required by this test. */
            TestUtils.clearIsolationLevel(envConfig);
        }

        /* Disable the daemons so the don't interfere with stats. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");

        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        if (disableBtreeVerifier) {
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        }
        env = create(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(dups);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    /**
     * Closes the environment and database.
     */
    private void closeEnv()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            close(env);
            env = null;
        }
    }

    @Test
    public void testGetSearchKey_Success()
        throws DatabaseException {

        openEnv(false);

        /* Insert key 2. */
        insert(2);

        /* getSearchKey returns key 2. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));

        /* Insertions are never blocked. */
        try {
            insert(1);
            insert(3);
        } catch (LockConflictException e) {
            fail();
        }

        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        closeEnv();
    }

    @Test
    public void testGetSearchKey_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);

        /* Insert dups. */
        insert(1, 2);
        insert(1, 3);

        /* getSearchKey returns key {1,2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 2));

        /* Insertions after {1, 2} are never blocked. */
        try {
            insert(1, 4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,1} in a writer thread. */
        startInsert(1, 1);

        /*
         * If serializable, getSearchKey should return {1,2} again, otherwise
         * getSearchKey should see {1,1}.
         */
        if (txnSerializable) {
            assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 2));
        } else {
            assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 1));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKey returns {1,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 1));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchKey_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);

        /* Insert key 1. */
        insert(1);

        /* getSearchKey for key 2 returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.NOTFOUND, searchKey(cursor, 2));

        /* Insertions before 2 are never blocked. */
        try {
            insert(0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getSearchKey should return NOTFOUND again;
         * otherwise getSearchKey should see key 2.
         */
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, searchKey(cursor, 2));
        } else {
            assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKey returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchKey_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);

        /* Insert dups. */
        insert(2, 1);
        insert(2, 2);

        /* getSearchKey for {1,1} returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.NOTFOUND, searchKey(cursor, 1, 1));

        /* Insertions after {2,2} are never blocked. */
        try {
            insert(2, 3);
            insert(3, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,1} in a writer thread. */
        startInsert(1, 1);

        /*
         * If serializable, getSearchKey should return NOTFOUND again;
         * otherwise getSearchKey should see {1,1}.
         */
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, searchKey(cursor, 1, 1));
        } else {
            assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 1));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKey returns {1,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 1));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchBoth_Success()
        throws DatabaseException {

        doTestGetSearchBoth_Success(false /*useRangeSearch*/);
    }

    /**
     * In a non-duplicates DB, getSearchBoth and getSearchBothRange are
     * equivalent.
     */
    private void doTestGetSearchBoth_Success(boolean useRangeSearch)
        throws DatabaseException {

        openEnv(false);

        /* Insert key 2. */
        insert(2);

        /* getSearchBoth[Range] returns {2,0}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS,
                     searchBoth(cursor, 2, 0, useRangeSearch));

        /* Insertions are never blocked. */
        try {
            insert(1);
            insert(3);
        } catch (LockConflictException e) {
            fail();
        }

        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        closeEnv();
    }

    @Test
    public void testGetSearchBoth_Success_Dup()
        throws DatabaseException {

        openEnv(true);

        /* Insert dups. */
        insert(1, 1);
        insert(1, 3);

        /* getSearchBoth returns key {1,3}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));

        /* Insertions are never blocked. */
        try {
            insert(0, 0);
            insert(1, 0);
            insert(1, 2);
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        closeEnv();
    }

    @Test
    public void testGetSearchBoth_NotFound()
        throws DatabaseException, InterruptedException {

        doTestGetSearchBoth_NotFound(false /*useRangeSearch*/);
    }

    /**
     * In a non-duplicates DB, getSearchBoth and getSearchBothRange are
     * equivalent.
     */
    private void doTestGetSearchBoth_NotFound(boolean useRangeSearch)
        throws DatabaseException, InterruptedException {

        openEnv(false);

        /* Insert key 1. */
        insert(1);

        /* getSearchBoth for key 2 returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.NOTFOUND,
                     searchBoth(cursor, 2, useRangeSearch));

        /* Insertions before 2 are never blocked. */
        try {
            insert(0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getSearchBoth should return NOTFOUND again;
         * otherwise getSearchBoth should see key 2.
         */
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND,
                         searchBoth(cursor, 2, useRangeSearch));
        } else {
            assertEquals(OperationStatus.SUCCESS,
                         searchBoth(cursor, 2, useRangeSearch));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchBoth returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS,
                     searchBoth(cursor, 2, useRangeSearch));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchBoth_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);

        /* Insert dups. */
        insert(1, 1);
        insert(1, 3);

        /* getSearchBoth for {1,2} returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.NOTFOUND, searchBoth(cursor, 1, 2));

        /* Insertions before {1,2} or after {1,3} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,2} in a writer thread. */
        startInsert(1, 2);

        /*
         * If serializable, getSearchBoth should return NOTFOUND again;
         * otherwise getSearchBoth should see {1,2}.
         */
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, searchBoth(cursor, 1, 2));
        } else {
            assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchBoth returns {1,2}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchKeyRange_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1 and 3. */
        insert(1);
        insert(3);

        /* getSearchKeyRange for key 2 returns key 3. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));

        /* Insertions before 2 and after 3 are never blocked. */
        try {
            insert(0);
            insert(4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getSearchKeyRange should return key 3 again;
         * otherwise getSearchKeyRange should see key 2.
         */
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(3, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKeyRange returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchKeyRange_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);
        insert(3, 2);
        insert(3, 3);

        /* getSearchKeyRange for key 2 returns {3,2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));
        assertEquals(OperationStatus.SUCCESS, status);

        /* Insertions before 2 and after {3,3} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(3, 4);
            insert(4, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {3,1} in a writer thread. */
        startInsert(3, 1);

        /*
         * If serializable, getSearchKeyRange should return {3,2} again;
         * otherwise getSearchKeyRange should see {3,1}.
         */
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKeyRange returns {3,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchKeyRange_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1. */
        insert(1);

        /* getSearchKeyRange for key 2 returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before 2 are never blocked. */
        try {
            insert(0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 3 in a writer thread. */
        startInsert(3);

        /*
         * If serializable, getSearchKeyRange should return NOTFOUND again;
         * otherwise getSearchKeyRange should see key 3.
         */
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(3, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKeyRange returns key 3. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchKeyRange_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);

        /* getSearchKeyRange for key 2 returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before 2 are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {3,1} in a writer thread. */
        startInsert(3, 1);

        /*
         * If serializable, getSearchKeyRange should return NOTFOUND again;
         * otherwise getSearchKeyRange should see {3,1}.
         */
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchKeyRange returns {3,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        status = cursor.getSearchKeyRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    /*
     * A testGetSearchBothRange_Success test case is not possible because it is
     * not possible to insert a duplicate when only one LN for the key already
     * exists, without locking the existing LN.  Therefore, the insert thread
     * will deadlock with the reader thread, which has the existing LN locked.
     * This is a testing anomoly, not a bug.
     */

    @Test
    public void testGetSearchBothRange_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);
        insert(3, 2);
        insert(3, 3);

        /* getSearchBothRange for {3, 0} returns {3,2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(3, key);
        IntegerBinding.intToEntry(0, data);
        status = cursor.getSearchBothRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));

        /* Insertions before {1,1} and after {3,2} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(3, 4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {3,1} in a writer thread. */
        startInsert(3, 1);

        /*
         * If serializable, getSearchBothRange should return {3,2} again;
         * otherwise getSearchBothRange should see {3,1}.
         */
        IntegerBinding.intToEntry(3, key);
        IntegerBinding.intToEntry(0, data);
        status = cursor.getSearchBothRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchBothRange returns {3,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(3, key);
        IntegerBinding.intToEntry(0, data);
        status = cursor.getSearchBothRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetSearchBothRange_NotFound()
        throws DatabaseException, InterruptedException {

        doTestGetSearchBoth_NotFound(true /*useRangeSearch*/);
    }

    @Test
    public void testGetSearchBothRange_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(3, 0);
        insert(3, 1);

        /* getSearchBothRange for {3, 2} returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(3, key);
        IntegerBinding.intToEntry(2, data);
        status = cursor.getSearchBothRange(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before {3,0} are never blocked. */
        try {
            insert(3, -1);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {3,3} in a writer thread. */
        startInsert(3, 3);

        /*
         * If serializable, getSearchBothRange should return NOTFOUND again;
         * otherwise getSearchBothRange should see {3,3}.
         */
        IntegerBinding.intToEntry(3, key);
        IntegerBinding.intToEntry(2, data);
        status = cursor.getSearchBothRange(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(3, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getSearchBothRange returns {3,3}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(3, key);
        IntegerBinding.intToEntry(2, data);
        status = cursor.getSearchBothRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(3, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetFirst_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 2. */
        insert(2);

        /* getFirst returns key 2. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));

        /* Insertions after 2 are never blocked. */
        try {
            insert(3);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 1 in a writer thread. */
        startInsert(1);

        /*
         * If serializable, getFirst should return key 2 again; otherwise
         * getFirst should see key 1.
         */
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(2, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getFirst returns key 1. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetFirst_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 2);
        insert(1, 3);

        /* getFirst returns {1,2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));

        /* Insertions after {1,3} are never blocked. */
        try {
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,1} in a writer thread. */
        startInsert(1, 1);

        /*
         * If serializable, getFirst should return {1,2} again; otherwise
         * getFirst should see {1,1}.
         */
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getFirst returns {1,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetFirst_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* getFirst returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insert key 1 in a writer thread. */
        startInsert(1);

        /*
         * If serializable, getFirst should return NOTFOUND again; otherwise
         * getFirst should see key 1.
         */
        status = cursor.getFirst(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getFirst returns key 1. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetFirst_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* getFirst returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insert {1,1} in a writer thread. */
        startInsert(1, 1);

        /*
         * If serializable, getFirst should return NOTFOUND again; otherwise
         * getFirst should see {1,1}.
         */
        status = cursor.getFirst(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getFirst returns {1,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getFirst(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetLast_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1. */
        insert(1);

        /* getLast returns key 1. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));

        /* Insertions before current position are never blocked. */
        try {
            insert(0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getLast should return key 1 again; otherwise
         * getLast should see key 2.
         */
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getLast returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetLast_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 0);
        insert(1, 2);

        /* getLast returns {1,2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));

        /* Insertions before current position are never blocked. */
        try {
            insert(1, 1);
            insert(0, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,3} in a writer thread. */
        startInsert(1, 3);

        /*
         * If serializable, getLast should return {1,2} again; otherwise
         * getLast should see {1,3}.
         */
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(3, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getLast returns {1,3}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(3, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetLast_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* getLast returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insert key 1 in a writer thread. */
        startInsert(1);

        /*
         * If serializable, getLast should return NOTFOUND again; otherwise
         * getLast should see key 1.
         */
        status = cursor.getLast(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getLast returns key 1. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetLast_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* getLast returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insert {1,1} in a writer thread. */
        startInsert(1, 1);

        /*
         * If serializable, getLast should return NOTFOUND again; otherwise
         * getLast should see {1,1}.
         */
        status = cursor.getLast(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getLast returns {1,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        status = cursor.getLast(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNext_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1 and 3. */
        insert(1);
        insert(3);

        /* getNext returns key 3. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));

        /* Insertions before 1 and after 3 are never blocked. */
        try {
            insert(0);
            insert(4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getNext should return key 3 again; otherwise
         * getNext should see key 2.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(3, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNext returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNext_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 3);

        /* getNext returns {1,3}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(3, IntegerBinding.entryToInt(data));

        /* Insertions before {1,1} and after {1,3} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,2} in a writer thread. */
        startInsert(1, 2);

        /*
         * If serializable, getNext should return {1,3} again; otherwise
         * getNext should see {1,2}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(3, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNext returns {1,2}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNext_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1. */
        insert(1);

        /* getNext returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before 1 are never blocked. */
        try {
            insert(0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getNext should return NOTFOUND again; otherwise
         * getNext should see key 2.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNext(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNext returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNext_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);

        /* getNext returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before {1,1} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,3} in a writer thread. */
        startInsert(1, 3);

        /*
         * If serializable, getNext should return NOTFOUND again; otherwise
         * getNext should see {1,3}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        status = cursor.getNext(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(3, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNext returns {1,3}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        status = cursor.getNext(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(3, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNextDup_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 3);

        /* getNextDup returns {1,3}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(3, IntegerBinding.entryToInt(data));

        /* Insertions before {1,1} and after {1,3} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,2} in a writer thread. */
        startInsert(1, 2);

        /*
         * If serializable, getNextDup should return {1,3} again; otherwise
         * getNextDup should see {1,2}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(3, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNextDup returns {1,2}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNextDup_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);
        insert(2, 1);
        insert(2, 2);

        /* getNextDup returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        status = cursor.getNextDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before {1,1} and after {2,2} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(2, 3);
            insert(3, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,3} in a writer thread. */
        startInsert(1, 3);

        /*
         * If serializable, getNextDup should return NOTFOUND again; otherwise
         * getNextDup should see {1,3}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        status = cursor.getNextDup(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(3, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNextDup returns {1,3}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 2));
        status = cursor.getNextDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(3, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNextNoDup_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1 and 3. */
        insert(1);
        insert(3);

        /* getNextNoDup returns key 3. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));

        /* Insertions before 1 and after 3 are never blocked. */
        try {
            insert(0);
            insert(4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getNextNoDup should return key 3 again; otherwise
         * getNextNoDup should see key 2.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(3, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNextNoDup returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNextNoDup_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);
        insert(3, 1);
        insert(3, 2);

        /* getNextNoDup returns {3,1}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(3, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));

        /* Insertions before {1,1} and after {3,2} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(3, 3);
            insert(4, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {2,1} in a writer thread. */
        startInsert(2, 1);

        /*
         * If serializable, getNextNoDup should return {3,1} again; otherwise
         * getNextNoDup should see {2,1}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(3, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNextNoDup returns {2,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNextNoDup_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1. */
        insert(1);

        /* getNextNoDup returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before 1 are never blocked. */
        try {
            insert(0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getNextNoDup should return NOTFOUND again;
         * otherwise getNextNoDup should see key 2.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNextNoDup(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNextNoDup returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetNextNoDup_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 2);

        /* getNextNoDup returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions before {1,1} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {2,1} in a writer thread. */
        startInsert(2, 1);

        /*
         * If serializable, getNextNoDup should return NOTFOUND again;
         * otherwise getNextNoDup should see {2,1}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextNoDup(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(2, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getNextNoDup returns {2,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        status = cursor.getNextNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrev_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1 and 3. */
        insert(1);
        insert(3);

        /* getPrev returns key 1. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 3));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));

        /* Insertions before 1 and after 3 are never blocked. */
        try {
            insert(0);
            insert(4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getPrev should return key 1 again; otherwise
         * getPrev should see key 2.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 3));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrev returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 3));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrev_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 3);

        /* getPrev returns {1,1}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));

        /* Insertions before {1,1} and after {1,3} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,2} in a writer thread. */
        startInsert(1, 2);

        /*
         * If serializable, getPrev should return {1,1} again; otherwise
         * getPrev should see {1,2}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrev returns {1,2}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrev_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 2. */
        insert(2);

        /* getPrev returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions after 2 are never blocked. */
        try {
            insert(3);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 1 in a writer thread. */
        startInsert(1);

        /*
         * If serializable, getPrev should return NOTFOUND again; otherwise
         * getPrev should see key 1.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        status = cursor.getPrev(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrev returns key 1. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrev_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(2, 2);
        insert(2, 3);

        /* getPrev returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions after {2,3} are never blocked. */
        try {
            insert(2, 4);
            insert(3, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {2,1} in a writer thread. */
        startInsert(2, 1);

        /*
         * If serializable, getPrev should return NOTFOUND again; otherwise
         * getPrev should see {2,1}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrev(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(2, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrev returns {2,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrev(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrevDup_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(1, 3);

        /* getPrevDup returns {1,1}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));
        status = cursor.getPrevDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));

        /* Insertions before {1,1} and after {1,3} are never blocked. */
        try {
            insert(1, 0);
            insert(0, 0);
            insert(1, 4);
            insert(2, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,2} in a writer thread. */
        startInsert(1, 2);

        /*
         * If serializable, getPrevDup should return {1,1} again; otherwise
         * getPrevDup should see {1,2}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));
        status = cursor.getPrevDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrevDup returns {1,2}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 3));
        status = cursor.getPrevDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrevDup_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(2, 2);
        insert(2, 3);

        /* getPrevDup returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrevDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions after {2,3} are never blocked. */
        try {
            insert(2, 4);
            insert(3, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {2,1} in a writer thread. */
        startInsert(2, 1);

        /*
         * If serializable, getPrevDup should return NOTFOUND again; otherwise
         * getPrevDup should see {2,1}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrevDup(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(2, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrevDup returns {2,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrevDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrevNoDup_Success()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 1 and 3. */
        insert(1);
        insert(3);

        /* getPrevNoDup returns key 1. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 3));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));

        /* Insertions before 1 and after 3 are never blocked. */
        try {
            insert(0);
            insert(4);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 2 in a writer thread. */
        startInsert(2);

        /*
         * If serializable, getPrevNoDup should return key 1 again; otherwise
         * getPrevNoDup should see key 2.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 3));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrevNoDup returns key 2. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 3));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrevNoDup_Success_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 0);
        insert(1, 2);
        insert(3, 1);
        insert(3, 2);

        /* getPrevNoDup returns {1,2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 3, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));

        /* Insertions before {1,2} and after {3,2} are never blocked. */
        try {
            insert(1, 1);
            insert(0, 0);
            insert(3, 3);
            insert(4, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {2,1} in a writer thread. */
        startInsert(2, 1);

        /*
         * If serializable, getPrevNoDup should return {1,2} again; otherwise
         * getPrevNoDup should see {2,1}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 3, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        if (txnSerializable) {
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(2, IntegerBinding.entryToInt(data));
        } else {
            assertEquals(2, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrevNoDup returns {2,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 3, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrevNoDup_NotFound()
        throws DatabaseException, InterruptedException {

        openEnv(false);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 2. */
        insert(2);

        /* getPrevNoDup returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions after 2 are never blocked. */
        try {
            insert(3);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert key 1 in a writer thread. */
        startInsert(1);

        /*
         * If serializable, getPrevNoDup should return NOTFOUND again;
         * otherwise getPrevNoDup should see key 1.
         */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        status = cursor.getPrevNoDup(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrevNoDup returns key 1. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testGetPrevNoDup_NotFound_Dup()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(2, 1);
        insert(2, 2);

        /* getPrevNoDup returns NOTFOUND. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        /* Insertions after {2,2} are never blocked. */
        try {
            insert(2, 3);
            insert(3, 0);
        } catch (LockConflictException e) {
            fail();
        }

        /* Insert {1,1} in a writer thread. */
        startInsert(1, 1);

        /*
         * If serializable, getPrevNoDup should return NOTFOUND again;
         * otherwise getPrevNoDup should see {1,1}.
         */
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrevNoDup(key, data, null);
        if (txnSerializable) {
            assertEquals(OperationStatus.NOTFOUND, status);
        } else {
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(1, IntegerBinding.entryToInt(key));
            assertEquals(1, IntegerBinding.entryToInt(data));
        }

        /* Close reader to allow writer to finish. */
        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        waitForInsert();

        /* getPrevNoDup returns {1,1}. */
        readerTxn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(readerTxn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 2));
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(1, IntegerBinding.entryToInt(key));
        assertEquals(1, IntegerBinding.entryToInt(data));
        cursor.close();
        readerTxn.commit();

        closeEnv();
    }

    @Test
    public void testIllegalTransactionConfig()
        throws DatabaseException {

        openEnv(false);
        TransactionConfig config = new TransactionConfig();
        config.setSerializableIsolation(true);
        config.setReadUncommitted(true);
        try {
            Transaction txn = env.beginTransaction(null, config);
            txn.abort();
            fail();
        } catch (IllegalArgumentException expected) {
        }
        closeEnv();
    }

    /*
     * In other tests we test TransactionConfig.setReadUncommitted and
     * TransactionConfig.setSerializableIsolation to make sure they result in
     * expected non-serializable or serializable behavior.  Below we check
     * EnvironmentConfig.setSerializableIsolation,
     * CursorConfig.setSerializableIsolation, CursorConfig.setReadUncommitted
     * and LockMode.READ_UNCOMMITTED, although for a single test case only.
     */

    @Test
    public void testEnvironmentConfig()
        throws DatabaseException {

        EnvironmentConfig config = TestUtils.initEnvConfig();
        /* Control over isolation level is required by this test. */
        TestUtils.clearIsolationLevel(config);
        checkSerializable(false, config, null, null);

        config.setTxnSerializableIsolation(true);
        checkSerializable(true, config, null, null);
    }

    @Test
    public void testCursorConfig()
        throws DatabaseException {

        CursorConfig config = new CursorConfig();
        checkSerializable(false, null, config, null);

        config.setReadUncommitted(true);
        checkSerializable(false, null, config, null);
    }

    @Test
    public void testReadUncommittedLockMode()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        /* Control over isolation level is required by this test. */
        TestUtils.clearIsolationLevel(envConfig);
        envConfig.setTxnSerializableIsolation(true);

        checkSerializable(false, envConfig, null, LockMode.READ_UNCOMMITTED);
    }

    private void checkSerializable(boolean expectSerializable,
                                   EnvironmentConfig envConfig,
                                   CursorConfig cursorConfig,
                                   LockMode lockMode)
        throws DatabaseException {

        openEnv(false, envConfig);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert key 2. */
        insert(2);

        /* getFirst returns key 2. */
        Transaction readerTxn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(readerTxn, cursorConfig);
        status = cursor.getFirst(key, data, lockMode);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));

        /* Should deadlock iff serializable. */
        try {
            insert(1);
            assertTrue(!expectSerializable);
        } catch (LockConflictException e) {
            assertTrue(expectSerializable);
        }

        cursor.close();
        readerTxn.commit();

        /* This method is called multiple times so remove the database. */
        db.close();
        db = null;
        env.removeDatabase(null, DB_NAME);

        closeEnv();
    }

    /**
     * Tests that with a single degree 3 txn we don't obtain the extra lock
     * during insert.
     */
    @Test
    public void testSingleDegree3TxnOptimization()
        throws DatabaseException {

        disableBtreeVerifier = true;

        try {
            openEnv(false);
    
            /* Insert key 2. */
            insert(2);
    
            StatsConfig clearStats = new StatsConfig();
            clearStats.setClear(true);
    
            /* Clear before inserting. */
            EnvironmentStats stats = env.getStats(clearStats);
    
            /* Insert key 1, which would lock key 2 while inserting. */
            insert(1);
    
            /* Expect a single lock was requested. */
            stats = env.getStats(clearStats);
            assertEquals(1, stats.getNRequests());
    
            closeEnv();
        } finally {
            disableBtreeVerifier = false;
        }
    }

    /**
     * Tests a particular getSearchBothRange bug that has come up in several
     * contexts.  This test is probably redundant with GetSearchBothTest but
     * I've left it here for good measure.
     */
    @Test
    public void testSingleDatumBug()
        throws DatabaseException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        insert(1, 1);
        insert(2, 2);

        /* getSearchBothRange for {2, 1} returns {2, 2}. */
        Transaction readerTxn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(readerTxn, null);
        IntegerBinding.intToEntry(2, key);
        IntegerBinding.intToEntry(1, data);
        status = cursor.getSearchBothRange(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);
        assertEquals(2, IntegerBinding.entryToInt(key));
        assertEquals(2, IntegerBinding.entryToInt(data));

        /* If serializable, inserting in the locked range should deadlock. */
        try {
            insert(1, 2);
            if (txnSerializable) {
                fail();
            }
        } catch (LockConflictException e) {
            if (!txnSerializable) {
                fail();
            }
        }

        cursor.close();
        readerTxn.commit(Durability.COMMIT_NO_SYNC);
        closeEnv();
    }

    /**
     * Tests that searchKey returns SUCCESS when it must skip over a deleted
     * duplicate.  This did not work at one point and was causing warnings
     * (Cursor Not Initialized) in duplicate.conf testing.
     */
    @Test
    public void testSearchKeySkipDeletedDup()
        throws DatabaseException {

        openEnv(true);

        /* Insert {1,1} and {1,2}. */
        insert(1, 1);
        insert(1, 2);

        /* Delete {1,1}. */
        Transaction txn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(txn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 1, 1));
        OperationStatus status = cursor.delete();
        assertEquals(OperationStatus.SUCCESS, status);

        /* Search for key 1 -- should not return NOTFOUND. */
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 2));

        cursor.close();
        txn.commit(Durability.COMMIT_NO_SYNC);
        closeEnv();
    }

    /**
     * Tests that getNextDup returns NOTFOUND when it skips over a deleted
     * duplicate for the following main key.  [#19026]
     */
    @Test
    public void testNextAfterDupDeleteBug()
        throws DatabaseException, InterruptedException {

        openEnv(true);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Insert dups. */
        insert(1, 1);
        insert(2, 1);
        insert(2, 2);

        /* Delete {2,1}. */
        Transaction txn = env.beginTransaction(null, txnConfig);
        Cursor cursor = db.openCursor(txn, null);
        assertEquals(OperationStatus.SUCCESS, searchBoth(cursor, 2, 1));
        assertEquals(OperationStatus.SUCCESS, cursor.delete());
        cursor.close();
        txn.commit(Durability.COMMIT_NO_SYNC);

        /*
         * When positioned on {1,1}, getNextDup should always return NOTFOUND.
         * A bug (fixed in [#19026]) caused the cursor to move to {2,2} and
         * return SUCCESS.  This only occurred with serializable isolation, and
         * when the deleted {2,1} record had not been compressed. The
         * underlying cause is that CursorImpl.getNextWithKeyChangeStatus was
         * not indicating a key change when skipping over the first deleted
         * duplicate ({2,1} in this case) in the duplicate set.
         */
        txn = env.beginTransaction(null, txnConfig);
        cursor = db.openCursor(txn, null);
        assertEquals(OperationStatus.SUCCESS, searchKey(cursor, 1, 1));
        status = cursor.getNextDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);
        cursor.close();
        txn.commit(Durability.COMMIT_NO_SYNC);

        closeEnv();
    }

    /**
     * Performs getSearchKey on the given key, expects data to be zero.
     */
    private OperationStatus searchKey(Cursor cursor, int keyVal)
        throws DatabaseException {

        return searchKey(cursor, keyVal, 0);
    }

    /**
     * Performs getSearchKey on the given key, expects given data value.
     */
    private OperationStatus searchKey(Cursor cursor, int keyVal, int dataVal)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(keyVal, key);
        OperationStatus status = cursor.getSearchKey(key, data, null);
        if (status == OperationStatus.SUCCESS) {
            assertEquals(keyVal, IntegerBinding.entryToInt(key));
            assertEquals(dataVal, IntegerBinding.entryToInt(data));
        }
        return status;
    }

    /**
     * Performs getSearchBoth on the given key and zero data.
     */
    private OperationStatus searchBoth(Cursor cursor, int keyVal)
        throws DatabaseException {

        return searchBoth(cursor, keyVal, 0, false);
    }

    /**
     * Performs getSearchBoth on the given key and zero data.
     *
     * getSearchBoth and getSearchBothRange are equivalent for a non-dup DB, so
     * we allowing testing either.
     */
    private OperationStatus searchBoth(Cursor cursor,
                                       int keyVal,
                                       boolean useRangeSearch)
        throws DatabaseException {

        return searchBoth(cursor, keyVal, 0, useRangeSearch);
    }

    /**
     * Performs getSearchBoth on the given key and data.
     */
    private OperationStatus searchBoth(Cursor cursor, int keyVal, int dataVal)
        throws DatabaseException {

        return searchBoth(cursor, keyVal, dataVal, false);
    }

    /**
     * Performs getSearchBoth on the given key and data.
     *
     * getSearchBoth and getSearchBothRange are equivalent for a non-dup DB, so
     * we allowing testing either.
     */
    private OperationStatus searchBoth(Cursor cursor,
                                       int keyVal,
                                       int dataVal,
                                       boolean useRangeSearch)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(keyVal, key);
        IntegerBinding.intToEntry(dataVal, data);
        OperationStatus status;
        if (useRangeSearch) {
            status = cursor.getSearchBothRange(key, data, null);
        } else {
            status = cursor.getSearchBoth(key, data, null);
        }
        if (status == OperationStatus.SUCCESS) {
            assertEquals(keyVal, IntegerBinding.entryToInt(key));
            assertEquals(dataVal, IntegerBinding.entryToInt(data));
        }
        return status;
    }

    /**
     * Inserts the given key in a new transaction and commits it.
     */
    private void insert(int keyVal)
        throws DatabaseException {

        insert(keyVal, 0);
    }

    /**
     * Inserts the given key and data in a new transaction and commits it.
     */
    private void insert(int keyVal, int dataVal)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(keyVal, key);
        IntegerBinding.intToEntry(dataVal, data);
        OperationStatus status;
        Transaction writerTxn = env.beginTransaction(null, txnConfig);
        try {
            if (dups) {
                status = db.putNoDupData(writerTxn, key, data);
            } else {
                status = db.putNoOverwrite(writerTxn, key, data);
            }
        } catch (LockConflictException e) {
            writerTxn.abort();
            throw e;
        }
        assertEquals(OperationStatus.SUCCESS, status);
        writerTxn.commit(Durability.COMMIT_NO_SYNC);
    }

    /**
     * Starts writer thread and waits for it to start the insert.
     */
    private void startInsert(final int keyVal)
        throws DatabaseException, InterruptedException {

        startInsert(keyVal, 0);
    }

    /**
     * Starts writer thread and waits for it to start the insert.
     */
    private void startInsert(final int keyVal, final int dataVal)
        throws DatabaseException, InterruptedException {

        EnvironmentStats origStats = env.getStats(null);
        insertFinished = false;

        writerThread = new JUnitThread("Writer") {
            public void testBody()
                throws DatabaseException {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                OperationStatus status;
                IntegerBinding.intToEntry(keyVal, key);
                IntegerBinding.intToEntry(dataVal, data);
                Transaction writerTxn = env.beginTransaction(null, txnConfig);
                if (dups) {
                    status = db.putNoDupData(writerTxn, key, data);
                } else {
                    status = db.putNoOverwrite(writerTxn, key, data);
                }
                assertEquals(OperationStatus.SUCCESS, status);
                writerTxn.commit(Durability.COMMIT_NO_SYNC);
                insertFinished = true;
            }
        };

        writerThread.start();

        long startTime = System.currentTimeMillis();
        while (true) {

            /* Give some time to the writer thread. */
            Thread.yield();
            Thread.sleep(10);
            if (System.currentTimeMillis() - startTime > MAX_INSERT_MILLIS) {
                fail("Timeout doing insert");
            }

            if (txnSerializable) {

                /* Wait for the insert to block. */
                EnvironmentStats stats = env.getStats(null);
                if (stats.getNWaiters() > origStats.getNWaiters()) {
                    break;
                }
            } else {

                /* Wait for the operation to complete. */
                if (insertFinished) {
                    insertFinished = false;
                    break;
                }
            }
        }
    }

    /**
     * Waits for the writer thread to finish.
     */
    private void waitForInsert() {

        try {
            writerThread.finishTest();
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.toString());
        } finally {
            writerThread = null;
        }
    }
}
