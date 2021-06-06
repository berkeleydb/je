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

package com.sleepycat.je.txn;

import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE_TXNS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_BEGINS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_COMMITS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XAABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XACOMMITS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XAPREPARES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Date;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.ActiveTxnArrayStat;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * @excludeDualMode
 * This test checks the value of nAborts, but the replication environment may
 * legitimately have a nAborts value of 1 or 0, due to the
 * transaction handling in RepImpl.openGroupDB. Exclude the test.
 *
 * Test transaction aborts and commits.
 */
public class TxnEndTest extends TestBase {
    private static final int NUM_DBS = 1;
    private Environment env;
    private final File envHome;
    private Database[] dbs;
    private Cursor[] cursors;
    private JUnitThread junitThread;

    public TxnEndTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        /*
         * Run environment without in compressor on so we can check the
         * compressor queue in a deterministic way.
         */
        super.setUp();
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "6");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);
    }

    @After
    public void tearDown() {
        if (junitThread != null) {
            junitThread.shutdown();
            junitThread = null;
        }

        if (env != null) {
            try {
                env.close();
            } catch (Exception e) {
                System.out.println("tearDown: " + e);
            }
        }
        env = null;
        TestUtils.removeFiles("TearDown", envHome, FileManager.JE_SUFFIX);
    }

    private void createDbs()
        throws DatabaseException {

        dbs = new Database[NUM_DBS];
        cursors = new Cursor[NUM_DBS];

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        for (int i = 0; i < NUM_DBS; i++) {
            dbs[i] = env.openDatabase(null, "testDB" + i, dbConfig);
        }
    }

    private void closeAll()
        throws DatabaseException {

        for (int i = 0; i < NUM_DBS; i++) {
            dbs[i].close();
        }
        dbs = null;
        env.close();
        env = null;
    }

    /**
     * Create cursors with this owning transaction
     */
    private void createCursors(Transaction txn)
        throws DatabaseException {

        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = dbs[i].openCursor(txn, null);
        }
    }

    /**
     * Close the current set of cursors
     */
    private void closeCursors()
        throws DatabaseException {

        for (int i = 0; i < cursors.length; i++) {
            cursors[i].close();
        }
    }

    /**
     * Insert keys from i=start; i <end using a cursor
     */
    private void cursorInsertData(int start, int end)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < NUM_DBS; i++) {
            for (int d = start; d < end; d++) {
                key.setData(TestUtils.getTestArray(d));
                data.setData(TestUtils.getTestArray(d));
                cursors[i].put(key, data);
            }
        }
    }
    /**
     * Insert keys from i=start; i < end using a db
     */
    private void dbInsertData(int start, int end, Transaction txn)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < NUM_DBS; i++) {
            for (int d = start; d < end; d++) {
                key.setData(TestUtils.getTestArray(d));
                data.setData(TestUtils.getTestArray(d));
                dbs[i].put(txn, key, data);
            }
        }
    }

    /**
     * Modify keys from i=start; i <end
     */
    private void cursorModifyData(int start, int end, int valueOffset)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < NUM_DBS; i++) {
            OperationStatus status =
                cursors[i].getFirst(key, data, LockMode.DEFAULT);
            for (int d = start; d < end; d++) {
                assertEquals(OperationStatus.SUCCESS, status);
                byte[] changedVal =
                    TestUtils.getTestArray(d + valueOffset);
                data.setData(changedVal);
                cursors[i].putCurrent(data);
                status = cursors[i].getNext(key, data, LockMode.DEFAULT);
            }
        }
    }

    /**
     * Delete records from i = start; i < end.
     */
    private void cursorDeleteData(int start, int end)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        for (int i = 0; i < NUM_DBS; i++) {
            for (int d = start; d < end; d++) {
                byte[] searchValue =
                    TestUtils.getTestArray(d);
                key.setData(searchValue);
                OperationStatus status =
                    cursors[i].getSearchKey(key, foundData, LockMode.DEFAULT);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(OperationStatus.SUCCESS, cursors[i].delete());
            }
        }
    }

    /**
     * Delete records with a db.
     */
    private void dbDeleteData(int start, int end, Transaction txn)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        for (int i = 0; i < NUM_DBS; i++) {
            for (int d = start; d < end; d++) {
                byte[] searchValue =
                    TestUtils.getTestArray(d);
                key.setData(searchValue);
                dbs[i].delete(txn, key);
            }
        }
    }

    /**
     * Check that there are numKeys records in each db, and their value
     * is i + offset.
     */
    private void verifyData(int numKeys, int valueOffset)
        throws DatabaseException {

        for (int i = 0; i < NUM_DBS; i++) {
            /* Run verify */
            DatabaseImpl dbImpl = DbInternal.getDbImpl(dbs[i]);
            dbImpl.verify(new VerifyConfig());

            Cursor verifyCursor =
                dbs[i].openCursor(null, CursorConfig.READ_UNCOMMITTED);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus status =
                verifyCursor.getFirst(key, data, LockMode.DEFAULT);
            for (int d = 0; d < numKeys; d++) {
                assertEquals("key=" + d, OperationStatus.SUCCESS, status);
                byte[] expected = TestUtils.getTestArray(d + valueOffset);
                assertTrue(Arrays.equals(expected, key.getData()));
                assertTrue("Expected= " + TestUtils.dumpByteArray(expected) +
                           " saw=" + TestUtils.dumpByteArray(data.getData()),
                           Arrays.equals(expected, data.getData()));
                status = verifyCursor.getNext(key, data, LockMode.DEFAULT);
            }
            /* Should be the end of this database. */
            assertTrue("More data than expected",
                       (status != OperationStatus.SUCCESS));
            verifyCursor.close();
        }
    }

    /**
     * Test basic commits, aborts with cursors
     */
    @Test
    public void testBasicCursor()
        throws Throwable {

        try {
            int numKeys = 7;
            createDbs();

            /* Insert more data with a user transaction, commit. */
            Transaction txn = env.beginTransaction(null, null);
            createCursors(txn);
            cursorInsertData(0, numKeys*2);
            closeCursors();
            txn.commit();
            verifyData(numKeys*2, 0);

            /* Insert more data, abort, check that data is unchanged. */
            txn = env.beginTransaction(null, null);
            createCursors(txn);
            cursorInsertData(numKeys*2, numKeys*3);
            closeCursors();
            txn.abort();
            verifyData(numKeys*2, 0);

            /*
             * Check the in compressor queue, we should have some number of
             * bins on. If the queue size is 0, then check the processed stats,
             * the in compressor thread may have already woken up and dealt
             * with the entries.
             */
            EnvironmentStats envStat = env.getStats(TestUtils.FAST_STATS);
            long queueSize = envStat.getInCompQueueSize();
            assertTrue(queueSize > 0);

            /* Modify data, abort, check that data is unchanged. */
            txn = env.beginTransaction(null, null);
            createCursors(txn);
            cursorModifyData(0, numKeys * 2, 1);
            closeCursors();
            txn.abort();
            verifyData(numKeys*2, 0);

            /* Delete data, abort, check that data is still there. */
            txn = env.beginTransaction(null, null);
            createCursors(txn);
            cursorDeleteData(numKeys+1, numKeys*2);
            closeCursors();
            txn.abort();
            verifyData(numKeys*2, 0);
            /* Check the in compressor queue, nothing should be loaded. */
            envStat = env.getStats(TestUtils.FAST_STATS);
            assertEquals(queueSize, envStat.getInCompQueueSize());

            /* Delete data, commit, check that data is gone. */
            txn = env.beginTransaction(null, null);
            createCursors(txn);
            cursorDeleteData(numKeys, numKeys*2);
            closeCursors();
            txn.commit();
            verifyData(numKeys, 0);

            /* Check the inCompressor queue, there should be more entries. */
            envStat = env.getStats(TestUtils.FAST_STATS);
            assertTrue(envStat.getInCompQueueSize() > queueSize);

            closeAll();

        } catch (Throwable t) {
            /* Print stacktrace before attempt to run tearDown. */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test that txn commit fails with open cursors.
     */
    @Test
    public void testTxnClose()
        throws DatabaseException {

        createDbs();
        Transaction txn = env.beginTransaction(null, null);
        createCursors(txn);

        try {
            txn.commit();
            fail("Commit should fail, cursors are open.");
        } catch (IllegalStateException e) {
            txn.abort();
        }
        closeCursors();

        txn = env.beginTransaction(null, null);
        createCursors(txn);
        closeCursors();
        txn.commit();

        try {
            txn.abort();
        } catch (RuntimeException e) {
            fail("Txn abort after commit shouldn't fail.");
        }

        txn = env.beginTransaction(null, null);
        createCursors(txn);
        closeCursors();
        txn.abort();

        try {
            txn.abort();
        } catch (RuntimeException e) {
            fail("Double abort shouldn't fail.");
        }

        closeAll();
    }

    /**
     * Test use through db.
     */
    @Test
    public void testBasicDb()
        throws Throwable {

        try {
            TransactionStats stats =
                env.getTransactionStats(TestUtils.FAST_STATS);
            int initialAborts = 0;
            assertEquals(initialAborts, stats.getNAborts());
            /* 3 commits for adding cleaner dbs. */
            int initialCommits = 3;
            assertEquals(initialCommits, stats.getNCommits());

            long locale = new Date().getTime();
            TransactionStats.Active[] at = new TransactionStats.Active[4];

            for(int i = 0; i < 4; i++) {
                at[i] = new TransactionStats.Active("TransactionStatForTest",
                                                    i, i - 1);
            }

            StatGroup group = new StatGroup("test", "test");
            ActiveTxnArrayStat arrayStat =
                new ActiveTxnArrayStat(group, TXN_ACTIVE_TXNS, at);
            new LongStat(group, TXN_ABORTS, 12);
            new LongStat(group, TXN_XAABORTS, 15);
            new IntStat(group, TXN_ACTIVE, 20);
            new LongStat(group, TXN_BEGINS, 25);
            new LongStat(group, TXN_COMMITS, 1);
            new LongStat(group, TXN_XACOMMITS, 30);
            new LongStat(group, TXN_XAPREPARES, 20);
            stats = new TransactionStats(group);

            TransactionStats.Active[] at1 = stats.getActiveTxns();

            for(int i = 0; i < 4; i++) {
                assertEquals("TransactionStatForTest", at1[i].getName());
                assertEquals(i, at1[i].getId());
                assertEquals(i - 1, at1[i].getParentId());
                at1[i].toString();
            }
            assertEquals(12, stats.getNAborts());
            assertEquals(15, stats.getNXAAborts());
            assertEquals(20, stats.getNActive());
            assertEquals(25, stats.getNBegins());
            assertEquals(1, stats.getNCommits());
            assertEquals(30, stats.getNXACommits());
            assertEquals(20, stats.getNXAPrepares());
            stats.toString();

            arrayStat.set(null);
            stats.toString();

            int numKeys = 7;
            createDbs();

            /* Insert data with autocommit. */
            dbInsertData(0, numKeys, null);
            verifyData(numKeys, 0);

            /* Insert data with a txn. */
            Transaction txn = env.beginTransaction(null, null);
            dbInsertData(numKeys, numKeys*2, txn);
            txn.commit();
            verifyData(numKeys*2, 0);

            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(initialAborts, stats.getNAborts());
            assertEquals((initialCommits + 1 +  // 1 explicit commit above
                          (1 * NUM_DBS) +       // 1 per create/open
                          (numKeys*NUM_DBS)),   // 1 per record, using autotxn
                         stats.getNCommits());

            /* Delete data with a txn, abort. */
            txn = env.beginTransaction(null, null);
            dbDeleteData(numKeys, numKeys * 2, txn);
            verifyData(numKeys, 0);  // verify w/dirty read
            txn.abort();

            closeAll();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test TransactionStats.
     */
    @Test
    public void testTxnStats()
        throws Throwable {

        try {
            TransactionStats stats =
                env.getTransactionStats(TestUtils.FAST_STATS);
            int initialAborts = 0;
            assertEquals(initialAborts, stats.getNAborts());
            /* 3 commits for adding cleaner dbs. */
            int numBegins = 3;
            int numCommits = 3;
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());

            int numKeys = 7;
            createDbs();
            numBegins += NUM_DBS; // 1 begins per database
            numCommits += NUM_DBS; // 1 commits per database
            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());

            /* Insert data with autocommit. */
            dbInsertData(0, numKeys, null);
            numBegins += (numKeys * NUM_DBS);
            numCommits += (numKeys * NUM_DBS);
            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());
            verifyData(numKeys, 0);

            /* Insert data with a txn. */
            Transaction txn = env.beginTransaction(null, null);
            numBegins++;
            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());
            assertEquals(1, stats.getNActive());
            dbInsertData(numKeys, numKeys*2, txn);
            txn.commit();
            numCommits++;
            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());
            assertEquals(0, stats.getNActive());
            verifyData(numKeys*2, 0);

            /* Delete data with a txn, abort. */
            txn = env.beginTransaction(null, null);
            numBegins++;
            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());
            assertEquals(1, stats.getNActive());

            dbDeleteData(numKeys, numKeys * 2, txn);
            verifyData(numKeys, 0);  // verify w/dirty read
            txn.abort();
            stats = env.getTransactionStats(TestUtils.FAST_STATS);
            assertEquals(numBegins, stats.getNBegins());
            assertEquals(numCommits, stats.getNCommits());
            assertEquals(initialAborts + 1, stats.getNAborts());
            assertEquals(0, stats.getNActive());

            closeAll();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test db creation and deletion
     */

    @Test
    public void testDbCreation()
        throws DatabaseException {

        Transaction txnA = env.beginTransaction(null, null);
        Transaction txnB = env.beginTransaction(null, null);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database dbA =
            env.openDatabase(txnA, "foo", dbConfig);

        /* Try to see this database with another txn -- we should not see it. */

        dbConfig.setAllowCreate(false);

        try {
            txnB.setLockTimeout(1000);

                env.openDatabase(txnB, "foo", dbConfig);
            fail("Shouldn't be able to open foo");
        } catch (DatabaseException e) {
        }

        /* txnB must be aborted since openDatabase timed out. */
        txnB.abort();

        /* Open this database with the same txn and another handle. */
        Database dbC =
            env.openDatabase(txnA, "foo", dbConfig);

        /* Now commit txnA and txnB should be able to open this. */
        txnA.commit();
        txnB = env.beginTransaction(null, null);
        Database dbB =
            env.openDatabase(txnB, "foo", dbConfig);
        txnB.commit();

        /* XXX, test db deletion. */

        dbA.close();
        dbB.close();
        dbC.close();
    }

    /* Test that the transaction is unusable after a close. */
    @Test
    public void testClose()
        throws DatabaseException {

        Transaction txnA = env.beginTransaction(null, null);
        txnA.commit();

        try {
            env.openDatabase(txnA, "foo", null);
            fail("Should not be able to use a closed transaction");
        } catch (IllegalArgumentException expected) {
        }
    }

    /**
     * Simulates a race condition between two threads that previously caused a
     * latch deadlock.  [#19321]
     *
     * One thread is aborting a txn.  The other thread is using the same txn to
     * perform a cursor operation.  While the BIN is held, it attempts to get a
     * non-blocking lock.
     */
    @Test
    public void testAbortLatchDeadlock() {

        /* Create DB. */
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        final Database db = env.openDatabase(null, "foo", dbConfig);

        /* Insert one record. */
        final DatabaseEntry key = new DatabaseEntry(new byte[1]);
        final DatabaseEntry data = new DatabaseEntry(new byte[1]);
        assertSame(OperationStatus.SUCCESS, 
                   db.putNoOverwrite(null, key, data));

        /* Begin txn, to be shared by both threads. */
        final Transaction txn = env.beginTransaction(null, null);

        /* Simulate cursor operation that latches BIN. */
        final Cursor cursor = db.openCursor(txn, null);
        assertSame(OperationStatus.SUCCESS, cursor.put(key, data));
        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
        cursorImpl.latchBIN();

        /* Run abort in a separate thread. */
        junitThread = new JUnitThread("testAbortLatchDeadlock") {
            @Override
            public void testBody() {

                /*
                 * The cursor is not closed before the abort is allowed to
                 * continue, sometimes causing an "open cursors" exception,
                 * depending on timing.  This is acceptable for this test,
                 * since we are checking that a hang does not occur.
                 */
                try {
                    txn.abort();
                } catch (IllegalStateException e) {
                    assertTrue(e.getMessage().contains
                        ("detected open cursors"));
                }
            }
        };
        junitThread.start();

        /*
         * Wait for abort to attempt latch on BIN.
         *
         * Because now BtreeVerifier code also need to latch the BIN, so the
         * previous condition 'cursorImpl.getBIN().getLatchNWaiters() == 1'
         * can not guarantee that txn.abort has already attempted to latch
         * on the BIN. 'cursorImpl.getBIN().getLatchNWaiters() == 2' can
         * guarantee that the BtreeVerifier and txn.abort both wait for the
         * latch.
         *
         * But this new condition may need to wait the BtreeVerifier to run
         * btree verify action, because now we set the schedule to be every
         * minutes for unit test, this means that this condition may need to
         * wait 1 minute.
         */
        while (cursorImpl.getBIN().getLatchNWaiters() < 2) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                fail();
            }
        }

        /*
         * Simulate cursor operation that gets non-blocking lock.  Before the
         * fix [#19321], a latch deadlock would occur here.
         */
        try {
            cursorImpl.getLocker().nonBlockingLock
                (123L, LockType.WRITE, false, DbInternal.getDbImpl(db));
            fail();
        } catch (IllegalStateException expected) {
        } finally {
            /* Release latch, allow abort to continue. */
            cursorImpl.releaseBIN();
            cursor.close();
        }

        /* Finish test. */
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

        db.close();
    }
    
    /* 
     * Test the case where truncateDatabase and removeDatabase operations are
     * done on the same database in the same txn. [#19636]
     */
    @Test
    public void testTruncateDeleteDB() {
        /* Create test DB. */
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        String dbName = "test-db";

        /* Test single truncation and removement. */
        Database db = env.openDatabase(null, dbName, dbConfig);
        db.close();
        Transaction txn = env.beginTransaction(null, null);
        /* Truncate and remove a database in the same txn. */
        env.truncateDatabase(txn, dbName, false);
        env.removeDatabase(txn, dbName);
        txn.abort();
        /* No database is removed after aborting the txn.. */
        assertEquals(1, env.getDatabaseNames().size());
        txn = env.beginTransaction(null, null);
        env.truncateDatabase(txn, dbName, false);
        env.removeDatabase(txn, dbName);
        txn.commit();
        /* the database has been removed after committing the txn. */
        assertEquals(0, env.getDatabaseNames().size());
        
        /* Test multiple truncations before single removement. */
        db = env.openDatabase(null, dbName, dbConfig);
        db.close();
        txn = env.beginTransaction(null, null);
        /* Truncate a database three times then remove it in the same txn. */
        env.truncateDatabase(txn, dbName, false);
        env.truncateDatabase(txn, dbName, false);
        env.truncateDatabase(txn, dbName, false);
        env.removeDatabase(txn, dbName);
        txn.abort();
        /* No database is removed after aborting the txn. */
        assertEquals(1, env.getDatabaseNames().size());
        txn = env.beginTransaction(null, null);
        env.truncateDatabase(txn, dbName, false);
        env.truncateDatabase(txn, dbName, false);
        env.truncateDatabase(txn, dbName, false);
        env.removeDatabase(txn, dbName);
        txn.commit();
        /* the database has been removed after committing the txn. */
        assertEquals(0, env.getDatabaseNames().size());
    }
}
