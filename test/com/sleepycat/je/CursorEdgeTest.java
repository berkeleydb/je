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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Test edge case in cursor traversals. In particular, look at duplicates and
 * sets of keys interspersed with deletions.
 */
public class CursorEdgeTest extends DualTestCase {

    private static final boolean DEBUG = false;
    private Environment env;
    private final File envHome;
    private boolean operationStarted;

    public CursorEdgeTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    public void initEnv()
        throws DatabaseException {

        /*
         * Create an environment w/transactions and a max node size of 6.
         * Be sure to disable the compressor, we want some holes in the
         * tree.
         */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_INCOMPRESSOR.
                                 getName(),
                                 "false");
        envConfig.setAllowCreate(true);
        env = create(envHome, envConfig);
    }

    /**
     * Insert a number of duplicates, making sure that the duplicate tree
     * has multiple bins. Make sure that we can skip over the duplicates and
     * find the right value.
     */
    @Test
    public void testSearchOnDuplicatesWithDeletions()
        throws Throwable {

        initEnv();

        Database myDb = null;
        Cursor cursor = null;
        try {
            /* Set up a db */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setSortedDuplicates(true);
            dbConfig.setAllowCreate(true);
            myDb = env.openDatabase(null, "foo", dbConfig);

            /*
             * Insert k1/d1, then a duplicate range of k2/d1 -> k2/d15, then
             * k3/d1. Now delete the beginning part of the duplicate range,
             * trying to get more than a whole bin's worth (k2/d1 ->
             * k2/d7). Because the compressor is not enabled, there will be a
             * hole in the k2 range. While we're at it, delete k2/d10 - k2/d13
             * too, make sure we can traverse a hole in the middle of the
             * duplicate range.
             */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            key.setData(TestUtils.getTestArray(1));
            data.setData(TestUtils.getTestArray(1));
            myDb.put(null, key, data);          // k1/d1
            key.setData(TestUtils.getTestArray(3));
            myDb.put(null, key, data);          // k3/d1

            /* insert k2 range */
            key.setData(TestUtils.getTestArray(2));
            for (int i = 1; i <= 15; i++) {
                data.setData(TestUtils.getTestArray(i));
                myDb.put(null, key, data);
            }

            /* Now delete k2/d1 -> k2/d7 */
            Transaction txn =
                env.beginTransaction(null, TransactionConfig.DEFAULT);
            cursor = myDb.openCursor(txn, CursorConfig.DEFAULT);
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchKey(key, data, LockMode.DEFAULT));
            for (int i = 0; i < 7; i ++) {
                assertEquals(OperationStatus.SUCCESS, cursor.delete());
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getNext(key, data, LockMode.DEFAULT));
            }

            /* Also delete k2/d10 - k2/d13 */
            data.setData(TestUtils.getTestArray(10));
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchBoth(key, data, LockMode.DEFAULT));
            for (int i = 0; i < 3; i ++) {
                assertEquals(OperationStatus.SUCCESS, cursor.delete());
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getNext(key, data, LockMode.DEFAULT));
            }

            /* Double check what's in the tree */
            if (DEBUG) {
                Cursor checkCursor = myDb.openCursor(txn,
                                                     CursorConfig.DEFAULT);
                while (checkCursor.getNext(key, data, LockMode.DEFAULT) ==
                       OperationStatus.SUCCESS) {
                    System.out.println("key=" +
                                       TestUtils.getTestVal(key.getData()) +
                                       " data=" +
                                       TestUtils.getTestVal(data.getData()));
                }
                checkCursor.close();
            }
            cursor.close();
            cursor = null;
            txn.commit();

            if (DualTestCase.isReplicatedTest(getClass())) {
                return;
            }

            /*
             * Now make sure we can find k2/d8
             */
            Cursor readCursor = myDb.openCursor(null, CursorConfig.DEFAULT);
            key.setData(TestUtils.getTestArray(2));

            /* Use key search */
            assertEquals(OperationStatus.SUCCESS,
                         readCursor.getSearchKey(key, data, LockMode.DEFAULT));
            assertEquals(2, TestUtils.getTestVal(key.getData()));
            assertEquals(8, TestUtils.getTestVal(data.getData()));

            /* Use range search */
            assertEquals(OperationStatus.SUCCESS,
                         readCursor.getSearchKeyRange(key, data,
                                                      LockMode.DEFAULT));
            assertEquals(2, TestUtils.getTestVal(key.getData()));
            assertEquals(8, TestUtils.getTestVal(data.getData()));

            /* Use search both */
            data.setData(TestUtils.getTestArray(8));
            assertEquals(OperationStatus.SUCCESS,
                         readCursor.getSearchBoth(key, data,
                                                  LockMode.DEFAULT));
            assertEquals(2, TestUtils.getTestVal(key.getData()));
            assertEquals(8, TestUtils.getTestVal(data.getData()));

            /* Use search both range, starting data at 8 */
            data.setData(TestUtils.getTestArray(8));
            assertEquals(OperationStatus.SUCCESS,
                         readCursor.getSearchBothRange(key, data,
                                                       LockMode.DEFAULT));
            assertEquals(2, TestUtils.getTestVal(key.getData()));
            assertEquals(8, TestUtils.getTestVal(data.getData()));

            /* Use search both range, starting at 1 */
            data.setData(TestUtils.getTestArray(1));
            assertEquals(OperationStatus.SUCCESS,
                         readCursor.getSearchBothRange(key, data,
                                                       LockMode.DEFAULT));
            assertEquals(2, TestUtils.getTestVal(key.getData()));
            assertEquals(8, TestUtils.getTestVal(data.getData()));

            /*
             * Make sure we can find k2/d13 with a range search.
             */

            /*
             * Insert a set of duplicates, k5/d0 -> k5/d9, then delete all of
             * them (but don't compress). Make sure no form of search every
             * finds them.
             */
            key.setData(TestUtils.getTestArray(5));
            for (int i = 0; i < 10; i++) {
                data.setData(TestUtils.getTestArray(i));
                myDb.put(null, key, data);
            }
            myDb.delete(null, key);  // delete all k5's

            /* All searches on key 5 should fail */
            assertFalse(readCursor.getSearchKey(key, data, LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS);
            assertFalse(readCursor.getSearchKeyRange(key, data,
                                                     LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS);
            data.setData(TestUtils.getTestArray(0));
            assertFalse(readCursor.getSearchBoth(key, data,
                                                 LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS);
            assertFalse(readCursor.getSearchBothRange(key, data,
                                                      LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS);

            /* All ranges on key 4 should also fail. */
            key.setData(TestUtils.getTestArray(4));
            assertFalse(readCursor.getSearchKeyRange(key, data,
                                                     LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS);
            assertFalse(readCursor.getSearchBothRange(key, data,
                                                      LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS);

            readCursor.close();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            myDb.close();
            close(env);
        }
    }

    /**
     * Test the case where we allow duplicates in the database, but don't
     * actually insert a duplicate.  So we have a key/value pair and do a
     * getSearchBothRange using key and data-1 (i.e. we land on the key, but
     * just before the data in the dup set (which isn't a dup set since there's
     * only one).  getSearchBothRange should land on the key/value pair in this
     * case.  See SR #9248.
     */
    @Test
    public void testSearchBothWithOneDuplicate()
        throws Throwable {

        initEnv();

        Database myDb = null;
        Cursor cursor = null;
        try {
            if (DualTestCase.isReplicatedTest(getClass())) {
                return;
            }

            /* Set up a db */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setSortedDuplicates(true);
            dbConfig.setAllowCreate(true);
            myDb = env.openDatabase(null, "foo", dbConfig);

            /* Put one record */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            key.setData(TestUtils.getTestArray(1));
            data.setData(TestUtils.getTestArray(1));
            myDb.put(null, key, data);

            key.setData(TestUtils.getTestArray(1));
            data.setData(TestUtils.getTestArray(0));
            cursor = myDb.openCursor(null, CursorConfig.DEFAULT);
            OperationStatus status =
                cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
            assertSame(status, OperationStatus.SUCCESS);
            assertEquals(1, TestUtils.getTestVal(key.getData()));
            assertEquals(1, TestUtils.getTestVal(data.getData()));
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (myDb != null) {
                myDb.close();
            }
            close(env);
        }
    }

    /**
     * Tests a bug fix to CursorImpl.fetchCurrent [#11195].
     *
     * T1 inserts K1-D1 and holds WRITE on K1-D1 (no dup tree yet)
     * T2 calls getFirst and waits for READ on K1-D1
     * T1 inserts K1-D2 which creates the dup tree
     * T1 commits, allowing T2 to proceed
     *
     * T2 is in the middle of CursorImpl.fetchCurrent, and assumes incorrectly
     * that it has a lock on an LN in BIN; actually the LN was replaced by a
     * DIN and a ClassCastException occurs.
     */
    @Test
    public void testGetCurrentDuringDupTreeCreation()
        throws Throwable {

        initEnv();

        /* Set up a db */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setAllowCreate(true);
        final Database myDb = env.openDatabase(null, "foo", dbConfig);

        /* T1 inserts K1-D1. */
        Transaction t1 = env.beginTransaction(null, null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(1));
        myDb.put(t1, key, data);

        /* T2 calls getFirst. */
        JUnitThread thread = new JUnitThread("getFirst") {
            @Override
            public void testBody()
                throws DatabaseException {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                Transaction t2 = env.beginTransaction(null, null);
                operationStarted = true;
                Cursor cursor = myDb.openCursor(t2, null);
                OperationStatus status = cursor.getFirst(key, data, null);
                assertEquals(1, TestUtils.getTestVal(key.getData()));
                assertEquals(1, TestUtils.getTestVal(data.getData()));
                assertEquals(OperationStatus.SUCCESS, status);
                cursor.close();
                t2.commit(Durability.COMMIT_NO_SYNC);
            }
        };
        thread.start();
        while (!operationStarted) {
            Thread.yield();
        }
        Thread.sleep(10);

        /* T1 inserts K1-D2. */
        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(2));
        myDb.put(t1, key, data);
        t1.commit(Durability.COMMIT_NO_SYNC);

        try {
            thread.finishTest();
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.toString());
        }
        myDb.close();
        close(env);
    }

    /**
     * Tests a bug fix to CursorImpl.fetchCurrent [#11700] that caused
     * ArrayIndexOutOfBoundsException.
     */
    @Test
    public void testGetPrevNoDupWithEmptyTree()
        throws Throwable {

        initEnv();

        OperationStatus status;

        /*
         * Set up a db
         */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(true);
        dbConfig.setAllowCreate(true);
        Database myDb = env.openDatabase(null, "foo", dbConfig);

        /*
         * Insert two sets of duplicates.
         */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(1));
        myDb.put(null, key, data);
        data.setData(TestUtils.getTestArray(2));
        myDb.put(null, key, data);

        key.setData(TestUtils.getTestArray(2));
        data.setData(TestUtils.getTestArray(1));
        myDb.put(null, key, data);
        data.setData(TestUtils.getTestArray(2));
        myDb.put(null, key, data);

        /*
         * Delete all duplicates with a cursor.
         */
        Cursor cursor = myDb.openCursor(null, null);
        while ((status = cursor.getNext(key, data, null)) ==
                OperationStatus.SUCCESS) {
            cursor.delete();
        }

        /*
         * Compress to empty the two DBINs.  The BIN will not be deleted
         * because a cursor is attached to it.  This causes a cursor to be
         * positioned on an empty DBIN, which brings out the bug.
         */
        env.compress();

        /*
         * Before the bug fix, getPrevNoDup caused
         * ArrayIndexOutOfBoundsException.
         */
        status = cursor.getPrevNoDup(key, data, null);
        assertEquals(OperationStatus.NOTFOUND, status);

        cursor.close();
        myDb.close();
        close(env);
        env = null;
    }

    /*
     * Check that non transactional cursors can't do update operations against
     * a transactional database.
     */
    @Test
    public void testNonTxnalCursorNoUpdates()
        throws Throwable {

        initEnv();

        Database myDb = null;
        SecondaryDatabase mySecDb = null;
        Cursor cursor = null;
        SecondaryCursor secCursor = null;
        try {
            /* Set up a db with a secondary, insert something. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            myDb = env.openDatabase(null, "foo", dbConfig);

            SecondaryConfig secConfig = new SecondaryConfig();
            secConfig.setTransactional(true);
            secConfig.setAllowCreate(true);
            secConfig.setKeyCreator(new KeyCreator());
            mySecDb = env.openSecondaryDatabase(null, "fooSecDb", myDb,
                                                secConfig);

            /* Insert something. */
            DatabaseEntry key = new DatabaseEntry(new byte[1]);
            assertEquals(myDb.put(null, key, key), OperationStatus.SUCCESS);

            if (DualTestCase.isReplicatedTest(getClass())) {
                return;
            }

            /* Open a non-txnal cursor on the primary database. */
            cursor = myDb.openCursor(null, null);
            DatabaseEntry data = new DatabaseEntry();
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, LockMode.DEFAULT));

            /* All updates should be prohibited. */
            updatesShouldBeProhibited(cursor);

            /* Open a secondary non-txnal cursor. */
            secCursor = mySecDb.openSecondaryCursor(null, null);
            assertEquals(OperationStatus.SUCCESS,
                         secCursor.getNext(key, data, LockMode.DEFAULT));

            /* All updates should be prohibited. */
            updatesShouldBeProhibited(secCursor);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (secCursor != null) {
                secCursor.close();
            }

            if (cursor != null) {
                cursor.close();
            }

            if (mySecDb != null) {
                mySecDb.close();
            }

            myDb.close();
            close(env);
        }
    }

    /* Updates should not be possible with this cursor. */
    private void updatesShouldBeProhibited(Cursor c) {
        try {
            c.delete();
            fail("Should not be able to do a delete");
        } catch (UnsupportedOperationException e) {
            checkForCursorUpdateException(false, e);
        }

        DatabaseEntry key = new DatabaseEntry(new byte[0]);
        DatabaseEntry data = new DatabaseEntry(new byte[0]);

        try {
            c.put(key, data);
            fail("Should not be able to do a put");
        } catch (UnsupportedOperationException e) {
            checkForCursorUpdateException(c instanceof SecondaryCursor, e);
        }

        try {
            c.putCurrent(data);
            fail("Should not be able to do a putCurrent");
        } catch (UnsupportedOperationException e) {
            checkForCursorUpdateException(c instanceof SecondaryCursor, e);
        }

        try {
            c.putNoDupData(key, data);
            fail("Should not be able to do a putNoDupData");
        } catch (UnsupportedOperationException e) {
            checkForCursorUpdateException(c instanceof SecondaryCursor, e);
        }

        try {
            c.putNoOverwrite(key, data);
            fail("Should not be able to do a putNoOverwrite");
        } catch (UnsupportedOperationException e) {
            checkForCursorUpdateException(c instanceof SecondaryCursor, e);
        }
    }

    private void checkForCursorUpdateException(boolean isSecUpdateError,
                                               RuntimeException e) {

        /*
         * Check that it's a transaction or secondary problem. Crude, but since
         * we don't want to add exception types, necessary.
         */
        String msg = e.getMessage();
        if (isSecUpdateError) {
            assertTrue(
                msg, msg.startsWith("Operation not allowed on a secondary"));
        } else {
            assertTrue(msg, msg.contains("a Transaction was not supplied"));
        }
    }

    private static class KeyCreator implements SecondaryKeyCreator {
        public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                          DatabaseEntry keyEntry,
                                          DatabaseEntry dataEntry,
                                          DatabaseEntry resultEntry) {
            resultEntry.setData(dataEntry.getData());
            return true;
        }
    }

    /**
     * Tests that when a LockNotAvailableException is thrown as the result of a
     * cursor operation, all latches are released properly.  There are two
     * cases corresponding to the two methods in CursorImpl --
     * lockLNDeletedAllowed and lockDupCountLN, which lock leaf LNs and dup
     * count LNs, respectively -- that handle locking and latching.  These
     * methods optimize by not releasing latches while obtaining a non-blocking
     * lock.  Prior to the fix for [#15142], these methods did not release
     * latches when LockNotAvailableException, which can occur when a
     * transaction is configured for "no wait".
     */
    @Test
    public void testNoWaitLatchRelease()
        throws Throwable {

        initEnv();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Open the database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        Database db = env.openDatabase(null, "foo", dbConfig);

        /* Insert record 1. */
        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(1));
        db.put(null, key, data);

        /* Open cursor1 with txn1 and lock record 1. */
        Transaction txn1 = env.beginTransaction(null, null);
        Cursor cursor1 = db.openCursor(txn1, null);
        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(1));
        OperationStatus status = cursor1.getSearchBoth(key, data, null);
        assertSame(status, OperationStatus.SUCCESS);
        assertEquals(1, TestUtils.getTestVal(key.getData()));
        assertEquals(1, TestUtils.getTestVal(data.getData()));

        /* Open cursor2 with no-wait txn2 and try to delete record 1. */
        TransactionConfig noWaitConfig = new TransactionConfig();
        noWaitConfig.setNoWait(true);
        Transaction txn2 = env.beginTransaction(null, noWaitConfig);
        Cursor cursor2 = db.openCursor(txn2, null);
        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(1));
        status = cursor2.getSearchBoth(key, data, null);
        assertSame(status, OperationStatus.SUCCESS);
        assertEquals(1, TestUtils.getTestVal(key.getData()));
        assertEquals(1, TestUtils.getTestVal(data.getData()));
        try {
            cursor2.delete();
            fail("Expected LockNotAvailableException");
        } catch (LockNotAvailableException expected) {
        }

        /*
         * Before the [#15142] bug fix, this could have failed.  However, that
         * failure was not reproducible because all callers of
         * lockLNDeletedAllowed redundantly release the BIN latches.  So this is
         * just an extra check to ensure such a bug is never introduced.
         */
        assertEquals(0, LatchSupport.nBtreeLatchesHeld());

        /* Close cursors and txns to release locks. */
        cursor1.close();
        cursor2.close();
        txn1.commit();
        txn2.commit();

        /*
         * Since DupCountLNs are no longer used, the second portion of the test
         * was deleted.
         */

        /* Close all. */
        db.close();
        close(env);
    }

    /**
     * Checks that an uncommitted deleted record will cause other txns that
     * access it to block.  This reproduces a bug [#22892] where other txns
     * accessing the record would return NOTFOUND rather than blocking.
     */
    @Test
    public void testReadDeletedUncommitted()
        throws Throwable {

        initEnv();

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        final Database db = env.openDatabase(null, "foo", dbConfig);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        /* Insert record 1. */
        key.setData(TestUtils.getTestArray(1));
        data.setData(TestUtils.getTestArray(1));
        db.put(null, key, data);

        /* Use txn1 to delete record 1.  Leave txn1 open. */
        final Transaction txn1 = env.beginTransaction(null, null);
        key.setData(TestUtils.getTestArray(1));
        OperationStatus status = db.delete(txn1, key);
        assertSame(status, OperationStatus.SUCCESS);

        /* Try to read record 1 with no-wait txn2.  Should fail. */
        final TransactionConfig txn2Config = new TransactionConfig();
        txn2Config.setNoWait(true);
        final Transaction txn2 = env.beginTransaction(null, txn2Config);
        try {
            key.setData(TestUtils.getTestArray(1));
            status = db.get(txn2, key, data, null);
            fail(status.toString());
        } catch (LockNotAvailableException expected) {
        }
        final Cursor cursor2 = db.openCursor(txn2, null);
        try {
            key.setData(TestUtils.getTestArray(1));
            status = cursor2.getSearchKey(key, data, null);
            fail(status.toString());
        } catch (LockNotAvailableException expected) {
        }

        /* Commit txn1.  Then expect NOTFOUND when reading. */
        txn1.commit();
        key.setData(TestUtils.getTestArray(1));
        status = db.get(txn2, key, data, null);
        assertSame(status, OperationStatus.NOTFOUND);
        key.setData(TestUtils.getTestArray(1));
        status = cursor2.getSearchKey(key, data, null);
        assertSame(status, OperationStatus.NOTFOUND);

        cursor2.close();
        txn2.commit();
        db.close();
        close(env);
    }
}
