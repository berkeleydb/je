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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import org.junit.After;
import org.junit.Test;

/**
 * Tests the read-committed (degree 2) isolation level.
 */
public class ReadCommittedTest extends DualTestCase {

    private final File envHome;
    private Environment env;
    private Database db;

    public ReadCommittedTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown()
        throws Exception {

        LockManager.afterLockHook = null;

        super.tearDown();

        if (env != null) {
            env.close();
        }
    }

    private void open()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        /* Control over isolation level is required by this test. */
        TestUtils.clearIsolationLevel(envConfig);

        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

//        envConfig.setConfigParam(
//            EnvironmentConfig.LOCK_TIMEOUT, "" + Integer.MAX_VALUE);

        env = create(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        db = env.openDatabase(null, "foo", dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int i = 100; i <= 200; i += 100) {
            for (int j = 1; j <= 5; j += 1) {
                IntegerBinding.intToEntry(i + j, key);
                IntegerBinding.intToEntry(0, data);
                db.put(null, key, data);
            }
        }
    }

    private void close()
        throws DatabaseException {

        db.close();
        db = null;
        close(env);
        env = null;
    }

    @Test
    public void testIllegalConfig()
        throws DatabaseException {

        open();

        CursorConfig cursConfig;
        TransactionConfig txnConfig;

        /* Disallow transaction ReadCommitted and Serializable. */
        txnConfig = new TransactionConfig();
        txnConfig.setReadCommitted(true);
        txnConfig.setSerializableIsolation(true);
        try {
            env.beginTransaction(null, txnConfig);
            fail();
        } catch (IllegalArgumentException expected) {}

        /* Disallow transaction ReadCommitted and ReadUncommitted. */
        txnConfig = new TransactionConfig();
        txnConfig.setReadCommitted(true);
        txnConfig.setReadUncommitted(true);
        try {
            env.beginTransaction(null, txnConfig);
            fail();
        } catch (IllegalArgumentException expected) {}

        /* Disallow cursor ReadCommitted and ReadUncommitted. */
        cursConfig = new CursorConfig();
        cursConfig.setReadCommitted(true);
        cursConfig.setReadUncommitted(true);
        Transaction txn = env.beginTransaction(null, null);
        try {
            db.openCursor(txn, cursConfig);
            fail();
        } catch (IllegalArgumentException expected) {}
        txn.abort();

        close();
    }

    @Test
    public void testWithTransactionConfig()
        throws DatabaseException {

        doTestWithTransactionConfig(false /*nonSticky*/);
    }

    @Test
    public void testNonCloningWithTransactionConfig()
        throws DatabaseException {

        doTestWithTransactionConfig(true /*nonSticky*/);
    }

    private void doTestWithTransactionConfig(boolean nonSticky)
        throws DatabaseException {

        open();

        TransactionConfig config = new TransactionConfig();
        config.setReadCommitted(true);
        Transaction txn = env.beginTransaction(null, config);
        Cursor cursor = db.openCursor(
            txn, new CursorConfig().setNonSticky(nonSticky));

        checkReadCommitted(cursor, 100, true, nonSticky);

        cursor.close();
        txn.commit();
        close();
    }

    @Test
    public void testWithCursorConfig()
        throws DatabaseException {

        doTestWithCursorConfig(false /*nonSticky*/);
    }

    @Test
    public void testNonCloningWithCursorConfig()
        throws DatabaseException {

        doTestWithCursorConfig(true /*nonSticky*/);
    }

    private void doTestWithCursorConfig(boolean nonSticky) {

        open();

        Transaction txn = env.beginTransaction(null, null);
        CursorConfig config = new CursorConfig();
        config.setReadCommitted(true);
        config.setNonSticky(nonSticky);
        Cursor cursor = db.openCursor(txn, config);
        Cursor degree3Cursor = db.openCursor(txn, null);

        checkReadCommitted(cursor, 100, true, nonSticky);
        checkReadCommitted(degree3Cursor, 200, false, nonSticky);

        degree3Cursor.close();
        cursor.close();
        txn.commit();
        close();
    }

    @Test
    public void testWithLockMode()
        throws DatabaseException {

        open();

        Transaction txn = env.beginTransaction(null, null);

        checkReadCommitted(txn, LockMode.READ_COMMITTED, 100, true);
        checkReadCommitted(txn, null, 200, false);

        txn.commit();
        close();
    }

    /**
     * Checks that the given cursor provides the given
     * expectReadLocksAreReleased behavior.
     */
    private void checkReadCommitted(Cursor cursor,
                                    int startKey,
                                    boolean expectReadLocksAreReleased,
                                    boolean nonSticky)
        throws DatabaseException {

        final EnvironmentStats baseStats = env.getStats(null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        class MyHook implements TestHook<Void> {
            int maxReadLocks = 0;

            @Override
            public void doHook() {
                maxReadLocks = Math.max(
                    maxReadLocks, getNReadLocks(baseStats));
            }

            @Override
            public void doHook(Void obj) {
            }
            @Override
            public void hookSetup() {
            }
            @Override
            public void doIOHook() throws IOException {
            }
            @Override
            public Void getHookValue() {
                return null;
            }
        }

        MyHook hook = new MyHook();
        LockManager.afterLockHook = hook;

        /* Move to first record. */
        checkNReadLocks(baseStats, 0);
        IntegerBinding.intToEntry(startKey + 1, key);
        OperationStatus status = cursor.getSearchKey(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);

        /* Check read locks calling next/prev. [#23775] */
        for (int i = 2; i <= 5; i += 1) {
            status = cursor.getNext(key, data, null);
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(startKey + i, IntegerBinding.entryToInt(key));
            if (expectReadLocksAreReleased) {
                /* Read locks are released as the cursor moves. */
                checkNReadLocks(baseStats, 1);
            } else {
                /* Read locks are not released. */
                checkNReadLocks(baseStats, i);
            }
        }
        for (int i = 4; i >= 1; i -= 1) {
            status = cursor.getPrev(key, data, null);
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(startKey + i, IntegerBinding.entryToInt(key));
            if (expectReadLocksAreReleased) {
                /* Read locks are released as the cursor moves. */
                checkNReadLocks(baseStats, 1);
            } else {
                /* Read locks are not released. */
                checkNReadLocks(baseStats, 5);
            }
        }

        /* Move to last key in range to normalize write locking checking. */
        IntegerBinding.intToEntry(startKey + 5, key);
        status = cursor.getSearchKey(key, data, null);
        assertEquals(OperationStatus.SUCCESS, status);

        /* Check write locks calling put. */
        checkNWriteLocks(baseStats, 0);
        for (int i = 1; i <= 5; i += 1) {
            IntegerBinding.intToEntry(startKey + i, key);
            IntegerBinding.intToEntry(0, data);
            cursor.put(key, data);
            /* Write locks are not released. */
            if (expectReadLocksAreReleased) {
                /* A single new write lock, no upgrade. */
                checkNWriteLocks(baseStats, i);
            } else {
                /* Upgraded lock plus new write lock. */
                checkNWriteLocks(baseStats, i * 2);
            }
        }

        /* All read locks were upgraded by the put() calls above. */
        checkNReadLocks(baseStats, 0);

        /*
         * The max number of read locks held at one time is indicative of
         * whether read-committed is used.  Only one lock may be held when
         * read-committed is used with a non-sticky cursor, since the
         * non-sticky mode is intended to avoid deadlocks. [#23775]
         */
        if (!expectReadLocksAreReleased) {
            /* All records are locked at once with repeatable read. */
            assertEquals(5, hook.maxReadLocks);
        } else if (nonSticky) {
            /* Special case: one lock for read-committed and non-sticky. */
            assertEquals(1, hook.maxReadLocks);
        } else {
            /*
             * With read-committed with without non-sticky, two locks are held
             * temporarily during the movement from one record to the next.
             */
            assertEquals(2, hook.maxReadLocks);
        }
    }

    /**
     * Checks that the given lock mode provides the given
     * expectReadLocksAreReleased behavior.
     */
    private void checkReadCommitted(Transaction txn,
                                    LockMode lockMode,
                                    int startKey,
                                    boolean expectReadLocksAreReleased)
        throws DatabaseException {

        EnvironmentStats baseStats = env.getStats(null);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Check read locks calling search. */
        checkNReadLocks(baseStats, 0);
        for (int i = 1; i <= 5; i += 1) {
            IntegerBinding.intToEntry(startKey + i, key);
            OperationStatus status = db.get(txn, key, data, lockMode);
            assertEquals(OperationStatus.SUCCESS, status);
            if (expectReadLocksAreReleased) {
                /* Read locks are released when the cursor is closed. */
                checkNReadLocks(baseStats, 0);
            } else {
                /* Read locks are not released. */
                checkNReadLocks(baseStats, i);
            }
        }

        /* Check write locks calling put. */
        checkNWriteLocks(baseStats, 0);
        for (int i = 1; i <= 5; i += 1) {
            IntegerBinding.intToEntry(startKey + i, key);
            IntegerBinding.intToEntry(0, data);
            db.put(txn, key, data);
            /* Write locks are not released. */
            if (expectReadLocksAreReleased) {
                /* A single new write lock, no upgrade. */
                checkNWriteLocks(baseStats, i);
            } else {
                /* Upgraded lock plus new write lock. */
                checkNWriteLocks(baseStats, i * 2);
            }
        }

        /* All read locks were upgraded by the put() calls above. */
        checkNReadLocks(baseStats, 0);
    }

    private void checkNReadLocks(EnvironmentStats baseStats,
                                 int nReadLocksExpected) {
        assertEquals(
            "Read locks -- ", nReadLocksExpected, getNReadLocks(baseStats));
    }

    private void checkNWriteLocks(EnvironmentStats baseStats,
                                  int nWriteLocksExpected) {
        assertEquals(
            "Write locks -- ", nWriteLocksExpected, getNWriteLocks(baseStats));
    }

    private int getNReadLocks(EnvironmentStats baseStats) {
        EnvironmentStats stats = env.getStats(null);
        return stats.getNReadLocks() - baseStats.getNReadLocks();
    }

    private int getNWriteLocks(EnvironmentStats baseStats) {
        EnvironmentStats stats = env.getStats(null);
        return stats.getNWriteLocks() - baseStats.getNWriteLocks();
    }

    /**
     * Current disabled because we haven't fixed the bug [#24453] that this
     * test reproduces.
     *
     * To debug, uncomment code in open() that sets a large lock timeout.
     */
//    @Test
    public void testRepeatableReadCombination() throws InterruptedException {

        open();

        final CountDownLatch t1Latch = new CountDownLatch(1);

        /* T1 gets a read lock and holds it until t1Latch is ready. */

        final Thread t1 = new Thread() {

            @Override
            public void run() {

                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry data = new DatabaseEntry();

                try (final Cursor cursor = db.openCursor(null, null)) {

                    final OperationStatus s = cursor.getFirst(key, data, null);
                    assert (OperationStatus.SUCCESS == s);

                    t1Latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        t1.start();

        try {
            /* Main thread gets a read-lock also. It is not the first owner. */

            final Transaction txn = env.beginTransaction(null, null);

            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            printStats(0);

            try (final Cursor cursor = db.openCursor(txn, null)) {

                OperationStatus s = cursor.getFirst(key, data, null);
                assert (OperationStatus.SUCCESS == s);
            }

            printStats(1);

            /* T2 attempts to get a write lock, but blocks. */

            final Thread t2 = new Thread() {

                @Override
                public void run() {

                    final DatabaseEntry key = new DatabaseEntry();
                    final DatabaseEntry data = new DatabaseEntry();

                    try (final Cursor cursor = db.openCursor(null, null)) {

                        final OperationStatus s =
                            cursor.getFirst(key, data, LockMode.RMW);
                        assert (OperationStatus.SUCCESS == s);

                        printStats(4);
                    }
                }
            };

            t2.start();

            try {
                while (env.getStats(null).getNWaiters() == 0) {
                    Thread.yield();
                }

                printStats(2);

                /* Main thread gets read lock again using read-committed. */

                try (final Cursor cursor =
                         db.openCursor(txn, CursorConfig.READ_COMMITTED)) {

                    final OperationStatus s = cursor.getFirst(key, data, null);
                    assert (OperationStatus.SUCCESS == s);
                }

                printStats(3);

                t1Latch.countDown();
                t1.join(10);

                txn.abort();
                t2.join(10);

                printStats(5);

            } finally {
                while (t2.isAlive()) {
                    t2.interrupt();
                    t2.join(10);
                }
            }
        } finally {
            while (t1.isAlive()) {
                t1.interrupt();
                t1.join(10);
            }
        }

        close();
    }

    private void printStats(final int seq) {

        final EnvironmentStats stats = env.getStats(null);

        System.out.println("[" + seq +
            "] write-locks = " + stats.getNWriteLocks() +
            " read-locks = " + stats.getNReadLocks() +
            " waiters = " + stats.getNWaiters());
    }
}
