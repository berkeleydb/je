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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.DbInternal.Search;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.utilint.StringUtils;

import org.junit.Test;

public class CursorTest extends DualTestCase {
    private static final boolean DEBUG = false;
    private static final int NUM_RECS = 257;

    /*
     * Use a ridiculous value because we've seen extreme slowness on ocicat
     * where dbperf is often running.
     */
    private static final long LOCK_TIMEOUT = 50000000L;

    private static final String DUPKEY = "DUPKEY";

    private Environment env;
    private Database db;
    private PhantomTestConfiguration config;

    private File envHome;

    private volatile int sequence;

    public CursorTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testGetConfig()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        env = create(envHome, envConfig);
        Transaction txn = env.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(txn, "testDB", dbConfig);
        txn.commit();
        Cursor cursor = null;
        Transaction txn1 =
            env.beginTransaction(null, TransactionConfig.DEFAULT);
        try {
            cursor = db.openCursor(txn1, CursorConfig.DEFAULT);
            CursorConfig config = cursor.getConfig();
            if (config == CursorConfig.DEFAULT) {
                fail("didn't clone");
            }
        } catch (DatabaseException DBE) {
            DBE.printStackTrace();
            fail("caught DatabaseException " + DBE);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            txn1.abort();
            db.close();
            close(env);
            env = null;
        }
    }

    /**
     * Put some data in a database, take it out. Yank the file size down so we
     * have many files.
     */
    @Test
    public void testBasic()
        throws Throwable {

        try {
            insertMultiDb(1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testMulti()
        throws Throwable {

        try {
            insertMultiDb(4);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Specifies a test configuration.  This is just a struct for holding
     * parameters to be passed down to threads in inner classes.
     */
    class PhantomTestConfiguration {
        String testName;
        String thread1EntryToLock;
        String thread1OpArg;
        String thread2Start;
        String expectedResult;
        boolean doInsert;
        boolean doGetNext;
        boolean doCommit;

        PhantomTestConfiguration(String testName,
                                 String thread1EntryToLock,
                                 String thread1OpArg,
                                 String thread2Start,
                                 String expectedResult,
                                 boolean doInsert,
                                 boolean doGetNext,
                                 boolean doCommit) {
            this.testName = testName;
            this.thread1EntryToLock = thread1EntryToLock;
            this.thread1OpArg = thread1OpArg;
            this.thread2Start = thread2Start;
            this.expectedResult = expectedResult;
            this.doInsert = doInsert;
            this.doGetNext = doGetNext;
            this.doCommit = doCommit;
        }
    }

    /**
     * This series of tests sets up a simple 2 BIN tree with a specific set of
     * elements (see setupDatabaseAndEnv()).  It creates two threads.
     *
     * Thread 1 positions a cursor on an element on the edge of a BIN (either
     * the last element on the left BIN or the first element on the right BIN).
     * This locks that element.  It throws control to thread 2.
     *
     * Thread 2 positions a cursor on the adjacent element on the other BIN
     * (either the first element on the right BIN or the last element on the
     * left BIN, resp.)  It throws control to thread 1.  After it signals
     * thread 1 to continue, thread 2 does either a getNext or getPrev.  This
     * should block because thread 1 has the next/prev element locked.
     *
     * Thread 1 then waits a short time (250ms) so that thread 2 can execute
     * the getNext/getPrev.  Thread 1 then inserts or deletes the "phantom
     * element" right in between the cursors that were set up in the previous
     * two steps, sleeps a second, and either commits or aborts.
     *
     * Thread 2 will then return from the getNext/getPrev.  The returned key
     * from the getNext/getPrev is then verified.
     *
     * The Serializable isolation level is not used for either thread so as to
     * allow phantoms; otherwise, this test would deadlock.
     *
     * These parameters are all configured through a PhantomTestConfiguration
     * instance passed to phantomWorker which has the template for the steps
     * described above.
     */

    /**
     * Phantom test inserting and committing a phantom while doing a getNext.
     */
    @Test
    public void testPhantomInsertGetNextCommit()
        throws Throwable {

        try {
            phantomWorker
                (new PhantomTestConfiguration
                 ("testPhantomInsertGetNextCommit",
                  "F", "D", "C", "D",
                  true, true, true));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Phantom test inserting and aborting a phantom while doing a getNext.
     */
    @Test
    public void testPhantomInsertGetNextAbort()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomInsertGetNextAbort",
              "F", "D", "C", "F",
              true, true, false));
    }

    /**
     * Phantom test inserting and committing a phantom while doing a getPrev.
     */
    @Test
    public void testPhantomInsertGetPrevCommit()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomInsertGetPrevCommit",
              "C", "F", "G", "F",
              true, false, true));
    }

    /**
     * Phantom test inserting and aborting a phantom while doing a getPrev.
     */
    @Test
    public void testPhantomInsertGetPrevAbort()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomInsertGetPrevAbort",
              "C", "F", "G", "C",
              true, false, false));
    }

    /**
     * Phantom test deleting and committing an edge element while doing a
     * getNext.
     */
    @Test
    public void testPhantomDeleteGetNextCommit()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomDeleteGetNextCommit",
              "F", "F", "C", "G",
              false, true, true));
    }

    /**
     * Phantom test deleting and aborting an edge element while doing a
     * getNext.
     */
    @Test
    public void testPhantomDeleteGetNextAbort()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomDeleteGetNextAbort",
              "F", "F", "C", "F",
              false, true, false));
    }

    /**
     * Phantom test deleting and committing an edge element while doing a
     * getPrev.
     */
    @Test
    public void testPhantomDeleteGetPrevCommit()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomDeleteGetPrevCommit",
              "F", "F", "G", "C",
              false, false, true));
    }

    /**
     * Phantom test deleting and aborting an edge element while doing a
     * getPrev.
     */
    @Test
    public void testPhantomDeleteGetPrevAbort()
        throws Throwable {

        phantomWorker
            (new PhantomTestConfiguration
             ("testPhantomDeleteGetPrevAbort",
              "F", "F", "G", "F",
              false, false, false));
    }

    /**
     * Phantom Dup test inserting and committing a phantom while doing a
     * getNext.
     */
    @Test
    public void testPhantomDupInsertGetNextCommit()
        throws Throwable {

        try {
            phantomDupWorker
                (new PhantomTestConfiguration
                 ("testPhantomDupInsertGetNextCommit",
                  "F", "D", "C", "D",
                  true, true, true));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Phantom Dup test inserting and aborting a phantom while doing a getNext.
     */
    @Test
    public void testPhantomDupInsertGetNextAbort()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupInsertGetNextAbort",
              "F", "D", "C", "F",
              true, true, false));
    }

    /**
     * Phantom Dup test inserting and committing a phantom while doing a
     * getPrev.
     */
    @Test
    public void testPhantomDupInsertGetPrevCommit()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupInsertGetPrevCommit",
              "C", "F", "G", "F",
              true, false, true));
    }

    /**
     * Phantom Dup test inserting and aborting a phantom while doing a getPrev.
     */
    @Test
    public void testPhantomDupInsertGetPrevAbort()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupInsertGetPrevAbort",
              "C", "F", "G", "C",
              true, false, false));
    }

    /**
     * Phantom Dup test deleting and committing an edge element while doing a
     * getNext.
     */
    @Test
    public void testPhantomDupDeleteGetNextCommit()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupDeleteGetNextCommit",
              "F", "F", "C", "G",
              false, true, true));
    }

    /**
     * Phantom Dup test deleting and aborting an edge element while doing a
     * getNext.
     */
    @Test
    public void testPhantomDupDeleteGetNextAbort()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupDeleteGetNextAbort",
              "F", "F", "C", "F",
              false, true, false));
    }

    /**
     * Phantom Dup test deleting and committing an edge element while doing a
     * getPrev.
     */
    @Test
    public void testPhantomDupDeleteGetPrevCommit()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupDeleteGetPrevCommit",
              "F", "F", "G", "C",
              false, false, true));
    }

    /**
     * Phantom Dup test deleting and aborting an edge element while doing a
     * getPrev.
     */
    @Test
    public void testPhantomDupDeleteGetPrevAbort()
        throws Throwable {

        phantomDupWorker
            (new PhantomTestConfiguration
             ("testPhantomDupDeleteGetPrevAbort",
              "F", "F", "G", "F",
              false, false, false));
    }

    private void phantomWorker(PhantomTestConfiguration c)
        throws Throwable {

        try {
            this.config = c;
            setupDatabaseAndEnv(false);

            if (config.doInsert &&
                !config.doGetNext) {

                Transaction txnDel =
                    env.beginTransaction(null, TransactionConfig.DEFAULT);

                /*
                 * Delete the first entry in the second bin so that we can
                 * reinsert it in tester1 and have it be the first entry in
                 * that bin.  If we left F and then tried to insert something
                 * to the left of F, it would end up in the first bin.
                 */
                assertEquals
                    (OperationStatus.SUCCESS,
                     db.delete(txnDel,
                               new DatabaseEntry(StringUtils.toUTF8("F"))));
                txnDel.commit();
            }

            JUnitThread tester1 =
                new JUnitThread(config.testName + "1") {
                    public void testBody()
                        throws Throwable {

                        Cursor cursor = null;
                        try {
                            Transaction txn1 =
                                env.beginTransaction(null, null);
                            cursor = db.openCursor(txn1, CursorConfig.DEFAULT);
                            OperationStatus status =
                                cursor.getSearchKey
                                (new DatabaseEntry(StringUtils.toUTF8
                                    (config.thread1EntryToLock)),
                                 new DatabaseEntry(),
                                 LockMode.RMW);
                            assertEquals(OperationStatus.SUCCESS, status);
                            sequence++;  // 0 -> 1

                            /* Wait for tester2 to position cursor. */
                            while (sequence < 2) {
                                Thread.yield();
                            }

                            if (config.doInsert) {
                                status = db.put
                                    (txn1,
                                     new DatabaseEntry
                                     (StringUtils.toUTF8(config.thread1OpArg)),
                                     new DatabaseEntry(new byte[10]));
                            } else {
                                status = db.delete
                                    (txn1,
                                     new DatabaseEntry
                                     (StringUtils.toUTF8(config.thread1OpArg)));
                            }
                            assertEquals(OperationStatus.SUCCESS, status);
                            sequence++;     // 2 -> 3

                            /*
                             * Since we can't increment sequence when tester2
                             * blocks on the getNext call, all we can do is
                             * bump sequence right before the getNext, and then
                             * wait a little in this thread for tester2 to
                             * block.
                             */
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException IE) {
                            }

                            cursor.close();
                            cursor = null;
                            if (config.doCommit) {
                                txn1.commit();
                            } else {
                                txn1.abort();
                            }
                        } catch (DatabaseException DBE) {
                            if (cursor != null) {
                                cursor.close();
                            }
                            DBE.printStackTrace();
                            fail("caught DatabaseException " + DBE);
                        }
                    }
                };

            JUnitThread tester2 =
                new JUnitThread(config.testName + "2") {
                    public void testBody()
                        throws Throwable {

                        Cursor cursor = null;
                        try {
                            Transaction txn2 =
                                env.beginTransaction(null, null);
                            txn2.setLockTimeout(LOCK_TIMEOUT);
                            cursor = db.openCursor(txn2, CursorConfig.DEFAULT);

                            /* Wait for tester1 to position cursor. */
                            while (sequence < 1) {
                                Thread.yield();
                            }

                            OperationStatus status =
                                cursor.getSearchKey
                                (new DatabaseEntry
                                 (StringUtils.toUTF8(config.thread2Start)),
                                 new DatabaseEntry(),
                                 LockMode.DEFAULT);
                            assertEquals(OperationStatus.SUCCESS, status);

                            sequence++;           // 1 -> 2

                            /* Wait for tester1 to insert/delete. */
                            while (sequence < 3) {
                                Thread.yield();
                            }

                            DatabaseEntry nextKey = new DatabaseEntry();
                            try {

                                /*
                                 * This will block until tester1 above commits.
                                 */
                                if (config.doGetNext) {
                                    status =
                                        cursor.getNext(nextKey,
                                                       new DatabaseEntry(),
                                                       LockMode.DEFAULT);
                                } else {
                                    status =
                                        cursor.getPrev(nextKey,
                                                       new DatabaseEntry(),
                                                       LockMode.DEFAULT);
                                }
                            } catch (DatabaseException DBE) {
                                System.out.println("t2 caught " + DBE);
                            }
                            assertEquals(3, sequence);
                            assertEquals(config.expectedResult,
                                         StringUtils.fromUTF8
                                         (nextKey.getData()));
                            cursor.close();
                            cursor = null;
                            txn2.commit();
                        } catch (DatabaseException DBE) {
                            if (cursor != null) {
                                cursor.close();
                            }
                            DBE.printStackTrace();
                            fail("caught DatabaseException " + DBE);
                        }
                    }
                };

            tester1.start();
            tester2.start();

            tester1.finishTest();
            tester2.finishTest();
        } finally {
            db.close();
            close(env);
            env = null;
        }
    }

    private void phantomDupWorker(PhantomTestConfiguration c)
        throws Throwable {

        Cursor cursor = null;
        try {
            this.config = c;
            setupDatabaseAndEnv(true);

            if (config.doInsert &&
                !config.doGetNext) {

                Transaction txnDel =
                    env.beginTransaction(null, TransactionConfig.DEFAULT);
                cursor = db.openCursor(txnDel, CursorConfig.DEFAULT);

                /*
                 * Delete the first entry in the second bin so that we can
                 * reinsert it in tester1 and have it be the first entry in
                 * that bin.  If we left F and then tried to insert something
                 * to the left of F, it would end up in the first bin.
                 */
                assertEquals(OperationStatus.SUCCESS, cursor.getSearchBoth
                             (new DatabaseEntry(StringUtils.toUTF8(DUPKEY)),
                              new DatabaseEntry(StringUtils.toUTF8("F")),
                              LockMode.DEFAULT));
                assertEquals(OperationStatus.SUCCESS, cursor.delete());
                cursor.close();
                cursor = null;
                txnDel.commit();
            }

            JUnitThread tester1 =
                new JUnitThread(config.testName + "1") {
                    public void testBody()
                        throws Throwable {

                        Cursor cursor = null;
                        Cursor c = null;
                        try {
                            Transaction txn1 =
                                env.beginTransaction(null, null);
                            cursor = db.openCursor(txn1, CursorConfig.DEFAULT);
                            OperationStatus status =
                                cursor.getSearchBoth
                                (new DatabaseEntry(StringUtils.toUTF8(DUPKEY)),
                                 new DatabaseEntry(StringUtils.toUTF8
                                     (config.thread1EntryToLock)),
                                 LockMode.RMW);
                            assertEquals(OperationStatus.SUCCESS, status);
                            cursor.close();
                            cursor = null;
                            sequence++;  // 0 -> 1

                            /* Wait for tester2 to position cursor. */
                            while (sequence < 2) {
                                Thread.yield();
                            }

                            if (config.doInsert) {
                                status = db.put
                                    (txn1,
                                     new DatabaseEntry
                                     (StringUtils.toUTF8(DUPKEY)),
                                     new DatabaseEntry
                                     (StringUtils.toUTF8
                                        (config.thread1OpArg)));
                            } else {
                                c = db.openCursor(txn1, CursorConfig.DEFAULT);
                                assertEquals(OperationStatus.SUCCESS,
                                             c.getSearchBoth
                                             (new DatabaseEntry
                                              (StringUtils.toUTF8(DUPKEY)),
                                              new DatabaseEntry
                                              (StringUtils.toUTF8
                                               (config.thread1OpArg)),
                                              LockMode.DEFAULT));
                                assertEquals(OperationStatus.SUCCESS,
                                             c.delete());
                                c.close();
                                c = null;
                            }
                            assertEquals(OperationStatus.SUCCESS, status);
                            sequence++;     // 2 -> 3

                            /*
                             * Since we can't increment sequence when tester2
                             * blocks on the getNext call, all we can do is
                             * bump sequence right before the getNext, and then
                             * wait a little in this thread for tester2 to
                             * block.
                             */
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException IE) {
                            }

                            if (config.doCommit) {
                                txn1.commit();
                            } else {
                                txn1.abort();
                            }
                        } catch (DatabaseException DBE) {
                            if (cursor != null) {
                                cursor.close();
                            }
                            if (c != null) {
                                c.close();
                            }
                            DBE.printStackTrace();
                            fail("caught DatabaseException " + DBE);
                        }
                    }
                };

            JUnitThread tester2 =
                new JUnitThread("testPhantomInsert2") {
                    public void testBody()
                        throws Throwable {

                        Cursor cursor = null;
                        try {
                            Transaction txn2 =
                                env.beginTransaction(null, null);
                            txn2.setLockTimeout(LOCK_TIMEOUT);
                            cursor = db.openCursor(txn2, CursorConfig.DEFAULT);

                            /* Wait for tester1 to position cursor. */
                            while (sequence < 1) {
                                Thread.yield();
                            }

                            OperationStatus status =
                                cursor.getSearchBoth
                                (new DatabaseEntry(StringUtils.toUTF8(DUPKEY)),
                                 new DatabaseEntry
                                 (StringUtils.toUTF8(config.thread2Start)),
                                 LockMode.DEFAULT);
                            assertEquals(OperationStatus.SUCCESS, status);

                            sequence++;           // 1 -> 2

                            /* Wait for tester1 to insert/delete. */
                            while (sequence < 3) {
                                Thread.yield();
                            }

                            DatabaseEntry nextKey = new DatabaseEntry();
                            DatabaseEntry nextData = new DatabaseEntry();
                            try {

                                /*
                                 * This will block until tester1 above commits.
                                 */
                                if (config.doGetNext) {
                                    status =
                                        cursor.getNextDup(nextKey, nextData,
                                                          LockMode.DEFAULT);
                                } else {
                                    status =
                                        cursor.getPrevDup(nextKey, nextData,
                                                          LockMode.DEFAULT);
                                }
                            } catch (DatabaseException DBE) {
                                System.out.println("t2 caught " + DBE);
                            }
                            assertEquals(3, sequence);
                            byte[] data = nextData.getData();
                            assertEquals(config.expectedResult,
                                         StringUtils.fromUTF8(data));
                            cursor.close();
                            cursor = null;
                            txn2.commit();
                        } catch (DatabaseException DBE) {
                            if (cursor != null) {
                                cursor.close();
                            }
                            DBE.printStackTrace();
                            fail("caught DatabaseException " + DBE);
                        }
                    }
                };

            tester1.start();
            tester2.start();

            tester1.finishTest();
            tester2.finishTest();
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            db.close();
            close(env);
            env = null;
        }
    }

    /**
     * Sets up a small database with a tree containing 2 bins, one with A, B,
     * and C, and the other with F, G, H, and I.
     */
    private void setupDatabaseAndEnv(boolean writeAsDuplicateData)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();

        /* RepeatableRead isolation is required by this test. */
        TestUtils.clearIsolationLevel(envConfig);

        DbInternal.disableParameterValidation(envConfig);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "6");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX_DUPTREE.getName(),
                                 "6");
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 "1024");
        envConfig.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                                 "true");
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        env = create(envHome, envConfig);
        Transaction txn = env.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(txn, "testDB", dbConfig);

        if (writeAsDuplicateData) {
            writeDuplicateData(db, txn);
        } else {
            writeData(db, txn);
        }

        txn.commit();
    }

    String[] dataStrings = {
        "A", "B", "C", "F", "G", "H", "I"
    };

    private void writeData(Database db, Transaction txn)
        throws DatabaseException {

        for (int i = 0; i < dataStrings.length; i++) {
            db.put(txn, new DatabaseEntry(StringUtils.toUTF8(dataStrings[i])),
                   new DatabaseEntry(new byte[10]));
        }
    }

    private void writeDuplicateData(Database db, Transaction txn)
        throws DatabaseException {

        for (int i = 0; i < dataStrings.length; i++) {
            db.put(txn, new DatabaseEntry(StringUtils.toUTF8(DUPKEY)),
                   new DatabaseEntry(StringUtils.toUTF8(dataStrings[i])));
        }
    }

    /**
     * Insert data over many databases.
     */
    private void insertMultiDb(int numDbs)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();

        /* RepeatableRead isolation is required by this test. */
        TestUtils.clearIsolationLevel(envConfig);

        DbInternal.disableParameterValidation(envConfig);
        envConfig.setTransactional(true);
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(), "1024");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_CHECK_LEAKS.getName(), "true");
        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX_DUPTREE.getName(), "6");
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.setAllowCreate(true);
        Environment env = create(envHome, envConfig);

        Database[] myDb = new Database[numDbs];
        Cursor[] cursor = new Cursor[numDbs];
        Transaction txn =
            env.beginTransaction(null, TransactionConfig.DEFAULT);

        /* In a non-replicated environment, the txn id should be positive. */
        assertTrue(txn.getId() > 0);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        for (int i = 0; i < numDbs; i++) {
            myDb[i] = env.openDatabase(txn, "testDB" + i, dbConfig);
            cursor[i] = myDb[i].openCursor(txn, CursorConfig.DEFAULT);

            /*
             * In a non-replicated environment, the db id should be
             * positive.
             */
            DatabaseImpl dbImpl = DbInternal.getDbImpl(myDb[i]);
            assertTrue(dbImpl.getId().getId() > 0);
        }

        /* Insert data in a round robin fashion to spread over log. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = NUM_RECS; i > 0; i--) {
            for (int c = 0; c < numDbs; c++) {
                key.setData(TestUtils.getTestArray(i + c));
                data.setData(TestUtils.getTestArray(i + c));
                if (DEBUG) {
                    System.out.println("i = " + i +
                                       TestUtils.dumpByteArray(key.getData()));
                }
                cursor[c].put(key, data);
            }
        }

        for (int i = 0; i < numDbs; i++) {
            cursor[i].close();
            myDb[i].close();
        }
        txn.commit();

        assertTrue(env.verify(null, System.err));
        close(env);
        env = null;

        envConfig.setAllowCreate(false);
        env = create(envHome, envConfig);

        /*
         * Before running the verifier, run the cleaner to make sure it has
         * completed.  Otherwise, the cleaner will be running when we call
         * verify, and open txns will be reported.
         */
        env.cleanLog();

        env.verify(null, System.err);

        /* Check each db in turn, using null transactions. */
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(false);
        for (int d = 0; d < numDbs; d++) {
            Database checkDb = env.openDatabase(null, "testDB" + d,
                                                dbConfig);
            Cursor myCursor = checkDb.openCursor(null, CursorConfig.DEFAULT);

            OperationStatus status =
                myCursor.getFirst(key, data, LockMode.DEFAULT);

            int i = 1;
            while (status == OperationStatus.SUCCESS) {
                byte[] expectedKey = TestUtils.getTestArray(i + d);
                byte[] expectedData = TestUtils.getTestArray(i + d);

                if (DEBUG) {
                    System.out.println("Database " + d + " Key " + i +
                                       " expected = " +
                                       TestUtils.dumpByteArray(expectedKey) +
                                       " seen = " +
                                       TestUtils.dumpByteArray(key.getData()));
                }

                assertTrue("Database " + d + " Key " + i + " expected = " +
                           TestUtils.dumpByteArray(expectedKey) +
                           " seen = " +
                           TestUtils.dumpByteArray(key.getData()),
                           Arrays.equals(expectedKey, key.getData()));
                assertTrue("Data " + i, Arrays.equals(expectedData,
                                                      data.getData()));
                i++;

                status = myCursor.getNext(key, data, LockMode.DEFAULT);
            }
            myCursor.close();
            assertEquals("Number recs seen", NUM_RECS, i-1);
            checkDb.close();
        }
        close(env);
        env = null;
    }

    /**
     * This is a rudimentary test of DbInternal.search, just to make sure we're
     * passing parameters down correctly the RangeCursor. RangeCursor is tested
     * thoroughly elsewhere.
     */
    @Test
    public void testDbInternalSearch() {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        final Environment env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        final Database db = env.openDatabase(null, "testDB", dbConfig);

        insert(db, 1, 1);
        insert(db, 3, 3);
        insert(db, 5, 5);

        final Cursor cursor = db.openCursor(null, null);

        checkSearch(cursor, Search.GT,  0, 1);
        checkSearch(cursor, Search.GTE, 0, 1);
        checkSearch(cursor, Search.GT,  1, 3);
        checkSearch(cursor, Search.GTE, 1, 1);
        checkSearch(cursor, Search.GT,  2, 3);
        checkSearch(cursor, Search.GTE, 2, 3);
        checkSearch(cursor, Search.GT,  3, 5);
        checkSearch(cursor, Search.GTE, 3, 3);
        checkSearch(cursor, Search.GT,  4, 5);
        checkSearch(cursor, Search.GTE, 4, 5);
        checkSearch(cursor, Search.GT,  5, -1);
        checkSearch(cursor, Search.GTE, 5, 5);
        checkSearch(cursor, Search.GT,  6, -1);
        checkSearch(cursor, Search.GTE, 6, -1);

        checkSearch(cursor, Search.LT,  0, -1);
        checkSearch(cursor, Search.LTE, 0, -1);
        checkSearch(cursor, Search.LT,  1, -1);
        checkSearch(cursor, Search.LTE, 1, 1);
        checkSearch(cursor, Search.LT,  2, 1);
        checkSearch(cursor, Search.LTE, 2, 1);
        checkSearch(cursor, Search.LT,  3, 1);
        checkSearch(cursor, Search.LTE, 3, 3);
        checkSearch(cursor, Search.LT,  4, 3);
        checkSearch(cursor, Search.LTE, 4, 3);
        checkSearch(cursor, Search.LT,  5, 3);
        checkSearch(cursor, Search.LTE, 5, 5);
        checkSearch(cursor, Search.LT,  6, 5);
        checkSearch(cursor, Search.LTE, 6, 5);

        cursor.close();
        db.close();
        close(env);
    }

    private void insert(Database db, int key, int data) {
        final DatabaseEntry keyEntry =
            new DatabaseEntry(new byte[] { (byte) key });
        final DatabaseEntry dataEntry =
            new DatabaseEntry(new byte[] { (byte) data });

        final OperationStatus status = db.put(null, keyEntry, dataEntry);
        assertSame(OperationStatus.SUCCESS, status);
    }

    private void checkSearch(Cursor cursor,
                             Search searchMode,
                             int searchKey,
                             int expectKey) {

        final DatabaseEntry key = new DatabaseEntry(
            new byte[] { (byte) searchKey });

        final DatabaseEntry data = new DatabaseEntry();

        final OperationStatus status = DbInternal.search(
            cursor, key, null, data, searchMode, (LockMode) null);

        if (expectKey < 0) {
            assertSame(OperationStatus.NOTFOUND, status);
            return;
        }

        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(expectKey, key.getData()[0]);
        assertEquals(expectKey, data.getData()[0]);
    }

    /**
     * This is a rudimentary test of DbInternal.searchBoth, just to make sure
     * we're passing parameters down correctly the RangeCursor. RangeCursor is
     * tested thoroughly elsewhere.
     */
    @Test
    public void testDbInternalSearchBoth() {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        final Environment env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        final Database db = env.openDatabase(null, "testDB", dbConfig);

        final SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(true);
        secConfig.setTransactional(true);
        secConfig.setSortedDuplicates(true);
        secConfig.setKeyCreator(new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData(data.getData());
                return true;
            }
        });
        final SecondaryDatabase secDb = env.openSecondaryDatabase(
            null, "testDupsDB", db, secConfig);

        insert(db, 0, 1);
        insert(db, 1, 2);
        insert(db, 3, 2);
        insert(db, 5, 2);

        final SecondaryCursor cursor = secDb.openCursor(null, null);

        checkSearchBoth(cursor, Search.GT,  2, 0, 1);
        checkSearchBoth(cursor, Search.GTE, 2, 0, 1);
        checkSearchBoth(cursor, Search.GT,  2, 1, 3);
        checkSearchBoth(cursor, Search.GTE, 2, 1, 1);
        checkSearchBoth(cursor, Search.GT,  2, 2, 3);
        checkSearchBoth(cursor, Search.GTE, 2, 2, 3);
        checkSearchBoth(cursor, Search.GT,  2, 3, 5);
        checkSearchBoth(cursor, Search.GTE, 2, 3, 3);
        checkSearchBoth(cursor, Search.GT,  2, 4, 5);
        checkSearchBoth(cursor, Search.GTE, 2, 4, 5);
        checkSearchBoth(cursor, Search.GT,  2, 5, -1);
        checkSearchBoth(cursor, Search.GTE, 2, 5, 5);
        checkSearchBoth(cursor, Search.GT,  2, 6, -1);
        checkSearchBoth(cursor, Search.GTE, 2, 6, -1);

        checkSearchBoth(cursor, Search.LT,  2, 0, -1);
        checkSearchBoth(cursor, Search.LTE, 2, 0, -1);
        checkSearchBoth(cursor, Search.LT,  2, 1, -1);
        checkSearchBoth(cursor, Search.LTE, 2, 1, 1);
        checkSearchBoth(cursor, Search.LT,  2, 2, 1);
        checkSearchBoth(cursor, Search.LTE, 2, 2, 1);
        checkSearchBoth(cursor, Search.LT,  2, 3, 1);
        checkSearchBoth(cursor, Search.LTE, 2, 3, 3);
        checkSearchBoth(cursor, Search.LT,  2, 4, 3);
        checkSearchBoth(cursor, Search.LTE, 2, 4, 3);
        checkSearchBoth(cursor, Search.LT,  2, 5, 3);
        checkSearchBoth(cursor, Search.LTE, 2, 5, 5);
        checkSearchBoth(cursor, Search.LT,  2, 6, 5);
        checkSearchBoth(cursor, Search.LTE, 2, 6, 5);

        cursor.close();
        secDb.close();
        db.close();
        close(env);
    }

    private void checkSearchBoth(Cursor cursor,
                                 Search searchMode,
                                 int searchKey,
                                 int searchPKey,
                                 int expectPKey) {

        final DatabaseEntry key = new DatabaseEntry(
            new byte[] { (byte) searchKey });

        final DatabaseEntry pKey = new DatabaseEntry(
            new byte[] { (byte) searchPKey });

        final DatabaseEntry data = new DatabaseEntry();

        final OperationStatus status = DbInternal.searchBoth(
            cursor, key, pKey, data, searchMode, (LockMode) null);

        if (expectPKey < 0) {
            assertSame(OperationStatus.NOTFOUND, status);
            return;
        }

        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(expectPKey, pKey.getData()[0]);
        assertEquals(searchKey, data.getData()[0]);
    }

    /**
     * Checks that Cursor.getSearchKeyRange (as well as internal range
     * searches) works even when insertions occur while doing a getNextBin in
     * the window where no latches are held. In particular there is a scenario
     * where it did not work, if a split during getNextBin arranges things in
     * a particular way.  This is a very specific scenario and requires many
     * insertions in the window, so it seems unlikely to occur in the wild.
     * This test creates that scenario.
     */
    @Test
    public void testInsertionDuringGetNextBinDuringRangeSearch() {

        /*
         * Disable daemons for predictability.  Disable BIN deltas so we can
         * compress away a deleted slot below (if a delta would be logged next,
         * slots won't be compressed).
         */
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.TREE_BIN_DELTA, "0");
        env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        final Database db = env.openDatabase(null, "testDB", dbConfig);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();
        OperationStatus status;

        /*
         * Create a tree that contains:
         *   A BIN with keys 0-63.
         *   Additional BINs with keys 1000-1063.
         *
         * Keys 1000-1063 are inserted in reverse order to make sure the split
         * occurs in the middle of the BIN (rather than a "special" split).
         *
         * Key 64 is deleted so that key 63 will be the last one in the BIN
         * when a split occurs later on, in the hook method below.
         */
        for (int i = 0; i < 64; i += 1) {
            insertRecord(db, i);
        }
        for (int i = 1063; i >= 1000; i -= 1) {
            insertRecord(db, i);
        }
        insertRecord(db, 64);
        deleteRecord(db, 64);
        env.compress();

        /*
         * Set a hook that will be called during the window when no INs are
         * latched when getNextBin is called when Cursor.searchInternal
         * processes a range search for key 500.  searchInternal first calls
         * CursorImpl.searchAndPosition which lands on key 63.  It then calls
         * CursorImpl.getNext, which does the getNextBin and calls the hook.
         */
        DbInternal.getDbImpl(db).getTree().setGetParentINHook(
            new TestHook() {
                @Override
                public void doHook() {
                    /* Only process the first call to the hook. */
                    DbInternal.getDbImpl(db).getTree().
                        setGetParentINHook(null);
                    /*
                     * Cause a split, leaving keys 0-63 in the first BIN and
                     * keys 64-130 in the second BIN.
                     */
                    for (int i = 64; i < 130; i += 1) {
                        insertRecord(db, i);
                    }
                }
                @Override public void hookSetup() { }
                @Override public void doIOHook() { }
                @Override public void doHook(Object obj) { }
                @Override public Object getHookValue() { return null; }
            }
        );

        /*
         * Search for a key >= 500, which should find key 1000.  But due to the
         * bug, CursorImpl.getNext advances to key 64 in the second BIN, and
         * returns it.
         */
        IntegerBinding.intToEntry(500, key);
        final Cursor c = db.openCursor(null, null);
        status = c.getSearchKeyRange(key, value, null);

        c.close();
        db.close();
        close(env);

        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(1000, IntegerBinding.entryToInt(key));
    }

    private void insertRecord(Database db, int key) {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry(new byte[1]);
        IntegerBinding.intToEntry(key, keyEntry);
        final OperationStatus status =
            db.putNoOverwrite(null, keyEntry, dataEntry);
        assertSame(OperationStatus.SUCCESS, status);
    }

    private void deleteRecord(Database db, int key) {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        IntegerBinding.intToEntry(key, keyEntry);
        final OperationStatus status = db.delete(null, keyEntry);
        assertSame(OperationStatus.SUCCESS, status);
    }

    @Test
    public void testGetStorageSize() {
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "testDB", dbConfig);

        final DatabaseEntry key = new DatabaseEntry(new byte[1]);
        final DatabaseEntry value = new DatabaseEntry();

        Transaction t = env.beginTransaction(null, null);
        Cursor c = db.openCursor(t, null);

        value.setData(new byte[100]);
        OperationResult r = c.put(key, value, Put.OVERWRITE, null);
        assertNotNull(r);
        final int separateLN1 = DbInternal.getCursorImpl(c).getStorageSize();

        r = c.get(key, value, Get.FIRST, null);
        assertNotNull(r);
        final int separateLN2 = DbInternal.getCursorImpl(c).getStorageSize();

        value.setData(new byte[10]);
        r = c.put(key, value, Put.OVERWRITE, null);
        assertNotNull(r);
        final int embeddedLN1 = DbInternal.getCursorImpl(c).getStorageSize();

        r = c.get(key, value, Get.FIRST, null);
        assertNotNull(r);
        final int embeddedLN2 = DbInternal.getCursorImpl(c).getStorageSize();

        c.close();
        t.commit();
        db.close();

        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "testDBDups", dbConfig);

        t = env.beginTransaction(null, null);
        c = db.openCursor(t, null);

        value.setData(new byte[10]);
        r = c.put(key, value, Put.OVERWRITE, null);
        assertNotNull(r);
        final int duplicateLN1 = DbInternal.getCursorImpl(c).getStorageSize();

        r = c.get(key, value, Get.FIRST, null);
        assertNotNull(r);
        final int duplicateLN2 = DbInternal.getCursorImpl(c).getStorageSize();

        c.close();
        t.commit();
        db.close();

        dbConfig.setSortedDuplicates(false);
        db = env.openDatabase(null, "testDBPri", dbConfig);

        final SecondaryConfig sdbConfig = new SecondaryConfig();
        sdbConfig.setAllowCreate(true);
        sdbConfig.setTransactional(true);
        sdbConfig.setKeyCreator(new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData(key.getData());
                return true;
            }
        });
        final SecondaryDatabase sdb = env.openSecondaryDatabase(
            null, "testDBSec", db, sdbConfig);

        t = env.beginTransaction(null, null);
        c = db.openCursor(t, null);
        SecondaryCursor sc = sdb.openCursor(t, null);

        value.setData(new byte[100]);
        r = c.put(key, value, Put.OVERWRITE, null);
        assertNotNull(r);
        final int priRec1 = DbInternal.getCursorImpl(c).getStorageSize();

        r = sc.get(key, value, Get.FIRST, null);
        assertNotNull(r);
        final int secEqPri1 = DbInternal.getCursorImpl(sc).getStorageSize();

        r = sc.get(key, null, Get.FIRST, null);
        assertNotNull(r);
        final int secNePri1 = DbInternal.getCursorImpl(sc).getStorageSize();

        sc.close();
        c.close();
        t.commit();
        sdb.close();
        db.close();

        db = env.openDatabase(null, "testDBSec", dbConfig);
        t = env.beginTransaction(null, null);
        c = db.openCursor(t, null);

        r = c.get(key, value, Get.FIRST, null);
        assertNotNull(r);
        final int secNePri2 = DbInternal.getCursorImpl(c).getStorageSize();

        /* Check whether embedded LNs are disabled in this test. */
        final boolean embeddedLNsConfigured =
            DbInternal.getEnvironmentImpl(env).getMaxEmbeddedLN() >= 10;

        c.close();
        t.commit();
        db.close();

        close(env);

        /*
         * Exact sizes are checked below because these have been manually
         * confirmed to be roughly the size expected. If something changes in
         * the code above, the exact sizes below may need to be adjusted.
         *
         * The StorageSize.getStorageSize javadoc explains the large inaccuracy
         * for the embedded LN and the smaller inaccuracy for the separate LN.
         * The duplicate LN size is accurate, and this accuracy matters because
         * the total size is small.
         *
         * The embedded LN size is 2 bytes larger than the duplicate LN size
         * because PRI_SLOT_OVERHEAD - SEC_SLOT_OVERHEAD = 2.
         */
        final boolean rep = isReplicatedTest(this.getClass());

        /* Customer formula: 100 (data size) + 2 * 1 (key size) + 64 = 166 */
        assertEquals(rep ? 144 : 135, separateLN1);
        assertEquals(separateLN1, separateLN2);

        if (embeddedLNsConfigured) {
            /* Customer formula: 10 (data size) + 2 * 1 (key size) + 64 = 76 */
            assertEquals(31, embeddedLN1);
            assertEquals(embeddedLN1, embeddedLN2);
        }

        /* Customer formula: 10 (data size) + 1 (key size) + 12 = 23 */
        assertEquals(23, duplicateLN1);
        assertEquals(duplicateLN1, duplicateLN2);

        /* Sec cursor returns pri size, but only when reading pri data. */
        assertEquals(priRec1, secEqPri1);
        assertTrue(priRec1 > secNePri1);
        assertEquals(secNePri1, secNePri2);

        /*
         * We do not show the actual storage sizes here because we don't have a
         * simple way to serialize a single slot.
         */
    }
}
