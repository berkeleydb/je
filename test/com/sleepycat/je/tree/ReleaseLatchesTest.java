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
package com.sleepycat.je.tree;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * @excludeDualMode
 * Check that latches are release properly even if we run into read errors.
 */
@RunWith(Parameterized.class)
public class ReleaseLatchesTest extends TestBase {
    private static final boolean DEBUG = false;

    private Environment env;
    private final File envHome;
    private Database db;
    private TestDescriptor testActivity;

    /*
     * The OPERATIONS declared here define the test cases for this test.  Each
     * TestDescriptor describes a particular JE activity. The
     * testCheckLatchLeaks method generates read i/o exceptions during the test
     * descriptor's action, and will check that we come up clean.
     */
    public static TestDescriptor[] OPERATIONS = {

        /*
         * TestDescriptor params:
         *  - operation name: for debugging
         *  - number of times to generate an exception. For example if N,
         *   the test action will be executed in a loop N times, with an
         *   read/io on read 1, read 2, read 3 ... read n-1
         *  - number of records in the database.
         */
        new TestDescriptor("database put", 6, 30, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.populate(false);
            }

            @Override
            void reinit(ReleaseLatchesTest test)
                throws DatabaseException{

                test.closeDb();
                    test.getEnv().truncateDatabase(null, "foo", false);
            }
        },
        new TestDescriptor("cursor scan", 31, 20, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.scan();
            }
        },
        new TestDescriptor("cursor scan duplicates", 23, 3, true) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.scan();
            }
        },
//*
        new TestDescriptor("database get", 31, 20, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.get();
            }
        },
//*/
        new TestDescriptor("database delete", 40, 30, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.delete();
            }

            @Override
            void reinit(ReleaseLatchesTest test)
                throws DatabaseException{

                test.populate(false);
            }
        },
        new TestDescriptor("checkpoint", 40, 10, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.modify(exceptionCount);
                CheckpointConfig config = new CheckpointConfig();
                config.setForce(true);
                if (DEBUG) {
                    System.out.println("Got to checkpoint");
                }
                test.getEnv().checkpoint(config);
            }
        },
        new TestDescriptor("clean", 100, 5, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                test.modify(exceptionCount);
                CheckpointConfig config = new CheckpointConfig();
                config.setForce(true);
                if (DEBUG) {
                    System.out.println("Got to cleaning");
                }
                test.getEnv().cleanLog();
            }
        },
        new TestDescriptor("compress", 20, 10, false) {
            @Override
            void doAction(ReleaseLatchesTest test, int exceptionCount)
                throws DatabaseException {

                     test.delete();
                     if (DEBUG) {
                         System.out.println("Got to compress");
                     }
                     test.getEnv().compress();
            }

            @Override
            void reinit(ReleaseLatchesTest test)
                throws DatabaseException{

                test.populate(false);
            }
        }
    };

    @Parameters
    public static List<Object[]> genParams() {
        List<Object[]> list = new ArrayList<Object[]>();
        for (TestDescriptor action : OPERATIONS)
            list.add(new Object[]{action});
        
        return list;
     }

    public ReleaseLatchesTest(TestDescriptor action) {

        envHome = SharedTestUtils.getTestDir();
        testActivity = action;
        customName = action.getName();
    }

    private void init(boolean duplicates)
        throws DatabaseException {

        openEnvAndDb();

        populate(duplicates);
        env.checkpoint(null);
        db.close();
        db = null;
        env.close();
        env = null;
    }

    private void openEnvAndDb()
        throws DatabaseException {

        /*
         * Make an environment with small nodes and no daemons.
         */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "4");
        envConfig.setConfigParam("je.env.runEvictor", "false");
        envConfig.setConfigParam("je.env.runCheckpointer", "false");
        envConfig.setConfigParam("je.env.runCleaner", "false");
        envConfig.setConfigParam("je.env.runINCompressor", "false");
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "90");
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                  Integer.toString(20000));
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    /* Calling close under -ea will check for leaked latches. */
    private void doCloseAndCheckLeaks()
        throws Throwable {

        try {
            if (db != null) {
                db.close();
                db = null;
            }

            if (env != null) {
                env.close();
                env = null;
            }
        } catch (Throwable t) {
            System.out.println("operation = " + testActivity.name);
            t.printStackTrace();
            throw t;
        }
    }

    private void closeDb()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
    }

    private Environment getEnv() {
        return env;
    }

    /*
     * This is the heart of the unit test. Given a TestDescriptor, run the
     * operation's activity in a loop, generating read i/o exceptions at
     * different points. Check for latch leaks after the i/o exception
     * happens.
     */
    @Test
    public void testCheckLatchLeaks()
        throws Throwable {

        int maxExceptionCount = testActivity.getNumExceptions();
        if (DEBUG) {
            System.out.println("Starting test: " + testActivity.getName());
        }

        try {
            init(testActivity.getDuplicates());

            /*
             * Run the action repeatedly, generating exceptions at different
             * points.
             */
            for (int i = 1; i <= maxExceptionCount; i++) {

                /*
                 * Open the env and database anew each time, so that we need to
                 * fault in objects and will trigger read i/o exceptions.
                 */
                openEnvAndDb();
                EnvironmentImpl envImpl =
                    DbInternal.getNonNullEnvImpl(env);
                boolean exceptionOccurred = false;

                try {
                    ReadIOExceptionHook readHook = new ReadIOExceptionHook(i);
                    envImpl.getLogManager().setReadHook(readHook);
                    testActivity.doAction(this, i);
                } catch (Throwable e) {
                    if (!env.isValid()) {

                        /*
                         * It's possible for a read error to induce a
                         * RunRecoveryException if the read error happens when
                         * we are opening a new write file channel. (We read
                         * and validate the file header). In that case, check
                         * for latches, and re-open the database.
                         */
                        checkLatchCount((DatabaseException) e, i);
                        env.close();
                        openEnvAndDb();
                        envImpl = DbInternal.getNonNullEnvImpl(env);
                        exceptionOccurred = true;
                    } else if (e instanceof DatabaseException) {
                        checkLatchCount((DatabaseException) e, i);
                        exceptionOccurred = true;
                    } else {
                        throw e;
                    }
                }

                if (DEBUG && !exceptionOccurred) {
                    System.out.println("Don't need ex count " + i +
                                       " for test activity " +
                                       testActivity.getName());
                }

                envImpl.getLogManager().setReadHook(null);
                testActivity.reinit(this);
                doCloseAndCheckLeaks();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void checkLatchCount(DatabaseException e,
                                 int exceptionCount)
        throws DatabaseException {

        /* Only rethrow the exception if we didn't clean up latches. */
        if (LatchSupport.nBtreeLatchesHeld() > 0) {
            LatchSupport.dumpBtreeLatchesHeld();
            System.out.println("Operation = " + testActivity.getName() +
                               " exception count=" + exceptionCount +
                               " Held latches = " +
                               LatchSupport.nBtreeLatchesHeld());
            /* Show stacktrace where the latch was lost. */
            e.printStackTrace();
            throw e;
        }
    }

    /* Insert records into a database. */
    private void populate(boolean duplicates)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry data1 = new DatabaseEntry();
        DatabaseEntry data2 = new DatabaseEntry();
        DatabaseEntry data3 = new DatabaseEntry();
        DatabaseEntry data4 = new DatabaseEntry();
        IntegerBinding.intToEntry(0, data);
        IntegerBinding.intToEntry(1, data1);
        IntegerBinding.intToEntry(2, data2);
        IntegerBinding.intToEntry(3, data3);
        IntegerBinding.intToEntry(4, data4);

        for (int i = 0; i < testActivity.getNumRecords(); i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS,  db.put(null, key, data));
            if (duplicates) {
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null, key, data1));
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null, key, data2));
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null, key, data3));
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null, key, data4));
            }
        }
    }

    /* Modify the database. */
    private void modify(int dataVal)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(dataVal, data);

        for (int i = 0; i < testActivity.getNumRecords(); i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS,  db.put(null, key, data));
        }
    }

    /* Cursor scan the data. */
    private void scan()
        throws DatabaseException {

        Cursor cursor = null;
        try {
            cursor = db.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            while (cursor.getNext(key, data, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /* Database.get() for all records. */
    private void get()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < testActivity.getNumRecords(); i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS,
                         db.get(null, key, data, LockMode.DEFAULT));
        }
    }

    /* Delete all records. */
    private void delete()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        for (int i = 0; i < testActivity.getNumRecords(); i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals("key = " + IntegerBinding.entryToInt(key),
                         OperationStatus.SUCCESS, db.delete(null, key));
        }
    }

    /*
     * This TestHook implementation generates io exceptions during reads.
     */
    static class ReadIOExceptionHook implements TestHook {
        private int counter = 0;
        private final int throwCount;

        ReadIOExceptionHook(int throwCount) {
            this.throwCount = throwCount;
        }
        public void doIOHook()
            throws IOException {

            if (throwCount == counter) {
                counter++;
                throw new IOException("Generated exception: " +
                                      this.getClass().getName());
            } else {
                counter++;
            }
        }
        public Object getHookValue() {
            throw new UnsupportedOperationException();
        }
        public void doHook() {
            throw new UnsupportedOperationException();
        }
        public void hookSetup() {
            throw new UnsupportedOperationException();
        }
        public void doHook(Object obj) {
            throw new UnsupportedOperationException();
        }
    }

    static abstract class TestDescriptor {
        private final String name;
        private final int numExceptions;
        private final int numRecords;
        private final boolean duplicates;

        TestDescriptor(String name,
                       int numExceptions,
                       int numRecords,
                       boolean duplicates) {
            this.name = name;
            this.numExceptions = numExceptions;
            this.numRecords = numRecords;
            this.duplicates = duplicates;
        }

        int getNumRecords() {
            return numRecords;
        }

        int getNumExceptions() {
            return numExceptions;
        }

        String getName() {
            return name;
        }

        boolean getDuplicates() {
            return duplicates;
        }

        /* Do a series of operations. */
        abstract void doAction(ReleaseLatchesTest test,
                               int exceptionCount)
            throws DatabaseException;

        /**
         * Reinitialize the database if doAction modified it.
         * @throws DatabaseException from subclasses.
         */
        void reinit(ReleaseLatchesTest test)
            throws DatabaseException {

        }
    }
}
