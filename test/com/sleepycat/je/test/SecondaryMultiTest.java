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
import static org.junit.Assert.fail;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Tests involving secondary indexes with respect to deadlock free access.
 * Access is performed using Database, SecondaryDatabase, Cursor and
 * SecondaryCursor. When serialization is NOT being used, concurrent access
 * of a record should not deadlock.
 *
 * @author dwchung
 *
 */
@RunWith(Parameterized.class)
public class SecondaryMultiTest extends TestBase {

    private enum ReadType {GETSEARCHKEY, GETSEARCHKEY2, GETSEARCHBOTH,
                           GETSEARCHBOTH2, GETSEARCHKEYRANGE,
                           GETSEARCHKEYRANGE2};
    private final ReadType[] readTypes = ReadType.values();
    private static final String DB_NAME = "foo";
    private static final String SDB_NAME = "fooSec";
    private volatile int currentEvent;
    private volatile boolean testDone = false;
    private volatile int threadid = 0;
    private final boolean db1UsePrimary;
    private final boolean db2UsePrimary;
    private final boolean useSerialization;
    Environment env = null;
    Database db = null;
    SecondaryDatabase sdb = null;

    public SecondaryMultiTest(boolean db1UsePrimary,
                              boolean db2UsePrimary,
                              boolean useSerialization) {
        this.db1UsePrimary = db1UsePrimary;
        this.db2UsePrimary = db2UsePrimary;
        this.useSerialization = useSerialization;
    }

    @Parameters
    public static List<Object[]> genParams() {
       return paramsHelper(false);
    }

    /*
     * The parameters for the test are three booleans
     * db1UsePrimary, db2UsePrimary, useSeralization.
     * Test may access the data using the various
     * combinations of primary/secondary access
     * methods.
     */
    private static List<Object[]> paramsHelper(boolean rep) {
        final List<Object[]> newParams = new ArrayList<Object[]>();
        newParams.add(new Object[] {false, false, false});
        newParams.add(new Object[] {false, false, true});
        newParams.add(new Object[] {false, true, false});
        newParams.add(new Object[] {true, false, false});

        /*
         * The next two tests cause deadlocks. The tests
         * are written to handle them, but the time the
         * test runs is not predictable. The tests are
         * currently commented out since this set of tests
         * target secondary index deadlock avoidance.
         * Both of the tests use serialization, which is
         * not part of the "deadlock avoidance project.
        newParams.add(new Object[] {false, true, true});
        newParams.add(new Object[] {true, false, true});
        */
        newParams.add(new Object[] {true, true, false});
        newParams.add(new Object[] {true, true, true});

        return newParams;
    }

    /*
     *
     */
    @Before
    public void setup() {
        File envHome = SharedTestUtils.getTestDir();

        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setTxnTimeout(5, TimeUnit.SECONDS);
        envConfig.setLockTimeout(5, TimeUnit.SECONDS);
        envConfig.setTxnSerializableIsolation(useSerialization);

        env = new Environment(envHome, envConfig);

        /* Open a database and insert some data. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
        sdb = openSecondary(env, db, SDB_NAME, new SecondaryConfig());
    }

    @After
    public void tearDown() throws Exception {
        if (sdb != null) {
            sdb.close();
            sdb = null;
        }

        if (db != null) {
            db.close();
            db = null;
        }

        if (env != null) {
            env.close();
            env = null;
        }
    }

    /**
     * Have two threads attempt to delete the same record.
     * One thread commits the other should get not found.
     */
    @Test
    public void testMultiDelete() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        DeleteIt t1 =
            new DeleteIt(KEY, env, db1UsePrimary ? db : sdb,
                         0, 3, 1000);
        DeleteIt t2 =
            new DeleteIt(KEY, env, db2UsePrimary ? db : sdb,
                         2, 4, 1000);

        new Thread(t1).start();
        new Thread(t2).start();
        while (currentEvent < 6 && !testDone) {
            try {
                Thread.sleep(1000);
            } catch (ThreadInterruptedException e) {

            }
        }

        assertSame(t1.getResult(), OperationStatus.SUCCESS);
        assertSame(t2.getResult(), OperationStatus.NOTFOUND);
    }

    /**
     * Have multiple threads trying to delete the same record.
     *
     * @throws Exception
     */
    @Test
    public void testMultiDeleteUnordered() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int DELETE_ERS = 20;

        Thread[] threads = new Thread[DELETE_ERS];
        DeleteIt[] deleters = new DeleteIt[DELETE_ERS];

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < threads.length; i++) {
            Database tdb;
            if ((i % 2) == 0) {
                tdb = db1UsePrimary ? db : sdb;
            } else {
                tdb = db2UsePrimary ? db : sdb;
            }
            deleters[i] =
                new DeleteIt(KEY, env, tdb, 0, deleters.length / 2, 1);
            threads[i] = new Thread(deleters[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        int successcount = 0;

        for (int i = 0; i < threads.length; i++) {
            OperationStatus result = deleters[i].getResult();
            if (result == OperationStatus.SUCCESS) {
                successcount++;
            } else {
                assertSame(result, OperationStatus.NOTFOUND);
            }
        }

        /*
         * Exactly one deleter should have succeeded.
         */
        assertEquals(successcount, 1);
    }

    /*
     * Have multiple readers and writer threads accessing the
     * same record.
     */
    @Test
    public void testMultiReadInsert() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int RTHREADS = 20;
        final int WTHREADS = 2;

        Thread[] threads = new Thread[RTHREADS + WTHREADS];
        ReadIt[] readers = new ReadIt[RTHREADS];
        InsertIt[] writers = new InsertIt[WTHREADS];
        int curReadType = 0;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            if (i == KEY) {
                continue;
            }
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < readers.length; i++) {
            Database tdb;
            if ((i % 2) == 0) {
                tdb = db1UsePrimary ? db : sdb;
            } else {
                tdb = db2UsePrimary ? db : sdb;
            }
            ReadType trt = ReadType.GETSEARCHKEY;
            if (tdb instanceof SecondaryDatabase) {
                trt = readTypes[curReadType % readTypes.length];
                curReadType = curReadType + 1;
            }

            readers[i] =
                    new ReadIt(KEY, env, tdb, 0, 0, 1,
                               OperationStatus.NOTFOUND, trt);
            threads[i] = new Thread(readers[i]);
        }

        for (int i = 0; i < writers.length; i++) {
            writers[i] = new InsertIt(KEY, env, db, 0, 0, 1);
            threads[i + readers.length] = new Thread(writers[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        for (int i = 0; i < readers.length; i++) {
            OperationStatus result = readers[i].getResult();
            assertSame(result, OperationStatus.SUCCESS);
        }

        for (int i = 0; i < writers.length; i++) {
            OperationStatus result = writers[i].getResult();
            assertSame(result, OperationStatus.SUCCESS);
        }
    }

    /*
     * Have multiple readers accessing a record that is deleted.
     */
    @Test
    public void testMultiReadDelete() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int RTHREADS = 20;
        final int WTHREADS = 1;

        Thread[] threads = new Thread[RTHREADS + WTHREADS];
        ReadIt[] readers = new ReadIt[RTHREADS];
        DeleteIt[] writers = new DeleteIt[WTHREADS];
        int curReadType = 0;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < readers.length; i++) {
            Database tdb;
            if ((i % 2) == 0) {
                tdb = db1UsePrimary ? db : sdb;
            } else {
                tdb = db2UsePrimary ? db : sdb;
            }
            ReadType trt = tdb instanceof SecondaryDatabase ?
                                readTypes[curReadType++ % readTypes.length] :
                                ReadType.GETSEARCHKEY;
            readers[i] =
                    new ReadIt(KEY, env, tdb, 0, 0, 1,
                               OperationStatus.SUCCESS, trt);
            threads[i] = new Thread(readers[i]);
        }

        for (int i = 0; i < writers.length; i++) {
            writers[i] =
                new DeleteIt(KEY, env, db1UsePrimary ? db : sdb,
                             0, readers.length / 2, 1);
            threads[i + readers.length] = new Thread(writers[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        for (int i = 0; i < readers.length; i++) {
            OperationStatus result = readers[i].getResult();
            assertSame(result, OperationStatus.NOTFOUND);
        }

        for (int i = 0; i < writers.length; i++) {
            OperationStatus result = writers[i].getResult();
            assertSame(result, OperationStatus.SUCCESS);
        }
    }

    /*
     * Have multiple threads deleting, inserting, and reading
     * the same record.
     */
    @Test
    public void testMultiReadDeleteInsert() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int RTHREADS = 20;
        final int WTHREADS = 1;
        final int DTHREADS = 1;
        final long TESTRUNTIME = 2000;

        Thread[] threads = new Thread[RTHREADS + WTHREADS + DTHREADS];
        ReadTillITellYou[] readers = new ReadTillITellYou[RTHREADS];
        DeleteIt[] deleters = new DeleteIt[WTHREADS];
        InsertIt[] writers = new InsertIt[WTHREADS];

        int curReadType = 0;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < readers.length; i++) {
            Database tdb;
            if ((i % 2) == 0) {
                tdb = db1UsePrimary ? db : sdb;
            } else {
                tdb = db2UsePrimary ? db : sdb;
            }
            ReadType trt = ReadType.GETSEARCHKEY;
            if (tdb instanceof SecondaryDatabase) {
                trt = readTypes[curReadType % readTypes.length];
                curReadType = curReadType + 1;
            }
            readers[i] =
                new ReadTillITellYou(KEY, env, tdb, 0, 0, 1, trt, null);
            threads[i] = new Thread(readers[i]);
        }

        for (int i = 0; i < writers.length; i++) {
            writers[i] =
                new InsertIt(KEY, env, db,
                             0, readers.length / 2, 1);
            threads[i + readers.length] = new Thread(writers[i]);
        }

        for (int i = 0; i < deleters.length; i++) {
            deleters[i] =
                new DeleteIt(KEY, env, db1UsePrimary ? db : sdb,
                             0, readers.length / 2, 1);
            threads[i + readers.length + writers.length] =
                new Thread(writers[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        try {
            Thread.sleep(TESTRUNTIME);
        } catch (ThreadInterruptedException e) {

        }

        for (int i = 0; i < readers.length; i++) {
            readers[i].setDone(true);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        for (int i = 0; i < writers.length; i++) {
            OperationStatus result = writers[i].getResult();
            assertSame(result, OperationStatus.SUCCESS);
        }
    }

    /*
     * Have multiple threads update (delete/insert in a
     * transaction) and read the same record.
     */
    @Test
    public void testMultiReadUpdate() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int RTHREADS = 20;
        final int WTHREADS = 5;
        final long TESTRUNTIME = 2000;

        Thread[] threads = new Thread[RTHREADS + WTHREADS];
        ReadTillITellYou[] readers = new ReadTillITellYou[RTHREADS];
        UpdateIt[] writers = new UpdateIt[WTHREADS];

        int curReadType = 0;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < readers.length; i++) {
            Database tdb;
            if ((i % 2) == 0) {
                tdb = db1UsePrimary ? db : sdb;
            } else {
                tdb = db2UsePrimary ? db : sdb;
            }
            ReadType trt = ReadType.GETSEARCHKEY;
            if (tdb instanceof SecondaryDatabase) {
                trt = readTypes[curReadType % readTypes.length];
                curReadType = curReadType + 1;
            }
            readers[i] =
                new ReadTillITellYou(KEY, env, tdb, 0, 0, 1, trt, null);
            threads[i] = new Thread(readers[i]);
        }

        for (int i = 0; i < writers.length; i++) {
            writers[i] =
                new UpdateIt(KEY, env, db, sdb,
                             0, readers.length / 2, 1);
            threads[i + readers.length] = new Thread(writers[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        try {
            Thread.sleep(TESTRUNTIME);
        } catch (ThreadInterruptedException e) {

        }

        for (int i = 0; i < readers.length; i++) {
            readers[i].setDone(true);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        for (int i = 0; i < writers.length; i++) {
            OperationStatus result = writers[i].getResult();
            assertSame(result, OperationStatus.SUCCESS);
        }

        for (int i = 0; i < readers.length; i++) {
            assertEquals(readers[i].getNotFoundCount(), 0);
        }

    }

    /*
     * Multiple readers reading the same record with different
     * lock types.
     */
    @Test
    public void testMultiReaders() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int RTHREADS = 20;
        final long TESTRUNTIME = 2000;

        Thread[] threads = new Thread[RTHREADS];
        ReadTillITellYou[] readers = new ReadTillITellYou[RTHREADS];

        int curReadType = 0;
        LockMode lockMode;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(val));
        }

        for (int i = 0; i < readers.length; i++) {
            Database tdb;
            if ((i % 2) == 0) {
                tdb = db1UsePrimary ? db : sdb;
                lockMode = LockMode.RMW;
            } else {
                tdb = db2UsePrimary ? db : sdb;
                lockMode = null;
            }
            ReadType trt = ReadType.GETSEARCHKEY;
            if (tdb instanceof SecondaryDatabase) {
                trt = readTypes[curReadType % readTypes.length];
                curReadType = curReadType + 1;
            }
            readers[i] =
                new ReadTillITellYou(KEY, env, tdb, 0, 0, 1, trt, lockMode);
            threads[i] = new Thread(readers[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        try {
            Thread.sleep(TESTRUNTIME);
        } catch (ThreadInterruptedException e) {

        }

        for (int i = 0; i < readers.length; i++) {
            readers[i].setDone(true);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        for (int i = 0; i < readers.length; i++) {
            assertEquals(readers[i].getNotFoundCount(), 0);
        }

    }

    public boolean deadlocksCanHappen() {
        if (useSerialization &&
            db1UsePrimary ^ db2UsePrimary) {
            return true;
        }
        return false;
    }

    class DeleteIt implements Runnable {
        int key;
        Database db;
        Environment env;
        OperationStatus retstat = null;
        int waitEventPre;
        int waitEventPost;
        int id;
        long waittime;

        DeleteIt(int key,
                 Environment env,
                 Database db,
                 int waiteventpre,
                 int waiteventpost,
                 long waittime) {
            this.key = key;
            this.db = db;
            this.env = env;
            this.waitEventPre = waiteventpre;
            this.waitEventPost = waiteventpost;
            id = threadid++;
            this.waittime = waittime;
         }

       private boolean deadlockCanHappen() {
            if (useSerialization &&
                db1UsePrimary ^ db2UsePrimary) {
                return true;
            }
            return false;
        }

        public void run() {
            while (!doWork());
        }

        private boolean doWork() {
            boolean done = false;
            while (currentEvent < waitEventPre) {
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
                    testDone = true;
                }
            }
            currentEvent++;
            while (!done) {
                Transaction xact = env.beginTransaction(null, null);
                try {
                    retstat = db.delete(xact, createEntry(key));
                } catch (LockConflictException e) {
                    if (!deadlockCanHappen() ) {
                        fail("deadlock occured but not expected.");
                    }
                    Transaction tx = xact;
                    xact = null;
                    tx.abort();
                }
                currentEvent++;
                while (currentEvent < waitEventPost) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        testDone = true;
                    }
                }
                // sleep after the event is flagged
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
                    testDone = true;
                }

                if (xact != null) {
                    xact.commit();
                    done = true;
                }
            }
            currentEvent++;
            return done;
        }

        public OperationStatus getResult() {
            return retstat;
        }
    }

    class ReadIt implements Runnable {
        int key;
        Database db;
        Environment env;
        int waitEventPre;
        int waitEventPost;
        long waitTime;
        OperationStatus result;
        int id;
        OperationStatus initStatus;
        ReadType readType;

        ReadIt(int key,
               Environment env,
               Database db,
               int waitEventPre,
               int waitEventPost,
               long waitTime,
               OperationStatus initStatus,
               ReadType readType) {
           this.key = key;
           this.db = db;
           this.env = env;
           this.waitEventPre = waitEventPre;
           this.waitEventPost = waitEventPost;
           this.waitTime = waitTime;
           id = threadid++;
           this.initStatus = initStatus;
           this.readType = readType;
        }

        public void run() {

            while (currentEvent < waitEventPre) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                }
            }
            currentEvent++;

            while (result == null || result == initStatus) {
                doWork();
            }

            currentEvent++;
            while (currentEvent < waitEventPost) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    testDone = true;
                }
            }
        }

        private void doWork() {
            DatabaseEntry dek = createEntry(key);
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry seckey = new DatabaseEntry(dek.getData());
            DatabaseEntry prikey = new DatabaseEntry(dek.getData());
            Transaction xact = null;
            Cursor c = null;
            try {
                xact = env.beginTransaction(null, null);
                c = db.openCursor(xact, null);
                if (readType == ReadType.GETSEARCHKEY) {
                    result = c.getSearchKey(dek, data, null);
                } else if (readType == ReadType.GETSEARCHKEY2) {
                    result =
                        ((SecondaryCursor)c).getSearchKey(dek, dek,
                                                          data, null);
                } else if (readType == ReadType.GETSEARCHBOTH) {
                    result =
                        ((SecondaryCursor)c).getSearchBoth(dek, dek,
                                                           data, null);
                } else if (readType == ReadType.GETSEARCHBOTH2) {
                    result =
                        ((SecondaryCursor)c).getSearchBoth(dek, dek,
                                                           data, null);
                } else if (readType == ReadType.GETSEARCHKEYRANGE) {
                    result =
                        ((SecondaryCursor)c).getSearchKeyRange(seckey,
                                                               data, null);
                    if (result == OperationStatus.SUCCESS) {
                        if (!seckey.equals(dek)) {
                            result = OperationStatus.NOTFOUND;
                        }
                    }
                } else if (readType == ReadType.GETSEARCHKEYRANGE2) {
                    result =
                        ((SecondaryCursor)c).getSearchKeyRange(seckey,
                                                               prikey,
                                                               data,
                                                               null);
                    if (result == OperationStatus.SUCCESS) {
                        if (!seckey.equals(dek)) {
                            result = OperationStatus.NOTFOUND;
                        }
                    }
                }
            } catch (LockConflictException e) {
                if (c != null) {
                    c.close();
                    c = null;
                }
                Transaction tx = xact;
                xact = null;
                tx.abort();
                xact = null;
                result = null;
                if (!deadlockCanHappen()) {
                    fail("deadlock occured but not expected.");
                }
            }
            finally {
                if (c != null) {
                    c.close();
                }
                if (xact != null) {
                    xact.commit();
                }
            }
        }

        public OperationStatus getResult() {
            return result;
        }

        private boolean deadlockCanHappen() {
            if (useSerialization &&
                db1UsePrimary ^ db2UsePrimary) {
                return true;
            }
            return false;
        }
    }

    class ReadTillITellYou implements Runnable {
        int key;
        Database db;
        Environment env;
        int waitEventPre;
        int waitEventPost;
        long waitTime;
        OperationStatus result;
        int id;
        boolean done = false;
        ReadType readType;
        long found;
        long notFound;
        LockMode lockMode;

        ReadTillITellYou(int key,
                         Environment env,
                         Database db,
                         int waitEventPre,
                         int waitEventPost,
                         long waitTime,
                         ReadType readType,
                         LockMode lkmode) {
           this.key = key;
           this.db = db;
           this.env = env;
           this.waitEventPre = waitEventPre;
           this.waitEventPost = waitEventPost;
           this.waitTime = waitTime;
           id = threadid++;
           this.readType = readType;
           lockMode = lkmode;
        }

        public void run() {

            while (currentEvent < waitEventPre) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                }
            }
            currentEvent++;

            while (!done) {
                doWork();
            }
            currentEvent++;
            while (currentEvent < waitEventPost) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    testDone = true;
                }
            }
        }

        private void doWork() {
            DatabaseEntry dek = createEntry(key);
            DatabaseEntry data = new DatabaseEntry();
            DatabaseEntry seckey = new DatabaseEntry(dek.getData());
            DatabaseEntry prikey = new DatabaseEntry(dek.getData());
            Transaction xact = null;
            Cursor c = null;
            try {
                xact = env.beginTransaction(null, null);
                c = db.openCursor(xact, null);
                if (readType == ReadType.GETSEARCHKEY) {
                    result = c.getSearchKey(dek, data, lockMode);
                } else if (readType == ReadType.GETSEARCHKEY2) {
                    result =
                        ((SecondaryCursor)c).getSearchKey(dek, dek,
                                                          data, lockMode);
                } else if (readType == ReadType.GETSEARCHBOTH) {
                    result =
                        ((SecondaryCursor)c).getSearchBoth(dek, dek,
                                                           data, lockMode);
                } else if (readType == ReadType.GETSEARCHBOTH2) {
                    result =
                        ((SecondaryCursor)c).getSearchBoth(dek, dek,
                                                           data, lockMode);
                } else if (readType == ReadType.GETSEARCHKEYRANGE) {
                    result =
                        ((SecondaryCursor)c).getSearchKeyRange(seckey,
                                                                data, lockMode);
                    if (result == OperationStatus.SUCCESS) {
                        if (!seckey.equals(dek)) {
                            result = OperationStatus.NOTFOUND;
                        }
                    }
                } else if (readType == ReadType.GETSEARCHKEYRANGE2) {
                    result =
                        ((SecondaryCursor)c).getSearchKeyRange(seckey,
                                                               prikey,
                                                               data,
                                                               lockMode);
                    if (result == OperationStatus.SUCCESS) {
                        if (!seckey.equals(dek)) {
                            result = OperationStatus.NOTFOUND;
                        }
                    }
                }
                if (result == OperationStatus.SUCCESS) {
                    found++;
                } else if (result == OperationStatus.NOTFOUND) {
                    notFound++;
                } else {
                    fail("Read operation returned "+result);
                }
            } catch (LockConflictException e) {
                if (!deadlockCanHappen()) {
                    fail("deadlock occured but not expected.");
                }
                Transaction tx = xact;
                xact = null;
                tx.abort();
                result = null;
            }
            finally {
                if (c != null) {
                    c.close();
                }
                if (xact != null) {
                    xact.commit();
                }
            }
        }

        public OperationStatus getResult() {
            return result;
        }

        public synchronized void setDone(boolean done) {
            this.done = done;
        }

        public long getFoundCount() {
            return found;
        }

        public long getNotFoundCount() {
            return notFound;
        }

        private boolean deadlockCanHappen() {
            if (useSerialization &&
                db1UsePrimary ^ db2UsePrimary) {
                return true;
            }
            return false;
        }
    }

    class InsertIt implements Runnable {
        int key;
        Database db;
        Environment env;
        OperationStatus retstat = null;
        int waitEventPre;
        int waitEventPost;
        int id;
        long waittime;

        InsertIt(int key,
                 Environment env,
                 Database db,
                 int waiteventpre,
                 int waiteventpost,
                 long waittime) {
            this.key = key;
            this.db = db;
            this.env = env;
            this.waitEventPre = waiteventpre;
            this.waitEventPost = waiteventpost;
            id = threadid++;
            this.waittime = waittime;
         }

        public void run() {
            Transaction xact = env.beginTransaction(null, null);
            while (currentEvent < waitEventPre) {
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
                    testDone = true;
                }
            }
            currentEvent++;
            try {
                retstat = db.put(xact, createEntry(key), createEntry(key));
            } catch (LockConflictException e) {
                if (!deadlockCanHappen()) {
                    fail("deadlock not expected.");
                }
                Transaction tx = xact;
                xact = null;
                tx.abort();
            }
            currentEvent++;
            while (currentEvent < waitEventPost) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    testDone = true;
                }
            }
            // sleep after the event is flagged
            try {
                Thread.sleep(waittime);
            } catch (InterruptedException e) {
                testDone = true;
            }

            if (xact != null) {
                xact.commit();
            }
            currentEvent++;
        }

        private boolean deadlockCanHappen() {
            if (useSerialization &&
                db1UsePrimary ^ db2UsePrimary) {
                return true;
            }
            return false;
        }

        public OperationStatus getResult() {
            return retstat;
        }
    }

    class UpdateIt implements Runnable {
        int key;
        Database db;
        SecondaryDatabase sdb;
        Environment env;
        OperationStatus retstat = null;
        int waitEventPre;
        int waitEventPost;
        int id;
        long waittime;

        UpdateIt(int key,
                 Environment env,
                 Database db,
                 SecondaryDatabase sdb,
                 int waiteventpre,
                 int waiteventpost,
                 long waittime) {
            this.key = key;
            this.db = db;
            this.env = env;
            this.waitEventPre = waiteventpre;
            this.waitEventPost = waiteventpost;
            id = threadid++;
            this.waittime = waittime;
            this.sdb = sdb;
         }

       private boolean deadlockCanHappen() {
            if (useSerialization &&
                db1UsePrimary ^ db2UsePrimary) {
                return true;
            }
            return false;
        }

        public void run() {
            while (!doWork());
        }

        private boolean doWork() {
            boolean done = false;
            while (currentEvent < waitEventPre) {
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
                    testDone = true;
                }
            }
            currentEvent++;
            while (!done) {
                Transaction xact = env.beginTransaction(null, null);
                try {
                    retstat = sdb.delete(xact, createEntry(key));
                    assertEquals(retstat, OperationStatus.SUCCESS);
                    retstat = db.put(xact, createEntry(key), createEntry(key));
                    assertEquals(retstat, OperationStatus.SUCCESS);
                } catch (LockConflictException e) {
                    if (!deadlockCanHappen() ) {
                        fail("deadlock occured but not expected.");
                    }
                    Transaction tx = xact;
                    xact = null;
                    tx.abort();
                }
                currentEvent++;
                while (currentEvent < waitEventPost) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        testDone = true;
                    }
                }
                // sleep after the event is flagged
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
                    testDone = true;
                }

                if (xact != null) {
                    xact.commit();
                    done = true;
                }
            }
            currentEvent++;
            return done;
        }

        public OperationStatus getResult() {
            return retstat;
        }
    }

    DatabaseEntry createEntry(int val) {
        return new DatabaseEntry(Integer.valueOf(val).toString().getBytes());
    }

    private SecondaryDatabase
        openSecondary(Environment env,
                      Database priDb,
                      String dbName,
                      SecondaryConfig dbConfig) {
        dbConfig.setAllowPopulate(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setKeyCreator(new MyKeyCreator());
        return env.openSecondaryDatabase(null, dbName,
                                         priDb, dbConfig);
    }

    class MyKeyCreator implements SecondaryKeyCreator {
        @Override
        public boolean createSecondaryKey(SecondaryDatabase secondary,
                DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
            result.setData(key.getData());

            return true;
        }
    }
}
