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
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test involving secondary indexes with respect to deadlock free access.
 *
 * @author dwchung
 *
 */
@RunWith(Parameterized.class)
public class SecondaryMultiComplexTest extends TestBase {

    private enum ReadType {GETSEARCHKEY, GETSEARCHKEY2, GETSEARCHBOTH,
                           GETSEARCHBOTH2, GETSEARCHKEYRANGE,
                           GETSEARCHKEYRANGE2};
    private final ReadType[] readTypes = ReadType.values();
    private static final String DB_NAME = "foo";
    private static final String SDB_NAME = "fooSec";
    private volatile int currentEvent;
    private volatile int threadid = 0;
    private final boolean useDuplicate;
    Environment env = null;
    Database db = null;
    SecondaryDatabase sdb = null;

    public SecondaryMultiComplexTest(boolean useDuplicate) {
        this.useDuplicate = useDuplicate;
    }

    @Parameters
    public static List<Object[]> genParams() {
       return paramsHelper(false);
    }

    /*
     * The parameters for the test is a boolean that
     * determines if the secondary db supports duplicates.
     */
    private static List<Object[]> paramsHelper(boolean rep) {
        final List<Object[]> newParams = new ArrayList<Object[]>();
        newParams.add(new Object[] {false});
        newParams.add(new Object[] {true});
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
        envConfig.setTxnSerializableIsolation(false);

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

    /*
     * This test checks the code in the area of secondary read
     * deadlock avoidance. The initial non-locking scan on the
     * secondary, followed by the locking primary scan is exercised.
     * The following is the test:
     * Primary  Secondary Data Description
     * A        A        1    Data populated
     *                        readers started reading secondary key A
     *                         Writer transaction begin
     * -        -         -      delete Pk
     * A        B         0      insert record
     * B        A         1      insert record
     *                         Writer commit transaction
     *
     * The readers may block on the primary lock after having
     * retrieved the primary key from the secondary index
     * without locking. When the primary lock is granted,
     * the primary/secondary association has changed from
     * what it was on the initial scan on the secondary to
     * get the primary key. Initially B -> A, now A -> B.
     * So this row should be skipped (non-serializable isolation).
     */
    @Test
    public void testMultiReadMoveSecondary() throws Exception {

        final int DATACOUNT = 99;
        final int KEY = 55;
        final int NEWKEY = DATACOUNT + 1;
        final int RTHREADS = 20;
        final int WTHREADS = 1;

        Thread[] threads = new Thread[RTHREADS + WTHREADS];
        ReadIt[] readers = new ReadIt[RTHREADS];
        MoveIt[] writers = new MoveIt[WTHREADS];
        int curReadType = 0;

        /* populate */
        for (int i = 0; i < DATACOUNT; i++) {
            byte[] val = Integer.valueOf(i).toString().getBytes();
            db.put(null,
                   new DatabaseEntry(val),
                   new DatabaseEntry(createData(1, i)));
        }

        for (int i = 0; i < readers.length; i++) {
            Database tdb;
            tdb = sdb;
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
                new MoveIt(KEY, NEWKEY, env, db,
                             readers.length / 2, 0, 1);
            threads[i + readers.length] = new Thread(writers[i]);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < writers.length; i++) {
            threads[i + readers.length].join();
        }
        for (int i = 0; i < readers.length; i++) {
            readers[i].setDone(true);
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        for (int i = 0; i < readers.length; i++) {
            if (readers[i].getFailure() != null) {
                fail(readers[i].getFailure().getMessage());
            }
        }
        for (int i = 0; i < writers.length; i++) {
            if (writers[i].getFailure() != null) {
                fail(writers[i].getFailure().getMessage());
            }
        }

        for (int i = 0; i < writers.length; i++) {
            OperationStatus result = writers[i].getResult();
            assertSame(result, OperationStatus.SUCCESS);
        }
    }

    class MoveIt implements Runnable {
        int oldkey;
        int newkey;
        Database db;
        Environment env;
        OperationStatus retstat = null;
        int waitEventPre;
        int waitEventPost;
        int id;
        long waittime;
        Exception failureException;

        MoveIt(int oldkey,
               int newkey,
                 Environment env,
                 Database db,
                 int waiteventpre,
                 int waiteventpost,
                 long waittime) {
            this.oldkey = oldkey;
            this.newkey = newkey;
            this.db = db;
            this.env = env;
            this.waitEventPre = waiteventpre;
            this.waitEventPost = waiteventpost;
            id = threadid++;
            this.waittime = waittime;
         }

        public void run() {
            try {
                while (!doWork());
            } catch (Exception e) {
                failureException = e;
            }
        }

        private boolean doWork() throws Exception {
            boolean done = false;
            while (currentEvent < waitEventPre) {
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
                }
            }
            currentEvent++;
            while (!done) {
                Transaction xact = env.beginTransaction(null, null);
                try {
                    retstat = db.delete(xact, createEntry(oldkey));
                    retstat =
                        db.put(xact,
                               createEntry(oldkey),
                               new DatabaseEntry(createData(0, newkey)));
                    retstat =
                        db.put(xact,
                               createEntry(newkey),
                               new DatabaseEntry(createData(1, oldkey)));

                } catch (LockConflictException e) {
                    Transaction tx = xact;
                    xact = null;
                    tx.abort();
                    throw new Exception("deadlock occured but not expected.");
                }
                currentEvent++;
                while (currentEvent < waitEventPost) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                }
                // sleep after the event is flagged
                try {
                    Thread.sleep(waittime);
                } catch (InterruptedException e) {
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

        Exception getFailure() {
            return failureException;
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
        boolean done = false;
        Exception failureException;

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

            while (!done) {
                try {
                    doWork();
                } catch (Exception e) {
                    failureException = e;
                }
            }

            currentEvent++;
            while (currentEvent < waitEventPost) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                }
            }
        }

        private void doWork() throws Exception {
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
                if (result == OperationStatus.SUCCESS) {
                    int readSKey = byteToInt(data.getData(), 0);
                    int readDVal = byteToInt(data.getData(), 4);
                    if (readDVal != 1) {
                        throw new Exception(
                            "read invalid data value expected 1 read " +
                            readDVal);
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
                    throw new Exception("deadlock occured but not expected.");
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
            return false;
        }
        public synchronized void setDone(boolean done) {
            this.done = done;
        }
        public Exception getFailure() {
            return failureException;
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
        dbConfig.setSortedDuplicates(useDuplicate);
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setKeyCreator(new MyKeyCreator());
        return env.openSecondaryDatabase(null, dbName,
                                         priDb, dbConfig);
    }

    int byteToInt(byte[] b, int offset) {
        int i =
            (b[0 + offset] << 24) & 0xff000000 |
            (b[1 + offset] << 16) & 0xff0000 |
            (b[2 + offset] << 8) & 0xff00 |
            (b[3 + offset] << 0) & 0xff;
        return i;
    }

    void intToByte(int input, byte[] dest, int destOffset) {
        dest[destOffset + 3] = (byte) (input & 0xff);
        input >>= 8;
        dest[destOffset + 2] = (byte) (input & 0xff);
        input >>= 8;
        dest[destOffset + 1] = (byte) (input & 0xff);
        input >>= 8;
        dest[destOffset] = (byte) input;
    }

    byte[] createData(int data, int seckey) {
        byte[] retval = new byte[8];
        intToByte(data, retval, 4);
        intToByte(seckey, retval, 0);
        return retval;
    }

    class MyKeyCreator implements SecondaryKeyCreator {
        @Override
        public boolean createSecondaryKey(SecondaryDatabase secondary,
                DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
            byte[] dbuf = data.getData();
            int skey = byteToInt(dbuf, 0);

            result.setData( Integer.valueOf(skey).toString().getBytes());
            return true;
        }
    }
}
