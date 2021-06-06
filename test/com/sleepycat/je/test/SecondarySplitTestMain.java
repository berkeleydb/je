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

import java.io.File;
import java.util.Random;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Tests that splits during secondary inserts don't cause a LatchException
 * (latch already held).  This was caused by a latch that wasn't released
 * during a duplicate insert, when a split occurred during the insert.  See
 * [#12841] in Tree.java.
 *
 * The record keys are random long values and the record data is the long
 * time (millis) of the record creation.  The secondary database is indexed on
 * the full data value (the timestamp).  When a record is updated, its timstamp
 * is changed to the current time, cause secondary deletes and inserts.  This
 * scenario is what happened to bring out the bug in SR [#12841].
 */
public class SecondarySplitTestMain {

    private static final int WRITER_THREADS = 2;
    private static final int INSERTS_PER_ITER = 2;
    private static final int UPDATES_PER_ITER = 1;
    private static final int ITERS_PER_THREAD = 20000;
    private static final int ITERS_PER_TRACE = 1000;

    private final File envHome;
    private Environment env;
    private Database priDb;
    private SecondaryDatabase secDb;
    private final Random rnd = new Random(123);

    public static void main(String[] args) {
        try {
            SecondarySplitTestMain test = new SecondarySplitTestMain();
            test.doTest();
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    public SecondarySplitTestMain() {
        envHome = SharedTestUtils.getTestDir();
    }

    private void doTest()
        throws Exception {

        TestUtils.removeLogFiles("Setup", envHome, false);
        open();
        Thread[] writers = new Thread[WRITER_THREADS];
        for (int i = 0; i < writers.length; i += 1) {
            writers[i] = new Writer(i);
        }
        for (int i = 0; i < writers.length; i += 1) {
            writers[i].start();
        }
        for (int i = 0; i < writers.length; i += 1) {
            writers[i].join();
        }
        close();
        TestUtils.removeLogFiles("TearDown", envHome, false);
        System.out.println("SUCCESS");
    }

    private void open()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        env = new Environment(envHome, envConfig);

        DatabaseConfig priConfig = new DatabaseConfig();
        priConfig.setAllowCreate(true);

        priDb = env.openDatabase(null, "pri", priConfig);

        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);
        secConfig.setKeyCreator(new KeyCreator());

        secDb = env.openSecondaryDatabase(null, "sec", priDb, secConfig);
    }

    private void close()
        throws DatabaseException {

        secDb.close();
        secDb = null;

        priDb.close();
        priDb = null;

        env.close();
        env = null;
    }

    static class KeyCreator implements SecondaryKeyCreator {

        public boolean createSecondaryKey(SecondaryDatabase db,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            result.setData(data.getData(), data.getOffset(), data.getSize());
            return true;
        }
    }

    private class Writer extends Thread {

        Writer(int id) {
            super("[Writer " + id + ']');
        }

        @Override
        public void run() {

            int inserts = 0;
            int updates = 0;
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus status;

            for (int iter = 1; iter <= ITERS_PER_THREAD; iter += 1) {

                Cursor cursor = null;

                try {

                    /* Inserts */
                    for (int i = 0; i < INSERTS_PER_ITER; i += 1) {
                        LongBinding.longToEntry(rnd.nextLong(), key);
                        long time = System.currentTimeMillis();
                        LongBinding.longToEntry(time, data);
                        status = priDb.putNoOverwrite(null, key, data);
                        if (status == OperationStatus.SUCCESS) {
                            inserts += 1;
                        } else {
                            System.out.println
                                (getName() + " *** INSERT " + status);
                        }
                    }

                    /* Updates */
                    for (int i = 0; i < UPDATES_PER_ITER; i += 1) {

                        cursor = priDb.openCursor(null, null);

                        LongBinding.longToEntry(rnd.nextLong(), key);
                        status = cursor.getSearchKeyRange(key, data,
                                                          LockMode.RMW);
                        if (status == OperationStatus.NOTFOUND) {
                            status = cursor.getFirst(key, data, LockMode.RMW);
                        }

                        if (status == OperationStatus.SUCCESS) {
                            long time = System.currentTimeMillis();
                            LongBinding.longToEntry(time, data);
                            cursor.putCurrent(data);
                            updates += 1;
                        } else {
                            System.out.println
                                (getName() + " *** UPDATE " + status);
                        }

                        cursor.close();
                        cursor = null;
                    }

                } catch (Throwable e) {

                    e.printStackTrace(System.out);

                    if (cursor != null) {
                        try {
                            cursor.close();
                        } catch (Exception e2) {
                            e2.printStackTrace(System.out);
                        }
                    }
                }

                if (iter % ITERS_PER_TRACE == 0) {
                    System.out.println
                        (getName() +
                         " inserts=" + inserts +
                         " updates=" + updates);
                }
            }
        }
    }
}
