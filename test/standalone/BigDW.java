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

import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * A large database with random key distribution has lots of IN waste,
 * especially if records are small; this creates a worst-case scenario for the
 * cleaner and also possibly for the evictor.  Simulate such an application and
 * measure how well the cleaner and evictor keep up.
 *
 * Some commonly used command lines for running this program are:
 *
 *   # Init new DB, causes duplicates to be created and deleted [#15588]
 *   java BigDW -h HOME -init -dupdel
 *
 * Each transaction does the following in "grow" mode.  In "no grow" mode, it
 * does one less insert, keeping the total number of keys constant.
 *
 *   2 inserts, 1 delete, 1 update, 10 reads
 *
 * The delete and update operations include a read to find the record.
 *
 */
public class BigDW implements Runnable {

    private String homeDir = "tmp";
    private Environment env;
    private Database refDB;
    private Database testDB;
    private boolean done;
    private int nDeadlocks;
    private boolean init;
    private boolean verbose;
    private boolean dupDel;
    private int nTransactions;
    private int nMaxTransactions = 20000;
    private int nThreads = 4;

    private int subDir = 0;
    private int keySize = 10;
    private int dataSize = 10;
    private int nReadsPerWrite = 1;
    private int maxRetries = 100;
    private float totalSecs;
    private float throughput;
    private SecureRandom random = new SecureRandom();
    private long startTime;
    private long time;
    private long mainCacheSize = 20000000;

    public static void main(String args[]) {
        try {
            new BigDW().run(args);
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }
    
    /* Output command-line input arguments to log. */
    private void printArgs(String[] args) {
        System.out.print("\nCommand line arguments:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
    }

    private void usage(String error) {

        if (error != null) {
            System.err.println(error);
        }
        System.err.println
            ("java " + getClass().getName() + '\n' +
             "      [-h <homeDir>] [-v] [-init] [-dupdel]\n" +
             "      [-txns <maxTxns>]\n");
        System.exit(1);
    }
    
    private void run(String args[]) throws Exception {
        
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException();
            }
            /* Parse command-line input arguments. */
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                boolean moreArgs = i < args.length - 1;
                if (arg.equals("-v")) {
                    verbose = true;
                } else if (arg.equals("-dupdel")) {
                    dupDel = true;
                } else if (arg.equals("-h") && moreArgs) {
                    homeDir = args[++i];
                } else if (arg.equals("-init")) {
                    init = true;
                } else if (arg.equals("-txns") && moreArgs) {
                    nMaxTransactions = Integer.parseInt(args[++i]);
                } else if (arg.equals("-threads") && moreArgs) {
                    nThreads = Integer.parseInt(args[++i]);
                } else if (arg.equals("-subDir") && moreArgs) {
                    subDir = Integer.parseInt(args[++i]);
                } else {
                    usage("Unknown arg: " + arg);
                }
            }
            printArgs(args);
        } catch (IllegalArgumentException e) {
            usage("IllegalArguments! ");
            e.printStackTrace();
            System.exit(1);
        }

        openEnv();
        startTime = System.currentTimeMillis();

        Thread[] threads = new Thread[nThreads];
        for (int i = 0; i < nThreads; i += 1) {
            threads[i] = new Thread(this);
            threads[i].start();
            Thread.sleep(1000); /* Stagger threads. */
        }
        for (int i = 0; i < nThreads; i += 1) {
            if (threads[i] != null) {
                threads[i].join();
            }
        }

        time = System.currentTimeMillis();
        closeEnv();

        totalSecs = (float) (time - startTime) / 1000;
        throughput = (float) nTransactions / totalSecs;
        if (verbose) {
            System.out.println("\nTotal seconds: " + totalSecs +
                               " txn/sec: " + throughput);
        }
    }

    private void openEnv() throws Exception {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(init);
        envConfig.setCacheSize(mainCacheSize);
        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(homeDir), subDir);
        }
        env = new Environment(new File(homeDir), envConfig);
        
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(init);
        dbConfig.setExclusiveCreate(init);
        dbConfig.setSortedDuplicates(dupDel);
        refDB = env.openDatabase(null, "BigDWRef", dbConfig);

        dbConfig.setDeferredWrite(true);
        testDB = env.openDatabase(null, "BigDWTest", dbConfig);

        compare();
    }

    private void closeEnv()
        throws Exception {

        refDB.close();
        testDB.sync();
        testDB.close();
        env.close();
    }

    public void run() {

        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        byte[] lastInsertKey = null;

        while (!done) {

            /* JE-only begin */
            try {
                
                /* Perform the transaction. */
                for (int retry = 0;; retry += 1) {
                    Cursor refCursor = refDB.openCursor(null, null);
                    Cursor testCursor = testDB.openCursor(null, null);

                    try {
                        if (init) {
                            key.setData(lastInsertKey);
                            insert(refCursor, testCursor, key, data);
                            lastInsertKey = copyData(key);
                        }
                        
                        /* Insert */
                        key.setData(lastInsertKey);
                        insert(refCursor, testCursor, key, data);
                        lastInsertKey = copyData(key);
                
                        /* Dup-key insert. */
                        byte[] dupDataBA = copyData(data);
                        for (int i = 0; i < 5; i++) {
                            dupDataBA[0]++;
                            DatabaseEntry dupData =
                                new DatabaseEntry(dupDataBA);
                            OperationStatus status1 =
                                refCursor.put(key, dupData);
                            @SuppressWarnings("unused")
                            boolean insertDone1 = checkInsertStatus(status1);
                            if (status1 != OperationStatus.SUCCESS) {
                                throw new RuntimeException("insert1 " +
                                                           status1);
                            }
                            OperationStatus status2 =
                                testCursor.put(key, dupData);
                            if (status2 != OperationStatus.SUCCESS) {
                                throw new RuntimeException("insert2 " +
                                                           status2);
                            }
                            @SuppressWarnings("unused")
                            boolean insertDone2 = checkInsertStatus(status2);
                        }

                        /* Delete */
                        getRandom(refCursor, "BigDWRef",
                                  testCursor, "BigDWTest",
                                  key, data, LockMode.RMW);
                        DatabaseEntry dummy1 = new DatabaseEntry();
                        DatabaseEntry dummy2 = new DatabaseEntry();
                        while (refCursor.delete() ==
                               OperationStatus.SUCCESS &&
                               refCursor.getNextDup
                               (dummy1, dummy2, null) ==
                               OperationStatus.SUCCESS) {
                        }
                        while (testCursor.delete() ==
                               OperationStatus.SUCCESS &&
                               refCursor.getNextDup
                               (dummy1, dummy2, null) ==
                               OperationStatus.SUCCESS) {
                        }
                                
                        /* Read */
                        for (int i = 0; i < nReadsPerWrite; i += 1) {
                            getRandom(refCursor, "BigDWRef",
                                      testCursor, "BigDWTest",
                                      key, data, LockMode.RMW);
                        }
                        refCursor.close();
                        testCursor.close();
                        nTransactions += 1;
                        if (nMaxTransactions != 0 &&
                            nTransactions >= nMaxTransactions) {
                            done = true;
                        }
                        break;
                    } catch (LockConflictException e) {
                        refCursor.close();
                        testCursor.close();
                        if (retry >= maxRetries) {
                            throw e;
                        }
                        /* Break deadlock cycle with a small sleep. */
                        Thread.sleep(5);
                        nDeadlocks += 1;
                    }
                } /* for */

            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private void checkStatus(OperationStatus status)
        throws Exception {
        if (status != OperationStatus.SUCCESS) {
            throw new Exception("problemStatus = " + status);
        }
    }

    private void compare()
        throws Exception {

        DatabaseEntry refKey = new DatabaseEntry();
        DatabaseEntry refData = new DatabaseEntry();
        DatabaseEntry testKey = new DatabaseEntry();
        DatabaseEntry testData = new DatabaseEntry();

        Cursor refCursor = refDB.openCursor(null, null);
        Cursor testCursor = testDB.openCursor(null, null);

        System.out.println("Compare starts");
        try {
            while (refCursor.getNext(refKey, refData, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {
                checkStatus(testCursor.getNext(testKey, testData,
                                               LockMode.DEFAULT));

                if (!Arrays.equals(refKey.getData(),
                                   testKey.getData())) {
                    throw new Exception("Keys don't match");
                }

                if (!Arrays.equals(refData.getData(),
                                   testData.getData())) {
                    throw new Exception("Data don't match");
                }
            }

            if (testCursor.getNext(testKey, testData, LockMode.DEFAULT) !=
                OperationStatus.NOTFOUND) {
                throw new Exception("testCursor has extra data");
            }
        } finally {
            refCursor.close();
            testCursor.close();
        }
        System.out.println("Compare ends");
    }

    private void insert(Cursor c1, Cursor c2,
                        DatabaseEntry key, DatabaseEntry data)
        throws DatabaseException {

        makeData(data);
        boolean insertDone1 = false;
        while (!insertDone1) {
            makeInsertKey(key);
            OperationStatus status1 = c1.putNoOverwrite(key, data);
            insertDone1 = checkInsertStatus(status1);
            OperationStatus status2 = c2.putNoOverwrite(key, data);
            boolean insertDone2 = checkInsertStatus(status2);
            assert insertDone1 == insertDone2 :
                "status1=" + status1 +
                " status2=" + status2;
        }
    }

    private boolean checkInsertStatus(OperationStatus status) {
        if (status == OperationStatus.KEYEXIST) {
            System.out.println("****** Duplicate random key.");
            return false; // try again.
        } else {
            if (status != OperationStatus.SUCCESS) {
                System.out.println
                    ("Unexpected return value from insert(): " + status);
            }
            return true; // end one way or another
        }
    }

    private void getRandom(Cursor c1, String db1,
                           Cursor c2, String db2,
                           DatabaseEntry key, DatabaseEntry data,
                           LockMode lockMode)
        throws DatabaseException {

        makeRandomKey(key);
        getRandomWork(c1, db1, key, data, lockMode);
        getRandomWork(c2, db2, key, data, lockMode);
    }

    private void getRandomWork(Cursor c,
                               String dbName,
                               DatabaseEntry key,
                               DatabaseEntry data,
                               LockMode lockMode)
        throws DatabaseException {

        OperationStatus status = c.getSearchKeyRange(key, data, lockMode);
        if (status == OperationStatus.NOTFOUND) {
            status = c.getLast(key, data, lockMode);
            if (status != OperationStatus.SUCCESS) {
                System.out.println
                    ("Unexpected return value from " + dbName +
                     ".getRandomWork(): " + status);
            }
        }
    }

    private void makeInsertKey(DatabaseEntry key) {
        if (key.getData() != null) {
            BigInteger num = new BigInteger(copyData(key));
            num = num.add(BigInteger.ONE);
            key.setData(num.toByteArray());
        } else {
            makeRandomKey(key);
        }
    }

    private void makeRandomKey(DatabaseEntry key) {
        byte[] bytes = new byte[keySize];
        random.nextBytes(bytes);
        key.setData(bytes);
    }

    private void makeData(DatabaseEntry data) {

        byte[] bytes = new byte[dataSize];
        for (int i = 0; i < bytes.length; i += 1) {
            bytes[i] = (byte) i;
        }
        data.setData(bytes);
    }

    private byte[] copyData(DatabaseEntry data) {

        byte[] buf = new byte[data.getSize()];
        System.arraycopy(data.getData(), data.getOffset(), buf, 0, buf.length);
        return buf;
    }
}
