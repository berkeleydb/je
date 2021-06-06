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
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationStatus;

/*
 * A temporary database may throw a LFNF exception if it runs with high 
 * concurrency of Cleaner and Evictor.
 *
 * This test simulates such case: a small cache size, a relatively large 
 * database and large data value setting, so that there exists lots of eviction
 * during the test, also multiple cleaner threads is enabled, so that the test
 * is running with high concurrency.
 *
 * The UpdataThread would update all the threads in this database, and the
 * test starts 4 threads, so there are lots of updates, and that would create
 * even more eviction.
 *
 * Commonly used command lines for running this program are:
 *
 * java TemporaryDbStress -h HOME
 */
public class TemporaryDbStress {

    private String envHome;
    private static final String DB_NAME = "testDb";
    /* Database size. */
    private int dbSize = 20000;
    /* Number of updating threads. */
    private int numThreads = 4;
    /* Set a large cleaner threads number. */ 
    private String numCleanerThreads = "4";

    /* Set a small cache size, which is 10M. */
    private int cacheSize = 10 * 1024 * 1024;
    /* Set large record value size, so that it needs more eviction. */
    private int dataSize = 2000;
    private int subDir = 0;
    /* Total update operations. */
    private volatile int totalOps = 50000000;
    /* The data field for a value. */
    private String dataValue = "";

    private Environment env;
    private Database db;

    public static void main(String args[]) {
        try {
            TemporaryDbStress test = new TemporaryDbStress();
            test.parseArgs(args);
            test.doWork();
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(-1);
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
             "      [-h <envHome>] [-cacheSize] [-dataSize] [-dbSize]\n" +
             "      [-threads <update threads>]\n" +
             "      [-cleanerThreads <cleaner threads>]\n" +
             "      [-subDir <sub directories number>]\n" + 
             "      [-totalOps]\n");
        System.exit(1);
    }
    
    private void parseArgs(String args[]) {
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException();
            }
            /* Parse command-line input arguments. */
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                boolean moreArgs = i < args.length - 1;
                if (arg.equals("-h") && moreArgs) {
                    envHome = args[++i];
                } else if (arg.equals("-cacheSize") && moreArgs) {
                    cacheSize = Integer.parseInt(args[++i]);
                } else if (arg.equals("-dataSize") && moreArgs) {
                    dataSize = Integer.parseInt(args[++i]);
                } else if (arg.equals("-dbSize") && moreArgs) {
                    dbSize = Integer.parseInt(args[++i]);
                } else if (arg.equals("-threads") && moreArgs) {
                    numThreads = Integer.parseInt(args[++i]);
                } else if (arg.equals("-cleanerThreads") && moreArgs) {
                    numCleanerThreads = args[++i];
                } else if (arg.equals("-totalOps") && moreArgs) {
                    totalOps = Integer.parseInt(args[++i]);
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
    }

    /* Do the work. */
    public void doWork() 
        throws Exception {

        openEnv();

        System.out.println("Starting test.....");

        /* Insert some records first. */
        insertRecords();

        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch endSignal = new CountDownLatch(numThreads);

        /* Start the threads. */
        for (int i = 0; i < numThreads; i++) {
            UpdateThread thread = new UpdateThread(startSignal, endSignal);
            thread.start();
        }
        startSignal.countDown();

        endSignal.await();

        System.out.println("Test finishes.");
        closeEnv();
    }

    /* Open the Environment and insert some data to database. */
    private void openEnv() 
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setCacheSize(cacheSize);
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, 
                                 numCleanerThreads);

        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(envHome), subDir);
        }

        env = new Environment(new File(envHome), envConfig);
        
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTemporary(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    private void insertRecords() 
        throws Exception {

        for (int i = 1; i <= dataSize; i++) {
            dataValue += "a";
        }

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < dbSize; i++) {
            IntegerBinding.intToEntry(i, key);
            StringBinding.stringToEntry(dataValue, data);
            db.put(null, key, data);
        }
    }

    /* Close the database and Environment. */
    private void closeEnv()
        throws Exception {

        if (db != null) {
            db.close();
        }

        if (env != null) {
            env.close();
        }
    }

    /* The updating thread on temporary database. */
    class UpdateThread extends Thread {
        private final CountDownLatch start;
        private final CountDownLatch end;

        public UpdateThread(CountDownLatch start, CountDownLatch end) {
            this.start = start;
            this.end = end;
        }

        public void run() {
            try {
                start.await();

                Random random = new Random();
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                /* Do updates on each record in the database. */
                while (true) {
                    synchronized (this) {
                        int currentIndex = totalOps;

                        if (--totalOps <= 0) {
                            break;
                        }

                        IntegerBinding.intToEntry(random.nextInt(dbSize), key);
                        StringBinding.stringToEntry
                            (dataValue + currentIndex, data);
                        int retries = 10;
                        while (retries > 0) {
                            try {
                                OperationStatus status = 
                                    db.put(null, key, data);
                                if (status != OperationStatus.SUCCESS) {
                                    System.err.println
                                        ("Update a new key failed " + 
                                         currentIndex + ".");
                                }
                                break;
                            } catch (LockConflictException e) {
                                retries--;
                            }
                        }
                    }
                }

                end.countDown();
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
