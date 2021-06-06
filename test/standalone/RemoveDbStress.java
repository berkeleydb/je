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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.MemoryBudget;

/**
 * Make sure no bugs are spotted when remove/truncate database is being called
 * and the log cleaner and checkpointer threads are interacting with the db.
 */
public class RemoveDbStress {

    /*
     * The following check ensures that locks are held by the handle in
     * the face of NameLN migration.  However, it causes the program
     * to take over 20 hours to run.  So if it is enabled, the number
     * of iterations should be decreased.
     */
    private static final boolean CHECK_REMOVE_WITH_OPEN_HANDLE = false;

    private int totalIterations = 500000;
    private int totalThreads = 4;
    private boolean offHeap = false;
    private int subDir = 0;
    private String envHome = "./tmp";
    private Random random = new Random();
    private Environment env = null;

    public static void main(String[] args) {

        RemoveDbStress stress = new RemoveDbStress();
        try {
            stress.run(args);
        } catch (Throwable e){
            System.err.println("Error initializing env!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Kickoff the run. */
    private void run(String args[]) throws Exception {

        for (int i = 0; i < args.length; i += 1) {
            String arg = args[i];
            boolean moreArgs = i < args.length - 1;
            if (arg.equals("-h") && moreArgs) {
                envHome = args[++i];
            } else if (arg.equals("-iter") && moreArgs) {
                totalIterations = Integer.parseInt(args[++i]);
            } else if (arg.equals("-threads") && moreArgs) {
                totalThreads = Integer.parseInt(args[++i]);
            } else if (arg.equals("-offheap") && moreArgs) {
                offHeap = Boolean.parseBoolean(args[++i]);
            } else if (arg.equals("-subDir") && moreArgs) {
                subDir = Integer.parseInt(args[++i]);
            } else {
                usage("Unknown arg: " + arg);
            }
        }
        openEnv();
        printArgs(args);
        
        /*
         * Perform some operations to simulate a scenario to find bugs:
         * make sure remove/truncate database is being called and the log
         * cleaner and checkpointer threads are interacting with the db.
         */
        Worker[] workers = new Worker[totalThreads];
        for (int i = 0; i < totalThreads; i += 1) {
            workers[i] = new Worker(i);
            workers[i].start();
            Thread.sleep(1000); /* Stagger threads. */
        }
        for (int i = 0; i < totalThreads; i += 1) {
            workers[i].join();
        }
        
        closeEnv();
    }
    
    /** Print usage. */
    private void usage(String error) {

        if (error != null) {
            System.err.println(error);
        }
        System.err.println
            ("java " + getClass().getName() + '\n' +
             "      [-h <homeDir>] [-iter <iterations>] " +
             "[-threads <appThreads>]\n");
        System.exit(1);
    }

    /** Print cmd arguments and database env settings to log file. */
    private void printArgs(String[] args)
        throws DatabaseException {

        System.out.print("Command line arguments:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
        System.out.println();
        System.out.println("Environment configuration:");
        System.out.println(env.getConfig());
        System.out.println();
    }
    
    /**
     * Open an Environment.
     */
    private void openEnv() throws Exception {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);

        if (offHeap) {
            config.setOffHeapCacheSize(500000);
        }
        config.setConfigParam(
            EnvironmentConfig.MAX_MEMORY,
            MemoryBudget.MIN_MAX_MEMORY_SIZE_STRING);
        /* Don't track detail with a tiny cache size. */
        config.setConfigParam(
            EnvironmentParams.CLEANER_TRACK_DETAIL.getName(), "false");
        config.setConfigParam(
            EnvironmentConfig.CLEANER_BYTES_INTERVAL, "100");
        config.setConfigParam(
            EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, "100");
        config.setConfigParam(
            EnvironmentConfig.COMPRESSOR_WAKEUP_INTERVAL, "1000000");
        config.setConfigParam(
            EnvironmentConfig.LOG_TOTAL_BUFFER_BYTES,
            EnvironmentParams.LOG_MEM_SIZE_MIN_STRING);
        config.setConfigParam(
            EnvironmentConfig.LOG_NUM_BUFFERS, "2");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "true");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "true");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "true");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "true");
        /*
        config.setConfigParam(EnvironmentConfig.TXN_DEADLOCK_STACK_TRACE,
                              "true");
        */

        if (subDir > 0) {
            config.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(envHome), subDir);
        }

        env = new Environment(new File(envHome), config);
    }

    private void closeEnv()
        throws DatabaseException {
        env.close();
    }
    
    class Worker extends Thread {
        private int iterations = 0;

        /** The identifier of the current thread. */
        private int id;

        /**
         * Creates a new worker thread object.
         */
        public Worker(int id) {
            this.id = id;
        }

        /**
         * This thread is responsible for executing transactions.
         */
        public void run() {

            long startTime = System.currentTimeMillis();
            while (iterations < totalIterations) {
                try {
                    doOperations();
                } catch (Throwable e) {
                    System.err.println
                        ("Error! " + iterations +
                         " iterations processed so far.");
                    e.printStackTrace();
                    System.exit(1);
                }
                iterations += 1;
                if ((iterations % 1000) == 0)
                    System.out.println
                        (new java.util.Date() + ": Thread " + id +
                         " finishes " + iterations + " iterations.");
            }
            long endTime = System.currentTimeMillis();
            float elapsedSec = (float) ((endTime - startTime) / 1e3);
            float throughput = ((float) totalIterations) / elapsedSec;
            System.out.println
                ("Thread " + id + " finishes " + iterations +
                 " iterations in: " + elapsedSec +
                 " sec, average throughput: " + throughput + " op/sec.");
        }
        
        /**
         * Perform some insert and delete operations in order to wakeup the
         * checkpointer, cleaner and evictor.
         */
        private void doOperations() throws DatabaseException {
            String dbName = "testDb" + id;
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(true);
            Database db = env.openDatabase(null, dbName, dbConfig);
            Cursor cursor = db.openCursor(null, null);
            doSimpleCursorPutAndDelete(cursor);
            cursor.close();

            if (CHECK_REMOVE_WITH_OPEN_HANDLE) {
                try {
                    Thread.sleep(1000);
                    env.removeDatabase(null, dbName);
                    throw new RuntimeException("Expected LockConflict");
                } catch (LockConflictException expected) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            db.close();
            
            if (random.nextFloat() < .5) {
                env.removeDatabase(null, dbName);
            } else {
                env.truncateDatabase(null, dbName, false);
            }
        }

        /**
         * Write some data to wakeup the checkpointer, cleaner and evictor.
         */
        protected void doSimpleCursorPutAndDelete(Cursor cursor)
            throws DatabaseException {
            
            String[] simpleKeyStrings = {
                    "foo", "bar", "baz", "aaa", "fubar",
                    "foobar", "quux", "mumble", "froboy" };

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry(new byte[10240]);

            for (int i = 0; i < simpleKeyStrings.length; i++) {
                key.setData(simpleKeyStrings[i].getBytes());
                if (cursor.putNoOverwrite(key, data) !=
                    OperationStatus.SUCCESS) {
                    throw new RuntimeException("Could not insert");
                }
            }

            OperationStatus status =
                cursor.getFirst(key, data, LockMode.DEFAULT);

            while (status == OperationStatus.SUCCESS) {
                cursor.delete();
                status = cursor.getNext(key, data, LockMode.DEFAULT);
            }
        }
    }
}
