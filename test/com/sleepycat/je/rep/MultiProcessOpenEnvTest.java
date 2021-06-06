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

package com.sleepycat.je.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.junit.JUnitProcessThread;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Every replication node manages a single replicated JE environment
 * directory. The environment follows the usual regulations governing a JE
 * environment; namely, only a single read/write process can access the
 * environment at a single point in time.
 *
 * In this unit test, exercise the rules by opening a single replicated
 * environment from two processes.  One process will open a
 * ReplicatedEnvironment handle. The other will open a standalone
 * environment. The expected results are:
 *
 * Env handle\Open    Open for write     Open for read
 *=====================   ===============
 * ReplicatedEnvironment  OK.                IllegalArgEx
 *
 * Environment            IllegalArgEx       OK. Verify that we are
 *                                           seeing a snapshot in the presence
 *                                           of ongoing changes (in another
 *                                           process) at a master and Replica
 *                                           and that a reopen of the handle
 *                                           updates the snapshot.
 */
public class MultiProcessOpenEnvTest extends TestBase {
   
    private final File envRoot; 
    private final File masterEnvHome;
    private final File replicaEnvHome;
    private final File lockFile;
    private static final int MAX_RETRIES = 20;
    private static final String sleepInterval = "5000";
    private static final String DB_NAME = "testDB";
    private static final String LOCK_FILE_NAME = "filelocks.txt";
    
    /* Name of the process which opens a ReplicatedEnvironment. */
    private static final String repProcessName = "repProcess";
    /* Name of the process which opens a standalone Environment. */
    private static final String envProcessName = "envProcess";
    
    public MultiProcessOpenEnvTest() 
        throws Exception {

        envRoot = SharedTestUtils.getTestDir();
        /* Make rep0 as the environment home. */
        File[] envHomes = RepTestUtils.makeRepEnvDirs(envRoot, 2);
        masterEnvHome = envHomes[0];
        replicaEnvHome = envHomes[1];
        lockFile = new File(envRoot, LOCK_FILE_NAME);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a replicated Environment on 
     *      envHome, then sleeps.
     *   2. Start a new process, p2, which opens a r/w standalone Environment 
     *      on the same envHome.
     *   3. p2 should get an EnvironmentLockedException and exit with value 4.
     *   4. p1 should exit with value 0.
     */
    @Test
    public void testEnvWriteOnRepEnv() {
        JUnitProcessThread repThread = getRepProcess(true);
        JUnitProcessThread envThread = getEnvProcess(false, false);

        startThreads(repThread, envThread, 2000);

        checkExitVals(repThread, 0, envThread, 4);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a replicated Environment on 
     *      envHome, then sleeps.
     *   2. Start a new process, p2, which opens a read only standalone 
     *      Environment on the same envHome.
     *   3. Both p1 and p2 should exit normally with value 0.
     */
    @Test
    public void testEnvReadOnRepEnv() {
        JUnitProcessThread repThread = getRepProcess(true);
        JUnitProcessThread envThread = getEnvProcess(true, false);
        
        startThreads(repThread, envThread, 2000);

        checkExitVals(repThread, 0, envThread, 0);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a r/w standalone Environment on 
     *      envHome, then sleeps.
     *   2. Start a new process, p2, which opens a replicated Environment on
     *      the same envHome.
     *   3. p2 should get an EnvironmentLockedException and exit with value 1.
     *   4. p1 should exit with value 0.
     */
    @Test
    public void testRepEnvOnEnvWrite() {
        JUnitProcessThread repThread = getRepProcess(false);
        JUnitProcessThread envThread = getEnvProcess(false, true);

        startThreads(envThread, repThread, 500);        

        checkExitVals(repThread, 1, envThread, 0);
    }

    /* 
     * Test the following case:
     *   1. Start a process, p1, which opens a read only standalone Environment
     *      on envHome, then sleeps.
     *   2. Start a new process, p2, which opens a replicated Environment on
     *      the same envHome.
     *   3. p2 should get an UnsupportedOperationException and exit with 
     *      value 2.
     *   4. p1 should exit with value 0.
     */
    @Test
    public void testRepEnvOnEnvRead() {
        testRepEnvOnEnvWrite();

        JUnitProcessThread repThread = getRepProcess(false);
        JUnitProcessThread envThread = getEnvProcess(true, true);

        startThreads(envThread, repThread, 500);

        checkExitVals(repThread, 2, envThread, 0);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a replicated Environment on 
     *      envHome, then sleeps.
     *   2. Start a new process, p2, which opens a replicated Environment on 
     *      the same envHome.
     *   3. p2 should get an EnvironmentLockedException and exit with value 1.
     *   4. p1 should exit with value 0.
     */
    @Test
    public void testRepEnvOnRepEnv() {
        JUnitProcessThread repThread1 = getRepProcess(true);
        JUnitProcessThread repThread2 = getRepProcess(false);

        startThreads(repThread1, repThread2, 2000);

        checkExitVals(repThread1, 0, repThread2, 1);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a r/w standalone Environment on
     *      envHome, then sleeps.
     *   2. Start a new process, p2, which opens a r/w standalone Environment 
     *      on the same envHome.
     *   3. p2 should get an EnvironmentLockedException and exit with value 4.
     *   4. p1 should exit with value 0.
     */ 
    @Test
    public void testEnvWriteOnEnvWrite() {
        JUnitProcessThread envThread1 = getEnvProcess(false, true);
        JUnitProcessThread envThread2 = getEnvProcess(false, false);

        startThreads(envThread1, envThread2, 500);

        checkExitVals(envThread1, 0, envThread2, 4);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a r/w standalone Environment on
     *      envHome, then sleeps.
     *   2. Start a new process, p2, which opens a read only standalone 
     *      Environment on the same envHome.
     *   3. Both p1 and p2 should exit with value 0.
     */
    @Test
    public void testEnvWriteOnEnvRead() {
        JUnitProcessThread envThread1 = getEnvProcess(false, true);
        JUnitProcessThread envThread2 = getEnvProcess(true, false);

        startThreads(envThread1, envThread2, 2000);

        checkExitVals(envThread1, 0, envThread2, 0);
    }

    /*
     * Test the following case:
     *   1. Start a process, p1, which opens a read only standalone Environment 
     *      on envHome, then sleeps.
     *   2. Start a new process, p2, which opens a read only standalone 
     *      Environment on the same envHome.
     *   3. Both p1 and p2 should exit with value 0.
     */
    @Test
    public void testEnvReadOnEnvRead() 
        throws Throwable {

        if (readPreserveRecordVersionProperty()) {
            return;
        }

        testEnvWriteOnEnvRead();

        /* Write some data. */
        JUnitProcessThread writeThread = getEnvProcess(false, true, "-1");
        writeThread.start();
        writeThread.finishTest();
        assertTrue(writeThread.getExitVal() == 0);

        JUnitProcessThread envThread1 = getEnvProcess(true, true);
        JUnitProcessThread envThread2 = getEnvProcess(true, false);

        startThreads(envThread1, envThread2, 500);

        checkExitVals(envThread1, 0, envThread2, 0);
    }

    private boolean readPreserveRecordVersionProperty() 
        throws Exception {

        FileInputStream fis = 
            new FileInputStream(new File(envRoot, "je.properties"));
        Properties jeProperties = new Properties();
        jeProperties.load(fis);

        return new Boolean(jeProperties.getProperty
            (RepParams.PRESERVE_RECORD_VERSION.getName()));
    }

    /* 
     * Test the following case:
     *   1. Start process p1:
     *      1. It will get FileLock A and FileLock C at the begining to make 
     *         sure it gets theset two locks before p2.
     *      2. Then it will start a new replication group with two replicas, 
     *         and do some inserts.
     *      3. Sync all the nodes and do a node equality check.
     *      4. Release lock A.
     *      5. Try to get FileLock B.
     *   2. Start process p2 right after staring p1:
     *      1. It will get FileLock B at the begining to make sure p1 gets lock
     *         B before p1.
     *      2. Try to get lock A, if it gets A, which means there are some data
     *         on the replicas, then open two read only standalone Environments
     *         on the two replicas.
     *      3. Release lock A and lock B.
     *      4. Read the records and do a compare between replicas, also check 
     *         to see if the values are expected.
     *      5. Try to get FileLock C.
     *   3. When p1 gets FileLock B, it continues:
     *      1. When p1 gets FileLock B, it knows p2 has read a snapshot, so it
     *         can do further updates.
     *      2. Do updates and make sure the replicas have same data.
     *      3. Release FileLock C.
     *      4. Exit the process.
     *   4. When p2 gets FileLock C, it continues:
     *      1. When p2 gets FileLock C, it knows p1 has finished updates, then
     *         it does read on replicas.
     *      2. Do the compare
     *      3. Release FileLock C and exit.
     *   5. The two processes should exit successfully with value 0.
     */
    @Test
    public void testEnvReadSnapshotOnRepEnv() {
        /* Start the process which starts a writing replication group. */
        String[] repCommand = new String[5];
        repCommand[0] = 
            "com.sleepycat.je.rep.MultiProcessOpenEnvTest$RepGroupWriteProcess";
        repCommand[1] = envRoot.getAbsolutePath();
        repCommand[2] = DB_NAME;
        /* Make process sleep for a while to make sure p2 get lock BBB. */
        repCommand[3] = "1000";
        repCommand[4] = lockFile.getAbsolutePath();
        JUnitProcessThread p1 = 
            new JUnitProcessThread(repProcessName, repCommand);

        /* Start the process which starts reading Environments on replicas. */
        String[] envCommand = new String[5];
        envCommand[0] = "com.sleepycat.je.rep.MultiProcessOpenEnvTest$" +
                        "EnvReadRepGroupProcess";
        envCommand[1] = masterEnvHome.getAbsolutePath();
        envCommand[2] = replicaEnvHome.getAbsolutePath();
        envCommand[3] = DB_NAME;
        envCommand[4] = lockFile.getAbsolutePath();
        JUnitProcessThread p2 =
            new JUnitProcessThread(envProcessName, envCommand);

        startThreads(p1, p2, 300);

        checkExitVals(p1, 0, p2, 0);
    }

    /* Start these two processes. */
    private void startThreads(JUnitProcessThread thread1,
                              JUnitProcessThread thread2,
                              long sleepTime) {
        thread1.start();
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread2.start();
    }

    /* Create a process which opens a replicated Environment. */
    private JUnitProcessThread getRepProcess(boolean sleep) {
        String[] repCommand = new String[3];
        repCommand[0] = 
            "com.sleepycat.je.rep.MultiProcessOpenEnvTest$RepEnvProcess";
        repCommand[1] = masterEnvHome.getAbsolutePath();
        repCommand[2] = (sleep ? sleepInterval : "0");

        return new JUnitProcessThread(repProcessName, repCommand);
    }

    /* Create a process which opens a standalone Environment. */
    private JUnitProcessThread getEnvProcess(boolean readOnly, 
                                             boolean sleep) {
        return getEnvProcess(readOnly, sleep, sleepInterval);
    }

    private JUnitProcessThread getEnvProcess(boolean readOnly,
                                             boolean sleep,
                                             String sleepTime) {
        String[] envCommand = new String[4];
        envCommand[0] = 
            "com.sleepycat.je.rep.MultiProcessOpenEnvTest$EnvProcess";
        envCommand[1] = masterEnvHome.getAbsolutePath();
        envCommand[2] = (readOnly ? "true" : "false");
        envCommand[3] = (sleep ? sleepTime : "0");

        return new JUnitProcessThread(envProcessName, envCommand);
    }

    /* Check the exit value of processes. */
    private void checkExitVals(JUnitProcessThread thread1, 
                               int exitVal1,
                               JUnitProcessThread thread2,
                               int exitVal2) {
        /* End these threads. */
        try {
            thread1.finishTest();
            thread2.finishTest();
        } catch (Throwable t) {
            System.err.println(t.toString());
        }

        /* Check whether the processes exit with expected values. */
        assertEquals(thread1.getExitVal(), exitVal1);
        assertEquals(thread2.getExitVal(), exitVal2);
    }

    /**
     * Open a ReplicatedEnvironment depends on the configuration.
     */
    static class RepEnvProcess {
        private final File envHome;

        /* 
         * Sleep interval for waiting a ReplicatedEnvironment to open on this
         * ReplicatedEnvironment. 
         */
        private final long sleepTime;
        private RepEnvInfo repEnvInfo;

        public RepEnvProcess(File envHome, long sleepTime) {
            this.envHome = envHome;
            this.sleepTime = sleepTime;
        }

        public void openEnv() {
            try {
                Durability durability = 
                    new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                                   Durability.SyncPolicy.WRITE_NO_SYNC,
                                   Durability.ReplicaAckPolicy.ALL);
                EnvironmentConfig envConfig = 
                    RepTestUtils.createEnvConfig(durability);
                ReplicationConfig repConfig =
                    RepTestUtils.createRepConfig(1);
                repEnvInfo = RepTestUtils.setupEnvInfo
                    (envHome, envConfig, repConfig, null);
                repEnvInfo.openEnv();
                Thread.sleep(sleepTime);
            } catch (EnvironmentLockedException e) {

                /* 
                 * Exit the process with value 1, don't print out the exception
                 * since it's expected.
                 */
                System.exit(1);
            } catch (UnsupportedOperationException e) {

                /* 
                 * Exit the process with value 2, don't print out the exception
                 * since it's expected. 
                 *
                 * Note: this exception thrown because we can't start a 
                 * replicated Environment on an existed standalone Environment.
                 */
                System.exit(2);
            } catch (Exception e) {
                /* Dump unexpected exceptions, exit processs with value 3. */
                e.printStackTrace();
                System.exit(3);
            } finally {
                if (repEnvInfo.getEnv() != null) {
                    repEnvInfo.closeEnv();
                }
            }
        }

        public static void main(String args[]) {
            RepEnvProcess thread = 
                new RepEnvProcess(new File(args[0]), new Long(args[1]));
            thread.openEnv();
        }
    }

    /**
     * Open a standalone Environment, specifying the configuration.
     */
    static class EnvProcess {
        private final File envHome;
        private final boolean readOnly;

        /* 
         * Sleep interval for waiting a ReplicatedEnvironment to open on this 
         * Environment. 
         */
        private final long sleepTime;
        private Environment env;

        public EnvProcess(File envHome, boolean readOnly, long sleepTime) {
            this.envHome = envHome;
            this.readOnly = readOnly;
            this.sleepTime = sleepTime;
        }

        public void openEnv() {
            try {
                EnvironmentConfig envConfig = new EnvironmentConfig();
                envConfig.setReadOnly(readOnly);
                envConfig.setAllowCreate(!readOnly);

                env = new Environment(envHome, envConfig);
                if (sleepTime < 0) {
                    DatabaseConfig dbConfig = new DatabaseConfig();
                    dbConfig.setAllowCreate(!readOnly);
                    Database db = env.openDatabase(null, "testDB", dbConfig);

                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry data = new DatabaseEntry();
                    for (int i = 1; i <= 50; i++) {
                        IntegerBinding.intToEntry(i, key);
                        StringBinding.stringToEntry("herococo", data);
                        db.put(null, key, data);
                    }
                    db.close();
                } else {
                    Thread.sleep(sleepTime);
                }
            } catch (EnvironmentLockedException e) {

                /* 
                 * Exit the process with value 4, the exception is expected in 
                 * this casse, don't dump it out. 
                 */
                System.exit(4);
            } catch (Exception e) {
                /* Dump unexpected exception, exit process with value 5. */
                e.printStackTrace();
                System.exit(5);
            } finally {
                if (env != null) {
                    env.close();
                }
            }
        }

        public static void main(String args[]) {
            EnvProcess process = new EnvProcess(new File(args[0]), 
                                                new Boolean(args[1]), 
                                                new Long(args[2]));
            process.openEnv();
        }
    }

    /* Close a RandomAccessFile. */
    private static void closeLockFile(RandomAccessFile lockFile) {
        if (lockFile != null) {
            try {
                lockFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /* Get a FileLock with retry. */
    private static FileLock getLockWithReTry(FileChannel channel, 
                                             long position, 
                                             long size) 
        throws Exception {

        int retries = 0;
        FileLock lock = channel.tryLock(position, size, false);
        
        while (lock == null && retries <= MAX_RETRIES) {
            Thread.sleep(1000);
            lock = channel.tryLock(position, size, false);
            retries++;
        }

        if (lock == null) {
            System.err.println("Can't get a FileLock during " + 
                               MAX_RETRIES + " seconds.");
            System.exit(6);
        }

        return lock;
    }

    /**
     * Open a replication group and do some work.
     */
    static class RepGroupWriteProcess {
        private final File envRoot;
        private RandomAccessFile lockFile;
        private final long sleepTime;
        private final String dbName;
        private RepEnvInfo[] repEnvInfo;

        public RepGroupWriteProcess(File envRoot, 
                                    String dbName, 
                                    long sleepTime,
                                    RandomAccessFile lockFile) {
            this.envRoot = envRoot;
            this.dbName = dbName;
            this.sleepTime = sleepTime;
            this.lockFile = lockFile;
        }

        public void run() {
            try {
                /* Get FileLocks. */
                FileChannel channel = lockFile.getChannel();
                FileLock lockA = channel.lock(1, 1, false);
                FileLock lockC = channel.lock(3, 1, false);

                ReplicatedEnvironment master = getMaster();
                doWork(master, dbName, 1);
                /* Release lock A so that read process can do reads. */
                lockA.release();

                /* Make sure read process get lock B before this process. */
                Thread.sleep(sleepTime);

                /* Get lock B means read process finish reading, do updates. */
                FileLock lockB = getLockWithReTry(channel, 2, 1);
                doWork(master, dbName, 101);

                /* Release lock B and lock C. */
                lockB.release();
                lockC.release();
            } catch (Exception e) {
                /* Dump exceptions and exit with value 6. */
                e.printStackTrace();
                System.exit(7);
            } finally {
                RepTestUtils.shutdownRepEnvs(repEnvInfo);
                closeLockFile(lockFile);
            }
        }

        /* Start a replication group with 2 nodes and returns the master. */
        private ReplicatedEnvironment getMaster() 
            throws Exception {

            Durability durability =
                new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                               Durability.SyncPolicy.WRITE_NO_SYNC,
                               Durability.ReplicaAckPolicy.ALL);
            EnvironmentConfig envConfig =
                RepTestUtils.createEnvConfig(durability);
            repEnvInfo =
                RepTestUtils.setupEnvInfos(envRoot, 2, envConfig);
            
            return RepTestUtils.joinGroup(repEnvInfo);
        }

        /* Insert 100 records begins with the beginKey. */
        private void doWork(Environment master, String dbName, int beginKey)
            throws Exception {

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);

            /* Insert/Update the records of the database. */
            Database db = master.openDatabase(null, dbName, dbConfig);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            for (int i = 0; i < 100; i++) {
                IntegerBinding.intToEntry(beginKey + i, key);
                StringBinding.stringToEntry("herococo", data);
                db.put(null, key, data);
            }
            db.close();

            /* 
             * Do a sync at the end of the stage to make sure master and 
             * replica have the same data set.
             */
            VLSN commitVLSN = 
                RepTestUtils.syncGroupToLastCommit(repEnvInfo, 
                                                   repEnvInfo.length);
            RepTestUtils.checkNodeEquality(commitVLSN, false, repEnvInfo);
        }

        public static void main(String args[]) {
            try {
                RandomAccessFile lockFile = 
                    new RandomAccessFile(args[3], "rw");
                RepGroupWriteProcess process = 
                    new RepGroupWriteProcess(new File(args[0]), args[1], 
                                             new Long(args[2]), lockFile);
                process.run();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(8);
            }
        }
    }

    /**
     * Open a ReplicatedEnvironment depends on the configuration.
     */
    static class EnvReadRepGroupProcess {
        private final File masterEnvHome;
        private final File replicaEnvHome;
        private RandomAccessFile lockFile;
        private final String dbName;
        private final ArrayList<TestObject> prevMasterRecords = 
            new ArrayList<TestObject>();
        private final ArrayList<TestObject> prevReplicaRecords =
            new ArrayList<TestObject>();
        private final ArrayList<TestObject> newMasterRecords =
            new ArrayList<TestObject>();
        private final ArrayList<TestObject> newReplicaRecords =
            new ArrayList<TestObject>();
        private Environment master;
        private Environment replica;

        public EnvReadRepGroupProcess(File masterEnvHome,
                                      File replicaEnvHome, 
                                      String dbName,
                                      RandomAccessFile lockFile) {
            this.masterEnvHome = masterEnvHome;
            this.replicaEnvHome = replicaEnvHome;
            this.dbName = dbName;
            this.lockFile = lockFile;
        }

        public void run() {
            try {
                FileChannel channel = lockFile.getChannel();
                /* Get lock B so that write process waits. */
                FileLock lockB = channel.lock(2, 1, false);

                /* Get lock A means write process finish the first phase. */
                FileLock lockA = getLockWithReTry(channel, 1, 1);
                openEnvironments();

                /* Release lock A and B so that write process can continue. */
                lockB.release();
                lockA.release();

                /* Read records and check the node equality. */
                readRecords
                    (master, prevMasterRecords, replica, prevReplicaRecords);
                doEqualityCompare(prevMasterRecords, prevReplicaRecords, 100);
                closeEnvironments();

                /* Get lock C means second phase of write process finishes. */
                FileLock lockC = getLockWithReTry(channel, 3, 1);
                /* Reopen and read records, then do the compare. */
                openEnvironments();
                readRecords
                    (master, newMasterRecords, replica, newReplicaRecords);
                doEqualityCompare(newMasterRecords, newReplicaRecords, 200);

                /* Do compare between two snapshots. */
                doDiffCompare(prevMasterRecords, newMasterRecords);
                doDiffCompare(prevReplicaRecords, newReplicaRecords);
                lockC.release();
            } catch (Exception e) {
                /* Dump exceptions and exit process with value 7.*/
                e.printStackTrace();
                System.exit(9);
            } finally {
                closeEnvironments();
                closeLockFile(lockFile);
            }
        }

        /* Open read only standalone Environment on replicated nodes. */
        private void openEnvironments()
            throws Exception {

            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setReadOnly(true);
            envConfig.setAllowCreate(false);

            master = new Environment(masterEnvHome, envConfig);
            replica = new Environment(replicaEnvHome, envConfig);
        }

        /* Close the Environments after finishing reading operations. */
        private void closeEnvironments() {
            if (master != null) {
                master.close();
            }
            if (replica != null) {
                replica.close();
            }
        }

        /* Read records for these two Environments. */
        private void readRecords(Environment masterEnv, 
                                 ArrayList<TestObject> masterData, 
                                 Environment replicaEnv,
                                 ArrayList<TestObject> replicaData)
            throws Exception {

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);

            doRead(masterEnv, dbConfig, masterData);
            doRead(replicaEnv, dbConfig, replicaData);
        }

        /* Do the real reading work. */
        private void doRead(Environment env, 
                            DatabaseConfig dbConfig,
                            ArrayList<TestObject> list) 
            throws Exception {

            Database db = env.openDatabase(null, dbName, dbConfig);
            Cursor cursor = db.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while (OperationStatus.SUCCESS == 
                   cursor.getNext(key, data, null)) {
                list.add(new TestObject(IntegerBinding.entryToInt(key), 
                                        StringBinding.entryToString(data)));
            }
            cursor.close();
            db.close();
        }
        
        /* Do compare between master and replica, expected to be the same. */
        private void doEqualityCompare(ArrayList<TestObject> masterData, 
                                       ArrayList<TestObject> replicaData,
                                       int expectedSize) {
            assertEquals(masterData.size(), replicaData.size());
            for (int i = 0; i < masterData.size(); i++) {
                assertEquals(masterData.get(i), replicaData.get(i));
            }
            assertEquals(masterData.size(), expectedSize);
            for (int i = 0; i < expectedSize; i++) {
                TestObject object = new TestObject(i + 1, "herococo");
                assertEquals(masterData.get(i), object);
                assertEquals(replicaData.get(i), object);
            }
        }

        /* Do compare between two snapshots, should be different. */
        private void doDiffCompare(ArrayList<TestObject> prevData,
                                   ArrayList<TestObject> newData) {
            assertEquals(newData.size(), prevData.size() * 2);
            for (int i = 0; i < prevData.size(); i++) {
                assertEquals(prevData.get(i), newData.get(i));
            }
            for (int i = 0; i < prevData.size(); i++) {
                assertEquals(newData.get(i + 100).getKey() - 100,
                             prevData.get(i).getKey());
            }
        }

        static class TestObject {
            private final int key;
            private final String name;

            public TestObject(int key, String name) {
                this.key = key;
                this.name = name;
            }

            public int getKey() {
                return key;
            }

            public String getName() {
                return name;
            }

            public boolean equals(Object obj) {
                if (obj == null || !(obj instanceof TestObject)) {
                    return false;
                }

                TestObject tObj = (TestObject) obj;
                if (tObj.getKey() == key && tObj.getName().equals(name)) {
                    return true;
                }

                return false;
            }
        }
 
        public static void main(String args[]) {
            try {
                RandomAccessFile lockFile = new RandomAccessFile(args[3], "rw");
                EnvReadRepGroupProcess process = 
                    new EnvReadRepGroupProcess(new File(args[0]), 
                                               new File(args[1]), 
                                               args[2], 
                                               lockFile);
                process.run();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(8);
            }
        }
    }
}
