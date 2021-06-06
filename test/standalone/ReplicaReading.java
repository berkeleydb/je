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
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityIndex;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

/**
 * Applications does reading operations on replica may cause reader transaction
 * deadlocks, since JE ReplayTxn would steal locks to make sure it can finish
 * its own work. Simulate such an application and measure how many retries the
 * reader transactions would do and check whether the log cleaning works as 
 * expected in HA.
 *
 * This test uses DPL and is divided into two phases: ramp up stage and steady 
 * stage. It's not a fail-over test, all replicas are alive during the test.
 *
 * Configurations
 * ==========================================================================
 * envRoot:      environment home root for the whole replication group, it's 
 *               the same as we used in HA unit tests.
 * repNodeNum:  size of the replication group, default is 2.
 * dbSize:       number of records in the database, default is 300.
 * roundPerSync: master would traverse the database for this number before it 
 *               does a sync for the whole group.
 * totalRounds:  total number of traversing the database in the whole test, 
 *               default is 20.
 * txnOps:      number of operations the test wants to do protected by a
 *              transaction, default is 10.
 * nPriThreads: number of threads reading primary index, default is 2.
 * nSecThreads: number of threads reading secondary index, default is 2.
 *
 * Work Flow
 * ==========================================================================
 * During the ramp up stage, master will do "dbSize" insertions and sync whole
 * replication group.
 *
 * During the steady stage, the test would start "nPriThreads + nSecThreads" 
 * reading on replica, all of them read backwards. The reading threads can get
 * records from primary index and secondary index, and check the data 
 * correctness. 
 *
 * At the same time, the master would do "totalRounds * dbSize" updating 
 * operations. It would first delete the smallest "txnOps" records in the 
 * database. Next, it will do updates, and the transaction would abort 
 * randomly. At last, insert "txnOps" new records at the end of the database. 
 * After it traverses "roundPerSync" time on the database, the test would sync 
 * the whole group and check node equality. 
 *
 * After finishing the "totalRounds" database traverse, both the reading 
 * operations on the replica and the update operations on master would stop. 
 * Then the test would close all the replicas and check whether log cleaning 
 * does work in this test.
 *
 * How To Run This Test
 * ==========================================================================
 * All the test configurations have a default value, except the envRoot, so 
 * you need to assign a directory to "envRoot" to start the test, like:
 *    java ReplicaReading -envRoot data
 *
 * If you want to specify some configurations, please see the usage.
 */
public class ReplicaReading {
    /* Master of the replication group. */
    private ReplicatedEnvironment master;
    private RepEnvInfo[] repEnvInfo;
    private boolean runnable = true;
    /* The two variables saves the maximum and minimum reading retry number. */
    private int minNum = 100;
    private int maxNum = 0;
    /* The smallest and largest key in the database. */
    private volatile int beginKey;
    private volatile int endKey;

    /* ----------------Configurable params-----------------*/    
    /* Environment home root for whole replication group. */
    private File envRoot;
    /* Replication group size. */
    private int nNodes = 2;
    /* Database size. */
    private int dbSize = 300;
    /* Steady state would finish after traversing these rounds of database. */
    private int totalRounds = 4000;
    /* Do a sync after traversing the database for these rounds. */ 
    private int roundPerSync= 20; 
    /* Transaction commits after doing this number of operations. */
    private int txnOps = 10;
    /* Thread number of reading PrimaryIndex on replica. */
    private int nPriThreads = 2;
    /* Thread number of reading SecondaryIndex on replica. */
    private int nSecThreads = 2;
    private int subDir = 3;
    /* True if replica reading thread doing reverse reads. */
    private boolean isReverseRead = true;
    /* Size of each JE log file. */
    private long logFileSize = 5000000;
    /* More than enough disk space to hold the data set and allow cleaning. */
    private long maxDisk = 500 * 1000000;
    /* Checkpointer wakes up when JE writes checkpointBytes bytes. */
    private long checkpointBytes = 10000000;
    /* The latch used to start all the threads at the same time. */
    private CountDownLatch startSignal;
    /* The latch used to stop the threads. */
    private CountDownLatch endSignal;
    private final Random random = new Random();
    /* Database and PrimaryIndex used in this test.*/
    private EntityStore dbStore;
    private PrimaryIndex<Integer, RepTestData> primaryIndex;

    /*
     * A map saves abort transactions, represented by txnRounds, maps from
     * txnRounds to Key, so that we can remove those deleted transactions.
     */
    private final ConcurrentHashMap<Integer, Integer> abortMap =
        new ConcurrentHashMap<>();

    private void doRampup()
        throws Exception {

        repEnvInfo = Utils.setupGroup(
            envRoot, nNodes, logFileSize, maxDisk, checkpointBytes,
            subDir, 0, 0);
        master = Utils.getMaster(repEnvInfo);
        RepTestData.insertData
            (Utils.openStore(master, Utils.DB_NAME), dbSize, true);
        beginKey = 1;
        endKey = dbSize;
        Utils.doSyncAndCheck(repEnvInfo);
    }

    private void doSteadyState()
        throws Exception {

        startSignal = new CountDownLatch(1);
        endSignal = new CountDownLatch(nPriThreads + nSecThreads);
        /* Start the threads. */
        startThreads(nPriThreads, false);
        startThreads(nSecThreads, true);
        /* Count down the latch, so that all threads start to work. */
        startSignal.countDown();
        /* Doing the updates. */
        doMasterUpdates();

        /* Print out the minimum and maximum retry number of this test. */
        if (Utils.VERBOSE) {
            System.out.println("The minimum retry number is: " + minNum);
            System.out.println("The maximum retry number is: " + maxNum);
        }

        /* Do the sync until the reading threads finish their work. */
        endSignal.await();

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    /* Start the reading threads. */
    private void startThreads(int threadNum,
                              boolean secondary) {
        for (int i = 0; i < threadNum; i++) {
            Thread thread = new ReplicaReadingThread(repEnvInfo[1].getEnv(), 
                                                     secondary);
            thread.start();
        }
    }

    /* Do the updates on master. */
    private void doMasterUpdates() 
        throws Exception {

        int txnRounds = 0;
        int tempRound = roundPerSync;

        UpdateRange range = new UpdateRange();

        openStore();

        Transaction txn = null;
        while (runnable) {
            if (Utils.VERBOSE) {
                System.out.println
                    ("Updating rounds left on Master: " + totalRounds);
            }

            /* Traverse the database and do updates. */
            for (int i = range.getStart(); i <= range.getEnd(); i++) {
                /* Create a new transaction for every txnOps operations. */
                if ((i - 1) % txnOps == 0) {
                    txn = master.beginTransaction(null, null);
                    txnRounds++;
                }

                /* Do updates. */
                if (range.doDelete(i)) {
                    doDeleteWork(primaryIndex, txn, i);
                } else {
                    /* Updates and inserts are actually putting a record. */
                    doPutWork(primaryIndex, txn, range, i, txnRounds);
                }
            }

            /* Shift the range so that it can traverse the database again. */
            range.shift();

            /* Exit the loop if the updates have been finished. */
            if (--totalRounds == 0) {
                runnable = false;
            }

            /* If a round of traverses finishes, synch the group. */
            if (--tempRound == 0 || !runnable) {
                dbStore.close();
                Utils.doSyncAndCheck(repEnvInfo);
                if (runnable) {
                    openStore();
                }
                tempRound = roundPerSync;
            }
        }
    }

    /* Open the EntityStore. */
    private void openStore()
        throws Exception {

        dbStore = Utils.openStore(master, Utils.DB_NAME);
        primaryIndex = 
            dbStore.getPrimaryIndex(Integer.class, RepTestData.class);
    }

    /* Delete records on the database. */
    private void doDeleteWork(PrimaryIndex<Integer, RepTestData> pIndex,
                              Transaction txn,
                              int key) 
        throws Exception {

        pIndex.delete(txn, key);
        if (key % txnOps == 0) {
            txn.commit();
            /* Increase the beginKey since the smallest key has changed. */
            beginKey += txnOps;
            /* Delete those entries whose values are smaller than beginKey. */
            if (abortMap.size() != 0) {
                ArrayList<Integer> keys = new ArrayList<>();
                for (Map.Entry<Integer, Integer> entry : abortMap.entrySet()) {
                    if (entry.getValue() <= beginKey) {
                        keys.add(entry.getKey());
                    }
                }
                for (Integer abortKey : keys) {
                    abortMap.remove(abortKey);
                }
            }
        }
    }

    /* Put records into database if the operations are updates or inserts. */
    private void doPutWork(PrimaryIndex<Integer, RepTestData> pIndex,
                           Transaction txn,
                           UpdateRange range,
                           int key,
                           int txnRounds) 
        throws Exception {

        /* 
         * Put a record into the database. If the key exists, doing updates. 
         * If the key doesn't exist, doing inserts.
         */
        RepTestData data = new RepTestData();
        data.setKey(key);
        data.setData(key);
        data.setName("test" + txnRounds);
        pIndex.put(txn, data);

        if (key % txnOps == 0) {
            if (range.doUpdate(key)) {
                /* Random abort if it's an update operation. */
                if (random.nextBoolean()) {
                    txn.abort();
                    /* Put this abort data to the abort map. */
                    abortMap.put(txnRounds, key);
                } else {
                    txn.commit();
                }
            } else {
                /* Increase the endKey if the insertion finishes. */
                txn.commit();
                endKey += txnOps;
            }
        }
    }

    protected void parseArgs(String args[]) 
        throws Exception {

        for (int i = 0; i < args.length; i++) {
            boolean moreArgs = i < args.length - 1;
            if (args[i].equals("-h") && moreArgs) {
                envRoot = new File(args[++i]);
            } else if (args[i].equals("-repNodeNum") && moreArgs) {
                nNodes = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-dbSize") && moreArgs) {
                dbSize = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-totalRounds") && moreArgs) {
                totalRounds = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-roundPerSync") && moreArgs) {
                roundPerSync = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-txnOps") && moreArgs) {
                txnOps = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-logFileSize") && moreArgs) {
                logFileSize = Long.parseLong(args[++i]);
            } else if (args[i].equals("-maxDisk") && moreArgs) {
                maxDisk = Long.parseLong(args[++i]);
            } else if (args[i].equals("-checkpointBytes") && moreArgs) {
                checkpointBytes = Long.parseLong(args[++i]);
            } else if (args[i].equals("-nPriThreads") && moreArgs) {
                nPriThreads = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-nSecThreads") && moreArgs) {
                nSecThreads = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-isReverseRead") && moreArgs) {
                isReverseRead = Boolean.parseBoolean(args[++i]);
            } else if (args[i].equals("-subDir") && moreArgs) {
                subDir = Integer.parseInt(args[++i]);
            } else {
                usage("Error: Unknown arg: " + args[i]);
            }
        }

        if (nNodes < 2) {
            throw new IllegalArgumentException
                ("Replication group size should > 2!");
        }

        if (txnOps >= dbSize || dbSize % txnOps != 0) {
            throw new IllegalArgumentException
                ("dbSize should be larger and integral multiple of txnOps!");
        }
    }

    private void usage(String error) {
        System.err.println(error);
        System.err.println
            ("java " + getClass().getName() + "\n" +
             "     [-h <replication group Environment home dir>]\n" +
             "     [-repNodeNum <replication group size>]\n" +
             "     [-dbSize <records' number of the tested database>]\n" +
             "     [-totalRounds <the total number of traversing the " +
             "database insteady state>]\n" +
             "     [-roundPerSync <do a sync in the replication group " + 
             "after traversing the database of this number]\n" +
             "     [-txnOps <number of operations in each transaction>]\n" +
             "     [-logFileSize <size of each log file>]\n" +
             "     [-checkpointBytes <checkpointer wakes up after writing " +
             "these bytes into the on disk log>]\n" +
             "     [-nPriThreads <number of threads reading PrimaryIndex " +
             "on replica>]\n" +
             "     [-nSecThreads <number of threads reading SecondaryIndex " +
             "on replica>]\n" +
             "     [-isReverseRead <true if replica reading threads read " +
             "backwards>]");
        System.exit(2);
    }

    public static void main(String args[]) {
        try {
            ReplicaReading test = new ReplicaReading();
            test.parseArgs(args);
            test.doRampup();
            test.doSteadyState();
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /* 
     * This test traverses the database multiple times. In each traversal, 
     *  - the first <txnOps> records are deleted 
     *  - the rest of the records are updated,
     *  - an additional <txnOps> worth of records are inserted, in order to 
     * keep the database the same size. The class saves the range for delete,
     * update and insert.
     */
    class UpdateRange {
        private int deleteStart;
        private int deleteEnd;
        private int updateStart;
        private int updateEnd;
        private int insertStart;
        private int insertEnd;

        UpdateRange() {
            deleteStart = 1;
            deleteEnd = deleteStart + txnOps - 1;
            updateStart = deleteEnd + 1;
            updateEnd = dbSize;
            insertStart = updateEnd + 1;
            insertEnd = insertStart + txnOps - 1;
        }

        public int getStart() {
            return deleteStart;
        }

        public int getEnd() {
            return insertEnd;
        }

        /* Returns true if the key is in the scope of deletion. */
        boolean doDelete(int index) {
            return (index >= deleteStart) && (index <= deleteEnd);
        }

        /* Returns true if the key is in the scope of updates. */
        boolean doUpdate(int index) {
            return (index >= updateStart) && (index <= updateEnd);
        }

        /* 
         * Adjust the traversal parameters for the next traverse of the 
         * database, since some records have been deleted and some added.
         */
        void shift() {
            deleteStart += txnOps;
            deleteEnd += txnOps;
            updateStart += txnOps;
            updateEnd += txnOps;
            insertStart += txnOps;
            insertEnd += txnOps;
        }
    }

    /* The reading thread on replica. */
    class ReplicaReadingThread extends Thread {
        private final ReplicatedEnvironment repEnv;
        private boolean secondary;
        private final ArrayList<RepTestData> list;

        public ReplicaReadingThread(ReplicatedEnvironment repEnv, 
                                    boolean secondary) {
            this.repEnv = repEnv;
            this.secondary = secondary;
            list = new ArrayList<>();
        }

        @Override
        public void run() {
            try {
                startSignal.await();

                EntityStore repStore = Utils.openStore(repEnv, Utils.DB_NAME);
                EntityIndex<Integer, RepTestData> index =  
                    repStore.getPrimaryIndex(Integer.class, RepTestData.class);
                if (secondary) {
                    index = repStore.getSecondaryIndex
                       ((PrimaryIndex<Integer, RepTestData>) index, 
                         Integer.class, "data");
                }
                
                while (runnable) {
                    int numIters = (endKey - beginKey) / txnOps;
                    int startKey = beginKey;
                    /* Do txnOps read operations during each transaction. */
                    for (int i = 0; runnable && i < numIters; i++) {
                        int start = i * txnOps + startKey;
                        int end = start + txnOps - 1;

                        /* 
                         * If there is no data between start and end, then 
                         * break and get a new start and end. 
                         */
                        if (doRetries(start, end, repEnv, index)) {
                            break;
                        }
                    }
                }
                repStore.close();
                endSignal.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /* Retry if there exists deadlock. */
        private boolean doRetries(int start,
                                  int end,
                                  ReplicatedEnvironment env, 
                                   EntityIndex<Integer, RepTestData> index) {
            boolean success = false;
            boolean noData = false;
            int maxTries = 100;

            for (int tries = 0; !success && tries < maxTries; tries++) {
                try {
                    Transaction txn = env.beginTransaction(null, null);
                    int realStart = 0;
                    EntityCursor<RepTestData> cursor = null;
                    try {
                        cursor = index.entities(txn, null);
                        realStart = cursor.first(null).getKey();
                        cursor.close();
                        cursor = 
                            index.entities(txn, start, true, end, true, null);
                        noData = addRecordsToList(cursor);
                        success = true;
                    } finally {
                        if (cursor != null) {
                            cursor.close();
                        }

                        if (success) {
                            if (noData) {
                                checkNoDataCorrectness(start, realStart, tries);
                            } else {
                                checkCorrectness(tries);
                            }
                            txn.commit();
                        } else {
                            txn.abort();
                        }
                        list.clear();
                    }
                } catch (LockConflictException e) {
                    success = false;
                }
            }

            return noData;
        }

        /* 
         * If there is no data in this cursor, return true. If there exists
         * data in the cursor, add the datas into the list and return false.
         */
        private boolean addRecordsToList(EntityCursor<RepTestData> cursor) 
            throws DatabaseException {

            if (isReverseRead) {
                RepTestData data = cursor.last(null);
                if (data == null) {
                    return true;
                } else {
                    list.add(data);
                    while ((data = cursor.prev(null)) != null) {
                        list.add(data);
                    }
                }
            } else {
                RepTestData data = cursor.first(null);
                if (data == null) {
                    return true;
                } else {
                    list.add(data);
                    while ((data = cursor.next(null)) != null) {
                        list.add(data);
                    }
                }
            }

            return false;
        }

        /* Check the correctness if there is no data in the cursor. */
        private void checkNoDataCorrectness(int start,
                                            int realStart,
                                            int tries) {
            /* Expect the list size to 0. */
            if (list.size() != 0) {
                System.err.println(
                    "Error: The expected number of records should be 0, " +
                    "but it is " + list.size() + "!");
                System.exit(-1);
            }

            /* 
             * The actual beginKey should be larger than the specified 
             * beginKey, and the distance between them should be integral
             * multiple of txnOps.
             */
            if (realStart < start || ((realStart - start) % txnOps != 0)) {
                System.err.println(
                    "Error: There are some deleted key exists in database!");
                System.err.println("Expected start key is: " + start +
                                   ", real start key is: " + realStart);
                System.exit(-1);
            } 
            updateRetries(tries);
        }

        private void checkCorrectness(int tries) {
            if (list.size() == txnOps) {
                int minus = isReverseRead ? 1 : -1;
                RepTestData firstData = list.get(0);

                /* Check if the firstData is an abort data. */
                if (!("").equals(firstData.getName().substring(4))) {
                    Integer txnRounds =
                        new Integer(firstData.getName().substring(4));
                    /* If this data is in the abort map, fail the test. */
                    if (abortMap.get(txnRounds) != null) {
                        System.err.println("Error: Read an aborted record.");
                        System.exit(-1);
                    }
                }

                /* Check that records in this list are valid. */
                for (int i = 0; i < list.size(); i++) {
                    if (!firstData.logicEquals(list.get(i), i * minus)) {
                        System.err.println("Error: Reading data is wrong!" +
                                           "FirstData: " + firstData + 
                                           "WrontData: " + list.get(i));
                        for (RepTestData each : list) {
                            System.err.println(each);
                        }
                        System.exit(-1);
                    }
                }
                updateRetries(tries);
            } else {
                System.err.println(
                    "Error: The expected number of records should be: " +
                    txnOps + ", but it is " + list.size() + "!");
                System.exit(-1);
            }
        }

        private void updateRetries(int tries) {
            /* Assign the value to maxNum and minNum. */
            synchronized(this) {
                maxNum = maxNum < tries ? tries : maxNum;
                minNum = minNum < tries ? minNum : tries;
            }
            if (tries > 0 && Utils.VERBOSE) {
                System.err.println("Retries this round: " + tries);
            }
        }
    }
}
