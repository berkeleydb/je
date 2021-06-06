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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * MasterTransferExercise exercises master transfer at the JE level. The test
 * starts a JE HA group of a configurable size, devoting a thread to each
 * node. If a node is a master, it will in turn start numAppThreads threads,
 * which each executes a stream of application operations to present a busy
 * write-only workload. The operation load can be configured so that it
 * consists of record inserts, record updates, or database operations.
 *
 * Concurrently, a task requesst a master transfer. The master transfer
 * protocol is a two phase affair which will hold block transactions in the
 * second phase, so the goal is to exercise that vulnerable blocking period
 * by making the application load and the transfer execute in parallel.
 *
 * The test is a success if:
 * - no node experiences a RollbackProhibitedException
 * - in theory, the nodes should all take roughly equal turns being the master
 * - the test maintains a set of committed data, and checks that each node
 * contains the correct data.
 *
 * One flavor of test has mimics an application load of data record inserts,
 * updates and deletes, while the other executes database creates, renames, and
 * removes. Both flavors must be tested, because there is specialized txn
 * handling for database operations.
 *
 * Here's a timeline of what happens:
 * 
 * Test starts up N threads, each instantiating a MasterTransferExercise
 * instance, which operates a JE HA node.
 *
 * Each exercise instance waits to become the master. If it is the master,it
 * spawns X threads to act as application threads, and 1 thread to instigate a
 * master transfer.
 *
 * When the master transfer begins, the application threads are caught mid-txn,
 * and must abort and cleanup the inflight txns. The old master becomes a
 * replica.
 *
 * At the end of the test, each node should contain the same data. The same
 * environment handle should have been used for the entire time of the test.
 */

public class MasterTransferExercise {

    private static final String TEST = "TEST: ";
    private static final String TEST_FAIL = "FAILURE - ";
    private static final String DB_NAME = "MasterTransferDB";
    private static final String REP_GROUP_NAME = "MasterTransferGroup";
    private static final int TRANSFER_TIME = 10;
    private static final TimeUnit TRANSFER_UNIT = TimeUnit.SECONDS;
    private static final String NODE_NAME_PREFIX = "node";

    /*
     * Types of test data -- either regular records, or creating and
     * removing database.
     */
    private static final String GEN_KEY_DATA = "KeyData";
    private static final String GEN_DB_OPS = "DbOps";
    /*
     * Coordinate the node threads that execute as the replication nodes:
     * - shutdownBarrier: wait for them all to finish inserts before closing
     *  env and losing quorum
     * - verifyBarrier: wait for them all to finish shutting down before
     * reopening for verify.
     */
    private static CyclicBarrier initBarrier;
    private static CyclicBarrier startWorkBarrier;
    private static CyclicBarrier shutdownBarrier;
    private static CyclicBarrier verifyBarrier;

    /*
     * Report on test stats, and verify db contents after all nodes are
     * finished.
     */
    private static CountDownLatch waitForInsertEnd;

    /*
     * Let the main thread wait for the nodes to be done before letting the
     * test complete.
     */
    private static CountDownLatch waitForVerify;

    /* Coordinate which records are used in the test */
    private static DataGenerator dataGenerator;

    private static TestConfiguration testConfig;
    private static AtomicBoolean testSuccess;
    private static AtomicInteger totalMasterStints;

    private final String nodeName;
    private final int nodeId;
    private Logger logger;
    private Formatter formatter;
    private ReplicatedEnvironment repEnv;
    private Database db;

    private AtomicInteger numMasterStints = new AtomicInteger(0);
    private AtomicInteger taskNumber = new AtomicInteger(0);

    public static void main(String[] argv)
        throws Exception {

        testConfig = new TestConfiguration(argv);
        System.out.println(TEST + testConfig.showParams());

        testSuccess = new AtomicBoolean(true);
        totalMasterStints = new AtomicInteger(0);

        /*
         * waitForInit provides coordination so the test begins only after all
         * the environments are open.
         */
        initBarrier = new CyclicBarrier(testConfig.groupSize);
        startWorkBarrier = new CyclicBarrier(testConfig.groupSize);
        shutdownBarrier = new CyclicBarrier(testConfig.groupSize);
        verifyBarrier = new CyclicBarrier(testConfig.groupSize);

        waitForInsertEnd = new CountDownLatch(testConfig.groupSize);
        waitForVerify = new CountDownLatch(testConfig.groupSize);

        /* Each "tester" is a single JE environment */
        MasterTransferExercise[] testers =
            new MasterTransferExercise[testConfig.groupSize];

        /*
         * Each tester will countdown on the waitForInsertEnd latch when it has
         * run for the prescribed time limit.
         */
        if (testConfig.testType.equals(GEN_KEY_DATA)) {
            dataGenerator = new DataAndKeyGenerator();
        } else {
            dataGenerator = new DbOpsGenerator();
        }

        for (int i = 0; i < testConfig.groupSize; i++) {
            final MasterTransferExercise test =
                new MasterTransferExercise(i + 1);
            testers[i] = test;
            Thread t = new Thread() {
                    @Override
                        public void run() {
                        test.runTest();
                    }};
            t.start();
        }
        waitForInsertEnd.await();

        /*
         * Verify that each node contains the expected data. The verify will
         * issue an ack-all transaction to ensure that everyone's environment
         * is up to date, so it must be spawned by multiple threads, rather
         * than serially.
         */
        for (int i = 0; i < testConfig.groupSize; i++) {
            final MasterTransferExercise test = testers[i];
            Thread t = new Thread() {
                    @Override
                    public void run() {
                        test.verify();
                    }};
            t.start();
        }
        waitForVerify.await();

        /*
         * Check that the nodes shared mastership a reasonable number of
         * times. Arbitrarily, a sufficient amount of sharing would be a fifth
         * of the average number of master stints.
         */
        int minStints = (totalMasterStints.get()/testConfig.groupSize)/5;
        for (int i = 0; i < testConfig.groupSize; i++) {
            testers[i].reportStints(minStints);
        }

        System.out.println(TEST +  (testSuccess.get() ? "SUCCEEDED" :
                           "FAILED"));
    }

    MasterTransferExercise(int nodeId) {

        this.nodeName = NODE_NAME_PREFIX + nodeId;
        this.nodeId = nodeId;
    }

    /**
     * Main thread, runs the replication node.
     *
     * - open the node
     * - when the node becomes master, do inserts and simultaneously request
     * a master transfer.
     * - when the node loses mastership, close all application txns.
     * - loop and wait for the next stint as master.
     *
     * The loop ends when the test has executed for the prescribed amount of
     * time, or an unexpected exception has been thrown.
     * @throws InterruptedException
     */
    final void runTest() {

        try {
            /* Initialize the environment */
            setupEnvironment(false);

            /* Wait for all nodes to open their environments */
            initBarrier.await();

            /*
             * If you are the master, load up any existing records into the
             * committedRecords set, so that the test verification works even
             * if the test is running from a non-empty db.
             */
            if (repEnv.getState() == State.MASTER) {
                Set<TestRecord> existing =
                    dataGenerator.readTestRecords(nodeName, repEnv, db);
                System.out.println(TEST + "loading " + existing.size() +
                                   " pre-existing records");
                dataGenerator.updateCommittedRecords(existing);
            }

            startWorkBarrier.await();

            /* The test will only run for "testMinutes" amount of time */
            long endTime = System.currentTimeMillis() +
                TimeUnit.MINUTES.toMillis(testConfig.testMinutes);

            ResettableMasterListener masterWaiter =
                new ResettableMasterListener();
            repEnv.setStateChangeListener(masterWaiter);

            final Map<Transaction,AbortProblem> failedTxns =
                new ConcurrentHashMap<Transaction, AbortProblem>();
            while (true) {
                try {
                    /*
                     * Wait until the node becomes a master, so it will do
                     * work.
                     */
                    LoggerUtils.logMsg(logger, formatter, Level.CONFIG,
                            "Waiting for mastership");

                    /*
                     * Come out of the wait because either
                     * a. the test timed out
                     * b. this node is a master. It did some work, and
                     *    triggers a master-replica transition
                     * c. this node is a replica. The application thread
                     *    has nothing to do, so it will loop around and wait
                     *    for a new state change.
                     */
                    long timeLeftInTest = endTime - System.currentTimeMillis();
                    boolean gotMaster =
                        masterWaiter.awaitMastership(timeLeftInTest,
                                                     TimeUnit.MILLISECONDS);
                    masterWaiter.reset();

                    if (gotMaster) {

                        /*
                         * Check the failed txns from the last round. All
                         * transactions that did not successfully commit should
                         * be aborted.
                         */
                        System.err.println(TEST + " Round " + 
                                           totalMasterStints.get() +
                                           nodeName + " became master");
                        cleanupFailedTxns(failedTxns);
                        startInsertsAndTransfer(failedTxns);
                    } else {
                        LoggerUtils.logMsg(logger, formatter, Level.INFO,
                                           "test is out of time");
                    }
                } finally {
                    if (System.currentTimeMillis() > endTime) {
                        LoggerUtils.logMsg(logger, formatter, Level.INFO,
                                           "test is out of time");
                        break;
                    }
                }
            }
            cleanupFailedTxns(failedTxns);

        } catch (Throwable e) {
            markTestAsFailed("ended with " + e, e);
        } finally {
            try {
                /*
                 * Make sure all nodes/inserters are quiescent before we
                 * shutdown, so that we don't get InsufficientReplicaExceptions
                 */
                shutdownBarrier.await();
            } catch (InterruptedException e) {
                markTestAsFailed("shutdown barrier interrupted", e);
            } catch (BrokenBarrierException e) {
                markTestAsFailed("shutdown barrier broken", e);
            } finally {
                waitForInsertEnd.countDown();
            }
        }
    }

    /*
     * Check that this node contains all (and only) the committed data. Do
     * this in a second pass where we re-open the environment, so that
     * there's no issue with leftover master transfers.
     */
    void verify() {
        System.out.println(TEST + nodeName + " starting verify");

        /* 
         * Sync up the persistent storage of the group by doing one write with
         * SYNC/commitAll
         */
        if (repEnv.getState().equals(State.MASTER)) {
            
            System.err.println("Syncing up group before verify");
            Durability all = new Durability(SyncPolicy.SYNC,
                                            SyncPolicy.SYNC,
                                            ReplicaAckPolicy.ALL);
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setDurability(all);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setReadOnly(false);
            dbConfig.setAllowCreate(true);
            Transaction txn = repEnv.beginTransaction(null, txnConfig);
            System.err.println("Syncing up group before verify using txn " +
                               txn.getId());
            Database newDb = repEnv.openDatabase(txn, "VerifyMarker", dbConfig);
            newDb.close();
            txn.commit();
        }

        Set<TestRecord> foundKeys = null;
        try {
            /* Read all the records in this node */
            foundKeys = dataGenerator.readTestRecords(nodeName, repEnv, db);
        } finally {
            
            /* 
             * If you are node 1, reconcile the problematic commits using your
             * database. Then all the other nodes can check against that 
             * record.
             */
            if (nodeName.equals("node1")) {
                dataGenerator.reconcileProblematicCommits(nodeName, repEnv, db);
            }

            if (db != null) {
                db.close();
                db = null;
            }

            /*
             * Don't close the environment before everyone is done reading
             */
            try {
                verifyBarrier.await();
            } catch (InterruptedException e) {
                markTestAsFailed("verify barrier interrupted", e);
            } catch (BrokenBarrierException e) {
                markTestAsFailed("verify barrier broken", e);
            }

            if (repEnv != null) {
                repEnv.close();
                repEnv = null;
            }
        }

        /* 
         * Use the committedRecords map now that it's been updated appropriately
         * with the problematic commits.
         */
        Set<TestRecord> expectedKeys =
            new HashSet<TestRecord>(dataGenerator.getCommittedRecords());
        if (expectedKeys.size() != foundKeys.size()) {
            testSuccess.set(false);
            System.err.println(TEST + TEST_FAIL + " " + nodeName + ": " +
                               " expected " + expectedKeys.size() +
                               " records but found " + foundKeys.size());
        }

        expectedKeys.removeAll(foundKeys);
        if (expectedKeys.size() != 0) {
            testSuccess.set(false);
            System.err.println(TEST + TEST_FAIL + " " + nodeName + ": " +
                               " missing records: " +
                               expectedKeys);
        }

        foundKeys.removeAll(dataGenerator.getCommittedRecords());
        if (foundKeys.size() != 0) {
            testSuccess.set(false);
            System.err.println(TEST + TEST_FAIL + " " + nodeName + ": " +
                               " found unexpected records: " +
                               foundKeys);
        }
        waitForVerify.countDown();
        System.out.println(TEST + nodeName + " ending verify");
    }

    private void cleanupFailedTxns(Map<Transaction, AbortProblem> failedTxns) {
        for (Map.Entry<Transaction, AbortProblem> entry :
                 failedTxns.entrySet()) {
            Transaction t = entry.getKey();
            if (t.getState().equals(Transaction.State.ABORTED)) {
                continue;
            }

            if (Boolean.getBoolean("verbose")) {
                LoggerUtils.logMsg(logger, formatter, Level.INFO,
                                   "Cleaning up txn " + t.getId() + "/" +
                                   t.getState() + " after " +
                                   entry.getValue());
            }

            try {
                t.abort();
            } catch (UnknownMasterException e) {
                /* Okay to get this exception. */
                System.err.println(TEST + nodeName +
                                   " OK: Got UnknownMasterEx when aborting txn"
                                   + t.getId());
            } catch (ReplicaWriteException e) {
                /* Okay to get this exception. */
                System.err.println(TEST + nodeName +
                                   " OK: Got ReplicaWriteEx when aborting " +
                                   "txn " + t.getId());
            } catch (Throwable e) {
                markTestAsFailed("Problem when cleaning up " + t.getId() +
                                 "/" + t.getState() + " after " +
                                 entry.getValue(), e);
            }
        }
        failedTxns.clear();
    }

    /**
     * Set up environment handle.
     */
    private void setupEnvironment(boolean openForReads) {

        File envHome = new File(testConfig.baseEnvDir, nodeName);
        envHome.mkdir();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        if (testConfig.testType.equals(GEN_DB_OPS)) {
            /* replaying database operations can be expensive */
            envConfig.setTxnTimeout(1, TimeUnit.MINUTES);
        }

        ReplicationConfig repConfig = new ReplicationConfig();
        repConfig.setNodeHostPort(testConfig.hostName +
                                  ":" + testConfig.startPort + nodeId);
        repConfig.setHelperHosts(testConfig.hostName +
                                  ":" + testConfig.startPort + 1);

        repConfig.setGroupName(REP_GROUP_NAME);
        repConfig.setNodeName(nodeName);

        System.out.println(TEST + nodeName + " opening environment");
        if (openForReads) {
            repConfig.setConfigParam
              (ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "1 ms");
        }

        repEnv = new ReplicatedEnvironment(envHome, repConfig, envConfig);
        logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        formatter = new ReplicationFormatter
            (new NameIdPair("Tester_" + nodeName, 0));

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setReadOnly(false);
        dbConfig.setAllowCreate(true);
        db = repEnv.openDatabase(null, DB_NAME, dbConfig);
    }

    private String insertPrefix(final int insertIndex) {
        return "[task " + insertIndex + "]";
    }

    /**
     * Insert records until we run into an exception.
     * @param failedTxns are returned to the application so it can attempt
     * to clean them up.
     */
    private void insertRecords(final int inserterIndex,
                               final CountDownLatch readyForMasterTransfer,
                               Map<Transaction, AbortProblem> failedTxns) {

        Transaction txn = null;
        int i = 0;

        Exception endException = null;
        try {
            while (true) {
                i++;
                txn = repEnv.beginTransaction(null, null);
                Set<TestRecord> results = null;
                try {
                    results = dataGenerator.doOneTransaction
                        (this, repEnv, logger, formatter, txn, db);

                    /*
                     * The master transfer is waiting for the inserts jobs
                     * to get started. We want a few committed transactions and
                     * hopefuly some inflight transactions.
                     */
                    if (i == 10) {
                        readyForMasterTransfer.countDown();
                    }

                    txn.commit();
                    dataGenerator.updateCommittedRecords(results);
                } catch (IllegalStateException e) {
                    /*
                     * It's expected that the transaction may be aborted or
                     * closed, probably by the repNode when it transitioning to
                     * replica state. In this case, it's ok to get an ISE,
                     * because the txn has been abruptly shut down without
                     * the application's knowledge.This is the same thing as
                     * would happen if the user calls txn.commit() twice in a
                     * row. However, if the txn is still open, something is
                     * wrong.
                     */
                    if (txn.getState().equals(Transaction.State.OPEN)) {
                        throw e;
                    }

                    System.err.println(TEST + nodeName +
                                       insertPrefix(inserterIndex) +
                                       " warning: txn " + txn.getId() +
                                       " in state " + txn.getState() +
                                       " with actions " + results +
                                       " got illegal state exception" + e);
                    dataGenerator.addProblematicCommits(results);
                } catch (InsufficientAcksException e) {

                    /*
                     * Make note of this. May happen, particularly with the
                     * database ops, because they have more contention. Repeat
                     * the transaction until we get a UnknownMaster or
                     * ReplicaWriteException.
                     */
                    System.err.println(TEST + nodeName +
                                       insertPrefix(inserterIndex) +
                                       " warning: txn " + txn.getId() +
                                       " in state " + txn.getState() +
                                       " with actions " + results +
                                       " got insufficient acks: " + e);
                    
                    dataGenerator.addProblematicCommits(results);
                }


                /* Print this only -if -Dverbose is on */
                if (Boolean.getBoolean("verbose")) {
                    if ((i % 10) == 0) {
                        LoggerUtils.logMsg(logger, formatter, Level.CONFIG,
                                           insertPrefix(inserterIndex) +
                                           " running txn " + i);
                    }
                }
            }
        } catch (ReplicaWriteException e) {
            /* issue a briefer message */
            LoggerUtils.logMsg(logger, formatter, Level.CONFIG,
                               insertPrefix(inserterIndex) + " " + i +
                               " inserts ending due to master->rep " +
                               "transition");
            endException = e;

        } catch (UnknownMasterException e) {
            /* issue a briefer message */
            LoggerUtils.logMsg(logger, formatter, Level.CONFIG,
                               insertPrefix(inserterIndex) + " " + i +
                               "th insert ending due to unknown master");

            endException = e;
        } catch (Throwable e) {
            markTestAsFailed(insertPrefix(inserterIndex) +
                             " inserts saw unexpected exception", e);
        } finally {
            LoggerUtils.logMsg(logger, formatter, Level.CONFIG,
                               insertPrefix(inserterIndex) +
                               " Executed " + i + " txns");
            abortTxn(txn, inserterIndex, endException, failedTxns);
            dataGenerator.lastMasterInsertersDone.countDown();
        }
    }

    /**
     * Do the abort for a transaction that has incurred an exception during
     * an insert.
     */
    private void abortTxn(Transaction txn,
                          int inserterIndex,
                          Exception reasonForAbort,
                          Map<Transaction, AbortProblem> failedTxns) {
        try {
            if (txn != null) {
                txn.abort();
            }
        } catch (Exception e) {
            if (txn != null) {
                failedTxns.put(txn, new AbortProblem(reasonForAbort, e));
                displayTxnFailure(inserterIndex, failedTxns, txn);
            }
        }
    }

    private String displayTxnFailure(int inserterIndex,
                                     Map<Transaction, AbortProblem> failedTxns,
                                     Transaction txn) {
        AbortProblem problem = failedTxns.get(txn);
        return (insertPrefix(inserterIndex) +
                           "txn " + txn.getId() + "/" + txn.getState() +
                           " aborted because of " +
                           problem.getOriginalProblem() +
                           " and had problem aborting " +
                           problem.getAbortProblem());
    }

    private void reportStints(int minStints) {
        int numStints =  numMasterStints.get();
        System.out.println(TEST + nodeName + " was master " +
                           numStints + " times");
        if (minStints > numStints) {
            testSuccess.set(false);
            System.err.println(TEST + TEST_FAIL + ":" + nodeName +
                               " had " + numStints +
                               " turns as master, but should have a " +
                               "minimum of " + minStints);
        }
    }

    /*
     *  Request a master transfer
     */
    private void requestTransfer(final CountDownLatch readyForMasterTransfer)
        throws InterruptedException {

        /* Wait until data operations are in flight. */
        readyForMasterTransfer.await();

        Set<String> replicas = new HashSet<String>();
        for (ReplicationNode rn : repEnv.getGroup().getNodes()) {
            if (rn.getName().equals(nodeName)) {
                continue;
            }
            replicas.add(rn.getName());
        }

        if (replicas.size() == 0) {
            return;
        }

        LoggerUtils.logMsg(logger, formatter, Level.INFO,
                           "requesting transfer");
        try {
            String newMasterName =
                repEnv.transferMaster(replicas, TRANSFER_TIME, TRANSFER_UNIT);
            System.err.println(TEST + nodeName + " finishing transfer to " +
                               newMasterName);
        } catch (Throwable e) {
            markTestAsFailed("Transfer failed", e);
        }
    }

    /**
     * Begin the work of a master.
     * @throws InterruptedException
     */
    private void startInsertsAndTransfer
        (final Map<Transaction, AbortProblem> failedTxns)
        throws InterruptedException {

        ExecutorService execService = Executors.newCachedThreadPool();

        /*
         * readyForMasterTransfer lets the thread that will start the master
         * transfer wait until data operations are in flight before initiating
         * the transfer, to better test concurrent txns.
         */
        final CountDownLatch readyForMasterTransfer = new CountDownLatch(1);
        numMasterStints.incrementAndGet();
        totalMasterStints.incrementAndGet();
        dataGenerator.newMasterReset(nodeName, repEnv, db);

        /*
         * Start multiple jobs to do inserts. These mimic the application
         * threads.
         */
        for (int i = 0; i < testConfig.numAppThreads; i++) {
            final int index = taskNumber.incrementAndGet();

            Callable<Void> doInserts = new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    insertRecords(index, readyForMasterTransfer, failedTxns);
                    return null;
                }
            };
            execService.submit(doInserts);
        }

        /*  Start another job to do a master transfer */
        Callable<Void> doTransfer = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                requestTransfer(readyForMasterTransfer);
                return null;
            }
        };
        execService.submit(doTransfer);

        /* Stop taking any more tasks, and wait for them to finish */
        execService.shutdown();
        execService.awaitTermination(10, TimeUnit.MINUTES);
        LoggerUtils.logMsg(logger, formatter, Level.FINE,
                           "succeeded waiting for inserts and transfers " +
                           "to finish");
    }

    /**
     * The test failed, exit.
     */
    private void markTestAsFailed(String message, Throwable e) {
        testSuccess.set(false);
        System.err.println(TEST + TEST_FAIL + " " + nodeName +
                           " "  + message);
        e.printStackTrace();
        System.exit(1);
    }

    /**
     * A state change listener that can be used to repeatedly wait for
     * master status. This is preferable to using a new state listener each
     * time we want to listen for a new transition to master status, because
     * ReplicatedEnvironment.setStateListener will fire off the state change
     * notification if the node is already master at the point when the
     * listener is set, which complicates the handling of the test loop.
     */
    private class ResettableMasterListener implements StateChangeListener {

        private CountDownLatch waitForMaster = new CountDownLatch(1);

        @Override
            public void stateChange(StateChangeEvent stateChangeEvent) {

            if (stateChangeEvent.getState().equals
                (ReplicatedEnvironment.State.MASTER)) {
                waitForMaster.countDown();
            }

            if (stateChangeEvent.getState().isDetached()) {
                waitForMaster.countDown();
            }
        }

        boolean awaitMastership(long time, TimeUnit timeUnit)
            throws InterruptedException {
            return waitForMaster.await(time, timeUnit);
        }

        void reset() {
            waitForMaster = new CountDownLatch(1);
        }
    }

    /* Struct to hold info about abort attempts. */
    private class AbortProblem {
        /* The exception that made it necessary to abort this txn */
        private final Exception originalProblem;

        /* The exception experienced when attempting to abort this txn */
        private final Exception abortProblem;

        AbortProblem(Exception original, Exception abortProblem) {
            this.originalProblem = original;
            this.abortProblem = abortProblem;
        }

        @Override
        public String toString() {
            return "original exception=" + originalProblem +
                " abort exception=" + abortProblem;
        }

        Exception getOriginalProblem() {
            return abortProblem;
        }

        Exception getAbortProblem() {
            return abortProblem;
        }
    }

    /* Parse test parameters, hold test configuration */
    private static class TestConfiguration {

        /* Each JE env is created under this directory */
        File baseEnvDir;

        /* How long the test runs. */
        int testMinutes = 10;

        /* How many concurrent threads limit the application load */
        int numAppThreads = 20;

        String hostName = "localhost";
        int startPort = 5000;
        int groupSize = 3;

        /* number of records in each transaction */
        int recordsInTxn = 4;

        String testType = GEN_KEY_DATA;

        TestConfiguration(String[] argv) {
            parseParams(argv);
        }

        /**
         * Parse the command line parameters for a replication node and set up
         * any configuration parameters.
         */
        void parseParams(String[] argv)
            throws IllegalArgumentException {

            int argc = 0;
            int nArgs = argv.length;

            if (nArgs == 0) {
                usage("-h, -hostName, -startPort and -groupSize " +
                      "are required arguments.");
            }

            while (argc < nArgs) {
                String thisArg = argv[argc++];

                if (thisArg.equals("-h")) {
                    if (argc < nArgs) {
                        baseEnvDir = new File(argv[argc++]);
                    } else {
                        usage("-h requires an argument");
                    }
                }  else if (thisArg.equals("-hostName")) {
                    /* The node hostname, port pair. */
                    if (argc < nArgs) {
                        hostName = argv[argc++];
                    } else {
                        usage("-hostName requires an argument");
                    }
                } else if (thisArg.equals("-groupSize")) {
                    if (argc < nArgs) {
                        groupSize = Integer.parseInt(argv[argc++]);
                    } else {
                        usage("-groupSize requires an argument");
                    }
                } else if (thisArg.equals("-startPort")) {
                    if (argc < nArgs) {
                        startPort = Integer.parseInt(argv[argc++]);
                    } else {
                        usage("-startPort requires an argument");
                    }
                } else if (thisArg.equals("-testMinutes")) {
                    if (argc < nArgs) {
                        testMinutes = Integer.parseInt(argv[argc++]);
                    } else {
                        usage("-testMinutes requires an argument");
                    }
                } else if (thisArg.equals("-numAppThreads")) {
                    if (argc < nArgs) {
                        numAppThreads = Integer.parseInt(argv[argc++]);
                    } else {
                        usage("-numAppThreads requires an argument");
                    }
                } else if (thisArg.equals("-recordsInTxn")) {
                    if (argc < nArgs) {
                        recordsInTxn = Integer.parseInt(argv[argc++]);
                    } else {
                        usage("-recordsInTxn requires an argument");
                    }
                } else if (thisArg.equals("-testType")) {
                    if (argc < nArgs) {
                        testType = argv[argc++];
                        if ((!testType.equals(GEN_KEY_DATA)) &&
                            (!testType.equals(GEN_DB_OPS))) {
                            usage("-testType must be " + GEN_KEY_DATA +
                                  " or " + GEN_DB_OPS);
                        }
                    } else {
                        usage("-testType must be " + GEN_KEY_DATA +
                              " or " + GEN_DB_OPS);
                    }
                } else {
                    usage("Unknown argument; " + thisArg);
                }
            }

            if (baseEnvDir == null) {
                usage("-h is a required parameter");
            }

            if (hostName == null) {
                usage("-hostName is a required parameter");
            }

            if (startPort == 0) {
                usage("-startPort is a required parameter");
            }

            if (groupSize < 2) {
                usage("-groupSize is a required parameter and must be >= 2");
            }
        }

        private void usage(String message) {
            System.out.println();
            System.out.println(message);
            System.out.println();
            System.out.print("usage: " + getClass().getName());

            System.out.println
                ("-h <environment dir> " +
                 "-hostName <hostName> " +
                 "-startPort<port> " +
                 "-groupSize <num> " +
                 "-testMinutes <num>" +
                 "-numAppThreads <num>" +
                 "-recordsInTxn <num>" +
                 "-testType <KeyData|DbOps>");

            System.out.println
                ("\t -h: the  base directory that will house subdirectories " +
                 "for each replicated environment\n" +
                 "\t -hostName: hostname for the test machine\n" +
                 "\t -startPort: starting port number for the replication "+
                 "group. The test will consume <groupSize> ports starting " +
                 "with that port number\n" +
                 "\t -groupSize the number of nodes in the group\n" +
                 "\t -testMinutes the number of minutes that the test should " +
                 "\t -recordsInTxn number of operation in each txn\n" +
                 "\t -numAppThreads number of concurrently inserting threads" +
                 "\t -testType <KeyData|DbOps>");
            System.exit(-1);
        }

        String showParams() {
            return "Test parameters are:\n" +
                "envDir = " + baseEnvDir +
                "\nhostName = " + hostName +
                "\nstartPort = " + startPort +
                "\ngroupSize = " + groupSize +
                "\ntestMinutes = " + testMinutes +
                "\nnumAppThreads = " + numAppThreads +
                "\nrecordsInTxn = " + recordsInTxn +
                "\ntestType = " + testType;
        }
    }

    /**
     * A class used by any node that is currently master to generate some
     * application workload, and to save the set of valid, committed data
     * records that should be present in the store. Work is done by multiple
     * concurrent application threads.
     */
    abstract private static class DataGenerator {
        protected final static String DBPREFIX = "testrecord";
        private final static String FIRST_NAME = "first";
        private final static String SECOND_NAME = "second";

        AtomicInteger reusedKeys;

        /* 
         * Ensures that the inserters from one stint have finished updating
         * the expected results set before the next stint starts.
         */
        public CountDownLatch lastMasterInsertersDone;

        /*
         * The set of records that should be in environment, after all the
         * master transfers, at the end of the test. Use a map of keys,
         * because records are updated, and we want the latest version of
         * the record.
         */
        protected final Map<Integer, TestRecord> committedRecords;

        /* 
         * These records received an insufficient ack exception, so it's
         * unknown whether the change was propagated. Using a Map instead
         * of a Set so as to use ConcurrentHashMap.
         */
        protected final Map<Integer, TestRecord> problematicCommits;

        DataGenerator() {
            committedRecords = new ConcurrentHashMap<Integer, TestRecord>();
            problematicCommits = new ConcurrentHashMap<Integer, TestRecord>();
        }

        public void addProblematicCommits(Set<TestRecord> results) {
            for (TestRecord r : results) {
                problematicCommits.put(r.getKey(), r);
            }
        }

        abstract void deleteRecord(MasterTransferExercise tester,
                                   Transaction txn,
                                   int key,
                                   Environment repEnv,
                                   Database db,
                                   String secondVal);

        abstract void updateRecord(Transaction txn,
                                   int key,
                                   Environment repEnv,
                                   Database db,
                                   String firstVal,
                                   String secondVal);

        abstract void insertRecord(Transaction txn,
                                   int key,
                                   Environment repEnv,
                                   Database db,
                                   String firstVal);

        /**
         * @param committed these records were committed, so update the
         * map used for test verification accordingly.
         */
        public void updateCommittedRecords(final Set<TestRecord> committed) {

            for (TestRecord r: committed) {
                TestRecord prev = null;
                if (r.getData() == null) {
                    prev = committedRecords.remove(r.getKey());
                    if (prev == null) {
                        throw new IllegalStateException
                            ("Should be able to delete " + r +
                             " from committedRecords map");
                    }
                } else {
                    prev = committedRecords.put(r.getKey(), r);
                }
                // System.err.println("committed: " + r + " prev=" + prev);
            }
        }

        /*
         * Return the set of keys that should be in environment, after all the
         * master transfers, as the end of the test.
         */
        public synchronized Collection<TestRecord> getCommittedRecords() {
            return committedRecords.values();
        }

        /*
         * Each new master will do both updates to the same set of keys and
         * inserts of new records, because updates will exercise the
         * rollbacks better.
         */
        public synchronized void newMasterReset(String nodeName,
                                                Environment repEnv,
                                                Database db) 
            throws InterruptedException {
            
            if (lastMasterInsertersDone != null) {
                lastMasterInsertersDone.await();
            }
            lastMasterInsertersDone = 
                new CountDownLatch(testConfig.numAppThreads);
            reusedKeys = new AtomicInteger(0);
            reconcileProblematicCommits(nodeName, repEnv, db);
        }

        public void reconcileProblematicCommits(String nodeName,
                                                Environment repEnv,
                                                Database db) {

            /* 
             * These records got an insufficient ack exception from the last
             * master, and therefore weren't added to the verify set See if the
             * action really happened, and if verify set needs to be updated.
             */
            if (problematicCommits.size() > 0) {
                System.err.println(TEST + " " + nodeName + 
                                   " sees " + problematicCommits.size() +
                                   " problematic commits");

                if (Boolean.getBoolean("verbose")) {
                    System.err.println(TEST + " " + nodeName + " " +
                                       problematicCommits.values());
                }

                setupVerifyCheck(repEnv);
                for (TestRecord r: problematicCommits.values()) {
                    String val = findRecord(r.getKey(), repEnv, db);
                    if (val == null) {
                        /* The delete really happened, update the verify set */
                        if (r.getData() == null) {
                            committedRecords.remove(r.getKey());
                            if (Boolean.getBoolean("verbose")) {
                                System.err.println
                                    ("correct records, delete got " +
                                     " insufficientAcks but was" +
                                     " propagated:" + r);
                            }
                        }
                    } else if (val.equals(r.getData())) {
                        /* The insert or update really happened*/
                        committedRecords.put(r.getKey(), r);
                        if (Boolean.getBoolean("verbose")) {
                            System.err.println
                                ("correct the records, Insert/update got " +
                                 " insufficient acks, but was  propagated:" +
                                 r);
                        }
                    } 
                }
                problematicCommits.clear();
            }
        }

        abstract void setupVerifyCheck(Environment repEnv);

        abstract String findRecord(int key, Environment repEnv, Database db);

        /*
         * Do one transaction's worth of work, and return the test records
         * contained in that transaction.
         */
        public Set<TestRecord> doOneTransaction(MasterTransferExercise tester,
                                                ReplicatedEnvironment repEnv,
                                                Logger logger,
                                                Formatter formatter,
                                                Transaction txn,
                                                Database db) {
            Set<TestRecord> actions = new HashSet<TestRecord>();

            for (int i = 0; i < testConfig.recordsInTxn; i++) {
                int key = reusedKeys.incrementAndGet();
                TestRecord existing = committedRecords.get(key);

                if (existing == null) {
                    String firstVal = makeFirstVal(key, txn.getId());
                    LoggerUtils.logMsg(logger, formatter, Level.FINE,
                                       "Txn " + txn.getId() +
                                       " inserting " + firstVal);
                    insertRecord(txn, key, repEnv, db, firstVal);
                    actions.add(new TestRecord(key, firstVal));
                } else {
                    String existingVal = existing.getData();
                    if (isFirstVal(existingVal)) {
                        String secondVal = makeSecondVal(key, txn.getId());
                        LoggerUtils.logMsg(logger, formatter, Level.FINE,
                                           "Txn " + txn.getId() +
                                           " Updating " + existingVal +
                                           " to " + secondVal);
                        updateRecord(txn, key, repEnv, db, existingVal,
                                     secondVal);
                        actions.add(new TestRecord(key, secondVal));
                    } else if (isSecondVal(existingVal)) {
                        LoggerUtils.logMsg(logger, formatter, Level.FINE,
                                           "Txn " + txn.getId() +
                                           " Deleting " + existingVal +
                                           " using key " + key);
                        deleteRecord(tester, txn, key, repEnv, db, existingVal);
                        actions.add(new TestRecord(key, null));
                    } else {
                        throw new IllegalStateException
                            ("Found unexpected record in committed test set " +
                             "when doing dbop testing: " + existing);
                    }
                }
            }
            return actions;
        }

        private String makeFirstVal(int val, long txnId) {
            return DBPREFIX + "_" + FIRST_NAME + "_" + val + "_t" + txnId;
        }

        private String makeSecondVal(int val, long txnId) {
            return DBPREFIX + "_" + SECOND_NAME + "_" + val + "_t" + txnId;
        }

        protected int getKeyFromName(String name) {
            String[] split = name.split("_");
            if (split.length == 4) {
                return Integer.parseInt(split[2]);
            }

            throw new IllegalStateException
                ("Unexpected database name in test: " +  name);
        }

        private boolean isFirstVal(String name) {
            String[] split = name.split("_");
            if (split.length == 4) {
                return split[1].equals(FIRST_NAME);
            }
            return false;
        }

        private boolean isSecondVal(String name) {
            String[] split = name.split("_");
            if (split.length == 4) {
                return split[1].equals(SECOND_NAME);
            }
            return false;
        }

        /**
         * @return the set of test records that are currently in the
         * environment.
         */
        abstract public Set<TestRecord>
            readTestRecords(String nodeName,
                            ReplicatedEnvironment repEnv,
                            Database db);

    }

    /**
     * The workload consists of creating, renaming and deleting databases.
     */
    private static class DbOpsGenerator extends DataGenerator {
        DatabaseConfig dbConfig;
        private List<String> existingDbs;

        DbOpsGenerator() {
            dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setReadOnly(false);
            dbConfig.setAllowCreate(true);
        }

        @Override
            void insertRecord(Transaction txn,
                              int key,
                              Environment repEnv,
                              Database db,
                              String firstVal) {
            Database newDb = repEnv.openDatabase(txn, firstVal, dbConfig);
            newDb.close();
        }

        @Override
            void updateRecord(Transaction txn,
                              int key,
                              Environment repEnv,
                              Database db,
                              String firstVal,
                              String secondVal) {
            repEnv.renameDatabase(txn, firstVal, secondVal);
        }

        @Override
            void deleteRecord(MasterTransferExercise tester,
                              Transaction txn,
                              int key,
                              Environment repEnv,
                              Database db,
                              String secondVal) {
            repEnv.removeDatabase(txn, secondVal);
        }

        @Override
        public Set<TestRecord> readTestRecords(String nodeName,
                                                   ReplicatedEnvironment repEnv,
                                                   Database db) {
            Set<TestRecord> foundDBs = new HashSet<TestRecord>();
            List<String> dbNames = repEnv.getDatabaseNames();
            for (String name : dbNames) {
                if (isTestDb(name)) {
                    foundDBs.add(new TestRecord(getKeyFromName(name),
                                                name));
                }
            }
            return foundDBs;
        }

        private boolean isTestDb(String name) {
            return name.startsWith(DBPREFIX);
        }

        /* Return the database name blah blah */
        @Override
        String findRecord(int key, Environment repEnv, Database db) {
            for (String name : existingDbs) {
                if (!isTestDb(name)) {
                    continue;
                }

                if (getKeyFromName(name) == key) {
                    return name;
                }
            }
            return null;
        }

        @Override
        void setupVerifyCheck(Environment repEnv) {
            existingDbs = repEnv.getDatabaseNames();
        }
    }

    /**
     * The workload consists of inserting and updating records in a single db.
     * On each round of inserts, the node should both:
     * -insert into the same set of keys, so as to cause a lot of updates
     * -insert into a random set of new keys
     * -coordinate multiple threads do a lot of inserts concurrently without
     *  blocking on the same key values, so there's more application activity
     *  while the master transfer is going on

     */
    private static class DataAndKeyGenerator extends DataGenerator {

        @Override
        void insertRecord(Transaction txn,
                          int key,
                          Environment repEnv,
                          Database db,
                          String firstVal) {
                DatabaseEntry keyEntry = new DatabaseEntry();
                IntegerBinding.intToEntry(key, keyEntry);
                DatabaseEntry dataEntry =
                    new DatabaseEntry(firstVal.getBytes());
                db.put(txn, keyEntry, dataEntry);
        }

        @Override
         void updateRecord(Transaction txn,
                           int key,
                           Environment repEnv,
                           Database db,
                           String firstVal,
                           String secondVal) {
            insertRecord(txn, key, repEnv, db, secondVal);
        }

        @Override
         void deleteRecord(MasterTransferExercise tester,
                           Transaction txn,
                           int key,
                           Environment repEnv,
                           Database db,
                           String secondVal) {
            DatabaseEntry keyEntry = new DatabaseEntry();
            IntegerBinding.intToEntry(key, keyEntry);
            OperationStatus status = db.delete(txn, keyEntry);
            if (!(status.equals(OperationStatus.SUCCESS))) {
                tester.markTestAsFailed("Delete of " + key + " got status " +
                                 status, null);
            }
        }

        /**
         * Assumes environment is open, and turns the data in the
         * environment into a set of test records for verification.
         */
        @Override
        public Set<TestRecord> readTestRecords(String nodeName,
                                               ReplicatedEnvironment repEnv,
                                               Database db) {
            Set<TestRecord> existing = new HashSet<TestRecord>();
            Transaction txn = repEnv.beginTransaction(null, null);
            Cursor cursor = db.openCursor(txn, CursorConfig.READ_COMMITTED);
            int i = 0;
            try {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                while (cursor.getNext(key, value, null) ==
                       OperationStatus.SUCCESS) {
                    int keyVal = IntegerBinding.entryToInt(key);
                    existing.add(new TestRecord(keyVal,
                                                new String(value.getData())));
                    if ((i++ % 1000) == 0) {
                        System.out.println(TEST + nodeName +
                                           " Scan at record " + i);
                    }
                }
            } finally {
                cursor.close();
                txn.commit();
            }
            return existing;
        }

        @Override
        String findRecord(int key, Environment repEnv, Database db) {
            DatabaseEntry keyEntry = new DatabaseEntry();
            IntegerBinding.intToEntry(key, keyEntry);
            DatabaseEntry dataEntry = new DatabaseEntry();
            OperationStatus status = db.get(null, keyEntry, dataEntry, null);
            if (status.equals(OperationStatus.SUCCESS)) {
                return new String(dataEntry.getData());
            }
            return null;
        }

        @Override
        void setupVerifyCheck(Environment repEnv) {
            /* nothing to do */
        }
    }

    /* Struct to hold the key and data */
    private static class TestRecord {
        private final Integer key;
        private final String data;

        TestRecord(int keyVal, String dataVal) {
            this.key = keyVal;
            this.data = dataVal;
        }

        @Override
            public String toString() {
            return key + " / " + data;
        }

        @Override
            public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((data == null) ? 0 :
                                       data.hashCode());
            result = prime * result + ((key == null) ? 0 :
                                       key.hashCode());
            return result;
        }

        @Override
            public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TestRecord other = (TestRecord) obj;
            if (data == null) {
                if (other.getData() != null)
                    return false;
            } else if (!data.equals(other.getData()))
                return false;
            if (key == null) {
                if (other.getKey() != null)
                    return false;
            } else if (!key.equals(other.getKey()))
                return false;
            return true;
        }

        public Integer getKey() {
            return key;
        }

        public String getData() {
            return data;
        }
    }
}
