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

package com.sleepycat.je.rep.txn;

import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;

/*
 * Rollback to a matchpoint, used for HA.
 *
 * The test follows a common pattern dictated by runWorkload. Different tests
 * use workloads that vary whether duplicates and custom comparators are used.
 *
 * Rollback functionality is only available on a ReplayTxn. To make testing
 * simpler, a wrapper Transaction class is used, for ease of setting up test
 * scenarios. The TestWrapperTransaction lets us artificially uses a ReplayTxn
 * for api operations, so we don't need to run two nodes to create the database
 * needed.
 */
public class RollbackToMatchpointTest extends RepTestBase {
    private long TEST_TXN_ID = -1000;
    private final File envHome;
    private Environment env;
    private Database dbA;
    private Database dbB;
    private final boolean verbose = Boolean.getBoolean("verbose");
    private Logger logger;

    public RollbackToMatchpointTest() {
        envHome = SharedTestUtils.getTestDir();
        groupSize = 1;
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");
        super.setUp();
    }

    /**
     * Test transaction locking and releasing.
     */
    @Test
    public void testBasicRollback()
        throws Exception {

        Workload w = new BasicWorkload();
        runWorkload(w);
    }

    @Test
    public void testCustomBtreeComparator()
        throws Exception {

        Workload w = new CustomBtreeComparatorWorkload();
        runWorkload(w);
    }

    @Test
    public void testDups()
        throws Exception {

        Workload w = new DupWorkload();
        runWorkload(w);
    }

    // TODO: add testing for
    // database operations when that's implemented.
    // public void testDatabaseOperations
    // public void testDupCustomComparator

    private void runWorkload(Workload workload)
        throws Exception {
        if (verbose) {
            System.out.println("Workload = " + workload);
        }

        workload.openEnvironment();

        rollbackStepByStep(workload, false /* retransmit */);
        rollbackStepByStep(workload, true /* retransmit */);
        rollbackEntireTransaction(workload);
        doAbort(workload);

        workload.closeEnvironment();
    }

    /**
     * Create a transaction with the workload and roll it back to a given
     * point. Systematically repeat in order to roll back to all possible
     * points.
     * This test does some inserts and deletes to create an initial data
     * context. Then it enters a loop which executes a to-be-rolled back
     * transaction encompassing inserts, updates. The loop rolls back the
     * transaction mix systematically entry by entry in order to exercise a
     * full mix of rollbacks.
     *
     * @param retransmit if true, the workload re-executes the rolled back
     * entries, to simulate how a master would re-send the replication stream.
     */
    private void rollbackStepByStep(Workload workload, boolean retransmit)
        throws DatabaseException {

        int matchpointIndex = 0;
        int numTxnOperations = 0;

        do {
            workload.openStore();
            Set<TestData> committedData = workload.setupInitialData();
            if (verbose && false) {
                System.out.println("------- committed ----------");
                System.out.println(committedData);
            }

            TestWrapperTransaction wrapperTransaction = makeTransaction();

            /*
             * The workHistory is used to find points to roll back to.  The
             * loop iterates over all points in the work history in order to
             * fully test all combinatios.
             */
            List<TestData> workHistory = workload.doWork(wrapperTransaction);
            Set<TestData> beforeRollbackContents = dumpStore(workload);
            numTxnOperations = workHistory.size();

            /* Choose a point to roll back to. */
            long matchpointLsn = workHistory.get(matchpointIndex).loggedLsn;

            if (verbose) {
                System.out.println("\ndump store before rollback - " +
                                   beforeRollbackContents);

                System.out.println("Rollback transaction " +
                                   wrapperTransaction.getId() + " to step " +
                                   matchpointIndex + " " +
                                   DbLsn.getNoFormatString(matchpointLsn));
                System.out.println("history=" + workHistory);
            }

            /* Rollback to the specified matchpoint. */
            ReplayTxn replayTxn = wrapperTransaction.getReplayTxn();
            replayTxn.rollback(matchpointLsn);

            Collection<TestData> expected = expectedData(committedData,
                                                         workHistory,
                                                         matchpointIndex);
            checkContents(expected, workload, "step by step");
            assertEquals(matchpointLsn, replayTxn.getLastLsn());
            assertFalse(replayTxn.isClosed());

            ReplicationContext endContext =
                new ReplicationContext(new VLSN(100));
            if (retransmit) {
                workload.simulateRetransmit(workHistory, wrapperTransaction,
                                            matchpointIndex);

                wrapperTransaction.commit(SyncPolicy.NO_SYNC, endContext, 1);
                checkContents(beforeRollbackContents, workload, "retransmit");
            } else {

                /*
                 * Call abort after the rollback, to make sure that a final
                 * abort, such as might happen after a master failover, will
                 * work.
                 */
                wrapperTransaction.abort(endContext, 1 /* masterId */);
                checkContents(committedData, workload, "abort");
            }

            workload.closeStore();
            workload.truncateDatabases();

            matchpointIndex++;
        } while (matchpointIndex < numTxnOperations);
    }

    private void checkContents(Collection<TestData> expected,
                               Workload workload,
                               String label)
        throws DatabaseException {

        /*
         * Do a dirty read to see what's there, and check against the
         * expected results.
         */
        Set<TestData> currentContents = dumpStore(workload);
        if (verbose){
            System.out.println(" ==> Check for " + label);
            System.out.println("current = " +  currentContents);
            System.out.println("expected = " +  expected);
        }

        assertEquals("expected=" + expected + " current=" +
                     currentContents,
                     expected.size(), currentContents.size());
        assertTrue("expected=" + expected + " current=" +
                   currentContents,
                   expected.containsAll(currentContents));
    }

    /*
     * For completeness, run the transaction and abort it without doing any
     * rollback.
     */
    private void doAbort(Workload workload)
        throws DatabaseException {

        workload.openStore();
        Set<TestData> committedData = workload.setupInitialData();
        TestWrapperTransaction wrapperTransaction = makeTransaction();
        workload.doWork(wrapperTransaction);
        wrapperTransaction.abort(new ReplicationContext(new VLSN(100)),
                                 1 /* masterId */);
        checkContents(committedData, workload, "doAbort");
        workload.closeStore();
        workload.truncateDatabases();
    }

    /**
     * Rollback to a matchpoint earlier than anything in the transaction.
     * Should be the equivalent of abort().
     */
    private void rollbackEntireTransaction(Workload workload)
        throws DatabaseException {

        workload.openStore();
        Set<TestData> committedData = workload.setupInitialData();

        TestWrapperTransaction wrapperTransaction = makeTransaction();
        workload.doWork(wrapperTransaction);
        ReplayTxn txn = (ReplayTxn) DbInternal.getTxn(wrapperTransaction);
        txn.rollback(txn.getFirstActiveLsn() - 1);

        /* Check the transaction chain. */
        assertEquals(DbLsn.NULL_LSN, txn.getLastLsn());

        /* Check the number of locks. */
        StatGroup stats = txn.collectStats();
        assertEquals(0, stats.getInt(LOCK_WRITE_LOCKS));

        checkContents(committedData, workload, "entire");

        /*
         * When the transaction is entirely rolled back, it's closed
         * and deregistered.
         */
        assertTrue(txn.isClosed());

        workload.closeStore();
    }

    /**
     * Dump the store using dirty reads.
     * @throws DatabaseException
     */
    private Set<TestData> dumpStore(Workload w)
        throws DatabaseException {

        Set<TestData> resultSet = new HashSet<TestData>();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Cursor cursor = dbA.openCursor(null, CursorConfig.READ_UNCOMMITTED);
        try {
            while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
                   OperationStatus.SUCCESS) {
                TestData result = w.unmarshall(dbA, key, data);
                resultSet.add(result);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        cursor = dbB.openCursor(null, CursorConfig.READ_UNCOMMITTED);
        try {
            while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
                   OperationStatus.SUCCESS) {
                TestData result = w.unmarshall(dbB, key, data);
                resultSet.add(result);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return resultSet;
    }

    /**
     * Calculate what the data set should be after the rollback.
     */
    private Collection<TestData> expectedData(Set<TestData> committedData,
                                              List<TestData> workHistory,
                                              int matchpointIndex) {

        /*
         * Use a map of id->TestData to calculate the expected set so we can
         * mimic updates.
         */
        Map<Integer, TestData> expected = new HashMap<Integer, TestData>();
        for (TestData t : committedData) {
            expected.put(t.getHashMapKey(), t);
        }

        for (TestData t : workHistory.subList(0, matchpointIndex + 1)) {
            if (t.isDeleted) {
                TestData removed = expected.remove(t.getHashMapKey());
                assert removed != null;
            } else {
                expected.put(t.getHashMapKey(), t);
            }
        }
        return expected.values();
    }

    TestWrapperTransaction makeTransaction()
        throws DatabaseException {

        ReplayTxn replayTxn =
            new ReplayTxn(DbInternal.getNonNullEnvImpl(env),
                          TransactionConfig.DEFAULT,
                          (TEST_TXN_ID--),
                          logger);

        return new TestWrapperTransaction(env, replayTxn);
    }

    /**
     * The test needs a Transaction to use for regular operations like put()
     * and get(). But the rollback functionality is only available for a
     * ReplayTxn. The TestWrapperTransaction wraps up a ReplayTxn within a
     * Transaction solely for test purposes. This way, we can set up rollback
     * situations in a single node, without replication, which is easier to
     * do. But we can also call ReplayTxn.rollback(), which is usually
     * only executed on a Replica.
     */
    class TestWrapperTransaction extends Transaction {

        private final ReplayTxn replayTxn;

        TestWrapperTransaction(Environment env, ReplayTxn replayTxn) {
            super(env, replayTxn);
            this.replayTxn = replayTxn;
        }

        public long commit(SyncPolicy syncPolicy,
                           ReplicationContext replayContext,
                           int masterNodeId)
            throws DatabaseException {

            return replayTxn.commit(syncPolicy, replayContext, masterNodeId,
                                    1 /* DTVLSN */);
        }

        public long abort(ReplicationContext replayContext,
                          int masterNodeId)
            throws DatabaseException {

            return replayTxn.abort(replayContext, masterNodeId,
                                   1 /* DTVLSN */);
        }

        ReplayTxn getReplayTxn() {
            return replayTxn;
        }
    }

    /**
     * Embodies the data set for each test case. Different data sets vary
     * whether the database uses duplicates or not, custom comparators, etc.
     */
    abstract class Workload {

        /**
         * Open the environment and database. Different workloads try
         * different database configurations.
         */
        abstract void openStore()
            throws DatabaseException;

        void closeStore() {
            try {
                if (dbA != null) {
                    dbA.close();
                    dbA = null;
                }

                if (dbB != null) {
                    dbB.close();
                    dbB = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Truncate the databases in order to reset the context for repeated
         * execution of the workload.
         */
        void truncateDatabases() {
            env.truncateDatabase(null, "testA", false /* returnCount */);
            env.truncateDatabase(null, "testB", false /* returnCount */);
        }

        void openEnvironment()
            throws DatabaseException {

            env = RepTestUtils.joinGroup(repEnvInfo);
        }

        void closeEnvironment() {
            try {
                if (env != null) {
                    env.close();
                    env = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        abstract Set<TestData> setupInitialData()
            throws DatabaseException;

        abstract List<TestData> doWork(Transaction trans)
            throws DatabaseException;

        /**
         * Read the log to find the lsns of this transactions's entries.  We
         * need those to find the candidate matchpoints.
         */
        void findLsns(Txn txn, List<TestData> workHistory)
            throws DatabaseException {

            long entryLsn = txn.getLastLsn();
            int lastIndex = workHistory.size() - 1;
            LogManager logManager =
                DbInternal.getNonNullEnvImpl(env).getLogManager();

            /* Troll through LN_TX entries and save their lsns. */
            while (entryLsn != DbLsn.NULL_LSN) {
                LNLogEntry<?> undoEntry = (LNLogEntry<?>)
                    logManager.getLogEntryHandleFileNotFound(entryLsn);

                /*
                 * Skip any DupCountLNs, they don't correspond to
                 * the application operations tracked in workHistory.
                 */
                if (!undoEntry.getLogType().equals
                    (LogEntryType.LOG_DUPCOUNTLN_TRANSACTIONAL)) {
                    workHistory.get(lastIndex).loggedLsn = entryLsn;
                    lastIndex--;
                }

                entryLsn = undoEntry.getUserTxn().getLastLsn();
            }
        }

        void putData(Database db, Transaction t, TestData testData)
            throws DatabaseException {

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            IntegerBinding.intToEntry(testData.id, key);
            IntegerBinding.intToEntry(testData.data, data);
            OperationStatus status = db.put(t, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        void deleteData(Database db, Transaction t, TestData testData)
            throws DatabaseException {

            DatabaseEntry key = new DatabaseEntry();
            IntegerBinding.intToEntry(testData.id, key);
            DatabaseEntry data = new DatabaseEntry();
            IntegerBinding.intToEntry(testData.data, data);
            Cursor c = db.openCursor(t, null);
            OperationStatus status = c.getSearchBoth(key, data, LockMode.RMW);
            assertEquals(OperationStatus.SUCCESS, status);
            status = c.delete();
            assertEquals(OperationStatus.SUCCESS, status);
            c.close();
        }

        /*
         * Well, if we could use the DPL, we wouldn't have to have
         * this method! Unmarshall the data into the expected return object.
         */
        abstract TestData unmarshall(Database db,
                                     DatabaseEntry key,
                                     DatabaseEntry data);

        void simulateRetransmit(List<TestData> workHistory,
                                Transaction transaction,
                                int matchpointIndex)
            throws DatabaseException {

            int startPoint = matchpointIndex + 1;
            for (int i = startPoint; i < workHistory.size(); i++) {
                TestData t = workHistory.get(i);
                if (t.isDeleted) {
                    deleteData(t.db, transaction, t);
                } else {
                    putData(t.db, transaction, t);
                }
            }
        }
    }

    /**
     * Exercise a non-dup, default comparator database.
     */
    class BasicWorkload extends Workload {
        @Override
        void openStore()
            throws DatabaseException {

            try {
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                dbA = env.openDatabase(null, "testA", dbConfig);
                dbB = env.openDatabase(null, "testB", dbConfig);
            } catch (DatabaseException e) {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        Set<TestData> setupInitialData()
            throws DatabaseException {

            /*
             * Make sure that there's a delete and an overwrite in the
             * initial data. These exercise BIN slot re-use scenarios.
             */
            putData(dbA, null, new TestData(dbA, 10, 1));
            putData(dbA, null, new TestData(dbA, 20, 1));
            putData(dbA, null, new TestData(dbA, 20, 2));
            putData(dbA, null, new TestData(dbA, 30, -1));

            putData(dbB, null, new TestData(dbB, 20, 1));

            /*
             * DeleteData uses a cursor in order to delete precisely this
             * record, and that needs a non-null transaction.
             */
            Transaction t = env.beginTransaction(null, null);
            deleteData(dbA, t, new TestData(dbA, 10, 1));
            t.commit();

            return dumpStore(this);
        }

        /**
         * If we used the DPL, we wouldn't need to have this unmarshall
         * method, because the store would automatically know what type of
         * object to return.
         */
        @Override
        TestData unmarshall(Database db,
                                DatabaseEntry key,
                                DatabaseEntry data) {

            return new TestData(db,
                                IntegerBinding.entryToInt(key),
                                IntegerBinding.entryToInt(data));
        }

        /**
         * Do a mix of inserts, updates, and deletes. For inserts instigated
         * by this transaction, make sure these cases are covered:
         * - first record for this key
         * - key exists, was previously deleted by another txn.
         * - key is overwritten.
         * @throws DatabaseException
         */
        @Override
        List<TestData> doWork(Transaction trans)
            throws DatabaseException {

            List<TestData> workHistory = new ArrayList<TestData>();

            /* Insert new record. */
            TestData t = new TestData(dbA, 30, 1, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Insert new record, reusing a slot from previous txn. */
            t = new TestData(dbA, 10, 2, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Update record created in this txn. */
            t = new TestData(dbA, 30, 2, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* delete a record created outside this txn. */
            t = new TestData(dbA, 20, 2, true);
            deleteData(dbA, trans, t);
            workHistory.add(t);

            /* delete a record created inside this txn. */
            t = new TestData(dbA, 30, 2, true);
            deleteData(dbA, trans, t);
            workHistory.add(t);

            /* Insert new record, reusing a slot from this txn. */
            t = new TestData(dbA, 30, 10, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Update record created in this txn. */
            t = new TestData(dbA, 30, 11, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Insert a new record into another database */
            t = new TestData(dbB, 30, 11, false);
            putData(dbB, trans, t);
            workHistory.add(t);

            findLsns(DbInternal.getTxn(trans), workHistory);
            return workHistory;
        }
    }

    /**
     * Exercise a non-dup, default comparator database.
     */
    class DupWorkload extends BasicWorkload {
        @Override
        void openStore()
            throws DatabaseException {

            try {
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                dbConfig.setSortedDuplicates(true);
                dbA = env.openDatabase(null, "testA", dbConfig);
                dbB = env.openDatabase(null, "testB", dbConfig);
            } catch (DatabaseException e) {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        Set<TestData> setupInitialData()
            throws DatabaseException {

            putData(dbA, null, new DupData(dbA, 10, 1));
            putData(dbA, null, new DupData(dbA, 20, 1));
            putData(dbA, null, new DupData(dbA, 20, 2));
            putData(dbA, null, new DupData(dbA, 30, -1));

            /*
             * DeleteData uses a cursor in order to delete precisely this
             * record, and that needs a non-null transaction.
             */
            Transaction t = env.beginTransaction(null, null);
            deleteData(dbA, t, new DupData(dbA, 10, 1));
            t.commit();
            return dumpStore(this);
        }

        @Override
        List<TestData> doWork(Transaction trans)
            throws DatabaseException {

            List<TestData> workHistory = new ArrayList<TestData>();

            /* Insert new record. */
            TestData t = new DupData(dbA, 30, 1, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Insert new record, reusing a slot from previous txn. */
            t = new DupData(dbA, 10, 2, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Update record created in this txn. */
            t = new DupData(dbA, 30, 2, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* delete a record created outside this txn. */
            t = new DupData(dbA, 20, 2, true);
            deleteData(dbA, trans, t);
            workHistory.add(t);

            /* delete a record created inside this txn. */
            t = new DupData(dbA, 30, 2, true);
            deleteData(dbA, trans, t);
            workHistory.add(t);

            /* Insert new record, reusing a slot from this txn. */
            t = new DupData(dbA, 30, 10, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Update record created in this txn. */
            t = new DupData(dbA, 30, 11, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            findLsns(DbInternal.getTxn(trans), workHistory);
            return workHistory;
        }

        @Override
        TestData unmarshall(Database db,
                                DatabaseEntry key,
                                DatabaseEntry data) {

            return new DupData(db,
                               IntegerBinding.entryToInt(key),
                               IntegerBinding.entryToInt(data));
        }
    }

    /**
     */
    class CustomBtreeComparatorWorkload extends Workload {
        @Override
        void openStore()
            throws DatabaseException {

            try {
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                dbConfig.setBtreeComparator(new RoundTo2Comparator());
                dbA = env.openDatabase(null, "testA", dbConfig);
                dbB = env.openDatabase(null, "testB", dbConfig);
            } catch (DatabaseException e) {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        Set<TestData> setupInitialData()
            throws DatabaseException {

            /* 10 and 11 equate to the same record. */
            putData(dbA, null, new RoundTo2Data(dbA, 10, 1));
            putData(dbA, null, new RoundTo2Data(dbA, 11, 1));

            /* 20 and 21 equate to the same record. */
            putData(dbA, null, new RoundTo2Data(dbA, 20, 1));
            putData(dbA, null, new RoundTo2Data(dbA, 21, 2));

            putData(dbA, null, new RoundTo2Data(dbA, 30, -1));
            /*
             * DeleteData uses a cursor in order to delete precisely this
             * record, and that needs a non-null transaction.
             */
            Transaction t = env.beginTransaction(null, null);
            deleteData(dbA, t, new RoundTo2Data(dbA, 10, 1));
            t.commit();

            /* Expect to see (20,2) and (30,-1) */
            return dumpStore(this);
        }

        /**
         * Do a mix of inserts, updates, and deletes. For inserts instigated
         * by this transaction, make sure these cases are covered:
         * - first record for this key
         * - key exists, was previously deleted by another txn.
         * - key is overwritten.
         * @throws DatabaseException
         */
        @Override
        List<TestData> doWork(Transaction trans)
            throws DatabaseException {

            List<TestData> workHistory = new ArrayList<TestData>();

            /* Update an existing record, (30,-1). */
            TestData t = new RoundTo2Data(dbA, 31, 99, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Insert new record, reusing a slot from previous txn. */
            t = new RoundTo2Data(dbA, 10, 77, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Update 10,77 */
            t = new RoundTo2Data(dbA, 11, 7, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            /* Delete a record. */
            t = new RoundTo2Data(dbA, 10, 7, true);
            deleteData(dbA, trans, t);
            workHistory.add(t);

            /* New record, 10,200 */
            t = new RoundTo2Data(dbA, 10, 200, false);
            putData(dbA, trans, t);
            workHistory.add(t);

            findLsns(DbInternal.getTxn(trans), workHistory);
            return workHistory;
        }

        /**
         * If we used the DPL, we wouldn't need to have this unmarshall
         * method, because the store would automatically know what type of
         * object to return.
         */
        @Override
        TestData unmarshall(Database db,
                                DatabaseEntry key,
                                DatabaseEntry data) {

            return new RoundTo2Data(db,
                                    IntegerBinding.entryToInt(key),
                                    IntegerBinding.entryToInt(data));
        }
    }

    /**
     * Ideally we'd use the DPL and store TestData directly, but this
     * test needs to exercise duplicates and custom comparators, and therefore
     * uses the base API.
     */
    static class TestData {

        int id;        // key
        int data;      // data
        Database db;

        /* The loggedLsn is the location for this piece of test data. */
        transient
            long loggedLsn;

        /* IsPut is true if put() was called, false if delete() was called. */
        transient
            boolean isDeleted;

        TestData() {
            isDeleted = false;
        }

        TestData(Database db, int id, int data, boolean isDeleted) {
            this.id = id;
            this.data = data;
            this.isDeleted = isDeleted;
            this.db = db;
        }

        TestData(Database db, int id, int data) {
            this.id = id;
            this.data = data;
            this.isDeleted = false;
            this.db = db;
        }

        @Override
        public String toString() {
            String val =  "\n" + id + "/" + data;
            if (isDeleted) {
                val += " (deleted)";
            }

            if (loggedLsn != 0) {
                val += " lsn=" + DbLsn.getNoFormatString(loggedLsn);
            }

            val += " dbName=" + db.getDatabaseName();
            return val;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TestData) {
                TestData t = (TestData) other;
                return ((t.id == id) &&
                        (t.data == data) &&
                        (t.isDeleted == isDeleted) &&
                        (t.db.getDatabaseName().equals(db.getDatabaseName())));
            }

            return false;
        }

        @Override
        public int hashCode() {
            return (id + data + db.getDatabaseName().hashCode());
        }

        int getHashMapKey() {
            return id + db.getDatabaseName().hashCode();
        }
    }

    static class DupData extends TestData {

        DupData(Database db, int id, int data, boolean isDeleted) {
            super(db, id, data, isDeleted);
        }

        DupData(Database db, int id, int data) {
            super(db, id, data);
        }

        @Override
        int getHashMapKey() {
            return id + data;
        }
    }

    @SuppressWarnings("serial")
    static class RoundTo2Comparator
        implements Comparator<byte[]>, Serializable {
        public int compare(byte[] a, byte[] b) {
            int valA = new TupleInput(a).readInt();
            int valB = new TupleInput(b).readInt();

            int roundA = RoundTo2Data.roundTo2(valA);
            int roundB = RoundTo2Data.roundTo2(valB);

            return roundA - roundB;
        }
    }

    /**
     * Test data which uses a partial btree comparator.
     */
    static class RoundTo2Data extends TestData {

        public RoundTo2Data(Database db, int id, int data, boolean isDeleted) {
            super(db, id, data, isDeleted);
        }

        public RoundTo2Data(Database db, int id, int data) {
            super(db, id, data);
        }

        static int roundTo2(int i) {
            return (i & 0xfffffffe);
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TestData) {
                TestData t = (TestData) other;

                return ((roundTo2(t.id) == roundTo2(id)) &&
                        (t.data == data) &&
                        (t.isDeleted == isDeleted));
            }

            return false;
        }

        @Override
        public int hashCode() {
            return (roundTo2(id) + data);
        }

        @Override
        public String toString() {
            return "Rd:" + super.toString();
        }

        @Override
        int getHashMapKey() {
            return roundTo2(id);
        }
    }
}
