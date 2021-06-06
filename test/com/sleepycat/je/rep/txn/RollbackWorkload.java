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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.txn.Utils.RollbackData;
import com.sleepycat.je.rep.txn.Utils.SavedData;
import com.sleepycat.je.rep.txn.Utils.TestData;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * A RollbackWorkload is a pattern of data operations designed to test the
 * permutations of ReplayTxn rollback.
 *
 * Each workload defines a set of operations that will happen before and after
 * the syncup matchpoint. The workload knows what should be rolled back and
 * and what should be preserved, and can check the results. Concrete workload
 * classes add themselves to the static set of WorkloadCombinations, and the
 * RollbackTest generates a test case for each workload element.
 */
abstract class RollbackWorkload {

    private static final String TEST_DB = "testdb";
    private static final String DB_NAME_PREFIX = "persist#" + TEST_DB + "#";
    private static final String TEST_DB2 = "testdb2";
    private static final String DB_NAME_PREFIX2 = "persist#" + TEST_DB2 + "#";

    private static final boolean verbose = Boolean.getBoolean("verbose");

    final Set<TestData> saved;
    final Set<TestData> rolledBack;

    /*
     * Most tests use only a single store. A second store may be used when
     * multiple databases are needed, but be careful not to use the same key in
     * both stores, since the code that dumps the stores and compares records
     * uses only the record key.
     */
    private EntityStore store;
    private EntityStore store2;
    PrimaryIndex<Long, TestData> testIndex;
    PrimaryIndex<Long, TestData> testIndex2;

    final Random rand = new Random(10);

    RollbackWorkload() {
        saved = new HashSet<>();
        rolledBack = new HashSet<>();
    }

    boolean isMasterDiesWorkload() {
        return true;
    }

    void masterSteadyWork(ReplicatedEnvironment master) {
    }

    void beforeMasterCrash(ReplicatedEnvironment master)
        throws DatabaseException {
    }

    void afterMasterCrashBeforeResumption(ReplicatedEnvironment master)
        throws DatabaseException {
    }

    void afterReplicaCrash(ReplicatedEnvironment master)
        throws DatabaseException {
    }

    boolean noLockConflict() {
        return true;
    }

    void releaseDbLocks() {
    }

    boolean openStore(ReplicatedEnvironment replicator, boolean readOnly) {
        store = openStoreByName(replicator, TEST_DB, readOnly);
        if (store == null) {
            testIndex = null;
            return false;
        }
        testIndex = store.getPrimaryIndex(Long.class, TestData.class);
        return true;
    }

    boolean openStore2(ReplicatedEnvironment replicator, boolean readOnly) {
        store2 = openStoreByName(replicator, TEST_DB2, readOnly);
        if (store2 == null) {
            testIndex2 = null;
            return false;
        }
        testIndex2 = store2.getPrimaryIndex(Long.class, TestData.class);
        return true;
    }

    EntityStore openStoreByName(ReplicatedEnvironment replicator,
                                String storeName,
                                boolean readOnly) {
        if (readOnly) {
            String catalogDbName =
                "persist#" + storeName + "#com.sleepycat.persist.formats";
            if (!replicator.getDatabaseNames().contains(catalogDbName)) {
                return null;
            }
        }

        StoreConfig config = new StoreConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        config.setReadOnly(readOnly);
        return new EntityStore(replicator, storeName, config);
    }

    /**
     * Dump all the data out of the test db on this replicator. Use
     * READ_UNCOMMITTED so we can see the data for in-flight transactions.
     */
    Set<TestData> dumpData(ReplicatedEnvironment replicator)
        throws DatabaseException {

        Set<TestData> dumpedData = new HashSet<>();

        if (openStore(replicator, true /* readOnly */)) {
            dumpedData.addAll(dumpIndexData(testIndex, replicator));
        }

        if (openStore2(replicator, true /* readOnly */)) {
            dumpedData.addAll(dumpIndexData(testIndex2, replicator));
        }

        close();

        if (verbose) {
            System.out.println("Replicator " + replicator.getNodeName());
            displayDump(dumpedData);
        }

        return dumpedData;
    }

    Set<TestData> dumpIndexData(PrimaryIndex<Long, TestData> index,
                                ReplicatedEnvironment replicator) {

        Transaction txn = replicator.beginTransaction(null, null);

        EntityCursor<TestData> cursor =
            index.entities(txn, CursorConfig.READ_UNCOMMITTED);

        Set<TestData> dumpedData = new HashSet<>();

        for (TestData t : cursor) {
            dumpedData.add(t);
        }

        cursor.close();
        txn.commit();

        return dumpedData;
    }

    private void displayDump(Set<TestData> data) {
        for (TestData t : data) {
            System.out.println(t);
        }
    }

    void close() throws DatabaseException {
        if (store != null) {
            store.close();
            store = null;
            testIndex = null;
        }
        if (store2 != null) {
            store2.close();
            store2 = null;
            testIndex2 = null;
        }
    }

    void removeStore(ReplicatedEnvironment master,
                             String dbNamePrefix) {
        for (String dbName : master.getDatabaseNames()) {
            if (dbName.startsWith(dbNamePrefix)) {
                master.removeDatabase(null, dbName);
            }
        }
    }

    boolean containsAllData(ReplicatedEnvironment replicator)
        throws DatabaseException {

        Set<TestData> dataInStore = dumpData(replicator);
        if (!checkSubsetAndRemove(dataInStore, saved, "saved")) {
            return false;
        }
        if (!checkSubsetAndRemove(dataInStore, rolledBack, "rollback")) {
            return false;
        }
        if (dataInStore.size() == 0) {
            return true;
        }
        if (verbose) {
            System.out.println("DataInStore has an unexpected " +
                               "remainder: " + dataInStore);
        }
        return false;
    }

    boolean containsSavedData(ReplicatedEnvironment replicator)
        throws DatabaseException {

        Set<TestData> dataInStore = dumpData(replicator);
        if (!checkSubsetAndRemove(dataInStore, saved, "saved")) {
            return false;
        }
        if (dataInStore.size() == 0) {
            return true;
        }
        if (verbose) {
            System.out.println("DataInStore has an unexpected " +
                "remainder: " + dataInStore);
        }
        return false;
    }

    private boolean checkSubsetAndRemove(Set<TestData> dataInStore,
                                         Set<TestData> subset,
                                         String checkType) {
        if (dataInStore.containsAll(subset)) {
            /*
             * Doesn't work, why?
             * boolean removed = dataInStore.removeAll(subset);
             */
            for (TestData t: subset) {
                boolean removed = dataInStore.remove(t);
                assert removed;
            }
            return true;
        }

        if (verbose) {
            System.out.println("DataInStore didn't contain " +
                               " subset " + checkType +
                               ". DataInStore=" + dataInStore +
                               " subset = " + subset);
        }
        return false;
    }

    void insertRandom(PrimaryIndex<Long, TestData> index,
                      Transaction txn,
                      Set<TestData> addToSet) {
        assertTrue(addToSet == saved || addToSet == rolledBack);
        int payload = rand.nextInt();
        TestData data = (addToSet == saved) ?
            new SavedData(payload) : new RollbackData(payload);
        /* Must call put() to assign primary key before adding to set. */
        index.put(txn, data);
        addToSet.add(data);
    }

    /**
     * This workload rolls back an unfinished transaction which is entirely
     * after the matchpoint. It tests a complete undo.
     */
    static class IncompleteTxnAfterMatchpoint extends RollbackWorkload {

        IncompleteTxnAfterMatchpoint() {
            super();
        }

        @Override
        void beforeMasterCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction matchpointTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, matchpointTxn, saved);
            insertRandom(testIndex, matchpointTxn, saved);

            /* This commit will serve as the syncup matchpoint */
            matchpointTxn.commit();

            /* This data is in an uncommitted txn, it will be rolled back. */
            Transaction rollbackTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, rollbackTxn, rolledBack);
            insertRandom(testIndex, rollbackTxn, rolledBack);
            insertRandom(testIndex, rollbackTxn, rolledBack);
            insertRandom(testIndex, rollbackTxn, rolledBack);

            close();
        }

        /**
         * The second workload should have a fewer number of updates from the
         * incomplete, rolled back transaction from workloadBeforeNodeLeaves,
         * so that we can check that the vlsn sequences have been rolled back
         * too.  This work is happening while the crashed node is still down.
         */
        @Override
        void afterMasterCrashBeforeResumption(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction whileAsleepTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, whileAsleepTxn, saved);
            whileAsleepTxn.commit();

            Transaction secondTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            close();
        }

        @Override
        void afterReplicaCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction whileReplicaDeadTxn =
                master.beginTransaction(null, null);
            insertRandom(testIndex, whileReplicaDeadTxn, saved);
            whileReplicaDeadTxn.commit();

            Transaction secondTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            close();
        }
    }

    /**
     * This workload creates an unfinished transaction in which all operations
     * exist before the matchpoint. It should be preserved, and then undone
     * by an abort issued by the new master.
     */
    static class IncompleteTxnBeforeMatchpoint extends RollbackWorkload {

        IncompleteTxnBeforeMatchpoint() {
            super();
        }

        @Override
        void beforeMasterCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction matchpointTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, matchpointTxn, saved);
            insertRandom(testIndex, matchpointTxn, saved);

            /* This data is in an uncommitted txn, it will be rolled back. */
            Transaction rollbackTxnA = master.beginTransaction(null, null);
            insertRandom(testIndex, rollbackTxnA, rolledBack);
            insertRandom(testIndex, rollbackTxnA, rolledBack);

            /* This commit will serve as the syncup matchpoint */
            matchpointTxn.commit();

            /* This data is in an uncommitted txn, it will be rolled back. */
            Transaction rollbackTxnB = master.beginTransaction(null, null);
            insertRandom(testIndex, rollbackTxnB, rolledBack);
            close();
        }

        /**
         * The second workload will re-insert some of the data that
         * was rolled back.
         */
        @Override
        void afterMasterCrashBeforeResumption(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction whileAsleepTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, whileAsleepTxn, saved);
            whileAsleepTxn.commit();

            Transaction secondTxn =  master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            close();
        }

        @Override
        void afterReplicaCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction whileReplicaDeadTxn =
                master.beginTransaction(null, null);
            insertRandom(testIndex, whileReplicaDeadTxn, saved);
            whileReplicaDeadTxn.commit();

            Transaction secondTxn =  master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            close();
        }
    }

    /**
     * This workload creates an unfinished transaction in which operations
     * exist before and after the matchpoint. Only the operations after the
     * matchpoint should be rolled back. Ultimately, the rollback transaction
     * will be aborted, because the master is down.
     */
    static class IncompleteTxnStraddlesMatchpoint extends RollbackWorkload {

        IncompleteTxnStraddlesMatchpoint() {
            super();
        }

        @Override
        void beforeMasterCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction matchpointTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, matchpointTxn, saved);
            insertRandom(testIndex, matchpointTxn, saved);

            /* This data is in an uncommitted txn, it will be rolled back. */
            Transaction rollbackTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, rollbackTxn, rolledBack);
            insertRandom(testIndex, rollbackTxn, rolledBack);

            /* This commit will serve as the syncup matchpoint */
            matchpointTxn.commit();

            /* This data is in an uncommitted txn, it will be rolled back. */
            insertRandom(testIndex, rollbackTxn, rolledBack);
            close();
        }

        /**
         * The second workload will re-insert some of the data that
         * was rolled back.
         */
        @Override
        void afterMasterCrashBeforeResumption(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction whileAsleepTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, whileAsleepTxn, saved);
            whileAsleepTxn.commit();

            Transaction secondTxn =  master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            close();
        }

        @Override
        void afterReplicaCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);

            Transaction whileReplicaDeadTxn = 
                master.beginTransaction(null, null);
            insertRandom(testIndex, whileReplicaDeadTxn, saved);
            whileReplicaDeadTxn.commit();

            Transaction secondTxn =  master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            close();
        }
    }

    /**
     * Exercise the rollback of database operations.
     */
    static class DatabaseOpsStraddlesMatchpoint extends RollbackWorkload {

        private DatabaseConfig dbConfig;
        
        private List<String> expectedDbNames;
        private List<String> allDbNames;
        private Transaction incompleteTxn;

        DatabaseOpsStraddlesMatchpoint() {
            super();
            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
        }

        /* Executed by node that is the first master. */
        @Override
        void beforeMasterCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            Transaction txn1 = master.beginTransaction(null, null);
            Transaction txn2 = master.beginTransaction(null, null);
            Transaction txn3 = master.beginTransaction(null, null);

            Database dbA = master.openDatabase(txn1, "AAA", dbConfig);
            dbA.close();
            Database dbB = master.openDatabase(txn2, "BBB", dbConfig);
            dbB.close();
            
            /* 
             * This will be the syncpoint. 
             * txn 1 is commmited.
             * txn 2 will be partially rolled back.
             * txn 3 will be fully rolled back.
             */
            txn1.commit();

            /* Txn 2 will have to be partially rolled back. */
            master.removeDatabase(txn2, "BBB");

            /* Txn 3 will be fully rolled back. */
            master.removeDatabase(txn3, "AAA");

            expectedDbNames = new ArrayList<>();
            expectedDbNames.add("AAA");

            allDbNames = new ArrayList<>();
            allDbNames.add("AAA");
        }

        /* Executed by node that was a replica, and then became master */
        @Override
        void afterMasterCrashBeforeResumption(ReplicatedEnvironment master)
            throws DatabaseException {

            Transaction whileAsleepTxn = master.beginTransaction(null, null);
            Database dbC = master.openDatabase(whileAsleepTxn, "CCC", 
                                               dbConfig);
            dbC.close();
            whileAsleepTxn.commit();

            incompleteTxn = master.beginTransaction(null, null);
            Database dbD = master.openDatabase(incompleteTxn, "DDD", dbConfig);
            dbD.close();

            expectedDbNames = new ArrayList<>();
            expectedDbNames.add("AAA");
            expectedDbNames.add("CCC");
            expectedDbNames.add("DDD");
        }

        @Override
        void releaseDbLocks() {
            incompleteTxn.commit();
        }

        /* Executed while node that has never been a master is asleep. */
        @Override
        void afterReplicaCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            Transaction whileReplicaDeadTxn =
                master.beginTransaction(null, null);
            master.renameDatabase(whileReplicaDeadTxn,
                                  "CCC", "renamedCCC");
            whileReplicaDeadTxn.commit();
            expectedDbNames = new ArrayList<>();
            expectedDbNames.add("AAA");
            expectedDbNames.add("renamedCCC");
            expectedDbNames.add("DDD");
        }

        boolean noLockConflict() {
            return false;
        }

        @Override
        boolean containsSavedData(ReplicatedEnvironment master) {
            List<String> names = master.getDatabaseNames();
            if (!(names.containsAll(expectedDbNames) &&
                  expectedDbNames.containsAll(names))) {
                System.out.println("master names = " + names +
                                   " expected= " + expectedDbNames);
                return false;
            }
            return true;
        }

        @Override
        boolean containsAllData(ReplicatedEnvironment master)
            throws DatabaseException {

            List<String> names = master.getDatabaseNames();
            return names.containsAll(allDbNames) &&
                allDbNames.containsAll(names);
        }
    }

    /**
     * An incomplete transaction containing LN writes is rolled back, and then
     * the database containing those LNs is removed.  Recovery of this entire
     * sequence requires rollback to process the LNs belonging to the removed
     * database.  When this test was written, rollback threw an NPE when such
     * LNs were encountered (due to the removed database), and a bug fix was
     * required [#22052].
     */
    static class RemoveDatabaseAfterRollback
        extends IncompleteTxnAfterMatchpoint {

        @Override
        void afterReplicaCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            super.afterReplicaCrash(master);
            removeStore(master, DB_NAME_PREFIX);
            rolledBack.addAll(saved);
            saved.clear();
        }
    }

    /**
     * Similar to RemoveDatabaseAfterRollback except that the txn contains LNs
     * in multiple databases (two in this test) and only some databases (one in
     * this test), not all, are removed after rollback.
     *
     * When undoing an LN in recovery, if the LN is in a removed database,
     * the undo code will do nothing.  This is the case handed by the earlier
     * test, RemoveDatabaseAfterRollback.
     *
     * But when the LN is not in a removed database, the undo must proceed.
     * The tricky thing is that a TxnChain must be created even though some of
     * the entries in the actual txn chain (in the data log) will be for LNs in
     * removed databases.
     *
     * This test does not subclass RemoveDatabaseAfterRollback or
     * IncompleteTxnAfterMatchpoint because those tests only use one database.
     *
     * [#22071]
     */
    static class RemoveSomeDatabasesAfterRollback extends RollbackWorkload {

        @Override
        void beforeMasterCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);
            openStore2(master, false /* readOnly */);

            Transaction matchpointTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, matchpointTxn, saved);
            insertRandom(testIndex2, matchpointTxn, saved);
            insertRandom(testIndex, matchpointTxn, saved);
            insertRandom(testIndex2, matchpointTxn, saved);

            /* This commit will serve as the syncup matchpoint */
            matchpointTxn.commit();

            /* This data is in an uncommitted txn, it will be rolled back. */
            Transaction rollbackTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, rollbackTxn, rolledBack);
            insertRandom(testIndex2, rollbackTxn, rolledBack);
            insertRandom(testIndex, rollbackTxn, rolledBack);
            insertRandom(testIndex2, rollbackTxn, rolledBack);

            close();
        }

        /**
         * @see IncompleteTxnAfterMatchpoint#afterMasterCrashBeforeResumption
         */
        @Override
        void afterMasterCrashBeforeResumption(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);
            openStore2(master, false /* readOnly */);

            Transaction whileAsleepTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, whileAsleepTxn, saved);
            insertRandom(testIndex2, whileAsleepTxn, saved);
            whileAsleepTxn.commit();

            Transaction secondTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            insertRandom(testIndex2, secondTxn, saved);
            close();
        }

        @Override
        void afterReplicaCrash(ReplicatedEnvironment master)
            throws DatabaseException {

            openStore(master, false /* readOnly */);
            openStore2(master, false /* readOnly */);

            Transaction whileReplicaDeadTxn =
                master.beginTransaction(null, null);
            insertRandom(testIndex, whileReplicaDeadTxn, saved);
            insertRandom(testIndex2, whileReplicaDeadTxn, saved);
            whileReplicaDeadTxn.commit();

            Transaction secondTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, secondTxn, saved);
            insertRandom(testIndex2, secondTxn, saved);

            Set<TestData> index2Data = dumpIndexData(testIndex2, master);
            close();
            removeStore(master, DB_NAME_PREFIX2);
            rolledBack.addAll(index2Data);
            saved.removeAll(index2Data);
        }
    }

    /**
     * This workload simulates a master that is just doing a steady stream of
     * work.
     */
    static class SteadyWork extends RollbackWorkload {

        private Transaction straddleTxn = null;

        @Override
        boolean isMasterDiesWorkload() {
            return false;
        }

        @Override
        void masterSteadyWork(ReplicatedEnvironment master)
            throws DatabaseException {

            if (straddleTxn != null) {
                straddleTxn.commit();
            }

            openStore(master, false /* readOnly */);

            Transaction matchpointTxn = master.beginTransaction(null, null);
            insertRandom(testIndex, matchpointTxn, saved);
            insertRandom(testIndex, matchpointTxn, saved);

            /* This transaction straddles the matchpoint. */
            straddleTxn = master.beginTransaction(null, null);
            insert();
            insert();

            /* This commit will serve as the syncup matchpoint */
            matchpointTxn.commit();

            for (int i = 0; i < 10; i++) {
                insert();
            }
            
            close();
        }

        private void insert()  {
            TestData d = new SavedData(12);
            /* Must call put() to assign primary key before adding to set. */
            testIndex.put(straddleTxn, d);
            saved.add(d);
        }
    }
}
