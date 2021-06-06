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

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.WaitForMasterListener;
import com.sleepycat.je.rep.utilint.WaitForReplicaListener;
import com.sleepycat.je.util.FileHandler;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test what happens when a master node loses mastership and becomes a replica.
 */
public class MasterChangeTest extends TestBase {

    private final boolean verbose = Boolean.getBoolean("verbose");
    private static final String DB_NAME = "testDb";
    private final File envRoot;
    private final Logger logger;

    public MasterChangeTest() {
        envRoot = SharedTestUtils.getTestDir();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");
        FileHandler.STIFLE_DEFAULT_ERROR_MANAGER = true;
    }

    /**
     * Checks that a master node that becomes a replica functions
     * properly as a replica. Preexisting transactions will be closed and any
     * attempt to do more writes, or to commit or abort old transactions will
     * result in a ReplicaWriteException.
     */
    @Test
    public void testMasterBecomesReplica()
        throws Exception {

        RepEnvInfo[] repEnvInfo = null;
        Database db = null;
        int numNodes = 3;
        Transaction oldMasterIncompleteTxn1 = null;
        Transaction oldMasterIncompleteTxn3 = null;
        Transaction newMasterIncompleteTxn5 = null;
        try {
            repEnvInfo = RepTestUtils.setupEnvInfos
                (envRoot, numNodes, RepTestUtils.SYNC_SYNC_NONE_DURABILITY);

            /*
             * Start the master first, to ensure that it is the master, and
             * then start the rest of the group.
             */
            ReplicatedEnvironment firstMaster = repEnvInfo[0].openEnv();
            assert firstMaster != null;

            db = createDb(firstMaster);
            db.close();
            oldMasterIncompleteTxn1 = doInsert(firstMaster, 1, false);

            for (int i = 1; i < numNodes; i++) {
                repEnvInfo[i].openEnv();
            }

            /*
             * After node1 and node2 join, make sure that their presence in the
             * rep group db is propagated before we do a forceMaster, by doing
             * a consistent read. When a node calls for an election, it must
             * have its own id available to itself from the rep group db on
             * disk. If it doesn't, it will send an election request with an
             * illegal node id. In real life, this can never happen, because a
             * node that does not have its own id won't win mastership, since
             * others will be ahead of it.
             */
            Transaction oldMasterTxn2 = doInsert(firstMaster, 2, true);
            makeReplicasConsistent(oldMasterTxn2, repEnvInfo[1], repEnvInfo[2]);

            /*
             * Mimic a network partition by forcing one replica to become the
             * master.
             */
            int lastIndex = numNodes - 1;
            WaitForMasterListener masterWaiter = new WaitForMasterListener();
            ReplicatedEnvironment forcedMaster = repEnvInfo[lastIndex].getEnv();
            forcedMaster.setStateChangeListener(masterWaiter);
            RepNode lastRepNode =  repEnvInfo[lastIndex].getRepNode();

            WaitForReplicaListener replicaWaiter = new WaitForReplicaListener();
            firstMaster.setStateChangeListener(replicaWaiter);

            /*
             * Write record 3 and do one last incomplete transaction on node1,
             * the current master, used to test that the transaction will later
             * be aborted after mastership transfer.
             */
            oldMasterIncompleteTxn3 = doInsert(firstMaster, 3, false);

            /*
             * Make node3 the master
             */
            lastRepNode.forceMaster(true);
            masterWaiter.awaitMastership();

            /* 
             * Write record 4 on the new master, node3. Insert and commit on
             * the new master. 
             */
            doInsert(forcedMaster, 4, true);

            /*
             * We expect the old master to have become a replica, and for the
             * same environment handle to still be valid. Use the old handle
             * to be sure it's still valid. The old transactions should be
             * aborted.
             */
            replicaWaiter.awaitReplica();
            assertEquals(State.REPLICA, firstMaster.getState());
            assertEquals(Transaction.State.ABORTED, 
                         oldMasterIncompleteTxn1.getState());
            assertEquals(Transaction.State.ABORTED, 
                         oldMasterIncompleteTxn3.getState());

            /*
             * Sync up the group. We should now see records 2 and 4 on all
             * nodes, but records 1 and 3 were not committed, and should be
             * aborted.
             */
            logger.info("sync group");
            VLSN commitVLSN = RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                                 numNodes);

            /*
             * Make sure we can do a transactional cursor scan of the data on
             * all nodes. This will ensure that we don't have any dangling
             * locks.
             */
            scanData(repEnvInfo, 2, 4);

            logger.info("run check");

            RepTestUtils.checkNodeEquality(commitVLSN, verbose, repEnvInfo);
            
            /* 
             * Now return mastership back to node 1, and make sure it can serve
             * as master.
             */
            newMasterIncompleteTxn5 = doInsert(forcedMaster, 5, false);
            RepTestUtils.syncGroup(repEnvInfo);
            WaitForMasterListener oldMasterWaiter =
                new WaitForMasterListener();
            firstMaster.setStateChangeListener(oldMasterWaiter);

            /* Make it possible to check that node3 has become a replica again*/
            WaitForReplicaListener newReplicaWaiter =
                new WaitForReplicaListener();
            forcedMaster.setStateChangeListener(newReplicaWaiter);

            logger.info("returning mastership to first node");
            ReplicationMutableConfig config = new ReplicationMutableConfig();
            config.setNodePriority(2);
            firstMaster.setRepMutableConfig(config);
            repEnvInfo[0].getRepNode().forceMaster(true);
            logger.info("wait for transition of mastership");
            oldMasterWaiter.awaitMastership();
            newReplicaWaiter.awaitReplica();
            logger.info("transition done");
            assertEquals(Transaction.State.ABORTED, 
                         newMasterIncompleteTxn5.getState());

            /* Insert and commit on the old, original master */
            doInsert(firstMaster, 6, true);
            commitVLSN = RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                            numNodes);
            scanData(repEnvInfo, 2, 4, 6);
        } catch (Exception e) {
            logger.info("Unexpected failure");
            e.printStackTrace();
            throw e;
        } finally {
            if (oldMasterIncompleteTxn1 != null) {
                /* Should still be valid to call abort */
                oldMasterIncompleteTxn1.abort();
            }
            
            if (oldMasterIncompleteTxn3 != null) {
                /* Should still be valid to call abort */
                oldMasterIncompleteTxn3.abort();
            }
            
            if (newMasterIncompleteTxn5 != null) {
                /* Should still be valid to call abort */
                newMasterIncompleteTxn5.abort();
            }
            
            if (db != null) {
                db.close();
                db = null;
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }
    
    /**
     * Move mastership around a group by feigning network partitions.
     * @throws Throwable
     */
    @Test
    public void testTransitions() 
        throws Throwable {

        RepEnvInfo[] repEnvInfo = null;
        Database db = null;
        int numNodes = 3;

        repEnvInfo = RepTestUtils.setupEnvInfos
            (envRoot, numNodes, RepTestUtils.SYNC_SYNC_NONE_DURABILITY);

        /*
         * Start the master first, to ensure that it is the master, and
         * then start the rest of the group.
         */
        ReplicatedEnvironment firstMaster = repEnvInfo[0].openEnv();
        assert firstMaster != null;

        db = createDb(firstMaster);
        db.close();

        for (int i = 1; i < numNodes; i++) {
            repEnvInfo[i].openEnv();
        }

        Set<Integer> expectedValues = new HashSet<Integer>();

        int numSwitches = 5;
        int firstIndex = 0;
        AtomicInteger useVal = new AtomicInteger();
        /* 
         * Do a round of switching masters, by mimicking network partitions.
         * Check that open txns are aborted if they have writes, and left open
         * if they only did reads. 
         */
        while (numSwitches-- > 0) {
            WorkGenerator generator = new WorkGenerator(expectedValues, 
                                                        useVal);
            int targetIndex = (firstIndex == 2) ? 0 : firstIndex + 1;
            logger.fine("==> Switching from " + firstIndex + " to " + 
                        targetIndex + " starting with record " + useVal.get());
            startTxnAndSwitch(repEnvInfo, firstIndex, targetIndex, generator);
            firstIndex = targetIndex;
        }
        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    private void startTxnAndSwitch(RepEnvInfo[] repEnvInfo,
                                   int currentIndex,
                                   int targetIndex,
                                   WorkGenerator generator) 
        throws DatabaseException, InterruptedException {

        ReplicatedEnvironment currentMaster = 
            repEnvInfo[currentIndex].getEnv();

        try {
            /*
             * Do one last incomplete transaction on the master, used to test
             * that the transaction will later be aborted after mastership
             * transfer.
             */
            generator.doWorkOnOldMaster(currentMaster);

            /*
             * Mimic a network partition by forcing a replica to become the
             * new master. Keep the old master alive; don't shoot it, because
             * we want to test master->replica transitions.
             */
            WaitForMasterListener masterWaiter = new WaitForMasterListener();
            WaitForReplicaListener replicaWaiter = 
                new WaitForReplicaListener();
            ReplicatedEnvironment targetMaster = 
                repEnvInfo[targetIndex].getEnv();
            targetMaster.setStateChangeListener(masterWaiter);
            currentMaster.setStateChangeListener(replicaWaiter);
            RepNode targetRepNode = repEnvInfo[targetIndex].getRepNode();
            ReplicationMutableConfig config = new ReplicationMutableConfig();
            config.setNodePriority(2);
            targetMaster.setRepMutableConfig(config);
            targetRepNode.forceMaster(true);
            masterWaiter.awaitMastership();
            replicaWaiter.awaitReplica();

            /* Revert the priority back */
            config.setNodePriority(1);
            targetMaster.setRepMutableConfig(config);

            /* Insert and commit on the new master. */
            generator.doWorkOnNewMaster(targetMaster);

            /* 
             * Sync up the group before checking that it holds committed
             * records only.
             */ 
            logger.info("sync group");
            VLSN commitVLSN = 
                RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                   repEnvInfo.length);
            /*
             * Make sure we can do a transactional cursor scan of the data on
             * all nodes. This will ensure that we don't have any dangling
             * locks.
             */
            generator.scanData(repEnvInfo);
            logger.info("run check");
            RepTestUtils.checkNodeEquality(commitVLSN, verbose, repEnvInfo);
        } finally {
            /* 
             * Check that all the non-committed transactions from the old
             * master are not usable anymore. 
             */
            generator.assertIncompleteTxnsInvalidated();

            /* In this test, there should be no recoveries. TODO, better
               test? */
            generator.assertNoRestarts(repEnvInfo);

            /* Close off the old transactions */
            generator.abortIncompleteTxns();
        }
    }

    /**
     * Do a transactional cursor scan and check each node to see if the 
     * expected records are there. Using a transactional cursor means we'll 
     * check for dangling locks.  If a lock is left over, it will cause a 
     * deadlock.
     */
    private void scanData(RepEnvInfo[] repEnvInfo, int... expected) {
        for (RepEnvInfo info: repEnvInfo) {

            Database db = openDb(info.getEnv());
            Cursor c = db.openCursor(null, CursorConfig.READ_COMMITTED);
            DatabaseEntry key = new  DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            for (int val : expected) {
                assertEquals(OperationStatus.SUCCESS, 
                             c.getNext(key, data, null));
                assertEquals(val, IntegerBinding.entryToInt(key));
            }

            /* Assert that there are no other records */
            assertEquals(OperationStatus.NOTFOUND,  c.getNext(key, data, null));

            c.close();
            db.close();
        }
    }

    private Database createDb(ReplicatedEnvironment master) {
        return openDb(master, true);
    }

    private Database openDb(ReplicatedEnvironment master) {
        return openDb(master, false);
    }

    private Database openDb(ReplicatedEnvironment master, boolean allowCreate) {

        Transaction txn = master.beginTransaction(null,null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(allowCreate);
        dbConfig.setTransactional(true);
        Database db = master.openDatabase(txn, DB_NAME, dbConfig);
        txn.commit();
        return db;
    }

    /**
     * @return the transaction for the unfinished txn
     */
    private Transaction doInsert(ReplicatedEnvironment master,
                                 int val,
                                 boolean doCommit) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = null;

        Database db = openDb(master);
        txn = master.beginTransaction(null, null);
        IntegerBinding.intToEntry(val, key);
        IntegerBinding.intToEntry(val, data);
        assertEquals(OperationStatus.SUCCESS, db.put(txn, key, data));

        if (doCommit) {
            txn.commit();
        }

        db.close();

        return txn;
    }


    /** Just do an insert, transactions are managed outside this method. */
    private void doInsert(ReplicatedEnvironment master,
                          final Transaction txn,
                          final int val) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Database db = openDb(master);
        IntegerBinding.intToEntry(val, key);
        IntegerBinding.intToEntry(val, data);
        assertEquals(OperationStatus.SUCCESS, db.put(txn, key, data));
        db.close();
    }

    /**
     * Ensure that the specified nodes are consistent.with the specified
     * transaction.
     */
    private void makeReplicasConsistent(Transaction targetTxn,
                                        RepEnvInfo... repEnvInfo) {
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setConsistencyPolicy(new CommitPointConsistencyPolicy
                                       (targetTxn.getCommitToken(),
                                        1000,
                                        TimeUnit.SECONDS));

        for (RepEnvInfo info : repEnvInfo) {

            /*
             * Open a read transaction that forces the replicas to become
             * consistent.
             */
            Transaction txn = info.getEnv().beginTransaction(null, txnConfig);
            txn.commit();
        }
    }

    /**
     * Generates a different work load for each test iteration. The pattern is
     *    - bring up a group, nodeA is the master
     *    - call doWorkOnOldMaster() to execute a variety of committed and
     *      uncommited transactions on nodeA
     *    - force the mastership of the group to nodeB, leaving nodeA up
     *    - call doWorkOnNewMaster on nodeA
     *    - sync up the group
     *    - call scanData to make sure all nodes have the proper set of 
     *      committed data, and don't see any uncommitted data
     *    - make sure that the transactions used from doWorkOnOldMaster are
     *      invalid, and that the application can no longer use them.
     */
    public class WorkGenerator {

        protected final Set<Transaction> oldMasterIncompleteTxns = 
            new HashSet<Transaction>();
        protected final Set<Transaction> oldMasterUnusedTxns = 
            new HashSet<Transaction>();
        protected final Set<Integer> committedValues;
        protected final AtomicInteger useVal;
        private Cursor oldMasterCursor;
        private Database oldMasterDb;

        WorkGenerator(Set<Integer> committedValues, AtomicInteger useVal) {
            this.committedValues = committedValues;
            this.useVal = useVal;
        }

        public void assertNoRestarts(RepEnvInfo[] repEnvInfo) {
            for (RepEnvInfo rInfo : repEnvInfo) {
                ReplicatedEnvironment.State state = rInfo.getEnv().getState();
                assert (state == State.MASTER) || (state == State.REPLICA) :
                    "state is unexpectedly " + state;
            }
        }

        public void doWorkOnOldMaster(ReplicatedEnvironment master) {

            Transaction t1 = master.beginTransaction(null, null);
            Transaction t2 = master.beginTransaction(null, null);
            Transaction tUnused = master.beginTransaction(null, null);

            /* not committed */
            doInsert(master, t1, useVal.incrementAndGet());

            int recordVal = useVal.incrementAndGet();
            doInsert(master, t2, recordVal);
            committedValues.add(recordVal);
            
            /* not committed */
            doInsert(master, t1, useVal.incrementAndGet());
            
            recordVal = useVal.incrementAndGet();
            doInsert(master, t2, recordVal);
            committedValues.add(recordVal);

            oldMasterIncompleteTxns.add(t1);
            assertEquals("txn " + tUnused.getId(), 0, 
                         DbInternal.getTxn(tUnused).getWriteLockIds().size());
            assertEquals("txn " + tUnused.getId(), 0, 
                         DbInternal.getTxn(tUnused).getReadLockIds().size());
            oldMasterUnusedTxns.add(tUnused);
            t2.commit();

            /* Get read locks */
            oldMasterDb = openDb(master);
            oldMasterCursor = 
                oldMasterDb.openCursor(t1, CursorConfig.READ_COMMITTED);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            while (oldMasterCursor.getNext(key, data, null) == 
                   OperationStatus.SUCCESS) {
                logger.fine("scanning " + IntegerBinding.entryToInt(key));
            }
        }
        
        public void doWorkOnNewMaster(ReplicatedEnvironment currentMaster) {
            Transaction t1 = currentMaster.beginTransaction(null, null);
            int recordVal = useVal.incrementAndGet();
            doInsert(currentMaster, t1, recordVal);
            committedValues.add(recordVal);
            t1.commit();
        }

        public void scanData(RepEnvInfo[] repEnvInfo) {

            for (RepEnvInfo info: repEnvInfo) {
                Database db = openDb(info.getEnv());
                Cursor c = db.openCursor(null, CursorConfig.READ_COMMITTED);

                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();

                Set<Integer> results = new HashSet<Integer>();
                while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
                    results.add(IntegerBinding.entryToInt(key));
                }

                c.close();
                db.close();

                logger.fine("Node " + info.getEnv().getNodeName() + 
                            " contains " + results);
                assert results.containsAll(committedValues) :
                    "Results do not contain all committed values. " +       
                    "results=" + results + " committed=" + committedValues;
                       
                    assert committedValues.containsAll(results) :
                        "CommittedValues do not contain all results. " +
                        "results=" + results + " committed=" + committedValues;
            }
        }

        public void assertIncompleteTxnsInvalidated() {
            for (Transaction txn : oldMasterIncompleteTxns) {
                assert !txn.isValid() : txn.toString() + " " + txn.getState();
            }

            for (Transaction txn : oldMasterUnusedTxns) {
                assert txn.isValid() : txn.toString() + " " + txn.getState();
            }
        }

        public void abortIncompleteTxns() {
            for (Transaction txn : oldMasterIncompleteTxns) {
                txn.abort();
            }

            for (Transaction txn : oldMasterUnusedTxns) {
                txn.abort();
            }
            
            if (oldMasterCursor != null) {
                oldMasterCursor.close();
            }

            if (oldMasterDb != null) {
                oldMasterDb.close();
            }
        }
    }
}
