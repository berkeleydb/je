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

package com.sleepycat.je.rep.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.VLSN;

/**
 * Test the properties associated with the durable vlsn.
 */
public class DTVLSNTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = 3;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verify that the DTVLSN becomes consistent across the shard and persists
     * across shutdown.
     */
    @Test
    public void testDTVLSNPersistence() throws InterruptedException {
        createGroup();
        final long qvlsn1 =
            repEnvInfo[0].getRepNode().getCurrentTxnEndVLSN().getSequence();
        awaitDTVLSNQuiesce(qvlsn1);

        closeNodes(repEnvInfo);

        final RepEnvInfo minfo = restartNodes(repEnvInfo);
        final long qvlsn2 =
            minfo.getRepNode().getCurrentTxnEndVLSN().getSequence();
        awaitDTVLSNQuiesce(qvlsn1);

        /* Cannot go backwards */
        assertTrue(qvlsn2  >= qvlsn1);
    }

    /**
     * Check the values of the durable transaction VLSN at strategic times to
     * make sure it's tracking changes as expected.
     */
    @Test
    public void testDTVLSN() throws InterruptedException {

        createGroup();

        long dtvlsn = repEnvInfo[0].getRepNode().getDTVLSN();
        assertTrue(!VLSN.isNull(dtvlsn));

        final CommitToken ct =
            populateDB(repEnvInfo[0].getEnv(), "db1", 1, 10,
                   RepTestUtils.SYNC_SYNC_ALL_TC);

        dtvlsn = repEnvInfo[0].getRepNode().getDTVLSN();

        /*
         * The in-memory DTVLSN must be current as a result of ALL acks. A
         * pollCondition is needed here, since the DTVLSN is updated
         * asynchronously wrt the transaction commit above.
         */
        boolean pass = new PollCondition(1, 5000) {

            @Override
            protected boolean condition() {
               return repEnvInfo[0].getRepNode().getDTVLSN() == ct.getVLSN();
            }

        }.await();
        assertTrue(pass);

        final CommitToken ct2 =
            populateDB(repEnvInfo[0].getEnv(), "db2", 1, 10,
                       RepTestUtils.WNSYNC_NONE_TC);

        /*
         * No acks to update DTVLSN. Ensure that the null transaction is
         * created to update the DTVLSN.
         */
        awaitDTVLSNQuiesce(ct2.getVLSN());

        /* Verify that no spurious null commits are being generated. */
        final ReplicationConfig repConfig =
            repEnvInfo[0].getEnv().getRepConfig();
        final long feederManagerPollTimeout =
            PropUtil.parseDuration(repConfig.getConfigParam
                                   (RepParams.
                                    FEEDER_MANAGER_POLL_TIMEOUT.getName()));

        Thread.sleep(10 * feederManagerPollTimeout);

        /* Calculate the VLSN of the null commit which immediately follows.*/
        final long nullCommitVLSN = ct2.getVLSN() + 1;
        assertEquals(nullCommitVLSN,
                     repEnvInfo[0].getRepNode().getCurrentTxnEndVLSN().
                     getSequence());
    }

    /**
     * Ensure that a log that is created by a master and is replayed to a
     * replica can in turn be used to feed a subsequent replica when it becomes
     * the master. In this a rep group is grown to a size of 3 with the master
     * moving to each subsequent new node, after it has initialized itself from
     * a previous copy of the stream. DTVLSNs are checked during replay to
     * ensure they observe their sequencing invariants during replay.
     */
    @Test
    public void testMultiGenerationalStream() throws InterruptedException {
        ReplicatedEnvironment n1 = repEnvInfo[0].openEnv();

        populateDB(n1, 100);

        /* Reads first generation stream to become current. */
        ReplicatedEnvironment n2 = repEnvInfo[1].openEnv();

        populateDB(n1, 100);

        forceMaster(1);

        populateDB(n2, 100);

        /* Reads second generation stream to become current. */
        ReplicatedEnvironment n3 = repEnvInfo[2].openEnv();

        populateDB(n2, 100);

        forceMaster(2);

        populateDB(n3, 100);
    }

    /**
     * Verify that DTVLSN(commitVLSN) == commitVLSN for a RG consisting of a
     * single durable node.
     */
    @Test
    public void testDoesNotNeedAcks() {
        ReplicatedEnvironment n1 = repEnvInfo[0].openEnv();

        CommitToken ct = populateDB(n1, 10);

        assertEquals(ct.getVLSN(), repEnvInfo[0].getRepNode().getAnyDTVLSN());

        ct = populateDB(n1, 10);
        assertEquals(ct.getVLSN(), repEnvInfo[0].getRepNode().getAnyDTVLSN());
    }

    @Test
    public void testSimulatePreDTVLSNGroup() throws InterruptedException {
        RepImpl.setSimulatePreDTVLSNMaster(true);

        createGroup();

        populateDB(repEnvInfo[0].getEnv(), 1);
        populateDB(repEnvInfo[0].getEnv(), 1);

        /*
         * Verify that all nodes have zero dtvlsn values.
         */
        for (RepEnvInfo element : repEnvInfo) {
            assertEquals(VLSN.UNINITIALIZED_VLSN_SEQUENCE,
                         element.getRepNode().getAnyDTVLSN());
        }

        closeNodes(repEnvInfo);

        /* Revert back to postDTVLSN operation. */
        RepImpl.setSimulatePreDTVLSNMaster(false);

        /* The new nodes should be able to hold an election and proceed. */
        RepEnvInfo newMaster = restartNodes(repEnvInfo[0], repEnvInfo[1]);

        populateDB(newMaster.getEnv(), 1);

        /*
         * Now bring up the third node, it should see a pre to post dtvlsn
         * transition in the HA stream.
         */
        repEnvInfo[2].openEnv();

        CommitToken ct = populateDB(newMaster.getEnv(), 1);

        /* Non-zero DTVLSN advancing as expected. */
        awaitDTVLSNQuiesce(ct.getVLSN());
    }

    /**
     * Write concurrently to the environment relying on the checks in HA replay
     * to catch any DTVLSN sequences that are invalid.
     */
    @Test
    public void testConcurrentDTVLSequence()
        throws InterruptedException {


        createGroup();

        final ReplicatedEnvironment master = repEnvInfo[0].getEnv();

        /* Create parallel workload. */

        final int poolThreads = 30;
        ExecutorService requestPool = Executors.newFixedThreadPool(poolThreads);

        populateDB(master, 1);

        for (RepEnvInfo r : repEnvInfo) {
            assertTrue(r.getEnv().getNodeName(), r.getEnv().isValid());
        }

        final Database db = master.openDatabase(null, TEST_DB_NAME, dbconfig);
        final AtomicInteger count = new AtomicInteger(poolThreads * 100);
        final AtomicReference<Throwable> exception = new AtomicReference<>();

        for (int i=0; i < poolThreads; i++) {
            requestPool.
            submit(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    final DatabaseEntry key1 = new DatabaseEntry();
                    final DatabaseEntry value = new DatabaseEntry();
                    LongBinding.longToEntry(1, value);

                    int keyId;
                    Transaction txn = null;
                    while (((keyId = count.decrementAndGet()) > 0) &&
                           (exception.get() == null)) {
                        try {
                            txn = master.beginTransaction(null, null);
                            IntegerBinding.intToEntry(keyId, key1);
                            db.put(txn, key1, value);
                            txn.commit();
                        } catch(Throwable e) {
                            if (txn != null) {
                                txn.abort();
                            }
                            exception.compareAndSet(null, e);
                        }
                    }
                    return 0l;
                }
            });
        }
        requestPool.shutdown();
        assertTrue(requestPool.awaitTermination(60, TimeUnit.SECONDS));
        db.close();

        assertNull(exception.get());
    }


    /**
     * Verify that we roll back non-durable transactions upon startup without
     * a RollbackProhibitedException.
     *
     * The HardRecoveryTest is used to test the converse, that is, that a
     * RollBackProhibitedException is thrown when one is warranted.
     */
    @Test
    public void testRollbackNonDurable() throws InterruptedException {

        createGroup();

        /* Shutdown replicas, leave master intact. */
        closeNodes(repEnvInfo[1], repEnvInfo[2]);
        int rollbackTxns =
            2 * Integer.parseInt(RepParams.TXN_ROLLBACK_LIMIT.getDefault());

        /* Created non-durable transactions on node 1. */
        for (int i=0; i < rollbackTxns; i++) {
            populateDB(repEnvInfo[0].getEnv(), "db1", 1, 1,
                       RepTestUtils.WNSYNC_NONE_TC);
        }

        /* shutdown the master. */
        repEnvInfo[0].closeEnv();

        /* restart nodes 2 and 3 with a new master */
        RepEnvInfo masterInfo = restartNodes(repEnvInfo[1], repEnvInfo[2]);
        for (int i=0; i < rollbackTxns; i++) {
            populateDB(masterInfo.getEnv(), "db2", 1, 1);
        }

        /* Open and rollback without any exceptions. */
        repEnvInfo[0].openEnv();
    }


    /**
     * Verify that in the case of a DTVLSN tie during elections, the one node
     * with the most advanced VLSN wins to minimize the chance of rollbacks.
     */
    @Test
    public void testDTVLSNRanking()
        throws InterruptedException {

        createGroup();

        /*
         * Run a statistically significant number of iterations to ensure that
         * the master is being picked for a good reason.
         */
        for (int n=0; n < repEnvInfo.length; n++) {

            /* Vary the choice of master in the group. */

            final int n1 = n;
            int n2 = (n1 + 1) % repEnvInfo.length;
            int n3 = (n2 + 1) % repEnvInfo.length;

            /* Start with the right master. */
            forceMaster(n1);

            for (int i=0; i < 2; i++) {
                final long qvlsn1 =
                    repEnvInfo[n1].getRepNode().getCurrentTxnEndVLSN().getSequence();
                awaitDTVLSNQuiesce(qvlsn1);
                repEnvInfo[n2].closeEnv();
                repEnvInfo[n3].closeEnv();

                /*
                 * Created non-durable transactions on node 1 to give it the
                 * most advanced VLSN.
                 */
                for (int j=0; j < 2; j++) {
                    populateDB(repEnvInfo[n1].getEnv(), "db1", 1, 1,
                               RepTestUtils.WNSYNC_NONE_TC);
                }

                /* shutdown the master, all nodes are now down. */
                repEnvInfo[n1].closeEnv();

                /*
                 * Hold back node 3, since we don't want n2 and n3 forming
                 * election quorum.
                 */
                restartNodes(repEnvInfo[n1], repEnvInfo[n2]);
                assertTrue(repEnvInfo[n1].getEnv().getState().isMaster());
                repEnvInfo[n3].openEnv();
            }
        }
    }

    private void forceMaster(final int n1) throws InterruptedException {
        repEnvInfo[n1].getRepImpl().getRepNode().forceMaster(true);
        assertTrue(new PollCondition(10, 30000) {

            @Override
            protected boolean condition() {
                return repEnvInfo[n1].getEnv().getState().isMaster();
            }
        }.await());
        repEnvInfo[n1].getRepImpl().getRepNode().forceMaster(false);
    }

    /**
     * Returns after waiting for the DTVLSN across the entire shard to advance
     * past quiesceVLSN
     */
    private void awaitDTVLSNQuiesce(long quiesceVLSN) {
        final AwaitDTVLSN qcond =
            new AwaitDTVLSN(100, 30 * 1000, quiesceVLSN);

        assertTrue(qcond.await());
    }

    /**
     * Utility class to check for a DTVLSN to advance past some expected value
     */
    private class AwaitDTVLSN extends PollCondition {

        final long targetVLSN;

        public AwaitDTVLSN(long checkPeriodMs, long timeoutMs,
                           long targetVLSN) {
            super(checkPeriodMs, timeoutMs);
            this.targetVLSN = targetVLSN;
        }

        @Override
        protected boolean condition() {
            for (RepEnvInfo rfi : repEnvInfo) {
                final long dtvlsn = rfi.getRepNode().getDTVLSN();

                if (dtvlsn >= targetVLSN) {
                    continue;
                }
                return false;
            }
            return true;
        }
    }
}
