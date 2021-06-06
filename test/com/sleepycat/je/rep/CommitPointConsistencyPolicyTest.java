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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;

public class CommitPointConsistencyPolicyTest extends RepTestBase {

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 2;
        super.setUp();

        /* Add a secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        repEnvInfo[repEnvInfo.length-1].getRepConfig().setNodeType(
            NodeType.SECONDARY);
    }

    @Test
    public void testCommitPointConsistencyOnOpen() {
        ReplicatedEnvironment menv = repEnvInfo[0].openEnv();
        CommitToken token = populateDB(menv, TEST_DB_NAME, 10);
        CommitPointConsistencyPolicy cp =
            new CommitPointConsistencyPolicy(token, 100, TimeUnit.SECONDS);
        for (int i = 1; i < repEnvInfo.length; i++) {
            ReplicatedEnvironment renv = repEnvInfo[i].openEnv(cp);
            /* Verify that the database is available on the replica. */
            Database rdb = renv.openDatabase(null, TEST_DB_NAME, dbconfig);
            rdb.close();
        }
    }

    @Test
    public void testVLSNConsistencyJoinGroup()
        throws UnknownMasterException,
               DatabaseException,
               InterruptedException {

        createGroup();
        leaveGroupAllButMaster();
        ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();

        /* Populate just the master. */
        CommitToken commitToken = populateDB(masterRep, TEST_DB_NAME, 100);
        CommitPointConsistencyPolicy cp1 =
            new CommitPointConsistencyPolicy(commitToken, 1, TimeUnit.SECONDS);

        final int failTimeout = 2000;
        final int passTimeout = 5000;
        final StatsConfig statsConf = new StatsConfig().setClear(true);
        TxnThread[] txnThreads = new TxnThread[repEnvInfo.length];

        for (int i = 1; i < repEnvInfo.length; i++) {
            ReplicatedEnvironment replica = repEnvInfo[i].openEnv();

            // In sync to the commit point
            TransactionConfig tc = new TransactionConfig();
            tc.setConsistencyPolicy(cp1);
            Transaction txn = replica.beginTransaction(null, tc);
            txn.commit();

            CommitToken futureCommitToken =
                new CommitToken(commitToken.getRepenvUUID(),
                                commitToken.getVLSN() + 100);

            tc.setConsistencyPolicy(
                new CommitPointConsistencyPolicy(
                    futureCommitToken, failTimeout, TimeUnit.MILLISECONDS));
            long start = System.currentTimeMillis();
            try {
                txn = null;
                // Unable to reach consistency, timeout.
                txn = replica.beginTransaction(null, tc);
                txn.abort();
                fail("Exception expected");
            } catch (ReplicaConsistencyException rce) {
                long policyTimeout = rce.getConsistencyPolicy().getTimeout(
                    TimeUnit.MILLISECONDS);
                assertTrue(policyTimeout <=
                           (System.currentTimeMillis() - start));
            }

            // reset statistics
            replica.getRepStats(statsConf);

            // Have a replica transaction actually wait
            tc.setConsistencyPolicy(
                new CommitPointConsistencyPolicy(
                    futureCommitToken, passTimeout, TimeUnit.MILLISECONDS));
            TxnThread txnThread = new TxnThread(replica, tc);
            txnThreads[i] = txnThread;
            txnThread.start();
            Thread.yield(); // give the other threads a chance to block
        }

        // Advance the master
        populateDB(masterRep, TEST_DB_NAME, 100, 100);

        for (int i = 1; i < repEnvInfo.length; i++) {
            ReplicatedEnvironment replica = repEnvInfo[i].getEnv();
            TxnThread txnThread = txnThreads[i];
            txnThread.join(passTimeout);
            assertTrue(!txnThread.isAlive());
            assertNull("i=" + i + ": Exception: " + txnThread.testException,
                       txnThread.testException);
            ReplicatedEnvironmentStats stats = replica.getRepStats(statsConf);
            assertEquals(1, stats.getTrackerVLSNConsistencyWaits());

            // Test with a commit token which is in the past replica does not
            // need to wait.

            TransactionConfig tc = new TransactionConfig();
            tc.setConsistencyPolicy(cp1);
            Transaction txn = replica.beginTransaction(null, tc);
            stats = replica.getRepStats(statsConf.setClear(true));
            assertEquals(0, stats.getTrackerVLSNConsistencyWaits());
            txn.commit();
        }
    }

    class TxnThread extends Thread {
        final ReplicatedEnvironment replicator;
        final TransactionConfig tc;
        Exception testException = null;

        TxnThread(ReplicatedEnvironment replicator, TransactionConfig tc) {
            this.replicator = replicator;
            this.tc = tc;
        }

        @Override
        public void run() {
            try {
                Transaction txn = replicator.beginTransaction(null, tc);
                txn.commit();
            } catch (Exception e) {
                testException = e;
                e.printStackTrace();
            }
        }
    }
}
