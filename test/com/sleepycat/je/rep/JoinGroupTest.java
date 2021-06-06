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

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;

public class JoinGroupTest extends RepTestBase {

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        /* Add a secondary node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        repEnvInfo[repEnvInfo.length - 1].getRepConfig().setNodeType(
            NodeType.SECONDARY);
    }

    /**
     * Simulates the scenario where an entire group goes down and is restarted.
     */
    @Test
    public void testAllJoinLeaveJoinGroup()
        throws DatabaseException,
               InterruptedException {

        createGroup();
        ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();
        populateDB(masterRep, TEST_DB_NAME, 100);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Shutdown the entire group. */
        closeNodes(repEnvInfo);

        /*
         * Restart the group, using a longer join wait time to allow the
         * secondary to query the primaries a second time after the election is
         * complete.  See RepNode.MASTER_QUERY_INTERVAL.
         */
        final long masterQueryInterval = 10000;
        restartNodes(JOIN_WAIT_TIME + masterQueryInterval, repEnvInfo);
    }

    // Tests repeated opens of the same environment
    @Test
    public void testRepeatedOpen()
        throws UnknownMasterException, DatabaseException {

        /* All nodes have joined. */
        createGroup();

        /* Already joined, rejoin master. */
        State state = repEnvInfo[0].getEnv().getState();
        assertEquals(State.MASTER, state);

        /* Already joined, rejoin replica, by creating another handle. */
        ReplicatedEnvironment r1Handle = new ReplicatedEnvironment
            (repEnvInfo[1].getEnvHome(),
             repEnvInfo[1].getRepConfig(),
             repEnvInfo[1].getEnvConfig());
        state = r1Handle.getState();
        assertEquals(State.REPLICA, state);
        r1Handle.close();
    }

    @Test
    public void testDefaultJoinGroup()
        throws UnknownMasterException,
               DatabaseException {

        createGroup();
        ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();
        assertEquals(State.MASTER, masterRep.getState());
        leaveGroupAllButMaster();
        /* Populates just the master. */
        CommitToken ct = populateDB(masterRep, TEST_DB_NAME, 100);

        /* Replicas should have caught up when they re-open their handles. */
        for (RepEnvInfo ri : repEnvInfo) {
            ReplicatedEnvironment rep =
                (ri.getEnv() == null) ? ri.openEnv() : ri.getEnv();
            VLSN repVLSN = RepInternal.getNonNullRepImpl(rep).
                getVLSNIndex().getRange().getLast();
            assertTrue(new VLSN(ct.getVLSN()).compareTo(repVLSN) <= 0);
        }
    }

    @Test
    public void testDefaultJoinGroupHelper()
        throws UnknownMasterException,
               DatabaseException {

        for (int i = 0; i < repEnvInfo.length; i++) {
            RepEnvInfo ri = repEnvInfo[i];
            if ((i + 1) == repEnvInfo.length) {
                /* Use a non-master helper for the last replicator. */
                ReplicationConfig config =
                    RepTestUtils.createRepConfig((short) (i + 1));
                String hpPairs = "";
                // Skip the master, use all the other nodes
                for (int j = 1; j < i; j++) {
                    hpPairs +=
                        "," + repEnvInfo[j].getRepConfig().getNodeHostPort();
                }
                hpPairs = hpPairs.substring(1);
                config.setHelperHosts(hpPairs);
                File envHome = ri.getEnvHome();
                ri = repEnvInfo[i] =
                        new RepEnvInfo(envHome,
                                       config,
                                       RepTestUtils.createEnvConfig
                                       (Durability.COMMIT_SYNC));
            }
            ri.openEnv();
            State state = ri.getEnv().getState();
            assertEquals((i == 0) ? State.MASTER : State.REPLICA, state);
        }
    }

    @Test
    public void testTimeConsistencyJoinGroup()
        throws UnknownMasterException,
               DatabaseException{

        createGroup();
        ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();
        assertEquals(State.MASTER, masterRep.getState());

        leaveGroupAllButMaster();
        /* Populates just the master. */
        populateDB(masterRep, TEST_DB_NAME, 1000);

        repEnvInfo[1].openEnv
            (new TimeConsistencyPolicy(1, TimeUnit.MILLISECONDS,
                                       RepTestUtils.MINUTE_MS,
                                       TimeUnit.MILLISECONDS));
        ReplicatedEnvironmentStats stats =
            repEnvInfo[1].getEnv().getRepStats(StatsConfig.DEFAULT);

        assertEquals(1, stats.getTrackerLagConsistencyWaits());
        assertTrue(stats.getTrackerLagConsistencyWaitMs() > 0);
    }

    @Test
    public void testVLSNConsistencyJoinGroup()
        throws UnknownMasterException,
               DatabaseException,
               InterruptedException {

        createGroup();
        ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();
        assertEquals(State.MASTER, masterRep.getState());
        leaveGroupAllButMaster();
        /* Populates just the master. */
        populateDB(masterRep, TEST_DB_NAME, 100);
        UUID uuid =
            RepInternal.getNonNullRepImpl(masterRep).getRepNode().getUUID();
        long masterVLSN = RepInternal.getNonNullRepImpl(masterRep).
            getVLSNIndex().getRange().getLast().
            getSequence()+2 /* 1 new entry + txn commit record */;

        JoinCommitThread jt =
            new JoinCommitThread(new CommitToken(uuid,masterVLSN),
                                 repEnvInfo[1]);
        jt.start();
        Thread.sleep(5000);
        // supply the vlsn it's waiting for. Record count MUST sync up with
        // the expected masterVLSN
        populateDB(masterRep, TEST_DB_NAME, 1);
        jt.join(JOIN_WAIT_TIME);

        assertTrue(!jt.isAlive());
        assertNull("Join thread exception", jt.testException);
    }

    /*
     * Test that a replica using the jdb files copied from the master can join
     * the group.
     */
    @Test
    public void testCopyEnvJoin()
        throws Throwable {

        createGroup(1);
        assertTrue(repEnvInfo[0].isMaster());

        /* Create some data on the master. */
        populateDB(repEnvInfo[0].getEnv(), "testDB", 1000);

        /* Close the Environment before copy. */
        repEnvInfo[0].closeEnv();

        /* First check there is no jdb files in the second replica. */
        File repEnvHome = repEnvInfo[1].getEnvHome();
        File[] envFiles = repEnvHome.listFiles();
        for (File envFile : envFiles) {
            if (envFile.getName().contains(".jdb")) {
                throw new IllegalStateException
                    ("Replica home should not contain any jdb files");
            }
        }

        /* Copy the jdb files from the master to the replica. */
        SharedTestUtils.copyFiles(repEnvInfo[0].getEnvHome(),
                                  repEnvInfo[1].getEnvHome());

        /* Reopen the master. */
        repEnvInfo[0].openEnv();
        assertTrue(repEnvInfo[0].isMaster());

        /* Open the replica. */
        repEnvInfo[1].openEnv();
        assertTrue(repEnvInfo[1].isReplica());

        /* Read the data to make sure data is correctly copied. */
        Database db =
            repEnvInfo[1].getEnv().openDatabase(null, "testDB", dbconfig);
        for (int i = 0; i < 1000; i++) {
            IntegerBinding.intToEntry(i, key);
            db.get(null, key, data, null);
            assertEquals(i, (int) LongBinding.entryToLong(data));
        }
        db.close();
    }

    /* Utility thread for joining group. */
    class JoinCommitThread extends Thread {
        final RepEnvInfo replicator;
        final CommitToken commitToken;
        Exception testException = null;

        JoinCommitThread(CommitToken commitToken, RepEnvInfo replicator) {
            this.commitToken = commitToken;
            this.replicator = replicator;
        }

        @Override
        public void run() {
            try {
                ReplicatedEnvironment repenv= replicator.openEnv
                    (new CommitPointConsistencyPolicy(commitToken,
                                                      RepTestUtils.MINUTE_MS,
                                                      TimeUnit.MILLISECONDS));
                assertEquals(ReplicatedEnvironment.State.REPLICA,
                             repenv.getState());
                ReplicatedEnvironmentStats stats =
                    replicator.getEnv().getRepStats(StatsConfig.DEFAULT);

                assertEquals(1, stats.getTrackerVLSNConsistencyWaits());
                assertTrue(stats.getTrackerVLSNConsistencyWaitMs() > 0);
            } catch (UnknownMasterException e) {
                testException = e;
                throw new RuntimeException(e);
            } catch (DatabaseException e) {
                testException = e;
                throw new RuntimeException(e);
            }
        }
    }
}
