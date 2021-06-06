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
import static org.junit.Assert.fail;

import static com.sleepycat.je.rep.impl.RepParams.COMMIT_TO_NETWORK;
import static com.sleepycat.je.rep.impl.RepParams.TEST_JE_VERSION;
import static com.sleepycat.je.rep.impl.RepParams.TEST_REPLICA_DELAY;

import java.util.Arrays;

import org.junit.Test;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.TestHookAdapter;
import com.sleepycat.je.utilint.WaitTestHook;

/** Specific tests for secondary nodes. */
public class SecondaryNodeTest extends RepTestBase {

    public SecondaryNodeTest() {

        /* Use a smaller group size, since we'll be adding secondaries */
        groupSize = 3;
    }

    /* Tests */

    /** Add a secondary to a group, shutdown secondary, and restart. */
    @Test
    public void testJoinLeaveJoin()
        throws Exception {

        /* Create initial group */
        createGroup();

        /* Add the secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();

        /* Check that the node ID is assigned */
        int nodeId = secondaryInfo.getRepNode().getNodeId();
        assertTrue("Node ID should be non-negative: " + nodeId, nodeId > 0);

        /* Populate and wait for replication to complete */
        final ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();
        populateDB(masterRep, 100);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Close and reopen the secondary */
        secondaryInfo.closeEnv();
        secondaryInfo.openEnv();
        nodeId = secondaryInfo.getRepNode().getNodeId();
        assertTrue("Node ID should be non-negative: " + nodeId, nodeId > 0);
    }

    /** Add a secondary to a group, then shutdown and restart all nodes. */
    @Test
    public void testJoinLeaveAllJoinAll()
        throws Exception {

        /* Create initial group */
        createGroup();

        /* Add the secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();

        /* Check that the node ID is assigned */
        int nodeId = secondaryInfo.getRepNode().getNodeId();
        assertTrue("Node ID should be non-negative: " + nodeId, nodeId > 0);

        /* Populate and wait for replication to complete */
        final ReplicatedEnvironment masterRep = repEnvInfo[0].getEnv();
        populateDB(masterRep, 100);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Close all members */
        closeNodes(repEnvInfo);

        /*
         * Reopen all members, using a longer join wait time to allow the
         * secondary to query the primaries a second time after the election is
         * complete.  See RepNode.MASTER_QUERY_INTERVAL.
         */
        final long masterQueryInterval = 10000;
        restartNodes(JOIN_WAIT_TIME + masterQueryInterval, repEnvInfo);

        nodeId = secondaryInfo.getRepNode().getNodeId();
        assertTrue("Node ID should be non-negative: " + nodeId, nodeId > 0);
    }

    /**
     * Add a secondary node to a group and force a new master, to test that the
     * secondary node can successfully replicate with the new master.  [#22980]
     */
    @Test
    public void testSecondaryChangeMaster()
        throws Exception {

        /* Create initial group */
        createGroup();

        /* Add secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();

        /* Restart master */
        final RepEnvInfo masterInfo1 = findMasterWait(5000, repEnvInfo);
        masterInfo1.closeEnv();
        masterInfo1.openEnv();

        /* Find new master */
        final RepEnvInfo masterInfo2 = findMasterWait(5000, repEnvInfo);
        assertTrue("Shouldn't have same master", masterInfo1 != masterInfo2);

        /* Write and await propagation */
        populateDB(masterInfo2.getEnv(), 100);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);
    }

    /** Test election and durability quorums. */
    @Test
    public void testQuorums()
        throws Exception {

        /* Create initial group with three members */
        createGroup(3);

        /* Add a secondary node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();

        /*
         * Close one primary and confirm that quorum is maintained.  Note that
         * 2/3 is a majority if the secondary is not counted, but 2/4 is not,
         * so closing one primary should confirm that the secondary is not
         * being included in the quorum count.
         */
        repEnvInfo[0].closeEnv();
        ReplicatedEnvironment master =
            findMasterAndWaitForReplicas(5000, 1, repEnvInfo).getEnv();
        logger.info("2/3 plus secondary, master " + master.getNodeName());
        populateDB(master, TEST_DB_NAME, 0, 100,
                   new TransactionConfig().setDurability(
                       RepTestUtils.DEFAULT_DURABILITY));

        /* Close the secondary and confirm again. */
        secondaryInfo.closeEnv();
        master = findMasterWait(5000, repEnvInfo).getEnv();
        logger.info("2/3 without secondary, master " + master.getNodeName());
        populateDB(master, TEST_DB_NAME, 100, 100,
                   new TransactionConfig().setDurability(
                       RepTestUtils.DEFAULT_DURABILITY));

        /*
         * Open the secondary and close another primary to confirm that the
         * secondary is not being counted.
         */
        secondaryInfo.openEnv();
        repEnvInfo[1].closeEnv();
        master = findMasterWait(5000, repEnvInfo).getEnv();
        logger.info("1/3 with secondary, master " + master.getNodeName());
        try {
            populateDB(master, TEST_DB_NAME, 200, 100,
                       new TransactionConfig().setDurability(
                           RepTestUtils.DEFAULT_DURABILITY));
            fail("Expected exception");
        } catch (InsufficientReplicasException e) {
            logger.info("Got expected exception: " + e);
        }

        /*
         * Reopen the last closed primary but introduce a long delay for
         * acknowledgments so that its acknowledgment isn't counted, to
         * confirm that the secondary is not providing an acknowledgment.
         */
        repEnvInfo[1].getRepConfig().setConfigParam(
            TEST_REPLICA_DELAY.getName(), "10000");
        repEnvInfo[1].openEnv();
        master = findMasterWait(5000, repEnvInfo).getEnv();
        logger.info("2/3 with delay and secondary, master " +
                    master.getNodeName());
        try {
            populateDB(master, TEST_DB_NAME, 300, 100,
                       new TransactionConfig().setDurability(
                           RepTestUtils.DEFAULT_DURABILITY));
            fail("Expected exception");
        } catch (InsufficientAcksException e) {
            logger.info("Got expected exception: " + e);
        }
        repEnvInfo[1].getRepConfig().setConfigParam(
            TEST_REPLICA_DELAY.getName(), "0");

        /*
         * Close and restart using commitToNetwork, to test that separate code
         * path.
         */
        closeNodes(repEnvInfo);
        for (final RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(
                COMMIT_TO_NETWORK.getName(), "true");
        }
        restartNodes(repEnvInfo[1], repEnvInfo[2]);
        RepTestUtils.syncGroupToLastCommit(
            new RepEnvInfo[] { repEnvInfo[1], repEnvInfo[2] }, 2);
        secondaryInfo.openEnv();
        RepEnvInfo masterInfo = findMasterWait(5000, repEnvInfo);
        master = masterInfo.getEnv();
        RepEnvInfo replicaInfo = (masterInfo == repEnvInfo[1]) ?
            repEnvInfo[2] :
            repEnvInfo[1];
        Feeder replicaFeeder = masterInfo.getRepNode().feederManager().
            getFeeder(replicaInfo.getEnv().getNodeName());
        WaitTestHook<Message> writeMessageHook = new WaitTestHook<Message>();
        logger.info("2/3 with commitToNetwork, delay, and secondary, master " +
                    master.getNodeName());
        try {
            replicaFeeder.setWriteMessageHook(writeMessageHook);
            populateDB(master, TEST_DB_NAME, 300, 100,
                       new TransactionConfig().setDurability(
                           RepTestUtils.DEFAULT_DURABILITY));
            fail("Expected exception");
        } catch (InsufficientAcksException e) {
            logger.info("Got expected exception: " + e);
        } finally {
            writeMessageHook.stopWaiting();
            replicaFeeder.setWriteMessageHook(null);
        }
        for (final RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(
                COMMIT_TO_NETWORK.getName(), "false");
        }

        /*
         * Close and restart primaries, and test operations with quorum=ALL to
         * make sure it does not include the secondary.
         */
        closeNodes(repEnvInfo);
        final RepEnvInfo[] primaries = Arrays.copyOf(repEnvInfo, groupSize);
        for (final RepEnvInfo primary : primaries) {
            primary.setInitialElectionPolicy(QuorumPolicy.ALL);
        }
        master = restartNodes(primaries).getEnv();
        logger.info("ALL without secondary, master " + master.getNodeName());
        populateDB(master, TEST_DB_NAME, 0, 100,
                   new TransactionConfig().setDurability(
                       RepTestUtils.SYNC_SYNC_ALL_DURABILITY));
    }

    /**
     * Test that a lagging secondary node triggers the N_MAX_REPLICA_LAG_NAME
     * statistic.
     */
    @Test
    public void testSecondaryLag()
        throws Exception {

        /* Create group with secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        createGroup();
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Clear and discard stats. */
        final RepEnvInfo masterInfo = findMaster(repEnvInfo);
        final StatsConfig statsConfig = new StatsConfig().setClear(true);
        masterInfo.getEnv().getRepStats(statsConfig);

        /* Configure a message delay for the secondary */
        final String secondaryName = secondaryInfo.getEnv().getNodeName();
        final Feeder secondaryFeeder =
            masterInfo.getRepNode().feederManager().getFeeder(secondaryName);
        try {
            secondaryFeeder.setWriteMessageHook(
                new TestHookAdapter<Message>() {
                    @Override
                    public void doHook(Message msg) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    }
                });

            /* Populate */
            populateDB(masterInfo.getEnv(), 100);
            RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);
        } finally {
            secondaryFeeder.setWriteMessageHook(null);
        }

        /* Check stats */
        final ReplicatedEnvironmentStats stats =
            masterInfo.getEnv().getRepStats(statsConfig);
        assertEquals("Secondary node should be the slowest",
                     secondaryName, stats.getNMaxReplicaLagName());
    }

    /**
     * Test setting ENV_UNKNOWN_STATE_TIMEOUT to permit reading from a
     * disconnected secondary.
     */
    @Test
    public void testReadDisconnectedSecondary()
        throws Exception {

        /* Create group with secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        createGroup();

        /* Populate */
        final ReplicatedEnvironment master = findMaster(repEnvInfo).getEnv();
        populateDB(master, 100);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Close all nodes */
        closeNodes(repEnvInfo);

        /* Start up secondary in unknown state */
        secondaryInfo.getRepConfig()
            .setConfigParam(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT,
                            "200 ms");
        secondaryInfo.openEnv();

        /* Read from secondary */
        readDB(secondaryInfo.getEnv(), 100);
    }

    /** Convert between ELECTABLE and SECONDARY node types. */
    @Test
    public void testConvertNodeType()
        throws Exception {

        createGroup();

        /* Convert ELECTABLE to SECONDARY */
        repEnvInfo[1].closeEnv();
        repEnvInfo[1].getRepConfig().setNodeType(NodeType.SECONDARY);
        try {
            repEnvInfo[1].openEnv();
            fail("Convert ELECTABLE to SECONDARY should throw" +
                 " EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Convert ELECTABLE to SECONDARY: " + e);
        }

        /* Convert SECONDARY to ELECTABLE. */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();
        secondaryInfo.closeEnv();
        secondaryInfo.getRepConfig().setNodeType(NodeType.ELECTABLE);

        /* Should succeed */
        secondaryInfo.openEnv();
        final ReplicationGroup group = secondaryInfo.getEnv().getGroup();
        assertEquals("Group size",
                     repEnvInfo.length,
                     group.getElectableNodes().size());
    }

    /**
     * Test creating a secondary when not all replicas are up to the current
     * version, or they are offline.
     */
    @Test
    public void testCheckCompatible()
        throws Exception {

        /* Start replicas using old version that doesn't support secondaries */
        final JEVersion rgV3 = RepGroupImpl.FORMAT_VERSION_3_JE_VERSION;
        final JEVersion oldVersion =
            new JEVersion(String.format("%d.%d.%d",
                                        rgV3.getMajor(),
                                        rgV3.getMinor(),
                                        rgV3.getPatch() - 1));
        final RepEnvInfo secondary;
        setJEVersion(oldVersion, repEnvInfo);

        createGroup();

        /* Add secondary configuration */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        secondary = repEnvInfo[repEnvInfo.length - 1];
        secondary.getRepConfig().setNodeType(NodeType.SECONDARY);

        /* Attempt to create with all nodes up */
        try {
            secondary.openEnv();
            fail("Create secondary before upgrade should fail with" +
                 " EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Create secondary before upgrade: " + e);
        }

        /*
         * Shutdown nodes, and bring up a quorum, which should upgrade to the
         * latest version, but need all nodes upgraded for this to succeed.
         */
        closeNodes(repEnvInfo);

        setJEVersion(rgV3, repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1]);
        try {
            secondary.openEnv();
            fail("Create secondary after partial upgrade should fail with" +
                 " EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Create secondary after partial upgrade: " + e);
        }

        /* Bring up the remaining node */
        restartNodes(repEnvInfo[2]);

        /* Bring up the secondary */
        setJEVersion(rgV3, secondary);
        secondary.openEnv();
    }

    /** Test that a secondary node can be a helper. */
    @Test
    public void testHelper()
        throws Exception {

        /* Create initial group */
        createGroup();

        /* Add a secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();

        /* Use the secondary as a helper when creating an electable node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo electableInfo = repEnvInfo[repEnvInfo.length - 1];
        electableInfo.openEnv(secondaryInfo);

        /* Use the secondary as a helper when creating another secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo2 = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo2.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo2.openEnv(secondaryInfo);
    }

    /**
     * Test that it is possible to add a secondary node to a replication group
     * that has lost quorum.
     */
    @Test
    public void testAddSecondaryWithNonAuthoritativeMaster()
        throws Exception {

        /* Create initial group */
        createGroup();

        /* Lose quorum */
        repEnvInfo[1].closeEnv();
        repEnvInfo[2].closeEnv();

        /* Add a secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();
    }

    /* Utilities */

    /** Set the JE version for the specified nodes. */
    private void setJEVersion(final JEVersion jeVersion,
                              final RepEnvInfo... nodes) {
        for (final RepEnvInfo node : nodes) {
            node.getRepConfig().setConfigParam(
                TEST_JE_VERSION.getName(), jeVersion.toString());
        }
    }
}
