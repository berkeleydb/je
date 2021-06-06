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

package com.sleepycat.je.rep.impl.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static com.sleepycat.je.rep.impl.RepParams.TEST_JE_VERSION;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.impl.MinJEVersionUnsupportedException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.stream.Protocol.FeederJEVersions;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.TestAction;
import com.sleepycat.je.utilint.WaitTestHook;

/** Test the RepNode.checkJEVersionSupport method. */
public class UpdateJEVersionTest extends RepTestBase {

    private static final JEVersion RG_V3 = JEVersion.CURRENT_VERSION;
    private static final JEVersion RG_V2 =
        new JEVersion(
            String.format(
                "%d.%d.%d",
                RepGroupImpl.FORMAT_VERSION_3_JE_VERSION.getMajor(),
                RepGroupImpl.FORMAT_VERSION_3_JE_VERSION.getMinor(),
                RepGroupImpl.FORMAT_VERSION_3_JE_VERSION.getPatch() - 1));
    private static final JEVersion RG_V3PLUS =
        new JEVersion(String.format("%d.%d.%d",
                                    RG_V3.getMajor(),
                                    RG_V3.getMinor(),
                                    RG_V3.getPatch() + 1));

    /**
     * Test that the RepGroupImpl object is stored in version 3 format by
     * default.
     */
    @Test
    public void testRgv2Compatible()
        throws Exception {

        createGroup(3);
        final RepGroupImpl group = getMasterRepNode().getGroup();
        assertEquals(RepGroupImpl.FORMAT_VERSION_3, group.getFormatVersion());
    }

    @Test
    public void testUpdateJEVersion()
        throws Exception {

        /*
         * Create the group with the JE version for RepGroupImpl version 2,
         * which does not store replica JE version information persistently.
         */
        setJEVersion(RG_V2, repEnvInfo);
        createGroup(3);

        /* Test that RepGroupImpl version 2 is supported */
        RepNode masterRepNode = getMasterRepNode();
        masterRepNode.setMinJEVersion(RG_V2);

        /*
         * Test that the JE version for RepGroupImpl version 3, which is the
         * version that stores replica JE version information, is not
         * supported.
         */
        masterRepNode = getMasterRepNode();
        try {
            masterRepNode.setMinJEVersion(RG_V3);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Not upgraded to " + RG_V3 + ": " + e);
        }

        /*
         * Restart just a quorum of the group using RepGroupImpl version 3, and
         * test that version 3 is still not supported because the status of the
         * offline node is not known.
         */
        closeNodes(repEnvInfo);
        setJEVersion(RG_V3, repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1]);
        masterRepNode = getMasterRepNode();
        try {
            masterRepNode.setMinJEVersion(RG_V3);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Partially upgraded to " + RG_V3 + ": " + e);
        }

        /*
         * Restart the last node, and make sure the new version is now
         * supported
         */
        restartNodes(repEnvInfo[2]);
        masterRepNode = getMasterRepNode();
        masterRepNode.setMinJEVersion(RG_V3);

        /*
         * Make sure the RepGroupImpl version 3 changes are stored.  Sync first
         * to make sure the rep group format update sticks, and then restart
         * and sync to get the persistent data for the nodes to be updated.
         */
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(RG_V3, repEnvInfo[i].getRepNode().getMinJEVersion());
        }
        closeNodes(repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 3);

        /*
         * Restart a quorum with a still newer version and make sure the
         * changes were stored persistently for the offline node.
         */
        closeNodes(repEnvInfo);
        setJEVersion(RG_V3PLUS, repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1]);
        RepTestUtils.syncGroupToLastCommit(
            new RepEnvInfo[] { repEnvInfo[0], repEnvInfo[1] }, 2);
        masterRepNode = getMasterRepNode();
        try {
            masterRepNode.setMinJEVersion(RG_V3PLUS);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Partially upgraded to " + RG_V3PLUS + ": " + e);
            assertEquals("Node version not upgraded", RG_V3, e.nodeVersion);
        }

        /*
         * Restart a different quorum and add a secondary node running the
         * older version.  Since the changes were sync'ed, the fact that node 2
         * was upgraded should be known even though that node is currently
         * offline, but the secondary node should still prevent the upgrade.
         * Testing the version with only a subset of the nodes online should
         * also confirm that attempting to update the latest JE version during
         * the handshake does not require a quorum, which will not be available
         * because the replica will not yet be online at that point.
         */
        closeNodes(repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[2]);
        setJEVersion(RG_V3, repEnvInfo[3]);
        repEnvInfo[3].getRepConfig().setNodeType(NodeType.SECONDARY);

        /*
         * Use a longer join wait time to allow the secondary to query the
         * primaries a second time after the election is complete.  See
         * RepNode.MASTER_QUERY_INTERVAL.
         */
        final long masterQueryInterval = 10000;
        restartNodes(JOIN_WAIT_TIME + masterQueryInterval, repEnvInfo[3]);
        masterRepNode = getMasterRepNode();
        final String secondaryNodeName = repEnvInfo[3].getEnv().getNodeName();
        try {
            masterRepNode.setMinJEVersion(RG_V3PLUS);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Partially upgraded to " + RG_V3PLUS + ": " + e);
            assertEquals("Wrong node", secondaryNodeName, e.nodeName);
        }

        /* Close the secondary to enable new version */
        closeNodes(repEnvInfo[3]);
        masterRepNode.setMinJEVersion(RG_V3PLUS);

        /* Restart node 2, just for completeness */
        restartNodes(repEnvInfo[1]);
        masterRepNode = getMasterRepNode();
        masterRepNode.setMinJEVersion(RG_V3PLUS);

        /*
         * Switch back to RepGroupImpl version 3, create a new node, and make
         * sure that it cannot join.
         */
        setJEVersion(RG_V3, repEnvInfo);
        try {
            repEnvInfo[4].openEnv();
            fail("Expected EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Node with old version can't join: " + e);
        }
    }

    /**
     * Test that calling RepNode.setMinJEVersion on a replica will throw
     * DatabaseException if it attempts to update the rep group DB.
     */
    @Test
    public void testUpdateJEVersionReplica()
        throws Exception {

        /*
         * Create the group with the JE version for RepGroupImpl version 3,
         * which stores replica JE version information persistently.
         */
        createGroup(3);
        RepNode masterRepNode = getMasterRepNode();
        masterRepNode.setMinJEVersion(RG_V3);

        /* Restart all nodes using a newer version */
        closeNodes(repEnvInfo);
        setJEVersion(RG_V3PLUS, repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 3);
        masterRepNode = getMasterRepNode();

        /* Test that updating the minimum JE version on a replica fails */
        for (int i = 0; i < 3; i++) {
            final RepNode repNode = repEnvInfo[i].getRepNode();
            if (repNode ==  masterRepNode) {
                continue;
            }
            try {
                repNode.setMinJEVersion(RG_V3PLUS);
                fail("Expected DatabaseException");
            } catch (DatabaseException e) {
                logger.info("No update on replica: " + e);
            }
        }
    }

    @Test
    public void testUpdateJEVersionDowngrade()
        throws Exception {

        /*
         * Create the group with the JE version for RepGroupImpl version 3,
         * which stores replica JE version information persistently.
         */
        createGroup(3);
        RepNode masterRepNode = getMasterRepNode();
        masterRepNode.setMinJEVersion(RG_V3);

        /*
         * Restart two nodes using a newer version and test that the group does
         * not support that version.
         */
        closeNodes(repEnvInfo);
        setJEVersion(RG_V3PLUS, repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1]);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 2);
        masterRepNode = getMasterRepNode();
        try {
            masterRepNode.setMinJEVersion(RG_V3PLUS);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Partially upgraded to " + RG_V3PLUS + ": " + e);
            assertEquals("Node version not upgraded", RG_V3, e.nodeVersion);
        }

        /* Revert to the original version. */
        closeNodes(repEnvInfo[0], repEnvInfo[1]);
        setJEVersion(RG_V3, repEnvInfo);
        restartNodes(repEnvInfo[0], repEnvInfo[1]);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 2);
        masterRepNode = getMasterRepNode();
        try {
            masterRepNode.setMinJEVersion(RG_V3PLUS);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Partially upgraded to " + RG_V3PLUS + ": " + e);
            assertEquals("Node version not upgraded", RG_V3, e.nodeVersion);
        }

        /*
         * Close all three nodes, and restart a different two using the new
         * version, confirming that the system remembers that other node was
         * downgraded.
         */
        closeNodes(repEnvInfo);
        setJEVersion(RG_V3PLUS, repEnvInfo);
        restartNodes(repEnvInfo[1], repEnvInfo[2]);
        RepTestUtils.syncGroupToLastCommit(
            new RepEnvInfo[] { repEnvInfo[1], repEnvInfo[2] }, 2);
        masterRepNode = getMasterRepNode();
        try {
            masterRepNode.setMinJEVersion(RG_V3PLUS);
            fail("Expected MinJEVersionUnsupportedException");
        } catch (MinJEVersionUnsupportedException e) {
            logger.info("Partially upgraded to " + RG_V3PLUS + ": " + e);
            assertEquals("Node version not upgraded", RG_V3, e.nodeVersion);
        }

        /* Restart first node and make sure new version is supported */
        restartNodes(repEnvInfo[0]);
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, 3);
        masterRepNode = getMasterRepNode();
        masterRepNode.setMinJEVersion(RG_V3PLUS);
    }

    /**
     * Test adding a secondary node running an older version just as the
     * cluster's minimum JE version is being increased to make that version
     * incompatible.  This test checks that the logic in
     * RepNode.setMinJEVersion performs the proper interlock for secondary
     * nodes joining the cluster while the minimum JE version is being
     * modified.
     */
    @Test
    public void testUpgradeOldSecondaryNodeJoinRace()
        throws Exception {

        /* Create a version 3 cluster with three members */
        setJEVersion(RG_V3, repEnvInfo);
        createGroup(3);

        /* Upgrade nodes to version 3+ */
        closeNodes(repEnvInfo);
        setJEVersion(RG_V3PLUS, repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);
        restartNodes(repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);

        final RepNode masterRepNode = getMasterRepNode();
        try {

            /*
             * Hook to wait before sending the FeederJEVersions reply message
             * during the secondary node handshake.
             */
            final WaitTestHook<Message> writeMessageHook =
                new WaitTestHook<Message>() {
                    @Override
                    public void doHook(final Message msg) {
                        if (msg instanceof FeederJEVersions) {
                            doHook();
                        }
                    }
                };
            Feeder.setInitialWriteMessageHook(writeMessageHook);

            /* Add a secondary node running the older version 3 */
            final TestAction addOldNode = new TestAction() {
                @Override
                protected void action() {
                    repEnvInfo[3].getRepConfig().setNodeType(
                        NodeType.SECONDARY);
                    repEnvInfo[3].openEnv();
                }
            };
            addOldNode.start();

            /*
             * Wait until the node's feeder completes checking JE version
             * compatibility and waits before continuing
             */
            writeMessageHook.awaitWaiting(10000);

            /* Increase the group's minimum JE version to 3+ */
            masterRepNode.setMinJEVersion(RG_V3PLUS);

            /*
             * Continue activating the new node, which is now incompatible and
             * should fail
             */
            writeMessageHook.stopWaiting();
            addOldNode.assertException(
                10000, EnvironmentFailureException.class);

        } finally {
            Feeder.setInitialWriteMessageHook(null);
        }
    }

    private RepNode getMasterRepNode() {
        return RepInternal.getNonNullRepImpl(
            findMaster(repEnvInfo).getEnv()).getRepNode();
    }

    /** Set the JE version for the specified nodes. */
    private void setJEVersion(final JEVersion jeVersion,
                              final RepEnvInfo... nodes) {
        assert jeVersion != null;
        for (final RepEnvInfo node : nodes) {
            node.getRepConfig().setConfigParam(
                TEST_JE_VERSION.getName(), jeVersion.toString());
        }
    }
}
