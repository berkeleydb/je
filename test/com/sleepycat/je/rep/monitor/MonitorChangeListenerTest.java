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

package com.sleepycat.je.rep.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Test the MonitorChangeListener behaviors.
 */
public class MonitorChangeListenerTest extends MonitorTestBase {
    private TestChangeListener testListener;

    /**
     * Test basic behaviors of MonitorChangeListener.
     */
    @Test
    public void testBasicBehaviors()
        throws Exception {

        checkGroupStart();

        /*
         * Close the master, expect to see a NewMasterEvent and a
         * LeaveGroupEvent.
         */
        testListener.masterBarrier = new CountDownLatch(1);
        testListener.leaveGroupBarrier = new CountDownLatch(1);
        repEnvInfo[0].closeEnv();
        /* Wait for a LeaveGroupEvent. */
        testListener.awaitEvent(testListener.leaveGroupBarrier);
        /* Wait for elections to settle down. */
        testListener.awaitEvent(testListener.masterBarrier);

        /* Do the check. */
        assertEquals(1, testListener.getMasterEvents());
        assertEquals(1, testListener.getLeaveGroupEvents());
        assertTrue(!repEnvInfo[0].getRepConfig().getNodeName().equals
                   (testListener.masterNodeName));

        /*
         * Shutdown all the replicas, see if it generates the expected number
         * of LeaveGroupEvents.
         */
        testListener.clearLeaveGroupEvents();
        shutdownReplicasNormally();
    }

    /* Check the monitor events during the group start up. */
    private void checkGroupStart()
        throws Exception {

        repEnvInfo[0].openEnv();
        RepNode master = repEnvInfo[0].getRepNode();
        assertTrue(master.isMaster());

        testListener = new TestChangeListener();
        testListener.masterBarrier = new CountDownLatch(1);
        testListener.groupBarrier = new CountDownLatch(1);

        /*
         * Start the listener first, so the Listener is guaranteed to get
         * the group change event when the monitor is registered.
         */

        /* generates sync master change event */
        monitor.startListener(testListener);
        /* generates async group change event */
        monitor.register();
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Make sure it fires a NewMasterEvent, and do the check. */
        testListener.awaitEvent(testListener.masterBarrier);
        assertEquals(1, testListener.getMasterEvents());
        NewMasterEvent masterEvent = testListener.masterEvent;
        assertEquals(masterEvent.getNodeName(), master.getNodeName());
        assertEquals(masterEvent.getMasterName(), master.getNodeName());

        /* Adding a monitor fires an ADD GroupChangeEvent, do check. */
        testListener.awaitEvent(testListener.groupBarrier);
        assertEquals(1, testListener.getGroupAddEvents());
        GroupChangeEvent groupEvent = testListener.groupEvent;
        assertEquals(monitor.getNodeName(), groupEvent.getNodeName());

        /* Get the JoinGroupEvents for current active node: master. */
        assertEquals(1, testListener.getJoinGroupEvents());
        JoinGroupEvent joinEvent = testListener.joinEvent;
        assertEquals(master.getNodeName(), joinEvent.getNodeName());
        assertEquals(master.getNodeName(), joinEvent.getMasterName());

        testListener.clearMasterEvents();
        testListener.clearJoinGroupEvents();
        testListener.clearGroupAddEvents();
        for (int i = 1; i < repEnvInfo.length; i++) {
            testListener.groupBarrier = new CountDownLatch(1);
            testListener.joinGroupBarrier = new CountDownLatch(1);
            repEnvInfo[i].openEnv();
            String nodeName = repEnvInfo[i].getEnv().getNodeName();
            testListener.awaitEvent(nodeName, testListener.groupBarrier);
            /* Wait for a JoinGroupEvent. */
            testListener.awaitEvent(nodeName, testListener.joinGroupBarrier);
            /* No change in master. */
            assertEquals(0, testListener.getMasterEvents());
            assertEquals(i, testListener.getGroupAddEvents());
            assertEquals(i, testListener.getJoinGroupEvents());

            /* Do the GroupChangeEvent check. */
            groupEvent = testListener.groupEvent;
            assertEquals(nodeName, groupEvent.getNodeName());
            assertEquals(groupEvent.getRepGroup().getNodes().size(), i + 2);

            /* Do the JoinGroupEvent check. */
            joinEvent = testListener.joinEvent;
            assertEquals(nodeName, joinEvent.getNodeName());
            assertEquals(master.getNodeName(), joinEvent.getMasterName());
        }
    }

    /*
     * Shutdown all the replicas normally, do not shutdown the master before
     * shutting down all replicas so that there is no NewMasterEvent
     * generated during this process.
     */
    private void shutdownReplicasNormally()
        throws Exception {

        RepEnvInfo master = null;
        int shutdownReplicas = 0;
        for (RepEnvInfo repInfo : repEnvInfo) {
            ReplicatedEnvironment env = repInfo.getEnv();
            if ((env == null) || !env.isValid()) {
                continue;
            }
            if (env.getState().isMaster()) {
                master = repInfo;
                continue;
            }
            shutdownReplicas++;
            shutdownReplicaAndDoCheck(repInfo, shutdownReplicas);
        }

        /* Shutdown the master. */
        if (master != null) {
            shutdownReplicas++;
            shutdownReplicaAndDoCheck(master, shutdownReplicas);
        }
    }

    /* Shutdown a replica and do the check. */
    private void shutdownReplicaAndDoCheck(RepEnvInfo replica,
                                           int index)
        throws Exception {

        testListener.leaveGroupBarrier = new CountDownLatch(1);
        String nodeName = replica.getEnv().getNodeName();
        replica.closeEnv();
        testListener.awaitEvent(nodeName, testListener.leaveGroupBarrier);

        /* Do the check. */
        LeaveGroupEvent event = testListener.leaveEvent;
        assertEquals(index, testListener.getLeaveGroupEvents());
        assertEquals(nodeName, event.getNodeName());

        checkShutdownReplicaLeaveReason(event);
    }

    void checkShutdownReplicaLeaveReason(final LeaveGroupEvent event) {
        assertEquals(LeaveReason.NORMAL_SHUTDOWN, event.getLeaveReason());
    }

    /**
     * Test removeMember which would create GroupChangeEvent, but no
     * LeaveGroupEvents.
     */
    @Test
    public void testRemoveMember()
        throws Exception {

        checkGroupStart();

        RepNode master = repEnvInfo[0].getRepNode();
        assertTrue(master.isMaster());

        /*
         * Remove replica from RepGroupDB, see if it fires a REMOVE
         * GroupChangeEvent.
         */
        testListener.clearGroupAddEvents();
        testListener.clearLeaveGroupEvents();
        for (int i = 1; i < repEnvInfo.length; i++) {
            testListener.groupBarrier = new CountDownLatch(1);
            String nodeName = repEnvInfo[i].getRepNode().getNodeName();
            master.removeMember(nodeName);
            testListener.awaitEvent(nodeName, testListener.groupBarrier);
            assertEquals(0, testListener.getGroupAddEvents());
            assertEquals(i, testListener.getGroupRemoveEvents());
            assertEquals(nodeName, testListener.groupEvent.getNodeName());
        }
        assertEquals(0, testListener.getLeaveGroupEvents());

        /*
         * Shutdown all the replicas, see if it generates the expected number
         * of LeaveGroupEvents.
         */
        shutdownReplicasNormally();
    }

    @Test
    public void testActiveNodesWhenMonitorStarts()
        throws Exception {

        RepTestUtils.joinGroup(repEnvInfo);
        testListener = new TestChangeListener();
        /* generates sync master change event */
        monitor.startListener(testListener);
        /* generates async group change event */
        monitor.register();
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        assertEquals(1, testListener.getMasterEvents());
        assertEquals(5, testListener.getJoinGroupEvents());
        JoinGroupEvent event = testListener.joinEvent;
        assertEquals
            (repEnvInfo[0].getEnv().getNodeName(), event.getMasterName());

        shutdownReplicasNormally();
    }

    /**
     * Test monitor events when adding a secondary node.
     */
    @Test
    public void testAddSecondaryNode()
        throws Exception {

        checkGroupStart();
        final RepNode master = repEnvInfo[0].getRepNode();
        assertTrue("Master", master.isMaster());

        testListener.clearGroupAddEvents();
        testListener.clearGroupRemoveEvents();
        testListener.clearJoinGroupEvents();
        testListener.clearLeaveGroupEvents();
        testListener.joinGroupBarrier = new CountDownLatch(1);

        /* Create a new secondary */
        final int pos = repEnvInfo.length;
        repEnvInfo = Arrays.copyOf(repEnvInfo, pos + 1);
        repEnvInfo[pos] = RepTestUtils.setupEnvInfo(
            RepTestUtils.makeRepEnvDir(envRoot, pos),
            RepTestUtils.createEnvConfig(RepTestUtils.DEFAULT_DURABILITY),
            RepTestUtils.createRepConfig(pos + 1).setNodeType(
                NodeType.SECONDARY),
            repEnvInfo[0]);
        repEnvInfo[pos].openEnv();
        final String nodeName = repEnvInfo[pos].getEnv().getNodeName();

        testListener.awaitEvent(testListener.joinGroupBarrier);
        assertEquals("Add events", 0, testListener.getGroupAddEvents());
        assertEquals("Remove events", 0, testListener.getGroupRemoveEvents());
        assertEquals("Join events", 1, testListener.getJoinGroupEvents());
        assertEquals("Join event node name",
                     nodeName, testListener.joinEvent.getNodeName());
        assertEquals("Leave events", 0, testListener.getLeaveGroupEvents());

        testListener.clearGroupAddEvents();
        testListener.clearGroupRemoveEvents();
        testListener.clearJoinGroupEvents();
        testListener.clearLeaveGroupEvents();
        testListener.leaveGroupBarrier = new CountDownLatch(1);

        /* Close secondary */
        repEnvInfo[pos].closeEnv();

        testListener.awaitEvent(testListener.leaveGroupBarrier);
        assertEquals("Add events", 0, testListener.getGroupAddEvents());
        assertEquals("Remove events", 0, testListener.getGroupRemoveEvents());
        assertEquals("Join events", 0, testListener.getJoinGroupEvents());
        assertEquals("Leave events", 1, testListener.getLeaveGroupEvents());
        assertEquals("Leave event node name",
                     nodeName, testListener.leaveEvent.getNodeName());
        testListener.clearLeaveGroupEvents();

        /* Shutdown */
        shutdownReplicasNormally();
    }

    /**
     * Test monitor events when replacing a primary replica with a secondary
     * node that reuses its environment.
     */
    @Test
    public void testReplaceAsSecondaryNode()
        throws Exception {

        checkGroupStart();
        final RepNode master = repEnvInfo[0].getRepNode();
        assertTrue("Master", master.isMaster());

        /* Remove replica */
        testListener.clearGroupAddEvents();
        testListener.clearGroupRemoveEvents();
        testListener.clearJoinGroupEvents();
        testListener.clearLeaveGroupEvents();
        testListener.groupBarrier = new CountDownLatch(1);
        testListener.leaveGroupBarrier = new CountDownLatch(1);
        master.removeMember(repEnvInfo[1].getRepNode().getNodeName());
        testListener.awaitEvent(testListener.groupBarrier);
        assertEquals("Add events", 0, testListener.getGroupAddEvents());
        assertEquals("Remove events", 1, testListener.getGroupRemoveEvents());
        assertEquals("Remove event node name",
                     repEnvInfo[1].getEnv().getNodeName(),
                     testListener.groupEvent.getNodeName());
        assertEquals("Join events", 0, testListener.getJoinGroupEvents());
        assertEquals("Leave events", 0, testListener.getLeaveGroupEvents());

        /* Close environment */
        String closedName = repEnvInfo[1].getEnv().getNodeName();
        repEnvInfo[1].closeEnv();
        testListener.awaitEvent(testListener.leaveGroupBarrier);
        assertEquals(closedName, testListener.leaveEvent.getNodeName());

        /* Open replica as a (new) secondary */
        testListener.clearGroupAddEvents();
        testListener.clearGroupRemoveEvents();
        testListener.clearJoinGroupEvents();
        testListener.clearLeaveGroupEvents();
        testListener.joinGroupBarrier = new CountDownLatch(1);
        repEnvInfo[1].getRepConfig()
            .setNodeName("Node2-secondary")
            .setNodeType(NodeType.SECONDARY);
        repEnvInfo[1].openEnv();
        testListener.awaitEvent(testListener.joinGroupBarrier);
        assertEquals("Add events", 0, testListener.getGroupAddEvents());
        assertEquals("Remove events", 0, testListener.getGroupRemoveEvents());
        assertEquals("Join events", 1, testListener.getJoinGroupEvents());
        assertEquals("Join event node name",
                     repEnvInfo[1].getEnv().getNodeName(),
                     testListener.joinEvent.getNodeName());
        assertEquals("Leave events", 0, testListener.getLeaveGroupEvents());

        /* Shutdown */
        shutdownReplicasNormally();
    }
}
