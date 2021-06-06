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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.junit.JUnitProcessThread;
import com.sleepycat.je.rep.AppStateMonitor;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.utilint.StringUtils;

/**
 * Test behaviors of the underlying Ping thread.
 */
public class PingCommandTest extends MonitorTestBase {
    private static final String TEST_STRING = "Hello, Ping Command!";

    /* Test those missed ADD GroupChangeEvent are fired. */
    @Test
    public void testMissedAddGroupChangeEvents()
        throws Exception {

        repEnvInfo[0].openEnv();

        TestChangeListener listener = new TestChangeListener();
        /* Disable notifying any MonitorChangeEvents. */
        monitor.disableNotify(true);
        monitor.startListener(listener);
        monitor.register();

        /* Make sure no JoinGroupEvents are fired because master is up. */
        assertEquals(0, listener.getGroupAddEvents());
        assertEquals(0, listener.getJoinGroupEvents());

        /*
         * Await from here in case node 5 fires immediately after enabling
         * events notification.  Note that there will be only 4 ADD events
         * because the listener will notice that node 1 is already present when
         * it is created.
         */
        listener.groupBarrier = new CountDownLatch(4);
        listener.joinGroupBarrier = new CountDownLatch(5);

        repEnvInfo[1].openEnv();
        repEnvInfo[2].openEnv();
        repEnvInfo[3].openEnv();
        repEnvInfo[4].openEnv();

        /* Sleep a while to make sure no Events fired. */
        Thread.sleep(10000);

        assertEquals(0, listener.getGroupAddEvents());
        assertEquals(0, listener.getJoinGroupEvents());

        /* Enable notification. */
        monitor.disableNotify(false);

        listener.awaitEvent(listener.groupBarrier);
        assertEquals(4, listener.getGroupAddEvents());

        listener.awaitEvent(listener.joinGroupBarrier);
        assertEquals(5, listener.getJoinGroupEvents());
    }

    /* Test those missed Remove GroupChangeEvents are fired. */
    @Test
    public void testMissedRemoveGroupChangeEvents()
        throws Exception {

        repEnvInfo[0].openEnv();
        RepNode master = repEnvInfo[0].getRepNode();

        TestChangeListener listener = new TestChangeListener();
        listener.joinGroupBarrier = new CountDownLatch(1);
        listener.groupBarrier = new CountDownLatch(1);
        monitor.startListener(listener);
        monitor.register();

        /* Check a JoinGroupEvent is fired. */
        listener.awaitEvent(listener.joinGroupBarrier);
        listener.awaitEvent(listener.groupBarrier);
        assertEquals(1, listener.getJoinGroupEvents());
        assertEquals(1, listener.getGroupAddEvents());

        listener.joinGroupBarrier = new CountDownLatch(4);
        listener.groupBarrier = new CountDownLatch(4);

        /* Open the rest four replicas. */
        repEnvInfo[1].openEnv();
        repEnvInfo[2].openEnv();
        repEnvInfo[3].openEnv();
        repEnvInfo[4].openEnv();

        listener.awaitEvent(listener.joinGroupBarrier);
        listener.awaitEvent(listener.groupBarrier);
        /* 5 JoinGroupEvents for 5 replicas. */
        assertEquals(5, listener.getJoinGroupEvents());

        /*
         * 5 ADD GroupChangeEvents for the 5 replicas, note that the monitor
         * does not belong to the Electable nodes set, so we don't fire missed
         * ADD GroupChangeEvent for it.
         */
        assertEquals(5, listener.getGroupAddEvents());

        /* Disable the MonitorChangeEvents notification and remove nodes. */
        monitor.disableNotify(true);

        for (int i = 1; i < repEnvInfo.length; i++) {
            master.removeMember(repEnvInfo[i].getRepNode().getNodeName());
        }

        assertEquals(0, listener.getGroupRemoveEvents());

        /*
         * Enable notification and make sure there are 4 missed REMOVE
         * GroupChangeEvents.
         */
        listener.groupBarrier = new CountDownLatch(4);
        monitor.disableNotify(false);
        listener.awaitEvent(listener.groupBarrier);
        assertEquals(4, listener.getGroupRemoveEvents());
    }

    /**
     * Test those missed JoinGroupEvent can be fired through ping thread.
     */
    @Test
    public void testMissedJoinGroupEvents()
        throws Exception {

        RepTestUtils.joinGroup(repEnvInfo);

        TestChangeListener listener = new TestChangeListener();
        /* Disable notifying JoinGroupEvents. */
        monitor.disableNotify(true);
        monitor.startListener(listener);
        monitor.register();

        /* Expect no JoinGroupEvents fired. */
        assertEquals(0, listener.getJoinGroupEvents());

        listener.joinGroupBarrier = new CountDownLatch(repEnvInfo.length);
        /* Enable notifying JoinGroupEvents. */
        monitor.disableNotify(false);
        listener.awaitEvent(listener.joinGroupBarrier);
        /* Make sure all missed JoinGroupEvents are fired. */
        assertEquals(groupSize, listener.getJoinGroupEvents());
    }

    /**
     * Before Ping thread is enabled, abnormal close won't fire a
     * LeaveGroupEvent, now they should be fired by Ping thread.
     */
    @Test
    public void testAbnormalClose()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        final String masterName = master.getRepConfig().getNodeName();

        TestChangeListener listener = new TestChangeListener();
        monitor.startListener(listener);
        monitor.register();
        assertEquals(groupSize, listener.getJoinGroupEvents());

        /* Abnormally shutting down nodes one by one. */
        for (int i = repEnvInfo.length - 1; i >= 0; i--) {
            listener.leaveGroupBarrier = new CountDownLatch(1);
            final String nodeName = repEnvInfo[i].getRepConfig().getNodeName();
            repEnvInfo[i].abnormalCloseEnv();
            listener.awaitEvent(listener.leaveGroupBarrier);
            assertEquals(LeaveReason.ABNORMAL_TERMINATION,
                         listener.leaveEvent.getLeaveReason());
            assertEquals(nodeName, listener.leaveEvent.getNodeName());
            assertEquals(masterName, listener.leaveEvent.getMasterName());
        }

        /* Expect all LeaveGroupEvents are fired as we expected. */
        assertEquals(groupSize, listener.getLeaveGroupEvents());
    }

    /**
     * Test those two new utility methods added in ReplicationGroupAdmin.
     */
    @Test
    public void testReplicationGroupAdmin()
        throws Exception {

        testNewUtility(new TestAppStateMonitor());

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
        RepTestUtils.removeRepEnvironments(envRoot);
        monitor.shutdown();

        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, groupSize);
        monitor = createMonitor(100, "mon10000");
        testNewUtility(new NullAppStateMonitor());
    }

    /*
     * Test the new utility provided in ReplicationGroupAdmin with different
     * AppStateMonitor implementations.
     */
    private void testNewUtility(AppStateMonitor stateMonitor)
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        TestChangeListener listener = new TestChangeListener();
        monitor.startListener(listener);
        monitor.register();
        assertEquals(groupSize, listener.getJoinGroupEvents());

        for (int i = 0; i < groupSize; i++) {
            repEnvInfo[i].getEnv().
                registerAppStateMonitor(stateMonitor);
        }

        ReplicationConfig masterConfig = master.getRepConfig();
        HashSet<InetSocketAddress> addresses =
            new HashSet<InetSocketAddress>();
        addresses.add(masterConfig.getNodeSocketAddress());
        ReplicationGroupAdmin groupAdmin = new ReplicationGroupAdmin
            (RepTestUtils.TEST_REP_GROUP_NAME, addresses,
             RepTestUtils.readRepNetConfig());

        for (int i = 0; i < groupSize; i++) {
            ReplicationConfig repConfig = repEnvInfo[i].getRepConfig();
            NodeState state =
                groupAdmin.getNodeState(new RepNodeImpl(repConfig), 10000);
            checkNodeState(state, i, stateMonitor);
        }

        for (int i = groupSize - 1; i >= 3; i--) {
            ReplicationConfig config = repEnvInfo[i].getRepConfig();
            repEnvInfo[i].closeEnv();

            try {
                groupAdmin.getNodeState(new RepNodeImpl(config), 10000);
                fail("Expected exceptions here.");
            } catch (IOException e) {
                /* Expected exception. */
            } catch (ServiceConnectFailedException e) {
                /* Expected exception. */
            }
        }
    }

    /* Check to see whether the node state is correct. */
    private void checkNodeState(NodeState state,
                                int index,
                                AppStateMonitor stateMonitor)
        throws Exception {

        if (index != 0) {
            assertTrue(state.getNodeState() == State.REPLICA);
        } else {
            assertTrue(state.getNodeState() == State.MASTER);
        }

        if (stateMonitor instanceof TestAppStateMonitor) {
            assertTrue(StringUtils.fromUTF8(state.getAppState()).
                       equals(TEST_STRING));
        } else {
            assertTrue(state.getAppState() == null);
        }
    }

    /**
     * Test whether the Ping thread can notify a LeaveGroupEvent for a suddenly
     * exited node.
     *
     * The test would do following steps:
     *   1. Start a 5 nodes group.
     *   2. Start a process which will add a new node to the group.
     *   3. Exit the process so that the new node is unachievable.
     *   4. The Ping thread should detect there is an abnormally shutdown node.
     */
    @Test
    public void testKillReplicas()
        throws Throwable {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        TestChangeListener listener = new TestChangeListener();
        monitor.startListener(listener);
        monitor.register();
        assertEquals(groupSize, listener.getJoinGroupEvents());

        /*
         * Create the environment home for the new replica, and start it in a
         * new process.
         */
        File envHome = RepTestUtils.makeRepEnvDir(envRoot, (groupSize + 1));
        final String nodeName = "replica6";
        String[] processCommands = new String[3];
        processCommands[0] =
            "com.sleepycat.je.rep.monitor.PingCommandTest$NodeProcess";
        processCommands[1] = envHome.getAbsolutePath();
        processCommands[2] = nodeName;

        assertEquals(groupSize, listener.getJoinGroupEvents());

        /* Start a new process. */
        listener.joinGroupBarrier = new CountDownLatch(1);
        listener.leaveGroupBarrier = new CountDownLatch(1);
        JUnitProcessThread process =
            new JUnitProcessThread(nodeName, processCommands);
        process.start();
        listener.awaitEvent(listener.joinGroupBarrier);
        listener.awaitEvent(listener.leaveGroupBarrier);
        process.finishTest();
        assertEquals(process.getExitVal(), 2);

        /*
         * Expect to detect groupSize + 1 JoinGroupEvents, which means the
         * replica in the process successfully joins the group.
         */
        assertEquals(groupSize + 1, listener.getJoinGroupEvents());

        /* Expect to detect an abnormally LeaveGroupEvent. */
        assertEquals(1, listener.getLeaveGroupEvents());
        /* Make sure the LeaveGroupEvent is notified by replica6. */
        assertEquals(LeaveReason.ABNORMAL_TERMINATION,
                     listener.leaveEvent.getLeaveReason());
        assertEquals(nodeName, listener.leaveEvent.getNodeName());
        assertEquals(master.getRepConfig().getNodeName(),
                     listener.leaveEvent.getMasterName());

        /* Delete the environment home for the process. */
        if (envHome.exists()) {
            for (File file : envHome.listFiles()) {
                file.delete();
            }
            envHome.delete();
        }
    }

    /* A class implements the AppStateMonitor used in the test. */
    private class TestAppStateMonitor implements AppStateMonitor {
        public byte[] getAppState() {
            return StringUtils.toUTF8(TEST_STRING);
        }
    }

    /* A class which returns a null application state. */
    private class NullAppStateMonitor implements AppStateMonitor {
        public byte[] getAppState() {
            return null;
        }
    }

    /* A process starts a node and exits itself without closing the replica. */
    static class NodeProcess {
        private final File envRoot;
        private final String nodeName;

        public NodeProcess(File envRoot, String nodeName) {
            this.envRoot = envRoot;
            this.nodeName = nodeName;
        }

        public void run()
            throws Exception {

            ReplicationConfig repConfig = RepTestUtils.createRepConfig(6);
            repConfig.setHelperHosts("localhost:5001");
            repConfig.setNodeName(nodeName);
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            ReplicatedEnvironment replica =
                new ReplicatedEnvironment(envRoot, repConfig, envConfig);
            assertTrue(replica.getState().isReplica());

            /* Sleep a while. */
            Thread.sleep(10000);

            /* Crash the replica. */
            System.exit(2);
        }

        public static void main(String args[])
            throws Exception {

            NodeProcess process = new NodeProcess(new File(args[0]), args[1]);
            process.run();
        }
    }
}
