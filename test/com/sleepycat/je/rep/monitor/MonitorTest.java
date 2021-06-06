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

import static com.sleepycat.je.rep.impl.RepParams.TEST_JE_VERSION;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepGroupProtocol;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

public class MonitorTest extends MonitorTestBase {

    /**
     * Test the direct API calls to get the master and group.
     */
    @Test
    public void testMonitorDirect()
        throws Exception {

        /* Add a secondary node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        repEnvInfo[repEnvInfo.length-1].getRepConfig().setNodeType(
            NodeType.SECONDARY);

        repEnvInfo[0].openEnv();
        RepNode master =
            RepInternal.getNonNullRepImpl(repEnvInfo[0].getEnv()).getRepNode();
        assertTrue(master.isMaster());
        String masterNodeName = master.getNodeName();
        assertEquals(masterNodeName, monitor.getMasterNodeName());

        for (int i = 1; i < repEnvInfo.length; i++) {
            repEnvInfo[i].openEnv();
            Thread.sleep(1000);
            assertEquals(masterNodeName, monitor.getMasterNodeName());
            ReplicationGroup group = monitor.getGroup();
            assertEquals(master.getGroup(),
                         RepInternal.getRepGroupImpl(group));
        }
        repEnvInfo[0].closeEnv();
        /* Wait for elections to settle down. */
        Thread.sleep(10000);
        assert(!masterNodeName.equals(monitor.getMasterNodeName()));
    }

    /**
     * Make sure code snippet in class javadoc compiles.
     * @throws IOException
     * @throws DatabaseException
     */
    @SuppressWarnings("hiding")
    @Test
    public void testJavadoc()
        throws DatabaseException, IOException {

        // Initialize the monitor node config
        try {
            MonitorConfig monConfig = new MonitorConfig();
            monConfig.setGroupName("PlanetaryRepGroup");
            monConfig.setNodeName("mon1");
            monConfig.setNodeHostPort("monhost1.acme.com:7000");
            monConfig.setHelperHosts("mars.acme.com:5000,jupiter.acme.com:5000");
            Monitor monitor = new Monitor(monConfig);

            // If the monitor has not been registered as a member of the group,
            // register it now. register() returns the current node that is the
            // master.

            @SuppressWarnings("unused")
            ReplicationNode currentMaster = monitor.register();

            // Start up the listener, so that it can be used to track changes
            // in the master node, or group composition.
            monitor.startListener(new MyChangeListener());
        } catch (IllegalArgumentException expected) {
        }
    }

    /* For javadoc and GSG */
    class MyChangeListener implements MonitorChangeListener {

        public void notify(NewMasterEvent newMasterEvent) {

            String newNodeName = newMasterEvent.getNodeName();

            InetSocketAddress newMasterAddr =
                newMasterEvent.getSocketAddress();
            String newMasterHostName = newMasterAddr.getHostName();
            int newMasterPort = newMasterAddr.getPort();

            // Do something with this information here.
        }

        public void notify(GroupChangeEvent groupChangeEvent) {
            ReplicationGroup repGroup = groupChangeEvent.getRepGroup();

            // Do something with the new ReplicationGroup composition here.
        }

        public void notify(JoinGroupEvent joinGroupEvent) {
        }

        public void notify(LeaveGroupEvent leaveGroupEvent) {
        }
    }

    /** Convert between ELECTABLE and MONITOR node types. */
    @Test
    public void testConvertNodeType()
        throws Exception {

        createGroup();

        /* Convert ELECTABLE to MONITOR */
        repEnvInfo[1].closeEnv();
        final ReplicationConfig repConfig = repEnvInfo[1].getRepConfig();
        repConfig.setNodeType(NodeType.MONITOR);
        try {
            repEnvInfo[1].openEnv();
            fail("Convert ELECTABLE to MONITOR should throw" +
                 " EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Convert ELECTABLE to MONITOR: " + e);
        }

        /* Convert ELECTABLE to Monitor. */
        final Properties accessProps = RepTestUtils.readNetProps();
        final Monitor monitor2 = new Monitor(
            new MonitorConfig()
            .setGroupName(repConfig.getGroupName())
            .setNodeName(repConfig.getNodeName())
            .setNodeHostPort(repConfig.getNodeHostPort())
            .setHelperHosts(repConfig.getHelperHosts())
            .setRepNetConfig(ReplicationNetworkConfig.create(accessProps)));
        try {
            monitor2.register();
            fail("Convert ELECTABLE to Monitor should throw" +
                 " EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Convert ELECTABLE to Monitor: " + e);
        }

        /* Convert Monitor to ELECTABLE. */
        monitor.register();
        monitor.shutdown();
        final InetSocketAddress monitorAddress =
            monitor.getMonitorSocketAddress();
        repConfig.setNodeName(monitor.getNodeName())
            .setNodeHostPort(monitorAddress.getHostName() + ":" +
                             monitorAddress.getPort())
            .setNodeType(NodeType.ELECTABLE);
        try {
            repEnvInfo[1].openEnv();
            fail("Convert Monitor to ELECTABLE should throw" +
                 " EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Convert Monitor to ELECTABLE: " + e);
        }
    }

    /**
     * Attempt to connect an old monitor that doesn't understand secondary
     * nodes to a group containing a secondary node to check that the version
     * negotiation happens correctly.
     */
    @Test
    public void testOldMonitorSecondary()
        throws Exception {

        /* Add a secondary node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        repEnvInfo[repEnvInfo.length-1].getRepConfig().setNodeType(
            NodeType.SECONDARY);

        createGroup();
        monitor.shutdown();

        /* Start a monitor using RepGroupImpl version 2 */
        final ReplicationConfig repConfig = repEnvInfo[0].getRepConfig();
        final int monitorPort = Integer.parseInt(
            RepParams.DEFAULT_PORT.getDefault()) + 200;
        RepGroupProtocol.setTestVersion(RepGroupProtocol.REP_GROUP_V2_VERSION);
        final Properties accessProps = RepTestUtils.readNetProps();
        try {
            monitor = new Monitor(
                new MonitorConfig()
                .setGroupName(repConfig.getGroupName())
                .setNodeName("oldMonitor")
                .setNodeHostPort(RepTestUtils.TEST_HOST + ":" + monitorPort)
                .setHelperHosts(repConfig.getHelperHosts())
                .setRepNetConfig(ReplicationNetworkConfig.create(accessProps)));

            monitor.register();
            final ReplicationGroup group = monitor.getGroup();

            assertEquals("No secondary nodes expected",
                         0, group.getSecondaryNodes().size());

        } finally {
            RepGroupProtocol.setTestVersion(null);
        }
    }

    /**
     * Attempt to connect a new monitor to a cluster that doesn't understand
     * secondary nodes, to make sure that the version negotiation happens
     * correctly.
     */
    @Test
    public void testNewMonitor()
        throws Exception {

        /*
         * Use RepGroupImpl version 2, which doesn't understand secondary
         * nodes.
         */
        for (final RepEnvInfo node : repEnvInfo) {
            node.getRepConfig().setConfigParam(
                TEST_JE_VERSION.getName(),
                RepGroupImpl.MIN_FORMAT_VERSION_JE_VERSION.toString());
        }

        createGroup();

        monitor.register();
        final ReplicationGroup group = monitor.getGroup();

        assertEquals("No secondary nodes expected",
                     0, group.getSecondaryNodes().size());
    }
}
