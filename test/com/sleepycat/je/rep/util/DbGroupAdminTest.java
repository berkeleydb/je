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

package com.sleepycat.je.rep.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.MasterTransferTest;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/*
 * A unit test which tests the DbGroupAdmin utility and also the utilities
 * provided by ReplicationGroupAdmin.
 */
public class DbGroupAdminTest extends TestBase {
    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;
    private final Logger logger;
    private boolean useRepNetConfig;

    public DbGroupAdminTest() {
        envRoot = SharedTestUtils.getTestDir();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
        ReplicationNetworkConfig repNetConfig = RepTestUtils.readRepNetConfig();
        useRepNetConfig = !repNetConfig.getChannelType().isEmpty();
    }

    @Override
    @After
    public void tearDown() {
        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    /*
     * Test the removeMember and deleteMember behavior of DbGroupAdmin, since
     * DbGroupAdmin invokes ReplicationGroupAdmin, so it tests
     * ReplicationGroupAdmin too.
     *
     * TODO: When the simple majority is configurable, need to test that a
     * group becomes electable again when some nodes are removed.
     */
    @Test
    public void testRemoveMember()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        /* Construct a DbGroupAdmin instance. */
        DbGroupAdmin dbAdmin =
            useRepNetConfig ?
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             RepTestUtils.readRepNetConfig()) :
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets());

        /* Removing the master will result in MasterStateException. */
        try {
            dbAdmin.removeMember(master.getNodeName());
            fail("Shouldn't execute here, expect an exception.");
        } catch (MasterStateException e) {
            /* Expected exception. */
        }
        try {
            dbAdmin.deleteMember(master.getNodeName());
            fail("Shouldn't execute here, expect an exception.");
        } catch (MasterStateException e) {
            /* Expected exception. */
        }

        /*
         * Removing a non-existent node will result in
         * MemberNotFoundException.
         */
        try {
            dbAdmin.removeMember("Unknown Node");
            fail("Removing a non-existent node should fail.");
        } catch (MemberNotFoundException e) {
            /* Expected exception. */
        }
        try {
            dbAdmin.deleteMember("Unknown Node");
            fail("Removing a non-existent node should fail.");
        } catch (MemberNotFoundException e) {
            /* Expected exception. */
        }

        /* Remove a monitor */
        RepNodeImpl monitorNode = openMonitor(master);
        dbAdmin.deleteMember(monitorNode.getName());
        monitorNode = openMonitor(master);
        dbAdmin.removeMember(monitorNode.getName());

        /* Attempt to remove a secondary */
        final RepEnvInfo secondaryInfo = openSecondaryNode();
        try {
            dbAdmin.removeMember(secondaryInfo.getEnv().getNodeName());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            logger.info("Remove secondary: " + e);
        }
        try {
            dbAdmin.deleteMember(secondaryInfo.getEnv().getNodeName());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            logger.info("Remove secondary: " + e);
        }

        final DataChannelFactory channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig());

        /* Check node state for secondary */
        final ReplicationGroupAdmin groupAdmin =
            new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                      master.getRepConfig().getHelperSockets(),
                                      channelFactory);

        final NodeState secondaryNodeState =
            groupAdmin.getNodeState(
                groupAdmin.getGroup().getMember(
                    secondaryInfo.getEnv().getNodeName()),
                secondaryInfo.getRepConfig().getNodePort());
        assertEquals(secondaryNodeState.getNodeState(), State.REPLICA);

        /*
         * Try removing the active Node 3 from the group using the deleteMember
         * API
         */
        try {
            dbAdmin.deleteMember(repEnvInfo[2].getEnv().getNodeName());
            fail("Expected EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            /* Should fail with active node */
        }

        /* Shut down Node 3 and try again */
        final String node3Name = repEnvInfo[2].getEnv().getNodeName();
        repEnvInfo[2].closeEnv();
        dbAdmin.deleteMember(node3Name);
        RepGroupImpl groupImpl = master.getGroup().getRepGroupImpl();
        assertEquals(groupImpl.getAllElectableMembers().size(), 2);

        /* Add Node 3 back, and then remove it using the removeMember API. */
        repEnvInfo[2].openEnv();
        try {
            dbAdmin.removeMember(repEnvInfo[2].getEnv().getNodeName());
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
        groupImpl = master.getGroup().getRepGroupImpl();
        assertEquals(groupImpl.getAllElectableMembers().size(), 2);

        /* Close and delete Node 2 from the group using main method. */
        final String node2Name = repEnvInfo[1].getEnv().getNodeName();
        repEnvInfo[1].closeEnv();
        try {
            String[] args;
            if (useRepNetConfig) {
                /*
                 * We don't actually use the repNetConfig here, but we
                 * do the equivalent through the -netProps argument.
                 */
                File propertyFile =
                    new File(repEnvInfo[0].getEnvHome().getPath(),
                             "je.properties");
                args = new String[] {
                    "-groupName", RepTestUtils.TEST_REP_GROUP_NAME,
                    "-helperHosts", master.getRepConfig().getNodeHostPort(),
                    "-netProps", propertyFile.getPath(),
                    "-deleteMember", node2Name };
            } else {
                args = new String[] {
                    "-groupName", RepTestUtils.TEST_REP_GROUP_NAME,
                    "-helperHosts", master.getRepConfig().getNodeHostPort(),
                    "-deleteMember", node2Name };
            }
            DbGroupAdmin.main(args);
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
        groupImpl = master.getGroup().getRepGroupImpl();
        assertEquals(groupImpl.getAllElectableMembers().size(), 1);

        /* Add Node 2 back and remove it using main method. */
        repEnvInfo[1].openEnv();
        try {
            String[] args;
            if (useRepNetConfig) {
                /*
                 * We don't actually use the repNetConfig here, but we
                 * do the equivalent through the -netProps argument.
                 */
                File propertyFile =
                    new File(repEnvInfo[0].getEnvHome().getPath(),
                             "je.properties");
                args = new String[] {
                    "-groupName", RepTestUtils.TEST_REP_GROUP_NAME,
                    "-helperHosts", master.getRepConfig().getNodeHostPort(),
                    "-netProps", propertyFile.getPath(),
                    "-removeMember", repEnvInfo[1].getEnv().getNodeName() };
            } else {
                args = new String[] {
                    "-groupName", RepTestUtils.TEST_REP_GROUP_NAME,
                    "-helperHosts", master.getRepConfig().getNodeHostPort(),
                    "-removeMember", repEnvInfo[1].getEnv().getNodeName() };
            }
            DbGroupAdmin.main(args);
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
        groupImpl = master.getGroup().getRepGroupImpl();
        assertEquals(groupImpl.getAllElectableMembers().size(), 1);
    }

    /**
     * Create a secondary node, adding it to the end of the repEnvInfo array,
     * and returning it.
     */
    private RepEnvInfo openSecondaryNode()
        throws IOException {

        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo info = repEnvInfo[repEnvInfo.length - 1];
        info.getRepConfig().setNodeType(NodeType.SECONDARY);
        info.openEnv();
        return info;
    }

    /**
     * Create a monitor node, using the node ID and port after the last one in
     * repEnvInfo, and return it.
     */
    private RepNodeImpl openMonitor(final ReplicatedEnvironment master) {
        final DataChannelFactory channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig());

        final ReplicationGroupAdmin repGroupAdmin =
            new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                      master.getRepConfig().getHelperSockets(),
                                      channelFactory);
        final int id = repEnvInfo.length + 1;
        final RepNodeImpl monitorNode =
            new RepNodeImpl(new NameIdPair("Monitor" + id, id),
                            NodeType.MONITOR,
                            "localhost",
                            5000 + id,
                            null);
        repGroupAdmin.ensureMonitor(monitorNode);
        return monitorNode;
    }

    /*
     * Test that mastership transfer behavior of DbGroupAdmin.
     * @see com.sleepycat.je.rep.impl.node.MasterTransferTest
     */
    @Test
    public void testMastershipTransfer()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(master.getState().isMaster());

        /* Construct a DbGroupAdmin instance. */
        DbGroupAdmin dbAdmin =
            useRepNetConfig ?
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             RepTestUtils.readRepNetConfig()) :
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets());

        /* Transfer master to a nonexistent replica. */
        try {
            dbAdmin.transferMaster("node 5", "5 s");
            fail("Shouldn't execute here, expect an exception");
        } catch (MemberNotFoundException e) {
            /* Expected exception. */
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
        assertTrue(master.getState().isMaster());

        /* Transfer master to a monitor */
        final RepNodeImpl monitorNode = openMonitor(master);
        try {
            dbAdmin.transferMaster(monitorNode.getName(), "1 s");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* Expected exception */
        } catch (Exception e) {
            /* Unexpected exception */
            throw e;
        }

        /* Transfer master to a secondary node */
        final RepEnvInfo secondaryInfo = openSecondaryNode();
        try {
            dbAdmin.transferMaster(secondaryInfo.getEnv().getNodeName(),
                                   "1 s");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            logger.info("Transfer master to secondary: " + e);
        }

        /* Transfer the mastership to node 1. */
        PrintStream original = System.out;
        try {
            /* Avoid polluting the test output. */
            System.setOut(new PrintStream(new ByteArrayOutputStream()));
            dbAdmin.transferMaster(repEnvInfo[1].getEnv().getNodeName(),
                                   "5 s");
            MasterTransferTest.awaitSettle(repEnvInfo[0], repEnvInfo[1]);
        } catch (Exception e) {
            fail("Unexpected exception: " + LoggerUtils.getStackTrace(e));
        } finally {
            System.setOut(original);
        }

        /* Check the node state. */
        assertTrue(repEnvInfo[0].isReplica());
        assertTrue(repEnvInfo[1].isMaster());

        /* Do some database operations to make sure everything is OK. */
        final String dbName = "testDB";
        final String dataString = "herococo";
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db =
            repEnvInfo[1].getEnv().openDatabase(null, dbName , dbConfig);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= 50; i++) {
            IntegerBinding.intToEntry(i, key);
            StringBinding.stringToEntry(dataString, data);
            assertTrue(OperationStatus.SUCCESS == db.put(null, key, data));
        }
        db.close();

        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        /* Check the old master is OK. */
        db = repEnvInfo[0].getEnv().openDatabase(null, dbName, dbConfig);
        for (int i = 1; i <= 50; i++) {
            IntegerBinding.intToEntry(i, key);
            assertTrue
                (OperationStatus.SUCCESS == db.get(null, key, data, null));
            assertTrue(StringBinding.entryToString(data).equals(dataString));
        }
        db.close();
    }

    /*
     * Test the update address utility of DbGroupAdmin. */
    @Test
    public void testUpdateAddress()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(master.getState().isMaster());

        /* Construct a DbGroupAdmin instance. */
        DbGroupAdmin dbAdmin =
            useRepNetConfig ?
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             RepTestUtils.readRepNetConfig()) :
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets());

        try {
            dbAdmin.updateAddress("node 5", "localhost", 5004);
            fail("Expected MemberNotFoundException");
        } catch (MemberNotFoundException e) {
            logger.info("Update address member not found: " + e);
        }
        assertTrue(master.getState().isMaster());

        try {
            dbAdmin.updateAddress(master.getNodeName(), "localhost", 5004);
            fail("Expected MasterStateException");
        } catch (MasterStateException e) {
            logger.info("Update address for master: " + e);
        }
        assertTrue(master.getState().isMaster());

        final String nodeName = repEnvInfo[1].getEnv().getNodeName();
        final File envHome = repEnvInfo[1].getEnvHome();
        final EnvironmentConfig envConfig = repEnvInfo[1].getEnvConfig();
        final ReplicationConfig repConfig = repEnvInfo[1].getRepConfig();
        try {
            dbAdmin.updateAddress(nodeName, "localhost", 5004);
            fail("Expected ReplicaStateException");
        } catch (ReplicaStateException e) {
            logger.info("Update address for live node: " + e);
        }
        assertTrue(master.getState().isMaster());
        assertTrue(repEnvInfo[1].isReplica());

        /* Attempt to update the address of a secondary */
        final RepEnvInfo secondaryInfo = openSecondaryNode();
        try {
            dbAdmin.updateAddress(secondaryInfo.getEnv().getNodeName(),
                                  "localhost", 5004);
            fail("Expected ReplicaStateException");
        } catch (ReplicaStateException e) {
            logger.info("Update address for secondary node: " + e);
        }

        /*
         * Update the address by shutting down the environment, and reopening
         * it with a different address.
         */
        secondaryInfo.closeEnv();
        secondaryInfo.getRepConfig().setNodeHostPort("localhost:5004");
        secondaryInfo.openEnv();

        /* Clean up */
        secondaryInfo.closeEnv();

        /* Shutdown the second replica. */
        repEnvInfo[1].closeEnv();
        try {
            dbAdmin.updateAddress(nodeName, "localhost", 5004);
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        /* Reopen the repEnvInfo[1] with updated configurations. */
        repConfig.setNodeHostPort("localhost:5004");
        ReplicatedEnvironment replica =
            new ReplicatedEnvironment(envHome, repConfig, envConfig);
        assertTrue(replica.getState().isReplica());
        assertEquals(replica.getRepConfig().getNodeHostname(),
                     "localhost");
        assertEquals(replica.getRepConfig().getNodePort(), 5004);
        replica.close();
    }

    /* Test behaviors of ReplicationGroupAdmin. */
    @Test
    public void testReplicationGroupAdmin()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        final DataChannelFactory channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig());

        ReplicationGroupAdmin groupAdmin = new ReplicationGroupAdmin
            (RepTestUtils.TEST_REP_GROUP_NAME,
             master.getRepConfig().getHelperSockets(),
             channelFactory);

        /* Test the DbPing utility. */
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);
        VLSN commitVLSN = repEnvInfo[0].getRepNode().getCurrentTxnEndVLSN();
        String groupName = groupAdmin.getGroupName();
        String masterName = groupAdmin.getMasterNodeName();

        Set<ReplicationNode> replicationNodes =
            groupAdmin.getGroup().getElectableNodes();
        for (ReplicationNode replicationNode : replicationNodes) {
            NodeState nodeState =
                groupAdmin.getNodeState(replicationNode, 10000);
            assertEquals(nodeState.getGroupName(), groupName);
            assertEquals(nodeState.getMasterName(), masterName);
            assertEquals(nodeState.getJEVersion(), JEVersion.CURRENT_VERSION);
            /* Check the values depends on the node state. */
            if (replicationNode.getName().equals(masterName)) {
                assertEquals(nodeState.getNodeState(), State.MASTER);
                assertEquals(nodeState.getActiveFeeders(),
                             repEnvInfo.length - 1);
                assertEquals(nodeState.getKnownMasterTxnEndVLSN(), 0);
            } else {
                assertEquals(nodeState.getNodeState(), State.REPLICA);
                assertEquals(nodeState.getActiveFeeders(), 0);
                assertEquals(nodeState.getKnownMasterTxnEndVLSN(),
                             commitVLSN.getSequence());
            }
            assertEquals(nodeState.getCurrentTxnEndVLSN(),
                         commitVLSN.getSequence());
            assertEquals(nodeState.getLogVersion(), LogEntryType.LOG_VERSION);
        }

        /* Check the master name. */
        assertEquals(master.getNodeName(), groupAdmin.getMasterNodeName());

        /* Check the group information. */
        RepGroupImpl repGroupImpl = master.getGroup().getRepGroupImpl();
        assertEquals(repGroupImpl, groupAdmin.getGroup().getRepGroupImpl());

        /* Check the ensureMember utility, no monitors at the begining. */
        assertEquals(repGroupImpl.getMonitorMembers().size(), 0);

        openMonitor(master);

        /* Check the group information and monitor after insertion. */
        repGroupImpl = master.getGroup().getRepGroupImpl();
        assertEquals(repGroupImpl, groupAdmin.getGroup().getRepGroupImpl());
        assertEquals(repGroupImpl.getMonitorMembers().size(), 1);
    }

    /**
     * Test the dumpGroup with just electable nodes.
     */
    @Test
    public void testDumpGroupNoMonitorNoSecondary() {
        dumpGroup();
    }

    private String dumpGroup() {
        final ReplicatedEnvironment master =
            RepTestUtils.joinGroup(repEnvInfo);
        final DbGroupAdmin dbAdmin =
            useRepNetConfig ?
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             RepTestUtils.readRepNetConfig()) :
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets());

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        final PrintStream bytesOut = new PrintStream(bytes);
        try {
            System.setOut(bytesOut);
            dbAdmin.dumpGroup();
        } finally {
            System.setOut(originalOut);
        }
        bytesOut.close();
        final String dump = bytes.toString();
        logger.info("Dump group output:\n" + dump);
        for (final RepEnvInfo info : repEnvInfo) {
            assertThat(dump, containsString(info.getEnv().getNodeName()));
        }
        return dump;
    }

    /**
     * Test the dumpGroup with monitor and secondary nodes.
     */
    @Test
    public void testDumpGroupMonitorSecondary()
        throws IOException, InterruptedException {

        final ReplicatedEnvironment master =
            RepTestUtils.joinGroup(repEnvInfo);

        openSecondaryNode();
        final RepNodeImpl monitor = openMonitor(master);

        Thread.sleep(2000);

        final String dump = dumpGroup();
        assertThat(dump, containsString(monitor.getName()));
    }

    /*
     * Test the non-default network properties constructor.
     */
    @Test
    public void testNetConfigCtor()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(master.getState().isMaster());

        /* Construct a DbGroupAdmin instance providing a Properties object. */
        DbGroupAdmin dbAdmin =
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             RepTestUtils.readRepNetConfig());

        tryDbGroupAdmin(dbAdmin);
    }

    /*
     * Test the non-default net properties file constructor
     */
    @Test
    public void testNetPropertiesFileCtor()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(master.getState().isMaster());

        File propertyFile = new File(repEnvInfo[0].getEnvHome().getPath(),
                                     "je.properties");

        /* Construct a DbGroupAdmin instance providing a property file. */
        DbGroupAdmin dbAdmin =
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             propertyFile);

        tryDbGroupAdmin(dbAdmin);
    }

    /*
     * Support method for net properties constructor variants.  This just
     * does a basic master transfer test to make sure communications works.
     */
    private void tryDbGroupAdmin(DbGroupAdmin dbAdmin)
        throws Exception {

        assertTrue(repEnvInfo[0].isMaster());
        assertTrue(repEnvInfo[1].isReplica());

        /* Transfer the mastership to node 1. */
        PrintStream original = System.out;
        try {
            /* Avoid polluting the test output. */
            System.setOut(new PrintStream(new ByteArrayOutputStream()));
            dbAdmin.transferMaster(repEnvInfo[1].getEnv().getNodeName(),
                                   "5 s");
            MasterTransferTest.awaitSettle(repEnvInfo[0], repEnvInfo[1]);
        } catch (Exception e) {
            fail("Unexpected exception: " + LoggerUtils.getStackTrace(e));
        } finally {
            System.setOut(original);
        }

        /* Check the node state. */
        assertTrue(repEnvInfo[0].isReplica());
        assertTrue(repEnvInfo[1].isMaster());
    }

    /*
     * Test the -netProps command-line argument
     */
    @Test
    public void testNetPropsCommandLine()
        throws Exception {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        assertTrue(master.getState().isMaster());

        File propertyFile = new File(repEnvInfo[0].getEnvHome().getPath(),
                                     "je.properties");

        String[] args = new String[] {
            "-groupName", RepTestUtils.TEST_REP_GROUP_NAME,
            "-helperHosts", master.getRepConfig().getNodeHostPort(),
            "-netProps", propertyFile.getPath(),
            "-transferMaster", "-force", repEnvInfo[1].getEnv().getNodeName(), "5 s" };

        /* Transfer the mastership to node 1. */
        PrintStream original = System.out;
        try {
            /* Avoid polluting the test output. */
            System.setOut(new PrintStream(new ByteArrayOutputStream()));

            DbGroupAdmin.main(args);

            MasterTransferTest.awaitSettle(repEnvInfo[0], repEnvInfo[1]);
        } catch (Exception e) {
            fail("Unexpected exception: " + LoggerUtils.getStackTrace(e));
        } finally {
            System.setOut(original);
        }



        /* Check the node state. */
        assertTrue(repEnvInfo[0].isReplica());
        assertTrue(repEnvInfo[1].isMaster());
    }
}
