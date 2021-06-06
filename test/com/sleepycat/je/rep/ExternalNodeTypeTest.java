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

import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test to test the EXTERNAL node type
 *
 */
public class ExternalNodeTypeTest extends RepTestBase {

    /* test db */
    private final String dbName = "SUBSCRIPTION_UNIT_TEST_DB";
    private final int startKey = 1;
    /* test db with 10k keys */
    private final int numKeys = 1024*10;
    private final List<Integer> keys = new ArrayList<>();

    /* a rep group with 1 master, 2 replicas  */
    private final int numReplicas = 2;
    private final int numDataNodes = 1 + numReplicas;
    private final int groupSize = 1 + numReplicas;
    private Logger logger;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(),
                                                  "ExternalNodeTypeTest");
    }

    @Override
    @After
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /* Test an external node is able to handshake and syncup with feeder */
    @Test
    public void testExternalNodeType() throws Exception {

        /* create and verify a replication group */
        prepareTestEnv();

        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        /* populate some data and verify */
        populateDataAndVerify(masterEnv);

        final MockClientNode mockClientNode =
            new MockClientNode(NodeType.EXTERNAL, masterEnv, logger);

        /* handshake with feeder */
        mockClientNode.handshakeWithFeeder();

        /* sync up with feeder */
        final VLSN startVLSN = mockClientNode.syncupWithFeeder(VLSN.FIRST_VLSN);

        /* verify */
        assertTrue("Mismatch start vlsn ", startVLSN.equals(VLSN.FIRST_VLSN));

        /* receive 10K keys */
        mockClientNode.consumeMsgLoop(numKeys);
        assertTrue("Expect receive " + numKeys + " keys while actually " +
                   "receiving " + mockClientNode.getReceivedMsgs().size() +
                   " keys.",
                   mockClientNode.getReceivedMsgs().size() == numKeys);

        mockClientNode.shutdown();
    }

    /* Test an external node correctly dumped by ReplicationGroupAdmin */
    @Test
    public void testDumpExternalNode() throws Exception {

        /* create and verify a replication group */
        prepareTestEnv();

        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        /* populate some data and verify */
        populateDataAndVerify(masterEnv);

        final MockClientNode mockClientNode =
            new MockClientNode(NodeType.EXTERNAL, masterEnv, logger);

        /* handshake with feeder */
        mockClientNode.handshakeWithFeeder();

        /* sync up with feeder */
        mockClientNode.syncupWithFeeder(VLSN.FIRST_VLSN);

        /* get an instance of rep group admin */
        final ReplicationNetworkConfig repNetConfig =
            RepTestUtils.readRepNetConfig();
        final ReplicationGroupAdmin repGroupAdmin =
            repNetConfig.getChannelType().isEmpty() ?
                new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                          masterEnv.getRepConfig()
                                                   .getHelperSockets())
                :
                new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                          masterEnv.getRepConfig()
                                                   .getHelperSockets(),
                                          RepTestUtils.readRepNetConfig());

        final RepGroupImpl repGroupImpl = repGroupAdmin.getGroup()
                                                       .getRepGroupImpl();
        final Set<RepNodeImpl> result = repGroupImpl.getExternalMembers();
        assertTrue("Expect only one external node.", result.size() == 1);
        final RepNodeImpl node = result.iterator().next();
        assertEquals("Node name mismatch", mockClientNode.nodeName,
                     node.getName());
        assertEquals("Node type mismatch", NodeType.EXTERNAL, node.getType());

        mockClientNode.shutdown();
    }

    /* Create a test env and verify it is in good shape */
    private void prepareTestEnv() throws InterruptedException {

        createGroup(numDataNodes);

        for (int i=0; i < numDataNodes; i++) {
            final ReplicatedEnvironment env = repEnvInfo[i].getEnv();
            final int targetGroupSize = groupSize;

            ReplicationGroup group = null;
            for (int j=0; j < 100; j++) {
                group = env.getGroup();
                if (group.getNodes().size() == targetGroupSize) {
                    break;
                }
                /* Wait for the replica to catch up. */
                Thread.sleep(1000);
            }
            assertEquals("Nodes", targetGroupSize, group.getNodes().size());
            assertEquals(RepTestUtils.TEST_REP_GROUP_NAME, group.getName());
            for (int ii = 0; ii < targetGroupSize; ii++) {
                RepTestUtils.RepEnvInfo rinfo = repEnvInfo[ii];
                final ReplicationConfig repConfig = rinfo.getRepConfig();
                ReplicationNode member =
                    group.getMember(repConfig.getNodeName());
                assertNotNull("Member", member);
                assertEquals(repConfig.getNodeName(), member.getName());
                assertEquals(repConfig.getNodeType(), member.getType());
                assertEquals(repConfig.getNodeSocketAddress(),
                             member.getSocketAddress());
            }

            /* verify data nodes */
            final Set<ReplicationNode> dataNodes = group.getDataNodes();
            for (final ReplicationNode n : dataNodes) {
                assertEquals(NodeType.ELECTABLE, n.getType());
            }
            logger.info("data nodes verified");
        }
    }

    /* Populate data into test db and verify */
    private void populateDataAndVerify(ReplicatedEnvironment masterEnv) {
        createTestData();
        populateDB(masterEnv, dbName, keys);
        readDB(masterEnv, dbName, startKey, numKeys);
        logger.info(numKeys + " records (start key: " +
                    startKey + ") have been populated into db " +
                    dbName + " and verified");
    }

    /* Create a list of (k, v) pairs for testing */
    private void createTestData() {
        for (int i = startKey; i < startKey + numKeys; i++) {
            keys.add(i);
        }
    }

}
