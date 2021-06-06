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

package com.sleepycat.je.rep.elections;

import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE1_NO_NON_ZERO_PRIO;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PHASE1_NO_QUORUM;
import static com.sleepycat.je.rep.elections.ProposerStatDefinition.PROMISE_COUNT;
import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.arbitration.Arbiter;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.ElectionQuorum;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.util.test.TestBase;

/**
 * Tests for elections as a whole.
 */
public class ElectionsTest extends TestBase {

    /* Number of nodes in the test */
    private static final int nodes = 3;
    private static final int monitors = 1;
    private int nretries;

    private final Object notificationsLock = new Object();
    private int listenerNotifications = 0;

    private final ReplicationConfig repConfig[] =
        new ReplicationConfig[nodes + 1];
    // private Monitor monitor;
    private boolean monitorInvoked = false;

    private final List<Elections> electionNodes = new LinkedList<Elections>();
    private MasterValue winningValue = null;

    /* Latch to ensure that required all listeners have made it through. */
    CountDownLatch listenerLatch;

    private RepGroupImpl repGroup = null;

    @Override
    @Before
    public void setUp() throws IOException {
        repGroup = RepTestUtils.createTestRepGroup(nodes, monitors);
        for (RepNodeImpl rn : repGroup.getAllElectableMembers()) {
            ReplicationConfig config = new ReplicationConfig();
            config.setRepNetConfig(
                ReplicationNetworkConfig.create(
                    RepTestUtils.readNetProps()));
            repConfig[rn.getNodeId()] = config;
            config.setNodeName(rn.getName());
            config.setNodeHostPort(rn.getHostName()+ ":" +rn.getPort());
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if ((electionNodes != null) && (electionNodes.size() > 0)) {
            electionNodes.get(0).shutdownAcceptorsLearners(
                repGroup.getAllAcceptorSockets(),
                repGroup.getAllHelperSockets());

            for (Elections node : electionNodes) {
                node.getServiceDispatcher().shutdown();
            }
        }
    }

    /**
     * Simulates the start up of the first "n" nodes. If < n nodes are started,
     * the others simulate being down.
     *
     * @param nstart nodes to start up
     * @param groupSize the size of the group
     * @throws IOException
     */
    public void startReplicationNodes(final int nstart,
                                      final int groupSize,
                                      final boolean testPriority)
        throws IOException {

        for (short nodeNum = 1; nodeNum <= nstart; nodeNum++) {
            RepNode rn = newRepNode(groupSize, nodeNum, testPriority);
            RepElectionsConfig ec = new RepElectionsConfig(rn);
            ec.setGroupName("TEST_GROUP");
            Elections elections =
                new Elections(ec,
                              new TestListener(),
                              newSuggestionGenerator(nodeNum, testPriority));
            elections.getRepNode().getServiceDispatcher().start();
            elections.startLearner();
            elections.participate();
            electionNodes.add(elections);
            elections.updateRepGroup(repGroup);
        }

        // Start up the Monitor as well.

        /*
        InetSocketAddress monitorSocket =
            repGroup.getMonitors().iterator().next().getLearnerSocket();
        monitor = new Monitor(repConfig[1].getGroupName(),
                              monitorSocket,
                              repGroup);
        monitor.setMonitorChangeListener(new MonitorChangeListener() {
            @Override
            public void replicationChange(
                MonitorChangeEvent monitorChangeEvent) {
                monitorInvoked = true;
                assertEquals(winningValue.getMasterNodeId(),
                        ((NewMasterEvent) monitorChangeEvent).getMasterId());
            }
        });
        monitor.startMonitor();
        */
    }

    public void startReplicationNodes(final int nstart,
                                      final int groupSize)
        throws IOException {
            startReplicationNodes(nstart, groupSize, false);
    }

    private RepNode newRepNode(final int groupSize,
                               final short nodeNum,
                               final boolean testPriority)
        throws IOException {

        final DataChannelFactory channelFactory =
            DataChannelFactoryBuilder.construct(
                repConfig[nodeNum].getRepNetConfig(),
                repConfig[nodeNum].getNodeName());
        final ServiceDispatcher serviceDispatcher =
            new ServiceDispatcher(repConfig[nodeNum].getNodeSocketAddress(),
                                  channelFactory);

        return new RepNode(new NameIdPair(repConfig[nodeNum].getNodeName(),
                                          nodeNum),
                                          serviceDispatcher) {
            @Override
            public ElectionQuorum getElectionQuorum() {
                return new ElectionQuorum() {

                    @Override
                    public boolean haveQuorum(QuorumPolicy quorumPolicy,
                                              int votes) {
                        return votes >= quorumPolicy.quorumSize(groupSize);
                    }
                };
            }

            @Override
            public int getElectionPriority() {
                return testPriority ? (groupSize - nodeNum + 1) :
                    repConfig[nodeNum].getNodePriority();
            }

            /**
             * This faked out test node never really does arbitration, but
             * needs to be able to return an Arbiter instance
             * for election retries.
             */
            @Override
            public Arbiter getArbiter() {
                return new Arbiter(null) {
                   @Override
                    public synchronized boolean activateArbitration() {
                        return false;
                    }
                };
            }
        };
    }

    private SuggestionGenerator newSuggestionGenerator(
        final short nodeNum,
        final boolean testPriority) {
        return new Acceptor.SuggestionGenerator() {
            @Override
            public Value get(Proposal proposal) {
                return new MasterValue("testhost", 9999,
                                       new NameIdPair("n" + nodeNum,
                                                      nodeNum));
            }

            @Override
            public Ranking getRanking(Proposal proposal) {
                return new Ranking(testPriority ? 1000l : nodeNum * 10l, 0);
            }
        };
    }

    public void startReplicationNodes(int nstart)
        throws IOException {
        startReplicationNodes(nstart, nstart);
    }

    class TestListener implements Learner.Listener {

        @Override
        public void notify(Proposal proposal, Value value) {
            synchronized (notificationsLock) {
                listenerNotifications++;
            }
            assertEquals(winningValue, value);
            listenerLatch.countDown();
        }
    }

    private Elections setupAndRunElection(QuorumPolicy qpolicy,
                                          int activeNodes,
                                          int groupSize)
            throws IOException, InterruptedException {

        /* Start all of them. */
        startReplicationNodes(activeNodes, groupSize);
        winningValue = new MasterValue("testhost", 9999,
                                       new NameIdPair("n" + (activeNodes),
                                                      (activeNodes)));
        return runElection(qpolicy, activeNodes);
    }

    private Elections setupAndRunElection(int activeNodes) throws IOException,
            InterruptedException {
        return setupAndRunElection(QuorumPolicy.SIMPLE_MAJORITY,
                                   activeNodes,
                                   activeNodes);
    }

    private Elections setupAndRunElection(int activeNodes, int groupSize)
        throws IOException, InterruptedException {
        return setupAndRunElection(QuorumPolicy.SIMPLE_MAJORITY,
                                   activeNodes,
                                   groupSize);
    }

    private Elections runElection(QuorumPolicy qpolicy, int activeNodes)
            throws InterruptedException {
        listenerNotifications = 0;
        monitorInvoked = false;
        nretries = 2;
        listenerLatch = new CountDownLatch(activeNodes);
        /* Initiate an election on the first node. */
        Elections testElections = electionNodes.iterator().next();

        testElections.initiateElection(repGroup, qpolicy, nretries);
        /* Ensure that Proposer has finished. */
        testElections.waitForElection();
        return testElections;
    }

    private Elections runElection(int activeNodes)
        throws InterruptedException {

        return runElection(QuorumPolicy.SIMPLE_MAJORITY, activeNodes);
    }

    /**
     * Simulates presence of a simple majority, but with prio zero nodes
     */
    @Test
    public void testBasicZeroPrio() throws InterruptedException,
            IOException {

        /* Elections with a mix of zero and non-zero prio nodes. */
        final int majority = (nodes / 2);
        /* Have the first < majority nodes be zero prio. */

        for (int i = 1; i < nodes; i++) {
            repConfig[i].setNodePriority(0);
        }
        setupAndRunElection(nodes);
        listenerLatch.await();
        assertEquals(nodes, listenerNotifications);

        /* Now remove all non-zero prio nodes and try hold an election. */

        // Now remove one node and the elections should give up after
        // retries have expired.
        electionNodes.get(nodes-1).getAcceptor().shutdown();
        Elections testElections = runElection(nodes);
        /* No successful elections, hence no notification. */
        assertEquals(0, listenerNotifications);

        /* Ensure that all retries were due to lack of a Quorum. */
        assertEquals
            (nretries,
             testElections.getStats().getInt(PHASE1_NO_NON_ZERO_PRIO));
    }

    /**
     * Tests a basic election with everything being normal.
     */
    @Test
    public void testBasicAllNodes()
        throws InterruptedException, IOException {

        /* Start all of them. */
        setupAndRunElection(nodes);
        listenerLatch.await();

        assertEquals(nodes, listenerNotifications);
        // assertTrue(monitorInvoked);
        runElection(nodes);
        listenerLatch.await();
        assertEquals(nodes, listenerNotifications);
        assertFalse(monitorInvoked);
    }

    @Test
    public void testBasicAllPrioNodes()
        throws InterruptedException, IOException {

        /* Start all of them. */
        startReplicationNodes(nodes, nodes, true);
        winningValue = new MasterValue("testhost", 9999,
                                       new NameIdPair("n1", 1));
        runElection(QuorumPolicy.SIMPLE_MAJORITY, nodes);
        listenerLatch.await();

        assertEquals(nodes, listenerNotifications);
        // assertTrue(monitorInvoked);
        runElection(nodes);
        listenerLatch.await();
        assertEquals(nodes, listenerNotifications);
        assertFalse(monitorInvoked);
    }

    /**
     * Simulates one node never having come up.
     */
    @Test
    public void testBasicAllButOneNode() throws InterruptedException,
            IOException {

        /*
         * Simulate one node down at startup, but sufficient nodes for a quorum.
         */
        setupAndRunElection(nodes - 1);
        listenerLatch.await();
        assertEquals(nodes - 1, listenerNotifications);
        // assertTrue(monitorInvoked);
    }

    /**
     * Tests a basic election with one node having crashed.
     */
    @Test
    public void testBasicOneNodeCrash() throws InterruptedException,
            IOException {
        /* Start all of them. */
        Elections testElections = setupAndRunElection(nodes);
        listenerLatch.await();

        assertEquals(nodes, listenerNotifications);
        // assertTrue(monitorInvoked);
        assertEquals(nodes, testElections.getStats().getInt(PROMISE_COUNT));
        electionNodes.get(0).getAcceptor().shutdown();
        testElections = runElection(nodes);
        listenerLatch.await();
        /* The listener should have still obtained a notification. */
        assertEquals(nodes, listenerNotifications);
        /* Master unchanged so monitor not invoked */
        assertFalse(monitorInvoked);
        assertEquals(nodes - 1, testElections.getStats().getInt(PROMISE_COUNT));
    }

    /**
     * Tests a QuorumPolicy of ALL.
     */
    @Test
    public void testQuorumPolicyAll() throws InterruptedException, IOException {

        /* Start all of them. */
        Elections testElections =
            setupAndRunElection(QuorumPolicy.ALL, nodes, nodes);
        listenerLatch.await();

        assertEquals(nodes, listenerNotifications);
        // assertTrue(monitorInvoked);
        assertEquals(nodes, testElections.getStats().getInt(PROMISE_COUNT));

        // Now remove one node and the elections should give up after
        // retries have expired.
        electionNodes.get(0).getAcceptor().shutdown();
        testElections = runElection(QuorumPolicy.ALL, nodes);

        assertEquals(0, listenerNotifications);
        assertFalse(monitorInvoked);

        /* Ensure that all retries were due to lack of a Quorum. */
        assertEquals
            (nretries, testElections.getStats().getInt(PHASE1_NO_QUORUM));
    }

    /**
     * Tests the case where a quorum could not be reached.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNoQuorum() throws IOException, InterruptedException {

        Elections testElections = setupAndRunElection(nodes / 2, nodes);

        /*
         * No listeners were invoked so don't wait for a latch. No quorum,
         * therefore no listener invocations.
         */
        assertEquals(0, listenerNotifications);
        assertFalse(monitorInvoked);
        /* No listeners were invoked. */
        assertEquals(nodes / 2, listenerLatch.getCount());
        /* Ensure that all retries were due to lack of a Quorum. */
        assertEquals
            (nretries, testElections.getStats().getInt(PHASE1_NO_QUORUM));
    }

    private class RepElectionsConfig implements ElectionsConfig {

        private final RepNode repNode;
        private String groupName;

        public RepElectionsConfig(RepNode repNode) {
            this.repNode = repNode;

            if (repNode.getRepImpl() == null) {
                /* when used for unit testing */
                return;
            }
            groupName =
                repNode.getRepImpl().getConfigManager().get(GROUP_NAME);
        }

        /**
         * used for testing only
         * @param groupName
         */
        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }
        public String getGroupName() {
            return groupName;
        }
        public NameIdPair getNameIdPair() {
            return repNode.getNameIdPair();
        }
        public ServiceDispatcher getServiceDispatcher() {
            return repNode.getServiceDispatcher();
        }
        public int getElectionPriority() {
            return repNode.getElectionPriority();
        }
        public int getLogVersion() {
            return repNode.getLogVersion();
        }
        public RepImpl getRepImpl() {
            return repNode.getRepImpl();
        }
        public RepNode getRepNode() {
            return repNode;
        }
    }
}
