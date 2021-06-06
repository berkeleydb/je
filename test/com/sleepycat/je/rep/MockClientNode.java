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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.ChannelTimeoutTask;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshake;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshakeConfig;
import com.sleepycat.je.rep.stream.SubscriberFeederSyncup;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.NamedChannelWithTimeout;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * A mock client node that can be created to a given node type, used in test
 * only.
 */
class MockClientNode {

    public final String nodeName = "MockClientNode";

    private final Logger logger;
    private final SubscriptionConfig config;
    private final NodeType nodeType;
    private final RepImpl repImpl;

    /* communication channel between subscriber and feeder */
    private NamedChannelWithTimeout namedChannel;
    /* task to register channel with timeout */
    private ChannelTimeoutTask channelTimeoutTask;
    /* protocol used to communicate with feeder */
    private Protocol protocol;
    /* received msgs */
    private List<BinaryProtocol.Message> receivedMsgs;

    MockClientNode(NodeType nodeType, ReplicatedEnvironment env, Logger logger)
        throws Exception {

        this.nodeType = nodeType;
        this.logger = logger;

        repImpl = RepInternal.getNonNullRepImpl(env);
        config = createConfig(env, true);
        receivedMsgs = new ArrayList<>();
        protocol = null;
        namedChannel = null;
    }

    void handshakeWithFeeder() throws Exception {

        openChannel();
        ReplicaFeederHandshake handshake =
            new ReplicaFeederHandshake(
                new MockClientNodeFeederHandshakeConfig());
        protocol = handshake.execute();

    }

    VLSN syncupWithFeeder(VLSN reqVLSN) {
        final SubscriberFeederSyncup syncup =
            new SubscriberFeederSyncup(namedChannel, protocol,
                                       config.getFeederFilter(),
                                       repImpl,
                                       EntryRequestType.DEFAULT,
                                       logger);
        return syncup.execute(reqVLSN);
    }

    void consumeMsgLoop(long expected)
        throws InternalException, IOException {

        long counter = 0;
        while (counter < expected) {
            final BinaryProtocol.Message message = protocol.read(namedChannel);
            if ((message == null)) {
                return;
            }

            final BinaryProtocol.MessageOp messageOp = message.getOp();
            /* ignore heartbeat in mock client */
            if (messageOp != Protocol.HEARTBEAT) {
                if (messageOp == Protocol.SHUTDOWN_REQUEST) {
                    throw new InternalException("Receive shutdown msg from " +
                                                "feeder " + message);
                } else {
                    /* a regular data entry message */
                    receivedMsgs.add(message);
                    counter++;
                }
            }
        }
    }

    void shutdown() {
        RepUtils.shutdownChannel(namedChannel);
        if (channelTimeoutTask != null) {
            channelTimeoutTask.cancel();
        }
    }

    List<BinaryProtocol.Message> getReceivedMsgs() {
        return receivedMsgs;
    }

    /* Open a data channel to feeder */
    private NamedChannel openChannel() throws Exception {

        if (repImpl == null) {
            throw new IllegalStateException("Replication env is unavailable.");
        }

        DataChannelFactory.ConnectOptions connectOpts =
            new DataChannelFactory
                .ConnectOptions()
                .setTcpNoDelay(config.TCP_NO_DELAY)
                .setReceiveBufferSize(config.getReceiveBufferSize())
                .setOpenTimeout((int) config
                    .getStreamOpenTimeout(TimeUnit.MILLISECONDS))
                .setBlocking(config.BLOCKING_MODE_CHANNEL);

        final DataChannel channel =
            RepUtils.openBlockingChannel(config.getInetSocketAddress(),
                                         repImpl.getChannelFactory(),
                                         connectOpts);

        ServiceDispatcher.doServiceHandshake(channel,
                                             FeederManager.FEEDER_SERVICE);
        final int timeoutMs = repImpl.getConfigManager().
            getDuration(RepParams.PRE_HEARTBEAT_TIMEOUT);

        channelTimeoutTask = new ChannelTimeoutTask(new Timer(true));
        namedChannel =
            new NamedChannelWithTimeout(repImpl, logger, channelTimeoutTask,
                                        channel, timeoutMs);

        return namedChannel;
    }

    /* Create a subscription configuration */
    private SubscriptionConfig createConfig(ReplicatedEnvironment masterEnv,
                                            boolean useGroupUUID)
        throws Exception {

        final String home = "./subhome/";
        final String subNodeName = "test-mockclient-node";
        final String nodeHostPortPair = "localhost:6001";

        String feederNode;
        int feederPort;
        String groupName;

        final File envRoot = SharedTestUtils.getTestDir();
        final File subHome = new File(envRoot.getAbsolutePath() +
                                      File.separator + home);
        if (!subHome.exists()) {
            if (!subHome.mkdir()) {
                fail("unable to create test dir, fail the test");
            }
        }

        ReplicationGroup group = masterEnv.getGroup();
        ReplicationNode member = group.getMember(masterEnv.getNodeName());
        feederNode = member.getHostName();
        feederPort = member.getPort();
        groupName = group.getName();

        UUID uuid;
        if (useGroupUUID) {
            uuid = group.getRepGroupImpl().getUUID();
        } else {
            uuid = null;
        }

        final String feederHostPortPair = feederNode + ":" + feederPort;
        return new SubscriptionConfig(subNodeName, subHome.getAbsolutePath(),
                                      nodeHostPortPair, feederHostPortPair,
                                      groupName, uuid, nodeType);
    }

    /*-----------------------------------*/
    /*-         Inner Classes           -*/
    /*-----------------------------------*/
    private class MockClientNodeFeederHandshakeConfig
        implements ReplicaFeederHandshakeConfig {

        MockClientNodeFeederHandshakeConfig() {
        }

        public RepImpl getRepImpl() {
            return repImpl;
        }

        public NameIdPair getNameIdPair() {
            return new NameIdPair(nodeName);
        }

        public RepUtils.Clock getClock() {
            return new RepUtils.Clock(RepImpl.getClockSkewMs());
        }

        public NodeType getNodeType() {
            return nodeType;
        }

        public NamedChannel getNamedChannel() {
            return namedChannel;
        }

        /* create a group impl from group name and group uuid */
        public RepGroupImpl getGroup() {

            RepGroupImpl repGroupImpl = new RepGroupImpl(
                config.getGroupName(),
                true, /* unknown group uuid */
                repImpl.getCurrentJEVersion());

            /* use uuid if specified, otherwise unknown uuid will be used */
            if (config.getGroupUUID() != null) {
                repGroupImpl.setUUID(config.getGroupUUID());
            }
            return repGroupImpl;
        }
    }
}
