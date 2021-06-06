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
package com.sleepycat.je.rep.impl;

import java.nio.ByteBuffer;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.BinaryProtocol;

/**
 * Defines the protocol used in support of node state querying.
 *
 * Because this protocol has to transfer byte array between two nodes, so
 * instead of using the former NodeStateProtocol, we introduce this new
 * protocol which inherits from BinaryProtocol.
 *
 * Note: once we support active version update, we can use one protocol only.
 *
 * The message request sequence:
 *    NODE_STATE_REQ -> NODE_STATE_RESP
 */
public class BinaryNodeStateProtocol extends BinaryProtocol {

    public static final int VERSION = 1;

    /* The messages defined by this class. */
    public final static MessageOp BIN_NODE_STATE_REQ =
        new MessageOp((short) 1, BinaryNodeStateRequest.class);
    public final static MessageOp BIN_NODE_STATE_RESP =
        new MessageOp((short) 2, BinaryNodeStateResponse.class);

    public BinaryNodeStateProtocol(NameIdPair nameIdPair,
                                   RepImpl repImpl) {

        super(nameIdPair, VERSION, VERSION, repImpl);

        this.initializeMessageOps(new MessageOp[] {
                BIN_NODE_STATE_REQ,
                BIN_NODE_STATE_RESP
        });
    }

    /* Message request the state of the specified node. */
    public class BinaryNodeStateRequest extends SimpleMessage {
        /* The name of the node whose status is requested. */
        private final String nodeName;
        /* The name of the group the node belongs to. */
        private final String groupName;

        public BinaryNodeStateRequest(String nodeName, String groupName) {
            super();
            this.nodeName = nodeName;
            this.groupName = groupName;
        }

        public BinaryNodeStateRequest(ByteBuffer buffer) {
            super();
            nodeName = getString(buffer);
            groupName = getString(buffer);
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getGroupName() {
            return groupName;
        }

        @Override
        public MessageOp getOp() {
            return BIN_NODE_STATE_REQ;
        }

        @Override
        public ByteBuffer wireFormat() {
           return wireFormat(nodeName, groupName);
        }
    }

    /* Message return state of specified node. */
    public class BinaryNodeStateResponse extends SimpleMessage {
        /* The name of the node which requests the status. */
        private final String nodeName;
        /* The name of the group which this node joins. */
        private final String groupName;
        /* The name of the current master in the group. */
        private final String masterName;
        /* The JEVersion that this node runs. */
        private final JEVersion jeVersion;
        /* Time when this node joins the group. */
        private final long joinTime;
        /* The current state of this node. */
        private final State nodeState;
        /* The last commit VLSN on this node. */
        private final long commitVLSN;
        /* The last master commit VLSN known by this node. */
        private final long masterCommitVLSN;
        /* The number of running feeders on this node. */
        private final int activeFeeders;
        /* The log version of this node. */
        private final int logVersion;
        /* The state of the JE application, specified by users themselves. */
        private final byte[] appState;

        /*
         * The system load of the node, it is serialized and deserialized in
         * String format.
         */
        private final double systemLoad;

        public BinaryNodeStateResponse(String nodeName,
                                       String groupName,
                                       String masterName,
                                       JEVersion jeVersion,
                                       long joinTime,
                                       State nodeState,
                                       long commitVLSN,
                                       long masterCommitVLSN,
                                       int activeFeeders,
                                       int logVersion,
                                       byte[] appState,
                                       double systemLoad) {
            super();
            this.nodeName = nodeName;
            this.groupName = groupName;
            /*
             * Master could be unknown, but must be non-null for
             * serialization.
             */
            this.masterName = (masterName == null) ? "" : masterName;
            this.jeVersion = jeVersion;
            this.joinTime = joinTime;
            this.nodeState = nodeState;
            this.commitVLSN = commitVLSN;
            this.masterCommitVLSN = masterCommitVLSN;
            this.activeFeeders = activeFeeders;
            this.logVersion = logVersion;
            this.appState = appState;
            this.systemLoad = systemLoad;
        }

        public BinaryNodeStateResponse(ByteBuffer buffer) {
            super();
            nodeName = getString(buffer);
            groupName = getString(buffer);
            masterName = getString(buffer);
            jeVersion = new JEVersion(getString(buffer));
            joinTime = LogUtils.readLong(buffer);
            nodeState = getEnum(State.class, buffer);
            commitVLSN = LogUtils.readLong(buffer);
            masterCommitVLSN = LogUtils.readLong(buffer);
            activeFeeders = LogUtils.readInt(buffer);
            logVersion = LogUtils.readInt(buffer);
            appState = getByteArray(buffer);
            systemLoad = getDouble(buffer);
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getGroupName() {
            return groupName;
        }

        public String getMasterName() {
            return masterName;
        }

        public JEVersion getJEVersion() {
            return jeVersion;
        }

        public long getJoinTime() {
            return joinTime;
        }

        public State getNodeState() {
            return nodeState;
        }

        public long getCommitVLSN() {
            return commitVLSN;
        }

        public long getKnownMasterCommitVLSN() {
            return masterCommitVLSN;
        }

        public int getActiveFeeders() {
            return activeFeeders;
        }

        public int getLogVersion() {
            return logVersion;
        }

        public byte[] getAppState() {
            if (appState.length == 0) {
                return null;
            }

            return appState;
        }

        public double getSystemLoad() {
            return systemLoad;
        }

        @Override
        public MessageOp getOp() {
            return BIN_NODE_STATE_RESP;
        }

        @Override
        public ByteBuffer wireFormat() {

            /*
             * If the appState is null, make a new zero byte array, since
             * writing null byte array would cause a NPE, and a meaningful
             * application state shouldn't be a zero length byte array.
             */
            byte[] realAppState = (appState == null ? new byte[0] : appState);
            return wireFormat(nodeName,
                              groupName,
                              masterName,
                              jeVersion.toString(),
                              joinTime,
                              nodeState,
                              commitVLSN,
                              masterCommitVLSN,
                              activeFeeders,
                              logVersion,
                              realAppState,
                              systemLoad);
        }

        /* Convert the response to the NodeState. */
        public NodeState convertToNodeState() {
            return new NodeState(nodeName,
                                 groupName,
                                 nodeState,
                                 masterName,
                                 jeVersion,
                                 joinTime,
                                 commitVLSN,
                                 masterCommitVLSN,
                                 activeFeeders,
                                 logVersion,
                                 getAppState(),
                                 systemLoad);
        }
    }
}
