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

import java.io.Serializable;
import java.net.InetSocketAddress;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.node.cbvlsn.CleanerBarrierState;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.VLSN;

/**
 * Describes a node that is a member of the replication group.
 */
public class RepNodeImpl implements ReplicationNode, Serializable {

    private static final long serialVersionUID = 1L;

    /* Identifies the node both by external name and internal node ID. */
    private final NameIdPair nameIdPair;

    /* The node type, electable, monitor, etc. */
    private final NodeType type;

    /*
     * True if the node was acknowledged by a quorum and its entry is therefore
     * considered durable.  SECONDARY and EXTERNAL nodes are always considered
     * acknowledged.
     */
    private boolean quorumAck;

    /*
     * True if the node has been removed and is no longer an active part of the
     * group
     */
    private boolean isRemoved;

    /* The hostname used for communications with the node. */
    private String hostName;

    /* The port used by a node. */
    private int port;

    /*
     * The CBVLSN Barrier state associated with the node. Is unused if the
     * GlobalCBVLSN is defunct. See GlobalCBVLSN.
     */
    private CleanerBarrierState barrierState;

    /*
     * This version is used in conjunction with the group level change
     * version to identify the incremental changes made to individual
     * changes made to a group.
     */
    private int changeVersion = NULL_CHANGE;

    private static final int NULL_CHANGE = -1;

    /**
     * The JE version most recently noted running on this node, or null if not
     * known.
     */
    private volatile JEVersion jeVersion;

    /**
     * @hidden
     *
     * Constructor used to de-serialize a Node. All other convenience
     * constructors funnel through this one so that argument checks can
     * be systematically enforced.
     */
    public RepNodeImpl(final NameIdPair nameIdPair,
                       final NodeType type,
                       final boolean quorumAck,
                       final boolean isRemoved,
                       final String hostName,
                       final int port,
                       final CleanerBarrierState barrierState,
                       final int changeVersion,
                       final JEVersion jeVersion) {

        if (nameIdPair.getName().equals(RepGroupDB.GROUP_KEY)) {
            throw EnvironmentFailureException.unexpectedState
            ("Member node ID is the reserved key value: " + nameIdPair);
        }

        if (hostName == null) {
            throw EnvironmentFailureException.unexpectedState
            ("The hostname argument must not be null");
        }

        if (type == null) {
            throw EnvironmentFailureException.unexpectedState
            ("The nodeType argument must not be null");
        }

        this.nameIdPair = nameIdPair;
        this.type = type;
        this.quorumAck = quorumAck || type.isSecondary() || type.isExternal();
        this.isRemoved = isRemoved;
        this.hostName = hostName;
        this.port = port;
        this.barrierState = barrierState;
        this.changeVersion = changeVersion;
        this.jeVersion = jeVersion;
    }

    /**
     * @hidden
     *
     * Convenience constructor for the above.
     */
    public RepNodeImpl(final NameIdPair nameIdPair,
                       final NodeType type,
                       final boolean quorumAck,
                       final boolean isRemoved,
                       final String hostName,
                       final int port,
                       final int changeVersion,
                       final JEVersion jeVersion) {
        this(nameIdPair, type, quorumAck, isRemoved, hostName, port,
             new CleanerBarrierState(VLSN.NULL_VLSN, System.currentTimeMillis()),
             changeVersion, jeVersion);
    }

    /**
     * @hidden
     * Convenience constructor for transient nodes
     */
    public RepNodeImpl(final NameIdPair nameIdPair,
                       final NodeType type,
                       final String hostName,
                       final int port,
                       final JEVersion jeVersion) {
        this(nameIdPair, type, false, false, hostName, port, NULL_CHANGE,
             jeVersion);
    }

    /**
     * @hidden
     * Convenience constructor for transient nodes during unit tests.
     */
    public RepNodeImpl(final ReplicationConfig repConfig) {
        this(new NameIdPair(repConfig.getNodeName(), NameIdPair.NULL_NODE_ID),
             repConfig.getNodeType(),
             repConfig.getNodeHostname(),
             repConfig.getNodePort(),
             JEVersion.CURRENT_VERSION);
    }

    /**
     * @hidden
     *
     * Convenience constructor for the above.
     */
    public RepNodeImpl(final String nodeName,
                       final String hostName,
                       final int port,
                       final JEVersion jeVersion) {
        this(new NameIdPair(nodeName, NameIdPair.NULL.getId()),
             NodeType.ELECTABLE, hostName, port, jeVersion);
    }

    /**
     * @hidden
     *
     * Convenience constructor for the above.
     */
    public RepNodeImpl(Protocol.NodeGroupInfo mi) {
        this(mi.getNameIdPair(),
             mi.getNodeType(),
             mi.getHostName(),
             mi.port(),
             mi.getJEVersion());
    }

    /* (non-Javadoc)
     * @see com.sleepycat.je.rep.ReplicationNode#getSocketAddress()
     */
    @Override
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(hostName, port);
    }

    /**
     * Returns whether the node was acknowledged by a quorum and its entry is
     * therefore considered durable.  Secondary nodes are always considered
     * acknowledged.
     */
    public boolean isQuorumAck() {
        return quorumAck;
    }

    public boolean isRemoved() {
        assert !(isRemoved && type.hasTransientId())
            : "Nodes with transient IDs are never marked removed";
        return isRemoved;
    }

    public void setChangeVersion(int changeVersion) {
        this.changeVersion = changeVersion;
    }

    public int getChangeVersion() {
        return changeVersion;
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    /* (non-Javadoc)
     * @see com.sleepycat.je.rep.ReplicationNode#getName()
     */
    @Override
    public String getName() {
        return nameIdPair.getName();
    }

    public int getNodeId() {
        return nameIdPair.getId();
    }

    /* (non-Javadoc)
     * @see com.sleepycat.je.rep.ReplicationNode#getNodeType()
     */
    @Override
    public NodeType getType() {
        return type;
    }

    /* (non-Javadoc)
     * @see com.sleepycat.je.rep.ReplicationNode#getHostName()
     */
    @Override
    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /* (non-Javadoc)
     * @see com.sleepycat.je.rep.ReplicationNode#getPort()
     */
    @Override
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostPortPair() {
        return HostPortPair.getString(hostName, port);
    }

    public CleanerBarrierState getBarrierState() {
        return barrierState;
    }

    public CleanerBarrierState setBarrierState(CleanerBarrierState barrierState) {
        return this.barrierState = barrierState;
    }

    public void setQuorumAck(boolean quorumAck) {
        this.quorumAck = quorumAck;
    }

    public void setRemoved(boolean isRemoved) {
        this.isRemoved = isRemoved;
    }

    /**
     * Returns the JE Version most recently noted running on this node, or
     * {@code null} if not known.
     */
    public JEVersion getJEVersion() {
        return jeVersion;
    }

    /**
     * Updates the JE version most recently known running on this node to match
     * the version specified.  Does nothing if the argument is null.
     *
     * @param otherJEVersion the version or {@code null}
     */
    public void updateJEVersion(final JEVersion otherJEVersion) {
        if (otherJEVersion != null) {
            jeVersion = otherJEVersion;
        }
    }

    @Override
    public String toString() {

        String acked = " (is member)";

        if (!quorumAck) {
            acked = " (not yet a durable member)";
        }

        if (isRemoved) {
            acked = " (is removed)";
        }

        String info =
            String.format("Node:%s %s:%d%s%s changeVersion:%d %s%s\n",
                          getName(), getHostName(), getPort(),
                          acked,
                          (!type.isElectable() ? " " + type : ""),
                          getChangeVersion(),
                          barrierState,
                          ((jeVersion != null) ?
                           " jeVersion:" + jeVersion :
                           ""));
        return info;

    }

    /**
     * Checks if the argument represents the same node, ignoring fields that
     * might legitimately vary over time.  Like the equals method, considers
     * all fields, except ignores the quorumAck field (which may change
     * temporarily), the nodeId (since it may not have been resolved as yet),
     * and the isRemoved, barrierState, changeVersion, and jeVersion fields
     * (which can change over time).
     *
     * @param mi the other object in the comparison
     *
     * @return true if the two are equivalent
     */
    public boolean equivalent(RepNodeImpl mi) {
        if (this == mi) {
            return true;
        }

        if (mi == null) {
            return false;
        }

        if (port != mi.port) {
            return false;
        }

        if (hostName == null) {
            if (mi.hostName != null) {
                return false;
            }
        } else if (!hostName.equals(mi.hostName)) {
            return false;
        }

        /* Ignore the id. */
        if (!nameIdPair.getName().equals(mi.nameIdPair.getName())) {
            return false;
        }

        if (getType() != mi.getType()) {
            return false;
        }

        /*
         * Ignore quorumAck, isRemoved, barrierState, changeVersion, and
         * jeVersion
         */

        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((hostName == null) ? 0 : hostName.hashCode());
        result = prime * result + nameIdPair.hashCode();
        result = prime * result + port;
        result = prime * result + (isQuorumAck() ? 1231 : 1237);
        result = prime * result +
            (jeVersion == null ? 0 : jeVersion.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RepNodeImpl)) {
            return false;
        }
        final RepNodeImpl other = (RepNodeImpl) obj;
        if (hostName == null) {
            if (other.hostName != null) {
                return false;
            }
        } else if (!hostName.equals(other.hostName)) {
            return false;
        }
        if (!nameIdPair.equals(other.nameIdPair)) {
            return false;
        }
        if (getType() != other.getType()) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        if (isQuorumAck() != other.isQuorumAck()) {
            return false;
        }
        if (jeVersion == null) {
            if (other.jeVersion != null) {
                return false;
            }
        } else if (!jeVersion.equals(other.getJEVersion())) {
            return false;
        }
        return true;
    }
}
