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
package com.sleepycat.je.rep.stream;

import java.net.InetSocketAddress;

import com.sleepycat.je.rep.impl.node.NameIdPair;

/**
 * Class used by a node to track changes in Master Status. It's updated by
 * the Listener. It represents the abstract notion that the notion of the
 * current Replica Group is definitive and is always in advance of the notion
 * of a master at each node. A node is typically playing catch up as it tries
 * to bring its view in line with that of the group.
 */
public class MasterStatus implements Cloneable {

    /* This node's identity */
    private final NameIdPair nameIdPair;

    /* The current master resulting from election notifications */
    private String groupMasterHostName = null;
    private int groupMasterPort = 0;
    /* The node ID used to identify the master. */
    private NameIdPair groupMasterNameId = NameIdPair.NULL;

    /*
     * The Master as implemented by the Node. It can lag the groupMaster
     * as the node tries to catch up.
     */
    private String nodeMasterHostName = null;
    private int nodeMasterPort = 0;
    private NameIdPair nodeMasterNameId = NameIdPair.NULL;

    public MasterStatus(NameIdPair nameIdPair) {
        this.nameIdPair = nameIdPair;
    }

    /**
     * Returns a read-only snapshot of the object.
     */
    @Override
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            assert(false);
        }
        return null;
    }

    /**
     * Returns true if it's the master from the Group's perspective
     */
    public synchronized boolean isGroupMaster() {
        final int id = nameIdPair.getId();
        return (id != NameIdPair.NULL_NODE_ID) &&
            (id == groupMasterNameId.getId());
    }

    /**
     * Returns true if it's the master from the node's localized perspective
     */
    public synchronized boolean isNodeMaster() {
        final int id = nameIdPair.getId();
        return (id != NameIdPair.NULL_NODE_ID) &&
            (id == nodeMasterNameId.getId());
    }

    public synchronized void setGroupMaster(String hostname,
                                            int port,
                                            NameIdPair newGroupMasterNameId) {
        groupMasterHostName = hostname;
        groupMasterPort = port;
        groupMasterNameId = newGroupMasterNameId;
    }

    /**
     * Predicate to determine whether the group and node have a consistent
     * notion of the Master.
     *
     * @return false if the node does not know of a Master, or the group Master
     * is different from the node's notion the master.
     */

    public synchronized boolean inSync() {
        return !nodeMasterNameId.hasNullId() &&
               (groupMasterNameId.getId() == nodeMasterNameId.getId());
    }

    public synchronized void unSync() {
        nodeMasterHostName = null;
        nodeMasterPort = 0;
        nodeMasterNameId = NameIdPair.NULL;
    }

    /**
     * An assertion form of the above. By combining the check and exception
     * generation in an atomic operation, it provides for an accurate exception
     * message.
     *
     * @throws MasterSyncException
     */
    public synchronized void assertSync()
        throws MasterSyncException {

        if (!inSync()) {
            throw new MasterSyncException();
        }
    }

    /**
     * Syncs to the group master
     */
    public synchronized void sync() {
        nodeMasterHostName = groupMasterHostName;
        nodeMasterPort = groupMasterPort;
        nodeMasterNameId = groupMasterNameId;
    }

    /**
     * Returns the Node's current idea of the Master. It may be "out of sync"
     * with the Group's notion of the Master
     */
    public synchronized InetSocketAddress getNodeMaster() {
        if (nodeMasterHostName == null) {
            return null;
        }
        return new InetSocketAddress(nodeMasterHostName, nodeMasterPort);
    }

    public synchronized NameIdPair getNodeMasterNameId() {
        return nodeMasterNameId;
    }

    /**
     * Returns a socket that can be used to communicate with the group master.
     * It can return null, if there is no current group master, that is,
     * groupMasterNameId is NULL.
     */
    public synchronized InetSocketAddress getGroupMaster() {
        if (groupMasterHostName == null) {
             return null;
        }
        return new InetSocketAddress(groupMasterHostName, groupMasterPort);
    }

    public synchronized NameIdPair getGroupMasterNameId() {
        return groupMasterNameId;
    }

    @SuppressWarnings("serial")
    public class MasterSyncException extends Exception {
        private final NameIdPair savedGroupMasterId;
        private final NameIdPair savedNodeMasterId;

        MasterSyncException () {
            savedGroupMasterId = MasterStatus.this.getGroupMasterNameId();
            savedNodeMasterId = MasterStatus.this.getNodeMasterNameId();
        }

        @Override
        public String getMessage() {
            return "Master change. Node master id: " + savedNodeMasterId +
            " Group master id: " + savedGroupMasterId;
        }
    }
}
