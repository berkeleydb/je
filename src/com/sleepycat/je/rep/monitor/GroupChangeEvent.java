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

/**
 * The event used to track changes to the composition and status of the
 * group. An instance of this event is created each time there is any change to
 * the group.
 */
package com.sleepycat.je.rep.monitor;

import com.sleepycat.je.rep.ReplicationGroup;

/**
 * The event generated when the group composition changes. A new instance of
 * this event is generated each time a node is added or removed from the
 * group. Note that SECONDARY nodes do not generate these events.
 */
/*
 * TODO: EXTERNAL is hidden for now. The doc need updated to include
 * EXTERNAL when it becomes public.
 */
public class GroupChangeEvent extends MonitorChangeEvent {

    /**
     * The kind of GroupChangeEvent.
     */
    public static enum GroupChangeType {

        /**
         * A new node was <code>added</code> to the replication group.
         */
        ADD,

        /**
         * A node was <code>removed</code> from the replication group.
         */
        REMOVE
    };

    /**
     * The latest information about the replication group.
     */
    private final ReplicationGroup repGroup;

    /**
     * The type of this change.
     */
    private final GroupChangeType opType;

    GroupChangeEvent(ReplicationGroup repGroup,
                     String nodeName,
                     GroupChangeType opType) {
        super(nodeName);
        this.repGroup = repGroup;
        this.opType = opType;
    }

    /**
     * Returns the current description of the replication group.
     */
    public ReplicationGroup getRepGroup() {
        return repGroup;
    }

    /**
     * Returns the type of the change (the addition of a new member or the
     * removal of an existing member) made to the group. The method
     * {@link MonitorChangeEvent#getNodeName() MonitorChangeEvent.getNodeName}
     * can be used to identify the node that triggered the event.
     *
     * @return the group change type.
     */
    public GroupChangeType getChangeType() {
        return opType;
    }

    @Override
    public String toString() {
        return "Node " + getNodeName() + " change type=" + getChangeType();
    }
}
