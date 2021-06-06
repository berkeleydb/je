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

/**
 * The different types of nodes that can be in a replication group.
 */
public enum NodeType {

    /**
     * A node that passively listens for the results of elections, but does not
     * participate in them. It does not have a replicated environment
     * associated with it.
     * @see com.sleepycat.je.rep.monitor.Monitor
     */
    MONITOR {
        @Override
        public boolean isMonitor() {
            return true;
        }
    },

    /**
     * A full fledged member of the replication group with an associated
     * replicated environment that can serve as both a Master and a Replica.
     */
    ELECTABLE {
        @Override
        public boolean isElectable() {
            return true;
        }
        @Override
        public boolean isDataNode() {
            return true;
        }
    },

    /**
     * A member of the replication group with an associated replicated
     * environment that serves as a Replica but does not participate in
     * elections or durability decisions.  Secondary nodes are only remembered
     * by the group while they maintain contact with the Master.
     *
     * <p>You can use SECONDARY nodes to:
     * <ul>
     * <li>Provide a copy of the data available at a distant location
     * <li>Maintain an extra copy of the data to increase redundancy
     * <li>Change the number of replicas to adjust to dynamically changing read
     *     loads
     * </ul>
     *
     * @since 6.0
     */
    SECONDARY {
        @Override
        public boolean isSecondary() {
            return true;
        }
        @Override
        public boolean isDataNode() {
            return true;
        }
        @Override
        public boolean hasTransientId() {
            return true;
        }
    },

    ARBITER {
        @Override
        public boolean isArbiter() {
            return true;
        }
        @Override
        public boolean isElectable() {
            return true;
        }
    },

    /**
     * @hidden
     * For internal use only.
     *
     * A node that receives replication data, but does not participate in
     * elections or durability decisions, and is not considered a data node
     * and cannot be depended on to maintain a copy of the data.
     *
     * @since 7.2
     */
    EXTERNAL {
        @Override
        public boolean isExternal() {
            return true;
        }
        @Override
        public boolean hasTransientId() {
            return true;
        }
    };

    /**
     * Returns whether this is the {@link #MONITOR} type.
     *
     * @return whether this is {@code MONITOR}
     * @since 6.0
     */
    public boolean isMonitor() {
        return false;
    }

    /**
     * Returns whether this is the {@link #ELECTABLE} type.
     *
     * @return whether this is {@code ELECTABLE}
     * @since 6.0
     */
    public boolean isElectable() {
        return false;
    }

    /**
     * Returns whether this is the {@link #SECONDARY} type.
     *
     * @return whether this is {@code SECONDARY}
     * @since 6.0
     */
    public boolean isSecondary() {
        return false;
    }

    /**
     * Returns whether this type represents a data node, either {@link
     * #ELECTABLE} or {@link #SECONDARY}.
     *
     * @return whether this represents a data node
     * @since 6.0
     */
    public boolean isDataNode() {
        return false;
    }

    /**
     * Returns whether this is the {@link #ARBITER} type.
     *
     * @return whether this is {@code ARBITER}
     * @since 6.0
     */
    public boolean isArbiter() {
        return false;
    }

    /**
     * @hidden
     * For internal use only.
     *
     * Returns whether this is the {@link #EXTERNAL} type.
     *
     * @return whether this is {@code EXTERNAL}
     * @since 7.2
     */
    public boolean isExternal() {
        return false;
    }

    /**
     * @hidden
     * For internal use only
     *
     * Returns whether this node has a transient node ID.  New transient node
     * IDs are assigned each time the node connects to the feeder.
     *
     * @return whether this node has a transient node ID
     * @since 7.2
     */
    public boolean hasTransientId() {
        return false;
    }
}
