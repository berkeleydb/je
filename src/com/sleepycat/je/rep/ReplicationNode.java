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

import java.net.InetSocketAddress;

/**
 * An administrative view of a node in a replication group.
 */
public interface ReplicationNode {

    /**
     * Returns the unique name associated with the node.
     *
     * @return the name of the node
     */
    String getName();

    /**
     * Returns the type associated with the node.
     *
     * @return the node type
     */
    NodeType getType();

    /**
     * The socket address used by other nodes in the replication group to
     * communicate with this node.
     *
     * @return the socket address
     */
    InetSocketAddress getSocketAddress();

    /**
     * Returns the host name associated with the node.
     *
     * @return the host name of the node
     */
    String getHostName();

    /**
     * Returns the port number associated with the node.
     *
     * @return the port number of the node
     */
    int getPort();
}
