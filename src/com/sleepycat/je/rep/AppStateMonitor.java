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
 * A mechanism for adding application specific information when asynchronously
 * tracking the state of a running JE HA application.
 * <p>
 * {@link NodeState} provides information about the current state of a member
 * of the replication group. The application can obtain NodeState via {@link
 * com.sleepycat.je.rep.util.ReplicationGroupAdmin#getNodeState} or {@link
 * com.sleepycat.je.rep.util.DbPing#getNodeState}. A NodeState contains mostly
 * JE-centric information, such as whether the node is a master or
 * replica. However, it may be important to add in some application specific
 * information to enable the best use of the status.
 * <p> 
 * For example, an application may want to direct operations to specific nodes
 * based on whether the node is available. The fields in {@link NodeState} will
 * tell the application whether the node is up and available in a JE HA sense,
 * but the application may also need information about an application level
 * resource, which would affect the load balancing decision. The AppStateMonitor
 * is a way for the application to inject this kind of application specific
 * information into the replicated node status.
 * <p>
 * The AppStateMonitor is registered with the replicated environment using
 * {@link ReplicatedEnvironment#registerAppStateMonitor(AppStateMonitor)}.
 * There is at most one AppStateMonitor associated with the actual environment
 * (not an {@link com.sleepycat.je.Environment} handle) at any given time.  JE
 * HA calls {@link AppStateMonitor#getAppState} when it is assembling status
 * information for a given node.
 * <p>
 * After registration, the application can obtain this application specific
 * information along with other JE HA status information when it obtains a
 * {@link NodeState}, through {@link NodeState#getAppState}.
 * <p>
 * {@link AppStateMonitor#getAppState()} returns a byte array whose length
 * should be larger than 0. An IllegalStateException will be thrown if the 
 * returned byte array is 0 size. Users are responsible for serializing and
 * deserializing the desired information into this byte array.
 * @since 5.0
 */
public interface AppStateMonitor {

    /**
     * Return a byte array which holds information about the application's 
     * state. The application is responsible for serialize and deserialize this
     * information.
     * <p>
     * Note the returned byte array's length should be larger than 0.
     *
     * @return the application state 
     */
    public byte[] getAppState();
}
