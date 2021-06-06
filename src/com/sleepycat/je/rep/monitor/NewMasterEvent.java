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

package com.sleepycat.je.rep.monitor;

import java.net.InetSocketAddress;

import com.sleepycat.je.rep.elections.MasterValue;

/**
 * The event generated upon detecting a new Master. A new instance of this
 * event is generated each time a new master is elected for the group.
 */
public class NewMasterEvent extends MemberChangeEvent {
    /* The node ID identifying the master node. */
    private final MasterValue masterValue;

    NewMasterEvent(MasterValue masterValue) {
        super(masterValue.getNodeName(), masterValue.getNodeName());
        this.masterValue = masterValue;
    }

    /**
     * Returns the socket address associated with the new master
     */
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(masterValue.getHostName(),
                                     masterValue.getPort());
    }
    
    @Override
    public String toString() {
        return getNodeName() + " is new master";
    }
}
