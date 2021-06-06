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

import java.util.Date;

/**
 * The event generated when a node joins the group. A new instance of this 
 * event is generated each time a node joins the group.
 *
 * The event is generated on a "best effort" basis. It may not be generated,
 * for example, if the joining node was unable to communicate with the monitor
 * due to a network problem. The application must be resilient in the face of
 * such missing events.
 */
public class JoinGroupEvent extends MemberChangeEvent {

    /**
     * The time when this node joins the group. 
     */
    private final long joinTime;

    JoinGroupEvent(String nodeName, String masterName, long joinTime) {
        super(nodeName, masterName);
        this.joinTime = joinTime;
    }

    /**
     * Returns the time at which the node joined the group.
     */
    public Date getJoinTime() {
        return new Date(joinTime);
    }
    
    @Override
    public String toString() {
        return "Node " + getNodeName() + " joined at " + getJoinTime();
    }
}
