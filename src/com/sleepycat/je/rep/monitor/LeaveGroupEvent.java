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
 * The event generated when a node leaves the group. A new instance of this 
 * event is generated each time a node leaves the group.
 *
 * The events is generated on a "best effort" basis. It may not be generated if
 * the node leaving the group dies before it has a chance to generate the
 * event, for example, if the process was killed, or the node was unable to
 * communicate with the monitor due to a network problem. The application must
 * be resilient in the face of such missing events.
 */
public class LeaveGroupEvent extends MemberChangeEvent {

    /**
     * The reason for why the node leaves the group.
     */
    public static enum LeaveReason {

        /**
         * Normal replica shutdown.
         */
        NORMAL_SHUTDOWN,

        /**
         * Abnormal termination.
         */
        ABNORMAL_TERMINATION,

        /**
         * Master initiated shutdown.
         */
        MASTER_SHUTDOWN_GROUP
    };

    /** 
     * The time when this node joins the group. 
     */
    private final long joinTime;

    /**
     * The time when this node leaves the group. 
     */
    private final long leaveTime;

    /** 
     * The reason why this node leaves the group. 
     */
    private final LeaveReason leaveReason;

    LeaveGroupEvent(String nodeName,
                    String masterName,
                    LeaveReason leaveReason, 
                    long joinTime, 
                    long leaveTime) {
        super(nodeName, masterName);
        this.joinTime = joinTime;
        this.leaveTime = leaveTime;
        this.leaveReason = leaveReason;
    }

    /**
     * @return the time this node joins the group.
     */
    public Date getJoinTime() {
        return new Date(joinTime);
    }

    /** 
     * Returns the time at which the node left the group.
     */
    public Date getLeaveTime() {
        return new Date(leaveTime);
    }

    /**
     * Returns the reason why the node left the group.
     */
    public LeaveReason getLeaveReason() {
        return leaveReason;
    }
    
    @Override
    public String toString() {
        return "Node " + getNodeName() + " left at " + getLeaveTime() + 
                " because of " + getLeaveReason();
    }
}
