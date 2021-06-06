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

/**
 * MemberChangeEvent is the base class for all member status changed events. 
 * Its subclasses provide additional event-specific information.
 */
public abstract class MemberChangeEvent extends MonitorChangeEvent {

    /**
     * The master name when this event happens.
     */
    private final String masterName;

    MemberChangeEvent(String nodeName, String masterName) {
        super(nodeName);
        this.masterName = masterName;
    }

    /**
     * Returns the name of the master at the time of this event. The return
     * value may be null if there is no current master.
     */
    public String getMasterName() {
        return masterName;
    }
}
