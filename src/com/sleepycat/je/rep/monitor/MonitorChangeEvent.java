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
 * MonitorChangeEvent is the base class for all Monitor events. Its subclasses
 * provide additional event-specific information.
 * <p>
 * See {@link <a href="{@docRoot}/../ReplicationGuide/monitors.html"
 * target="_blank">Replication Guide, Writing Monitor Nodes</a>}
 */
public abstract class MonitorChangeEvent {

    /**
     * The name of the node associated with the event
     */
    private final String nodeName;

    MonitorChangeEvent(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * Returns the name of the node associated with the event.
     */
    public String getNodeName() {
        return nodeName;
    }
}
