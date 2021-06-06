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

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Applications can register for Monitor event notification through
 * {@link Monitor#startListener}. The interface defines an overloaded notify
 * event for each event supported by the Monitor.
 * <p>
 * Changes in the composition of the replication group, or in the dynamic state
 * of a member, are communicated to the listener as events that are represented
 * as subclasses of {@link MonitorChangeEvent MonitorChangeEvent}. Classes
 * implementing this interface supply implementations for a <code>notify</code>
 * associated with each type of event, so they can respond with some
 * application-specific course of action.
 * <p>
 * See {@link <a href="{@docRoot}/../ReplicationGuide/monitors.html"
 * target="_blank">Replication Guide, Writing Monitor Nodes</a>}
 */
public interface MonitorChangeListener {

    /**
     * The method is invoked whenever there is new master associated with the
     * replication group.
     *
     * If the method throws an exception, JE will log the exception as a trace
     * message, which will be propagated through the usual channels.
     *
     * @param newMasterEvent the event that resulted in the notify. It
     * identifies the new master.
     */
    public void notify(NewMasterEvent newMasterEvent);

    /**
     * The method is invoked whenever there is a change in the composition of
     * the replication group. That is, a new node has been added to the group
     * or an existing member has been removed from the group. Note that
     * SECONDARY nodes do not produce these events.
     *
     * If the method throws an exception, JE will log the exception as a trace
     * message, which will be propagated through the usual channels.
     *
     * @param groupChangeEvent the event that resulted in the notify. It
     * describes the new group composition and identifies the node that
     * provoked the change.
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public void notify(GroupChangeEvent groupChangeEvent);

    /**
     * The method is invoked whenever a node joins the group, by successfully
     * opening its first
     * {@link ReplicatedEnvironment ReplicatedEnvironment} handle.
     *
     * @param joinGroupEvent the event that resulted in the notify. It
     * identifies the node that joined the group.
     */
    public void notify(JoinGroupEvent joinGroupEvent);

    /**
     * The method is invoked whenever a node leaves the group by closing its
     * last {@link ReplicatedEnvironment ReplicatedEnvironment} handle.
     *
     * @param leaveGroupEvent the event that resulted in the notify. It
     * identifies the node that left the group.
     */
    public void notify(LeaveGroupEvent leaveGroupEvent);
}
