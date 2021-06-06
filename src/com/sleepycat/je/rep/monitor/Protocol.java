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

import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.TextProtocol;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.monitor.GroupChangeEvent.GroupChangeType;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.net.DataChannelFactory;

/**
 * @hidden
 * For internal use only.
 *
 * Defines the protocol used by the Monitor to keep informed about group
 * changes, and a node joins/leaves the group. The Master uses the protocol to
 * inform all Monitors about group change and node join/leave change.
 *
 * GCHG -> no response expected from the monitor.
 * JoinGroup -> no response expected from the monitor.
 * LeaveGroup -> no response expected from the monitor.
 */
public class Protocol extends TextProtocol {

    /** The latest protocol version. */
    public static final String VERSION = "2.0";

    /** The protocol version introduced to support RepGroupImpl version 3. */
    static final String REP_GROUP_V3_VERSION = "2.0";

    /** The protocol version used with RepGroupImpl version 2. */
    static final String REP_GROUP_V2_VERSION = "1.0";

    /* The messages defined by this class. */
    public final MessageOp GROUP_CHANGE_REQ =
        new MessageOp("GCHG", GroupChange.class);

    public final MessageOp JOIN_GROUP_REQ =
        new MessageOp("JG", JoinGroup.class);

    public final MessageOp LEAVE_GROUP_REQ =
        new MessageOp("LG", LeaveGroup.class);

    /**
     * Creates an instance of this class using the current protocol version.
     */
    public Protocol(String groupName, NameIdPair nameIdPair, RepImpl repImpl,
                    DataChannelFactory channelFactory) {
        this(VERSION, groupName, nameIdPair, repImpl, channelFactory);
    }

    /**
     * Creates an instance of this class using the specified protocol version.
     */
    Protocol(String version,
             String groupName,
             NameIdPair nameIdPair,
             RepImpl repImpl,
             DataChannelFactory channelFactory) {

        super(version, groupName, nameIdPair, repImpl, channelFactory);

        initializeMessageOps(new MessageOp[] {
            GROUP_CHANGE_REQ,
            JOIN_GROUP_REQ,
            LEAVE_GROUP_REQ
        });

        setTimeouts(repImpl,
                    RepParams.MONITOR_OPEN_TIMEOUT,
                    RepParams.MONITOR_READ_TIMEOUT);
    }

    private abstract class ChangeEvent extends RequestMessage {
        /* Name of node which this change event happens on. */
        private final String nodeName;

        public ChangeEvent(String nodeName) {
            this.nodeName = nodeName;
        }

        public ChangeEvent(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            nodeName = nextPayloadToken();
        }

        public String getNodeName() {
            return nodeName;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + nodeName + SEPARATOR;
        }
    }

    public class GroupChange extends ChangeEvent {
        private final RepGroupImpl group;
        /* Represents it's a ADD or REMOVE change event. */
        private final GroupChangeType opType;

        public GroupChange(RepGroupImpl group,
                           String nodeName,
                           GroupChangeType opType) {
            super(nodeName);
            this.group = group;
            this.opType = opType;
        }

        public GroupChange(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            opType = GroupChangeType.valueOf(nextPayloadToken());
            group = RepGroupImpl.deserializeHex
                (tokens, getCurrentTokenPosition());
        }

        public RepGroupImpl getGroup() {
            return group;
        }

        public GroupChangeType getOpType() {
            return opType;
        }

        @Override
        public MessageOp getOp() {
            return GROUP_CHANGE_REQ;
        }

        @Override
        public String wireFormat() {
            final int repGroupVersion =
                (Double.parseDouble(sendVersion) <=
                 Double.parseDouble(REP_GROUP_V2_VERSION)) ?
                RepGroupImpl.FORMAT_VERSION_2 :
                RepGroupImpl.MAX_FORMAT_VERSION;
            return super.wireFormat() +
                   opType.toString() + SEPARATOR +
                   group.serializeHex(repGroupVersion);
        }
    }

    private abstract class MemberEvent extends ChangeEvent {
        private final String masterName;
        private final long joinTime;

        public MemberEvent(String nodeName, String masterName, long joinTime) {
            super(nodeName);
            this.masterName = masterName;
            this.joinTime = joinTime;
        }

        public MemberEvent(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            masterName = nextPayloadToken();
            joinTime = Long.parseLong(nextPayloadToken());
        }

        public long getJoinTime() {
            return joinTime;
        }

        public String getMasterName() {
            return masterName;
        }

        @Override
        public String wireFormat() {
            return super.wireFormat() +
                   masterName + SEPARATOR +
                   Long.toString(joinTime);
        }
    }

    /* Represents the event that a node joins the group. */
    public class JoinGroup extends MemberEvent {
        public JoinGroup(String nodeName, String masterName, long joinTime) {
            super(nodeName, masterName, joinTime);
        }

        public JoinGroup(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
        }

        @Override
        public MessageOp getOp() {
            return JOIN_GROUP_REQ;
        }
    }

    /* Represents the event that a node leaves the group. */
    public class LeaveGroup extends MemberEvent {
        private final LeaveReason leaveReason;
        private final long leaveTime;

        public LeaveGroup(String nodeName,
                          String masterName,
                          LeaveReason leaveReason,
                          long joinTime,
                          long leaveTime) {
            super(nodeName, masterName, joinTime);
            this.leaveReason = leaveReason;
            this.leaveTime = leaveTime;
        }

        public LeaveGroup(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            leaveReason = LeaveReason.valueOf(nextPayloadToken());
            leaveTime = Long.parseLong(nextPayloadToken());
        }

        public LeaveReason getLeaveReason() {
            return leaveReason;
        }

        public long getLeaveTime() {
            return leaveTime;
        }

        @Override
        public MessageOp getOp() {
            return LEAVE_GROUP_REQ;
        }

        @Override
        public String wireFormat() {
            return super.wireFormat() + SEPARATOR +
                   leaveReason.toString() + SEPARATOR +
                   Long.toString(leaveTime);
        }
    }
}
