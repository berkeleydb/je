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
package com.sleepycat.je.rep.impl;

import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;

/**
 * Defines the protocol used in support of group membership.
 *
 * API to Master
 *   ENSURE_NODE -> ENSURE_OK | FAIL
 *   REMOVE_MEMBER -> OK | FAIL
 *   TRANSFER_MASTER -> TRANSFER_OK | FAIL
 *   DELETE_MEMBER -> OK | FAIL
 *
 * Monitor to Master
 *   GROUP_REQ -> GROUP | FAIL
 */
public class RepGroupProtocol extends TextProtocol {

    /** The current protocol version. */
    public static final String VERSION = "4";

    /** The protocol version introduced to support RepGroupImpl version 3. */
    public static final String REP_GROUP_V3_VERSION = "4";

    /** The protocol version used with RepGroupImpl version 2. */
    public static final String REP_GROUP_V2_VERSION = "3";

    /**
     * Used during testing: A non-null value overrides the actual protocol
     * version.
     */
    private static volatile String testCurrentVersion = null;

    public static enum FailReason {
        DEFAULT, MEMBER_NOT_FOUND, IS_MASTER, IS_REPLICA, TRANSFER_FAIL,
        MEMBER_ACTIVE;
    }

    /* The messages defined by this class. */

    public final MessageOp ENSURE_NODE =
        new MessageOp("ENREQ", EnsureNode.class);
    public final MessageOp ENSURE_OK =
        new MessageOp("ENRESP", EnsureOK.class);
    public final MessageOp REMOVE_MEMBER =
        new MessageOp("RMREQ", RemoveMember.class);
    public final MessageOp GROUP_REQ =
        new MessageOp("GREQ", GroupRequest.class);
    public final MessageOp GROUP_RESP =
        new MessageOp("GRESP", GroupResponse.class);
    public final MessageOp RGFAIL_RESP =
        new MessageOp("GRFAIL", Fail.class);
    public final MessageOp UPDATE_ADDRESS =
        new MessageOp("UPDADDR", UpdateAddress.class);
    public final MessageOp TRANSFER_MASTER =
        new MessageOp("TMASTER", TransferMaster.class);
    public final MessageOp TRANSFER_OK =
        new MessageOp("TMRESP", TransferOK.class);
    public final MessageOp DELETE_MEMBER =
        new MessageOp("DLREQ", DeleteMember.class);

    /**
     * Creates an instance of this class using the current protocol version.
     */
    public RepGroupProtocol(String groupName,
                            NameIdPair nameIdPair,
                            RepImpl repImpl,
                            DataChannelFactory channelFactory) {

        this(getCurrentVersion(), groupName, nameIdPair, repImpl,
             channelFactory);
    }

    /**
     * Creates an instance of this class using the specified protocol version.
     */
    RepGroupProtocol(String version,
                     String groupName,
                     NameIdPair nameIdPair,
                     RepImpl repImpl,
                     DataChannelFactory channelFactory) {

        super(version, groupName, nameIdPair, repImpl, channelFactory);

        this.initializeMessageOps(new MessageOp[] {
                ENSURE_NODE,
                ENSURE_OK,
                REMOVE_MEMBER,
                GROUP_REQ,
                GROUP_RESP,
                RGFAIL_RESP,
                UPDATE_ADDRESS,
                TRANSFER_MASTER,
                TRANSFER_OK,
                DELETE_MEMBER
        });

        setTimeouts(repImpl,
                    RepParams.REP_GROUP_OPEN_TIMEOUT,
                    RepParams.REP_GROUP_READ_TIMEOUT);
    }

    /** Get the current version, supporting a test override. */
    public static String getCurrentVersion() {
        return (testCurrentVersion != null) ? testCurrentVersion : VERSION;
    }

    /**
     * Set the current version to a different value, for testing.  Specifying
     * {@code null} reverts to the standard value.
     */
    public static void setTestVersion(final String testVersion) {
        testCurrentVersion = testVersion;
    }

    /**
     * Returns the RepGroupImpl format version to use for the specified
     * RepGroupProtocol version.
     */
    private static int getGroupFormatVersion(final String protocolVersion) {
        return (Double.parseDouble(protocolVersion) <=
                Double.parseDouble(REP_GROUP_V2_VERSION)) ?
            RepGroupImpl.FORMAT_VERSION_2 :
            RepGroupImpl.MAX_FORMAT_VERSION;
    }

    private abstract class CommonRequest extends RequestMessage {
        private final String nodeName;

        public CommonRequest(String nodeName) {
            this.nodeName = nodeName;
        }

        public CommonRequest(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            nodeName = nextPayloadToken();
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + nodeName;
        }

        public String getNodeName() {
            return nodeName;
        }
    }

    public class RemoveMember extends CommonRequest {
        public RemoveMember(String nodeName) {
            super(nodeName);
        }

        public RemoveMember(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return REMOVE_MEMBER;
        }
    }

    /**
     * Like RemoveMember, but also deletes the node's entry from the rep group
     * DB.
     */
    public class DeleteMember extends CommonRequest {
        public DeleteMember(String nodeName) {
            super(nodeName);
        }

        public DeleteMember(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
        }

        @Override
        public MessageOp getOp() {
            return DELETE_MEMBER;
        }
    }

    public class TransferMaster extends RequestMessage {
        private final String nodeNameList;
        private final long timeout;
        private final boolean force;

        public TransferMaster(String nodeNameList,
                              long timeout,
                              boolean force) {
            super();
            this.nodeNameList = nodeNameList;
            this.timeout = timeout;
            this.force = force;
        }

        public TransferMaster(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            this.nodeNameList = nextPayloadToken();
            this.timeout = Long.parseLong(nextPayloadToken());
            this.force = Boolean.parseBoolean(nextPayloadToken());
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + nodeNameList +
                SEPARATOR + timeout + SEPARATOR + force;
        }

        @Override
        public MessageOp getOp() {
            return TRANSFER_MASTER;
        }

        public String getNodeNameList() {
            return nodeNameList;
        }

        public long getTimeout() {
            return timeout;
        }

        public boolean getForceFlag() {
            return force;
        }
    }

    public class GroupRequest extends RequestMessage {

        public GroupRequest() {
        }

        public GroupRequest(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);
        }

        @Override
        public MessageOp getOp() {
           return GROUP_REQ;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
           return wireFormatPrefix();
        }
    }

    public class UpdateAddress extends CommonRequest {
        private final String newHostName;
        private final int newPort;

        public UpdateAddress(String nodeName,
                             String newHostName,
                             int newPort) {
            super(nodeName);
            this.newHostName = newHostName;
            this.newPort = newPort;
        }

        public UpdateAddress(String requestLine, String[] tokens)
            throws InvalidMessageException {

            super(requestLine, tokens);
            this.newHostName = nextPayloadToken();
            this.newPort = new Integer(nextPayloadToken());
        }

        @Override
        public MessageOp getOp() {
            return UPDATE_ADDRESS;
        }

        public String getNewHostName() {
            return newHostName;
        }

        public int getNewPort() {
            return newPort;
        }

        @Override
        public String wireFormat() {
            return super.wireFormat() + SEPARATOR + newHostName + SEPARATOR +
                   newPort;
        }
    }

    public class EnsureNode extends RequestMessage {
        final RepNodeImpl node;

        public EnsureNode(RepNodeImpl node) {
            assert node.getType().isMonitor();
            this.node = node;
        }

        public EnsureNode(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            node = RepGroupImpl.hexDeserializeNode(
                nextPayloadToken(), getGroupFormatVersion(sendVersion));
        }

        public RepNodeImpl getNode() {
            return node;
        }

        @Override
        public MessageOp getOp() {
            return ENSURE_NODE;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR +
                   RepGroupImpl.serializeHex(
                       node, getGroupFormatVersion(sendVersion));
        }
    }

    public class EnsureOK extends OK {
        private final NameIdPair nameIdPair;

        public EnsureOK(EnsureNode request, NameIdPair nameIdPair) {
            super(request);
            this.nameIdPair = nameIdPair;
        }

        public EnsureOK(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);
            nameIdPair = new NameIdPair(nextPayloadToken(),
                                        Integer.parseInt(nextPayloadToken()));
        }

        public NameIdPair getNameIdPair() {
            return nameIdPair;
        }

        @Override
        public MessageOp getOp() {
            return ENSURE_OK;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR +
                   nameIdPair.getName() + SEPARATOR +
                   Integer.toString(nameIdPair.getId());
        }
    }

    public class TransferOK extends OK {
        private final String winner;

        public TransferOK(TransferMaster request, String winner) {
            super(request);
            this.winner = winner;
        }

        public TransferOK(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);
            winner = nextPayloadToken();
        }

        public String getWinner() {
            return winner;
        }

        @Override
        public MessageOp getOp() {
            return TRANSFER_OK;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR + winner;
        }
    }

    public class GroupResponse extends ResponseMessage {
        final RepGroupImpl group;

        public GroupResponse(GroupRequest request, RepGroupImpl group) {
            super(request);
            this.group = group;
        }

        public GroupResponse(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            group = RepGroupImpl.deserializeHex
                (tokens, getCurrentTokenPosition());
        }

        public RepGroupImpl getGroup() {
            return group;
        }

        @Override
        public MessageOp getOp() {
            return GROUP_RESP;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {

            /*
             * Use the requested group version, unless it is newer than the
             * current group version.
             */
            int groupFormatVersion = getGroupFormatVersion(sendVersion);
            if (group.getFormatVersion() < groupFormatVersion) {
                groupFormatVersion = group.getFormatVersion();
            }
            return wireFormatPrefix() + SEPARATOR +
                group.serializeHex(groupFormatVersion);
        }
    }

    /**
     * Extends the class Fail, adding a reason code to distinguish amongst
     * different types of failures.
     */
    public class Fail extends TextProtocol.Fail {
        final FailReason reason;

        /**
         * Create a failure response that is not related to a specific request.
         */
        public Fail(FailReason reason, String message) {
            super(message);
            this.reason = reason;
        }

        public Fail(RequestMessage request, FailReason reason, String message) {
            super(request, message);
            this.reason = reason;
        }

        public Fail(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
            reason = FailReason.valueOf(nextPayloadToken());
        }

        @Override
        public MessageOp getOp() {
            return RGFAIL_RESP;
        }

        @Override
        public String wireFormat() {
            return super.wireFormat() + SEPARATOR + reason.toString();
        }

        public FailReason getReason() {
            return reason;
        }
    }
}
