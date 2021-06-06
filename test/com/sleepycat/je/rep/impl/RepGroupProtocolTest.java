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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.RepGroupProtocol.EnsureNode;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupRequest;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupResponse;
import com.sleepycat.je.rep.impl.RepGroupProtocol.TransferMaster;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.Message;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;

/**
 * Tests the protocols used to maintain the rep group and support the Monitor.
 */
public class RepGroupProtocolTest extends TextProtocolTestBase {

    private RepGroupProtocol protocol;
    private DataChannelFactory channelFactory;

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig(), GROUP_NAME);

        protocol =
            new RepGroupProtocol(GROUP_NAME,
                                 new NameIdPair("n1", (short) 1),
                                 null,
                                 channelFactory);
    }

    @Override
    protected Message[] createMessages() {
        try {
            final List<Message> m = new ArrayList<Message>();
            final EnsureNode ensureNode =
                protocol.new EnsureNode(
                    new RepNodeImpl(new NameIdPair(NODE_NAME, 1),
                                    NodeType.MONITOR,
                                    "localhost",
                                    5000,
                                    null));
            m.add(ensureNode);
            m.add(protocol.new EnsureOK(
                      ensureNode, new NameIdPair(NODE_NAME, 1)));
            m.add(protocol.new RemoveMember("m1"));
            final GroupRequest groupRequest =
                protocol.new GroupRequest();
            m.add(groupRequest);
            m.add(protocol.new GroupResponse(
                      groupRequest, RepTestUtils.createTestRepGroup(5, 5)));
            m.add(protocol.new Fail(
                      RepGroupProtocol.FailReason.DEFAULT, "failed"));
            m.add(protocol.new UpdateAddress("test", "localhost", 5001));
            final TransferMaster transferMaster =
                protocol.new TransferMaster("mercury,venus,mars",
                                            10000, false);
            m.add(transferMaster);
            m.add(protocol.new TransferOK(transferMaster, "venus"));
            m.add(protocol.new DeleteMember("m1"));
            return m.toArray(new Message[m.size()]);
        } catch (UnknownHostException e) {
            fail("Unexpected exception: " + e.getStackTrace());
            return null;
        }
    }

    @Override
    protected TextProtocol getProtocol() {
        return protocol;
    }

    /**
     * Test protocol changes to support RepGroupImpl version 3 for EnsureNode.
     */
    @Test
    public void testRepGroupImplV3EnsureNode()
        throws InvalidMessageException {

        /* Node with JE version info */
        final RepNodeImpl newNode = new RepNodeImpl(
            new NameIdPair("m1", 1), NodeType.MONITOR, "localhost", 5000,
            JEVersion.CURRENT_VERSION);

        /* Old protocol using old RepGroupImpl version 2 format */
        final RepGroupProtocol oldProtocol =
            new RepGroupProtocol(RepGroupProtocol.REP_GROUP_V2_VERSION,
                                 GROUP_NAME, new NameIdPair("n2", 2), null,
                                 channelFactory);

        /* Old node format with no JE version */
        final RepNodeImpl oldNode = new RepNodeImpl(
            new NameIdPair("m1", 1), NodeType.MONITOR, "localhost", 5000,
            null);

        /* Old message format, using new node format, to check conversion */
        final EnsureNode oldEnsureNode = oldProtocol.new EnsureNode(newNode);

        /* Receive old format with old protocol */
        final EnsureNode oldEnsureNodeViaOld =
            (EnsureNode) oldProtocol.parse(oldEnsureNode.wireFormat());
        assertEquals("Old message format via old protocol should use old" +
                     " node format",
                     oldNode, oldEnsureNodeViaOld.getNode());

        /* Receive old format with new protocol */
        final EnsureNode oldEnsureNodeViaNew =
            (EnsureNode) protocol.parse(oldEnsureNode.wireFormat());
        assertEquals("Old message format via new protocol should use old" +
                     " node format",
                     oldNode, oldEnsureNodeViaNew.getNode());

        /* Receive new format with old protocol */
        final EnsureNode newEnsureNode = protocol.new EnsureNode(newNode);
        try {
            oldProtocol.parse(newEnsureNode.wireFormat());
            fail("Expected InvalidMessageException when old protocol" +
                 " receives new format message");
        } catch (InvalidMessageException e) {
            assertEquals("New message format via old protocol should produce" +
                         " a version mismatch",
                         TextProtocol.MessageError.VERSION_MISMATCH,
                         e.getErrorType());
        }
    }

    /**
     * Test protocol changes to support RepGroupImpl version 3 for
     * GroupResponse.
     */
    @Test
    public void testRepGroupImplV3GroupResponse()
        throws InvalidMessageException {

        /* New group format with JE version and new node types */
        final RepNodeImpl newNode = new RepNodeImpl(
            new NameIdPair("m1", 1), NodeType.MONITOR, "localhost", 5000,
            JEVersion.CURRENT_VERSION);
        final RepNodeImpl secondaryNode = new RepNodeImpl(
            new NameIdPair("s1", 2), NodeType.SECONDARY, "localhost", 5001,
            JEVersion.CURRENT_VERSION);
        final RepGroupImpl newGroup = new RepGroupImpl(GROUP_NAME, null);
        final Map<Integer, RepNodeImpl> nodeMap =
            new HashMap<Integer, RepNodeImpl>();
        nodeMap.put(1, newNode);
        nodeMap.put(2, secondaryNode);
        newGroup.setNodes(nodeMap);

        /* Old protocol using old RepGroupImpl version 2 format */
        final RepGroupProtocol oldProtocol =
            new RepGroupProtocol(RepGroupProtocol.REP_GROUP_V2_VERSION,
                                 GROUP_NAME, new NameIdPair("n2", 2), null,
                                 channelFactory);

        /* Old group format with no JE version or new node types */
        final RepNodeImpl oldNode = new RepNodeImpl(
            new NameIdPair("m1", 1), NodeType.MONITOR, "localhost", 5000,
            null);
        final RepGroupImpl oldGroup =
            new RepGroupImpl(GROUP_NAME, newGroup.getUUID(),
                             RepGroupImpl.FORMAT_VERSION_2);
        oldGroup.setNodes(Collections.singletonMap(1, oldNode));

        /* Old message format, using new node format, to check conversion */
        final GroupResponse oldGroupResponse =
            oldProtocol.new GroupResponse(
                oldProtocol.new GroupRequest(), newGroup);

        /* Receive old format with old protocol */
        final GroupResponse oldGroupResponseViaOld =
            (GroupResponse) oldProtocol.parse(oldGroupResponse.wireFormat());
        assertEquals("Old message format via old protocol should use old" +
                     " group format",
                     oldGroup, oldGroupResponseViaOld.getGroup());

        /* Receive old format with new protocol */
        final GroupResponse oldGroupResponseViaNew =
            (GroupResponse) protocol.parse(oldGroupResponse.wireFormat());
        assertEquals("Old message format via new protocol should use old" +
                     " group format",
                     oldGroup, oldGroupResponseViaNew.getGroup());

        /* Receive new format with old protocol */
        final GroupResponse newGroupResponse =
            protocol.new GroupResponse(protocol.new GroupRequest(), newGroup);
        try {
            oldProtocol.parse(newGroupResponse.wireFormat());
            fail("Expected InvalidMessageException when old protocol" +
                 " receives new format message");
        } catch (InvalidMessageException e) {
            assertEquals("New message format via old protocol should produce" +
                         " a version mismatch",
                         TextProtocol.MessageError.VERSION_MISMATCH,
                         e.getErrorType());
        }
    }

    /**
     * Test the message format of InvalidMessageExceptions thrown by
     * TextProtocol for malformed messages.  In particular, test that the
     * exception includes information about the protocol message that caused
     * the failure.  [#23352]
     */
    @Test
    public void testInvalidMessageExceptions()
        throws Exception {

        /* Message with too few tokens */
        String message = "msg-with-too-few-tokens";
        try {
            protocol.parse(message);
            fail("Expected InvalidMessageException");
        } catch (InvalidMessageException e) {
            assertEquals("Missing message op in message: " + message,
                         e.getMessage());
        }

        /* Message version is too high */
        protocol.updateNodeIds(Collections.singleton(1));
        final String badVersion = RepGroupProtocol.VERSION + "99";
        message = badVersion + "|" + GROUP_NAME + "|1|ENREQ|my-payload";
        try {
            protocol.parse(message);
            fail("Expected InvalidMessageException");
        } catch (InvalidMessageException e) {
            assertEquals("Version argument mismatch." +
                         " Expected: " + RepGroupProtocol.VERSION +
                         ", found: " + badVersion +
                         ", in message: " + message,
                         e.getMessage());
        }

        /* Wrong message group name */
        message = RepGroupProtocol.VERSION +
            "|wrong-group-name|1|ENREQ|my-payload";
        try {
            protocol.parse(message);
            fail("Expected InvalidMessageException");
        } catch (InvalidMessageException e) {
            assertEquals("Group name mismatch;" +
                         " this group name: " + GROUP_NAME +
                         ", message group name: wrong-group-name" +
                         ", in message: " + message,
                e.getMessage());
        }

        /* Unknown member */
        message = RepGroupProtocol.VERSION + "|" + GROUP_NAME +
            "|99|ENREQ|my-payload";
        try {
            protocol.parse(message);
            fail("Expected InvalidMessageException");
        } catch (InvalidMessageException e) {
            assertEquals("Sender's member id: 99, message op: ENREQ" +
                         ", was not a member of the group: [1]" +
                         ", in message: " + message,
                         e.getMessage());
        }

        /* Missing payload */
        message = RepGroupProtocol.VERSION + "|" + GROUP_NAME + "|1|ENREQ";
        try {
            protocol.parse(message);
            fail("Expected InvalidMessageException");
        } catch (InvalidMessageException e) {
            assertEquals("Bad format; missing token at position: 4" +
                         ", in message: " + message,
                         e.getMessage());
        }
    }
}
