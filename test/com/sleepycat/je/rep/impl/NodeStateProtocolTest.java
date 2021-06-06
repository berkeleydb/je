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

import org.junit.Before;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.TextProtocol.Message;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;

/**
 * Tests the protocols used to querying the current state of a replica.
 */
public class NodeStateProtocolTest extends TextProtocolTestBase {

    private NodeStateProtocol protocol;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        protocol =
            new NodeStateProtocol(GROUP_NAME,
                                  new NameIdPair("n1", (short) 1),
                                  null,
                                  DataChannelFactoryBuilder.construct(
                                      RepTestUtils.readRepNetConfig()));
    }

    @Override
    protected Message[] createMessages() {
        Message[] messages = new Message[] {
            protocol.new NodeStateRequest(NODE_NAME),
            protocol.new NodeStateResponse(NODE_NAME,
                                           NODE_NAME,
                                           System.currentTimeMillis(),
                                           State.MASTER)
        };

        return messages;
    }

    @Override
    protected TextProtocol getProtocol() {
        return protocol;
    }
}
