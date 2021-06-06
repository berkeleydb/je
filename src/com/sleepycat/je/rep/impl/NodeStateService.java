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

import java.io.IOException;
import java.util.logging.Logger;

import com.sleepycat.je.rep.impl.NodeStateProtocol.NodeStateRequest;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingService;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingRunnable;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * The service registered by a RepNode to answer the state request from
 * another node. It can also be extended to be used by "Ping" command.
 */
public class NodeStateService extends ExecutingService {

    private final RepNode repNode;
    private final NodeStateProtocol protocol;
    private final Logger logger;

    /* Identifies the Node State querying Service. */
    public static final String SERVICE_NAME = "NodeState";

    public NodeStateService(ServiceDispatcher dispatcher, RepNode repNode) {
        super(SERVICE_NAME, dispatcher);
        this.repNode = repNode;

        String groupName =
            repNode.getRepImpl().cloneRepConfig().getGroupName();
        protocol = new NodeStateProtocol
            (groupName, repNode.getNameIdPair(), repNode.getRepImpl(),
             dispatcher.getChannelFactory());
        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Process a node state querying request.
     */
    @SuppressWarnings("unused")
    public ResponseMessage process(NodeStateRequest stateRequest) {
        long joinTime = repNode.getMonitorEventManager().getJoinTime();
        return protocol.new NodeStateResponse(repNode.getNodeName(),
                                              repNode.getMasterName(),
                                              joinTime,
                                              repNode.getRepImpl().getState());
    }

    @Override
    public Runnable getRunnable(DataChannel dataChannel) {
        return new NodeStateServiceRunnable(dataChannel, protocol);
    }

    class NodeStateServiceRunnable extends ExecutingRunnable {
        NodeStateServiceRunnable(DataChannel dataChannel,
                                 NodeStateProtocol protocol) {
            super(dataChannel, protocol, true);
        }

        @Override
        protected ResponseMessage getResponse(RequestMessage request)
            throws IOException {

            return protocol.process(NodeStateService.this, request);
        }

        @Override
        protected void logMessage(String message) {
            LoggerUtils.warning(logger, repNode.getRepImpl(), message);
        }
    }
}
