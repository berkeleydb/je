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

package com.sleepycat.je.rep.elections;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Formatter;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * ElectionAgentThread is the base class for the election agent threads
 * underlying the Acceptor and Learner agents.
 */
public class ElectionAgentThread extends StoppableThread {

    /* The instance of the protocol bound to a specific Value and Proposal */
    protected final Protocol protocol;

    protected final Logger logger;

    /*
     * Used when the unit test AcceptorTest creates a RepNode without a RepIml
     * instance.
     */
    protected final Formatter formatter;

    /*
     * The queue into which the ServiceDispatcher queues socket channels for
     * new Feeder instances.
     */
    protected final BlockingQueue<DataChannel> channelQueue =
        new LinkedBlockingQueue<DataChannel>();

    protected ElectionAgentThread(RepNode repNode,
                                  Protocol protocol,
                                  String threadName) {
        super((repNode == null ? null : repNode.getRepImpl()), threadName);
        this.protocol = protocol;

        logger = (envImpl != null) ?
            LoggerUtils.getLogger(getClass()) :
            LoggerUtils.getLoggerFormatterNeeded(getClass());

        formatter = new ReplicationFormatter(protocol.getNameIdPair());
    }

    protected ElectionAgentThread(EnvironmentImpl envImpl,
            Protocol protocol,
            String threadName) {
        super(envImpl, threadName);
        this.protocol = protocol;

        logger = (envImpl != null) ?
           LoggerUtils.getLogger(getClass()) :
           LoggerUtils.getLoggerFormatterNeeded(getClass());

        formatter = new ReplicationFormatter(protocol.getNameIdPair());
     }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Shuts down the Agent.
     * @throws InterruptedException
     */
    public void shutdown()
        throws InterruptedException{

        if (shutdownDone(logger)) {
            return;
        }
        shutdownThread(logger);
    }

    @Override
    protected int initiateSoftShutdown() {
        channelQueue.clear();
        /* Add special entry so that the channelQueue.poll operation exits. */
        channelQueue.add(RepUtils.CHANNEL_EOF_MARKER);
        return 0;
    }
}
