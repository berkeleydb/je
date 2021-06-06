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
package com.sleepycat.je.rep.arbiter.impl;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThreadBase;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.utilint.VLSN;

/**
 * The ArbiterOutputThread reads transaction identifiers
 * from the outputQueue and writes a acknowledgment
 * response to to the network channel. Also used
 * to write responses for heart beat messages.
 */
public class ArbiterOutputThread extends ReplicaOutputThreadBase {
    private final ArbiterVLSNTracker vlsnTracker;

    public ArbiterOutputThread(RepImpl repImpl,
                               BlockingQueue<Long> outputQueue,
                               Protocol protocol,
                               DataChannel replicaFeederChannel,
                               ArbiterVLSNTracker vlsnTracker) {
        super(repImpl, outputQueue, protocol, replicaFeederChannel);
        this.vlsnTracker = vlsnTracker;
    }

    public void writeHeartbeat(Long txnId) throws IOException {
        VLSN vlsn = vlsnTracker.get();
        protocol.write(protocol.new HeartbeatResponse
                (VLSN.NULL_VLSN,
                 vlsn),
                 replicaFeederChannel);
    }

    @Override
    public void writeReauthentication() throws IOException {
    }
}
