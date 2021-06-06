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
package com.sleepycat.je.rep.subscription;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThreadBase;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.BaseProtocol.HeartbeatResponse;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object of the output thread created by subscription to respond the
 * heartbeat ping from feeder
 */
class SubscriptionOutputThread extends ReplicaOutputThreadBase {

    /* handle to statistics */
    private final SubscriptionStat stats;
    private final SubscriptionAuthHandler authenticator;
    private final SubscriptionThread parentThread;

    SubscriptionOutputThread(SubscriptionThread parentThread,
                             RepImpl repImpl,
                             BlockingQueue<Long> outputQueue,
                             Protocol protocol,
                             DataChannel replicaFeederChannel,
                             SubscriptionAuthHandler authenticator,
                             SubscriptionStat stats) {
        super(repImpl, outputQueue, protocol, replicaFeederChannel);
        this.parentThread = parentThread;
        this.authenticator = authenticator;
        this.stats = stats;
    }

    /**
     * Implements the reauthentication response for output thread. It sends
     * token to server which would conduct security check for the subscriber
     * with the new token.
     *
     * @throws ReplicationSecurityException if fail to obtain a new login
     * token by renewal or reauthentication;
     * @throws IOException if fail to write reauth message to channel.
     */
    @Override
    public void writeReauthentication()
        throws ReplicationSecurityException, IOException {

        if (authenticator != null && authenticator.hasNewToken()) {

            Protocol.ReAuthenticate response =
                protocol.new ReAuthenticate(authenticator.getToken());

            protocol.write(response, replicaFeederChannel);
        }
    }

    /**
     * Implements the heartbeat response for output thread
     *
     * @param txnId  txn id
     * @throws IOException if fail to write heartbeat message to channel
     */
    @Override
    public void writeHeartbeat(Long txnId) throws IOException {

        /* report the most recently received VLSN to feeder */
        HeartbeatResponse response =
            protocol.new HeartbeatResponse(VLSN.NULL_VLSN,
                                           stats.getHighVLSN());

        protocol.write(response, replicaFeederChannel);
        stats.getNumMsgResponded().increment();
    }
}
