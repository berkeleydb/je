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

package com.sleepycat.je.rep.impl.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * The thread used to write responses asynchronously to the network, to avoid
 * network stalls in the replica replay thread. This thread, like the
 * Replica.ReplayThread, is created each time the node establishes contact with
 * a new feeder and starts replaying the log from it.
 *
 * The inputs and outputs of this thread are schematically described as:
 *
 * outputQueue -> ReplicaOutputThread (does write) -> writes to network
 *
 * It's the third component of the three thread structure outlined in the
 * Replica's class level comment.
 */
public abstract class ReplicaOutputThreadBase extends StoppableThread {

    /**
     * The size of the write queue.
     */
    protected final int queueSize;

    /*
     * The heartbeat interval in ms.
     */
    protected final int heartbeatMs;

    /**
     * Thread exit exception. It's non-null if the thread exited due to an
     * exception. It's the responsibility of the main replica thread to
     * propagate the exception across the thread boundary in this case.
     */
    protected volatile Exception exception;

    protected final RepImpl repImpl;

    /*
     * A reference to the common output queue shared with Replay
     */
    protected final BlockingQueue<Long> outputQueue;

    protected final Protocol protocol ;

    protected final DataChannel replicaFeederChannel;

    /*
     * Reserved transaction ids, that don't represent transaction Acks
     * when encountered in the write queue.
     */

    /*
     * Forces the replica thread to exit when encountered in the write
     * queue.
     */
    public final static Long EOF = Long.MAX_VALUE;

    /*
     * Results in a heartbeat response when encountered in the write queue.
     */
    public final static Long HEARTBEAT_ACK = EOF - 1;

    /*
     * Results in a shutdown response when encountered in the write queue.
     */
    public final static Long SHUTDOWN_ACK = EOF - 2;

    private TestHook<Object> outputHook;

    /* Keep the max size below Maximum Segment Size = 1460 bytes. */
    private final static int maxGroupedAcks = (1460 - 100) / 8;

    final ArrayList<Long> groupAcks = new ArrayList<>(maxGroupedAcks);

    protected final boolean groupAcksEnabled;

    protected volatile long numGroupedAcks = 0;

    protected final Logger logger;

    ReplicaOutputThreadBase(RepImpl repImpl) {
        this(repImpl,
             repImpl.getReplay().getOutputQueue(),
             repImpl.getRepNode().getReplica().getProtocol(),
             repImpl.getRepNode().getReplica().getReplicaFeederChannel());
    }

    public ReplicaOutputThreadBase(RepImpl repImpl,
                                   BlockingQueue<Long> outputQueue,
                                   Protocol protocol,
                                   DataChannel replicaFeederChannel) {
        super(repImpl, "ReplicaOutputThread");

        logger = repImpl.getLogger();
        this.repImpl = repImpl;

        this.outputQueue = outputQueue;
        this.protocol = protocol;
        this.replicaFeederChannel = replicaFeederChannel;

        heartbeatMs =
            repImpl.getConfigManager().getInt(RepParams.HEARTBEAT_INTERVAL);

        queueSize = outputQueue.remainingCapacity();

        groupAcksEnabled =
        (protocol.getVersion() > Protocol.VERSION_5) &&
         repImpl.getConfigManager().getBoolean(RepParams.ENABLE_GROUP_ACKS);

    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public Exception getException() {
        return exception;
    }

    public long getNumGroupedAcks() {
        return numGroupedAcks;
    }

    /**
     * For testing only.
     */
    public long getOutputQueueSize() {
        return outputQueue.size();
    }

    public void setOutputHook(TestHook<Object> outputHook) {
        this.outputHook = outputHook;
    }

    @Override
    public void run() {

        /* Max number of pending responses in the output queue. */
        long maxPending = 0;

        /* Number of singleton acks. */
        long numAcks = 0;

        LoggerUtils.info(logger, repImpl,
                         "Replica output thread started. Queue size:" +
                          queueSize +
                          " Max grouped acks:" + maxGroupedAcks);

        try {
            for (Long txnId = outputQueue.poll(heartbeatMs,
                                               TimeUnit.MILLISECONDS);
                 !EOF.equals(txnId);
                 txnId = outputQueue.poll(heartbeatMs,
                                          TimeUnit.MILLISECONDS)) {

                assert TestHookExecute.doHookIfSet(outputHook, this);

                writeReauthentication();

                if ((txnId == null) || HEARTBEAT_ACK.equals(txnId)) {

                    /*
                     * Send a heartbeat if requested, or unsolicited in the
                     * absence of output activity for a heartbeat interval.
                     */
                    writeHeartbeat(txnId);
                    continue;
                } else if (SHUTDOWN_ACK.equals(txnId)) {

                    /*
                     * Acknowledge the shutdown request, the actual shutdown is
                     * processed in the replay thread.
                     */
                    protocol.write(protocol.new ShutdownResponse(),
                                   replicaFeederChannel);
                    continue;
                }

                final int pending = outputQueue.size();
                if (pending > maxPending) {
                    maxPending = pending;
                    if ((maxPending % 100) == 0) {
                        LoggerUtils.info(logger, repImpl,
                                         "Max pending acks:" + maxPending);
                    }
                }

                if ((pending == 0) || (! groupAcksEnabled)) {
                    /* A singleton ack. */
                    numAcks++;
                    protocol.write(protocol.new Ack(txnId),
                                   replicaFeederChannel);
                } else {

                    /*
                     * Have items pending inthe queue and group acks are
                     * enabled.
                     */
                    if (groupWriteAcks(txnId)) {
                        /* At eof */
                        break;
                    }
                }
            }
        } catch (ReplicationSecurityException rse) {
            exception = rse;
            LoggerUtils.info(logger, repImpl,
                             "Output thread exiting due to security error, " +
                             "client: " + rse.getConsumer() +
                             ", error: " + rse.getMessage());
        } catch (Exception e) {
            exception = e;

            /*
             * Get the attention of the main replica thread.
             */
            RepUtils.shutdownChannel(replicaFeederChannel);

            LoggerUtils.info(logger, repImpl,
                             this + "exiting with exception:" + e);
        } finally {
            LoggerUtils.info(logger, repImpl,
                             this + "exited. " +
                                 "Singleton acks sent:" + numAcks +
                                 " Grouped acks sent:" + numGroupedAcks +
                                 " Max pending acks:" + maxPending);
        }
    }

    public abstract void writeReauthentication() throws IOException;

    public abstract void writeHeartbeat(Long txnId) throws IOException;

    /**
     * Writes out the acks that are currently queued in the output queue
     *
     * Returns true if it encountered an EOF or a request for a shutdown.
     */
    protected boolean groupWriteAcks(long txnId)
        throws IOException {

        /* More potential acks, group them. */
        boolean eof = false;
        groupAcks.clear();
        groupAcks.add(txnId);
        outputQueue.drainTo(groupAcks, maxGroupedAcks - 1);
        long txnIds[] = new long[groupAcks.size()];

        int i = 0;
        for (long gtxnId : groupAcks) {
            if (gtxnId == EOF) {
                eof = true;
                break;
            } else if (gtxnId == SHUTDOWN_ACK) {
                protocol.write(protocol.new ShutdownResponse(),
                               replicaFeederChannel);
                eof = true;
                break;
            } else if (gtxnId == HEARTBEAT_ACK) {

                /*
                 * Heartbeat could be out of sequence relative to acks, but
                 * that's ok.
                 */
                writeHeartbeat(gtxnId);
                continue;
            }
            txnIds[i++] = gtxnId;
        }

        if (i > 0) {
            if (txnIds.length > i) {
                long la[] = new long[txnIds.length - 1];
                System.arraycopy(txnIds, 0, la, 0, la.length);
                txnIds = la;
            }

            protocol.write(protocol.new GroupAck(txnIds), replicaFeederChannel);
            numGroupedAcks += txnIds.length;
        }
        return eof;
    }

    @Override
    protected int initiateSoftShutdown() {

        /* Queue EOF to terminate the thread */
        if (! outputQueue.offer(EOF)) {
            /* No room in write queue, resort to an interrupt. */
            return -1;
        }

        /* Wait up to 10 seconds for any queued writes to be flushed out. */
        return 10000;
    }
}
