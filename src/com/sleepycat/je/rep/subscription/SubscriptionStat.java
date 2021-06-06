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

import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object to represent subscription statistics
 */
public class SubscriptionStat {

    /*
     * VLSN from which feeder agrees to stream log entries, it is returned from
     * the feeder and can be equal to or earlier than the VLSN requested by the
     * client, which is specified in subscription configuration.
     */
    private VLSN startVLSN;

    /* the last VLSN that has been processed */
    private VLSN highVLSN;

    /* used by main thread: # of retries to insert msgs into input queue */
    private final LongStat nReplayQueueOverflow;
    /* used by main thread: # of msgs received from feeder */
    private final LongStat nMsgReceived;
    /* used by main thread: max # of items pending in input queue */
    private final LongStat maxPendingInput;
    /* used by output thread: # of acks sent to feeder */
    private final LongStat nMsgResponded;
    /* used by input thread: # of data ops processed */
    private final LongStat nOpsProcessed;
    /* used by input thread: # of txn aborted and committed */
    private final LongStat nTxnAborted;
    private final LongStat nTxnCommitted;

    SubscriptionStat() {

        startVLSN = VLSN.NULL_VLSN;

        /* initialize statistics */
        StatGroup stats = new StatGroup("subscription",
                                        "subscription " + "statistics");
        nReplayQueueOverflow = new LongStat(stats,
                SubscriptionStatDefinition.SUB_N_REPLAY_QUEUE_OVERFLOW, 0L);
        nMsgReceived = new LongStat(stats,
                SubscriptionStatDefinition.SUB_MSG_RECEIVED, 0L);
        nMsgResponded = new LongStat(stats,
                SubscriptionStatDefinition.SUB_MSG_RESPONDED, 0L);
        maxPendingInput = new LongStat(stats,
                SubscriptionStatDefinition.SUB_MAX_PENDING_INPUT, 0L);

        nOpsProcessed = new LongStat(stats,
                SubscriptionStatDefinition.SUB_OPS_PROCESSED, 0L);
        nTxnAborted = new LongStat(stats,
                SubscriptionStatDefinition.SUB_TXN_ABORTED, 0L);
        nTxnCommitted = new LongStat(stats,
                SubscriptionStatDefinition.SUB_TXN_COMMITTED, 0L);
        
    }

    /*--------------*/
    /*-  Getters   -*/
    /*--------------*/
    public synchronized LongStat getNumReplayQueueOverflow() {
        return nReplayQueueOverflow;
    }
    
    public synchronized LongStat getMaxPendingInput() {
        return maxPendingInput;
    }
    
    public synchronized LongStat getNumMsgResponded() {
        return nMsgResponded;
    }

    public synchronized LongStat getNumMsgReceived() {
        return nMsgReceived;
    }

    public synchronized LongStat getNumOpsProcessed() {
        return nOpsProcessed;
    }

    public synchronized LongStat getNumTxnAborted() {
        return nTxnAborted;
    }

    public synchronized LongStat getNumTxnCommitted() {
        return nTxnCommitted;
    }

    public synchronized VLSN getStartVLSN() {
        return startVLSN;
    }

    public synchronized VLSN getHighVLSN() {
        return highVLSN;
    }

    /*--------------*/
    /*-  Setters   -*/
    /*--------------*/
    public synchronized void setStartVLSN(VLSN vlsn) {
        startVLSN = vlsn;
    }

    public synchronized void setHighVLSN(VLSN vlsn) {
        highVLSN = vlsn;
    }
}
