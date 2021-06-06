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

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Object to represent subscription statistics
 */
class SubscriptionStatDefinition {

    public static final String GROUP_NAME = "Subscription";
    public static final String GROUP_DESC = "Subscription statistics";

    public static final StatDefinition SUB_N_REPLAY_QUEUE_OVERFLOW =
            new StatDefinition(
                "nReplayQueueOverflow",
                "The number inserts into the replay queue that failed " +
                "because the queue was full.");

    public static final StatDefinition SUB_MSG_RECEIVED =
            new StatDefinition(
                "msg_received",
                "The number of messages received from feeder");

    public static final StatDefinition SUB_MSG_RESPONDED =
            new StatDefinition(
                "msg_responded",
                "The number of messages responded to feeder");

    public static final StatDefinition SUB_OPS_PROCESSED =
            new StatDefinition(
                "ops_processed",
                "The number of data operations processed by subscriber");

    public static final StatDefinition SUB_TXN_COMMITTED =
            new StatDefinition(
                "txn_committed",
                "The number of committed transactions received from feeder ");

    public static final StatDefinition SUB_TXN_ABORTED =
            new StatDefinition(
                "txn_aborted",
                "The number of aborted transactions received from feeder ");

    public static final StatDefinition SUB_MAX_PENDING_INPUT =
            new StatDefinition(
                "max_pending_input",
                "The max number of pending items in the input queue");
}
