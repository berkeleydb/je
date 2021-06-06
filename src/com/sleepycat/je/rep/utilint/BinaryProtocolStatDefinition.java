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

package com.sleepycat.je.rep.utilint;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for each BinaryProtocol statistics.
 */
public class BinaryProtocolStatDefinition {

    public static final String GROUP_NAME = "BinaryProtocol";
    public static final String GROUP_DESC =
        "Network traffic due to the replication stream.";

    public static final String N_READ_NANOS_NAME =
        "nReadNanos";
    public static final String N_READ_NANOS_DESC =
        "The number of nanoseconds spent reading from the network channel.";
    public static final StatDefinition N_READ_NANOS =
        new StatDefinition(
            N_READ_NANOS_NAME,
            N_READ_NANOS_DESC);

    public static final String N_WRITE_NANOS_NAME =
        "nWriteNanos";
    public static final String N_WRITE_NANOS_DESC =
        "The number of nanoseconds spent writing to the network channel.";
    public static final StatDefinition N_WRITE_NANOS =
        new StatDefinition(
            N_WRITE_NANOS_NAME,
            N_WRITE_NANOS_DESC);

    public static final String N_BYTES_READ_NAME =
        "nBytesRead";
    public static final String N_BYTES_READ_DESC =
        "The number of bytes of Replication Stream read over the network. It " +
            "does not include the TCP/IP overhead.";
    public static final StatDefinition N_BYTES_READ =
        new StatDefinition(
            N_BYTES_READ_NAME,
            N_BYTES_READ_DESC);

    public static final String N_MESSAGES_READ_NAME =
        "nMessagesRead";
    public static final String N_MESSAGES_READ_DESC =
        "The number of Replication Stream messages read over the network.";
    public static final StatDefinition N_MESSAGES_READ =
        new StatDefinition(
            N_MESSAGES_READ_NAME,
            N_MESSAGES_READ_DESC);

    public static final String N_BYTES_WRITTEN_NAME =
        "nBytesWritten";
    public static final String N_BYTES_WRITTEN_DESC =
        "The number of Replication Stream bytes written over the network.";
    public static final StatDefinition N_BYTES_WRITTEN =
        new StatDefinition(
            N_BYTES_WRITTEN_NAME,
            N_BYTES_WRITTEN_DESC);

    public static final String N_MESSAGES_WRITTEN_NAME =
        "nMessagesWritten";
    public static final String N_MESSAGES_WRITTEN_DESC =
        "The total number of Replication Stream messages written over the " +
            "network.";
    public static final StatDefinition N_MESSAGES_WRITTEN =
        new StatDefinition(
            N_MESSAGES_WRITTEN_NAME,
            N_MESSAGES_WRITTEN_DESC);

    public static final String N_MESSAGES_BATCHED_NAME =
        "nMessagesBatched";
    public static final String N_MESSAGES_BATCHED_DESC =
        "The number of Replication Stream messages that were batched into " +
            "larger network level writes instead of being written " +
            "individually (a subset of N_MESSAGES_WRITTEN.)";
    public static final StatDefinition N_MESSAGES_BATCHED =
        new StatDefinition(
            N_MESSAGES_BATCHED_NAME,
            N_MESSAGES_BATCHED_DESC);

    public static final String N_MESSAGE_BATCHES_NAME =
        "nMessageBatches";
    public static final String N_MESSAGE_BATCHES_DESC =
        "The number of message batches written.";
    public static final StatDefinition N_MESSAGE_BATCHES =
        new StatDefinition(
            N_MESSAGE_BATCHES_NAME,
            N_MESSAGE_BATCHES_DESC);

    public static final String MESSAGE_READ_RATE_NAME =
        "messagesReadPerSecond";
    public static final String MESSAGE_READ_RATE_DESC =
        "Incoming message throughput.";
    public static final StatDefinition MESSAGE_READ_RATE =
        new StatDefinition(
            MESSAGE_READ_RATE_NAME,
            MESSAGE_READ_RATE_DESC);

    public static final String MESSAGE_WRITE_RATE_NAME =
        "messagesWrittenPerSecond";
    public static final String MESSAGE_WRITE_RATE_DESC =
        "Outgoing message throughput.";
    public static final StatDefinition MESSAGE_WRITE_RATE =
        new StatDefinition(
            MESSAGE_WRITE_RATE_NAME,
            MESSAGE_WRITE_RATE_DESC);

    public static final String BYTES_READ_RATE_NAME =
        "bytesReadPerSecond";
    public static final String BYTES_READ_RATE_DESC =
        "Bytes read throughput.";
    public static final StatDefinition BYTES_READ_RATE =
        new StatDefinition(
            BYTES_READ_RATE_NAME,
            BYTES_READ_RATE_DESC);

    public static final String BYTES_WRITE_RATE_NAME =
        "bytesWrittenPerSecond";
    public static final String BYTES_WRITE_RATE_DESC =
        "Bytes written throughput.";
    public static final StatDefinition BYTES_WRITE_RATE =
        new StatDefinition(
            BYTES_WRITE_RATE_NAME,
            BYTES_WRITE_RATE_DESC);

    public static final String N_ACK_MESSAGES_NAME =
        "nAckMessages";
    public static final String N_ACK_MESSAGES_DESC =
        "Count of all singleton ACK messages.";
    public static final StatDefinition N_ACK_MESSAGES =
        new StatDefinition(
            N_ACK_MESSAGES_NAME,
            N_ACK_MESSAGES_DESC);

    public static final String N_GROUP_ACK_MESSAGES_NAME =
        "nGroupAckMessages";
    public static final String N_GROUP_ACK_MESSAGES_DESC =
        "Count of all group ACK messages.";
    public static final StatDefinition N_GROUP_ACK_MESSAGES =
        new StatDefinition(
            N_GROUP_ACK_MESSAGES_NAME,
            N_GROUP_ACK_MESSAGES_DESC);

    public static final String N_MAX_GROUPED_ACKS_NAME =
        "nMaxGroupedAcks";
    public static final String N_MAX_GROUPED_ACKS_DESC =
        "Max number of acks sent via a single group ACK message.";
    public static final StatDefinition N_MAX_GROUPED_ACKS =
        new StatDefinition(
            N_MAX_GROUPED_ACKS_NAME,
            N_MAX_GROUPED_ACKS_DESC);

    public static final String N_GROUPED_ACKS_NAME =
        "nGroupedAcks";
    public static final String N_GROUPED_ACKS_DESC =
        "Sum of all acks sent via group ACK messages.";
    public static final StatDefinition N_GROUPED_ACKS =
        new StatDefinition(
            N_GROUPED_ACKS_NAME,
            N_GROUPED_ACKS_DESC);

    public static final String N_ENTRIES_WRITTEN_OLD_VERSION_NAME =
        "nEntriesOldVersion";
    public static final String N_ENTRIES_WRITTEN_OLD_VERSION_DESC =
        "The number of messages containing log entries that were written to " +
            "the replication stream using the previous log format, to support" +
            " replication to a replica running an earlier version during an " +
            "upgrade.";
    public static final StatDefinition N_ENTRIES_WRITTEN_OLD_VERSION =
        new StatDefinition(
            N_ENTRIES_WRITTEN_OLD_VERSION_NAME,
            N_ENTRIES_WRITTEN_OLD_VERSION_DESC);
}
