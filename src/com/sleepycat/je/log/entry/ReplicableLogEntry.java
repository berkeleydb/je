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

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.VersionedWriteLoggable;

/**
 * A sub-interface of {@link LogEntry} that must be implemented by all log
 * entries that can be replicated.  Replicable log entries are all those
 * entries for which the associated {@link LogEntryType}'s {@link
 * LogEntryType#isReplicationPossible} method returns {@code true}.  These are
 * the log entries that can be included in the replication stream distributed
 * from feeders to replicas during replication.  See [#22336].
 *
 * <p>Starting with the release using log version 9, as specified by {@link
 * LogEntryType#LOG_VERSION_REPLICATE_OLDER}, all replicable log entries
 * need to support writing themselves in earlier log formats, to support
 * replication during an upgrade when the master is replicated first.  Any
 * loggable objects that they reference should also implement {@link
 * com.sleepycat.je.log.VersionedWriteLoggable} for the same reason.
 *
 * <p>The {@link #getLastFormatChange} method identifies the log version for
 * which the entry's log format has most recently changed.  This information is
 * used to determine if the current log format is compatible with a
 * non-upgraded replica.
 *
 * <p>The {@link #getSize(int, boolean)} method overloading is used when
 * creating the buffer that will be used to transmit the log entry data in the
 * earlier format.
 *
 * <p>The {@link #writeEntry(ByteBuffer, int, boolean)} method overloading is
 * used to convert the in-memory format of the log entry into the log data in
 * the earlier format.
 *
 * <p>To simplify the implementation of writing log entries in multiple log
 * version formats, a log entry that needs to be written in a previous format
 * will first be read into its in-memory format in the current version, and
 * then written from there to the previous format.
 */
public interface ReplicableLogEntry extends LogEntry {

    /**
     * Returns the log version of the most recent format change for this log
     * entry.
     *
     * @return the log version of the most recent format change
     */
    int getLastFormatChange();

    /**
     * Returns all possible {@link VersionedWriteLoggable} objects that may be
     * embedded in the binary data of this log entry.
     *
     * <p>This is used by tests to ensure that for each X:Y pair, where X is a
     * ReplicableLogEntry and Y is a VersionedWriteLoggable, and X embeds Y
     * either directly or indirectly, X.getLastFormatChange is greater than or
     * equal to Y.getLastFormatChange.
     *
     * <p>Each ReplicableLogEntry and VersionedWriteLoggable class typically
     * has a LAST_FORMAT_CHANGE constant that is returned by its
     * getLastFormatChange method. When bumping this constant for an object X
     * embedded by an log entry Y, Y.LAST_FORMAT_CHANGE should also be set to
     * the minimum of its current value and X.LAST_FORMAT_CHANGE.
     *
     * <p>Enforcing this rule in a general way is made possible by the
     * getEmbeddedLoggables method of each ReplicableLogEntry and
     * VersionedWriteLoggable. Note that this method is not intended to be
     * called outside of tests.
     */
    Collection<VersionedWriteLoggable> getEmbeddedLoggables();

    /**
     * Returns the number of bytes needed to store this entry in the format for
     * the specified log version.  Earlier log versions only need to be
     * supported for log entries with format changes made in {@link
     * LogEntryType#LOG_VERSION_REPLICATE_OLDER} or greater.
     *
     * @param logVersion the log version
     * @param forReplication whether the entry will be sent over the wire,
     * and not written to the log.
     * @return the number of bytes to store this entry for the log version
     */
    int getSize(int logVersion, boolean forReplication);

    /**
     * Serializes this object into the specified buffer in the format for the
     * the specified log version.  Earlier log versions only need to be
     * supported for log entries with format changes made in {@link
     * LogEntryType#LOG_VERSION_REPLICATE_OLDER} or greater.
     *
     * @param logBuffer the destination buffer
     * @param forReplication whether the entry will be sent over the wire,
     * and not written to the log.
     * @param logVersion the log version
     */
    void writeEntry(ByteBuffer logBuffer,
                    int logVersion,
                    boolean forReplication);

    /**
     * Returns whether this format has a variant that is optimized for
     * replication.
     */
    boolean hasReplicationFormat();

    /**
     * Returns whether it is worthwhile to materialize and then re-serialize a
     * log entry in a format optimized for replication. Implementations should
     * attempt to check efficiently, without instantiating the log entry
     * object. Some implementations will simply return false.
     *
     * <p>WARNING: The logBuffer position must not be changed by this method.
     *
     * <p>WARNING: The shared LogEntry object is used for calling this method,
     * and this method must not change any of the fields in the object.
     *
     * @param logBuffer contains the entry that would be re-serialized.
     * @param srcVersion the log version of entry in logBuffer.
     * @param destVersion the version that would be used for re-serialization.
     */
    boolean isReplicationFormatWorthwhile(ByteBuffer logBuffer,
                                          int srcVersion,
                                          int destVersion);
}
