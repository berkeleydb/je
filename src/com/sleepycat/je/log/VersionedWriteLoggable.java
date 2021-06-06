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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.sleepycat.je.log.entry.ReplicableLogEntry;

/**
 * A sub-interface of {@link Loggable} implemented by classes that can write
 * themselves to a byte buffer in an earlier log format, for use by instances
 * of {@link ReplicableLogEntry} that need to support an earlier log format
 * during replication.  See [#22336].
 *
 * <p>Classes that implement {@code Loggable} should implement this interface
 * if they are included in replication data.
 *
 * <p>Implementing classes should document the version of the class's most
 * recent format change.  Log entry classes that contain {@code
 * VersionedWriteLoggable} items can use that information to determine if they
 * can copy the log contents for an entry directly or if they need to convert
 * them in order to be compatible with a particular log version.
 */
public interface VersionedWriteLoggable extends Loggable {

    /**
     * Returns the log version of the most recent format change for this
     * loggable item.
     *
     * @return the log version of the most recent format change
     *
     * @see ReplicableLogEntry#getLastFormatChange()
     */
    int getLastFormatChange();

    /**
     * @see ReplicableLogEntry#getEmbeddedLoggables()
     */
    Collection<VersionedWriteLoggable> getEmbeddedLoggables();

    /**
     * Returns the number of bytes needed to store this object in the format
     * for the specified log version.  Earlier log versions only need to be
     * supported for log entries with format changes made in {@link
     * LogEntryType#LOG_VERSION_REPLICATE_OLDER} or greater.
     *
     * @param logVersion the log version
     * @param forReplication whether the entry will be sent over the wire,
     * and not written to the log.
     * @return the number of bytes to store this object for the log version
     */
    int getLogSize(int logVersion, boolean forReplication);

    /**
     * Serializes this object into the specified buffer in the format for the
     * specified log version.  Earlier log versions only need to be
     * supported for log entries with format changes made in {@link
     * LogEntryType#LOG_VERSION_REPLICATE_OLDER} or greater.
     *
     * @param logBuffer the destination buffer
     * @param logVersion the log version
     * @param forReplication whether the entry will be sent over the wire,
     * and not written to the log.
     */
    void writeToLog(ByteBuffer logBuffer,
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
