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

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.VersionedWriteLoggable;

/**
 * A basic implementation of a replicable log entry that provides for writing
 * in a single format by default.  Starting with log version 9, as specified by
 * {@link LogEntryType#LOG_VERSION_REPLICATE_OLDER}, entry classes whose log
 * format has changed since the previous log version will need to override the
 * {@link ReplicableLogEntry#getLastFormatChange}, {@link #getSize(int,
 * boolean)} and {@link #writeEntry(ByteBuffer, int, boolean)} methods to
 * support writing the entry in the previous log format.
 *
 * @param <T> the type of the loggable items in this entry
 */
abstract class BaseReplicableEntry<T extends VersionedWriteLoggable>
        extends BaseEntry<T>
        implements ReplicableLogEntry {

    /**
     * Creates an instance of this class for reading a log entry.
     *
     * @param logClass the class of the contained loggable item or items
     * @see BaseEntry#BaseEntry(Class)
     */
    BaseReplicableEntry(final Class<T> logClass) {
        super(logClass);
    }

    /**
     * Creates an instance of this class for writing a log entry.
     */
    BaseReplicableEntry() {
    }

    @Override
    public void writeEntry(final ByteBuffer destBuffer) {
        writeEntry(
            destBuffer, LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public int getSize() {
        return getSize(LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public boolean hasReplicationFormat() {
        return false;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return false;
    }
}
