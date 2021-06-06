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
import java.util.Collections;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.VersionedWriteLoggable;

/**
 * A basic implementation of a replicable log entry that has a single loggable
 * item and provides for writing in a single format by default.  Starting with
 * log version 9, entry classes whose log format has changed since the previous
 * log version will need to override the {@link #getSize(int, boolean)} and
 * {@link #writeEntry(ByteBuffer, int, boolean)} methods to support writing the
 * entry in earlier log formats.
 *
 * @param <T> the type of the loggable items in this entry
 */
abstract class SingleItemReplicableEntry<T extends VersionedWriteLoggable>
        extends SingleItemEntry<T> implements ReplicableLogEntry {

    /**
     * Creates an instance of this class for reading a log entry.
     *
     * @param logClass the class of the contained loggable item
     */
    SingleItemReplicableEntry(final Class<T> logClass) {
        super(logClass);
    }

    /**
     * Creates an instance of this class for writing a log entry.
     *
     * @param entryType the associated log entry type
     * @param item the contained loggable item
     */
    SingleItemReplicableEntry(final LogEntryType entryType, final T item) {
        super(entryType, item);
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        /* The cast is needed due to quirks of Java generics. */
        return Collections.singleton(
            (VersionedWriteLoggable) newInstanceOfType());
    }

    @Override
    public int getSize(final int logVersion, final boolean forReplication) {
        return getMainItem().getLogSize(logVersion, forReplication);
    }

    @Override
    public void writeEntry(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        getMainItem().writeToLog(logBuffer, logVersion,forReplication);
    }

    @Override
    public boolean hasReplicationFormat() {
        return getMainItem().hasReplicationFormat();
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return newInstanceOfType().isReplicationFormatWorthwhile(
            logBuffer, srcVersion, destVersion);
    }
}
