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
import java.util.Collections;

/**
 * A basic implementation of {@link VersionedWriteLoggable} that provides for
 * writing in a single format by default.  Starting with log version 9, as
 * specified by {@link LogEntryType#LOG_VERSION_REPLICATE_OLDER}, loggable
 * classes whose log format has changed since the previous log version will
 * need to override the {@link VersionedWriteLoggable#getLastFormatChange},
 * {@link #getLogSize(int, boolean)} and {@link #writeToLog(ByteBuffer,
 * int, boolean)} methods to support writing the entry in earlier log formats.
 */
public abstract class BasicVersionedWriteLoggable
        implements VersionedWriteLoggable {

    /**
     * Creates an instance of this class.
     */
    public BasicVersionedWriteLoggable() {
    }

    @Override
    public int getLogSize() {
        return getLogSize(LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer) {
        writeToLog(
            logBuffer, LogEntryType.LOG_VERSION, false /*forReplication*/);
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
