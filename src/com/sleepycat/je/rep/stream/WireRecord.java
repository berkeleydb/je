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

package com.sleepycat.je.rep.stream;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LogEntry;

/**
 * Format for log entries sent across the wire for replication. Instead of
 * sending a direct copy of the log entry as it is stored on the JE log files
 * (LogEntryHeader + LogEntry), select parts of the header are sent.
 *
 * @see InputWireRecord
 * @see OutputWireRecord
 */
abstract class WireRecord {

    final LogEntryHeader header;

    WireRecord(final LogEntryHeader header) {
        this.header = header;
    }

    /**
     * Returns the log entry type for this record.
     */
    LogEntryType getLogEntryType()
        throws DatabaseException {

        final LogEntryType type = LogEntryType.findType(header.getType());
        if (type == null) {
            throw EnvironmentFailureException.unexpectedState(
                "Unknown header type:" + header.getType());
        }
        return type;
    }

    /**
     * Instantiates the log entry for this wire record using the specified
     * environment and data.
     */
    LogEntry instantiateEntry(final EnvironmentImpl envImpl,
                              final ByteBuffer buffer)
        throws DatabaseException {

        final LogEntry entry = getLogEntryType().getNewLogEntry();
        buffer.mark();
        entry.readEntry(envImpl, header, buffer);
        buffer.reset();
        return entry;
    }
}
