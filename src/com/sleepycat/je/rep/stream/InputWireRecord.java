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
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.VLSN;

/**
 * Format for messages received at across the wire for replication. Instead of
 * sending a direct copy of the log entry as it is stored on the JE log files
 * (LogEntryHeader + LogEntry), select parts of the header are sent.
 *
 * An InputWireRecord de-serializes the logEntry from the message bytes and
 * releases any claim on the backing ByteBuffer.
 */
public class InputWireRecord extends WireRecord {

    private final LogEntry logEntry;

    /**
     * Make a InputWireRecord from an incoming replication message buffer for
     * applying at a replica.
     * @throws DatabaseException
     */
    InputWireRecord(final EnvironmentImpl envImpl,
                    final ByteBuffer msgBuffer,
                    final BaseProtocol protocol)
        throws DatabaseException {

        super(createLogEntryHeader(msgBuffer, protocol));

        logEntry = instantiateEntry(envImpl, msgBuffer);
    }

    private static LogEntryHeader createLogEntryHeader(
        final ByteBuffer msgBuffer, final BaseProtocol protocol) {

        final byte entryType = msgBuffer.get();
        int entryVersion = LogUtils.readInt(msgBuffer);
        final int itemSize = LogUtils.readInt(msgBuffer);
        final VLSN vlsn = new VLSN(LogUtils.readLong(msgBuffer));

        /*
         * Check to see if we need to fix the entry's log version to work
         * around [#25222].
         */
        if ((entryVersion > LogEntryType.LOG_VERSION_EXPIRE_INFO)
            && protocol.getFixLogVersion12Entries()) {
            entryVersion = LogEntryType.LOG_VERSION_EXPIRE_INFO;
        }

        return new LogEntryHeader(entryType, entryVersion, itemSize, vlsn);
    }

    /**
     * Unit test support.
     * @throws DatabaseException
     */
    InputWireRecord(final EnvironmentImpl envImpl,
                    final byte entryType,
                    final int entryVersion,
                    final int itemSize,
                    final VLSN vlsn,
                    final ByteBuffer entryBuffer)
        throws DatabaseException {

        super(new LogEntryHeader(entryType, entryVersion, itemSize, vlsn));
        logEntry = LogEntryType.findType(header.getType()).
            getNewLogEntry();
        logEntry.readEntry(envImpl, header, entryBuffer);

    }

    public VLSN getVLSN() {
        return header.getVLSN();
    }

    public byte getEntryType() {
        return header.getType();
    }

    public LogEntry getLogEntry() {
        return logEntry;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        header.dumpRep(sb);
        sb.append(" ");
        logEntry.dumpRep(sb);
        return sb.toString();
    }

    /**
     * Convert the full version of the log entry to a string.
     */
    public String dumpLogEntry() {
        StringBuilder sb = new StringBuilder();
        sb.append(header);
        sb.append(" ").append(logEntry);
        return sb.toString();
    }
}
