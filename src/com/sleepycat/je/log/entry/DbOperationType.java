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
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.VersionedWriteLoggable;

/**
 * DbOperationType is a persistent enum used in NameLNLogEntries. It supports
 * replication of database operations by documenting the type of api operation
 * which instigated the logging of a NameLN.
 */
public enum DbOperationType implements VersionedWriteLoggable {

    NONE((byte) 0),
    CREATE((byte) 1),
    REMOVE((byte) 2),
    TRUNCATE((byte) 3),
    RENAME((byte) 4),
    UPDATE_CONFIG((byte) 5);

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    private byte value;

    private DbOperationType(byte value) {
        this.value = value;
    }

    public static DbOperationType readTypeFromLog(final ByteBuffer entryBuffer,
                                                  @SuppressWarnings("unused")
                                                  int entryVersion) {
        byte opVal = entryBuffer.get();
        switch (opVal) {
        case 1:
            return CREATE;

        case 2:
            return REMOVE;

        case 3:
            return TRUNCATE;

        case 4:
            return RENAME;

        case 5:
            return UPDATE_CONFIG;

        case 0:
        default:
            return NONE;

        }
    }

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Collections.emptyList();
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
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return 1;
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        logBuffer.put(value);
    }

    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        value = itemBuffer.get();
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

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<DbOp val=\"").append(this).append("\"/>");
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public boolean logicalEquals(Loggable other) {
        if (!(other instanceof DbOperationType))
            return false;

        return value == ((DbOperationType) other).value;
    }

    /**
     * Return true if this database operation type needs to write
     * DatabaseConfig.
     */
    public static boolean isWriteConfigType(DbOperationType opType) {
        return (opType == CREATE || opType == UPDATE_CONFIG);
    }
}
