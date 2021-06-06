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

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.VLSN;

/**
 * A Log entry allows you to read, write and dump a database log entry.  Each
 * entry may be made up of one or more loggable items.
 *
 * <p>The log entry on disk consists of a log header defined by LogManager and
 * the specific contents of the log entry.
 *
 * <p>Log entries that support replication are required to implement {@link
 * ReplicableLogEntry}.
 */
public interface LogEntry extends Cloneable {

    /**
     * Inform a LogEntry instance of its corresponding LogEntryType.
     */
    public void setLogType(LogEntryType entryType);

    /**
     * @return the type of log entry
     */
    public LogEntryType getLogType();

    /**
     * Read in a log entry.
     */
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer);

    /**
     * Print out the contents of an entry.
     */
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose);

    /**
     * @return the first item of the log entry
     */
    public Object getMainItem();

    /**
     * Construct a complete item from a item entry, fetching additional log
     * entries as needed to ensure that a usable main object is available.
     *
     * For an OldBINDeltaLogEntry, fetches the full BIN and merges the delta
     * information.  This is necessary to return a Node main object.
     * However, for the new BINDeltaLogEntry, the full BIN is not fetched,
     * since the partial BIN (the delta) is usable as a Node.
     */
    public Object getResolvedItem(DatabaseImpl dbImpl);

    /**
     * @return the ID of the database containing this entry, or null if this
     * entry type is not part of a database.
     */
    public DatabaseId getDbId();

    /**
     * @return return the transaction id if this log entry is transactional,
     * 0 otherwise.
     */
    public long getTransactionId();

    /**
     * @return size of byte buffer needed to store this entry.
     */
    public int getSize();

    /**
     * Serialize this object into the buffer.
     * @param logBuffer is the destination buffer
     */
    public void writeEntry(ByteBuffer logBuffer);

    /**
     * Returns true if this item should be counted as obsolete when logged.
     */
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl);

    /**
     * Returns whether this is a deleted LN.
     */
    public boolean isDeleted();

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     */
    public void postLogWork(LogEntryHeader header,
                            long justLoggedLsn,
                            VLSN vlsn);

    /**
     * @return a shallow clone.
     */
    public LogEntry clone();

    /**
     * @return true if these two log entries are logically the same.
     * Used for replication.
     */
    public boolean logicalEquals(LogEntry other);

    /**
     * Dump the contents of the log entry that are interesting for
     * replication.
     */
    public void dumpRep(StringBuilder sb);
}
