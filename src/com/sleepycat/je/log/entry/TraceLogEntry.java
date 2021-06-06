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
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Trace;

/**
 * Log entry for a trace object.
 */
public class TraceLogEntry extends SingleItemReplicableEntry<Trace> {

    /**
     * The log version number of the most recent change for this log entry,
     * including any changes to the format of the underlying {@link Trace}
     * object.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    /**
     * If non-null, write this object when asked to write in the log format
     * prior to the last changed version, for testing.
     */
    private static volatile Loggable testPriorItem = null;

    /** Construct a log entry for reading a {@link Trace} object. */
    public TraceLogEntry() {
        super(Trace.class);
    }

    /** Construct a log entry for writing a {@link Trace} object. */
    public TraceLogEntry(final Trace trace) {
        super(LogEntryType.LOG_TRACE, trace);
    }

    /**
     * Specify an object to write instead of the enclosed item when asked to
     * write this entry in the log format prior to the last changed version,
     * for testing.
     */
    public static void setTestPriorItem(final Loggable priorItem) {
        testPriorItem = priorItem;
    }

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation provides additional behavior for testing.
     */
    @Override
    public int getSize(final int logVersion, final boolean forReplication) {
        if (testPriorItem != null && logVersion == LAST_FORMAT_CHANGE - 1) {
            return testPriorItem.getLogSize();
        }
        return super.getSize(logVersion, forReplication);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation provides additional behavior for testing.
     */
    @Override
    public void writeEntry(final ByteBuffer destBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        if (testPriorItem != null && logVersion == LAST_FORMAT_CHANGE - 1) {
            testPriorItem.writeToLog(destBuffer);
            return;
        }
        super.writeEntry(destBuffer, logVersion, forReplication);
    }
}
