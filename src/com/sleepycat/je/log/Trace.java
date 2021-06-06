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
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.TraceLogEntry;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Timestamp;

/**
 * Trace logs event tracing messages into .jdb files. Only critical messages
 * that should always be included in a log should use this functionality.
 */
public class Trace extends BasicVersionedWriteLoggable {

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    /* Contents of a debug message. */
    private Timestamp time;
    private String msg;

    /** Create a new debug record. */
    public Trace(String msg) {
        this.time = getCurrentTimestamp();
        this.msg = msg;
    }

    /** Create a trace record that will be filled in from the log. */
    public Trace() {
    }

    /**
     * @return message part of trace record.
     */
    public String getMessage() {
        return msg;
    }

    /* Generate a timestamp for the current time. */
    private Timestamp getCurrentTimestamp() {
        Calendar cal = Calendar.getInstance();

        return new Timestamp(cal.getTime().getTime());
    }

    /* Check to see if this Environment supports writing. */
    private static boolean isWritePermitted(EnvironmentImpl envImpl) {
        if (envImpl == null ||
            envImpl.isReadOnly() ||
            envImpl.mayNotWrite() ||
            envImpl.isDbLoggingDisabled()) {
            return false;
        }

        return true;
    }

    /** Convenience method to create a log entry containing this trace msg. */
    public static void trace(EnvironmentImpl envImpl, String message) {
        trace(envImpl, new Trace(message));
    }

    /** Trace a trace object, unit tests only. */
    public static long trace(EnvironmentImpl envImpl, Trace traceMsg) {
        if (isWritePermitted(envImpl)) {
            return envImpl.getLogManager().log(
                new TraceLogEntry(traceMsg),
                ReplicationContext.NO_REPLICATE);
        }

    	return DbLsn.NULL_LSN;
    }

    /**
     * Convenience method to create a log entry (lazily) containing this trace
     * msg. Lazy tracing is used when tracing is desired, but the .jdb files
     * are not initialized.
     */
    public static void traceLazily(EnvironmentImpl envImpl,
                                   String message) {
        if (isWritePermitted(envImpl)) {
            envImpl.getLogManager().logLazily(
                new TraceLogEntry(new Trace(message)),
                ReplicationContext.NO_REPLICATE);
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
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return (LogUtils.getTimestampLogSize(time) +
                LogUtils.getStringLogSize(msg));
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        LogUtils.writeTimestamp(logBuffer, time);
        LogUtils.writeString(logBuffer, msg);
    }

    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        time = LogUtils.readTimestamp(itemBuffer, unpacked);
        msg = LogUtils.readString(itemBuffer, unpacked, entryVersion);
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<Dbg time=\"");
        sb.append(time);
        sb.append("\">");
        sb.append("<msg val=\"");
        sb.append(msg);
        sb.append("\"/>");
        sb.append("</Dbg>");
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof Trace))
            return false;

        return msg.equals(((Trace) other).msg);
    }

    @Override
    public String toString() {
        return (time + "/" + msg);
    }

    /**
     *  Just in case it's ever used as a hash key.
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        /* Same instance? */
        if (this == obj) {
            return true;
        }

        /* Is it another Trace? */
        if (!(obj instanceof Trace)) {
            return false;
        }

        /*
         * We could compare all the fields individually, but since they're all
         * placed in our toString() method, we can just compare the String
         * version of each offer.
         */
        return (toString().equals(obj.toString()));
    }
}
