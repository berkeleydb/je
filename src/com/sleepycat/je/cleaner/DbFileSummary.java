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

package com.sleepycat.je.cleaner;

import java.nio.ByteBuffer;

import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;

/**
 * Per-DB-per-file utilization counters.  The DatabaseImpl stores a persistent
 * map of file number to DbFileSummary.
 */
public class DbFileSummary implements Loggable, Cloneable {

    /* Persistent fields. */
    public int totalINCount;    // Number of IN log entries
    public int totalINSize;     // Byte size of IN log entries
    public int totalLNCount;    // Number of LN log entries
    public int totalLNSize;     // Byte size of LN log entries
    public int obsoleteINCount; // Number of obsolete IN log entries
    public int obsoleteLNCount; // Number of obsolete LN log entries
    public int obsoleteLNSize;  // Byte size of obsolete LN log entries
    public int obsoleteLNSizeCounted;  // Number obsolete LNs with size counted

    /**
     * Creates an empty summary.
     */
    public DbFileSummary() {
    }

    /**
     * Add the totals of the given summary object to the totals of this object.
     */
    public void add(DbFileSummary o) {

        totalINCount += o.totalINCount;
        totalINSize += o.totalINSize;
        totalLNCount += o.totalLNCount;
        totalLNSize += o.totalLNSize;
        obsoleteINCount += o.obsoleteINCount;
        obsoleteLNCount += o.obsoleteLNCount;
        obsoleteLNSize += o.obsoleteLNSize;
        obsoleteLNSizeCounted += o.obsoleteLNSizeCounted;
    }

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        return
            LogUtils.getPackedIntLogSize(totalINCount) +
            LogUtils.getPackedIntLogSize(totalINSize) +
            LogUtils.getPackedIntLogSize(totalLNCount) +
            LogUtils.getPackedIntLogSize(totalLNSize) +
            LogUtils.getPackedIntLogSize(obsoleteINCount) +
            LogUtils.getPackedIntLogSize(obsoleteLNCount) +
            LogUtils.getPackedIntLogSize(obsoleteLNSize) +
            LogUtils.getPackedIntLogSize(obsoleteLNSizeCounted);
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer buf) {

        LogUtils.writePackedInt(buf, totalINCount);
        LogUtils.writePackedInt(buf, totalINSize);
        LogUtils.writePackedInt(buf, totalLNCount);
        LogUtils.writePackedInt(buf, totalLNSize);
        LogUtils.writePackedInt(buf, obsoleteINCount);
        LogUtils.writePackedInt(buf, obsoleteLNCount);
        LogUtils.writePackedInt(buf, obsoleteLNSize);
        LogUtils.writePackedInt(buf, obsoleteLNSizeCounted);
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer buf, int entryTypeVersion) {

        totalINCount = LogUtils.readPackedInt(buf);
        totalINSize = LogUtils.readPackedInt(buf);
        totalLNCount = LogUtils.readPackedInt(buf);
        totalLNSize = LogUtils.readPackedInt(buf);
        obsoleteINCount = LogUtils.readPackedInt(buf);
        obsoleteLNCount = LogUtils.readPackedInt(buf);
        obsoleteLNSize = LogUtils.readPackedInt(buf);
        obsoleteLNSizeCounted = LogUtils.readPackedInt(buf);
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder buf, boolean verbose) {

        buf.append("<summary totalINCount=\"");
        buf.append(totalINCount);
        buf.append("\" totalINSize=\"");
        buf.append(totalINSize);
        buf.append("\" totalLNCount=\"");
        buf.append(totalLNCount);
        buf.append("\" totalLNSize=\"");
        buf.append(totalLNSize);
        buf.append("\" obsoleteINCount=\"");
        buf.append(obsoleteINCount);
        buf.append("\" obsoleteLNCount=\"");
        buf.append(obsoleteLNCount);
        buf.append("\" obsoleteLNSize=\"");
        buf.append(obsoleteLNSize);
        buf.append("\" obsoleteLNSizeCounted=\"");
        buf.append(obsoleteLNSizeCounted);
        buf.append("\"/>");
    }

    /**
     * Never called.
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    @Override
    public DbFileSummary clone() {
        try {
            return (DbFileSummary) super.clone();
        } catch (CloneNotSupportedException e) {
            /* Should never happen. */
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        dumpLog(buf, true);
        return buf.toString();
    }
}
