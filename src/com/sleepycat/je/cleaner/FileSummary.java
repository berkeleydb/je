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
import com.sleepycat.je.log.entry.LNLogEntry;

/**
 * Per-file utilization counters.  The UtilizationProfile stores a persistent
 * map of file number to FileSummary.
 */
public class FileSummary implements Loggable, Cloneable {

    /* Persistent fields. */
    public int totalCount;      // Total # of log entries
    public int totalSize;       // Total bytes in log file
    public int totalINCount;    // Number of IN log entries
    public int totalINSize;     // Byte size of IN log entries
    public int totalLNCount;    // Number of LN log entries
    public int totalLNSize;     // Byte size of LN log entries
    public int maxLNSize;       // Byte size of largest LN log entry
    public int obsoleteINCount; // Number of obsolete IN log entries
    public int obsoleteLNCount; // Number of obsolete LN log entries
    public int obsoleteLNSize;  // Byte size of obsolete LN log entries
    public int obsoleteLNSizeCounted;  // Number obsolete LNs with size counted

    /**
     * Creates an empty summary.
     */
    public FileSummary() {
    }

    public FileSummary clone() {
        try {
            return (FileSummary) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns whether this summary contains any non-zero totals.
     */
    public boolean isEmpty() {

        return totalCount == 0 &&
               totalSize == 0 &&
               obsoleteINCount == 0 &&
               obsoleteLNCount == 0;
    }

    /**
     * Returns the approximate byte size of all obsolete LN entries, using the
     * average LN size for LN sizes that were not counted.
     */
    public int getObsoleteLNSize() {

        if (totalLNCount == 0) {
            return 0;
        }

        /* Normalize obsolete amounts to account for double-counting. */
        final int obsLNCount = Math.min(obsoleteLNCount, totalLNCount);
        final int obsLNSize = Math.min(obsoleteLNSize, totalLNSize);
        final int obsLNSizeCounted = Math.min(obsoleteLNSizeCounted,
                                              obsLNCount);

        /*
         * Use the tracked obsolete size for all entries for which the size was
         * counted, plus the average size for all obsolete entries whose size
         * was not counted.
         */
        long obsSize = obsLNSize;
        final int obsCountNotCounted = obsLNCount - obsLNSizeCounted;
        if (obsCountNotCounted > 0) {

            /*
             * When there are any obsolete LNs with sizes uncounted, we add an
             * obsolete amount that is the product of the number of LNs
             * uncounted and the average LN size.
             */
            final float avgLNSizeNotCounted = getAvgObsoleteLNSizeNotCounted();
            if (!Float.isNaN(avgLNSizeNotCounted)) {
                obsSize += (int) (obsCountNotCounted * avgLNSizeNotCounted);
            }
        }

        /* Don't return an impossibly large estimate. */
        return (obsSize > totalLNSize) ? totalLNSize : (int) obsSize;
    }

    /**
     * Returns the average size for LNs with sizes not counted, or NaN if
     * there are no such LNs.
     *
     * In FileSummaryLN version 3 and greater the obsolete size is normally
     * counted, but not in exceptional circumstances such as recovery.  If it
     * is not counted, obsoleteLNSizeCounted will be less than obsoleteLNCount.
     *
     * In log version 8 and greater, we don't count the size when the LN is not
     * resident in cache during update/delete, and CLEANER_FETCH_OBSOLETE_SIZE
     * is false (the default setting).
     *
     * We added maxLNSize in version 8 for use in estimating obsolete LN sizes.
     *
     * To compute the average LN size, we only consider the LNs (both obsolete
     * and non-obsolete) for which the size has not been counted.  This
     * increases accuracy when counted and uncounted LN sizes are not uniform.
     * An example is when large LNs are inserted and deleted.  The size of the
     * deleted LN log entry (which is small) is always counted, but the
     * previous version (which has a large size) may not be counted.
     */
    public float getAvgObsoleteLNSizeNotCounted() {

        /* Normalize obsolete amounts to account for double-counting. */
        final int obsLNCount = Math.min(obsoleteLNCount, totalLNCount);
        final int obsLNSize = Math.min(obsoleteLNSize, totalLNSize);
        final int obsLNSizeCounted = Math.min(obsoleteLNSizeCounted,
                                              obsLNCount);

        final int obsCountNotCounted = obsLNCount - obsLNSizeCounted;
        if (obsCountNotCounted <= 0) {
            return Float.NaN;
        }

        final int totalSizeNotCounted = totalLNSize - obsLNSize;
        final int totalCountNotCounted = totalLNCount - obsLNSizeCounted;

        if (totalSizeNotCounted <= 0 || totalCountNotCounted <= 0) {
            return Float.NaN;
        }

        return totalSizeNotCounted / ((float) totalCountNotCounted);
    }

    /**
     * Returns the maximum possible obsolete LN size, using the maximum LN size
     * for LN sizes that were not counted.
     */
    public int getMaxObsoleteLNSize() {

        /*
         * In log version 7 and earlier the maxLNSize is not available.  It is
         * safe to use getObsoleteLNSize in that case, because LSN locking was
         * not used and the obsolete size was counted for updates and deletes.
         */
        if (maxLNSize == 0) {
            return getObsoleteLNSize();
        }

        if (totalLNCount == 0) {
            return 0;
        }

        /* Normalize obsolete amounts to account for double-counting. */
        final int obsLNCount = Math.min(obsoleteLNCount, totalLNCount);
        final int obsLNSize = Math.min(obsoleteLNSize, totalLNSize);
        final int obsLNSizeCounted = Math.min(obsoleteLNSizeCounted,
                                              obsLNCount);

        /*
         * Use the tracked obsolete size for all entries for which the size was
         * counted, plus the maximum possible size for all obsolete entries
         * whose size was not counted.
         */
        long obsSize = obsLNSize;
        final long obsCountNotCounted = obsLNCount - obsLNSizeCounted;
        if (obsCountNotCounted > 0) {

            /*
             * When there are any obsolete LNs with sizes uncounted, we add an
             * obsolete amount that is the minimum of two values.  Either value
             * may be much higher than the true obsolete amount, but by taking
             * their minimum we use a much more realistic obsolete amount.
             *
             * maxLNSizeNotCounted is the maximum obsolete not counted, based
             * on the multiplying maxLNSize and the number of obsolete LNs not
             * counted.
             *
             * maxObsSizeNotCounted is also an upper bound on the obsolete size
             * not not counted.  The (totalLNSize - obsLNSize) gives the amount
             * non-obsolete plus the obsolete amount not counted.  From this we
             * subtract the minimum non-obsolete size, based on the minimum
             * size of any LN.  This leaves the maximum obsolete size not
             * counted.
             *
             * Note that the mutiplication immediately below would overflow if
             * type 'int' instead of 'long' were used for the operands.  This
             * was fixed in [#21106].
             */
            final long maxLNSizeNotCounted = obsCountNotCounted * maxLNSize;

            final long maxObsSizeNotCounted = totalLNSize - obsLNSize -
                ((totalLNCount - obsLNCount) * LNLogEntry.MIN_LOG_SIZE);

            obsSize += Math.min(maxLNSizeNotCounted, maxObsSizeNotCounted);
        }

        /* Don't return an impossibly large estimate. */
        return (obsSize > totalLNSize) ? totalLNSize : (int) obsSize;
    }

    /**
     * Returns the approximate byte size of all obsolete IN entries.
     */
    public int getObsoleteINSize() {

        if (totalINCount == 0) {
            return 0;
        }

        /* Normalize obsolete amounts to account for double-counting. */
        final int obsINCount = Math.min(obsoleteINCount, totalINCount);

        /* Use average IN size to compute total. */
        final float size = totalINSize;
        final float avgSizePerIN = size / totalINCount;
        return (int) (obsINCount * avgSizePerIN);
    }

    /**
     * Returns an estimate of the total bytes that are obsolete, using
     * getObsoleteLNSize instead of getMaxObsoleteLNSize.
     */
    public int getObsoleteSize() {
        return calculateObsoleteSize(getObsoleteLNSize());
    }

    /**
     * Returns an estimate of the total bytes that are obsolete, using
     * getMaxObsoleteLNSize instead of getObsoleteLNSize.
     */
    public int getMaxObsoleteSize() {
        return calculateObsoleteSize(getMaxObsoleteLNSize());
    }

    private int calculateObsoleteSize(int lnObsoleteSize) {
        if (totalSize <= 0) {
            return 0;
        }
            /* Leftover (non-IN non-LN) space is considered obsolete. */
        final int leftoverSize = totalSize - (totalINSize + totalLNSize);

        int obsoleteSize = lnObsoleteSize +
            getObsoleteINSize() +
            leftoverSize;

        /*
         * Don't report more obsolete bytes than the total.  We may
         * calculate more than the total because of (intentional)
         * double-counting during recovery.
         */
        if (obsoleteSize > totalSize) {
            obsoleteSize = totalSize;
        }
        return obsoleteSize;
    }

    /**
     * Returns the total number of entries counted.  This value is guaranteed
     * to increase whenever the tracking information about a file changes.  It
     * is used a key discriminator for FileSummaryLN records.
     */
    int getEntriesCounted() {
        return totalCount + obsoleteLNCount + obsoleteINCount;
    }

    /**
     * Calculates utilization percentage using average LN sizes.
     */
    public int utilization() {
        return utilization(getObsoleteSize(), totalSize);
    }

    /**
     * Calculates a utilization percentage.
     */
    public static int utilization(long obsoleteSize, long totalSize) {
        if (totalSize == 0) {
            return 0;
        }
        return Math.round(((100.0F * (totalSize - obsoleteSize)) / totalSize));
    }

    /**
     * Reset all totals to zero.
     */
    public void reset() {

        totalCount = 0;
        totalSize = 0;
        totalINCount = 0;
        totalINSize = 0;
        totalLNCount = 0;
        totalLNSize = 0;
        maxLNSize = 0;
        obsoleteINCount = 0;
        obsoleteLNCount = 0;
        obsoleteLNSize = 0;
        obsoleteLNSizeCounted = 0;
    }

    /**
     * Add the totals of the given summary object to the totals of this object.
     */
    public void add(FileSummary o) {

        totalCount += o.totalCount;
        totalSize += o.totalSize;
        totalINCount += o.totalINCount;
        totalINSize += o.totalINSize;
        totalLNCount += o.totalLNCount;
        totalLNSize += o.totalLNSize;
        if (maxLNSize < o.maxLNSize) {
            maxLNSize = o.maxLNSize;
        }
        obsoleteINCount += o.obsoleteINCount;
        obsoleteLNCount += o.obsoleteLNCount;
        obsoleteLNSize += o.obsoleteLNSize;
        obsoleteLNSizeCounted += o.obsoleteLNSizeCounted;
    }

    public int getLogSize() {

        return 11 * LogUtils.getIntLogSize();
    }

    public void writeToLog(ByteBuffer buf) {

        LogUtils.writeInt(buf, totalCount);
        LogUtils.writeInt(buf, totalSize);
        LogUtils.writeInt(buf, totalINCount);
        LogUtils.writeInt(buf, totalINSize);
        LogUtils.writeInt(buf, totalLNCount);
        LogUtils.writeInt(buf, totalLNSize);
        LogUtils.writeInt(buf, maxLNSize);
        LogUtils.writeInt(buf, obsoleteINCount);
        LogUtils.writeInt(buf, obsoleteLNCount);
        LogUtils.writeInt(buf, obsoleteLNSize);
        LogUtils.writeInt(buf, obsoleteLNSizeCounted);
    }

    public void readFromLog(ByteBuffer buf, int entryVersion) {

        totalCount = LogUtils.readInt(buf);
        totalSize = LogUtils.readInt(buf);
        totalINCount = LogUtils.readInt(buf);
        totalINSize = LogUtils.readInt(buf);
        totalLNCount = LogUtils.readInt(buf);
        totalLNSize = LogUtils.readInt(buf);
        if (entryVersion >= 8) {
            maxLNSize = LogUtils.readInt(buf);
        }
        obsoleteINCount = LogUtils.readInt(buf);
        if (obsoleteINCount == -1) {

            /*
             * If INs were not counted in an older log file written by 1.5.3 or
             * earlier, consider all INs to be obsolete.  This causes the file
             * to be cleaned, and then IN counting will be accurate.
             */
            obsoleteINCount = totalINCount;
        }
        obsoleteLNCount = LogUtils.readInt(buf);

        /*
         * obsoleteLNSize and obsoleteLNSizeCounted were added in FileSummaryLN
         * version 3.
         */
        if (entryVersion >= 3) {
            obsoleteLNSize = LogUtils.readInt(buf);
            obsoleteLNSizeCounted = LogUtils.readInt(buf);
        } else {
            obsoleteLNSize = 0;
            obsoleteLNSizeCounted = 0;
        }
    }

    public void dumpLog(StringBuilder buf, boolean verbose) {

        buf.append("<summary totalCount=\"");
        buf.append(totalCount);
        buf.append("\" totalSize=\"");
        buf.append(totalSize);
        buf.append("\" totalINCount=\"");
        buf.append(totalINCount);
        buf.append("\" totalINSize=\"");
        buf.append(totalINSize);
        buf.append("\" totalLNCount=\"");
        buf.append(totalLNCount);
        buf.append("\" totalLNSize=\"");
        buf.append(totalLNSize);
        buf.append("\" maxLNSize=\"");
        buf.append(maxLNSize);
        buf.append("\" obsoleteINCount=\"");
        buf.append(obsoleteINCount);
        buf.append("\" obsoleteLNCount=\"");
        buf.append(obsoleteLNCount);
        buf.append("\" obsoleteLNSize=\"");
        buf.append(obsoleteLNSize);
        buf.append("\" obsoleteLNSizeCounted=\"");
        buf.append(obsoleteLNSizeCounted);
        buf.append("\" getObsoleteSize=\"");
        buf.append(getObsoleteSize());
        buf.append("\" getObsoleteINSize=\"");
        buf.append(getObsoleteINSize());
        buf.append("\" getObsoleteLNSize=\"");
        buf.append(getObsoleteLNSize());
        buf.append("\" getMaxObsoleteSize=\"");
        buf.append(getMaxObsoleteSize());
        buf.append("\" getMaxObsoleteLNSize=\"");
        buf.append(getMaxObsoleteLNSize());
        buf.append("\" getAvgObsoleteLNSizeNotCounted=\"");
        buf.append(getAvgObsoleteLNSizeNotCounted());
        buf.append("\"/>");
    }

    /**
     * Never called.
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        dumpLog(buf, true);
        return buf.toString();
    }
}
