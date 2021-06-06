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
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.OldBINDeltaLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Summarizes the utilized and unutilized portion of each log file by examining
 * each log entry.  Does not use the Cleaner UtilizationProfile information in
 * order to provide a second measure against which to evaluation the
 * UtilizationProfile accuracy.
 *
 * Limitations
 * ===========
 * BIN-deltas are all considered obsolete, as an implementation short cut and
 * for efficiency. 90% (by default) of deltas are obsolete anyway, and it
 * would be expensive to fetch the parent BIN to find the lookup key.
 *
 * Assumes that any currently open transactions will be committed.  For
 * example, if a deletion or update has been performed but not yet committed,
 * the old record will be considered obsolete.  Perhaps this behavior could be
 * changed in the future by attempting to lock a record (non-blocking) and
 * considering a locked record to be non-obsolete; this might make it match
 * live utilization counting more closely.
 *
 * Accesses the Btree, using JE cache memory if necessary and contending with
 * other accessors, to check whether an entry is active.
 *
 * Historical note: This implementation, which uses the Btree to determine
 * whether a node is active, replaced an earlier implementation that attempted
 * to duplicate the Btree in memory and read the entire log.  This older
 * implementation had inaccuracies and was less efficient.  With the new
 * implementation it is also possible to calculation utilization for a range of
 * LSNs, reading only that portion of the log.  [#22208]
 */
public class UtilizationFileReader extends FileReader {

    /* Long file -> FileSummary */
    private final Map<Long, FileSummary> summaries;

    /* Cache of DB ID -> DatabaseImpl for reading live databases. */
    private final Map<DatabaseId, DatabaseImpl> dbCache;
    private final DbTree dbTree;

    private UtilizationFileReader(EnvironmentImpl envImpl,
                                  int readBufferSize,
                                  long startLsn,
                                  long finishLsn)
        throws DatabaseException {

        super(envImpl,
              readBufferSize,
              true,            // read forward
              startLsn,
              null,            // single file number
              DbLsn.NULL_LSN,  // end of file LSN
              finishLsn);

        summaries = new HashMap<Long, FileSummary>();
        dbCache = new HashMap<DatabaseId, DatabaseImpl>();
        dbTree = envImpl.getDbTree();
    }

    @Override
    protected boolean isTargetEntry() {

        /* 
         * UtilizationTracker is supposed to mimic the UtilizationProfile. 
         * Accordingly it does not count the file header or invisible log 
         * entries because those entries are not covered by the U.P.
         */
        return ((currentEntryHeader.getType() !=
                 LogEntryType.LOG_FILE_HEADER.getTypeNum()) &&
                !currentEntryHeader.isInvisible());
    }

    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        final LogEntryType lastEntryType =
            LogEntryType.findType(currentEntryHeader.getType());
        final LogEntry entry = lastEntryType.getNewLogEntry();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer);

        ExtendedFileSummary summary = 
                (ExtendedFileSummary) summaries.get(window.currentFileNum());
        if (summary == null) {
            summary = new ExtendedFileSummary();
            summaries.put(window.currentFileNum(), summary);
        }

        final int size = getLastEntrySize();

        summary.totalCount += 1;
        summary.totalSize += size;

        if (entry instanceof LNLogEntry) {
            final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
            final DatabaseImpl dbImpl = getActiveDb(lnEntry.getDbId());
            final boolean isActive = (dbImpl != null) &&
                                     !lnEntry.isImmediatelyObsolete(dbImpl) &&
                                     isLNActive(lnEntry, dbImpl);
            applyLN(summary, size, isActive);
        } else if (entry instanceof BINDeltaLogEntry ||
                   entry instanceof OldBINDeltaLogEntry) {
            /* Count Delta as IN. */
            summary.totalINCount += 1;
            summary.totalINSize += size;
            /* Most deltas are obsolete, so count them all obsolete. */
            summary.obsoleteINCount += 1;
            summary.recalcObsoleteINSize += size;
        } else if (entry instanceof INLogEntry) {
            final INLogEntry<?> inEntry = (INLogEntry<?>) entry;
            final DatabaseImpl dbImpl = getActiveDb(inEntry.getDbId());
            final boolean isActive = dbImpl != null &&
                                     isINActive(inEntry, dbImpl);
            applyIN(summary, size, isActive);
        }

        return true;
    }

    private DatabaseImpl getActiveDb(DatabaseId dbId) {
        final DatabaseImpl dbImpl =
            dbTree.getDb(dbId, -1 /*timeout*/, dbCache);
        if (dbImpl == null) {
            return null;
        }
        if (dbImpl.isDeleteFinished()) {
            return null;
        }
        return dbImpl;
    }

    /**
     * Mimics lookup in com.sleepycat.je.cleaner.FileProcessor.processLN.
     */
    private boolean isLNActive(LNLogEntry<?> lnEntry, DatabaseImpl dbImpl) {
        lnEntry.postFetchInit(dbImpl);
        final byte[] key = lnEntry.getKey();
        final Tree tree = dbImpl.getTree();
        final TreeLocation location = new TreeLocation();

        final boolean parentFound = tree.getParentBINForChildLN(
            location, key, false /*splitsAllowed*/,
            false /*blindDeltaOps*/, CacheMode.DEFAULT);

        final BIN bin = location.bin;

        try {
            if (!parentFound || bin.isEntryKnownDeleted(location.index)) {
                return false;
            }
            final int index = location.index;
            final long treeLsn = bin.getLsn(index);
            if (treeLsn == DbLsn.NULL_LSN) {
                return false;
            }
            final long logLsn = getLastLsn();
            return treeLsn == logLsn;

        } finally {
            if (bin != null) {
                bin.releaseLatch();
            }
        }
    }

    /**
     * Mimics lookup in com.sleepycat.je.cleaner.FileProcessor.processIN.
     */
    private boolean isINActive(INLogEntry<?> inEntry, DatabaseImpl dbImpl) {

        final long logLsn = getLastLsn();
        final IN logIn = inEntry.getIN(dbImpl);
        logIn.setDatabase(dbImpl);
        final Tree tree = dbImpl.getTree();
        if (logIn.isRoot()) {
            return logLsn == tree.getRootLsn();
        }

        logIn.latch(CacheMode.DEFAULT);

        final SearchResult result = tree.getParentINForChildIN(
            logIn, true, /*useTargetLevel*/
            true, /*doFetch*/ CacheMode.DEFAULT);

        if (!result.exactParentFound) {
            return false;
        }
        try {
            long treeLsn = result.parent.getLsn(result.index);

            if (treeLsn == DbLsn.NULL_LSN) {
                return false;
            }

            if (treeLsn == logLsn) {
                return true;
            }

            if (!logIn.isBIN()) {
                return false;
            }

            /* The treeLsn may refer to a BIN-delta. */
            final IN treeIn =
                result.parent.fetchIN(result.index, CacheMode.DEFAULT);

            treeLsn = treeIn.getLastFullLsn();

            return treeLsn == logLsn;
        } finally {
            result.parent.releaseLatch();
        }
    }

    private void applyLN(ExtendedFileSummary summary,
                         int size,
                         boolean isActive) {
        summary.totalLNCount += 1;
        summary.totalLNSize += size;
        if (!isActive) {
            summary.obsoleteLNCount += 1;
            summary.recalcObsoleteLNSize += size;
        }
    }

    private void applyIN(ExtendedFileSummary summary,
                         int size,
                         boolean isActive) {
        summary.totalINCount += 1;
        summary.totalINSize += size;
        if (!isActive) {
            summary.obsoleteINCount += 1;
            summary.recalcObsoleteINSize += size;
        }
    }

    private void cleanUp() {
        dbTree.releaseDbs(dbCache);
    }

    /**
     * Creates a UtilizationReader, reads the log, and returns the resulting
     * Map of Long file number to FileSummary.
     */
    public static Map<Long, FileSummary>
        calcFileSummaryMap(EnvironmentImpl envImpl) {
        return calcFileSummaryMap(envImpl, DbLsn.NULL_LSN, DbLsn.NULL_LSN);
    }

    public static Map<Long, FileSummary>
        calcFileSummaryMap(EnvironmentImpl envImpl,
                           long startLsn,
                           long finishLsn) {

        final int readBufferSize = envImpl.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        final UtilizationFileReader reader = new UtilizationFileReader
            (envImpl, readBufferSize, startLsn, finishLsn);
        try {
            while (reader.readNextEntry()) {
                /* All the work is done in processEntry. */
            }
            return reader.summaries;
        } finally {
            reader.cleanUp();
        }
    }

    private static class ExtendedFileSummary extends FileSummary {
        private int recalcObsoleteINSize;
        private int recalcObsoleteLNSize;

        /**
         * Overrides the LN size calculation to return the recalculated number
         * of obsolete LN bytes.
         */
        @Override
        public int getObsoleteLNSize() {
            return recalcObsoleteLNSize;
        }

        /**
         * Overrides the IN size calculation to return the recalculated number
         * of obsolete IN bytes.
         */
        @Override
        public int getObsoleteINSize() {
            return recalcObsoleteINSize;
        }

        /**
         * Overrides to add the extended data fields.
         */
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(super.toString());
            buf.append("<extended-info recalcObsoleteINSize=\"");
            buf.append(recalcObsoleteINSize);
            buf.append("\" recalcObsoleteLNSize=\"");
            buf.append(recalcObsoleteLNSize);
            buf.append("\"/>");
            return buf.toString();
        }
    }

    private static class NodeInfo {
        ExtendedFileSummary summary;
        int size;
        long dbId;
    }
}
