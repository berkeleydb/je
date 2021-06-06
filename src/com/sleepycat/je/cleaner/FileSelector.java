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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.VLSN;

/**
 * Keeps track of the status of files for which cleaning is in progress.
 */
public class FileSelector {

    /**
     * Each file for which cleaning is in progress has one of the following
     * status values.  Files numbers migrate from one status to another, in
     * the order declared below.
     */
    enum FileStatus {

        /**
         * A file's status is initially TO_BE_CLEANED when it is selected as
         * part of a batch of files that, when deleted, will bring total
         * utilization down to the minimum configured value.  All files with
         * this status will be cleaned in lowest-cost-to-clean order.  For two
         * files of equal cost to clean, the lower numbered (oldest) files is
         * selected; this is why the fileInfoMap is sorted by key (file
         * number).
         */
        TO_BE_CLEANED,

        /**
         * When a TO_BE_CLEANED file is selected for processing by
         * FileProcessor, it is moved to the BEING_CLEANED status.  This
         * distinction is used to prevent a file from being processed by more
         * than one thread.
         */
        BEING_CLEANED,

        /**
         * A file is moved to the CLEANED status when all its log entries have
         * been read and processed.  However, entries needing migration will be
         * marked with the BIN entry MIGRATE flag, entries that could not be
         * locked will be in the pending LN set, and the DBs that were pending
         * deletion will be in the pending DB set.
         */
        CLEANED,

        /**
         * A file is moved to the CHECKPOINTED status at the end of a
         * checkpoint if it was CLEANED at the beginning of the checkpoint.
         * Because all dirty BINs are flushed during the checkpoints, no files
         * in this set will have entries with the MIGRATE flag set.  However,
         * some entries may be in the pending LN set and some DBs may be in the
         * pending DB set.
         */
        CHECKPOINTED,

        /**
         * A file is moved from the CHECKPOINTED status to the FULLY_PROCESSED
         * status when the pending LN/DB sets become empty.  Since a pending LN
         * was not locked successfully, we don't know its original file.  But
         * we do know that when no pending LNs are present for any file, all
         * log entries in CHECKPOINTED files are either obsolete or have been
         * migrated.  Note, however, that the parent BINs of the migrated
         * entries may not have been logged yet.
         *
         * No special handling is required to coordinate syncing of deferred
         * write databases for pending, deferred write LNs, because
         * non-temporary deferred write DBs are always synced during
         * checkpoints, and temporary deferred write DBs are not recovered.
         * Note that although DW databases are non-txnal, their LNs may be
         * pended because of lock collisions.
         */
        FULLY_PROCESSED,
    }

    /**
     * Information about a file being cleaned.
     */
    static class FileInfo {
        private FileStatus status;
        private int requiredUtil = -1;

        /* Per-file metadata. */
        Set<DatabaseId> dbIds;
        VLSN firstVlsn = VLSN.NULL_VLSN;
        VLSN lastVlsn = VLSN.NULL_VLSN;

        @Override
        public String toString() {
            return "status = " + status +
                   " dbIds = " + dbIds +
                   " firstVlsn = " + firstVlsn +
                   " lastVlsn = " + lastVlsn;
        }
    }

    /**
     * Information about files being cleaned, keyed by file number.  The map is
     * sorted by file number to clean older files before newer files.
     */
    private SortedMap<Long, FileInfo> fileInfoMap;

    /**
     * Pending LN info, keyed by original LSN.  These are LNs that could not be
     * locked, either during processing or during migration.
     */
    private Map<Long, LNInfo> pendingLNs;

    /**
     * For processed entries with DBs that are pending deletion, we consider
     * them to be obsolete but we store their DatabaseIds in a set.  Until the
     * DB deletion is complete, we can't delete the log files containing those
     * entries.
     */
    private Set<DatabaseId> pendingDBs;

    /**
     * If during a checkpoint there are no pending LNs or DBs added, we can
     * move CLEANED files directly to reserved status at the end of the
     * checkpoint. This is an optimization that allows deleting files more
     * quickly when possible. In particular this impacts the checkpoint
     * during environment close, since no user operations are active during
     * that checkpoint; this optimization allows us to delete all cleaned
     * files after the final checkpoint.
     */
    private boolean anyPendingDuringCheckpoint;

    FileSelector() {
        fileInfoMap = new TreeMap<>();
        pendingLNs = new HashMap<>();
        pendingDBs = new HashSet<>();
    }

    /**
     * Returns the best file that qualifies for cleaning, or null if no file
     * qualifies.
     *
     * @param forceCleaning is true to always select a file, even if its
     * utilization is above the minimum utilization threshold.
     *
     * @return {file number, required utilization for 2-pass cleaning},
     * or null if no file qualifies for cleaning.
     */
    synchronized Pair<Long, Integer> selectFileForCleaning(
        UtilizationCalculator calculator,
        SortedMap<Long, FileSummary> fileSummaryMap,
        boolean forceCleaning) {

        final Set<Long> toBeCleaned = getToBeCleanedFiles();

        if (!toBeCleaned.isEmpty()) {
            final Long fileNum = toBeCleaned.iterator().next();
            final FileInfo info = setStatus(fileNum, FileStatus.BEING_CLEANED);
            return new Pair<>(fileNum, info.requiredUtil);
        }

        final Pair<Long, Integer> result = calculator.getBestFile(
            fileSummaryMap, forceCleaning);

        if (result == null) {
            return null;
        }

        final Long fileNum = result.first();
        final int requiredUtil = result.second();

        assert !fileInfoMap.containsKey(fileNum);

        final FileInfo info = setStatus(fileNum, FileStatus.BEING_CLEANED);
        info.requiredUtil = requiredUtil;

        return result;
    }

    /**
     * Returns the number of files having the given status.
     */
    private synchronized int getNumberOfFiles(FileStatus status) {
        int count = 0;
        for (FileInfo info : fileInfoMap.values()) {
            if (info.status == status) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * Returns a sorted set of files having the given status.
     */
    private synchronized NavigableSet<Long> getFiles(FileStatus status) {
        final NavigableSet<Long> set = new TreeSet<>();
        for (Map.Entry<Long, FileInfo> entry : fileInfoMap.entrySet()) {
            if (entry.getValue().status == status) {
                set.add(entry.getKey());
            }
        }
        return set;
    }

    /**
     * Moves a file to a given status, adding the file to the fileInfoMap if
     * necessary.
     *
     * This method must be called while synchronized.
     */
    private FileInfo setStatus(Long fileNum, FileStatus newStatus) {
        FileInfo info = fileInfoMap.get(fileNum);
        if (info == null) {
            info = new FileInfo();
            fileInfoMap.put(fileNum, info);
        }
        info.status = newStatus;
        return info;
    }

    /**
     * Moves a collection of files to a given status, adding the files to the
     * fileInfoMap if necessary.
     *
     * This method must be called while synchronized.
     */
    private void setStatus(Collection<Long> files, FileStatus newStatus) {
        for (Long fileNum : files) {
            setStatus(fileNum, newStatus);
        }
    }

    /**
     * Moves all files with oldStatus to newStatus.
     *
     * This method must be called while synchronized.
     */
    private void setStatus(FileStatus oldStatus, FileStatus newStatus) {
        for (FileInfo info : fileInfoMap.values()) {
            if (info.status == oldStatus) {
                info.status = newStatus;
            }
        }
    }

    /**
     * Asserts that a file has a given status.  Should only be called under an
     * assertion to avoid the overhead of the method call and synchronization.
     * Always returns true to enable calling it under an assertion.
     *
     * This method must be called while synchronized.
     */
    private boolean checkStatus(Long fileNum, FileStatus expectStatus) {
        final FileInfo info = fileInfoMap.get(fileNum);
        assert info != null : "Expected " + expectStatus + " but was missing";
        assert info.status == expectStatus :
            "Expected " + expectStatus + " but was " + info.status;
        return true;
    }

    /**
     * Calls checkStatus(Long, FileStatus) for a collection of files.
     *
     * This method must be called while synchronized.
     */
    private boolean checkStatus(final Collection<Long> files,
                                final FileStatus expectStatus) {
        for (Long fileNum : files) {
            checkStatus(fileNum, expectStatus);
        }
        return true;
    }

    /**
     * Returns whether the file is in any stage of the cleaning process.
     */
    private synchronized boolean isFileCleaningInProgress(Long fileNum) {
        return fileInfoMap.containsKey(fileNum);
    }

    synchronized int getRequiredUtil(Long fileNum) {
        FileInfo info = fileInfoMap.get(fileNum);
        return (info != null) ? info.requiredUtil : -1;
    }

    /**
     * Removes all references to a file.
     */
    synchronized FileInfo removeFile(Long fileNum, MemoryBudget budget) {
        FileInfo info = fileInfoMap.get(fileNum);
        if (info == null) {
            return null;
        }
        adjustMemoryBudget(budget, info.dbIds, null /*newDatabases*/);
        fileInfoMap.remove(fileNum);
        return info;
    }

    /**
     * When file cleaning is aborted, move the file back from BEING_CLEANED to
     * TO_BE_CLEANED.
     */
    synchronized void putBackFileForCleaning(Long fileNum) {
        assert checkStatus(fileNum, FileStatus.BEING_CLEANED);
        setStatus(fileNum, FileStatus.TO_BE_CLEANED);
    }

    /**
     * For unit testing.
     */
    public synchronized void injectFileForCleaning(Long fileNum) {
        if (!isFileCleaningInProgress(fileNum)) {
            final FileInfo info = setStatus(fileNum, FileStatus.TO_BE_CLEANED);
            info.requiredUtil = -1;
        }
    }

    /**
     * When cleaning is complete, move the file from the BEING_CLEANED to
     * CLEANED.
     */
    synchronized void addCleanedFile(Long fileNum,
                                     Set<DatabaseId> databases,
                                     VLSN firstVlsn,
                                     VLSN lastVlsn,
                                     MemoryBudget budget) {
        assert checkStatus(fileNum, FileStatus.BEING_CLEANED);
        FileInfo info = setStatus(fileNum, FileStatus.CLEANED);
        adjustMemoryBudget(budget, info.dbIds, databases);
        info.dbIds = databases;
        info.firstVlsn = firstVlsn;
        info.lastVlsn = lastVlsn;
    }

    /**
     * Returns a read-only copy of TO_BE_CLEANED files that can be accessed
     * without synchronization.
     */
    synchronized Set<Long> getToBeCleanedFiles() {
        return getFiles(FileStatus.TO_BE_CLEANED);
    }

    /**
     * Returns a copy of the CLEANED and FULLY_PROCESSED files at the time a
     * checkpoint starts.
     */
    synchronized CheckpointStartCleanerState getFilesAtCheckpointStart() {

        anyPendingDuringCheckpoint =
            !pendingLNs.isEmpty() ||
            !pendingDBs.isEmpty();

        return new CheckpointStartCleanerState(
            getFiles(FileStatus.CLEANED),
            getFiles(FileStatus.FULLY_PROCESSED));
    }

    /**
     * Returns whether any files are cleaned or fully-processed, meaning that a
     * checkpoint is needed before they can be deleted.
     */
    public synchronized boolean isCheckpointNeeded() {
        return getNumberOfFiles(FileStatus.CLEANED) > 0 ||
               getNumberOfFiles(FileStatus.FULLY_PROCESSED) > 0;
    }

    /**
     * When a checkpoint is complete, move the previously CLEANED and
     * FULLY_PROCESSED files to the CHECKPOINTED and reserved status.
     * Reserved files are removed from the FileSelector and their reserved
     * status is maintained in FileProtector.
     *
     * @return map of {fileNum, FileInfo} for the files whose status was
     * changed to reserved.
     */
    synchronized Map<Long, FileInfo> updateFilesAtCheckpointEnd(
        final EnvironmentImpl env,
        final CheckpointStartCleanerState info) {

        if (info.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<Long, FileInfo> reservedFiles = new HashMap<>();
        final Set<Long> previouslyCleanedFiles = info.getCleanedFiles();
        final Set<Long> previouslyProcessedFiles =
            info.getFullyProcessedFiles();

        if (previouslyCleanedFiles != null) {

            assert checkStatus(previouslyCleanedFiles, FileStatus.CLEANED);

            if (anyPendingDuringCheckpoint) {
                setStatus(previouslyCleanedFiles, FileStatus.CHECKPOINTED);
            } else {
                makeReservedFiles(env, previouslyCleanedFiles, reservedFiles);
            }
        }

        if (previouslyProcessedFiles != null) {

            assert checkStatus(previouslyProcessedFiles,
                               FileStatus.FULLY_PROCESSED);

            makeReservedFiles(env, previouslyProcessedFiles, reservedFiles);
        }

        updateProcessedFiles();

        return reservedFiles;
    }

    private void makeReservedFiles(
        final EnvironmentImpl env,
        final Set<Long> safeToDeleteFiles,
        final Map<Long, FileInfo> reservedFiles) {

        final FileProtector fileProtector = env.getFileProtector();
        final MemoryBudget memoryBudget = env.getMemoryBudget();

        for (Long file : safeToDeleteFiles) {
            final FileInfo info = removeFile(file, memoryBudget);
            fileProtector.reserveFile(file, info.lastVlsn);
            reservedFiles.put(file, info);
        }

        env.getUtilizationProfile().removeFileSummaries(safeToDeleteFiles);
    }

    /**
     * Adds the given LN info to the pending LN set.
     */
    synchronized boolean addPendingLN(final long logLsn, final LNInfo info) {
        
        anyPendingDuringCheckpoint = true;
        return pendingLNs.put(logLsn, info) != null;
    }

    /**
     * Returns a map of LNInfo for LNs that could not be migrated in a prior
     * cleaning attempt, or null if no LNs are pending.
     */
    synchronized Map<Long, LNInfo> getPendingLNs() {

        if (pendingLNs.size() > 0) {
            return new HashMap<>(pendingLNs);
        } else {
            return null;
        }
    }

    /**
     * Removes the LN for the given LSN from the pending LN set.
     */
    synchronized void removePendingLN(long originalLsn) {

        pendingLNs.remove(originalLsn);
        updateProcessedFiles();
    }

    /**
     * Returns number of LNs pending.
     */
    synchronized int getPendingLNQueueSize() {
        return pendingLNs.size();
    }

    /**
     * Adds the given DatabaseId to the pending DB set.
     */
    synchronized boolean addPendingDB(DatabaseId dbId) {

        boolean added = pendingDBs.add(dbId);

        anyPendingDuringCheckpoint = true;
        return added;
    }

    /**
     * Returns an array of DatabaseIds for DBs that were pending deletion in a
     * prior cleaning attempt, or null if no DBs are pending.
     */
    synchronized DatabaseId[] getPendingDBs() {

        if (pendingDBs.size() > 0) {
            DatabaseId[] dbs = new DatabaseId[pendingDBs.size()];
            pendingDBs.toArray(dbs);
            return dbs;
        } else {
            return null;
        }
    }

    /**
     * Removes the DatabaseId from the pending DB set.
     */
    synchronized void removePendingDB(DatabaseId dbId) {

        pendingDBs.remove(dbId);
        updateProcessedFiles();
    }

    /**
     * Returns a copy of the in-progress files, or an empty set if there are
     * none.
     */
    public synchronized NavigableSet<Long> getInProgressFiles() {
        return new TreeSet<>(fileInfoMap.keySet());
    }

    /**
     * Update memory budgets when the environment is closed and will never be
     * accessed again.
     */
    synchronized void close(MemoryBudget budget) {
        for (FileInfo info : fileInfoMap.values()) {
            adjustMemoryBudget(budget, info.dbIds, null /*newDatabases*/);
        }
    }

    /**
     * If there are no pending LNs or DBs outstanding, move the CHECKPOINTED
     * files to FULLY_PROCESSED.  The check for pending LNs/DBs and the copying
     * of the CHECKPOINTED files must be done atomically in a synchronized
     * block.  All methods that call this method are synchronized.
     */
    private void updateProcessedFiles() {
        if (pendingLNs.isEmpty() && pendingDBs.isEmpty()) {
            setStatus(FileStatus.CHECKPOINTED, FileStatus.FULLY_PROCESSED);
        }
    }

    /**
     * Adjust the memory budget when an entry is added to or removed from the
     * cleanedFilesDatabases map.
     */
    private void adjustMemoryBudget(MemoryBudget budget,
                                    Set<DatabaseId> oldDatabases,
                                    Set<DatabaseId> newDatabases) {
        long adjustMem = 0;
        if (oldDatabases != null) {
            adjustMem -= getCleanedFilesDatabaseEntrySize(oldDatabases);
        }
        if (newDatabases != null) {
            adjustMem += getCleanedFilesDatabaseEntrySize(newDatabases);
        }
        budget.updateAdminMemoryUsage(adjustMem);
    }

    /**
     * Returns the size of a HashMap entry that contains the given set of
     * DatabaseIds.  We don't count the DatabaseId size because it is likely
     * that it is also stored (and budgeted) in the DatabaseImpl.
     */
    private long getCleanedFilesDatabaseEntrySize(Set<DatabaseId> databases) {
        return MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
               MemoryBudget.HASHSET_OVERHEAD +
               (databases.size() * MemoryBudget.HASHSET_ENTRY_OVERHEAD);
    }

    /**
     * Holds copy of all checkpoint-dependent cleaner state.
     */
    public static class CheckpointStartCleanerState {

        /* A snapshot of the cleaner collections at the checkpoint start. */
        private Set<Long> cleanedFiles;
        private Set<Long> fullyProcessedFiles;

        private CheckpointStartCleanerState(Set<Long> cleanedFiles,
                                            Set<Long> fullyProcessedFiles) {

            /*
             * Save snapshots of the collections of various files at the
             * beginning of the checkpoint.
             */
            this.cleanedFiles = cleanedFiles;
            this.fullyProcessedFiles = fullyProcessedFiles;
        }

        public boolean isEmpty() {
            return ((cleanedFiles.size() == 0) &&
                    (fullyProcessedFiles.size() == 0));
        }

        public Set<Long> getCleanedFiles() {
            return cleanedFiles;
        }

        public Set<Long> getFullyProcessedFiles() {
            return fullyProcessedFiles;
        }
    }

    @Override
    public synchronized String toString() {
        return "files = " + fileInfoMap +
               " pendingLNs = " + pendingLNs +
               " pendingDBs = " + pendingDBs +
               " anyPendingDuringCheckpoint = " + anyPendingDuringCheckpoint;
    }
}
