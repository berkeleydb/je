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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.CleanerFileReader;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.OldBINDelta;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Reads all entries in a log file and either determines them to be obsolete or
 * active. Active LNs are migrated immediately (by logging them). Active INs
 * are marked for migration by setting the dirty flag.
 *
 * May be invoked explicitly by calling doClean, or woken up if used as a
 * daemon thread.
 */
public class FileProcessor extends DaemonThread {

    /**
     * The number of LN log entries after we process pending LNs.  If we do
     * this too seldom, the pending LN queue may grow large, and it isn't
     * budgeted memory.  If we process it too often, we will repeatedly request
     * a non-blocking lock for the same locked node.
     */
    private static final int PROCESS_PENDING_EVERY_N_LNS = 100;

    private final Cleaner cleaner;
    private final FileSelector fileSelector;
    private final UtilizationProfile profile;
    private final UtilizationCalculator calculator;

    /** @see #onWakeup() */
    private volatile boolean activate = false;
    private long lastWakeupLsn = 0;

    /*
     * The first thread (out of N cleaner threads) does certain housekeeping
     * duties that don't need to be done by all threads.
     */
    private final boolean firstThread;

    /* Log version for the target file. */
    private int fileLogVersion;

    /* Per Run counters. Reset before each file is processed. */

    /*
     * Number of full IN (BIN or UIN) logrecs that were known to be apriory
     * obsolete and did not need any further processing (i.e., they did not
     * need to be searched-for in the tree). These include logrecs whose
     * offset is recorded in the FS DB, or whose DB has been deleted or is
     * being deleted.
     */
    private int nINsObsoleteThisRun = 0;

    /*
     * Number of full IN (BIN or UIN) logrecs that were not known apriory to
     * be obsolete, and as a result, needed further processing.
     */
    private int nINsCleanedThisRun = 0;

    /*
     * Number of full IN (BIN or UIN) logrecs that were found to be obsolete
     * after having been looked up in the tree.
     */
    private int nINsDeadThisRun = 0;

    /*
     * Number of full IN (BIN or UIN) logrecs that were still active and were
     * marked dirty so that they will be migrated during the next ckpt.
     */
    private int nINsMigratedThisRun = 0;

    /*
     * Number of BIN-delta logrecs that were known to be apriory
     * obsolete and did not need any further processing (i.e., they did not
     * need to be searched-for in the tree). These include logrecs whose
     * offset is recorded in the FS DB, or whose DB has been deleted or is
     * being deleted.
     */
    private int nBINDeltasObsoleteThisRun = 0;

    /*
     * Number of BIN-delta logrecs that were not known apriory to be obsolete,
     * and as a result, needed further processing.
     */
    private int nBINDeltasCleanedThisRun = 0;

    /*
     * Number of BIN-delta logrecs that were found to be obsolete after having
     * been looked up in the tree.
     */
    private int nBINDeltasDeadThisRun = 0;

    /*
     * Number of BIN-delta logrecs that were still active and were marked
     * dirty so that they will be migrated during the next ckpt.
     */
    private int nBINDeltasMigratedThisRun = 0;

    /*
     * Number of LN logrecs that were known to be apriory obsolete and did not
     * need any further processing (for example, they did not need to be
     * searched-for in the tree). These include logrecs that are immediately
     * obsolete, or whose offset is recorded in the FS DB, or whose DB has been
     * deleted or is being deleted.
     */
    private int nLNsObsoleteThisRun = 0;

    /*
     * Number of LN logrecs that were expired.
     */
    private int nLNsExpiredThisRun = 0;

    /*
     * Number of LN logrecs that were not known apriory to be obsolete, and as
     * a result, needed further processing. These include LNs that had to be
     * searched-for in the tree as well as the nLNQueueHitsThisRun (see below).
     */
    private int nLNsCleanedThisRun = 0;

    /*
     * Number of LN logrecs that were processed without tree search. Let L1 and
     * L2 be two LN logrecs and R1 and R2 be their associated records. We will 
     * avoid a tree search for L1 if L1 is in the to-be-proccessed cache when
     * L2 is processed, R2 must be searched-for in the tree, R2 is found in a
     * BIN B, and L1 is also pointed-to by a slot in B.
     */
    private int nLNQueueHitsThisRun = 0;

    /*
     * Number of LN logrecs that were found to be obsolete after haning been
     * processed further.
     */
    private int nLNsDeadThisRun = 0;

    /*
     * Number of LN logrecs whose LSN had to be locked in order to check their
     * obsoleteness, and this non-blocking lock request was denied (and as a
     * result, the logrec was placed in the "pending LNs" queue.
     */
    private int nLNsLockedThisRun = 0;

    /*
     * Number of LN logrecs that were still active and were migrated.
     */
    private int nLNsMigratedThisRun = 0;

    /*
     * This applies to temporary DBs only. It is the number of LN logrecs that
     * were still active, but intead of migrating them, we attached the LN to
     * the meory-resident tree and marked the LN as dirty.
     */
    private int nLNsMarkedThisRun = 0;

    /*
     * Number of log entries read during cleaning.
     */
    private int nEntriesReadThisRun;

    private long nRepeatIteratorReadsThisRun;

    FileProcessor(String name,
                  boolean firstThread,
                  EnvironmentImpl env,
                  Cleaner cleaner,
                  UtilizationProfile profile,
                  UtilizationCalculator calculator,
                  FileSelector fileSelector) {
        super(0, name, env);
        this.cleaner = cleaner;
        this.fileSelector = fileSelector;
        this.profile = profile;
        this.calculator = calculator;
        this.firstThread = firstThread;
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    @Override
    protected long nDeadlockRetries() {
        return cleaner.nDeadlockRetries;
    }

    void activateOnWakeup() {
        activate = true;
    }

    /**
     * The thread is woken, either by an explicit notify (call to {@link
     * Cleaner#wakeupActivate()}, or when the timed wakeup interval elapses.
     *
     * In the former case (a call to wakeupActivate), the 'activate' field will
     * be true and the doClean method is called here. This happens when the
     * number of bytes written exceeds the cleanerBytesInterval, a config
     * change is made that could impact cleaning, etc.
     *
     * In the latter case (the wakeup interval elapsed), 'activate' will be
     * false. In this case, when there has been no writing since the last
     * wakeup, we perform cleaning and checkpointing, if needed to reclaim
     * space. This handles the situation where writing stops, but cleaning
     * or checkpointing or reserved file deletion may be needed. See {@link
     * com.sleepycat.je.EnvironmentConfig#CLEANER_WAKEUP_INTERVAL}.
     *
     * In all cases, when a disk limit is in violation we always call the
     * doClean method to ensure that {@link Cleaner#manageDiskUsage()} is
     * called in this situation. This is important to free disk space whenever
     * possible.
     */
    @Override
    protected synchronized void onWakeup() {

        if (!activate && cleaner.getDiskLimitViolation() == null) {
            /*
             * This is a timed wakeup and no disk limit is violated. We should
             * only call doClean if writing has stopped.
             */
            final long nextLsn = envImpl.getFileManager().getNextLsn();
            if (lastWakeupLsn != nextLsn) {
                /*
                 * If the last LSN in the log has changed since the last timed
                 * wakeup, do nothing, because writing has not stopped. As long
                 * as writing continues, we expect the cleaner and checkpointer
                 * to be woken via their byte interval params.
                 */
                lastWakeupLsn = nextLsn;
                return;
            }

            /*
             * There has been no writing since the last wakeup. Schedule a
             * checkpoint, if needed to reclaim disk space for already cleaned
             * files. Then fall through and activate (call doClean).
             */
            envImpl.getCheckpointer().wakeupAfterNoWrites();
        }

        doClean(
            true /*invokedFromDaemon*/,
            true /*cleanMultipleFiles*/,
            false /*forceCleaning*/);

        activate = false;
    }

    /**
     * Selects files to clean and cleans them. It returns the number of
     * successfully cleaned files. May be called by the daemon thread or
     * programatically.
     *
     * @param invokedFromDaemon currently has no effect.
     *
     * @param cleanMultipleFiles is true to clean until we're under budget,
     * or false to clean at most one file.
     *
     * @param forceCleaning is true to clean even if we're not under the
     * utilization threshold.
     *
     * @return the number of files cleaned, not including files cleaned
     * unsuccessfully.
     */
    synchronized int doClean(
        boolean invokedFromDaemon,
        boolean cleanMultipleFiles,
        boolean forceCleaning) {

        if (envImpl.isClosed()) {
            return 0;
        }

        /*
         * Get all file summaries including tracked files.  Tracked files may
         * be ready for cleaning if there is a large cache and many files have
         * not yet been flushed and do not yet appear in the profile map.
         */
        SortedMap<Long, FileSummary> fileSummaryMap =
            profile.getFileSummaryMap(true /*includeTrackedFiles*/);

        /* Clean until no more files are selected.  */
        final int nOriginalLogFiles = fileSummaryMap.size();
        int nFilesCleaned = 0;

        while (true) {

            /* Stop if the daemon is paused or the environment is closing. */
            if ((invokedFromDaemon && isPaused()) || envImpl.isClosing()) {
                break;
            }

            /*
             * Manage disk usage (refresh stats and delete files) periodically.
             *
             * Do this before cleaning, to reduce the chance of filling the
             * disk while cleaning and migrating/logging LNs. Also do it after
             * cleaning (before deciding whether to clean another file), even
             * if there are no more files to clean, to ensure space is freed
             * after a long run.
             */
            cleaner.manageDiskUsage();

            /*
             * Stop if we cannot write because of a disk limit violation. */
            try {
                envImpl.checkDiskLimitViolation();
            } catch (DiskLimitException e) {
                if (!invokedFromDaemon) {
                    throw e;
                }
                break;
            }

            /*
             * Process pending LNs periodically.  Pending LNs can prevent file
             * deletion.
             */
            cleaner.processPending();

            if (nFilesCleaned > 0) {

                /* If we should only clean one file, stop now. */
                if (!cleanMultipleFiles) {
                    break;
                }

                /* Don't clean forever. */
                if (nFilesCleaned >= nOriginalLogFiles) {
                    break;
                }

                /* Refresh file summary info for next file selection. */
                fileSummaryMap =
                    profile.getFileSummaryMap(true /*includeTrackedFiles*/);
            }

            /*
             * Select the next file for cleaning and update the Cleaner's
             * read-only file collections.
             */
            final Pair<Long, Integer> result =
                fileSelector.selectFileForCleaning(
                    calculator, fileSummaryMap, forceCleaning);

            /* Stop if no file is selected for cleaning. */
            if (result == null) {
                break;
            }

            final Long fileNum = result.first();
            final int requiredUtil = result.second();
            final boolean twoPass = (requiredUtil >= 0);

            boolean finished = false;
            boolean fileDeleted = false;
            final long fileNumValue = fileNum;

            final long runId = cleaner.totalRuns.incrementAndGet();
            final MemoryBudget budget = envImpl.getMemoryBudget();
            nFilesCleaned += 1;

            try {
                TestHookExecute.doHookIfSet(cleaner.fileChosenHook);

                /* Perform 1st pass of 2-pass cleaning. */
                String passOneMsg = "";
                if (twoPass) {

                    final FileSummary recalcSummary = new FileSummary();

                    final ExpirationTracker expTracker =
                        new ExpirationTracker(fileNumValue);

                    processFile(
                        fileNum, recalcSummary, new INSummary(), expTracker);

                    final int expiredSize =
                        expTracker.getExpiredBytes(TTL.currentSystemTime());

                    final int obsoleteSize = recalcSummary.getObsoleteSize();

                    final int recalcUtil = FileSummary.utilization(
                        obsoleteSize + expiredSize, recalcSummary.totalSize);

                    passOneMsg =
                        " pass1RecalcObsolete=" + obsoleteSize +
                        " pass1RecalcExpired=" + expiredSize +
                        " pass1RecalcUtil=" + recalcUtil +
                        " pass1RequiredUtil=" + requiredUtil;

                    if (recalcUtil > requiredUtil) {

                        cleaner.nRevisalRuns.increment();

                        cleaner.getExpirationProfile().putFile(
                            expTracker, expiredSize);

                        final String logMsg = "CleanerRevisalRun " + runId +
                            " on file 0x" + Long.toHexString(fileNumValue) +
                            " ends:" + passOneMsg;

                        LoggerUtils.logMsg(logger, envImpl, Level.INFO, logMsg);

                        fileSelector.removeFile(fileNum, budget);

                        finished = true;
                        continue;
                    }
                }

                resetPerRunCounters();
                cleaner.nCleanerRuns.increment();

                if (twoPass) {
                    cleaner.nTwoPassRuns.increment();
                }

                /* Keep track of estimated and true utilization. */
                final FileSummary estimatedFileSummary =
                    fileSummaryMap.containsKey(fileNum) ?
                    fileSummaryMap.get(fileNum).clone() : null;

                final FileSummary recalculatedFileSummary = new FileSummary();
                final INSummary inSummary = new INSummary();

                final String msgHeader =
                    (twoPass ? "CleanerTwoPassRun " : "CleanerRun ") +
                    runId + " on file 0x" + Long.toHexString(fileNumValue);

                final String beginMsg = msgHeader + " begins:";

                /* Trace is unconditional for log-based debugging. */
                LoggerUtils.traceAndLog(logger, envImpl, Level.FINE, beginMsg);

                /* Process all log entries in the file. */
                if (!processFile(
                    fileNum, recalculatedFileSummary, inSummary, null)) {
                    return nFilesCleaned;
                }

                /* File is fully processed, update stats. */
                accumulatePerRunCounters();
                finished = true;

                /* Trace is unconditional for log-based debugging. */
                final String endMsg = msgHeader + " ends:" +
                    " invokedFromDaemon=" + invokedFromDaemon +
                    " finished=" + finished +
                    " fileDeleted=" + fileDeleted +
                    " nEntriesRead=" + nEntriesReadThisRun +
                    " nINsObsolete=" + nINsObsoleteThisRun +
                    " nINsCleaned=" + nINsCleanedThisRun +
                    " nINsDead=" + nINsDeadThisRun +
                    " nINsMigrated=" + nINsMigratedThisRun +
                    " nBINDeltasObsolete=" + nBINDeltasObsoleteThisRun +
                    " nBINDeltasCleaned=" + nBINDeltasCleanedThisRun +
                    " nBINDeltasDead=" + nBINDeltasDeadThisRun +
                    " nBINDeltasMigrated=" + nBINDeltasMigratedThisRun +
                    " nLNsObsolete=" + nLNsObsoleteThisRun +
                    " nLNsCleaned=" + nLNsCleanedThisRun +
                    " nLNsDead=" + nLNsDeadThisRun +
                    " nLNsExpired=" + nLNsExpiredThisRun +
                    " nLNsMigrated=" + nLNsMigratedThisRun +
                    " nLNsMarked=" + nLNsMarkedThisRun +
                    " nLNQueueHits=" + nLNQueueHitsThisRun +
                    " nLNsLocked=" + nLNsLockedThisRun;

                Trace.trace(envImpl, endMsg);

                /* Only construct INFO level message if needed. */
                if (logger.isLoggable(Level.INFO)) {

                    final int estUtil = (estimatedFileSummary != null) ?
                        estimatedFileSummary.utilization() : -1;

                    final int recalcUtil =
                        recalculatedFileSummary.utilization();

                    LoggerUtils.logMsg(
                        logger, envImpl, Level.INFO,
                        endMsg +
                        " inSummary=" + inSummary +
                        " estSummary=" + estimatedFileSummary +
                        " recalcSummary=" + recalculatedFileSummary +
                        " estimatedUtil=" + estUtil +
                        " recalcUtil=" + recalcUtil +
                        passOneMsg);
                }
            } catch (FileNotFoundException e) {

                /*
                 * File was deleted.  Although it is possible that the file was
                 * deleted externally it is much more likely that the file was
                 * deleted normally after being cleaned earlier.  This can
                 * occur when tracked obsolete information is collected and
                 * processed after the file has been cleaned and deleted.
                 * Since the file does not exist, ignore the error so that the
                 * cleaner will continue.  Remove the file completely from the
                 * FileSelector, UtilizationProfile and ExpirationProfile so
                 * that we don't repeatedly attempt to process it. [#15528]
                 */
                fileDeleted = true;
                profile.removeDeletedFile(fileNum);
                cleaner.getExpirationProfile().removeFile(fileNum);
                fileSelector.removeFile(fileNum, budget);

                LoggerUtils.logMsg(
                    logger, envImpl, Level.INFO,
                    "Missing file 0x" + Long.toHexString(fileNum) +
                        " ignored by cleaner");

            } catch (IOException e) {

                LoggerUtils.traceAndLogException(
                    envImpl, "Cleaner", "doClean", "", e);

                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INTEGRITY, e);

            } catch (DiskLimitException e) {

                LoggerUtils.logMsg(
                    logger, envImpl, Level.WARNING,
                    "Cleaning of file 0x" + Long.toHexString(fileNum) +
                    " aborted because of disk limit violation: " + e);

                if (!invokedFromDaemon) {
                    throw e;
                }

            } catch (RuntimeException e) {

                LoggerUtils.traceAndLogException(
                    envImpl, "Cleaner", "doClean", "", e);

                throw e;

            } finally {
                if (!finished && !fileDeleted) {
                    fileSelector.putBackFileForCleaning(fileNum);
                }
            }
        }

        return nFilesCleaned;
    }

    /**
     * Calculates expired bytes without performing any migration or other side
     * effects. The expired sizes will not overlap with obsolete data, because
     * expired sizes are accumulated only for non-obsolete entries.
     *
     * @param fileNum file to read.
     *
     * @return the expiration tracker.
     */
    public ExpirationTracker countExpiration(long fileNum) {

        final ExpirationTracker tracker = new ExpirationTracker(fileNum);

        try {
            final boolean result = processFile(
                fileNum, new FileSummary(), new INSummary(), tracker);

            assert result;

        } catch (IOException e) {

            LoggerUtils.traceAndLogException(
                envImpl, "Cleaner", "countExpiration", "", e);

            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_INTEGRITY, e);
        }

        return tracker;
    }

    /**
     * Process all log entries in the given file.
     *
     * Note that we gather obsolete offsets at the beginning of the method and
     * do not check for obsolete offsets of entries that become obsolete while
     * the file is being processed.  An entry in this file can become obsolete
     * before we process it when normal application activity deletes or
     * updates the entry.  Also, large numbers of entries also become obsolete
     * as the result of LN migration while processing the file, but these
     * Checking the TrackedFileSummary while processing the file would be
     * expensive if it has many entries, because we perform a linear search in
     * the TFS.  There is a tradeoff between the cost of the TFS lookup and its
     * benefit, which is to avoid a tree search if the entry is obsolete.  Many
     * more lookups for non-obsolete entries than obsolete entries will
     * typically be done.  Because of the high cost of the linear search,
     * especially when processing large log files, we do not check the TFS.
     * [#19626]
     *
     * @param fileNum the file being cleaned.
     *
     * @param fileSummary used to return the true utilization.
     *
     * @param inSummary used to return IN utilization info for debugging.
     *
     * @param expTracker if non-null, enables countOnly mode, in which
     * expiration info is returned via this param, obsolete info returned via
     * fileSummary does not include expired data, and no migration is
     * performed, i.e., there are no side effects.
     *
     * @return false if we aborted file processing because the environment is
     * being closed.
     */
    private boolean processFile(Long fileNum,
                                FileSummary fileSummary,
                                INSummary inSummary,
                                ExpirationTracker expTracker)
        throws IOException {

        final boolean countOnly = (expTracker != null);
        final LockManager lockManager = envImpl.getTxnManager().getLockManager();

        /* Get the current obsolete offsets for this file. */
        final PackedOffsets obsoleteOffsets =
            profile.getObsoleteDetailPacked(fileNum, !countOnly /*logUpdate*/);

        final PackedOffsets.Iterator obsoleteIter = obsoleteOffsets.iterator();
        long nextObsolete = -1;

        /* Copy to local variables because they are mutable properties. */
        final int readBufferSize = cleaner.readBufferSize;
        final int lookAheadCacheSize =
            countOnly ? 0 : cleaner.lookAheadCacheSize;

        /*
         * Add the overhead of this method to the budget.  Two read buffers are
         * allocated by the file reader. The log size of the offsets happens to
         * be the same as the memory overhead.
         */
        final int adjustMem = (2 * readBufferSize) +
                               obsoleteOffsets.getLogSize() +
                               lookAheadCacheSize;
        final MemoryBudget budget = envImpl.getMemoryBudget();
        budget.updateAdminMemoryUsage(adjustMem);

        /* Evict after updating the budget. */
        if (Cleaner.DO_CRITICAL_EVICTION) {
            envImpl.daemonEviction(true /*backgroundIO*/);
        }

        /*
         * We keep a look ahead cache of non-obsolete LNs.  When we lookup a
         * BIN in processLN, we also process any other LNs in that BIN that are
         * in the cache.  This can reduce the number of tree lookups.
         */
        final LookAheadCache lookAheadCache =
            countOnly ? null : new LookAheadCache(lookAheadCacheSize);

        /*
         * For obsolete entries we must check for pending deleted DBs.  To
         * avoid the overhead of DbTree.getDb on every entry we keep a set of
         * all DB IDs encountered and do the check once per DB at the end.
         */
        final Set<DatabaseId> checkPendingDbSet =
            countOnly ? null : new HashSet<>();

        /*
         * Use local caching to reduce DbTree.getDb overhead.  Do not call
         * releaseDb after getDb with the dbCache, since the entire dbCache
         * will be released at the end of this method.
         */
        final Map<DatabaseId, DatabaseImpl> dbCache = new HashMap<>();
        final DbTree dbMapTree = envImpl.getDbTree();

        /*
         * Expired entries are counted obsolete so that this is reflected in
         * total utilization. A separate tracker is used so it can be added in
         * a single call under the log write latch.
         */
        final LocalUtilizationTracker localTracker =
            countOnly ? null : new LocalUtilizationTracker(envImpl);

        /* Keep track of all database IDs encountered. */
        final Set<DatabaseId> databases = new HashSet<>();

        /* Create the file reader. */
        final CleanerFileReader reader = new CleanerFileReader(
            envImpl, readBufferSize, DbLsn.makeLsn(fileNum, 0), fileNum,
            fileSummary, inSummary, expTracker);

        /* Validate all entries before ever deleting a file. */
        reader.setAlwaysValidateChecksum(true);

        try {
            final TreeLocation location = new TreeLocation();

            int nProcessedLNs = 0;
            int nProcessedEntries = 0;

            while (reader.readNextEntryAllowExceptions()) {

                nProcessedEntries += 1;
                cleaner.nEntriesRead.increment();

                int nReads = reader.getAndResetNReads();
                if (nReads > 0) {
                    cleaner.nDiskReads.add(nReads);
                }

                long logLsn = reader.getLastLsn();
                long fileOffset = DbLsn.getFileOffset(logLsn);
                boolean isLN = reader.isLN();
                boolean isIN = reader.isIN();
                boolean isBINDelta = reader.isBINDelta();
                boolean isOldBINDelta = reader.isOldBINDelta();
                boolean isDbTree = reader.isDbTree();
                boolean isObsolete = false;
                long expirationTime = 0;

                /* Remember the version of the log file. */
                if (reader.isFileHeader()) {
                    fileLogVersion = reader.getFileHeader().getLogVersion();
                    /* No expiration info exists before version 12. */
                    if (countOnly && fileLogVersion < 12) {
                        return true; // TODO caller must abort also
                    }
                }

                /* Stop if the daemon is shut down. */
                if (!countOnly && envImpl.isClosing()) {
                    return false;
                }

                /* Exit loop if we can't write. */
                if (!countOnly) {
                    envImpl.checkDiskLimitViolation();
                }

                /* Update background reads. */
                if (nReads > 0) {
                    envImpl.updateBackgroundReads(nReads);
                }

                /* Sleep if background read/write limit was exceeded. */
                envImpl.sleepAfterBackgroundIO();

                /* Check for a known obsolete node. */
                while (nextObsolete < fileOffset && obsoleteIter.hasNext()) {
                    nextObsolete = obsoleteIter.next();
                }
                if (nextObsolete == fileOffset) {
                    isObsolete = true;
                }

                /* Check for the entry type next because it is very cheap. */
                if (!isObsolete &&
                    !isLN &&
                    !isIN &&
                    !isBINDelta &&
                    !isOldBINDelta &&
                    !isDbTree) {
                    /* Consider all entries we do not process as obsolete. */
                    isObsolete = true;
                }

                /*
                 * Ignore deltas before log version 8.  Before the change to
                 * place deltas in the Btree (in JE 5.0), all deltas were
                 * considered obsolete by the cleaner.  Processing an old delta
                 * would be very wasteful (a Btree lookup, and possibly
                 * dirtying and flushing a BIN), and for duplicates databases
                 * could cause an exception due to the key format change.
                 * [#21405]
                 */
                if (!isObsolete &&
                    isOldBINDelta &&
                    fileLogVersion < 8) {
                    isObsolete = true;
                }

                /* Maintain a set of all databases encountered. */
                final DatabaseId dbId = reader.getDatabaseId();
                if (dbId != null) {
                    databases.add(dbId);
                }

                /*
                 * Get database. This is postponed until we need it, to reduce
                 * contention in DbTree.getDb.
                 */
                final DatabaseImpl db;
                if (!isObsolete && dbId != null) {


                    /*
                     * Clear DB cache after dbCacheClearCount entries, to
                     * prevent starving other threads that need exclusive
                     * access to the MapLN (for example, DbTree.deleteMapLN).
                     * [#21015]
                     */
                    if ((nProcessedEntries % cleaner.dbCacheClearCount) == 0) {
                        dbMapTree.releaseDbs(dbCache);
                        dbCache.clear();
                    }

                    db = dbMapTree.getDb(dbId, cleaner.lockTimeout, dbCache);

                    /*
                     * If the DB is gone, this entry is obsolete.  If delete
                     * cleanup is in progress, we will put the DB into the DB
                     * pending set further below.  This entry will be declared
                     * deleted after the delete cleanup is finished.
                     */
                    if (db == null || db.isDeleted()) {
                        isObsolete = true;
                    }
                } else {
                    db = null;
                }

                /*
                 * Also ignore INs in dup DBs before log version 8.  These must
                 * be obsolete, just as DINs and DBINs must be obsolete (and
                 * are also ignored here) after dup DB conversion.  Also, the
                 * old format IN key cannot be used for lookups. [#21405]
                 */
                if (!isObsolete &&
                    isIN &&
                    db.getSortedDuplicates() &&
                    fileLogVersion < 8) {
                    isObsolete = true;
                }

                if (!isObsolete && isLN) {

                    final LNLogEntry<?> lnEntry = reader.getLNLogEntry();

                    /*
                     * SR 14583: In JE 2.0 and later we can assume that all
                     * deleted LNs are obsolete. Either the delete committed
                     * and the BIN parent is marked with a pending deleted bit,
                     * or the delete rolled back, in which case there is no
                     * reference to this entry. JE 1.7.1 and earlier require a
                     * tree lookup because deleted LNs may still be reachable
                     * through their BIN parents.
                     */
                    if (lnEntry.isDeleted() && fileLogVersion > 2) {
                        isObsolete = true;
                    }

                    /* "Immediately obsolete" LNs can be discarded. */
                    if (!isObsolete && 
                        (db.isLNImmediatelyObsolete() ||
                         lnEntry.isEmbeddedLN())) {
                        isObsolete = true;
                    }

                    /*
                     * Check for expired LN. If locked, add to pending queue.
                     */
                    if (!isObsolete && !countOnly) {

                        expirationTime = TTL.expirationToSystemTime(
                            lnEntry.getExpiration(),
                            lnEntry.isExpirationInHours());

                        if (envImpl.expiresWithin(
                            expirationTime,
                            0 - envImpl.getTtlLnPurgeDelay())) {

                            if (!lockManager.isLockUncontended(logLsn)) {
                                fileSelector.addPendingLN(
                                    logLsn,
                                    new LNInfo(null /*LN*/, dbId,
                                        lnEntry.getKey(), expirationTime));
                                nLNsLockedThisRun++;
                                continue;
                            }

                            isObsolete = true;
                            nLNsExpiredThisRun += 1;

                            /*
                             * Inexact counting is used to avoid overhead of
                             * adding obsolete offset.
                             */
                            localTracker.countObsoleteNodeInexact(
                                logLsn, null /*type*/,
                                reader.getLastEntrySize(), db);
                        }
                    }
                }

                /* Skip known obsolete nodes. */
                if (isObsolete) {
                    /* Count obsolete stats. */
                    if (!countOnly) {
                        if (isLN) {
                            nLNsObsoleteThisRun++;
                        } else if (isBINDelta || isOldBINDelta) {
                            nBINDeltasObsoleteThisRun++;
                        } else if (isIN) {
                            nINsObsoleteThisRun++;
                        }
                    }
                    /* Update the pending DB set for obsolete entries. */
                    if (checkPendingDbSet != null && dbId != null) {
                        checkPendingDbSet.add(dbId);
                    }
                    /* Count utilization for obsolete entry. */
                    reader.countObsolete();
                    continue;
                }

                /* If not obsolete, count expired. */
                reader.countExpired();

                /* Don't process further if we are only calculating. */
                if (countOnly) {
                    continue;
                }

                /* Evict before processing each entry. */
                if (Cleaner.DO_CRITICAL_EVICTION) {
                    envImpl.daemonEviction(true /*backgroundIO*/);
                }

                /* The entry is not known to be obsolete -- process it now. */
                assert lookAheadCache != null;
                assert checkPendingDbSet != null;

                if (isLN) {

                    final LNLogEntry<?> lnEntry = reader.getLNLogEntry();
                    lnEntry.postFetchInit(db);

                    final LN targetLN = lnEntry.getLN();
                    final byte[] key = lnEntry.getKey();

                    lookAheadCache.add(
                        DbLsn.getFileOffset(logLsn),
                        new LNInfo(targetLN, dbId, key, expirationTime));

                    if (lookAheadCache.isFull()) {
                        processLN(fileNum, location, lookAheadCache, dbCache,
                                  checkPendingDbSet);
                    }

                    /*
                     * Process pending LNs before proceeding in order to
                     * prevent the pending list from growing too large.
                     */
                    nProcessedLNs += 1;
                    if (nProcessedLNs % PROCESS_PENDING_EVERY_N_LNS == 0) {
                        cleaner.processPending();
                    }

                } else if (isIN) {

                    final IN targetIN = reader.getIN(db);
                    targetIN.setDatabase(db);

                    processIN(targetIN, db, logLsn);

                } else if (isOldBINDelta) {

                    final OldBINDelta delta = reader.getOldBINDelta();
                    processOldBINDelta(delta, db, logLsn);

                } else if (isBINDelta) {

                    final BIN delta = reader.getBINDelta();
                    processBINDelta(delta, db, logLsn);

                } else if (isDbTree) {

                    envImpl.rewriteMapTreeRoot(logLsn);
                } else {
                    assert false;
                }
            }

            /* Don't process further if we are only calculating. */
            if (countOnly) {
                return true;
            }

            /* Process remaining queued LNs. */
            while (!lookAheadCache.isEmpty()) {
                if (Cleaner.DO_CRITICAL_EVICTION) {
                    envImpl.daemonEviction(true /*backgroundIO*/);
                }
                processLN(fileNum, location, lookAheadCache, dbCache,
                          checkPendingDbSet);
                /* Sleep if background read/write limit was exceeded. */
                envImpl.sleepAfterBackgroundIO();
            }

            /* Update the pending DB set. */
            for (final DatabaseId pendingDbId : checkPendingDbSet) {
                final DatabaseImpl db = dbMapTree.getDb
                    (pendingDbId, cleaner.lockTimeout, dbCache);
                cleaner.addPendingDB(db);
            }

            /* Update reader stats. */
            nEntriesReadThisRun = reader.getNumRead();
            nRepeatIteratorReadsThisRun = reader.getNRepeatIteratorReads();

            envImpl.getUtilizationProfile().flushLocalTracker(localTracker);

        } catch (ChecksumException e) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_CHECKSUM, e);
        } finally {
            /* Subtract the overhead of this method from the budget. */
            budget.updateAdminMemoryUsage(0 - adjustMem);

            /* Release all cached DBs. */
            dbMapTree.releaseDbs(dbCache);
        }

        /* File is fully processed, update status information. */
        fileSelector.addCleanedFile(
            fileNum, databases, reader.getFirstVLSN(), reader.getLastVLSN(),
            budget);

        return true;
    }

    /**
     * Processes the first LN in the look ahead cache and removes it from the
     * cache. While the BIN is latched, look through the BIN for other LNs in
     * the cache; if any match, process them to avoid a tree search later.
     */
    private void processLN(
        final Long fileNum,
        final TreeLocation location,
        final LookAheadCache lookAheadCache,
        final Map<DatabaseId, DatabaseImpl> dbCache,
        final Set<DatabaseId> checkPendingDbSet) {

        /* Get the first LN from the queue. */
        final Long offset = lookAheadCache.nextOffset();
        final LNInfo info = lookAheadCache.remove(offset);

        final LN lnFromLog = info.getLN();
        final byte[] keyFromLog = info.getKey();
        final long logLsn = DbLsn.makeLsn(fileNum, offset);

        /*
         * Refresh the DB before processing an LN, in case the DB has been
         * deleted since it was added to the lookAheadCache.  If the DB has
         * been deleted, perform the housekeeping tasks for an obsolete LN.
         *
         * Normally the DB will already be present (and non-deleted) in the
         * dbCache. But because of the lookAheadCache and the periodic clearing
         * of the dbCache in processFile, it's possible for a DB to be deleted
         * in between placing it in the lookAheadCache and the call to
         * processLN. [#22202]
         */
        final DatabaseId dbId = info.getDbId();

        final DatabaseImpl db = envImpl.getDbTree().getDb(
            dbId, cleaner.lockTimeout, dbCache);

        if (db == null || db.isDeleted()) {
            nLNsObsoleteThisRun++;
            if (checkPendingDbSet != null) {
                checkPendingDbSet.add(dbId);
            }
            return;
        }

        nLNsCleanedThisRun++;

        /* Status variables are used to generate debug tracing info. */
        boolean processedHere = true; // The LN was cleaned here.
        boolean obsolete = false;     // The LN is no longer in use.
        boolean completed = false;    // This method completed.

        BIN bin = null;
        Map<Long, LNInfo> pendingLNs = null;

        try {
            final Tree tree = db.getTree();
            assert tree != null;

            /* Find parent of this LN. */
            final boolean parentFound = tree.getParentBINForChildLN(
                location, keyFromLog, false /*splitsAllowed*/,
                false /*blindDeltaOps*/, Cleaner.UPDATE_GENERATION);

            bin = location.bin;
            final int index = location.index;

            if (!parentFound) {

                nLNsDeadThisRun++;
                obsolete = true;
                completed = true;
                return;
            }

            /*
             * Now we're at the BIN parent for this LN.  If knownDeleted, LN is
             * deleted and can be purged.
             */
            if (bin.isEntryKnownDeleted(index)) {
                nLNsDeadThisRun++;
                obsolete = true;
                completed = true;
                return;
            }

            /* Process this LN that was found in the tree. */
            processedHere = false;

            LNInfo pendingLN =
                processFoundLN(info, logLsn, bin.getLsn(index), bin, index);

            if (pendingLN != null) {
                pendingLNs = new HashMap<>();
                pendingLNs.put(logLsn, pendingLN);
            }

            completed = true;

            /*
             * For all other non-deleted LNs in this BIN, lookup their LSN
             * in the LN queue and process any matches.
             */
            for (int i = 0; i < bin.getNEntries(); i += 1) {

                final long binLsn = bin.getLsn(i);

                if (i != index &&
                    !bin.isEntryKnownDeleted(i) &&
                    !bin.isEntryPendingDeleted(i) &&
                    DbLsn.getFileNumber(binLsn) == fileNum) {

                    final Long myOffset = DbLsn.getFileOffset(binLsn);
                    final LNInfo myInfo = lookAheadCache.remove(myOffset);

                    /* If the offset is in the cache, it's a match. */
                    if (myInfo != null) {
                        nLNQueueHitsThisRun++;
                        nLNsCleanedThisRun++;

                        pendingLN =
                            processFoundLN(myInfo, binLsn, binLsn, bin, i);

                        if (pendingLN != null) {
                            if (pendingLNs == null) {
                                pendingLNs = new HashMap<>();
                            }
                            pendingLNs.put(binLsn, pendingLN);
                        }
                    }
                }
            }
        } finally {
            if (bin != null) {
                bin.releaseLatch();
            }

            /* BIN must not be latched when synchronizing on FileSelector. */
            if (pendingLNs != null) {
                for (Map.Entry<Long, LNInfo> entry : pendingLNs.entrySet()) {
                    fileSelector.addPendingLN(
                        entry.getKey(), entry.getValue());
                }
            }

            if (processedHere) {
                cleaner.logFine(Cleaner.CLEAN_LN, lnFromLog, logLsn,
                    completed, obsolete, false /*migrated*/);
            }
        }
    }

    /**
     * Processes an LN that was found in the tree.  Lock the LN's LSN and
     * then migrates the LN, if the LSN of the LN log entry is the active LSN
     * in the tree.
     *
     * @param info identifies the LN log entry.
     *
     * @param logLsn is the LSN of the log entry.
     *
     * @param treeLsn is the LSN found in the tree.
     *
     * @param bin is the BIN found in the tree; is latched on method entry and
     * exit.
     *
     * @param index is the BIN index found in the tree.
     *
     * @return a non-null LNInfo if it should be added to the pending LN list,
     * after releasing the BIN latch.
     */
    private LNInfo processFoundLN(
        final LNInfo info,
        final long logLsn,
        final long treeLsn,
        final BIN bin,
        final int index) {

        final LN lnFromLog = info.getLN();
        final byte[] key = info.getKey();

        final DatabaseImpl db = bin.getDatabase();
        final boolean isTemporary = db.isTemporary();

        /* Status variables are used to generate debug tracing info. */
        boolean obsolete = false;  // The LN is no longer in use.
        boolean migrated = false;  // The LN was in use and is migrated.
        boolean completed = false; // This method completed.

        BasicLocker locker = null;
        try {
            final Tree tree = db.getTree();
            assert tree != null;

            /*
             * Before migrating an LN, we must lock it and then check to see
             * whether it is obsolete or active.
             *
             * 1. If the LSN in the tree and in the log are the same, we will
             * attempt to migrate it.
             *
             * 2. If the LSN in the tree is < the LSN in the log, the log entry
             * is obsolete, because this LN has been rolled back to a previous
             * version by a txn that aborted.
             *
             * 3. If the LSN in the tree is > the LSN in the log, the log entry
             * is obsolete, because the LN was advanced forward by some
             * now-committed txn.
             *
             * 4. If the LSN in the tree is a null LSN, the log entry is
             * obsolete. A slot can only have a null LSN if the record has
             * never been written to disk in a deferred write database, and
             * in that case the log entry must be for a past, deleted version
             * of that record.
             */
            if (lnFromLog.isDeleted() &&
                treeLsn == logLsn &&
                fileLogVersion <= 2) {

                /*
                 * SR 14583: After JE 2.0, deleted LNs are never found in the
                 * tree, since we can assume they're obsolete and correctly
                 * marked as such in the obsolete offset tracking. JE 1.7.1 and
                 * earlier did not use the pending deleted bit, so deleted LNs
                 * may still be reachable through their BIN parents.
                 */
                obsolete = true;
                nLNsDeadThisRun++;
                bin.setPendingDeleted(index);
                completed = true;
                return null;
            }

            if (treeLsn == DbLsn.NULL_LSN) {

                /*
                 * Case 4: The LN in the tree is a never-written LN for a
                 * deferred-write db, so the LN in the file is obsolete.
                 */
                nLNsDeadThisRun++;
                obsolete = true;
                completed = true;
                return null;
            }

            if (treeLsn != logLsn && isTemporary) {

                /*
                 * Temporary databases are always non-transactional.  If the
                 * tree and log LSNs are different then we know that the logLsn
                 * is obsolete.  Even if the LN is locked, the tree cannot be
                 * restored to the logLsn because no abort is possible without
                 * a transaction.  We should consider a similar optimization in
                 * the future for non-transactional durable databases.
                 */
                nLNsDeadThisRun++;
                obsolete = true;
                completed = true;
                return null;
            }

            if (!isTemporary) {

                /*
                 * Get a lock on the LN if we will migrate it now. (Temporary
                 * DB LNs are dirtied below and migrated later.)
                 *
                 * We can hold the latch on the BIN since we always attempt to
                 * acquire a non-blocking read lock.
                 */
                locker = BasicLocker.createBasicLocker(envImpl, false /*noWait*/);
                /* Don't allow this short-lived lock to be preempted/stolen. */
                locker.setPreemptable(false);
                final LockResult lockRet = locker.nonBlockingLock(
                    treeLsn, LockType.READ, false /*jumpAheadOfWaiters*/, db);

                if (lockRet.getLockGrant() == LockGrantType.DENIED) {

                    /*
                     * LN is currently locked by another Locker, so we can't
                     * assume anything about the value of the LSN in the bin.
                     */
                    nLNsLockedThisRun++;
                    completed = true;

                    return new LNInfo(
                        null /*LN*/, db.getId(), key,
                        info.getExpirationTime());
                }

                if (treeLsn != logLsn) {
                    /* The LN is obsolete and can be purged. */
                    nLNsDeadThisRun++;
                    obsolete = true;
                    completed = true;
                    return null;
                }
            }

            /*
             * The LN must be migrated because it is not obsolete, the lock was
             * not denied, and treeLsn==logLsn.
             */
            assert !obsolete;
            assert treeLsn == logLsn;

            if (bin.isEmbeddedLN(index)) {
                throw EnvironmentFailureException.unexpectedState(
                    envImpl,
                    "LN is embedded although its associated logrec (at " +
                    logLsn + " does not have the embedded flag on");
            }

            /*
             * For active LNs in non-temporary DBs, migrate the LN now.
             * In this case we acquired a lock on the LN above.
             *
             * If the LN is not resident, populate it using the LN we read
             * from the log so it does not have to be fetched.  We must
             * call postFetchInit to initialize MapLNs that have not been
             * fully initialized yet [#13191].  When explicitly migrating
             * (for a non-temporary DB) we will evict the LN after logging.
             *
             * Note that we do not load LNs from the off-heap cache here
             * because it's unnecessary. We have the current LN in hand
             * (from the log) and the off-heap cache does not hold dirty
             * LNs, so the IN in hand is identical to the off-heap LN.
             *
             * MapLNs must be logged by DbTree.modifyDbRoot (the Tree root
             * latch must be held) [#23492]. Here we simply dirty it via
             * setDirty, which ensures it will be logged during the next
             * checkpoint. Delaying until the next checkpoint also allows
             * for write absorption, since MapLNs are often logged every
             * checkpoint due to utilization changes.
             *
             * For temporary databases, we wish to defer logging for as
             * long as possible.  Therefore, dirty the LN to ensure it is
             * flushed before its parent is written.  Because we do not
             * attempt to lock temporary database LNs (see above) we know
             * that if it is non-obsolete, the tree and log LSNs are equal.
             * If the LN from the log was populated here, it will be left
             * in place for logging at a later time.
             *
             * Also for temporary databases, make both the target LN and
             * the BIN or IN parent dirty. Otherwise, when the BIN or IN is
             * evicted in the future, it will be written to disk without
             * flushing its dirty, migrated LNs.  [#18227]
             */
            if (bin.getTarget(index) == null) {
                lnFromLog.postFetchInit(db, logLsn);
                /* Ensure keys are transactionally correct. [#15704] */
                bin.attachNode(index, lnFromLog, key /*lnSlotKey*/);
            }

            if (db.getId().equals(DbTree.ID_DB_ID)) {
                final MapLN targetLn = (MapLN) bin.getTarget(index);
                assert targetLn != null;
                targetLn.getDatabase().setDirty();

            } else if (isTemporary) {
                ((LN) bin.getTarget(index)).setDirty();
                bin.setDirty(true);
                nLNsMarkedThisRun++;

            } else {
                final LN targetLn = (LN) bin.getTarget(index);
                assert targetLn != null;

                final LogItem logItem = targetLn.log(
                    envImpl, db, null /*locker*/, null /*writeLockInfo*/,
                    false /*newEmbeddedLN*/, bin.getKey(index),
                    bin.getExpiration(index), bin.isExpirationInHours(),
                    false /*newEmbeddedLN*/, logLsn,
                    bin.getLastLoggedSize(index),
                    false/*isInsertion*/, true /*backgroundIO*/,
                    Cleaner.getMigrationRepContext(targetLn));

                bin.updateEntry(
                    index, logItem.lsn, targetLn.getVLSNSequence(),
                    logItem.size);

                /* Evict LN if we populated it with the log LN. */
                if (lnFromLog == targetLn) {
                    bin.evictLN(index);
                }

                /* Lock new LSN on behalf of existing lockers. */
                CursorImpl.lockAfterLsnChange(
                    db, logLsn, logItem.lsn, locker /*excludeLocker*/);

                nLNsMigratedThisRun++;
            }

            migrated = true;
            completed = true;
            return null;

        } finally {
            if (locker != null) {
                locker.operationEnd();
            }

            cleaner.logFine(Cleaner.CLEAN_LN, lnFromLog, logLsn, completed,
                obsolete, migrated);
        }
    }

    /**
     * If this OldBINDelta is still in use in the in-memory tree, dirty the
     * associated BIN. The next checkpoint will log a new delta or a full
     * version, which will make this delta obsolete.
     *
     * For OldBINDeltas, we do not optimize and must fetch the BIN if it is not
     * resident.
     */
    private void processOldBINDelta(
        OldBINDelta deltaClone,
        DatabaseImpl db,
        long logLsn) {

        nBINDeltasCleanedThisRun++;

        /*
         * Search Btree for the BIN associated with this delta.
         */
        final byte[] searchKey = deltaClone.getSearchKey();

        final BIN treeBin = db.getTree().search(
            searchKey, Cleaner.UPDATE_GENERATION);

        if (treeBin == null) {
            /* BIN for this delta is no longer in the tree. */
            nBINDeltasDeadThisRun++;
            return;
        }

        /* Tree BIN is non-null and latched. */
        try {
            final long treeLsn = treeBin.getLastLoggedLsn();

            if (treeLsn == DbLsn.NULL_LSN) {
                /* Current version was never logged. */
                nBINDeltasDeadThisRun++;
                return;
            }

            final int cmp = DbLsn.compareTo(treeLsn, logLsn);

            if (cmp > 0) {
                /* Log entry is obsolete. */
                nBINDeltasDeadThisRun++;
                return;
            }

            /*
             * Log entry is same or newer than what's in the tree.  Dirty the
             * BIN and let the checkpoint write it out.  There is no need to
             * prohibit a delta when the BIN is next logged (as is done when
             * migrating full INs) because logging a new delta will obsolete
             * this delta.
             */
            treeBin.setDirty(true);
            nBINDeltasMigratedThisRun++;

        } finally {
            treeBin.releaseLatch();
        }
    }

    /**
     * If this BIN-delta is still in use in the in-memory tree, dirty the
     * associated BIN. The next checkpoint will log a new delta or a full
     * version, which will make this delta obsolete.
     *
     * We optimize by placing the delta from the log into the tree when the
     * BIN is not resident.
     */
    private void processBINDelta(
        BIN deltaClone,
        DatabaseImpl db,
        long logLsn) {

        nBINDeltasCleanedThisRun++;

        /* Search for the BIN's parent by level, to avoid fetching the BIN. */
        deltaClone.setDatabase(db);
        deltaClone.latch(CacheMode.UNCHANGED);

        final SearchResult result = db.getTree().getParentINForChildIN(
            deltaClone, true /*useTargetLevel*/,
            true /*doFetch*/, CacheMode.UNCHANGED);

        try {
            if (!result.exactParentFound) {
                /* BIN for this delta is no longer in the tree. */
                nBINDeltasDeadThisRun++;
                return;
            }

            final long treeLsn = result.parent.getLsn(result.index);
            if (treeLsn == DbLsn.NULL_LSN) {
                /* Current version was never logged. */
                nBINDeltasDeadThisRun++;
                return;
            }

            /*
             * If cmp is > 0 then log entry is obsolete because it is older
             * than the version in the tree.
             *
             * If cmp is < 0 then log entry is also obsolete, because the old
             * parent slot was deleted and we're now looking at a completely
             * different IN due to the by-level search above.
             */
            final int cmp = DbLsn.compareTo(treeLsn, logLsn);
            if (cmp != 0) {
                /* Log entry is obsolete. */
                nBINDeltasDeadThisRun++;
                return;
            }

            /*
             * Log entry is the version that's in the tree. Dirty the BIN and
             * let the checkpoint write it out. There is no need to prohibit a
             * delta when the BIN is next logged (as is done when migrating
             * full BINs) because logging a new delta will obsolete this delta.
             */
            BIN treeBin = (BIN) result.parent.loadIN(
                result.index, CacheMode.UNCHANGED);

            if (treeBin == null) {
                /* Place delta from log into tree to avoid fetching. */
                treeBin = deltaClone;
                treeBin.latchNoUpdateLRU(db);

                treeBin.postFetchInit(db, logLsn);

                result.parent.attachNode(
                    result.index, treeBin, null /*lnSlotKey*/);
            } else {
                treeBin.latch(CacheMode.UNCHANGED);
            }

            /*
             * Compress to reclaim space for expired slots, including dirty
             * slots. However, if treeBin is a BIN-delta, this does nothing.
             */
            envImpl.lazyCompress(treeBin, true /*compressDirtySlots*/);

            treeBin.setDirty(true);
            treeBin.releaseLatch();

            nBINDeltasMigratedThisRun++;

        } finally {
            if (result.parent != null) {
                result.parent.releaseLatch();
            }
        }
    }

    /**
     * If an IN is still in use in the in-memory tree, dirty it. The checkpoint
     * invoked at the end of the cleaning run will end up rewriting it.
     */
    private void processIN(
        IN inClone,
        DatabaseImpl db,
        long logLsn) {

        boolean obsolete = false;
        boolean dirtied = false;
        boolean completed = false;

        try {
            nINsCleanedThisRun++;

            Tree tree = db.getTree();
            assert tree != null;

            IN inInTree = findINInTree(tree, db, inClone, logLsn);

            if (inInTree == null) {
                /* IN is no longer in the tree.  Do nothing. */
                nINsDeadThisRun++;
                obsolete = true;
            } else {

                /*
                 * IN is still in the tree.  Dirty it.  Checkpoint or eviction
                 * will write it out.
                 *
                 * Prohibit the next delta, since the original version must be
                 * made obsolete.
                 *
                 * Compress to reclaim space for expired slots, including dirty
                 * slots.
                 */
                nINsMigratedThisRun++;
                inInTree.setDirty(true);
                inInTree.setProhibitNextDelta(true);
                envImpl.lazyCompress(inInTree, true /*compressDirtySlots*/);
                inInTree.releaseLatch();
                dirtied = true;
            }

            completed = true;
        } finally {
            cleaner.logFine(Cleaner.CLEAN_IN, inClone, logLsn, completed,
                            obsolete, dirtied);
        }
    }

    /**
     * Given a clone of an IN that has been taken out of the log, try to find
     * it in the tree and verify that it is the current one in the log.
     * Returns the node in the tree if it is found and it is current re: LSN's.
     * Otherwise returns null if the clone is not found in the tree or it's not
     * the latest version.  Caller is responsible for unlatching the returned
     * IN.
     */
    private IN findINInTree(
        Tree tree,
        DatabaseImpl db,
        IN inClone,
        long logLsn) {

        /* Check if inClone is the root. */
        if (inClone.isRoot()) {
            IN rootIN = isRoot(tree, db, inClone, logLsn);
            if (rootIN == null) {

                /*
                 * inClone is a root, but no longer in use. Return now, because
                 * a call to tree.getParentNode will return something
                 * unexpected since it will try to find a parent.
                 */
                return null;
            } else {
                return rootIN;
            }
        }

        /* It's not the root.  Can we find it, and if so, is it current? */
        inClone.latch(Cleaner.UPDATE_GENERATION);
        SearchResult result = null;
        try {
            result = tree.getParentINForChildIN(
                inClone, true /*useTargetLevel*/,
                true /*doFetch*/, Cleaner.UPDATE_GENERATION);

            if (!result.exactParentFound) {
                return null;
            }

            /* Note that treeLsn may be for a BIN-delta, see below. */
            IN parent = result.parent;
            long treeLsn = parent.getLsn(result.index);

            /*
             * The IN in the tree is a never-written IN for a DW db so the IN
             * in the file is obsolete. [#15588]
             */
            if (treeLsn == DbLsn.NULL_LSN) {
                return null;
            }

            /*
             * If tree and log LSNs are equal, then we've found the exact IN we
             * read from the log.  We know the treeLsn is not for a BIN-delta,
             * because it is equal to LSN of the IN (or BIN) we read from the
             * log.  To avoid a fetch, we can place the inClone in the tree if
             * it is not already resident, or use the inClone to mutate the
             * delta in the tree to a full BIN.
             */
            if (treeLsn == logLsn) {
                IN in = parent.loadIN(result.index, Cleaner.UPDATE_GENERATION);

                if (in != null) {

                    in.latch(Cleaner.UPDATE_GENERATION);

                    if (in.isBINDelta()) {
                        /*
                         * The BIN should be dirty here because the most
                         * recently written logrec for it is a full-version
                         * logrec. After that logrec was written, the BIN
                         * was dirtied again, and then mutated to a delta.
                         * So this delta should still be dirty.
                         */
                        assert(in.getDirty());

                        /*
                         * Since we want to clean the inClone full version of
                         * the bin, we must mutate the cached delta to a full 
                         * BIN so that the next logrec for this BIN can be a
                         * full-version logrec.
                         */
                        final BIN bin = (BIN) in;
                        bin.mutateToFullBIN(
                            (BIN) inClone, false /*leaveFreeSlot*/);
                    }
                } else {
                    in = inClone;

                    /*
                     * Latch before calling postFetchInit and attachNode to
                     * make those operations atomic. Must use latchNoUpdateLRU
                     * before the node is attached.
                     */
                    in.latchNoUpdateLRU(db);
                    in.postFetchInit(db, logLsn);
                    parent.attachNode(result.index, in, null /*lnSlotKey*/);
                }

                return in;
            }

            if (inClone.isUpperIN()) {
                /* No need to deal with BIN-deltas. */
                return null;
            }

            /*
             * If the tree and log LSNs are unequal, then we must get the full
             * version LSN in case the tree LSN is actually for a BIN-delta.
             * The only way to do that is to fetch the IN in the tree; however,
             * we only need the delta not the full BIN.
             */
            final BIN bin =
                (BIN) parent.fetchIN(result.index, Cleaner.UPDATE_GENERATION);

            treeLsn = bin.getLastFullLsn();

            /* Now compare LSNs, since we know treeLsn is the full version. */
            final int compareVal = DbLsn.compareTo(treeLsn, logLsn);

            /*
             * If cmp is > 0 then log entry is obsolete because it is older
             * than the version in the tree.
             *
             * If cmp is < 0 then log entry is also obsolete, because the old
             * parent slot was deleted and we're now looking at a completely
             * different IN due to the by-level search above.
             */
            if (compareVal != 0) {
                return null;
            }

            /*
             * Log entry is the full version associated with the BIN-delta
             * that's in the tree.  To avoid a fetch, we can use the inClone to
             * mutate the delta in the tree to a full BIN.
             */
            bin.latch(Cleaner.UPDATE_GENERATION);
            if (bin.isBINDelta()) {
                bin.mutateToFullBIN((BIN) inClone, false /*leaveFreeSlot*/);
            }

            return bin;

        } finally {
            if (result != null && result.exactParentFound) {
                result.parent.releaseLatch();
            }
        }
    }

    /**
     * Get the current root in the tree, or null if the inClone is not the
     * current root.
     */
    private static class RootDoWork implements WithRootLatched {
        private final DatabaseImpl db;
        private final IN inClone;
        private final long logLsn;

        RootDoWork(DatabaseImpl db, IN inClone, long logLsn) {
            this.db = db;
            this.inClone = inClone;
            this.logLsn = logLsn;
        }

        public IN doWork(ChildReference root) {

            if (root == null ||
                (root.getLsn() == DbLsn.NULL_LSN) || // deferred write root
                (((IN) root.fetchTarget(db, null)).getNodeId() !=
                 inClone.getNodeId())) {
                return null;
            }

            /*
             * A root LSN less than the log LSN must be an artifact of when we
             * didn't properly propagate the logging of the rootIN up to the
             * root ChildReference.  We still do this for compatibility with
             * old log versions but may be able to remove it in the future.
             */
            if (DbLsn.compareTo(root.getLsn(), logLsn) <= 0) {
                IN rootIN = (IN) root.fetchTarget(db, null);
                rootIN.latch(Cleaner.UPDATE_GENERATION);
                return rootIN;
            } else {
                return null;
            }
        }
    }

    /**
     * Check if the cloned IN is the same node as the root in tree.  Return the
     * real root if it is, null otherwise.  If non-null is returned, the
     * returned IN (the root) is latched -- caller is responsible for
     * unlatching it.
     */
    private IN isRoot(Tree tree, DatabaseImpl db, IN inClone, long lsn) {
        RootDoWork rdw = new RootDoWork(db, inClone, lsn);
        return tree.withRootLatchedShared(rdw);
    }

    /**
     * Reset per-run counters.
     */
    private void resetPerRunCounters() {
        nINsObsoleteThisRun = 0;
        nINsCleanedThisRun = 0;
        nINsDeadThisRun = 0;
        nINsMigratedThisRun = 0;
        nBINDeltasObsoleteThisRun = 0;
        nBINDeltasCleanedThisRun = 0;
        nBINDeltasDeadThisRun = 0;
        nBINDeltasMigratedThisRun = 0;
        nLNsObsoleteThisRun = 0;
        nLNsExpiredThisRun = 0;
        nLNsCleanedThisRun = 0;
        nLNsDeadThisRun = 0;
        nLNsMigratedThisRun = 0;
        nLNsMarkedThisRun = 0;
        nLNQueueHitsThisRun = 0;
        nLNsLockedThisRun = 0;
        nEntriesReadThisRun = 0;
        nRepeatIteratorReadsThisRun = 0;
    }

    /**
     * Add per-run counters to total counters.
     */
    private void accumulatePerRunCounters() {
        cleaner.nINsObsolete.add(nINsObsoleteThisRun);
        cleaner.nINsCleaned.add(nINsCleanedThisRun);
        cleaner.nINsDead.add(nINsDeadThisRun);
        cleaner.nINsMigrated.add(nINsMigratedThisRun);
        cleaner.nBINDeltasObsolete.add(nBINDeltasObsoleteThisRun);
        cleaner.nBINDeltasCleaned.add(nBINDeltasCleanedThisRun);
        cleaner.nBINDeltasDead.add(nBINDeltasDeadThisRun);
        cleaner.nBINDeltasMigrated.add(nBINDeltasMigratedThisRun);
        cleaner.nLNsObsolete.add(nLNsObsoleteThisRun);
        cleaner.nLNsExpired.add(nLNsExpiredThisRun);
        cleaner.nLNsCleaned.add(nLNsCleanedThisRun);
        cleaner.nLNsDead.add(nLNsDeadThisRun);
        cleaner.nLNsMigrated.add(nLNsMigratedThisRun);
        cleaner.nLNsMarked.add(nLNsMarkedThisRun);
        cleaner.nLNQueueHits.add(nLNQueueHitsThisRun);
        cleaner.nLNsLocked.add(nLNsLockedThisRun);
        cleaner.nRepeatIteratorReads.add(nRepeatIteratorReadsThisRun);
    }

    /**
     * A cache of LNInfo by LSN offset.  Used to hold a set of LNs that are
     * to be processed.  Keeps track of memory used, and when full (over
     * budget) the next offset should be queried and removed.
     */
    private static class LookAheadCache {

        private final SortedMap<Long,LNInfo> map;
        private final int maxMem;
        private int usedMem;

        LookAheadCache(int lookAheadCacheSize) {
            map = new TreeMap<>();
            maxMem = lookAheadCacheSize;
            usedMem = MemoryBudget.TREEMAP_OVERHEAD;
        }

        boolean isEmpty() {
            return map.isEmpty();
        }

        boolean isFull() {
            return usedMem >= maxMem;
        }

        Long nextOffset() {
            return map.firstKey();
        }

        void add(Long lsnOffset, LNInfo info) {
            map.put(lsnOffset, info);
            usedMem += info.getMemorySize();
            usedMem += MemoryBudget.TREEMAP_ENTRY_OVERHEAD;
        }

        LNInfo remove(Long offset) {
            LNInfo info = map.remove(offset);
            if (info != null) {
                usedMem -= info.getMemorySize();
                usedMem -= MemoryBudget.TREEMAP_ENTRY_OVERHEAD;
            }
            return info;
        }
    }
}
