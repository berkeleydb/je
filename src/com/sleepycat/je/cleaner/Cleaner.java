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

import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_ACTIVE_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_AVAILABLE_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_CLUSTER_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_DELETIONS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_DISK_READS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_ENTRIES_READ;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNQUEUE_HITS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_EXPIRED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_LOCKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_MARKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MARKED_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MAX_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MIN_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LNS_LOCKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LN_QUEUE_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE_MAP;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_REPEAT_ITERATOR_READS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_RESERVED_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_REVISAL_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TOTAL_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TO_BE_CLEANED_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TWO_PASS_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.GROUP_DESC;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.GROUP_NAME;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.FileSelector.CheckpointStartCleanerState;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.AtomicLongMapStat;
import com.sleepycat.je.utilint.DaemonRunner;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.FileStoreInfo;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;

/**
 * The Cleaner is responsible for effectively garbage collecting the JE log.
 * It selects the least utilized log file for cleaning (see FileSelector),
 * reads through the log file (FileProcessor) and determines whether each entry
 * is obsolete (no longer relevant) or active (referenced by the Btree).
 * Entries that are active are migrated (copied) to the end of the log, and
 * finally the cleaned file is deleted.
 *
 * The migration of active entries is a multi-step process that can be
 * configured to operate in different ways.  Eviction and checkpointing, as
 * well as the cleaner threads (FileProcessor instances) are participants in
 * this process.  Migration may be immediate or lazy.
 *
 * Active INs are always migrated lazily, which means that they are marked
 * dirty by the FileProcessor, and then logged later by an eviction or
 * checkpoint.  Active LNs are always migrated immediately by the FileProcessor
 * by logging them.
 *
 * When the FileProcessor is finished with a file, all lazy migration for that
 * file is normally completed by the end of the next checkpoint, if not sooner
 * via eviction.  The checkpoint/recovery mechanism will ensure that obsolete
 * entries will not be referenced by the Btree.  At the end of the checkpoint,
 * it is therefore safe to delete the log file.
 *
 * There is one exception to the above paragraph.  When attempting to migrate
 * an LN, if the LN cannot be locked then we must retry the migration at a
 * later time.  Also, if a database removal is in progress, we consider all
 * entries in the database obsolete but cannot delete the log file until
 * database removal is complete.  Such "pending" LNs and databases are queued
 * and processed periodically during file processing and at the start of a
 * checkpoint; see processPending().  In this case, we may have to wait for
 * more than one checkpoint to occur before the log file can be deleted.  See
 * FileSelector and the use of the pendingLNs and pendingDBs collections.
 */
public class Cleaner implements DaemonRunner, EnvConfigObserver {
    /* From cleaner */
    static final String CLEAN_IN = "CleanIN:";
    static final String CLEAN_LN = "CleanLN:";
    static final String CLEAN_PENDING_LN = "CleanPendingLN:";

    private static final NumberFormat INT_FORMAT =
        NumberFormat.getIntegerInstance();

    /**
     * The CacheMode to use for Btree searches.  This is currently UNCHANGED
     * because we update the generation of the BIN when we migrate an LN.
     * In other other cases, it is not desirable to keep INs in cache.
     */
    static final CacheMode UPDATE_GENERATION = CacheMode.UNCHANGED;

    /**
     * Whether the cleaner should participate in critical eviction.  Ideally
     * the cleaner would not participate in eviction, since that would reduce
     * the cost of cleaning.  However, the cleaner can add large numbers of
     * nodes to the cache.  By not participating in eviction, other threads
     * could be kept in a constant state of eviction and would effectively
     * starve.  Therefore, this setting is currently enabled.
     */
    static final boolean DO_CRITICAL_EVICTION = true;

    private static final String DELETED_SUBDIR = "deleted";

    /* Used to ensure that the cleaner is woken often enough. */
    private final static long MAX_CLEANER_BYTES_INTERVAL = 100L << 20;;

    /* 10GB is the lower threshold for adjusting MAX_DISK. */
    private final static long MAX_DISK_ADJUSTMENT_THRESHOLD =
        10L * 1024L * 1024L * 1024L;

    /* Used to disable processing of safe-to-delete files during testing. */
    private boolean fileDeletionEnabled = true;

    /* Used to limit manageDiskUsage calls to one thread at a time. */
    private final ReentrantLock manageDiskUsageLock = new ReentrantLock();

    /*
     * Cleaner stats. Updates to these counters occur in multiple threads,
     * including FileProcessor threads, and are not synchronized. This could
     * produce errors in counting, but avoids contention around stat updates.
     */
    private final StatGroup statGroup;
    final LongStat nCleanerRuns;
    final LongStat nTwoPassRuns;
    final LongStat nRevisalRuns;
    private final LongStat nCleanerDeletions;
    final LongStat nINsObsolete;
    final LongStat nINsCleaned;
    final LongStat nINsDead;
    final LongStat nINsMigrated;
    final LongStat nBINDeltasObsolete;
    final LongStat nBINDeltasCleaned;
    final LongStat nBINDeltasDead;
    final LongStat nBINDeltasMigrated;
    final LongStat nLNsObsolete;
    final LongStat nLNsExpired;
    final LongStat nLNsCleaned;
    final LongStat nLNsDead;
    final LongStat nLNsLocked;
    final LongStat nLNsMigrated;
    final LongStat nLNsMarked;
    final LongStat nLNQueueHits;
    private final LongStat nPendingLNsProcessed;
    private final LongStat nMarkedLNsProcessed;
    private final LongStat nToBeCleanedLNsProcessed;
    private final LongStat nClusterLNsProcessed;
    private final LongStat nPendingLNsLocked;
    final LongStat nEntriesRead;
    final LongStat nDiskReads;
    final LongStat nRepeatIteratorReads;
    /*
     * Log size stats. These are CUMMULATIVE and the stat objects are created
     * by loadStats. They are accessed as a group while synchronized on
     * statGroup to ensure the set of values is consistent/coherent.
     */
    private FileProtector.LogSizeStats logSizeStats;
    private long availableLogSize;
    private long totalLogSize;

    /*
     * Unlike availableLogSize, maxDiskOverage and freeDiskShortage are
     * calculated based on actual disk usage, without subtracting the size of
     * the reserved files. So these values may be GT zero even if
     * availableLogSize is GTE zero. If maxDiskOverage or freeDiskShortage
     * is GT zero, then manageDiskUsage will try to delete log files to
     * avoid a violation.
     */
    private long maxDiskOverage;
    private long freeDiskShortage;

    /* Message summarizing current log size stats, with limit violations. */
    private String diskUsageMessage;

    /*
     * If a disk usage limit is violated, this is diskUsageMessage; otherwise
     * it is null. It is volatile so it can be checked cheaply during CRUD ops.
     */
    private volatile String diskUsageViolationMessage;

    /*
     * Used to prevent repeated logging about a disk limit violation.
     * Protected by manageDiskUsageLock.
     */
    private boolean loggedDiskLimitViolation;

    /*
     * Configuration parameters.
     */
    long lockTimeout;
    int readBufferSize;
    int lookAheadCacheSize;
    long nDeadlockRetries;
    boolean expunge;
    private boolean useDeletedDir;
    int twoPassGap;
    int twoPassThreshold;
    boolean gradualExpiration;
    long cleanerBytesInterval;
    boolean trackDetail;
    private boolean fetchObsoleteSize;
    int dbCacheClearCount;
    private final boolean rmwFixEnabled;
    int minUtilization;
    int minFileUtilization;
    int minAge;
    private long maxDiskLimit;
    private long freeDiskLimit;
    private long adjustedMaxDiskLimit;

    private final String name;
    private final EnvironmentImpl env;
    private final FileStoreInfo fileStoreInfo;
    private final FileProtector fileProtector;
    private final UtilizationProfile profile;
    private final UtilizationTracker tracker;
    private final ExpirationProfile expirationProfile;
    private final UtilizationCalculator calculator;
    private final FileSelector fileSelector;
    private FileProcessor[] threads;

    private final Logger logger;
    final AtomicLong totalRuns;
    TestHook fileChosenHook;

    /** @see #processPending */
    private final AtomicBoolean processPendingReentrancyGuard =
        new AtomicBoolean(false);

    /** @see #wakeupAfterWrite */
    private final AtomicLong bytesWrittenSinceActivation = new AtomicLong(0);

    public Cleaner(EnvironmentImpl env, String name) {
        this.env = env;
        this.name = name;

        /* Initialize the non-CUMULATIVE stats definitions. */
        statGroup = new StatGroup(GROUP_NAME, GROUP_DESC);
        nCleanerRuns = new LongStat(statGroup, CLEANER_RUNS);
        nTwoPassRuns = new LongStat(statGroup, CLEANER_TWO_PASS_RUNS);
        nRevisalRuns = new LongStat(statGroup, CLEANER_REVISAL_RUNS);
        nCleanerDeletions = new LongStat(statGroup, CLEANER_DELETIONS);
        nINsObsolete = new LongStat(statGroup, CLEANER_INS_OBSOLETE);
        nINsCleaned = new LongStat(statGroup, CLEANER_INS_CLEANED);
        nINsDead = new LongStat(statGroup, CLEANER_INS_DEAD);
        nINsMigrated = new LongStat(statGroup, CLEANER_INS_MIGRATED);
        nBINDeltasObsolete = new LongStat(statGroup, CLEANER_BIN_DELTAS_OBSOLETE);
        nBINDeltasCleaned = new LongStat(statGroup, CLEANER_BIN_DELTAS_CLEANED);
        nBINDeltasDead = new LongStat(statGroup, CLEANER_BIN_DELTAS_DEAD);
        nBINDeltasMigrated = new LongStat(statGroup, CLEANER_BIN_DELTAS_MIGRATED);
        nLNsObsolete = new LongStat(statGroup, CLEANER_LNS_OBSOLETE);
        nLNsExpired = new LongStat(statGroup, CLEANER_LNS_EXPIRED);
        nLNsCleaned = new LongStat(statGroup, CLEANER_LNS_CLEANED);
        nLNsDead = new LongStat(statGroup, CLEANER_LNS_DEAD);
        nLNsLocked = new LongStat(statGroup, CLEANER_LNS_LOCKED);
        nLNsMigrated = new LongStat(statGroup, CLEANER_LNS_MIGRATED);
        nLNsMarked = new LongStat(statGroup, CLEANER_LNS_MARKED);
        nLNQueueHits = new LongStat(statGroup, CLEANER_LNQUEUE_HITS);
        nPendingLNsProcessed =
            new LongStat(statGroup, CLEANER_PENDING_LNS_PROCESSED);
        nMarkedLNsProcessed =
            new LongStat(statGroup, CLEANER_MARKED_LNS_PROCESSED);
        nToBeCleanedLNsProcessed =
            new LongStat(statGroup, CLEANER_TO_BE_CLEANED_LNS_PROCESSED);
        nClusterLNsProcessed =
            new LongStat(statGroup, CLEANER_CLUSTER_LNS_PROCESSED);
        nPendingLNsLocked = new LongStat(statGroup, CLEANER_PENDING_LNS_LOCKED);
        nEntriesRead = new LongStat(statGroup, CLEANER_ENTRIES_READ);
        nDiskReads = new LongStat(statGroup, CLEANER_DISK_READS);
        nRepeatIteratorReads =
            new LongStat(statGroup, CLEANER_REPEAT_ITERATOR_READS);

        logSizeStats =
            new FileProtector.LogSizeStats(0, 0, 0, new HashMap<>());

        if (env.isMemOnly()) {
            fileStoreInfo = null;
        } else {
            try {
                fileStoreInfo = FileStoreInfo.getInfo(
                    env.getEnvironmentHome().getAbsolutePath());
            } catch (IOException e) {
                throw EnvironmentFailureException.unexpectedException(env, e);
            }
        }
        fileProtector = new FileProtector(env);
        tracker = new UtilizationTracker(env, this);
        profile = new UtilizationProfile(env, tracker);
        expirationProfile = new ExpirationProfile(env);
        calculator = new UtilizationCalculator(env, this);
        fileSelector = new FileSelector();
        threads = new FileProcessor[0];
        logger = LoggerUtils.getLogger(getClass());
        totalRuns = new AtomicLong(0);

        /*
         * The trackDetail property is immutable because of the complexity (if
         * it were mutable) in determining whether to update the memory budget
         * and perform eviction.
         */
        trackDetail = env.getConfigManager().getBoolean
            (EnvironmentParams.CLEANER_TRACK_DETAIL);

        rmwFixEnabled = env.getConfigManager().getBoolean
            (EnvironmentParams.CLEANER_RMW_FIX);

        /* Initialize mutable properties and register for notifications. */
        setMutableProperties(env.getConfigManager());
        env.addConfigObserver(this);
    }

    /**
     * Process notifications of mutable property changes.
     *
     * @throws IllegalArgumentException via Environment ctor and
     * setMutableConfig.
     */
    public void envConfigUpdate(DbConfigManager cm,
                                EnvironmentMutableConfig ignore) {

        setMutableProperties(cm);

        /* A parameter that impacts cleaning may have changed. */
        wakeupActivate();
    }

    private void setMutableProperties(final DbConfigManager cm) {

        lockTimeout = cm.getDuration(EnvironmentParams.CLEANER_LOCK_TIMEOUT);

        readBufferSize = cm.getInt(EnvironmentParams.CLEANER_READ_SIZE);
        if (readBufferSize <= 0) {
            readBufferSize =
                cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        }

        lookAheadCacheSize =
            cm.getInt(EnvironmentParams.CLEANER_LOOK_AHEAD_CACHE_SIZE);

        nDeadlockRetries = cm.getInt(EnvironmentParams.CLEANER_DEADLOCK_RETRY);

        expunge = cm.getBoolean(EnvironmentParams.CLEANER_REMOVE);

        useDeletedDir =
            cm.getBoolean(EnvironmentParams.CLEANER_USE_DELETED_DIR);

        twoPassGap =
            cm.getInt(EnvironmentParams.CLEANER_TWO_PASS_GAP);

        twoPassThreshold =
            cm.getInt(EnvironmentParams.CLEANER_TWO_PASS_THRESHOLD);

        if (twoPassThreshold == 0) {
            twoPassThreshold =
                cm.getInt(EnvironmentParams.CLEANER_MIN_UTILIZATION) - 5;
        }

        gradualExpiration =
            cm.getBoolean(EnvironmentParams.CLEANER_GRADUAL_EXPIRATION);

        dbCacheClearCount =
            cm.getInt(EnvironmentParams.ENV_DB_CACHE_CLEAR_COUNT);

        int nThreads = cm.getInt(EnvironmentParams.CLEANER_THREADS);
        assert nThreads > 0;

        if (nThreads != threads.length) {

            /* Shutdown threads when reducing their number. */
            for (int i = nThreads; i < threads.length; i += 1) {
                if (threads[i] == null) {
                    continue;
                }
                threads[i].shutdown();
                threads[i] = null;
            }

            /* Copy existing threads that are still used. */
            FileProcessor[] newThreads = new FileProcessor[nThreads];
            for (int i = 0; i < nThreads && i < threads.length; i += 1) {
                newThreads[i] = threads[i];
            }

            /* Don't lose track of new threads if an exception occurs. */
            threads = newThreads;

            /* Start new threads when increasing their number. */
            for (int i = 0; i < nThreads; i += 1) {
                if (threads[i] != null) {
                    continue;
                }
                threads[i] = new FileProcessor(
                    name + '-' + (i + 1),
                    i == 0 /*firstThread*/,
                    env, this, profile, calculator, fileSelector);
            }
        }

        cleanerBytesInterval = cm.getLong(
            EnvironmentParams.CLEANER_BYTES_INTERVAL);

        if (cleanerBytesInterval == 0) {
            cleanerBytesInterval =
                cm.getLong(EnvironmentParams.LOG_FILE_MAX) / 4;

            cleanerBytesInterval = Math.min(
                cleanerBytesInterval, MAX_CLEANER_BYTES_INTERVAL);
        }

        final int wakeupInterval =
            cm.getDuration(EnvironmentParams.CLEANER_WAKEUP_INTERVAL);

        for (FileProcessor thread : threads) {
            if (thread == null) {
                continue;
            }
            thread.setWaitTime(wakeupInterval);
        }

        fetchObsoleteSize =
            cm.getBoolean(EnvironmentParams.CLEANER_FETCH_OBSOLETE_SIZE);

        minAge = cm.getInt(EnvironmentParams.CLEANER_MIN_AGE);
        minUtilization = cm.getInt(EnvironmentParams.CLEANER_MIN_UTILIZATION);
        minFileUtilization =
            cm.getInt(EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION);

        maxDiskLimit = cm.getLong(EnvironmentParams.MAX_DISK);
        adjustedMaxDiskLimit = maxDiskLimit;

        if (env.isMemOnly()) {
            /* Env home dir may not exist, can't query file system info. */
            freeDiskLimit = 0;
        } else {
            final int replayFreeDiskPct = env.getReplayFreeDiskPercent();
            if (replayFreeDiskPct == 0) {
                /* No backward compatibility is needed. */
                freeDiskLimit = cm.getLong(EnvironmentParams.FREE_DISK);
            } else {
                /* Use replayFreeDiskPercent for backward compatibility. */
                if (cm.isSpecified(EnvironmentParams.FREE_DISK)) {
                    throw new IllegalArgumentException(
                        "Cannot specify both " + EnvironmentConfig.FREE_DISK +
                            " and je.rep.replayFreeDiskPercent.");
                }
                freeDiskLimit =
                    (getDiskTotalSpace() * replayFreeDiskPct) / 100;
            }

            if (maxDiskLimit > MAX_DISK_ADJUSTMENT_THRESHOLD ||
                cm.isSpecified(EnvironmentParams.FREE_DISK) ||
                replayFreeDiskPct != 0) {
                adjustedMaxDiskLimit -= freeDiskLimit;
            }
        }
    }

    public FileProtector getFileProtector() {
        return fileProtector;
    }

    public UtilizationTracker getUtilizationTracker() {
        return tracker;
    }

    public UtilizationProfile getUtilizationProfile() {
        return profile;
    }

    UtilizationCalculator getUtilizationCalculator() {
        return calculator;
    }

    public ExpirationProfile getExpirationProfile() {
        return expirationProfile;
    }

    public FileSelector getFileSelector() {
        return fileSelector;
    }

    public boolean getFetchObsoleteSize(DatabaseImpl db) {
        return fetchObsoleteSize && !db.isLNImmediatelyObsolete();
    }

    /**
     * @see EnvironmentParams#CLEANER_RMW_FIX
     * @see FileSummaryLN#postFetchInit
     */
    public boolean isRMWFixEnabled() {
        return rmwFixEnabled;
    }

    /* For unit testing only. */
    void setFileChosenHook(TestHook hook) {
        fileChosenHook = hook;
    }

    /*
     * Delegate the run/pause/wakeup/shutdown DaemonRunner operations.  We
     * always check for null to account for the possibility of exceptions
     * during thread creation.  Cleaner daemon can't ever be run if No Locking
     * mode is enabled.
     */
    public void runOrPause(boolean run) {

        if (env.isNoLocking()) {
            return;
        }

        for (FileProcessor processor : threads) {
            if (processor == null) {
                continue;
            }
            if (run) {
                processor.activateOnWakeup();
            }
            processor.runOrPause(run);
        }
    }

    /**
     * If the number of bytes written since the last activation exceeds the
     * cleaner's byte interval, wakeup the file processor threads in activate
     * mode.
     */
    public void wakeupAfterWrite(int writeSize) {

        if (bytesWrittenSinceActivation.addAndGet(writeSize) >
            cleanerBytesInterval) {

            bytesWrittenSinceActivation.set(0);
            wakeupActivate();
        }
    }

    /**
     * Wakeup the file processor threads in activate mode, meaning that
     * FileProcessor.doClean will be called.
     *
     * @see FileProcessor#onWakeup()
     */
    public void wakeupActivate() {

        for (FileProcessor thread : threads) {
            if (thread == null) {
                continue;
            }
            thread.activateOnWakeup();
            thread.wakeup();
        }
    }

    public void requestShutdown() {
        for (FileProcessor thread : threads) {
            if (thread == null) {
                continue;
            }
            thread.requestShutdown();
        }
    }

    public void shutdown() {
        for (int i = 0; i < threads.length; i += 1) {
            if (threads[i] == null) {
                continue;
            }
            threads[i].shutdown();
            threads[i] = null;
        }
    }

    public int getNWakeupRequests() {
        int count = 0;
        for (FileProcessor thread : threads) {
            if (thread == null) {
                continue;
            }
            count += thread.getNWakeupRequests();
        }
        return count;
    }

    /**
     * Cleans selected files and returns the number of files cleaned.  This
     * method is not invoked by a deamon thread, it is programatically.
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
    public int doClean(boolean cleanMultipleFiles, boolean forceCleaning) {

        FileProcessor processor = createProcessor();

        return processor.doClean
            (false /*invokedFromDaemon*/, cleanMultipleFiles, forceCleaning);
    }

    public FileProcessor createProcessor() {
        return new FileProcessor(
            "", false, env, this, profile, calculator, fileSelector);
    }

    /**
     * Load stats.
     */
    public StatGroup loadStats(StatsConfig config) {

        final StatGroup stats = statGroup.cloneGroup(config.getClear());

        /* Add all CUMULATIVE stats explicitly. */
        new IntStat(
            stats, CLEANER_MIN_UTILIZATION,
            calculator.getCurrentMinUtilization());
        new IntStat(
            stats, CLEANER_MAX_UTILIZATION,
            calculator.getCurrentMaxUtilization());
        new IntStat(
            stats, CLEANER_PENDING_LN_QUEUE_SIZE,
            fileSelector.getPendingLNQueueSize());

        /*
         * Synchronize on statGroup while adding log size stats, to return a
         * consistent set of values.
         */
        synchronized (statGroup) {
            new LongStat(
                stats, CLEANER_ACTIVE_LOG_SIZE,
                logSizeStats.activeSize);
            new LongStat(
                stats, CLEANER_RESERVED_LOG_SIZE,
                logSizeStats.reservedSize);
            new LongStat(
                stats, CLEANER_PROTECTED_LOG_SIZE,
                logSizeStats.protectedSize);
            new LongStat(
                stats, CLEANER_AVAILABLE_LOG_SIZE,
                availableLogSize);
            new LongStat(
                stats, CLEANER_TOTAL_LOG_SIZE,
                totalLogSize);

            final AtomicLongMapStat protectedSizeMap =
                new AtomicLongMapStat(stats, CLEANER_PROTECTED_LOG_SIZE_MAP);

            for (final Map.Entry<String, Long> entry :
                logSizeStats.protectedSizeMap.entrySet()) {

                protectedSizeMap.
                    createStat(entry.getKey()).
                    set(entry.getValue());
            }
        }

        return stats;
    }

    /**
     * Enables or disabling processing of safe-to-delete files, including
     * the truncation of the VLSN index. Disabling this is needed for tests,
     * when VLSNIndex changes should be prevented.
     */
    public synchronized void enableFileDeletion(boolean enable) {
        fileDeletionEnabled = enable;
    }

    /**
     * Updates log size stats and deletes unprotected reserved files in order
     * to stay within disk limits.
     *
     * This method must be called frequently enough to maintain disk usage
     * safely below the limits. For an HA env this is particularly important
     * since we retain all reserved files until we approach the disk limits.
     * For this, calling this method at least every CLEANER_BYTES_INTERVAL
     * should suffice.
     *
     * It is also important to call this method based on a time interval
     * when writing stops, to retry deletions when files are protected or the
     * env is locked by read-only processes. For this, calling this method at
     * least every CLEANER_WAKEUP_INTERVAL should suffice.
     */
    public void manageDiskUsage() {

        /* Fail loudly if the environment is invalid. */
        env.checkIfInvalid();

        if (env.isMemOnly() || env.mayNotWrite() || !fileDeletionEnabled) {
            return;
        }

        /*
         * Only one thread at a time can truncate the VLSNIndex head, request
         * the environment lock, and update stats. This is a periodic action,
         * so probe the lock to avoid blocking other cleaner threads.
         */
        if (!manageDiskUsageLock.tryLock()) {
            return;
        }

        try {
            /* Periodically update the stats. */
            freshenLogSizeStats();

            if (fileProtector.getNReservedFiles() > 0) {

                boolean freshenStats = false;

                if (env.isReplicated()) {
                    /*
                     * Reserved files are retained until we approach a disk
                     * limit. Determine how many bytes we need to reclaim by
                     * deleting reserved files. Add a reasonable value to stay
                     * safely below the limits between cleaner wakeups. Note
                     * that max(overage,shortage) may be negative, since
                     * overage and shortage may be negative.
                     */
                    final long origBytesNeeded =
                        ((maxDiskLimit > 0) ?
                            Math.max(maxDiskOverage, freeDiskShortage) :
                            freeDiskShortage) +
                        Math.max(
                            1L << 20,
                            3 * cleanerBytesInterval);

                    /*
                     * First try deleting files without truncating the
                     * VLSNIndex head.
                     */
                    long bytesNeeded = origBytesNeeded;
                    if (bytesNeeded > 0) {
                        bytesNeeded = deleteUnprotectedFiles(bytesNeeded);
                    }

                    /*
                     * If we still need space, try truncating the VLSNIndex
                     * and then deleting files. See FileProtector for details.
                     */
                    if (bytesNeeded > 0 &&
                        env.tryVlsnHeadTruncate(bytesNeeded)) {

                        bytesNeeded = deleteUnprotectedFiles(bytesNeeded);
                        freshenStats = true;
                    }

                    if (bytesNeeded < origBytesNeeded) {
                        freshenStats = true;
                    }
                } else {
                    /*
                     * For a non-HA env, simply try to delete all the reserved
                     * files.
                     */
                    final long bytesNeeded =
                        deleteUnprotectedFiles(Long.MAX_VALUE);

                    if (bytesNeeded < Long.MAX_VALUE) {
                        freshenStats = true;
                    }
                }

                /*
                 * Freshen the stats if any files are deleted, so write
                 * operations can occur ASAP if they were previously
                 * prohibited. Also freshen the stats if we truncated the
                 * VLSNIndex, so that the stats reflect the current factors
                 * gating file deletion.
                 */
                if (freshenStats) {
                    freshenLogSizeStats();
                }
            }

            /*
             * If there is still a violation, and we have not logged it since
             * the violation status changed, then log it now. We do not expect
             * the violation status to change frequently.
             */
            final String violation = diskUsageViolationMessage;
            if (violation != null) {
                if (!loggedDiskLimitViolation) {
                    LoggerUtils.logMsg(logger, env, Level.SEVERE, violation);
                    loggedDiskLimitViolation = true;
                }
            } else {
                loggedDiskLimitViolation = false;
            }

        } catch (EnvLockedException e) {

            LoggerUtils.logMsg(
                logger, env, Level.SEVERE,
                "Could not delete files due to read-only processes. " +
                    diskUsageMessage);

        } finally {
            manageDiskUsageLock.unlock();
        }
    }

    /** @see #deleteUnprotectedFiles */
    private static class EnvLockedException extends Exception {}

    /**
     * Deletes unprotected reserved files in an attempt to free bytesNeeded.
     * In a non-HA env, attempts to delete all reserved files, irrespective of
     * bytesNeeded.
     *
     * <p>An exclusive environment is held while deleting the files to lock
     * out read-only processes. The lock is held while deleting the reserved
     * file records as well, but this is inexpensive and should not cause long
     * delays for read-only processes.</p>
     *
     * @param bytesNeeded the amount of space we need to reclaim to stay within
     * disk limits.
     *
     * @return number of bytes we could not reclaim due to protected files, or
     * zero if we deleted files totaling bytesNeeded or more.
     *
     * @throws EnvLockedException if we can't get an exclusive environment
     * lock because the env is locked by read-only processes, and therefore no
     * files can be deleted.
     */
    private long deleteUnprotectedFiles(long bytesNeeded)
        throws EnvLockedException {

        final FileManager fileManager = env.getFileManager();
        final SortedSet<Long> deletedFiles = new TreeSet<>();

        if (!fileManager.lockEnvironment(false, true)) {
            throw new EnvLockedException();
        }
        try {
            long file = -1;

            while (bytesNeeded > 0 || !env.isReplicated()) {

                final Pair<Long, Long> pair =
                    fileProtector.takeCondemnedFile(file + 1);

                if (pair == null) {
                    break;
                }

                file = pair.first();
                final long size = pair.second();

                if (!deleteFile(file)) {
                    /* Sometimes files cannot be deleted on Windows. */
                    fileProtector.putBackCondemnedFile(file, size);
                    continue;
                }

                bytesNeeded = Math.max(0, bytesNeeded - size);
                profile.deleteReservedFileRecord(file);
                nCleanerDeletions.increment();
                deletedFiles.add(file);
            }

        } finally {
            fileManager.releaseExclusiveLock();

            if (!deletedFiles.isEmpty()) {

                final StringBuilder sb = new StringBuilder(
                    "Cleaner deleted files:");

                for (final Long file : deletedFiles) {
                    sb.append(" 0x");
                    sb.append(Long.toHexString(file));
                }

                LoggerUtils.traceAndLog(
                    logger, env, Level.INFO, sb.toString());
            }
        }

        return bytesNeeded;
    }

    /**
     * Attempts to delete the file and returns whether it has been deleted.
     */
    private boolean deleteFile(final Long file) {
        final FileManager fileManager = env.getFileManager();

        final String expungeLabel = expunge ? "delete" : "rename";
        final String expungedLabel = expungeLabel + "d";

        try {
            if (expunge) {
                if (fileManager.deleteFile(file)) {
                    return true;
                }
            } else {
                /* See EnvironmentConfig.CLEANER_EXPUNGE. */

                final File newFile = fileManager.renameFile(
                    file, FileManager.DEL_SUFFIX,
                    useDeletedDir ? DELETED_SUBDIR : null);

                if (newFile != null) {
                    newFile.setLastModified(System.currentTimeMillis());
                    return true;
                }
            }
        } catch (IOException e) {
            throw new EnvironmentFailureException(
                env, EnvironmentFailureReason.LOG_WRITE,
                "Unable to " + expungeLabel + " " + file, e);
        }

        /*
         * If the file is not valid (missing) then the file was previously
         * deleted. This can occur on Windows when we retry deletion (see
         * below).
         */
        if (!fileManager.isFileValid(file)) {
            return true;
        }

        /*
         * Log a message and return false to retry the deletion later. The
         * deletion is known to fail on Windows, and probably this occurs whe
         * the file was recently closed.
         */
        LoggerUtils.traceAndLog(
            logger, env, Level.WARNING,
            "Log file 0x" + Long.toHexString(file) + " could not be " +
                expungedLabel + ". The deletion will be retried later.");

        return false;
    }

    /**
     * Updates the cached set of log size stats, including maxDiskOverage,
     * freeDiskShortage, diskUsageMessage and diskUsageViolationMessage.
     *
     * Normally this should only be called by manageDiskUsage while holding
     * the manageDiskUsageLock. However, it may be called directly during
     * (single threaded) recovery.
     */
    public void freshenLogSizeStats() {

        recalcLogSizeStats(
            fileProtector.getLogSizeStats(), getDiskFreeSpace());
    }

    /**
     * Implementation of freshenLogSizeStats. Exposed for testing.
     */
    void recalcLogSizeStats(final FileProtector.LogSizeStats stats,
                            final long diskFreeSpace) {

        /*
         * Use locals for limits, since they may be changed by other threads.
         */
        final long maxLimit = maxDiskLimit;
        final long adjustedMax = adjustedMaxDiskLimit;
        final long freeLimit = freeDiskLimit;

        /*
         * Calculate overage/shortage and available size. Below are examples of
         * availableLogBytes values where:
         *
         *  totalLS=75 activeLS=50 reservedLS=25 protectedLS=5
         *
         *    freeDL maxDL diskFS freeB1 freeB2 availableLS
         *      5      -     20     15      15      35
         *     25      -      5    -20     -20       0
         *     30      -      5    -25     -25      -5
         *      5    100     20     15      15      35
         *     25    100     20     -5      -5      15
         *      5     80     20     15       0      20
         *     25     80     20     -5     -20       0
         *     25    200      5    -20     -20       0
         *     25     75     20     -5     -25      -5
         *     50     80     90     40     -45     -25
         */
        final long freeBytes1 = diskFreeSpace - freeLimit;
        final long freeShortage = 0 - freeBytes1;
        final long totalSize = stats.activeSize + stats.reservedSize;
        final long maxOverage;
        final long freeBytes2;

        if (adjustedMax > 0) {
            maxOverage = totalSize - adjustedMax;
            freeBytes2 = Math.min(freeBytes1, adjustedMax - totalSize);
        } else {
            maxOverage = 0;
            freeBytes2 = freeBytes1;
        }

        final long availBytes =
            freeBytes2 + stats.reservedSize - stats.protectedSize;

        final StringBuilder sb = new StringBuilder();

        if (availBytes <= 0) {
            sb.append("Disk usage is not within je.maxDisk or je.freeDisk ");
            sb.append("limits and write operations are prohibited:");
        } else {
            sb.append("Disk usage is currently within je.maxDisk and ");
            sb.append("je.freeDisk limits:");
        }

        sb.append(" maxDiskLimit=");
        sb.append(INT_FORMAT.format(maxLimit));
        sb.append(" freeDiskLimit=");
        sb.append(INT_FORMAT.format(freeLimit));
        sb.append(" adjustedMaxDiskLimit=");
        sb.append(INT_FORMAT.format(adjustedMax));
        sb.append(" maxDiskOverage=");
        sb.append(INT_FORMAT.format(maxOverage));
        sb.append(" freeDiskShortage=");
        sb.append(INT_FORMAT.format(freeShortage));
        sb.append(" diskFreeSpace=");
        sb.append(INT_FORMAT.format(diskFreeSpace));
        sb.append(" availableLogSize=");
        sb.append(INT_FORMAT.format(availBytes));
        sb.append(" totalLogSize=");
        sb.append(INT_FORMAT.format(totalSize));
        sb.append(" activeLogSize=");
        sb.append(INT_FORMAT.format(stats.activeSize));
        sb.append(" reservedLogSize=");
        sb.append(INT_FORMAT.format(stats.reservedSize));
        sb.append(" protectedLogSize=");
        sb.append(INT_FORMAT.format(stats.protectedSize));
        sb.append(" protectedLogSizeMap={");

        for (final Map.Entry<String, Long> entry :
                stats.protectedSizeMap.entrySet()) {

            sb.append(entry.getKey()).append(":");
            sb.append(INT_FORMAT.format(entry.getValue()));
        }

        sb.append("}");

        final String msg = sb.toString();

        /* Synchronize on statGroup to maintain consistent set of stats. */
        synchronized (statGroup) {
            maxDiskOverage = maxOverage;
            freeDiskShortage = freeShortage;
            diskUsageMessage = msg;
            diskUsageViolationMessage = (availBytes <= 0) ? msg : null;
            availableLogSize = availBytes;
            totalLogSize = totalSize;
            logSizeStats = stats;
        }
    }

    private long getDiskFreeSpace() {
        try {
            return fileStoreInfo.getUsableSpace();
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(env, e);
        }
    }

    private long getDiskTotalSpace() {
        try {
            return fileStoreInfo.getTotalSpace();
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(env, e);
        }
    }

    /**
     * Returns a message describing disk space limits and usage, regardless of
     * whether the limit is violated or not. If there is a limit violation,
     * returns the same value as {@link #getDiskLimitViolation()}. Does not
     * return null.
     */
    public String getDiskLimitMessage() {
        return diskUsageMessage;
    }

    /**
     * Uses cached disk usage info to determine whether disk space limits are
     * currently violated. This method simply returns a volatile field. The
     * cached information is updated frequently enough to prevent violating the
     * limits by a large amount.
     *
     * @return a non-null message (appropriate for an exception) if a disk
     * limit is currently violated, else null.
     */
    public String getDiskLimitViolation() {
        return diskUsageViolationMessage;
    }

    long getMaxDiskOverage() {
        return maxDiskOverage;
    }

    long getFreeDiskShortage() {
        return freeDiskShortage;
    }

    /**
     * Returns a copy of the cleaned and processed files at the time a
     * checkpoint starts.
     *
     * <p>If non-null is returned, the checkpoint should flush an extra level,
     * and addCheckpointedFiles() should be called when the checkpoint is
     * complete.</p>
     */
    public CheckpointStartCleanerState getFilesAtCheckpointStart() {

        /* Pending LNs can prevent file deletion. */
        processPending();

        return fileSelector.getFilesAtCheckpointStart();
    }

    /**
     * When a checkpoint is complete, update the files that were returned at
     * the beginning of the checkpoint.
     */
    public void updateFilesAtCheckpointEnd(CheckpointStartCleanerState info) {

        /* Update cleaned file status and get newly reserved files. */
        final Map<Long, FileSelector.FileInfo> reservedFiles =
            fileSelector.updateFilesAtCheckpointEnd(env, info);

        /*
         * Insert reserved file db record and delete other (unnecessary)
         * metadata for reserved files.
         */
        profile.reserveFiles(reservedFiles);

        /* Try deleting files since file status may have changed. */
        manageDiskUsage();

        /*
         * Periodically process completed expiration trackers. This is done
         * here in case cleaner threads are disabled.
         */
        expirationProfile.processCompletedTrackers();
    }

    /**
     * If any LNs or databases are pending, process them.  This method should
     * be called often enough to prevent the pending LN set from growing too
     * large.
     */
    void processPending() {

        /*
         * This method is not synchronized because that would block cleaner
         * and checkpointer threads unnecessarily.  However, we do prevent
         * reentrancy, for two reasons:
         * 1. It is wasteful for two threads to process the same pending
         *    entries.
         * 2. Many threads calling getDb may increase the liklihood of
         *    livelock. [#20816]
         */
        if (!processPendingReentrancyGuard.compareAndSet(false, true)) {
            return;
        }

        try {
            final DbTree dbMapTree = env.getDbTree();

            final LockManager lockManager =
                env.getTxnManager().getLockManager();

            final Map<Long, LNInfo> pendingLNs = fileSelector.getPendingLNs();

            if (pendingLNs != null) {
                final TreeLocation location = new TreeLocation();

                for (final Map.Entry<Long, LNInfo> entry :
                     pendingLNs.entrySet()) {

                    if (!env.isValid()) {
                        return;
                    }

                    if (diskUsageViolationMessage != null) {
                        break; /* We can't write. */
                    }

                    final long logLsn = entry.getKey();
                    final LNInfo info = entry.getValue();

                    if (env.expiresWithin(
                        info.getExpirationTime(),
                        0 - env.getTtlLnPurgeDelay())) {

                        if (lockManager.isLockUncontended(logLsn)) {
                            fileSelector.removePendingLN(logLsn);
                            nLNsExpired.increment();
                            nLNsObsolete.increment();
                        } else {
                            nPendingLNsLocked.increment();
                        }
                        continue;
                    }

                    final DatabaseId dbId = info.getDbId();
                    final DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);

                    try {
                        final byte[] key = info.getKey();

                        /* Evict before processing each entry. */
                        if (DO_CRITICAL_EVICTION) {
                            env.daemonEviction(true /*backgroundIO*/);
                        }

                        processPendingLN(logLsn, db, key, location);

                    } finally {
                        dbMapTree.releaseDb(db);
                    }
                }
            }

            final DatabaseId[] pendingDBs = fileSelector.getPendingDBs();
            if (pendingDBs != null) {
                for (final DatabaseId dbId : pendingDBs) {
                    if (!env.isValid()) {
                        return;
                    }
                    final DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);
                    try {
                        if (db == null || db.isDeleteFinished()) {
                            fileSelector.removePendingDB(dbId);
                        }
                    } finally {
                        dbMapTree.releaseDb(db);
                    }
                }
            }
        } finally {
            processPendingReentrancyGuard.set(false);
        }
    }

    /**
     * Processes a pending LN, getting the lock first to ensure that the
     * overhead of retries is minimal.
     */
    private void processPendingLN(
        final long logLsn,
        final DatabaseImpl db,
        final byte[] keyFromLog,
        final TreeLocation location) {

        boolean parentFound;          // We found the parent BIN.
        boolean processedHere = true; // The LN was cleaned here.
        boolean lockDenied = false;   // The LN lock was denied.
        boolean obsolete = false;     // The LN is no longer in use.
        boolean completed = false;    // This method completed.

        BasicLocker locker = null;
        BIN bin = null;

        try {
            nPendingLNsProcessed.increment();

            /*
             * If the DB is gone, this LN is obsolete.  If delete cleanup is in
             * progress, put the DB into the DB pending set; this LN will be
             * declared deleted after the delete cleanup is finished.
             */
            if (db == null || db.isDeleted()) {
                addPendingDB(db);
                nLNsDead.increment();
                obsolete = true;
                completed = true;
                return;
            }

            final Tree tree = db.getTree();
            assert tree != null;

            /*
             * Get a non-blocking read lock on the original log LSN.  If this
             * fails, then the original LSN is still write-locked.  We may have
             * to lock again, if the LSN has changed in the BIN, but this
             * initial check prevents a Btree lookup in some cases.
             */
            locker = BasicLocker.createBasicLocker(env, false /*noWait*/);

            /* Don't allow this short-lived lock to be preempted/stolen. */
            locker.setPreemptable(false);

            final LockResult lockRet = locker.nonBlockingLock(
                logLsn, LockType.READ, false /*jumpAheadOfWaiters*/, db);

            if (lockRet.getLockGrant() == LockGrantType.DENIED) {
                /* Try again later. */
                nPendingLNsLocked.increment();
                lockDenied = true;
                completed = true;
                return;
            }

            /*
             * Search down to the bottom most level for the parent of this LN.
             */
            parentFound = tree.getParentBINForChildLN(
                location, keyFromLog, false /*splitsAllowed*/,
                false /*blindDeltaOps*/, UPDATE_GENERATION);

            bin = location.bin;
            final int index = location.index;

            if (!parentFound) {
                nLNsDead.increment();
                obsolete = true;
                completed = true;
                return;
            }

            /* Migrate an LN. */
            processedHere = false;

            lockDenied =
                migratePendingLN(db, logLsn, bin.getLsn(index), bin, index);

            completed = true;

        } catch (RuntimeException e) {
            e.printStackTrace();
            LoggerUtils.traceAndLogException(
                env, "com.sleepycat.je.cleaner.Cleaner",
                "processLN", "Exception thrown: ", e);
            throw e;
        } finally {
            if (bin != null) {
                bin.releaseLatch();
            }

            if (locker != null) {
                locker.operationEnd();
            }

            /* BIN must not be latched when synchronizing on FileSelector. */
            if (completed && !lockDenied) {
                fileSelector.removePendingLN(logLsn);
            }

            /*
             * If migratePendingLN was not called above, perform tracing in
             * this method.
             */
            if (processedHere) {
                logFine(CLEAN_PENDING_LN, null /*node*/, DbLsn.NULL_LSN,
                        completed, obsolete, false /*migrated*/);
            }
        }
    }

    /**
     * Migrate a pending LN in the given BIN entry, if it is not obsolete.  The
     * BIN must be latched on entry and is left latched by this method.
     *
     * @return whether migration could not be completed because the LN lock was
     * denied.
     */
    private boolean migratePendingLN(
        final DatabaseImpl db,
        final long logLsn,
        final long treeLsn,
        final BIN bin,
        final int index) {

        /* Status variables are used to generate debug tracing info. */
        boolean obsolete = false;    // The LN is no longer in use.
        boolean migrated = false;    // The LN was in use and is migrated.
        boolean completed = false;   // This method completed.
        boolean clearTarget = false; // Node was non-resident when called.

        /*
         * If wasCleaned is false we don't count statistics unless we migrate
         * the LN.  This avoids double counting.
         */
        BasicLocker locker = null;
        LN ln = null;

        try {
            if (treeLsn == DbLsn.NULL_LSN) {
                /* This node was never written, no need to migrate. */
                completed = true;
                return false;
            }

            /* If the record has been deleted, the logrec is obsolete */
            if (bin.isEntryKnownDeleted(index)) {
                nLNsDead.increment();
                obsolete = true;
                completed = true;
                return false;
            }

            /*
             * Get a non-blocking read lock on the LN.  A pending node is
             * already locked, but the original pending LSN may have changed.
             * We must lock the current LSN to guard against aborts.
             */
            if (logLsn != treeLsn) {

                locker = BasicLocker.createBasicLocker(env, false /*noWait*/);
                /* Don't allow this short-lived lock to be preempted/stolen. */
                locker.setPreemptable(false);

                final LockResult lockRet = locker.nonBlockingLock(
                    treeLsn, LockType.READ, false /*jumpAheadOfWaiters*/, db);

                if (lockRet.getLockGrant() == LockGrantType.DENIED) {

                    /*
                     * LN is currently locked by another Locker, so we can't
                     * assume anything about the value of the LSN in the bin.
                     */
                    nLNsLocked.increment();
                    completed = true;
                    return true;
                } else {
                    nLNsDead.increment();
                    obsolete = true;
                    completed = true;
                    return false;
                }

            } else if (bin.isEmbeddedLN(index)) {
                throw EnvironmentFailureException.unexpectedState(
                    env,
                    "LN is embedded although its associated logrec (at " +
                    treeLsn + " does not have the embedded flag on");
            }

            /*
             * Get the ln so that we can log it to its new position.
             * Notice that the fetchLN() call below will return null if the
             * slot is defunct and the LN has been purged by the cleaner.
             */
            ln = (LN) bin.getTarget(index);
            if (ln == null) {
                ln = bin.fetchLN(index, CacheMode.EVICT_LN);
                clearTarget = !db.getId().equals(DbTree.ID_DB_ID);
            }

            /* Don't migrate defunct LNs. */
            if (ln == null || ln.isDeleted()) {
                bin.setKnownDeletedAndEvictLN(index);
                nLNsDead.increment();
                obsolete = true;
                completed = true;
                return false;
            }

            /*
             * Migrate the LN.
             *
             * Do not pass a locker, because there is no need to lock the new
             * LSN, as done for user operations.  Another locker cannot attempt
             * to lock the new LSN until we're done, because we release the
             * lock before we release the BIN latch.
             */
            final LogItem logItem = ln.log(
                env, db, null /*locker*/, null /*writeLockInfo*/,
                false /*newEmbeddedLN*/, bin.getKey(index),
                bin.getExpiration(index), bin.isExpirationInHours(),
                false /*currEmbeddedLN*/, treeLsn, bin.getLastLoggedSize(index),
                false /*isInsertion*/, true /*backgroundIO*/,
                getMigrationRepContext(ln));

            bin.updateEntry(
                index, logItem.lsn, ln.getVLSNSequence(),
                logItem.size);

            nLNsMigrated.increment();

            /* Lock new LSN on behalf of existing lockers. */
            CursorImpl.lockAfterLsnChange(
                db, treeLsn, logItem.lsn, null /*excludeLocker*/);

            migrated = true;
            completed = true;
            return false;

        } finally {
            /*
             * If the node was originally non-resident, evict it now so that we
             * don't create more work for the evictor and reduce the cache
             * memory available to the application.
             */
            if (clearTarget) {
                bin.evictLN(index);
            }

            if (locker != null) {
                locker.operationEnd();
            }

            logFine(
                CLEAN_PENDING_LN, ln, treeLsn, completed, obsolete, migrated);
        }
    }

    /**
     * Returns the ReplicationContext to use for migrating the given LN.  If
     * VLSNs are preserved in this Environment then the VLSN is logically part
     * of the data record, and LN.getVLSNSequence will return the VLSN, which
     * should be included in the migrated LN.
     */
    static ReplicationContext getMigrationRepContext(LN ln) {
        long vlsnSeq = ln.getVLSNSequence();
        if (vlsnSeq <= 0) {
            return ReplicationContext.NO_REPLICATE;
        }
        return new ReplicationContext(new VLSN(vlsnSeq),
                                      false /*inReplicationStream*/);
    }

    /**
     * Adds the DB ID to the pending DB set if it is being deleted but deletion
     * is not yet complete.
     */
    void addPendingDB(DatabaseImpl db) {
        if (db != null && db.isDeleted() && !db.isDeleteFinished()) {
            DatabaseId id = db.getId();
            if (fileSelector.addPendingDB(id)) {
                LoggerUtils.logMsg(logger, env, Level.FINE,
                                   "CleanAddPendingDB " + id);
            }
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void logFine(String action,
               Node node,
               long logLsn,
               boolean completed,
               boolean obsolete,
               boolean dirtiedMigrated) {

        if (logger.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder();
            sb.append(action);
            if (node instanceof IN) {
                sb.append(" node=");
                sb.append(((IN) node).getNodeId());
            }
            sb.append(" logLsn=");
            sb.append(DbLsn.getNoFormatString(logLsn));
            sb.append(" complete=").append(completed);
            sb.append(" obsolete=").append(obsolete);
            sb.append(" dirtiedOrMigrated=").append(dirtiedMigrated);

            LoggerUtils.logMsg(logger, env, Level.FINE, sb.toString());
        }
    }

    /**
     * Release resources and update memory budget. Should only be called
     * when this environment is closed and will never be accessed again.
     */
    public void close() {
        profile.close();
        tracker.close();
        fileSelector.close(env.getMemoryBudget());
    }
}
