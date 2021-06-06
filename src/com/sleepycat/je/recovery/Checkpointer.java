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

package com.sleepycat.je.recovery;

import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_CHECKPOINTS;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_DELTA_IN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_FULL_BIN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_FULL_IN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPTID;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_END;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_INTERVAL;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_START;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.GROUP_DESC;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.GROUP_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.cleaner.FileSelector.CheckpointStartCleanerState;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LSNStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * The Checkpointer looks through the tree for internal nodes that must be
 * flushed to the log. Checkpoint flushes must be done in ascending order from
 * the bottom of the tree up.
 *
 * Checkpoint and IN Logging Rules
 * -------------------------------
 * The checkpoint must log, and make accessible via non-provisional ancestors,
 * all INs that are dirty at CkptStart.  If we crash and recover from that
 * CkptStart onward, any IN that became dirty (before the crash) after the
 * CkptStart must become dirty again as the result of replaying the action that
 * caused it to originally become dirty.
 *
 * Therefore, when an IN is dirtied at some point in the checkpoint interval,
 * but is not logged by the checkpoint, the log entry representing the action
 * that dirtied the IN must follow either the CkptStart or the FirstActiveLSN
 * that is recorded in the CkptEnd entry.  The FirstActiveLSN is less than or
 * equal to the CkptStart LSN.  Recovery will process LNs between the
 * FirstActiveLSN and the end of the log.  Other entries are only processed
 * from the CkptStart forward.  And provisional entries are not processed.
 *
 * Example: Non-transactional LN logging.  We take two actions: 1) log the LN
 * and then 2) dirty the parent BIN.  What if the LN is logged before CkptStart
 * and the BIN is dirtied after CkptStart?  How do we avoid breaking the rules?
 * The answer is that we log the LN while holding the latch on the parent BIN,
 * and we don't release the latch until after we dirty the BIN.   The
 * construction of the checkpoint dirty map requires latching the BIN.  Since
 * the LN was logged before CkptStart, the BIN will be dirtied before the
 * checkpointer latches it during dirty map construction.  So the BIN will
 * always be included in the dirty map and logged by the checkpoint.
 *
 * Example: Abort.  We take two actions: 1) log the abort and then 2) undo the
 * changes, which modifies (dirties) the BIN parents of the undone LNs.  There
 * is nothing to prevent logging CkptStart in between these two actions, so how
 * do we avoid breaking the rules?  The answer is that we do not unregister the
 * transaction until after the undo phase.  So although the BINs may be dirtied
 * by the undo after CkptStart is logged, the FirstActiveLSN will be prior to
 * CkptStart.  Therefore, we will process the Abort and replay the action that
 * modifies the BINs.
 *
 * Exception: Lazy migration.  The log cleaner will make an IN dirty without
 * logging an action that makes it dirty.  This is an exception to the general
 * rule that actions should be logged when they cause dirtiness.   The reasons
 * this is safe are:
 * 1. The IN contents are not modified, so there is no information lost if the
 *    IN is never logged, or is logged provisionally and no ancestor is logged
 *    non-provisionally.
 * 2. If the IN is logged non-provisionally, this will have the side effect of
 *    recording the old LSN as being obsolete. However, the general rules for
 *    checkpointing and recovery will ensure that the new version is used in
 *    the Btree.  The new version will either be replayed by recovery or
 *    referenced in the active Btree via a non-provisional ancestor.
 *
 * Checkpoint Algorithm TODO update this
 * --------------------
 * The final checkpointDirtyMap field is used to hold (in addition to the dirty
 * INs) the state of the checkpoint and highest flush levels.  Access to this
 * object is synchronized so that eviction and checkpointing can access it
 * concurrently.  When a checkpoint is not active, the state is CkptState.NONE
 * and the dirty map is empty.  When a checkpoint runs, we do this:
 *
 * 1. Get set of files from cleaner that can be deleted after this checkpoint.
 * 2. Set checkpointDirtyMap state to DIRTY_MAP_INCOMPLETE, meaning that dirty
 *    map construction is in progress.
 * 3. Log CkptStart
 * 4. Construct dirty map, organized by Btree level, from dirty INs in INList.
 *    The highest flush levels are calculated during dirty map construction.
 *    Set checkpointDirtyMap state to DIRTY_MAP_COMPLETE.
 * 5. Flush INs in dirty map.
 *        + First, flush the bottom two levels a sub-tree at a time, where a
 *          sub-tree is one IN at level two and all its BIN children.  Higher
 *          levels (above level two) are logged strictly by level, not using
 *          subtrees.
 *              o If je.checkpointer.highPriority=false, we log one IN at a
 *                time, whether or not the IN is logged as part of a subtree,
 *                and do a Btree search for the parent of each IN.
 *              o If je.checkpointer.highPriority=true, for the bottom two
 *                levels we log each sub-tree in a single call to the
 *                LogManager with the parent IN latched, and we only do one
 *                Btree search for each level two IN.  Higher levels are logged
 *                one IN at a time as with highPriority=false.
 *        + The Provisional property is set as follows, depending on the level
 *          of the IN:
 *              o level is max flush level:  Provisional.NO
 *              o level is bottom level: Provisional.YES
 *              o Otherwise (middle levels): Provisional.BEFORE_CKPT_END
 *  6. Flush VLSNIndex cache to make VLSNIndex recoverable.
 *  7. Flush UtilizationTracker (write FileSummaryLNs) to persist all
 *     tracked obsolete offsets and utilization summary info, to make this info
 *     recoverable.
 *  8. Log CkptEnd
 *  9. Delete cleaned files from step 1.
 * 10. Set checkpointDirtyMap state to NONE.
 *
 * Per-DB Highest Flush Level
 * --------------------------
 * As mentioned above, when the dirty map is constructed we also determine the
 * highest flush level for each database.  This is the maximum Btree level at
 * which a dirty node exists in the DB.
 *
 * When logging a node below the maxFlushLevel, we add the parent to the dirty
 * map.  It may or may not have been added when the dirty map was constructed.
 * The idea is to flush all ancestors of all nodes in the dirty map, up to and
 * including the maxFlushLevel, even if those ancestors were not dirty when the
 * dirty map was constructed.
 *
 * This is done to avoid orphaning a dirty node as shown in this example.
 *
 *          IN-A       (root level=4)
 *         /    \
 *   (d) IN-B   IN-C   (maxFlushLevel=3)
 *               \
 *           (d) IN-D
 *
 * IN-C is not dirty (d) when the dirty map is constructed, but it will be
 * logged because its child (IN-D) is dirty, and it is not above maxFlushLevel.
 *
 * If IN-C were not logged, and there were a crash after the checkpoint, the
 * changes to IN-D would be lost.  IN-D would not be replayed by recovery
 * because it is logged provisionally, and it would not be accessible via its
 * parent.  This is because only nodes at maxFlushLevel are logged
 * non-provisionally.  The actions that led to the changes in IN-D may not be
 * replayed either, because they may appear before the firstActiveLsn
 * associated with the checkpoint.
 *
 * When log files are to be deleted at the end of the checkpoint (after being
 * processed by the log cleaner), the maxFlushLevel is increased by one.
 * This is to ensure that LSNs in deleted files will not be fetched during
 * recovery.  Such files are in the FileSelector.CLEANED state, which means
 * they have been processed by the cleaner since the last checkpoint.
 *
 * TODO: Document circumstances and motivation for the extra flush level.
 *
 * Lastly, for Database.sync or a checkpoint with MinimizeRecoveryTime
 * configured, we will flush all the way to the root rather than using the
 * maxFlushLevel computed as described above.
 *
 * Provisional.BEFORE_CKPT_END
 * ---------------------------
 * See Provisional.java for a description of the relationship between the
 * checkpoint algorithm above and the BEFORE_CKPT_END property.
 *
 * Coordination of Eviction and Checkpointing
 * ------------------------------------------
 * Eviction can proceed concurrently with all phases of a checkpoint, and
 * eviction may take place concurrently in multiple threads.  This concurrency
 * is crucial to avoid blocking application threads that perform eviction and
 * to reduce the amount of eviction required in application threads.
 *
 * Eviction calls Checkpointer.coordinateEvictionWithCheckpoint, which calls
 * DirtyINMap.coordinateEvictionWithCheckpoint, just before logging an IN.
 * coordinateEvictionWithCheckpoint returns whether the IN should be logged
 * provisionally (Provisional.YES) or non-provisionally (Provisional.NO).
 *
 * Other coordination necessary depends on the state of the checkpoint:
 *   + NONE: No additional action.
 *      o return Provisional.NO
 *   + DIRTY_MAP_INCOMPLETE: The parent IN is added to the dirty map, exactly
 *     as if it were encountered as dirty in the INList during dirty map
 *     construction.
 *      o IN is root: return Provisional.NO
 *      o IN is not root: return Provisional.YES
 *   + DIRTY_MAP_COMPLETE:
 *      o IN level GTE highest flush level: return Provisional.NO
 *      o IN level LT highest flush level: return Provisional.YES
 *
 * In general this is designed so that eviction will use the same provisional
 * value that would be used by the checkpoint, as if the checkpoint itself were
 * logging the IN.  However, there are several conditions where this is not
 * exactly the case.
 *
 * 1. Eviction may log an IN with Provisional.YES when the IN was not dirty at
 *    the time of dirty map creation, if it became dirty afterwards.  In this
 *    case, the checkpointer would not have logged the IN at all.  This is safe
 *    because the actions that made that IN dirty are logged in the recovery
 *    period.
 * 2. Eviction may log an IN with Provisional.YES after the checkpoint has
 *    logged it, if it becomes dirty again.  In this case the IN is logged
 *    twice, which would not have been done by the checkpoint alone.  This is
 *    safe because the actions that made that IN dirty are logged in the
 *    recovery period.
 * 3. An intermediate level IN (not bottom most and not the highest flush
 *    level) will be logged by the checkpoint with Provisional.BEFORE_CKPT_END
 *    but will be logged by eviction with Provisional.YES.  See below for why
 *    this is safe.
 * 4. Between checkpoint step 8 (log CkptEnd) and 10 (set checkpointDirtyMap
 *    state to NONE), eviction may log an IN with Provisional.YES, although a
 *    checkpoint is not strictly active during this interval.  See below for
 *    why this is safe.
 *
 * It is safe for eviction to log an IN as Provisional.YES for the last two
 * special cases, because this does not cause incorrect recovery behavior.  For
 * recovery to work properly, it is only necessary that:
 *
 *  + Provisional.NO is used for INs at the max flush level during an active
 *    checkpoint.
 *  + Provisional.YES or BEFORE_CKPT_END is used for INs below the max flush
 *    level, to avoid replaying an IN during recovery that may depend on a file
 *    deleted as the result of the checkpoint.
 *
 * You may ask why we don't use Provisional.YES for eviction when a checkpoint
 * is not active.  There are two reason, both related to performance:
 *
 * 1. This would be wasteful when an IN is evicted in between checkpoints, and
 *    that portion of the log is processed by recovery later, in the event of a
 *    crash.  The evicted INs would be ignored by recovery, but the actions
 *    that caused them to be dirty would be replayed and the INs would be
 *    logged again redundantly.
 * 2. Logging a IN provisionally will not count the old LSN as obsolete
 *    immediately, so cleaner utilization will be inaccurate until the a
 *    non-provisional parent is logged, typically by the next checkpoint.  It
 *    is always important to keep the cleaner from stalling and spiking, to
 *    keep latency and throughput as level as possible.
 *
 * Therefore, it is safe to log with Provisional.YES in between checkpoints,
 * but not desirable.
 *
 * Although we don't do this, it would be safe and optimal to evict with
 * BEFORE_CKPT_END in between checkpoints, because it would be treated by
 * recovery as if it were Provisional.NO.  This is because the interval between
 * checkpoints is only processed by recovery if it follows the last CkptEnd,
 * and BEFORE_CKPT_END is treated as Provisional.NO if the IN follows the last
 * CkptEnd.
 *
 * However, it would not be safe to evict an IN with BEFORE_CKPT_END during a
 * checkpoint, when logging of the IN's ancestors does not occur according to
 * the rules of the checkpoint.  If this were done, then if the checkpoint
 * completes and is used during a subsequent recovery, an obsolete offset for
 * the old version of the IN will mistakenly be recorded.  Below are two cases
 * where BEFORE_CKPT_END is used correctly and one showing how it could be used
 * incorrectly.
 *
 * 1. Correct use of BEFORE_CKPT_END when the checkpoint does not complete.
 *
 *        050 BIN-A
 *        060 IN-B parent of BIN-A
 *        100 CkptStart
 *        200 BIN-A logged with BEFORE_CKPT_END
 *        300 FileSummaryLN with obsolete offset for BIN-A at 050
 *        Crash and recover
 *
 *    Recovery will process BIN-A at 200 (it will be considered
 *    non-provisional) because there is no following CkptEnd.  It is
 *    therefore correct that BIN-A at 050 is obsolete.
 *
 * 2. Correct use of BEFORE_CKPT_END when the checkpoint does complete.
 *
 *        050 BIN-A
 *        060 IN-B parent of BIN-A
 *        100 CkptStart
 *        200 BIN-A logged with BEFORE_CKPT_END
 *        300 FileSummaryLN with obsolete offset for BIN-A at 050
 *        400 IN-B parent of BIN-A, non-provisional
 *        500 CkptEnd
 *        Crash and recover
 *
 *    Recovery will not process BIN-A at 200 (it will be considered
 *    provisional) because there is a following CkptEnd, but it will
 *    process its parent IN-B at 400, and therefore the BIN-A at 200 will be
 *    active in the tree.  It is therefore correct that BIN-A at 050 is
 *    obsolete.
 *
 * 3. Incorrect use of BEFORE_CKPT_END when the checkpoint does complete.
 *
 *        050 BIN-A
 *        060 IN-B parent of BIN-A
 *        100 CkptStart
 *        200 BIN-A logged with BEFORE_CKPT_END
 *        300 FileSummaryLN with obsolete offset for BIN-A at 050
 *        400 CkptEnd
 *        Crash and recover
 *
 *    Recovery will not process BIN-A at 200 (it will be considered
 *    provisional) because there is a following CkptEnd, but no parent
 *    IN-B is logged, and therefore the IN-B at 060 and BIN-A at 050 will be
 *    active in the tree.  It is therefore incorrect that BIN-A at 050 is
 *    obsolete.
 *
 * This last case is what caused the LFNF in SR [#19422], when BEFORE_CKPT_END
 * was mistakenly used for logging evicted BINs via CacheMode.EVICT_BIN.
 * During the checkpoint, we evict BIN-A and log it with BEFORE_CKPT_END, yet
 * neither it nor its parent are part of the checkpoint.  After being counted
 * obsolete, we crash and recover.  Then the file containing the BIN (BIN-A at
 * 050 above) is cleaned and deleted.  During cleaning, it is not migrated
 * because an obsolete offset was previously recorded.  The LFNF occurs when
 * trying to access this BIN during a user operation.
 *
 * CacheMode.EVICT_BIN
 * -------------------
 * Unlike in JE 4.0 where EVICT_BIN was first introduced, in JE 4.1 and later
 * we do not use special rules when an IN is evicted.  Since concurrent
 * eviction and checkpointing are supported in JE 4.1, the above rules apply to
 * EVICT_BIN as well as all other types of eviction.
 */
public class Checkpointer extends DaemonThread implements EnvConfigObserver {

    /**
     * For unit testing only.  Called before we flush the max level.  This
     * field is static because it is called from the static flushIN method.
     */
    private static TestHook<?> maxFlushLevelHook = null;

    private static TestHook<?> beforeFlushHook = null;

    static TestHook<IN> examineINForCheckpointHook = null;

    /* Checkpoint sequence, initialized at recovery. */
    private long checkpointId;

    /*
     * How much the log should grow between checkpoints. If 0, we're using time
     * based checkpointing.
     */
    private final long logSizeBytesInterval;
    private final long logFileMax;
    private final long timeInterval;
    private long lastCheckpointMillis;
    private volatile boolean wakeupAfterNoWrites;

    /* Configured to true to minimize checkpoint duration. */
    private boolean highPriority;

    private long nCheckpoints;
    private long lastCheckpointStart;
    private long lastCheckpointEnd;
    private long lastCheckpointInterval;
    private final FlushStats flushStats;

    /**
     * The DirtyINMap for checkpointing is created once and is reset after each
     * checkpoint is complete.  Access to this object is synchronized so that
     * eviction and checkpointing can access it concurrently.
     */
    private final DirtyINMap checkpointDirtyMap;

    public Checkpointer(EnvironmentImpl envImpl,
                        long waitTime,
                        String name) {
        super(waitTime, name, envImpl);
        logSizeBytesInterval =
            envImpl.getConfigManager().getLong
                (EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL);
        logFileMax =
            envImpl.getConfigManager().getLong(EnvironmentParams.LOG_FILE_MAX);
        timeInterval = waitTime;
        lastCheckpointMillis = 0;

        nCheckpoints = 0;
        flushStats = new FlushStats();

        checkpointDirtyMap = new DirtyINMap(envImpl);

        /* Initialize mutable properties and register for notifications. */
        envConfigUpdate(envImpl.getConfigManager(), null);
        envImpl.addConfigObserver(this);
    }

    /**
     * Process notifications of mutable property changes.
     */
    @Override
    public void envConfigUpdate(DbConfigManager cm,
                                EnvironmentMutableConfig ignore) {
        highPriority = cm.getBoolean
            (EnvironmentParams.CHECKPOINTER_HIGH_PRIORITY);
    }

    /**
     * Initializes the checkpoint intervals when no checkpoint is performed
     * while opening the environment.
     */
    void initIntervals(long lastCheckpointStart,
                       long lastCheckpointEnd,
                       long lastCheckpointMillis) {
        this.lastCheckpointStart = lastCheckpointStart;
        this.lastCheckpointEnd = lastCheckpointEnd;
        this.lastCheckpointMillis = lastCheckpointMillis;
    }

    /**
     * Coordinates an eviction with an in-progress checkpoint and returns
     * whether provisional logging is needed.
     *
     * @return the provisional status to use for logging the target.
     */
    public Provisional coordinateEvictionWithCheckpoint(
        final DatabaseImpl db,
        final int targetLevel,
        final IN parent) {

        return checkpointDirtyMap.
            coordinateEvictionWithCheckpoint(db, targetLevel, parent);
    }

    /**
     * Coordinates a split with an in-progress checkpoint.
     *
     * @param newSibling the sibling IN created by the split.
     */
    public void coordinateSplitWithCheckpoint(final IN newSibling) {
        checkpointDirtyMap.coordinateSplitWithCheckpoint(newSibling);
    }

    /**
     * Figure out the wakeup period. Supplied through this static method
     * because we need to pass wakeup period to the superclass and need to do
     * the calcuation outside this constructor.
     *
     * @throws IllegalArgumentException via Environment ctor and
     * setMutableConfig.
     */
    public static long getWakeupPeriod(DbConfigManager configManager)
        throws IllegalArgumentException {

        long wakeupPeriod = configManager.getDuration
            (EnvironmentParams.CHECKPOINTER_WAKEUP_INTERVAL);
        long bytePeriod = configManager.getLong
            (EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL);

        /* Checkpointing period must be set either by time or by log size. */
        if ((wakeupPeriod == 0) && (bytePeriod == 0)) {
            throw new IllegalArgumentException
                (EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getName() +
                 " and " +
                 EnvironmentParams.CHECKPOINTER_WAKEUP_INTERVAL.getName() +
                 " cannot both be 0. ");
        }

        /*
         * Checkpointing by log size takes precendence over time based period.
         */
        if (bytePeriod == 0) {
            return wakeupPeriod;
        } else {
            return 0;
        }
    }

    /**
     * Set checkpoint id -- can only be done after recovery.
     */
    synchronized void setCheckpointId(long lastCheckpointId) {
        checkpointId = lastCheckpointId;
    }

    /**
     * Load stats.
     */
    @SuppressWarnings("unused")
    public StatGroup loadStats(StatsConfig config) {
        StatGroup stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        new LongStat(stats, CKPT_LAST_CKPTID, checkpointId);
        new LongStat(stats, CKPT_CHECKPOINTS, nCheckpoints);
        new LongStat(stats, CKPT_LAST_CKPT_INTERVAL, lastCheckpointInterval);
        new LSNStat(stats, CKPT_LAST_CKPT_START, lastCheckpointStart);
        new LSNStat(stats, CKPT_LAST_CKPT_END, lastCheckpointEnd);
        new LongStat(stats, CKPT_FULL_IN_FLUSH, flushStats.nFullINFlush);
        new LongStat(stats, CKPT_FULL_BIN_FLUSH, flushStats.nFullBINFlush);
        new LongStat(stats, CKPT_DELTA_IN_FLUSH, flushStats.nDeltaINFlush);

        if (config.getClear()) {
            nCheckpoints = 0;
            flushStats.nFullINFlush = 0;
            flushStats.nFullBINFlush = 0;
            flushStats.nDeltaINFlush = 0;
        }

        return stats;
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    @Override
    protected long nDeadlockRetries() {
        return envImpl.getConfigManager().getInt
            (EnvironmentParams.CHECKPOINTER_RETRY);
    }

    /**
     * Called whenever the DaemonThread wakes up from a sleep.
     */
    @Override
    protected void onWakeup() {

        if (envImpl.isClosing()) {
            return;
        }

        doCheckpoint(
            CheckpointConfig.DEFAULT, "daemon", true /*invokedFromDaemon*/);

        wakeupAfterNoWrites = false;
    }

    /**
     * Wakes up the checkpointer if a checkpoint log interval is configured and
     * the number of bytes written since the last checkpoint exceeds the size
     * of the interval.
     */
    public void wakeupAfterWrite() {

        if ((logSizeBytesInterval != 0) && !isRunning()) {

            long nextLsn = envImpl.getFileManager().getNextLsn();

            if (DbLsn.getNoCleaningDistance(
                    nextLsn, lastCheckpointStart, logFileMax) >=
                logSizeBytesInterval) {

                wakeup();
            }
        }
    }

    /**
     * Wakes up the checkpointer if a checkpoint is needed to reclaim disk
     * space for already cleaned files. This method is called after an idle
     * period with no writes.
     */
    public void wakeupAfterNoWrites() {

        if (!isRunning() && needCheckpointForCleanedFiles()) {
            wakeupAfterNoWrites = true;
            wakeup();
        }
    }

    private boolean needCheckpointForCleanedFiles() {
        return envImpl.getCleaner().getFileSelector().isCheckpointNeeded();
    }

    /**
     * Determine whether a checkpoint should be run.
     */
    private boolean isRunnable(CheckpointConfig config) {
        /* Figure out if we're using log size or time to determine interval.*/
        long useBytesInterval = 0;
        long useTimeInterval = 0;
        long nextLsn = DbLsn.NULL_LSN;
        boolean runnable = false;
        try {
            if (config.getForce()) {
                runnable = true;
                return true;
            }

            if (wakeupAfterNoWrites && needCheckpointForCleanedFiles()) {
                runnable = true;
                return true;
            }

            if (config.getKBytes() != 0) {
                useBytesInterval = config.getKBytes() << 10;

            } else if (config.getMinutes() != 0) {
                /* Convert to millis. */
                useTimeInterval = config.getMinutes() * 60 * 1000;

            } else if (logSizeBytesInterval != 0) {
                useBytesInterval = logSizeBytesInterval;

            } else {
                useTimeInterval = timeInterval;
            }

            /*
             * If our checkpoint interval is defined by log size, check on how
             * much log has grown since the last checkpoint.
             */
            if (useBytesInterval != 0) {
                nextLsn = envImpl.getFileManager().getNextLsn();

                if (DbLsn.getNoCleaningDistance(
                    nextLsn, lastCheckpointStart, logFileMax) >=
                    useBytesInterval) {

                    runnable = true;
                }

            } else if (useTimeInterval != 0) {

                /*
                 * Our checkpoint is determined by time.  If enough time has
                 * passed and some log data has been written, do a checkpoint.
                 */
                final long lastUsedLsn =
                    envImpl.getFileManager().getLastUsedLsn();

                if (((System.currentTimeMillis() - lastCheckpointMillis) >=
                     useTimeInterval) &&
                    (DbLsn.compareTo(lastUsedLsn, lastCheckpointEnd) != 0)) {

                    runnable = true;
                }
            }
            return runnable;

        } finally {
            if (logger.isLoggable(Level.FINEST)) {
                final StringBuilder sb = new StringBuilder();
                sb.append("size interval=").append(useBytesInterval);
                if (nextLsn != DbLsn.NULL_LSN) {
                    sb.append(" nextLsn=").
                        append(DbLsn.getNoFormatString(nextLsn));
                }
                if (lastCheckpointEnd != DbLsn.NULL_LSN) {
                    sb.append(" lastCkpt=");
                    sb.append(DbLsn.getNoFormatString(lastCheckpointEnd));
                }
                sb.append(" time interval=").append(useTimeInterval);
                sb.append(" force=").append(config.getForce());
                sb.append(" runnable=").append(runnable);

                LoggerUtils.finest(logger, envImpl, sb.toString());
            }
        }
    }

    /**
     * The real work to do a checkpoint. This may be called by the checkpoint
     * thread when waking up, or it may be invoked programatically through the
     * api.
     *
     * @param invokingSource a debug aid, to indicate who invoked this
     *       checkpoint. (i.e. recovery, the checkpointer daemon, the cleaner,
     *       programatically)
     */
    public synchronized void doCheckpoint(CheckpointConfig config,
                                          String invokingSource,
                                          boolean invokedFromDaemon) {
        if (envImpl.isReadOnly()) {
            return;
        }

        if (!isRunnable(config)) {
            return;
        }

        /* Stop if we cannot write because of a disk limit violation. */
        try {
            envImpl.checkDiskLimitViolation();
        } catch (DiskLimitException e) {
            if (!invokedFromDaemon) {
                throw e;
            }
            return;
        }

        /*
         * If minimizing recovery time is desired, then flush all the way to
         * the top of the dbtree instead of stopping at the highest level last
         * modified, so that only the root INs are processed by recovery.
         */
        final boolean flushAll = config.getMinimizeRecoveryTime();

        /*
         * If there are cleaned files to be deleted, flush an extra level to
         * write out the parents of cleaned nodes.  This ensures that no node
         * will contain the LSN of a cleaned file.
         *
         * Note that we don't currently distinguish between files in the
         * CLEANED and FULLY_PROCESSED states.  For a FULLY_PROCESSED file, a
         * pending LN may have been processed since the prior checkpoint.
         * However, the BIN containing the LSN of the LN is guaranteed to be
         * logged, so there is no need to increment maxFlushLevel.  So we could
         * optimize in the future and only set flushExtraLevel when some files
         * are CLEANED (i.e., do not set flushExtraLevel when all files are
         * FULLY_PROCESSED or cleanerState.isEmpty()).
         */
        final Cleaner cleaner = envImpl.getCleaner();

        final CheckpointStartCleanerState cleanerState =
            cleaner.getFilesAtCheckpointStart();

        final boolean flushExtraLevel = !cleanerState.isEmpty();

        lastCheckpointMillis = System.currentTimeMillis();
        flushStats.resetPerRunCounters();

        /* Get the next checkpoint id. */
        checkpointId++;
        nCheckpoints++;

        boolean success = false;
        boolean traced = false;

        final LogManager logManager = envImpl.getLogManager();

        /*
         * Set the checkpoint state so that concurrent eviction can be
         * coordinated.
         */
        checkpointDirtyMap.beginCheckpoint(flushAll, flushExtraLevel);

        try {
            /* Log the checkpoint start. */
            final SingleItemEntry<CheckpointStart> startEntry =
                SingleItemEntry.create(
                    LogEntryType.LOG_CKPT_START,
                    new CheckpointStart(checkpointId, invokingSource));

            final long checkpointStart =
                logManager.log(startEntry, ReplicationContext.NO_REPLICATE);

            /*
             * Note the first active LSN point. The definition of
             * firstActiveLsn is that all log entries for active transactions
             * are equal to or after that LSN.  This is the starting point for
             * replaying LNs during recovery and will be stored in the CkptEnd
             * entry.
             *
             * Use the checkpointStart as the firstActiveLsn if firstActiveLsn
             * is null, meaning that no txns are active.
             *
             * The current value must be retrieved from TxnManager after
             * logging CkptStart. If it were instead retrieved before logging
             * CkptStart, the following failure could occur.  [#20270]
             *
             *  ... getFirstActiveLsn returns NULL_LSN, will use 200 CkptStart
             *  100 LN-A in Txn-1
             *  200 CkptStart
             *  300 BIN-B refers to 100 LN-A
             *  400 CkptEnd
             *  ... Crash and recover.  Recovery does not undo 100 LN-A.
             *  ... Txn-1 is uncommitted, yet 100 LN-A takes effect.
             */
            long firstActiveLsn = envImpl.getTxnManager().getFirstActiveLsn();
            if (firstActiveLsn == DbLsn.NULL_LSN) {
                firstActiveLsn = checkpointStart;
            }

            /*
             * In a replicated system, the checkpointer will be flushing out
             * the VLSNIndex, which is HA metadata. Check that the in-memory
             * version encompasses all metadata up to the point of the
             * CheckpointStart record. This is no-op for non-replicated
             * systems. [#19754]
             */
            envImpl.awaitVLSNConsistency();

            /* Find the set of dirty INs that must be logged. */
            checkpointDirtyMap.selectDirtyINsForCheckpoint();

            /* Call hook after dirty map creation and before flushing. */
            TestHookExecute.doHookIfSet(beforeFlushHook);

            /* Flush IN nodes. */
            flushDirtyNodes(
                envImpl, checkpointDirtyMap, checkpointStart, highPriority,
                flushStats);

            if (DirtyINMap.DIRTY_SET_DEBUG_TRACE) {
                LoggerUtils.logMsg(
                    envImpl.getLogger(), envImpl, Level.INFO,
                    "Ckpt flushed" +
                        " nFullINFlushThisRun = " +
                        flushStats.nFullINFlushThisRun +
                        " nFullBINFlushThisRun = " +
                        flushStats.nFullBINFlushThisRun +
                        " nDeltaINFlushThisRun = " +
                        flushStats.nDeltaINFlushThisRun);

            }

            /*
             * Flush MapLNs if not already done by flushDirtyNodes.  Only flush
             * a database if it has not already been flushed since checkpoint
             * start.  Lastly, flush the DB mapping tree root.
             */
            checkpointDirtyMap.flushMapLNs(checkpointStart);
            checkpointDirtyMap.flushRoot(checkpointStart);

            /*
             * Flush replication information if necessary so that the VLSNIndex
             * cache is flushed and is recoverable.
             */
            envImpl.preCheckpointEndFlush();

            /*
             * Flush utilization info AFTER flushing IN nodes to reduce the
             * inaccuracies caused by the sequence FileSummaryLN-LN-BIN.
             */
            envImpl.getUtilizationProfile().flushFileUtilization
                (envImpl.getUtilizationTracker().getTrackedFiles());

            final DbTree dbTree = envImpl.getDbTree();
            final boolean willDeleteFiles = !cleanerState.isEmpty();

            final CheckpointEnd ckptEnd = new CheckpointEnd(
                invokingSource, checkpointStart, envImpl.getRootLsn(),
                firstActiveLsn,
                envImpl.getNodeSequence().getLastLocalNodeId(),
                envImpl.getNodeSequence().getLastReplicatedNodeId(),
                dbTree.getLastLocalDbId(), dbTree.getLastReplicatedDbId(),
                envImpl.getTxnManager().getLastLocalTxnId(),
                envImpl.getTxnManager().getLastReplicatedTxnId(),
                checkpointId, willDeleteFiles);

            final SingleItemEntry<CheckpointEnd> endEntry =
                SingleItemEntry.create(LogEntryType.LOG_CKPT_END, ckptEnd);

            /*
             * Log checkpoint end and update state kept about the last
             * checkpoint location. Send a trace message *before* the
             * checkpoint end log entry. This is done so that the normal trace
             * message doesn't affect the time-based isRunnable() calculation,
             * which only issues a checkpoint if a log record has been written
             * since the last checkpoint.
             */
            trace(envImpl, invokingSource, true);
            traced = true;

            lastCheckpointInterval = DbLsn.getNoCleaningDistance(
                checkpointStart, lastCheckpointStart, logFileMax);

            /*
             * We must flush and fsync to ensure that cleaned files are not
             * referenced. This also ensures that this checkpoint is not wasted
             * if we crash.
             */
            lastCheckpointEnd = logManager.logForceFlush(
                endEntry, true /*fsyncRequired*/,
                ReplicationContext.NO_REPLICATE);

            lastCheckpointStart = checkpointStart;

            success = true;
            cleaner.updateFilesAtCheckpointEnd(cleanerState);

        } catch (DiskLimitException e) {

            LoggerUtils.logMsg(
                envImpl.getLogger(), envImpl, Level.WARNING,
                "Ckpt id=" + checkpointId + " success=" + success +
                " aborted because of disk limit violation: " + e);

            if (!invokedFromDaemon) {
                throw e;
            }

        } catch (DatabaseException e) {
            LoggerUtils.traceAndLogException(envImpl, "Checkpointer",
                                             "doCheckpoint", "checkpointId=" +
                                             checkpointId, e);
            throw e;
        } finally {

            /*
             * Reset the checkpoint state so evictor activity knows there's no
             * further requirement for provisional logging. SR 11163.
             */
            checkpointDirtyMap.reset();

            if (!traced) {
                trace(envImpl, invokingSource, success);
            }
        }
    }

    private void trace(EnvironmentImpl envImpl,
                       String invokingSource,
                       boolean success ) {

        final StringBuilder sb = new StringBuilder();
        sb.append("Checkpoint ").append(checkpointId);
        sb.append(": source=" ).append(invokingSource);
        sb.append(" success=").append(success);
        sb.append(" nFullINFlushThisRun=");
        sb.append(flushStats.nFullINFlushThisRun);
        sb.append(" nDeltaINFlushThisRun=");
        sb.append(flushStats.nDeltaINFlushThisRun);
        LoggerUtils.logMsg(logger, envImpl, Level.CONFIG, sb.toString());
    }

    /**
     * Flush a given database to disk. Like checkpoint, log from the bottom
     * up so that parents properly represent their children.
     */
    public void syncDatabase(EnvironmentImpl envImpl,
                             DatabaseImpl dbImpl,
                             boolean flushLog) {
        if (envImpl.isReadOnly()) {
            return;
        }

        envImpl.checkDiskLimitViolation();

        final DirtyINMap dirtyMap = new DirtyINMap(envImpl);
        final FlushStats fstats = new FlushStats();

        try {
            /* Find the dirty set. */
            dirtyMap.selectDirtyINsForDbSync(dbImpl);

            if (dirtyMap.getNumEntries() > 0) {
                /* Write all dirtyINs out.*/
                flushDirtyNodes(
                    envImpl, dirtyMap, DbLsn.NULL_LSN  /*ckptStart*/,
                    false /*highPriority*/, fstats);

                /* Make changes durable. [#15254] */
                if (flushLog) {
                    envImpl.getLogManager().flushSync();
                }
            }
        } catch (DiskLimitException e) {
            throw e;
        } catch (DatabaseException e) {
            LoggerUtils.traceAndLogException
                (envImpl, "Checkpointer", "syncDatabase",
                 "of " + dbImpl.getDebugName(), e);
            throw e;
        } finally {
            dirtyMap.reset();
        }
    }

    /* For unit testing only. */
    public static void setMaxFlushLevelHook(TestHook<?> hook) {
        maxFlushLevelHook = hook;
    }

    /* For unit testing only. */
    public static void setBeforeFlushHook(TestHook<?> hook) {
        beforeFlushHook = hook;
    }

    /**
     * Flush the nodes in order, from the lowest level to highest level.  As a
     * flush dirties its parent, add it to the dirty map, thereby cascading the
     * writes up the tree. If flushAll wasn't specified, we need only cascade
     * up to the highest level set at the start of checkpointing.
     *
     * Note that all but the top level INs are logged provisionally. That's
     * because we don't need to process lower INs during recovery because the
     * higher INs will end up pointing at them.
     */
    private static void flushDirtyNodes(EnvironmentImpl envImpl,
                                        DirtyINMap dirtyMap,
                                        long checkpointStart,
                                        boolean highPriority,
                                        FlushStats fstats) {

        final DbTree dbTree = envImpl.getDbTree();
        final Map<DatabaseId, DatabaseImpl> dbCache = new HashMap<>();

        try {
            while (dirtyMap.getNumLevels() > 0) {

                /*
                 * Work on one level's worth of nodes in ascending level order.
                 */
                final Integer currentLevel = dirtyMap.getLowestLevelSet();
                final int currentLevelVal = currentLevel;

                /*
                 * Flush MapLNs just prior to flushing the first level of the
                 * mapping tree.  Only flush a database if it has not already
                 * been flushed since checkpoint start.
                 */
                if (currentLevelVal == IN.DBMAP_LEVEL) {
                    dirtyMap.flushMapLNs(checkpointStart);
                }

                /* Flush the nodes at the current level. */
                while (true) {
                    final CheckpointReference targetRef =
                        dirtyMap.removeNextNode(currentLevel);

                    if (targetRef == null) {
                        break;
                    }

                    envImpl.checkDiskLimitViolation();

                    /*
                     * Check to make sure the DB was not deleted after putting
                     * it in the dirty map, and prevent the DB from being
                     * deleted while we're working with it.
                     */
                    final DatabaseImpl db = dbTree.getDb(
                        targetRef.dbId, -1 /*lockTimeout*/, dbCache);

                    if (db != null && !db.isDeleted()) {

                        /* Flush if we're below maxFlushLevel. */
                        final int maxFlushLevel =
                            dirtyMap.getHighestFlushLevel(db);

                        if (currentLevelVal <= maxFlushLevel) {

                            flushIN(
                                db, targetRef, dirtyMap, maxFlushLevel,
                                highPriority, fstats, true /*allowLogSubtree*/);

                            /*
                             * Sleep if background read/write limit was
                             * exceeded.
                             */
                            envImpl.sleepAfterBackgroundIO();
                        }
                    }

                    /*
                     * If the environment was invalidated by other activity,
                     * get out of this loop, and re-throw the invalidating
                     * exception to indicate that the checkpoint did not
                     * succeed.
                     */
                    envImpl.checkIfInvalid();
                }

                /* We're done with this level. */
                dirtyMap.removeLevel(currentLevel);
            }
        } finally {
            dbTree.releaseDbs(dbCache);
        }

        /*
         * Do not flush FileSummaryLNs/MapLNs (do not call
         * UtilizationProfile.flushLocalTracker) here because that flushing is
         * already done by the checkpoint.
         */
    }

    /**
     * Flush the target IN.
     *
     * Where applicable, also attempt to flush the subtree that houses this
     * target, which means we flush the siblings of this target to promote
     * better cleaning throughput. The problem lies in the fact that
     * provisionally logged nodes are not available for log cleaning until
     * their parent is logged non-provisionally.  On the other hand, we want to
     * log nodes in provisional mode as much as possible, both for recovery
     * performance, and for correctness to avoid fetches against cleaned log
     * files. (See [#16037].) These conflicting goals are reconciled by
     * flushing nodes in subtree grouping, because writing the non-provisional
     * parent of a set of provisionally written nodes frees the cleaner to work
     * on that set of provisional nodes as soon as possible. For example, if a
     * tree consists of:
     *
     *             INa
     *       +------+-------+
     *      INb            INc
     * +-----+----+         +-----+
     * BINd BINe BINf      BINg BINh
     *
     * It is more efficient for cleaning throughput to log in this order:
     *       BINd, BINe, BINf, INb, BINg, BINh, INc, INa
     * rather than:
     *       BINd, BINe, BINf, BINg, BINh, INb, INc, INa
     *
     * Suppose the subtree in question is INb->{BINd, BINe, BINf}
     *
     * Suppose we see BINd in the dirty map first, before BINe and BINf.
     *  - flushIN(BINd) is called
     *  - we fetch and latch its parent, INb
     *
     * If this is a high priority checkpoint, we'll hold the INb latch across
     * the time it takes to flush all three children.  In flushIN(BINd), we
     * walk through INb, create a local map of all the siblings that can be
     * found in the dirty map, and then call logSiblings with that local map.
     * Then we'll write out INb.
     *
     * If high priority is false, we will not hold the INb latch across
     * multiple IOs. Instead, we
     *  - write BINd out, using logSiblings
     *  - while still holding the INb latch, we create a list of dirty siblings
     *  - release the INb latch
     *  - call flushIN() recursively on each entry in the local sibling map,
     *    which will result in a search and write of each sibling.  These
     *    recursive calls to flushIN are called with the allowLogSubtree
     *    parameter of false to halt the recursion and prevent a repeat of the
     *    sibling examination.
     *  - write INb
     */
    private static void flushIN(final DatabaseImpl db,
                                final CheckpointReference targetRef,
                                final DirtyINMap dirtyMap,
                                final int maxFlushLevel,
                                final boolean highPriority,
                                final FlushStats fstats,
                                final boolean allowLogSubtree) {

        final EnvironmentImpl envImpl = db.getEnv();
        final Tree tree = db.getTree();
        final int targetLevel = targetRef.nodeLevel;

        /* Call test hook when we reach the max level. */
        assert (targetLevel < maxFlushLevel) ||
            TestHookExecute.doHookIfSet(maxFlushLevelHook);

        if (targetRef.isRoot) {

            final RootFlusher flusher =
                new RootFlusher(db, targetRef.nodeId);

            tree.withRootLatchedExclusive(flusher);

            /*
             * Update the tree's owner, whether it's the env root or the
             * db-mapping tree.
             */
            if (flusher.getFlushed()) {
                DbTree dbTree = envImpl.getDbTree();
                dbTree.modifyDbRoot(db);
                fstats.nFullINFlushThisRun++;
                fstats.nFullINFlush++;
            }

            /*
             * If this target isn't the root anymore, we'll have to handle it
             * like a regular node.
             */
            if (flusher.stillRoot()) {
                return;
            }
        }

        /*
         * The following applies to two cases:
         * (1) the target was not ever the root
         * (2) the target was the root, when the checkpoint dirty set was
         * assembled but is not the root now.
         */
        final SearchResult result = tree.getParentINForChildIN(
            -1 /*nodeId*/, targetRef.treeKey,
            targetRef.nodeLevel /*targetLevel*/,
            targetRef.nodeLevel + 1 /*exclusiveLevel*/,
            false  /*requireExactMatch*/, false /*doFetch*/,
            CacheMode.UNCHANGED, null /*trackingList*/);

        /*
         * If no possible parent is found, the compressor may have deleted
         * this item before we got to processing it. (Although it seems this
         * cannot currently happen since we never delete the root node.)
         */
        if (result.parent == null) {
            return;
        }

        final IN parent = result.parent;
        final int index = result.index;
        final int parentLevel = parent.getLevel();
        final CheckpointReference parentRef;

        /* List of siblings to log after releasing the parent latch. */
        final List<CheckpointReference> logSiblingsSeparately;

        try {
            /*
             * If bottomLevelTarget is true, the parent IN contains bottom
             * level BINs.  The masking is used to normalize the level for
             * ordinary DBs and the mapping tree DB.
             */
            final boolean bottomLevelTarget =
                ((parentLevel & IN.LEVEL_MASK) == 2);

            /*
             * INs at the max flush level are always non-provisional and
             * INs at the bottom level (when this is not also the max flush
             * level) are always provisional.  In between INs are
             * provisional BEFORE_CKPT_END (see Provisional).
             */
            final Provisional provisional;
            if (targetLevel >= maxFlushLevel) {
                provisional = Provisional.NO;
            } else if (bottomLevelTarget) {
                provisional = Provisional.YES;
            } else {
                provisional = Provisional.BEFORE_CKPT_END;
            }

            /*
             * If we didn't reach the target level, a child wasn't resident
             * and there is nothing to log at this level. To be on the safe
             * side, we'll put the parent into the dirty set to be logged when
             * that level is processed.
             *
             * Only do this if the parent we found is at a higher level than
             * the child.  This ensures that the non-exact search does not
             * find a sibling rather than a parent. [#11555]
             */
            if (!result.exactParentFound) {
                if (parentLevel > targetLevel) {
                    dirtyMap.addIN(
                        parent, -1 /*index*/,
                        false /*updateFlushLevels*/,
                        true /*updateMemoryBudget*/);
                }
                return;
            }

            /*
             * We found the parent. Add it unconditionally to the dirty map. We
             * must make sure that every IN that was selected for the
             * checkpointer's dirty IN set at the beginning of checkpoint is
             * written into the log and can be properly accessed from
             * ancestors. Eviction or a split may have written out a member of
             * this dirty set before the checkpointer got to it. See [#10249].
             */
            assert parentLevel == targetLevel + 1;

            dirtyMap.addIN(
                parent, -1 /*index*/,
                false /*updateFlushLevels*/,
                true /*updateMemoryBudget*/);

            /*
             * Determine whether our search found the IN identified by either
             * targetRef.nodeId or targetRef.lsn. If there is not a match, then
             * the node was deleted, logged or split since creating the
             * reference.
             *
             * For a non-DW DB, targetRef.lsn will be not null and we match on
             * it. If the LSN has changed then of course the node was logged,
             * and possibly split, and we will not log this target here.
             *
             * For a DW DB we also match on LSN if it is non-null. If the LSN
             * is null then the reference was created for a never-logged IN and
             * targetRef.nodeId >= 0. In that case we match on the nodeId. If
             * the LSN or nodeId doesn't match, there must have been a split,
             * and we will not log this target here. However, because splits
             * are not logged for DW, this is not sufficient to cause both
             * siblings that were part of split to be logged, when one node was
             * added to the dirty map. We account for this when the parent is
             * logged by calling logDirtyChildren. This approach relies on the
             * fact that a split will dirty the parent.
             *
             * TODO:
             * Why not always call logDirtyIN for a DW IN, whether or not the
             * LSN or nodeId matches? logDirtyChildren is going to log it
             * anyway if it is dirty.
             */
            if (targetRef.lsn != DbLsn.NULL_LSN) {

                if (targetRef.lsn != parent.getLsn(index)) {
                    return;
                }
            } else {
                assert targetRef.nodeId >= 0;
                assert db.isDeferredWriteMode();

                final IN target = (IN) parent.getTarget(index);

                if (target == null ||
                    targetRef.nodeId != target.getNodeId()) {
                    return;
                }
            }

            /* Log the target, if dirty. */
            logDirtyIN(envImpl, parent, index, provisional, fstats);

            /*
             * We will log a sub-tree when the target is at the bottom level
             * and this is not a recursive call to flushIN during sub-tree
             * logging. Return if we are only logging the target node here.
             */
            if (!bottomLevelTarget || !allowLogSubtree) {
                return;
            }

            /*
             * Log sub-tree siblings with the latch held when highPriority
             * is configured and this is not a DW DB. For a DW DB, dirty LNs
             * are logged for each BIN.  If we were to log a DW sub-tree with
             * the parent latch held, the amount of logging may cause the latch
             * to be held for too long a period.
             */
            if (highPriority && !db.isDurableDeferredWrite()) {
                logSiblingsSeparately = null;
            } else {
                logSiblingsSeparately = new ArrayList<>();
            }

            for (int i = 0; i < parent.getNEntries(); i += 1) {

                if (i == index) {
                    continue;
                }

                final IN child = (IN) parent.getTarget(i);
                final long childId = (child != null) ? child.getNodeId() : -1;
                final long childLsn = parent.getLsn(i);

                final CheckpointReference childRef =
                    dirtyMap.removeNode(targetLevel, childLsn, childId);

                if (childRef == null) {
                    continue;
                }

                if (logSiblingsSeparately != null) {
                    logSiblingsSeparately.add(childRef);
                } else {
                    logDirtyIN(envImpl, parent, i, provisional, fstats);
                }
            }

            /* Get parentRef before releasing the latch. */
            if (parentLevel <= maxFlushLevel) {
                parentRef = dirtyMap.removeNode(
                    parentLevel, parent.getLastLoggedLsn(),
                    parent.getNodeId());
            } else {
                parentRef = null;
            }
        } finally {
            parent.releaseLatch();
        }

        /*
         * If highPriority is false, we don't hold the latch while logging
         * the bottom level siblings.  We log them here with flushIN,
         * performing a separate search for each one, after releasing the
         * parent latch above.
         */
        if (logSiblingsSeparately != null) {
            for (final CheckpointReference childRef : logSiblingsSeparately) {
                flushIN(
                    db, childRef, dirtyMap, maxFlushLevel, highPriority,
                    fstats, false /*allowLogSubtree*/);
            }
        }

        /*
         * Log the sub-tree parent, which will be logged non-provisionally,
         * in order to update cleaner utilization. This must be done with
         * flushIN after releasing the parent latch above, since we must search
         * and acquire the grandparent latch.
         */
        if (parentRef != null) {
            flushIN(
                db, parentRef, dirtyMap, maxFlushLevel, highPriority, fstats,
                false /*allowLogSubtree*/);
        }
    }

    /**
     * Note that if this method is called, the parent must also be logged. This
     * is true even if this method finds that the child is not dirty. In that
     * case the child has already been flushed (e.g., by eviction) and the
     * parent must be logged according to the rule for max flush level.
     */
    private static void logDirtyIN(
        final EnvironmentImpl envImpl,
        final IN parent,
        final int index,
        final Provisional provisional,
        final FlushStats fstats) {

        final IN child = (IN) parent.getTarget(index);
        final long newLsn;
        final boolean isBIN;
        final boolean isDelta;

        if (child != null) {
            child.latch(CacheMode.UNCHANGED);
            try {
                if (!child.getDirty()) {
                    return;
                }

                if (child.getDatabase().isDurableDeferredWrite()) {

                    /*
                     * Find dirty descendants to avoid logging nodes with
                     * never-logged children. See [#13936] and
                     * IN.logDirtyChildren for description of the case.
                     *
                     * Note that we must log both dirty and never-logged
                     * descendants to be sure to have a consistent view of
                     * the split. If we didn't, we could end up with the
                     * post-split version of a new sibling and the
                     * pre-split version of an split sibling in the log,
                     * which could result in a recovery where descendants
                     * are incorrectly duplicated, because they are in both
                     * the pre-split split sibling, and the post-split
                     * version of the new sibling.
                     */
                    child.logDirtyChildren();
                }

                newLsn = child.log(
                    true /*allowDeltas*/, provisional,
                    true /*backgroundIO*/, parent);

                assert (newLsn != DbLsn.NULL_LSN);

                isBIN = child.isBIN();
                isDelta = (newLsn == child.getLastDeltaLsn());
            } finally {
                child.releaseLatch();
            }
        } else {
            final OffHeapCache ohCache = envImpl.getOffHeapCache();

            final INLogEntry<BIN> logEntry =
                ohCache.createBINLogEntryForCheckpoint(parent, index);

            if (logEntry == null) {
                return;
            }

            isBIN = true;
            isDelta = logEntry.isBINDelta();

            newLsn = IN.logEntry(
                logEntry, provisional, true /*backgroundIO*/, parent);

            ohCache.postBINLog(parent, index, logEntry, newLsn);
        }

        parent.updateEntry(index, newLsn, VLSN.NULL_VLSN_SEQUENCE, 0);

        if (isDelta) {
            fstats.nDeltaINFlushThisRun++;
            fstats.nDeltaINFlush++;
        } else {
            fstats.nFullINFlushThisRun++;
            fstats.nFullINFlush++;
            if (isBIN) {
                fstats.nFullBINFlush++;
                fstats.nFullBINFlushThisRun++;
            }
        }
    }

    /*
     * RootFlusher lets us write out the root IN within the root latch.
     */
    private static class RootFlusher implements WithRootLatched {
        private final DatabaseImpl db;
        private boolean flushed;
        private boolean stillRoot;
        private final long targetNodeId;

        RootFlusher(final DatabaseImpl db,
                    final long targetNodeId) {
            this.db = db;
            flushed = false;
            this.targetNodeId = targetNodeId;
            stillRoot = false;
        }

        /**
         * Flush the rootIN if dirty.
         */
        @Override
        public IN doWork(ChildReference root) {

            if (root == null) {
                return null;
            }

            IN rootIN = (IN) root.fetchTarget(db, null);
            rootIN.latch(CacheMode.UNCHANGED);
            try {
                if (rootIN.getNodeId() == targetNodeId) {

                    /*
                     * Find dirty descendants to avoid logging nodes with
                     * never-logged children. See [#13936]
                     */
                    if (rootIN.getDatabase().isDurableDeferredWrite()) {
                        rootIN.logDirtyChildren();
                    }

                    /*
                     * stillRoot handles the situation where the root was split
                     * after it was placed in the checkpointer's dirty set.
                     */
                    stillRoot = true;

                    if (rootIN.getDirty()) {
                        long newLsn = rootIN.log();
                        root.setLsn(newLsn);
                        flushed = true;
                    }
                }
            } finally {
                rootIN.releaseLatch();
            }
            return null;
        }

        boolean getFlushed() {
            return flushed;
        }

        boolean stillRoot() {
            return stillRoot;
        }
    }

    /*
     * CheckpointReferences are used to identify nodes that must be flushed as
     * part of the checkpoint. We don't keep an actual reference to the node
     * because that prevents nodes from being GC'ed during checkpoint.
     *
     * Using a checkpointReference introduces a window between the point when
     * the checkpoint dirty set is created and when the node is flushed. Some
     * of the fields saved in the reference are immutable: db, nodeId.  The
     * others are not and we have to handle potential change:
     *
     * isRoot: it's possible for isRoot to go from true->false, but not
     *         false->true. True->false is handled by the flushIN method
     *         by finding the root and checking if it is the target.
     * treeKey: This can change only in the event of a split. If it does, there
     *         is the chance that the checkpointer will find the wrong node to
     *         flush, but that's okay because the split guarantees flushing to
     *         the root, so the target will be properly logged within the
     *         checkpoint period.
     *
     * The class and ctor are public for the Sizeof program.
     */
    public static class CheckpointReference {
        final DatabaseId dbId;
        final long nodeId;
        final int nodeLevel;
        final boolean isRoot;
        final byte[] treeKey;
        final long lsn;

        CheckpointReference(final DatabaseId dbId,
                            final long nodeId,
                            final int nodeLevel,
                            final boolean isRoot,
                            final byte[] treeKey,
                            final long lsn) {
            this.dbId = dbId;
            this.nodeId = nodeId;
            this.nodeLevel = nodeLevel;
            this.isRoot = isRoot;
            this.treeKey = treeKey;
            this.lsn = lsn;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CheckpointReference)) {
                return false;
            }

            CheckpointReference other = (CheckpointReference) o;
            return nodeId == other.nodeId;
        }

        @Override
        public int hashCode() {
            return (int) nodeId;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("db=").append(dbId);
            sb.append(" nodeId=").append(nodeId);
            return sb.toString();
        }
    }

    /**
     * A struct to hold log flushing stats for checkpoint and database sync.
     */
    public static class FlushStats {

        public long nFullINFlush;
        public long nFullBINFlush;
        public long nDeltaINFlush;
        long nFullINFlushThisRun;
        long nFullBINFlushThisRun;
        long nDeltaINFlushThisRun;

        /* For future addition to stats:
           private int nAlreadyEvictedThisRun;
        */

        /* Reset per-run counters. */
        void resetPerRunCounters() {
            nFullINFlushThisRun = 0;
            nFullBINFlushThisRun = 0;
            nDeltaINFlushThisRun = 0;
            /* nAlreadyEvictedThisRun = 0; -- for future */
        }
    }
}
