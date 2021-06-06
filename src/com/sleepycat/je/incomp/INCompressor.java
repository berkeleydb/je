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

package com.sleepycat.je.incomp;

import static com.sleepycat.je.incomp.INCompStatDefinition.GROUP_DESC;
import static com.sleepycat.je.incomp.INCompStatDefinition.GROUP_NAME;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_CURSORS_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_DBCLOSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_NON_EMPTY_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_PROCESSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_QUEUE_SIZE;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_SPLIT_BINS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.tree.CursorsExistException;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.NodeNotEmptyException;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * JE compression consists of removing BIN slots for deleted and expired
 * records, and pruning empty IN/BINs from the tree which is also called a
 * reverse split.
 *
 * One of the reasons compression is treated specially is that slot compression
 * cannot be performed inline as part of a delete operation.  When we delete an
 * LN, a cursor is always present on the LN.  The API dictates that the cursor
 * will remain positioned on the deleted record.  In addition, if the deleting
 * transaction aborts we must restore the slot and the possibility of a split
 * during an abort is something we wish to avoid; for this reason, compression
 * will not occur if the slot's LSN is locked.  In principle, slot compression
 * could be performed during transaction commit, but that would be expensive
 * because a Btree lookup would be required, and this would negatively impact
 * operation latency.  For all these reasons, slot compression is performed
 * after the delete operation is complete and committed, and not in the thread
 * performing the operation or transaction commit.
 *
 * Compression is of two types:
 *
 * + "Queued compression" is carried out by the INCompressor daemon thread.
 *    Both slot compression and pruning are performed.
 *
 * + "Lazy compression" is carried out opportunistically at various times when
 *    compression is beneficial.
 *
 * The use of BIN-deltas has a big impact on slot compression because dirty
 * slots cannot be compressed until we know that a full BIN will be logged
 * next. If a dirty slot were compressed prior to logging a BIN-delta, the
 * record of the compression would be lost and the slot would "reappear" when
 * the BIN is reconstituted. Normally we do not compress dirty slots when a
 * delta would next be logged. However, there are times when we do compress
 * dirty slots, and in that case the "prohibit next logged delta" flag is set
 * on the BIN.
 *
 * Queued compression prior to logging a BIN-delta is also wasteful because the
 * dequeued entry cannot be processed.  Therefore, lazy compression is relied
 * on when a BIN-delta will next be logged.  Because, BIN-deltas are logged
 * more often than BINs, lazy compression is used for slot compression more
 * often than queued compression.
 *
 * Lazy compression is used for compressing expired slots, in addition to
 * deleted slots. This is done opportunistically as described above. Expired
 * slots are normally not dirty, so they can often be compressed even when a
 * BIN will be logged next as a BIN-delta. The same is true of deleted slots
 * that are not dirty, although these occur infrequently.
 *
 * Since we don't lazy-compress BIN-deltas, how can we prevent their expired
 * slots from using space for long time periods? Currently the expired space
 * will be reclaimed only when the delta is mutated to a full BIN and then
 * compressed, including when the full BIN is cleaned. This is the same as
 * reclaiming space for deleted slots, so this is acceptable for now at least.
 *
 * You may wonder, since lazy compression is necessary, why use queued
 * compression for slot compression at all? Queued compression is useful for
 * the following reasons:
 *
 * + If a BIN-delta will not be logged next, queued compression will cause the
 *   compression to occur sooner than with lazy compression.
 *
 * + When a cursor is on a BIN or a deleted entry is locked during lazy
 *   compression, we cannot compress the slot. Queuing allows it to be
 *   compressed sooner than if we waited for the next lazy compression.
 *
 * + The code to process a queue entry must do slot compression anyway, even if
 *   we only want to prune the BIN.  We have to account for the case where all
 *   slots are deleted but not yet compressed.  So the code to process the
 *   queue entry could not be simplified even if we were to decide not to queue
 *   entries for slot compression. 
 */
public class INCompressor extends DaemonThread {
    private static final boolean DEBUG = false;

    private final long lockTimeout;

    /* stats */
    private StatGroup stats;
    private LongStat splitBins;
    private LongStat dbClosedBins;
    private LongStat cursorsBins;
    private LongStat nonEmptyBins;
    private LongStat processedBins;
    private LongStat compQueueSize;

    /* per-run stats */
    private int splitBinsThisRun = 0;
    private int dbClosedBinsThisRun = 0;
    private int cursorsBinsThisRun = 0;
    private int nonEmptyBinsThisRun = 0;
    private int processedBinsThisRun = 0;

    /*
     * The following stats are not kept per run, because they're set by
     * multiple threads doing lazy compression. They are debugging aids; it
     * didn't seem like a good idea to add synchronization to the general path.
     */
    private int lazyProcessed = 0;
    private int wokenUp = 0;

    /*
     * Store logical references to BINs that have deleted entries and are
     * candidates for compaction.
     */
    private Map<Long, BINReference> binRefQueue;
    private final Object binRefQueueSync;

    /* For unit tests */
    private TestHook beforeFlushTrackerHook; // [#15528]

    public INCompressor(EnvironmentImpl env, long waitTime, String name) {
        super(waitTime, name, env);
        lockTimeout = env.getConfigManager().getDuration
            (EnvironmentParams.COMPRESSOR_LOCK_TIMEOUT);
        binRefQueue = new HashMap<>();
        binRefQueueSync = new Object();
 
        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        splitBins = new LongStat(stats, INCOMP_SPLIT_BINS);
        dbClosedBins = new LongStat(stats, INCOMP_DBCLOSED_BINS);
        cursorsBins = new LongStat(stats, INCOMP_CURSORS_BINS);
        nonEmptyBins = new LongStat(stats, INCOMP_NON_EMPTY_BINS);
        processedBins = new LongStat(stats, INCOMP_PROCESSED_BINS);
        compQueueSize = new LongStat(stats, INCOMP_QUEUE_SIZE);
    }

    /* For unit testing only. */
    public void setBeforeFlushTrackerHook(TestHook hook) {
        beforeFlushTrackerHook = hook;
    }

    public synchronized void verifyCursors()
        throws DatabaseException {

        /*
         * Environment may have been closed.  If so, then our job here is done.
         */
        if (envImpl.isClosed()) {
            return;
        }

        /*
         * Use a snapshot to verify the cursors.  This way we don't have to
         * hold a latch while verify takes locks.
         */
        final List<BINReference> queueSnapshot;
        synchronized (binRefQueueSync) {
            queueSnapshot = new ArrayList<>(binRefQueue.values());
        }

        /*
         * Use local caching to reduce DbTree.getDb overhead.  Do not call
         * releaseDb after each getDb, since the entire dbCache will be
         * released at the end.
         */
        final DbTree dbTree = envImpl.getDbTree();
        final Map<DatabaseId, DatabaseImpl> dbCache = new HashMap<>();

        try {
            for (final BINReference binRef : queueSnapshot) {
                final DatabaseImpl db = dbTree.getDb(
                    binRef.getDatabaseId(), lockTimeout, dbCache);

                final BIN bin = searchForBIN(db, binRef);
                if (bin != null) {
                    bin.verifyCursors();
                    bin.releaseLatch();
                }
            }
        } finally {
            dbTree.releaseDbs(dbCache);
        }
    }

    public int getBinRefQueueSize() {
        synchronized (binRefQueueSync) {
            return binRefQueue.size();
        }
    }

    /*
     * There are multiple flavors of the addBin*ToQueue methods. All allow
     * the caller to specify whether the daemon should be notified. Currently
     * no callers proactively notify, and we rely on lazy compression and
     * the daemon timebased wakeup to process the queue.
     */

    /**
     * Adds the BIN to the queue if the BIN is not already in the queue.
     */
    public void addBinToQueue(BIN bin) {
        synchronized (binRefQueueSync) {
            addBinToQueueAlreadyLatched(bin);
        }
    }

    /**
     * Adds the BINReference to the queue if the BIN is not already in the
     * queue.
     */
    private void addBinRefToQueue(BINReference binRef) {
        synchronized (binRefQueueSync) {
            addBinRefToQueueAlreadyLatched(binRef);
        }
    }

    /**
     * Adds an entire collection of BINReferences to the queue at once.  Use
     * this to avoid latching for each add.
     */
    public void addMultipleBinRefsToQueue(Collection<BINReference> binRefs) {
        synchronized (binRefQueueSync) {
            for (final BINReference binRef : binRefs) {
                addBinRefToQueueAlreadyLatched(binRef);
            }
        }
    }

    /**
     * Adds the BINReference with the latch held.
     */
    private void addBinRefToQueueAlreadyLatched(BINReference binRef) {

        final Long node = binRef.getNodeId();

        if (binRefQueue.containsKey(node)) {
            return;
        }

        binRefQueue.put(node, binRef);
    }

    /**
     * Adds the BIN with the latch held.
     */
    private void addBinToQueueAlreadyLatched(BIN bin) {

        final Long node = bin.getNodeId();

        if (binRefQueue.containsKey(node)) {
            return;
        }

        binRefQueue.put(node, bin.createReference());
    }

    public boolean exists(long nodeId) {
        synchronized (binRefQueueSync) {
            return binRefQueue.containsKey(nodeId);
        }
    }

    /**
     * Return stats
     */
    public StatGroup loadStats(StatsConfig config) {
        compQueueSize.set((long) getBinRefQueueSize());

        if (DEBUG) {
            System.out.println("lazyProcessed = " + lazyProcessed);
            System.out.println("wokenUp=" + wokenUp);
        }

        if (config.getClear()) {
            lazyProcessed = 0;
            wokenUp = 0;
        }

        return stats.cloneGroup(config.getClear());
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    @Override
    protected long nDeadlockRetries() {
        return envImpl.getConfigManager().getInt
            (EnvironmentParams.COMPRESSOR_RETRY);
    }

    @Override
    public synchronized void onWakeup()
        throws DatabaseException {

        if (envImpl.isClosing()) {
            return;
        }
        wokenUp++;
        doCompress();
    }

    /**
     * The real work to doing a compress. This may be called by the compressor
     * thread or programatically.
     */
    public synchronized void doCompress()
        throws DatabaseException {

        /*
         * Make a snapshot of the current work queue so the compressor thread
         * can safely iterate over the queue. Note that this impacts lazy
         * compression, because it lazy compressors will not see BINReferences
         * that have been moved to the snapshot.
         */
        final Map<Long, BINReference> queueSnapshot;
        final int binQueueSize;
        synchronized (binRefQueueSync) {
            binQueueSize = binRefQueue.size();
            if (binQueueSize <= 0) {
                return;
            }
            queueSnapshot = binRefQueue;
            binRefQueue = new HashMap<>();
        }

        /* There is work to be done. */
        resetPerRunCounters();
        LoggerUtils.fine(logger, envImpl,
                         "InCompress.doCompress called, queue size: " +
                         binQueueSize);
        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.expectBtreeLatchesHeld(0);
        }

        /*
         * Compressed entries must be counted as obsoleted.  A separate
         * tracker is used to accumulate tracked obsolete info so it can be
         * added in a single call under the log write latch.  We log the
         * info for deleted subtrees immediately because we don't process
         * deleted IN entries during recovery; this reduces the chance of
         * lost info.
         */
        final LocalUtilizationTracker localTracker =
            new LocalUtilizationTracker(envImpl);

        /* Use local caching to reduce DbTree.getDb overhead. */
        final Map<DatabaseId, DatabaseImpl> dbCache = new HashMap<>();

        final DbTree dbTree = envImpl.getDbTree();
        final BINSearch binSearch = new BINSearch();

        try {
            for (final BINReference binRef : queueSnapshot.values()) {

                if (envImpl.isClosed()) {
                    return;
                }

                if (!findDBAndBIN(binSearch, binRef, dbTree, dbCache)) {

                    /*
                     * Either the db is closed, or the BIN doesn't exist.
                     * Don't process this BINReference.
                     */
                    continue;
                }

                /* Compress deleted slots and prune if possible. */
                compressBin(binSearch.db, binSearch.bin, binRef, localTracker);
            }

            /* SR [#11144]*/
            assert TestHookExecute.doHookIfSet(beforeFlushTrackerHook);

            /*
             * Count obsolete nodes and write out modified file summaries
             * for recovery.  All latches must have been released.
             */
            envImpl.getUtilizationProfile().flushLocalTracker(localTracker);

        } finally {
            dbTree.releaseDbs(dbCache);
            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.expectBtreeLatchesHeld(0);
            }
            accumulatePerRunCounters();
        }
    }

    /**
     * Compresses a single BIN and then deletes the BIN if it is empty.
     *
     * @param bin is latched when this method is called, and unlatched when it
     * returns.
     */
    private void compressBin(
        DatabaseImpl db,
        BIN bin,
        BINReference binRef,
        LocalUtilizationTracker localTracker) {

        /* Safe to get identifier keys; bin is latched. */
        final byte[] idKey = bin.getIdentifierKey();
        boolean empty = (bin.getNEntries() == 0);

        try {
            if (!empty) {

                /*
                 * Deltas in cache cannot be compressed.
                 *
                 * We strive not to add a slot to the queue when we will log a
                 * delta.  However, it is possible that an entry is added, or
                 * that an entry is not cleared by lazy compression prior to
                 * logging a full BIN.  Clean-up for such queue entries is
                 * here.
                */
                if (bin.isBINDelta()) {
                    return;
                }

                /* If there are cursors on the BIN, requeue and try later. */
                if (bin.nCursors() > 0) {
                    addBinRefToQueue(binRef);
                    cursorsBinsThisRun++;
                    return;
                }

                /*
                 * If a delta should be logged, do not compress dirty slots,
                 * since this would prevent logging a delta.
                 */
                if (!bin.compress(
                    !bin.shouldLogDelta() /*compressDirtySlots*/,
                    localTracker)) {

                    /* If compression is incomplete, requeue and try later. */
                    addBinRefToQueue(binRef);
                    return;
                }

                /* After compression the BIN may be empty. */
                empty = (bin.getNEntries() == 0);
            }
        } finally {
            bin.releaseLatch();
        }

        /* After releasing the latch, prune the BIN if it is empty. */
        if (empty) {
            pruneBIN(db, binRef, idKey);
        }
    }

    /**
     * If the target BIN is empty, attempt to remove the empty branch of the
     * tree.
     */
    private void pruneBIN(DatabaseImpl dbImpl,
                          BINReference binRef,
                          byte[] idKey) {

        try {
            final Tree tree = dbImpl.getTree();
            tree.delete(idKey);
            processedBinsThisRun++;
        } catch (NodeNotEmptyException NNEE) {

            /*
             * Something was added to the node since the point when the
             * deletion occurred; we can't prune, and we can throw away this
             * BINReference.
             */
             nonEmptyBinsThisRun++;
        } catch (CursorsExistException e) {
            /* If there are cursors in the way of the delete, retry later. */
            addBinRefToQueue(binRef);
            cursorsBinsThisRun++;
        }
    }

    /**
     * Search the tree for the BIN that corresponds to this BINReference.
     *
     * @param binRef the BINReference that indicates the bin we want.
     *
     * @return the BIN that corresponds to this BINReference. The
     * node is latched upon return. Returns null if the BIN can't be found.
     */
    public BIN searchForBIN(DatabaseImpl db, BINReference binRef) {
        return db.getTree().search(binRef.getKey(), CacheMode.UNCHANGED);
    }

    /**
     * Reset per-run counters.
     */
    private void resetPerRunCounters() {
        splitBinsThisRun = 0;
        dbClosedBinsThisRun = 0;
        cursorsBinsThisRun = 0;
        nonEmptyBinsThisRun = 0;
        processedBinsThisRun = 0;
    }

    private void accumulatePerRunCounters() {
        splitBins.add(splitBinsThisRun);
        dbClosedBins.add(dbClosedBinsThisRun);
        cursorsBins.add(cursorsBinsThisRun);
        nonEmptyBins.add(nonEmptyBinsThisRun);
        processedBins.add(processedBinsThisRun);
    }

    /**
     * Lazily/opportunistically compress a full BIN.
     *
     * The target IN should be latched when we enter, and it will be remain
     * latched.
     *
     * If compression succeeds, does not prune empty BINs, but does queue them
     * for pruning later. If compression fails because a record lock cannot be
     * obtained, queues the BIN to retry later.
     *
     * Note that we do not bother to delete queue entries for the BIN if
     * compression succeeds.  Queue entries are normally removed quickly by the
     * compressor.  In the case where queue entries happen to exist when we do
     * the final compression below, we rely on the compressor to clean them up
     * later on when they are processed.
     */
    public void lazyCompress(final IN in, final boolean compressDirtySlots) {

        assert in.isLatchOwner();

        /* Only full BINs can be compressed. */
        if (!in.isBIN() || in.isBINDelta()) {
            return;
        }

        final BIN bin = (BIN) in;

        /*
         * Cursors prohibit compression. We queue for later when there is
         * anything that can be compressed.
         */
        if (bin.nCursors() > 0) {
            for (int i = 0; i < bin.getNEntries(); i += 1) {
                if (bin.isDefunct(i)) {
                    addBinToQueue(bin);
                    break;
                }
            }
            return;
        }

        if (bin.compress(compressDirtySlots, null /*localTracker*/)) {
            if (bin.getNEntries() == 0) {
                /* The BIN is empty. Prune it later. */
                addBinToQueue(bin);
            }
        } else {
            /* A record lock prevented slot removal. Try again later. */
            addBinToQueue(bin);
        }

        lazyProcessed++;
    }

    /*
     * Find the db and bin for a BINReference.
     * @return true if the db is open and the target bin is found.
     */
    private boolean findDBAndBIN(
        BINSearch binSearch,
        BINReference binRef,
        DbTree dbTree,
        Map<DatabaseId, DatabaseImpl> dbCache)
        throws DatabaseException {

        /*
         * Find the database.  Do not call releaseDb after this getDb, since
         * the entire dbCache will be released later.
         */
        binSearch.db = dbTree.getDb(
            binRef.getDatabaseId(), lockTimeout, dbCache);

        if (binSearch.db == null || binSearch.db.isDeleted()) {
            /* The db was deleted. Ignore this BIN Ref. */
            dbClosedBinsThisRun++;
            return false;
        }

        /* Perform eviction before each operation. */
        envImpl.daemonEviction(true /*backgroundIO*/);

        /* Find the BIN. */
        binSearch.bin = searchForBIN(binSearch.db, binRef);

        if (binSearch.bin == null ||
            binSearch.bin.getNodeId() != binRef.getNodeId()) {
            /* The BIN may have been split. */
            if (binSearch.bin != null) {
                binSearch.bin.releaseLatch();
            }
            splitBinsThisRun++;
            return false;
        }

        return true;
    }

    /* Struct to return multiple values from findDBAndBIN. */
    private static class BINSearch {
        public DatabaseImpl db;
        public BIN bin;
    }
}
