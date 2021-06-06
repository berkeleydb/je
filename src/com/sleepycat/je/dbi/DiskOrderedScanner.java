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

package com.sleepycat.je.dbi;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.cleaner.DbFileSummary;
import com.sleepycat.je.cleaner.FileProtector;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.OldBINDelta;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Provides an enumeration of all key/data pairs in a database, striving to
 * fetch in disk order.
 *
 * Unlike SortedLSNTreeWalker, for which the primary use case is preload, this
 * class notifies the callback while holding a latch only if that can be done
 * without blocking (e.g., when the callback can buffer the data without
 * blocking).  In other words, while holding a latch, the callback will not be
 * notified if it might block.  This is appropriate for the DOS
 * (DiskOrderedCursor) use case, since the callback will block if the DOS queue
 * is full, and the user's consumer thread may not empty the queue as quickly
 * as it can be filled by the producer thread.  If the callback were allowed to
 * block while a latch is held, this would block other threads attempting to
 * access the database, including JE internal threads, which would have a very
 * detrimental impact.
 *
 * Algorithm
 * =========
 *
 * Terminology
 * -----------
 * callback: object implementing the RecordProcessor interface
 * process: invoking the callback with a key-data pair
 * iteration: top level iteration consisting of phase I and II
 * phase I: accumulate LSNs
 * phase II: sort, fetch and process LSNs
 *
 * Phase I and II
 * --------------
 * To avoid processing resident nodes (invoking the callback with a latch
 * held), a non-recursive algorithm is used.  Instead of recursively
 * accumulating LSNs in a depth-first iteration of the tree (like the
 * SortedLSNTreeWalker algorithm), level 2 INs are traversed in phase I and
 * LSNs are accumulated for LNs or BINs (more on this below).  When the memory
 * or LSN batch size limit is exceeded, phase I ends and all tree latches are
 * released.  During phase II the previously accumulated LSNs are fetched and
 * the callback is invoked for each key or key-data pair.  Since no latches are
 * held, it is permissible for the callback to block.
 *
 * One iteration of phase I and II processes some subset of the database.
 * Since INs are traversed in tree order in phase I, this subset is described
 * by a range of keys.  When performing the next iteration, the IN traversal is
 * restarted at the highest key that was processed by the previous iteration.
 * The previous highest key is used to avoid duplication of entries, since some
 * overlap between iterations may occur.
 *
 * LN and BIN modes
 * ----------------
 * As mentioned above, we accumulate LSNs for either LNs or BINs.  The BIN
 * accumulation mode provides an optimization for key-only traversals and for
 * all traversals of duplicate DBs (in a dup DB, the data is included in the
 * key).  In these cases we never need to fetch the LN, so we can sort and
 * fetch the BIN LSNs instead.  This supports at least some types of traversals
 * that are efficient when all BINs are not in the JE cache.
 *
 * We must only accumulate LN or BIN LSNs, never both, and never the LSNs of
 * other INs (above level 1).  If we broke this rule, there would be no way to
 * constrain memory usage in our non-recursive approach, since we could not
 * easily predict in advance how much memory would be needed to fetch the
 * nested nodes.  Even if we were able predict the memory needed, it would be
 * of little advantage to sort and fetch a small number of higher level nodes,
 * only to accumulate the LSNs of their descendants (which are much larger in
 * number).  The much smaller number of higher level nodes would likely be
 * fetched via random IO anyway, in a large data set anyway.
 *
 * The above justification also applies to the algorithm we use in LN mode, in
 * which we accumulate and fetch only LN LSNs.  In this mode we always fetch
 * BINs explicitly (not in LSN sorted order), if they are not resident, for the
 * reasons stated above.
 *
 * Furthermore, in BIN mode we must account for BIN-deltas.  Phase I must keep
 * a copy any BIN-deltas encountered in the cache.  And phase II must make two
 * passes for the accumulated LSNs: one pass to load the deltas and another to
 * load the full BINs and merge the previously loaded deltas.  Unfortunately
 * we must budget memory for the deltas during phase I; since most BIN LSNs are
 * for deltas, not full BINs, we assume that we will need to temporarily save a
 * delta for each LSN.  This two pass approach is not like the recursive
 * algorithm we rejected above, however, in two respects: 1) we know in advance
 * (roughly anyway) how much memory we will need for both passes, and 2) the
 * number of LSNs fetched in each pass is roughly the same.
 *
 * Data Lag
 * --------
 * In phase I, as an exception to what was said above, we sometimes process
 * nodes that are resident in the Btree (in the JE cache) if this is possible
 * without blocking.  The primary intention of this is to provide more recent
 * data to the callback.  When accumulating BINs, if the BIN is dirty then
 * fetching its LSN later means that some recently written LNs will not be
 * included.  Therefore, if the callback would not block, we process the keys
 * in a dirty BIN during phase I.  Likewise, when accumulating LNs in a
 * deferred-write database, we process dirty LNs if the callback would not
 * block.  When accumulating LN LSNs for a non-deferred-write database, we can
 * go further and process all resident LNs, as long as the callback would not
 * block, since we know that no LNs are dirty.
 *
 * In spite of our attempt to process resident nodes, we may not be able to
 * process all of them if doing so would cause the callback to block.  When we
 * can't process a dirty, resident node, the information added (new, deleted or
 * updated records) since the node was last flushed will not be visible to the
 * callback.
 *
 * In other words, the data presented to the callback may lag back to the time
 * of the last checkpoint.  It cannot lag further back than the last
 * checkpoint, because: 1) the scan doesn't accumulate LSNs any higher than the
 * BIN level, and 2) checkpoints flush all dirty BINs.  For a DOS, the user may
 * decrease the likelihood of stale data by increasing the DOS queue size,
 * decreasing the LSN batch size, decreasing the memory limit, or performing a
 * checkpoint immediately before the start of the scan.  Even so, it may be
 * impossible to guarantee that all records written at the start of the scan
 * are visible to the callback.
 */
public class DiskOrderedScanner {

    /**
     * Interface implemented by the callback.
     */
    interface RecordProcessor {

        /**
         * Process a key-data pair, in user format (dup DB keys are already
         * split into key and data).
         *
         * @param key always non-null.
         * @param data is null only in keys-only mode.
         */
        void process(
            int dbIdx,
            byte[] key,
            byte[] data,
            int expiration,
            boolean expirationInHours);

        /**
         * Returns whether process() can be called nRecords times, immediately
         * after calling this method, without any possibility of blocking.
         * For example, with DOS this method returns true if the DOS queue has
         * nRecords or more remaining capacity.
         */
        boolean canProcessWithoutBlocking(int nRecords);

        int getCapacity();

        void checkShutdown();
    }

    /*
     *
     */
    private static class DBContext {

        final int dbIdx;

        final DatabaseImpl dbImpl;

        final Map<Long, DbFileSummary> dbFileSummaries;

        boolean done = false;

        byte[] prevEndingKey = null;
        byte[] newEndingKey = null;

        long lastBinLsn = DbLsn.NULL_LSN;
        boolean safeToUseCachedDelta = false;

        IN parent = null;
        boolean parentIsLatched;
        int pidx = 0;
        long plsn;
        byte[] pkey;

        boolean checkLevel2Keys = true;

        byte[] binKey;
        boolean reuseBin = false;

        DBContext(int dbIdx, DatabaseImpl db) {
            this.dbIdx = dbIdx;
            dbImpl = db;
            dbFileSummaries = dbImpl.cloneDbFileSummaries();
        }
    }

    /*
     * WeakBinRefs are used to reduce the memory consumption for BIN deltas
     * that are found in the je cache during phase 1. In an older implementation
     * of DOS all such deltas were copied locally and the copies retained
     * until phase 2b, when they would be merged with their associated full
     * bins. Obviously this could consume a lot of memory.
     *
     * In the current implementation, dirty deltas are still treated the old
     * way, i.e. copied (see below for a justification). But for clean deltas,
     * a WeakBinRef is created when, during phase 1, such a delta is found in
     * the je cache (the WeakBinRef is created while the delta is latched). At
     * creation time, the WeakBinRef stores (a) a WeakReference to the BIN obj,
     * (b) the lsn found in the parent slot for this bin (the on-disk image
     * pointed to by this lsn must be the same as the latched, in-memory image,
     * given that we use WeakBinRefs for clean deltas only) (c) the lsn of
     * the full bin associated with this bin delta (copied out from the delta
     * itself), and (d) the average size of deltas for the database and log
     * file that the delta belongs to.
     *
     * DiskOrderedScanner.binDeltas is an ArrayList of Object refs, pointing
     * to either WeakBinRefs (for clean deltas) or BINs (for copies of dirty
     * deltas). The list is built during phase 1 and is used during phase 2b
     * to merge the referenced deltas with their associated full BINs. The
     * list is first sorted by full-bin LSN so that the full bins are fetched
     * in disk order.
     *
     * Shortly after a WeakBinRef is created, its associated bin gets unlatched
     * and remains unlatched until the WeakBinRef is used in phase 2b. As a
     * result, anything can happen to the bin in between. The bin may be evicted
     * and then garbage-collected, or it may be converted to a full BIN, split,
     * logged, and mutated to a delta again. If the BIN obj gets GC-ed,
     * this.get() will be null when we process this WeakBinRef in phase 2b.
     * In this case, to avoid doing one or two random I/Os in the middle of
     * sequential I/Os, this.binLsn is saved in a set of "deferred LSNs"
     * (see below) to be processed in a subsequent iteration. If the BIN obj
     * is still accessible, we compare its current full-bin LSN with the one
     * saved in this.fullBinLsn. If they are different, we again add
     * this.binLsn to deferred-LSNs set. Again this is done to avoid
     * disturbing the sequential I/O (we could use the current full-bin LSN to
     * merge the delta with the correct full bin, but this would require a
     * random I/O), but also because the bin could have split, and there is no
     * easy way to find the other bin(s) where slots from this bin were moved
     * to. Note that detecting splits via comparing the 2 full-bin LSNs is a
     * conservative approach that relies on splits being logged immediately. 
     *
     * WeakReferences are used to ref cached clean deltas in order to avoid
     * counting the size of the delta in the DOS budget or having to pin the
     * bin for a long period of time. Of course, if this.binLsn needs to be
     * deferred for the reasons mentioned above, then an average delta size will
     * be added to the memory usage for the DOS as well as the je cache.
     *
     * What about dirty cached deltas? We could be doing the same as for clean
     * deltas. However, for dirty deltas, their unsaved updates could have
     * happened before the start of the DOS and we would loose these updates
     * if instead of processing the original bin delta, we have to fetch
     * this.binLsn from disk. So, we would be violating the DOS contract that
     * says that the returned data are not older than the start of the DOS.  
     */
    public static class WeakBinRef extends WeakReference<BIN> {

        final long binLsn;
        final long fullBinLsn;
        final int memSize;

        /* For Sizeof util program */
        public WeakBinRef() {
            super(null);
            binLsn = 0;
            fullBinLsn = 0;
            memSize = 0;
        }

        WeakBinRef(
            DiskOrderedScanner scanner,
            DBContext ctx,
            long lsn,
            BIN delta) {

            super(delta);
            binLsn = lsn;
            fullBinLsn = delta.getLastFullLsn();

            assert(lsn != delta.getLastFullLsn());

            if (lsn != delta.getLastFullLsn()) {
                memSize = getDeltaMemSize(ctx, lsn);
            } else {
                memSize = 0;
            }
        }
    }

    public static class OffHeapBinRef {

        final int ohBinId;
        final long binLsn;
        final long fullBinLsn;
        final int memSize;

        /* For Sizeof util program */
        public OffHeapBinRef() {
            ohBinId = 0;
            binLsn = 0;
            fullBinLsn = 0;
            memSize = 0;
        }

        OffHeapBinRef(
            DiskOrderedScanner scanner,
            DBContext ctx,
            long binLsn,
            long fullBinLsn,
            int ohBinId) {

            this.ohBinId = ohBinId;
            this.binLsn = binLsn;
            this.fullBinLsn = fullBinLsn;
            memSize = getDeltaMemSize(ctx, binLsn);
        }
    }

    /*
     * A DeferredLsnsBatch obj stores a set of lsns whose processing cannot be
     * done in the current iteration, and as a result, must be deferred until
     * one of the subsequent iterations. 
     *
     * The DeferredLsnsBatch obj also stores the total (approximate) memory
     * that will be consumed when, during a subsequent phase 1a, for each bin
     * pointed-to by these lsns, the bin is fetched from disk and, if it is a
     * delta, stored in memory until it is merged with its associated full bin
     * (which is done in phase 2b).
     *
     * Given that we cannot exceed the DOS memory budget during each iteration,
     * no more LSNs are added to a DeferredLsnsBatch once its memoryUsage gets
     * >= memoryLimit. Instead, a new DeferredLsnsBatch is created and added
     * at the tail of the DiskOrderedScanner.deferredLsns queue. Only the batch
     * at the head of the deferredLsns queue can be processed during each
     * iteration. So, if we have N DeferredLsnsBatches in the queue, the next
     * N iteration will process only deferred lsns (no phase 1 will be done
     * during these N iterations).
     */
    public static class DeferredLsnsBatch {

        /*
         * Fixed overhead for adding an lsn to this batch 
         */
        final static int LSN_MEM_OVERHEAD =
            (MemoryBudget.HASHSET_ENTRY_OVERHEAD +
             MemoryBudget.LONG_OVERHEAD);

        final HashSet<Long> lsns;
        long memoryUsage = 0;

        /* For Sizeof util program */
        public DeferredLsnsBatch() {
            this.lsns = new HashSet<>();
            memoryUsage = 0;
        }

        DeferredLsnsBatch(DiskOrderedScanner scanner) {
            this.lsns = new HashSet<>();
            scanner.addGlobalMemory(SIZEOF_DeferredLsnsBatch);
            memoryUsage += SIZEOF_DeferredLsnsBatch;
        }

        void free(DiskOrderedScanner scanner) {
            scanner.addGlobalMemory(-SIZEOF_DeferredLsnsBatch);
            memoryUsage -= SIZEOF_DeferredLsnsBatch;
            assert(memoryUsage == 0);
        }

        boolean containsLsn(long lsn) {
            return lsns.contains(lsn);
        }

        /*
         * Called during phase 2b, when a WeakBinRef or OffHeapBinRef has to
         * be deferred.
         */
        boolean addLsn(DiskOrderedScanner scanner, long lsn, int memSize) {

            this.lsns.add(lsn);

            int currentMemConsumption = LSN_MEM_OVERHEAD;
            scanner.addGlobalMemory(currentMemConsumption);

            /*
             * ref.memSize is the memory that will be needed during a
             * subsequent phase 2a to store the delta fetched via ref.binLsn,
             * if ref.binLsn is indeed pointing to a delta; 0 if ref.binLsn
             * points to a full bin.
             */
            int futureMemConsumption = memSize;

            /* For storing ref.binLsn in the lsns array created in phase 2a. */
            futureMemConsumption += 8;

            /*
             * For the DeferredDeltaRef created in phase 2a to reference the
             * delta fetched via ref.binLsn.
             */
            futureMemConsumption += SIZEOF_DeferredDeltaRef;

            memoryUsage += (currentMemConsumption + futureMemConsumption);

            return scanner.accLimitExceeded(memoryUsage, lsns.size());
        }

        /*
         * Called during phase 2a after fetching the bin pointed-to by the
         * given lsn. 
         */
        boolean removeLsn(
            DiskOrderedScanner scanner,
            long lsn,
            int memSize) {
            
            boolean found =  lsns.remove(lsn);

            if (found) {

                scanner.addGlobalMemory(-LSN_MEM_OVERHEAD);

                /*
                 * We don't really need to subtract from this.memoryUsage here.
                 * It is done only for sanity checking (the assertion in
                 * this.free()).
                 */
                int memDelta = LSN_MEM_OVERHEAD;
                memDelta += memSize;
                memDelta += 8;
                memDelta += SIZEOF_DeferredDeltaRef;
                memoryUsage -= memDelta;
            }

            return found;
        }

        void undoLsn(
            DiskOrderedScanner scanner,
            long lsn,
            int memSize) {
            
            boolean found =  lsns.remove(lsn);
            assert(found);

            scanner.addGlobalMemory(-LSN_MEM_OVERHEAD);

            int memDelta = LSN_MEM_OVERHEAD;
            memDelta += memSize;
            memDelta += 8;
            memDelta += SIZEOF_DeferredDeltaRef;
            memoryUsage -= memDelta;
        }
    }

    /*
     * Wrapper for a BIN ref. Used to distinguish whether the fetching of a
     * bin delta during phase 1a was done via a deferred lsn or not. This info
     * is needed during phase 2b: after the delta is merged with its associated
     * full bin, we need to know whether it is a deferred bin, in which case
     * its records will be processed without checking their keys against
     * prevEndingKey.
     */
    public static class DeferredDeltaRef {

        final BIN delta;

        /* For Sizeof util program */
        public DeferredDeltaRef() {
            delta = null;
        }

        DeferredDeltaRef(DiskOrderedScanner scanner, BIN delta) {
            this.delta = delta;
            scanner.addGlobalMemory(SIZEOF_DeferredDeltaRef);
        }

        void free(DiskOrderedScanner scanner) {
            scanner.addGlobalMemory(-SIZEOF_DeferredDeltaRef);
        }
    }

    private static final LogEntryType[] LN_ONLY = new LogEntryType[] {
        LogEntryType.LOG_INS_LN /* Any LN type will do. */
    };

    private static final LogEntryType[] BIN_ONLY = new LogEntryType[] {
        LogEntryType.LOG_BIN
    };

    private static final LogEntryType[] BIN_OR_DELTA = new LogEntryType[] {
        LogEntryType.LOG_BIN,
        LogEntryType.LOG_BIN_DELTA,
        LogEntryType.LOG_OLD_BIN_DELTA,
    };

    private final static int SIZEOF_JAVA_REF =
        MemoryBudget.OBJECT_ARRAY_ITEM_OVERHEAD;

    private final static int SIZEOF_WeakBinRef =
        MemoryBudget.DOS_WEAK_BINREF_OVERHEAD;

    private final static int SIZEOF_OffHeapBinRef =
        MemoryBudget.DOS_OFFHEAP_BINREF_OVERHEAD;

    private final static int SIZEOF_DeferredDeltaRef =
        MemoryBudget.DOS_DEFERRED_DELTAREF_OVERHEAD;

    private final static int SIZEOF_DeferredLsnsBatch =
        MemoryBudget.DOS_DEFERRED_LSN_BATCH_OVERHEAD;

    private final static int ACCUMULATED_MEM_LIMIT = 100000; // bytes

    private final static int SUSPENSION_INTERVAL = 50; // in milliseconds

    private final boolean scanSerial;

    private final boolean countOnly;
    private final boolean keysOnly;
    private final boolean binsOnly;

    private final long lsnBatchSize;
    private final long memoryLimit;

    private final EnvironmentImpl env;

    private final RecordProcessor processor;

    private final int numDBs;

    private final DBContext[] dbs;

    private final Map<DatabaseId, Integer> dbid2dbidxMap;

    private final boolean dupDBs;

    private final ArrayList<Object> binDeltas;

    private final LSNAccumulator lsnAcc;

    private final LinkedList<DeferredLsnsBatch> deferredLsns;

    private long localMemoryUsage = 0;

    private long globalMemoryUsage = 0;

    private long accumulatedMemDelta = 0;

    private long numLsns = 0;

    private volatile int nIterations;

    private TestHook testHook1;

    private TestHook evictionHook;

    private final boolean debug;

    DiskOrderedScanner(
        DatabaseImpl[] dbImpls,
        RecordProcessor processor,
        boolean scanSerial,
        boolean binsOnly,
        boolean keysOnly,
        boolean countOnly,
        long lsnBatchSize,
        long memoryLimit,
        boolean dbg) {
        
        this.processor = processor;

        env = dbImpls[0].getEnv();

        dupDBs = dbImpls[0].getSortedDuplicates();

        this.scanSerial = scanSerial;

        this.countOnly = countOnly;
        this.keysOnly = keysOnly || countOnly;
        this.binsOnly = binsOnly || dupDBs || keysOnly || countOnly;

        this.lsnBatchSize = lsnBatchSize;
        this.memoryLimit = memoryLimit;

        this.debug = dbg;

        numDBs = dbImpls.length;

        dbs = new DBContext[numDBs];

        dbid2dbidxMap = new HashMap<>(numDBs);

        for (int i = 0; i < numDBs; ++i) {

            dbid2dbidxMap.put(dbImpls[i].getId(), i);

            dbs[i] = new DBContext(i, dbImpls[i]);
        }

        lsnAcc =
            new LSNAccumulator() {
                @Override
                void noteMemUsage(long increment) {
                    addLocalMemory(increment);
                    addGlobalMemory(increment);
                }
            };

        if (this.binsOnly) {
            binDeltas = new ArrayList<>();
            deferredLsns = new LinkedList<>();
        } else {
            binDeltas = null;
            deferredLsns = null;
        }
    }

    int getNIterations() {
        return nIterations;
    }

    /*
     * For unit testing only.
     */
    long getNumLsns() {
        return numLsns;
    }

    private void addLocalMemory(long delta) {
        localMemoryUsage += delta;
        assert(localMemoryUsage >= 0);
    }

    private void addGlobalMemory(long delta) {

        globalMemoryUsage += delta;
        assert(globalMemoryUsage >= 0);

        accumulatedMemDelta += delta;
        if (accumulatedMemDelta > ACCUMULATED_MEM_LIMIT ||
            accumulatedMemDelta < -ACCUMULATED_MEM_LIMIT) {

            env.getMemoryBudget().updateDOSMemoryUsage(accumulatedMemDelta);
            accumulatedMemDelta = 0;
        }
    }


    /**
     * Returns whether phase I should terminate because the memory or LSN batch
     * size limit has been exceeded.
     *
     * This method need not be called every LN processed; exceeding the
     * limits by a reasonable amount should not cause problems, since the
     * limits are very approximate measures anyway. It is acceptable to check
     * for exceeded limits once per BIN, and this is currently how it is used.
     */
    private boolean accLimitExceeded() {
        return accLimitExceeded(localMemoryUsage, numLsns);
    }

    private boolean accLimitExceeded(long mem, long nLsns) {
        return (mem >= memoryLimit || nLsns > lsnBatchSize);
    }

    /**
     * Called to perform a disk-ordered scan.  Returns only when the scan is
     * complete; i.e, when all records in the database have been passed to the
     * callback.
     */
    void scan(final String protectedFilesNamePrefix,
              final long protectedFilesId) {

        final FileProtector.ProtectedFileSet protectedFileSet =
            env.getFileProtector().protectActiveFiles(
                protectedFilesNamePrefix + "-" + protectedFilesId);
        try {
            if (scanSerial) {
                scanSerial();
            } else {
                scanInterleaved();
            }

            assert (globalMemoryUsage == MemoryBudget.TREEMAP_OVERHEAD) :
                "MemoryUsage is wrong at DOS end: " + globalMemoryUsage;
                
        } finally {
            env.getFileProtector().removeFileProtection(protectedFileSet);
            final long budgeted = globalMemoryUsage - accumulatedMemDelta;
            env.getMemoryBudget().updateDOSMemoryUsage(-budgeted);
        }
    }

    private void scanSerial() {

        int dbidx = 0;
        boolean overBudget;
        DBContext ctx = null;

        while (true) {

            /*
             * Phase I.
             */
            try {
                /*
                 * Skip phase 1 if we have already exceeded the DOS budget
                 * due to delta lsns deferred from phase 2b.
                 */
                overBudget = accLimitExceeded();

                while (dbidx < numDBs && !overBudget) {

                    ctx = dbs[dbidx];

                    if (ctx.parent == null) {
                        getFirstIN(ctx, ctx.prevEndingKey);
                    }

                    if (ctx.done) {
                        ++dbidx;
                        assert(ctx.parent == null);
                        continue;
                    }

                    while (ctx.parent != null) {

                        if (binsOnly) {
                            accumulateBINs(ctx);
                        } else {
                            accumulateLNs(ctx);
                        }

                        if (accLimitExceeded()) {
                            overBudget = true;
                            break;
                        }

                        processor.checkShutdown();

                        getNextIN(ctx);
                    
                        if (ctx.done) {
                            ++dbidx;
                            assert(ctx.parent == null);
                        }
                    }
                }

            } finally {
                if (ctx.parent != null && ctx.parentIsLatched) {
                    ctx.parent.releaseLatchIfOwner();
                }
            }

            /*
             * Phase II.
             */
            if (binsOnly) {
                fetchAndProcessBINs();
            } else {
                fetchAndProcessLNs();
            }

            /*
             * Check if DOS is done; if not prepare for next iteration
             */
            if (dbidx >= numDBs && (!binsOnly || deferredLsns.isEmpty())) {
                break;
            }

            initNextIteration();
        }
    }

    private void scanInterleaved() {

        boolean done;
        boolean overBudget;

        while (true) {

            /*
             * Phase I.
             */
            try {
                do {
                    done = true;
                    overBudget = false;

                    /*
                     * Skip phase 1 if we have already exceeded the DOS budget
                     * due to delta lsns deferred from phase 2b.
                     */
                    if (accLimitExceeded()) {
                        overBudget = true;
                        break;
                    }

                    for (int dbidx = 0; dbidx < numDBs; ++dbidx) {

                        DBContext ctx = dbs[dbidx];

                        if (ctx.done) {
                            continue;
                        }

                        if (ctx.parent == null) {
                            getFirstIN(ctx, ctx.prevEndingKey);
                        } else if (numDBs > 1) {
                            resumeParent(ctx);
                        }

                        if (ctx.done) {
                            continue;
                        }

                        done = false;

                        if (binsOnly) {
                            accumulateBINs(ctx);
                        } else {
                            accumulateLNs(ctx);
                        }

                        if (accLimitExceeded()) {
                            overBudget = true;
                            break;
                        }

                        processor.checkShutdown();

                        if (ctx.pidx >= ctx.parent.getNEntries()) {
                            getNextIN(ctx);
                             if (ctx.done) {
                                continue;
                            }
                        }

                        if (numDBs > 1) {
                            releaseParent(ctx);
                        }
                    }
                } while (!done && !overBudget);

            } finally {
                for (int dbidx = 0; dbidx < numDBs; ++dbidx) {
                    if (dbs[dbidx].parent != null &&
                        dbs[dbidx].parentIsLatched) {
                        dbs[dbidx].parent.releaseLatchIfOwner();
                    }
                }
            }

            if (debug) {
                if (overBudget) {
                    System.out.println(
                        "Finished Phase 1." + nIterations +
                        " because DOS budget exceeded." +
                        " localMemoryUsage = " + localMemoryUsage +
                        " globalMemoryUsage = " + globalMemoryUsage);
                } else {
                    System.out.println(
                        "Finished Phase 1." + nIterations +
                        " because no more records to scan." +
                        " localMemoryUsage = " + localMemoryUsage +
                        " globalMemoryUsage = " + globalMemoryUsage);
                }
            }

            TestHookExecute.doHookIfSet(evictionHook);

            /*
             * Phase II.
             */
 
            if (binsOnly) {
                fetchAndProcessBINs();
            } else {
                fetchAndProcessLNs();
            }

            if (debug) {
                System.out.println(
                    "Finished Phase 2." + nIterations +
                    " localMemoryUsage = " + localMemoryUsage +
                    " globalMemoryUsage = " + globalMemoryUsage);
            }

            /*
             * Check if DOS is done; if not prepare for next iteration.
             */
            if (done && (!binsOnly || deferredLsns.isEmpty())) {
                break;
            }

            initNextIteration();
        }

        if (debug) {
            System.out.println("Producer done in " + nIterations +
                               " iterations");
        }
    }

    private void initNextIteration() {

        for (int i = 0; i < numDBs; ++i) {

            DBContext ctx = dbs[i];

            ctx.parent = null;
            ctx.parentIsLatched = false;
            ctx.checkLevel2Keys = true;

            ctx.prevEndingKey = ctx.newEndingKey;
            ctx.safeToUseCachedDelta = false;

            /*
             * If this is a bin-only DOS and phase 1 was actually executed
             * during the current iteration, see whether prevEndingKey must
             * be moved forward.
             */
            if (binsOnly && ctx.lastBinLsn != DbLsn.NULL_LSN) {

                for (DeferredLsnsBatch batch : deferredLsns) {

                    if (batch.containsLsn(ctx.lastBinLsn)) {

                        BIN bin = (BIN)fetchItem(
                            ctx.lastBinLsn, BIN_OR_DELTA);

                        int memSize = 0;

                        if (bin.isBINDelta(false)) {
                            bin = bin.reconstituteBIN(ctx.dbImpl);
                            memSize = getDeltaMemSize(ctx, ctx.lastBinLsn);
                        }

                        processBINInternal(ctx, bin, false);

                        batch.undoLsn(this, ctx.lastBinLsn, memSize);

                        assert(Key.compareKeys(ctx.newEndingKey,
                                               ctx.prevEndingKey,
                                               ctx.dbImpl.getKeyComparator())
                               >= 0);

                        ctx.prevEndingKey = ctx.newEndingKey;

                        if (debug) {
                            System.out.println(
                                "LSN " + ctx.lastBinLsn +
                                " for bin " + bin.getNodeId() +
                                " was the last bin lsn seen during Phase 1." +
                                nIterations + " and it got deferred during " +
                                "Phase " + "2." + nIterations + ". Moved " + 
                                "prevEndingKey forward");
                        }

                        break;
                    }
                }
            }

            /*
             * Set lastBinLsn to NULL_LSN as a way to indicate that phase 1
             * has not started yet. If phase 1 is not skipped during the next
             * iteration, lastBinLsn will be set to a real lsn.
             */
            ctx.lastBinLsn = DbLsn.NULL_LSN;
        }

        localMemoryUsage = lsnAcc.getMemoryUsage();
        numLsns = 0;

        if (binsOnly && !deferredLsns.isEmpty()) {
            DeferredLsnsBatch batch = deferredLsns.getFirst();
            numLsns = batch.lsns.size();
            addLocalMemory(batch.memoryUsage);
        }

        nIterations += 1;
    }

    /**
     * Implements guts of phase I in binsOnly mode.  Accumulates BIN deltas and
     * BIN LSNs for the children of the given level 2 IN parent, and processes
     * resident BINs under certain conditions; see algorithm at top of file.
     */
    private void accumulateBINs(DBContext ctx) {

        OffHeapCache ohCache = env.getOffHeapCache();

        while (ctx.pidx < ctx.parent.getNEntries()) {

            /* Skip BINs that were processed on the previous iteration. */
            if (skipParentSlot(ctx)) {
                ++ctx.pidx;
                continue;
            }

            /*
             * A cached delta must be copied it if it may contain any keys <=
             * prevEndingKey. Otherwise, if it is the last bin processed in
             * the previous iteration, it will be processed again if we take
             * a weak ref to the delta and this weak ref is then cleared and
             * the delta lsn being deferred as a result. Because no key checking
             * is done for deferred bins, processing the bin again would result
             * in duplicate records being returned to the app.
             */
            if (ctx.prevEndingKey == null ||
                (!ctx.safeToUseCachedDelta &&
                 ctx.pidx > 0 &&
                 Key.compareKeys(ctx.prevEndingKey,
                                 ctx.parent.getKey(ctx.pidx),
                                 ctx.dbImpl.getKeyComparator()) < 0)) {

                ctx.safeToUseCachedDelta = true;
            }

            boolean waitForConsumer = false;
            int binNEntries = 0;
            long binLsn = ctx.parent.getLsn(ctx.pidx);
            int ohBinId = ctx.parent.getOffHeapBINId(ctx.pidx);
            boolean ohBinPri2 = ctx.parent.isOffHeapBINPri2(ctx.pidx);

            ctx.lastBinLsn = binLsn;

            BIN bin = (BIN)ctx.parent.getTarget(ctx.pidx);

            if (bin != null) {
                bin.latch(CacheMode.UNCHANGED);
            }

            try {
                if (bin != null || ohBinId >= 0) {

                    boolean isBinDelta;
                    OffHeapCache.BINInfo ohInfo = null;

                    if (bin != null) {
                        isBinDelta = bin.isBINDelta();
                    } else {
                        ohInfo = ohCache.getBINInfo(env, ohBinId);
                        isBinDelta = ohInfo.isBINDelta;
                    }

                    if (isBinDelta) {

                        if (bin != null) {
                            if (bin.getDirty() || !ctx.safeToUseCachedDelta) {
                                addDirtyDeltaRef(bin.cloneBINDelta());
                            } else {
                                addCleanDeltaRef(ctx, binLsn, bin);
                            }

                        } else {
                            if (ctx.parent.isOffHeapBINDirty(ctx.pidx) ||
                                !ctx.safeToUseCachedDelta) {

                                addDirtyDeltaRef(
                                    ohCache.loadBIN(env, ohBinId));
                            } else {
                                addCleanDeltaOffHeapRef(
                                    ctx, binLsn, ohInfo.fullBINLsn,
                                    ohBinId, ohBinPri2);
                            }
                        }

                        ++ctx.pidx;
                        if (scanSerial) {
                            continue;
                        } else {
                            return;
                        }
                    }

                    if (bin == null) {
                        bin = ohCache.loadBIN(env, ohBinId);
                        bin.latchNoUpdateLRU(ctx.dbImpl);
                    }

                    binNEntries = bin.getNEntries();
                    if (binNEntries == 0) {
                        ++ctx.pidx;
                        continue;
                    }

                    /*
                     * Skip the bin if its last key is <= prevEndingKey.
                     * Normally, this should happen only if the bin is the last
                     * bin from the previous iteration and the 1st bin of the
                     * current iteration.
                     */
                    if (ctx.prevEndingKey != null) {
                        int cmp = Key.compareKeys(
                            bin.getKey(binNEntries - 1),
                            ctx.prevEndingKey,
                            ctx.dbImpl.getKeyComparator());

                        if (cmp <= 0) {
                            ++ctx.pidx;
                            continue;
                        }
                    }
                }

                /*
                 * If the BIN is not resident, accumulate this LSN for later
                 * processing during phase 2. During phase 2a, if the lsn
                 * points to a delta, that delta will be fetched from disk and
                 * stored until all lsns accumulated here have been fetched.
                 * Then, in phase 2b the full BINs corresponding to the stored
                 * deltas will be read in lsn order. So, we must budget memory
                 * for these deltas. Since most lsns point to deltas, we assume
                 * that they all point to deltas. 
                 */
                if (bin == null || processor.getCapacity() < binNEntries) {

                    lsnAcc.add(binLsn);
                    ++numLsns;

                    addLocalMemory(8 + getDeltaMemSize(ctx, binLsn));

                    if (debug) {
                        System.out.println(
                            "Phase 1." + nIterations +
                            ": accumulated bin lsn: " + binLsn);
                    }

                } else if (processor.canProcessWithoutBlocking(binNEntries)) {

                    if (debug) {
                        System.out.println(
                            "Phase 1." + nIterations +
                            ": Processing bin: " + bin.getNodeId());
                    }

                    processBINInternal(ctx, bin, false);

                } else {
                    if (debug) {
                        System.out.println(
                            "Phase 1." + nIterations +
                            ": Producer must wait before it can process bin " +
                            bin.getNodeId());
                    }

                    waitForConsumer = true;
                    ctx.binKey = bin.getKey(0);
                }

            } finally {
                if (bin != null) {
                    bin.releaseLatch();
                }
            }

            if (waitForConsumer) {
                waitForConsumer(ctx, binNEntries);
            } else {
                ++ctx.pidx;
                if (!scanSerial) {
                    return;
                }
            }
        } /* parent slot iteration */
    }

    void addCleanDeltaRef(DBContext ctx, long binLsn, BIN bin) {

        binDeltas.add(new WeakBinRef(this, ctx, binLsn, bin));

        bin.updateLRU(CacheMode.DEFAULT);

        /*
         * For both the local and global memory, we count the size of the
         * WeakBinRef obj, plus the ref to it in this.binDeltas. For
         * the local memory we count 2 additional overheads: (a) another
         * ref in the deltaArray allocated at the start of phase 2a and
         * (b) we assume the ref will have to be deferred during phase 2b
         * and add the memory taken to store a deferred lsn.
         */
        addLocalMemory(SIZEOF_WeakBinRef +
                       2 * SIZEOF_JAVA_REF +
                       DeferredLsnsBatch.LSN_MEM_OVERHEAD);

        addGlobalMemory(SIZEOF_WeakBinRef + SIZEOF_JAVA_REF);

        if (debug) {
            System.out.println(
                "Phase 1." + nIterations +
                ": added weak bin ref for bin delta " +
                bin.getNodeId() + " at LSN = " + binLsn);
        }
    }

    void addCleanDeltaOffHeapRef(DBContext ctx,
                                 long binLsn,
                                 long fullBINLsn,
                                 int ohBinId,
                                 boolean ohBinPri2) {

        binDeltas.add(new OffHeapBinRef(
            this, ctx, binLsn, fullBINLsn, ohBinId));

        env.getOffHeapCache().moveBack(ohBinId, ohBinPri2);

        /*
         * For both the local and global memory, we count the size of the
         * OffHeapBinRef obj, plus the ref to it in this.binDeltas. For
         * the local memory we count 2 additional overheads: (a) another
         * ref in the deltaArray allocated at the start of phase 2a and
         * (b) we assume the ref will have to be deferred during phase 2b
         * and add the memory taken to store a deferred lsn.
         */
        addLocalMemory(SIZEOF_OffHeapBinRef +
            2 * SIZEOF_JAVA_REF +
            DeferredLsnsBatch.LSN_MEM_OVERHEAD);

        addGlobalMemory(SIZEOF_OffHeapBinRef + SIZEOF_JAVA_REF);

        if (debug) {
            System.out.println(
                "Phase 1." + nIterations +
                    ": added off-heap bin ref for bin delta ID " +
                    ohBinId + " at LSN = " + binLsn);
        }
    }

    void addDirtyDeltaRef(BIN delta) {

        binDeltas.add(delta);

        /*
         * For the local mem, we account for the copy of the delta plus 2 refs
         * to it: one in this.dirtyBinDeltas, and another in the deltaArray
         * allocated at the start of phase 2a. For the global memory, the 2nd
         * ref will be counted when the deltaArray is actually allocated.
         */
        addLocalMemory(delta.getInMemorySize() + 2 * SIZEOF_JAVA_REF);

        addGlobalMemory(delta.getInMemorySize() + SIZEOF_JAVA_REF);

        if (debug) {
            System.out.println(
                "Phase 1." + nIterations +
                ": copied dirty or unsafe bin delta " + delta.getNodeId());
        }
    }

    /**
     * Implements guts of phase I in LNs-only mode (binsOnly is false).
     * Accumulates LN LSNs for the BIN children of the given level 2 IN parent,
     * and processes resident LNs under certain conditions; see algorithm at
     * top of file.
     */
    private void accumulateLNs(DBContext ctx) {

        OffHeapCache ohCache = env.getOffHeapCache();
        DatabaseImpl dbImpl = ctx.dbImpl;

        IN parent = ctx.parent;
        assert(parent != null);

        BIN bin = null;

        ctx.reuseBin = false;
        boolean waitForConsumer = false;

        while (ctx.pidx < parent.getNEntries()) {

            /* Skip BINs that were processed on the previous iteration. */
            if (skipParentSlot(ctx)) {
                ++ctx.pidx;
                continue;
            }

            long plsn = parent.getLsn(ctx.pidx);

            /*
             * Explicitly fetch the BIN if it is not resident, merging it with
             * a delta if needed. Do not call currParent.fetchIN(i) or loadIN
             * because we don't want the BIN to be attached to the in-memory
             * tree.
             */
            if (!ctx.reuseBin) {
                bin = (BIN)parent.getTarget(ctx.pidx);
            }

            if (bin == null) {
                final Object item;
                final int ohBinId = parent.getOffHeapBINId(ctx.pidx);
                if (ohBinId >= 0) {
                    item = ohCache.loadBIN(env, ohBinId);
                } else {
                    item = fetchItem(plsn, BIN_OR_DELTA);
                }

                if (item instanceof BIN) {
                    bin = (BIN) item;
                    if (bin.isBINDelta(false)) {
                        bin = bin.reconstituteBIN(dbImpl);
                    } else {
                        bin.setDatabase(dbImpl);
                    }
                } else {
                    final OldBINDelta delta = (OldBINDelta) item;
                    bin = (BIN) fetchItem(delta.getLastFullLsn(), BIN_ONLY);
                    delta.reconstituteBIN(dbImpl, bin);
                }

                bin.latchNoUpdateLRU(dbImpl);

            } else {

                bin.latchNoUpdateLRU();

                if (bin.isBINDelta()) {
                    final BIN fullBIN;
                    try {
                        fullBIN = bin.reconstituteBIN(dbImpl);
                    } finally {
                        bin.releaseLatch();
                    }
                    bin = fullBIN;
                    bin.latchNoUpdateLRU(dbImpl);
                }
            }

            try {
                int bidx = 0;

                if (waitForConsumer) {
                    waitForConsumer = false;
                    ctx.reuseBin = false;

                    bidx = bin.findEntry(ctx.binKey, true, false);

                    boolean exact =
                        (bidx >= 0 && ((bidx & IN.EXACT_MATCH) != 0));

                    if (exact) {
                        bidx = (bidx & ~IN.EXACT_MATCH);
                    } else {
                        ++bidx;
                    }
                }

                boolean checkBinKeys = isBinProcessedBefore(ctx, bin);

                for (; bidx < bin.getNEntries(); ++bidx) {

                    ctx.binKey = bin.getKey(bidx);

                    if (skipSlot(ctx, bin, bidx, checkBinKeys)) {
                        continue;
                    }

                    LN ln = (LN) bin.getTarget(bidx);

                    /*
                     * Accumulate LSNs of non-resident, non-embedded LNs
                     */
                    if (ln == null && !bin.isEmbeddedLN(bidx)) {

                        ln = ohCache.loadLN(bin, bidx, CacheMode.UNCHANGED);

                        if (ln == null) {

                            if (!DbLsn.isTransientOrNull(bin.getLsn(bidx))) {
                                lsnAcc.add(bin.getLsn(bidx));
                                ++numLsns;
                                addLocalMemory(8);
                            }

                            continue;
                        }
                    }

                    /*
                     * LN is resident or embedded. Process it now unless the
                     * queue is full.
                     */
                    if (processor.canProcessWithoutBlocking(1)) {

                        byte[] data = (ln != null ?
                                       ln.getData() : bin.getData(bidx));

                        processRecord(
                            ctx, ctx.binKey, data,
                            bin.getExpiration(bidx),
                            bin.isExpirationInHours());
                        continue;
                    }

                    /*
                     * LN is resident or embedded but the queue is full. We
                     * will suspend the producer and try again after the queue
                     * has free slots.
                     */
                    waitForConsumer = true;
                    break;
                } /* BIN slot iteration */
            } finally {
                bin.releaseLatch();
            }

            if (waitForConsumer) {
                waitForConsumer(ctx, 0);
                parent = ctx.parent;
            } else {
                ++ctx.pidx;
                if (!scanSerial) {
                    return;
                }
            }
        } /* parent slot iteration */
    }

    boolean skipParentSlot(DBContext ctx) {

        if (ctx.checkLevel2Keys &&
            ctx.prevEndingKey != null &&
            ctx.pidx + 1 < ctx.parent.getNEntries() &&
            Key.compareKeys(ctx.prevEndingKey,
                            ctx.parent.getKey(ctx.pidx + 1),
                            ctx.dbImpl.getKeyComparator()) >= 0) {
            return true;
        }

        ctx.checkLevel2Keys = false;
        return false;
    }

    private void waitForConsumer(DBContext ctx, int binNEntries) {

        releaseParent(ctx);

        try {
            int minFree = (binsOnly ? binNEntries : 1);
            int free = Math.max(minFree, processor.getCapacity() / 5);

            while (!processor.canProcessWithoutBlocking(free)) {
                synchronized (processor) {
                    processor.wait(SUSPENSION_INTERVAL);
                    processor.checkShutdown();
                }
            }
        } catch (InterruptedException IE) {
            ctx.parent.unpin();
            throw new ThreadInterruptedException(ctx.dbImpl.getEnv(), IE);
        } catch (Error E) {
            ctx.parent.unpin();
            throw E;
        }

        resumeParent(ctx);
    }

    void releaseParent(DBContext ctx) {

        ctx.plsn = ctx.parent.getLsn(ctx.pidx);
        ctx.pkey = ctx.parent.getKey(ctx.pidx);

        ctx.parent.pin();
        ctx.parent.releaseLatch();
        ctx.parentIsLatched = false;
    }

    void resumeParent(DBContext ctx) {

        ctx.parent.latchShared();
        ctx.parentIsLatched = true;

        ctx.parent.unpin();

        /*
         * See "big" comment in IN.fetchINWithNoLatch() for an
         * explanation of these conditions.
         */
        if (ctx.pidx >= ctx.parent.getNEntries() ||
            ctx.plsn != ctx.parent.getLsn(ctx.pidx) ||
            (ctx.dbImpl.isDeferredWriteMode() &&
             ctx.parent.getTarget(ctx.pidx) != null)) {

            TestHookExecute.doHookIfSet(testHook1);
            
            ctx.pidx = ctx.parent.findEntry(ctx.pkey, false, true/*exact*/);
            
            /*
             * If we cannot re-establish the position in currParent, we search
             * the tree for the parent of the BIN that should contain binKey.
             *
             * Note: The last-slot key is always good, because we are basically
             * searching for keys >= pkey. The 1st-slot key is never good
             * because it may cover keys that are < pkey.
             */
            if (ctx.pidx <= 0) {
                ctx.parent.releaseLatch();
                ctx.parentIsLatched = false;

                TestHookExecute.doHookIfSet(testHook1);

                getFirstIN(ctx, ctx.binKey);

                /*
                 * We know for sure that binKey is contained in currParent,
                 * so the 1st and last lost slots are safe.
                 */
                ctx.pidx = ctx.parent.findEntry(ctx.binKey, false, false);
            }
        } else if (ctx.plsn == ctx.parent.getLsn(ctx.pidx)) {
            ctx.reuseBin = true;
        }
    }

    /**
     * Implements guts of phase II in binsOnly mode.
     */
    private void fetchAndProcessBINs() {

        /*
         * Phase 2a
         */

        /*
         * Create and sort an array of LSNs out of the LSNs gathered in phase 1
         * of the current iteration (stored in the LSNAccumulator) and the LSNs
         * deferred from previous iterations (stored in deferredLsns). Only the
         * 1st batch of LSNs in deferredLsns is used here, because using more
         * deferred LSNs (if any) would exceed the DOS budget.
         */
        int nAccLsns = lsnAcc.getNTotalEntries();
        int nDeferredLsns = 0;
        DeferredLsnsBatch deferredBatch = null;
        DeferredLsnsBatch nextDeferredBatch = null;

        if (!deferredLsns.isEmpty()) {
            deferredBatch = deferredLsns.removeFirst();
            nDeferredLsns = deferredBatch.lsns.size();
        }

        long[] lsns = new long[nAccLsns + nDeferredLsns];

        addGlobalMemory(lsns.length * 8);

        lsnAcc.getLSNs(lsns, 0); // releases the mem held by the accumulator

        int nLsns = nAccLsns;

        if (deferredBatch != null) {

            for (Long lsn : deferredBatch.lsns) {

                if (debug) {
                    System.out.println(
                        "Phase 2." + nIterations + " Found deferred LSN: " +
                        lsn);
                }

                lsns[nLsns] = lsn;
                ++nLsns;
            }
        }

        if (debug) {
            System.out.println(
                "Phase 2." + nIterations +
                " Num LSNs to read during phase 2a: " + lsns.length);
        }

        Arrays.sort(lsns);

        /*
         * Create an array of delta refs. This array will be sorted later by
         * full BIN LSN in order to read the corresponding full bins in disk
         * order and merge them with the deltas. We populate the array from
         * 3 sources: (a) the WaekBinRefs collected for clean cached deltas
         * during phase 1, (b) the copies of dirty cached deltas found during
         * phase 1, and (c) by reading from the log the bins whose LSN is in
         * the "lsns" array, and adding to the deltaArray each such bin that
         * is indeed a delta.
         *
         * The size of the deltaArray is pre-computed based on the assumption
         * that all the LSNs in "lsns" point to deltas and all the weak bin
         * refs in this.cleanBinDeltas are still deltas.
         *
         * Note that the elements of the deltaArray may be type OldBINDelta, or
         * BIN, or DeferredDeltaRef, or WeakBinRef.
         */
        int nDeltas = binDeltas.size();

        final Object[] deltaArray = new Object[nDeltas + lsns.length];

        addGlobalMemory(deltaArray.length * SIZEOF_JAVA_REF);

        for (int i = 0; i < nDeltas; ++i) {
            deltaArray[i] = binDeltas.get(i);
        }

        binDeltas.clear();
        binDeltas.trimToSize();
        addGlobalMemory(-(nDeltas * SIZEOF_JAVA_REF));

        for (long lsn : lsns) {

            boolean isDeferred;

            LogEntry logEntry = fetchEntry(lsn, BIN_OR_DELTA);
            Object item = logEntry.getMainItem();

            DatabaseId dbId = logEntry.getDbId();
            DBContext ctx = getDbCtx(dbId);

            /*
             * For a delta, queue fetching of the full BIN and combine the full
             * BIN with the delta when it is processed below.
             */
            if (item instanceof OldBINDelta) {
                OldBINDelta o = (OldBINDelta) item;
                deltaArray[nDeltas] = item;
                ++nDeltas;
                addGlobalMemory(o.getMemorySize());
                continue;
            }

            BIN bin = (BIN) item;
            bin.setDatabase(ctx.dbImpl);

            if (bin.isBINDelta(false/*checkLatched*/)) {

                addGlobalMemory(bin.getInMemorySize());

                int memSize = getDeltaMemSize(ctx, lsn);
                isDeferred = (deferredBatch != null &&
                              deferredBatch.removeLsn(this, lsn, memSize));

                if (debug) {
                    System.out.println(
                        "Phase 2a." + nIterations + " Saving bin delta " +
                        bin.getNodeId() + " fetched via LSN " + lsn);
                }

                deltaArray[nDeltas] = (isDeferred ?
                                       new DeferredDeltaRef(this, bin) :
                                       bin);
                ++nDeltas;
                continue;
            }

            isDeferred = (deferredBatch != null &&
                          deferredBatch.removeLsn(this, lsn, 0));

            if (debug) {
                System.out.println(
                    "Phase 2a." + nIterations + " Processing full bin " +
                    bin.getNodeId() + " fetched via LSN " + lsn);
            }

            /* LSN was for a full BIN, so we can just process it. */
            processBIN(ctx, bin, isDeferred);
        }

        addGlobalMemory(-(lsns.length * 8));

        lsns = null; // allow GC

        if (deferredBatch != null) {
            deferredBatch.free(this);
            deferredBatch = null; // allow GC
        }

        if (debug) {
            System.out.println(
                "Finished Phase 2a." + nIterations +
                " localMemoryUsage = " + localMemoryUsage +
                " globalMemoryUsage = " + globalMemoryUsage);
        }

        if (nDeltas == 0) {
            addGlobalMemory(-(deltaArray.length * SIZEOF_JAVA_REF));
            return;
        }

        /*
         * Phase 2b
         */

        /* Sort deltas by full BIN LSN. */
        Arrays.sort(deltaArray, 0, nDeltas, new Comparator<Object>() {

            public int compare(Object a, Object b) {
                return DbLsn.compareTo(getLsn(a), getLsn(b));
            }

            private long getLsn(Object o) {

                if (o instanceof OldBINDelta) {
                    return ((OldBINDelta) o).getLastFullLsn();
                } else if (o instanceof BIN) {
                    return ((BIN) o).getLastFullLsn();
                } else if (o instanceof DeferredDeltaRef) {
                    return ((DeferredDeltaRef) o).delta.getLastFullLsn();
                } else if (o instanceof OffHeapBinRef) {
                    return ((OffHeapBinRef) o).fullBinLsn;
                } else {
                    return ((WeakBinRef)o).fullBinLsn;
                }
            }
        });

        /*
         * Fetch each full BIN and merge it with its corresponding delta, and
         * process each resulting BIN.
         */
        for (int i = 0; i < nDeltas; i += 1) {

            Object o = deltaArray[i];
            deltaArray[i] = null;  // for GC

            if (o instanceof OldBINDelta) {

                OldBINDelta delta = (OldBINDelta) o;

                DBContext ctx = getDbCtx(delta.getDbId());

                BIN bin = (BIN) fetchItem(delta.getLastFullLsn(), BIN_ONLY);
                delta.reconstituteBIN(ctx.dbImpl, bin);

                processBINInternal(ctx, bin, false);

                addGlobalMemory(-delta.getMemorySize());

            } else if (o instanceof BIN || o instanceof DeferredDeltaRef) {

                /*
                 * The bin may be (a) a delta copied in phase 1, or (b) a
                 * delta fetched via lsn during phase 2a; in this case the lsn
                 * may be one that was collected during phase 1 of the current
                 * iteration, or a deferred lsn from an earlier iteration. In
                 * all cases we don't need to latch the bin because it was
                 * fetched from disk and not attached to the in-memory tree.
                 */
                BIN delta;
                BIN fullBin;
                boolean isDeferred;

                if (o instanceof DeferredDeltaRef) {
                    delta = ((DeferredDeltaRef)o).delta;
                    isDeferred = true;
                    ((DeferredDeltaRef)o).free(this);
                } else {
                    delta = (BIN)o;
                    isDeferred = false;
                }

                assert(delta.isBINDelta(false));

                DBContext ctx = getDbCtx(delta.getDatabaseId());

                fullBin = delta.reconstituteBIN(ctx.dbImpl);

                processBINInternal(ctx, fullBin, isDeferred);

                addGlobalMemory(-delta.getInMemorySize());

            } else if (o instanceof OffHeapBinRef){

                OffHeapBinRef ref = (OffHeapBinRef)o;

                BIN bin = env.getOffHeapCache().loadBINIfLsnMatches(
                    env, ref.ohBinId, ref.binLsn);

                if (bin == null) {
                    nextDeferredBatch = addDeferredLsn(
                        nextDeferredBatch, ref.binLsn, ref.memSize);

                    if (debug) {
                        System.out.println(
                            "Phase 2." + nIterations +
                            ": Found stale OffHeapBinRef - " +
                            "Deferring LSN: " + ref.binLsn +
                            " delta mem: " +
                            (DeferredLsnsBatch.LSN_MEM_OVERHEAD + ref.memSize));
                    }

                } else {
                    try {
                        DBContext ctx = getDbCtx(bin.getDatabaseId());

                        BIN fullBin;

                        if (bin.isBINDelta()) {
                            fullBin = bin.reconstituteBIN(ctx.dbImpl);
                        } else {
                            fullBin = bin;
                        }

                        processBINInternal(ctx, fullBin, false);
                    } finally {
                        bin.releaseLatch();
                    }
                }

                addGlobalMemory(-SIZEOF_OffHeapBinRef);

            } else {
                assert(o instanceof WeakBinRef);

                WeakBinRef ref = (WeakBinRef)o;
                BIN bin = ref.get();

                if (bin == null) {
                    nextDeferredBatch = addDeferredLsn(
                        nextDeferredBatch, ref.binLsn, ref.memSize);

                    if (debug) {
                        System.out.println(
                            "Phase 2." + nIterations +
                            ": Found cleared WeakBinRef - " +
                            "Deferring LSN: " + ref.binLsn +
                            " delta mem: " +
                            (DeferredLsnsBatch.LSN_MEM_OVERHEAD + ref.memSize));
                    }

                } else {
                    DBContext ctx = getDbCtx(bin.getDatabaseId());

                    bin.latch(CacheMode.UNCHANGED);
                    
                    try {
                        if (bin.getLastFullLsn() != ref.fullBinLsn) {
                            nextDeferredBatch = addDeferredLsn(
                                nextDeferredBatch, ref.binLsn, ref.memSize);

                            if (debug) {
                                System.out.println(
                                    "Phase 2." + nIterations +
                                    ": Found stale WeakBinRef - " +
                                    "Deferring LSN: " + ref.binLsn);
                            }

                        } else {
                            BIN fullBin;

                            if (bin.isBINDelta()) {
                                fullBin = bin.reconstituteBIN(ctx.dbImpl);
                            } else {
                                fullBin = bin;
                            }

                            processBINInternal(ctx, fullBin, false);
                        }
                    } finally {
                        bin.releaseLatch();
                    }
                }

                addGlobalMemory(-SIZEOF_WeakBinRef);
            }
        }

        addGlobalMemory(-(deltaArray.length * SIZEOF_JAVA_REF));
    }

    private DeferredLsnsBatch addDeferredLsn(
        DeferredLsnsBatch batch,
        long lsn,
        int memSize) {

        if (batch == null) {
            batch = new DeferredLsnsBatch(this);
            deferredLsns.addLast(batch);
        }

        if (batch.addLsn(this, lsn, memSize)) {
            batch = new DeferredLsnsBatch(this);
            deferredLsns.addLast(batch);
        }

        return batch;
    }

    /**
     * Process a BIN during phase II in binsOnly mode.
     * 
     * @param bin the exclusively latched BIN.
     */
    private void processBIN(DBContext ctx, BIN bin, boolean isDeferred) {

        bin.latch(CacheMode.UNCHANGED);

        try {
            processBINInternal(ctx, bin, isDeferred);
        } finally {
            bin.releaseLatch();
        }
    }

    private void processBINInternal(
        DBContext ctx,
        BIN bin,
        boolean isDeferred) {

        /*
        if (!processedBINs.add(bin.getNodeId())) {
            System.out.println("XXXXX bin " + bin.getNodeId() +
                               " has been processed before");
        }
        */

        boolean checkBinKeys = !isDeferred && isBinProcessedBefore(ctx, bin);

        for (int i = 0; i < bin.getNEntries(); i += 1) {

            ctx.binKey = bin.getKey(i);

            if (skipSlot(ctx, bin, i, checkBinKeys)) {
                continue;
            }

            /* Only the key is needed, as in accumulateBINs. */

            byte[] key = ctx.binKey;
            byte[] data = (keysOnly ? null : bin.getData(i));

            processRecord(
                ctx, key, data,
                bin.getExpiration(i), bin.isExpirationInHours());
        }
    }

    /**
     * Implements guts of phase II in LNs-only mode (binsOnly is false).
     */
    private void fetchAndProcessLNs() {

        long[] lsns = lsnAcc.getAndSortPendingLSNs();

        addGlobalMemory(lsns.length * 8);

        for (long lsn : lsns) {

            final LNLogEntry<?> entry =
                (LNLogEntry<?>) fetchEntry(lsn, LN_ONLY);

            DBContext ctx = getDbCtx(entry.getDbId());

            entry.postFetchInit(ctx.dbImpl);

            final LN ln = entry.getMainItem();
            if (ln.isDeleted()) {
                continue;
            }

            processRecord(
                ctx, entry.getKey(), ln.getData(),
                entry.getExpiration(), entry.isExpirationInHours());
        }

        addGlobalMemory(-(lsns.length * 8));
    }

    private DBContext getDbCtx(DatabaseId dbId) {
        int dbIdx = dbid2dbidxMap.get(dbId);
        return dbs[dbIdx];
    }

    /**
     * Invokes the callback to process a single key-data pair. The parameters
     * are in the format stored in the Btree, and are translated here to
     * user-format for dup DBs.
     */
    private void processRecord(
        DBContext ctx,
        byte[] treeKey,
        byte[] treeData,
        int expiration,
        boolean expirationInHours) {

        assert treeKey != null;

        final byte[] key;
        final byte[] data;

        if (dupDBs && !countOnly) {
            
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry =
                (keysOnly ? null : new DatabaseEntry());

            DupKeyData.split(treeKey, treeKey.length, keyEntry, dataEntry);
            key = keyEntry.getData();

            data = keysOnly ? null : dataEntry.getData();

        } else {
            key = (countOnly ? null : treeKey);
            data = ((countOnly || keysOnly) ? null : treeData);
        }

        processor.process(ctx.dbIdx, key, data, expiration, expirationInHours);

        /* Save the highest valued key for this iteration. */
        if (ctx.newEndingKey == null ||
            Key.compareKeys(ctx.newEndingKey, treeKey,
                            ctx.dbImpl.getKeyComparator()) < 0) {
            ctx.newEndingKey = treeKey;
        }
    }

    /**
     * Fetches a log entry for the given LSN and returns its main item.
     *
     * @param expectTypes is used to validate the type of the entry; an
     * internal exception is thrown if the log entry does not have one of the
     * given types.
     */
    private Object fetchItem(long lsn, LogEntryType[] expectTypes) {
        return fetchEntry(lsn, expectTypes).getMainItem();
    }

    /**
     * Fetches a log entry for the given LSN and returns it.
     *
     * @param expectTypes is used to validate the type of the entry; an
     * internal exception is thrown if the log entry does not have one of the
     * given types.
     */
    private LogEntry fetchEntry(
        long lsn,
        LogEntryType[] expectTypes) {

        final LogManager logManager = env.getLogManager();

        final LogEntry entry =
            logManager.getLogEntryHandleFileNotFound(lsn);

        final LogEntryType type = entry.getLogType();

        for (LogEntryType expectType : expectTypes) {
            if (expectType.isLNType()) {
                if (type.isLNType()) {
                    return entry;
                }
            } else {
                if (type.equals(expectType)) {
                    return entry;
                }
            }
        }

        throw EnvironmentFailureException.unexpectedState(
            "Expected: " + Arrays.toString(expectTypes) +
            " but got: " + type + " LSN=" + DbLsn.getNoFormatString(lsn));
    }

    /**
     * Calculates a rough estimate of the memory needed for a BIN-delta object.
     */
    private static int getDeltaMemSize(DBContext ctx, long lsn) {

        long fileNum = DbLsn.getFileNumber(lsn);

        final DbFileSummary summary = ctx.dbFileSummaries.get(fileNum);

        /*
         * If there are no deltas in this file, then the LSN must for a full
         * BIN, and no memory is needed for the delta.
         */
        if (summary == null) {
            return 0;
        }

        /*
         * The cleaner counts deltas as INs in the DbFileSummary, and most
         * are actually deltas, not INs. We double the average IN byte size
         * in the file to very roughly approximate the memory for a
         * deserialized BIN-delta object.
         */
        final float avgINSize =
            (((float) summary.totalINSize) / summary.totalINCount);

        return (int) (avgINSize * 2);
    }


    /*
     * Return true if the given bin has been processed in a previous
     * iteration, i.e., if the 1st key of the bin is <= prevEndingKey.
     * If not (which should be the common case), we don't need to check
     * the rest of the BIN keys against prevEndingKey.
     *
     * The result of this method is passed as the value of the checkBinKeys
     * param of the skipSlot method below.
     */
    private boolean isBinProcessedBefore(DBContext ctx, BIN bin) {

        if (ctx.prevEndingKey != null && bin.getNEntries() > 0) {
            final byte[] firstKey = bin.getKey(0);

            if (Key.compareKeys(firstKey, ctx.prevEndingKey,
                                ctx.dbImpl.getKeyComparator()) <= 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns whether to skip a BIN slot because its LN is deleted or expired,
     * or its key has already been processed in a previous iteration.
     */
    private boolean skipSlot(
        DBContext ctx,
        BIN bin,
        int index,
        boolean checkBinKeys) {

        if (bin.isDefunct(index)) {
            return true;
        }

        /* Skip a slot that was processed in a previous iteration. */
        return ctx.prevEndingKey != null &&
            checkBinKeys &&
            Key.compareKeys(
                ctx.prevEndingKey, ctx.binKey,
                ctx.dbImpl.getKeyComparator()) >= 0;
    }

    /**
     * Moves to the first level 2 IN in the database if searchKey is null
     * (signifying the first iteration), or the level 2 IN containing
     * searchKey if it is non-null.
     *
     * We take the liberty of fetching the BIN (first BIN or BIN for the
     * searchKey), when it is not resident, although in an ideal world no
     * BINs would be added to the cache. Since this only occurs once per
     * iteration, it is considered to be acceptable.
     */
    private void getFirstIN(DBContext ctx, byte[] searchKey) {

        /*
         * Use a retry loop to account for the possibility that after getting
         * the BIN we can't find its exact parent due to a split of some kind
         * while the BIN is unlatched.
         */
        final Tree tree = ctx.dbImpl.getTree();

        for (int i = 0; i < 25; i += 1) {

            final BIN bin;

            if (searchKey == null) {
                bin = tree.getFirstNode(CacheMode.UNCHANGED);
            } else {
                bin = tree.search(searchKey, CacheMode.UNCHANGED);
            }

            if (bin == null) {
                /* Empty database. */
                ctx.parent = null;
                ctx.done = true;
                return;
            }

            /*
             * Call getParentINForChildIN with 0 as exclusiveLevel so that
             * the parent will be latched in shared mode.
             */
            long targetId = bin.getNodeId();
            byte[] targetKey = bin.getIdentifierKey();

            bin.releaseLatch();

            ctx.parentIsLatched = true;

            final SearchResult result = tree.getParentINForChildIN(
                targetId, targetKey, -1/*useTargetLevel*/,
                0/*exclusiveLevel*/, true/*requireExactMatch*/,
                true/*doFetch*/, CacheMode.UNCHANGED,
                null/*trackingList*/);

            final IN parent = result.parent;

            if (!result.exactParentFound) {
                if (parent != null) {
                    parent.releaseLatch();
                }
                ctx.parentIsLatched = false;
                continue; /* Retry. */
            }

            ctx.parent = parent;
            ctx.pidx = 0;

            if (ctx.parent == null) {
                ctx.done = true;
            }

            return;
        }

        throw EnvironmentFailureException.unexpectedState(
            "Unable to find BIN for key: " +
            Arrays.toString(searchKey));
    }

    /**
     * Moves to the next level 2 IN in the database.
     */
    private void getNextIN(DBContext ctx) {
        
        ctx.parent = ctx.dbImpl.getTree().getNextIN(
            ctx.parent, true /*forward*/, true/*latchShared*/,
            CacheMode.UNCHANGED);

        ctx.pidx = 0;

        if (ctx.parent == null) {
            ctx.done = true;
        }
    }


    /*
     * UnitTesting support
     */

    public void setTestHook1(TestHook hook) {
        testHook1 = hook;
    }

    public void setEvictionHook(TestHook hook) {
        evictionHook = hook;
    }

    public void evictBinRefs() {

        if (debug) {
            System.out.println("DOS EVICTION HOOK");
        }

        for (Object o : binDeltas) {
            if (o instanceof OffHeapBinRef) {

                OffHeapBinRef ohRef = (OffHeapBinRef) o;

                env.getOffHeapCache().evictBINIfLsnMatch(
                    env, ohRef.ohBinId, ohRef.binLsn);

                continue;
            }

            if (!(o instanceof WeakBinRef)) {
                continue;
            }

            Evictor evictor = env.getEvictor();

            WeakBinRef binRef = (WeakBinRef) o;

            BIN bin = binRef.get();

            if (bin == null) {
                continue;
            }

            binRef.clear();

            bin.latch();

            if (!bin.getInListResident()) {
                bin.releaseLatch();
                continue;
            }

            long freedBytes =
                evictor.doTestEvict(bin, Evictor.EvictionSource.MANUAL);

            /*
             * Try another time; maybe the bin was just moved to the dirty LRU
             */
            if (freedBytes == 0) {
                bin.latch();
                evictor.doTestEvict(bin, Evictor.EvictionSource.MANUAL);
            }
        }
    }
}
