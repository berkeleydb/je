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

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.OldBINDeltaLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.OldBINDelta;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * SortedLSNTreeWalker uses ordered disk access rather than random access to
 * iterate over a database tree. Faulting in data records by on-disk order can
 * provide much improved performance over faulting in by key order, since the
 * latter may require random access.  SortedLSN walking does not obey cursor
 * and locking constraints, and therefore can only be guaranteed consistent for
 * a quiescent tree which is not being modified by user or daemon threads.
 *
 * The class walks over the tree using sorted LSN fetching for parts of the
 * tree that are not in memory. It returns LSNs for each node in the tree,
 * <b>except</b> the root IN, in an arbitrary order (i.e. not key
 * order). The caller is responsible for getting the root IN's LSN explicitly.
 * <p>
 * A callback function specified in the constructor is executed for each LSN
 * found.
 * <p>
 * The walker works in two phases.  The first phase is to gather and return all
 * the resident INs using the roots that were specified when the SLTW was
 * constructed.  For each child of each root, if the child is resident it is
 * passed to the callback method (processLSN).  If the child was not in memory,
 * it is added to a list of LSNs to read.  When all of the in-memory INs have
 * been passed to the callback for all LSNs collected, phase 1 is complete.
 * <p>
 * In phase 2, for each of the sorted LSNs, the target is fetched, the type
 * determined, and the LSN and type passed to the callback method for
 * processing.  LSNs of the children of those nodes are retrieved and the
 * process repeated until there are no more nodes to be fetched for this
 * database's tree.  LSNs are accumulated in batches in this phase so that
 * memory consumption is not excessive.  For instance, if batches were not used
 * then the LSNs of all of the BINs would need to be held in memory.
 */
public class SortedLSNTreeWalker {

    /*
     * The interface for calling back to the user with each LSN.
     */
    public interface TreeNodeProcessor {
        void processLSN(long childLSN,
                        LogEntryType childType,
                        Node theNode,
                        byte[] lnKey,
                        int lastLoggedSize)
            throws FileNotFoundException;

        /* Used for processing dirty (unlogged) deferred write LNs. [#15365] */
        void processDirtyDeletedLN(long childLSN, LN ln, byte[] lnKey);

        /* Called when the internal memory limit is exceeded. */
        void noteMemoryExceeded();
    }

    /*
     * Optionally passed to the SortedLSNTreeWalker to be called when an
     * exception occurs.
     */
    interface ExceptionPredicate {
        /* Return true if the exception can be ignored. */
        boolean ignoreException(Exception e);
    }

    final DatabaseImpl[] dbImpls;
    protected final EnvironmentImpl envImpl;

    /*
     * Save the root LSN at construction time, because the root may be
     * nulled out before walk() executes.
     */
    private final long[] rootLsns;

    /*
     * Whether to call DatabaseImpl.finishedINListHarvest().
     */
    private final boolean setDbState;

    /* The limit on memory to be used for internal structures during SLTW. */
    private long internalMemoryLimit = Long.MAX_VALUE;

    /* The current memory usage by internal SLTW structures. */
    private long internalMemoryUsage;

    private final TreeNodeProcessor callback;

    /*
     * If true, then walker should fetch LNs and pass them to the
     * TreeNodeProcessor callback method.  Even if true, dup LNs are not
     * fetched because they are normally never used (see accumulateDupLNs).
     */
    boolean accumulateLNs = false;

    boolean preloadIntoOffHeapCache = false;

    /*
     * If true, fetch LNs in a dup DB.  Since LNs in a dup DB are not used by
     * cursor operations, fetching dup LNs should only be needed in very
     * exceptional situations.  Currently this field is never set to true.
     */
    boolean accumulateDupLNs = false;

    /*
     * If non-null, save any exceptions encountered while traversing nodes into
     * this savedException list, in order to walk as much of the tree as
     * possible. The caller of the tree walker will handle the exceptions.
     */
    private final List<DatabaseException> savedExceptions;

    private final ExceptionPredicate excPredicate;

    /*
     * The batch size of LSNs which will be sorted.
     */
    private long lsnBatchSize = Long.MAX_VALUE;

    /* Holder for returning LN key from fetchLSN. */
    private final DatabaseEntry lnKeyEntry = new DatabaseEntry();

    /*
     * This map provides an LSN to IN/index. When an LSN is processed by the
     * tree walker, the map is used to lookup the parent IN and child entry
     * index of each LSN processed by the tree walker.  Since fetchLSN is
     * called with an arbitrary LSN, and since when we fetch (for preload) we
     * need to setup the parent to refer to the node which we are prefetching,
     * we need to have the parent in hand at the time of the call to fetchLSN.
     * This map allows us to keep a reference to that parent so that we can
     * call fetchNode on that parent.
     *
     * It is also necessary to maintain this map for cases other than preload()
     * so that during multi-db walks (i.e. multi db preload), we can associate
     * an arbitrary LSN back to the parent IN and therefore connect a fetch'ed
     * Node into the proper place in the tree.
     *
     * LSN -> INEntry
     */
    /* struct to hold IN/entry-index pair. */
    public static class INEntry {
        final IN in;
        final int index;

        INEntry(IN in, int index) {
            assert in != null;
            assert in.getDatabase() != null;
            this.in = in;
            this.index = index;
        }

        public INEntry(@SuppressWarnings("unused") SizeofMarker marker) {
            this.in = null;
            this.index = 0;
        }

        Object getDelta() {
            return null;
        }

        long getDeltaLsn() {
            return DbLsn.NULL_LSN;
        }

        long getMemorySize() {
            return MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
                   MemoryBudget.INENTRY_OVERHEAD;
        }
    }

    /**
     * Supplements INEntry with BIN-delta information.  When a BIN-delta is
     * encountered during the fetching process, we cannot immediately place it
     * in the tree.  Instead we queue a DeltaINEntry for fetching the full BIN,
     * in LSN order as usual.  When the full BIN is fetched, the DeltaINEntry
     * is used to apply the delta and place the result in the tree.
     */
    public static class DeltaINEntry extends INEntry {
        private final Object delta;
        private final long deltaLsn;

        DeltaINEntry(IN in, int index, Object delta, long deltaLsn) {
            super(in, index);
            assert (delta != null);
            assert (deltaLsn != DbLsn.NULL_LSN);
            this.delta = delta;
            this.deltaLsn = deltaLsn;
        }

        public DeltaINEntry(@SuppressWarnings("unused") SizeofMarker marker) {
            super(marker);
            this.delta = null;
            this.deltaLsn = 0;
        }

        @Override
        Object getDelta() {
            return delta;
        }

        @Override
        long getDeltaLsn() {
            return deltaLsn;
        }

        @Override
        long getMemorySize() {
            final long deltaSize;
            if (delta instanceof OldBINDelta) {
                deltaSize = ((OldBINDelta) delta).getMemorySize();
            } else {
                deltaSize = ((BIN) delta).getInMemorySize();
            }
            return deltaSize +
                MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
                MemoryBudget.DELTAINENTRY_OVERHEAD;
        }
    }

    private final Map<Long, INEntry> lsnINMap = new HashMap<>();

    /*
     * @param dbImpls an array of DatabaseImpls which should be walked over
     * in disk order.  This array must be parallel to the rootLsns array in
     * that rootLsns[i] must be the root LSN for dbImpls[i].
     *
     * @param setDbState if true, indicate when the INList harvest has
     * completed for a particular DatabaseImpl.
     *
     * @param rootLsns is passed in addition to the dbImpls, because the
     * root may be nulled out on the dbImpl before walk() is called.
     *
     * @param callback the callback instance
     *
     * @param savedExceptions a List of DatabaseExceptions encountered during
     * the tree walk.
     *
     * @param excPredicate a predicate to determine whether a given exception
     * should be ignored.
     */
    public SortedLSNTreeWalker(DatabaseImpl[] dbImpls,
                               boolean setDbState,
                               long[] rootLsns,
                               TreeNodeProcessor callback,
                               List<DatabaseException> savedExceptions,
                               ExceptionPredicate excPredicate) {

        if (dbImpls == null || dbImpls.length < 1) {
            throw EnvironmentFailureException.unexpectedState
                ("DatabaseImpls array is null or 0-length for " +
                 "SortedLSNTreeWalker");
        }

        this.dbImpls = dbImpls;
        this.envImpl = dbImpls[0].getEnv();
        /* Make sure all databases are from the same environment. */
        for (DatabaseImpl di : dbImpls) {
            EnvironmentImpl ei = di.getEnv();
            if (ei == null) {
                throw EnvironmentFailureException.unexpectedState
                    ("environmentImpl is null for target db " +
                     di.getDebugName());
            }

            if (ei != this.envImpl) {
                throw new IllegalArgumentException
                    ("Environment.preload() must be called with Databases " +
                     "which are all in the same Environment. (" +
                     di.getDebugName() + ")");
            }
        }

        this.setDbState = setDbState;
        this.rootLsns = rootLsns;
        this.callback = callback;
        this.savedExceptions = savedExceptions;
        this.excPredicate = excPredicate;
    }

    void setLSNBatchSize(long lsnBatchSize) {
        this.lsnBatchSize = lsnBatchSize;
    }

    void setInternalMemoryLimit(long internalMemoryLimit) {
        this.internalMemoryLimit = internalMemoryLimit;
    }

    private void incInternalMemoryUsage(long increment) {
        internalMemoryUsage += increment;
    }

    private LSNAccumulator createLSNAccumulator() {
        return new LSNAccumulator() {
            @Override
            void noteMemUsage(long increment) {
                incInternalMemoryUsage(increment);
            }
        };
    }

    /**
     * Find all non-resident nodes, and execute the callback.  The root IN's
     * LSN is not returned to the callback.
     */
    public void walk() {
        walkInternal();
    }

    void walkInternal() {

        /*
         * Phase 1: seed the SLTW with all of the roots of the DatabaseImpl[].
         * For each root, look for all in-memory child nodes and process them
         * (i.e. invoke the callback on those LSNs).  For child nodes which are
         * not in-memory (i.e. they are LSNs only and no Node references),
         * accumulate their LSNs to be later sorted and processed during phase
         * 2.
         */
        LSNAccumulator pendingLSNs = createLSNAccumulator();
        for (int i = 0; i < dbImpls.length; i += 1) {
            processRootLSN(dbImpls[i], pendingLSNs, rootLsns[i]);
        }

        /*
         * Phase 2: Sort and process any LSNs we've gathered so far. For each
         * LSN, fetch the target record and process it as in Phase 1 (i.e.
         * in-memory children get passed to the callback, not in-memory children
         * have their LSN accumulated for later sorting, fetching, and
         * processing.
         */
        processAccumulatedLSNs(pendingLSNs);
    }

    /*
     * Retrieve the root for the given DatabaseImpl and then process its
     * children.
     */
    private void processRootLSN(DatabaseImpl dbImpl,
                                LSNAccumulator pendingLSNs,
                                long rootLsn) {
        IN root = getOrFetchRootIN(dbImpl, rootLsn);
        if (root != null) {
            try {
                accumulateLSNs(root, pendingLSNs, null, -1);
            } finally {
                releaseRootIN(root);
            }
        }

        if (setDbState) {
            dbImpl.finishedINListHarvest();
        }
    }

    /*
     * Traverse the in-memory tree rooted at "parent". For each visited node N
     * call the callback method on N and put in pendingLSNs the LSNs of N's
     * non-resident children.
     *
     * On entering this method, parent is latched and remains latched on exit.
     */
    private void accumulateLSNs(final IN parent,
                                final LSNAccumulator pendingLSNs,
                                final IN ohBinParent,
                                final int ohBinIndex) {

        final DatabaseImpl db = parent.getDatabase();
        final boolean dups = db.getSortedDuplicates();

        /*
         * Without dups, all BINs contain only LN children.  With dups, it
         * depends on the dup format.  Preload works with the old dup format
         * and the new.
         *
         * In the new dup format (or after dup conversion), BINs contain only
         * LNs and no DBINs exist.  In the old dup format, DBINs contain only
         * LN children, but BINs may contain a mix of LNs and DINs.
         */
        final boolean allChildrenAreLNs;
        if (!dups || db.getDupsConverted()) {
            allChildrenAreLNs = parent.isBIN();
        } else {
            allChildrenAreLNs = parent.isBIN() && parent.containsDuplicates();
        }

        /*
         * If LNs are not needed, there is no need to accumulate the child LSNs
         * when all children are LNs.
         */
        final boolean accumulateChildren =
            !allChildrenAreLNs || (dups ? accumulateDupLNs : accumulateLNs);

        final BIN parentBin = parent.isBIN() ? ((BIN) parent) : null;
        final OffHeapCache ohCache = envImpl.getOffHeapCache();

        /*
         * Process all children, but only accumulate LSNs for children that are
         * not in memory.
         */
        for (int i = 0; i < parent.getNEntries(); i += 1) {

            final long lsn = parent.getLsn(i);
            Node child = parent.getTarget(i);
            final boolean childCached = child != null;

            final byte[] lnKey =
                (allChildrenAreLNs || (childCached && child.isLN())) ?
                parent.getKey(i) : null;

            if (parentBin != null && parentBin.isDefunct(i)) {

                /* Dirty LNs (deferred write) get special treatment. */
                processDirtyLN(child, lsn, lnKey);
                /* continue; */

            } else if (!childCached &&
                parentBin != null &&
                parentBin.getOffHeapLNId(i) != 0) {

                /* Embedded LNs are not stored off-heap */
                assert !parent.isEmbeddedLN(i);

                child = ohCache.loadLN(parentBin, i, CacheMode.UNCHANGED);
                assert child != null;

                processChild(
                    lsn, child, lnKey, parent.getLastLoggedSize(i),
                    pendingLSNs, null, -1);

            } else if (!childCached && parent.getOffHeapBINId(i) >= 0) {

                child = ohCache.materializeBIN(
                    envImpl, ohCache.getBINBytes(parent, i));

                final BIN bin = (BIN) child;
                bin.latchNoUpdateLRU(db);
                boolean isLatched = true;

                try {
                    if (bin.isBINDelta()) {

                        /* Deltas not allowed with deferred-write. */
                        assert (lsn != DbLsn.NULL_LSN);

                        /*
                         * Storing an off-heap reference would use less memory,
                         * but we prefer to optimize in the future by
                         * re-implementing preload.
                         */
                        final long fullLsn = bin.getLastFullLsn();
                        assert fullLsn != DbLsn.NULL_LSN;
                        pendingLSNs.add(fullLsn);
                        addToLsnINMap(fullLsn, parent, i, bin, lsn);

                    } else {

                        bin.releaseLatch();
                        isLatched = false;

                        processChild(
                            lsn, bin, lnKey, parent.getLastLoggedSize(i),
                            pendingLSNs, parent, i);
                    }
                } finally {
                    if (isLatched) {
                        bin.releaseLatch();
                    }
                }

            } else if (accumulateChildren &&
                       !childCached &&
                       lsn != DbLsn.NULL_LSN) {

                /*
                 * Child is not in cache. Put its LSN in the current batch of
                 * LSNs to be sorted and fetched in phase 2. But don't do
                 * this if the child is an embedded LN.
                 */
                if (!parent.isEmbeddedLN(i)) {
                    pendingLSNs.add(lsn);
                    if (ohBinParent != null) {
                        addToLsnINMap(lsn, ohBinParent, ohBinIndex);
                    } else {
                        addToLsnINMap(lsn, parent, i);
                    }
                } else {
                    processChild(
                        DbLsn.NULL_LSN, null /*child*/, lnKey,
                        0 /*lastLoggedSize*/, pendingLSNs, null, -1);
                }

            } else if (childCached) {

                child.latchShared();
                boolean isLatched = true;

                try {
                    if (child.isBINDelta()) {

                        /* Deltas not allowed with deferred-write. */
                        assert (lsn != DbLsn.NULL_LSN);

                        final BIN delta = (BIN) child;
                        final long fullLsn = delta.getLastFullLsn();
                        pendingLSNs.add(fullLsn);
                        addToLsnINMap(fullLsn, parent, i, delta, lsn);

                    } else {

                        child.releaseLatch();
                        isLatched = false;

                        processChild(
                            lsn, child, lnKey, parent.getLastLoggedSize(i),
                            pendingLSNs, null, -1);
                    }
                } finally {
                    if (isLatched) {
                        child.releaseLatch();
                    }
                }

            } else {
                /*
                 * We are here because the child was not cached and was not
                 * accumulated either (because it was an LN and LN accumulation
                 * is turned off or its LSN was NULL). 
                 */
                processChild(
                    lsn, null /*child*/, lnKey, parent.getLastLoggedSize(i),
                    pendingLSNs, null, -1);
            }

            /*
             * If we've exceeded the batch size then process the current
             * batch and start a new one.
             */
            final boolean internalMemoryExceeded =
                internalMemoryUsage > internalMemoryLimit;

            if (pendingLSNs.getNTotalEntries() > lsnBatchSize ||
                internalMemoryExceeded) {
                if (internalMemoryExceeded) {
                    callback.noteMemoryExceeded();
                }
                processAccumulatedLSNs(pendingLSNs);
                pendingLSNs.clear();
            }
        }
    }

    private void processDirtyLN(Node node, long lsn, byte[] lnKey) {
        if (node != null && node.isLN()) {
            LN ln = (LN) node;
            if (ln.isDirty()) {
                callback.processDirtyDeletedLN(lsn, ln, lnKey);
            }
        }
    }

    private void processChild(
        final long lsn,
        final Node child,
        final byte[] lnKey,
        final int lastLoggedSize,
        final LSNAccumulator pendingLSNs,
        final IN ohBinParent,
        final int ohBinIndex) {

        final boolean childCached = (child != null);

        /*
         * If the child is resident, use its log type, else it must be an LN.
         */
        callProcessLSNHandleExceptions(
            lsn,
            (!childCached ?
             LogEntryType.LOG_INS_LN /* Any LN type will do */ :
             child.getGenericLogType()),
            child, lnKey, lastLoggedSize);

        if (childCached && child.isIN()) {
            final IN nodeAsIN = (IN) child;
            try {
                nodeAsIN.latch(CacheMode.UNCHANGED);
                accumulateLSNs(nodeAsIN, pendingLSNs, ohBinParent, ohBinIndex);
            } finally {
                nodeAsIN.releaseLatch();
            }
        }
    }

    /*
     * Process a batch of LSNs by sorting and fetching each of them.
     */
    private void processAccumulatedLSNs(LSNAccumulator pendingLSNs) {

        while (!pendingLSNs.isEmpty()) {
            final long[] currentLSNs = pendingLSNs.getAndSortPendingLSNs();
            pendingLSNs = createLSNAccumulator();
            for (long lsn : currentLSNs) {
                fetchAndProcessLSN(lsn, pendingLSNs);
            }
        }
    }

    /*
     * Fetch the node at 'lsn' and callback to let the invoker process it.  If
     * it is an IN, accumulate LSNs for it.
     */
    private void fetchAndProcessLSN(long lsn, LSNAccumulator pendingLSNs) {

        lnKeyEntry.setData(null);

        final FetchResult result = fetchLSNHandleExceptions(
            lsn, lnKeyEntry, pendingLSNs);

        if (result == null) {
            return;
        }

        final boolean isIN = result.node.isIN();
        final IN in;
        if (isIN) {
            in = (IN) result.node;
            in.latch(CacheMode.UNCHANGED);
        } else {
            in = null;
        }

        try {
            callProcessLSNHandleExceptions(
                lsn, result.node.getGenericLogType(), result.node,
                lnKeyEntry.getData(), result.lastLoggedSize);

            if (isIN) {
                accumulateLSNs(
                    in, pendingLSNs, result.ohBinParent, result.ohBinIndex);
            }
        } finally {
            if (isIN) {
                in.releaseLatch();
            }
        }
    }

    private FetchResult fetchLSNHandleExceptions(
        long lsn,
        DatabaseEntry lnKeyEntry,
        LSNAccumulator pendingLSNs) {

        DatabaseException dbe = null;

        try {
            return fetchLSN(lsn, lnKeyEntry, pendingLSNs);

        } catch (DatabaseException e) {
            if (excPredicate == null ||
                !excPredicate.ignoreException(e)) {
                dbe = e;
            }
        }

        if (dbe != null) {
            if (savedExceptions != null) {

                /*
                 * This LSN fetch hit a failure. Do as much of the rest of
                 * the tree as possible.
                 */
                savedExceptions.add(dbe);
            } else {
                throw dbe;
            }
        }

        return null;
    }

    private void callProcessLSNHandleExceptions(long childLSN,
                                                LogEntryType childType,
                                                Node theNode,
                                                byte[] lnKey,
                                                int lastLoggedSize) {
        DatabaseException dbe = null;

        try {
            callback.processLSN(
                childLSN, childType, theNode, lnKey, lastLoggedSize);

        } catch (FileNotFoundException e) {
            if (excPredicate == null ||
                !excPredicate.ignoreException(e)) {
                dbe = new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
            }

        } catch (DatabaseException e) {
            if (excPredicate == null ||
                !excPredicate.ignoreException(e)) {
                dbe = e;
            }
        }

        if (dbe != null) {
            if (savedExceptions != null) {

                /*
                 * This LSN fetch hit a failure. Do as much of the rest of
                 * the tree as possible.
                 */
                savedExceptions.add(dbe);
            } else {
                throw dbe;
            }
        }
    }

    /**
     * Returns the root IN, latched shared.  Allows subclasses to override
     * getResidentRootIN and/or getRootIN to modify behavior.
     * getResidentRootIN is called first,
     */
    private IN getOrFetchRootIN(DatabaseImpl dbImpl, long rootLsn) {
        final IN root = getResidentRootIN(dbImpl);
        if (root != null) {
            return root;
        }
        if (rootLsn == DbLsn.NULL_LSN) {
            return null;
        }
        return getRootIN(dbImpl, rootLsn);
    }

    /**
     * The default behavior fetches the rootIN from the log and latches it
     * shared. Classes extending this may fetch (and latch) the root from the
     * tree.
     */
    IN getRootIN(DatabaseImpl dbImpl, long rootLsn) {
        final IN root = (IN)
            envImpl.getLogManager().getEntryHandleFileNotFound(rootLsn);
        if (root == null) {
            return null;
        }
        root.setDatabase(dbImpl);
        root.latchShared(CacheMode.DEFAULT);
        return root;
    }

    /**
     * The default behavior returns (and latches shared) the IN if it is
     * resident in the Btree, or null otherwise.  Classes extending this may
     * return (and latch) a known IN object.
     */
    IN getResidentRootIN(DatabaseImpl dbImpl) {
        return dbImpl.getTree().getResidentRootIN(true /*latched*/);
    }

    /**
     * Release the latch.  Overriding this method should not be necessary.
     */
    private void releaseRootIN(IN root) {
        root.releaseLatch();
    }

    /**
     * Add an LSN-IN/index entry to the map.
     */
    private void addToLsnINMap(long lsn, IN in, int index) {
        addEntryToLsnMap(lsn, new INEntry(in, index));
    }

    /**
     * Add an LSN-IN/index entry, along with a delta and delta LSN, to the map.
     */
    private void addToLsnINMap(long lsn,
                               IN in,
                               int index,
                               Object delta,
                               long deltaLsn) {
        addEntryToLsnMap(lsn, new DeltaINEntry(in, index, delta, deltaLsn));
    }

    private void addEntryToLsnMap(long lsn, INEntry inEntry) {
        if (lsnINMap.put(lsn, inEntry) == null) {
            incInternalMemoryUsage(inEntry.getMemorySize());
        }
    }

    private static class FetchResult {
        final Node node;
        final int lastLoggedSize;
        final IN ohBinParent;
        final int ohBinIndex;

        FetchResult(final Node node,
                    final int lastLoggedSize,
                    final IN ohBinParent,
                    final int ohBinIndex) {
            this.node = node;
            this.lastLoggedSize = lastLoggedSize;
            this.ohBinParent = ohBinParent;
            this.ohBinIndex = ohBinIndex;
        }
    }

    /*
     * Process an LSN.  Get & remove its INEntry from the map, then fetch the
     * target at the INEntry's IN/index pair.  This method will be called in
     * sorted LSN order.
     */
    private FetchResult fetchLSN(
        long lsn,
        DatabaseEntry lnKeyEntry,
        LSNAccumulator pendingLSNs) {

        final LogManager logManager = envImpl.getLogManager();
        final OffHeapCache ohCache = envImpl.getOffHeapCache();

        final INEntry inEntry = lsnINMap.remove(lsn);
        assert (inEntry != null) : DbLsn.getNoFormatString(lsn);
        
        incInternalMemoryUsage(- inEntry.getMemorySize());
        
        IN in = inEntry.in;
        int index = inEntry.index;

        IN ohBinParent = null;
        int ohBinIndex = -1;

        IN in1ToUnlatch = null;
        IN in2ToUnlatch = null;

        if (!in.isLatchExclusiveOwner()) {
            in.latch();
            in1ToUnlatch = in;
        }

        final DatabaseImpl dbImpl = in.getDatabase();
        byte[] lnKey = null;

        Node residentNode = in.getTarget(index);
        if (residentNode != null) {
            residentNode.latch();
        }

        try {
            /*
             * When the indexed slot contains an off-heap BIN, the node to
             * fetch is an LN within the off-heap BIN or the full BIN to merge
             * with an off-heap BIN-delta.
             */
            Object deltaObject = inEntry.getDelta();
            boolean isOffHeapBinInTree = in.getOffHeapBINId(index) >= 0;
            boolean isLnInOffHeapBin = false;

            if (isOffHeapBinInTree && deltaObject == null) {
                /*
                 * When fetching an LN within an off-heap BIN, materialize the
                 * parent BIN and set in/index to this true parent.
                 */
                isLnInOffHeapBin = true;

                final BIN ohBin = ohCache.materializeBIN(
                    envImpl, ohCache.getBINBytes(in, index));

                int foundIndex = -1;
                for (int i = 0; i < ohBin.getNEntries(); i += 1) {
                    if (ohBin.getLsn(i) == lsn) {
                        foundIndex = i;
                        break;
                    }
                }

                if (foundIndex == -1) {
                    return null; // See note on concurrent activity below.
                }

                ohBinParent = in;
                ohBinIndex = index;

                in = ohBin;
                index = foundIndex;

                in.latchNoUpdateLRU(dbImpl);
                in2ToUnlatch = in;
            }

            /*
             * Concurrent activity (e.g., log cleaning) that was active before
             * we took the root latch may have changed the state of a slot.
             * Repeat check for LN deletion/expiration and check that the LSN
             * has not changed.
             */
            if (in.isBIN() && ((BIN) in).isDefunct(index)) {
                return null;
            }

            if (deltaObject == null) {
                if (in.getLsn(index) != lsn) {
                    return null;
                }
            } else {
                if (in.getLsn(index) != inEntry.getDeltaLsn()) {
                    return null;
                }
            }

            boolean mutateResidentDeltaToFullBIN = false;

            if (residentNode != null) {
                /*
                 * If the resident node is not a delta then concurrent
                 * activity (e.g., log cleaning) must have loaded the node.
                 * Just return it and continue.
                 */
                if (!residentNode.isBINDelta()) {
                    if (residentNode.isLN()) {
                        lnKeyEntry.setData(in.getKey(index));
                    }
                    return new FetchResult(
                        residentNode, in.getLastLoggedSize(index), null, -1);
                }

                /* The resident node is a delta. */
                if (((BIN) residentNode).getLastFullLsn() != lsn) {
                    return null; // See note on concurrent activity above.
                }
                mutateResidentDeltaToFullBIN = true;
            }

            /* Fetch log entry. */
            final WholeEntry wholeEntry;
            try {
                wholeEntry = logManager.getWholeLogEntry(lsn);

            } catch (FileNotFoundException e) {
                final String msg =
                    (fetchAndInsertIntoTree() ?
                        "Preload failed" :
                        "SortedLSNTreeWalker failed") +
                    " dbId=" + dbImpl.getId() +
                    " isOffHeapBinInTree=" + isOffHeapBinInTree +
                    " isLnInOffHeapBin=" + isLnInOffHeapBin +
                    " deltaObject=" + (deltaObject != null) +
                    " residentNode=" + (residentNode != null);

                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_FILE_NOT_FOUND,
                    in.makeFetchErrorMsg(msg, lsn, index), e);
            }

            final LogEntry entry = wholeEntry.getEntry();
            final int lastLoggedSize = wholeEntry.getHeader().getEntrySize();

            /*
             * For a BIN delta, queue fetching of the full BIN and combine the
             * full BIN with the delta when it is processed later (see below).
             *
             * Note that for preload, this means that a BIN-delta is not placed
             * in the tree when there is not enough memory for the full BIN.
             * Ideally we should place the BIN-delta in the tree here.
             */
            if (entry instanceof BINDeltaLogEntry) {
                final BINDeltaLogEntry deltaEntry = (BINDeltaLogEntry) entry;
                final long fullLsn = deltaEntry.getPrevFullLsn();
                final BIN delta = deltaEntry.getMainItem();
                pendingLSNs.add(fullLsn);
                addToLsnINMap(fullLsn, in, index, delta, lsn);
                return null;
            }

            if (entry instanceof OldBINDeltaLogEntry) {
                final OldBINDelta delta = (OldBINDelta) entry.getMainItem();
                final long fullLsn = delta.getLastFullLsn();
                pendingLSNs.add(fullLsn);
                addToLsnINMap(fullLsn, in, index, delta, lsn);
                return null;
            }

            /* For an LNLogEntry, call postFetchInit and get the lnKey. */
            if (entry instanceof LNLogEntry) {
                final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
                lnEntry.postFetchInit(dbImpl);
                lnKey = lnEntry.getKey();
                lnKeyEntry.setData(lnKey);
            }

            /* Get the Node from the LogEntry. */
            final Node ret = (Node) entry.getResolvedItem(dbImpl);

            /*
             * For an IN Node, set the database so it will be passed down to
             * nested fetches.
             */
            long lastLoggedLsn = lsn;
            if (ret.isIN()) {
                final IN retIn = (IN) ret;
                retIn.setDatabase(dbImpl);
            }

            /*
             * If there is a delta, then this is the full BIN to which the
             * delta must be applied. The delta LSN is the last logged LSN.
             */
            if (mutateResidentDeltaToFullBIN) {
                final BIN fullBIN = (BIN) ret;
                BIN delta = (BIN) residentNode;
                if (fetchAndInsertIntoTree()) {
                    delta.mutateToFullBIN(fullBIN, false /*leaveFreeSlot*/);

                    return new FetchResult(
                        residentNode, lastLoggedSize, ohBinParent, ohBinIndex);
                } else {
                    delta.reconstituteBIN(
                        dbImpl, fullBIN, false /*leaveFreeSlot*/);

                    return new FetchResult(
                        ret, lastLoggedSize, ohBinParent, ohBinIndex);
                }
            }

            if (deltaObject != null) {
                final BIN fullBIN = (BIN) ret;

                if (deltaObject instanceof OldBINDelta) {
                    final OldBINDelta delta = (OldBINDelta) deltaObject;
                    assert lsn == delta.getLastFullLsn();
                    delta.reconstituteBIN(dbImpl, fullBIN);
                    lastLoggedLsn = inEntry.getDeltaLsn();
                } else {
                    final BIN delta = (BIN) deltaObject;
                    assert lsn == delta.getLastFullLsn();

                    delta.reconstituteBIN(
                        dbImpl, fullBIN, false /*leaveFreeSlot*/);

                    lastLoggedLsn = inEntry.getDeltaLsn();
                }
            }

            assert !ret.isBINDelta(false);

            /*
             * When we store an off-heap BIN here, the caller must pass its
             * parent/index to accumulateLSNs.
             */
            IN retOhBinParent = null;
            int retOhBinIndex = -1;

            /* During a preload, finally place the Node into the Tree. */
            if (fetchAndInsertIntoTree()) {

                /* Last logged size is not present before log version 9. */
                in.setLastLoggedSize(index, lastLoggedSize);

                /*
                 * We don't worry about the memory usage being kept below the
                 * max by the evictor, since we keep the root INs latched.
                 */
                final MemoryBudget memBudget = envImpl.getMemoryBudget();
                final boolean storeOffHeap =
                    preloadIntoOffHeapCache &&
                    memBudget.getCacheMemoryUsage() > memBudget.getMaxMemory();

                /*
                 * Note that UINs are always stored in the main cache even if
                 * it is full. The idea is that LNs and BINs should be evicted
                 * from main to make room. When the main cache fills with UINs,
                 * and an off-heap cache is also being filled, we currently
                 * allow the main cache to overflow.
                 */
                if (isOffHeapBinInTree || (storeOffHeap && !ret.isUpperIN())) {
                    if (ret.isLN()) {
                        /*
                         * Store LN off-heap. If an oh LN was added to an oh
                         * BIN we must re-store the oh BIN as well. This is
                         * inefficient but we don't know of a simple way to
                         * optimize.
                         */
                        final BIN bin = (BIN) in;
                        final LN retLn = (LN) ret;
                        ohCache.storePreloadedLN(bin, index, retLn);
                        if (isOffHeapBinInTree) {
                            assert isLnInOffHeapBin;
                            ohCache.storePreloadedBIN(
                                bin, ohBinParent, ohBinIndex);
                        }
                    } else {
                        /*
                         * Store full BIN off-heap. Note that setLastLoggedLSN
                         * is normally called by postFetchInit or postLoadInit,
                         * but neither is used during preload so we must call
                         * setLastLoggedLsn here.
                         */
                        assert !isLnInOffHeapBin;
                        final BIN retBin = (BIN) ret;
                        retBin.latchNoUpdateLRU(dbImpl);
                        retBin.setLastLoggedLsn(lsn);
                        try {
                            if (!ohCache.storePreloadedBIN(
                                retBin, in, index)) {
                                return null; // could not allocate memory
                            }
                        } finally {
                            retBin.releaseLatch();
                        }
                        retOhBinParent = in;
                        retOhBinIndex = index;
                    }
                } else {
                    /* Attach node to the Btree as in a normal operation. */
                    if (ret.isIN()) {
                        final IN retIn = (IN) ret;
                        retIn.latchNoUpdateLRU(dbImpl);
                        ret.postFetchInit(dbImpl, lastLoggedLsn);
                        in.attachNode(index, ret, lnKey);
                        retIn.releaseLatch();
                    } else {
                        ret.postFetchInit(dbImpl, lastLoggedLsn);
                        in.attachNode(index, ret, lnKey);
                    }

                    /* BINs with resident LNs shouldn't be in the dirty LRU. */
                    if (in.isBIN()) {
                        final CacheMode mode =
                            in.getDatabase().getDefaultCacheMode();

                        if (mode != CacheMode.EVICT_LN) {
                            envImpl.getEvictor().moveToPri1LRU(in);
                        }
                    }
                }

                /*
                 * Clear the fetched-cold flag set, since we want the preloaded
                 * data to be "hot". This is necessary because the node is not
                 * latched after being preloaded, as it normally would be after
                 * being attached.
                 */
                if (ret.isIN()) {
                    ((IN) ret).setFetchedCold(false);
                } else if (ret.isLN()) {
                    ((LN) ret).setFetchedCold(false);
                }
            }

            return new FetchResult(
                ret, lastLoggedSize, retOhBinParent, retOhBinIndex);

        } finally {
            if (residentNode != null) {
                residentNode.releaseLatch();
            }
            if (in1ToUnlatch != null) {
                in1ToUnlatch.releaseLatch();
            }
            if (in2ToUnlatch != null) {
                in2ToUnlatch.releaseLatch();
            }
        }
    }

    /*
     * Overriden by subclasses if fetch of an LSN should result in insertion
     * into tree rather than just instantiating the target.
     */
    protected boolean fetchAndInsertIntoTree() {
        return false;
    }
}
