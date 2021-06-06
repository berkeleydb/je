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

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.recovery.Checkpointer.CheckpointReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Manages the by-level map of checkpoint references that are to be flushed by
 * a checkpoint or Database.sync, the MapLNs to be flushed, the highest level
 * by database to be flushed, and the state of the checkpoint.
 *
 * An single instance of this class is used for checkpoints and has the same
 * lifetime as the checkpointer and environment.  An instance per Database.sync
 * is created as needed.  Only one checkpoint can occur at a time, but multiple
 * syncs may occur concurrently with each other and with the checkpoint.
 *
 * The methods in this class are synchronized to protect internal state from
 * concurrent access by the checkpointer and eviction, and to coordinate state
 * changes between the two.  Eviction must participate in the checkpoint so
 * that INs cascade up properly; see coordinateEvictionWithCheckpoint.
 *
 * When INs are latched along with synchronization on a DirtyINMap, the order
 * must be: 1) IN latches and 2) synchronize on DirtyINMap.  For example,
 * the evictor latches the parent and child IN before calling the synchronized
 * method coordinateEvictionWithCheckpoint, and selectDirtyINsForCheckpoint
 * latches the IN before calling the synchronized method selectForCheckpoint.
 */
class DirtyINMap {

    static final boolean DIRTY_SET_DEBUG_TRACE = false;

    private final EnvironmentImpl envImpl;
    private final SortedMap<Integer,
                            Pair<Map<Long, CheckpointReference>,
                                 Map<Long, CheckpointReference>>> levelMap;
    private int numEntries;
    private final Set<DatabaseId> mapLNsToFlush;
    private final Map<DatabaseImpl, Integer> highestFlushLevels;

    enum CkptState {
        /** No checkpoint in progress, or is used for Database.sync. */
        NONE,
        /** Checkpoint started but dirty map is not yet complete. */
        DIRTY_MAP_INCOMPLETE,
        /** Checkpoint in progress and dirty map is complete. */
        DIRTY_MAP_COMPLETE,
    };

    private CkptState ckptState;
    private boolean ckptFlushAll;
    private boolean ckptFlushExtraLevel;

    DirtyINMap(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        levelMap = new TreeMap<>();
        numEntries = 0;
        mapLNsToFlush = new HashSet<DatabaseId>();
        highestFlushLevels = new IdentityHashMap<DatabaseImpl, Integer>();
        ckptState = CkptState.NONE;
    }

    /**
     * Coordinates an eviction with an in-progress checkpoint and returns
     * whether or not provisional logging is needed.
     *
     * @return the provisional status to use for logging the target.
     */
    synchronized Provisional coordinateEvictionWithCheckpoint(
        final DatabaseImpl db,
        final int targetLevel,
        final IN parent) {

        /*
         * If the checkpoint is in-progress and has not finished dirty map
         * construction, we must add the parent to the dirty map.  That way the
         * dirtiness and logging will cascade up in the same way as if the
         * target were not evicted, and instead were encountered during dirty
         * map construction.  We don't want the evictor's actions to introduce
         * an IN in the log that has not cascaded up properly.
         *
         * Note that we add the parent even if it is not dirty here.  It will
         * become dirty after the target child is logged, but that hasn't
         * happened yet.
         *
         * We do not add the parent if it is null, which is the case when the
         * root is being evicted.
         */
        if (ckptState == CkptState.DIRTY_MAP_INCOMPLETE &&
            parent != null) {

            /* Add latched parent IN to dirty map. */
            selectForCheckpoint(parent, -1 /*index*/);

            /* Save dirty/temp DBs for later. */
            saveMapLNsToFlush(parent);
        }

        /*
         * The evictor has to log provisionally in three cases:
         *
         * 1 - The eviction target is part of a deferred write database.
         */
        if (db.isDeferredWriteMode()) {
            return Provisional.YES;
        }

        /*
         * 2 - The checkpoint is in-progress and has not finished dirty map
         *     construction, and the target is not the root. The parent IN has
         *     been added to the dirty map, so we know the child IN is at a
         *     level below the max flush level.
         */
        if (ckptState == CkptState.DIRTY_MAP_INCOMPLETE &&
            parent != null) {
            return Provisional.YES;
        }

        /*
         * 3 - The checkpoint is in-progress and has finished dirty map
         *     construction, and is at a level above the eviction target.
         */
        if (ckptState == CkptState.DIRTY_MAP_COMPLETE &&
            targetLevel < getHighestFlushLevel(db)) {
            return Provisional.YES;
        }

        /* Otherwise, log non-provisionally. */
        return Provisional.NO;
    }

    /**
     * Coordinates a split with an in-progress checkpoint.
     *
     * TODO:
     * Is it necessary to perform MapLN flushing for nodes logged by a split
     * (and not just the new sibling)?
     *
     * @param newSibling the sibling IN created by the split.
     */
    void coordinateSplitWithCheckpoint(final IN newSibling) {

        assert newSibling.isLatchExclusiveOwner();

        /*
         * If the checkpoint is in-progress and has not finished dirty map
         * construction, we must add the BIN children of the new sibling to the
         * dirty map. The new sibling will be added to the INList but it may or
         * may not be seen by the in-progress INList iteration, and we must
         * ensure that its dirty BIN children are logged by the checkpoint.
         *
         * Note that we cannot synchronize on 'this' before calling
         * selectDirtyBINChildrenForCheckpoint, since it latches BIN children.
         * IN latching must come before synchronization on 'this'. Eventually
         * after latching the IN, selectForCheckpoint is called , which  is
         * synchronized and checks for ckptState == DIRTY_MAP_INCOMPLETE.
         */
        selectDirtyBINChildrenForCheckpoint(newSibling);
    }

    /**
     * Must be called before starting a checkpoint, and must not be called for
     * Database.sync.  Updates memory budget and sets checkpoint state.
     */
    synchronized void beginCheckpoint(boolean flushAll,
                                      boolean flushExtraLevel) {
        assert levelMap.isEmpty();
        assert mapLNsToFlush.isEmpty();
        assert highestFlushLevels.isEmpty();
        assert numEntries == 0;
        assert ckptState == CkptState.NONE;
        ckptState = CkptState.DIRTY_MAP_INCOMPLETE;
        ckptFlushAll = flushAll;
        ckptFlushExtraLevel = flushExtraLevel;
    }

    /**
     * Must be called after a checkpoint or Database.sync is complete.  Updates
     * memory budget and clears checkpoint state.
     */
    synchronized void reset() {
        removeCostFromMemoryBudget();
        levelMap.clear();
        mapLNsToFlush.clear();
        highestFlushLevels.clear();
        numEntries = 0;
        ckptState = CkptState.NONE;
    }

    /**
     * Scan the INList for all dirty INs, excluding temp DB INs.  Save them in
     * a tree-level ordered map for level ordered flushing.
     *
     * Take this opportunity to recalculate the memory budget tree usage.
     *
     * This method itself is not synchronized to allow concurrent eviction.
     * Synchronization is performed on a per-IN basis to protect the data
     * structures here, and eviction can occur in between INs.
     */
    void selectDirtyINsForCheckpoint() {
        assert ckptState == CkptState.DIRTY_MAP_INCOMPLETE;

        /*
         * Opportunistically recalculate the INList memory budget while
         * traversing the entire INList.
         */
        final INList inMemINs = envImpl.getInMemoryINs();
        inMemINs.memRecalcBegin();

        boolean completed = false;
        try {
            for (IN in : inMemINs) {
                in.latchShared(CacheMode.UNCHANGED);
                try {
                    if (!in.getInListResident()) {
                        continue;
                    }

                    inMemINs.memRecalcIterate(in);

                    /* Add dirty UIN to dirty map. */
                    if (in.getDirty() && !in.isBIN()) {
                        selectForCheckpoint(in, -1 /*index*/);
                    }

                    /* Add dirty level 2 children to dirty map. */
                    selectDirtyBINChildrenForCheckpoint(in);

                    /* Save dirty/temp DBs for later. */
                    saveMapLNsToFlush(in);
                } finally {
                    in.releaseLatch();
                }

                /* Call test hook after releasing latch. */
                TestHookExecute.doHookIfSet(
                    Checkpointer.examineINForCheckpointHook, in);
            }
            completed = true;
        } finally {
            inMemINs.memRecalcEnd(completed);
        }

        /*
         * Finish filling out the highestFlushLevels map. For each entry in
         * highestFlushLevels that has a null level Integer value (set by
         * selectForCheckpoint), we call DbTree.getHighestLevel and replace the
         * null level. We must call DbTree.getHighestLevel, which latches the
         * root, only when not synchronized, to avoid breaking the
         * synchronization rules described in the class comment.  This must be
         * done in several steps to follow the sychronization rules, yet
         * protect the highestFlushLevels using synchronization.
         */
        final Map<DatabaseImpl, Integer> maxFlushDbs = new HashMap<>();

        /* Copy entries with a null level. */
        synchronized (this) {
            for (DatabaseImpl db : highestFlushLevels.keySet()) {

                if (highestFlushLevels.get(db) == null) {
                    maxFlushDbs.put(db, null);
                }
            }
        }

        /* Call getHighestLevel without synchronization. */
        final DbTree dbTree = envImpl.getDbTree();

        for (Map.Entry<DatabaseImpl, Integer> entry : maxFlushDbs.entrySet()) {

            entry.setValue(dbTree.getHighestLevel(entry.getKey()));
        }

        /* Fill in levels in highestFlushLevels. */
        synchronized (this) {

            for (Map.Entry<DatabaseImpl, Integer> entry :
                 maxFlushDbs.entrySet()) {

                highestFlushLevels.put(entry.getKey(), entry.getValue());
            }
        }

        /* Complete this phase of the checkpoint. */
        synchronized (this) {
            addCostToMemoryBudget();
            ckptState = CkptState.DIRTY_MAP_COMPLETE;
        }

        if (DIRTY_SET_DEBUG_TRACE) {
            traceDirtySet();
        }
    }

    /**
     * Add the IN to the dirty map if dirty map construction is in progress and
     * the IN is not in a temp DB. If added, the highest flush level map is
     * also updated.
     */
    synchronized void selectForCheckpoint(final IN in, final int index) {

        /*
         * Must check state while synchronized. The state may not be
         * DIRTY_MAP_INCOMPLETE when called from eviction or a split.
         */
        if (ckptState != CkptState.DIRTY_MAP_INCOMPLETE) {
            return;
        }

        final DatabaseImpl db = in.getDatabase();

        if (db.isTemporary()) {
            return;
        }

        addIN(in, index,
            true /*updateFlushLevels*/,
            false /*updateMemoryBudget*/);
    }

    /**
     * Adds the the dirty child BINs of the 'in' if dirty map construction is
     * in progress and the IN is not in a temp DB.
     *
     * Main cache resident BINs are added when their parent is encountered in
     * the INList iteration, rather than when the BIN is encountered in the
     * iteration. This is because a BIN can transition between main and
     * off-heap caches during the construction of the dirty map. When a BIN is
     * loaded from off-heap and added to the main cache, it is added to the
     * INList at that time, and such a BIN may not be encountered in the
     * iteration. (ConcurrentHashMap iteration only guarantees that nodes will
     * be encountered if they are present when the iterator is created). So if
     * we relied on encountering BINs in the iteration, some might be missed.
     *
     * Note that this method is not synchronized because it latches the BIN
     * children. IN latching must come before synchronizing on 'this'. The
     * selectForCheckpoint method, which is called after latching the BIN, is
     * synchronized.
     */
    private void selectDirtyBINChildrenForCheckpoint(final IN in) {

        if (in.getNormalizedLevel() != 2) {
            return;
        }

        for (int i = 0; i < in.getNEntries(); i += 1) {

            final IN bin = (IN) in.getTarget(i);

            if (bin != null) {

                /* When called via split a child may already be latched. */
                final boolean latchBinHere = !bin.isLatchOwner();

                if (latchBinHere) {
                    bin.latchShared(CacheMode.UNCHANGED);
                }

                try {
                    if (bin.getDirty()) {
                        selectForCheckpoint(bin, -1);
                    }
                } finally {
                    if (latchBinHere) {
                        bin.releaseLatch();
                    }
                }
            } else {
                if (in.isOffHeapBINDirty(i)) {
                    selectForCheckpoint(in, i);
                }
            }
        }
    }

    private void updateFlushLevels(Integer level,
                                   final DatabaseImpl db,
                                   final boolean isBIN,
                                   final boolean isRoot) {

        /*
         * IN was added to the dirty map.  Update the highest level seen
         * for the database.  Use one level higher when ckptFlushExtraLevel
         * is set.  When ckptFlushAll is set, use the maximum level for the
         * database.  Durable deferred-write databases must be synced, so
         * also use the maximum level.
         *
         * Always flush at least one level above the bottom-most BIN level so
         * that the BIN level is logged provisionally and the expense of
         * processing BINs during recovery is avoided.
         */
        if (ckptFlushAll || db.isDurableDeferredWrite()) {
            if (!highestFlushLevels.containsKey(db)) {

                /*
                 * Null is used as an indicator that getHighestLevel should be
                 * called in selectDirtyINsForCheckpoint, when not
                 * synchronized.
                 */
                highestFlushLevels.put(db, null);
            }
        } else {
            if ((ckptFlushExtraLevel || isBIN) && !isRoot) {
                /* Next level up in the same tree. */
                level += 1;
            }

            final Integer highestLevelSeen = highestFlushLevels.get(db);

            if (highestLevelSeen == null || level > highestLevelSeen) {
                highestFlushLevels.put(db, level);
            }
        }
    }

    /**
     * Scan the INList for all dirty INs for a given database.  Arrange them in
     * level sorted map for level ordered flushing.
     *
     * This method is not synchronized to allow concurrent eviction.
     * Coordination between eviction and Database.sync is not required.
     */
    void selectDirtyINsForDbSync(DatabaseImpl dbImpl) {

        assert ckptState == CkptState.NONE;

        final DatabaseId dbId = dbImpl.getId();

        for (IN in : envImpl.getInMemoryINs()) {
            if (in.getDatabaseId().equals(dbId)) {
                in.latch(CacheMode.UNCHANGED);
                try {
                    if (in.getInListResident() && in.getDirty()) {
                        addIN(
                            in, -1 /*index*/,
                            false /*updateFlushLevels*/,
                            false /*updateMemoryBudget*/);
                    }
                } finally {
                    in.releaseLatch();
                }
            }
        }

        /*
         * Create a single entry map that forces all levels of this DB to
         * be flushed.
         */
        highestFlushLevels.put(
            dbImpl, envImpl.getDbTree().getHighestLevel(dbImpl));

        /* Add the dirty map to the memory budget.  */
        addCostToMemoryBudget();
    }

    synchronized int getHighestFlushLevel(DatabaseImpl db) {

        assert ckptState != CkptState.DIRTY_MAP_INCOMPLETE;

        /*
         * This method is only called while flushing dirty nodes for a
         * checkpoint or Database.sync, not for an eviction, so an entry for
         * this database should normally exist.  However, if the DB root (and
         * DatabaseImpl) have been evicted since the highestFlushLevels was
         * constructed, the new DatabaseImpl instance will not be present in
         * the map.  In this case, we do not need to checkpoint the IN and
         * eviction should be non-provisional.
         */
        Integer val = highestFlushLevels.get(db);
        return (val != null) ? val : IN.MIN_LEVEL;
    }

    synchronized int getNumLevels() {
        return levelMap.size();
    }

    private synchronized void addCostToMemoryBudget() {
        final MemoryBudget mb = envImpl.getMemoryBudget();
        final long cost =
            ((long) numEntries) * MemoryBudget.CHECKPOINT_REFERENCE_SIZE;
        mb.updateAdminMemoryUsage(cost);
    }

    private synchronized void removeCostFromMemoryBudget() {
        final MemoryBudget mb = envImpl.getMemoryBudget();
        final long cost =
            ((long) numEntries) * MemoryBudget.CHECKPOINT_REFERENCE_SIZE;
        mb.updateAdminMemoryUsage(0 - cost);
    }

    /**
     * Add a node unconditionally to the dirty map.
     *
     * @param in is the IN to add, or the parent of an off-heap IN to add when
     * index >= 0.
     *
     * @param index is the index of the off-heap child to add, or -1 to add the
     * 'in' itself.
     *
     * @param updateMemoryBudget if true then update the memory budget as the
     * map is changed; if false then addCostToMemoryBudget must be called
     * later.
     */
    synchronized void addIN(final IN in,
                            final int index,
                            final boolean updateFlushLevels,
                            final boolean updateMemoryBudget) {
        final Integer level;
        final long lsn;
        final long nodeId;
        final boolean isRoot;
        final byte[] idKey;
        final boolean isBin;

        if (index >= 0) {
            level = in.getLevel() - 1;
            lsn = in.getLsn(index);
            nodeId = -1;
            isRoot = false;
            idKey = in.getKey(index);
            isBin = true;
        } else {
            level = in.getLevel();
            lsn = in.getLastLoggedLsn();
            nodeId = in.getNodeId();
            isRoot = in.isRoot();
            idKey = in.getIdentifierKey();
            isBin = in.isBIN();
        }

        final Map<Long, CheckpointReference> lsnMap;
        final Map<Long, CheckpointReference> nodeMap;

        Pair<Map<Long, CheckpointReference>,
             Map<Long, CheckpointReference>> pairOfMaps = levelMap.get(level);

        if (pairOfMaps != null) {
            lsnMap = pairOfMaps.first();
            nodeMap = pairOfMaps.second();
        } else {
            /*
             * We use TreeMap rather than HashMap because HashMap.iterator() is
             * a slow way of getting the first element (see removeNextNode).
             */
            lsnMap = new TreeMap<>();
            nodeMap = new TreeMap<>();
            pairOfMaps = new Pair<>(lsnMap, nodeMap);
            levelMap.put(level, pairOfMaps);
        }

        final DatabaseImpl db = in.getDatabase();

        final CheckpointReference ref = new CheckpointReference(
            db.getId(), nodeId, level, isRoot, idKey, lsn);

        final boolean added;

        if (lsn != DbLsn.NULL_LSN) {
            added = lsnMap.put(lsn, ref) == null;
        } else {
            assert nodeId >= 0;
            assert db.isDeferredWriteMode();
            added = nodeMap.put(nodeId, ref) == null;
        }

        if (!added) {
            return;
        }

        numEntries++;

        if (updateFlushLevels) {
            updateFlushLevels(level, db, isBin, isRoot);
        }

        if (updateMemoryBudget) {
            final MemoryBudget mb = envImpl.getMemoryBudget();
            mb.updateAdminMemoryUsage(MemoryBudget.CHECKPOINT_REFERENCE_SIZE);
        }
    }

    /**
     * Get the lowest level currently stored in the map.
     */
    synchronized Integer getLowestLevelSet() {
        return levelMap.firstKey();
    }

    /**
     * Removes the set corresponding to the given level.
     */
    synchronized void removeLevel(Integer level) {
        levelMap.remove(level);
    }

    synchronized CheckpointReference removeNode(final int level,
                                                final long lsn,
                                                final long nodeId) {

        final Pair<Map<Long, CheckpointReference>,
                   Map<Long, CheckpointReference>> pairOfMaps =
            levelMap.get(level);

        if (pairOfMaps == null) {
            return null;
        }

        final Map<Long, CheckpointReference> lsnMap = pairOfMaps.first();
        final Map<Long, CheckpointReference> nodeMap = pairOfMaps.second();

        if (lsn != DbLsn.NULL_LSN) {
            final CheckpointReference ref = lsnMap.remove(lsn);
            if (ref != null) {
                return ref;
            }
        }

        if (nodeId >= 0) {
            final CheckpointReference ref = nodeMap.remove(nodeId);
            if (ref != null) {
                return ref;
            }
        }

        return null;
    }

    synchronized CheckpointReference removeNextNode(Integer level) {

        final Pair<Map<Long, CheckpointReference>,
                   Map<Long, CheckpointReference>> pairOfMaps =
            levelMap.get(level);

        if (pairOfMaps == null) {
            return null;
        }

        final Map<Long, CheckpointReference> map;

        if (!pairOfMaps.first().isEmpty()) {
            map = pairOfMaps.first();
        } else if (!pairOfMaps.second().isEmpty()) {
            map = pairOfMaps.second();
        } else {
            return null;
        }

        final Iterator<Map.Entry<Long, CheckpointReference>> iter =
            map.entrySet().iterator();

        assert iter.hasNext();
        final CheckpointReference ref = iter.next().getValue();
        iter.remove();
        return ref;
    }

    /**
     * If the given IN is a BIN for the ID mapping database, saves all
     * dirty/temp MapLNs contained in it.
     */
    private synchronized void saveMapLNsToFlush(IN in) {

        if (in.isBIN() &&
            in.getDatabase().getId().equals(DbTree.ID_DB_ID)) {

            for (int i = 0; i < in.getNEntries(); i += 1) {
                final MapLN ln = (MapLN) in.getTarget(i);

                if (ln != null && ln.getDatabase().isCheckpointNeeded()) {
                    mapLNsToFlush.add(ln.getDatabase().getId());
                }
            }
        }
    }

    /**
     * Flushes all saved dirty/temp MapLNs and clears the saved set.
     *
     * <p>If dirty, a MapLN must be flushed at each checkpoint to record
     * updated utilization info in the checkpoint interval.  If it is a
     * temporary DB, the MapLN must be flushed because all temp DBs must be
     * encountered by recovery so they can be removed if they were not closed
     * (and removed) by the user.</p>
     *
     * This method is not synchronized because it takes the Btree root latch,
     * and we must never latch something in the Btree after synchronizing on
     * DirtyINMap; see class comments.  Special synchronization is performed
     * for accessing internal state; see below.
     *
     * @param checkpointStart start LSN of the checkpoint in progress.  To
     * reduce unnecessary logging, the MapLN is only flushed if it has not been
     * written since that LSN.
     */
    void flushMapLNs(long checkpointStart) {

        /*
         * This method is called only while flushing dirty nodes for a
         * checkpoint or Database.sync, not for an eviction, and mapLNsToFlush
         * is not changed during the flushing phase.  So we don't strictly need
         * to synchronize while accessing mapLNsToFlush.  However, for
         * consistency and extra safety we always synchronize while accessing
         * internal state.
         */
        final Set<DatabaseId> mapLNsCopy;

        synchronized (this) {
            assert ckptState != CkptState.DIRTY_MAP_INCOMPLETE;

            if (mapLNsToFlush.isEmpty()) {
                mapLNsCopy = null;
            } else {
                mapLNsCopy = new HashSet<>(mapLNsToFlush);
                mapLNsToFlush.clear();
            }
        }

        if (mapLNsCopy != null) {
            final DbTree dbTree = envImpl.getDbTree();

            for (DatabaseId dbId : mapLNsCopy) {
                envImpl.checkDiskLimitViolation();
                final DatabaseImpl db = dbTree.getDb(dbId);
                try {
                    if (db != null &&
                        !db.isDeleted() &&
                        db.isCheckpointNeeded()) {

                        dbTree.modifyDbRoot(
                            db, checkpointStart /*ifBeforeLsn*/,
                            true /*mustExist*/);
                    }
                } finally {
                    dbTree.releaseDb(db);
                }
            }
        }
    }

    /**
     * Flushes the DB mapping tree root at the end of the checkpoint, if either
     * mapping DB is dirty and the root was not flushed previously during the
     * checkpoint.
     *
     * This method is not synchronized because it does not access internal
     * state.  Also, it takes the DbTree root latch and although this latch
     * should never be held by eviction, for consistency we should not latch
     * something related to the Btree after synchronizing on DirtyINMap; see
     * class comments.
     *
     * @param checkpointStart start LSN of the checkpoint in progress.  To
     * reduce unnecessary logging, the Root is only flushed if it has not been
     * written since that LSN.
     */
    void flushRoot(long checkpointStart) {

        final DbTree dbTree = envImpl.getDbTree();

        if (dbTree.getDb(DbTree.ID_DB_ID).isCheckpointNeeded() ||
            dbTree.getDb(DbTree.NAME_DB_ID).isCheckpointNeeded()) {

            envImpl.logMapTreeRoot(checkpointStart);
        }
    }

    synchronized int getNumEntries() {
        return numEntries;
    }

    private void traceDirtySet() {
        assert DIRTY_SET_DEBUG_TRACE;

        final StringBuilder sb = new StringBuilder();
        sb.append("Ckpt dirty set");

        for (final Integer level : levelMap.keySet()) {

            final Pair<Map<Long, CheckpointReference>,
                       Map<Long, CheckpointReference>> pairOfMaps =
                levelMap.get(level);

            final Map<Long, CheckpointReference> lsnMap =
                pairOfMaps.first();

            final Map<Long, CheckpointReference> nodeMap =
                pairOfMaps.second();

            sb.append("\nlevel = 0x").append(Integer.toHexString(level));
            sb.append(" lsnMap = ").append(lsnMap.size());
            sb.append(" nodeMap = ").append(nodeMap.size());
        }

        sb.append("\ndbId:highestFlushLevel");

        for (final DatabaseImpl db : highestFlushLevels.keySet()) {
            sb.append(' ').append(db.getId()).append(':');
            sb.append(highestFlushLevels.get(db) & IN.LEVEL_MASK);
        }

        LoggerUtils.logMsg(
            envImpl.getLogger(), envImpl, Level.INFO, sb.toString());
    }
}
