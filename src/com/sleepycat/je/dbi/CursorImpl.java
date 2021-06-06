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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DuplicateDataException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINBoundary;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.StorageSize;
import com.sleepycat.je.tree.TrackingInfo;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeWalkerStatsAccumulator;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockInfo;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * A CursorImpl is the internal implementation of the cursor.
 */
public class CursorImpl implements Cloneable {

    private static final boolean DEBUG = false;

    private static final byte CURSOR_NOT_INITIALIZED = 1;
    private static final byte CURSOR_INITIALIZED = 2;
    private static final byte CURSOR_CLOSED = 3;
    private static final String TRACE_DELETE = "Delete";
    private static final String TRACE_MOD = "Mod:";
    private static final String TRACE_INSERT = "Ins:";

    public static final int FOUND = 0x1;
    /* Exact match on the key portion. */
    public static final int EXACT_KEY = 0x2;
    /* Record found is the last one in the dbImpl. */
    public static final int FOUND_LAST = 0x4;

    /*
     * Allocate hashCode ids from this. [#13896]
     */
    private static long lastAllocatedId = 0;

    /*
     * Unique id that we can return as a hashCode to prevent calls to
     * Object.hashCode(). [#13896]
     */
    private final int thisId;

    /* The dbImpl behind the handle. */
    private final DatabaseImpl dbImpl;

    /* Owning transaction. */
    private Locker locker;

    private final boolean retainNonTxnLocks;

    private final boolean isSecondaryCursor;

    /*
     * Cursor location in the dbImpl, represented by a BIN and an index
     * in the BIN.  The bin is null if not established, and the index is
     * negative if not established.
     */
    private volatile BIN bin;
    private volatile int index;

    /* State of the cursor. See CURSOR_XXX above. */
    private byte status;

    private CacheMode cacheMode;
    private boolean allowEviction;
    private BIN priorBIN;

    /*
     * A cache of the record version for the operation at the current position.
     * Is null if the cursor is uninitialized.  For a secondary cursor, is the
     * version of the primary record.
     */
    private RecordVersion currentRecordVersion;

    /*
     * A cache of the storage size for the operation at the cursor position.
     * Both values are zero if the cursor is uninitialized. priStorageSize is
     * non-zero only if Cursor.readPrimaryAfterGet was called.
     */
    private int storageSize;
    private int priStorageSize;

    /* Number of secondary records written by a primary put or delete. */
    private int nSecWrites;

    private ThreadLocal<TreeWalkerStatsAccumulator> treeStatsAccumulatorTL;

    private TestHook testHook;

    /**
     * Creates a cursor with retainNonTxnLocks=true, isSecondaryCursor=false.
     * These are the standard settings for an internal cursor.
     */
    public CursorImpl(DatabaseImpl database, Locker locker) {
        this(database, locker,
             true /*retainNonTxnLocks*/,
             false /*isSecondaryCursor*/);
    }

    /**
     * Creates a cursor.
     *
     * A cursor always retains transactional locks when it is reset or closed.
     * Non-transaction locks may be retained or not, depending on the
     * retainNonTxnLocks parameter value.
     *
     * Normally a user-created non-transactional Cursor releases locks on reset
     * and close, and a ThreadLocker is normally used.  However, by passing
     * true for retainNonTxnLocks a ThreadLocker can be made to retain locks;
     * this capability is used by SecondaryCursor.readPrimaryAfterGet.
     *
     * For internal (non-user) cursors, a BasicLocker is often used and locks
     * are retained. In these internal use cases the caller explicitly calls
     * BasicLocker.operationEnd() after the cursor is closed, and
     * retainNonTxnLocks is set to true to prevent the locks acquired by the
     * BasicLocker from being released when the cursor is closed.
     *
     * BasicLocker is also used for NameLN operations while opening a Database
     * handle.  Database handle locks must be retained, even if the Database is
     * opened non-transactionally.
     *
     * @param retainNonTxnLocks is true if non-transactional locks should be
     * retained (not released automatically) when the cursor is reset or
     * closed.
     *
     * @param isSecondaryCursor whether to treat this cursor as a secondary
     * cursor, e.g., secondary records don't have record versions.
     */
    public CursorImpl(
        DatabaseImpl dbImpl,
        Locker locker,
        boolean retainNonTxnLocks,
        boolean isSecondaryCursor) {

        thisId = (int) getNextCursorId();
        bin = null;
        index = -1;

        this.retainNonTxnLocks = retainNonTxnLocks;
        this.isSecondaryCursor = isSecondaryCursor;

        /* Associate this cursor with the dbImpl. */
        this.dbImpl = dbImpl;
        this.locker = locker;
        this.locker.registerCursor(this);

        /*
         * This default value is used only when the CursorImpl is used directly
         * (mainly for internal databases).  When the CursorImpl is created by
         * a Cursor, CursorImpl.setCacheMode will be called.
         */
        this.cacheMode = CacheMode.DEFAULT;

        status = CURSOR_NOT_INITIALIZED;

        /*
         * Do not perform eviction here because we may be synchronized on the
         * Database instance. For example, this happens when we call
         * Database.openCursor().  Also eviction may be disabled after the
         * cursor is constructed.
         */
    }

    /**
     * Performs a shallow copy and returns the new cursor.
     *
     * @param samePosition If true, this cursor's position is used for the new
     * cursor, and addCursor is called on the new cursor to register it with
     * the current BIN.  If false, the new cursor will be uninitialized.
     */
    public CursorImpl cloneCursor(final boolean samePosition) {

        assert assertCursorState(
            false /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        CursorImpl ret = null;
        try {
            latchBIN();

            ret = (CursorImpl) super.clone();

            if (!retainNonTxnLocks) {
                ret.locker = locker.newNonTxnLocker();
            }

            ret.locker.registerCursor(ret);

            if (samePosition) {
                ret.addCursor();
            } else {
                ret.clear();
            }
        } catch (CloneNotSupportedException cannotOccur) {
            return null;
        } finally {
            releaseBIN();
        }

        /* Perform eviction before and after each cursor operation. */
        criticalEviction();

        return ret;
    }

    /*
     * Allocate a new hashCode id.  Doesn't need to be synchronized since it's
     * ok for two objects to have the same hashcode.
     */
    private static long getNextCursorId() {
        return ++lastAllocatedId;
    }

    @Override
    public int hashCode() {
        return thisId;
    }

    public Locker getLocker() {
        return locker;
    }

    /**
     * Called when a cursor has been duplicated prior to being moved.  The new
     * locker is informed of the old locker, so that a preempted lock taken by
     * the old locker can be ignored. [#16513]
     *
     * @param closingCursor the old cursor that will be closed if the new
     * cursor is moved successfully.
     */
    public void setClosingLocker(CursorImpl closingCursor) {

        /*
         * If the two lockers are different, then the old locker will be closed
         * when the operation is complete.  This is currently the case only for
         * ReadCommitted cursors and non-transactional cursors that do not
         * retain locks.
         */
        if (!retainNonTxnLocks && locker != closingCursor.locker) {
            locker.setClosingLocker(closingCursor.locker);
        }
    }

    /**
     * Called when a cursor move operation is complete.  Clears the
     * closingLocker so that a reference to the old closed locker is not held.
     */
    public void clearClosingLocker() {
        locker.setClosingLocker(null);
    }

    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the effective cache mode to use for the next operation.  The
     * cacheMode field will never be set to null, and can be passed directly to
     * latching methods.
     *
     * @see #performCacheModeEviction
     */
    public void setCacheMode(final CacheMode mode) {
        cacheMode = mode;
    }

    public void setTreeStatsAccumulator(TreeWalkerStatsAccumulator tSA) {
        maybeInitTreeStatsAccumulator();
        treeStatsAccumulatorTL.set(tSA);
    }

    private void maybeInitTreeStatsAccumulator() {
        if (treeStatsAccumulatorTL == null) {
            treeStatsAccumulatorTL = new ThreadLocal<>();
        }
    }

    private TreeWalkerStatsAccumulator getTreeStatsAccumulator() {
        if (EnvironmentImpl.getThreadLocalReferenceCount() > 0) {
            maybeInitTreeStatsAccumulator();
            return treeStatsAccumulatorTL.get();
        } else {
            return null;
        }
    }

    public void incrementLNCount() {
        TreeWalkerStatsAccumulator treeStatsAccumulator =
            getTreeStatsAccumulator();
        if (treeStatsAccumulator != null) {
            treeStatsAccumulator.incrementLNCount();
        }
    }

    public int getIndex() {
        return index;
    }

    public BIN getBIN() {
        return bin;
    }

    public void setIndex(int idx) {
        index = idx;
    }

    public void setOnFirstSlot() {
        assert(bin.isLatchOwner());
        index = 0;
    }

    public void setOnLastSlot() {
        assert(bin.isLatchOwner());
        index = bin.getNEntries() - 1;
    }

    public boolean isOnBIN(BIN bin) {
        return this.bin == bin;
    }

    public void assertBIN(BIN bin) {
        assert this.bin == bin :
            "nodeId=" + bin.getNodeId() +
            " cursor=" + dumpToString(true);
    }

    public boolean isOnSamePosition(CursorImpl other) {
        return bin == other.bin && index == other.index;
    }

    public void setBIN(BIN newBin) {

        /*
         * Historical note. In the past we checked here that the cursor was
         * removed for the prior BIN by calling BIN.containsCursor [#16280].
         * Because the containsCursor method takes a latch on the prior BIN,
         * this causes a rare latch deadlock when newBin is latched (during an
         * insert, for example), since this thread will latch two BINs in
         * arbitrary order; so the assertion was removed [#21395].
         */
        bin = newBin;
    }

    public void latchBIN() {
        while (bin != null) {
            BIN waitingOn = bin;
            waitingOn.latch(cacheMode);
            if (bin == waitingOn) {
                return;
            }
            waitingOn.releaseLatch();
        }
    }

    public void releaseBIN() {
        if (bin != null) {
            bin.releaseLatchIfOwner();
        }
    }

    void addCursor(BIN bin) {
        if (bin != null) {
            assert bin.isLatchExclusiveOwner();
            bin.addCursor(this);
        }
    }

    /**
     * Add to the current cursor.
     */
    void addCursor() {
        if (bin != null) {
            addCursor(bin);
        }
    }

    /**
     * Change cursor to point to the given BIN/index.  If the new BIN is
     * different, then old BIN must be unlatched and the new BIN must be
     * latched.
     */
    private void setPosition(BIN newBin, int newIndex) {
        if (bin != newBin) {
            if (bin != null) {
                latchBIN();
                bin.removeCursor(this);
                bin.releaseLatch();
            }
            setBIN(newBin);
            addCursor();
        }
        setIndex(newIndex);
    }

    /**
     * Called for creating trace messages without any latching.
     */
    public long getCurrentNodeId() {
        final BIN b = bin;
        return (b == null ? -1 : b.getNodeId());
    }

    public long getCurrentLsn() {

        assert(bin != null && bin.isLatchOwner());
        assert(index >= 0 && index < bin.getNEntries());

        return bin.getLsn(index);
    }

    public byte[] getCurrentKey() {
        return getCurrentKey(false);
    }

    /**
     * Returns the key at the current position, regardless of whether the
     * record is defunct.  Does not lock. The key returned is not a copy and
     * may not be returned directly to the user without copying it first.
     *
     * The cursor must be initialized.
     *
     * TODO:
     * The returned byte array is normally, but not always a copied, and then
     * copied again into the user's DatabaseEntry. If this method always
     * returns a copy, the extra copy into DatabaseEntry could be avoided.
     */
    public byte[] getCurrentKey(boolean isLatched) {

        if (!isLatched) {
            latchBIN();
        }

        try {
            assert(bin != null);
            assert(index >= 0 && index < bin.getNEntries());

            return bin.getKey(index);
        } finally {
            if (!isLatched) {
                releaseBIN();
            }
        }
    }

    public boolean isProbablyExpired() {
        latchBIN();
        try {
            return bin.isProbablyExpired(index);
        } finally {
            releaseBIN();
        }
    }

    public long getExpirationTime() {
        latchBIN();
        try {
            return TTL.expirationToSystemTime(
                bin.getExpiration(index), bin.isExpirationInHours());
        } finally {
            releaseBIN();
        }
    }

    private void setInitialized() {
        status = CURSOR_INITIALIZED;
    }

    /**
     * @return true if this cursor is closed
     */
    public boolean isClosed() {
        return (status == CURSOR_CLOSED);
    }

    /**
     * @return true if this cursor is not initialized
     */
    public boolean isNotInitialized() {
        return (status == CURSOR_NOT_INITIALIZED);
    }

    public boolean isInternalDbCursor() {
        return dbImpl.isInternalDb();
    }

    public boolean hasDuplicates() {
        return dbImpl.getSortedDuplicates();
    }

    /**
     * For a non-sticky cursor, this method is called when the cursor is
     * initialized and an advancing operation (next/prev/skip) is about to be
     * performed. The cursor position is not reset as it would be if the
     * operation were a search or an insertion, for example.
     */
    public void beforeNonStickyOp() {

        /*
         * When the cache mode dictates that we evict the LN or BIN, we evict
         * the LN here before the cursor's position changes. We can assume that
         * either the position will change or the cursor will be reset. The BIN
         * is evicted later.
         */
        if (cacheMode != CacheMode.DEFAULT &&
            cacheMode != CacheMode.KEEP_HOT) {

            latchBIN();
            try {
                performCacheModeLNEviction();
            } finally {
                releaseBIN();
            }
        }

        releaseNonTxnLocks();

        criticalEviction();
    }

    /**
     * For a non-sticky cursor, this method is called after a successful
     * operation. The cursor position is not reset as it would be if the
     * operation failed.
     */
    public void afterNonStickyOp() {

        /*
         * To implement BIN eviction for a non-sticky cursor we must save the
         * prior BIN, and only evict it after the operation and only when the
         * BIN changes. The prior BIN is evicted after the operation (in this
         * method) and when the cursor is reset or closed.
         */
        performPriorBINEviction();

        if (priorBIN == null) {
            priorBIN = bin;
        }

        criticalEviction();
    }

    /**
     * Reset a cursor to an uninitialized state, but unlike close(), allow it
     * to be used further.
     */
    public void reset() {

        /* Must remove cursor before evicting BIN and releasing locks. */
        removeCursorAndPerformCacheEviction(null /*newCursor*/);

        releaseNonTxnLocks();

        /* Perform eviction before and after each cursor operation. */
        criticalEviction();
    }

    private void clear() {
        bin = null;
        index = -1;
        status = CURSOR_NOT_INITIALIZED;
        currentRecordVersion = null;
        storageSize = 0;
        priStorageSize = 0;
        nSecWrites = 0;
        priorBIN = null;
    }

    private void releaseNonTxnLocks() {
        if (!retainNonTxnLocks) {
            locker.releaseNonTxnLocks();
        }
    }

    public void close() {
        close(null);
    }

    /**
     * Close a cursor.
     *
     * @param newCursor is another cursor that is kept open by the parent
     * Cursor object, or null if no other cursor is kept open.
     */
    public void close(final CursorImpl newCursor) {

        assert assertCursorState(
            false /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        /* Must remove cursor before evicting BIN and releasing locks. */
        removeCursorAndPerformCacheEviction(newCursor);

        locker.unRegisterCursor(this);

        if (!retainNonTxnLocks) {
            locker.nonTxnOperationEnd();
        }

        status = CURSOR_CLOSED;

        /* Perform eviction before and after each cursor operation. */
        criticalEviction();
    }

    private void removeCursorAndPerformCacheEviction(CursorImpl newCursor) {

        performPriorBINEviction();

        latchBIN();

        if (bin == null) {
            clear(); // ensure that state is uninitialized
            return;
        }

        try {
            /* Must remove cursor before evicting BIN. */
            bin.removeCursor(this);
            performCacheModeEviction(newCursor); // may release latch
        } finally {
            releaseBIN();
            clear();
        }
    }

    /**
     * Performs cache mode-based eviction but for the prior BIN only. This is
     * called after a successful operation using a non-sticky cursor. The prior
     * BIN is evicted only if the BIN has changed.
     */
    private void performPriorBINEviction() {

        if (priorBIN == null || priorBIN == bin) {
            return;
        }

        /*
         * This priorBIN should not be processed again, and setting it to null
         * enables the setting of a new priorBIN.
         */
        BIN binToEvict = priorBIN;
        priorBIN = null;

        /* Short circuit modes that do not perform BIN eviction. */
        if (cacheMode== CacheMode.DEFAULT ||
            cacheMode == CacheMode.KEEP_HOT ||
            cacheMode == CacheMode.EVICT_LN) {
            return;
        }

        binToEvict.latch(CacheMode.UNCHANGED);
        try {
            performCacheModeBINEviction(binToEvict);
        } finally {
            binToEvict.releaseLatchIfOwner();
        }
    }

    /**
     * Disables or enables eviction during cursor operations.  For example, a
     * cursor used to implement eviction (e.g., in some UtilizationProfile and
     * most DbTree and VLSNIndex methods) should not itself perform eviction,
     * but eviction should be enabled for user cursors.  Eviction is disabled
     * by default.
     */
    public void setAllowEviction(boolean allowed) {
        allowEviction = allowed;
    }

    public void criticalEviction() {

        /*
         * In addition to disabling critical eviction for internal cursors (see
         * setAllowEviction above), we do not perform critical eviction when
         * UNCHANGED, EVICT_BIN or MAKE_COLD is used and the BIN is not dirty.
         * Operations using these modes for a non-dirty BIN generally do not
         * add any net memory to the cache, so they shouldn't have to perform
         * critical eviction or block while another thread performs eviction.
         */
        if (allowEviction &&
            ((bin != null && bin.getDirty()) ||
             (cacheMode != CacheMode.UNCHANGED &&
              cacheMode != CacheMode.EVICT_BIN &&
              cacheMode != CacheMode.MAKE_COLD))) {
            dbImpl.getEnv().criticalEviction(false /*backgroundIO*/);
        }
    }

    /**
     * When multiple operations are performed, CacheMode-based eviction is
     * performed for a given operation at the end of the next operation, which
     * calls close() or reset() on the CursorImpl of the previous operation.
     * Eviction for the last operation (including when only one operation is
     * performed) also occurs during Cursor.close(), which calls
     * CursorImpl.close().
     *
     * By default, the CacheMode returned by DatabaseImpl.getCacheMode is used,
     * and the defaults specified by the user for the Database or Environment
     * are applied.  However, the default mode can be overridden by the user by
     * calling Cursor.setCacheMode, and the mode may be changed prior to each
     * operation, if desired.
     *
     * To implement a per-operation CacheMode, two CacheMode fields are
     * maintained.  Cursor.cacheMode is the mode to use for the next operation.
     * CursorImpl.cacheMode is the mode that was used for the previous
     * operation, and that is used for eviction when that CursorImpl is closed
     * or reset.
     *
     * This method must be called with the BIN latched but may release it,
     * namely when the BIN is evicted.
     */
    private void performCacheModeEviction(final CursorImpl newCursor) {

        /* Short circuit modes that do not perform LN or BIN eviction. */
        if (cacheMode == CacheMode.DEFAULT ||
            cacheMode == CacheMode.KEEP_HOT) {
            return;
        }

        final boolean movedOffBin;
        final boolean movedOffLn;

        if (newCursor != null) {
            movedOffBin = (bin != newCursor.bin);
            movedOffLn = (movedOffBin || index != newCursor.index);
        } else {
            movedOffBin = true;
            movedOffLn = true;
        }

        if (movedOffLn) {
            performCacheModeLNEviction();
        }

        /* Short circuit modes that do not perform BIN eviction. */
        if (cacheMode == CacheMode.EVICT_LN) {
            return;
        }

        if (movedOffBin) {
            performCacheModeBINEviction(bin);
        }
    }

    /**
     * Performs the LN portion of CacheMode eviction. The BIN is latched on
     * method entry and exit. Must be called only for CacheMode.EVICT_LN,
     * EVICT_BIN, UNCHANGED and MAKE_COLD.
     */
    private void performCacheModeLNEviction() {
        switch (cacheMode) {
        case EVICT_LN:
        case EVICT_BIN:
            evictLN(true /*isLatched*/, false /*ifFetchedCold*/);
            break;
        case UNCHANGED:
        case MAKE_COLD:
            evictLN(true /*isLatched*/, true /*ifFetchedCold*/);
            break;
        default:
            assert false;
        }
    }

    /**
     * Performs the BIN portion of CacheMode eviction. The BIN is latched on
     * method entry, but may or may not be latched on exit. Must be called only
     * for CacheMode.EVICT_BIN, UNCHANGED and MAKE_COLD.
     */
    private void performCacheModeBINEviction(BIN binToEvict) {
        switch (cacheMode) {
        case EVICT_BIN:
            evictBIN(binToEvict, CacheMode.EVICT_BIN);
            break;
        case UNCHANGED:
        case MAKE_COLD:
            if (binToEvict.getFetchedCold()) {
                evictBIN(binToEvict, CacheMode.UNCHANGED);
            }
            break;
        default:
            assert false;
        }
    }

    /**
     * Evict the given BIN. Must already be latched. The latch will be released
     * inside the doCacheModeEvict() call.
     */
    private void evictBIN(BIN binToEvict, CacheMode cacheMode) {

        dbImpl.getEnv().getEvictor().doCacheModeEvict(binToEvict, cacheMode);
    }

    /**
     * Evict the LN node at the cursor position.
     */
    public void evictLN() {
        evictLN(false /*isLatched*/, false /*ifFetchedCold*/);
    }

    /**
     * Evict the LN node at the cursor position.
     */
    private void evictLN(boolean isLatched, boolean ifFetchedCold) {
        try {
            if (!isLatched) {
                latchBIN();
            }
            if (index >= 0) {
                bin.evictLN(index, ifFetchedCold);
            }
        } finally {
            if (!isLatched) {
                releaseBIN();
            }
        }
    }

    private boolean shouldEmbedLN(byte[] data) {

        return data.length <= dbImpl.getEnv().getMaxEmbeddedLN() &&
            !dbImpl.getSortedDuplicates() &&
            !dbImpl.getDbType().isInternal();
    }

    /**
     * Delete the item pointed to by the cursor. If the item is already
     * defunct, return KEYEMPTY. Returns with nothing latched.
     */
    public OperationResult deleteCurrentRecord(ReplicationContext repContext) {

        assert assertCursorState(
            true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        final EnvironmentImpl envImpl = dbImpl.getEnv();
        final DbType dbType = dbImpl.getDbType();
        final long currLsn;
        final LogItem logItem;

        boolean  success = false;
        
        latchBIN();

        try {
            /*
             * Get a write lock. An uncontended lock is permitted because we
             * will log a new LN before releasing the BIN latch.
             */
            final LockStanding lockStanding = lockLN(
                LockType.WRITE, true /*allowUncontended*/, false /*noWait*/);

            if (!lockStanding.recordExists()) {
                revertLock(lockStanding);
                success = true;
                return null;
            }

            currLsn = lockStanding.lsn;
            assert(currLsn != DbLsn.NULL_LSN);
            final boolean currEmbeddedLN = bin.isEmbeddedLN(index);
            final int currLoggedSize = bin.getLastLoggedSize(index);
            final byte[] currKey = bin.getKey(index);

            final int expiration = bin.getExpiration(index);
            final boolean expirationInHours = bin.isExpirationInHours();

            /*
             * Must fetch LN if the LN is not embedded and any of the following
             * are true:
             *  - CLEANER_FETCH_OBSOLETE_SIZE is configured and lastLoggedSize
             *    is unknown
             *  - this database does not use the standard LN class and we
             *    cannot call DbType.createdDeletedLN further below
             * For other cases, we are careful not to fetch, in order to avoid
             * a random read during a delete operation.
             */
            LN ln;
            if ((currLoggedSize == 0 &&
                 !currEmbeddedLN &&
                 envImpl.getCleaner().getFetchObsoleteSize(dbImpl)) ||
                !dbType.mayCreateDeletedLN()) {

                ln = bin.fetchLN(index, cacheMode);
                if (ln == null) {
                    /* An expired LN was purged. */
                    revertLock(lockStanding);
                    success = true;
                    return null;
                }
            } else {
                ln = bin.getLN(index, cacheMode);
            }

            /*
             * Make the existing LN deleted, if cached; otherwise, create a
             * new deleted LN (with ln.data == null), but do not attach it
             * to the tree yet.
             */
            long oldLNMemSize = 0;
            if (ln != null) {
                oldLNMemSize = ln.getMemorySizeIncludedByParent();
                ln.delete();
            } else {
                ln = dbType.createDeletedLN(envImpl);
            }

            /* Get a wli to log. */
            final WriteLockInfo wli = lockStanding.prepareForUpdate(bin, index);

            /* Log the deleted record version and lock its new LSN. */
            logItem = ln.optionalLog(
                envImpl, dbImpl, locker, wli,
                currEmbeddedLN /*newEmbeddedLN*/, currKey /*newKey*/,
                expiration, expirationInHours,
                currEmbeddedLN, currLsn, currLoggedSize,
                false/*isInsertion*/, repContext);

            /*
             * Now update the parent BIN to reference the logrec written
             * above, set the PD flag on, and do the BIN memory counting.
             */
            bin.deleteRecord(
                index, oldLNMemSize, logItem.lsn,
                ln.getVLSNSequence(), logItem.size);

            /*
             * If the LN is not cached, we don't need to attach the LN to the
             * tree, because as long as the PD flag is on, the record's data
             * will  never be accessed. But for DW DBs, we must attach the LN
             * because no logrec was generated above, and as a result, the LN
             * must be in the tree so that a logrec will be generated when
             * a db.sync() occurs later (that logrec is needed for crash
             * recovery, because BINs are not replayed during crash recovery).
             *
             * If the LN child is cached, it is desirable to evict it because
             * as long as the PD flag is on, the record's data will  never be
             * accessed. But for DW DBs we should not evict the dirty LN since
             * it will be logged unnecessarily.
             */
            if (bin.getTarget(index) == null) {
                if (dbImpl.isDeferredWriteMode()) {
                    bin.attachNode(index, ln, null /*lnKey*/);
                }
            } else {
                if (!dbImpl.isDeferredWriteMode()) {
                    bin.evictLN(index);
                }
            }

            /* Cache record version/size for delete operation. */
            setCurrentVersion(ln.getVLSNSequence(), logItem.lsn);
            setStorageSize();

            locker.addDeleteInfo(bin);
            success = true;

            trace(Level.FINER, TRACE_DELETE, bin, index, currLsn, logItem.lsn);

            return DbInternal.makeResult(expiration, expirationInHours);

        } finally {

            if (success &&
                !dbImpl.isInternalDb() &&
                bin != null &&
                bin.isBINDelta()) {
                dbImpl.getEnv().incBinDeltaDeletes();
            }

            releaseBIN();
        }
    }

    /**
     * Modify the current record with the given data, and optionally replace
     * the key.
     *
     * @param key The new key value for the BIN slot S to be updated. Cannot
     * be partial. For a no-dups DB, it is null. For dups DBs it is a 2-part
     * key combining the current primary key of slot S with the original,
     * user-provided data. "key" (if not null) must compare equal to S.key
     * (otherwise DuplicateDataException is thrown), but the 2 keys may not
     * be identical if custom comparators are used. So, S.key will actually
     * be replaced by "key".
     *
     * @param data The new data to (perhaps partially) replace the data of the
     * LN associated with the BIN slot. For dups DBs it is EMPTY_DUPS_DATA.
     * Note: for dups DBs the original, user-provided "data" must not be
     * partial.
     *
     * @param returnOldData To receive the old LN data (before the update).
     * It is needed only by DBs with indexes/triggers; will be null otherwise.
     *
     * @param returnNewData To receive the full data of the updated LN.
     * It is needed only by DBs with indexes/triggers and only if "data" is
     * partial; will be null otherwise. Note: "returnNewData" may be different
     * than "data" only if "data" is partial.
     *
     * @return OperationResult, or null if an expired LN was purged and a
     * partial 'data' param was supplied.
     */
    public OperationResult updateCurrentRecord(
        DatabaseEntry key,
        DatabaseEntry data,
        ExpirationInfo expInfo,
        DatabaseEntry returnOldData,
        DatabaseEntry returnNewData,
        ReplicationContext repContext) {

        assert assertCursorState(
            true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        if (returnOldData != null) {
            returnOldData.setData(null);
        }
        if (returnNewData != null) {
            returnNewData.setData(null);
        }

        final LockStanding lockStanding;
        OperationResult result = null;
        boolean success = false;

        latchBIN();

        try {
            /* Get a write lock. */
            lockStanding = lockLN(
                LockType.WRITE, true /*allowUncontended*/, false /*noWait*/);

            if (!lockStanding.recordExists()) {
                revertLock(lockStanding);
            } else {
                result = updateRecordInternal(
                    (key != null ? Key.makeKey(key) : null), data,
                    expInfo, returnOldData, returnNewData,
                    lockStanding, repContext);
            }

            success = true;
            return result;

        } finally {

            if (success &&
                !dbImpl.isInternalDb() &&
                bin != null &&
                bin.isBINDelta()) {
                dbImpl.getEnv().incBinDeltaUpdates();
            }

            releaseBIN();
        }
    }

    /**
     * Insert the given record (key + LN) in the tree or return false if the
     * key is already present.
     *
     * The cursor must initially be uninitialized.
     *
     * This method is called directly internally for putting tree map LNs
     * and file summary LNs.
     */
    public boolean insertRecord(
        byte[] key,
        LN ln,
        boolean blindInsertion,
        ReplicationContext repContext) {

        assert assertCursorState(
            false /*mustBeInitialized*/, true /*mustNotBeInitialized*/);
        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.expectBtreeLatchesHeld(0);
        }

        try {
            final Pair<LockStanding, OperationResult> result =
                insertRecordInternal(
                    key, ln, null /*expirationInfo*/, blindInsertion,
                    null /*returnNewData*/, repContext);

            return result.second() != null;
        } finally {
            releaseBIN();
        }
    }

    /**
     * Insert or update a given record. The method searches for the record
     * using its key. It will perform an update if the record is found, 
     * otherwise an insertion.
     *
     * The cursor must initially be uninitialized.
     *
     * Called by all the Cursor.putXXX() ops, except putCurrent().
     *
     * @param key The new key value for the BIN slot S to be inserted/updated.
     * Cannot be partial. For dups DBs it is a 2-part key combining the
     * original, user-provided key and data. In case of update, "key" must
     * compare equal to S.key (otherwise DuplicateDataException is thrown),
     * but the 2 keys may not be identical if custom comparators are used.
     * So, S.key will actually be replaced by "key".
     *
     * @param data In case of update, the new data to (perhaps partially)
     * replace the data of the LN associated with the BIN slot. For dups DBs
     * it is EMPTY_DUPS_DATA. Note: for dups DBs the original, user-provided
     * "data" must not be partial.
     *
     * @param ln is normally a new LN node that is created for insertion, and
     * will be discarded if an update occurs.  However, HA will pass an
     * existing node.
     *
     * @param putMode OVERWRITE or NO_OVERWRITE
     *
     * @param returnOldData To receive, in case of update, the old LN data
     * (before the update). It is needed only by DBs with indexes/triggers;
     * will be null otherwise.
     *
     * @param returnNewData To receive the full data of the new or updated LN.
     * It is needed only by DBs with indexes/triggers and only if "data" is
     * partial; will be null otherwise. Note: "returnNewData" may be different
     * than "data" only if "data" is partial.
     *
     * @return OperationResult where isUpdate() distinguishes insertions and
     * updates. Is null only if an expired LN was purged and a partial 'data'
     * param was supplied.
     */
    public OperationResult insertOrUpdateRecord(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LN ln,
        final ExpirationInfo expInfo,
        final PutMode putMode,
        final DatabaseEntry returnOldData,
        final DatabaseEntry returnNewData,
        final ReplicationContext repContext) {

        assert key != null;
        assert data != null;
        assert ln != null;
        assert putMode != null;
        assert assertCursorState(
            false /*mustBeInitialized*/, true /*mustNotBeInitialized*/);
        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.expectBtreeLatchesHeld(0);
        }

        if (putMode != PutMode.OVERWRITE &&
            putMode != PutMode.NO_OVERWRITE) {
            throw EnvironmentFailureException.unexpectedState(
                putMode.toString());
        }

        boolean success = false;
        boolean inserted = false;

        byte[] keyCopy = Key.makeKey(key);

        try {

            /*
             * Try to insert the key/data pair as a new record. Will succeed if
             * the record does not exist in the DB already. Otherwise, the 
             * insertRecord() returns with the cursor registered on the slot
             * whose key is equal to "key", with the LSN of that slot locked
             * in WRITE mode, and with the containing BIN latched. 
             */
            Pair<LockStanding, OperationResult> insertResult =
                insertRecordInternal(
                    keyCopy, ln, expInfo,
                    false /*blindInsertion*/,
                    returnNewData, repContext);

            if (insertResult.second() != null) {
                inserted = true;
                success = true;
                return insertResult.second();
            }

            /*
             * There is a non-defunct slot whose key is == "key". So, this is
             * going to be an update. Note: Cursor has been registered on the
             * existing slot by insertRecord()
             */
            if (putMode == PutMode.NO_OVERWRITE) {
                success = true;
                return null;
            }

            /*
             * Update the non-defunct record at the cursor position. We have
             * optimized by preferring to take an uncontended lock. The
             * lockStanding var is guaranteed to be non-null in this case.
             * The BIN must remain latched when calling this method.
             */
            final OperationResult result = updateRecordInternal(
                keyCopy, data, expInfo,
                returnOldData, returnNewData,
                insertResult.first(), repContext);
       
            success = true;
            return result;

        } finally {

            if (success &&
                !dbImpl.isInternalDb() &&
                bin != null &&
                bin.isBINDelta()) {
                if (inserted) {
                    dbImpl.getEnv().incBinDeltaInserts();
                } else {
                    dbImpl.getEnv().incBinDeltaUpdates();
                }
            }

            releaseBIN();
        }
    }

    /**
     * Try to insert the key/data pair as a new record. Will succeed if a
     * non-defunct record does not exist already with the given key.
     *
     * The cursor must initially be uninitialized.
     *
     * On return, this.bin is latched.
     *
     * @return a non-null pair of LockStanding and OperationResult.
     *
     *   + LockStanding will be non-null if a slot with the given key already
     *     exists, whether or not we reuse the slot for this record (i.e.,
     *     whether or not the result is non-null). In other words, we always
     *     lock the record in an existing slot for the give key.
     *
     *   + OperationResult will be non-null if we inserted a slot or reused a
     *     slot having a defunct record, or null if the insertion failed
     *     because a non-defunct record exists with the given key.
     */
    private Pair<LockStanding, OperationResult> insertRecordInternal(
        final byte[] key,
        final LN ln,
        ExpirationInfo expInfo,
        final boolean blindInsertion,
        final DatabaseEntry returnNewData,
        final ReplicationContext repContext) {

        final EnvironmentImpl envImpl = dbImpl.getEnv();
        final Tree tree = dbImpl.getTree();
        WriteLockInfo wli;
        LockStanding lockStanding = null;
        final boolean isSlotReuse;
        final long currLsn;

        final boolean currEmbeddedLN;
        final boolean newEmbeddedLN;
        final byte[] data;

        if (shouldEmbedLN(ln.getData())) {
            data = ln.getData();
            newEmbeddedLN = true;
        } else {
            newEmbeddedLN = false;
            data = null;
        }

        if (expInfo == null) {
            expInfo = ExpirationInfo.DEFAULT;
        }

        /*
         * At this point, this cursor does not have a position so it cannot be
         * registered with the BIN that will be used. This is good because it
         * allows slot compression to occur before BIN splits (thus avoiding
         * splits if compression finds and removes any defunct slots). However,
         * if another cursor, including the one from which this was cloned, is
         * registered with the BIN, then splits won't be allowed. This is a
         * good reason to use non-sticky cursors for insertions, especially
         * sequential insertions since they will often end up in the same BIN.
         *
         * Find and latch the BIN that should contain the "key". On return from
         * the tree search, this.bin is latched, but "this" is still not
         * registered.
         */
        bin = tree.findBinForInsert(key, getCacheMode());

        /*
         * In the case where logging occurs before locking, allow lockers to
         * reject the operation (e.g., if writing on a replica) and also
         * prepare to undo in the (very unlikely) event that logging succeeds
         * but locking fails. Call this method BEFORE slot insertion, in case
         * it throws an exception which would leave the slot with a null LSN.
         *
         * For Txn, creates the writeInfo map (if not done already), and
         * inserts dbImpl in the undoDatabases map. Noop for other
         * non-HA lockers.
         */
        locker.preLogWithoutLock(dbImpl);

        /*
         * If the key exists already, insertEntry1() does not insert, but
         * returns the index of the existing key.
         *
         * If bin is a delta and it does not contain the key, then:
         * (a) if blindInsertion is false, insertEntry1() will mutate it to a
         * full BIN and check again if the key exists or not.
         * (b) if blindInsertion is true, insertEntry1() will not mutate the
         * delta; it will just insert the key into the delta. This is OK,
         * because blindInsertion will be true only if we know already that the
         * key does not exist in the tree.
         */
        int insertIndex = bin.insertEntry1(
            ln, key, data, DbLsn.NULL_LSN, blindInsertion);

        if ((insertIndex & IN.INSERT_SUCCESS) == 0) {
            /*
             * Key exists. Insertion was not successful. Register the cursor on
             * the existing slot. If the slot is defunct, the key does not
             * really exist and the slot can be reused to do an insertion.
             */
            isSlotReuse = true;

            setIndex(insertIndex);
            addCursor();
            setInitialized();

            /*
             * Lock the LSN for the existing LN slot, and check defunct-ness.
             * An uncontended lock request is permitted because we are holding
             * the bin latch. If no locker holds a lock on the slot, then no
             * lock is taken by this cursor either.
             */
            lockStanding = lockLN(
                LockType.WRITE, true /*allowUncontended*/, false /*noWait*/);
            assert(lockStanding != null);

            if (lockStanding.recordExists()) {
                return new Pair<>(lockStanding, null);
            }

            /*
             * The record in the current slot is defunct. Note: it may have
             * been made defunct by this.locker itself.
             */
            currLsn = lockStanding.lsn;
            currEmbeddedLN = bin.isEmbeddedLN(index);

            /*
             * Create a new WriteLockInfo or get an existing one for the LSN
             * of the current slot, and set its abortLSN and abortKD fields,
             * if needed, i.e, if it is not the current txn the one who created
             * this LSN. The abortLSN and abortKD fields of the wli will be
             * included in the new logrec.
             */
            wli = lockStanding.prepareForUpdate(bin, index);

        } else {
            /*
             * Register the cursor at the slot that has been successfully
             * inserted.
             */
            isSlotReuse = false;
            currEmbeddedLN = newEmbeddedLN;
            currLsn = DbLsn.NULL_LSN;

            setIndex(insertIndex &= ~IN.INSERT_SUCCESS);
            addCursor();
            setInitialized();

            /* Create a new WriteLockInfo */
            wli = LockStanding.prepareForInsert(bin);
        }

        /*
         * Log the new LN and lock the LSN of the new logrec in WRITE mode.
         * Note: in case of slot reuse, we pass NULL_LSN for the oldLsn param
         * because the old defunct LN is counted obsolete by other means.
         */
        LogItem logItem = null;
        try {
            logItem = ln.optionalLog(
                envImpl, dbImpl, locker, wli,
                newEmbeddedLN, key,
                expInfo.expiration, expInfo.expirationInHours,
                currEmbeddedLN, currLsn, 0/*currSize*/,
                true/*isInsertion*/, repContext);
        } finally {
            if (logItem == null && !isSlotReuse) {
                /*
                 * Possible buffer overflow, out-of-memory, or I/O exception
                 * during logging. The BIN entry will contain a NULL_LSN. To
                 * prevent an exception during a future fetchLN() call, we
                 * set the KD flag. We do not call BIN.deleteEntry because it
                 * does not adjust cursors. We do not add this entry to the
                 * compressor queue to avoid complexity (this situation is
                 * rare).
                 */
                bin.setKnownDeletedAndEvictLN(index);
            }
        }

        assert logItem != null;

        if (lockStanding == null) {
            /*
             * No slot reuse; straight insertion. Update LSN in BIN slot.
             * The LN is already in the slot.
             */
            bin.updateEntry(
                index, logItem.lsn, ln.getVLSNSequence(),
                logItem.size);

            bin.setExpiration(
                index, expInfo.expiration, expInfo.expirationInHours);

            /*
             * The following call accounts for extra marshaled memory, i.e.,
             * memory that was added to the LN as a side-effect of logging it.
             * This can happen for FileSummaryLN's only (it is a noop for
             * other kinds of LNs).
             *
             * To avoid violating assertions (e.g., in IN.changeMemorySize), we
             * must must finish the memory adjustment while the BIN is still
             * latched. [#20069]
             *
             * This special handling does not apply to slot reuse, because the
             * updateEntry() version used in the slot reuse case will recalc
             * the BIN memory from scratch, and as a result, will take into
             * account the extra marshaled memory. [#20845]
             */
            if (bin.getTarget(index) == ln) {
                ln.addExtraMarshaledMemorySize(bin);
            }

        } else {

            /*
             * Slot reuse. When reusing a slot, the key is replaced in the BIN
             * slot. This ensures that the correct key value is used when the
             * new key is non-identical to the key in the slot but is
             * considered equal by the btree comparator.
             */
            bin.insertRecord(
                index, ln, logItem.lsn, logItem.size, key, data,
                expInfo.expiration, expInfo.expirationInHours);
        }

        if (returnNewData != null) {
            returnNewData.setData(null);
            ln.setEntry(returnNewData);
        }

        /* Cursor is positioned on new record. */
        setInitialized();

        /* Cache record version/size for insertion operation. */
        setCurrentVersion(ln.getVLSNSequence(), bin.getLsn(index));
        setStorageSize();

        /*
         * It is desirable to evict the LN in a duplicates DB because it will
         * never be fetched again. But for deferred-write DBs we should not
         * evict a dirty LN since it may be logged unnecessarily.
         */
        if (dbImpl.getSortedDuplicates() &&
            !dbImpl.isDeferredWriteMode() &&
            bin.getTarget(index) != null) {
            bin.evictLN(index);
        }

        traceInsert(Level.FINER, bin, logItem.lsn, index);

        return new Pair<>(
            lockStanding,
            DbInternal.makeResult(
                expInfo.expiration, expInfo.expirationInHours));
    }

    /**
     * Update the record where the cursor is currently positioned at. The
     * cursor is registered with this position, the associated bin is latched,
     * the BIN slot is not defunct, and it has been locked in WRITE mode.
     *
     * @param returnOldData if non-null, will be filled in with the
     * pre-existing record's data. However, if an expired LN was purged, it
     * will not be filled in and the caller should expect this; see {@link
     * Cursor#putNotify}.
     *
     * @return OperationResult, or null if an expired LN was purged and a
     * partial 'data' param was supplied.
     */
    private OperationResult updateRecordInternal(
        final byte[] key,
        final DatabaseEntry data,
        final ExpirationInfo expInfo,
        final DatabaseEntry returnOldData,
        final DatabaseEntry returnNewData,
        final LockStanding lockStanding,
        final ReplicationContext repContext) {

        assert(lockStanding.recordExists());

        final EnvironmentImpl envImpl = dbImpl.getEnv();
        final DbType dbType = dbImpl.getDbType();

        final long currLsn = lockStanding.lsn;
        assert(currLsn != DbLsn.NULL_LSN);
        final int currLoggedSize = bin.getLastLoggedSize(index);
        final byte[] currKey = bin.getKey(index);
        final byte[] currData;

        final boolean currEmbeddedLN = bin.isEmbeddedLN(index);
        final boolean newEmbeddedLN;

        final LogItem logItem;

        /*
         * Must fetch LN if it is not embedded and any of the following
         * are true:
         *  - returnOldData is non-null: data needs to be returned
         *  - data is a partial entry: needs to be resolved
         *  - CLEANER_FETCH_OBSOLETE_SIZE is configured and lastLoggedSize
         *    is unknown
         *  - this database does not use the standard LN class and we
         *    cannot call DbType.createdUpdatedLN further below (this is
         *    the case for NameLNs, MapLNs, and FileSummaryLNs).
         * For other cases, we are careful not to fetch, in order to avoid
         * a random read during an update operation.
         */
        LN ln;
        if (returnOldData != null ||
            data.getPartial() ||
            (currLoggedSize == 0 &&
             !currEmbeddedLN &&
             envImpl.getCleaner().getFetchObsoleteSize(dbImpl)) ||
            !dbType.mayCreateUpdatedLN()) {

            if (currEmbeddedLN) {
                currData = bin.getData(index);
                ln = bin.getLN(index, cacheMode);
            } else {
                ln = bin.fetchLN(index, cacheMode);
                currData = (ln != null ? ln.getData() : null);
            }
        } else {
            ln = bin.getLN(index, cacheMode);
            currData = (ln != null ? ln.getData() : null);
        }

        final byte[] newData;
        if (data.getPartial()) {
            if (currData == null) {
                /* Expired LN was purged. Cannot use a partial entry. */
                return null;
            }
            newData = LN.resolvePartialEntry(data, currData);
        } else {
            newData = LN.copyEntryData(data);
        }

        /*
         * If the key is changed (according to the comparator), we assume
         * it is actually the data that has changed for a duplicate's DB.
         */
        if (key != null &&
            Key.compareKeys(
                currKey, key, dbImpl.getKeyComparator()) != 0) {
            throw new DuplicateDataException(
                "Can't replace a duplicate with new data that is not " +
                "equal to the existing data according to the duplicate " +
                " comparator.");
        }

        if (returnOldData != null && currData != null) {
            returnOldData.setData(null);
            LN.setEntry(returnOldData, currData);
        }

        newEmbeddedLN = shouldEmbedLN(newData);

        /* Update the existing LN, if cached, else create new LN. */
        final long oldLNMemSize;
        if (ln != null) {
            oldLNMemSize = ln.getMemorySizeIncludedByParent();
            ln.modify(newData);
        } else {
            oldLNMemSize = 0;
            ln = dbType.createUpdatedLN(envImpl, newData);
        }

        final int oldExpiration = bin.getExpiration(index);
        final boolean oldExpirationInHours = bin.isExpirationInHours();

        if (expInfo != null) {
            expInfo.setOldExpirationTime(
                TTL.expirationToSystemTime(
                    oldExpiration, oldExpirationInHours));
        }

        final int expiration;
        final boolean expirationInHours;

        if (expInfo != null && expInfo.updateExpiration) {
            if (expInfo.expiration != oldExpiration ||
                expInfo.expirationInHours != oldExpirationInHours) {
                expInfo.setExpirationUpdated(true);
            }
            expiration = expInfo.expiration;
            expirationInHours = expInfo.expirationInHours;
        } else {
            expiration = oldExpiration;
            expirationInHours = oldExpirationInHours;
        }

        /*
         * Create a new WriteLockInfo or get an existing one for the LSN
         * of the current slot, and set its abortLSN and abortKD fields,
         * if needed, i.e, if it is not the current txn the one who created
         * this LSN. The abortLSN and abortKD fields of the wli will be
         * included in the new logrec.
         */
        final WriteLockInfo wli = lockStanding.prepareForUpdate(bin, index);
        
        /* Log the new record version and lock its new LSN . */
        logItem = ln.optionalLog(
            envImpl, dbImpl, locker, wli,
            newEmbeddedLN, (key != null ? key : currKey),
            expiration, expirationInHours,
            currEmbeddedLN, currLsn, currLoggedSize,
            false/*isInsertion*/, repContext);

        /* Return a copy of resulting data, if requested. [#16932] */
        if (returnNewData != null) {
            returnNewData.setData(null);
            ln.setEntry(returnNewData);
        }

        /*
         * Update the parent BIN. Update the key, if changed. [#15704]
         */
        bin.updateRecord(
            index, oldLNMemSize, logItem.lsn, ln.getVLSNSequence(),
            logItem.size, key, (newEmbeddedLN ? newData : null),
            expiration, expirationInHours);

        /*
         * If the LN child is not cached, attach it to the tree if the DB
         * is a DW one or if the record is not embedded in the BIN. For
         * DW DBs, we must attach the LN even if the record in embedded,
         * because no logrec was generated above, and as a result, the LN
         * must be in the tree so that a logrec will be generated when
         * a db.sync() occurs later (that logrec is needed for crash
         * recovery, because BINs are not replayed during crash recovery).
         *
         * If the LN child is cached, it is desirable to evict it if the
         * record is embedded because it will never be fetched again.
         * But for DW DBs we should not evict a dirty LN since it will
         * be logged unnecessarily.
         */
        final boolean shouldCache =
            (dbImpl.isDeferredWriteMode() ||
             (!dbImpl.getSortedDuplicates() && !newEmbeddedLN));

        if (bin.getTarget(index) == null) {
            if (shouldCache) {
                bin.attachNode(index, ln, null /*lnKey*/);
            }
        } else {
            if (!shouldCache) {
                bin.evictLN(index);
            }
        }

        /* Cache record version/size for update operation. */
        setCurrentVersion(ln.getVLSNSequence(), logItem.lsn);
        setStorageSize();

        trace(Level.FINER, TRACE_MOD, bin, index, currLsn, logItem.lsn);

        return DbInternal.makeUpdateResult(expiration, expirationInHours);
    }

    /**
     * Position the cursor at the first or last record of the dbImpl.
     * It's okay if this record is defunct.
     *
     * The cursor must initially be uninitialized.
     *
     * Returns with the target BIN latched!
     *
     * @return true if a first or last position is found, false if the
     * tree being searched is empty.
     */
    public boolean positionFirstOrLast(boolean first) {

        assert assertCursorState(
            false /*mustBeInitialized*/, true /*mustNotBeInitialized*/);

        boolean found = false;

        try {
            if (first) {
                bin = dbImpl.getTree().getFirstNode(cacheMode);
            } else {
                bin = dbImpl.getTree().getLastNode(cacheMode);
            }

            if (bin != null) {

                TreeWalkerStatsAccumulator treeStatsAccumulator =
                    getTreeStatsAccumulator();

                if (bin.getNEntries() == 0) {

                    /*
                     * An IN was found. Even if it's empty, let Cursor
                     * handle moving to the first non-defunct entry.
                     */
                    found = true;
                    index = -1;
                } else {
                    index = (first ? 0 : (bin.getNEntries() - 1));

                    if (treeStatsAccumulator != null &&
                        !bin.isEntryKnownDeleted(index) &&
                        !bin.isEntryPendingDeleted(index)) {
                        treeStatsAccumulator.incrementLNCount();
                    }

                    /*
                     * Even if the entry is defunct, just leave our
                     * position here and return.
                     */
                    found = true;
                }
            }

            addCursor(bin);
            setInitialized();

            return found;
        } catch (final Throwable e) {
            /* Release latch on error. */
            releaseBIN();
            throw e;
        }
    }

    /**
     * Position this cursor on the slot whose key is the max key less or equal
     * to the given search key.
     *
     * To be more precise, let K1 be search key. The method positions the
     * cursor on the BIN that should contain K1. If the BIN does contain K1,
     * this.index is set to the containing slot. Otherwise, this.index is
     * set to the right-most slot whose key is < K1, or to -1 if K1 is < than
     * all keys in the BIN.
     *
     * The cursor must initially be uninitialized.
     *
     * The method returns with the BIN latched, unless an exception is raised.
     *
     * The method returns an integer that encodes the search outcome: If the
     * FOUND bit is not set, the tree is completely empty (has no BINs). If
     * the FOUND bit is set, the EXACT_KEY bit says whether K1 was found or
     * not and the FOUND_LAST bit says whether the cursor is positioned to the
     * very last slot of the BTree (note that this state can only be counted
     * on as long as the BIN is latched). 
     *
     * Even if the search returns an exact result, the record may be defunct.
     * The caller must therefore check  whether the cursor is positioned on a
     * defunct record.
     *
     * This method does not lock the record. The caller is expected to call
     * lockAndGetCurrent to perform locking.
     */
    public int searchRange(
        DatabaseEntry searchKey,
        Comparator<byte[]> comparator) {

        assert assertCursorState(
            false /*mustBeInitialized*/, true /*mustNotBeInitialized*/);

        boolean foundSomething = false;
        boolean foundExactKey = false;
        boolean foundLast = false;
        BINBoundary binBoundary = new BINBoundary();

        try {
            byte[] key = Key.makeKey(searchKey);

            bin = dbImpl.getTree().search(
                key, Tree.SearchType.NORMAL, binBoundary, cacheMode,
                comparator);

            if (bin != null) {

                foundSomething = true;
                if (bin.isBINDelta() && comparator != null) {

                    /*
                     * We must mutate a BIN delta if a non-null comparator is
                     * used. Otherwise, if we positioned the cursor on the
                     * delta using the non-null comparator, we would not be
                     * able to adjust its position correctly later when the
                     * delta gets mutated for some reason (because at that
                     * later time, the comparator used here would not be
                     * known).
                     */
                    bin.mutateToFullBIN(false /*leaveFreeSlot*/);
                }

                index = bin.findEntry(
                    key, true /*indicateIfExact*/, false/*exact*/, comparator);

                if (bin.isBINDelta() &&
                    (index < 0 ||
                     (index & IN.EXACT_MATCH) == 0 ||
                     binBoundary.isLastBin)) {

                    /*
                     * Note: if binBoundary.isLastBin, we must mutate the BIN
                     * in order to compute the foundLast flag below.
                     */
                    bin.mutateToFullBIN(false /*leaveFreeSlot*/);
                    index = bin.findEntry(key, true, false, comparator);
                }

                if (index >= 0) {
                    if ((index & IN.EXACT_MATCH) != 0) {
                        foundExactKey = true;
                        index &= ~IN.EXACT_MATCH;
                    }

                    foundLast = (binBoundary.isLastBin &&
                                 index == bin.getNEntries() - 1);
                }

                /*
                 * Must call addCursor after mutateToFullBIN() to avoid having
                 * to reposition "this" inside mutateToFullBIN(), which would
                 * be both unnecessary and wrong given that this.index could
                 * have the IN.EXACT_MATCH still on.
                 */
                addCursor(bin);
            }

            setInitialized();

            /* Return a multi-part status value */
            return ((foundSomething ? FOUND : 0) |
                    (foundExactKey ? EXACT_KEY : 0) |
                    (foundLast ? FOUND_LAST : 0));

        } catch (final Throwable e) {
            releaseBIN();
            throw e;
        }
    }

    public boolean searchExact(DatabaseEntry searchKey, LockType lockType) {
        return searchExact(searchKey, lockType, false, false) != null;
    }

    /**
     * Position this cursor on the slot (if any) whose key matches the given
     * search key. If no such slot is found or the slot does not hold a "valid"
     * record, return null. Otherwise, lock the found record with the specified
     * lock type (which may be NONE) and return the LockStanding obj that was
     * created by the locking op. Whether the slot contains a "valid" record or
     * not depends on the slot's KD/PD flags and the lockType and dirtyReadAll
     * parameters. Four cases are considered; they are described in the
     * lockLNAndCheckDefunct() method.
     *
     * The cursor must initially be uninitialized.
     *
     * The method returns with the BIN latched, unless an exception is raised.
     *
     * In all cases, the method registers the cursor with the BIN that contains
     * or should contain the search key.
     *
     * @return the LockStanding for the found record, or null if no record was
     * found.
     */
    public LockStanding searchExact(
        final DatabaseEntry searchKey,
        final LockType lockType,
        final boolean dirtyReadAll,
        final boolean dataRequested) {

        assert assertCursorState(
            false /*mustBeInitialized*/, true /*mustNotBeInitialized*/);

        LockStanding lockStanding = null;

        try {
            byte[] key = Key.makeKey(searchKey);

            bin = dbImpl.getTree().search(key, cacheMode);

            if (bin != null) {

                index = bin.findEntry(key, false, true /*exact*/);

                if (index < 0 && bin.isBINDelta()) {

                    if (bin.mayHaveKeyInFullBin(key)) {
                        bin.mutateToFullBIN(false /*leaveFreeSlot*/);
                        index = bin.findEntry(key, false, true /*exact*/);
                    }
                }

                addCursor(bin);

                if (index >= 0) {
                    lockStanding = lockLNAndCheckDefunct(
                        lockType, dirtyReadAll, dataRequested);
                }
            }

            setInitialized();
            return lockStanding;

        } catch (final Throwable e) {
            /* Release latch on error. */
            releaseBIN();
            throw e;
        }
    }

    /**
     * Lock and copy current record into the key and data DatabaseEntry.
     * When calling this method, this.bin should not be latched already.
     * On return, this.bin is unlatched.
     */
    public OperationResult lockAndGetCurrent(
        DatabaseEntry foundKey,
        DatabaseEntry foundData,
        final LockType lockType) {

        return lockAndGetCurrent(
            foundKey, foundData, lockType, false, false, true);
    }

    /**
     * Let S be the slot where this cursor is currently positioned on. If S
     * does not hold a "valid" record, return null. Otherwise, lock the
     * record in S with the specified lock type(which may be NONE), copy its
     * key and data into the key and data DatabaseEntries, and return SUCCESS.
     * Whether the slot contains a "valid" record or not depends on the slot's
     * KD/PD flags, the lockType and dirtyReadAll parameters, and whether the
     * record has expired. For details see {@link #lockLNAndCheckDefunct}.
     *
     * On entry, the isLatched param says whether this.bin is latched or not.
     * On return, this.bin is unlatched if the unlatch param is true or an
     * exception is thrown.
     *
     * @return OperationResult, or null if the LN has been cleaned and cannot
     * be fetched.
     */
    public OperationResult lockAndGetCurrent(
        DatabaseEntry foundKey,
        DatabaseEntry foundData,
        final LockType lockType,
        final boolean dirtyReadAll,
        final boolean isLatched,
        final boolean unlatch) {

        /* Used in the finally to indicate whether exception was raised. */
        boolean success = false;

        try {
            assert assertCursorState(
                true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

            assert checkAlreadyLatched(isLatched) : dumpToString(true);

            if (!isLatched) {
                latchBIN();
            }

            assert(bin.getCursorSet().contains(this));

            TreeWalkerStatsAccumulator treeStatsAccumulator =
                getTreeStatsAccumulator();

            /*
             * If we encounter a deleted slot, opportunistically add the BIN
             * to the compressor queue. We do not queue expired slots to avoid
             * frequent compression, especially in the CRUD path; we rely
             * instead on the evictor to perform expired slot compression.
             */
            if (index >= 0 &&
                index < bin.getNEntries() &&
                bin.isDeleted(index)) {
                bin.queueSlotDeletion(index);
            }

            /*
             * Check the KD flag in the BIN slot and make sure this isn't an
             * empty BIN. The BIN could be empty by virtue of the compressor
             * running the size of this BIN to 0 but not having yet removed
             * it from the tree.
             *
             * The index may be negative if we're at an intermediate stage in
             * an higher level operation (e.g., the starting search for a range
             * scan op), and we expect a higher level method to do a next or
             * prev operation after this returns KEYEMPTY. [#11700]
             */
            if (index < 0 ||
                index >= bin.getNEntries() ||
                bin.isEntryKnownDeleted(index)) {
                /* Node is no longer present. */
                if (treeStatsAccumulator != null) {
                    treeStatsAccumulator.incrementDeletedLNCount();
                }

                success = true;
                return null;
            }

            assert TestHookExecute.doHookIfSet(testHook);

            final boolean dataRequested =
                (foundData != null &&
                (!foundData.getPartial() ||
                 foundData.getPartialLength() != 0));

            if (lockLNAndCheckDefunct(
                lockType, dirtyReadAll, dataRequested) == null) {
                if (treeStatsAccumulator != null) {
                    treeStatsAccumulator.incrementDeletedLNCount();
                }
                success = true;
                return null;
            }

            final OperationResult result = getCurrent(foundKey, foundData);

            success = true;
            return result;

        } finally {
            if (unlatch || !success) {
                releaseBIN();
            }
        }
    }

    /**
     * Let S be the slot where this cursor is currently positioned on. The
     * method locks S (i.e. its LSN), and depending on S's KD/PD flags and
     * expired status, it returns either null or the LockStanding obj that was
     * created by the locking op. The following 4 cases are considered. By
     * "defunct" below we mean S is KD/PD or expired.
     *
     * 1. If S is not defunct, return the LockStanding obj. In this case, we
     * know that S holds a valid (non-defunct) record.
     *
     * 2. If S is defunct, and the lock type is not NONE, return null. In this
     * case, we know that the record that used to be in S is definitely defunct.
     *
     * 3. If S is defunct, the lock kind is NONE, and dirtyReadAll is false,
     * return null. This case corresponds to the READ_UNCOMMITTED LockMode.
     * The record in S is defunct, but the deleting txn may be active still,
     * and if it aborts later, the record will be restored. To avoid a
     * potentially blocking lock, in READ_UNCOMMITTED mode we consider the
     * record to be non-existing and return null.
     *
     * 4. If S is defunct, the lock kind is NONE, and dirtyReadAll is true,
     * lock the record in READ mode. This case corresponds to the
     * READ_UNCOMMITTED_ALL LockMode, which requires that we do not skip
     * "provisionally defunct" records. There are two sub-cases:
     *
     *  4a. If dataRequested is true, we wait until the deleting txn finishes.
     *      In this case the READ lock is blocking. If after the lock is
     *      granted S is still defunct, release the lock and return null.
     *      Otherwise, release the lock and return the LockStanding obj.
     *
     *  4b. If dataRequested is false, then we check whether the deleting txn is
     *      still open by requested a non-blocking READ lock. If the lock is
     *      granted then the writing txn is closed or this cursor's locker is
     *      the writer, and we proceed as if the READ lock was granted in 4a.
     *      If the lock is denied then the deleting txn is still open, and we
     *      return the LockStanding obj so that the record is not skipped.
     *
     * The BIN must be latched on entry and is latched on exit.
     *
     * @param dirtyReadAll is true if using LockMode.READ_UNCOMMITTED_ALL.
     *
     * @param dataRequested is true if the read operation should return the
     * record data, meaning that a blocking lock must be used for dirtyReadAll.
     * Is ignored if dirtyReadAll is false. Is always false for a dup DB,
     * since data is never requested for dup DB ops at the CursorImpl level.
     */
    private LockStanding lockLNAndCheckDefunct(
        final LockType lockType,
        final boolean dirtyReadAll,
        final boolean dataRequested) {

        assert !(dirtyReadAll && lockType != LockType.NONE);
        assert !(dataRequested && dbImpl.getSortedDuplicates());

        LockStanding standing = lockLN(lockType);

        if (standing.recordExists()) {
            return standing;
        }

        /* The slot is defunct. */

        if (lockType != LockType.NONE) {
            revertLock(standing);

            /*
             * The record was committed by another locker, or has been
             * performed by this locker.
             */
            return null;
        }

        /* We're using dirty-read.  The lockLN above did not actually lock. */

        if (!dirtyReadAll) {
            /* READ_UNCOMMITTED -- skip defunct records without locking. */
            return null;
        }

        /*
         * READ_UNCOMMITTED_ALL -- get a read lock. Whether we can request a
         * no-wait or a blocking lock depends on the dataRequested parameter.
         *
         * Although there is some redundant processing in the sense that lockLN
         * is called more than once (above and below), this is not considered a
         * performance issue because accessing defunct records is normally
         * infrequent. Deleted slots are normally compressed away quickly.
         */
        standing = lockLN(
            LockType.READ, false /*allowUncontended*/,
            !dataRequested /*noWait*/);

        if (standing.lockResult.getLockGrant() == LockGrantType.DENIED) {

            /*
             * The no-wait lock request was denied, which means the data is not
             * needed and the writing transaction is still open. The defunct
             * record should not be skipped in this case, according to the
             * definition of READ_UNCOMMITTED_ALL.
             */
            assert !standing.recordExists();
            return standing;
        }

        /* We have acquired a temporary read lock. */
        revertLock(standing);

        if (standing.recordExists()) {
            /*
             * Another txn aborted the deletion or expiration time change while
             * we waited.
             */
            return standing;
        }

        /*
         * The write was committed by another locker, or has been performed by
         * this locker.
         */
        return null;
    }

    /**
     * Copy current record into the key and data DatabaseEntry.
     *
     * @return OperationResult, or null if the LN has been cleaned and cannot
     * be fetched.
     */
    public OperationResult getCurrent(
        final DatabaseEntry foundKey,
        final DatabaseEntry foundData) {

        assert(bin.isLatchExclusiveOwner());
        assert(index >= 0 && index < bin.getNEntries());
        assert(!bin.isEntryKnownDeleted(index));

        /*
         * We don't need to fetch the LN if the user has not requested that we
         * return the data, or if we know for sure that the LN is empty.
         */
        final boolean isEmptyLN = dbImpl.isLNImmediatelyObsolete();
        final boolean isEmbeddedLN = bin.isEmbeddedLN(index);

        final boolean dataRequested = 
            (foundData != null &&
             (!foundData.getPartial() || foundData.getPartialLength() != 0));

        final LN ln;
        if (!isEmptyLN && !isEmbeddedLN && dataRequested) {
            ln = bin.fetchLN(index, cacheMode);
            if (ln == null) {
                /* An expired LN was purged. */
                return null;
            }
        } else {
            ln = null;
        }

        /* Return the data. */
        if (dataRequested) {

            byte[] data;

            if (ln != null) {
                data = ln.getData();
            } else if (isEmptyLN || bin.isNoDataLN(index)) {
                data = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                assert(isEmbeddedLN);
                data = bin.getData(index);
            }

            LN.setEntry(foundData, data);
        }

        /* Return the key */
        if (foundKey != null) {
            LN.setEntry(foundKey, bin.getKey(index));
        }

        /* Cache record version/size for fetch operation. */
        final long vlsn = (ln != null ?
                           ln.getVLSNSequence() :
                           bin.getVLSN(index, false /*allowFetch*/, cacheMode));

        setCurrentVersion(vlsn, bin.getLsn(index));
        setStorageSize();

        return DbInternal.makeResult(
            bin.getExpiration(index), bin.isExpirationInHours());
    }

    public LN getCurrentLN(final boolean isLatched, final boolean unlatch) {

        /* Used in the finally to indicate whether exception was raised. */
        boolean success = false;

        try {
            assert assertCursorState(
                true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);
            assert checkAlreadyLatched(isLatched) : dumpToString(true);

            if (!isLatched) {
                latchBIN();
            }

            assert(bin.getCursorSet().contains(this));
            assert(!bin.isEmbeddedLN(index));

            LN ln = bin.fetchLN(index, cacheMode);

            success = true;
            return ln;
        } finally {
            if (unlatch || !success) {
                releaseBIN();
            }
        }
    }

    /**
     * Retrieve the current LN. BIN is unlatched on entry and exit.
     */
    public LN lockAndGetCurrentLN(final LockType lockType) {

        try {
            assert assertCursorState(
                true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);
            assert checkAlreadyLatched(false) : dumpToString(true);

            latchBIN();

            assert(bin.getCursorSet().contains(this));

            LockStanding lockStanding = lockLN(lockType);

            if (!lockStanding.recordExists()) {
                revertLock(lockStanding);
                return null;
            }

            assert(!bin.isEmbeddedLN(index));

            return bin.fetchLN(index, cacheMode);
        } finally {
            releaseBIN();
        }
    }

    /**
     * Returns the VLSN and LSN for the record at the current position.  Must
     * be called when the cursor is positioned on a record.
     *
     * If this method is called on a secondary cursor, the version of the
     * associated primary record is returned.  In that case, the allowFetch
     * parameter is ignored, and the version is available only if the primary
     * record was retrieved (see setPriInfo).
     *
     * @param allowFetch is true to fetch the LN to get the VLSN, or false to
     * return -1 for the VLSN if both the LN and VLSN are not cached.
     *
     * @throws IllegalStateException if the cursor is closed or uninitialized,
     * or this is a secondary cursor and the version is not cached.
     */
    public RecordVersion getCurrentVersion(boolean allowFetch) {

        /* Ensure cursor is open and initialized. */
        checkCursorState(
            true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        /*
         * For a secondary cursor, the cached version is all we have.
         * See setPriInfo.
         */
        if (isSecondaryCursor) {
            if (currentRecordVersion == null) {
                throw new IllegalStateException(
                    "Record version is available via a SecondaryCursor only " +
                    "if the associated primary record was retrieved.");
            }
            return currentRecordVersion;
        }

        /*
         * Use cached version if available.  Do not use cached version if it
         * does not contain a VLSN, and VLSNs are preserved, and fetching is
         * allowed; instead, try to fetch it below.
         */
        if (currentRecordVersion != null) {
            if ((currentRecordVersion.getVLSN() !=
                 VLSN.NULL_VLSN_SEQUENCE) ||
                !allowFetch ||
                !dbImpl.getEnv().getPreserveVLSN()) {

                return currentRecordVersion;
            }
        }

        /* Get the VLSN from the BIN, create the version and cache it. */
        latchBIN();
        try {
            setCurrentVersion(
                bin.getVLSN(index, allowFetch, cacheMode), bin.getLsn(index));
        } finally {
            releaseBIN();
        }
        return currentRecordVersion;
    }

    private void setCurrentVersion(long vlsn, long lsn) {
        currentRecordVersion = new RecordVersion(vlsn, lsn);
    }

    /**
     * Returns the estimated disk storage size for the record at the current
     * position. The size includes an estimation of the JE overhead for the
     * record, in addition to the user key/data sizes. But it does not include
     * obsolete overhead related to the record, i.e., space that could
     * potentially be reclaimed by the cleaner.
     *
     * <p>This method does not fetch the LN. Must be called when the
     * cursor is positioned on a record.</p>
     *
     * <p>When called on a secondary cursor that was used to return the primary
     * data, the size of the primary record is returned by this method.
     * Otherwise the size of the record at this cursor position is
     * returned.</p>
     *
     * @return the estimated storage size, or zero when the size is unknown
     * because a non-embedded LN is not resident and the LN was logged with a
     * JE version prior to 6.0.
     *
     * @throws IllegalStateException if the cursor is closed or uninitialized.
     *
     * @see StorageSize
     */
    public int getStorageSize() {

        assert assertCursorState(
            true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        return (priStorageSize > 0) ? priStorageSize : storageSize;
    }

    private void setStorageSize() {
        storageSize = StorageSize.getStorageSize(bin, index);
    }

    /**
     * When the primary record is read during a secondary operation, this
     * method is called to copy the primary version and storage size here.
     * This allows the secondary cursor API to return the version and size of
     * the primary record. Note that a secondary record does not have a version
     * of its own.
     *
     * @param sourceCursor contains the primary info, but may be a primary or
     * secondary cursor.
     */
    public void setPriInfo(final CursorImpl sourceCursor) {
        currentRecordVersion = sourceCursor.currentRecordVersion;
        priStorageSize = sourceCursor.storageSize;
    }

    /**
     * Returns the number of secondary records written by the last put/delete
     * operation at the current cursor position.
     *
     * NOTE: this method does not work (returns 0) if primary deletions are
     * performed via a secondary (SecondaryDatabase/SecondaryCursor.delete).
     *
     * @return number of writes, or zero if a put/delete operation was not
     * performed.
     */
    public int getNSecondaryWrites() {
        return nSecWrites;
    }

    public void setNSecondaryWrites(final int nWrites) {
        nSecWrites = nWrites;
    }

    /**
     * Advance a cursor.  Used so that verify can advance a cursor even in the
     * face of an exception [12932].
     * @param key on return contains the key if available, or null.
     * @param data on return contains the data if available, or null.
     */
    public boolean advanceCursor(DatabaseEntry key, DatabaseEntry data) {

        BIN oldBin = bin;
        int oldIndex = index;

        key.setData(null);
        data.setData(null);

        try {
            getNext(
                key, data, LockType.NONE, false /*dirtyReadAll*/,
                true /*forward*/, false /*isLatched*/,
                null /*rangeConstraint*/);
        } catch (DatabaseException ignored) {
            /* Klockwork - ok */
        }

        /*
         * If the position changed, regardless of an exception, then we believe
         * that we have advanced the cursor.
         */
        if (bin != oldBin || index != oldIndex) {

            /*
             * Return the key and data from the BIN entries, if we were not
             * able to read it above.
             */
            if (key.getData() == null && bin != null && index > 0) {
                LN.setEntry(key, bin.getKey(index));
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Move the cursor forward and return the next "valid" record. Whether a
     * slot contains a "valid" record or not depends on the slot's KD/PD flags
     * and the lockType and dirtyReadAll parameters. Four cases are considered;
     * they are described in the lockLNAndCheckDefunct() method.
     *
     * This will cross BIN boundaries. On return, no latches are held. If no
     * exceptions, the cursor is registered with its new location.
     *
     * @param foundKey DatabaseEntry to use for returning key
     *
     * @param foundData DatabaseEntry to use for returning data
     *
     * @param forward if true, move forward, else move backwards
     *
     * @param isLatched if true, the bin that we're on is already
     * latched.
     *
     * @param rangeConstraint if non-null, is called to determine whether a key
     * is out of range.
     */
    public OperationResult getNext(
        DatabaseEntry foundKey,
        DatabaseEntry foundData,
        LockType lockType,
        boolean dirtyReadAll,
        boolean forward,
        boolean isLatched,
        RangeConstraint rangeConstraint) {

        assert assertCursorState(
            true /*mustBeInitialized*/, false /*mustNotBeInitialized*/);

        assert checkAlreadyLatched(isLatched) : dumpToString(true);

        OperationResult result = null;
        BIN anchorBIN = null;

        try {
            while (bin != null) {

                assert checkAlreadyLatched(isLatched) : dumpToString(true);

                if (!isLatched) {
                    latchBIN();
                    isLatched = true;
                }

                if (DEBUG) {
                    verifyCursor(bin);
                }

                bin.mutateToFullBIN(false /*leaveFreeSlot*/);

                /* Is there anything left on this BIN? */
                if ((forward && ++index < bin.getNEntries()) ||
                    (!forward && --index > -1)) {

                    if (rangeConstraint != null &&
                        !rangeConstraint.inBounds(bin.getKey(index))) {

                        result = null;
                        releaseBIN();
                        break;
                    }

                    OperationResult ret = lockAndGetCurrent(
                        foundKey, foundData, lockType, dirtyReadAll,
                        true /*isLatched*/, false /*unlatch*/);

                    if (LatchSupport.TRACK_LATCHES) {
                        LatchSupport.expectBtreeLatchesHeld(1);
                    }

                    if (ret != null) {
                        incrementLNCount();
                        releaseBIN();
                        result = ret;
                        break;
                    }
                } else {
                    /*
                     * Make sure that the current BIN will not be pruned away
                     * if it is or becomes empty after it gets unlatched by
                     * Tree.getNextBin() or Tree.getPrevBin(). The operation
                     * of these Tree methods relies on the current BIN not
                     * getting pruned. 
                     */
                    anchorBIN = bin;
                    anchorBIN.pin();
                    bin.removeCursor(this);
                    bin = null;

                    final Tree tree = dbImpl.getTree();

                    /* SR #12736 Try to prune away oldBin */
                    assert TestHookExecute.doHookIfSet(testHook);

                    if (forward) {
                        bin = tree.getNextBin(anchorBIN, cacheMode);
                        index = -1;
                    } else {
                        bin = tree.getPrevBin(anchorBIN, cacheMode);
                        if (bin != null) {
                            index = bin.getNEntries();
                        }
                    }
                    isLatched = true;

                    if (bin == null) {
                        if (LatchSupport.TRACK_LATCHES) {
                            LatchSupport.expectBtreeLatchesHeld(0);
                        }
                        result = null;
                        break;
                    } else {
                        if (LatchSupport.TRACK_LATCHES) {
                            LatchSupport.expectBtreeLatchesHeld(1);
                        }

                        addCursor();
                        anchorBIN.unpin();
                        anchorBIN = null;
                    }
                }
            }
        } finally {
            if (anchorBIN != null) {
                anchorBIN.unpin();
            }
        }

        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.expectBtreeLatchesHeld(0);
        }

        return result;
    }

    /**
     * Used to detect phantoms during "get next" operations with serializable
     * isolation.  If this method returns true, the caller should restart the
     * operation from the prior position.
     *
     * Something may have been added to the original cursor (cursorImpl) while
     * we were getting the next BIN.  cursorImpl would have been adjusted
     * properly but we would have skipped a BIN in the process.  This can
     * happen when all INs are unlatched in Tree.getNextBin.  It can also
     * happen without a split, simply due to inserted entries in the previous
     * BIN.
     *
     * @return true if an unaccounted for insertion happened.
     *
     * TODO:
     * Unfortunately, this method doesn't cover all cases where a phantom may
     * have been inserted.  Another case is described below.
     *
     *                        IN-0
     *                        ----------------------
     *                        |    | 50 | 100 |    |
     *                        ----------------------
     *                        /    /         \     \
     *                            /           \
     *                       /----             ----\
     *        IN-1          /                       \       IN-2
     *        ----------------                   ----------------
     *        | 60 | 70 | 80 |                   |    |    |    |
     *        ----------------                   ----------------
     *         /      |      \                     /
     *        /       |       \                   /
     *                   ----------------     -----------------
     *                   | 81 | 83 | 85 |     | 110 |    |    |
     *                   ----------------     -----------------
     *                   BIN-3                BIN-4
     *
     * Initially, the tree looks as above and a cursor (C) is located on the
     * last slot of BIN-3. For simplicity, assume no duplicates and no
     * serializable isolation. Also assume that C is a sticky cursor.
     *
     * 1. Thread 1 calls C.getNext(), which calls retrieveNextAllowPhantoms(),
     *    which duplicates C's cursorImpl, and calls dup.getNext().
     *
     *    dup.getNext() latches BIN-3, sets dup.binToBeRemoved to BIN-3 and
     *    then calls Tree.getNextBin(BIN-3).
     *
     *    Tree.getNextBin(BIN-3) does the following:
     *    - sets searchKey to 85
     *    - calls Tree.getParentINForChildIN(BIN-3).
     *    - Tree.getParentINForChildIN(BIN-3) unlatches BIN-3 and searches for
     *       BIN-3 parent, thus reaching IN-1.
     *    - IN-1.findEntry(85) sets "index" to 2,
     *    - "index" is incremented,
     *    - "moreEntriesThisIn" is set to false,
     *    - "next" is set to IN-1, .
     *    - Tree.getParentINForChildIN(IN-1) is called and unlatches IN-1.
     *
     *    Assume at this point thread 1 looses the cpu.
     *
     * 2. Thread 2 inserts keys 90 and 95, causing a split of both BIN-3 and
     *    IN-1. So the tree now looks like this:
     *
     *                       IN-0
     *                       ---------------------------
     *                       |    | 50 | 80 | 100 |    |
     *                       ---------------------------
     *                       /    /      |      \      \
     *                           /       |       \
     *                 /---------        |        ----------\
     *       IN-1     /                  |                   \       IN-2
     *               /              IN-5 |                    \
     *       -----------            -----------               ----------------
     *       | 60 | 70 |            | 80 | 90 |               |    |    |    |
     *       -----------            -----------               ----------------
     *        /      |              /         \                /
     *       /       |             /           \              /
     *                     ----------------  -----------   -----------------
     *                     | 81 | 83 | 85 |  | 90 | 95 |   | 110 |    |    |
     *                     ----------------  -----------   -----------------
     *                     BIN-3             BIN-6         BIN-4
     *
     *
     *    Notice that C.cursorImpl still points to the last slot of BIN-3.
     *
     * 3. Thread 1 resumes:
     *
     *    - Tree.getParentINForChildIN(IN-1) reaches IN-0.
     *    - IN-0.findEntry(85) sets "index" to 2,
     *    - "index" is incremented,
     *    - "nextIN" is set to IN-2, which is latched.
     *    - Tree.searchSubTree(IN-2, LEFT) is called, and returns BIN-4.
     *    - BIN-4 is the result of Tree.getNextBin(BIN-3), i.e., BIN-6 was
     *      skipped
     *
     *      Now we are back in dup.getNext():
     *    - dup.bin is set to BIN-4, dup.index to -1, and dup is added to BIN-4
     *    - the while loop repeats, dup.index is set to 0, the 1st slot of
     *      BIN-4 is locked, and dup.getNext() returns SUCCESS.
     *
     *      Now we are back in C.retrieveNextAllowPhantoms():
     *    - C.checkForInsertion() is called
     *    - C.cursorImpl and dup are on different BINs, but the condition:
     *      origBIN.getNEntries() - 1 > origCursor.getIndex()
     *      is false, so C.checkForInsertion() returns false.
     *
     * The end result is that BIN-6 has been missed. This is not be a "bug" for
     * non-serializable isolation, but the above scenario applies to
     * serializable isolation as well, and in that case, BIN-6 should really
     * not be missed. This could be solved by re-implementing
     * Tree.getNext/PrevBIN() do a more "logical" kind of search.
     */
    public boolean checkForInsertion(
        final GetMode getMode,
        final CursorImpl dupCursor) {

        final CursorImpl origCursor = this;
        boolean forward = getMode.isForward();
        boolean ret = false;

        if (origCursor.bin != dupCursor.bin) {

            /*
             * We jumped to the next BIN during getNext().
             *
             * Be sure to operate on the BIN returned by latchBIN, not a cached
             * var [#21121].
             *
             * Note that a cursor BIN can change after the check above, but
             * that's not relevant; what we're trying to detect are BIN changes
             * during the operation that has already completed.
             *
             * Note that we can call isDefunct without locking.  If we see a
             * non-committed defunct entry, we'll just iterate around in the
             * caller. So a false positive is ok.
             */
            origCursor.latchBIN();
            final BIN origBIN = origCursor.bin;

            origBIN.mutateToFullBIN(false /*leaveFreeSlot*/);

            try {
                if (forward) {
                    if (origBIN.getNEntries() - 1 > origCursor.getIndex()) {

                        /*
                         * We were adjusted to something other than the
                         * last entry so some insertion happened.
                         */
                        for (int i = origCursor.getIndex() + 1;
                             i < origBIN.getNEntries();
                             i++) {
                            if (!origBIN.isDefunct(i)) {
                                /* See comment above about locking. */
                                ret = true;
                                break;
                            }
                        }
                    }
                } else {
                    if (origCursor.getIndex() > 0) {

                        /*
                         * We were adjusted to something other than the
                         * first entry so some insertion happened.
                         */
                        for (int i = 0; i < origCursor.getIndex(); i++) {
                            if (!origBIN.isDefunct(i)) {
                                /* See comment above about locking. */
                                ret = true;
                                break;
                            }
                        }
                    }
                }
            } finally {
                origCursor.releaseBIN();
            }
            return ret;
        }
        return false;
    }

    /**
     * Skips over entries until a boundary condition is satisfied, either
     * because maxCount is reached or RangeConstraint.inBounds returns false.
     *
     * If a maxCount is passed, this allows advancing the cursor quickly by N
     * entries.  If a rangeConstraint is passed, this allows returning the
     * entry count after advancing until the predicate returns false, e.g., the
     * number of entries in a key range.  In either case, the number of entries
     * advanced is returned.
     *
     * Optimized to scan using level two of the tree when possible, to avoid
     * calling getNextBin/getPrevBin for every BIN of the database.  All BINs
     * beneath a level two IN can be skipped quickly, with the level two parent
     * IN latched, when all of its children BINs are resident and can be
     * latched without waiting.  When a child BIN is not resident or latching
     * waits, we revert to the getNextBin/getPrevBin approach, to avoid keeping
     * the parent IN latched for long time periods.
     *
     * Although this method positions the cursor on the last non-defunct entry
     * seen (before the boundary condition is satisfied), because it does not
     * lock the LN it is possible that it is made defunct by another thread
     * after the BIN is unlatched.
     *
     * @param forward is true to skip forward, false to skip backward.
     *
     * @param maxCount is the maximum number of non-defunct entries to skip,
     * and may be LTE zero if no maximum is enforced.
     *
     * @param rangeConstraint is a predicate that returns false at a position
     * where advancement should stop, or null if no predicate is enforced.
     *
     * @return the number of non-defunct entries that were skipped.
     */
    public long skip(
        boolean forward,
        long maxCount,
        RangeConstraint rangeConstraint) {

        final CursorImpl c = cloneCursor(true /*samePosition*/);
        c.setCacheMode(CacheMode.UNCHANGED);

        try {
            return c.skipInternal(forward, maxCount, rangeConstraint, this);
        } catch (final Throwable e) {
            /*
             * Get more info on dbsim duplicate.conf failure when c.close below
             * throws because the BIN latch is already held.  It should have
             * been released by skipInternal and therefore an unexpected
             * exception must have been throw and the error handling must be
             * incorrect.
             */
            e.printStackTrace(System.out);
            throw e;
        } finally {
            c.close();
        }
    }

    /**
     * Use this cursor to reference the current BIN in the traversal, to
     * prevent the current BIN from being compressed away.  But set the given
     * finalPositionCursor (the 'user' cursor) position only at non-defunct
     * entries, since it should be positioned on a valid entry when this method
     * returns.
     */
    private long skipInternal(
        boolean forward,
        long maxCount,
        RangeConstraint rangeConstraint,
        CursorImpl finalPositionCursor) {

        /* Start with the entry at the cursor position. */
        final Tree tree = dbImpl.getTree();
        
        latchBIN();

        IN parent = null;
        BIN prevBin = null;
        BIN curBin = bin;
        int curIndex = getIndex();
        long count = 0;
        boolean success = false;

        try {
            while (true) {
                curBin.mutateToFullBIN(false /*leaveFreeSlot*/);

                /* Skip entries in the current BIN. */
                count = skipEntries(
                    forward, maxCount, rangeConstraint, finalPositionCursor,
                    curBin, curIndex, count);

                if (count < 0) {
                    curBin.releaseLatch();
                    success = true;
                    return (- count);
                }

                /*
                 * Get the parent IN at level two. The BIN is unlatched by
                 * getParentINForChildIN.  Before releasing the BIN latch, get
                 * the search key for the last entry.
                 */
                final byte[] idKey =
                    (curBin.getNEntries() == 0 ?
                     curBin.getIdentifierKey() :
                     (forward ?
                      curBin.getKey(curBin.getNEntries() - 1) :
                      curBin.getKey(0)));

                final SearchResult result = tree.getParentINForChildIN(
                    curBin, false, /*useTargetLevel*/
                    true, /*doFetch*/ CacheMode.DEFAULT);

                parent = result.parent;

                if (!result.exactParentFound) {
                    throw EnvironmentFailureException.unexpectedState(
                        "Cannot get parent of BIN id=" +
                        curBin.getNodeId() + " key=" +
                        Arrays.toString(idKey));
                }

                /*
                 * Find and latch previous child BIN by matching idKey rather
                 * than using result.index, as in Tree.getNextIN (see comments
                 * there).
                 */
                int parentIndex = parent.findEntry(idKey, false, false);

                curBin = (BIN) parent.fetchIN(parentIndex, CacheMode.DEFAULT);
                curBin.latch();

                if (forward ?
                    (parentIndex < parent.getNEntries() - 1) :
                    (parentIndex > 0)) {
                    
                    /*
                     * There are more entries in the parent. Skip entries for
                     * child BINs that are resident and can be latched no-wait.
                     */
                    final int incr = forward ? 1 : (-1);

                    for (parentIndex += incr;; parentIndex += incr) {

                        prevBin = curBin;
                        curBin = null;

                        /* Break is no more entries in parent. */
                        if ((forward ?
                             parentIndex >= parent.getNEntries() :
                             parentIndex < 0)) {
                            parent.releaseLatch();
                            break;
                        }

                        /*
                         * Latch next child BIN, if cached and unlatched.
                         *
                         * Note that although 2 BINs are latched here, this
                         * can't cause deadlocks because the 2nd latch is
                         * no-wait.
                         */
                        curBin = (BIN) parent.getTarget(parentIndex);

                        if (curBin == null ||
                            !curBin.latchNoWait(CacheMode.DEFAULT)) {
                            parent.releaseLatch();
                            break;
                        }

                        /* Unlatch the prev BIN */
                        prevBin.releaseLatch();
                        prevBin = null;

                        /* Position at new BIN to prevent compression. */
                        setPosition(curBin, -1);

                        curBin.mutateToFullBIN(false /*leaveFreeSlot*/);

                        /* Skip entries in new child BIN. */
                        count = skipEntries(
                            forward, maxCount, rangeConstraint,
                            finalPositionCursor, curBin,
                            forward ? (-1) : curBin.getNEntries(), count);

                        if (count < 0) {
                            parent.releaseLatch();
                            curBin.releaseLatch();
                            success = true;
                            return (- count);
                        }
                    }
                } else {
                    /* No more entries in the parent. */
                    parent.releaseLatch();
                    prevBin = curBin;
                }

                /*
                 * Only the prevBin is still latched here. Move to the next
                 * BIN the "hard" way (i.e., via full tree searches).
                 */
                curBin = forward ?
                    tree.getNextBin(prevBin, CacheMode.DEFAULT) :
                    tree.getPrevBin(prevBin, CacheMode.DEFAULT);

                assert(!prevBin.isLatchOwner());

                if (curBin == null) {
                    success = true;
                    return count;
                }

                prevBin = null;
                curIndex = forward ? (-1) : curBin.getNEntries();

                /* Position at new BIN to prevent compression. */
                setPosition(curBin, -1);
            }
        } finally {
            if (curBin != null && !success) {
                curBin.releaseLatchIfOwner();
            }
            if (prevBin != null && !success) {
                prevBin.releaseLatchIfOwner();
            }
            if (parent != null && !success) {
                parent.releaseLatchIfOwner();
            }

            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.expectBtreeLatchesHeld(0);
            }
        }
    }

    /**
     * Skip entries in curBin from one past curIndex and onward.  Returns
     * non-negative count if skipping should continue, or negative count if
     * bounds is exceeded.
     */
    private long skipEntries(
        boolean forward,
        long maxCount,
        RangeConstraint rangeConstraint,
        CursorImpl finalPositionCursor,
        BIN curBin,
        int curIndex,
        long count) {
        
        assert(!curBin.isBINDelta());

        final int incr = forward ? 1 : (-1);
        
        for (int i = curIndex + incr;; i += incr) {
            if (forward ? (i >= curBin.getNEntries()) : (i < 0)) {
                break;
            }
            if (rangeConstraint != null &&
                !rangeConstraint.inBounds(curBin.getKey(i))) {
                return (- count);
            }
            if (!curBin.isDefunct(i)) {
                count += 1;
                finalPositionCursor.setPosition(curBin, i);
                if (maxCount > 0 && count >= maxCount) {
                    return (- count);
                }
            }
        }
        return count;
    }

    /**
     * Returns the stack of ancestor TrackingInfo for the BIN at the cursor, or
     * null if a split occurs and the information returned would be
     * inconsistent.
     *
     * Used by CountEstimator.
     */
    public List<TrackingInfo> getAncestorPath() {

        /*
         * Search for parent of BIN, get TrackingInfo for ancestors.  If the
         * exact parent is not found, a split occurred and null is returned.
         */
        final List<TrackingInfo> trackingList = new ArrayList<>();
        
        latchBIN();

        final BIN origBin = bin;
        final Tree tree = dbImpl.getTree();

        final SearchResult result = tree.getParentINForChildIN(
            origBin, false, /*useTargetLevel*/
            true /*doFetch*/, CacheMode.UNCHANGED, trackingList);

        if (!result.exactParentFound) {
            /* Must have been a split. */
            return null;
        }

        /*
         * The parent was found and is now latched. If the child BIN does not
         * match the cursor's BIN, then a split occurred and null is returned.
         */
        final long binLsn;
        try {
            if (origBin != result.parent.getTarget(result.index) ||
                origBin != bin) {
                /* Must have been a split. */
                return null;
            }

            binLsn = result.parent.getLsn(result.index);
            bin.latch();

        } finally {
            result.parent.releaseLatch();
        }

        /*
         * The child BIN is now latched. Subtract defunct entries from BIN's
         * total entries and adjust the index accordingly.  Add TrackingInfo
         * for child BIN.
         */
        try {
            int binEntries = bin.getNEntries();
            int binIndex = getIndex();

            for (int i = bin.getNEntries() - 1; i >= 0; i -= 1) {

                if (bin.isDefunct(i)) {
                    binEntries -= 1;
                    if (i < binIndex) {
                        binIndex -= 1;
                    }
                }
            }

            final TrackingInfo info = new TrackingInfo(
                binLsn, bin.getNodeId(), binEntries, binIndex);

            trackingList.add(info);

            return trackingList;

        } finally {
            bin.releaseLatch();
        }
    }

    /**
     * Search for the next key following the given key, and acquire a range
     * insert lock on it.  If there are no more records following the given
     * key, lock the special EOF node for the dbImpl.
     */
    public void lockNextKeyForInsert(DatabaseEntry key) {

        DatabaseEntry tempKey = new DatabaseEntry(
            key.getData(), key.getOffset(), key.getSize());

        boolean lockedNextKey = false;
        boolean latched = true;

        try {
            while (true) {

                int searchResult = searchRange(tempKey, null /*comparator*/);

                if ((searchResult & FOUND) != 0 &&
                    (searchResult & FOUND_LAST) == 0) {

                    /*
                     * The search positioned "this" on the BIN that should
                     * contain K1 and this BIN is now latched. If the BIN does
                     * contain K1, this.index points to K1's slot. Otherwise,
                     * this.index points to the right-most slot whose key is
                     * < K1 (or this.index is -1 if K1 is < than all keys in
                     * the BIN). Furthermore, "this" is NOT positioned on the
                     * very last slot of the BTree.
                     *
                     * Call getNext() to advance "this" to the next *valid*
                     * (i.e., not defunct) slot and lock that slot in
                     * RANGE_INSERT mode. Normally, getNext() will move the
                     * cursor to the 1st slot with a key K2 > K1. However, it
                     * is possible that K2 <= K1 (see the comments in
                     * Cursor.searchRangeAdvanceAndCheckKey() about how this
                     * can happen. We handle this race condition by restarting
                     * the search.
                     */
                    DatabaseEntry tempData = new DatabaseEntry();
                    tempData.setPartial(0, 0, true);

                    OperationResult result = getNext(
                        tempKey, tempData, LockType.RANGE_INSERT,
                        false, true, true,
                        null /*rangeConstraint*/);

                    latched = false;

                    if (result != null) {

                        Comparator<byte[]> comparator =
                            dbImpl.getKeyComparator();

                        int c = Key.compareKeys(tempKey, key, comparator);
                        if (c <= 0) {
                            tempKey.setData(
                                key.getData(), key.getOffset(), key.getSize());
                            continue;
                        }

                        lockedNextKey = true;
                    }
                }

                break;
            }
        } finally {
            if (latched) {
                releaseBIN();
            }
        }

        /* Lock the EOF node if no next key was found. */
        if (!lockedNextKey) {
            lockEof(LockType.RANGE_INSERT);
        }
    }

    /*
     * Locking
     */

    /**
     * Holds the result of a lockLN operation.  A lock may not actually be
     * held (getLockResult may return null) if an uncontended lock is allowed.
     */
    public static class LockStanding {

        private long lsn;
        private boolean defunct;
        private LockResult lockResult;

        /**
         * Returns true if the record is not deleted or expired.
         */
        public boolean recordExists() {
            return !defunct;
        }

        /**
         * Called by update and delete ops, after lockLN() and before logging
         * the LN and updating the BIN. It returns a WriteLockInfo that is
         * meant to be passed to the LN logging method, where its info will
         * be included in the LN log entry and also copied into the new
         * WriteLockInfo that will be created for the new LSN.
         *
         * If the locker is not transactional, or the current LSN has not been
         * write-locked before by this locker, a new WriteLockInfo is created
         * here and its abortLsn and abortKD fields are set. (note: even though
         * lockLN() is called before prepareForUpdate(), it may not actually
         * acquire a lock because of the uncontended optimization).
         * 
         * Otherwise, a WriteLockInfo exists already. It may have been created
         * by the lockLN() call during the current updating op, or a lockLN()
         * call during an earlier updating op by the same txn. In the later
         * case, the abortLsn and abortKD have been set already and should not
         * be overwriten here.
         */
        public WriteLockInfo prepareForUpdate(BIN bin, int idx) {

            DatabaseImpl db = bin.getDatabase();
            boolean abortKD = !recordExists();
            byte[] abortKey = null;
            byte[] abortData = null;
            long abortVLSN = VLSN.NULL_VLSN.getSequence();
            int abortExpiration = bin.getExpiration(idx);
            boolean abortExpirationInHours = bin.isExpirationInHours();

            if (bin.isEmbeddedLN(idx)) {

                abortData = bin.getData(idx);

                abortVLSN = bin.getVLSN(
                    idx, false/*allowFetch*/, null/*cacheMode*/);

                if (bin.getDatabase().allowsKeyUpdates()) {
                    abortKey = bin.getKey(idx);
                }
            }

            WriteLockInfo wri = (lockResult == null ?
                                 null :
                                 lockResult.getWriteLockInfo());
            if (wri == null) {
                wri = new WriteLockInfo();
                wri.setAbortLsn(lsn);
                wri.setAbortKnownDeleted(abortKD);
                wri.setAbortKey(abortKey);
                wri.setAbortData(abortData);
                wri.setAbortVLSN(abortVLSN);
                wri.setAbortExpiration(abortExpiration, abortExpirationInHours);
                wri.setDb(db);
            } else {
                lockResult.setAbortInfo(
                    lsn, abortKD, abortKey, abortData, abortVLSN,
                    abortExpiration, abortExpirationInHours, db);
            }
            return wri;
        }

        /**
         * Creates WriteLockInfo that is appropriate for a newly inserted slot.
         * The return value is meant to be passed to an LN logging method and
         * copied into the WriteLockInfo for the new LSN.  This method is
         * static because lockLN is never called prior to logging an LN for a
         * newly inserted slot.
         */
        public static WriteLockInfo prepareForInsert(BIN bin) {
            WriteLockInfo wri = new WriteLockInfo();
            wri.setDb(bin.getDatabase());
            return wri;
        }
    }

    /** Does not allow uncontended locks.  See lockLN(LockType, boolean). */
    public LockStanding lockLN(LockType lockType)
        throws LockConflictException {

        return lockLN(lockType, false /*allowUncontended*/, false /*noWait*/);
    }

    /**
     * Locks the LN at the cursor position.  Attempts to use a non-blocking
     * lock to avoid unlatching/relatching.
     *
     * Retries if necessary, to handle the case where the LSN is changed while
     * the BIN is unlatched.  Because it re-latches the BIN to check the LSN,
     * this serializes access to the LSN for locking, guaranteeing that two
     * lockers cannot obtain conflicting locks on the old and new LSNs.
     *
     * Preconditions: The BIN must be latched.
     *
     * Postconditions: The BIN is latched.
     *
     * LN Locking Rules
     * ----------------
     * The lock ID for an LN is its LSN in the parent BIN slot.  Because the
     * LSN changes when logging the LN, only two methods of locking an LN may
     * be used to support concurrent access:
     *
     * 1. This method may be called to lock the old LSN.  For read operations,
     * that is all that is necessary.  For write operations, the new LSN must
     * be locked after logging it, which is done by all the LN logging methods.
     * Be sure to pass a non-null locker to the LN logging method to lock the
     * LN, unless locking is not desired.
     *
     * 2. A non-blocking lock may be obtained on the old LSN (using
     * Locker.nonBlockingLock rather than this method), as long as the lock is
     * released before the BIN latch is released.  In this case a null locker
     * is passed to the LN logging method; locking the new LSN is unnecessary
     * because no other thread can access the new LSN until the BIN latch is
     * released.
     *
     * The first method is used for all user operations.  The second method is
     * used by the cleaner, when flushing dirty deferred-write LNs, and by
     * certain btree operations.
     *
     * Uncontended Lock Optimization
     * -----------------------------
     * The allowUncontended param is passed as true for update and delete
     * operations as an optimization for the case where no lock on the old LSN
     * is held by any locker.  In this case we don't need to lock the old LSN
     * at all, as long as we log the new LSN before releasing the BIN latch.
     *
     * 1. Latch BIN
     * 2. Determine that no lock/waiter exists for oldLsn
     * 3. Log LN and get lsn
     * 4. Lock lsn
     * 5. Update BIN
     * 6. Release BIN latch
     *
     * The oldLsn is never locked, saving operations on the lock table.  The
     * assumption is that another locker will first have to latch the BIN to
     * get oldLsn, before requesting a lock.
     *
     * A potential problem is that the other locker may release the BIN latch
     * before requesting the lock.
     *
     * This Operation        Another Operation
     * --------------        -----------------
     *                       Latch BIN, get oldLsn, release BIN latch
     * Step 1 and 2
     *                       Request lock for oldLsn, granted
     * Step 3 and 4
     *
     * Both operations now believe they have an exclusive lock, but they have
     * locks on different LSNs.
     *
     * However, this problem is handled as long as the other lock is performed
     * using a lockLN method in this class, which will release the lock and
     * retry if the LSN changes while acquiring the lock.  Because it
     * re-latches the BIN to check the LSN, this will serialize access to the
     * LSN for locking, guaranteeing that two conflicting locks cannot be
     * granted on the old and new LSNs.
     *
     * Deferred-Write Locking
     * ----------------------
     * When one of the LN optionalLog methods is called, a deferred-write LN is
     * dirtied but not actually logged.  In order to lock an LN that has been
     * inserted but not yet assigned a true LSN, a transient LSNs is assigned.
     * These LSNs serve to lock the LN but never appear in the log.  See
     * LN.assignTransientLsn.
     *
     * A deferred-write LN is logged when its parent BIN is logged, or when the
     * LN is evicted.  This will replace transient LSNs with durable LSNs.  If
     * a lock is held by a cursor on a deferred-write LN when it is logged, the
     * same lock is acquired on the new LSN by the cursor.  See
     * lockAfterLsnChange.
     *
     * Cleaner Migration Locking
     * -------------------------
     * The cleaner takes a non-blocking read lock on the old LSN before
     * migrating/logging the LN, while holding the BIN latch.  It does not take
     * a lock on the new LSN, since it does not need to retain a lock after
     * releasing the BIN latch.
     *
     * Because a read, not write, lock is taken, other read locks may be held
     * during migration.  After logging, the cleaner calls lockAfterLsnChange
     * to lock the new LSN on behalf of other lockers.
     *
     * For more info on migration locking, see HandleLocker.
     *
     * Expired Record Locking
     * ----------------------
     * To support repeatable-read semantics when a record expires after being
     * locked, we must check whether a record was previously locked before
     * attempting to lock it. If it was previously locked, then it is treated
     * as not expired, even if its expiration time has passed.
     *
     * By was previously "locked" here we mean that any lock type is held, or
     * shared with its owner, by this cursor's locker. Since a read lock will
     * prevent modification of the expiration time, any lock type is adequate.
     * A shared lock is considered adequate to account for the case where
     * multiple lockers are used internally for a single virtual locker, as
     * seen by the user. This is the case when using a read-committed locker or
     * a thread-locker, for example.
     *
     * To avoid unnecessary added overhead, we do not check whether a record
     * was previously locked except when expiration is imminent, which is
     * defined as expiring within {@link
     * EnvironmentParams#ENV_TTL_MAX_TXN_TIME}. The ENV_TTL_MAX_TXN_TIME buffer
     * is used because the expiration time may pass while waiting for a lock.
     *
     * Another case to account for is when the expiration time of the record
     * changes while waiting for the lock. This can happen if the record is
     * updated or an update is aborted. In this case we can assume that the
     * was not previously locked, since that would have prevented the update.
     *
     * Note that when an uncontended lock applies, the expiration of the record
     * with the current LSN cannot change. It is possible that the update or
     * deletion requesting the uncontended lock will be aborted, and the LSN of
     * an expired record will be reinstated in the BIN, but this does not
     * create a special case.
     *
     * Historical Notes
     * ----------------
     * In JE 4.1 and earlier, each LN had a node ID that was used for locking,
     * rather than using the LSN.  The node ID changed only if a deleted slot
     * was reused.  The node ID was stored in the LN, requiring that the LN be
     * fetched when locking the LN.  With LSN locking a fetch is not needed.
     *
     * When LN node IDs were used, deferred-write LNs were not assigned an LSN
     * until they were actually logged. Deferred-write LNs were initially
     * assigned a null LSN and transient LSNs were not needed.
     *
     * @param lockType the type of lock requested.
     *
     * @param allowUncontended is true to return immediately (no lock is taken)
     * when no locker holds or waits for the lock.
     *
     * @param noWait is true to perform a no-wait lock request while keeping
     * the BIN latched.  The caller must check the lock result to see whether
     * the lock was granted.
     *
     * @return all information about the lock; see LockStanding.
     *
     * @throws LockConflictException if the lsn is non-null, the lock is
     * contended, and a lock could not be obtained by blocking.
     */
    public LockStanding lockLN(
        final LockType lockType,
        final boolean allowUncontended,
        final boolean noWait)
        throws LockConflictException {

        final EnvironmentImpl envImpl = dbImpl.getEnv();

        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        final LockStanding standing = new LockStanding();
        standing.lsn = bin.getLsn(index);

        /* Check for a known-deleted null LSN. */
        if (standing.lsn == DbLsn.NULL_LSN) {
            assert bin.isEntryKnownDeleted(index);
            standing.defunct = true;
            return standing;
        }

        /*
         * We can avoid taking a lock if uncontended.  However, we must
         * call preLogWithoutLock to prevent logging on a replica, and as
         * good measure to prepare for undo.
         */
        if (allowUncontended && lockManager.isLockUncontended(standing.lsn)) {
            assert verifyPendingDeleted(lockType);
            locker.preLogWithoutLock(dbImpl);
            standing.defunct = bin.isDefunct(index);
            return standing;
        }

        /*
         * If wasLockedAndExpiresSoon is true, we will treat the record as not
         * expired. If false, we will check for expiration after locking.
         */
        boolean wasLockedAndExpiresSoon = false;
        final int prevExpiration = bin.getExpiration(index);
        final boolean prevExpirationInHours = bin.isExpirationInHours();

        if (envImpl.expiresWithin(
            prevExpiration, prevExpirationInHours,
            dbImpl.getEnv().getTtlMaxTxnTime())) {

            if (lockManager.ownsOrSharesLock(locker, standing.lsn)) {
                wasLockedAndExpiresSoon = true;
            }
        }

        /*
         * Try a non-blocking lock first, to avoid unlatching.  If the default
         * is no-wait, use the standard lock method so
         * LockNotAvailableException is thrown; there is no need to try a
         * non-blocking lock twice.
         *
         * Even for dirty-read (LockType.NONE) we must call Locker.lock() since
         * it checks the locker state and may throw LockPreemptedException.
         */
        if (locker.getDefaultNoWait()) {
            try {
                standing.lockResult = locker.lock(
                    standing.lsn, lockType, true /*noWait*/, dbImpl);

            } catch (LockNotAvailableException e) {
                releaseBIN();
                throw e;

            } catch (LockConflictException e) {
                releaseBIN();
                throw EnvironmentFailureException.unexpectedException(e);
            }
        } else {
            standing.lockResult = locker.nonBlockingLock(
                standing.lsn, lockType, false /*jumpAheadOfWaiters*/,
                dbImpl);
        }

        if (standing.lockResult.getLockGrant() != LockGrantType.DENIED) {

            /* Lock was granted whiled latched, no need to check LSN. */
            assert verifyPendingDeleted(lockType);

            standing.defunct = wasLockedAndExpiresSoon ?
                bin.isDeleted(index) : bin.isDefunct(index);

            return standing;
        }

        if (noWait) {
            /* We did not acquire the lock. */

            standing.defunct = wasLockedAndExpiresSoon ?
                bin.isDeleted(index) : bin.isDefunct(index);

            return standing;
        }

        /*
         * Unlatch, get a blocking lock, latch, and get the current LSN from
         * the slot.  If the LSN changes while unlatched, revert the lock and
         * repeat.
         */
        while (true) {

            /* Request a blocking lock. */
            releaseBIN();

            standing.lockResult = locker.lock(
                standing.lsn, lockType, false /*noWait*/, dbImpl);

            latchBIN();

            /* Check current LSN after locking. */
            final long newLsn = bin.getLsn(index);
            if (standing.lsn == newLsn) {

                /*
                 * If the expiration time changes while unlatched, then it
                 * could not have been previously locked.
                 */
                if (prevExpiration != bin.getExpiration(index) ||
                    prevExpirationInHours != bin.isExpirationInHours()) {
                    wasLockedAndExpiresSoon = false;
                }

                standing.defunct = wasLockedAndExpiresSoon ?
                    bin.isDeleted(index) : bin.isDefunct(index);

                assert verifyPendingDeleted(lockType);
                return standing;
            }

            /* The LSN changed, revert the lock and try again. */
            revertLock(standing);
            standing.lsn = newLsn;

            /* Check for a known-deleted null LSN. */
            if (newLsn == DbLsn.NULL_LSN) {
                assert bin.isEntryKnownDeleted(index);
                standing.defunct = true;
                return standing;
            }
        }
    }

    /**
     * After logging a deferred-write LN during eviction/checkpoint or a
     * migrated LN during cleaning, for every existing lock on the old LSN held
     * by another locker, we must lock the new LSN on behalf of that locker.
     *
     * This is done while holding the BIN latch so that the new LSN does not
     * change during the locking process.  The BIN must be latched on entry and
     * is left latched by this method.
     *
     * We release the lock on the oldLsn to prevent locks from accumulating
     * over time on a HandleLocker, as the cleaner migrates LNs, because
     * Database handle locks are legitimately very long-lived.  It is important
     * to first acquire all lsn locks and then release the oldLsn locks.
     * Releasing an oldLsn lock might allow another locker to acquire it, and
     * then acquiring another lsn lock may encounter a conflict. [#20617]
     *
     * @see com.sleepycat.je.txn.HandleLocker
     * @see #lockLN
     */
    public static void lockAfterLsnChange(
        DatabaseImpl dbImpl,
        long oldLsn,
        long newLsn,
        Locker excludeLocker) {

        final LockManager lockManager =
            dbImpl.getEnv().getTxnManager().getLockManager();

        final Set<LockInfo> owners = lockManager.getOwners(oldLsn);
        if (owners == null) {
            return;
        }
        /* Acquire lsn locks. */
        for (LockInfo lockInfo : owners) {
            final Locker locker = lockInfo.getLocker();
            if (locker != excludeLocker) {
                locker.lockAfterLsnChange(oldLsn, newLsn, dbImpl);
            }
        }
        /* Release oldLsn locks. */
        for (LockInfo lockInfo : owners) {
            final Locker locker = lockInfo.getLocker();
            if (locker != excludeLocker &&
                locker.allowReleaseLockAfterLsnChange()) {
                locker.releaseLock(oldLsn);
            }
        }
    }

    /**
     * For debugging. Verify that a BINs cursor set refers to the BIN.
     */
    private void verifyCursor(BIN bin) {

        if (!bin.getCursorSet().contains(this)) {
            throw new EnvironmentFailureException(
                dbImpl.getEnv(),
                EnvironmentFailureReason.UNEXPECTED_STATE,
                "BIN cursorSet is inconsistent");
        }
    }

    /**
     * Calls checkCursorState and asserts false if an exception is thrown.
     * Otherwise returns true, so it can be called under an assertion.
     */
    private boolean assertCursorState(
        boolean mustBeInitialized,
        boolean mustNotBeInitialized) {

        try {
            checkCursorState(mustBeInitialized, mustNotBeInitialized);
            return true;
        } catch (RuntimeException e) {
            assert false : e.toString() + " " + dumpToString(true);
            return false; // for compiler
        }
    }

    /**
     * Check that the cursor is open and optionally if it is initialized or
     * uninitialized.
     *
     * @throws IllegalStateException via all Cursor methods that call
     * Cursor.checkState (all get and put methods, plus more).
     */
    public void checkCursorState(
        boolean mustBeInitialized,
        boolean mustNotBeInitialized) {

        switch (status) {
        case CURSOR_NOT_INITIALIZED:
            if (mustBeInitialized) {
                throw new IllegalStateException("Cursor not initialized.");
            }
            break;
        case CURSOR_INITIALIZED:
            if (mustNotBeInitialized) {
                throw EnvironmentFailureException.unexpectedState(
                    "Cursor is initialized.");
            }
            if (DEBUG) {
                if (bin != null) {
                    verifyCursor(bin);
                }
            }
            break;
        case CURSOR_CLOSED:
            throw new IllegalStateException("Cursor has been closed.");
        default:
            throw EnvironmentFailureException.unexpectedState(
                "Unknown cursor status: " + status);
        }
    }

    /**
     * Checks that LN deletedness matches KD/PD flag state, at least when the
     * LN is resident.  Should only be called under an assertion.
     */
    private boolean verifyPendingDeleted(LockType lockType) {

        /* Cannot verify deletedness if LN is not locked. */
        if (lockType == LockType.NONE) {
            return true;
        }

        /* Cannot verify deletedness if cursor is not intialized. */
        if (bin == null || index < 0) {
            return true;
        }

        /* Cannot verify deletedness if LN is not resident. */
        final LN ln = (LN) bin.getTarget(index);
        if (ln == null) {
            return true;
        }

        /*
         * If the LN is deleted then KD or PD must be set.  If the LN is not
         * deleted then PD must not be set, but KD may or may not be set since
         * it used for various purposes (see IN.java).
         */
        final boolean kd = bin.isEntryKnownDeleted(index);
        final boolean pd = bin.isEntryPendingDeleted(index);
        final boolean lnDeleted = ln.isDeleted();
        assert ((lnDeleted && (kd || pd)) || (!lnDeleted && !pd)) :
               "Deleted state mismatch LNDeleted = " + lnDeleted +
               " PD = " + pd + " KD = " + kd;
        return true;
    }

    public void revertLock(LockStanding standing) {

        if (standing.lockResult != null) {
            revertLock(standing.lsn, standing.lockResult);
            standing.lockResult = null;
        }
    }

    /**
     * Return this lock to its prior status. If the lock was just obtained,
     * release it. If it was promoted, demote it.
     */
    private void revertLock(long lsn, LockResult lockResult) {

        LockGrantType lockStatus = lockResult.getLockGrant();

        if ((lockStatus == LockGrantType.NEW) ||
            (lockStatus == LockGrantType.WAIT_NEW)) {
            locker.releaseLock(lsn);
        } else if ((lockStatus == LockGrantType.PROMOTION) ||
                   (lockStatus == LockGrantType.WAIT_PROMOTION)){
            locker.demoteLock(lsn);
        }
    }

    /**
     * Locks the logical EOF node for the dbImpl.
     */
    public void lockEof(LockType lockType) {

        locker.lock(dbImpl.getEofLsn(), lockType,
                    false /*noWait*/, dbImpl);
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     */
    public void checkEnv() {
        dbImpl.getEnv().checkIfInvalid();
    }

    /**
     * Callback object for traverseDbWithCursor.
     */
    public interface WithCursor {

        /**
         * Called for each record in the dbImpl.
         * @return true to continue or false to stop the enumeration.
         */
        boolean withCursor(CursorImpl cursor,
                           DatabaseEntry key,
                           DatabaseEntry data);
    }

    /**
     * Enumerates all records in a dbImpl non-transactionally and calls
     * the withCursor method for each record.  Stops the enumeration if the
     * callback returns false.
     *
     * @param db DatabaseImpl to traverse.
     *
     * @param lockType non-null LockType for reading records.
     *
     * @param allowEviction should normally be true to evict when performing
     * multiple operations, but may be false if eviction is disallowed in a
     * particular context.
     *
     * @param withCursor callback object.
     */
    public static void traverseDbWithCursor(
        DatabaseImpl db,
        LockType lockType,
        boolean allowEviction,
        WithCursor withCursor) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Locker locker = null;
        CursorImpl cursor = null;

        try {
            EnvironmentImpl envImpl = db.getEnv();

            locker = LockerFactory.getInternalReadOperationLocker(envImpl);

            cursor = new CursorImpl(db, locker);
            cursor.setAllowEviction(allowEviction);

            if (cursor.positionFirstOrLast(true /*first*/)) {

                OperationResult result = cursor.lockAndGetCurrent(
                    key, data, lockType, false /*dirtyReadAll*/,
                    true /*isLatched*/, true /*unlatch*/);

                boolean done = false;
                while (!done) {

                    /*
                     * lockAndGetCurrent may have returned non-SUCCESS if the
                     * first record is defunct, but we can call getNext below
                     * to move forward.
                     */
                    if (result != null) {
                        if (!withCursor.withCursor(cursor, key, data)) {
                            done = true;
                        }
                    }

                    if (!done) {
                        result = cursor.getNext(
                            key, data, lockType, false /*dirtyReadAll*/,
                            true /*forward*/, false /*isLatched*/,
                            null /*rangeConstraint*/);

                        if (result == null) {
                            done = true;
                        }
                    }
                }
            }
        } finally {
            if (cursor != null) {
                cursor.releaseBIN();
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Dump the cursor for debugging purposes.  Dump the bin that the cursor
     * refers to if verbose is true.
     */
    public void dump(boolean verbose) {
        System.out.println(dumpToString(verbose));
    }

    /**
     * dump the cursor for debugging purposes.
     */
    public void dump() {
        System.out.println(dumpToString(true));
    }

    /*
     * dumper
     */
    private String statusToString(byte status) {
        switch(status) {
        case CURSOR_NOT_INITIALIZED:
            return "CURSOR_NOT_INITIALIZED";
        case CURSOR_INITIALIZED:
            return "CURSOR_INITIALIZED";
        case CURSOR_CLOSED:
            return "CURSOR_CLOSED";
        default:
            return "UNKNOWN (" + Byte.toString(status) + ")";
        }
    }

    /*
     * dumper
     */
    public String dumpToString(boolean verbose) {
        StringBuilder sb = new StringBuilder();

        sb.append("<Cursor idx=\"").append(index).append("\"");
        sb.append(" status=\"").append(statusToString(status)).append("\"");
        sb.append(">\n");
        if (verbose) {
            sb.append((bin == null) ? "" : bin.dumpString(2, true));
        }
        sb.append("\n</Cursor>");

        return sb.toString();
    }

    /*
     * For unit tests
     */
    public StatGroup getLockStats() {
        return locker.collectStats();
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void trace(
        Level level,
        String changeType,
        BIN theBin,
        int lnIndex,
        long oldLsn,
        long newLsn) {

        EnvironmentImpl envImpl = dbImpl.getEnv();
        if (envImpl.getLogger().isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(changeType);
            sb.append(" bin=");
            sb.append(theBin.getNodeId());
            sb.append(" lnIdx=");
            sb.append(lnIndex);
            sb.append(" oldLnLsn=");
            sb.append(DbLsn.getNoFormatString(oldLsn));
            sb.append(" newLnLsn=");
            sb.append(DbLsn.getNoFormatString(newLsn));

            LoggerUtils.logMsg
                (envImpl.getLogger(), envImpl, level, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceInsert(
        Level level,
        BIN insertingBin,
        long lnLsn,
        int index) {

        EnvironmentImpl envImpl = dbImpl.getEnv();
        if (envImpl.getLogger().isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(TRACE_INSERT);
            sb.append(" bin=");
            sb.append(insertingBin.getNodeId());
            sb.append(" lnLsn=");
            sb.append(DbLsn.getNoFormatString(lnLsn));
            sb.append(" index=");
            sb.append(index);

            LoggerUtils.logMsg(envImpl.getLogger(), envImpl, level,
                               sb.toString());
        }
    }

    /* For unit testing only. */
    public void setTestHook(TestHook hook) {
        testHook = hook;
    }

    /* Check that the target bin is latched. For use in assertions. */
    private boolean checkAlreadyLatched(boolean isLatched) {
        if (isLatched) {
            if (bin != null) {
                return bin.isLatchExclusiveOwner();
            }
        }
        return true;
    }
}
