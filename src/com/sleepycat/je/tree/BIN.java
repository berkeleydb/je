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

package com.sleepycat.je.tree;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.TinyHashSet;
import com.sleepycat.je.utilint.VLSN;

/**
 * A BIN represents a Bottom Internal Node in the JE tree.
 *
 * BIN-deltas
 * ==========
 * A BIN-delta is a BIN with the non-dirty slots omitted. A "full BIN", OTOH
 * contains all slots.  On disk and in memory, the format of a BIN-delta is the
 * same as that of a BIN.  In memory, a BIN object is actually a BIN-delta when
 * the BIN-delta flag is set (IN.isBINDelta).  On disk, the NewBINDelta log
 * entry type (class BINDeltaLogEntry) is the only thing that distinguishes it
 * from a full BIN, which has the BIN log entry type.
 *
 * BIN-deltas provides two benefits: Reduced writing and reduced memory usage.
 *
 * Reduced Writing
 * ---------------
 * Logging a BIN-delta rather a full BIN reduces writing significantly.  The
 * cost, however, is that two reads are necessary to reconstruct a full BIN
 * from scratch.  The reduced writing is worth this cost, particularly because
 * less writing means less log cleaning.
 *
 * A BIN-delta is logged when 25% or less (configured with EnvironmentConfig
 * TREE_BIN_DELTA) of the slots in a BIN are dirty. When a BIN-delta is logged,
 * the dirty flag is cleared on the the BIN in cache.  If more slots are
 * dirtied and another BIN-delta is logged, it will contain all entries dirtied
 * since the last full BIN was logged.  In other words, BIN-deltas are
 * cumulative and not chained, to avoid reading many (more than two) log
 * entries to reconstruct a full BIN.  The dirty flag on each slot is cleared
 * only when a full BIN is logged.
 *
 * In addition to the cost of fetching two entries on a BIN cache miss, another
 * drawback of the current approach is that dirtiness propagates upward in the
 * Btree due to BIN-delta logging, causing repeated logging of upper INs.  The
 * slot of the parent IN contains the LSN of the most recent BIN-delta or full
 * BIN that was logged.  A BINDeltaLogEntry in turn contains the LSN of the
 * last full BIN logged.
 *
 *   Historical note:  The pre-JE 5 implementation of OldBINDeltas worked
 *   differently and had a different cost/benefit trade-off.  When an
 *   OldBINDelta was logged, its dirty flag was not cleared, causing it to be
 *   logged repeatedly at every checkpoint.  A full BIN was logged after 10
 *   deltas, to prevent endless logging of the same BIN.  One benefit of this
 *   approach is that the BIN's parent IN was not dirtied when logging the
 *   OldBINDelta, preventing dirtiness from propagating upward.  Another
 *   benefit is that the OldBINDelta was only processed by recovery, and did
 *   not have to be fetched to reconstruct a full BIN from scratch on a cache
 *   miss.  But the cost (the logging of an OldBINDelta every checkpoint, even
 *   when it hadn't changed since the last time logged) outweighed the
 *   benefits.  When the current approach was implemented in JE 5, performance
 *   improved due to less logging.
 *
 *   In JE 6, deltas were also maintained in the Btree cache.  This was done to
 *   provide the reduced memory benefits described in the next section.  The
 *   log format for a delta was also changed.  The OldBINDelta log format is
 *   different (not the same as the BIN format) and is supported for backward
 *   compatibility as the OldBINDeltaLogEntry.  Its log entry type name is
 *   still BINDelta, which is why the new type is named NewBINDelta (for
 *   backward compatibility, log entry type names cannot be changed.)  This is
 *   also why the spelling "BIN-delta" is used to refer to deltas in the new
 *   approach.  The old BINDelta class was renamed to OldBINDelta and there is
 *   no longer a class named BINDelta.
 *
 * Reduced Memory Usage
 * --------------------
 * In the Btree cache, a BIN may be represented as a full BIN or a BIN-delta.
 * Eviction will mutate a full BIN to a BIN-delta in preference to discarding
 * the entire BIN. A BIN-delta in cache occupies less memory than a full BIN,
 * and can be exploited as follows:
 *
 *  - When a full BIN is needed, it can be constructed with only one fetch
 *    rather than two, reducing IO overall.  IN.fetchIN implements this
 *    optimization.
 *
 *  - Certain operations can sometimes be performed using the BIN-delta alone,
 *    allowing such operations on a given data set to take place using less
 *    less IO (for a given cache size).
 *
 * The latter benefit is not yet implemented.   No user CRUD operations are
 * currently implemented using BIN-deltas. In the future we plan to implement
 * the following operations using the BIN-delta alone.
 *
 *  - Consider recording deletions in a BIN-delta.  Currently, slot deletion
 *    prohibits a BIN-delta from being logged.  To record deletion in
 *    BIN-deltas, slot deletion will have to be deferred until a full BIN is
 *    logged.
 *
 *  - User reads by key, updates and deletions can be implemented if the key
 *    happens to appear in the BIN-delta.
 *
 *  - The Cleaner can migrate an LN if its key happens to appear in the
 *    BIN-delta.  This is similar to a user update operation, but in a
 *    different code path.
 *
 *  - Insertions, deletions and updates can always be performed in a BIN-delta
 *    during replica replay, since the Master operation has already determined
 *    whether the key exists.
 *
 *  - Recovery LN redo could also apply insertions, updates and inserts in the
 *    manner described.
 *
 *  - Add idempotent put/delete operations, which can always be applied in a
 *    BIN-delta.
 *
 *  - Store a hash of the keys in the full BIN in the BIN-delta and use it to
 *    perform the following in the delta:
 *    - putIfAbsent (true insertion)
 *    - get/delete/putIfPresent operations that return NOTFOUND
 *    - to avoid accumulating unnecessary deletions
 *
 * However, some internal operations do currently exploit BIN-deltas to avoid
 * unnecessary IO.  The following are currently implemented.
 *
 *  - The Evictor and Checkpointer log a BIN-delta that is present in the
 *    cache, without having to fetch the full BIN.
 *
 *  - The Cleaner can use the BIN-delta to avoid fetching when processing a BIN
 *    log entry (delta or full) and the BIN is not present in cache,
 *
 * To support BIB-delta-aware operations, the IN.fetchIN() and IN.getTarget()
 * methods may return a BIN delta. IN.getTarget() will return whatever object
 * is cached under the parent IN, and IN.fetchIN() will do a single I/O to
 * fetch the most recently log record for the requested BIN, which may be a
 * full BIN or a delta. Callers of these methods must be prepared to handle
 * a BIN delta; either doing their operation directly on the delta, if
 * possible, or mutating the delta to a full BIN by calling
 * BIN.mutateToFullBIN().
 */
public class BIN extends IN {

    private static final String BEGIN_TAG = "<bin>";
    private static final String END_TAG = "</bin>";

    /**
     * Used as the "empty rep" for the INLongRep lastLoggedSizes field.
     *
     * minLength is 1 because log sizes are unpredictable.
     *
     * allowSparseRep is false because all slots have log sizes and less
     * mutation is better.
     */
    private static final INLongRep.EmptyRep EMPTY_LAST_LOGGED_SIZES =
        new INLongRep.EmptyRep(1, false);

    /**
     * Used as the "empty rep" for the INLongRep vlsnCache field.
     *
     * minLength is 5 because VLSNS grow that large fairly quickly, and less
     * mutation is better. The value 5 accomodates data set sizes up to 100
     * billion. If we want to improve memory utilization for smaller data sets
     * or reduce mutation for larger data sets, we could dynamically determine
     * a value based on the last assigned VLSN.
     *
     * allowSparseRep is false because either all slots typically have VLSNs,
     * or none do, and less mutation is better.
     */
    private static final INLongRep.EmptyRep EMPTY_VLSNS =
        new INLongRep.EmptyRep(5, false);

    /**
     * Used as the "empty rep" for the INLongRep offHeapLNIds field.
     *
     * minLength is 8 because memory IDs are 64-bit pointers.
     *
     * allowSparseRep is true because some workloads will only load LN IDs for
     * a subset of the LNs in the BIN.
     */
    private static final INLongRep.EmptyRep EMPTY_OFFHEAP_LN_IDS =
        new INLongRep.EmptyRep(8, true);

    /**
     * Used as the "empty rep" for the INLongRep expirationValues field.
     *
     * minLength is 1 because we expect most expiration values, which are an
     * offset from a base day/hour, to fit in one byte.
     *
     * allowSparseRep is true because some workloads only set TTLs on some of
     * the LNs in a BIN.
     */
    private static final INLongRep.EmptyRep EMPTY_EXPIRATION =
        new INLongRep.EmptyRep(1, true);

    /*
     * The set of cursors that are currently referring to this BIN.
     * This field is set to null when there are no cursors on this BIN.
     */
    private TinyHashSet<CursorImpl> cursorSet;

    /*
     * Support for logging BIN deltas. (Partial BIN logging)
     */

    /*
     * If this is a delta, fullBinNEntries stores the number of entries
     * in the full version of the BIN. This is a persistent field for
     * BIN-delta logrecs only, and for log versions >= 10.
     */
    private int fullBinNEntries = -1;

    /*
     * If this is a delta, fullBinMaxEntries stores the max number of
     * entries (capacity) in the full version of the BIN. This is a
     * persistent field for BIN-delta logrecs only, and for log versions >= 10.
     */
    private int fullBinMaxEntries = -1;

    /*
     * If "this" is a BIN-delta, bloomFilter is a bloom-filter representation
     * of the set of keys in the clean slots of the full version of the same
     * BIN. It is used to allow blind put operations in deltas, by answering
     * the question whether the put key is in the full BIN or not. See the
     * javadoc of the  TREE_BIN_DELTA_BLIND_PUTS config param for more info.
     * This is a persistent field for BIN-delta logrecs only, and for log
     * versions >= 10.
     */
    byte[] bloomFilter;

    /*
     * See comment in IN.java, right after the lastFullVersion data field.
     */
    private long lastDeltaVersion = DbLsn.NULL_LSN;

    /*
     * Caches the VLSN sequence for the LN entries in a BIN, when VLSN
     * preservation and caching are configured.
     *
     * A VLSN is added to the cache when an LN is evicted from a BIN. When the
     * LN is resident, there is no need for caching because the LN contains the
     * VLSN. See BIN.setTarget.  This strategy works because an LN is always
     * cached during a read or write operation, and only evicted after that,
     * based on eviction policies.
     *
     * For embedded LNs a VLSN is added to the cache every time the record is
     * logged. Furthermore, the vlsn cache is made persistent for such LNs.
     *
     * An EMPTY_REP is used initially until the need arises to add a non-zero
     * value.  The cache will remain empty if LNs are never evicted or version
     * caching is not configured, which is always the case for standalone JE.
     */
    private INLongRep vlsnCache = EMPTY_VLSNS;

    /*
     * Stores the size of the most recently written logrec of each LN, or zero
     * if the size is unknown.
     *
     * We use INLongRep in spite of the fact that sizes are int not long;
     * INLongRep will store the minimum number of bytes. An EMPTY_REP is
     * used initially until the need arises to add a non-zero value.
     */
    private INLongRep lastLoggedSizes = EMPTY_LAST_LOGGED_SIZES;

    /**
     * When some LNs are in the off-heap cache, the offHeapLruId is this BIN's
     * index in the off-heap LRU list.
     */
    private INLongRep offHeapLNIds = EMPTY_OFFHEAP_LN_IDS;
    private int offHeapLruId = -1;

    /**
     * An expirationValues slot value is one more than the number of days/hours
     * to add to the expirationBase to get the true expiration days/hours. A
     * slot value of zero means no expiration, and a non-zero slot value is one
     * greater than the actual offset to be added. The base is the smallest
     * non-zero offset that has been encountered.
     */
    private INLongRep expirationValues = EMPTY_EXPIRATION;
    private int expirationBase = -1;

    /**
     * Can be set to true by tests to prevent last logged sizes from being
     * stored.
     */
    public static boolean TEST_NO_LAST_LOGGED_SIZES = false;

    public BIN() {
    }

    public BIN(
        DatabaseImpl db,
        byte[] identifierKey,
        int capacity,
        int level) {

        super(db, identifierKey, capacity, level);
    }

    /**
     * For Sizeof.
     */
    public BIN(@SuppressWarnings("unused") SizeofMarker marker) {
        super(marker);
    }

    /**
     * Create a new BIN.  Need this because we can't call newInstance()
     * without getting a 0 for nodeId.
     */
    @Override
    protected IN createNewInstance(
        byte[] identifierKey,
        int maxEntries,
        int level) {

        return new BIN(getDatabase(), identifierKey, maxEntries, level);
    }

    public BINReference createReference() {
      return new BINReference(
          getNodeId(), getDatabase().getId(), getIdentifierKey());
    }

    @Override
    public boolean isBIN() {
        return true;
    }

    /*
     * Return whether the shared latch for this kind of node should be of the
     * "always exclusive" variety.  Presently, only IN's are actually latched
     * shared.  BINs are latched exclusive only.
     */
    @Override
    boolean isAlwaysLatchedExclusively() {
        return true;
    }

    @Override
    public String shortClassName() {
        return "BIN";
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    boolean isVLSNCachingEnabled() {
        return (!databaseImpl.getSortedDuplicates() && getEnv().getCacheVLSN());
    }

    public void setCachedVLSN(int idx, long vlsn) {

        /*
         * We do not cache the VLSN for dup DBs, because dup DBs are typically
         * used only for indexes, and the overhead of VLSN maintenance would be
         * wasted.  Plus, although technically VLSN preservation might apply to
         * dup DBs, the VLSNs are not reliably available since the LNs are
         * immediately obsolete.
         */
        if (!isVLSNCachingEnabled()) {
            return;
        }
        setCachedVLSNUnconditional(idx, vlsn);
    }

    void setCachedVLSNUnconditional(int idx, long vlsn) {
        vlsnCache = vlsnCache.set(
            idx,
            (vlsn == VLSN.NULL_VLSN_SEQUENCE ? 0 : vlsn),
            this);
    }

    long getCachedVLSN(int idx) {
        final long vlsn = vlsnCache.get(idx);
        return (vlsn == 0 ? VLSN.NULL_VLSN_SEQUENCE : vlsn);
    }

    /**
     * Returns the VLSN.  VLSN.NULL_VLSN_SEQUENCE (-1) is returned in two
     * cases:
     * 1) This is a standalone environment.
     * 2) The VLSN is not cached (perhaps VLSN caching is not configured), and
     *    the allowFetch param is false.
     *
     * WARNING: Because the vlsnCache is only updated when an LN is evicted, it
     * is critical that getVLSN returns the VLSN for a resident LN before
     * getting the VLSN from the cache.
     */
    public long getVLSN(int idx, boolean allowFetch, CacheMode cacheMode) {

        /* Must return the VLSN from the LN, if it is resident. */
        LN ln = (LN) getTarget(idx);
        if (ln != null) {
            return ln.getVLSNSequence();
        }

        /* Next try the vlsnCache. */
        long vlsn = getCachedVLSN(idx);
        if (!VLSN.isNull(vlsn)) {
            return vlsn;
        }

        /* Next try the off-heap cache. */
        final OffHeapCache ohCache = getOffHeapCache();
        if (ohCache.isEnabled()) {

            vlsn = ohCache.loadVLSN(this, idx);

            if (!VLSN.isNull(vlsn)) {
                return vlsn;
            }
        }

        /* As the last resort, fetch the LN if fetching is allowed. */
        if (!allowFetch || isEmbeddedLN(idx)) {
            return vlsn;
        }

        ln = fetchLN(idx, cacheMode);
        if (ln != null) {
            return ln.getVLSNSequence();
        }

        return VLSN.NULL_VLSN_SEQUENCE;
    }

    /** For unit testing. */
    public INLongRep getVLSNCache() {
        return vlsnCache;
    }

    /**
     * The last logged size is never needed when the LN is counted obsolete
     * immediately, since it is only needed for counting an LN obsolete
     * during an update or deletion.
     *
     * This method may not be called until after the database is initialized,
     * i,e., it may not be called during readFromLog.
     */
    @Override
    boolean isLastLoggedSizeStored(int idx) {

        return mayHaveLastLoggedSizeStored() && !isEmbeddedLN(idx);
    }

    @Override
    boolean mayHaveLastLoggedSizeStored() {

        /* Check final static first so all test code is optimized away. */
        if (DatabaseUtil.TEST) {
            /* Don't skew test measurements with internal DBs. */
            if (TEST_NO_LAST_LOGGED_SIZES &&
                !databaseImpl.getDbType().isInternal()) {
                return false;
            }
        }

        return !databaseImpl.isLNImmediatelyObsolete();
    }

    /**
     * Sets last logged size if necessary.
     *
     * This method does not dirty the IN because the caller methods dirty it,
     * for example, when setting the LSN, key, or node.
     *
     * This method is sometimes called to add the logged size for a pre log
     * version 9 BIN, for example, during fetchTarget and preload.  This makes
     * the logged size available for obsolete counting but does not dirty the
     * IN, since that could cause an unexpected write of the IN being read.
     *
     * @param lastLoggedSize is positive if the size is known, zero if the size
     * is unknown, or -1 if the size should not be changed because logging of
     * the LN was deferred.
     */
    @Override
    public void setLastLoggedSize(int idx, int lastLoggedSize) {

        if ((lastLoggedSize < 0) || !isLastLoggedSizeStored(idx)) {
            return;
        }

        setLastLoggedSizeUnconditional(idx, lastLoggedSize);
    }

    @Override
    public void clearLastLoggedSize(int idx) {

        setLastLoggedSizeUnconditional(idx, 0);
    }

    /**
     * Sets the size without checking whether it is necessary.
     *
     * This method is used when reading from the log because the databaseImpl
     * is not yet initialized and isLastLoggedSizeStored cannot be called.
     * It is also called for efficiency reasons when it is known that storing
     * the logged size is necessary, for example, when copying values between
     * slots.
     */
    @Override
    void setLastLoggedSizeUnconditional(int idx, int lastLoggedSize) {

        lastLoggedSizes = lastLoggedSizes.set(idx, lastLoggedSize, this);
    }

    /**
     * @return a positive value if the size is known, or zero if unknown.
     */
    @Override
    public int getLastLoggedSize(int idx) {

        if (isLastLoggedSizeStored(idx)) {
            return (int) lastLoggedSizes.get(idx);
        }

        return 0;
    }

    /**
     * Sets the expiration time for a slot in days or hours.
     */
    public void setExpiration(final int idx, int value, final boolean hours) {

        /* This slot has no expiration. */
        if (value == 0) {
            expirationValues = expirationValues.set(idx, 0, this);
            return;
        }

        /*
         * If this is the first slot with an expiration, initialize the base to
         * the value and set the offset (slot value) to one.
         */
        if (expirationBase == -1 || nEntries == 1) {
            expirationBase = value;
            setExpirationOffset(idx, 1);
            setExpirationInHours(hours);
            return;
        }

        if (hours) {
            /* Convert existing values to hours if necessary. */
            if (!isExpirationInHours()) {

                expirationBase *= 24;
                setExpirationInHours(true);

                for (int i = 0; i < nEntries; i += 1) {

                    if (i == idx) {
                        continue;
                    }

                    final int offset = (int) expirationValues.get(i);

                    if (offset == 0) {
                        continue;
                    }

                    setExpirationOffset(i, ((offset - 1) * 24) + 1);
                }
            }
        } else {
            /* If values are stored in hours, convert days to hours. */
            if (isExpirationInHours()) {
                value *= 24;
            }
        }

        /*
         * Slot's expiration must not be less than the base. If it is, decrease
         * the base and increase the offset in other slots accordingly.
         */
        if (value < expirationBase) {

            final int adjustment = expirationBase - value;
            expirationBase = value;

            for (int i = 0; i < nEntries; i += 1) {

                if (i == idx) {
                    continue;
                }

                final int offset = (int) expirationValues.get(i);

                if (offset == 0) {
                    continue;
                }

                setExpirationOffset(i, offset + adjustment);
            }
        }

        setExpirationOffset(idx, value - expirationBase + 1);
    }

    public boolean hasExpirationValues() {

        return !expirationValues.isEmpty();
    }

    /**
     * Returns the expiration time for a slot. The return value is in days or
     * hours, depending on isExpirationTimeInHours.
     */
    public int getExpiration(int idx) {

        final int offset = (int) expirationValues.get(idx);

        if (offset == 0) {
            return 0;
        }

        return offset - 1 + expirationBase;
    }

    int getExpirationBase() {
        return expirationBase;
    }

    int getExpirationOffset(int idx) {
        return (int) expirationValues.get(idx);
    }

    void setExpirationBase(int base) {
        expirationBase = base;
    }

    void setExpirationOffset(int idx, int offset) {
        expirationValues = expirationValues.set(idx, offset, this);
    }

    /**
     * Returns whether the slot is known-deleted, pending-deleted, or expired.
     */
    public boolean isDefunct(int idx) {
        return isDeleted(idx) || isExpired(idx);
    }

    /**
     * Returns whether the slot is known-deleted or pending-deleted.
     */
    public boolean isDeleted(int idx) {
        return isEntryKnownDeleted(idx) || isEntryPendingDeleted(idx);
    }

    /**
     * Returns whether the slot is expired.
     */
    public boolean isExpired(int idx) {
        return getEnv().isExpired(getExpiration(idx), isExpirationInHours());
    }

    public boolean isProbablyExpired(int idx) {

        return getEnv().expiresWithin(
            getExpiration(idx), isExpirationInHours(),
            getEnv().getTtlClockTolerance());
    }

    public int getLastLoggedSizeUnconditional(int idx) {
        return (int) lastLoggedSizes.get(idx);
    }

    public void setOffHeapLNId(int idx, long memId) {

        if (offHeapLNIds.get(idx) == memId) {
            return;
        }

        offHeapLNIds = offHeapLNIds.set(idx, memId, this);
    }

    public void clearOffHeapLNIds() {
        offHeapLNIds = offHeapLNIds.clear(this, EMPTY_OFFHEAP_LN_IDS);
    }

    public long getOffHeapLNIdsMemorySize() {
        return offHeapLNIds.getMemorySize();
    }

    public long getOffHeapLNId(int idx) {

        return offHeapLNIds.get(idx);
    }

    public boolean hasOffHeapLNs() {

        return !offHeapLNIds.isEmpty();
    }

    public void setOffHeapLruId(int id) {

        assert id >= 0 || !hasOffHeapLNs();

        offHeapLruId = id;
    }

    public int getOffHeapLruId() {
        return offHeapLruId;
    }

    void freeOffHeapLN(int idx) {
        getOffHeapCache().freeLN(this, idx);
    }

    /**
     * Updates the vlsnCache when an LN target is evicted.  See vlsnCache.
     */
    @Override
    void setTarget(int idx, Node target) {

        if (target == null) {
            final Node oldTarget = getTarget(idx);
            if (oldTarget instanceof LN) {
                setCachedVLSN(idx, ((LN) oldTarget).getVLSNSequence());
            }
        }
        super.setTarget(idx, target);
    }

    /**
     * Overridden to account for BIN-specific slot info.
     */
    @Override
    void appendEntryFromOtherNode(IN from, int fromIdx) {

        super.appendEntryFromOtherNode(from, fromIdx);

        final BIN fromBin = (BIN) from;
        final int idx = nEntries - 1;

        setCachedVLSNUnconditional(idx, fromBin.getCachedVLSN(fromIdx));
        setLastLoggedSizeUnconditional(idx, from.getLastLoggedSize(fromIdx));

        setExpiration(
            idx, fromBin.getExpiration(fromIdx),
            fromBin.isExpirationInHours());

        final OffHeapCache ohCache = getOffHeapCache();

        if (ohCache.isEnabled()) {

            offHeapLNIds = offHeapLNIds.set(
                idx, fromBin.offHeapLNIds.get(fromIdx), this);

            ohCache.ensureOffHeapLNsInLRU(this);
        }
    }

    /**
     * Overridden to account for BIN-specific slot info.
     */
    @Override
    void copyEntries(int from, int to, int n) {
        super.copyEntries(from, to, n);
        vlsnCache = vlsnCache.copy(from, to, n, this);
        lastLoggedSizes = lastLoggedSizes.copy(from, to, n, this);
        expirationValues = expirationValues.copy(from, to, n, this);
        offHeapLNIds = offHeapLNIds.copy(from, to, n, this);
    }

    /**
     * Overridden to account for BIN-specific slot info.
     */
    @Override
    void clearEntry(int idx) {
        super.clearEntry(idx);
        setCachedVLSNUnconditional(idx, VLSN.NULL_VLSN_SEQUENCE);
        setLastLoggedSizeUnconditional(idx, 0);
        setExpiration(idx, 0, false);
        offHeapLNIds = offHeapLNIds.set(idx, 0, this);
    }

    /*
     * Cursors
     */

    /* public for the test suite. */
    public Set<CursorImpl> getCursorSet() {
       if (cursorSet == null) {
           return Collections.emptySet();
       }
       return cursorSet.copy();
    }

    /**
     * Register a cursor with this BIN.  Caller has this BIN already latched.
     * @param cursor Cursor to register.
     */
    public void addCursor(CursorImpl cursor) {
        assert isLatchExclusiveOwner();
        if (cursorSet == null) {
            cursorSet = new TinyHashSet<CursorImpl>();
        }
        cursorSet.add(cursor);
    }

    /**
     * Unregister a cursor with this bin.  Caller has this BIN already
     * latched.
     *
     * @param cursor Cursor to unregister.
     */
    public void removeCursor(CursorImpl cursor) {
        assert isLatchExclusiveOwner();
        if (cursorSet == null) {
            return;
        }
        cursorSet.remove(cursor);
        if (cursorSet.size() == 0) {
            cursorSet = null;
        }
    }

    /**
     * @return the number of cursors currently referring to this BIN.
     */
    public int nCursors() {

        /*
         * Use a local var to concurrent assignment to the cursorSet field by
         * another thread. This method is called via eviction without latching.
         * LRU-TODO: with the new evictor this method is called with the node
         * EX-latched. So, cleanup after the old evictor is scrapped.
         */
        final TinyHashSet<CursorImpl> cursors = cursorSet;
        if (cursors == null) {
            return 0;
        }
        return cursors.size();
    }

    /**
     * Adjust any cursors that are referring to this BIN.  This method is
     * called during a split operation.  "this" is the BIN being split.
     * newSibling is the new BIN into which the entries from "this" between
     * newSiblingLow and newSiblingHigh have been copied.
     *
     * @param newSibling - the newSibling into which "this" has been split.
     * @param newSiblingLow
     * @param newSiblingHigh - the low and high entry of
     * "this" that were moved into newSibling.
     */
    @Override
    void adjustCursors(
        IN newSibling,
        int newSiblingLow,
        int newSiblingHigh)
    {
        assert newSibling.isLatchExclusiveOwner();
        assert this.isLatchExclusiveOwner();
        if (cursorSet == null) {
            return;
        }
        int adjustmentDelta = (newSiblingHigh - newSiblingLow);
        Iterator<CursorImpl> iter = cursorSet.iterator();

        while (iter.hasNext()) {
            CursorImpl cursor = iter.next();
            int cIdx = cursor.getIndex();
            cursor.assertBIN(this);
            assert newSibling instanceof BIN;

            /*
             * There are four cases to consider for cursor adjustments,
             * depending on (1) how the existing node gets split, and (2) where
             * the cursor points to currently.  In cases 1 and 2, the id key of
             * the node being split is to the right of the splitindex so the
             * new sibling gets the node entries to the left of that index.
             * This is indicated by "new sibling" to the left of the vertical
             * split line below.  The right side of the node contains entries
             * that will remain in the existing node (although they've been
             * shifted to the left).  The vertical bar (^) indicates where the
             * cursor currently points.
             *
             * case 1:
             *
             *   We need to set the cursor's "bin" reference to point at the
             *   new sibling, but we don't need to adjust its index since that
             *   continues to be correct post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *         cursor ^
             *
             * case 2:
             *
             *   We only need to adjust the cursor's index since it continues
             *   to point to the current BIN post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *                              cursor ^
             *
             * case 3:
             *
             *   Do nothing.  The cursor continues to point at the correct BIN
             *   and index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *         cursor ^
             *
             * case 4:
             *
             *   Adjust the "bin" pointer to point at the new sibling BIN and
             *   also adjust the index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *                                 cursor ^
             */
            BIN ns = (BIN) newSibling;
            if (newSiblingLow == 0) {
                if (cIdx < newSiblingHigh) {
                    /* case 1 */
                    iter.remove();
                    cursor.setBIN(ns);
                    ns.addCursor(cursor);
                } else {
                    /* case 2 */
                    cursor.setIndex(cIdx - adjustmentDelta);
                }
            } else {
                if (cIdx >= newSiblingLow) {
                    /* case 4 */
                    cursor.setIndex(cIdx - newSiblingLow);
                    iter.remove();
                    cursor.setBIN(ns);
                    ns.addCursor(cursor);
                }
            }
        }
    }

    /**
     * For each cursor in this BIN's cursor set, ensure that the cursor is
     * actually referring to this BIN.
     */
    public void verifyCursors() {
        if (cursorSet == null) {
            return;
        }
        for (CursorImpl cursor : cursorSet) {
            cursor.assertBIN(this);
        }
    }

    /**
     * Adjust cursors referring to this BIN following an insert.
     *
     * @param insertIndex - The index of the new entry.
     */
    @Override
    void adjustCursorsForInsert(int insertIndex) {

        assert this.isLatchExclusiveOwner();
        if (cursorSet == null) {
            return;
        }

        for (CursorImpl cursor : cursorSet) {
            int cIdx = cursor.getIndex();
            if (insertIndex <= cIdx) {
                cursor.setIndex(cIdx + 1);
            }
        }
    }

    /**
     * Called when we know we are about to split on behalf of a key that is the
     * minimum (leftSide) or maximum (!leftSide) of this node.  This is
     * achieved by just forcing the split to occur either one element in from
     * the left or the right (i.e. splitIndex is 1 or nEntries - 1).
     */
    @Override
    IN splitSpecial(
        IN parent,
        int parentIndex,
        IN grandParent,
        int maxEntriesPerNode,
        byte[] key,
        boolean leftSide)
        throws DatabaseException {

        int nEntries = getNEntries();

        int index = findEntry(key, true, false);

        boolean exact = (index & IN.EXACT_MATCH) != 0;
        index &= ~IN.EXACT_MATCH;

        if (leftSide && index < 0) {
            return splitInternal(
                parent, parentIndex, grandParent, maxEntriesPerNode, 1);

        } else if (!leftSide && !exact && index == (nEntries - 1)) {
            return splitInternal(
                parent, parentIndex, grandParent, maxEntriesPerNode,
                nEntries - 1);

        } else {
            return split(
                parent, parentIndex, grandParent, maxEntriesPerNode);
        }
    }

    /**
     * Compress a full BIN by removing any slots that are deleted or expired.
     *
     * This must not be a BIN-delta. No cursors can be present on the BIN.
     * Caller is responsible for latching and unlatching this node.
     *
     * If the slot containing the identifier is removed, the identifier key
     * will be changed to the key in the first remaining slot.
     *
     * Normally when a slot is removed, the IN is dirtied. However, during
     * compression the BIN is not dirtied when a slot is removed. This is safe
     * for the reasons described below. Note that the BIN being compressed is
     * always a full BIN, not a delta.
     *
     *  + If the BIN is not dirty and it does not become dirty before shutdown,
     *  i.e., it is not logged, then it is possible that this compression will
     *  be "lost". However, the state of the slot on disk is expired/deleted,
     *  and when the BIN is later fetched from disk, this state will be
     *  restored and the compression will be performed again.
     *
     *  + If the slot is dirty, the BIN may also be dirty or may become dirty
     *  later, and be logged. Logging a delta would cause the information in
     *  the dirty slot to be lost. Therefore, when a dirty slot is removed, we
     *  set a flag that prohibits the next BIN logged from being a delta.
     *
     * This optimization (that we don't dirty the BIN and we allow logging a
     * delta after removing a non-dirty slot) has one minor and one major
     * impact:
     *
     * 1. When a slot is removed for a deleted record, normally the slot and
     * the BIN will be dirty. Although it is unusual, we may encounter a
     * non-dirty slot for a deleted record. This happens if the slot could not
     * be removed by this method when a full BIN is logged, due to a lock or a
     * cursor, and we compress the full BIN later.
     *
     * 2. When a slot is removed for an expired record, it is common that the
     * slot will not be be dirty. In this case, without the optimization, the
     * removal of expired slots would cause more logging and less deltas would
     * be logged.
     *
     * @param localTracker is used only for temporary DBs, and may be specified
     * to consolidate multiple tracking operations.  If null, the tracking is
     * performed immediately in this method.
     *
     * @return true if all deleted and expired slots were compressed, or false
     * if one or more slots could not be compressed because we were unable to
     * obtain a lock. A false return value means "try again later".
     */
    public boolean compress(boolean compressDirtySlots,
                            final LocalUtilizationTracker localTracker) {

        /*
         * If the environment is not yet recovered we can't rely on locks
         * being set up to safeguard active data and so we can't compress
         * safely.
         */
        if (!databaseImpl.getEnv().isValid()) {
            return true;
        }

        if (nCursors() > 0) {
            throw EnvironmentFailureException.unexpectedState();
        }

        if (isBINDelta()) {
            throw EnvironmentFailureException.unexpectedState();
        }

        final DatabaseImpl db = getDatabase();
        final EnvironmentImpl envImpl = db.getEnv();

        final LockManager lockManager =
            envImpl.getTxnManager().getLockManager();

        boolean setNewIdKey = false;
        boolean anyLocked = false;

        for (int i = 0; i < getNEntries(); i++) {

            if (!compressDirtySlots && isDirty(i)) {
                continue;
            }

            final boolean expired =
                envImpl.isExpired(getExpiration(i), isExpirationInHours());

            final boolean deleted = isDeleted(i);

            if (!deleted && !expired) {
                continue;
            }

            /*
             * We have to be able to lock the LN before we can compress the
             * entry. If we can't, then skip over it. For a deleted record, a
             * read lock is sufficient because it means the deletion has been
             * committed, and other lockers don't hold read locks on a deleted
             * record. For an expired record, a write lock is needed to prevent
             * removal of a slot for a record that is read-locked elsewhere.
             * In both cases it is more efficient to call isLockUncontended
             * than to actually lock the LN, since we would release the lock
             * immediately.
             *
             * We must be able to lock the LN even if isKnownDeleted is true,
             * because locks protect the aborts. (Aborts may execute multiple
             * operations, where each operation latches and unlatches. It's the
             * LN lock that protects the integrity of the whole multi-step
             * process.)
             *
             * For example, during abort, there may be cases where we have
             * deleted and then added an LN during the same txn.  This means
             * that to undo/abort it, we first delete the LN (leaving
             * knownDeleted set), and then add it back into the tree.  We want
             * to make sure the entry is in the BIN when we do the insert back
             * in.
             */
            final long lsn = getLsn(i);

            /* Can discard a NULL_LSN entry without locking. */
            if (lsn != DbLsn.NULL_LSN &&
                !lockManager.isLockUncontended(lsn)) {

                anyLocked = true;
                continue;
            }

            /* At this point, we know we can remove the slot. */
            if (entryKeys.compareKeys(
                getIdentifierKey(), keyPrefix, i, haveEmbeddedData(i),
                getKeyComparator()) == 0) {

                /*
                 * We're about to remove the entry with the idKey so the
                 * node will need a new idkey.
                 */
                setNewIdKey = true;
            }

            /*
             * When we compress a deleted slot in a deferred-write DB, we
             * must either log a dirty LN or count it obsolete. However, if
             * we compress an expired slot, neither is necessary; see TTL.
             */
            if (!expired && db.isDeferredWriteMode()) {

                final LN ln = (LN) getTarget(i);

                if (ln != null &&
                    ln.isDirty() &&
                    !DbLsn.isTransient(lsn)) {

                    if (db.isTemporary()) {

                        /*
                         * When a previously logged LN in a temporary DB is
                         * dirty, we can count the LSN of the last logged LN as
                         * obsolete without logging. There is no requirement
                         * for the dirty deleted LN to be durable past
                         * recovery.  There is no danger of the last logged LN
                         * being accessed again (after log cleaning, for
                         * example), since temp DBs do not survive recovery.
                         */
                        if (localTracker != null) {
                            localTracker.countObsoleteNode(
                                lsn, ln.getGenericLogType(),
                                getLastLoggedSize(i), db);
                        } else {
                            envImpl.getLogManager().countObsoleteNode(
                                lsn, ln.getGenericLogType(),
                                getLastLoggedSize(i), db,
                                true /*countExact*/);
                        }
                    } else {

                        /*
                         * When a previously logged deferred-write LN is dirty,
                         * we log the dirty deleted LN to make the deletion
                         * durable. The act of logging will also count the last
                         * logged LSN as obsolete.
                         */
                        logDirtyLN(i, ln, true /*allowEviction*/);
                    }
                }
            }

            deleteEntry(i, false /*makeDirty*/, true /*validate*/);

            /* Since we're deleting the current entry, decrement the index. */
            i--;
        }

        if (getNEntries() != 0 && setNewIdKey) {
            setIdentifierKey(getKey(0), false /*makeDirty*/);
        }

        if (getNEntries() == 0) {
            /* This BIN is empty and expendable. */
            updateLRU(CacheMode.MAKE_COLD); // TODO actually make cold
        }

        /*
         * Reduce capacity if this BIN is larger than the configured capacity,
         * and has less entries then the configured capacity. This could be due
         * to enlarging the BIN during recovery (see reconstituteBIN) or
         * because the configured capacity was changed.
         */
        final int configuredCapacity = databaseImpl.getNodeMaxTreeEntries();
        if (getMaxEntries() > configuredCapacity &&
            getNEntries() < configuredCapacity) {
            resize(configuredCapacity);
        }

        return !anyLocked;
    }

    /**
     * This method is called opportunistically at certain places where a
     * deleted slot is observed (when the slot's PendingDeleted or KnownDeleted
     * flag is set), to ensure that the slot is compressed away. This is an
     * attempt to process slots that were not compressed during the mainstream
     * record deletion process because of cursors on the BIN during compress,
     * or a crash prior to compression.
     */
    public void queueSlotDeletion(final int idx) {

        /*
         * If the next logrec for this BIN should be a BIN-delta, don't queue
         * the BIN if the deleted slot is dirty, because removing dirty BIN
         * slots prevents logging a delta.
         */
        if (isDirty(idx) && shouldLogDelta()) {
            return;
        }

        getEnv().addToCompressorQueue(this);
    }

    /* For debugging.  Overrides method in IN. */
    @Override
    boolean validateSubtreeBeforeDelete(int index) {

        assert(!isBINDelta());

        return true;
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It may not have any LN children.
     *
     * We assume that this is only called under an assert.
     */
    @Override
    boolean isValidForDelete()
        throws DatabaseException {

        assert(isLatchExclusiveOwner());

        if (isBINDelta()) {
            return false;
        }

        int numValidEntries = 0;

        for (int i = 0; i < getNEntries(); i++) {
            if (!isEntryKnownDeleted(i)) {
                numValidEntries++;
            }
        }

        if (numValidEntries > 0) { // any valid entries, not eligible
            return false;
        }
        if (nCursors() > 0) {      // cursors on BIN, not eligible
            return false;
        }
        return true;               // 0 entries, no cursors
    }

    @Override
    public long compactMemory() {
        final long oldSize = inMemorySize;
        super.compactMemory();
        offHeapLNIds = offHeapLNIds.compact(this, EMPTY_OFFHEAP_LN_IDS);
        expirationValues = expirationValues.compact(this, EMPTY_EXPIRATION);
        return oldSize - inMemorySize;
    }

    /**
     * Adds vlsnCache size to computed memory size.
     */
    @Override
    public long computeMemorySize() {

        long size = super.computeMemorySize();

        /*
         * vlsnCache, lastLoggedSizes, etc, are null only when this method is
         * called by the superclass constructor, i.e., before this class
         * constructor has run. Luckily the initial representations have a
         * memory size of zero, so we can ignore them in this case.
         */
        if (vlsnCache != null) {
            size += vlsnCache.getMemorySize();
        }

        if (lastLoggedSizes != null) {
            size += lastLoggedSizes.getMemorySize();
        }

        if (expirationValues != null) {
            size += expirationValues.getMemorySize();
        }

        if (offHeapLNIds != null) {
            size += offHeapLNIds.getMemorySize();
        }

        if (bloomFilter != null) {
            size += BINDeltaBloomFilter.getMemorySize(bloomFilter);
        }

        return size;
    }

    /* Utility method used during unit testing. */
    @Override
    protected long printMemorySize() {
        final long inTotal = super.printMemorySize();
        final long vlsnCacheOverhead = vlsnCache.getMemorySize();
        final long logSizesOverhead = lastLoggedSizes.getMemorySize();
        final long expirationOverhead = expirationValues.getMemorySize();
        final long offHeapLNIdOverhead = offHeapLNIds.getMemorySize();

        final long binTotal = inTotal +
            vlsnCacheOverhead + logSizesOverhead + offHeapLNIdOverhead;

        System.out.format(
            "BIN: %d vlsns: %d logSizes: %d expiration: %d offHeapLNIds: %d %n",
            binTotal, vlsnCacheOverhead, logSizesOverhead, expirationOverhead,
            offHeapLNIdOverhead);

        return binTotal;
    }

    @Override
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.BIN_FIXED_OVERHEAD;
    }

    /**
     * Returns the treeAdmin memory in objects referenced by this BIN.
     * Specifically, this refers to the DbFileSummaryMap held by
     * MapLNs
     */
    @Override
    public long getTreeAdminMemorySize() {

        if (getDatabase().getId().equals(DbTree.ID_DB_ID)) {
            long treeAdminMem = 0;
            for (int i = 0; i < getMaxEntries(); i++) {
                Node n = getTarget(i);
                if (n != null) {
                    MapLN mapLN = (MapLN) n;
                    treeAdminMem += mapLN.getDatabase().getTreeAdminMemory();
                }
            }
            return treeAdminMem;
        } else {
            return 0;
        }
    }

    /**
     * Reduce memory consumption. Note that evicting deferred-write LNs may
     * require logging them, which will mark this BIN dirty. Compression of
     * deleted slots will also mark the BIN dirty.
     *
     * The BIN should be latched by the caller.
     *
     * @return a long number encoding (a) the number of evicted bytes, and
     * (b) whether this BIN  is evictable. (b) will be false if the BIN has
     * any cursors on it, or has any non-evictable children.
     */
    @Override
    public long partialEviction() {

        /* Try compressing non-dirty slots. */
        final long oldMemSize = inMemorySize;
        getEnv().lazyCompress(this);
        if (oldMemSize > inMemorySize) {
            return oldMemSize - inMemorySize;
        }

        /* Try LN eviction. Return if any were evicted. */
        final long lnBytesAndStatus = evictLNs();
        if ((lnBytesAndStatus & ~IN.NON_EVICTABLE_IN) != 0) {
            return lnBytesAndStatus;
        }

        /* Try discarding the VLSNCache. Return bytes and evictable status. */
        return discardVLSNCache() | lnBytesAndStatus;
    }

    public long discardVLSNCache() {

        final long vlsnBytes = vlsnCache.getMemorySize();

        if (vlsnBytes > 0) {

            int numEntries = getNEntries();
            for (int i = 0; i < numEntries; ++i) {
                if (isEmbeddedLN(i)) {
                    return 0;
                }
            }

            vlsnCache = EMPTY_VLSNS;
            updateMemorySize(0 - vlsnBytes);
        }

        return vlsnBytes;
    }

    /**
     * Reduce memory consumption by evicting all LN targets. Note that this may
     * cause LNs to be logged, which will mark this BIN dirty.
     *
     * The BIN should be latched by the caller.
     *
     * @return a long number encoding (a) the number of evicted bytes, and
     * (b) whether this BIN  is evictable. (b) will be false if the BIN has
     * any cursors on it, or has any non-evictable children.
     */
    public long evictLNs()
        throws DatabaseException {

        assert isLatchExclusiveOwner() :
            "BIN must be latched before evicting LNs";

        /*
         * We can't evict an LN which is pointed to by a cursor, in case that
         * cursor has a reference to the LN object. We'll take the cheap choice
         * and avoid evicting any LNs if there are cursors on this BIN. We
         * could do a more expensive, precise check to see entries have which
         * cursors. This is something we might move to later.
         */
        if (nCursors() > 0) {
            return IN.NON_EVICTABLE_IN;
        }

        /* Try to evict each child LN. */
        long totalRemoved = 0;
        long numLNsEvicted = 0;
        boolean haveNonEvictableLN = false;

        for (int i = 0; i < getNEntries(); i++) {

            if (getTarget(i) == null) {
                continue;
            }

            long lnRemoved = evictLNInternal(i, false /*ifFetchedCold*/);

            if (lnRemoved < 0) {
                haveNonEvictableLN = true;
            } else {
                totalRemoved += lnRemoved;
                ++numLNsEvicted;
            }
        }

        /*
         * compactMemory() may decrease the memory footprint by mutating the
         * representations of the target and key sets.
         */
        if (totalRemoved > 0) {
            updateMemorySize(totalRemoved, 0);
            totalRemoved += compactMemory();
        }

        getEvictor().incNumLNsEvicted(numLNsEvicted);

        if (haveNonEvictableLN) {
            return (totalRemoved | IN.NON_EVICTABLE_IN);
        } else {
            return totalRemoved;
        }
    }

    public void evictLN(int index) {
        evictLN(index, false /*ifFetchedCold*/);
    }

    public void evictLN(int index, boolean ifFetchedCold)
        throws DatabaseException {

        final long removed = evictLNInternal(index, ifFetchedCold);

        /* May decrease the memory footprint by changing the INTargetRep. */
        if (removed > 0) {
            updateMemorySize(removed, 0);
            compactMemory();
        }
    }

    /**
     * Evict a single LN if allowed. The amount of memory freed is returned
     * and must be subtracted from the memory budget by the caller.
     *
     * @param ifFetchedCold If true, evict the LN only if it has the
     * FetchedCold flag set.
     *
     * @return number of evicted bytes or -1 if the LN is not evictable.
     */
    private long evictLNInternal(int index, boolean ifFetchedCold)
        throws DatabaseException {

        final Node n = getTarget(index);

        assert(n == null || n instanceof LN);

        if (n == null) {
            return 0;
        }

        final LN ln = (LN) n;

        if (ifFetchedCold && !ln.getFetchedCold()) {
            return 0;
        }

        /*
         * Don't evict MapLNs for open databases (LN.isEvictable) [#13415].
         */
        if (!ln.isEvictable(getLsn(index))) {
            return -1;
        }

        /*
         * Log target if necessary. Do not allow eviction since we evict
         * here and that would cause double-counting of the memory freed.
         */
        logDirtyLN(index, ln, false /*allowEviction*/);

        /* Clear target. */
        setTarget(index, null);
        ln.releaseMemoryBudget();

        final OffHeapCache ohCache = getOffHeapCache();
        if (ohCache.isEnabled()) {
            ohCache.storeEvictedLN(this, index, ln);
        }

        return n.getMemorySizeIncludedByParent();
    }

    /**
     * @see IN#logDirtyChildren
     */
    @Override
    public void logDirtyChildren()
        throws DatabaseException {

        /* Look for LNs that are dirty or have never been logged before. */
        for (int i = 0; i < getNEntries(); i++) {
            Node node = getTarget(i);
            if (node != null) {
                logDirtyLN(i, (LN) node, true /*allowEviction*/);
            }
        }
    }

    /**
     * Logs the LN at the given index if it is dirty.
     */
    private void logDirtyLN(
        int idx,
        LN ln,
        boolean allowEviction)
        throws DatabaseException {

        final long currLsn = getLsn(idx);

        final boolean force = getDatabase().isDeferredWriteMode() &&
                              DbLsn.isTransientOrNull(currLsn);

        if (force || ln.isDirty()) {
            final DatabaseImpl dbImpl = getDatabase();
            final EnvironmentImpl envImpl = dbImpl.getEnv();

            /* Only deferred write databases should have dirty LNs. */
            assert(dbImpl.isDeferredWriteMode() || ln instanceof MapLN);

            /*
             * Do not lock while logging.  Locking of new LSN is performed by
             * lockAfterLsnChange. This should never be part of the replication
             * stream, because this is a deferred-write DB.
             *
             * No reason to include the previous record version in this logrec
             * because this logrec will never be undone (DW databases are
             * non-transactional)
             */
            final LogItem logItem = ln.log(
                envImpl, dbImpl, null /*locker*/, null /*writeLockInfo*/,
                isEmbeddedLN(idx), getKey(idx),
                getExpiration(idx), isExpirationInHours(),
                isEmbeddedLN(idx), currLsn, getLastLoggedSize(idx),
                false/*isInsertion*/, true /*backgroundIO*/,
                ReplicationContext.NO_REPLICATE);

            updateEntry(
                idx, logItem.lsn, ln.getVLSNSequence(),
                logItem.size);

            /* Lock new LSN on behalf of existing lockers. */
            CursorImpl.lockAfterLsnChange(
                dbImpl, currLsn, logItem.lsn, null /*excludeLocker*/);

            /*
             * It is desirable to evict a non-dirty LN that is immediately
             * obsolete, because it will never be fetched again.
             */
            if (allowEviction &&
                (databaseImpl.isLNImmediatelyObsolete() ||
                 isEmbeddedLN(idx))) {
                evictLN(idx);
            }
        }
    }

    /*
     * Logging support
     */

    /**
     * @see IN#getLogType
     */
    @Override
    public LogEntryType getLogType() {
        return LogEntryType.LOG_BIN;
    }

    /**
     * Overrides the IN method to account for deltas.
     * Public for unit testing.
     */
    @Override
    public long getLastDeltaLsn() {
        return lastDeltaVersion;
    }

    public void setLastDeltaLsn(long lsn) {
        lastDeltaVersion = lsn;
    }

    /*
     * BIN delta support
     */

    public int getFullBinNEntries() {
        if (isBINDelta()) {
            return fullBinNEntries;
        } else {
            return nEntries;
        }
    }

    public void setFullBinNEntries(int n) {
        assert(isBINDelta(false));
        fullBinNEntries = n;
    }

    void incFullBinNEntries() {
        assert(isBINDelta());
        ++fullBinNEntries;
    }

    public int getFullBinMaxEntries() {
        if (isBINDelta()) {
            return fullBinMaxEntries;
        } else {
            return getMaxEntries();
        }
    }

    public void setFullBinMaxEntries(int n) {
        assert(isBINDelta(false));
        fullBinMaxEntries = n;
    }

    int getDeltaCapacity(int numDirtyEntries) {

        boolean blindOps =
            (getEnv().allowBlindOps() || getEnv().allowBlindPuts());

        if (isBINDelta()) {
            return getMaxEntries();
        }

        if (blindOps) {
            return (getNEntries() * databaseImpl.getBinDeltaPercent()) / 100;
        }

        return numDirtyEntries;
    }

    boolean allowBlindPuts() {
        boolean res = getEnv().allowBlindPuts();

        if (res) {
            res = res && databaseImpl.hasBtreeBinaryEqualityComparator();
            res = res && databaseImpl.hasDuplicateBinaryEqualityComparator();
        }

        return res;
    }

    /*
     * It is called in 3 cases listed below. In all cases, if blind puts are
     * not allowed, the method returns null.
     *
     * 1. A full BIN is being mutated to an in-memory delta. A new filter will
     *    be created here and will be stored in the delta by the caller.
     * 2. A full BIN is being logged as a delta. A new filter will be created
     *    here and will be written in the delta logrec by the caller.
     * 3. An in-memory BIN-delta is being logged. If the delta has a bloom
     *    filter already, that filter will be returned and written into the
     *    logrec. The delta may not have a filter already because it was read
     *    from an older-version logfile; in this case we return null.
     */
    byte[] createBloomFilter() {

        assert(bloomFilter == null || isBINDelta());

        boolean blindPuts = allowBlindPuts();

        if (!blindPuts) {
            assert(bloomFilter == null);
            return null;
        }

        if (bloomFilter != null) {
            /*
             * We are here because we are logging a delta that has a filter
             * already. We just need to log the existing filter.
             */
            return bloomFilter;
        }

        if (isBINDelta()) {
            return null;
        }

        int numKeys = getNEntries() - getNDeltas();
        int nbytes = BINDeltaBloomFilter.getByteSize(numKeys);

        byte[] bf = new byte[nbytes];

        BINDeltaBloomFilter.HashContext hc =
            new BINDeltaBloomFilter.HashContext();

        if (keyPrefix != null) {
            hc.hashKeyPrefix(keyPrefix);
        }

        for (int i = 0; i < getNEntries(); ++i) {

            if (isDirty(i)) {
                continue;
            }

            byte[] suffix = entryKeys.getKey(i, haveEmbeddedData(i));
            if (suffix == null) {
                suffix = Key.EMPTY_KEY;
            }

            BINDeltaBloomFilter.add(bf, suffix, hc);
        }

        return bf;
    }

    public boolean mayHaveKeyInFullBin(byte[] key) {

        assert(isBINDelta());

        if (bloomFilter == null) {
            return true;
        }

        return BINDeltaBloomFilter.contains(bloomFilter, key);
    }

    /*
     * Used in IN.getLogSize() only
     */
    int getBloomFilterLogSize() {

        if (!allowBlindPuts()) {
            return 0;
        }

        if (isBINDelta()) {
            if (bloomFilter != null) {
                return BINDeltaBloomFilter.getLogSize(bloomFilter);
            }

            return 0;

        } else {
            assert(bloomFilter == null);
            int numKeys = getNEntries() - getNDeltas();
            return BINDeltaBloomFilter.getLogSize(numKeys);
        }
    }

    boolean isDeltaProhibited() {
        return (getProhibitNextDelta() ||
            getDatabase().isDeferredWriteMode() ||
            getLastFullLsn() == DbLsn.NULL_LSN);
    }

    /**
     * Decide whether to log a full or partial BIN, depending on the ratio of
     * the delta size to full BIN size.
     *
     * Other factors are taken into account:
     * + a delta cannot be logged if the BIN has never been logged before
     * + deltas are not currently supported for DeferredWrite databases
     * + this particular delta may have been prohibited because the cleaner is
     *   migrating the BIN or a dirty slot has been removed
     * + if there are no dirty slots, we might as well log a full BIN
     *
     * The restriction on using BIN-deltas for deferred-write DBs is for
     * reasons that are probably no longer relevant. However, we have not
     * tested deltas with DW, so we still prohibit them. Because BIN-deltas
     * may be in cache at the time a DB is opened in DW mode, a workaround is
     * currently necessary: see Database.mutateDeferredWriteBINDeltas.
     *
     * @return true if we should log the deltas of this BIN
     */
    public boolean shouldLogDelta() {

        if (isBINDelta()) {
            /*
             * Cannot assert that db is not in DeferredWrite mode.
             * See Database.mutateDeferredWriteBINDeltas.
             */
            assert !getProhibitNextDelta();
            assert getLastFullLsn() != DbLsn.NULL_LSN;
            return true;
        }

        /* Cheapest checks first. */
        if (isDeltaProhibited()) {
            return false;
        }

        /* Must count deltas to check further. */
        final int numDeltas = getNDeltas();

        /* A delta with zero items is not valid. */
        if (numDeltas <= 0) {
            return false;
        }

        /* Check the configured BinDeltaPercent. */
        final int deltaLimit =
            (getNEntries() * databaseImpl.getBinDeltaPercent()) / 100;

        return numDeltas <= deltaLimit;
    }

    /**
     * Returns whether mutateToBINDelta can be called.
     */
    public boolean canMutateToBINDelta() {
        return (!isBINDelta() &&
                shouldLogDelta() &&
                (nCursors() == 0));
    }

    /**
     * Mutate to a delta (discard non-dirty entries and resize arrays).
     *
     * This method must be called with this node latched exclusively, and
     * canMutateToBINDelta must return true.
     *
     * @return the number of bytes freed.
     */
    public long mutateToBINDelta() {

        assert isLatchExclusiveOwner();
        assert canMutateToBINDelta();

        if (getInListResident()) {
            getEnv().getInMemoryINs().updateBINDeltaStat(1);
        }

        final long oldSize = getInMemorySize();
        final int nDeltas = getNDeltas();
        final int capacity = getDeltaCapacity(nDeltas);

        bloomFilter = createBloomFilter();

        initBINDelta(this, nDeltas, capacity, true);

        return oldSize - getInMemorySize();
    }

    /**
     * This method assumes that "this" BIN is a delta and creates a clone of
     * it. It is currently used by the DiskOrderedScanner only. The method
     * does not clone the targets array.
     */
    public BIN cloneBINDelta() {

        assert(isBINDelta());

        final BIN bin = new BIN(
            databaseImpl, getIdentifierKey(), 0/*capacity*/, getLevel());

        bin.nodeId = nodeId;
        bin.flags = flags;
        bin.lastFullVersion = lastFullVersion;

        final int nDeltas = getNDeltas();
        initBINDelta(bin, nDeltas, nDeltas, false);
        return bin;
    }

    /**
     * Replaces the contents of destBIN with the deltas in this BIN.
     */
    private void initBINDelta(
        final BIN destBIN,
        final int nDeltas,
        final int capacity,
        final boolean copyTargets) {

        long[] longLSNs = null;
        byte[] compactLSNs = null;

        if (entryLsnLongArray == null) {
            compactLSNs = new byte[nDeltas * 4];
        } else {
            longLSNs = new long[nDeltas];
        }

        final long[] vlsns = new long[nDeltas];
        final int[] sizes = new int[nDeltas];
        final byte[][] keys = new byte[nDeltas][];
        final byte[] states = new byte[nDeltas];
        long[] memIds = null;
        Node[] targets = null;
        int[] expiration = null;

        if (copyTargets) {
            targets = new Node[nDeltas];
            memIds = new long[nDeltas];
        }

        if (expirationBase != -1) {
            expiration = new int[nDeltas];
        }

        int j = 0;
        for (int i = 0; i < getNEntries(); i += 1) {

            if (!isDirty(i)) {
                freeOffHeapLN(i);
                continue;
            }

            if (entryLsnLongArray == null) {
                int doff = j << 2;
                int soff = i << 2;
                compactLSNs[doff] = entryLsnByteArray[soff];
                compactLSNs[doff+1] = entryLsnByteArray[soff+1];
                compactLSNs[doff+2] = entryLsnByteArray[soff+2];
                compactLSNs[doff+3] = entryLsnByteArray[soff+3];
            } else {
                longLSNs[j] = getLsn(i);
            }

            keys[j] = entryKeys.get(i);
            states[j] = getState(i);

            if (targets != null) {
                targets[j] = getTarget(i);
            }

            if (memIds != null) {
                memIds[j] = getOffHeapLNId(i);
            }

            vlsns[j] = getCachedVLSN(i);
            sizes[j] = getLastLoggedSize(i);

            if (expiration != null) {
                expiration[j] = getExpiration(i);
            }

            j += 1;
        }

        /*
         * Do this before resetContent() because destBIN and "this" may be the
         * same java obj
         */
        destBIN.fullBinNEntries = getFullBinNEntries();
        destBIN.fullBinMaxEntries = getFullBinMaxEntries();

        destBIN.resetContent(
            capacity, nDeltas,
            baseFileNumber, compactLSNs, longLSNs,
            states, keyPrefix, keys, targets,
            sizes, memIds, vlsns,
            expiration, isExpirationInHours());

        destBIN.setBINDelta(true);

        destBIN.compactMemory();
    }

    /**
     * Replaces the contents of this BIN with the given contents.
     * Used in mutating a full BIN to a BIN-delta or for creating
     * a new BIN delta with the given content.
     */
    private void resetContent(
        final int capacity,
        final int newNEntries,
        final long baseFileNumber,
        final byte[] compactLSNs,
        final long[] longLSNs,
        final byte[] states,
        final byte[] keyPrefix,
        final byte[][] keys,
        final Node[] targets,
        final int[] loggedSizes,
        final long[] memIds,
        final long[] vlsns,
        final int[] expiration,
        final boolean expirationInHours) {

        updateRepCacheStats(false);

        nEntries = newNEntries;

        this.baseFileNumber = baseFileNumber;
        if (longLSNs == null) {
            entryLsnByteArray = new byte[capacity << 2];
            entryLsnLongArray = null;
        } else {
            entryLsnByteArray = null;
            entryLsnLongArray = new long[capacity];
        }

        this.keyPrefix = keyPrefix;

        entryKeys = new INKeyRep.Default(capacity);
        entryTargets = INTargetRep.NONE;
        vlsnCache = EMPTY_VLSNS;
        lastLoggedSizes = EMPTY_LAST_LOGGED_SIZES;
        expirationValues = EMPTY_EXPIRATION;
        expirationBase = -1;
        offHeapLNIds = EMPTY_OFFHEAP_LN_IDS;

        updateRepCacheStats(true);

        entryStates = new byte[capacity];

        for (int i = 0; i < newNEntries; i += 1) {

            if (longLSNs == null) {
                int off = i << 2;
                entryLsnByteArray[off] = compactLSNs[off];
                entryLsnByteArray[off+1] = compactLSNs[off+1];
                entryLsnByteArray[off+2] = compactLSNs[off+2];
                entryLsnByteArray[off+3] = compactLSNs[off+3];
            } else {
                entryLsnLongArray[i] = longLSNs[i];
            }

            entryKeys = entryKeys.set(i, keys[i], this);
            entryStates[i] = states[i];

            if (targets != null) {
                entryTargets = entryTargets.set(i, targets[i], this);
            }

            if (memIds != null) {
                setOffHeapLNId(i, memIds[i]);
            }

            if (expiration != null) {
                setExpiration(i, expiration[i], expirationInHours);
            }

            setLastLoggedSizeUnconditional(i, loggedSizes[i]);
            setCachedVLSNUnconditional(i, vlsns[i]);
        }

        updateMemorySize(inMemorySize, computeMemorySize());
    }

    /**
     * Fetch the full BIN and apply the deltas in this BIN to it, then use the
     * merged result to replace the contents of this BIN.
     *
     * This method must be called with this node latched exclusively. If 'this'
     * is not a delta, this method does nothing.
     */
    @Override
    public void mutateToFullBIN(boolean leaveFreeSlot) {

        if (!isBINDelta()) {
            return;
        }

        final BIN fullBIN = fetchFullBIN(databaseImpl);

        mutateToFullBIN(fullBIN, leaveFreeSlot);

        getEvictor().incFullBINMissStats();
    }

    /**
     * Mutates this delta to a full BIN by applying this delta to the fullBIN
     * param and then replacing this BIN's contents with it.
     *
     * This method must be called with this node latched exclusively. 'this'
     * must be a delta.
     *
     * After mutation, the full BIN is compressed and compacted. The
     * compression is particularly important, since BIN-deltas in cache cannot
     * be compressed.
     *
     * The method is public because it is called directly from FileProcessor
     * when it finds a BIN that must be migrated. In that case, fullBIN is a
     * full BIN that has just been read from the log, and it is not part of
     * the memory-resident tree.
     */
    public void mutateToFullBIN(BIN fullBIN, boolean leaveFreeSlot) {

        assert isLatchExclusiveOwner();
        assert isBINDelta() : this;

        byte[][] keys = null;
        int i = 0;

        if (cursorSet != null) {
            keys = new byte[cursorSet.size()][];

            for (CursorImpl cursor : cursorSet) {
                final int index = cursor.getIndex();
                if (index >= 0 && index < getNEntries()) {
                    keys[i] = cursor.getCurrentKey(true/*isLatched*/);
                }
                ++i;
            }
        }

        reconstituteBIN(databaseImpl, fullBIN, leaveFreeSlot);

        resetContent(fullBIN);

        setBINDelta(false);

        /*
         * The fullBIN identifierKey may have changed when reconstituteBIN
         * called BIN.compress. We cannot call setIdentifierKey in resetContent
         * because assert(!isBINDelta()) will fail, so call it here.
         */
        setIdentifierKey(fullBIN.getIdentifierKey(), false);

        if (cursorSet != null) {

            i = 0;
            for (CursorImpl cursor : cursorSet) {

                if (keys[i] != null) {
                    /*
                     * Do not ask for an exact match from findEntry because if
                     * the cursor was on a KD slot, findEntry would return -1.
                     */
                    int index = findEntry(keys[i], true, false);

                    if ((index & IN.EXACT_MATCH) == 0) {
                        throw EnvironmentFailureException.unexpectedState(
                            getEnv(), "Failed to reposition cursor during " +
                            "mutation of a BIN delta to a full BIN");
                    }

                    index &= ~IN.EXACT_MATCH;

                    assert(index >= 0 && index < getNEntries());
                    cursor.setIndex(index);
                }
                ++i;
            }
        }

        getEnv().lazyCompress(this);
        compactMemory();

        if (getInListResident()) {
            getEnv().getInMemoryINs().updateBINDeltaStat(-1);
        }
    }

    private BIN fetchFullBIN(DatabaseImpl dbImpl) {

        final EnvironmentImpl envImpl = dbImpl.getEnv();
        final long lsn = getLastFullLsn();

        try {
            return (BIN)
                envImpl.getLogManager().getEntryHandleFileNotFound(lsn);

        } catch (EnvironmentFailureException e) {
            e.addErrorMessage(makeFetchErrorMsg(null, lsn, -1));
            throw e;

        } catch (RuntimeException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                makeFetchErrorMsg(e.toString(), lsn, -1), e);
        }
    }

    /**
     * Replaces the contents of this BIN with the contents of the given BIN,
     * including lsns, states, keys and targets.  Key prefixing and key/target
     * representations will also be those of the given BIN.
     */
    private void resetContent(final BIN other) {

        updateRepCacheStats(false);

        nEntries = other.nEntries;

        baseFileNumber = other.baseFileNumber;
        entryLsnByteArray = other.entryLsnByteArray;
        entryLsnLongArray = other.entryLsnLongArray;

        keyPrefix = other.keyPrefix;
        entryKeys = other.entryKeys;

        entryTargets = other.entryTargets;

        entryStates = other.entryStates;

        lastLoggedSizes = other.lastLoggedSizes;

        expirationValues = other.expirationValues;
        expirationBase = other.expirationBase;

        offHeapLNIds = other.offHeapLNIds;
        assert (getOffHeapLruId() >= 0) || !hasOffHeapLNs();

        vlsnCache = other.vlsnCache;

        bloomFilter = null;

        updateMemorySize(inMemorySize, computeMemorySize());

        updateRepCacheStats(true);
    }

    private void resize(final int newCapacity) {

        assert newCapacity >= getNEntries();

        updateRepCacheStats(false);

        if (entryLsnByteArray != null) {
            entryLsnByteArray = Arrays.copyOfRange(
                entryLsnByteArray, 0, newCapacity * 4);
        }
        if (entryLsnLongArray != null) {
            entryLsnLongArray = Arrays.copyOfRange(
                entryLsnLongArray, 0, newCapacity);
        }
        if (entryStates != null) {
            entryStates = Arrays.copyOfRange(
                entryStates, 0, newCapacity);
        }

        entryKeys = entryKeys.resize(newCapacity);
        entryTargets = entryTargets.resize(newCapacity);
        lastLoggedSizes = lastLoggedSizes.resize(newCapacity);
        expirationValues = expirationValues.resize(newCapacity);
        offHeapLNIds = offHeapLNIds.resize(newCapacity);
        vlsnCache = vlsnCache.resize(newCapacity);

        updateMemorySize(inMemorySize, computeMemorySize());

        updateRepCacheStats(true);
    }

    /**
     * Create a BIN by fetching its most recent full version from the log and
     * applying to it the deltas in this BIN delta. The new BIN is not added
     * to the INList or the BTree.
     *
     * Called from DiskOrderedScanner.fetchAndProcessBINs() and
     * DiskOrderedScanner.accumulateLNs()
     *
     * @return the full BIN with deltas applied.
     */
    public BIN reconstituteBIN(DatabaseImpl dbImpl) {
        final BIN fullBIN = fetchFullBIN(dbImpl);
        reconstituteBIN(dbImpl, fullBIN, false /*leaveFreeSlot*/);
        return fullBIN;
    }

    /**
     * Given a full version BIN, apply to it the deltas in this BIN delta. The
     * fullBIN will then be complete, but its memory will not be compacted.
     *
     * Called from mutateToFullBIN() above and from SortedLSNTreewalker.
     *
     * @param leaveFreeSlot should be true if a slot will be inserted into the
     * resulting full BIN, without first checking whether the full BIN must be
     * split, and performing the split if necessary. If this param is true, the
     * returned BIN will contain at least one free slot. If this param is
     * false, a BIN with no free slots may be returned. For example, it is
     * important that false is passed when a split will be performed, since if
     * true were passed, the BIN would grow beyond its bounds unnecessarily.
     */
    public void reconstituteBIN(
        DatabaseImpl dbImpl,
        BIN fullBIN,
        boolean leaveFreeSlot) {

        fullBIN.setDatabase(dbImpl);
        fullBIN.latch(CacheMode.UNCHANGED);

        try {
            if (databaseImpl == null) {
                setDatabase(dbImpl);
            }

            assert fullBIN.getOffHeapLruId() < 0;
            assert !fullBIN.hasOffHeapLNs();

            /*
             * The BIN's lastFullLsn is set here, while its lastLoggedLsn is
             * set by postFetchInit or postRecoveryInit.
             */
            fullBIN.setLastFullLsn(getLastFullLsn());

            /*
             * Compress the full BIN before applying deltas, to handle the
             * following scenario: Non-dirty slots were compressed away
             * earlier, leaving room for inserted records, and a delta was
             * logged with the inserted records. The full version of the BIN
             * (after compression) was not logged, because the BIN is not
             * dirtied when non-dirty slots were compressed away. If we don't
             * compress here, there may not be room in the original BIN for the
             * slots inserted when applying the deltas.
             *
             * However, during recovery we can't compress because locking is
             * not used during recovery, and the compressor may delete a slot
             * for a record that is part of an active transaction. In addition,
             * even when compression is performed here, it is possible that it
             * doesn't compress all deleted/expired slots that were compressed
             * originally in the scenario described, for one of the following
             * reasons:
             *
             *  + The record is locked temporarily by a read operation that
             *    will skip the record. Note that the compressor uses
             *    non-blocking locking.
             *
             *  + If expiration has been disabled, or the system clock has been
             *    changed, slots that were expired originally may not be
             *    expired now.
             *
             * Therefore, in all cases we enlarge the BIN if necessary to hold
             * all slots to be inserted when applying the delta. An extra slot
             * is added if leaveFreeSlot is true, to handle cases
             * where mutation to a full BIN is performed after calling
             * Tree.searchSplitsAllowed, or one of the methods that calls it
             * such as Tree.findBinForInsert and Tree.getParentBINForChildLN.
             * If the search returns a BIN-delta without splitting, and then we
             * must mutate to full BIN in order to insert, because blind
             * insertions do not apply, then the scenario described can occur.
             *
             * If the BIN is enlarged, we add it to the compressor queue so it
             * will be shrunk back down to the Database's configured maxEntries
             * during normal compression.
             */
            if (!dbImpl.getEnv().isInInit()) {
                fullBIN.compress(
                    false /*compressDirtySlots*/, null /*localTracker*/);
            }
            int nInsertions = leaveFreeSlot ? 1 : 0;
            for (int i = 0; i < getNEntries(); i += 1) {
                final int foundIndex = fullBIN.findEntry(
                    getKey(i), true, false);
                if (foundIndex < 0 || (foundIndex & IN.EXACT_MATCH) == 0) {
                    nInsertions += 1;
                }
            }
            final int maxEntries = nInsertions + fullBIN.getNEntries();
            if (maxEntries > fullBIN.getMaxEntries()) {
                fullBIN.resize(maxEntries);
                dbImpl.getEnv().addToCompressorQueue(fullBIN);
            }

            /* Process each delta. */
            for (int i = 0; i < getNEntries(); i++) {

                assert isDirty(i) : this;

                fullBIN.applyDelta(
                    getKey(i), getData(i), getLsn(i), getState(i),
                    getLastLoggedSize(i), getOffHeapLNId(i),
                    getCachedVLSN(i), getTarget(i),
                    getExpiration(i), isExpirationInHours());
            }

            /*
             * The applied deltas will leave some slots dirty, which is
             * necessary as a record of changes that will be included in the
             * next delta.  However, the BIN itself should not be dirty,
             * because this delta is a persistent record of those changes.
             */
            fullBIN.setDirty(false);
        } finally {
            fullBIN.releaseLatch();
        }
    }

    /**
     * Apply (insert, update) a given delta slot in this full BIN.
     * Note: also called from OldBINDelta class.
     */
    void applyDelta(
        final byte[] key,
        final byte[] data,
        final long lsn,
        final byte state,
        final int lastLoggedSize,
        final long ohLnId,
        final long vlsn,
        final Node child,
        final int expiration,
        final boolean expirationInHours) {

        /*
         * The delta is the authoritative version of the entry. In all cases,
         * it should supersede the entry in the full BIN.  This is true even if
         * the BIN Delta's entry is knownDeleted or if the full BIN's version
         * is knownDeleted. Therefore we use the flavor of findEntry that will
         * return a knownDeleted entry if the entry key matches (i.e. true,
         * false) but still indicates exact matches with the return index.
         * findEntry only returns deleted entries if third arg is false, but we
         * still need to know if it's an exact match or not so indicateExact is
         * true.
         */
        int foundIndex = findEntry(key, true, false);

        if (foundIndex >= 0 && (foundIndex & IN.EXACT_MATCH) != 0) {

            foundIndex &= ~IN.EXACT_MATCH;

            /*
             * The entry exists in the full version, update it with the delta
             * info.  Note that all state flags should be restored [#22848].
             */
            applyDeltaSlot(
                foundIndex, child, lsn, lastLoggedSize, state, key, data);

        } else {

            /*
             * The entry doesn't exist, insert the delta entry. We insert the
             * entry even when it is known or pending deleted, since the
             * deleted (and dirty) entry will be needed to log the next delta.
             * [#20737]
             */
            final int result = insertEntry1(
                child, key, data, lsn, state, false/*blindInsertion*/);

            assert (result & INSERT_SUCCESS) != 0;
            foundIndex = result & ~IN.INSERT_SUCCESS;

            setLastLoggedSizeUnconditional(foundIndex, lastLoggedSize);
        }

        setCachedVLSNUnconditional(foundIndex, vlsn);
        setOffHeapLNId(foundIndex, ohLnId);
        setExpiration(foundIndex, expiration, expirationInHours);
    }

    /*
     * DbStat support.
     */
    @Override
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
        acc.processBIN(this, Long.valueOf(getNodeId()), getLevel());
    }
}
