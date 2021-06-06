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

package com.sleepycat.je.tree.dupConvert;

import java.util.ArrayList;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Performs post-recovery conversion of all dup DBs during Environment
 * construction, when upgrading from JE 4.1 and earlier.  In JE 5.0, duplicates
 * are represented by a two-part (key + data) key, and empty data.  In JE 4.1
 * and earlier, the key and data were separate as with non-dup DBs.
 *
 * Uses the DbTree.DUPS_CONVERTED_BIT to determine whether conversion of the
 * environment is necessary.  When all databases are successfully converted,
 * this bit is set and the mapping tree is flushed.  See
 * EnvironmentImpl.convertDupDatabases.
 *
 * Uses DatabaseImpl.DUPS_CONVERTED to determine whether an individual database
 * has been converted, to handle the case where the conversion crashes and is
 * restarted later.  When a database is successfully converted, this bit is set
 * and the entire database is flushed using Database.sync.
 *
 * The conversion of each database is atomic -- either all INs or none are
 * converted and made durable.  This is accomplished by putting the database
 * into Deferred Write mode so that splits won't log and eviction will be
 * provisional (eviction will not flush the root IN if it is dirty).  The
 * Deferred Write mode is cleared after conversion is complete and
 * Database.sync has been called.
 *
 * The memory budget is updated during conversion and daemon eviction is
 * invoked periodically.  This provides support for arbitrarily large DBs.
 *
 * Uses preload to load all dup trees (DINs/DBINs) prior to conversion, to
 * minimize random I/O.  See EnvironmentConfig.ENV_DUP_CONVERT_PRELOAD_ALL.
 *
 * The preload config does not specify loading of LNs, because we do not need
 * to load LNs from DBINs.  The fact that DBIN LNs are not loaded is the main
 * reason that conversion is quick.  LNs are converted lazily instead; see
 * LNLogEntry.postFetchInit.  The DBIN LNs do not need to be loaded because the
 * DBIN slot key contains the LN 'data' that is needed to create the two-part
 * key.
 *
 * Even when LN loading is not configured, it turns out that preload does load
 * BIN (not DBIN) LNs in a dup DB, which is what we want.  The singleton LNs
 * must be loaded in order to get the LN data to create the two-part key.  When
 * preload has not loaded a singleton LN, it will be fetched during conversion.
 *
 * The DIN, DBIN and DupCount LSN are counted obsolete during conversion using
 * a local utilization tracker.  The tracker must not be flushed until the
 * conversion of a database is complete.  Inexact counting can be used, because
 * DIN/DBIN/DupCountLN entries are automatically considered obsolete by the
 * cleaner.  Since only totals are tracked, the memory overhead of the local
 * tracker is not substantial.
 *
 * Database Conversion Algorithm
 * -----------------------------
 * 1. Set Deferred Write mode for the database. Preload the database, including
 *    INs/BINs/DINs/DBINs, but not LNs except for singleton LNs (LNs with a BIN
 *    parent).
 *
 * 2. Convert all IN/BIN keys to "prefix keys", which are defined by the
 *    DupKeyData class.  This allows tree searches and slot insertions to work
 *    correctly as the conversion is performed.
 *
 * 3. Traverse through the BIN slots in forward order.
 *
 * 4. If a singleton LN is encountered, ensure it is loaded. IN.fetchLN()
 *    automatically updates the slot key if the LNLogEntry's key is different
 *    from the one already in the slot.  Because LNLogEntry's key is converted
 *    on the fly, a two-part key is set in the slot as a side effect of
 *    fetching the LN.
 *
 * 5. If a DIN is encountered, first delete the BIN slot containing the DIN.
 *    Then iterate through all LNs in the DBINs of this dup tree, assign each
 *    a two-part key, and insert the slot into a BIN.  The LSN and state flags
 *    of the DBIN slot are copied to the new BIN slot.
 *
 * 6. If a deleted singleton (BIN) LN is encountered, delete the slot rather
 *    than converting the key.  If a deleted DBIN LN is encountered, simply
 *    discard it.
 *
 * 7. Count the DIN and DupCount LSN obsolete for each DIN encountered, using
 *    a local utilization tracker.
 *
 * 8. When all BIN slots have been processed, set the
 *    DatabaseImpl.DUPS_CONVERTED flag, call Database.sync to flush all INs and
 *    the MapLN, clear Deferred Write mode, and flush the local utilization
 *    tracker.
 */
public class DupConvert {
    private final static boolean DEBUG = false;

    private final EnvironmentImpl envImpl;
    private final DbTree dbTree;
    private final boolean preloadAll;
    private final PreloadConfig preloadConfig;
    private LocalUtilizationTracker localTracker;
    private long nConverted; // for debugging

    /* Current working tree position. */
    private BIN bin;
    private int index;

    /**
     * Creates a conversion object.
     */
    public DupConvert(final EnvironmentImpl envImpl, final DbTree dbTree) {
        this.envImpl = envImpl;
        this.dbTree = dbTree;
        this.preloadAll = envImpl.getConfigManager().getBoolean
            (EnvironmentParams.ENV_DUP_CONVERT_PRELOAD_ALL);
        this.preloadConfig = (envImpl.getDupConvertPreloadConfig() != null) ?
            envImpl.getDupConvertPreloadConfig() : (new PreloadConfig());
    }

    /**
     * Converts all dup DBs that need conversion.
     */
    public void convertDatabases() {
        if (DEBUG) {
            System.out.println("DupConvert.convertDatabases");
        }
        if (preloadAll) {
            preloadAllDatabases();
        }
        for (DatabaseId dbId : dbTree.getDbNamesAndIds().keySet()) {
            final DatabaseImpl dbImpl = dbTree.getDb(dbId);
            try {
                if (!needsConversion(dbImpl)) {
                    continue;
                }
                convertDatabase(dbImpl);
            } finally {
                dbTree.releaseDb(dbImpl);
            }
        }

        assert noDupNodesPresent();
    }

    private boolean noDupNodesPresent() {
        for (IN in : envImpl.getInMemoryINs()) {
            if (in instanceof DIN || in instanceof DBIN) {
                System.out.println(in.toString());
                return false;
            }
        }
        return true;
    }

    /**
     * Preload all dup DBs to be converted.
     */
    private void preloadAllDatabases() {

        final ArrayList<DatabaseImpl> dbsToConvert =
            new ArrayList<DatabaseImpl>();
        try {
            for (DatabaseId dbId : dbTree.getDbNamesAndIds().keySet()) {
                final DatabaseImpl dbImpl = dbTree.getDb(dbId);
                boolean releaseDbImpl = true;
                try {
                    if (!needsConversion(dbImpl)) {
                        continue;
                    }
                    dbsToConvert.add(dbImpl);
                    releaseDbImpl = false;
                } finally {
                    if (releaseDbImpl) {
                        dbTree.releaseDb(dbImpl);
                    }
                }
            }

            if (dbsToConvert.size() == 0) {
                return;
            }

            final DatabaseImpl[] dbArray =
                new DatabaseImpl[dbsToConvert.size()];
            dbsToConvert.toArray(dbArray);

            envImpl.preload(dbArray, preloadConfig);
        } finally {
            for (DatabaseImpl dbImpl : dbsToConvert) {
                dbTree.releaseDb(dbImpl);
            }
        }
    }

    /**
     * Returns whether the given DB needs conversion.
     */
    public static boolean needsConversion(final DatabaseImpl dbImpl) {
        return (dbImpl.getSortedDuplicates() &&
                !dbImpl.getDupsConverted() &&
                !dbImpl.isDeleted());
    }

    /**
     * Converts a single database.
     */
    private void convertDatabase(final DatabaseImpl dbImpl) {
        if (DEBUG) {
            System.out.println("DupConvert.convertDatabase " +
                               dbImpl.getId());
        }
        final boolean saveDeferredWrite = dbImpl.isDurableDeferredWrite();
        try {
            localTracker = new LocalUtilizationTracker(envImpl);
            dbImpl.setDeferredWrite(true);
            dbImpl.setKeyPrefixing();
            if (!preloadAll) {
                dbImpl.preload(preloadConfig);
            }
            bin = dbImpl.getTree().getFirstNode(CacheMode.UNCHANGED);
            if (bin == null) {
                return;
            }
            index = -1;
            while (getNextBinSlot()) {
                convertBinSlot();
            }
            dbImpl.setDupsConverted();
            dbImpl.sync(false /*flushLog*/);
            envImpl.getUtilizationProfile().flushLocalTracker(localTracker);
        } finally {
            dbImpl.setDeferredWrite(saveDeferredWrite);
        }
    }

    /**
     * Advances the bin/index fields to the next BIN slot.  When moving past
     * the last BIN slot, the bin field is set to null and false is returned.
     *
     * Enter/leave with bin field latched.
     */
    private boolean getNextBinSlot() {
        index += 1;
        if (index < bin.getNEntries()) {
            return true;
        }

        /* Compact keys after finishing with a BIN. */
        bin.compactMemory();

        assert bin.verifyMemorySize();

        /* Cannot evict between BINs here, because a latch is held. */

        bin = bin.getDatabase().getTree().getNextBin(bin, CacheMode.UNCHANGED);
        if (bin == null) {
            return false;
        }
        index = 0;
        return true;
    }

    /**
     * Converts the bin/index slot, whether a singleton LN or a DIN root.
     *
     * Enter/leave with bin field latched, although bin field may change.
     *
     * When a singleton LN is converted, leaves with bin/index fields
     * unchanged.
     *
     * When a dup tree is converted, leaves with bin/index fields set to last
     * inserted slot.  This is the slot of the highest key in the dup tree.
     */
    private void convertBinSlot() {
        if (DEBUG) {
            System.out.println("DupConvert BIN LSN " +
                               DbLsn.getNoFormatString(bin.getLsn(index)) +
                               " index " + index +
                               " nEntries " + bin.getNEntries());
        }
        /* Delete slot if LN is deleted. */
        if (isLNDeleted(bin, index)) {
            deleteSlot();
            return;
        }

        final Node node = bin.fetchLNOrDIN(index, CacheMode.DEFAULT);

        if (!node.containsDuplicates()) {
            if (DEBUG) {
                System.out.println("DupConvert BIN LN " +
                                   Key.dumpString(bin.getKey(index), 0));
            }
            /* Fetching a non-deleted LN updates the slot key; we're done. */
            assert node instanceof LN;
            nConverted += 1;
            return;
        }

        /*
         * Delete the slot containing the DIN before re-inserting the dup tree,
         * so that the DIN slot key doesn't interfere with insertions.
         *
         * The DIN is evicted and memory usage is decremented. This is not
         * exactly correct because we keep a local reference to the DIN until
         * the dup tree is converted, but we tolerate this temporary
         * inaccuracy.
         */
        final byte[] binKey = bin.getKey(index);
        final DIN din = (DIN) node;
        deleteSlot();
        convertDin(din, binKey);
    }
    
    /**
     * Returns true if the LN at the given bin/index slot is permanently
     * deleted.  Returns false if it is not deleted, or if it is deleted but
     * part of an unclosed, resurrected txn.
     *
     * Enter/leave with bin field latched.
     */
    private boolean isLNDeleted(BIN checkBin, int checkIndex) {

        if (!checkBin.isEntryKnownDeleted(checkIndex) &&
            !checkBin.isEntryPendingDeleted(checkIndex)) {
            /* Not deleted. */
            return false;
        }

        final long lsn = checkBin.getLsn(checkIndex);
        if (lsn == DbLsn.NULL_LSN) {
            /* Can discard a NULL_LSN entry without locking. */
            return true;
        }

        /* Lock LSN to guarantee deletedness. */
        final BasicLocker lockingTxn = BasicLocker.createBasicLocker(envImpl);
        /* Don't allow this short-lived lock to be preempted/stolen. */
        lockingTxn.setPreemptable(false);
        try {
            final LockResult lockRet = lockingTxn.nonBlockingLock
                (lsn, LockType.READ, false /*jumpAheadOfWaiters*/,
                 checkBin.getDatabase());
            if (lockRet.getLockGrant() == LockGrantType.DENIED) {
                /* Is locked by a resurrected txn. */
                return false;
            }
            return true;
        } finally {
            lockingTxn.operationEnd();
        }
    }

    /**
     * Deletes the bin/index slot, assigned a new identifier key if needed.
     *
     * Enter/leave with bin field latched.
     */
    private void deleteSlot() {
        bin.deleteEntry(index);
        if (index == 0 && bin.getNEntries() != 0) {
            bin.setIdentifierKey(bin.getKey(0), true /*makeDirty*/);
        }
        index -= 1;
    }

    /**
     * Converts the given DIN and its descendants.
     *
     * Enter/leave with bin field latched, although bin field will change to
     * last inserted slot.
     */
    private void convertDin(final DIN din, final byte[] binKey) {
        din.latch();
        try {
            for (int i = 0; i < din.getNEntries(); i += 1) {

                final IN child = din.fetchIN(i, CacheMode.DEFAULT);

                assert(!child.isBINDelta(false));

                if (child instanceof DBIN) {
                    final DBIN dbin = (DBIN) child;
                    dbin.latch();
                    try {
                        for (int j = 0; j < dbin.getNEntries(); j += 1) {
                            if (!isLNDeleted(dbin, j)) {
                                convertDbinSlot(dbin, j, binKey);
                            }
                        }
                        assert dbin.verifyMemorySize();

                        /* Count DBIN obsolete. */
                        if (dbin.getLastLoggedLsn() != DbLsn.NULL_LSN) {
                            localTracker.countObsoleteNodeInexact
                                (dbin.getLastLoggedLsn(),
                                 dbin.getLogType(), 0, dbin.getDatabase());
                        }
                    } finally {
                        dbin.releaseLatch();
                    }
                } else {
                    convertDin((DIN) child, binKey);
                }

                /* Evict DIN child. */
                din.detachNode(i, false/*updateLsn*/, -1/*lsn*/);
            }

            assert din.verifyMemorySize();

            /* Count DIN and DupCountLN obsolete. */
            if (din.getLastLoggedLsn() != DbLsn.NULL_LSN) {
                localTracker.countObsoleteNodeInexact
                    (din.getLastLoggedLsn(), din.getLogType(), 0,
                     din.getDatabase());
            }
            final ChildReference dupCountRef = din.getDupCountLNRef();
            if (dupCountRef != null &&
                dupCountRef.getLsn() != DbLsn.NULL_LSN) {
                localTracker.countObsoleteNodeInexact
                    (dupCountRef.getLsn(), LogEntryType.LOG_DUPCOUNTLN, 0,
                     din.getDatabase());
            }
        } finally {
            din.releaseLatch();
        }
    }

    /**
     * Converts the given DBIN slot, leaving bin/index set to the inserted
     * BIN slot.
     *
     * Enter/leave with bin field latched, although bin field may change.
     *
     * If slot is inserted into current bin, leave bin field unchanged and
     * set index field to inserted slot.
     *
     * If slot is inserted into a different bin, set bin/index fields to
     * inserted slot.
     */
    private void convertDbinSlot(
        final DBIN dbin,
        final int dbinIndex,
        final byte[] binKey) {
        
        final byte[] newKey =
            DupKeyData.replaceData(binKey, dbin.getKey(dbinIndex));

        if (DEBUG) {
            System.out.println("DupConvert DBIN LN " +
                               Key.dumpString(newKey, 0));
        }

        /*
         * If the current BIN can hold the new slot, don't bother to do a
         * search to find it.
         */
        if (bin.needsSplitting() || !bin.isKeyInBounds(newKey)) {

            /* Compact keys after finishing with a BIN. */
            bin.compactMemory();

            /* Evict without latches, before moving to a new BIN. */
            bin.releaseLatch();
            envImpl.daemonEviction(false /*backgroundIO*/);

            /* Find a BIN for insertion, split if necessary. */
            bin = dbin.getDatabase().getTree().searchSplitsAllowed(
                newKey, CacheMode.UNCHANGED);
        }

        final int newIndex = bin.insertEntry1(
            null/*ln*/, newKey, null/*data*/, dbin.getLsn(dbinIndex),
            dbin.getState(dbinIndex), false);

        if ((newIndex & IN.INSERT_SUCCESS) == 0) {
            throw EnvironmentFailureException.unexpectedState
                ("Key not inserted: " + Key.dumpString(newKey, 0) +
                 " DB: " + dbin.getDatabase().getId());
        }

        index = newIndex & ~IN.INSERT_SUCCESS;

        /*
         * Evict LN from DBIN slot. Although we don't explicitly load DBIN LNs,
         * it may have been loaded by recovery.
         */
        dbin.detachNode(dbinIndex, false/*updateLsn*/, -1/*lsn*/);

        nConverted += 1;
    }

    /**
     * Changes all keys to "prefix keys" in the given IN.  Called after reading
     * an IN from disk via IN.postFetchInit.
     *
     * The conversion of IN keys is invoked from the IN class when an IN is
     * fetched, rather than invoked from the DupConvert class directly, for
     * performance and simplicity.  If it were invoked from the DupConvert
     * class, we would have to iterate over all INs in a separate initial pass.
     * This is both more time consuming, and more complex to implement properly
     * so that eviction is possible.  Instead, conversion occurs when an old
     * format IN is loaded.
     *
     * Enter/leave with 'in' unlatched.
     */
    public static void convertInKeys(final DatabaseImpl dbImpl, final IN in) {

        /* Nothing to convert for non-duplicates DB. */
        if (!dbImpl.getSortedDuplicates()) {
            return;
        }

        /* DIN/DBIN do not need conversion either. */
        if (in instanceof DIN || in instanceof DBIN) {
            return;
        }

        for (int i = 0; i < in.getNEntries(); i += 1) {
            byte[] oldKey = in.getKey(i);
            byte[] newKey =
                DupKeyData.makePrefixKey(oldKey, 0, oldKey.length);

            in.convertKey(i, newKey);
        }

        byte[] oldKey = in.getIdentifierKey();
        byte[] newKey = DupKeyData.makePrefixKey(oldKey, 0, oldKey.length);
        in.setIdentifierKey(newKey, true /*makeDirty*/);

        assert in.verifyMemorySize();
    }
}
