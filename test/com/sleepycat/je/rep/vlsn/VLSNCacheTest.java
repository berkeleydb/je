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

package com.sleepycat.je.rep.vlsn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.INLongRep;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Tests the vlsnCache class.
 */
public class VLSNCacheTest extends TestBase {

    private final long NULL_VLSN = VLSN.NULL_VLSN_SEQUENCE;

    private ReplicatedEnvironment master;
    private final File envRoot;

    private boolean embeddedLNs = false;

    public VLSNCacheTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        if (master != null) {
            try {
                master.close();
            } catch (Exception e) {
                System.out.println("During tearDown: " + e);
            }
            master = null;
        }
    }

    /**
     * White box test that checks that all values allowed for VLSNs, and
     * special/boundary values, can be stored into and retrieved from the
     * cache.  Also checks that mutation occurs when the value exceeds the
     * threshold for each byte length.
     */
    @Test
    public void testCacheValues() {

        /* Entries per BIN, which is entries per cache. */
        final int N_ENTRIES = 5;

        /*
         * Open env and db only to create a BIN, although the BIN is not
         * attached to the Btree.
         */
        final ReplicationConfig repConfig = new ReplicationConfig();
        repConfig.setConfigParam
            (RepParams.PRESERVE_RECORD_VERSION.getName(), "true");

        final Database db = openEnv(repConfig);
        final BIN bin = new BIN(DbInternal.getDbImpl(db), new byte[1],
                                N_ENTRIES, 1);
        final long oldMemSize = bin.getInMemorySize();
        closeEnv(db);

        /* Max value plus one that can be stored for each byte length. */
        final long[] limitPerByteLength = {
            1L,
            1L << 8,
            1L << 16,
            1L << 24,
            1L << 32,
            1L << 40,
            1L << 48,
            1L << 56,
            Long.MAX_VALUE,
        };

        /*
         * The BIN always starts with an EMPTY_REP. Set minLength to 1 so we
         * can test the full range of version byte sizes.
         */
        INLongRep c = new INLongRep.EmptyRep(1, false);

        /* Check each block of values, one for each byte length. */
        for (int block = 1; block < limitPerByteLength.length; block += 1) {

            /* Never mutate when storing a null. */
            for (int entry = 0; entry < N_ENTRIES; entry += 1) {
                assertSame(c, c.set(entry, 0, bin));
                assertEquals(0, c.get(entry));
            }

            /* Define low and high (max plus one) limits of the block. */
            final long lowVal = limitPerByteLength[block - 1];
            final long highVal = limitPerByteLength[block];

            /*
             * Check several values at each of the block's value range.
             * Expect the cache to mutate when the first value is stored.
             */
            boolean mutated = false;
            for (long val :
                 new long[] { lowVal, lowVal + 1, lowVal + 2,
                              highVal - 2, highVal - 1 }) {

                /* Set and get the value in each slot. */
                for (int entry = 0; entry < N_ENTRIES; entry += 1) {
                    final String msg = "val=" + val +
                                       " entry=" + entry +
                                       " block=" + block;
                    assertTrue(msg, val != 0);
                    final INLongRep newCache = c.set(entry, val, bin);
                    if (mutated) {
                        assertSame(msg, c, newCache);
                    } else {
                        assertNotSame(msg, c, newCache);
                        c = newCache;
                        mutated = true;
                    }
                    assertEquals(msg, val, c.get(entry));
                }

                /* Get values again to check for overwriting boundaries. */
                for (int entry = 0; entry < N_ENTRIES; entry += 1) {
                    assertEquals(val, c.get(entry));
                }
            }
        }

        /*
         * BIN mem size should have increased by current cache size.  We cannot
         * check bin.computeMemorySize here because the BIN was not actually
         * used to cache VLSNs, so it doesn't contain the mutated cache object.
         */
        assertEquals(oldMemSize + c.getMemorySize(), bin.getInMemorySize());
    }

    /**
     * After eviction of LNs, the version is available although the vlsnCache
     * without having to fetch the LNs.
     */
    @Test
    public void testEvictLNs() {
        final Database db = openEnv();

        /* After insertion, VLSN is available without fetching. */
        insert(db, 1);

        final long v1 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs /*expectEmptyCache*/);
        assertTrue(v1 != NULL_VLSN);

        /* After LN eviction, VLSN is available via the vlsnCache. */
        evict(db, 1, CacheMode.EVICT_LN);

        final long v2 = getVLSN(
            db, 1, false /*allowFetch*/, false /*expectEmptyCache*/);
        assertTrue(v2 != NULL_VLSN);
        assertEquals(v1, v2);

        /* After update, new VLSN is available without fetching. */
        update(db, 1);
        final long v3 = getVLSN(
            db, 1, false /*allowFetch*/, false /*expectEmptyCache*/);
        assertTrue(v3 != NULL_VLSN);
        assertTrue(v3 > v2);

        /*
         * When an update is performed and the vlsnCache has the old value, the
         * vlsnCache is not updated.  The vlsnCache is out-of-date but this is
         * harmless because the VLSN in the LN takes precedence over the
         * vlsnCache.
         */
        final BIN bin = (BIN) DbInternal.getDbImpl(db).
                                         getTree().
                                         getFirstNode(CacheMode.DEFAULT);
        try {
            if (embeddedLNs) {
                assertEquals(v3, bin.getVLSNCache().get(0));
            } else {
                assertEquals(v2, bin.getVLSNCache().get(0));
            }
        } finally {
            bin.releaseLatch();
        }

        /* After LN eviction, VLSN is available via the vlsnCache. */
        evict(db, 1, CacheMode.EVICT_LN);
        final long v4 = getVLSN(
            db, 1, false /*allowFetch*/, false /*expectEmptyCache*/);
        assertTrue(v4 != NULL_VLSN);
        assertEquals(v3, v4);

        /* After fetch, new VLSN is available without fetching. */
        fetch(db, 1);
        final long v5 = getVLSN(
            db, 1, false /*allowFetch*/, false /*expectEmptyCache*/);
        assertEquals(v5, v4);

        /* After LN eviction, VLSN is available via the vlsnCache. */
        evict(db, 1, CacheMode.EVICT_LN);
        final long v6 = getVLSN(
            db, 1, false /*allowFetch*/, false /*expectEmptyCache*/);
        assertTrue(v6 != NULL_VLSN);
        assertEquals(v6, v5);

        closeEnv(db);
    }

    /**
     * After eviction of BINs, the version is only available by fetching the
     * LNs.
     */
    @Test
    public void testEvictBINs() {
        final Database db = openEnv();

        /* After insertion, VLSN is available without fetching. */
        insert(db, 1);

        final long v1 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertTrue(v1 != NULL_VLSN);

        /* After BIN eviction, VLSN is only available by fetching. */
        evict(db, 1, CacheMode.EVICT_BIN);

        final long v2 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        if (embeddedLNs) {
            assertEquals(v1, v2);
        } else {
            assertEquals(v2, NULL_VLSN);
        }

        final long v3 = getVLSN(
            db, 1, true /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertTrue(v3 != NULL_VLSN);
        assertEquals(v3, v1);

        /* After update, new VLSN is available without fetching. */
        update(db, 1);

        final long v4 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertTrue(v4 != NULL_VLSN);
        assertTrue(v4 > v3);

        /* After BIN eviction, VLSN is only available by fetching. */
        evict(db, 1, CacheMode.EVICT_BIN);

        final long v5 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        if (embeddedLNs) {
            assertEquals(v4, v5);
        } else {
            assertEquals(v5, NULL_VLSN);
        }

        final long v6 = getVLSN(
            db, 1, true /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertTrue(v6 != NULL_VLSN);
        assertEquals(v6, v4);

        closeEnv(db);
    }

    /**
     * With no eviction, LNs are resident so version is available without any
     * fetching even though the vlsnCache is empty.
     */
    @Test
    public void testNoEviction() {
        final Database db = openEnv();

        /* After insertion, VLSN is available without fetching. */
        insert(db, 1);
        final long v1 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertTrue(v1 != NULL_VLSN);

        /* After update, new VLSN is available without fetching. */
        update(db, 1);
        final long v2 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertTrue(v2 != NULL_VLSN);
        assertTrue(v2 > v1);

        /* After fetch, new VLSN is available without fetching. */
        fetch(db, 1);
        final long v3 = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs/*expectEmptyCache*/);
        assertEquals(v3, v2);

        closeEnv(db);
    }

    /**
     * Inserts the given record, should not fetch.
     */
    private void insert(Database db, int keyNum) {

        final EnvironmentStats stats1 = master.getStats(null);

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(keyNum, key);
        final DatabaseEntry data = new DatabaseEntry(new byte[1]);

        final OperationStatus status = db.putNoOverwrite(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);

        final EnvironmentStats stats2 = master.getStats(null);

        assertEquals(stats1.getNLNsFetchMiss(),
                     stats2.getNLNsFetchMiss());
        assertEquals(stats1.getNBINsFetchMiss(),
                     stats2.getNBINsFetchMiss());
    }

    /**
     * Updates the given record, should not fetch.
     */
    private void update(Database db, int keyNum) {

        final EnvironmentStats stats1 = master.getStats(null);

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(keyNum, key);
        final DatabaseEntry data = new DatabaseEntry(new byte[1]);

        final OperationStatus status = db.put(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);

        final EnvironmentStats stats2 = master.getStats(null);

        assertEquals(stats1.getNLNsFetchMiss(),
                     stats2.getNLNsFetchMiss());
        assertEquals(stats1.getNBINsFetchMiss(),
                     stats2.getNBINsFetchMiss());
    }

    /**
     * Fetches given record.
     */
    private void fetch(Database db, int keyNum) {

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(keyNum, key);
        final DatabaseEntry data = new DatabaseEntry();

        final OperationStatus status = db.get(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
    }

    /**
     * Evict the given record according to the given CacheMode.
     */
    private void evict(Database db, int keyNum, CacheMode cacheMode) {

        assertNotNull(db);
        assertTrue(cacheMode == CacheMode.EVICT_LN ||
                   cacheMode == CacheMode.EVICT_BIN);

        if (cacheMode == CacheMode.EVICT_BIN) {
            /* EVICT_BIN will not evict a dirty BIN. */
            master.sync();
        }

        final EnvironmentStats stats1 = master.getStats(null);

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(keyNum, key);
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(0, 0, true);

        final Cursor cursor = db.openCursor(null, null);
        final BIN bin;
        final int binIndex;
        try {
            cursor.setCacheMode(cacheMode);
            final OperationStatus status =
                cursor.getSearchKey(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);
            bin = DbInternal.getCursorImpl(cursor).getBIN();
            binIndex = DbInternal.getCursorImpl(cursor).getIndex();
        } finally {
            cursor.close();
        }

        if (cacheMode == CacheMode.EVICT_LN) {
            assertNull(bin.getTarget(binIndex));
            assertFalse(bin.getVLSNCache() instanceof INLongRep.EmptyRep);
        }

        final EnvironmentStats stats2 = master.getStats(null);
        assertEquals(stats1.getNLNsFetchMiss(),
                     stats2.getNLNsFetchMiss());
        assertEquals(stats1.getNBINsFetchMiss(),
                     stats2.getNBINsFetchMiss());

        if (cacheMode == CacheMode.EVICT_BIN) {
            assertEquals(1, stats2.getNNodesEvicted() -
                            stats1.getNNodesEvicted());
            assertTrue(stats1.getNBytesEvictedCacheMode() <
                       stats2.getNBytesEvictedCacheMode());
        } else {
            assertEquals(stats1.getNBytesEvictedCacheMode(),
                         stats2.getNBytesEvictedCacheMode());
        }
    }

    /**
     * Deletes the given record.
     */
    private void delete(Database db, int keyNum) {

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(keyNum, key);

        final OperationStatus status = db.delete(null, key);
        assertSame(OperationStatus.SUCCESS, status);

        master.compress();
    }

    /**
     * Move cursor to first record and return its VLSN.
     *
     * For a primary DB, we do not need to fetch the data to get the VLSN, but
     * for a secondary DB we do (because a Btree lookup in the primary DB is
     * needed to get its VLSN).  In the future we could add a way to do a
     * key-only secondary read that does the primary DB lookup and populates
     * the VLSN, if needed.
     */
    private long getVLSN(Database db,
                         int keyNum,
                         boolean allowFetch,
                         boolean expectEmptyCache) {

        assertNotNull(db);

        final EnvironmentStats stats1 = master.getStats(null);

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(keyNum, key);
        final DatabaseEntry data = new DatabaseEntry();
        if (!(db instanceof SecondaryDatabase)) {
            data.setPartial(0, 0, true);
        }

        final Cursor cursor = db.openCursor(null, null);
        final long vlsn;
        final BIN bin;
        try {
            final OperationStatus status =
                cursor.getSearchKey(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);

            bin = DbInternal.getCursorImpl(cursor).getBIN();

            final EnvironmentStats stats2 = master.getStats(null);
            assertEquals(stats1.getNLNsFetchMiss(),
                         stats2.getNLNsFetchMiss());

            vlsn = DbInternal.getCursorImpl(cursor).
                              getCurrentVersion(allowFetch).
                              getVLSN();

            assertTrue(vlsn != 0);

            if (allowFetch) {
                assertTrue(vlsn != NULL_VLSN);
            }

            if (!allowFetch) {
                final EnvironmentStats stats3 = master.getStats(null);
                assertEquals(stats2.getNLNsFetchMiss(),
                             stats3.getNLNsFetchMiss());
            }
        } finally {
            cursor.close();
        }

        assertEquals(
            expectEmptyCache,
            bin.getVLSNCache() instanceof INLongRep.EmptyRep);

        return vlsn;
    }

    /**
     * In a ReplicatedEnvironment it is possible to have non-txnl databases,
     * although currently these are only allowed for internal databases.  The
     * LNs for such databases will not have VLSNs.  Do a basic test of eviction
     * here to ensure that version caching for non-txnl databases doesn't cause
     * problems.
     *
     * At one point the eviction in a non-txnl DB caused an assertion to fire
     * in vlsnCache.DefaultRep.set because the VersionedLN was returning a
     * zero VLSN sequence.  That bug was fixed and VersionedLN.getVLSNSequence
     * now returns -1 (null) for the VLSN sequence, when the LN does not have a
     * VLSN and therefore VersionedLN.setVLSNSequence is never called.
     */
    @Test
    public void testNonTxnlEviction() {

        final Database db = openEnv();

        /*
         * Reach into internals and get the VLSNIndex database, which happens
         * to be non-transactional, and its first BIN.  We flush the VLSNIndex
         * database to create a BIN.
         */
        final DatabaseImpl nonTxnlDbImpl =
            RepInternal.getNonNullRepImpl(master).
                getVLSNIndex().getDatabaseImpl();
        RepInternal.getNonNullRepImpl(master).getVLSNIndex().
             flushToDatabase(Durability.COMMIT_NO_SYNC);
        final BIN bin =
            (BIN) nonTxnlDbImpl.getTree().getFirstNode(CacheMode.DEFAULT);

        /* Evict LNs and ensure that the EMPTY_REP is still used. */
        try {
            bin.evictLNs();
        } finally {
            bin.releaseLatch();
        }
        assertTrue(bin.getVLSNCache() instanceof INLongRep.EmptyRep);

        closeEnv(db);
    }

    /**
     * Tests that vlsnCache is adjusted correctly after movement of a slot due
     * to an insertion or deletion.
     */
    @Test
    public void testInsertAndDelete() {

        final Database db = openEnv();
        final List<Long> vlsnList = new ArrayList<Long>();

        /* Insert keys 1, 3, 5. */
        for (int i = 1; i <= 5; i += 2) {
            vlsnList.add(insertEvictAndGetVLSN(db, i));
            checkVlsns(db, vlsnList);
        }

        /* Insert keys 0, 2, 4, 6. */
        for (int i = 0; i <= 6; i += 2) {
            vlsnList.add(i, insertEvictAndGetVLSN(db, i));
            checkVlsns(db, vlsnList);
        }

        /* Delete keys 1, 3, 5. */
        for (int i = 5; i >= 1; i -= 2) {
            delete(db, i);
            vlsnList.remove(i);
            checkVlsns(db, vlsnList);
        }

        /* Delete keys 0, 2, 4, 6. */
        for (int i = 6; i >= 0; i -= 2) {
            delete(db, i);
            vlsnList.remove(vlsnList.size() - 1);
            checkVlsns(db, vlsnList);
        }

        closeEnv(db);
    }

    private long insertEvictAndGetVLSN(Database db, int keyNum) {
        insert(db, keyNum);
        evict(db, keyNum, CacheMode.EVICT_LN);
        final long vlsn = getVLSN
            (db, keyNum, false /*allowFetch*/, false /*expectEmptyCache*/);
        assertTrue(vlsn != NULL_VLSN);
        return vlsn;
    }

    /**
     * Checks that the given VLSNs match the VLSNs in the Btree.
     */
    private void checkVlsns(Database db, List<Long> vlsnList) {
        int listIndex = 0;
        final Cursor cursor = db.openCursor(null, null);
        try {
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            data.setPartial(0, 0, true);
            OperationStatus status = cursor.getFirst(key, data, null);
            while (status == OperationStatus.SUCCESS) {

                /* Check VLSN via Cursor. */
                Long vlsn =
                    DbInternal.getCursorImpl(cursor).
                               getCurrentVersion(false /*allowFetch*/).
                               getVLSN();
                assertEquals(vlsnList.get(listIndex), vlsn);

                /* Check VLSN via vlsnCache. */
                final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
                final int binIndex =
                    DbInternal.getCursorImpl(cursor).getIndex();
                vlsn = bin.getVLSNCache().get(binIndex);
                assertEquals(vlsnList.get(listIndex), vlsn);

                /* Check VLSNs in BINCache that are out of bounds. */
                for (int i = bin.getNEntries();
                     i < bin.getMaxEntries();
                     i += 1) {
                    assertEquals(0, bin.getVLSNCache().get(i));
                }

                listIndex += 1;
                status = cursor.getNext(key, data, null);
            }
        } finally {
            cursor.close();
        }
        assertEquals(vlsnList.size(), listIndex);
    }

    /**
     * Tests that vlsnCache is adjusted correctly after movement of slots due
     * to a split.
     */
    @Test
    public void testSplit() {

        final Database db = openEnv();
        final List<Long> vlsnList = new ArrayList<Long>();

        for (int i = 0; i < 200; i += 1) {
            vlsnList.add(insertEvictAndGetVLSN(db, i));
        }
        checkVlsns(db, vlsnList);

        closeEnv(db);
    }

    /**
     * Checks that the version returned by a secondary cursor is actually the
     * version of the associated primary record.
     */
    @Test
    public void testSecondaryVersion() {
        final Database db = openEnv();

        final SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setTransactional(true);
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);

        /* Secondary key has the same value as the primary key. */
        secConfig.setKeyCreator(new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData(key.getData());
                return true;
            }
        });

        final SecondaryDatabase secDb =
            master.openSecondaryDatabase(null, "TEST_SEC", db, secConfig);

        insert(db, 1);

        final long priVlsn = getVLSN(
            db, 1, false /*allowFetch*/, !embeddedLNs /*expectEmptyCache*/);

        final long secVlsn = getVLSN(
            secDb, 1, false /*allowFetch*/, true /*expectEmptyCache*/);

        assertTrue(priVlsn != NULL_VLSN);
        assertEquals(priVlsn, secVlsn);

        secDb.close();
        closeEnv(db);
    }

    private Database openEnv() {
        final ReplicationConfig repConfig = new ReplicationConfig();

        repConfig.setConfigParam
            (RepParams.PRESERVE_RECORD_VERSION.getName(), "true");

        return openEnv(repConfig);
    }

    private Database openEnv(ReplicationConfig repConfig) {

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setAllowCreate(true);

        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        final RepEnvInfo masterInfo = RepTestUtils.setupEnvInfo
            (envRoot, envConfig, repConfig, null);

        master = RepTestUtils.joinGroup(masterInfo);

        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(master);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() >= 4);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        return master.openDatabase(null, "TEST", dbConfig);
    }

    private void closeEnv(Database db) {
        TestUtils.validateNodeMemUsage(DbInternal.getNonNullEnvImpl(master),
                                       true /*assertOnError*/);
        db.close();
        master.close();
        master = null;
    }
}
