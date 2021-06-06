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

package com.sleepycat.je.evictor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.test.TTLTest;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Needs testing (not executed in coverage report):
 *   - enable checksums and run unit tests
 *   - loadBINIfLsnMatches, evictBINIfLsnMatch
 */
public class OffHeapCacheTest extends TestBase {

    private File envHome;
    private Environment env;
    private Database db;
    private OffHeapCache ohCache;
    private OffHeapAllocator allocator;

    public OffHeapCacheTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    @Override
    public void tearDown() {

        try {
            if (env != null) {
                env.close();
            }
        } finally {
            env = null;
            db = null;
            ohCache = null;
            allocator = null;
            TTL.setTimeTestHook(null);
        }

        TestUtils.removeLogFiles("TearDown", envHome, false);
    }

    private void open() {
        open(false);
    }

    private void open(final boolean transactional) {

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(transactional);
        envConfig.setOffHeapCacheSize(1024 * 1024);

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        env = new Environment(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(transactional);

        db = env.openDatabase(null, "foo", dbConfig);

        ohCache = DbInternal.getNonNullEnvImpl(env).getOffHeapCache();

        allocator = ohCache.getAllocator();
    }

    private void close() {
        db.close();
        env.close();
    }

    @Test
    public void testBINSerialization() throws Exception {

        open();

        final BIN bin = new BIN(
            DbInternal.getDbImpl(db),
            new byte[] { 1, 2, 3 },
            128, IN.BIN_LEVEL);

        /* Avoid assertions setting LN memIds. */
        bin.setOffHeapLruId(1);

        bin.latch();
        try {
            checkBINSerialization(bin);
            checkBINSerialization(bin, 0);
            checkBINSerialization(bin, 100);
            checkBINSerialization(bin, 0, 0);
            checkBINSerialization(bin, 100, 0);
            checkBINSerialization(bin, 0, 101);
            checkBINSerialization(bin, 100, 101);
            checkBINSerialization(bin, 0, 0, 0);
            checkBINSerialization(bin, 0, 0, 102);
            checkBINSerialization(bin, 0, 101, 0);
            checkBINSerialization(bin, 0, 101, 102);
            checkBINSerialization(bin, 100, 0, 0);
            checkBINSerialization(bin, 100, 0, 102);
            checkBINSerialization(bin, 100, 101, 0);
            checkBINSerialization(bin, 100, 101, 102);
            checkBINSerialization(bin, 0, 0, 0, 0);
            checkBINSerialization(bin, 0, 0, 0, 103);
            checkBINSerialization(bin, 0, 0, 102, 0);
            checkBINSerialization(bin, 0, 0, 102, 103);
            checkBINSerialization(bin, 100, 101, 0, 0);
            checkBINSerialization(bin, 100, 101, 0, 103);
            checkBINSerialization(bin, 100, 101, 102, 0);
            checkBINSerialization(bin, 100, 101, 102, 103);
        } finally {
            bin.releaseLatch();
        }

        close();
    }

    private void checkBINSerialization(BIN bin, int... memIds) {

        assertTrue(memIds.length >= bin.getNEntries());

        for (int i = 0; i < memIds.length; i += 1) {

            if (i >= bin.getNEntries()) {
                assertTrue(bin.insertEntry(null, new byte[] {(byte) i}, 0));
            }

            bin.setOffHeapLNId(i, memIds[i]);
        }

        final long memId = ohCache.serializeBIN(bin, bin.isBINDelta());
        final byte[] bytes = new byte[allocator.size(memId)];
        allocator.copy(memId, 0, bytes, 0, bytes.length);
        allocator.free(memId);

        final BIN bin2 = ohCache.materializeBIN(bin.getEnv(), bytes);
        bin2.setDatabase(DbInternal.getDbImpl(db));

        /* Avoid assertions setting LN memIds. */
        bin2.setOffHeapLruId(1);

        bin2.latch();
        try {
            assertEquals(bin.isBINDelta(), bin2.isBINDelta());
            assertEquals(bin.getNEntries(), bin2.getNEntries());

            for (int i = 0; i < bin.getNEntries(); i += 1) {
                assertEquals(bin.getOffHeapLNId(i), bin2.getOffHeapLNId(i));
            }

            /*
             * We don't bother to check all BIN fields, since writeToLog and
             * readFromLog are used for serialization and tested in many places.
             */
        } finally {
            bin2.releaseLatch();
        }
    }

    /**
     * Makes a call to each getter to make sure it doesn't throw an exception.
     */
    @Test
    public void testStatGetters() throws Exception {

        open();

        final EnvironmentStats stats = env.getStats(null);

        stats.getOffHeapAllocFailures();
        stats.getOffHeapAllocOverflows();
        stats.getOffHeapThreadUnavailable();
        stats.getOffHeapNodesTargeted();
        stats.getOffHeapCriticalNodesTargeted();
        stats.getOffHeapNodesEvicted();
        stats.getOffHeapDirtyNodesEvicted();
        stats.getOffHeapNodesStripped();
        stats.getOffHeapNodesMutated();
        stats.getOffHeapNodesSkipped();
        stats.getOffHeapLNsEvicted();
        stats.getOffHeapLNsLoaded();
        stats.getOffHeapLNsStored();
        stats.getOffHeapBINsLoaded();
        stats.getOffHeapBINsStored();
        stats.getOffHeapCachedLNs();
        stats.getOffHeapCachedBINs();
        stats.getOffHeapCachedBINDeltas();
        stats.getOffHeapTotalBytes();
        stats.getOffHeapTotalBlocks();
        stats.getOffHeapLRUSize();

        close();
    }

    /**
     * Verifies the phases of eviction of a BIN in main cache with off-heap
     * LNs.
     */
    @Test
    public void testMainBINEviction() {

        open();

        long bytes;

        /*
         * BIN starts with 10 off-heap entries.
         */
        final BIN bin = createMainCacheBIN();
        assertEquals(10, bin.getNEntries());

        /*
         * First eviction: 5 expired LNs are evicted;
         * expired slots are removed except for one dirty slot.
         */
        bytes = ohCache.testEvictMainBIN(bin);
        assertTrue(bytes > 0);
        assertEquals(6, bin.getNEntries());

        assertEquals(0, bin.getOffHeapLNId(0));

        for (int i = 1; i < 6; i += 1) {
            assertTrue(bin.getOffHeapLNId(i) != 0);
        }

        /*
         * Second eviction: remaining LNs are evicted;
         * the one expired dirty slot remains.
         */
        bytes = ohCache.testEvictMainBIN(bin);
        assertTrue(bytes > 0);
        assertEquals(6, bin.getNEntries());
        assertTrue(bin.getOffHeapLruId() < 0);

        for (int i = 0; i < 6; i += 1) {
            assertEquals(0, bin.getOffHeapLNId(i));
        }

        close();
    }

    /**
     * Verifies the phases of eviction of an off-heap BIN.
     */
    @Test
    public void testOffHeapBINEviction() {

        open();

        final IN parent = createOffHeapBIN();
        long bytes;
        BIN bin;

        /*
         * BIN starts with 10 off-heap entries.
         */
        bin = materializeBIN(parent);
        assertNotNull(bin);
        assertEquals(10, bin.getNEntries());

        /*
         * First eviction: 5 expired LNs are evicted;
         * expired slots are removed except for one dirty slot.
         */
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);
        bin = materializeBIN(parent);
        assertNotNull(bin);
        assertFalse(bin.isBINDelta(false));
        assertEquals(6, bin.getNEntries());

        assertEquals(0, bin.getOffHeapLNId(0));

        for (int i = 1; i < 6; i += 1) {
            assertTrue(bin.getOffHeapLNId(i) != 0);
        }

        /*
         * Second eviction: remaining LNs are evicted;
         * the one expired dirty slot remains.
         */
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);
        bin = materializeBIN(parent);
        assertNotNull(bin);
        assertFalse(bin.isBINDelta(false));
        assertEquals(6, bin.getNEntries());

        for (int i = 0; i < 6; i += 1) {
            assertEquals(0, bin.getOffHeapLNId(i));
        }

        /*
         * Third eviction: mutate to delta with a single slot.
         */
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);
        bin = materializeBIN(parent);
        assertNotNull(bin);
        assertTrue(bin.isBINDelta(false));
        assertEquals(1, bin.getNEntries());

        /*
         * Fourth eviction: move from pri1 to pri2 LRU, because its dirty.
         */
        assertFalse(parent.isOffHeapBINPri2(0));
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes == 0);
        bin = materializeBIN(parent);
        assertNotNull(bin);
        assertTrue(bin.isBINDelta(false));
        assertEquals(1, bin.getNEntries());
        assertTrue(parent.isOffHeapBINPri2(0));

        /*
         * Fifth eviction: evict the off-heap BIN entirely.
         */
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);
        bin = materializeBIN(parent);
        assertNull(bin);

        close();
    }

    /**
     * Tests a bug fix where we were freeing an off-heap LN twice, when it
     * expired but was still locked. The scenario is:
     *
     * 1. LN with an expiration time is locked.
     * 2. LN is moved off-heap.
     * 3. The LN's parent BIN is moved off-heap.
     * 4. The LN's expiration time passes.
     * 5. Off-heap evictor processes BIN. It frees the expired LN, but cannot
     *    compress the BIN slot, since the record is locked. The serialized
     *    BIN is mistakenly not updated, so it still has the reference to the
     *    LN that was freed. The BIN is re-serialized, but only if a slot was
     *    compressed, and this didn't happen.
     * 6. Off-heap evictor processes BIN again. This time it tries to free the
     *    LN that was previously freed, resulting in a JVM crash.
     *
     * The fix is to always re-serialize the BIN when an expired LN was freed,
     * even if its slot cannot be compressed due to a lock.
     */
    @Test
    public void testLockedExpiredLNInOffHeapBIN() {

        open(true /*transactional*/);

        /* Used a fixed time for expiring records. */
        TTLTest.setFixedTimeHook(System.currentTimeMillis());

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(0, key);
        final DatabaseEntry data = new DatabaseEntry(new byte[100]);

        final WriteOptions options = new WriteOptions().
            setCacheMode(CacheMode.EVICT_BIN).
            setTTL(1, TimeUnit.HOURS);

        final Transaction txn = env.beginTransaction(null, null);
        final Cursor cursor = db.openCursor(txn, null);

        final OperationResult result =
            cursor.put(key, data, Put.NO_OVERWRITE, options);

        assertNotNull(result);

        final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        final IN parent = bin.getParent();
        assertEquals(1, parent.getNEntries());

        cursor.close();
        assertNull(parent.getTarget(0));

        /* Make the record expire. */
        TTLTest.fixedSystemTime += TTL.MILLIS_PER_HOUR * 2;

        /*
         * First eviction will evict expired LN, but cannot compress the slot
         * because of the record lock.
         */
        long bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);

        /*
         * Second eviction, prior to the bug fix, would mistakenly evict the
         * same expired LN and crash. We release the lock here before evicting,
         * but the crash would have occurred without releasing it. By releasing
         * the lock, we can compress and free the slot without evicting the
         * entire BIN.
         */
        txn.commit();
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);

        /*
         * Third time: moves it to the priority 2 LRU.
         */
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertEquals(0, bytes);

        /*
         * Fourth time: evicts the entire BIN.
         */
        bytes = ohCache.testEvictOffHeapBIN(parent, 0);
        assertTrue(bytes > 0);

        close();
    }

    /**
     * Tests the following compression scenario, which caused a "double free".
     * This was fixed by always considering off-heap BINs stale when a main
     * cache version is present.
     *
     * 1. BIN is off-heap and contains an LN in an expired slot.
     * 2. BIN will be loaded into main.
     * 3. After materializing, but before calling OffHeapCache.postBINLoad,
     *    we compress the BIN, which removes the expired slot and frees the
     *    off-heap LN.
     * 4. Because postBINLoad had not been called, the BIN's IN_OFFHEAP_BIT was
     *    not set before compressing, and BIN.setOffHeapLNId(idx, 0) did not
     *    set the BIN's IN_OFFHEAP_STALE_BIT.
     * 5. Later, when the off-heap evictor tries to strip LNs, it attempts to
     *    free an already freed block.
     *
     * Note that another, similar compression scenario, would have caused a
     * lost deletion:
     *
     * 1. BIN is dirty and is both on-heap and off-heap.
     * 2. Compression removes a dirty slot and sets ProhibitNextDelta.
     * 3. Main BIN is evicted, but is not re-copied off-heap, because off-heap
     *    BIN is not stale.
     * 4. Checkpoint logs the off-heap BIN as a delta, losing the deletion.
     *
     * This was not observed, because (we think) it is difficult to create
     * condition 1. This is because of another bug, where when loading a dirty
     * off-heap BIN, the off-heap version was immediately made stale when
     * postBINLoad called BIN.setDirty. This is also resolved by the new
     * approach where always consider off-heap BINs stale when a main cache
     * version is present.
     */
    @Test
    public void testCompressDuringLoad() {

        open();

        /* Used a fixed time for expiring records. */
        TTLTest.setFixedTimeHook(System.currentTimeMillis());

        final DatabaseEntry key = new DatabaseEntry();
        IntegerBinding.intToEntry(0, key);
        final DatabaseEntry data = new DatabaseEntry(new byte[100]);

        final WriteOptions options = new WriteOptions().
            setCacheMode(CacheMode.EVICT_BIN).
            setTTL(1, TimeUnit.HOURS);

        final Cursor cursor = db.openCursor(null, null);

        final OperationResult result =
            cursor.put(key, data, Put.NO_OVERWRITE, options);

        assertNotNull(result);

        BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        final IN parent = bin.getParent();
        assertEquals(1, parent.getNEntries());

        env.sync();

        cursor.close();
        assertNull(parent.getTarget(0));

        /* Make the record expire. */
        TTLTest.fixedSystemTime += TTL.MILLIS_PER_HOUR * 2;

        parent.latchNoUpdateLRU();
        bin = (BIN) parent.loadIN(0, CacheMode.UNCHANGED);
        parent.releaseLatch();

        final Evictor evictor =
            DbInternal.getNonNullEnvImpl(env).getEvictor();

        bin.latchNoUpdateLRU();
        evictor.doTestEvict(bin, Evictor.EvictionSource.MANUAL);

        /* This caused a double-free, before the bug fix. */
        ohCache.testEvictOffHeapBIN(parent, 0);

        close();
    }

    /**
     * Tests a fix to a bug where the ProhibitNextDelta flag was not honored
     * when logging an off-heap BIN, and the BIN was re-materialized in order
     * to compress expired slots. [#24973]
     */
    @Test
    public void testProhibitNextDeltaBug() {

        open();

        final IN parent = createOffHeapBIN(true /*prohibitNextDelta*/);

        parent.latchNoUpdateLRU();

        final INLogEntry<BIN> entry =
            ohCache.createBINLogEntryForCheckpoint(parent, 0);

        /* This failed prior to the bug fix. */
        assertFalse(entry.isBINDelta());

        entry.getMainItem().releaseLatch();
        parent.releaseLatch();

        close();
    }

    private BIN materializeBIN(final IN parent) {

        parent.latchNoUpdateLRU();
        final byte[] bytes = ohCache.getBINBytes(parent, 0);
        parent.releaseLatch();

        if (bytes == null) {
            return null;
        }

        return ohCache.materializeBIN(
            DbInternal.getNonNullEnvImpl(env), bytes);
    }

    /**
     * Calls createBIN and then moves it off-heap.
     *
     * @return the parent, which will only have one slot containing the
     * off-heap BIN.
     */
    private IN createOffHeapBIN(final boolean prohibitNextDelta) {
        final IN parent = createBIN(CacheMode.EVICT_BIN, prohibitNextDelta);
        assertNull(parent.getTarget(0));
        assertTrue(parent.getOffHeapBINId(0) >= 0);
        return parent;
    }

    private IN createOffHeapBIN() {
        return createOffHeapBIN(false /*prohibitNextDelta*/);
    }

    /**
     * Calls createBIN and moves its LNs off-heap.
     */
    private BIN createMainCacheBIN() {
        final IN parent = createBIN(
            CacheMode.EVICT_LN, false /*prohibitNextDelta*/);
        final BIN bin = (BIN) parent.getTarget(0);
        assertNotNull(bin);
        assertTrue(bin.getOffHeapLruId() >= 0);
        assertTrue(parent.getOffHeapBINId(0) < 0);
        return bin;
    }

    /**
     * Creates a BIN with:
     *  - 10 LNs
     *  - the first 5 LNs are expired
     *  - all LNs are moved off-heap if cacheMode is EVICT_LN, or the entire
     *    BIN is moved off-heap if it is EVICT_BIN
     *  - only one slot (an expired slot) is dirty, and therefore the BIN can
     *    be mutated to a delta
     *
     * @return the parent, which will only have one slot.
     */
    private IN createBIN(final CacheMode cacheMode,
                         final boolean prohibitNextDelta) {

        /* Used a fixed time for expiring records. */
        TTLTest.setFixedTimeHook(System.currentTimeMillis());

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[100]);

        final WriteOptions options =
            new WriteOptions().setCacheMode(cacheMode);

        final Cursor cursor = db.openCursor(null, null);
        OperationResult result;
        BIN bin = null;

        for (int i = 0; i < 10; i += 1) {

            options.setTTL((i < 5) ? 1 : 0, TimeUnit.HOURS);
            IntegerBinding.intToEntry(i, key);

            result = cursor.put(key, data, Put.NO_OVERWRITE, options);
            assertNotNull(result);

            final BIN cursorBin = DbInternal.getCursorImpl(cursor).getBIN();

            if (bin == null) {
                bin = cursorBin;
            } else {
                assertSame(bin, cursorBin);
            }
        }

        /*
         * Checkpoint and dirty one record, so that a delta should be logged
         * next.
         */
        env.checkpoint(new CheckpointConfig().setForce(true));
        IntegerBinding.intToEntry(0, key);
        result = db.put(null, key, data, Put.OVERWRITE, options);
        assertNotNull(result);

        bin.latchNoUpdateLRU();
        assertTrue(bin.shouldLogDelta());
        bin.setProhibitNextDelta(prohibitNextDelta);
        bin.releaseLatch();

        cursor.close();

        /* Make the 5 records expire. */
        TTLTest.fixedSystemTime += TTL.MILLIS_PER_HOUR * 2;

        final IN parent = bin.getParent();
        assertEquals(1, parent.getNEntries());
        return parent;
    }
}
