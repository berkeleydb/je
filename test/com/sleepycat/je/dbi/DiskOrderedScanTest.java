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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Assume;
import org.junit.Test;

public class DiskOrderedScanTest extends TestBase {

    class DOSTestHook implements TestHook<Integer> {
        
        int counter = 0;

        @Override
        public void hookSetup() {
            counter = 0;
        }

        @Override
        public void doIOHook() {
        }

        @Override
        public void doHook() {
            ++counter;
        }
        
        @Override
        public void doHook(Integer obj) {
        }
        
        @Override
        public Integer getHookValue() {
            return counter;
        }
    };

    class EvictionHook implements TestHook<Integer> {
        
        DiskOrderedScanner dos;

        EvictionHook(DiskOrderedScanner dos) {
            this.dos = dos;
        }

        @Override
        public void hookSetup() {
        }

        @Override
        public void doIOHook() {
        }

        @Override
        public void doHook() {
            dos.evictBinRefs();
        }
        
        @Override
        public void doHook(Integer obj) {
        }
        
        @Override
        public Integer getHookValue() {
            return 0;
        }
    };
 
    private static final int N_RECS = 10000;
    private static final int ONE_MB = 1 << 20;

    private final File envHome;
    private Environment env;
    private EnvironmentImpl envImpl;

    boolean embeddedLNs;

    private Database[] dbs;

    private int numDBs = 5;

    public DiskOrderedScanTest() {
        envHome = SharedTestUtils.getTestDir();

        dbs = new Database[numDBs];
    }

    @Test
    public void testScanArgChecks()
        throws Throwable {

        System.out.println("Running test testScanArgChecks");

        open(false, CacheMode.DEFAULT, 0);

        writeData(1/*nDBs*/, false, N_RECS);

        ForwardCursor dos = dbs[0].openCursor(new DiskOrderedCursorConfig());

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* lockMode must be null, DEFAULT or READ_UNCOMMITTED. */
        try {
            dos.getNext(key, data, LockMode.READ_COMMITTED);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException IAE) {
            // expected
        }

        dos.close();
        try {
            dos.close();
        } catch (IllegalStateException ISE) {
            fail("unexpected IllegalStateException");
        }
        close();
    }

    @Test
    public void testScanPermutations()
        throws Throwable {

        System.out.println("Running test testScanPermutations");

        for (final int nDBs : new int[] { numDBs, 1 }) {

            for (final boolean dups : new boolean[] { false, true }) {

                for (final int nRecs : new int[] { 0, N_RECS }) {

                    for (final CacheMode cacheMode :
                         new CacheMode[] { CacheMode.DEFAULT,
                                           CacheMode.EVICT_LN,
                                           CacheMode.EVICT_BIN }) {

                        for (int i = 0; i < 3; i += 1) {
                            final boolean keysOnly;
                            final boolean countOnly;
                            switch (i) {
                            case 0:
                                keysOnly = false;
                                countOnly = false;
                                break;
                            case 1:
                                keysOnly = true;
                                countOnly = false;
                                break;
                            case 2:
                                keysOnly = true;
                                countOnly = true;
                                break;
                            default:
                                throw new IllegalStateException();
                            }

                            for (final long memoryLimit :
                                 new long[] { Long.MAX_VALUE, ONE_MB}) {

                                for (final long lsnBatchSize :
                                     new long[] { Long.MAX_VALUE, 100 }) {

                                    TestUtils.removeFiles(
                                        "Setup", envHome,
                                        FileManager.JE_SUFFIX);

                                    try {
                                        doScan(nDBs, dups, nRecs, cacheMode,
                                            keysOnly, countOnly,
                                            memoryLimit, lsnBatchSize);
                                    } catch (AssertionError |
                                             RuntimeException e) {
                                        /* Wrap with context info. */
                                        throw new RuntimeException(
                                            "scan failed with" +
                                            " dups=" + dups +
                                            " nRecs=" + nRecs +
                                            " cacheMode=" + cacheMode +
                                            " keysOnly=" + keysOnly +
                                            " memoryLimit=" + memoryLimit +
                                            " lsnBatchSize=" + lsnBatchSize,
                                            e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks that a 3 level (or larger) Btree can be scanned.
     */
    @Test
    public void testLargeScan()
        throws Throwable {

        System.out.println("Running test testLargeScan");

        doScan(1/*nDBs*/, false /*dups*/, 5 * 1000 * 1000, CacheMode.DEFAULT,
            false /*keysOnly*/, false /*countOnly*/, 10L * ONE_MB,
            Long.MAX_VALUE);
    }

    @Test
    public void testLowMemoryLargeCount()
        throws Throwable {

        System.out.println("Running test testLowMemoryLargeCount");

        doScan(1/*nDBs*/, false /*dups*/, 100 * 1000, CacheMode.EVICT_BIN,
            true /*keysOnly*/, true /*countOnly*/, ONE_MB, 50);
    }

    private void doScan(
        final int nDBs,
        final boolean dups,
        final int nRecs,
        final CacheMode cacheMode,
        final boolean keysOnly,
        final boolean countOnly,
        final long memoryLimit,
        final long lsnBatchSize)
        throws Throwable {

        open(dups, cacheMode, 0);

        writeData(nDBs, dups, nRecs);

        DiskOrderedCursorConfig dosConfig = new DiskOrderedCursorConfig();
        dosConfig.setKeysOnly(keysOnly);
        dosConfig.setCountOnly(countOnly);
        dosConfig.setInternalMemoryLimit(memoryLimit);
        dosConfig.setLSNBatchSize(lsnBatchSize);

        DiskOrderedCursor dos;

        if (nDBs == 1) {
            dos = dbs[0].openCursor(dosConfig);
        } else {
            dos = env.openDiskOrderedCursor(dbs, dosConfig);
        }

        int cnt = 0;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        int expectedCnt = nDBs * (dups ? (nRecs * 2) : nRecs);

        BitSet seenKeys = new BitSet(expectedCnt);

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            int k1;
            if (countOnly) {
                k1 = 0;
            } else {
                k1 = entryToInt(key);
            }

            int d1;
            if (keysOnly) {
                assertNull(data.getData());
                d1 = 0;
            } else {
                d1 = entryToInt(data);
            }

            if (dups) {
                if (!keysOnly) {
                    boolean v1 = (k1 == (d1 * -1));
                    boolean v2 = (d1 == (-1 * (k1 + nRecs + nRecs)));
                    assertTrue(v1 ^ v2);
                }
            } else {
                if (!keysOnly) {
                    assertEquals(k1, (d1 * -1));
                }
            }

            if (!countOnly) {
                seenKeys.set(k1);
            }
            cnt++;
        }

        assertEquals(cnt, expectedCnt);

        if (!countOnly) {
            assertEquals(seenKeys.cardinality(), nRecs * nDBs);
        }

        /* [#21282] getNext should return NOTFOUND if called again. */
        assertEquals(dos.getNext(key, data, null), OperationStatus.NOTFOUND);
        dos.close();
        close();

        /*
        System.out.println("iters " +
                           DbInternal.getDiskOrderedCursorImpl(dos).
                                      getNScannerIterations() + ' ' +
                           getName());
        */
    }

    @Test
    public void testInterruptedDiskOrderedScan()
        throws Throwable {

        System.out.println("Running test testInterruptedDiskOrderedScan");

        open(false, CacheMode.DEFAULT, 0);

        writeData(1, false, N_RECS);

        ForwardCursor dos = dbs[0].openCursor(new DiskOrderedCursorConfig());

        int cnt = 0;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        assertTrue(dos.getNext(key, data, null) == OperationStatus.SUCCESS);

        assertEquals(dos.getDatabase(), dbs[0]);

        int k1 = entryToInt(key);
        int d1 = entryToInt(data);
        assertTrue(k1 == (d1 * -1));

        DatabaseEntry key2 = new DatabaseEntry();
        DatabaseEntry data2 = new DatabaseEntry();

        assertTrue(dos.getCurrent(key2, data2, null) ==
                   OperationStatus.SUCCESS);

        int k2 = entryToInt(key2);
        int d2 = entryToInt(data2);
        assertTrue(k1 == k2 && d1 == d2);

        dos.close();

        try {
            dos.getCurrent(key2, data2, null);
            fail("expected IllegalStateException from getCurrent");
        } catch (IllegalStateException ISE) {
            // expected
        }

        try {
            dos.getNext(key2, data2, null);
            fail("expected IllegalStateException from getNext");
        } catch (IllegalStateException ISE) {
            // expected
        }

        close();
    }

    /*
     * Test that a delete of the record that the DiskOrderedCursor is pointing
     * to doesn't affect the DOS.
     */
    @Test
    public void testDeleteOneDuringScan()
        throws Throwable {

        System.out.println("Running test testDeleteOneDuringScan");

        open(false, CacheMode.DEFAULT, 0);

        writeData(1, false, N_RECS);

        ForwardCursor dos = dbs[0].openCursor(new DiskOrderedCursorConfig());
        Cursor cursor = dbs[0].openCursor(null, null);
        int cnt = 0;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key2 = new DatabaseEntry();
        DatabaseEntry data2 = new DatabaseEntry();

        assertTrue(dos.getNext(key, data, null) == OperationStatus.SUCCESS);

        int k1 = entryToInt(key);
        int d1 = entryToInt(data);

        assertTrue(k1 == (d1 * -1));

        assertTrue(dos.getCurrent(key2, data2, null) ==
            OperationStatus.SUCCESS);

        int k2 = entryToInt(key2);
        int d2 = entryToInt(data2);
        assertTrue(k1 == k2 && d1 == d2);

        assertTrue(cursor.getSearchKey(key, data, null) ==
                   OperationStatus.SUCCESS);

        cursor.delete();
        assertTrue(dos.getCurrent(key2, data2, null) ==
                   OperationStatus.SUCCESS);

        k2 = entryToInt(key2);
        d2 = entryToInt(data2);

        dos.close();
        cursor.close();
        close();
    }

    /**
     * Checks that a consumer thread performing deletions does not cause a
     * deadlock.  This failed prior to the use of DiskOrderedScanner. [#21667]
     */
    @Test
    public void testDeleteAllDuringScan()
        throws Throwable {

        System.out.println("Running test testDeleteAllDuringScan");

        open(false, CacheMode.DEFAULT, 0);

        writeData(1, false, N_RECS);

        DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
        config.setQueueSize(10).setLSNBatchSize(10);

        DiskOrderedCursor dos = dbs[0].openCursor(config);
        DiskOrderedCursorImpl dosImpl =
            DbInternal.getDiskOrderedCursorImpl(dos);

        Cursor cursor = dbs[0].openCursor(null, null);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > 0) { }

        for (int cnt = 0; cnt < N_RECS; cnt += 1) {

            assertSame(OperationStatus.SUCCESS, dos.getNext(key, data, null));

            int k1 = entryToInt(key);
            int d1 = entryToInt(data);
            assertEquals(k1, d1 * -1);

            assertSame(OperationStatus.SUCCESS,
                       cursor.getSearchKey(key, data, LockMode.RMW));

            assertEquals(k1, entryToInt(key));
            assertEquals(d1, entryToInt(data));
            assertSame(OperationStatus.SUCCESS, cursor.delete());
        }

        assertSame(OperationStatus.NOTFOUND, cursor.getFirst(key, data, null));

        dos.close();
        cursor.close();
        close();
    }

    @Test
    public void testBlockedProducerKeysOnly() throws Throwable {
        System.out.println("Running test testBlockedProducerKeysOnly");
        testBlockedProducer(true, 10, 128 * 2);
    }

    @Test
    public void testBlockedProducerKeysAndData() throws Throwable {
        System.out.println("Running test testBlockedProducerKeysAndData");
        testBlockedProducer(false, 100, 10);
    }

    public void testBlockedProducer(
        boolean keysonly,
        int lsnBatchSize,
        int queueSize) throws Throwable {

        /* Cache size sensitivity makes Zing support very difficult. */
        Assume.assumeTrue(!JVMSystemUtils.ZING_JVM);

        /*
         * Use a small cache so that not all the full bins fit in it.
         *
         * This test is sensitive to cache sizes and isn't important to run
         * with an off-heap cache.
         */
        open(false, CacheMode.DEFAULT, ONE_MB/5, false /*allowOffHeapCache*/);

        /* Load the initial set of 10,000 records */
        writeData(1, false, N_RECS);

        DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
        config.setQueueSize(queueSize);
        config.setLSNBatchSize(lsnBatchSize);
        config.setKeysOnly(keysonly);
        //config.setDebug(true);

        DiskOrderedCursor dos = dbs[0].openCursor(config);

        DiskOrderedCursorImpl dosImpl =
            DbInternal.getDiskOrderedCursorImpl(dos);

        DOSTestHook hook = new DOSTestHook();
        dosImpl.getScanner().setTestHook1(hook);

        Cursor cursor = dbs[0].openCursor(null, null);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        int minFreeSlots = (keysonly ? 127 : 0);

        /*
         * Test 1
         */

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        int freeSlots = dosImpl.freeQueueSlots();
        long numLsns = dosImpl.getNumLsns();

        //System.out.println(
        //    "freeSlots = " + freeSlots + " numLsns = " + numLsns);

        /* Delete all the records */
        while (cursor.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor.delete());
        }

        assertSame(OperationStatus.NOTFOUND, cursor.getFirst(key, data, null));

        cursor.close();
        env.compress();

        /*
         * The dos cursor should return the records that were already in
         * the queue, plus the records from any lsns accumulated before
         * the records were deleted. These lsns correspond to bins not
         * found in the cache during phase 1.
         */
        int cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        if (embeddedLNs) {
            assertEquals(queueSize - freeSlots + numLsns * 127, cnt);
            assertEquals(2, hook.getHookValue().intValue());
        } else {
            assertEquals(0, hook.getHookValue().intValue());
            assertEquals((keysonly ? 1461: 191), cnt);
        }

        dos.close();

        /*
         * Test 2
         */

        //System.out.println("TEST 2 \n");

        /* Relaod the records */
        writeData(1, false, N_RECS);

        cursor = dbs[0].openCursor(null, null);

        dos = dbs[0].openCursor(config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        dosImpl.getScanner().setTestHook1(hook);

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        freeSlots = dosImpl.freeQueueSlots();

        /* Delete all the records except from the last 300 ones */
        cnt = 0;

        while (cursor.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor.delete());

            ++cnt;
            if (cnt >= (N_RECS - 300)) {
                break;
            }
        }

        cursor.close();
        env.compress();

        /*
         * The dos cursor should return the records that were already in
         * the queue plus the last 300 records.
         */
        cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        if (embeddedLNs) {
            assertEquals(keysonly ? 618 : (queueSize - freeSlots + 300), cnt);
            assertEquals(keysonly ? 4 : 2, hook.getHookValue().intValue());
        } else {
            assertEquals(0, hook.getHookValue().intValue());
            assertEquals((keysonly ? 1461 : 191) + 300, cnt);
        }

        dos.close();

        /*
         * Test 3
         */

        //System.out.println("TEST 3 \n");

        dos = dbs[0].openCursor(config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        dosImpl.getScanner().setTestHook1(hook);

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        freeSlots = dosImpl.freeQueueSlots();

        /* Relaod 1000 records, starting with record 1000 */
        writeData(dbs[0], false, 1000, 1000);

        /*
         * The dos cursor should return the last 300 records.
         */
        cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        assertEquals(300, cnt);

        if (embeddedLNs) {
            assertEquals(keysonly ? 4 : 3, hook.getHookValue().intValue());
        }

        dos.close();

        /*
         * Test 4
         */

        //System.out.println("TEST 4 \n");

        dos = dbs[0].openCursor(config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        dosImpl.getScanner().setTestHook1(hook);

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        freeSlots = dosImpl.freeQueueSlots();

        /* Relaod the first 1000 records */
        writeData(dbs[0], false, 1000, 0);

        /* Load 20000 new records */
        writeData(dbs[0], false, 20000, N_RECS);

        cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        assertEquals(21300, cnt);

        if (embeddedLNs) {
            assertEquals(5, hook.getHookValue().intValue());
        }

        dos.close();

        /*
         * Test 5
         */

        //System.out.println("TEST 5 \n");

        dos = dbs[0].openCursor(config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        //dosImpl.getScanner().debug = true;

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        dos.close();

        synchronized (this) {
            wait(2000);
        }

        assertTrue(dosImpl.isProcessorClosed());

        close();
    }

    @Test
    public void testBlockedProducerMultiDBInternal1() throws Throwable {
        System.out.println("Running test testBlockedProducerMultiDBInternal1");
        testBlockedProducerMultiDBInternal(false, false);
    }

    @Test
    public void testBlockedProducerMultiDBInternal2() throws Throwable {
        System.out.println("Running test testBlockedProducerMultiDBInternal2");
        testBlockedProducerMultiDBInternal(true, false);
    }

    @Test
    public void testBlockedProducerMultiDBInternal3() throws Throwable {
        System.out.println("Running test testBlockedProducerMultiDBInternal3");
        testBlockedProducerMultiDBInternal(false, true);
    }

    @Test
    public void testBlockedProducerMultiDBInternal4() throws Throwable {
        System.out.println("Running test testBlockedProducerMultiDBInternal4");
        testBlockedProducerMultiDBInternal(true, true);
    }

    public void testBlockedProducerMultiDBInternal(
        boolean serialScan,
        boolean keysonly)
        throws Throwable {

        /*
         * This test is sensitive to cache sizes and isn't important to run
         * with an off-heap cache.
         */
        open(false, CacheMode.DEFAULT, 5*ONE_MB, false /*allowOffHeapCache*/);

        /*
         * Load the initial data:
         * DB0: record keys      0 to  9,999 (10,000 records)
         * DB1: record keys 10,000 to 29,999 (20,000 records)
         * DB2: record keys 30,000 to 59,999 (30,000 records)
         */
        writeData(dbs[0], false, N_RECS, 0);
        writeData(dbs[1], false, 2 * N_RECS, N_RECS);
        writeData(dbs[2], false, 3 * N_RECS, 3 * N_RECS);

        int queueSize = 30000;

        DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
        config.setQueueSize(queueSize);
        config.setLSNBatchSize(1000);
        config.setSerialDBScan(serialScan);
        config.setKeysOnly(keysonly);

        DiskOrderedCursor dos = env.openDiskOrderedCursor(dbs, config);

        DiskOrderedCursorImpl dosImpl =
            DbInternal.getDiskOrderedCursorImpl(dos);

        DOSTestHook hook = new DOSTestHook();
        dosImpl.getScanner().setTestHook1(hook);

        Cursor cursor0 = dbs[0].openCursor(null, null);
        Cursor cursor1 = dbs[1].openCursor(null, null);
        Cursor cursor2 = dbs[2].openCursor(null, null);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        int minFreeSlots = (keysonly ? 128 : 0);

        /*
         * Test 1
         */

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        int freeSlots = dosImpl.freeQueueSlots();

        /* Delete all the records from all DBs */
        while (cursor0.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor0.delete());
        }

        while (cursor1.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor1.delete());
        }

        while (cursor2.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor2.delete());
        }

        assertSame(OperationStatus.NOTFOUND, cursor0.getFirst(key, data, null));
        assertSame(OperationStatus.NOTFOUND, cursor1.getFirst(key, data, null));
        assertSame(OperationStatus.NOTFOUND, cursor2.getFirst(key, data, null));

        cursor0.close();
        cursor1.close();
        cursor2.close();
        env.compress();

        /*
         * Thee dos cursor should return the records that were already in
         * the queue.
         */
        int cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        assertEquals(queueSize - freeSlots, cnt);

        dos.close();

        /*
         * Test 2
         */

        queueSize = 9000;
        config.setQueueSize(queueSize);

        /* Relaod the records */
        writeData(dbs[0], false, N_RECS, 0);
        writeData(dbs[1], false, 2 * N_RECS, N_RECS);
        writeData(dbs[2], false, 3 * N_RECS, 3 * N_RECS);

        cursor0 = dbs[0].openCursor(null, null);
        cursor1 = dbs[1].openCursor(null, null);
        cursor2 = dbs[2].openCursor(null, null);

        dos = env.openDiskOrderedCursor(dbs, config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        freeSlots = dosImpl.freeQueueSlots();

        /*
         * Delete all the records except from the last 100 ones in DB0,
         * the last 200 ones in DB1, and the last 300 ones in DB2.
         */
        cnt = 0;

        while (cursor0.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor0.delete());

            ++cnt;
            if (cnt >= (N_RECS - 100)) {
                break;
            }
        }

        cnt = 0;

        while (cursor1.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor1.delete());

            ++cnt;
            if (cnt >= (2*N_RECS - 200)) {
                break;
            }
        }

        cnt = 0;

        while (cursor2.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            assertSame(OperationStatus.SUCCESS, cursor2.delete());

            ++cnt;
            if (cnt >= (3*N_RECS - 300)) {
                break;
            }
        }

        cursor0.close();
        cursor1.close();
        cursor2.close();
        env.compress();

        /*
         * The dos cursor should return the records that were already in
         * the queue plus the last 600 records.
         */
        cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {
            ++cnt;
        }

        assertEquals(queueSize - freeSlots + 600, cnt);

        dos.close();

        /*
         * Test 3
         */

        /*
         * At this point, the 3 DBs contain 600 records as follows:
         * DB0: record keys  9,899 to  9,999 (100 records)
         * DB1: record keys 29,799 to 29,999 (200 records)
         * DB2: record keys 59,699 to 59,999 (300 records)
         */

        queueSize = 400;
        config.setQueueSize(queueSize);

        dos = env.openDiskOrderedCursor(dbs, config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        freeSlots = dosImpl.freeQueueSlots();

        /*
         * Relaod 1000 records in each DB. The records are inserted "behind"
         * the current position of the DOS and should not be returned by DOS.
         */
        writeData(dbs[0], false, 1000, 1000);
        writeData(dbs[1], false, 1000, 11000);
        writeData(dbs[2], false, 1000, 31000);

        /*
         * The dos cursor should return the last 600 records.
         */
        cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        assertEquals(600, cnt);

        dos.close();

        /*
         * Test 4
         */

        /*
         * At this point, the 3 DBs contain 3600 records as follows:
         * DB0: record keys  1,000 to 1,999  and  9,899 to  9,999 (1100 recs)
         * DB1: record keys 11,000 to 11,999 and 29,799 to 29,999 (1200 recs)
         * DB2: record keys 31,000 to 31,999 and 59,699 to 59,999 (1300 recs)
         */

        dos = env.openDiskOrderedCursor(dbs, config);
        dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);

        /* Loop until queue is full. */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        freeSlots = dosImpl.freeQueueSlots();

        /*
         * Reload the first 1000 records in each DB. Note that with
         * a linear scan, the queue is full with 400 records from DB0
         * only, so the DOS will pickup the 2000 records inserted in
         * DB1 and DB2.
         */
        writeData(dbs[0], false, 1000, 0);
        writeData(dbs[1], false, 1000, 10000);
        writeData(dbs[2], false, 1000, 30000);

        /* Load 20000 new records in each DB */
        writeData(dbs[0], false, 20000, N_RECS);
        writeData(dbs[1], false, 20000, 3 * N_RECS);
        writeData(dbs[2], false, 20000, 6 * N_RECS);

        cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {

            if (!keysonly) {
                int k1 = entryToInt(key);
                int d1 = entryToInt(data);
                assertEquals(k1, d1 * -1);
            }

            ++cnt;
        }

        if (serialScan) {
            assertEquals(65600, cnt);
        } else {
            assertEquals(63600, cnt);
        }

        dos.close();

        close();
    }

    @Test
    public void testCleanDeltasNoEviction() throws Throwable {
        System.out.println("Running test testCleanDeltasNoEviction");
        doTestDeltas(10000, 128 * 2, true, false);
    }

    @Test
    public void testCleanDeltasEviction() throws Throwable {
        System.out.println("Running test testCleanDeltasEviction");
        doTestDeltas(10000, 128 * 2, true, true);
    }

    @Test
    public void testDirtyDeltasNoEviction() throws Throwable {
        /* Cache size sensitivity makes Zing support very difficult. */
        Assume.assumeTrue(!JVMSystemUtils.ZING_JVM);
        System.out.println("Running test testDirtyDeltasNoEviction");
        doTestDeltas(10000, 128 * 2, false, false);
    }

    @Test
    public void testDirtyDeltasEviction() throws Throwable {
        /* Cache size sensitivity makes Zing support very difficult. */
        Assume.assumeTrue(!JVMSystemUtils.ZING_JVM);
        System.out.println("Running test testDirtyDeltasEviction");
        doTestDeltas(10000, 128 * 2, false, true);
    }

    public void doTestDeltas(
        int memoryLimit,
        int queueSize,
        boolean doCkpt,
        boolean allowEviction) throws Throwable {

        boolean debug = false;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        int nRecs = 3 * N_RECS;

        /*
         * Use a small cache so that not all the full bins fit in it.
         */
        int cacheSize = ONE_MB / 5;
        open(false, CacheMode.EVICT_LN, cacheSize);

        EnvironmentMutableConfig envConfig = env.getConfig();
        boolean useOffHeapCache = envConfig.getOffHeapCacheSize() > 0;

        if (useOffHeapCache) {
            int halfSize = cacheSize / 2;

            envConfig = envConfig.
                setCacheSize(halfSize).
                setOffHeapCacheSize(halfSize);

            env.setMutableConfig(envConfig);
        }

        /*
         * Disable all sources of eviction, except CACHEMODE (i.e., the
         * Evictor.doEvictOneIN() will still evict the given IN.
         */
        Evictor evictor = envImpl.getEvictor();
        evictor.setEnabled(allowEviction);

        if (!allowEviction && useOffHeapCache) {
            envConfig = envConfig.setOffHeapCacheSize(1024 * 1024 * 1024);
            env.setMutableConfig(envConfig);
        }

        /*
         * Load the initial set of 30,000 records. The record keys are even
         * numbers. Given that eviction has been disabled, the je cache stores
         * all the BINs in the env as full BINs.
         */
        putEvenRecords(1, nRecs);

        EnvironmentStats stats = env.getStats(null);
        long nDeltas = stats.getNCachedBINDeltas();
        long nBins = stats.getNCachedBINs();
        long nOhDeltas = stats.getOffHeapCachedBINDeltas();
        long nOhBins = stats.getOffHeapCachedBINs();
        assert(nDeltas == 0);

        if (debug) {
            System.out.println("Found " + nBins + " bins in main cache");
            System.out.println("Found " + nDeltas + " deltas in main cache");
            System.out.println("Found " + nOhBins + " bins off-heap");
            System.out.println("Found " + nOhDeltas + " deltas off-heap");
        }

        DiskOrderedCursorConfig dosConfig = new DiskOrderedCursorConfig();
        dosConfig.setQueueSize(queueSize);
        dosConfig.setInternalMemoryLimit(memoryLimit);
        dosConfig.setKeysOnly(true);
        dosConfig.setDebug(debug);

        DiskOrderedCursor dos = dbs[0].openCursor(dosConfig);

        DiskOrderedCursorImpl dosImpl = DbInternal.getDiskOrderedCursorImpl(dos);
        DiskOrderedScanner scanner = dosImpl.getScanner();

        /*
         * Create a non-sticky cursor so that we can have a stable CursorImpl
         * to use below.
         */
        CursorConfig config = new CursorConfig();
        config.setNonSticky(true);

        Cursor cursor = dbs[0].openCursor(null, config);
        CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        int minFreeSlots = 127;

        /*
         * Test 1
         */

        /*
         * Loop until queue is full. The dos producer fills the queue and
         * blocks during phase 1 (after processing 2 full bins).
         */
        while (dosImpl.freeQueueSlots() > minFreeSlots) { }

        synchronized (this) {
            wait(1000);
        }

        /*
         * Create deltas by updating 1 record in each bin and then explicitly
         * calling mutateToBINDelta on each bin.
         */
        List<BIN> bins = new ArrayList<BIN>();
        BIN bin = null;

        while (cursor.getNext(key, data, LockMode.RMW) ==
               OperationStatus.SUCCESS) {

            if (bin == null) {
                bin = cursorImpl.getBIN();
            } else if (bin != cursorImpl.getBIN()) {
                bin = cursorImpl.getBIN();
                bins.add(bin);
                assertSame(OperationStatus.SUCCESS, cursor.putCurrent(data));
            }
        }

        for (BIN bin2 : bins) {
            bin2.latch();
            if (bin2.getInListResident() && bin2.canMutateToBINDelta()) {
                bin2.mutateToBINDelta();
            }
            bin2.releaseLatch();
        }
        bins.clear();

        cursor.close();

        /* Mutate off-heap BINs to deltas also. */
        final DatabaseImpl dbImpl = DbInternal.getDbImpl(dbs[0]);

        for (IN in : envImpl.getInMemoryINs()) {

            if (in.getNormalizedLevel() != 2 ||
                in.getDatabase() != dbImpl) {
                continue;
            }

            for (int i = 0; i < in.getNEntries(); i += 1) {
                in.latchNoUpdateLRU();
                if (in.getOffHeapBINId(i) >= 0) {
                    envImpl.getOffHeapCache().stripLNs(in, i);
                    envImpl.getOffHeapCache().mutateToBINDelta(in, i);
                }
                in.releaseLatch();
            }
        }

        if (doCkpt) {
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            env.checkpoint(ckptConfig);
        }

        if (debug) {
            stats = env.getStats(null);
            nDeltas = stats.getNCachedBINDeltas();
            nBins = stats.getNCachedBINs();
            nOhDeltas = stats.getOffHeapCachedBINDeltas();
            nOhBins = stats.getOffHeapCachedBINs();
            System.out.println("Found " + nBins + " bins in main cache");
            System.out.println("Found " + nDeltas + " deltas in main cache");
            System.out.println("Found " + nOhBins + " bins off-heap");
            System.out.println("Found " + nOhDeltas + " deltas off-heap");
        }

        /*
         * Create a test hook and register it with the DOS producer. The hook
         * will be executed after phase 1 and before phase 2. For each bin
         * delta on which a WeakBinRef was created during phase 1, the hook
         * will evict the bin and clear the WeakReference on it. So, during
         * phase 2, all of these bins will have to be deferred. The total
         * (approximate) memory that will be needed to store these delta is
         * greater than the DOS budget, and as a result, more than one
         * subsequent iteration will be needed to process the deferred bins.
         */
        EvictionHook hook = new EvictionHook(scanner);
        scanner.setEvictionHook(hook);

        /*
         * The dos cursor should return all the records
         */
        int cnt = 0;

        while (dos.getNext(key, data, null) == OperationStatus.SUCCESS) {
            ++cnt;

            if (allowEviction && cnt % 10 == 0) {
                env.evictMemory();
            }
        }

        assertEquals(3*N_RECS, cnt);

        int nIter = scanner.getNIterations();

        //System.out.println("num iterations = " + nIter);

        if (useOffHeapCache) {
            if (doCkpt) {
                assertTrue(nIter > 33);
            } else {
                if (allowEviction) {
                    assertEquals((embeddedLNs ? 37 : 31), nIter);
                } else {
                    assertEquals(embeddedLNs ? 8 : 9, nIter);
                }
            }
        } else {
            if (doCkpt) {
                assertTrue(nIter > 34);
            } else {
                if (allowEviction) {
                    assertEquals((12), nIter);
                } else {
                    assertEquals((8), nIter);
                }
            }
        }

        dos.close();

        close();
    }

    private void putEvenRecords(int startRecord, int nRecords) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        CursorConfig config = new CursorConfig();
        config.setNonSticky(true);

        Cursor cursor = dbs[0].openCursor(null, config);

        for (int i = startRecord; i < nRecords + startRecord; i += 1) {

            key.setData(TestUtils.getTestArray(2*i));
            data.setData(TestUtils.getTestArray(2*i));

            assertEquals(OperationStatus.SUCCESS, cursor.put(key, data));
        }

        cursor.close();
    }

    private void open(
        final boolean allowDuplicates,
        final CacheMode cacheMode,
        final long cacheSize)
        throws Exception {

        open(allowDuplicates, cacheMode, cacheSize, true /*allowOffHeapCache*/);
    }

    private void open(
        final boolean allowDuplicates,
        final CacheMode cacheMode,
        final long cacheSize,
        final boolean allowOffHeapCache)
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();

        if (cacheSize > 0) {
            envConfig.setCacheSize(cacheSize);
        }

        if (!allowOffHeapCache) {
            /* Override what was set by initEnvConfig. */
            envConfig.setOffHeapCacheSize(0);
        }

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");

        envConfig.setConfigParam(
            EnvironmentConfig.DOS_PRODUCER_QUEUE_TIMEOUT, "2 seconds");

        /*
         * Why disable BtreeVerifier?
         *
         * At least, for assertion in line 685, TEST 2 of com.sleepycat.je.
         * dbi.DiskOrderedScanTest.testBlockedProducerKeysOnly,
         * assertEquals(keysonly ? 618 : (queueSize - freeSlots + 300), cnt)
         * encounters AssertionFailedError:
         *              expected:<618> but was:<491>.
         *
         * The reason is as follows.
         *
         * Without BtreeVerifier, the diskOrderedCusor will queue
         * BIN 87(In-memory, 64 entries), BIN 88(In-memory, 127 entries). Then
         * lsnAcc.add(binLsn) accumulates BIN 89(NOT in-memory, 127 entries)
         * for later processing during phase 2. At last, when
         * checking BIN 90, it will find that the remaining capacity is not
         * enough. So it will wait. During this process, the test code deletes
         * many records. When using diskOrderedCusor.getNext to retrieve
         * records, it will get the queued records and the accumulated records,
         * i.e. 64 + 127 + 127 = 318. Adding the left 300 records after
         * deleting, the final count is 618.
         *
         * With BtreeVerifier, the diskOrderedCusor will queue
         * BIN 87(In-memory, 64 entries), BIN 88(In-memory, 127 entries).
         * Now BIN 89(127 entries) is in-memory because BtreeVerifier may
         * cause this BIN to be fetched to cache. But, when checking BIN 89,
         * it will find that the remaining capacity is not
         * enough. So it will wait. During this process, the test code deletes
         * many records. When using diskOrderedCusor.getNext to retrieve
         * records, it will only get the queued records,
         * i.e. 64 + 127 = 191. Adding the left 300 records after
         * deleting, the final count is 419.
         *
         * At least, for some other test cases, BtreeVerifier can also
         * influence the count. So I choose to disable BtreeVerifier for
         * all the test cases in this test.
         *
         * TODO: But this may hide some real errors. So may need to test
         * further when we have enough time.
         * 
         */
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        envConfig.setTransactional(false);
        envConfig.setAllowCreate(true);

        env = new Environment(envHome, envConfig);

        envImpl = DbInternal.getNonNullEnvImpl(env);

        embeddedLNs = (envImpl.getMaxEmbeddedLN() > 0);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(false);
        dbConfig.setTransactional(false);
        dbConfig.setSortedDuplicates(allowDuplicates);
        dbConfig.setCacheMode(cacheMode);

        for (int i = 0; i < numDBs; ++i) {
            dbs[i] = env.openDatabase(null, "testDb" + i, dbConfig);
        }
    }

    private void writeData(
        int nDBs,
        boolean dups,
        int nRecs) {

        for (int i = 0; i < nDBs; ++i) {
            writeData(dbs[i], dups, nRecs, nRecs * i);
        }
    }

    private void writeData(Database db, boolean dups, int nRecs) {
        writeData(db, dups, nRecs, 0);
    }

    private void writeData(
        Database db,
        boolean dups,
        int nRecs,
        int start) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int i = start; i < start + nRecs; i++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i * -1, data);

            assertEquals(OperationStatus.SUCCESS,
                         db.putNoOverwrite(null, key, data));

            if (dups) {
                IntegerBinding.intToEntry(-1 * (i + nRecs + nRecs), data);

                assertEquals(OperationStatus.SUCCESS,
                             db.putNoDupData(null, key, data));
            }
        }

        /*
         * If the scanned data set is large enough, a checkpoint may be needed
         * to ensure all expected records are scanned.  It seems that a
         * checkpoint is needed on some machines but not others, probably
         * because the checkpointer thread gets more or less time.  Therefore,
         * to make the test more reliable we always do a checkpoint here.
         */
        env.checkpoint(new CheckpointConfig().setForce(true));
    }

    private int entryToInt(DatabaseEntry entry) {
        assertEquals(4, entry.getSize());
        return IntegerBinding.entryToInt(entry);
    }

    private void close()
        throws Exception {

        for (int i = 0; i < numDBs; ++i) {
            if (dbs[i] != null) {
                dbs[i].close();
                dbs[i] = null;
            }
        }

        if (env != null) {
            env.close();
            env = null;
        }
    }
}
