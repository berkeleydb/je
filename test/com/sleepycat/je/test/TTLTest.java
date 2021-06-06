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

package com.sleepycat.je.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Get;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.TxnTestCase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests TTL functionality.
 */
@RunWith(Parameterized.class)
public class TTLTest extends TxnTestCase {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final SimpleDateFormat TIME_FORMAT =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.zzz");

    private static final Calendar BASE_CAL = Calendar.getInstance(UTC);

    public static volatile long fixedSystemTime = 0;

    static {
        TIME_FORMAT.setTimeZone(UTC);

        BASE_CAL.set(2016, Calendar.FEBRUARY,
            3 /*day*/, 4 /*hour*/, 5 /*min*/, 6 /*sec*/);
    }

    private static final long BASE_MS = BASE_CAL.getTimeInMillis();

    private Database db;

    @Parameters
    public static List<Object[]> genParams() {

        return getTxnParams(null, false /*rep*/);

//        return getTxnParams(
//        new String[] {TxnTestCase.TXN_USER},
//        false /*rep*/);
    }

    public TTLTest(String type) {

        initEnvConfig();

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
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        txnType = type;
        isTransactional = !txnType.equals(TXN_NULL);
        customName = txnType;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        db = null;
        super.tearDown();
        TTL.setTimeTestHook(null);
        fixedSystemTime = 0;
    }

    /**
     * Tests TTL static functions.
     */
    @Test
    public void testTimeCalculations() {

        /*
         * Ensure that when dividing the system time by MILLIS_PER_DAY and
         * MILLIS_PER_HOUR you really get calendars days and hours, and there
         * isn't some funny business having to do with leap seconds or another
         * date/time anomaly.
         */
        Calendar cal = cloneCalendar(BASE_CAL);
        int calDays = 0;

        while (cal.getTimeInMillis() > 0) {
            cal.add(Calendar.DAY_OF_MONTH, -1);
            calDays += 1;
        }

        assertEquals(
            calDays,
            (BASE_MS + TTL.MILLIS_PER_DAY - 1) / TTL.MILLIS_PER_DAY);

        cal = cloneCalendar(BASE_CAL);
        int calHours = 0;

        while (cal.getTimeInMillis() > 0) {
            cal.add(Calendar.HOUR_OF_DAY, -1);
            calHours += 1;
        }

        assertEquals(
            calHours,
            (BASE_MS + TTL.MILLIS_PER_HOUR - 1) / TTL.MILLIS_PER_HOUR);

        /* Test with legal non-zero values. */

        for (final boolean hours : new boolean[]{false, true}) {

            for (final int ttl :
                new int[]{1, 2, 23, 24, 25, 364, 365, 366, 500, 10000}) {

                final TimeUnit ttlUnit =
                    hours ? TimeUnit.HOURS : TimeUnit.DAYS;

                setFixedTimeHook(BASE_MS);

                cal = cloneCalendar(BASE_CAL);

                cal.add(
                    hours ? Calendar.HOUR_OF_DAY : Calendar.DAY_OF_MONTH,
                    ttl);

                final long calMsBeforeRoundingUp = cal.getTimeInMillis();

                cal.add(
                    hours ? Calendar.HOUR_OF_DAY : Calendar.DAY_OF_MONTH,
                    1 /* round up */);

                if (hours) {
                    truncateToHours(cal);
                } else {
                    truncateToDays(cal);
                }

                final long calMs = cal.getTimeInMillis();

                final int expiration = TTL.ttlToExpiration(ttl, ttlUnit);

                final long expireSystemMs =
                    TTL.expirationToSystemTime(expiration, hours);

                final String label =
                    "ttl = " + ttl + " hours = " + hours +
                        " baseMs = " + BASE_MS +
                        " expect = " + TIME_FORMAT.format(calMs) +
                        " got = " + TIME_FORMAT.format(expireSystemMs);

                assertEquals(label, calMs, expireSystemMs);

                assertEquals(label, hours, TTL.isSystemTimeInHours(calMs));

                assertEquals(
                    label,
                    expiration, TTL.systemTimeToExpiration(calMs, hours));

                assertEquals(
                    ttl,
                    new WriteOptions().setExpirationTime(
                        calMsBeforeRoundingUp, ttlUnit).getTTL());

                assertSame(
                    ttlUnit,
                    new WriteOptions().setExpirationTime(
                        expireSystemMs, null).getTTLUnit());

                assertFalse(label, TTL.isExpired(expiration, hours));
                fixedSystemTime = expireSystemMs;
                assertFalse(label, TTL.isExpired(expiration, hours));
                fixedSystemTime = expireSystemMs + 1;
                assertTrue(label, TTL.isExpired(expiration, hours));
            }
        }

        /* Test legal zero values. */

        assertEquals(0, TTL.ttlToExpiration(0, null));
        assertFalse(TTL.isExpired(0, false));
        assertFalse(TTL.isExpired(0, true));

        /* Test with illegal values. */

        try {
            TTL.ttlToExpiration(-1, TimeUnit.DAYS);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Illegal ttl value"));
        }

        final Set<TimeUnit> badUnits =
            new HashSet<>(EnumSet.allOf(TimeUnit.class));

        badUnits.remove(TimeUnit.DAYS);
        badUnits.remove(TimeUnit.HOURS);
        badUnits.add(null);

        for (final TimeUnit unit : badUnits) {
            try {
                TTL.ttlToExpiration(1, unit);
                fail();
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("ttlUnits not allowed"));
            }
        }
    }

    private static Calendar cloneCalendar(Calendar cal) {
        Calendar clone = Calendar.getInstance(cal.getTimeZone());
        clone.setTimeInMillis(cal.getTimeInMillis());
        return clone;
    }

    private static Calendar truncateToDays(Calendar cal) {
        cal.set(Calendar.HOUR_OF_DAY, 0);
        truncateToHours(cal);
        return cal;
    }

    private static Calendar truncateToHours(Calendar cal) {
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    /**
     * Tests storage and retrieval of expiration time.
     */
    @Test
    public void testExpirationTimeStorage()
        throws FileNotFoundException {

        db = openDb("foo");

        setFixedTimeHook(BASE_MS);

        /*
         * Insert with and without a TTL. Units will be HOURS in all LNs, BINs.
         * The BIN's baseExpiration will change when we write record 3, because
         * it has a lower ttl than record 1.
         */
        write(1, 30, TimeUnit.DAYS);
        write(2, 0, TimeUnit.DAYS);
        write(3, 20, TimeUnit.DAYS);
        write(4, 0, TimeUnit.DAYS);

        read(1, 30, TimeUnit.DAYS);
        read(2, 0, TimeUnit.DAYS);
        read(3, 20, TimeUnit.DAYS);
        read(4, 0, TimeUnit.DAYS);

        /*
         * Updating with and without changing the TTL. When false is passed
         * for updateTtl we add 10 to the ttl and check that it doesn't change.
         */
        write(3, 20, TimeUnit.DAYS, TimeUnit.DAYS, false /*updateTtl*/);
        write(3, 0, TimeUnit.DAYS);
        write(3, 25, TimeUnit.DAYS);

        read(1, 30, TimeUnit.DAYS);
        read(2, 0, TimeUnit.DAYS);
        read(3, 25, TimeUnit.DAYS);
        read(4, 0, TimeUnit.DAYS);

        /*
         * Insert record 4 with HOURS and the BIN units in all other entries
         * will change to hours.
         */
        write(4, 17, TimeUnit.HOURS);

        read(1, 30, TimeUnit.DAYS, TimeUnit.HOURS);
        read(2, 0, TimeUnit.DAYS, TimeUnit.HOURS);
        read(3, 25, TimeUnit.DAYS, TimeUnit.HOURS);
        read(4, 17, TimeUnit.HOURS, TimeUnit.HOURS);

        /*
         * Special case: Change BIN expirationBase at same time as changing
         * units from DAYS to HOURS. Use a new empty database.
         */
        db.close();
        db = openDb("bar");

        write(1, 2, TimeUnit.DAYS);
        write(2, 30, TimeUnit.HOURS);

        read(1, 2, TimeUnit.DAYS, TimeUnit.HOURS);
        read(2, 30, TimeUnit.HOURS, TimeUnit.HOURS);

        /* Close and recover. */
        db.close();
        closeEnv();
        openEnv();

        db = openDb("foo");

        read(1, 30, TimeUnit.DAYS, TimeUnit.HOURS);
        read(2, 0, TimeUnit.DAYS, TimeUnit.HOURS);
        read(3, 25, TimeUnit.DAYS, TimeUnit.HOURS);
        read(4, 17, TimeUnit.HOURS, TimeUnit.HOURS);

        db.close();
        db = openDb("bar");

        read(1, 2, TimeUnit.DAYS, TimeUnit.HOURS);
        read(2, 30, TimeUnit.HOURS, TimeUnit.HOURS);

        db.close();
    }

    /**
     * Tests undo/redo of expiration time. Abort and recovery are tested, but
     * not rollback.
     */
    @Test
    public void testUndoAndRedo()
        throws FileNotFoundException {

        if (!isTransactional) {
            return;
        }

        db = openDb("foo");

        setFixedTimeHook(BASE_MS);

        /*
         * Insert a single record with 2 DAYS, then update it and change its
         * ttl to 30 HOURS, but abort the update. The previous ttl is restored
         * by the abort, including the BIN units because it is the only record
         * in the BIN.
         */
        write(1, 2, TimeUnit.DAYS);

        Transaction txn = txnBeginCursor();
        write(txn, 1, 30, TimeUnit.HOURS, TimeUnit.HOURS, true /*updateTtl*/);
        txnAbort(txn);

        read(1, 2, TimeUnit.DAYS);

        /*
         * Insert another record using DAYS, then repeat the update with abort.
         * In this case the BIN units stays set to HOURS. We don't currently
         * convert existing records back from HOURS to DAYS.
         */
        write(2, 3, TimeUnit.DAYS);

        txn = txnBeginCursor();
        write(txn, 1, 20, TimeUnit.HOURS, TimeUnit.HOURS, true /*updateTtl*/);
        txnAbort(txn);

        read(1, 2, TimeUnit.DAYS, TimeUnit.HOURS);
        read(2, 3, TimeUnit.DAYS, TimeUnit.HOURS);

        /*
         * Crash and recover. The BIN unit is restored to DAYS because only the
         * committed LNs are replayed.
         */
        env.flushLog(false);
        abnormalClose(env);
        openEnv();
        db = openDb("foo");

        read(1, 2, TimeUnit.DAYS);
        read(2, 3, TimeUnit.DAYS);

        /*
         * Insert one committed record, and do an update that is a aborted.
         * Then "crash" and recover, causing one redo and one undo.
         */
        write(3, 10, TimeUnit.DAYS);

        txn = txnBeginCursor();
        write(txn, 2, 6, TimeUnit.HOURS, TimeUnit.HOURS, true /*updateTtl*/);
        txnAbort(txn);

        read(1, 2, TimeUnit.DAYS, TimeUnit.HOURS);
        read(2, 3, TimeUnit.DAYS, TimeUnit.HOURS);
        read(3, 10, TimeUnit.DAYS, TimeUnit.HOURS);

        env.flushLog(false);
        abnormalClose(env);
        openEnv();
        db = openDb("foo");

        /* Recovery happens to restore the units to DAYS further above. */
        read(1, 2, TimeUnit.DAYS);
        read(2, 3, TimeUnit.DAYS);
        read(3, 10, TimeUnit.DAYS);

        db.close();
    }

    /**
     * Tests that the lazyCompress method removes expired slots correctly. Does
     * not test that lazyCompress is called at appropriate times.
     */
    @Test
    public void testCompression()
        throws FileNotFoundException {

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        db = openDb("foo");

        setFixedTimeHook(BASE_MS);

        /* Insert 100 records with a ttl equal to their key. */
        for (int i = 1; i <= 100; i += 1) {
            write(i, i, TimeUnit.DAYS);
        }

        final BIN bin = getFirstBIN();
        assertEquals(100, bin.getNEntries());

        /*
         * Incrementing the clock by one day at a time will purge one slot at a
         * time. We're compressing dirty slots here, just for testing.
         */
        bin.latch();
        try {
            for (int i = 0; i <= 100; i += 1) {
                fixedSystemTime += TTL.MILLIS_PER_DAY;
                envImpl.lazyCompress(bin, true /*compressDirtySlots*/);
                assertEquals(100 - i, bin.getNEntries());
            }
        } finally {
            bin.releaseLatch();
        }

        /*
         * Insert 100 records with a 1 day ttl, then sync to flush the full
         * BIN and clear the slot dirty flags, then insert 10 more records
         * that will be dirty and would cause logging as a BIN-delta.
         */
        fixedSystemTime = BASE_MS;

        for (int i = 1; i <= 100; i += 1) {
            write(i, 1, TimeUnit.DAYS);
        }

        env.sync();

        for (int i = 101; i <= 110; i += 1) {
            write(i, 1, TimeUnit.DAYS);
        }

        /*
         * lazyCompress with no second param will not purge dirty slots,
         * since a BIN-delta should be logged. Calling again to compress all
         * slots will purge the rest.
         */
        bin.latch();
        try {
            fixedSystemTime += 2 * TTL.MILLIS_PER_DAY;
            envImpl.lazyCompress(bin);
            assertEquals(10, bin.getNEntries());
            envImpl.lazyCompress(bin, true /*compressDirtySlots*/);
            assertEquals(0, bin.getNEntries());
        } finally {
            bin.releaseLatch();
        }

        db.close();
    }

    /**
     * Tests that the expiration time is preserved by BIN-deltas.
     */
    @Test
    public void testBINDelta()
        throws FileNotFoundException {

        db = openDb("foo");
        setFixedTimeHook(BASE_MS);

        /*
         * Insert 100 records with a a ttl equal to their key, then sync to
         * flush the full BIN and clear the slot dirty flags, then insert 10
         * more records that will be dirty and would cause logging as a
         * BIN-delta.
         */
        for (int i = 1; i <= 100; i += 1) {
            write(i, i, TimeUnit.DAYS);
        }

        final BIN bin = getFirstBIN();
        assertEquals(100, bin.getNEntries());

        env.sync();

        for (int i = 101; i <= 110; i += 1) {
            write(i, i, TimeUnit.DAYS);
        }

        assertEquals(110, bin.getNEntries());

        for (int i = 1; i <= 110; i += 1) {
            read(i, i, TimeUnit.DAYS);
        }

        /*
         * Check that BIN-delta has 10 slots with correct expiration.
         */
        bin.latch();
        bin.mutateToBINDelta();
        bin.releaseLatch();

        assertEquals(10, bin.getNEntries());

        for (int i = 101; i <= 110; i += 1) {
            read(i, i, TimeUnit.DAYS);
        }

        /*
         * Check that reconstituted BIN has all slots with correct expiration.
         */
        bin.latch();
        bin.mutateToFullBIN(false /*leaveSlotFree*/);
        bin.releaseLatch();

        for (int i = 1; i <= 110; i += 1) {
            read(i, i, TimeUnit.DAYS);
        }

        db.close();
    }

    /**
     * Tests that expired records are filtered out of queries.
     */
    @Test
    public void testFiltering()
        throws FileNotFoundException {

        db = openDb("foo");

        /*
         * Insert 11 records with a ttl equal to their key.
         * Record 0 has no TTL.
         */
        setFixedTimeHook(BASE_MS);

        final Map<Integer, Long> expected = new HashMap<>();
        for (int i = 0; i <= 10; i += 1) {
            final long expTime = write(i, i, TimeUnit.DAYS);
            expected.put(i, expTime);
        }

        final BIN bin = getFirstBIN();
        assertEquals(11, bin.getNEntries());

        /*
         * Incrementing the clock by one day at a time will cause one slot at a
         * time to expire, starting from record 1. Compression will not occur,
         * but expired records will be filtered out of queries.
         */
        for (int i = 0; i <= 10; i += 1) {

            fixedSystemTime += TTL.MILLIS_PER_DAY;

            if (i != 0) {
                expected.remove(i);
            }

            for (int j = 0; j <= 10; j += 1) {
                if (expected.containsKey(j)) {
                    readExpectFound(j);
                } else {
                    readExpectNotFound(j);
                }
            }

            diskOrderedScan(expected);

            final PreloadStats stats =
                db.preload(new PreloadConfig().setLoadLNs(true));

            assertEquals(
                11 - i, stats.getNLNsLoaded() + stats.getNEmbeddedLNs());

            /* Make sure compression did not occur. */
            assertEquals(11, bin.getNEntries());
        }

        db.close();
    }

    private long write(final int key, final int ttl, final TimeUnit ttlUnits)
        throws FileNotFoundException {

        return write(key, ttl, ttlUnits, ttlUnits, true /*updateTtl*/);
    }

    private long write(final int key,
                       final int ttl,
                       final TimeUnit ttlUnits,
                       final TimeUnit binUnits,
                       final boolean updateTtl)
        throws FileNotFoundException {

        final Transaction txn = txnBeginCursor();
        write(txn, key, ttl, ttlUnits, binUnits, updateTtl);
        txnCommit(txn);

        return read(key, ttl, ttlUnits, binUnits);
    }

    private void write(final Transaction txn,
                       final int key,
                       final int ttl,
                       final TimeUnit ttlUnits,
                       final TimeUnit binUnits,
                       final boolean updateTtl)
        throws FileNotFoundException {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        IntegerBinding.intToEntry(key, keyEntry);

        final WriteOptions options = new WriteOptions();

        if (updateTtl) {
            if (ttlUnits == TimeUnit.DAYS) {
                options.setTTL(ttl);
            } else {
                options.setTTL(ttl, ttlUnits);
            }
            options.setUpdateTTL(true);
        } else {
            options.setTTL(ttl + 10, ttlUnits);
        }

        IntegerBinding.intToEntry(ttl, dataEntry);

        final OperationResult result = db.put(
            txn, keyEntry, dataEntry, Put.OVERWRITE, options);

        assertNotNull(result);

        checkedStoredTtl(txn, keyEntry, ttl, ttlUnits, binUnits);
    }

    private long read(final int key,
                      final int ttl,
                      final TimeUnit ttlUnits)
        throws FileNotFoundException {

        return read(key, ttl, ttlUnits, ttlUnits);
    }

    private long read(final int key,
                      final int ttl,
                      final TimeUnit ttlUnits,
                      final TimeUnit binUnits)
        throws FileNotFoundException {

        final Transaction txn = txnBeginCursor();
        final Cursor cursor = db.openCursor(txn, null);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        IntegerBinding.intToEntry(key, keyEntry);

        final OperationResult result =
            cursor.get(keyEntry, dataEntry, Get.SEARCH, null);

        assertNotNull(result);
        assertEquals(ttl, IntegerBinding.entryToInt(dataEntry));

        checkedStoredTtl(txn, keyEntry, ttl, ttlUnits, binUnits);

        cursor.close();
        txnCommit(txn);

        return result.getExpirationTime();
    }

    private void checkedStoredTtl(final Transaction txn,
                                  final DatabaseEntry keyEntry,
                                  final int ttl,
                                  final TimeUnit ttlUnits,
                                  final TimeUnit binUnits)
        throws FileNotFoundException {

        final boolean ttlHours = (ttlUnits == TimeUnit.HOURS);
        final boolean binHours = (binUnits == TimeUnit.HOURS);

        /*
         * ttlToExpiration uses the current time, which may have changed since
         * the record was inserted. This happens to be OK, because the test
         * doesn't call this method after changing the time.
         */
        final int ttlExpiration = TTL.ttlToExpiration(ttl, ttlUnits);

        final long expireTime =
            TTL.expirationToSystemTime(ttlExpiration, ttlHours);

        OperationResult result = db.get(txn, keyEntry, null, Get.SEARCH, null);
        assertNotNull(result);

        assertEquals(expireTime, result.getExpirationTime());

        final int expiration =
            TTL.systemTimeToExpiration(expireTime, binHours);

        final Cursor cursor = db.openCursor(txn, null);
        result = cursor.get(keyEntry, null, Get.SEARCH, null);
        assertNotNull(result);
        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
        final BIN bin = cursorImpl.getBIN();
        final int index = cursorImpl.getIndex();
        cursor.close();

        assertEquals(binHours, bin.isExpirationInHours());
        assertEquals(expiration, bin.getExpiration(index));

        final LNLogEntry logEntry = (LNLogEntry)
            DbInternal.getNonNullEnvImpl(env).
                getLogManager().
                getLogEntry(bin.getLsn(index));

        assertEquals(ttlExpiration, logEntry.getExpiration());
        assertEquals(ttlHours, logEntry.isExpirationInHours());
    }

    private Database openDb(final String name) {

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);

        final Transaction txn = txnBegin();
        try {
            return env.openDatabase(txn, name, dbConfig);
        } finally {
            txnCommit(txn);
        }
    }

    private BIN getFirstBIN() {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        final Cursor cursor = db.openCursor(null, null);

        assertSame(
            OperationStatus.SUCCESS,
            cursor.getFirst(keyEntry, dataEntry, null));

        final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();

        cursor.close();

        return bin;
    }

    private void readExpectFound(final int key) {
        readExpectStatus(key, OperationStatus.SUCCESS);
    }

    private void readExpectNotFound(final int key) {
        readExpectStatus(key, OperationStatus.NOTFOUND);
    }

    private void readExpectStatus(final int key,
                                  final OperationStatus expectStatus) {

        final Transaction txn = txnBegin();

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        IntegerBinding.intToEntry(key, keyEntry);

        final OperationStatus status =
            db.get(null, keyEntry, dataEntry, null);

        assertSame("key = " + key, expectStatus, status);

        txnCommit(txn);
    }

    private void diskOrderedScan(final Map<Integer, Long> expected) {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final Map<Integer, Long> found = new HashMap<>();

        try (final DiskOrderedCursor cursor = db.openCursor(null)) {
            OperationResult result;
            while ((result = cursor.get(
                    keyEntry, dataEntry, Get.NEXT, null)) != null) {
                found.put(
                    IntegerBinding.entryToInt(keyEntry),
                    result.getExpirationTime());
            }
        }

        assertEquals(expected, found);
    }

    /**
     * Tests that the expiration time is stored in the secondary DB to match the
     * primary DB.
     */
    @Test
    public void testSecondaryExpirationTimeStorage()
        throws FileNotFoundException {

        /*
         * First byte of primary data is secondary key.
         * Byte value 100 means null or no key.
         */
        final SecondaryKeyCreator keyCreator = new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                final byte val = data.getData()[0];
                if (val == 100) {
                    return false;
                }
                result.setData(new byte[]{val});
                return true;
            }
        };

        /*
         * Each byte of primary data is secondary key.
         * Byte value 100 means null or no key.
         */
        final SecondaryMultiKeyCreator multiKeyCreator =
            new SecondaryMultiKeyCreator() {
                @Override
                public void createSecondaryKeys(SecondaryDatabase secondary,
                                                DatabaseEntry key,
                                                DatabaseEntry data,
                                                Set<DatabaseEntry> results) {
                    for (byte val : data.getData()) {
                        if (val == 100) {
                            continue;
                        }
                        results.add(new DatabaseEntry(new byte[]{val}));
                    }
                }
            };

        db = openDb("primary");
        final SecondaryDatabase secDb1;
        final SecondaryDatabase secDb2;

        final SecondaryConfig config = new SecondaryConfig();
        config.setTransactional(isTransactional);
        config.setAllowCreate(true);
        config.setSortedDuplicates(true);

        final Transaction txn = txnBegin();
        config.setKeyCreator(keyCreator);
        config.setMultiKeyCreator(null);
        secDb1 = env.openSecondaryDatabase(txn, "sec1", db, config);

        config.setMultiKeyCreator(multiKeyCreator);
        config.setKeyCreator(null);
        secDb2 = env.openSecondaryDatabase(txn, "sec2", db, config);
        txnCommit(txn);

        setFixedTimeHook(BASE_MS);

        final int priKey = 123;
        final byte[] data = new byte[3];

        data[0] = 0;
        data[1] = 1;
        data[2] = 2;

        checkSecondary(db, secDb1, secDb2, priKey, data, 1);
        checkSecondary(db, secDb1, secDb2, priKey, data, 2);

        data[0] = 1;
        data[1] = 2;
        data[2] = 3;

        checkSecondary(db, secDb1, secDb2, priKey, data, 3);

        data[0] = 2;
        data[1] = 2;
        data[2] = 4;

        checkSecondary(db, secDb1, secDb2, priKey, data, 4);

        data[0] = 2;
        data[1] = 3;
        data[2] = 100;

        checkSecondary(db, secDb1, secDb2, priKey, data, 5);

        data[0] = 100;
        data[1] = 3;
        data[2] = 4;

        checkSecondary(db, secDb1, secDb2, priKey, data, 6);

        secDb1.close();
        secDb2.close();
        db.close();
    }

    private void checkSecondary(final Database priDb,
                                final SecondaryDatabase secDb1,
                                final SecondaryDatabase secDb2,
                                final int priKey,
                                final byte[] data,
                                final int ttl) {

        final long baseExpirationTime =
            truncateToHours(cloneCalendar(BASE_CAL)).getTimeInMillis() +
                TTL.MILLIS_PER_HOUR;

        final long expireTime =
            baseExpirationTime + (ttl * TTL.MILLIS_PER_HOUR);

        final WriteOptions options = new WriteOptions();
        options.setTTL(ttl, TimeUnit.HOURS);
        options.setUpdateTTL(true);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        dataEntry.setData(data);
        IntegerBinding.intToEntry(priKey, keyEntry);

        final Transaction txn = txnBegin();

        final OperationResult result = priDb.put(
            txn, keyEntry, dataEntry, Put.OVERWRITE, options);

        assertNotNull(result);

        checkExpirationTime(priDb, txn, keyEntry, expireTime);

        keyEntry.setData(new byte[]{data[0]});

        checkExpirationTime(secDb1, txn, keyEntry, expireTime);

        for (final byte val : data) {
            keyEntry.setData(new byte[]{val});
            checkExpirationTime(secDb2, txn, keyEntry, expireTime);
        }

        txnCommit(txn);
    }

    private void checkExpirationTime(final Database db,
                                     final Transaction txn,
                                     final DatabaseEntry keyEntry,
                                     final long expireTime) {

        final OperationResult result =
            db.get(txn, keyEntry, null, Get.SEARCH, null);

        if (keyEntry.getData()[0] == 100) {
            assertNull(result);
            return;
        }

        assertNotNull(result);

        assertEquals(
            "Expect = " + TTL.formatExpirationTime(expireTime) +
            " Got = " + TTL.formatExpirationTime(result.getExpirationTime()),
            expireTime, result.getExpirationTime());
    }

    /**
     * If a record expires after being locked, it should be treated as not
     * expired.
     */
    @Test
    public void testRepeatableRead() throws Throwable {

        db = openDb("foo");

        setFixedTimeHook(BASE_MS);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        IntegerBinding.intToEntry(123, keyEntry);
        dataEntry.setData(new byte[1]);

        Transaction txn = txnBeginCursor();
        final Cursor cursor = db.openCursor(txn, CursorConfig.READ_COMMITTED);

        /*
         * Write a record that will expire in one hour.
         */
        final WriteOptions options =
            new WriteOptions().setTTL(1, TimeUnit.HOURS);

        OperationResult result = cursor.put(
            keyEntry, dataEntry, Put.NO_OVERWRITE, options);
        assertNotNull(result);

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNotNull(result);

        /*
         * When time advances, record will be expired but still returned when
         * reading a second time.
         */
        fixedSystemTime = result.getExpirationTime() + 1;

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNotNull(result);

        cursor.close();
        txnCommit(txn);

        /*
         * When reading with a different transaction, the expired record is not
         * returned.
         */
        txn = txnBegin();

        result = db.get(txn, keyEntry, dataEntry, Get.SEARCH, null);
        assertNull(result);

        txnCommit(txn);

        /*
         * A special case is when we have to wait for a lock, and while waiting
         * the TTL is changed via an update or an aborted update.
         */
        final CountDownLatch latch1 = new CountDownLatch(1);

        final AtomicReference<Throwable> thread1Ex =
            new AtomicReference<>(null);

        final Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    final Transaction txn1 = txnBeginCursor();
                    final Cursor cursor1 = db.openCursor(txn1, null);

                    /* First insert record with a one hour TTL. */
                    OperationResult result = cursor1.put(
                        keyEntry, dataEntry, Put.NO_OVERWRITE,
                        new WriteOptions().setTTL(1).setUpdateTTL(true));

                    assertNotNull(result);

                    fixedSystemTime = result.getExpirationTime() + 1;

                    latch1.countDown();

                    /*
                     * Give main thread time to block on read lock.
                     */
                    Thread.sleep(100);

                    /*
                     * While main thread is waiting for the lock, change the
                     * TTL to zero.
                     */
                    result = cursor1.put(
                        null, dataEntry, Put.CURRENT,
                        new WriteOptions().setTTL(0).setUpdateTTL(true));

                    assertNotNull(result);

                    cursor1.close();
                    txnCommit(txn1);

                } catch (Throwable e) {
                    thread1Ex.set(e);
                }
            }
        };

        try {
            /*
             * Record is initially expired (this was confirmed above). Thread1
             * will insert another record with no TTL in the same slot.
             */
            thread1.start();

            /* Wait for thread1 to get write lock. */
            latch1.await(30, TimeUnit.SECONDS);

            /*
             * While thread1 is sleeping, try to read, which will block.
             * CursorImpl.lockLN will initially see the record as expired,
             * which is the special case we're exercising.
             */
            txn = txnBegin();
            result = db.get(txn, keyEntry, dataEntry, Get.SEARCH, null);
            txnCommit(txn);

            /*
             * Thread1 has updated the record and given it a TTL of 1.
             * When thread1 commits, the record will not be expired.
             */
            assertNotNull(result);

        } finally {
            new PollCondition(1, 30000) {
                @Override
                public boolean condition() {
                    return !thread1.isAlive();
                }
            }.await();
        }

        if (thread1Ex.get() != null) {
            throw thread1Ex.get();
        }

        db.close();
    }

    /**
     * An extra lock on the secondary is needed to support repeatable-read. The
     * lock should only be taken when the record will expire within {@link
     * EnvironmentParams#ENV_TTL_MAX_TXN_TIME}.
     */
    @Test
    public void testSecondaryRepeatableRead() {

        /* First byte of primary data is secondary key. */
        final SecondaryKeyCreator keyCreator = new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData(new byte[]{data.getData()[0]});
                return true;
            }
        };

        db = openDb("primary");

        final SecondaryConfig config = new SecondaryConfig();
        config.setTransactional(isTransactional);
        config.setAllowCreate(true);
        config.setSortedDuplicates(true);

        config.setKeyCreator(keyCreator);
        config.setMultiKeyCreator(null);

        Transaction txn = txnBegin();

        final SecondaryDatabase secDb = env.openSecondaryDatabase(
            txn, "sec", db, config);

        txnCommit(txn);

        setFixedTimeHook(BASE_MS);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final DatabaseEntry secKeyEntry = new DatabaseEntry();

        IntegerBinding.intToEntry(123, keyEntry);
        dataEntry.setData(new byte[]{4});
        secKeyEntry.setData(new byte[]{4});

        /*
         * Write a record that will expire in one hour.
         */
        WriteOptions options = new WriteOptions().setTTL(1, TimeUnit.HOURS);

        txn = txnBeginCursor();

        OperationResult result = db.put(
            txn, keyEntry, dataEntry, Put.NO_OVERWRITE, options);
        assertNotNull(result);

        txnCommit(txn);

        txn = txnBeginCursor();

        Cursor cursor = db.openCursor(txn, CursorConfig.READ_COMMITTED);

        SecondaryCursor secCursor = secDb.openCursor(
            txn, CursorConfig.READ_COMMITTED);

        int nOrigLocks = env.getStats(null).getNTotalLocks();

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNotNull(result);

        assertEquals(1 + nOrigLocks, env.getStats(null).getNTotalLocks());

        result = secCursor.get(
            secKeyEntry, null, dataEntry, Get.SEARCH, null);

        assertNotNull(result);

        /* A lock on the secondary is held to support repeatable-read. */
        assertEquals(2 + nOrigLocks, env.getStats(null).getNTotalLocks());

        /*
         * When time advances, record will be expired but still returned when
         * reading a second time.
         */
        fixedSystemTime = result.getExpirationTime() + 1;

        result = secCursor.get(
            secKeyEntry, keyEntry, dataEntry, Get.SEARCH, null);

        assertNotNull(result);

        secCursor.close();
        cursor.close();
        txnCommit(txn);

        /*
         * When reading with a different transaction, the expired record is not
         * returned.
         */
        txn = txnBegin();

        result = secDb.get(
            txn, secKeyEntry, keyEntry, dataEntry, Get.SEARCH, null);

        assertNull(result);

        txnCommit(txn);

        /*
         * When the record does not expire within 24 hours, an extra lock is
         * not needed.
         */
        options = new WriteOptions().setTTL(3, TimeUnit.DAYS);

        txn = txnBeginCursor();

        result = db.put(txn, keyEntry, dataEntry, Put.NO_OVERWRITE, options);
        assertNotNull(result);

        txnCommit(txn);

        txn = txnBeginCursor();

        cursor = db.openCursor(txn, CursorConfig.READ_COMMITTED);

        secCursor = secDb.openCursor(
            txn, CursorConfig.READ_COMMITTED);

        nOrigLocks = env.getStats(null).getNTotalLocks();

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNotNull(result);

        assertEquals(1 + nOrigLocks, env.getStats(null).getNTotalLocks());

        result = secCursor.get(
            secKeyEntry, keyEntry, dataEntry, Get.SEARCH, null);

        assertNotNull(result);

        /* No extra lock on the secondary is needed. */
        assertEquals(1 + nOrigLocks, env.getStats(null).getNTotalLocks());

        secCursor.close();
        cursor.close();
        txnCommit(txn);

        secDb.close();
        db.close();
    }

    /**
     * In the set of records consisting of a primary record and its associated
     * secondary records, if only some records are locked, the other records
     * may expire. We test locking only the primary and only the secondary.
     */
    @Test
    public void testSecondaryLimitations() {

        /* First byte of primary data is secondary key. */
        final SecondaryKeyCreator keyCreator = new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(SecondaryDatabase secondary,
                                              DatabaseEntry key,
                                              DatabaseEntry data,
                                              DatabaseEntry result) {
                result.setData(new byte[]{data.getData()[0]});
                return true;
            }
        };

        db = openDb("primary");

        final SecondaryConfig config = new SecondaryConfig();
        config.setTransactional(isTransactional);
        config.setAllowCreate(true);
        config.setSortedDuplicates(true);

        config.setKeyCreator(keyCreator);
        config.setMultiKeyCreator(null);

        Transaction txn = txnBegin();

        final SecondaryDatabase secDb = env.openSecondaryDatabase(
            txn, "sec", db, config);

        txnCommit(txn);

        setFixedTimeHook(BASE_MS);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final DatabaseEntry secKeyEntry = new DatabaseEntry();

        IntegerBinding.intToEntry(123, keyEntry);
        dataEntry.setData(new byte[]{4});
        secKeyEntry.setData(new byte[]{4});

        /*
         * Write a record that will expire in one hour.
         */
        WriteOptions options = new WriteOptions().setTTL(1, TimeUnit.HOURS);

        txn = txnBeginCursor();

        OperationResult result = db.put(
            txn, keyEntry, dataEntry, Put.NO_OVERWRITE, options);
        assertNotNull(result);

        txnCommit(txn);

        txn = txnBeginCursor();

        /*
         * Lock the primary only.
         */
        Cursor cursor = db.openCursor(txn, CursorConfig.READ_COMMITTED);

        SecondaryCursor secCursor = secDb.openCursor(
            txn, CursorConfig.READ_COMMITTED);

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNotNull(result);

        /*
         * When time advances, the primary record will not expire, but the
         * secondary record will expire.
         */
        fixedSystemTime = result.getExpirationTime() + 1;

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNotNull(result);

        result = secCursor.get(
            secKeyEntry, keyEntry, dataEntry, Get.SEARCH, null);

        assertNull(result);

        secCursor.close();
        cursor.close();
        txnCommit(txn);

        /*
         * Write a record that will expire in one hour.
         */
        options = new WriteOptions().setTTL(1, TimeUnit.HOURS);

        txn = txnBeginCursor();

        result = db.put(
            txn, keyEntry, dataEntry, Put.NO_OVERWRITE, options);
        assertNotNull(result);

        txnCommit(txn);

        txn = txnBeginCursor();

        /*
         * Lock the secondary only. Null is passed for the data param, which
         * means that the primary is not read or locked.
         */
        cursor = db.openCursor(txn, CursorConfig.READ_COMMITTED);
        secCursor = secDb.openCursor(txn, CursorConfig.READ_COMMITTED);

        result = secCursor.get(
            secKeyEntry, keyEntry, null, Get.SEARCH, null);

        assertNotNull(result);

        /*
         * When time advances, the secondary record will not expire, but the
         * primary record will expire.
         */
        fixedSystemTime = result.getExpirationTime() + 1;

        result = secCursor.get(
            secKeyEntry, keyEntry, null, Get.SEARCH, null);

        assertNotNull(result);

        result = cursor.get(keyEntry, dataEntry, Get.SEARCH, null);
        assertNull(result);

        result = secCursor.get(
            secKeyEntry, keyEntry, dataEntry, Get.SEARCH, null);

        assertNull(result);

        secCursor.close();
        cursor.close();
        txnCommit(txn);

        secDb.close();
        db.close();
    }

    /**
     * Tests that disk space for purged LNs is reclaimed by the cleaner, and
     * that special cases of repeatable-read (when the LN is purged) are
     * handled properly.
     */
    @Test
    public void testPurgedLNs() throws Throwable {

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        final DatabaseEntry keyEntry = new DatabaseEntry();

        /* Each record with 1MB of data will fill a .jdb file. */
        final DatabaseEntry dataEntry =
            new DatabaseEntry(new byte[1024 * 1024 * 10]);

        db = openDb("foo");

        setFixedTimeHook(BASE_MS);

        /*
         * Write five records that will expire one each hour, each in a
         * separate .jdb file.
         */
        Transaction txn = txnBegin();
        OperationResult result;

        for (int i = 0; i < 5; i += 1) {

            final WriteOptions options =
                new WriteOptions().setTTL(i + 1, TimeUnit.HOURS);

            keyEntry.setData(new byte[]{(byte) i});

            result = db.put(
                txn, keyEntry, dataEntry, Put.NO_OVERWRITE, options);

            assertNotNull(result);
        }

        txnCommit(txn);

        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Add a couple more .jdb files so that the preceding .jdb files are
         * eligible for cleaning.
         */
        envImpl.forceLogFileFlip();
        envImpl.forceLogFileFlip();

        /*
         * Position a cursor on the BIN to prevent expired slots from being
         * purged. No lock is held so.
         */
        final Cursor holderCursor = db.openCursor(null, null);

        result = holderCursor.get(
            null, null, Get.FIRST,
            new ReadOptions().setLockMode(LockMode.READ_UNCOMMITTED));

        assertNotNull(result);

        /*
         * Advance time in one hour increments, and expect one record to expire
         * at each increments.
         *
         * Use EVICT_LN for reading to detect when an LN has been purged.
         *
         * No purging will have taken place (cleaning and compression are
         * disabled), so this is just confirming that filtering is happening as
         * expected.
         */
        final ReadOptions options =
            new ReadOptions().setCacheMode(CacheMode.EVICT_LN);

        fixedSystemTime += TTL.MILLIS_PER_HOUR;

        for (int j = 0; j < 5; j += 1) {

            fixedSystemTime += TTL.MILLIS_PER_HOUR;

            for (int i = 0; i < 5; i += 1) {
                txn = txnBegin();

                keyEntry.setData(new byte[]{(byte) i});
                result = db.get(txn, keyEntry, dataEntry, Get.SEARCH, options);

                final String msg = "i=" + i + " j=" + j;

                if (i > j) {
                    assertNotNull(msg, result);
                } else {
                    assertNull(msg, result);
                }

                txnCommit(txn);
            }
        }

        /*
         * Reset clock back to the write time. Expect all records exists. This
         * ensures that LNs have not been purged yet.
         */
        fixedSystemTime -= 6 * TTL.MILLIS_PER_HOUR;

        txn = txnBegin();

        for (int i = 0; i < 5; i += 1) {
            keyEntry.setData(new byte[]{(byte) i});
            result = db.get(txn, keyEntry, dataEntry, Get.SEARCH, options);
            assertNotNull(result);
        }

        txnCommit(txn);

        /*
         * Advance time so that all records expire and clean all eligible
         * files. The first checkpoint is needed to advance the FirstActiveLSN,
         * and the second to delete the cleaned files.
         */
        env.checkpoint(new CheckpointConfig().setForce(true));

        fixedSystemTime += 6 * TTL.MILLIS_PER_HOUR;

        envImpl.getCleaner().doClean(
            true /*cleanMultipleFiles*/, true /*forceCleaning*/);

        assertEquals(5, env.getStats(null).getNLNsExpired());

        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Confirm that all records are expired.
         */
        txn = txnBegin();

        for (int i = 0; i < 5; i += 1) {
            keyEntry.setData(new byte[]{(byte) i});
            result = db.get(txn, keyEntry, dataEntry, Get.SEARCH, options);
            assertNull(result);
        }

        txnCommit(txn);

        /*
         * Reset clock back to the write time, then advance time in one hour
         * increments. At this point all LNs have been purged, but slots still
         * exist and are not expired until we advance the time.
         */
        fixedSystemTime -= 6 * TTL.MILLIS_PER_HOUR;

        txn = txnBeginCursor();
        final Cursor cursor = db.openCursor(txn, null);

        fixedSystemTime += TTL.MILLIS_PER_HOUR;

        for (int i = 0; i < 5; i += 1) {

            /*
             * When reading with a null 'data' param, the slot is not expired
             * so the result is non-null, even though the LN was purged.
             */
            keyEntry.setData(new byte[]{(byte) i});

            result = cursor.get(keyEntry, null, Get.SEARCH, options);
            assertNotNull(result);

            fixedSystemTime += TTL.MILLIS_PER_HOUR;

            /*
             * Read again a null 'data' param. The slot is now expired, but the
             * result is non-null because we hold a lock.
             */
            result = cursor.get(keyEntry, null, Get.SEARCH, options);
            assertNotNull(result);

            /*
             * Read again with a non-null 'data' param. This time the result is
             * null because the LN cannot be fetched.
             */
            result = cursor.get(keyEntry, dataEntry, Get.CURRENT, options);
            assertNull(result);

            /*
             * Try to update with a partial 'data' param. Null is returned
             * because the old LN cannot be fetched.
             */
            final DatabaseEntry newData = new DatabaseEntry();
            newData.setData(new byte[1]);
            newData.setPartial(0, 1, true);
            result = cursor.put(null, newData, Put.CURRENT, null);
            assertNull(result);

            /*
             * An update of an expired record should work, however, when
             * reading the old LN is not required.
             */
            newData.setPartial(false);
            result = cursor.put(
                null, newData, Put.CURRENT,
                new WriteOptions().setUpdateTTL(true));
            assertNotNull(result);
        }

        cursor.close();
        txnCommit(txn);

        /*
         * Since we the TTL to zero in all records, expect they all exist.
         */
        txn = txnBegin();

        for (int i = 0; i < 5; i += 1) {
            keyEntry.setData(new byte[]{(byte) i});
            result = db.get(txn, keyEntry, dataEntry, Get.SEARCH, options);
            assertNotNull(result);
            assertEquals(1, dataEntry.getSize());
        }

        txnCommit(txn);

        holderCursor.close();
        db.close();
    }

    /**
     * Tests that disk space for purged slots is reclaimed by the cleaner.
     * All records in the test are small and should be embedded, so that LN
     * purging is factored out.
     */
    @Test
    public void testPurgedSlots() {

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final FileManager fileManager = envImpl.getFileManager();

        final String embedMaxSize = env.getConfig().getConfigParam(
            EnvironmentConfig.TREE_MAX_EMBEDDED_LN);

        if (Integer.parseInt(embedMaxSize) < 5) {
            System.out.println(
                "testPurgedSlots not run, embedded LN max size is too small");
            return;
        }

        db = openDb("foo");

        setFixedTimeHook(BASE_MS);

        /*
         * Write 5 files worth of records that expire in one hour, and other 5
         * files worth that expire in 2 hours. Each file is about half LNs that
         * are immediately obsolete and half BINs that are not. When we clean,
         * we'll reclaim all the LNs plus the expired BIN slots.
         */
        OperationResult result;

        final WriteOptions options = new WriteOptions();

        final long startFile = fileManager.getCurrentFileNum();

        /* Use large keys to fill files more quickly. */
        final DatabaseEntry keyEntry = new DatabaseEntry(new byte[500]);
        final DatabaseEntry dataEntry = new DatabaseEntry(new byte[5]);
        final DatabaseEntry tempEntry = new DatabaseEntry();

        for (int i = 0;; i += 1) {

            final long filesAdded =
                fileManager.getCurrentFileNum() - startFile;

            if (filesAdded >= 10) {
                break;
            }

            options.setTTL(filesAdded >= 5 ? 2 : 1, TimeUnit.HOURS);

            IntegerBinding.intToEntry(i, tempEntry);

            System.arraycopy(
                tempEntry.getData(), 0, keyEntry.getData(), 0,
                tempEntry.getSize());

            final Transaction txn = txnBegin();

            result = db.put(
                txn, keyEntry, dataEntry, Put.NO_OVERWRITE, options);

            assertNotNull(result);
            txnCommit(txn);
        }

        env.checkpoint(new CheckpointConfig().setForce(true));

        /*
         * Clean 3 times, advancing the clock an hour each time. To begin with
         * we have over 100GB of data. After each hour passes:
         *
         * 1. Nothing expires but we clean to below 50MB because LNs are all
         *    embedded and immediately obsolete.
         *
         * 2. Half the data expires and we clean to below 25MB.
         *
         * 3. The rest of the data expires and we clean to below 5MB.
         */
        final int[] expectMB = new int[] {50, 25, 5};
        for (int i = 0; i < 3; i += 1) {

            fixedSystemTime += TTL.MILLIS_PER_HOUR;

            /*
             * Add a couple more .jdb files so that the preceding .jdb files
             * are eligible for cleaning. The first checkpoint is needed to
             * advance the FirstActiveLSN, and the second to delete the cleaned
             * files.
             */
            envImpl.forceLogFileFlip();
            envImpl.forceLogFileFlip();

            envImpl.getCleaner().doClean(
                true /*cleanMultipleFiles*/, true /*forceCleaning*/);

            env.checkpoint(new CheckpointConfig().setForce(true));

            final long maxSize = expectMB[i] * 1024 * 1024L;

            final long actualSize = env.getStats(null).getTotalLogSize();

            final String msg = String.format(
                "actualSize=%,d maxSize=%,d", actualSize, maxSize);

            assertTrue(msg, actualSize < maxSize);
        }

        /*
         * After compressing, all data should be gone, leaving a single BIN.
         */
        env.compress();
        final BtreeStats dbStats = (BtreeStats) db.getStats(null);
        assertEquals(1, dbStats.getBottomInternalNodeCount());

        db.close();
    }

    /**
     * Sets a TTL.timeTestHook that provides a fixed time of initTime, meaning
     * that JE TTL processing will behave as if the system time is initTime.
     * Thereafter, changing fixedSystemTime will change the test time.
     *
     * In unit tests calling this method, add the following to tearDown:
     *  TTL.setTimeTestHook(null);
     *  fixedSystemTime = 0;
     */
    public static void setFixedTimeHook(final long initTime) {

        fixedSystemTime = initTime;

        TTL.setTimeTestHook(new TestHook<Long>() {

            @Override
            public Long getHookValue() {
                return fixedSystemTime;
            }

            @Override
            public void hookSetup() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void doIOHook() throws IOException {
                throw new UnsupportedOperationException();
            }
            @Override
            public void doHook() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void doHook(Long obj) {
                throw new UnsupportedOperationException();
            }
        });
    }

    /**
     * Checks that using TTL is not allowed in a replicated env with any node
     * having a version less than {@link TTL#MIN_JE_VERSION}.
     */
    @Test
    public void testTTLNotAvailable() {

        final boolean isRep = isReplicatedTest(getClass());
        db = openDb("foo");

        try {
            TTL.TEST_MIN_JE_VERSION = new JEVersion("10000.0.0");

            /* Allow put if no TTL is specified. */
            try {
                db.put(
                    null,
                    new DatabaseEntry(new byte[1]),
                    new DatabaseEntry(new byte[1]),
                    Put.OVERWRITE, null);
            } catch (IllegalStateException e) {
                fail();
            }

            /* Disallow put if a non-zero TTL is specified. */
            db.put(
                null,
                new DatabaseEntry(new byte[1]),
                new DatabaseEntry(new byte[1]),
                Put.OVERWRITE,
                new WriteOptions().setTTL(1));

            assertFalse(isRep);

        } catch (IllegalStateException e) {

            assertTrue(isRep);
            assertTrue(e.getMessage().contains("TTL"));

        } finally {
            TTL.TEST_MIN_JE_VERSION = null;
        }

        db.close();
    }
}
