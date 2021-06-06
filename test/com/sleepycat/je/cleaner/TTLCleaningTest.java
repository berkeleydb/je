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

package com.sleepycat.je.cleaner;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.Put;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.test.TTLTest;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import junit.framework.Assert;

/**
 * Tests purging/cleaning of expired data.
 */
public class TTLCleaningTest extends TestBase {

    private final File envHome;
    private Environment env;
    private Database db;
    private EnvironmentImpl envImpl;

    public TTLCleaningTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        TTL.setTimeTestHook(null);
        if (env != null) {
            try {
                env.close();
            } finally {
                env = null;
            }
        }
    }

    private EnvironmentConfig createEnvConfig() {

        final EnvironmentConfig envConfig = new EnvironmentConfig();

        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);

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

        return envConfig;
    }

    private void open() {
        open(createEnvConfig());
    }

    private void open(final EnvironmentConfig envConfig) {

        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        final DatabaseConfig dbConfig = new DatabaseConfig();

        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void close() {
        db.close();
        env.close();
    }

    /**
     * Check histogram with all TTLs in days, increasing then decreasing TTLs.
     */
    @Test
    public void testHistogram1() {

        final int[] ttls = new int[20];
        final TimeUnit units[] = new TimeUnit[20];

        for (int i = 0; i < 10; i += 1) {
            ttls[i] = i * 2;
            units[i] = TimeUnit.DAYS;
        }

        for (int i = 10; i < 20; i += 1) {
            ttls[i] = (20 - i) * 3;
            units[i] = TimeUnit.DAYS;
        }

        checkHistogram(ttls, units);
    }

    /**
     * Check histogram with all TTLs in hours, decreasing then increasing TTLs.
     */
    @Test
    public void testHistogram2() {

        final int[] ttls = new int[20];
        final TimeUnit units[] = new TimeUnit[20];

        for (int i = 0; i < 10; i += 1) {
            ttls[i] = (10 - i) * 3;
            units[i] = TimeUnit.HOURS;
        }

        for (int i = 10; i < 20; i += 1) {
            ttls[i] = i * 2;
            units[i] = TimeUnit.HOURS;
        }

        checkHistogram(ttls, units);
    }

    /**
     * Check histogram with a mixture of days and hours.
     */
    @Test
    public void testHistogram3() {

        final int[] ttls = new int[20];
        final TimeUnit units[] = new TimeUnit[20];

        for (int i = 0; i < 20; i += 1) {

            final int ttl;
            final TimeUnit unit;
            if (i % 3 == 0) {
                ttl = i * 3;
                unit = TimeUnit.HOURS;
            } else {
                ttl = i;
                unit = TimeUnit.DAYS;
            }

            ttls[i] = ttl;
            units[i] = unit;
        }

        checkHistogram(ttls, units);
    }

    /**
     * Writes some data for each TTL given, in the order of the array, and
     * confirms that the histogram is correct.
     */
    private void checkHistogram(final int[] ttls, final TimeUnit units[]) {

        TTLTest.setFixedTimeHook(TTL.MILLIS_PER_HOUR);

        open();

        final ExpirationProfile profile = envImpl.getExpirationProfile();
        final FileManager fileManager = envImpl.getFileManager();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        /*
         * Before writing and measuring the change in LSN, we must ensure an
         * insertion will not write the first BIN or cause a split. Do a put
         * (with no TTL) to write the first BIN, and assert that we won't
         * insert more records that will fit in one BIN.
         */
        assertTrue(ttls.length < 120);
        IntegerBinding.intToEntry(Integer.MAX_VALUE, key);
        data.setData(new byte[0]);
        db.put(null, key, data, Put.OVERWRITE, null);

        /*
         * Write a record for each specified TTL, measuring its size. Keep
         * track of the max expiration time and whether any units are in hours.
         */
        final WriteOptions options = new WriteOptions().setUpdateTTL(true);
        final int[] sizes = new int[ttls.length];
        long maxExpireTime = 0;
        boolean anyHours = false;

        for (int i = 0; i < ttls.length; i += 1) {

            options.setTTL(ttls[i], units[i]);

            IntegerBinding.intToEntry(i, key);
            data.setData(new byte[20 + i]); // Don't want embedded LNs.

            final Transaction txn = env.beginTransaction(null, null);

            final long offset0 = DbLsn.getFileOffset(fileManager.getNextLsn());
            db.put(txn, key, data, Put.OVERWRITE, options);
            final long offset1 = DbLsn.getFileOffset(fileManager.getNextLsn());

            txn.commit();

            sizes[i] = (int) (offset1 - offset0); // size of LN alone

            maxExpireTime = Math.max(
                maxExpireTime, getExpireTime(ttls[i], units[i]));

            if (units[i] == TimeUnit.HOURS) {
                anyHours = true;
            }
        }

        /* Everything should be in one file.
        assertEquals(0, fileManager.getCurrentFileNum());

        /* Moving to a new file will update the expiration profile. */
        envImpl.forceLogFileFlip();

        /*
         * Use the cleaner to explicitly count expiration, to simulate what
         * happens during two pass cleaning.
         */
        final FileProcessor processor = envImpl.getCleaner().createProcessor();
        final ExpirationTracker tracker = processor.countExpiration(0);

        /*
         * The interval that data expires depends on whether any TTL units are
         * in hours.
         */
        final long ttlInterval = anyHours ?
            TimeUnit.HOURS.toMillis(1) : TimeUnit.DAYS.toMillis(1);

        /*
         * Check 5 minute intervals, in time order, starting with the current
         * time and going a couple TTL intervals past the last expiration time.
         */
        final long maxCheckTime = maxExpireTime + (2 * ttlInterval);
        final long startCheckTime = TTL.currentSystemTime();
        final long checkInterval = TimeUnit.MINUTES.toMillis(5);

        for (long checkTime = startCheckTime;
             checkTime <= maxCheckTime;
             checkTime += checkInterval) {

            /*
             * Check that our estimated expiration bytes agrees with the
             * expiration profile, and with the expiration tracker.
             */
            profile.refresh(checkTime);
            final int profileBytes = profile.getExpiredBytes(0 /*file*/);
            final int trackerBytes = tracker.getExpiredBytes(checkTime);

            final int expiredBytes =
                getExpiredBytes(ttls, units, sizes, checkTime);

            assertEquals(expiredBytes, profileBytes);
            assertEquals(expiredBytes, trackerBytes);

            /*
             * Check that ExpirationProfile.getExpiredBytes(file, time) returns
             * gradually expired values for the current period.
             */
            if (expiredBytes == 0) {
                continue;
            }

            final long intervalStartTime =
                checkTime - (checkTime % ttlInterval);

            final long prevIntervalStart = intervalStartTime - ttlInterval;

            final int prevExpiredBytes = (prevIntervalStart < startCheckTime) ?
                0 : tracker.getExpiredBytes(prevIntervalStart);

            final int newlyExpiredBytes = expiredBytes - prevExpiredBytes;
            final long newMs = checkTime - intervalStartTime;

            final long expectGradualBytes = prevExpiredBytes +
                ((newlyExpiredBytes * newMs) / ttlInterval);

            final int gradualBytes = profile.getExpiredBytes(
                0 /*file*/, checkTime).second();

            assertEquals(expectGradualBytes, gradualBytes);
        }

        close();
    }

    /**
     * Returns the first millisecond on which bytes with the given TTL will
     * expire.
     */
    private long getExpireTime(final int ttl, final TimeUnit unit) {

        if (ttl == 0) {
            return 0;
        }

        final long interval = unit.toMillis(1);
        long time = TTL.currentSystemTime() + (ttl * interval);

        /* Round up to nearest interval. */
        final long remainder = time % interval;
        if (remainder != 0) {
            time = time - remainder + interval;
        }

        /* Check that TTL methods agree. */
        assertEquals(
            time,
            TTL.expirationToSystemTime(
                TTL.ttlToExpiration(ttl, unit),
                unit == TimeUnit.HOURS));

        return time;
    }

    /**
     * Returns the number of bytes that expire on or after the given system
     * time.
     */
    private int getExpiredBytes(final int[] ttls,
                                final TimeUnit units[],
                                final int[] sizes,
                                final long sysTime) {
        int bytes = 0;
        for (int i = 0; i < ttls.length; i += 1) {
            final long expireTime = getExpireTime(ttls[i], units[i]);
            if (expireTime != 0 && sysTime >= expireTime) {
                bytes += sizes[i];
            }
        }
        return bytes;
    }

    /**
     * Checks that cleaning occurs when LNs expire.
     *
     * Also checks that no two-pass or revisal runs occur, because this test
     * does not make LNs obsolete, so there is little overlap between expired
     * and obsolete data.
     */
    @Test
    public void testCleanLNs() {

        /*
         * Use start time near the end of the hour period. If the beginning of
         * the period were used, when two hours passes and all data expires at
         * once, only a small portion would be considered expired due to
         * gradual expiration.
         */
        TTLTest.setFixedTimeHook(TTL.MILLIS_PER_HOUR - 100);

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(10 * 1024 * 1024));

        open(envConfig);

        /*
         * Write 10 files, using 1024 byte records so there are no embedded
         * LNs, and giving all records a TTL.
         */
        writeFiles(10, 4, 1024, 1);

        /* Almost no cleaning occurs before data expires. */
        int nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned <= 1);

        /* Advance time and most files are cleaned. */
        TTLTest.fixedSystemTime += 2 * TTL.MILLIS_PER_HOUR;
        nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned >= 6);

        /* No overlap, so no two-pass or revisal runs. */
        final EnvironmentStats stats = env.getStats(null);
        assertEquals(0, stats.getNCleanerTwoPassRuns());
        assertEquals(0, stats.getNCleanerRevisalRuns());
        assertEquals(nFilesCleaned, stats.getNCleanerRuns());

        close();
    }

    /**
     * Checks that cleaning occurs when BIN slots expire, using embedded LNs to
     * remove LN expiration from the picture.
     *
     * We don't check for revisal or two-pass runs here because they are
     * unlikely but difficult to predict. They are unlikely because with
     * embedded LNs, normal cleaning, due to BINs made obsolete by checkpoints
     * and splits, will often clean expired data as well.
     */
    @Test
    public void testCleanBINs() {

        /* Use start time near the end of the hour period. See testCleanLNs. */
        TTLTest.setFixedTimeHook(TTL.MILLIS_PER_HOUR - 100);

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(10 * 1024 * 1024));

        open(envConfig);

        if (DbInternal.getNonNullEnvImpl(env).getMaxEmbeddedLN() < 16) {
            System.out.println(
                "testCleanBasicINs not applicable when embedded LNs " +
                "are disabled");
            close();
            return;
        }

        /*
         * Write 20 files, using 16 byte records to create embedded LNs, and
         * giving all records a TTL.
         */
        writeFiles(20, 4, 16, 1);

        /* Clean obsolete LNs, etc, to start with non-expired data. */
        env.cleanLog();
        env.checkpoint(new CheckpointConfig().setForce(true));

        /* Almost no further cleaning occurs before data expires. */
        int nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned <= 1);

        /* Advance time and several files are cleaned. */
        TTLTest.fixedSystemTime += 2 * TTL.MILLIS_PER_HOUR;
        nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned >= 2);

        final EnvironmentStats stats = env.getStats(null);
        assertTrue(stats.getNCleanerTwoPassRuns() <= stats.getNCleanerRuns());

        close();
    }

    /**
     * Checks that two-pass cleaning is used when:
     *  + expired and obsolete data partially overlap, and
     *  + max utilization per-file is over the two-pass threshold, but
     *  + the true utilization of the files is below the threshold.
     *
     * @see EnvironmentParams#CLEANER_TWO_PASS_GAP
     * @see EnvironmentParams#CLEANER_TWO_PASS_THRESHOLD
     */
    @Test
    public void testTwoPassCleaning() {

        /* Use start time near the end of the hour period. See testCleanLNs. */
        TTLTest.setFixedTimeHook(TTL.MILLIS_PER_HOUR - 100);

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(10 * 1024 * 1024));

        open(envConfig);

        /* Write 10 files, giving 1/2 records a TTL. */
        writeFiles(10, 4, 1024, 2);

        /* Almost no cleaning occurs before data expires. */
        int nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned <= 1);

        /*
         * Delete 1/3 records explicitly, causing an expired/obsolete overlap,
         * but leaving the per-file max utilization fairly high.
         */
        deleteRecords(3);

        /* Advance time and most files are cleaned. */
        TTLTest.fixedSystemTime += 2 * TTL.MILLIS_PER_HOUR;
        nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned >= 6);

        /* All runs are two-pass runs. */
        final EnvironmentStats stats = env.getStats(null);
        Assert.assertTrue(stats.getNCleanerTwoPassRuns() >= 6);
        assertEquals(stats.getNCleanerRuns(), stats.getNCleanerTwoPassRuns());
        assertEquals(0, stats.getNCleanerRevisalRuns());
        assertTrue(stats.getNCleanerTwoPassRuns() <= stats.getNCleanerRuns());

        close();
    }

    /**
     * Checks that a revisal run (not true cleaning) is used when:
     *  + expired and obsolete data mostly overlap, and
     *  + max utilization per-file is over the two-pass threshold, and
     *  + the true utilization of the files is also above the threshold.
     *
     * @see EnvironmentParams#CLEANER_TWO_PASS_GAP
     * @see EnvironmentParams#CLEANER_TWO_PASS_THRESHOLD
     */
    @Test
    public void testRevisalCleaning() {

        /* Use start time near the end of the hour period. See testCleanLNs. */
        TTLTest.setFixedTimeHook(TTL.MILLIS_PER_HOUR - 100);

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(10 * 1024 * 1024));

        open(envConfig);

        /* Write 10 files, giving 1/2 records a TTL. */
        writeFiles(10, 4, 1024, 2);

        /* Almost no cleaning occurs before data expires. */
        int nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned <= 1);

        /*
         * Delete 1/2 records explicitly, causing an expired/obsolete overlap,
         * but leaving the per-file max utilization fairly high.
         */
        deleteRecords(2);

        /* Advance time and most files are cleaned. */
        TTLTest.fixedSystemTime += 2 * TTL.MILLIS_PER_HOUR;
        nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned >= 6);

        /* Most runs are revisal runs. */
        EnvironmentStats stats = env.getStats(null);
        assertTrue(stats.getNCleanerRevisalRuns() >= 6);
        assertTrue(stats.getNCleanerTwoPassRuns() <= stats.getNCleanerRuns());

        /*
         * Now check that the same files can be cleaned again, if they become
         * eligible.
         */
        env.getStats(new StatsConfig().setClear(true));
        deleteRecords(1);
        nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned >= 6);
        stats = env.getStats(null);
        assertTrue(stats.getNCleanerRuns() >= 6);
        assertEquals(0, stats.getNCleanerRevisalRuns());

        close();
    }

    /**
     * Checks that cleaning is gradual rather than spiking on expiration time
     * boundaries. Hours is used here but the same test applies to days and the
     * histogram tests ensure that data expires gradually for days and hours.
     *
     * Cleaning will not itself occur gradually in this test,
     * unfortunately, because the data in all files expires at the same
     * time, so once utilization drops below 50 all files are cleaned.
     * This is unlike the real world, where data will expire at different
     * times. Therefore, we test gradual cleaning by looking at the
     * predictedMinUtilization, which is used to drive cleaning. This should
     * decrease gradually.
     */
    @Test
    public void testGradualExpiration() {

        /*
         * Use start time on an hour boundary because we want to test gradual
         * expiration from the start of the time period to the end.
         */
        final long startTime = TTL.MILLIS_PER_DAY;
        TTLTest.setFixedTimeHook(startTime);

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(10 * 1024 * 1024));

        open(envConfig);

        /* Write 10 files, giving all records a TTL. */
        writeFiles(10, 4, 1024, 1);

        /* Almost no cleaning occurs before data expires. */
        int nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned <= 1);

        final UtilizationCalculator calculator =
            envImpl.getCleaner().getUtilizationCalculator();

        /*
         * Collect predictedMinUtilization values as we bump the time by one
         * minute and attempt cleaning.
         */
        final long checkStartTime = startTime + TimeUnit.HOURS.toMillis(1);
        final long checkEndTime = checkStartTime + TimeUnit.HOURS.toMillis(1);
        final long checkInterval = TimeUnit.MINUTES.toMillis(1);
        final Deque<Integer> predictedUtils = new LinkedList<>();

        for (long time = checkStartTime;
             time <= checkEndTime;
             time += checkInterval) {

            TTLTest.fixedSystemTime = time;
            env.cleanLog();

            predictedUtils.add(calculator.getPredictedMinUtilization());
        }

        /*
         * Expect a wide spread of predictedMinUtilization values that are
         * decreasing and fairly evenly spaced.
         */
        final int firstUtil = predictedUtils.getFirst();
        final int lastUtil = predictedUtils.getLast();

        assertTrue(
            "firstUtil=" + firstUtil + " lastUtil=" + lastUtil,
            firstUtil > 90 && lastUtil < 10);

        int prevUtil = predictedUtils.removeFirst();

        for (final int util : predictedUtils) {

            final int decrease = prevUtil - util;

            assertTrue(
                "util=" + util + " prevUtil=" + prevUtil,
                decrease >= 0 && decrease < 5);

            prevUtil = util;
        }

        close();
    }

    @Test
    public void testExpirationDisabled() {

        /* Use start time near the end of the hour period. See testCleanLNs. */
        TTLTest.setFixedTimeHook(TTL.MILLIS_PER_HOUR - 100);

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(10 * 1024 * 1024));

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_EXPIRATION_ENABLED, "false");

        open(envConfig);

        /* Write 10 files, giving all records a TTL. */
        writeFiles(10, 4, 1024, 1);
        final long nRecords = db.count();

        /* Almost no cleaning occurs before data expires. */
        int nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned <= 1);

        /*
         * Advance time, but no files are cleaned because expiration is
         * disabled, and all data is still readable.
         */
        TTLTest.fixedSystemTime += 2 * TTL.MILLIS_PER_HOUR;
        nFilesCleaned = env.cleanLog();
        assertEquals(0, nFilesCleaned);

        long count = 0;

        try (final Cursor cursor = db.openCursor(null, null)) {

            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            while (cursor.get(key, data, Get.NEXT, null) != null) {
                count += 1;
            }
        }

        assertEquals(nRecords, count);

        /* Enable expiration and expect cleaning and filtering of all data. */
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_EXPIRATION_ENABLED, "true");

        env.setMutableConfig(envConfig);

        nFilesCleaned = env.cleanLog();
        assertTrue(String.valueOf(nFilesCleaned), nFilesCleaned >= 6);

        try (final Cursor cursor = db.openCursor(null, null)) {
            if (cursor.get(null, null, Get.NEXT, null) != null) {
                fail();
            }
        }

        assertEquals(0, db.count());

        close();
    }

    @Test
    public void testProfileRecovery() {

        final EnvironmentConfig envConfig = createEnvConfig();

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(1000000));

        open(envConfig);

        /* Write 10 files, giving all records a TTL. */
        writeFiles(10, 4, 1024, 1);

        close();

        final AtomicBoolean phaseSeen = new AtomicBoolean(false);
        final AtomicLong numSeen = new AtomicLong(0);

        envConfig.setRecoveryProgressListener(
            new ProgressListener<RecoveryProgress>() {
                @Override
                public boolean progress(
                    RecoveryProgress phase,
                    long n,
                    long total) {

                    if (phase ==
                        RecoveryProgress.POPULATE_EXPIRATION_PROFILE) {

                        phaseSeen.set(true);
                        numSeen.incrementAndGet();
                    }

                    return true;
                }
            }
        );

        open(envConfig);

        assertTrue(phaseSeen.get());
        assertEquals(1, numSeen.get());

        close();
    }

    /**
     * Inserts records with the given size until the given number of files are
     * added. Every expireNth record is assigned a 1 hour TTL.
     */
    private void writeFiles(final int nFiles,
                            final int keySize,
                            final int dataSize,
                            final int expireNth) {

        assert keySize >= 4;

        final byte[] keyBytes = new byte[keySize];
        final TupleOutput keyOut = new TupleOutput(keyBytes);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[dataSize]);
        final WriteOptions options = new WriteOptions();

        final FileManager fileManager = envImpl.getFileManager();
        final long prevLastFile = fileManager.getCurrentFileNum();

        for (int i = 0;; i += 1) {

            if (fileManager.getCurrentFileNum() - prevLastFile >= nFiles) {
                break;
            }

            keyOut.reset();
            keyOut.writeInt(i);
            key.setData(keyBytes);

            options.setTTL(
                (i % expireNth == 0) ? 1 : 0,
                TimeUnit.HOURS);

            db.put(null, key, data, Put.OVERWRITE, options);
        }
    }

    /**
     * Deletes every deleteNth record.
     */
    private void deleteRecords(final int deleteNth) {

        final Transaction txn = env.beginTransaction(null, null);

        try (final Cursor cursor = db.openCursor(txn, null)) {

            int counter = 0;

            while (cursor.get(null, null, Get.NEXT, null) != null) {
                if (counter % deleteNth == 0) {
                    cursor.delete(null);
                }
                counter += 1;
            }
        }

        txn.commit();
    }
}
