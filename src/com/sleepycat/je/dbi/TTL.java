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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.TestHook;

/**
 * Internal documentation and utility functions for the TTL feature.
 *
 * Repeatable-read
 * -----------------
 * As described in {@link WriteOptions#setTTL}, repeatable-read is supported
 * in simple cases by treating a record that expires after being locked as if
 * it were not expired. This is implemented and documented in {@link
 * CursorImpl#lockLN}.
 *
 * Unfortunately, we must check for whether a lock is already owned or shared
 * by the locker before we attempt to lock the record. To optimize and avoid
 * this extra overhead when it is unnecessary, we only do this when a record
 * might expire during the transaction, according to the {@link
 * EnvironmentParams#ENV_TTL_MAX_TXN_TIME} threshold.
 *
 * When a slot contains an expired record, {@link CursorImpl#lockLN} returns
 * true in the LockStanding.defunct field, just as it does for deleted records.
 * That way deleted records and expired records are filtered out of queries in
 * the same way.
 *
 * Locking (read or write locks) also protects a record from being purged. The
 * cleaner only considers an LN expired if its lock is uncontended, meaning
 * that it could write-lock it. It places locked LNs on the pending LN queue.
 * The compressor also only removes an expired slot if its lock is uncontended.
 *
 * However, if the clock was changed, purging may have occurred. Therefore,
 * when an LN being fetched is in a cleaned file (LOG_FILE_NOT_FOUND), we treat
 * it as a deleted record if it expires within {@link
 * EnvironmentParams#ENV_TTL_CLOCK_TOLERANCE}. Records for which {@link
 * IN#fetchLN} returns null must also be filtered out of queries. This can
 * happen even after locking the record and determining that the slot is not
 * expired.
 *
 * To prevent an LN from being purged while an operation is attempting to lock
 * it, due to thread scheduling, we purge LNs only if they are already expired
 * by at least {@link EnvironmentParams#ENV_TTL_MAX_TXN_TIME}. This is done to
 * compensate for the fact that the BIN is latched by the cleaner when locking
 * an expired LN, while all other LN locking does latch the BIN. This also
 * means that, when calculating utilization of a .jdb file, we don't consider
 * LNs expired until the ENV_TTL_MAX_TXN_TIME after their expiration time.
 *
 * There are several special cases involving LNs discovered to be purged after
 * locking the record. In the cases where the operation fails, the situation
 * is documented in {@link WriteOptions#setTTL}.
 *
 *  + For a read operation with a non-null 'data' param, if the LN was
 *    previously locked but the data was not requested, and the LN is found to
 *    be purged during the read, the operation fails (returns null).
 *
 *  + For an update operation with a partial 'data' param, if the LN was
 *    previously locked (but the data was not requested), and the LN is found
 *    to be purged during the update, the operation fails (returns null).
 *
 *  + For an update of a primary record with secondary keys, if the record is
 *    locked and then we find the LN has been purged, we simply don't delete
 *    any pre-existing secondary keys. This is OK because those secondary
 *    records are also expired and will be purged naturally.
 *
 * Note that when the expiration time is reduced, including setting it to zero,
 * no special handling is needed. The update operation itself will ensure that
 * the expiration time in the BIN and LN are in sync, in the case of a single
 * record, and that a primary record and its associated and secondary records
 * have expiration times that are in sync. Since expiration checking always
 * occurs after locking, the updated expiration time will always be used.
 *
 * Secondaries
 * -----------
 * Locking also supports repeatable-read for secondaries, as long as the
 * records being accessed were locked. To make this work when reading via a
 * secondary, we must lock the secondary if it expires within
 * {@link EnvironmentParams#ENV_TTL_MAX_TXN_TIME}. Normally we don't lock the
 * secondary at all in this case, and rely only on the primary record lock.
 * This extra lock is taken after the primary lock, so locking order it not
 * violated, i.e., this does not increase the potential for deadlocks.
 *
 * When reading via a secondary, if the secondary exists but the primary record
 * expired (within {@link EnvironmentParams#ENV_TTL_CLOCK_TOLERANCE}), then we
 * we treat the record as deleted.
 *
 * When updating or deleting a primary record and its associated secondary
 * records, we ignore integrity problems if the secondary record has expired
 * (within {@link EnvironmentParams#ENV_TTL_CLOCK_TOLERANCE}). Specifically
 * we ignore the integrity error when: 1. we are deleting the secondary record
 * and it does not exist; 2. we are updating secondary record and it does not
 * exist -- in this case we insert it.
 */
public class TTL {

    public static final long MILLIS_PER_HOUR = 1000L * 60 * 60;
    public static final long MILLIS_PER_DAY = MILLIS_PER_HOUR * 24;

    /* Minimum JE version required for using TTL. */
    private static final JEVersion MIN_JE_VERSION = new JEVersion("6.5.0");

    /* Set by tests to override MIN_JE_VERSION. */
    public static JEVersion TEST_MIN_JE_VERSION = null;

    private static TestHook<Long> timeTestHook = null;

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final SimpleDateFormat TIME_FORMAT =
        new SimpleDateFormat("yyyy-MM-dd.HH");

    static {
        TIME_FORMAT.setTimeZone(UTC);
    }

    public static JEVersion getMinJEVersion() {
        if (TEST_MIN_JE_VERSION != null) {
            return TEST_MIN_JE_VERSION;
        }
        return MIN_JE_VERSION;
    }

    /**
     * Sets a hook for simulating changes in the clock time that is used in TTL
     * processing.
     *
     * If the hook is non-null, {@link TestHook#getHookValue()} returns the
     * value used as the system clock time for all TTL processing. Other
     * methods in the hook interface are not used.
     * <p>
     * For unit testing, this might return a fixed time. For stress testing,
     * this might return a time that advances more quickly than the real clock.
     */
    public static void setTimeTestHook(TestHook<Long> hook) {
        timeTestHook = hook;
    }

    public static long currentSystemTime() {

        if (timeTestHook != null) {
            return timeTestHook.getHookValue();
        }

        return System.currentTimeMillis();
    }

    /**
     * Translates from expiration days or hours to a Java time in ms.
     */
    public static long expirationToSystemTime(final int expiration,
                                              final boolean hours) {
        assert expiration >= 0;

        if (expiration == 0) {
            return 0;
        }

        return expiration * (hours ? MILLIS_PER_HOUR : MILLIS_PER_DAY);
    }

    /**
     * Translates from the user-supplied ttl parameters to the expiration value
     * that we store internally. Validates the ttl parameters as a side effect.
     */
    public static int ttlToExpiration(final int ttl, final TimeUnit ttlUnits) {

        if (ttl < 0) {
            throw new IllegalArgumentException("Illegal ttl value: " + ttl);
        }

        if (ttl == 0) {
            return 0;
        }

        final int currentTime;

        if (ttlUnits == TimeUnit.DAYS) {

            currentTime = (int)
                ((currentSystemTime() + MILLIS_PER_DAY - 1) /
                    MILLIS_PER_DAY);

        } else if (ttlUnits == TimeUnit.HOURS) {

            currentTime = (int)
                ((currentSystemTime() + MILLIS_PER_HOUR - 1) /
                    MILLIS_PER_HOUR);

        } else {

            throw new IllegalArgumentException(
                "ttlUnits not allowed: " + ttlUnits);
        }

        return currentTime + ttl;
    }

    /**
     * Returns whether the given time in millis, when converted to hours,
     * rounding up, is not an even multiple of 24.
     */
    public static boolean isSystemTimeInHours(final long systemMs) {

        final long hours = (systemMs + MILLIS_PER_HOUR - 1) / MILLIS_PER_HOUR;

        return hours % 24 != 0;
    }

    /**
     * Converts the user-supplied expirationTime parameter to an internal
     * expiration time in days or hours. Assumes that the user parameter is
     * evenly divisible by days or hours (call isSystemTimeInHours first).
     */
    public static int systemTimeToExpiration(final long systemMs,
                                             final boolean hours) {
        return (int) (hours ?
            ((systemMs + MILLIS_PER_HOUR - 1) / MILLIS_PER_HOUR) :
            ((systemMs + MILLIS_PER_DAY - 1) / MILLIS_PER_DAY));
    }

    /** For logging and debugging output. */
    public static String formatExpiration(final int expiration,
                                          final boolean hours) {

        return formatExpirationTime(expirationToSystemTime(expiration, hours));
    }

    /** For logging and debugging output. */
    public static String formatExpirationTime(final long time) {

        final Date date = new Date(time);

        synchronized (TIME_FORMAT) {
            return TIME_FORMAT.format(date);
        }
    }

    /**
     * Returns whether a given expiration time precedes the current system
     * time, i.e., the expiration time has passed.
     */
    public static boolean isExpired(final int expiration,
                                    final boolean hours) {
        return expiration != 0 &&
            currentSystemTime() >
                expirationToSystemTime(expiration, hours);
    }

    /**
     * Returns whether a given expiration time precedes the current system
     * time, i.e., the expiration time has passed.
     */
    public static boolean isExpired(final long expirationTime) {
        return expirationTime != 0 &&
            currentSystemTime() > expirationTime;
    }

    /**
     * Returns whether the given expiration time is LT the current system time
     * plus withinMs. withinMs may be negative to check whether the expiration
     * time is LT the current system time minus abs(withinMs).
     */
    public static boolean expiresWithin(final int expiration,
                                        final boolean hours,
                                        final long withinMs) {
        return expiration != 0 &&
            currentSystemTime() + withinMs >
                expirationToSystemTime(expiration, hours);
    }

    /**
     * Same as {@link #expiresWithin(int, boolean, long)} but with a single
     * expirationTime param.
     */
    public static boolean expiresWithin(final long expirationTime,
                                        final long withinMs) {
        return expirationTime != 0 &&
            currentSystemTime() + withinMs > expirationTime;
    }
}
