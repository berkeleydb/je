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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.Key;

/**
 * Tracks the expired bytes in each time window, i.e., a histogram. A separate
 * ExpirationTracker instance is used for each tracked data file.
 * <p>
 * A copy-on-write approach is used to store a file number-to-counter mapping,
 * and AtomicIntegers are used for the counters. This avoids blocking when
 * tracking information for the current end-of-log file. That way, the
 * end-of-log tracker can be used by multiple threads without holding a global
 * mutex. This tracker is maintained by the LogManager and a new tracker is
 * created for each file, and then flushed to disk when starting a new file as
 * a FileExpirationLN.
 * <p>
 * An ExpirationTracker instance is used to track expired data when performing
 * the first pass of two pass cleaning, although in that case it is only used
 * by one thread, so the optimizations are irrelevant.
 * <p>
 * The {@link #serialize}} method is called to represent the histogram in a
 * single byte array. This array is the record "data" in a FileExpirationLN.
 * It is also stored in memory, in the UtilizationProfile, and used during
 * cleaning to calculate the number of expired bytes per file.
 */
public class ExpirationTracker {

    private final long fileNum;

    /* Copy-on-write map of expiration time (in hours) to byte counter. */
    private volatile Map<Integer, AtomicInteger> map = new HashMap<>();

    /**
     * We wait for pendingTrackCalls to go to zero before flushing the
     * tracker to its database.
     */
    private AtomicInteger pendingTrackCalls = new AtomicInteger(0);

    public ExpirationTracker(final long fileNum) {
        this.fileNum = fileNum;
    }

    public long getFileNum() {
        return fileNum;
    }

    /**
     * Tracks expiration of a BIN or LN.
     *
     * @param entry is the LogEntry that was just logged. INs and LNs will be
     * processed here, and must be protected by their parent latch.
     *
     * @param size byte size of logged entry.
     */
    public void track(final LogEntry entry, final int size) {

        pendingTrackCalls.decrementAndGet();

        final LogEntryType type = entry.getLogType();

        if (type.isUserLNType()) {

            final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
            final int expiration = lnEntry.getExpiration();

            if (expiration == 0) {
                return;
            }

            track(expiration, lnEntry.isExpirationInHours(), size);
            return;
        }

        if (!type.equals(LogEntryType.LOG_BIN) &&
            !type.equals(LogEntryType.LOG_BIN_DELTA)){
            return;
        }

        final INLogEntry<?> inEntry = (INLogEntry<?>) entry;
        final BIN bin = inEntry.getBINWithExpiration();

        if (bin == null) {
            return;
        }

        final boolean inHours = bin.isExpirationInHours();
        final int entrySize = size / bin.getNEntries();

        for (int i = 0; i < bin.getNEntries(); i += 1) {

            final int expiration = bin.getExpiration(i);

            if (expiration == 0) {
                continue;
            }

            track(expiration, inHours, entrySize);
        }
    }

    /**
     * Adds a single expiration value.
     */
    private void track(int expiration,
                       final boolean expirationInHours,
                       final int size) {

        final Integer expInHours =
            expirationInHours ? expiration : (24 * expiration);

        AtomicInteger counter = map.get(expInHours);

        /*
         * The map is modified only while synchronized, which prevents two
         * threads from adding the same entry or a reader thread from accessing
         * the map while it is being modified. To guarantee this we must
         * "install" the new map in the volatile field only after adding the
         * new counter.
         */
        if (counter == null) {
            synchronized (this) {
                /*
                 * Check again while synchronized, since another thread may
                 * have added it. This "double check" is safe because the 'map'
                 * field is volatile.
                 */
                counter = map.get(expInHours);
                if (counter == null) {
                    final Map<Integer, AtomicInteger> newMap =
                        new HashMap<>(map);
                    counter = new AtomicInteger(0);
                    newMap.put(expInHours, counter);
                    map = newMap;
                }
            }
        }

        counter.addAndGet(size);
    }

    /**
     * Increment the number of calls to {@link #track(int, boolean, int)}
     * that must be made before the tracked data can be flushed to its
     * database.
     */
    public void incrementPendingTrackCalls() {
        pendingTrackCalls.incrementAndGet();
    }

    /**
     * Returns whether to wait for outstanding calls to {@link
     * #track(int, boolean, int)} before flushing the tracked data to its
     * database.
     */
    boolean hasPendingTrackCalls() {
        return pendingTrackCalls.get() > 0;
    }

    /**
     * Computes the current expired bytes for the given time.
     */
    public int getExpiredBytes(final long time) {

        final int expLimit = (int) (time / TTL.MILLIS_PER_HOUR);

        int expiredSize = 0;

        for (final Map.Entry<Integer, AtomicInteger> entry : map.entrySet()) {
            final int exp = entry.getKey();
            if (exp > expLimit) {
                continue;
            }
            expiredSize += entry.getValue().get();
        }

        return expiredSize;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append("{ExpTracker file= ").append(fileNum);

        for (final Map.Entry<Integer, AtomicInteger> entry :
            new TreeMap<>(map).entrySet()) {

            final int exp = entry.getKey();
            sb.append(' ').append(TTL.formatExpiration(exp, true));
            sb.append('=').append(entry.getValue().get());
        }

        sb.append('}');
        return sb.toString();
    }

    public static String toString(final byte[] serializedForm) {

        final StringBuilder sb = new StringBuilder();
        sb.append("{ExpSerialized");

        final TupleInput in = new TupleInput(
            serializedForm, 1, serializedForm.length - 1);

        final boolean hours = isExpirationInHours(serializedForm);
        int prevExp = 0;

        while (in.available() > 0) {
            final int exp = in.readPackedInt() + prevExp;
            final int size = in.readPackedInt();
            sb.append(' ').append(TTL.formatExpiration(exp, hours));
            sb.append('=').append(size);
            prevExp = exp;
        }

        sb.append('}');
        return sb.toString();
    }

    /**
     * Computes the expired bytes for the given serialized histogram and
     * expiration time.
     */
    static int getExpiredBytes(final byte[] serializedForm,
                               final int dayLimit,
                               final int hourLimit) {
        final int expLimit =
            ExpirationTracker.isExpirationInHours(serializedForm) ?
            hourLimit : dayLimit;

        final TupleInput in = new TupleInput(
            serializedForm, 1, serializedForm.length - 1);

        int expiredSize = 0;
        int prevExp = 0;

        while (in.available() > 0) {
            final int exp = in.readPackedInt() + prevExp;
            if (exp > expLimit) {
                break;
            }
            expiredSize += in.readPackedInt();
            prevExp = exp;
        }

        return expiredSize;
    }

    /**
     * Converts this object to a serialized form that is compact and can be
     * used to quickly find the total bytes after a given time. Returns an
     * empty array if no data in this file has an expiration time.
     *
     * The serialized form is a series of {interval,byteSize} pairs that is
     * ordered by expiration time and run length encoded. The interval and
     * byteSize are packed integers. The interval is the delta between the
     * current and previous expiration value. All expiration values are in days
     * if all values are on a day boundary; otherwise they are in hours. Days
     * are used, when possible, to reduce the size of the delta, using less
     * space due to the packed integer format.
     */
    byte[] serialize() {

        final Map<Integer, AtomicInteger> myMap = map;

        if (myMap.isEmpty()) {
            return Key.EMPTY_KEY;
        }

        final List<Integer> expList = new ArrayList<>(myMap.size());
        expList.addAll(myMap.keySet());
        Collections.sort(expList);

        boolean hours = false;
        for (int exp : expList) {
            if (exp % 24 != 0) {
                hours = true;
                break;
            }
        }

        final TupleOutput out = new TupleOutput();
        out.write(hours ? 1 : 0);
        int prevExp = 0;

        for (int exp : expList) {
            final AtomicInteger counter = myMap.get(exp);
            if (!hours) {
                exp /= 24;
            }
            out.writePackedInt(exp - prevExp);
            out.writePackedInt(counter.get());
            prevExp = exp;
        }

        return out.toByteArray();
    }

    /**
     * Returns whether the given serialized form has expired values in hours.
     * If false is returned, all values expired on day boundaries.
     */
    static boolean isExpirationInHours(final byte[] serialized) {
        return (serialized[0] == 1);
    }
}
