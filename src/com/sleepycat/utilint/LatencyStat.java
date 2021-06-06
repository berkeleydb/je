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

package com.sleepycat.utilint;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A stat that keeps track of latency in milliseconds and presents average,
 * min, max, 95th and 99th percentile values.
 */
public class LatencyStat implements Cloneable {

    private static final long serialVersionUID = 1L;

    /*
     * The maximum tracked latency, in milliseconds, it's also the size of the
     * configurable array which is used to save latencies.
     */
    private final int maxTrackedLatencyMillis;

    private static class Values {

        /* The number of total operations that have been tracked. */
        final AtomicInteger numOps;

        /* The number of total requests that have been tracked. */
        final AtomicInteger numRequests;

        /* The number of total nanoseconds that have been tracked. */
        final AtomicLong totalNanos;

        /*
         * Array is indexed by latency in millis and elements contain the
         * number of ops for that latency.
         */
        final AtomicIntegerArray histogram;

        /*
         * Min and max latency. They may both exceed maxTrackedLatencyMillis.
         * A volatile int rather than an AtomicInteger is used because
         * AtomicInteger has no min() or max() method, so there is no advantage
         * to using it.
         */
        volatile int minIncludingOverflow;
        volatile int maxIncludingOverflow;

        /* Number of requests whose latency exceed maxTrackedLatencyMillis. */
        final AtomicInteger requestsOverflow;

        Values(final int maxTrackedLatencyMillis) {
            histogram = new AtomicIntegerArray(maxTrackedLatencyMillis);
            numOps = new AtomicInteger();
            numRequests = new AtomicInteger();
            requestsOverflow = new AtomicInteger();
            totalNanos = new AtomicLong();
            minIncludingOverflow = Integer.MAX_VALUE;
            maxIncludingOverflow = 0;
        }
    }

    /*
     * Contains the values tracked by set() and reported by calculate().
     *
     * To clear the values, this field is assigned a new instance.  This
     * prevents uninitialized values when set() and clear() run concurrently.
     * Methods that access the values (set and calculate) should assign
     * trackedValues to a local var and perform all access using the local var,
     * so that clear() will not impact the computation.
     *
     * Concurrent access by set() and calculate() is handled differently.  The
     * numOps and numRequests fields are incremented by set() last, and are
     * checked first by calculate().  If numOps or numRequests is zero,
     * calculate() will return an empty Latency object.  If numOps and
     * numRequests are non-zero, calculate() may still return latency values
     * that are inconsistent, when set() runs concurrently.  But at least
     * calculate() won't return uninitialized latency values.  Without
     * synchronizing set(), this is the best we can do.  Synchronizing set()
     * might introduce contention during CRUD operations.
     */
    private volatile Values trackedValues;

    private int saveMin;
    private int saveMax;
    private float saveAvg;
    private int saveNumOps;
    private int saveNumRequests;
    private int save95;
    private int save99;
    private int saveRequestsOverflow;

    public LatencyStat(long maxTrackedLatencyMillis) {
        this.maxTrackedLatencyMillis = (int) maxTrackedLatencyMillis;
        clear();
    }

    public void clear() {
        clearInternal();
    }

    /**
     * Returns and clears the current stats.
     */
    private synchronized Values clearInternal() {
        final Values values = trackedValues;

        /*
         * Create a new instance to support concurrent access.  See {@link
         * #trackedValues}.
         */
        trackedValues = new Values(maxTrackedLatencyMillis);

        return values;
    }

    /**
     * Generate the min, max, avg, 95th and 99th percentile for the collected
     * measurements. Do not clear the measurement collection.
     */
    public Latency calculate() {
        return calculate(false);
    }

    /**
     * Generate the min, max, avg, 95th and 99th percentile for the collected
     * measurements, then clear the measurement collection.
     */
    public Latency calculateAndClear() {
        return calculate(true);
    }

    /**
     * Calculate may be called on a stat that is concurrently updating, so
     * while it has to be thread safe, it's a bit inaccurate when there's
     * concurrent activity. That tradeoff is made in order to avoid the cost of
     * synchronization during the set() method.  See {@link #trackedValues}.
     */
    private synchronized Latency calculate(boolean clear) {

        /*
         * Use a local var to support concurrent access.  See {@link
         * #trackedValues}.
         */
        final Values values = clear ? clearInternal() : trackedValues;

        /*
         * Check numOps and numReqests first and return an empty Latency if
         * either one is zero.  This ensures that we don't report partially
         * computed values when they are zero.  This works because the other
         * values are calculated first by set(), and numOps and numRequests are
         * incremented last.
         */
        final int totalOps = values.numOps.get();
        final int totalRequests = values.numRequests.get();
        if (totalOps == 0 || totalRequests == 0) {
            return new Latency(maxTrackedLatencyMillis);
        }

        final long totalNanos = values.totalNanos.get();
        final int nOverflow = values.requestsOverflow.get();
        final int maxIncludingOverflow = values.maxIncludingOverflow;
        final int minIncludingOverflow = values.minIncludingOverflow;

        final float avgMs = (float) ((totalNanos * 1e-6) / totalRequests);

        /*
         * The 95% and 99% values will be -1 if there are no recorded latencies
         * in the histogram.
         */
        int percent95 = -1;
        int percent99 = -1;

        /*
         * Min/max can be inaccurate because of concurrent set() calls, i.e.,
         * values may be from a mixture of different set() calls.  Bound the
         * min/max to the average, so they are sensible.
         */
        final int avgMsInt = Math.round(avgMs);
        int max = Math.max(avgMsInt, maxIncludingOverflow);
        int min = Math.min(avgMsInt, minIncludingOverflow);

        final int percent95Count;
        final int percent99Count;
        final int nTrackedRequests = totalRequests - nOverflow;
        if (nTrackedRequests == 1) {
            /* For one request, always include it in the 95% and 99%. */
            percent95Count = 1;
            percent99Count = 1;
        } else {
            /* Otherwise truncate: never include the last/highest request. */
            percent95Count = (int) (nTrackedRequests * .95);
            percent99Count = (int) (nTrackedRequests * .99);
        }

        final int histogramLength = values.histogram.length();
        int numRequestsSeen = 0;
        for (int latency = 0; latency < histogramLength; latency++) {

            final int count = values.histogram.get(latency);

            if (count == 0) {
                continue;
            }

            if (min > latency) {
                min = latency;
            }

            if (max < latency) {
                max = latency;
            }

            if (numRequestsSeen < percent95Count) {
                percent95 = latency;
            }

            if (numRequestsSeen < percent99Count) {
                percent99 = latency;
            }

            numRequestsSeen += count;
        }

        saveMax = max;
        saveMin = min;
        saveAvg = avgMs;
        saveNumOps = totalOps;
        saveNumRequests = totalRequests;
        save95 = percent95;
        save99 = percent99;
        saveRequestsOverflow = nOverflow;

        return new Latency(maxTrackedLatencyMillis, saveMin, saveMax, saveAvg,
                           saveNumOps, saveNumRequests, save95, save99,
                           saveRequestsOverflow);
    }

    /**
     * Record a single operation that took place in a request of "nanolatency"
     * nanos.
     */
    public void set(long nanoLatency) {
        set(1, nanoLatency);
    }

    /**
     * Record "numRecordedOps" (one or more) operations that took place in a
     * single request of "nanoLatency" nanos.
     */
    public void set(int numRecordedOps, long nanoLatency) {

        /* ignore negative values [#22466] */
        if (nanoLatency < 0) {
            return;
        }

        /*
         * Use a local var to support concurrent access.  See {@link
         * #trackedValues}.
         */
        final Values values = trackedValues;

        /* Round the latency to determine where to mark the histogram. */
        final int millisRounded =
            (int) ((nanoLatency + (1000000l / 2)) / 1000000l);

        /* Record this latency. */
        if (millisRounded >= maxTrackedLatencyMillis) {
            values.requestsOverflow.incrementAndGet();
        } else {
            values.histogram.incrementAndGet(millisRounded);
        }

        /*
         * Update the min/max latency if necessary.  This is not atomic, so we
         * loop to account for lost updates.
         */
        while (values.maxIncludingOverflow < millisRounded) {
            values.maxIncludingOverflow = millisRounded;
        }
        while (values.minIncludingOverflow > millisRounded) {
            values.minIncludingOverflow = millisRounded;
        }

        /*
         * Keep a count of latency that is precise enough to record sub
         * millisecond values.
         */
        values.totalNanos.addAndGet(nanoLatency);

        /*
         * Increment numOps and numRequests last so that calculate() won't use
         * other uninitialized values when numOps or numRequests is zero.
         */
        values.numOps.addAndGet(numRecordedOps);
        values.numRequests.incrementAndGet();
    }

    public boolean isEmpty() {
        return (trackedValues.numOps.get() == 0) ||
               (trackedValues.numRequests.get() == 0);
    }

    @Override
    public String toString() {
        final Latency results =
            new Latency(maxTrackedLatencyMillis, saveMin, saveMax, saveAvg,
                        saveNumRequests, saveNumOps, save95, save99,
                        saveRequestsOverflow);
        return results.toString();
    }
}
