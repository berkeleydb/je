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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Maintain interval and cumulative stats for a given set of operations, as
 * well as a activityCounter that generates thread dumps if operations take too
 * long. The markStart and markFinish methods can be used to bracket each
 * tracked operation.
 */
public class StatsTracker<T> {

    /* Latency stats. */
    private final Map<T, LatencyStat> intervalLatencies;
    private final Map<T, LatencyStat> cumulativeLatencies;

    /*
     * ActivityCounter tracks throughput and dumps thread stacktraces when
     * throughput drops.
     */
    private final ActivityCounter activityCounter;

    /**
     * The logger is used for activity stack traces.
     */
    public StatsTracker(T[] opTypes,
                        Logger stackTraceLogger,
                        int activeThreadThreshold,
                        long threadDumpIntervalMillis,
                        int threadDumpMax,
                        int maxTrackedLatencyMillis) {

        this.intervalLatencies = new HashMap<T, LatencyStat>();
        this.cumulativeLatencies = new HashMap<T, LatencyStat>();

        for (T opType : opTypes) {
            intervalLatencies.put
                (opType, new LatencyStat(maxTrackedLatencyMillis));
            cumulativeLatencies.put
                (opType, new LatencyStat(maxTrackedLatencyMillis));
        }

        activityCounter = new ActivityCounter(activeThreadThreshold,
                                              threadDumpIntervalMillis,
                                              threadDumpMax, 
                                              stackTraceLogger);
    }

    /** 
     * Track the start of a operation.
     * @return the value of System.nanoTime, for passing to markFinish.
     */
    public long markStart() {
        activityCounter.start();
        return System.nanoTime();
    }

    /**
     * Track the end of an operation.
     * @param startTime should be the value returned by the corresponding call
     * to markStart
     */
    public void markFinish(T opType, long startTime) {
        markFinish(opType, startTime, 1);
    }
    /**
     * Track the end of an operation.
     * @param startTime should be the value returned by the corresponding call
     * to markStart
     */
    public void markFinish(T opType, long startTime, int numOperations) {
        try {
            if (numOperations == 0) {
                return;
            }

            if (opType != null) {
                long elapsed = System.nanoTime() - startTime;
                intervalLatencies.get(opType).set(numOperations, elapsed);
                cumulativeLatencies.get(opType).set(numOperations, elapsed);
            }
        } finally {
            /* Must be invoked to clear the ActivityCounter stats. */
            activityCounter.finish();
        }
    }

    /**
     * Should be called after each interval latency stat collection, to reset
     * for the next period's collection.
     */
    public void clearLatency() {
        for (Map.Entry<T, LatencyStat> e : intervalLatencies.entrySet()) {
            e.getValue().clear();
        }
    }

    public Map<T, LatencyStat> getIntervalLatency() {
        return intervalLatencies;
    }

    public Map<T, LatencyStat> getCumulativeLatency() {
        return cumulativeLatencies;
    }

    /**
     * For unit test support.
     */
    public int getNumCompletedDumps() {
        return activityCounter.getNumCompletedDumps();
    }
}
