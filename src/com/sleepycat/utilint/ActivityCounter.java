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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Tracks the number of operations begun, as a way of measuring level of
 * activity. Is capable of displaying thread dumps if the activity level
 * reaches a specified ceiling
 */
public class ActivityCounter {

    private final AtomicInteger activeCount;
    private final AtomicBoolean threadDumpInProgress;
    private volatile long lastThreadDumpTime;
    private volatile int numCompletedDumps;
    private final int activeThreshold;
    private final int  maxNumDumps;
    private final AtomicInteger maxActivity;

    /*
     * Thread dumps can only happen this many milliseconds apart, to avoid
     * overwhelming the system.
     */
    private final long requiredIntervalMillis;

    private final Logger logger;

    private final ExecutorService dumper;

    public ActivityCounter(int activeThreshold,
                           long requiredIntervalMillis,
                           int maxNumDumps,
                           Logger logger) {

        activeCount = new AtomicInteger(0);
        threadDumpInProgress = new AtomicBoolean(false);
        maxActivity = new AtomicInteger(0);

        this.activeThreshold = activeThreshold;
        this.requiredIntervalMillis = requiredIntervalMillis;
        this.maxNumDumps = maxNumDumps;
        this.logger = logger;

        dumper = Executors.newSingleThreadExecutor();
    }

    /* An operation has started. */
    public void start() {
        int numActive = activeCount.incrementAndGet();
        int max = maxActivity.get();
        if (numActive > max) {
            maxActivity.compareAndSet(max, numActive);
        }
        check(numActive);
    }

    /* An operation has finished. */
    public void finish() {
        activeCount.decrementAndGet();
    }

    public int getAndClearMaxActivity() {
        return maxActivity.getAndSet(0);
    }

    private boolean intervalIsTooShort() {
        /* Don't do a thread dump if the last dump was too recent */
        long interval = System.currentTimeMillis() - lastThreadDumpTime;
        return interval < requiredIntervalMillis;
    }

    /**
     * If the activity level is above a threshold, there is no other thread
     * that is dumping now, and a dump hasn't happened for a while, dump
     * thread stack traces.
     */
    private void check(int numActive) {

        /* Activity is low, no need to do any dumps. */
        if (numActive <= activeThreshold) {
            return;
        }

        if (numCompletedDumps >= maxNumDumps) {
            return;
        }

        /* Don't do a thread dump if the last dump was too recent */
        if (intervalIsTooShort()) {
            return;
        }

        /* There's one in progress. */
        if (threadDumpInProgress.get()) {
            return;
        }

        /*
         * Let's do a dump. The ExecutorServices guarantees that all activity
         * executes in a single thread, so further serialization and
         * synchronization is handled there.
         */
        dumper.execute(new GetStackTraces());
    }

    /**
     * For unit test support.
     */
    public int getNumCompletedDumps() {
        return numCompletedDumps;
    }

    private class GetStackTraces implements Runnable {

        public void run() {

            if (intervalIsTooShort()) {
                return;
            }

            if (!threadDumpInProgress.compareAndSet(false, true)) {
                logger.warning("Unexpected: ActivityCounter stack trace " +
                               "dumper saw threadDumpInProgress flag set.");
                return;
            }

            try {
                lastThreadDumpTime = System.currentTimeMillis();
                dumpThreads();
                numCompletedDumps++;
            } finally {
                boolean reset = threadDumpInProgress.compareAndSet(true, false);
                assert reset : "ThreadDump should have been in progress";
            }
        }

        private void dumpThreads() {

            int whichDump = numCompletedDumps;

            logger.info("[Dump " + whichDump +
                        " --Dumping stack traces for all threads]");

            Map<Thread, StackTraceElement[]> stackTraces =
                Thread.getAllStackTraces();

            for (Map.Entry<Thread, StackTraceElement[]> stme :
                     stackTraces.entrySet()) {
                if (stme.getKey() == Thread.currentThread()) {
                    continue;
                }
                logger.info(stme.getKey().toString());
                for (StackTraceElement ste : stme.getValue()) {
                    logger.info("     " + ste);
                }
            }

            logger.info("[Dump " + whichDump + " --Thread dump completed]");
        }
    }
}

