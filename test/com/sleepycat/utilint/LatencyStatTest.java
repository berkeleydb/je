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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

public class LatencyStatTest {

    private static final int STRESS_SECONDS = 20;
    private static final double DELTA = 1e-15;

    private volatile LatencyStat stressStat = new LatencyStat(100);
    private AtomicLong stressExpectOps = new AtomicLong();
    private AtomicLong stressActualOps = new AtomicLong();
    private AtomicLong stressExpectReq = new AtomicLong();
    private AtomicLong stressActualReq = new AtomicLong();
    private final Random globalRnd = new Random(123);

    @Test
    public void testMillisLatency() {
        LatencyStat interval = new LatencyStat(100);
        LatencyStat accumulate = new LatencyStat(100);

        long totalTime = 0;
        for (int i = 0; i <= 11; i++) {
            totalTime += (i * 10 * 1000000);
            interval.set(i * 10 * 1000000);
            accumulate.set(i * 10 * 1000000);
        }
    
        Latency results = interval.calculateAndClear();
        checkResults(results, 12, 12, 0, 110, 55.0f, 80, 80, 2);
        results = accumulate.calculate();
        checkResults(results, 12, 12, 0, 110, 55.0f, 80, 80, 2);

        for (int i = 0; i < 20; i++) {
            totalTime += 92000000;
            interval.set(92000000);
            accumulate.set(92000000);
        }
        
        checkResults(interval.calculateAndClear(),
                     20, 20, 92, 92, 92.0f, 92, 92, 0);
        checkResults(accumulate.calculate(),
                     32, 32, 0, 110, 78.125f, 92, 92, 2);

        interval.clear();
        accumulate.clear();

        for (int i = 0; i < 100; i++) {
            interval.set(i * 1000000);
            accumulate.set(i * 1000000);
        }
        checkResults(interval.calculateAndClear(),
                     100, 100, 0, 99, 49.5f, 94, 98, 0);
        checkResults(accumulate.calculate(),
                     100, 100, 0, 99, 49.5f, 94, 98, 0);
        
    }

    @Test
    public void testNanoLatency() {
        LatencyStat interval = new LatencyStat(100);
        LatencyStat accumulate = new LatencyStat(100);

        long totalTime = 0;
        for (int i = 0; i <= 11; i++) {
            totalTime += (i * 10000); 
            interval.set(i * 10000);
            accumulate.set(i * 10000);
        }
    
        checkResults(interval.calculateAndClear(),
                     12, 12, 0, 0, .055f, 0, 0, 0);
        checkResults(accumulate.calculate(),
                     12, 12, 0, 0, .055f, 0, 0, 0);

        long time2 = 0;
        for (int i = 1; i <= 10; i++) {
            time2 += (i * 1000000) + 500000;
            totalTime += (i * 1000000) + 500000;
            interval.set((i * 1000000) + 500000);
            accumulate.set((i * 1000000) + 500000);
        }
        checkResults(interval.calculateAndClear(), 
                     10, 10, 2, 11, 6.0f, 10, 10, 0);
        checkResults(accumulate.calculate(), 
                     22, 22, 0, 11, 2.7572727f, 9, 10, 0);
    }

    /** Test Latency rollup.  */
    @Test
    public void testRollup() {
        LatencyStat stat1 = new LatencyStat(100);
        LatencyStat stat2 = new LatencyStat(100);

        for (int i = 0; i <= 11; i++) {
            stat1.set(i * 10 * 1000000);
            stat2.set(5, i * 20 * 1000000);
        }

        Latency result1 = stat1.calculate();
        checkResults(result1, 12, 12, 0, 110, 55, 80, 80, 2);
        Latency result2 = stat2.calculate();
        checkResults(result2, 12, 60, 0, 220, 110, 60, 60, 7);

        /* 95th and 99th become 0 because they are not preserved by rollup. */
        result1.rollup(result2);
        checkResults(result1, 24, 72, 0, 220, 82.5f, 0, 0, 9);
    }

    /**
     * When there is only one op, the 95% and 99% numbers should be the latency
     * for that op, not -1. [#21763]
     *
     * For other small numbers of ops, only the highest value is not included
     * in the 95% and 99% values.
     */
    @Test
    public void testSmallNumberOfOps() {
        final LatencyStat stat = new LatencyStat(100);

        stat.set(6900000);
        checkResults(stat.calculateAndClear(),
                     1, 1, 7, 7, 6.9f, 7, 7, 0);

        stat.set(7 * 1000000);
        checkResults(stat.calculate(),
                     1, 1, 7, 7, 7, 7, 7, 0);

        stat.set(8 * 1000000);
        checkResults(stat.calculate(),
                     2, 2, 7, 8, 7.5f, 7, 7, 0);

        stat.set(9 * 1000000);
        checkResults(stat.calculate(),
                     3, 3, 7, 9, 8, 8, 8, 0);
    }

    /**
     * Tests LatencyStat.set when passing numRecordedOps GT 1.
     */
    @Test
    public void testMultiOps() {
        final LatencyStat stat = new LatencyStat(100);

        /* Basic check of a single request. */
        stat.set(10, 3 * 1000000);
        checkResults(stat.calculateAndClear(),
                     1, 10, 3, 3, 3f, 3, 3, 0);

        /* Two requests, no overflow  */
        stat.set(5, 1 * 1000000);
        stat.set(10, 3 * 1000000);
        checkResults(stat.calculateAndClear(),
                     2, 15, 1, 3, 2f, 1, 1, 0);

        /* Three requests, one overflow  */
        stat.set(5, 3 * 1000000);
        stat.set(10, 16 * 1000000);
        stat.set(10, 101 * 1000000);
        checkResults(stat.calculateAndClear(),
                     3, 25, 3, 101, 40f, 3, 3, 1);

        /* Three requests, all overflows. */
        stat.set(5, 101 * 1000000);
        stat.set(5, 102 * 1000000);
        stat.set(5, 103 * 1000000);
        checkResults(stat.calculateAndClear(),
                     3, 15, 101, 103, 102, -1, -1, 3);

        /*
         * Check that when the very highest recorded latency is high, and the
         * rest (95% and 99%) are low, we don't report the high value.  Prior
         * to a bug fix, the high value was reported. In particular, before the
         * bug fix both checks below reported 77 for the 95% and 99% values,
         * but 7 is the correct value.  [#21763]
         */
        for (int i = 0; i < 100; i += 1) {
            stat.set(10, 7 * 1000000);
        }
        stat.set(20, 1 * 77 * 1000000);
        checkResults(stat.calculateAndClear(),
                     101, 1020, 7, 77, 7.6930695f, 7, 7, 0);
    }

    private void checkResults(Latency results, 
                              int expectedReq, 
                              int expectedOps, 
                              int expectedMin,
                              int expectedMax,
                              float expectedAvg,
                              int expected95,
                              int expected99,
                              int reqOverflow) {
        assertEquals(expectedReq, results.getTotalRequests());
        assertEquals(expectedOps, results.getTotalOps());
        assertEquals(expectedMin, results.getMin());
        assertEquals(expectedMax, results.getMax());
        assertEquals(expectedAvg, results.getAvg(), DELTA);
        assertEquals(expected95, results.get95thPercent());
        assertEquals(expected99, results.get99thPercent());
        assertEquals(reqOverflow, results.getRequestsOverflow());
    }

    /**
     * Checks that when set(), calculate() and calculateAndClear() are run
     * concurrently, we see reasonable values returned by calculate().
     */
    @Test
    public void testConcurrentSetCalculateClear()
        throws Throwable {

        /* Zero counters. */
        stressStat.clear();
        stressExpectOps.set(0);
        stressActualOps.set(0);
        stressExpectReq.set(0);
        stressActualReq.set(0);

        final long endTime = System.currentTimeMillis() +
            (STRESS_SECONDS * 1000);

        /* Do the test. */
        exec(endTime,
             new DoSet(), new DoSet(), new DoSet(), new DoSet(),
             new DoSet(true), new DoSet(true), new DoSet(true),
             new DoCalc(), new DoCalc(), new DoCalc(true));

        /* Count the very last interval. */
        final Latency latency = stressStat.calculateAndClear();
        stressActualOps.addAndGet(latency.getTotalOps());
        stressActualReq.addAndGet(latency.getTotalRequests());

        final String msg = String.format
            ("expectOps=%,d actualOps=%,d expectReq=%,d actualReq=%,d",
             stressExpectOps.get(), stressActualOps.get(),
             stressExpectReq.get(), stressActualReq.get());

        /* Expect LT 0.1% missed ops/requests due to concurrent changes. */
        final double missedOps = stressExpectOps.get() - stressActualOps.get();
        final double missedReq = stressExpectReq.get() - stressActualReq.get();
        assertTrue(msg, missedOps >= 0);
        assertTrue(msg, missedReq >= 0);
        assertTrue(msg, (missedOps / stressExpectOps.get()) < 0.01);
        assertTrue(msg, (missedReq / stressExpectReq.get()) < 0.01);

        //System.out.println(missedOps / stressExpectOps.get());
        //System.out.println(missedReq / stressExpectReq.get());
    }

    class DoSet implements Runnable {
        private final boolean multi;
        private final Random rnd = new Random(globalRnd.nextInt());

        DoSet() {
            this(false);
        }

        DoSet(final boolean multi) {
            this.multi = multi;
        }

        public void run() {
            final int nanos = (rnd.nextInt(99) + 1) * 1000000;
            final int nOps = multi ? (rnd.nextInt(10) + 1) : 1;
            stressStat.set(nOps, nanos);
            stressExpectOps.addAndGet(nOps);
            stressExpectReq.addAndGet(1);
        }
    }

    class DoCalc implements Runnable {
        private final boolean clear;

        DoCalc() {
            this(false);
        }

        DoCalc(final boolean clear) {
            this.clear = clear;
        }

        public void run() {
            final Latency latency = clear ?
                stressStat.calculateAndClear() :
                stressStat.calculate();
            if (latency.getTotalOps() == 0) {
                return;
            }
            if (clear) {
                stressActualOps.addAndGet(latency.getTotalOps());
                stressActualReq.addAndGet(latency.getTotalRequests());
            }
            assertTrue(latency.toString(),
                       latency.get95thPercent() >= 0);
            assertTrue(latency.toString(),
                       latency.get95thPercent() >= 0);
            assertTrue(latency.toString(),
                       latency.getMin() >= 0);
            assertTrue(latency.toString(),
                       latency.getMin() != Integer.MAX_VALUE);
            assertTrue(latency.toString(),
                       latency.getMin() <= Math.round(latency.getAvg()));
            assertTrue(latency.toString(),
                       latency.getMin() <= latency.get95thPercent());
            assertTrue(latency.toString(),
                       latency.getMin() <= latency.get99thPercent());
            assertTrue(latency.toString(),
                       latency.getMax() >= latency.getMin());
            assertTrue(latency.toString(),
                       latency.getMax() >= Math.round(latency.getAvg()));
            assertTrue(latency.toString(),
                       latency.getMax() >= latency.get95thPercent());
            assertTrue(latency.toString(),
                       latency.getMax() >= latency.get99thPercent());
            assertTrue(latency.toString(),
                       latency.getAvg() > 0);
            assertTrue(latency.toString(),
                       latency.getRequestsOverflow() == 0);
        }
    }

    private static void exec(final long endTime, final Runnable... tasks)
        throws Throwable {

        final int nThreads = tasks.length;
        final Thread[] threads = new Thread[nThreads];
        final CountDownLatch startSignal = new CountDownLatch(nThreads);

        final AtomicReference<Throwable> firstEx =
            new AtomicReference<Throwable>(null);

        for (int i = 0; i < nThreads; i += 1) {
            final Runnable task = tasks[i];
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        startSignal.countDown();
                        startSignal.await();
                        while (System.currentTimeMillis() < endTime) {
                            task.run();
                        }
                    } catch (Throwable e) {
                        firstEx.compareAndSet(null, e);
                    }
                }
            };
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        if (firstEx.get() != null) {
            throw firstEx.get();
        }
    }

    /**
     * Checks that when a Latency object previously serialized with JE 5.0.69
     * is deserialized here, the totalRequests field (added in JE 5.0.70) is
     * initialized to the totalOps.  The latency-5-0-69 file in this package
     * was created using the WriteLatencyObject program that exists (only) in
     * JE 5.0.69, also in this package. [#21763]
     */
    @Test
    public void testNewTotalRequestsField()
        throws Exception {

        final InputStream is =
            getClass().getResourceAsStream("latency-5-0-69");
        assertNotNull(is);

        final ObjectInputStream ois = new ObjectInputStream(is);
        final Latency l = (Latency) ois.readObject();

        assertEquals(100, l.getMaxTrackedLatencyMillis());
        assertEquals(1, l.getMin());
        assertEquals(10, l.getMax());
        assertEquals(1.1f, l.getAvg(), DELTA);
        assertEquals(500, l.getTotalOps());
        assertEquals(2, l.get95thPercent());
        assertEquals(3, l.get99thPercent());
        assertEquals(4, l.getRequestsOverflow());

        assertEquals(500, l.getTotalRequests());
    }
}

