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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.util.TestUtils;

/**
 * Measure the amount of actual (RSS) memory used by the off-heap allocator.
 *
 * Usage:
 *   java com.sleepycat.je.evictor.MeasureOffHeapMemory \
 *    number-of-blocks
 *    min-block-size
 *    max-block-size
 *    number-of-threads
 *    create-new-threads (true or false)
 *
 * Allocates a given number of blocks. Then the loops forever freeing and
 * allocating every other block. Each malloc uses a random size between the
 * given min/max sizes (if equal, the size is fixed).
 *
 * Blocks allocated by one thread are freed by another, to mimic JE off-heap
 * cache usage. Half the blocks are allocated at the beginning and never freed,
 * to mimic hot data that stays in cache.
 *
 * If create-new-threads is true, each time number-of-blocks is allocated, all
 * threads are discarded and new threads are created. This is a worst case
 * scenario for malloc's per-thread allocation pools, which can cause behavior
 * that looks like a memory leak when blocks outlive the thread that allocated
 * them.
 *
 * The Java heap must be large enough to hold an array of block pointers, each
 * of which uses 8 bytes, so 8 * number-of-blocks.
 *
 * Example run to use around 220 GB:
 *  java -Xmx4g -cp test.jar com.sleepycat.je.evictor.MeasureOffHeapMemory \
 *    400000000 40 1040 4 false
 *
 * Output includes the estimated space used by off-heap blocks (JE's cache
 * usage statistic) and the actual memory usage calculated by subtracting the
 * initial RSS from the current RSS. Ops/s is also included, where one op is
 * an alloc and a free; however, each op is just an alloc during ramp-up.
 *
 * It runs forever, with output every number-of-blocks ops. To stop it, kill
 * the process.
 */
public class MeasureOffHeapMemory {

    private static final DateFormat DATE_FORMAT =
        new SimpleDateFormat("MM-dd HH:mm:ss.SSS");
    private static final Date DATE = new Date();

    public static void main(final String[] args) {
        try {
            new MeasureOffHeapMemory(args).runTest();
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private final long[] ids;
    private final int minSize;
    private final int range;
    private final int nThreads;
    private final int blocksPerThread;
    private final OffHeapAllocator allocator;
    private final String[] psCommand;
    private final Random rnd;
    private final boolean createNewThreads;
    private CyclicBarrier barrier;

    private MeasureOffHeapMemory(final String[] args) throws Exception {

        ids = new long[Integer.parseInt(args[0])];
        minSize = Integer.parseInt(args[1]);
        final int maxSize = Integer.parseInt(args[2]);
        createNewThreads = Boolean.parseBoolean(args[4]);
        range = maxSize - minSize;
        nThreads = Integer.parseInt(args[3]);
        blocksPerThread = ids.length / nThreads;

        if (ids.length % nThreads != 0) {
            throw new IllegalArgumentException(
                "Number of blocks not evenly divisible by number of threads");
        }

        final OffHeapAllocatorFactory factory = new OffHeapAllocatorFactory();
        allocator = factory.getDefaultAllocator();

        psCommand = new String[] { "ps", "o", "rss=,vsz=", "" + getPid() };
        rnd = new Random(123);
    }

    private void runTest() throws Exception {

        /* Causes the initial RSS size to include the heap size. */
        fillTheHeap();

        final long[] startRssAndVsz = new long[2];
        final long[] endRssAndVsz = new long[2];
        getRssAndVsz(startRssAndVsz);

        System.out.format(
            "Initial RSS: %,d VSZ: %,d\n",
            startRssAndVsz[0], startRssAndVsz[1]);

        final AtomicLong startTime = new AtomicLong(0L);
        final AtomicBoolean rampUp = new AtomicBoolean(true);

        final Runner[] threads = new Runner[nThreads];

        barrier = new CyclicBarrier(nThreads, new Runnable() {
            @Override
            public void run() {
                try {
                    final long endTime = System.currentTimeMillis();
                    getRssAndVsz(endRssAndVsz);

                    final long rate =
                        (ids.length * (1000L / 2)) /
                        (endTime - startTime.get());

                    System.out.format(
                        "%s Estimate: %,d RSS: %,d VSZ: %,d Ops/s: %,d %s\n",
                        getDate(endTime),
                        allocator.getUsedBytes(),
                        endRssAndVsz[0] - startRssAndVsz[0],
                        endRssAndVsz[1] - startRssAndVsz[1],
                        rate,
                        rampUp.get() ? "(ramp-up)" : "");

                    rampUp.set(false);

                    if (createNewThreads) {
                        barrier.reset();
                        startTime.set(System.currentTimeMillis());

                        for (int i = 0; i < nThreads; i += 1) {
                            threads[i] = new Runner(
                                threads[i].getRangeNumber(), false /*doInit*/);
                            threads[i].start();
                        }
                    } else {
                        for (int i = 0; i < nThreads; i += 1) {
                            threads[i].bumpRange();
                        }
                        startTime.set(System.currentTimeMillis());
                        barrier.reset();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        });

        startTime.set(System.currentTimeMillis());

        for (int i = 0; i < nThreads; i += 1) {
            threads[i] = new Runner(i, true /*doInit*/);
            threads[i].start();
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    private static synchronized String getDate(long time) {
        DATE.setTime(time);
        return DATE_FORMAT.format(DATE);
    }

    class Runner extends Thread {

        final boolean doInit;
        int rangeNum;
        int startBlock;
        int endBlock;

        Runner(int num, boolean doInit) {
            this.doInit = doInit;
            rangeNum = num;
            startBlock = rangeNum * blocksPerThread;
            endBlock = startBlock + blocksPerThread;
        }

        int getRangeNumber() {
            return rangeNum;
        }

        void bumpRange() {

            rangeNum += 1;

            if (rangeNum == nThreads) {
                rangeNum = 0;
            }

            startBlock = rangeNum * blocksPerThread;
            endBlock = startBlock + blocksPerThread;
        }

        @Override
        public void run() {

            try {
                if (doInit) {
                    for (int i = startBlock; i < endBlock; i += 1) {

                        final int size =
                            (range == 0) ? minSize : (minSize + rnd.nextInt(range));


                        try {
                            ids[i] = allocator.allocate(size);
                        } catch (Throwable e) {
                            System.err.println("Unable to allocate block " + i);
                            throw e;
                        }
                    }

                    try {
                        barrier.await();
                    } catch (BrokenBarrierException ignore) {
                    }

                    if (createNewThreads) {
                        return;
                    }
                }

                while (true) {

                    for (int i = startBlock; i < endBlock; i += 2) {

                        allocator.free(ids[i]);

                        final int size = (range == 0) ?
                            minSize : (minSize + rnd.nextInt(range));

                        try {
                            ids[i] = allocator.allocate(size);
                        } catch (Throwable e) {
                            System.err.println("Unable to allocate block " + i);
                            throw e;
                        }
                    }

                    try {
                        barrier.await();
                    } catch (BrokenBarrierException ignore) {
                    }

                    if (createNewThreads) {
                        return;
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void fillTheHeap() {
        Throwable e = null;
        while (true) {
            try {
                e = new Throwable(e);
            } catch (OutOfMemoryError e1) {
                return;
            }
        }
    }

    private void getRssAndVsz(long[] rssAndVsz) throws Exception {

        final ProcessBuilder pBuilder = new ProcessBuilder(psCommand);
        final Process process = pBuilder.start();

        final BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream()));

        String result = "";
        for (String line = reader.readLine(); line != null;
             line = reader.readLine()) {
            result += line;
        }

        result = result.trim();
        final int sep = result.indexOf(" ");
        final String rss = result.substring(0, sep);
        final String vsz = result.substring(sep + 1);

        try {
            rssAndVsz[0] = Long.parseLong(rss) * 1024;
            rssAndVsz[1] = Long.parseLong(vsz) * 1024;
        } catch (NumberFormatException e) {
            throw new RuntimeException(result, e);
        }
    }

    private static int getPid() throws Exception {

        final int pid = TestUtils.getPid(MeasureOffHeapMemory.class.getName());

        if (pid < 0) {
            throw new RuntimeException("Couldn't get my PID");
        }

        return pid;
    }
}
