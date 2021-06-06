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

package com.sleepycat.je.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.util.test.TestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Checks the DbCacheSize returns consistent results by comparing the
 * calculated and measured values.  If this test fails, it probably means the
 * technique used by DbCacheSize for estimating or measuring has become
 * outdated or incorrect.  Or, it could indicate a bug in memory budget
 * calculations or IN memory management.  Try running DbCacheSize manually to
 * debug, using the cmd string for the test that failed.
 */
@RunWith(Parameterized.class)
public class DbCacheSizeTest extends TestBase {

    private static final boolean VERBOSE = false;

    /*
     * It is acceptable for the measured values to be somewhat different than
     * the calculated values, due to differences in actual BIN density, for
     * example.
     */
    private static final double ERROR_ALLOWED = 0.08;

    static final String[] COMMANDS = {
        // 0
        "-records 100000 -key 10 -data 100",
        // 1
        "-records 100000 -key 10 -data 100 -orderedinsertion",
        // 2
        "-records 100000 -key 10 -data 100 -duplicates",
        // 3
        "-records 100000 -key 10 -data 100 -duplicates " +
        "-orderedinsertion",
        // 4
        "-records 100000 -key 10 -data 100 -nodemax 250",
        // 5
        "-records 100000 -key 10 -data 100 -nodemax 250 " +
        "-orderedinsertion",
        // 6
        "-records 100000 -key 20 -data 100 -keyprefix 10",
        // 7
        "-records 100000 -key 20 -data 100 -keyprefix 2 " +
        "-je.tree.compactMaxKeyLength 19",
        // 8
        "-records 100000 -key 10 -data 100 -replicated",
        // 9
        "-records 100000 -key 10 -data 100 " +
        "-replicated -je.rep.preserveRecordVersion true",
        // 10
        "-records 100000 -key 10 -data 100 -duplicates " +
        "-replicated -je.rep.preserveRecordVersion true",
        // 11
        "-records 100000 -key 10 -data 100 -orderedinsertion " +
        "-replicated -je.rep.preserveRecordVersion true",
        // 12
        "-records 100000 -key 10 -data 100 -ttl",
        // 13
        "-records 10000 -key 10 -data 20 " +
        "-offheap -maincache 9000000",
        // 14
        "-records 100000 -key 10 -data 100 " +
        "-offheap -maincache 9000000",
        // 15
        "-records 150000 -key 10 -data 100 " +
        "-offheap -maincache 9000000",
        // 16
        "-records 150000 -key 10 -data 100 -duplicates " +
        "-offheap -maincache 9000000",
        // 17
        "-records 10000 -key 10 -data 100 -duplicates " +
        "-offheap -maincache 9000000",
        // 18
        "-records 10000 -key 10 -data 20 " +
        "-offheap",
        // 19
        "-records 100000 -key 10 -data 100 " +
        "-offheap",
        // 20
        "-records 150000 -key 10 -data 100 " +
        "-offheap",
        // 21
        "-records 150000 -key 10 -data 100 -duplicates " +
        "-offheap",
        // 22
        "-records 10000 -key 10 -data 100 -duplicates " +
        "-offheap",
    };

    /*
     * We always use a large file size so that the LSN compact representation
     * is not used.  This representation is usually not effective for larger
     * data sets, and is disabled by DbCacheSize except under certain
     * conditions.  In this test we use smallish data sets, so we use a large
     * file size to ensure that the compact representation is not used.
     */
    private int ONE_GB = 1024 * 1024 * 1024;
    private final String ADD_COMMANDS =
        "-measure -btreeinfo -je.log.fileMax " + ONE_GB;

    private String cmd;
    private int testNum;

    @Parameters
    public static List<Object[]> genParams() {
        List<Object[]> list = new ArrayList<Object[]>();
//        if (true) {
//            list.add(new Object[]{COMMANDS[22], 0});
//            return list;
//        }
        int i = 0;
        for (String cmd : COMMANDS) {
            list.add(new Object[]{cmd, i});
            i++;
        }
       
        return list;
    }
    
    public DbCacheSizeTest(String cmd, int testNum){
        this.cmd = cmd;
        this.testNum = testNum;
        customName = "-" + testNum;
       
    }
    
    @Test
    public void testSize() {

        /* Get estimated cache sizes and measured sizes. */
        final String[] args = (cmd + " " + ADD_COMMANDS).split(" ");
        DbCacheSize util = new DbCacheSize();
        try {
            util.parseArgs(args);
            util.calculateCacheSizes();
            if (VERBOSE) {
                util.printCacheSizes(System.out);
            }
            util.measure(VERBOSE ? System.out : null);
        } finally {
            util.cleanup();
        }

        final boolean offHeap = cmd.contains("-offheap");

        /*
         * Check that calculated and measured sizes are within some error
         * tolerance.
         */
        check(
            "mainNoLNsOrVLSNs",
            util.getMainNoLNsOrVLSNs(),
            util.getMeasuredMainNoLNsOrVLSNs(),
            util.getMainNoLNsOrVLSNs() + util.getOffHeapNoLNsOrVLSNs());

        if (offHeap) {
            assertEquals(0, util.getMainNoLNsWithVLSNs());
        } else {
            check(
                "mainNoLNsWithVLSNs",
                util.getMainNoLNsWithVLSNs(),
                util.getMeasuredMainNoLNsWithVLSNs(),
                util.getMainNoLNsWithVLSNs());
        }

        check(
            "mainWithLNsAndVLSNs",
            util.getMainWithLNsAndVLSNs(),
            util.getMeasuredMainWithLNsAndVLSNs(),
            util.getMainWithLNsAndVLSNs() + util.getOffHeapWithLNsAndVLSNs());

        check(
            "offHeapNoLNsOrVLSNs",
            util.getOffHeapNoLNsOrVLSNs(),
            util.getMeasuredOffHeapNoLNsOrVLSNs(),
            util.getMainNoLNsOrVLSNs() + util.getOffHeapNoLNsOrVLSNs());

        check(
            "offHeapWithLNsAndVLSNs",
            util.getOffHeapWithLNsAndVLSNs(),
            util.getMeasuredOffHeapWithLNsAndVLSNs(),
            util.getMainWithLNsAndVLSNs() + util.getOffHeapWithLNsAndVLSNs());

        /*
         * Do the same for the preloaded values, which is really a self-check
         * to ensure that preload gives the same results.
         */
        check(
            "noLNsOrVLSNsPreload",
            util.getMainNoLNsOrVLSNs(),
            util.getPreloadMainNoLNsOrVLSNs(),
            util.getMainNoLNsOrVLSNs() + util.getOffHeapNoLNsOrVLSNs());

        if (offHeap) {
            assertEquals(0, util.getPreloadMainNoLNsWithVLSNs());
        } else {
            check(
                "noLNsWithVLSNsPreload",
                util.getMainNoLNsWithVLSNs(),
                util.getPreloadMainNoLNsWithVLSNs(),
                util.getMainNoLNsWithVLSNs());
        }

        check(
            "withLNsAndVLSNsPreload",
            util.getMainWithLNsAndVLSNs(),
            util.getPreloadMainWithLNsAndVLSNs(),
            util.getMainWithLNsAndVLSNs() + util.getOffHeapWithLNsAndVLSNs());
    }

    /**
     * @param name the name of the property being checked.
     *
     * @param expected the expected value as computed by DbCacheSize.
     *
     * @param actual the actual value as computed by measuring with an
     * actual data set.
     *
     * @param total the expected total of off-heap and main cache sizes. Used
     * to determine whether the difference between the expected and actual
     * values, when divided by the total, is under the allowed threshold. When
     * the main cache holds everything, small differences may result in a small
     * off-heap cache size, as compared to an expected zero size. If the
     * difference were divided by the expected value, the error would be
     * infinity. This illustrates why the error is calculated by dividing by
     * the total.
     */
    private void check(String name,
                       double expected,
                       double actual,
                       long total) {

        final double error = (Math.abs(expected - actual) / total);

        if (VERBOSE) {
            System.out.format("%d %s Error %.2f %n", testNum, name, error);
        }

        if (error > ERROR_ALLOWED) {
            fail(String.format(
                "%d %s Error allowed = %.2f but got = %.2f " +
                "Value expected= %,.0f but got = %,.0f out of total %d %n",
                testNum, name, ERROR_ALLOWED, error, expected, actual, total));
        }
    }
}
