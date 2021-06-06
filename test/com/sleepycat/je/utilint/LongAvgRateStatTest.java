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

package com.sleepycat.je.utilint;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.util.test.TestBase;

/** Test the LongAvgRateStat class. */
public class LongAvgRateStatTest extends TestBase {

    private static final StatGroup statGroup =
        new StatGroup("TestGroup", "Test group");
    private static int statDefCount;

    private LongAvgRateStat stat;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        stat = new LongAvgRateStat(
            statGroup, getStatDef(), 3000, MILLISECONDS);
    }

    private static StatDefinition getStatDef() {
        return new StatDefinition(getStatDefName(), "");
    }

    private static String getStatDefName() {
        return "stat" + Integer.toString(++statDefCount);
    }

    @Test
    public void testCopy() {
        stat.add(0, 1000);
        stat.add(3000, 2000);
        LongAvgRateStat copy = stat.copy();
        stat.add(9000, 3000);
        copy.add(15000, 3000);
        assertEquals(Long.valueOf(4), stat.get());
        assertEquals(Long.valueOf(6), copy.get());
    }

    @Test
    public void testComputeInterval() {
        LongAvgRateStat other = stat.copy();

        LongAvgRateStat interval = stat.computeInterval(other);
        assertTrue(interval.isNotSet());

        stat.add(0, 1000);
        stat.add(3000, 2000);
        interval = stat.computeInterval(other);
        assertEquals(Long.valueOf(3), interval.get());
        interval = other.computeInterval(stat);
        assertEquals(Long.valueOf(3), interval.get());

        other.add(10000, 4000);
        other.add(40000, 5000);
        interval = stat.computeInterval(other);
        assertEquals(Long.valueOf(20), interval.get());
        interval = other.computeInterval(stat);
        assertEquals(Long.valueOf(20), interval.get());

        stat.clear();
        other.clear();
        stat.add(10000, 1000);
        stat.add(40000, 2000);
        other.add(0, 4000);
        other.add(3000, 5000);
        interval = stat.computeInterval(other);
        assertEquals(Long.valueOf(13), interval.get());
        interval = other.computeInterval(stat);
        assertEquals(Long.valueOf(13), interval.get());
    }
}
