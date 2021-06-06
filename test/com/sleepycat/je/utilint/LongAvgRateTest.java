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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.util.test.TestBase;

/** Test the LongAvgRate class */
public class LongAvgRateTest extends TestBase {

    private LongAvgRate avg;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        avg = new LongAvgRate("stat", 3000, MILLISECONDS);
    }

    @Test
    public void testConstructorPeriodMillis() {
        avg.add(1000, 1000);
        avg.add(2000, 2000);
        avg.add(4000, 3000);
        avg.add(8000, 4000);
        assertEquals(Long.valueOf(2), avg.get());

        /* Shorter period skews result towards later entries */
        avg = new LongAvgRate("stat", 2000, MILLISECONDS);
        avg.add(1000, 1000);
        avg.add(2000, 2000);
        avg.add(4000, 3000);
        avg.add(8000, 4000);
        assertEquals(Long.valueOf(2), avg.get());
    }

    @Test
    public void testConstructorReportTimeUnit() {
        avg = new LongAvgRate("stat", 3000, NANOSECONDS);
        avg.add(2000000000L, 1000);
        avg.add(4000000000L, 2000);
        assertEquals(Long.valueOf(2), avg.get());

        avg = new LongAvgRate("stat", 3000, MICROSECONDS);
        avg.add(2000000000L, 1000);
        avg.add(4000000000L, 2000);
        assertEquals(Long.valueOf(2000), avg.get());

        avg = new LongAvgRate("stat", 3000, MILLISECONDS);
        avg.add(2000000000L, 1000);
        avg.add(4000000000L, 2000);
        assertEquals(Long.valueOf(2000000), avg.get());

        avg = new LongAvgRate("stat", 3000, SECONDS);
        avg.add(2000000000L, 1000);
        avg.add(4000000000L, 2000);
        assertEquals(Long.valueOf(2000000000L), avg.get());
    }

    /** Test behavior relative to MIN_PERIOD */
    @Test
    public void testMinPeriod() {
        avg.add(2000, 1000);

        /* Entry with time delta less than 200 ms is ignored */
        avg.add(3000, 1100);
        assertEquals(Long.valueOf(0), avg.get());

        /* Computes back to the initial entry */
        avg.add(4000, 2000);
        assertEquals(Long.valueOf(2), avg.get());
    }

    @Test
    public void testAdd() {
        // Empty
        assertEquals(Long.valueOf(0), avg.get());

        // One value, no delta
        avg.add(2000, 1000);
        assertEquals(Long.valueOf(0), avg.get());

        // Second value prior to MIN_PERIOD is ignored
        avg.add(7700, 1100);
        assertEquals(Long.valueOf(0), avg.get());

        avg.add(4000, 2000);
        avg.add(7700, 2100);
        assertEquals(Long.valueOf(2), avg.get());

        avg.add(8000, 3000);
        avg.add(17700, 3100);
        assertEquals(Long.valueOf(3), avg.get());
    }

    @Test(expected=NullPointerException.class)
    public void testAddAverageNullArg() {
        avg.add(null);
    }

    @Test
    public void testAddAverage() {
        LongAvgRate other = new LongAvgRate("stat", 3000, SECONDS);
        avg.add(other);
        assertTrue("Add empty on empty has no effect", avg.isNotSet());

        avg.add(3000, 1000);
        avg.add(6000, 2000);
        avg.add(other);
        assertEquals("Add empty has no effect", Long.valueOf(3), avg.get());

        other.add(6000, 1000);
        other.add(12000, 2000);
        avg.add(other);
        assertEquals("Add older has no effect", Long.valueOf(3), avg.get());

        other.clear();
        other.add(6000, 3000);
        other.add(12000, 4000);
        avg.add(other);
        assertEquals("Add newer has effect", Long.valueOf(4), avg.get());

        avg.clear();
        avg.add(other);
        assertEquals("Add to empty", 6, avg.get(), 0);
    }

    @Test
    public void testCopyLatest() {
        LongAvgRate other = new LongAvgRate("stat", 3000, MILLISECONDS);

        LongAvgRate latest = avg.copyLatest(other);
        assertTrue(latest.isNotSet());

        avg.add(0, 1000);
        avg.add(3000, 2000);
        latest = avg.copyLatest(other);
        assertEquals(Long.valueOf(3), latest.get());
        latest = other.copyLatest(avg);
        assertEquals(Long.valueOf(3), latest.get());

        /* The later rate is 30, so the result is closer to that */
        other.add(10000, 4000);
        other.add(40000, 5000);
        latest = avg.copyLatest(other);
        assertEquals(Long.valueOf(20), latest.get());
        latest = other.copyLatest(avg);
        assertEquals(Long.valueOf(20), latest.get());

        /* The later rate is 3, so the result is smaller */
        avg.clear();
        other.clear();
        avg.add(10000, 1000);
        avg.add(40000, 2000);
        other.add(0, 4000);
        other.add(3000, 5000);
        latest = avg.copyLatest(other);
        assertEquals(Long.valueOf(13), latest.get());
        latest = other.copyLatest(avg);
        assertEquals(Long.valueOf(13), latest.get());
    }

    @Test
    public void testClear() {
        avg.add(3, 1000);
        avg.add(6, 2000);
        avg.clear();
        assertEquals(Long.valueOf(0), avg.get());
        assertTrue(avg.isNotSet());
    }

    @Test
    public void testCopy() {
        avg.add(3000, 1000);
        avg.add(6000, 2000);
        LongAvgRate copy = avg.copy();
        assertEquals(avg.getName(), copy.getName());
        avg.add(12000, 3000);
        copy.add(24000, 3000);
        assertEquals(Long.valueOf(4), avg.get());
        assertEquals(Long.valueOf(7), copy.get());
    }

    @Test
    public void testGetFormattedValue() {
        avg = new LongAvgRate("stat", 3000, MICROSECONDS);
        assertEquals("unknown", avg.getFormattedValue(true));
        avg.add(0, 1000);
        avg.add(987698769, 2000);
        assertEquals("988", avg.getFormattedValue(true));
        assertEquals("988", avg.getFormattedValue(false));

        avg = new LongAvgRate("stat", 3000, MILLISECONDS);
        assertEquals("unknown", avg.getFormattedValue(true));
        avg.add(0, 1000);
        avg.add(987698769, 2000);
        assertEquals("987,699", avg.getFormattedValue(true));
        assertEquals("987699", avg.getFormattedValue(false));

        avg = new LongAvgRate("stat", 3000, SECONDS);
        assertEquals("unknown", avg.getFormattedValue(true));
        avg.add(0, 1000);
        avg.add(987698769, 2000);
        assertEquals("987,698,769", avg.getFormattedValue(true));
        assertEquals("987698769", avg.getFormattedValue(false));
    }

    @Test
    public void testIsNotSet() {
        assertTrue(avg.isNotSet());
        avg.add(0, 1000);
        assertTrue(avg.isNotSet());
        avg.add(2000, 2000);
        assertFalse(avg.isNotSet());
        avg.add(4000, 3000);
        assertFalse(avg.isNotSet());
    }
}
