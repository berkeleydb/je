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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.util.test.TestBase;

/** Test the DoubleExpMovingAvg class. */
public class DoubleExpMovingAvgTest extends TestBase {

    private DoubleExpMovingAvg avg;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        avg = new DoubleExpMovingAvg("stat", 3000);
    }

    @Test
    public void testConstructorPeriodMillis() {
        avg.add(1, 1000);
        avg.add(2, 2000);
        avg.add(4, 3000);
        avg.add(8, 4000);
        assertEquals(3.7, avg.get(), 0.1);

        /* Shorter period skews result towards later entries */
        avg = new DoubleExpMovingAvg("stat", 2000);
        avg.add(1, 1000);
        avg.add(2, 2000);
        avg.add(4, 3000);
        avg.add(8, 4000);
        assertEquals(4.6, avg.get(), 0.1);
    }

    @Test
    public void testCopyConstructor() {
        avg.add(2, 1000);
        assertEquals(2, avg.get(), 0);
        DoubleExpMovingAvg copy = new DoubleExpMovingAvg(avg);
        assertEquals(avg.get(), copy.get(), 0);
        copy.add(4, 2000);
        assertEquals(2, avg.get(), 0);
        assertEquals(2.5, copy.get(), 0.1);
    }

    @Test
    public void testGetAndAdd() {
        assertEquals(0, avg.get(), 0);

        avg.add(1, 1000);
        assertEquals(1, avg.get(), 0);
        avg.add(4.2, 2000);
        assertEquals(2, avg.get(), 0.1);
        avg.add(5.5, 3000);
        assertEquals(3, avg.get(), 0.1);
        avg.add(3, 4000);
        assertEquals(3, avg.get(), 0.1);
        avg.add(-0.3, 5000);
        assertEquals(2, avg.get(), 0.1);
        avg.add(-1.3, 6000);
        assertEquals(1, avg.get(), 0.1);
        avg.add(-2.4, 7000);
        assertEquals(0, avg.get(), 0.1);
        avg.add(0, 8000);
        assertEquals(0, avg.get(), 0.1);

        /* Ignore items at same and earlier times */
        avg.add(123, 8000);
        avg.add(456, 2000);
        assertEquals(0, avg.get(), 0.1);
    }

    @Test
    public void testGetFormattedValue() {
        assertEquals("unknown", avg.getFormattedValue(true));
        avg.add(10000, 1000);
        assertEquals("10,000", avg.getFormattedValue(true));
        assertEquals("10000.00", avg.getFormattedValue(false));

        /*
         * Probably don't want to add NaN values, since they will keep the
         * average as NaN from then on, but at least make sure that toString
         * doesn't do something weird in this case.
         */
        avg.add(Double.NaN, 2000);
        assertEquals("NaN", avg.getFormattedValue());
    }

    @Test
    public void testIsNotSet() {
        assertTrue(avg.isNotSet());
        avg.add(1, 1000);
        assertFalse(avg.isNotSet());
        avg.add(2, 2000);
        assertFalse(avg.isNotSet());
    }
}
