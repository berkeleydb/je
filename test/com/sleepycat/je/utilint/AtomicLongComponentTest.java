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

import org.junit.Test;

import com.sleepycat.util.test.TestBase;

/** Test the AtomicLongComponent class */
public class AtomicLongComponentTest extends TestBase {

    @Test
    public void testConstructor() {
        AtomicLongComponent comp = new AtomicLongComponent();
        assertEquals(Long.valueOf(0), comp.get());
    }

    @Test
    public void testSet() {
        AtomicLongComponent comp = new AtomicLongComponent();
        comp.set(72);
        assertEquals(Long.valueOf(72), comp.get());
    }

    @Test
    public void testClear() {
        AtomicLongComponent comp = new AtomicLongComponent();
        comp.set(37);
        comp.clear();
        assertEquals(Long.valueOf(0), comp.get());
    }

    @Test
    public void testCopy() {
        AtomicLongComponent comp = new AtomicLongComponent();
        comp.set(70);
        AtomicLongComponent copy = comp.copy();
        comp.clear();
        assertEquals(Long.valueOf(70), copy.get());
        copy.set(75);
        assertEquals(Long.valueOf(0), comp.get());
    }

    @Test
    public void testGetFormattedValue() {
        AtomicLongComponent comp = new AtomicLongComponent();
        comp.set(123456789);
        assertEquals("123,456,789", comp.getFormattedValue(true));
        assertEquals("123456789", comp.getFormattedValue(false));
    }

    @Test
    public void testIsNotSet() {
        AtomicLongComponent comp = new AtomicLongComponent();
        assertTrue(comp.isNotSet());
        comp.set(3);
        assertFalse(comp.isNotSet());
        comp.clear();
        assertTrue(comp.isNotSet());
    }

    @Test
    public void testToString() {
        AtomicLongComponent comp = new AtomicLongComponent();
        comp.set(987654321);
        assertEquals("987654321", comp.toString());
    }
}
