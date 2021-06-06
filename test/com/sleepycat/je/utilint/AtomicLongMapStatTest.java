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

import java.util.TreeMap;
import java.util.SortedMap;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.utilint.StatDefinition.StatType;
import com.sleepycat.util.test.TestBase;

/** Test the AtomicLongMapStat class */
public class AtomicLongMapStatTest extends TestBase {

    private static final StatGroup statGroup =
        new StatGroup("TestGroup", "Test group");
    private static int statDefCount;

    private AtomicLongMapStat map;
    private AtomicLongMapStat cumulativeMap;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        map = new AtomicLongMapStat(statGroup, getStatDef());
        cumulativeMap = new AtomicLongMapStat(
            statGroup,
            new StatDefinition(getStatDefName(), "", StatType.CUMULATIVE));
    }

    private StatDefinition getStatDef() {
        return new StatDefinition(getStatDefName(), "");
    }

    private String getStatDefName() {
        return "stat" + Integer.toString(++statDefCount);
    }

    @Test
    public void testCreateStat() {
        AtomicLongComponent compA = map.createStat("a");
        compA.set(1);
        AtomicLongComponent compB = map.createStat("b");
        compB.set(2);
        assertEquals("a=1;b=2", map.get());
    }

    @Test
    public void testCopy() {
        AtomicLongComponent compA = map.createStat("a");
        compA.set(1);
        AtomicLongMapStat copy = map.copy();
        AtomicLongComponent compB = map.createStat("b");
        compB.set(2);
        AtomicLongComponent compC = copy.createStat("c");
        compC.set(3);
        assertEquals("a=1;b=2", map.get());
        assertEquals("a=1;c=3", copy.get());
    }

    @Test
    public void testComputeInterval() {
        AtomicLongMapStat copy = map.copy();

        AtomicLongMapStat interval = map.computeInterval(copy);
        assertTrue(interval.isNotSet());

        AtomicLongComponent compA = map.createStat("a");
        AtomicLongComponent copyCompA = copy.createStat("a");

        interval = map.computeInterval(copy);
        assertTrue(interval.isNotSet());

        compA.set(3);
        copyCompA.set(1);
        interval = map.computeInterval(copy);
        assertEquals("a=2", interval.get());
        interval = copy.computeInterval(map);
        assertEquals("a=-2", interval.get());
        assertEquals("a=3", map.get());
        assertEquals("a=1", copy.get());

        AtomicLongComponent compB = map.createStat("b");
        compB.set(7);

        interval = map.computeInterval(copy);
        assertEquals("a=2;b=7", interval.get());
        interval = copy.computeInterval(map);

        /* Component b is not present in copy, so not included here */
        assertEquals("a=-2", interval.get());
        assertEquals("a=3;b=7", map.get());
        assertEquals("a=1", copy.get());

        AtomicLongComponent cumulativeCompA = cumulativeMap.createStat("a");
        cumulativeCompA.set(9);
        interval = cumulativeMap.computeInterval(map);
        assertEquals("a=9", interval.get());
    }

    @Test
    public void testNegate() {
        map.negate();
        assertEquals("", map.get());
        AtomicLongComponent compA = map.createStat("a");
        compA.set(-33);
        map.negate();
        assertEquals("a=33", map.get());

        compA = cumulativeMap.createStat("a");
        compA.set(-33);
        map.negate();
        assertEquals("a=-33", map.get());
    }

    @Test
    public void getMap() {
        SortedMap<String, Long> valueMap = new TreeMap<>();
        assertEquals(valueMap, map.getMap());
        AtomicLongComponent compA = map.createStat("a");
        assertEquals(valueMap, map.getMap());
        valueMap.put("a", 1L);
        compA.set(1);
        assertEquals(valueMap, map.getMap());
    }

    @Test
    public void testIsNotSet() {
        assertTrue(map.isNotSet());
        map.createStat("a");
        AtomicLongComponent compB = map.createStat("b");
        assertTrue(map.isNotSet());
        compB.set(2);
        assertFalse(map.isNotSet());
        compB.clear();
        assertTrue(map.isNotSet());
    }
}
