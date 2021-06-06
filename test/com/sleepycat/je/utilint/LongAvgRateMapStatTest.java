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
import static org.junit.Assert.assertTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.TreeMap;
import java.util.SortedMap;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.util.test.TestBase;

/** Test the LongAvgRateMapStat class */
public class LongAvgRateMapStatTest extends TestBase {

    private static final StatGroup statGroup =
        new StatGroup("TestGroup", "Test group");
    private static int statDefCount;

    private LongAvgRateMapStat map;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        map = new LongAvgRateMapStat(
            statGroup, getStatDef(), 3000, MILLISECONDS);
    }

    private StatDefinition getStatDef() {
        return new StatDefinition(getStatDefName(), "");
    }

    private String getStatDefName() {
        return "stat" + Integer.toString(++statDefCount);
    }

    @Test
    public void testCreateStat() {
        LongAvgRate compA = map.createStat("a");
        compA.add(0, 1000);
        compA.add(1000, 2000);
        LongAvgRate compB = map.createStat("b");
        compB.add(0, 1000);
        compB.add(2000, 2000);
        assertEquals("a=1;b=2", map.get());
    }

    @Test
    public void testRemoveStat() {
        map.removeStat("a");
        LongAvgRate compA = map.createStat("a");
        compA.add(0, 1000);
        compA.add(1000, 2000);
        LongAvgRate compB = map.createStat("b");
        compB.add(0, 1000);
        compB.add(2000, 2000);
        assertEquals("a=1;b=2", map.get());
        map.removeStat("c");
        assertEquals("a=1;b=2", map.get());
        map.removeStat("a");
        assertEquals("b=2", map.get());
    }

    @Test
    public void testCopy() {
        LongAvgRate compA = map.createStat("a");
        compA.add(0, 1000);
        compA.add(1000, 2000);
        LongAvgRateMapStat copy = map.copy();
        LongAvgRate compB = map.createStat("b");
        compB.add(0, 1000);
        compB.add(2000, 2000);
        LongAvgRate compC = copy.createStat("c");
        compC.add(0, 1000);
        compC.add(3000, 2000);
        assertEquals("a=1;b=2", map.get());
        assertEquals("a=1;c=3", copy.get());
    }

    @Test
    public void testComputeInterval() {
        LongAvgRateMapStat other = map.copy();

        LongAvgRateMapStat interval = map.computeInterval(other);
        assertTrue(interval.isNotSet());

        /*
         * Consider all combinations of entries with a value, an unset entry,
         * no entry, and a removed entry:
         *
         * a: present with non-zero value
         * b: present with zero/cleared value
         * c: absent
         * d: removed
         *
         * If both maps have an entry for the same key, then they should be
         * merged.  If the newer of the two maps has no entry, then the result
         * should not include one.  The removed case is one where the removal
         * can make the map newer even though it has no entries present to
         * prove it.  If the maps have the same modification time -- an
         * unlikely case in practice -- then the this argument controls which
         * entries appear.
         */
        LongAvgRate comp1a2a = map.createStat("1a2a");
        LongAvgRate comp1a2b = map.createStat("1a2b");
        LongAvgRate comp1a2c = map.createStat("1a2c");
        LongAvgRate comp1a2d = map.createStat("1a2d");
        map.createStat("1b2a");
        LongAvgRate comp1d2a = map.createStat("1d2a");
        LongAvgRate otherComp1a2a = other.createStat("1a2a");
        other.createStat("1a2b");
        LongAvgRate otherComp1a2d = other.createStat("1a2d");
        LongAvgRate otherComp1b2a = other.createStat("1b2a");
        LongAvgRate otherComp1c2a = other.createStat("1c2a");
        LongAvgRate otherComp1d2a = other.createStat("1d2a");

        /* Test with both maps empty */
        interval = map.computeInterval(other);
        assertTrue(interval.isNotSet());

        /* Test with other map empty */
        comp1a2a.add(0, 1000);
        comp1a2a.add(3000, 2000);
        comp1a2b.add(0, 1000);
        comp1a2b.add(6000, 2000);
        comp1a2c.add(0, 1000);
        comp1a2c.add(9000, 2000);
        comp1a2d.add(0, 1000);
        comp1a2d.add(12000, 2000);
        comp1d2a.add(0, 1000);
        comp1d2a.add(15000, 2000);
        map.removeStat("1d2a", 2001);

        interval = map.computeInterval(other);
        assertEquals("1a2a=3;1a2b=6;1a2c=9;1a2d=12",
                     interval.get());
        interval = other.computeInterval(map);
        assertEquals("1a2a=3;1a2b=6;1a2c=9;1a2d=12",
                     interval.get());

        /* Test with other map newer */
        otherComp1a2a.add(10000, 4000);
        otherComp1a2a.add(40000, 5000);
        otherComp1a2d.add(10000, 4000);
        otherComp1a2d.add(70000, 5000);
        other.removeStat("1a2d",5001);
        otherComp1b2a.add(10000, 4000);
        otherComp1b2a.add(100000, 5000);
        otherComp1c2a.add(10000, 4000);
        otherComp1c2a.add(130000, 5000);
        otherComp1d2a.add(10000, 4000);
        otherComp1d2a.add(160000, 5000);

        interval = map.computeInterval(other);
        assertEquals("1a2a=20;1a2b=6;1b2a=90;1c2a=120;1d2a=150",
                     interval.get());
        interval = other.computeInterval(map);
        assertEquals("1a2a=20;1a2b=6;1b2a=90;1c2a=120;1d2a=150",
                     interval.get());

        /* Test with other map older */
        comp1a2a.clear();
        comp1a2b.clear();
        comp1a2c.clear();
        comp1a2d.clear();
        comp1d2a = map.createStat("1d2a");

        otherComp1a2a.clear();
        otherComp1a2d = other.createStat("1a2d");
        otherComp1b2a.clear();
        otherComp1c2a.clear();
        otherComp1d2a.clear();

        comp1a2a.add(0, 4000);
        comp1a2a.add(3000, 5000);
        comp1a2b.add(0, 4000);
        comp1a2b.add(6000, 5000);
        comp1a2c.add(0, 4000);
        comp1a2c.add(9000, 5000);
        comp1a2d.add(0, 4000);
        comp1a2d.add(12000, 5000);
        comp1d2a.add(0, 4000);
        comp1d2a.add(15000, 5000);
        map.removeStat("1d2a", 5001);

        otherComp1a2a.add(10000, 1000);
        otherComp1a2a.add(40000, 2000);
        otherComp1a2d.add(10000, 1000);
        otherComp1a2d.add(70000, 2000);
        other.removeStat("1a2d", 2001);
        otherComp1b2a.add(10000, 1000);
        otherComp1b2a.add(100000, 2000);
        otherComp1c2a.add(10000, 1000);
        otherComp1c2a.add(130000, 2000);
        otherComp1d2a.add(10000, 1000);
        otherComp1d2a.add(160000, 2000);

        interval = map.computeInterval(other);
        assertEquals("1a2a=13;1a2b=6;1a2c=9;1a2d=12;1b2a=90",
                     interval.get());
        interval = other.computeInterval(map);
        assertEquals("1a2a=13;1a2b=6;1a2c=9;1a2d=12;1b2a=90",
                     interval.get());

        /*
         * Remove an entry from map using the same timestamp as on the other
         * map, and confirm that the entry only appears if it is present in the
         * this argument.
         */
        other.removeStat("1a2a", 5001);
        interval = map.computeInterval(other);
        assertEquals("1a2a=3;1a2b=6;1a2c=9;1a2d=12;1b2a=90",
                     interval.get());
        interval = other.computeInterval(map);
        assertEquals("1a2b=6;1b2a=90;1c2a=120;1d2a=150",
                     interval.get());

        /*
         * Now remove everything from the older map, and make sure makes the
         * result empty
         */
        map.removeStat("1a2a", 5002);
        map.removeStat("1a2b", 5002);
        map.removeStat("1a2c", 5002);
        map.removeStat("1a2d", 5002);
        map.removeStat("1b2a", 5002);
        interval = map.computeInterval(other);
        assertEquals("", interval.get());
        interval = other.computeInterval(map);
        assertEquals("", interval.get());
    }

    @Test
    public void testNegate() {
        map.negate();
        assertEquals("", map.get());
        LongAvgRate compA = map.createStat("a");
        compA.add(0, 1000);
        compA.add(3000, 2000);
        map.negate();
        assertEquals("a=3", map.get());
    }

    @Test
    public void getMap() {
        SortedMap<String, Long> valueMap = new TreeMap<>();
        assertEquals(valueMap, map.getMap());
        LongAvgRate compA = map.createStat("a");
        assertEquals(valueMap, map.getMap());
        valueMap.put("a", 3L);
        compA.add(0, 1000);
        compA.add(3000, 2000);
        assertEquals(valueMap, map.getMap());
    }
}
