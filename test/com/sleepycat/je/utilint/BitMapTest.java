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
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.util.test.TestBase;

public class BitMapTest extends TestBase {

    @Test
    public void testSegments() {

        BitMap bmap = new BitMap();
        int startBit = 15;
        int endBit = 62;
        assertEquals(0, bmap.cardinality());
        assertEquals(0, bmap.getNumSegments());

        assertFalse(bmap.get(1001L));
        assertEquals(0, bmap.getNumSegments());

        /* set a bit in different segments. */
        for (int i = startBit; i <= endBit; i++) {
            long index = 1L << i;
            index += 17;
            bmap.set(index);
        }

        assertEquals((endBit - startBit +1), bmap.cardinality());
        assertEquals((endBit - startBit + 1), bmap.getNumSegments());

        /* should be set. */
        for (int i = startBit; i <= endBit; i++) {
            long index = 1L << i;
            index += 17;
            assertTrue(bmap.get(index));
        }

        /* should be clear. */
        for (int i = startBit; i <= endBit; i++) {
            long index = 7 + (1L << i);
            assertFalse(bmap.get(index));
        }

        /* checking for non-set bits should not create more segments. */
        assertEquals((endBit - startBit +1), bmap.cardinality());
        assertEquals((endBit - startBit + 1), bmap.getNumSegments());
    }

    @Test
    public void testNegative() {
        BitMap bMap = new BitMap();

        try {
            bMap.set(-300);
            fail("should have thrown exception");
        } catch (IndexOutOfBoundsException expected) {
        }
    }
}
