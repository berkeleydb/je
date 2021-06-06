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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.sleepycat.je.utilint.HexFormatter;
import com.sleepycat.util.test.TestBase;

/**
 * Trivial formatting class that sticks leading 0's on the front of a hex
 * number.
 */
public class HexFormatterTest extends TestBase {
    
    @Test
    public void testFormatLong() {
        assertTrue(HexFormatter.formatLong(0).equals("0x0000000000000000"));
        assertTrue(HexFormatter.formatLong(1).equals("0x0000000000000001"));
        assertTrue(HexFormatter.formatLong(0x1234567890ABCDEFL).equals("0x1234567890abcdef"));
        assertTrue(HexFormatter.formatLong(0x1234567890L).equals("0x0000001234567890"));
        assertTrue(HexFormatter.formatLong(0xffffffffffffffffL).equals("0xffffffffffffffff"));
    }
}
