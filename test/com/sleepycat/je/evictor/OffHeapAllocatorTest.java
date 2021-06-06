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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import com.sleepycat.util.test.TestBase;
import org.junit.Test;

/**
 */
public class OffHeapAllocatorTest extends TestBase {

    @Test
    public void testBasic() throws Exception {

        final OffHeapAllocatorFactory factory = new OffHeapAllocatorFactory();
        final OffHeapAllocator allocator = factory.getDefaultAllocator();

        long memId = allocator.allocate(100);
        assertTrue(memId != 0);
        assertEquals(100, allocator.size(memId));

        byte[] buf = new byte[100];
        byte[] buf2 = new byte[100];

        Arrays.fill(buf, (byte) 1);
        allocator.copy(memId, 0, buf, 0, 100);
        Arrays.fill(buf2, (byte) 0);
        assertTrue(Arrays.equals(buf, buf2));

        Arrays.fill(buf, (byte) 1);
        allocator.copy(buf, 0, memId, 0, 100);
        Arrays.fill(buf2, (byte) 0);
        allocator.copy(memId, 0, buf2, 0, 100);
        assertTrue(Arrays.equals(buf, buf2));

        allocator.free(memId);
    }
}
