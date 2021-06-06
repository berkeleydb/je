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

package com.sleepycat.je.dbi;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.sleepycat.je.utilint.JVMSystemUtils;

import org.junit.Test;

public class CompressedOopsDetectorTest {

    /**
     * There is no easy way to test that the detector has determined correctly
     * if compact OOPs are in use -- that requires out-of-band confirmation.
     * Just test that the detector reports that it knows the answer.  A test
     * failure suggests that the CompressedOopsDetector class needs to be
     * updated for the current platform.
     */
    @Test
    public void testDetector() {
        if (JVMSystemUtils.ZING_JVM) {
            assertNull("Zing result should be unknown",
                       CompressedOopsDetector.isEnabled());
            return;
        }
        assertNotNull("CompressedOopsDetector result is unknown",
                      CompressedOopsDetector.isEnabled());
    }
}
