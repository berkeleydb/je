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

package com.sleepycat.je.recovery;

import java.io.File;

import org.junit.Test;

import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class MultiEnvTest extends TestBase {

    private final File envHome1;
    private final File envHome2;

    public MultiEnvTest() {
        envHome1 = SharedTestUtils.getTestDir();
        envHome2 = new File(envHome1,
                            "propTest");
    }

    @Test
    public void testNodeIdsAfterRecovery() {

            /*
             * TODO: replace this test which previously checked that the node
             * id sequence shared among environments was correct with a test
             * that checks all sequences, including replicated ones. This
             * change is appropriate because the node id sequence is no longer
             * a static field.
             */
    }
}
