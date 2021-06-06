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

package com.sleepycat.je.rep.dual.test;

import java.util.List;

import org.junit.runners.Parameterized.Parameters;

public class SecondaryDirtyReadTest extends
        com.sleepycat.je.test.SecondaryDirtyReadTest {

    public SecondaryDirtyReadTest(
        String type,
        boolean multiKey,
        boolean duplication,
        boolean dirtyReadAll) {

        super(type, multiKey, duplication, dirtyReadAll);
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        return paramsHelper(true);
    }
}
