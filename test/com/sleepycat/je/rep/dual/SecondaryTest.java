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

package com.sleepycat.je.rep.dual;

import java.util.List;

import org.junit.runners.Parameterized.Parameters;

public class SecondaryTest extends com.sleepycat.je.test.SecondaryTest {

    public SecondaryTest(String type,
                         boolean multiKey,
                         boolean customAssociation,
                         boolean resetOnFailure){
        super(type, multiKey, customAssociation, resetOnFailure);
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        return paramsHelper(true);
    }

    /**
     * Test is based on EnvironmentStats.getNLNsFetch which returns varied
     * results when replication is enabled, presumably because the feeder is
     * fetching.
     */
    @Override
    public void testImmutableSecondaryKey() {
    }

    /**
     * Same issue as testImmutableSecondaryKey.
     */
    @Override
    public void testExtractFromPrimaryKeyOnly() {
    }

    /**
     * Same issue as testImmutableSecondaryKey.
     */
    @Override
    public void testImmutableSecondaryKeyAndExtractFromPrimaryKeyOnly() {
    }
}
