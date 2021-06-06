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

public class DatabaseConfigTest extends com.sleepycat.je.DatabaseConfigTest {

    /* Environment in this test case is set non-transactional. */
    @Override
    public void testConfig() {
    }

    /* Environment in this test case is set non-transactional. */
    @Override
    public void testConfigConflict() {
    }

    /* Database in this test case is set non-transactional. */
    @Override
    public void testIsTransactional()  {
    }

    /* Environment in this test case is set non-transactional. */
    @Override
    public void testExclusive()  {
    }

    /* Environment in this test case is set non-transactional. */
    @Override
    public void testConfigOverrideUpdateSR15743() {
    }
}
