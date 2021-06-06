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

import org.junit.runners.Parameterized;

public class TTLTest extends com.sleepycat.je.test.TTLTest {

    @Parameterized.Parameters
    public static List<Object[]> genParams() {
        return getTxnParams(null, true /*rep*/);
    }

    public TTLTest(String type){
        super(type);
    }

    /*
     * Causes a data mismatch when comparing nodes, because the test only
     * compresses BINs on the master node and the time is set artificially.
     */
    @Override
    public void testCompression() {}

    /*
     * LNs are not purged as expected with replication, because .jdb files are
     * protected from deletion for various reasons, e.g., a temporary replica
     * lag.
     */
    @Override
    public void testPurgedLNs() {}

    /*
     * Same as for testPurgedLNs.
     */
    @Override
    public void testPurgedSlots() {}
}
