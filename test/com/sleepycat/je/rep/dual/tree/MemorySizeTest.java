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
package com.sleepycat.je.rep.dual.tree;

public class MemorySizeTest extends com.sleepycat.je.tree.MemorySizeTest {

    /* 
     * This test changes the KeyPrefix on the master, but not on the replicas, 
     * so it would fail on the rep mode, disable it. 
     */
    @Override
    public void testKeyPrefixChange() {
    }
}
