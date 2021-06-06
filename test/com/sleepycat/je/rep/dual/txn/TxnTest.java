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

package com.sleepycat.je.rep.dual.txn;

public class TxnTest extends com.sleepycat.je.txn.TxnTest {

    @Override
    public void testBasicLocking()
        throws Throwable {
    }

    /* 
     * This test case is excluded because it uses the deprecated durability
     * API, which is prohibited in dual mode tests.
     */
    @Override
    public void testSyncCombo() 
        throws Throwable {
    }

    /**
     * Excluded because it opens and closes the environment several times and
     * the rep utils don't behave well under these conditions.
     */
    @Override
    public void testPossiblyCommittedState() {
    }
}
