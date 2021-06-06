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

import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.util.test.TxnTestCase;

public class TxnFSyncTest extends com.sleepycat.je.txn.TxnFSyncTest {

    // TODO: Low level environment manipulation. Env not being closed. Multiple
    // active environment handles to the same environment.

    public TxnFSyncTest(String type) {
        super(type);
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        return getTxnParams(
            new String[] {TxnTestCase.TXN_USER, TxnTestCase.TXN_AUTO}, true);
    }

    /* junit.framework.AssertionFailedError: Address already in use
        at junit.framework.Assert.fail(Assert.java:47)
        at com.sleepycat.je.rep.RepEnvWrapper.create(RepEnvWrapper.java:60)
        at com.sleepycat.je.DualTestCase.create(DualTestCase.java:63)
        at com.sleepycat.je.txn.TxnFSyncTest.testFSyncButNoClose(TxnFSyncTest.java:105)
        ...

        */
    @Override
    public void testFSyncButNoClose() {
    }
}
