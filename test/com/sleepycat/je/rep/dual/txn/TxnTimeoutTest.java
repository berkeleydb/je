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


public class TxnTimeoutTest extends com.sleepycat.je.txn.TxnTimeoutTest {

    /* 
     * The following unit tests are excluded because they intentionally 
     * provoked to exceptions and handles those accordingly. The special 
     * handing is not available on the replica side, and would cause a replica
     * failure.
     */
    @Override
    public void testTxnTimeout() {
    }
    
    @Override
    public void testPerTxnTimeout() {
    }

    @Override
    public void testEnvTxnTimeout() {
    }

    @Override
    public void testEnvNoLockTimeout() {
    }

    @Override
    public void testPerLockTimeout() {
    }

    @Override
    public void testEnvLockTimeout() {
    }

    @Override
    public void testPerLockerTimeout() {
    }

    @Override
    public void testReadCommittedTxnTimeout() {
    }

    @Override
    public void testReadCommittedLockTimeout() {
    }

    @Override
    public void testSerializableTxnTimeout() {
    }

    @Override
    public void testSerializableLockTimeout() {
    }
}
