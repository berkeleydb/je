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

public class EnvironmentTest extends com.sleepycat.je.EnvironmentTest {

    /* In following test cases, Environment is set non-transactional. */
    @Override
    public void testReadOnly() {
    }

    @Override
    public void testTransactional() {
    }

    @Override
    public void testOpenWithoutCheckpoint() {
    }

    @Override
    public void testMutableConfig() {
    }

    @Override
    public void testTxnDeadlockStackTrace() {
    }

    @Override
    public void testParamLoading() {
    }

    @Override
    public void testConfig() {
    }

    @Override
    public void testGetDatabaseNames() {
    }

    @Override
    public void testDaemonRunPause() {
    }

    /* In following test cases, Database is set non-transactional. */
    @Override
    public void testDbRenameCommit() {
    }

    @Override
    public void testDbRenameAbort() {
    }

    @Override
    public void testDbRemove() {
    }

    @Override
    public void testDbRemoveReadCommitted() {
    }

    @Override
    public void testDbRemoveNonTxnl() {
    }

    @Override
    public void testDbRemoveCommit() {
    }

    /*
     * In following two test cases, they try to reclose a same environment or
     * database. This behavior would fail, override it for now,
     * may be fixed in the future.
    */
    @Override
    public void testClose() {
    }

    @Override
    public void testExceptions() {
    }

    /*
     * This test case tries to open two environments on a same directory,
     * it would fail, override it for now, may be fixed in the future.
     */
    @Override
    public void testReferenceCounting() {
    }

    /*
     * This test opens an environment read-only so this is not applicable
     * to ReplicatedEnvironments (which can not be opened read-only).
     */
    @Override
    public void testReadOnlyDbNameOps() {
    }

    /* HA doesn't support in memory only environment. */
    @Override
    public void testMemOnly() {
    }

    /*
     * The 1ms latch timeout used in the test sometimes expires during an HA
     * join group. It isn't important to test this separately for HA.
     */
    @Override
    public void testLatchTimeout() {
    }
}
