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

public class TruncateTest extends com.sleepycat.je.TruncateTest {

    // TODO: relies on exact standalone LN counts. Rep introduces additional
    // LNs.
    @Override
    public void testEnvTruncateAbort() {
    }

    @Override
    public void testEnvTruncateCommit() {
    }

    @Override
    public void testEnvTruncateAutocommit() {
    }

    @Override
    public void testEnvTruncateNoFirstInsert() {
    }

    // Skip since it's non-transactional
    @Override
    public void testNoTxnEnvTruncateCommit() {
    }

    @Override
    public void testTruncateCommit() {
    }

    @Override
    public void testTruncateCommitAutoTxn() {
    }

    @Override
    public void testTruncateEmptyDeferredWriteDatabase() {
    }

    // TODO: Complex setup -- replicators not shutdown resulting in an
    // attempt to rebind to an already bound socket address
    @Override
    public void testTruncateAfterRecovery() {
    }

    /* Non-transactional access is not supported by HA. */
    @Override
    public void testTruncateNoLocking() {
    }

    /* Calls EnvironmentImpl.abnormalShutdown. */
    @Override
    public void testTruncateRecoveryWithoutMapLNDeletion()
        throws Throwable {
    }

    /* Calls EnvironmentImpl.abnormalShutdown. */
    @Override
    public void testTruncateRecoveryWithoutMapLNDeletionNonTxnal()
        throws Throwable {
    }

    /* Calls EnvironmentImpl.abnormalShutdown. */
    @Override
    public void testRemoveRecoveryWithoutMapLNDeletion()
        throws Throwable {
    }

    /* Calls EnvironmentImpl.abnormalShutdown. */
    @Override
    public void testRemoveRecoveryWithoutMapLNDeletionNonTxnal()
        throws Throwable {
    }

    /* Calls EnvironmentImpl.abnormalShutdown. */
    @Override
    public void testRecoveryRenameMapLNDeletion()
        throws Throwable {
    }
}
