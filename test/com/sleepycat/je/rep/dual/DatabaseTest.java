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

public class DatabaseTest extends com.sleepycat.je.DatabaseTest {

    /* Non-transactional tests. */

    @Override
    public void testOpenCursor() {
    }

    @Override
    public void testOpenCursorAfterEnvInvalidation() {
    }

    @Override
    public void testPreloadBytesExceedsCache() {
    }

    @Override
    public void testPreloadEntireDatabase() {
    }

    @Override
    public void testPutNoOverwriteInADupDbNoTxn() {
    }

    @Override
    public void testDeferredWriteDatabaseCount() {
    }

    @Override
    public void testDeferredWriteDatabaseCountDups() {
    }

    /*
     * Replication disturbs the cache due to the presence of the feeder and as
     * a consequence invalidates the assumptions underlying the test.
     */
    @Override
    public void testPreloadAllInCache() {
    }

    @Override
    public void testPreloadAllInCacheOffHeap() {
    }

    @Override
    public void testPreloadCacheMemoryLimit() {
    }

    @Override
    public void testPreloadMultipleDatabases() {
    }

    @Override
    public void testPreloadTimeLimit() {
    }

    @Override
    public void testPreloadInternalMemoryLimit() {
    }

    @Override
    public void testPreloadWithProgress() {
    }

    @Override
    public void testDbPreempted() {
    }
}
