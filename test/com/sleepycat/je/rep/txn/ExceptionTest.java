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

package com.sleepycat.je.rep.txn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExceptionTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test for SR23970.
     *
     * A transaction begin should not invalidate the environment if it is
     * stalled in a beginTransaction while trying to get a quorum of replicas.
     * The transaction will fail later, without invalidating the environment,
     * when it tries to acquire locks and discovers that the environment has
     * been closed.
     */
    @Test
    public void test() throws InterruptedException {

        repEnvInfo[0].getRepConfig().
            setConfigParam(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT,
                           "60 s");
        createGroup(2);
        final ReplicatedEnvironment menv = repEnvInfo[0].getEnv();
        assertEquals(menv.getState(), State.MASTER);

        /* Shutdown the second RN so that the beginTransaction() below stalls */
        repEnvInfo[1].closeEnv();

        final AtomicReference<Exception> te = new AtomicReference<Exception>();

        final Thread t = new Thread () {
            @Override
            public void run() {
                Transaction txn = null;
                try {
                    txn = menv.beginTransaction(null,
                                                RepTestUtils.SYNC_SYNC_ALL_TC);
                } catch (Exception e) {
                    /* Test failed if there is an exception on this path. */
                    te.set(e);
                } finally {
                    if (txn != null) {
                        try {
                            txn.abort();
                        } catch (Exception e) {
                            /* Ignore it */
                        }
                    }
                }
            }
        };

        t.start();

        /* Let it get to the beginTransaction. */
        Thread.sleep(2000);

        try {
            menv.close();
        } catch (Exception e) {
            /*
             * The abort above may not execute in time leaving an unclosed txn.
             * Ignore the resulting exception.
             */
        }

        t.join(10000);

        assertTrue(menv.getInvalidatingException() == null);
    }
}
