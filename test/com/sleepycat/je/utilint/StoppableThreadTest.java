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

package com.sleepycat.je.utilint;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.EnvironmentWedgedException;
import com.sleepycat.je.rep.impl.RepTestBase;

/**
 * Tests to verify that StoppableThread shutdown works as expected.
 */
public class StoppableThreadTest extends RepTestBase {

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

    @Test
    public void testBasic() {
        createGroup(3);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final StoppableThread testThread =
            new StoppableThread(repEnvInfo[0].getRepImpl(), "test)") {

            @Override
            protected Logger getLogger() {
              return null;
            }

            @Override
            protected int initiateSoftShutdown() {
                return -1;
            }

            @Override
            public void run() {
                while (!stop.get()) {
                  /* loop uninterruptibly to simulate a runaway thread */
                }
            }
        };

        testThread.start();

        /* It should fail to stop the thread and invalidate the environment. */
        testThread.shutdownThread(Logger.getLogger("test"));

        /* Looping thread is still alive. */
        assertTrue(testThread.isAlive());

        /* Environment has been invalidated. */
        assertTrue(!repEnvInfo[0].getEnv().isValid());

        stop.set(true);
        boolean isDead = new PollCondition(100, 10000) {

            @Override
            protected boolean condition() {
                return !testThread.isAlive();
            }
        }.await();

        assertTrue(isDead);

        /* Close the invalidated environment. */
        try {
            repEnvInfo[0].getRepImpl().close();
            fail("Expected EnvironmentWedgedException");
        } catch (EnvironmentWedgedException e) {
            /* Expected. */
        }
    }
}
