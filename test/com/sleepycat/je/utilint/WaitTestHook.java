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

import java.util.logging.Logger;

import com.sleepycat.je.utilint.LoggerUtils;

/** Define a test hook for coordinating waiting. */
public class WaitTestHook<T> extends TestHookAdapter<T> {

    /** Logger for this class. */
    protected final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

    /** Whether the hook is waiting. */
    private boolean waiting = false;

    /** Whether the hook should stop waiting. */
    private boolean stopWaiting = false;

    /**
     * Creates a test hook that will cause {@link #awaitWaiting} to stop
     * waiting when it starts waiting, and will itself stop waiting when {@link
     * #stopWaiting()} is called.
     */
    public WaitTestHook() { }

    /**
     * Assert that the test hook is called and begins waiting within the
     * specified number of milliseconds.
     */
    public synchronized void awaitWaiting(final long timeout)
        throws InterruptedException {

        final long start = System.currentTimeMillis();
        while (!waiting && (start + timeout > System.currentTimeMillis())) {
            wait(10000);
        }
        logger.info(this + ": Awaited waiting for " +
                    (System.currentTimeMillis() - start) + " milliseconds");
        assertTrue(this + ": Should be waiting", waiting);
    }

    /**
     * Tell the test hook to stop waiting, asserting that it has started
     * waiting.
     */
    public synchronized void stopWaiting() {
        assertTrue(this + ": Should be waiting", waiting);
        stopWaiting = true;
        notifyAll();
        logger.info(this + ": Stopped waiting");
    }

    /** Wait until {@link #stopWaiting()} is called. */
    @Override
    public synchronized void doHook() {
        waiting = true;
        notifyAll();
        logger.info(this + ": Now waiting");
        while (!stopWaiting) {
            try {
                wait(10000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * Wait until {@link #stopWaiting()} is called, regardless of the argument.
     */
    @Override
    public void doHook(T obj) {
        doHook();
    }
}
