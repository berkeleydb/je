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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.logging.Logger;

import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Perform a test action in another thread, providing methods to wait for
 * completion, and to check for success and failure.
 */
public abstract class TestAction extends Thread {

    /** Logger for this class. */
    protected final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

    /** The exception thrown by the action or null. */
    public volatile Throwable exception;

    /** The action */
    protected abstract void action()
        throws Exception;

    /**
     * Assert that the action completed, either by succeeding or throwing an
     * exception, within the specified number of milliseconds.
     *
     * @param timeout the number of milliseconds to wait
     * @throws InterruptedException if waiting is interrupted
     */
    public void assertCompleted(final long timeout)
        throws InterruptedException {

        join(timeout);
        assertTrue("Thread should have completed", !isAlive());
    }

    /**
     * Assert that the action completed successfully within the specified
     * number of milliseconds.
     *
     * @param timeout the number of milliseconds to wait
     * @throws InterruptedException if waiting is interrupted
     */
    public void assertSucceeded(final long timeout)
        throws InterruptedException {

        assertCompleted(timeout);
        if (exception != null) {
            final AssertionError err =
                new AssertionError("Unexpected exception: " + exception);
            err.initCause(exception);
            throw err;
        }
    }

    /**
     * Assert that the action failed with an exception of the specified class,
     * or a subclass of it, within the specified number of milliseconds.
     *
     * @param timeout the number of milliseconds to wait
     * @param exceptionClass the exception class
     * @throws InterruptedException if waiting is interrupted
     */
    public void assertException(
        final long timeout,
        final Class<? extends Throwable> exceptionClass)
        throws InterruptedException {

        assertCompleted(timeout);
        assertNotNull("Expected exception", exception);
        if (!exceptionClass.isInstance(exception)) {
            final AssertionError err =
                new AssertionError("Unexpected exception: " + exception);
            err.initCause(exception);
            throw err;
        }
        logger.info("Got expected exception: " + exception);
    }

    /**
     * Call {@link #action} and catch any thrown exceptions.
     */
    @Override
    public void run() {
        try {
            action();
        } catch (Throwable t) {
            exception = t;
        }
    }
}
