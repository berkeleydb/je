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

package com.sleepycat.je.junit;

import junit.framework.Assert;

/**
 * JUnitThread is a utility class that allows JUnit assertions to be
 * run in other threads.  A JUnit assertion thrown from a
 * thread other than the invoking one can not be caught by JUnit.
 * This class allows these AssertionFailedErrors to be caught and
 * passed back to the original thread.
 * <p>
 * To use, create a JUnitThread and override the testBody() method with
 * the test code.  Then call doTest() on the thread to run the test
 * and re-throw any assertion failures that were thrown by the
 * subthread.
 * <p>
 * Example:
 * <pre>
    public void testEquality() {
    JUnitThread tester =
    new JUnitThread("testEquality") {
    public void testBody() {
    int one = 1;
    assertTrue(one == 1);
    }
    };
    tester.doTest();
    }
 * </pre>
 */
public class JUnitThread extends Thread {
    private Throwable errorReturn;

    /**
     * Construct a new JUnitThread.
     */
    public JUnitThread(String name) {
        super(name);
    }

    @Override
    public void run() {
        try {
            testBody();
        } catch (Throwable T) {
            errorReturn = T;
        }
    }

    /**
     * Method that is to be overridden by the user.  Code should be
     * the guts of the test.  assertXXXX() methods may be called in
     * this method.
     * @throws Throwable in subclasses.
     */
    public void testBody()
        throws Throwable {
    }

    /**
     * This method should be called after the JUnitThread has been
     * constructed to cause the actual test to be run and any failures
     * to be returned.
     */
    public void doTest()
        throws Throwable {

        start();
        finishTest();
    }

    /**
     * This method should be called after the JUnitThread has been
     * started to cause the test to report any failures.
     */
    public void finishTest()
        throws Throwable {

        try {
            join();
        } catch (InterruptedException IE) {
            Assert.fail("caught unexpected InterruptedException");
        }
        if (errorReturn != null) {
            throw new RuntimeException(
                "Test failed in JUnitThread, see nested exception.\n",
                errorReturn); 
        }
    }

    /**
     * Attempt to kill a thread that's still running due to a test problem.
     * Intended to be called during the test tearDown.  In other cases, call
     * {@link #finishTest()} instead.
     */
    @SuppressWarnings("deprecation")
    public void shutdown() {

        final long maxTime = System.currentTimeMillis() + (30 * 1000);

        while (isAlive() &&
               System.currentTimeMillis() < maxTime) {
            interrupt();
            yield();
        }

        if (isAlive()) {
            /* Although unsafe, it's best to stop the thread in a test. */
            stop();
        }
    }

    @Override
    public String toString() {
        return "<JUnitThread: " + super.toString() + ">";
    }
}
