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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class ExceptionListenerTest extends TestBase {

    private final File envHome;

    private volatile boolean exceptionThrownCalled = false;

    private DaemonThread dt = null;

    public ExceptionListenerTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testExceptionListener()
        throws Exception {

        /* Open with a listener. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setExceptionListener(new MyExceptionListener());
        envConfig.setAllowCreate(true);
        Environment env = new Environment(envHome, envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        assertSame(envConfig.getExceptionListener(),
                   envImpl.getExceptionListener());

        dt = new MyDaemonThread(0, Environment.CLEANER_NAME, envImpl);
        DaemonThread.stifleExceptionChatter = true;
        dt.runOrPause(true);
        long startTime = System.currentTimeMillis();
        while (!dt.isShutdownRequested() &&
               System.currentTimeMillis() - startTime < 10 * 10000) {
            Thread.yield();
        }
        assertTrue("ExceptionListener apparently not called",
                   exceptionThrownCalled);

        env.close();

        /* Open without a listener. */
        envConfig.setExceptionListener(null);
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        assertNull(envImpl.getExceptionListener());

        /* Set an exception listener. */
        envConfig = env.getConfig();
        exceptionThrownCalled = false;
        envConfig.setExceptionListener(new MyExceptionListener());
        env.setMutableConfig(envConfig);

        assertSame(envConfig.getExceptionListener(),
                   envImpl.getExceptionListener());

        dt = new MyDaemonThread(0, Environment.CLEANER_NAME, envImpl);
        dt.stifleExceptionChatter = true;
        dt.runOrPause(true);
        startTime = System.currentTimeMillis();
        while (!dt.isShutdownRequested() &&
               System.currentTimeMillis() - startTime < 10 * 10000) {
            Thread.yield();
        }
        assertTrue("ExceptionListener apparently not called",
                   exceptionThrownCalled);
    }

    private class MyDaemonThread extends DaemonThread {
        MyDaemonThread(long waitTime, String name, EnvironmentImpl envImpl) {
            super(waitTime, name, envImpl);
        }

        @Override
        protected void onWakeup() {
            throw new RuntimeException("test exception listener");
        }
    }

    private class MyExceptionListener implements ExceptionListener {
        public void exceptionThrown(ExceptionEvent event) {
            assertEquals("daemonName should be CLEANER_NAME",
                         Environment.CLEANER_NAME,
                         event.getThreadName());

	    /*
	     * Be sure to set the exceptionThrownFlag before calling
	     * shutdown, so the main test thread will see the right
	     * value of the flag when it comes out of the loop in
	     * testExceptionList that waits for the daemon thread to
	     * finish.
	     */
            exceptionThrownCalled = true;
            dt.requestShutdown();
        }
    }
}
