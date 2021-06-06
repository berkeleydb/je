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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentWedgedException;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * A StoppableThread is a daemon that obeys the following mandates:
 * - it sets the daemon property for the thread
 * - an uncaught exception handler is always registered
 * - the thread registers with the JE exception listener mechanism.
 * - its shutdown method can only be executed once. StoppableThreads are not
 *   required to implement shutdown() methods, because in some cases their
 *   shutdown processing must be coordinated by an owning, parent thread.
 *
 * StoppableThread is an alternative to the DaemonThread. It also assumes that
 * the thread's run() method may be more complex than that of the work-queue,
 * task oriented DaemonThread.
 *
 * A StoppableThread's run method should catch and handle all exceptions. By
 * default, unhandled exceptions are considered programming errors, and
 * invalidate the environment, but StoppableThreads may supply alternative
 * uncaught exception handling.
 *
 * StoppableThreads usually are created with an EnvironmentImpl, but on
 * occasion an environment may not be available (for components that can
 * execute without an environment). In that case, the thread obviously does not
 * invalidate the environment.
 *
 * Note that the StoppableThread.cleanup must be invoked upon, or soon after,
 * thread exit.
 */
public abstract class StoppableThread extends Thread {

    /* The environment, if any, that's associated with this thread. */
    protected final EnvironmentImpl envImpl;

    /*
     * Shutdown can only be executed once. The shutdown field protects against
     * multiple invocations.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /* The exception (if any) that forced this node to shut down. */
    private Exception savedShutdownException = null;

    /* Total cpu time used by thread */
    private long totalCpuTime = -1;

    /* Total user time used by thread */
    private long totalUserTime = -1;

    /**
     * The default wait period for an interrupted thread to exit as part of a
     * hard shutdown.
     */
    private static final int DEFAULT_INTERRUPT_WAIT_MS = 10 * 1000;

    /**
     * The wait period for joining a thread in which shutdown is running.
     * Use a large timeout since we want the shutdown to complete normally,
     * if at all possible.
     */
    private static final int WAIT_FOR_SHUTDOWN_MS =
        DEFAULT_INTERRUPT_WAIT_MS * 3;

    protected StoppableThread(final String threadName) {
        this(null, null, null, threadName);
    }

    protected StoppableThread(final EnvironmentImpl envImpl,
                              final String threadName) {
        this(envImpl, null /* handler */, null /* runnable */,threadName);
    }

    protected StoppableThread(final EnvironmentImpl envImpl,
                              final UncaughtExceptionHandler handler,
                              final String threadName) {
        this(envImpl, handler, null /* runnable */, threadName);
    }

    protected StoppableThread(final EnvironmentImpl envImpl,
                              final UncaughtExceptionHandler handler,
                              final Runnable runnable,
                              final String threadName) {
        super(null, runnable, threadName);
        this.envImpl = envImpl;

        /*
         * Set the daemon property so this thread will not hang up the
         * application.
         */
        setDaemon(true);

        setUncaughtExceptionHandler
            ((handler == null) ? new UncaughtHandler() : handler);
    }

    /**
     * @return a logger to use when logging uncaught exceptions.
     */
    abstract protected Logger getLogger();

    /**
     * Returns the exception if any that provoked the shutdown
     *
     * @return the exception, or null if it was a normal shutdown
     */
    public Exception getSavedShutdownException() {
        return savedShutdownException;
    }

    public void saveShutdownException(Exception shutdownException) {
        savedShutdownException = shutdownException;
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * If the shutdown flag is false, set it to true and return false; in this
     * case the caller should perform shutdown, including calling {@link
     * #shutdownThread}. If the shutdown flag is true, wait for this thread to
     * exit and return true; in this case the caller should not perform
     * shutdown.
     *
     * When shutdownDone is initially called by thread X (including from the
     * run method of the thread being shutdown), then a thread Y calling
     * shutdownDone should simply return without performing shutdown (this is
     * when shutdownDone returns true). In this case it is important that this
     * method calls {@link #waitForExit} in thread Y to ensure that thread X
     * really dies, or that an EnvironmentWedgedException is thrown if X does
     * not die. In particular it is important that all JE threads have died and
     * released their resources when Environment.close returns to the app
     * thread, or that EWE is thrown if any JE threads have not died. This
     * allows the app to reliably re-open the env, or exit the process if
     * necessary. [#25648]
     *
     * Note than when thread X has sub-components and manages their threads,
     * thread X's shutdown method will call shutdown for its managed threads.
     * Waiting for exit of thread X will therefore wait for exit of its managed
     * threads, assuming that all shutdown methods calls shutdownDone as
     * described.
     *
     * @param logger the logger on which to log messages
     *
     * @return true if shutdown is already set.
     */
    protected boolean shutdownDone(Logger logger) {

        if (shutdown.compareAndSet(false, true)) {
            return false;
        }

        waitForExit(logger);
        return true;
    }

    /**
     * Must be invoked upon, or soon after, exit from the thread to perform
     * any cleanup, and ensure that any allocated resources are freed.
     */
    protected void cleanup() {
    }

    /*
     * A static method to handle the uncaught exception. This method
     * can be called in other places, such as in FileManager.
     *
     * When an uncaught exception occurs, log it, publish it to the
     * exception handler, and invalidate the environment.
     */
    public static void handleUncaughtException(
        final Logger useLogger,
        final EnvironmentImpl envImpl,
        final Thread t,
        final Throwable e) {

        if (useLogger != null) {
            String envName = (envImpl == null)? "" : envImpl.getName();
            String message = envName + ":" + t.getName() +
                " exited unexpectedly with exception " + e;
            if (e != null) {
                message += LoggerUtils.getStackTrace(e);
            }

            if (envImpl != null) {
                /*
                 * If we have an environment, log this to all three
                 * handlers.
                 */
                LoggerUtils.severe(useLogger, envImpl, message);
            } else {
                /*
                 * We don't have an environment, but at least log this
                 * to the console.
                 */
                useLogger.log(Level.SEVERE, message);
            }
        }


        if (envImpl == null) {
            return;
        }

        /*
         * If not already invalid, invalidate environment by creating an
         * EnvironmentFailureException.
         */
        if (envImpl.isValid()) {

            /*
             * Create the exception to invalidate the environment, but do
             * not throw it since the handle is invoked in some internal
             * JVM thread and the exception is not meaningful to the
             * invoker.
             */
            @SuppressWarnings("unused")
            EnvironmentFailureException unused =
                new EnvironmentFailureException
                    (envImpl, EnvironmentFailureReason.UNCAUGHT_EXCEPTION,
                     e);
        }

        final ExceptionListener exceptionListener =
            envImpl.getExceptionListener();

        if (exceptionListener != null) {
            exceptionListener.exceptionThrown(
                DbInternal.makeExceptionEvent(
                    envImpl.getInvalidatingException(), t.getName()));
        }
    }

    /**
     * An uncaught exception should invalidate the environment. Check if the
     * environmentImpl is null, because there are a few cases where a
     * StoppableThread is created for components that work both in replicated
     * nodes and independently.
     */
    private class UncaughtHandler implements UncaughtExceptionHandler {

        /**
         * When an uncaught exception occurs, log it, publish it to the
         * exception handler, and invalidate the environment.
         */
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            Logger useLogger = getLogger();
            handleUncaughtException(useLogger, envImpl, t, e);
        }
    }

    /**
     * This method is invoked from another thread of control to shutdown this
     * thread. The method tries shutting down the thread using a variety of
     * techniques, starting with the gentler techniques in order to limit of
     * stopping the thread on the overall process and proceeding to harsher
     * techniques:
     *
     * 1) It first tries a "soft" shutdown by invoking
     * <code>initiateSoftShutdown()</code>. This is the technique of choice.
     * Each StoppableThread is expected to make provisions for a clean shutdown
     * via this method. The techniques used to implement this method may vary
     * based upon the specifics of the thread.
     *
     * 2) If that fails it interrupts the thread.
     *
     * 3) If the thread does not respond to the interrupt, it invalidates the
     * environment.
     *
     * All Stoppable threads are expected to catch an interrupt, clean up and
     * then exit. The cleanup may involve invalidation of the environment, if
     * the thread is not in a position to handle the interrupt cleanly.
     *
     * If the method has to resort to step 3, it means that thread and other
     * resources may not have been freed and it would be best to exit and
     * restart the process itself to ensure they are freed. In this case an
     * EnvironmentWedgedException is used to invalidate the env, and the EWE
     * will be thrown when the app calls Environment.close.
     *
     * @param logger the logger on which to log messages
     */
    public void shutdownThread(Logger logger) {

        /*
         * Save resource usage, since it will not be available once the
         * thread has exited.
         */
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        if (threadBean.isThreadCpuTimeSupported()) {
            totalCpuTime = threadBean.getThreadCpuTime(getId());
            totalUserTime = threadBean.getThreadUserTime(getId());
        } else if (threadBean.isCurrentThreadCpuTimeSupported() &&
                   Thread.currentThread() == this) {
            totalCpuTime = threadBean.getCurrentThreadCpuTime();
            totalUserTime = threadBean.getCurrentThreadUserTime();
        }

        if (Thread.currentThread() == this) {
            /* Shutdown was called from this thread's run method. */
            return;
        }

        try {
            LoggerUtils.info(logger, envImpl,
                             getName() + " soft shutdown initiated.");

            final int waitMs = initiateSoftShutdown();

            /*
             * Wait for a soft shutdown to take effect, the preferred method
             * for thread shutdown.
             */
            if (waitMs >= 0) {
                join(waitMs);
            }

            if (!isAlive()) {
                LoggerUtils.fine(logger, envImpl, this + " has exited.");
                return;
            }

            LoggerUtils.warning(
                logger, envImpl,
                "Soft shutdown failed for thread:" + this +
                    " after waiting for " +  waitMs +
                    "ms resorting to interrupt.");

            interrupt();

            /*
             * The thread must make provision to handle and exit on an
             * interrupt.
             */
            final long joinWaitTime =
                (waitMs > 0) ? 2 * waitMs : DEFAULT_INTERRUPT_WAIT_MS;

            join(joinWaitTime);

            if (!isAlive()) {
                LoggerUtils.warning(logger, envImpl,
                                    this + " shutdown via interrupt.");
                return;
            }

            /*
             * Failed to shutdown thread despite all attempts. It's
             * possible that the thread has a bug and/or is unable to
             * to get to an interruptible point.
             */
            final String msg = this +
                " shutdown via interrupt FAILED. " +
                "Thread still alive despite waiting for " +
                joinWaitTime + "ms.";

            LoggerUtils.severe(logger, envImpl, msg);
            LoggerUtils.fullThreadDump(logger, envImpl, Level.SEVERE);

            if (envImpl != null) {
                @SuppressWarnings("unused")
                EnvironmentFailureException unused =
                    new EnvironmentWedgedException(envImpl, msg);
            }
        } catch (InterruptedException e1) {
            LoggerUtils.warning(
                logger, envImpl,
                "Interrupted while shutting down thread:" + this);
        }
    }

    /**
     * Used to wait for thread shutdown, when {@link #shutdownDone} returns
     * true because it has been called by another thread.
     */
    private void waitForExit(Logger logger) {

        assert shutdown.get();

        if (Thread.currentThread() == this) {
            /* Shutdown was called from this thread's run method. */
            return;
        }

        try {
            join(WAIT_FOR_SHUTDOWN_MS);

            if (!isAlive()) {
                return;
            }

            /*
             * For some reason, shutdown has not finished. This is unlikely,
             * but possible. As in shutdownThread, we try interrupting the
             * thread before giving up.
             */
            LoggerUtils.warning(
                logger, envImpl,
                "Soft shutdown failed for thread:" + this +
                    " after waiting for " + WAIT_FOR_SHUTDOWN_MS +
                    "ms, resorting to interrupt in wait-for-shutdown.");

            interrupt();
            join(WAIT_FOR_SHUTDOWN_MS);

            if (!isAlive()) {
                return;
            }

            /*
             * Failed to shutdown thread despite all attempts. It's
             * possible that the thread has a bug and/or is unable to
             * to get to an interruptible point.
             */
            final String msg = this +
                " shutdown via interrupt FAILED during wait-for-shutdown. " +
                "Thread still alive despite waiting for " +
                WAIT_FOR_SHUTDOWN_MS + "ms.";

            LoggerUtils.severe(logger, envImpl, msg);
            LoggerUtils.fullThreadDump(logger, envImpl, Level.SEVERE);

            if (envImpl != null) {
                @SuppressWarnings("unused")
                EnvironmentFailureException unused =
                    new EnvironmentWedgedException(envImpl, msg);
            }
        } catch (InterruptedException e1) {
            LoggerUtils.warning(
                logger, envImpl,
                "Interrupted during wait-for-shutdown:" + this);
        }
    }

    /**
     * Threads that use shutdownThread() must define this method. It's invoked
     * by shutdownThread as an attempt at a soft shutdown.
     *
     * This method makes provisions for this thread to exit on its own. The
     * technique used to make the thread exit can vary based upon the nature of
     * the service being provided by the thread. For example, the thread may be
     * known to poll some shutdown flag on a periodic basis, or it may detect
     * that a channel that it waits on has been closed by this method.
     *
     * @return the amount of time in ms that the shutdownThread method will
     * wait for the thread to exit. A -ve value means that the method will not
     * wait. A zero value means it will wait indefinitely.
     */
    protected int initiateSoftShutdown() {
        return -1;
    }

    /**
     * Returns the total cpu time associated with the thread, after the thread
     * has been shutdown.
     */
    public long getTotalCpuTime() {
        return totalCpuTime;
    }

    /**
     * Returns the total cpu time associated with the thread, after the thread
     * has been shutdown.
     */
    public long getTotalUserTime() {
        return totalUserTime;
    }
}
