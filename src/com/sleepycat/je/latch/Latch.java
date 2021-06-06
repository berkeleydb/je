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

package com.sleepycat.je.latch;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Provides exclusive (mutex-like) latching. This is implemented with Java's
 * ReentrantLock, which is extended for a few reasons:
 * 1) To prevent reentrant use, since latches are not used reentrantly in JE.
 * This increases reliability by detecting accidental reentrant calls.
 * Checks for reentrancy are unconditional, i.e., checked in production.
 * 2) To support instrumentation for debugging (see LatchSupport).
 * 3) Automatic use of configured latch timeout.
 * 4) Built-in thread interrupt handling.
 *
 * Latches are expected to be held for short, defined periods of time.  No
 * deadlock detection is provided so it is the caller's responsibility to
 * sequence latch acquisition in an ordered fashion to avoid deadlocks.
 */
public interface Latch {

    /**
     * Acquires a latch for exclusive/write access.
     *
     * @throws EnvironmentFailureException if the latch is already held by the
     * calling thread.
     */
    void acquireExclusive();

    /**
     * Acquires a latch for exclusive/write access, but do not block if it's
     * not available.
     *
     * @return true if the latch was acquired, false if it is not available.
     *
     * @throws EnvironmentFailureException if the latch is already held by the
     * calling thread.
     */
    boolean acquireExclusiveNoWait();

    /**
     * Releases the latch.  If there are other thread(s) waiting for the latch,
     * they are woken up and granted the latch.
     *
     * @throws EnvironmentFailureException if the latch is not currently held.
     */
    void release();

    /**
     * Releases the latch.  If there are other thread(s) waiting for the latch,
     * one is woken up and granted the latch. If the latch was not owned by
     * the caller, just return.
     */
    void releaseIfOwner();

    /**
     * Returns true if the current thread holds this latch. For an exclusive
     * latch, is equivalent to calling {@link #isExclusiveOwner()}.
     */
    boolean isOwner();

    /**
     * Returns true if the current thread holds this latch for exclusive/write
     * access.
     */
    boolean isExclusiveOwner();

    /**
     * Returns the thread that currently holds the latch for exclusive access.
     */
    Thread getExclusiveOwner();

    /**
     * Returns an estimate of the number of threads waiting.
     */
    int getNWaiters();

    /**
     * Returns a stats group with information about this latch.
     *
     * @throws EnvironmentFailureException if stats were not requested when the
     * latch was created.  See LatchFactory.
     */
    StatGroup getStats();

    /**
     * Resets collected stat values to zero.
     *
     * @throws EnvironmentFailureException if stats were not requested when the
     * latch was created.  See LatchFactory.
     */
    void clearStats();

    /**
     * Returns the latch name and exclusive owner info.
     */
    @Override
    String toString();

    /**
     * Returns the same information as {@link #toString()} plus all known debug
     * info.
     */
    String debugString();
}
