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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.StatGroup;

/**
 * An exclusive latch without stats.
 *
 * SharedLatch (not just Latch) is implemented to support exclusive-only BIN
 * latches.
 */
@SuppressWarnings("serial")
public class LatchImpl extends ReentrantLock implements SharedLatch {

    private final LatchContext context;
    private OwnerInfo lastOwnerInfo;

    LatchImpl(final LatchContext context) {
        this.context = context;
    }

    String getName() {
        return context.getLatchName();
    }

    @Override
    public boolean isExclusiveOnly() {
        return true;
    }

    @Override
    public void acquireExclusive() {

        if (isHeldByCurrentThread()) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held: " + debugString());
        }

        if (LatchSupport.INTERRUPTIBLE_WITH_TIMEOUT) {
            try {
                if (!tryLock(
                    context.getLatchTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    throw LatchSupport.handleTimeout(this, context);
                }
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(
                    context.getEnvImplForFatalException(), e);
            }
        } else {
            lock();
        }

        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.trackAcquire(this, context);
        }
        if (LatchSupport.CAPTURE_OWNER) {
            lastOwnerInfo = new OwnerInfo(context);
        }
        assert EnvironmentImpl.maybeForceYield();
    }

    @Override
    public boolean acquireExclusiveNoWait() {

        if (isHeldByCurrentThread()) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held: " + debugString());
        }

        if (!tryLock()) {
            return false;
        }

        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.trackAcquire(this, context);
        }
        if (LatchSupport.CAPTURE_OWNER) {
            lastOwnerInfo = new OwnerInfo(context);
        }
        assert EnvironmentImpl.maybeForceYield();
        return true;
    }

    @Override
    public void acquireShared() {
        acquireExclusive();
    }

    @Override
    public void release() {
        if (!isHeldByCurrentThread()) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch not held: " + debugString());
        }
        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.trackRelease(this, context);
        }
        if (LatchSupport.CAPTURE_OWNER) {
            lastOwnerInfo = null;
        }
        unlock();
    }

    @Override
    public void releaseIfOwner() {
        if (!isHeldByCurrentThread()) {
            return;
        }
        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.trackRelease(this, context);
        }
        if (LatchSupport.CAPTURE_OWNER) {
            lastOwnerInfo = null;
        }
        unlock();
    }

    @Override
    public boolean isOwner() {
        return isHeldByCurrentThread();
    }

    @Override
    public boolean isExclusiveOwner() {
        return isHeldByCurrentThread();
    }

    @Override
    public Thread getExclusiveOwner() {
        return getOwner();
    }

    @Override
    public int getNWaiters() {
        return getQueueLength();
    }

    @Override
    public StatGroup getStats() {
        throw unexpectedState();
    }

    @Override
    public void clearStats() {
        throw unexpectedState();
    }

    @Override
    public String toString() {
        return LatchSupport.toString(this, context, lastOwnerInfo);
    }

    @Override
    public String debugString() {
        return LatchSupport.debugString(this, context, lastOwnerInfo);
    }
}
