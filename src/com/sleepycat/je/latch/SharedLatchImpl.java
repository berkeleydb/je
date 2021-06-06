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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.StatGroup;

@SuppressWarnings("serial")
public class SharedLatchImpl extends ReentrantReadWriteLock
    implements SharedLatch {

    private final LatchContext context;
    private OwnerInfo lastOwnerInfo;

    SharedLatchImpl(final boolean fair, final LatchContext context) {
        super(fair);
        this.context = context;
    }

    @Override
    public boolean isExclusiveOnly() {
        return false;
    }

    @Override
    public void acquireExclusive() {
        doAcquireExclusive(false /*noWait*/);
    }

    @Override
    public boolean acquireExclusiveNoWait() {
        return doAcquireExclusive(true /*noWait*/);
    }

    private boolean doAcquireExclusive(final boolean noWait) {
        if (isWriteLockedByCurrentThread() || (getReadHoldCount() > 0)) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held: " + debugString());
        }

        if (noWait) {
            if (!writeLock().tryLock()) {
                return false;
            }
        } else if (LatchSupport.INTERRUPTIBLE_WITH_TIMEOUT) {
            try {
                if (!writeLock().tryLock(
                    context.getLatchTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    throw LatchSupport.handleTimeout(this, context);
                }
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(
                    context.getEnvImplForFatalException(), e);
            }
        } else {
            writeLock().lock();
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
        if (isWriteLockedByCurrentThread()) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held exclusively: " + debugString());
        }

        if (getReadHoldCount() > 0) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held non-exclusively: " + debugString());
        }

        if (LatchSupport.INTERRUPTIBLE_WITH_TIMEOUT) {
            try {
                if (!readLock().tryLock(
                    context.getLatchTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    throw LatchSupport.handleTimeout(this, context);
                }
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(
                    context.getEnvImplForFatalException(), e);
            }
        } else {
            readLock().lock();
        }
        if (LatchSupport.TRACK_LATCHES) {
            LatchSupport.trackAcquire(this, context);
        }
        assert EnvironmentImpl.maybeForceYield();
    }

    @Override
    public void release() {
        doRelease(false /*ifOwner*/);
    }

    @Override
    public void releaseIfOwner() {
        doRelease(true /*ifOwner*/);
    }

    private void doRelease(final boolean ifOwner) {
        if (getReadHoldCount() > 0) {
            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.trackRelease(this, context);
            }
            readLock().unlock();
            return;
        }
        if (isWriteLockedByCurrentThread()) {
            if (LatchSupport.CAPTURE_OWNER) {
                lastOwnerInfo = null;
            }
            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.trackRelease(this, context);
            }
            writeLock().unlock();
            return;
        }
        if (!ifOwner) {
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch not held: " + debugString());
        }
    }

    @Override
    public Thread getExclusiveOwner() {
        return getOwner();
    }

    @Override
    public boolean isExclusiveOwner() {
        return isWriteLockedByCurrentThread();
    }

    @Override
    public boolean isOwner() {
        return isWriteLockedByCurrentThread() || (getReadHoldCount() > 0);
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
