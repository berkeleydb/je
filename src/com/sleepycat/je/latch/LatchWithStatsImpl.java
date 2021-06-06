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
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_CONTENTION;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_NOWAIT_SUCCESS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_NOWAIT_UNSUCCESS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_NO_WAITERS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_RELEASES;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_SELF_OWNED;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * An exclusive latch with stats.
 */
@SuppressWarnings("serial")
public class LatchWithStatsImpl extends ReentrantLock implements Latch {

    private final LatchContext context;
    private OwnerInfo lastOwnerInfo;
    private final StatGroup stats;
    private final IntStat nAcquiresNoWaiters;
    private final IntStat nAcquiresSelfOwned;
    private final IntStat nAcquiresWithContention;
    private final IntStat nAcquiresNoWaitSuccessful;
    private final IntStat nAcquiresNoWaitUnsuccessful;
    private final IntStat nReleases;

    LatchWithStatsImpl(final LatchContext context) {
        this.context = context;

        stats = new StatGroup(
            LatchStatDefinition.GROUP_NAME,
            LatchStatDefinition.GROUP_DESC);
        nAcquiresNoWaiters = new IntStat(stats, LATCH_NO_WAITERS);
        nAcquiresSelfOwned = new IntStat(stats, LATCH_SELF_OWNED);
        nAcquiresWithContention = new IntStat(stats, LATCH_CONTENTION);
        nAcquiresNoWaitSuccessful = new IntStat(stats, LATCH_NOWAIT_SUCCESS);
        nAcquiresNoWaitUnsuccessful =
            new IntStat(stats, LATCH_NOWAIT_UNSUCCESS);
        nReleases = new IntStat(stats, LATCH_RELEASES);
    }

    String getName() {
        return context.getLatchName();
    }

    @Override
    public void acquireExclusive() {

        if (isHeldByCurrentThread()) {
            nAcquiresSelfOwned.increment();
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held: " + debugString());
        }

        if (isLocked()) {
            nAcquiresWithContention.increment();
        } else {
            nAcquiresNoWaiters.increment();
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
            nAcquiresSelfOwned.increment();
            throw unexpectedState(
                context.getEnvImplForFatalException(),
                "Latch already held: " + debugString());
        }

        if (!tryLock()) {
            nAcquiresNoWaitUnsuccessful.increment();
            return false;
        }

        nAcquiresNoWaitSuccessful.increment();

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
        nReleases.increment();
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
        nReleases.increment();
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
        return stats;
    }

    @Override
    public void clearStats() {
        stats.clear();
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
