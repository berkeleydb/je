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

package com.sleepycat.je.log;

import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_FSYNCS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_FSYNC_REQUESTS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_TIMEOUTS;
import static com.sleepycat.je.log.LogStatDefinition.GRPCMGR_N_GROUP_COMMIT_REQUESTS;
import static com.sleepycat.je.log.LogStatDefinition.GRPCMGR_N_GROUP_COMMIT_WAITS;
import static com.sleepycat.je.log.LogStatDefinition.GRPCMGR_N_LOG_INTERVAL_EXCEEDED;
import static com.sleepycat.je.log.LogStatDefinition.GRPCMGR_N_LOG_MAX_GROUP_COMMIT;

import java.util.concurrent.CountDownLatch;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/*
 * The FsyncManager ensures that only one file fsync is issued at a time, for
 * performance optimization. The goal is to reduce the number of fsyncs issued
 * by the system by issuing 1 fsync on behalf of a number of threads.
 *
 * For example, suppose these writes happen which all need to be fsynced to
 * disk:
 *
 *  thread 1 writes a commit record
 *  thread 2 writes a checkpoint
 *  thread 3 writes a commit record
 *  thread 4 writes a commit record
 *  thread 5 writes a checkpoint
 *
 * Rather than executing 5 fsyncs, which all must happen synchronously, we hope
 * to issue fewer. How many fewer depend on timing. Note that the writes
 * themselves are serialized and are guaranteed to run in order.
 *
 * For example:
 *    thread 1 wants to fsync first, no other fsync going on, will issue fsync
 *    thread 2 waits
 *    thread 3 waits
 *    thread 4 waits
 *     - before thread 5 comes, thread 1 finishes fsyncing and returns to
 *     the caller. Now another fsync can be issued that will cover threads
 *     2,3,4. One of those threads (2, 3, 4} issues the fsync, the others
 *     block.
 *    thread 5 wants to fsync, but sees one going on, so will wait.
 *     - the fsync issued for 2,3,4 can't cover thread 5 because we're not sure
 *      if thread 5's write finished before that fsync call. Thread 5 will have
 *      to issue its own fsync.
 *
 * Target file
 * -----------
 * Note that when the buffer pool starts a new file, we fsync the previous file
 * under the log write latch. Therefore, at any time we only have one target
 * file to fsync, which is the current write buffer. We do this so that we
 * don't have to coordinate between files.  For example, suppose log files have
 * 1000 bytes and a commit record is 10 bytes.  An LSN of value 6/990 is in
 * file 6 at offset 990.
 *
 * thread 1: logWriteLatch.acquire()
 *         write commit record to LSN 6/980
 *         logWriteLatch.release()
 * thread 2: logWriteLatch.acquire()
 *         write commit record to LSN 6/990
 *         logWriteLatch.release
 * thread 3: logWriteLatch.acquire()
 *         gets 7/000 as the next LSN to use
 *          see that we flipped to a new file, so call fsync on file 6
 *         write commit record to LSN 7/000
 *         logWriteLatch.release()
 *
 * Thread 3 will fsync file 6 within the log write latch. That way, at any
 * time, any non-latched fsyncs should only fsync the latest file.  If we
 * didn't do, there's the chance that thread 3 would fsync file 7 and return to
 * its caller before the thread 1 and 2 got an fsync for file 6. That wouldn't
 * be correct, because thread 3's commit might depend on file 6.
 *
 * Note that the FileManager keeps a file descriptor that corresponds to the
 * current end of file, and that is what we fsync.
 */
class FSyncManager {
    private final EnvironmentImpl envImpl;
    private final long timeout;

    /* Use as the target for a synchronization block. */
    private final Object mgrMutex;

    private volatile boolean workInProgress;
    private FSyncGroup nextFSyncWaiters;
    private int numNextWaiters;
    private long startNextWait;
    /* Number of waiters that prevents a group commit wait */
    private final int grpcThreshold;
    private final long grpcInterval;
    private final boolean grpWaitOn;

    /* stats */
    private final StatGroup stats;
    private final LongStat nFSyncRequests;
    private final AtomicLongStat nFSyncs;
    private final LongStat nTimeouts;
    private final LongStat nRequests;
    private final LongStat nWaitersExceeded;
    private final LongStat nTimeExceeded;
    private final LongStat nWaits;

    /* For unit tests. */
    private TestHook<CountDownLatch> flushHook;

    FSyncManager(EnvironmentImpl envImpl) {
        timeout = envImpl.getConfigManager().getDuration
            (EnvironmentParams.LOG_FSYNC_TIMEOUT);
        grpcInterval = envImpl.getConfigManager().getDurationNS(
            EnvironmentParams.LOG_GROUP_COMMIT_INTERVAL);
        grpcThreshold =
            envImpl.getConfigManager().getInt(
                EnvironmentParams.LOG_GROUP_COMMIT_THRESHOLD);
        if (grpcInterval == 0 || grpcThreshold == 0) {
            grpWaitOn = false;
        } else {
            grpWaitOn = true;
        }

        this.envImpl = envImpl;

        mgrMutex = new Object();
        workInProgress = false;
        nextFSyncWaiters = new FSyncGroup(timeout, envImpl);

        stats = new StatGroup(LogStatDefinition.FSYNCMGR_GROUP_NAME,
                              LogStatDefinition.FSYNCMGR_GROUP_DESC);
        nFSyncRequests = new LongStat(stats, FSYNCMGR_FSYNC_REQUESTS);
        nFSyncs = new AtomicLongStat(stats, FSYNCMGR_FSYNCS);
        nTimeouts = new LongStat(stats, FSYNCMGR_TIMEOUTS);
        nRequests = new LongStat(stats, GRPCMGR_N_GROUP_COMMIT_REQUESTS);
        nTimeExceeded =
            new LongStat(stats, GRPCMGR_N_LOG_INTERVAL_EXCEEDED);
        nWaitersExceeded =
            new LongStat(stats, GRPCMGR_N_LOG_MAX_GROUP_COMMIT);
        nWaits =
            new LongStat(stats, GRPCMGR_N_GROUP_COMMIT_WAITS);
        numNextWaiters = 0;
    }

    /**
     * Request to flush the log buffer and optionally fsync to disk.
     * This thread may or may not actually execute the flush/fsync,
     * but will not return until a flush/fsync has been
     * issued and executed on behalf of its write. There is a timeout period
     * specified by EnvironmentParam.LOG_FSYNC_TIMEOUT that ensures that no
     * thread gets stuck here indefinitely.
     *
     * When a thread comes in, it will find one of two things.
     * 1. There is no work going on right now. This thread should go
     *    ahead and become the group leader. The leader may wait and
     *    executes the flush/fsync.
     * 2. There is work going on, wait on the next group commit.
     *
     * When a work is going on, all those threads that come along are grouped
     * together as the nextFsyncWaiters. When the current work is finished,
     * one of those nextFsyncWaiters will be selected as a leader to issue the
     * next flush/fsync. The other members of the group will merely wait until
     * the flush/fsync done on their behalf is finished.
     *
     * When a thread finishes a flush/fsync, it has to:
     * 1. wake up all the threads that were waiting in the group.
     * 2. wake up one member of the next group of waiting threads (the
     *    nextFsyncWaiters) so that thread can become the new leader
     *    and issue the next flush/fysnc call.
     *
     * If a non-leader member of the nextFsyncWaiters times out, it will issue
     * its own flush/fsync anyway, in case something happened to the leader.
     *
     * @param fsyncRequired true if fsync is required
     * @throws DatabaseException
     */
    void flushAndSync(boolean fsyncRequired)
        throws DatabaseException {

        long interval;
        boolean doWork = false;
        boolean isLeader = false;
        boolean needToWait = false;
        FSyncGroup inProgressGroup = null;
        FSyncGroup myGroup = null;

        synchronized (mgrMutex) {
            nRequests.increment();
            if (fsyncRequired) {
                nFSyncRequests.increment();
            }
            myGroup = nextFSyncWaiters;
            myGroup.setDoFsync(fsyncRequired);

            /* Figure out if we're calling fsync or waiting. */
            if (workInProgress) {
                needToWait = true;
                numNextWaiters++;
                if (grpWaitOn && numNextWaiters == 1) {
                    startNextWait = System.nanoTime();
                }
            } else {
                isLeader = true;
                doWork = true;
                workInProgress = true;
                if (grpWaitOn) {
                    if (numNextWaiters < grpcThreshold) {
                        interval = System.nanoTime() - startNextWait;
                        if (interval < grpcInterval) {
                            try {
                                nWaits.increment();
                                mgrMutex.wait(interval/1000000,
                                              (int) interval%1000000);
                            } catch (InterruptedException e) {
                                throw new ThreadInterruptedException(
                                    envImpl,
                                    "Unexpected interrupt while " +
                                    "waiting for write or fsync",
                                    e);
                            }
                        }
                        nTimeExceeded.increment();
                    } else {
                        nWaitersExceeded.increment();
                    }
                }
                inProgressGroup = nextFSyncWaiters;
                nextFSyncWaiters = new FSyncGroup(timeout, envImpl);
                numNextWaiters = 0;
            }
        }

        if (needToWait) {

            /*
             * Note that there's no problem if we miss the notify on this set
             * of waiters. We can check state in the FSyncGroup before we begin
             * to wait.
             *
             * All members of the group may return from their waitForFSync()
             * call with the need to do a fsync, because of timeout. Only one
             * will return as the leader.
             */
            int waitStatus = myGroup.waitForEvent();

            if (waitStatus == FSyncGroup.DO_LEADER_FSYNC) {
                synchronized (mgrMutex) {

                    /*
                     * Check if there's a fsync in progress; this might happen
                     * even if you were designated the leader if a new thread
                     * came in between the point when the old leader woke you
                     * up and now. This new thread may have found that there
                     * was no fsync in progress, and may have started a fsync.
                     */
                    if (workInProgress) {

                        /*
                         * Ensure that an fsync is done before returning by
                         * forcing an fsync in this thread. [#20717]
                         */
                        doWork = true;
                    } else {
                        isLeader = true;
                        doWork = true;
                        workInProgress = true;

                        if (grpWaitOn) {
                            if (numNextWaiters < grpcThreshold) {
                                interval = System.nanoTime() - startNextWait;
                                if (interval < grpcInterval) {
                                    try {
                                        nWaits.increment();
                                        mgrMutex.wait(interval/1000000,
                                                      (int) interval%1000000);
                                    } catch (InterruptedException e) {
                                        throw new ThreadInterruptedException(
                                            envImpl,
                                            "Unexpected interrupt while " +
                                            "waiting for write or fsync",
                                            e);
                                    }
                                }
                                nTimeExceeded.increment();
                            } else {
                                nWaitersExceeded.increment();
                            }
                        }
                        inProgressGroup = myGroup;
                        nextFSyncWaiters = new FSyncGroup(timeout, envImpl);
                        numNextWaiters = 0;
                    }
                }
            } else if (waitStatus == FSyncGroup.DO_TIMEOUT_FSYNC) {
                doWork = true;
                synchronized (mgrMutex) {
                    nTimeouts.increment();
                }
            }
        }

        if (doWork) {

            /*
             * There are 3 ways that this fsync gets called:
             *
             * 1. A thread calls sync and there is not a sync call already in
             * progress.  That thread executes fsync for itself only.  Other
             * threads requesting sync form a group of waiters.
             *
             * 2. A sync finishes and wakes up a group of waiters.  The first
             * waiter in the group to wake up becomes the leader.  It executes
             * sync for it's group of waiters.  As above, other threads
             * requesting sync form a new group of waiters.
             *
             * 3. If members of a group of waiters have timed out, they'll all
             * just go and do their own sync for themselves.
             */

            /* flush the log buffer */
            if (myGroup.getDoFsync()) {
                envImpl.getLogManager().flushBeforeSync();
            } else {
                envImpl.getLogManager().flushNoSync();
            }

            TestHookExecute.doHookIfSet(flushHook);

            /* execute fsync */
            if (myGroup.getDoFsync()) {
                executeFSync();
                nFSyncs.increment();
            }

            synchronized (mgrMutex) {
                if (isLeader) {

                    /*
                     * Wake up the group that requested the fsync before you
                     * started. They've piggybacked off your fsync.
                     */
                    inProgressGroup.wakeupAll();

                    /*
                     * Wake up a single waiter, who will become the next
                     * leader.
                     */
                    nextFSyncWaiters.wakeupOne();
                    workInProgress = false;
                }
            }
        }
    }

    /*
     * Stats.
     */
    long getNFSyncRequests() {
        return nFSyncRequests.get();
    }

    long getNFSyncs() {
        return nFSyncs.get();
    }

    long getNTimeouts() {
        return nTimeouts.get();
    }

    StatGroup loadStats(StatsConfig config) {
        return stats.cloneGroup(config.getClear());
    }

    /**
     * Put the fsync execution into this method so it can be overridden for
     * testing purposes.
     */
    protected void executeFSync()
        throws DatabaseException {

        envImpl.getFileManager().syncLogEnd();
    }

    /* For unit testing only. */
    public void setFlushLogHook(TestHook<CountDownLatch> hook) {
        flushHook = hook;
    }

    /*
     * Embodies a group of threads waiting for a common fsync. Note that
     * there's no collection here; group membership is merely that the threads
     * are all waiting on the same monitor.
     */
    static class FSyncGroup {
        static int DO_TIMEOUT_FSYNC = 0;
        static int DO_LEADER_FSYNC = 1;
        static int NO_FSYNC_NEEDED = 2;

        private volatile boolean doFsync = false;
        private volatile boolean workDone;
        private final long fsyncTimeout;
        private boolean leaderExists;
        private final EnvironmentImpl envImpl;

        FSyncGroup(long fsyncTimeout, EnvironmentImpl envImpl) {
            this.fsyncTimeout = fsyncTimeout;
            workDone = false;
            leaderExists = false;
            this.envImpl = envImpl;
        }

        synchronized boolean getLeader() {
            if (workDone) {
                return false;
            } else {
                if (leaderExists) {
                    return false;
                } else {
                    leaderExists = true;
                    return true;
                }
            }
        }

        /**
         * Wait for either a turn to execute a fsync, or to find out that a
         * fsync was done on your behalf.
         *
         * @return true if the fsync wasn't done, and this thread needs to
         * execute a fsync when it wakes up. This may be true because it's the
         * leader of its group, or because the wait timed out.
         */
        synchronized int waitForEvent()
            throws ThreadInterruptedException {

            int status = NO_FSYNC_NEEDED;

            if (!workDone) {
                long startTime = System.currentTimeMillis();
                while (true) {

                    try {
                        wait(fsyncTimeout);
                    } catch (InterruptedException e) {
                        throw new ThreadInterruptedException(envImpl,
                           "Unexpected interrupt while waiting "+
                           "for write or fsync", e);
                    }

                    /*
                     * This thread was awoken either by a timeout, by a notify,
                     * or by an interrupt. Is the fsync done?
                     */
                    if (workDone) {
                        /* The fsync we're waiting on is done, leave. */
                        status = NO_FSYNC_NEEDED;
                        break;
                    } else {

                        /*
                         * The fsync is not done -- were we woken up to become
                         * the leader?
                         */
                        if (!leaderExists) {
                            leaderExists = true;
                            status = DO_LEADER_FSYNC;
                            break;
                        } else {

                            /*
                             * We're just a waiter. See if we're timed out or
                             * have more to wait.
                             */
                            long now = System.currentTimeMillis();
                            if ((now - startTime) > fsyncTimeout) {
                                /* we timed out. */
                                status = DO_TIMEOUT_FSYNC;
                                break;
                            }
                        }
                    }
                }
            }

            return status;
        }

        synchronized void setDoFsync(boolean doSync) {
            this.doFsync |= doSync;
        }

        synchronized boolean getDoFsync() {
            return doFsync;
        }

        synchronized void wakeupAll() {
            workDone = true;
            notifyAll();
        }

        synchronized void wakeupOne() {
            /* FindBugs whines here. */
            notify();
        }
    }
}
