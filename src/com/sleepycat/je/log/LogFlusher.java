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

import static com.sleepycat.je.config.EnvironmentParams.LOG_FLUSH_NO_SYNC_INTERVAL;
import static com.sleepycat.je.config.EnvironmentParams.LOG_FLUSH_SYNC_INTERVAL;
import static com.sleepycat.je.config.EnvironmentParams.OLD_REP_LOG_FLUSH_TASK_INTERVAL;
import static com.sleepycat.je.config.EnvironmentParams.OLD_REP_RUN_LOG_FLUSH_TASK;

import java.util.Timer;
import java.util.TimerTask;

import com.sleepycat.je.Environment;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * Flush the log buffers (and write queue) periodically to disk and to the file
 * system, as specified by
 * {@link com.sleepycat.je.EnvironmentConfig#LOG_FLUSH_SYNC_INTERVAL} and
 * {@link com.sleepycat.je.EnvironmentConfig#LOG_FLUSH_NO_SYNC_INTERVAL}.
 *
 * Currently flushing occurs if any transactions were committed during the
 * interval. In the future we may want to flush if there were no writes or
 * fynscs in the interval, to allow specifying an even smaller interval for
 * NO_SYNC flushing. This would mean that the wakeup interval should be the
 * config interval divided by 2.
 */
public class LogFlusher {
    private final EnvironmentImpl envImpl;
    private final Timer timer;
    private int flushSyncInterval;
    private int flushNoSyncInterval;
    private FlushTask flushSyncTask;
    private FlushTask flushNoSyncTask;

    private boolean shutdownRequest = false;

    public LogFlusher(EnvironmentImpl envImpl) {

        this.envImpl = envImpl;

        this.timer = new Timer(
            envImpl.makeDaemonThreadName(Environment.LOG_FLUSHER_NAME),
            true /*isDaemon*/);
    }

    /**
     * Applies the new configuration, then cancels and reschedules the flush
     * tasks as needed.
     *
     * @throws IllegalArgumentException if an illegal combination of old and
     * new flush params were specified.
     */
    public void configFlushTask(DbConfigManager configMgr) {

        if (!updateConfig(configMgr)) {
            return;
        }

        synchronized (this) {
            if (!shutdownRequest) {
                cancel();

                if (flushSyncInterval > 0) {
                    flushSyncTask = new FlushTask(envImpl, true /*fsync*/);
        
                    timer.schedule(
                        flushSyncTask, flushSyncInterval, flushSyncInterval);
                }
        
                if (flushNoSyncInterval > 0) {
                    flushNoSyncTask = new FlushTask(envImpl, false /*fsync*/);
        
                    timer.schedule(
                        flushNoSyncTask, flushNoSyncInterval,
                        flushNoSyncInterval);
                }
            }
        }
    }

    private void cancel() {
        if (flushSyncTask != null) {
            flushSyncTask.cancel();
            flushSyncTask = null;
        }
        if (flushNoSyncTask != null) {
            flushNoSyncTask.cancel();
            flushNoSyncTask = null;
        }
    }

    public void requestShutdown() {
        shutdown();
    }

    public void shutdown() {
        synchronized (this) {
            shutdownRequest = true;
            cancel();
            timer.cancel();
        }
    }

    /**
     * Applies the new configuration and returns whether it changed.
     *
     * @throws IllegalArgumentException if an illegal combination of old and
     * new flush params were specified.
     */
    private boolean updateConfig(DbConfigManager configMgr) {

        int newSyncInternal;
        int newNoSyncInterval;

        /*
         * If specified and set to false (which is not the default), the
         * deprecated OLD_REP_RUN_LOG_FLUSH_TASK overrides other settings.
         */
        if (configMgr.isSpecified(OLD_REP_RUN_LOG_FLUSH_TASK) &&
            !configMgr.getBoolean(OLD_REP_RUN_LOG_FLUSH_TASK)) {

            if (configMgr.isSpecified(LOG_FLUSH_SYNC_INTERVAL) ||
                configMgr.isSpecified(LOG_FLUSH_NO_SYNC_INTERVAL)) {

                throw new IllegalArgumentException(
                    "When " + OLD_REP_RUN_LOG_FLUSH_TASK.getName() +
                    " is set to false, " + LOG_FLUSH_SYNC_INTERVAL +
                    " and " + LOG_FLUSH_NO_SYNC_INTERVAL +
                    " must not be specified.");
            }

            newSyncInternal = 0;
            newNoSyncInterval = 0;

        } else {

            /*
             * If specified, the deprecated OLD_REP_LOG_FLUSH_TASK_INTERVAL
             * overrides LOG_FLUSH_SYNC_INTERVAL.
             */
            if (configMgr.isSpecified(OLD_REP_LOG_FLUSH_TASK_INTERVAL)) {

                if (configMgr.isSpecified(LOG_FLUSH_SYNC_INTERVAL)) {

                    throw new IllegalArgumentException(
                        "Both " + OLD_REP_LOG_FLUSH_TASK_INTERVAL.getName() +
                        " and " + LOG_FLUSH_SYNC_INTERVAL +
                        " must not be specified.");
                }

                newSyncInternal =
                    configMgr.getDuration(OLD_REP_LOG_FLUSH_TASK_INTERVAL);
            } else {
                newSyncInternal =
                    configMgr.getDuration(LOG_FLUSH_SYNC_INTERVAL);
            }

            newNoSyncInterval =
                configMgr.getDuration(LOG_FLUSH_NO_SYNC_INTERVAL);
        }

        if (newSyncInternal == flushSyncInterval &&
            newNoSyncInterval == flushNoSyncInterval) {
            return false;
        }

        flushSyncInterval = newSyncInternal;
        flushNoSyncInterval = newNoSyncInterval;
        return true;
    }

    int getFlushSyncInterval() {
        return flushSyncInterval;
    }

    int getFlushNoSyncInterval() {
        return flushNoSyncInterval;
    }

    FlushTask getFlushSyncTask() {
        return flushSyncTask;
    }

    FlushTask getFlushNoSyncTask() {
        return flushNoSyncTask;
    }

    static class FlushTask extends TimerTask {
        private final EnvironmentImpl envImpl;
        private final boolean fsync;
        private long lastNCommits;
        private volatile int flushCount;

        FlushTask(EnvironmentImpl envImpl, boolean fsync) {
            this.envImpl = envImpl;
            this.fsync = fsync;
            this.lastNCommits = envImpl.getTxnManager().getNTotalCommits();
        }

        int getFlushCount() {
            return flushCount;
        }

        @Override
        public void run() {
            try {
                final long newNCommits =
                    envImpl.getTxnManager().getNTotalCommits();

                /* Do nothing if there have been no new commits. */
                if (newNCommits <= lastNCommits) {
                    return;
                }

                if (fsync) {
                    envImpl.getLogManager().flushSync();
                } else {
                    envImpl.getLogManager().flushNoSync();
                }

                lastNCommits = newNCommits;
                flushCount++;

            } catch (Throwable e) {
                if (envImpl.isValid()) {
                    StoppableThread.handleUncaughtException(
                        envImpl.getLogger(), envImpl, Thread.currentThread(),
                        e);
                }
            }
        }
    }
}
