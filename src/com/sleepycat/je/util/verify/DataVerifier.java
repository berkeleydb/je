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

package com.sleepycat.je.util.verify;

import static com.sleepycat.je.config.EnvironmentParams.ENV_RUN_VERIFIER;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_BTREE;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_BTREE_BATCH_DELAY;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_BTREE_BATCH_SIZE;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_DATA_RECORDS;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_LOG;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_LOG_READ_DELAY;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_MAX_TARDINESS;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_OBSOLETE_RECORDS;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_SCHEDULE;
import static com.sleepycat.je.config.EnvironmentParams.VERIFY_SECONDARIES;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.DbVerifyLog;
import com.sleepycat.je.utilint.CronScheduleParser;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.StoppableThread;

/**
 *  Periodically perform checksum verification, Btree verification, or both,
 *  depending on {@link com.sleepycat.je.EnvironmentConfig#VERIFY_LOG} and
 *  {@link com.sleepycat.je.EnvironmentConfig#VERIFY_BTREE}.
 *  
 *  The first-time start time and the period of the verification is determined
 *  by {@link com.sleepycat.je.EnvironmentConfig#VERIFY_SCHEDULE}.
 */
public class DataVerifier {
    private final EnvironmentImpl envImpl;
    private final Timer timer;
    private VerifyTask verifyTask;
    private boolean verifyLog;
    private boolean verifyBtree;
    private final DbVerifyLog dbLogVerifier;
    private final BtreeVerifier dbTreeVerifier;

    private long verifyDelay;
    private long verifyInterval;
    private String cronSchedule;

    private boolean shutdownRequest = false;
    
    private final String VERIFIER_SCHEDULE = "test.je.env.verifierSchedule"; 

    public DataVerifier(EnvironmentImpl envImpl) {

        this.envImpl = envImpl;
        this.timer = new Timer(
            envImpl.makeDaemonThreadName(
                Environment.DATA_CORRUPTION_VERIFIER_NAME),
            true /*isDaemon*/);
        dbLogVerifier = new DbVerifyLog(envImpl, 0);
        dbTreeVerifier = new BtreeVerifier(envImpl);
    }

    /**
     * Applies the new configuration, then cancels and reschedules the verify
     * task as needed.
     */
    public void configVerifyTask(DbConfigManager configMgr) {

        if (!updateConfig(configMgr)) {
            return;
        }

        synchronized (this) {
            if (!shutdownRequest) {
                cancel();

                if (cronSchedule != null) {
                    verifyTask = new VerifyTask(envImpl);

                    /*
                     * Use Timer.scheduleAtFixedRate to instead of
                     * Timer.schedule, since this is a long running task and
                     * the next task is NOT expected to be scheduled for
                     * 24 hours later, it is expected to be scheduled at
                     * a fixed time.
                     */
                    timer.scheduleAtFixedRate(
                        verifyTask, verifyDelay, verifyInterval);
                }
            }
        }
    }

    private void cancel() {
        if (verifyTask != null) {
            verifyTask.cancel();
        }

        /*
         * Stop verifier as soon as possible when it is disabled via
         * EnvironmentMutableConfig.
         */
        dbLogVerifier.setStopVerifyFlag(true);
        dbTreeVerifier.setStopVerifyFlag(true);
    }

    public void requestShutdown() {
        synchronized (this) {
            shutdownRequest = true;
            cancel();
            timer.cancel();
        }
    }

    public void shutdown() {
        requestShutdown();

        final int timeoutMs = 30000;

        final PollCondition cond = new PollCondition(2, timeoutMs) {
            @Override
            protected boolean condition() {
                /* Copy verifyTask since it may change in another thread. */
                final VerifyTask task = verifyTask;
                return task == null || !task.isRunning;
            }
        };

        if (!cond.await()) {
            LoggerUtils.warning(
                envImpl.getLogger(), envImpl,
                "Unable to shutdown data verifier after " + timeoutMs + "ms");
        }
    }

    public long getVerifyDelay() {
        return verifyDelay;
    }

    public long getVerifyInterval() {
        return verifyInterval;
    }

    public VerifyTask getVerifyTask() {
        return verifyTask;
    }

    public String getCronSchedule() {
        return cronSchedule;
    }

    /**
     * Applies the new configuration and returns whether it changed.
     */
    private boolean updateConfig(DbConfigManager configMgr) {

        /*
         * If set to false (which is not the default).
         */
        if (!configMgr.getBoolean(ENV_RUN_VERIFIER)) {
            if (cronSchedule == null) {
                return false;
            }
            cronSchedule = null;
            verifyDelay = 0;
            verifyInterval = 0;
            return true;
        } else {
            String newCronSchedule = configMgr.get(VERIFY_SCHEDULE);

            /*
             * If the data verifier schedule is set by system property and
             * it is not set through the JE source code explicitly, then
             * the system property will be used.
             *
             * This is used for JE unit test and JE standalone test.
             */
            String sysPropVerifySchedule =
                System.getProperty(VERIFIER_SCHEDULE);
            if (sysPropVerifySchedule != null &&
                !configMgr.isSpecified(VERIFY_SCHEDULE)) {
                newCronSchedule = sysPropVerifySchedule;
            }

            if (CronScheduleParser.checkSame(cronSchedule, newCronSchedule)) {
                return false;
            }
            CronScheduleParser csp = new CronScheduleParser(newCronSchedule);
            verifyDelay = csp.getDelayTime();
            verifyInterval = csp.getInterval();
            cronSchedule = newCronSchedule;
            return true;
        }
    }

    class VerifyTask extends TimerTask {
        private final EnvironmentImpl envImpl;
        private volatile boolean isRunning;

        VerifyTask(EnvironmentImpl envImpl) {
            this.envImpl = envImpl;
        }
        
        private void updateConfig() {
            DbConfigManager configMgr = envImpl.getConfigManager();

            verifyLog = configMgr.getBoolean(VERIFY_LOG);
            verifyBtree = configMgr.getBoolean(VERIFY_BTREE);

            dbLogVerifier.setReadDelay(
                configMgr.getDuration(VERIFY_LOG_READ_DELAY),
                TimeUnit.MILLISECONDS);

            final VerifyConfig btreeVerifyConfig = new VerifyConfig();
            btreeVerifyConfig.setVerifySecondaries(
                configMgr.getBoolean(VERIFY_SECONDARIES));
            btreeVerifyConfig.setVerifyDataRecords(
                configMgr.getBoolean(VERIFY_DATA_RECORDS));
            btreeVerifyConfig.setVerifyObsoleteRecords(
                configMgr.getBoolean(VERIFY_OBSOLETE_RECORDS));
            btreeVerifyConfig.setBatchSize(
                configMgr.getInt(VERIFY_BTREE_BATCH_SIZE));
            btreeVerifyConfig.setBatchDelay(
                configMgr.getDuration(VERIFY_BTREE_BATCH_DELAY),
                TimeUnit.MILLISECONDS);
            dbTreeVerifier.setBtreeVerifyConfig(btreeVerifyConfig);

            /*
             * 1. Why call dbTreeVerifier.setVerifyFlag here, rather than
             * immediately after call cancel in configVerifyTask?
             *    If we do that, then maybe before BtreeVerifier really gets
             *  the stop status of the flag, we set the status of the flag
             *  to be OK again. Then the previous task can not be stopped
             *  as soon as possible.
             *    If we do here, this means that one new task has already
             *    started, so the previous task has already been stopped.
             * 2. Why use synchronized?
             *    Consider the following scenario:
             *           shutdown Thread                 task Thread
             *
             *                                         !shutdownRequest
             *      
             *      shutdownRequest = true
             *      dbTreeVerifier.setStopVerifyFlag
             *
             *                                     dbTreeVerifier.setVerifyFlag
             */
            synchronized (DataVerifier.this) {
                if (!shutdownRequest) {
                    dbLogVerifier.setStopVerifyFlag(false);
                    dbTreeVerifier.setStopVerifyFlag(false);
                }
            }
        }

        @Override
        public void run() {
            /*
             * If the scheduled execution time of the scheduled run lags
             * very far from the current time due to the long-time current
             * verification, the scheduled run is just skipped.
             */
            if (System.currentTimeMillis() - scheduledExecutionTime() >=
                envImpl.getConfigManager().getDuration(VERIFY_MAX_TARDINESS)) {
                return;
            }

            isRunning = true;
            boolean success = false;
            updateConfig();
            try {
                if (verifyLog) {
                    dbLogVerifier.verifyAll();
                }
                if (verifyBtree) {
                    dbTreeVerifier.verifyAll();
                }
                success = true;
            } catch (EnvironmentFailureException efe) {
                /* Do nothing. Just cancel this timer in finally. */
            } catch (Throwable e) {
                if (envImpl.isValid()) {
                    StoppableThread.handleUncaughtException(
                        envImpl.getLogger(), envImpl, Thread.currentThread(),
                        e);
                }
            } finally {
                if (!success) {
                    requestShutdown();
                }
                isRunning = false;
            }
        }
    }
}
