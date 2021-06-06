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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class CheckpointActivationTest extends TestBase {

    private final File envHome;

    public CheckpointActivationTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Write elements to the log, check that the right number of
     * checkpoints ran.
     */
    @Test
    public void testLogSizeBasedCheckpoints()
        throws Exception {

        final int CKPT_INTERVAL = 5000;
        final int TRACER_OVERHEAD = 26;
        final int N_TRACES = 100;
        final int N_CHECKPOINTS = 10;
        final int WAIT_FOR_CHECKPOINT_SECS = 10;
        final int FILE_SIZE = 20000000;

        /* Init trace message with hyphens. */
        assertEquals(0, CKPT_INTERVAL % N_TRACES);
        int msgBytesPerTrace = (CKPT_INTERVAL / N_TRACES) - TRACER_OVERHEAD;
        StringBuilder traceBuf = new StringBuilder();
        for (int i = 0; i < msgBytesPerTrace; i += 1) {
            traceBuf.append('-');
        }
        String traceMsg = traceBuf.toString();

        Environment env = null;
        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            envConfig.setConfigParam(EnvironmentParams.
                                     CHECKPOINTER_BYTES_INTERVAL.getName(),
                                     String.valueOf(CKPT_INTERVAL));

            /*
             * This test needs to control exactly how much goes into the log,
             * so disable daemons.
             */
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_EVICTOR.getName(), "false");
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_INCOMPRESSOR.getName(), "false");
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_CLEANER.getName(), "false");
            env = new Environment(envHome, envConfig);

            /*
             * Get a first reading on number of checkpoints run. Read once
             * to clear, then read again.
             */
            StatsConfig statsConfig = new StatsConfig();
            statsConfig.setFast(true);
            statsConfig.setClear(true);
            EnvironmentStats stats = env.getStats(statsConfig); // clear stats

            stats = env.getStats(statsConfig);  // read again
            assertEquals(0, stats.getNCheckpoints());
            long lastCkptEnd = stats.getLastCheckpointEnd();

            EnvironmentImpl envImpl =
                DbInternal.getNonNullEnvImpl(env);
            Thread ckptThread = envImpl.getCheckpointer().getThread();

            /* Run several checkpoints to ensure they occur as expected.  */
            for (int i = 0; i < N_CHECKPOINTS; i += 1) {

                /* Wait for checkpointer thread to go to wait state. */
                while (true) {
                    Thread.State state = ckptThread.getState();
                    if (state == Thread.State.WAITING ||
                        state == Thread.State.TIMED_WAITING) {
                        break;
                    }
                }

                env.getStats(statsConfig); // Clear stats

                /*
                 * Write enough to prompt a checkpoint.  20% extra bytes are
                 * written to be sure that we exceed the checkpoint interval.
                 */
                long lastLsn = envImpl.getFileManager().getNextLsn();
                while (DbLsn.getNoCleaningDistance
                        (lastLsn, envImpl.getFileManager().getNextLsn(),
                         FILE_SIZE) < CKPT_INTERVAL + 
                                      ((CKPT_INTERVAL * 2)/10)) {
                    Trace.trace(envImpl, traceMsg);
                }

                /*
                 * Wait for a checkpoint to start (if the test succeeds it will
                 * start right away).  We take advantage of the fact that the
                 * NCheckpoints stat is set at the start of a checkpoint.
                 */
                 long startTime = System.currentTimeMillis();
                 boolean started = false;
                 while (!started &&
                        (System.currentTimeMillis() - startTime <
                         WAIT_FOR_CHECKPOINT_SECS * 1000)) {
                    Thread.yield();
                    Thread.sleep(1);
                    stats = env.getStats(statsConfig);
                    if (stats.getNCheckpoints() > 0) {
                        started = true;
                    }
                }
                assertTrue("Checkpoint " + i + " did not start after " +
                           WAIT_FOR_CHECKPOINT_SECS + " seconds",
                           started);

                /*
                 * Wait for the checkpointer daemon to do its work.  We do not
                 * want to continue writing until the checkpoint is complete,
                 * because the amount of data we write is calculated to be the
                 * correct amount in between checkpoints.  We know the
                 * checkpoint is finished when the LastCheckpointEnd LSN
                 * changes.
                 */
                while (true) {
                    Thread.yield();
                    Thread.sleep(1);
                    stats = env.getStats(statsConfig);
                    if (lastCkptEnd != stats.getLastCheckpointEnd()) {
                        lastCkptEnd = stats.getLastCheckpointEnd();
                        break;
                    }
                }
            }
        } catch (Exception e) {

            /*
             * print stack trace now, else it gets subsumed in exceptions
             * caused by difficulty in removing log files.
             */
            e.printStackTrace();
            throw e;
        } finally {
            if (env != null) {
                env.close();
            }
        }
    }

    /* Test programmatic call to checkpoint. */
    @Test
    public void testApiCalls()
        throws Exception {

        Environment env = null;
        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            envConfig.setConfigParam(EnvironmentParams.
                                     CHECKPOINTER_BYTES_INTERVAL.getName(),
                                     "1000");

            /*
             * Try disabling stat capture thread to see if erroneous failures
             * go away.  We're doing a checkpoint, yet the nCheckoints stat is
             * sometimes returned as zero.  See comment on stat capture below.
             */
            envConfig.setConfigParam(EnvironmentConfig.STATS_COLLECT, "false");

            /* Disable all daemons */
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_EVICTOR.getName(), "false");
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_INCOMPRESSOR.getName(), "false");
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_CLEANER.getName(), "false");
            envConfig.setConfigParam(EnvironmentParams.
                                     ENV_RUN_CHECKPOINTER.getName(), "false");
            env = new Environment(envHome, envConfig);

            /*
             * Get a first reading on number of checkpoints run. Read once
             * to clear, then read again.
             */
            StatsConfig statsConfig = new StatsConfig();
            statsConfig.setFast(true);
            statsConfig.setClear(true);
            EnvironmentStats stats = env.getStats(statsConfig); // clear stats

            stats = env.getStats(statsConfig);  // read again
            assertEquals(0, stats.getNCheckpoints());

            /*
             * From the last checkpoint start LSN, there should be the
             * checkpoint end log entry and a trace message. These take 196
             * bytes.
             */
            CheckpointConfig checkpointConfig = new CheckpointConfig();

            /* Should not cause a checkpoint, too little growth. */
            checkpointConfig.setKBytes(1);
            env.checkpoint(checkpointConfig);
            stats = env.getStats(statsConfig);  // read again
            assertEquals(0, stats.getNCheckpoints());

            /* Fill up the log, there should be a checkpoint. */
            String filler = "123456789012345678901245678901234567890123456789";
            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
            for (int i = 0; i < 20; i++) {
                Trace.trace(envImpl, filler);
            }
            env.checkpoint(checkpointConfig);
            stats = env.getStats(statsConfig);  // read again
            /* Following is sometimes 0. Try disabling stat capture above. */
            assertEquals(1, stats.getNCheckpoints());

            /* Try time based, should not checkpoint. */
            checkpointConfig.setKBytes(0);
            checkpointConfig.setMinutes(1);
            env.checkpoint(checkpointConfig);
            stats = env.getStats(statsConfig);  // read again
            assertEquals(0, stats.getNCheckpoints());

            /*
             * Sleep, enough time has passed for a checkpoint, but nothing was
             * written to the log.
             */
            Thread.sleep(1000);
            env.checkpoint(checkpointConfig);
            stats = env.getStats(statsConfig);  // read again
            assertEquals(0, stats.getNCheckpoints());

            /* Log something, now try a checkpoint. */
            Trace.trace(envImpl, filler);
            env.checkpoint(checkpointConfig);
            stats = env.getStats(statsConfig);  // read again
            // TODO: make this test more timing independent. Sometimes
            // the assertion will fail.
            // assertEquals(1, stats.getNCheckpoints());

        } catch (Exception e) {
            /*
             * print stack trace now, else it gets subsumed in exceptions
             * caused by difficulty in removing log files.
             */
            e.printStackTrace();
            throw e;
        } finally {
            if (env != null) {
                env.close();
            }
        }
    }
}
