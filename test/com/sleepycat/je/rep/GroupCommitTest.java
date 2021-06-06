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

package com.sleepycat.je.rep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;

/**
 * test for group commit functionality
 */
public class GroupCommitTest extends RepTestBase {

    @Override
    public void setUp()
        throws Exception {

        /* need just one replica for this test. */
        groupSize = 2;

        super.setUp();
    }

    /**
     * Verify that group commits can be initiated by either exceeding the
     * time interval, or the group commit size.
     */
    @Test
    public void testBasic()
        throws InterruptedException {

        /* Use a very generous full second for the group commit interval. */
        final long intervalNs = TimeUnit.SECONDS.toNanos(1);
        final int maxGroupCommit = 4;

        initGroupCommitConfig(intervalNs, maxGroupCommit);

        createGroup();
        State state = repEnvInfo[0].getEnv().getState();
        assertEquals(State.MASTER, state);
        ReplicatedEnvironment menv = repEnvInfo[0].getEnv();
        ReplicatedEnvironment renv = repEnvInfo[1].getEnv();

        long startNs = System.nanoTime();
        final StatsConfig statsConfig = new StatsConfig().setClear(true);

        /* Clear and discard stats. */
        renv.getRepStats(statsConfig);

        /* Just a single write. */
        doWrites(menv, 1);

        ReplicatedEnvironmentStats rstats = renv.getRepStats(statsConfig);

        /* Verify that the group commit was the result of a timeout. */
        assertTrue((System.nanoTime() - startNs) > intervalNs);

        assertEquals(1, rstats.getNReplayGroupCommitTxns());
        assertEquals(1, rstats.getNReplayGroupCommits());
        assertEquals(1, rstats.getNReplayGroupCommitTimeouts());
        assertEquals(0, rstats.getNReplayCommitSyncs());
        assertEquals(1, rstats.getNReplayCommitNoSyncs());

        /* Now force an exact group commit size overflow. */
        doWrites(menv, maxGroupCommit);
        rstats = renv.getRepStats(statsConfig);

        assertEquals(maxGroupCommit, rstats.getNReplayGroupCommitTxns());
        assertEquals(1, rstats.getNReplayGroupCommits());
        assertEquals(0, rstats.getNReplayGroupCommitTimeouts());
        assertEquals(0, rstats.getNReplayCommitSyncs());
        assertEquals(maxGroupCommit, rstats.getNReplayCommitNoSyncs());

        /* Group commit size + 1 timeout txn */
        doWrites(menv, maxGroupCommit + 1);
        rstats = renv.getRepStats(statsConfig);

        assertEquals(maxGroupCommit + 1, rstats.getNReplayGroupCommitTxns());
        assertEquals(2, rstats.getNReplayGroupCommits());
        assertEquals(1, rstats.getNReplayGroupCommitTimeouts());
        assertEquals(0, rstats.getNReplayCommitSyncs());
        assertEquals(maxGroupCommit + 1, rstats.getNReplayCommitNoSyncs());
    }

    private void initGroupCommitConfig(final long intervalMs,
                                       final int maxGroupCommit)
        throws IllegalArgumentException {

        for (int i=0; i < groupSize; i++) {
            repEnvInfo[i].getRepConfig().
                setConfigParam(ReplicationConfig.REPLICA_GROUP_COMMIT_INTERVAL,
                               intervalMs + " ns");
            repEnvInfo[i].getRepConfig().
                setConfigParam(ReplicationConfig.REPLICA_MAX_GROUP_COMMIT,
                               Integer.toString(maxGroupCommit));
        }
    }

    /**
     * Verify that group commits can be turned off.
     */
    @Test
    public void testGroupCommitOff()
        throws InterruptedException {

        /* Now turn off group commits on the replica */
        initGroupCommitConfig(Integer.MAX_VALUE, 0);

        createGroup();
        /* Already joined, rejoin master. */
        State state = repEnvInfo[0].getEnv().getState();
        assertEquals(State.MASTER, state);
        ReplicatedEnvironment menv = repEnvInfo[0].getEnv();
        ReplicatedEnvironment renv = repEnvInfo[1].getEnv();

        final StatsConfig statsConfig = new StatsConfig().setClear(true);

        /* Clear and discard stats. */
        renv.getRepStats(statsConfig);

        /* Just a single write. */
        doWrites(menv, 1);

        ReplicatedEnvironmentStats rstats = renv.getRepStats(statsConfig);

        assertEquals(0, rstats.getNReplayGroupCommitTxns());
        assertEquals(0, rstats.getNReplayGroupCommits());
        assertEquals(0, rstats.getNReplayGroupCommitTimeouts());
        assertEquals(1, rstats.getNReplayCommitSyncs());
        assertEquals(0, rstats.getNReplayCommitNoSyncs());
    }

    void doWrites(ReplicatedEnvironment menv, int count)
        throws InterruptedException {

        final WriteThread wt[] = new WriteThread[count];

        for (int i=0; i < count; i++) {
            wt[i] = new WriteThread(menv);
            wt[i].start();
        }

        for (int i=0; i < count; i++) {
            wt[i].join(60000);
        }
    }

    /* Used as the basis for producing unique db names. */
    private static AtomicInteger dbId = new AtomicInteger(0);

    /**
     * Thread used to create concurrent updates amenable to group commits.
     */
    private class WriteThread extends Thread {
        ReplicatedEnvironment menv;

        WriteThread(ReplicatedEnvironment menv) {
            super();
            this.menv = menv;
        }

        @Override
        public void run() {
            final TransactionConfig mtc = new TransactionConfig();
            mtc.setDurability(RepTestUtils.SYNC_SYNC_ALL_DURABILITY);
            Transaction mt = menv.beginTransaction(null, mtc);
            Database db = menv.openDatabase(mt,
                                            "testDB" + dbId.incrementAndGet(),
                                            dbconfig);
            mt.commit();
            db.close();
        }
    }
}
