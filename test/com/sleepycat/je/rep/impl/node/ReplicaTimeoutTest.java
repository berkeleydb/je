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
package com.sleepycat.je.rep.impl.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;

public class ReplicaTimeoutTest extends RepTestBase {

    @Before
    public void setUp()
        throws Exception {
        
        groupSize = 3;
        super.setUp();
    }

    @Test
    public void testFeederHeartbeatTimeout()
        throws InterruptedException {

        for (int i=1; i < groupSize; i++) {
            /*
             * Ensure that the replicas don't initiate a connection close due
             * to inactivity.
             */
            repEnvInfo[i].getRepConfig().
                setConfigParam(ReplicationConfig.REPLICA_TIMEOUT, "10000 s");
        }

        /*
         * The feeder should timeout the connection because of unresponsive
         * replicas. Configure it for 5 seconds.
         */
        repEnvInfo[0].getRepConfig().
            setConfigParam(RepParams.FEEDER_TIMEOUT.getName(), "5 s");

        createGroup();
        ReplicatedEnvironment renv1 = repEnvInfo[0].getEnv();
        assertTrue(renv1.getState().isMaster());
        final FeederManager feederManager =
            repEnvInfo[0].getRepNode().feederManager();

        for (int i=1; i < groupSize; i++) {
            /*
             * make the replicas unresponsive to heartbeat requests.
             */
            repEnvInfo[i].getRepNode().getReplica().setTestDelayMs(60000);
        }

        /* Wait a minute for the feeder connections to be shut down. */
        for (int i=0; i < 60; i++) {
            if (feederManager.activeReplicaCount() == 0) {
                break;
            }
            Thread.sleep(1000);
        }
        assertEquals(0, feederManager.activeReplicaCount());
    }

    /*
     * Ensure that the replica times out and holds an election if it's inactive
     * for the configured REPLICA_HEARTBEAT_TIMEOUT period.
     */
    @Test
    public void testReplicaHeartbeatTimeout() throws InterruptedException {
        /*
         * Slow down the heartbeat for the master, so it looks like a
         * missing heartbeat on the replica side.
         */
        repEnvInfo[0].getRepConfig().
            setConfigParam(RepParams.HEARTBEAT_INTERVAL.getName(), "60000");

        for (int i = 1; i < groupSize; i++) {

            /*
             * If the replica does not see a heartbeat in 5 seconds it will
             * transition to the unknown state and hold an election.
             */
            repEnvInfo[i].getRepConfig().
            setConfigParam(ReplicationConfig.REPLICA_TIMEOUT, "5 s");
        }

        createGroup();
        ReplicatedEnvironment renv1 = repEnvInfo[0].getEnv();
        assertTrue(renv1.getState().isMaster());

        final CountDownLatch latch = new CountDownLatch(1);
        repEnvInfo[1].getEnv().
            setStateChangeListener(new UnknownStateListener(latch));
        assertTrue(latch.await(60, TimeUnit.SECONDS));

        /*
         * Close the old master explicitly since it may be invalidated due to a
         * MasterStateChangeException if the election resulted in a new master
         */
        repEnvInfo[0].closeEnv();
    }

    /*
     * Listener used to trip the latch if the replica transitions through the
     * unknown state.
     */
    protected static class UnknownStateListener implements StateChangeListener{
        final CountDownLatch unknownLatch;

        public UnknownStateListener(CountDownLatch unknownLatch) {
            super();
            this.unknownLatch = unknownLatch;
        }

        public void stateChange(StateChangeEvent stateChangeEvent)
            throws RuntimeException {

            if (stateChangeEvent.getState().isUnknown()) {
                unknownLatch.countDown();
            }
        }
    }
}
