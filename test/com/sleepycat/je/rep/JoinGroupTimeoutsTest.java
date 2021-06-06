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
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup.TestHook;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.WaitForListener;

public class JoinGroupTimeoutsTest extends RepTestBase {

    @Before
    public void setUp() 
        throws Exception {
        
        groupSize = 3;
        super.setUp();
    }

    @After
    public void tearDown() 
        throws Exception {
        
        ReplicaFeederSyncup.setGlobalSyncupEndHook(null);
        super.tearDown();
    }

    /* Test for UnknownMasterException when the setup timeout is exceeded. */
    @Test
    public void testSetupTimeout() {

        createGroup();
        final RepEnvInfo riMaster = repEnvInfo[0];
        assertEquals(State.MASTER, riMaster.getEnv().getState());
        leaveGroupAllButMaster();
        riMaster.closeEnv();

        // Can't hold elections need at least two nodes, so timeout
        try {
            ReplicationConfig config = riMaster.getRepConfig();
            config.setConfigParam
                (RepParams.ENV_SETUP_TIMEOUT.getName(), "3 s");
            State status = riMaster.openEnv().getState();
            fail("Joined group in state: " + status);
        } catch (UnknownMasterException e) {
            // Expected exception
        }
    }

    /*
     * Test for an unknown state handle when the unknown state timeout is
     * exceeded.
     */
    @Test
    public void testUnknownStateTimeout()
        throws InterruptedException {

        createGroup();
        final RepEnvInfo ri0 = repEnvInfo[0];
        closeNodes(repEnvInfo);

        // Can't hold elections need at least two nodes, so timeout
        final ReplicationConfig config = ri0.getRepConfig();
        config.setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "5 s");

        config.setConfigParam
            (RepParams.ENV_UNKNOWN_STATE_TIMEOUT.getName(), "1 s");

        long startMs = System.currentTimeMillis();
        assertEquals(ri0.openEnv().getState(), State.UNKNOWN);

        /* Check that we waited for the "1 sec" timeout. */
        assertTrue((System.currentTimeMillis() - startMs) >= 1000);

        /*
         * Transition out of unknown state by starting up another node to
         * establish a quorum for elections.
         */
        repEnvInfo[1].openEnv();
        assertTrue(repEnvInfo[1].getEnv().getState().isActive());

        final WaitForListener listener =
                new WaitForListener(State.MASTER, State.REPLICA);
        repEnvInfo[0].getEnv().setStateChangeListener(listener);

        boolean success = listener.await();
        assertTrue("State:" + repEnvInfo[0].getEnv().getState(), success);
    }

    /**
     * Verify that a long syncup which exceeds the UNKNOWN_STATE_TIMEOUT
     * but is less than ENV_SETUP_TIMEOUT succeeds.
     */
    @Test
    public void testUnknownStateTimeoutAndProceed() {
        createGroup();
        final RepEnvInfo ri3 = repEnvInfo[0];
        ri3.closeEnv();

        final ReplicationConfig config = ri3.getRepConfig();
        config.setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "10 s");

        config.setConfigParam
            (RepParams.ENV_UNKNOWN_STATE_TIMEOUT.getName(), "5 s");

        /*
         * Simulate the syncup delay. It must be larger than unknown state
         * timeout, but less than the generous setup timeout.
         */
        final int syncupStallMs = 1500;
        stallSyncup(syncupStallMs);

        final long startMs = System.currentTimeMillis();

        /* No exceptions expected. */
        ri3.openEnv();
        assertTrue((System.currentTimeMillis() - startMs) >= syncupStallMs);
        assertTrue(ri3.getEnv().getState().isReplica());
    }

    /**
     * Verify that exceeding the setup timeout results in an exception.
     */
    @Test
    public void testEnvSetupTimeoutExceeded() {
        createGroup();
        final RepEnvInfo ri3 = repEnvInfo[0];
        ri3.closeEnv();

        final ReplicationConfig config = ri3.getRepConfig();
        config.setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "6 s");

        config.setConfigParam
            (RepParams.ENV_UNKNOWN_STATE_TIMEOUT.getName(), "5 s");

        final int syncupStallMs = 100000;
        stallSyncup(syncupStallMs);
        final long startMs = System.currentTimeMillis();
        try {
            ri3.openEnv();
            fail("Expected replicaConssitencyException");
        } catch (ReplicaConsistencyException ume) {
            /* Expected exception. */
        }
        assertTrue((System.currentTimeMillis() - startMs) >= 1000);
    }

    /**
     * Puts a hook in place to stalls the syncup for the designated time
     * period. It's the caller's responsibility to clear the hook if necessary.
     */
    private void stallSyncup(final int syncupStallMs) {
        final TestHook<Object> syncupEndHook = new TestHook<Object>() {
            public void doHook() throws InterruptedException {
                Thread.sleep(syncupStallMs);
            }
        };
        ReplicaFeederSyncup.setGlobalSyncupEndHook(syncupEndHook);
    }

    /**
     * Test for IAE on invalid configurations.
     */
    @Test
    public void testIllegalTimeoutArg() {
        final RepEnvInfo ri0 = repEnvInfo[0];
        final ReplicationConfig config = ri0.getRepConfig();
        config.setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "60 s");

        /* Election timeout larger than setup value. */
        config.setConfigParam
            (RepParams.ENV_UNKNOWN_STATE_TIMEOUT.getName(), "61 s");
        try {
            ri0.openEnv();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            // Expected exception
        }
    }
}
