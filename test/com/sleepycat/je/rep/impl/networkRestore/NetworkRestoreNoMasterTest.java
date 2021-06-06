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


package com.sleepycat.je.rep.impl.networkRestore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup.TestHook;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.WaitForListener;

/**
 * Verifies that a "minority" group (without a master) can be used to bootstrap
 * a fully functioning group by using network restore operations to establish a
 * quorum and select a new master.
 */
public class NetworkRestoreNoMasterTest extends RepTestBase {

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Tests a special case of failure recovery with a newly added node as
     * below:
     *
     * 1) Start with a two node group: rg1-rn1(M) and rg1-rn2(R)
     * 2) Add rg1-rn3, which joins as a replica.
     * 3) rg1-rn3 starts syncing up but before it can read the updates that
     * establish it in the group membership table the master rg1-rn1 goes down
     * hard. This results in rg1-rn3 transitioning to the UNKNOWN state,
     * where because it does not have a "node id", tries to "find" a master
     * by querying the nodes it knows about. rg1-rn2 does not respond because
     * it's no longer in contact with a master and is trying to initiate an
     * election.
     * 4) rg1-rn3 should network restore from rg1-rn2, so that it can help
     * establish a quorum, hold an election, and make the shard available for
     * writes again.
     *
     * This is sr22851, where the rep node incorrectly retains its cached
     * knowledge of a previous master.
     */
    @Test
    public void testSyncupFailure()
        throws InterruptedException {

        createGroup(2);
        final RepEnvInfo mInfo = repEnvInfo[0];
        assertTrue(mInfo.getEnv().getState().isMaster());

        /*
         * Populate enough of the env so that it can't all be buffered in the
         * network buffers, to ensure that rg1-rn3 does not have the group
         * database with changes that insert rg1-rn3 into it when rg1-rn1
         * subsequently goes down.
         */
        populateDB(mInfo.getEnv(), 10000);

        /* latches to coordinate killing of rg1-rn1 */
        final CountDownLatch syncupEnded = new CountDownLatch(1);
        final CountDownLatch masterDown = new CountDownLatch(1);

        TestHook<Object> syncupEndHook = new TestHook<Object>() {

            @Override
            public void doHook()
                throws InterruptedException {
                syncupEnded.countDown();
                masterDown.await();
            }
        };

        /* Ensure that rg1-rn3 has a current set of helper hosts */
        updateHelperHostConfig();

        ReplicaFeederSyncup.setGlobalSyncupEndHook(syncupEndHook);

        final AtomicReference<String> openFailed =
            new AtomicReference<String>();

        /*
         * Open the rg1-rn3 env and expect an ILE due to the interrupted syncup
         * and recover by doing a network restore.
         */
        Thread asyncOpen = new Thread() {
            @Override
            public void run() {
                try {
                    /*
                     * Slow down the feeder, so that the rep group db changes
                     * for rg1-rn3 are delayed
                     */
                    mInfo.getRepImpl().getRepNode().feederManager().
                        setTestDelayMs(1000);
                    openEnvExpectILE(repEnvInfo[2]);
                } catch (Throwable e) {
                   openFailed.set("open failed:" + e.getMessage());
                }
            }
        };

        asyncOpen.start();
        syncupEnded.await();

        /* Shutdown the master. */
        mInfo.closeEnv();

        /*
         * Release the replica feeder syncup resulting in the replica
         * discovering the closed connection and transitioning to the
         * UNKNOWN state and trying to find/elect a new master.
         */
        masterDown.countDown();

        /* Wait for the open, network restore, open sequence to finish. */
        asyncOpen.join();

        /* check for failures in the open thread. */
        assertNull(openFailed.get());

        /*
         * With two nodes up, we should have a functioning group.
         */
        final ReplicatedEnvironment r2Env = repEnvInfo[2].getEnv();
        assertNotNull(r2Env);
        assertTrue(r2Env.getState().isActive());
    }


    /**
     * This testcase implements the following scenario:
     *
     * 1) Create a 3 node group
     * 2) Stop 2 of the nodes, leaving the group without a master.
     * 3) Delete the log files of the above two nodes.
     * 4) Restart one of the nodes. It finds there is no master and does
     * a network restore. It subsequently proceeds to participate in an
     * election and choose a master.
     * 5) Restart the second node; it queries for and finds the master and
     * syncs up with it.
     *
     * This is sr 22815
     */
    @Test
    public void testMissingEnv()
        throws InterruptedException {

        createGroup();
        final WaitForListener rn3Unknown = new WaitForListener.Unknown();
        repEnvInfo[2].getEnv().setStateChangeListener(rn3Unknown);
        assertTrue(repEnvInfo[0].getEnv().getState().isMaster());

        /* Close rn1 and rn2, leaving just rn3. */
        repEnvInfo[1].closeEnv();
        repEnvInfo[0].closeEnv();

        assertTrue(rn3Unknown.await());

        /* Remove env directories for rn1 and rn2. */
        RepTestUtils.removeRepEnv(repEnvInfo[0].getEnvHome());
        RepTestUtils.removeRepEnv(repEnvInfo[1].getEnvHome());

        final WaitForListener rn3Active = new WaitForListener.Active();
        repEnvInfo[2].getEnv().setStateChangeListener(rn3Active);

        /* Setup helper hosts, so nodes can find each other. */
        /* Ensure that rg1-rn3 has a current set of helper hosts */
        updateHelperHostConfig();

        openEnvExpectILE(repEnvInfo[0]);

        /*
         * With two nodes up, we should have a functioning group.
         */
        assertTrue(rn3Active.await());

        /*
         * Now that there is a master, rn2 should just be able to locate the
         * master and sync up
         */
        assertTrue(repEnvInfo[1].openEnv().getState().isActive());
    }

    private void openEnvExpectILE(RepEnvInfo rinfo) {
        try {
            rinfo.openEnv();
            fail("Expected ILE");
        } catch (InsufficientLogException ile) {
            NetworkRestore nr = new NetworkRestore();
            NetworkRestoreConfig config = new NetworkRestoreConfig();
            nr.execute(ile, config);
            rinfo.openEnv();
        }
    }
}
