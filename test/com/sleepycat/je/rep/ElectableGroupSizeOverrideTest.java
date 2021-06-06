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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

public class ElectableGroupSizeOverrideTest extends RepTestBase {

    /*
     * Verify that elections can be held, and writes performed by a minority
     * with an override in place.
     */
    @Test
    public void testBasic()
        throws InterruptedException {
        createGroup();
        /* Shutdown the entire group. */
        closeNodes(repEnvInfo);

        /* Verify that the node cannot come up on its own. */
        RepEnvInfo ri0 = repEnvInfo[0];
        setEnvSetupTimeout("2 s");
        try {
            ri0.openEnv();
            fail("Unknow master exception expected.");
        } catch (UnknownMasterException ume) {
            /* Expected. */
        }
        /* Restore the timeout. */
        setEnvSetupTimeout(RepParams.ENV_SETUP_TIMEOUT.getDefault());

        startGroupWithOverride(1);

        /* Resume normal operations, eliminate override */
        setElectableGroupSize(0, repEnvInfo);
        RepEnvInfo mi = restartNodes(repEnvInfo);
        assertNotNull(mi);
        closeNodes(repEnvInfo);
    }

    /**
     * tests a 5 node group, with the master failing as part of the majority
     * of the nodes being lost.
     *
     * 1) Shutdown nodes n1-n3, including n1 the master
     * 2) Verify no master amongst the remaining
     * 3) Set override
     * 4) Verify master emerges and write transactions can be committed.
     * 5) Remove override
     * 6) Bring up down nodes n1-n3 -- group is in normal working order
     */
    @Test
    public void testMasterDownOverride() throws InterruptedException {
        createGroup();
        assertTrue(repEnvInfo[0].getEnv().getState().isMaster());

        /* Shutdown a simple majority, including the Master. */
        final int simpleMajority = (repEnvInfo.length + 1) / 2;
        RepEnvInfo[] downNodes = copyArray(repEnvInfo, 0, simpleMajority);
        RepEnvInfo[] activeNodes =
            copyArray(repEnvInfo, simpleMajority,
                      repEnvInfo.length - simpleMajority);
        closeNodes(downNodes);

        for (RepEnvInfo ri : activeNodes) {
            /* No master amongst the remaining nodes. */
            assertTrue(!ri.getEnv().getState().isMaster());
        }
        setMutableElectableGroupSize(simpleMajority-1, activeNodes);

        RepEnvInfo master =
            findMasterAndWaitForReplicas(10000, activeNodes.length - 1,
                                         activeNodes);

        /* They should now be able to conclude an election. */
        assertTrue(master != null);

        /* Write should succeed without exceptions with the override */
        tryWrite(findMaster(activeNodes).getEnv(), "dbok");

        /* Bring up a down node, restoring a simple majority of active nodes */
        ReplicatedEnvironment renv0 = downNodes[0].openEnv();
        assertTrue(renv0.getState().isReplica());

        /* Restore normal functioning. */
        setMutableElectableGroupSize(0, activeNodes);

        /* Bring up the rest of the nodes. */
        restartNodes(copyArray(downNodes, 1, downNodes.length - 1));
    }

    /* Copy a part of an array to a new array. */
    private RepEnvInfo[] copyArray(RepEnvInfo[] nodes,
                                   int srcStart,
                                   int copyLength) {
        RepEnvInfo[] newNodes = new RepEnvInfo[copyLength];
        System.arraycopy(nodes, srcStart, newNodes, 0, copyLength);

        return newNodes;
    }

    /**
     * tests a 5 node group, with the master being retained when the majority
     * of the nodes is lost.
     *
     * 1) Shutdown nodes n3-n5, n1 is the master and is alive.
     * 2) Verify that master can no longer commit transactions
     * 3) Set override
     * 4) Verify that write transactions can be committed
     * 5) Remove override
     * 6) Bring up down nodes n3-n5 -- group is in normal working order
     */
    @Test
    public void testMasterUpOverride() throws InterruptedException {
        createGroup();
        assertTrue(repEnvInfo[0].getEnv().getState().isMaster());

        /* Shutdown a simple majority, excluding the Master. */
        final int simpleMajority = (repEnvInfo.length + 1) / 2;
        RepEnvInfo[] downNodes =
            copyArray(repEnvInfo, simpleMajority - 1,
                      repEnvInfo.length - simpleMajority + 1);
        RepEnvInfo[] activeNodes = copyArray(repEnvInfo, 0,
                                             simpleMajority - 1);
        closeNodes(downNodes);

        assertTrue(repEnvInfo[0].getEnv().getState().isMaster());

        /* Write should fail without the override */
        try {
            tryWrite(findMaster(activeNodes).getEnv(), "dbfail");
            fail("Exception expected");
        } catch (InsufficientAcksException iae) {
            // ok
        } catch (InsufficientReplicasException ire) {
            // ok
        }
        setMutableElectableGroupSize(simpleMajority-1, activeNodes);

        /* Write should succeed without exceptions with the override */
        tryWrite(findMaster(activeNodes).getEnv(), "dbok");

        /* Bring up a down node, restoring a simple majority of active nodes */
        ReplicatedEnvironment renv = downNodes[0].openEnv();
        assertTrue(renv.getState().isReplica());

        /* Restore normal functioning. */
        setMutableElectableGroupSize(0, activeNodes);

        /* Bring up the rest of the nodes. */
        restartNodes(copyArray(downNodes, 1, downNodes.length - 1));
    }

    private void startGroupWithOverride(int override)
        throws InterruptedException {

        /* Now Try bringing up just one node using override */
        setElectableGroupSize(override, repEnvInfo);

        RepEnvInfo[] activeNodes = copyArray(repEnvInfo, 0, override);
        RepEnvInfo mi = restartNodes(activeNodes);
        assertNotNull(mi);

        ReplicatedEnvironment menv = mi.getEnv();
        /* Write must succeed without exceptions. */
        tryWrite(menv, "dbok" + override);
        /*
         * It should be possible for the other nodes to find the master
         * and join.
         */
        for (int i=override; i < repEnvInfo.length; i++) {
            repEnvInfo[i].openEnv();
            assertTrue(repEnvInfo[i].getEnv().getState().isReplica());
        }
        /* The master should be unchanged */
        assertTrue(menv.getState().isMaster());
        closeNodes(repEnvInfo);
    }

    /*
     * Attempt write operation by creating a database. Caller knows whether or
     * not to expect an exception.
     */
    private void tryWrite(ReplicatedEnvironment repEnv, String dbName) {
        Database db = null;

        try {
            Transaction txn = repEnv.beginTransaction(null, null);
            db = repEnv.openDatabase(txn, dbName, dbconfig);
            txn.commit(new Durability(SyncPolicy.SYNC,
                                      SyncPolicy.SYNC,
                                      ReplicaAckPolicy.SIMPLE_MAJORITY));
        } finally {
            if (db != null) {
                /* Close database even in presence of exceptions. */
                db.close();
            }
        }
    }

    /**
     * Sets the electable group size in the configuration associated with each
     * of the nodes.
     *
     * @param override the override value
     * @param nodes the configs where the override is to be applied
     */
    void setElectableGroupSize(int override, RepEnvInfo... nodes) {
        for (RepEnvInfo ri : nodes) {
            ri.getRepConfig().setElectableGroupSizeOverride(override);
        }
    }

    /**
     * Sets the electable group size mutable associated with an open
     * environment handle.
     *
     * @param override the override value
     * @param nodes the nodes where the override is to be applied
     */
    void setMutableElectableGroupSize(int override, RepEnvInfo... nodes) {
        for (RepEnvInfo ri : nodes) {
            ReplicationConfig mutableConfig = ri.getEnv().getRepConfig();
            mutableConfig.setElectableGroupSizeOverride(override);
            ri.getEnv().setRepMutableConfig(mutableConfig);
        }
    }

    /**
     * Sets the setup timeout associated with all the nodes in the test.
     *
     * @param duration the amount of time to wait
     */
    private void setEnvSetupTimeout(String duration) {
        for (RepEnvInfo ri : repEnvInfo) {
            ri.getRepConfig().setConfigParam
                (RepParams.ENV_SETUP_TIMEOUT.getName(), duration);
        }
    }
}
