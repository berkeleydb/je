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

package com.sleepycat.je.rep.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Tests to ensure that a group can eventually recover from ack failures that
 * occur during GroupDB update operations, and that conditions that might lead
 * to loss of "durably committed and replicated" transactions cannot arise.
 */
public class GroupDbAckFailureTest extends RepTestBase {

    /**
     * Exercises the scenario where addition of a new node originally fails,
     * and is later retried.
     */
    @Test
    public void testRetriedCreation() throws Exception {
        createGroup(3);
        repEnvInfo[1].getRepNode().replica().setDontProcessStream();
        repEnvInfo[2].getRepNode().replica().setDontProcessStream();
        try {
            repEnvInfo[3].openEnv();
            fail("expected first attempt to fail");
        } catch (UnknownMasterException e) {
            // expected
        }
        assertNull(repEnvInfo[3].getEnv());
        closeNodes(repEnvInfo[1], repEnvInfo[2]);
        restartNodes(repEnvInfo[1], repEnvInfo[2]);
        repEnvInfo[3].openEnv();
    }

    /**
     * Exercises a scenario in which the application ignores the failure of an
     * updateAddress() operation, and tries to start the moved node anyway.
     * Applications shouldn't do this, but it's good to make sure that the
     * system retains its integrity anyway.
     */
    @Test
    public void testIgnoreUpdateFailure() throws Exception {
        int size = 3;
        createGroup(size);
        RepEnvInfo master = repEnvInfo[0];
        // Database db =
        //     master.getEnv().openDatabase(null, TEST_DB_NAME, dbconfig);
        RepEnvInfo replica = repEnvInfo[1];
        RepEnvInfo mover = repEnvInfo[size-1];
        mover.closeEnv();

        replica.getRepNode().replica().setDontProcessStream();
        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        helpers.add(master.getRepConfig().getNodeSocketAddress());
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                      helpers,
                                      RepTestUtils.readRepNetConfig());
        ReplicationConfig rc = mover.getRepConfig();
        int newPort = rc.getNodePort() + 1732;
        try {
            logger.info("try first update operation");
            rga.updateAddress(rc.getNodeName(), "localhost", newPort);
            fail("should have failed");
        } catch (Exception e) {
            logger.info("test sees " + e.getClass());
            // expected (modulo exception type)
        }
        Thread.sleep(25000);  // wait for master's retry loop to give up
        replica.closeEnv();
        rc.setNodeHostPort("localhost:" + newPort);

        /* Imagine the clumsy application ignores previous failure. */
        logger.info("will restart the moved node");
        try {
            mover.openEnv();
            fail("expected restart of moved node to fail");
        } catch (UnknownMasterException e) {
            logger.info("restart of the moved node failed as expected");
            // expected
        }

        /*
         * This test is designed to demonstrate bug fix #21095.  Originally,
         * the above step failed.  Now, you could ask, is it necessarily bad
         * that the node is allowed to connect?  What's really critical is that
         * the master doesn't then think it has a quorum for authoritative
         * majority commits.  Originally, even that didn't work right: in other
         * words, a default db.put() operation would succeed at this point (the
         * commented-out code below).  Of course with the fix in place it is
         * impossible to even try this step.
         */

        // try {
        //     db.put(null, key, data);
        //     fail("");
        // db.close();
    }

    /**
     * Exercises the scenario where the master tries an updateAddress()
     * operation that has already been completed previously.  This isn't
     * completely silly: it could happen if the application tried to do the
     * update once, but failed to get the response message indicating that it
     * worked; the application would have to retry in order to be sure.
     * <p>
     * As simple as this is, it didn't work before bug fix #21095.
     */
    @Test
    public void testRedundantUpdateAddressOp() throws Exception {
        int size = 3;
        createGroup(size);
        RepEnvInfo master = repEnvInfo[0];
        RepEnvInfo mover = repEnvInfo[size-1];
        closeNodes(mover);

        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        helpers.add(master.getRepConfig().getNodeSocketAddress());
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                      helpers,
                                      RepTestUtils.readRepNetConfig());

        ReplicationConfig rc = mover.getRepConfig();
        String nodeName = rc.getNodeName();
        int newPort = rc.getNodePort() + 1732;
        rga.updateAddress(nodeName, "localhost", newPort);

        /* Pretend we lost the response to the above operation, so retry it. */
        rga.updateAddress(nodeName, "localhost", newPort);

        rc.setNodeHostPort(rc.getNodeHostname() + ":" + newPort);
        restartNodes(mover);
    }

    /**
     * Ensure that if an unfortunately timed network partition occurs around
     * the time of an updateAddress() operation, we avoid allowing two masters
     * to emerge both thinking they're authoritative.  There will be two
     * masters (which is bad), but one of them will reject the Feeder
     * connection from the zombie replica, so at least it won't become
     * authoritative.
     */
    @Test
    public void testZombie() throws Exception {

        /*
         * Super-class has set up 5 RepEnvInfo objects.  We'll use 4 of them,
         * even though our true group size is only 3.  We'll use the 4th one as
         * the new, moved incarnation of the node whose address is to be
         * changed.  In order to do this, we'll have to fake out some of the
         * configuration information in this 4th slot.
         */
        int realGroupSize = 3;
        createGroup(realGroupSize);
        RepEnvInfo node1 = repEnvInfo[0];
        RepEnvInfo node2 = repEnvInfo[1];

        RepEnvInfo master = node1;

        /* The (original incarnation of the) node to be moved. */
        RepEnvInfo mover = repEnvInfo[2];

        /* The new incarnation, at the new network address. */
        RepEnvInfo moved = repEnvInfo[3];

        closeNodes(mover);

        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        helpers.add(master.getRepConfig().getNodeSocketAddress());
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                      helpers,
                                      RepTestUtils.readRepNetConfig());

        /*
         * "Move" the node to a new port address.  The new port is the one
         * reserved in the as-yet-unused "moved" installation, so we must tell
         * the group to use this new port.
         */
        String nodeName = mover.getRepConfig().getNodeName();
        int newPort = moved.getRepConfig().getNodePort();
        rga.updateAddress(nodeName, "localhost", newPort);

        /*
         * Take the pre-existing node name and poke it into the new
         * installation.  (By default it was assigned a fourth, distinct node
         * name during set-up.)
         */
        moved.getRepConfig().setNodeName(nodeName);
        restartNodes(moved);

        /*
         * Now imagine a network partition occurs: node1 can only reach the
         * new, duly authorized incarnation of node3.  There's nothing wrong
         * with that.
         */
        closeNodes(repEnvInfo);
        restartNodes(node1, moved);

        /*
         * On the other side of the partition, node2 can only reach the old,
         * obsolete incarnation of node3.  Ideally that old node3 should never
         * have been allowed to restart.  But let's make sure that if it were
         * to start, no harm would come.
         *
         * It would be nice if we could use the usual restartNodes() method
         * here.  But that method throws an AssertionError if either node
         * fails to fully come up.  Here's what we expect to happen here:
         *
         * - both nodes start to come up
         * - an election is held, and node2 wins; we know this because node3
         *   had to be down in order to allow the updateAddress() operation to
         *   proceed, so it didn't see the associated GroupDB update; node2
         *   must have received it, because the master required the ack from it
         *   in order to confirm the operation.
         *
         *   Note that if this hadn't been the case we would be in trouble,
         *   because node3 would have no reason to reject a Feeder connection
         *   from node2
         * - node3 tries to establish a Feeder connection to master node2, but
         *   this is rejected because the obsolete network address no longer
         *   matches.
         */
        closeNodes(repEnvInfo);
        EnvOpenThread t2 = new EnvOpenThread(node2);
        t2.start();
        EnvOpenThread t3 = new EnvOpenThread(mover);
        t3.start();
        long waitTime = 2 * 60 * 1000;
        t2.join(waitTime);
        assertFalse(t2.isAlive());
        if (t2.testException != null) {
            t2.testException.printStackTrace();
            fail("expected non-authoritative master failed to restart");
        }
        t3.join(waitTime);
        assertFalse(t3.isAlive());
        assertTrue(t3.testException instanceof EnvironmentFailureException);

        final ReplicatedEnvironment env2 = node2.getEnv();
        assertNotNull(env2);
        assertTrue(env2.isValid());
        assertTrue(env2.getState().isMaster());
    }
}
