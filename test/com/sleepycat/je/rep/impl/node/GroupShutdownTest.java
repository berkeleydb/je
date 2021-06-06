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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.CommitToken;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.TestHookAdapter;

public class GroupShutdownTest extends RepTestBase {

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        /* Include a SECONDARY node. */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        repEnvInfo[repEnvInfo.length-1].getRepConfig().setNodeType(
            NodeType.SECONDARY);
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        RepNode.queryGroupForMembershipBeforeSleepHook = null;
        RepNode.beforeFindRestoreSupplierHook = null;
        RepNode.queryGroupForMembershipBeforeQueryForMaster = null;
        RepUtils.openSocketChannelHook = null;
    }

    @Test
    public void testShutdownExceptions() {
        createGroup();
        ReplicatedEnvironment mrep = repEnvInfo[0].getEnv();

        try {
            repEnvInfo[1].getEnv().shutdownGroup(10000, TimeUnit.MILLISECONDS);
            fail("expected exception");
        } catch (IllegalStateException e) {
            /* OK, shutdownGroup on Replica. */
        }

        ReplicatedEnvironment mrep2 =
            new ReplicatedEnvironment(repEnvInfo[0].getEnvHome(),
                                      repEnvInfo[0].getRepConfig(),
                                      repEnvInfo[0].getEnvConfig());

        try {
            mrep.shutdownGroup(10000, TimeUnit.MILLISECONDS);
            fail("expected exception");
        } catch (IllegalStateException e) {
            /* OK, multiple master handles. */
            mrep2.close();
        }
        mrep.shutdownGroup(10000, TimeUnit.MILLISECONDS);
        for (int i=1; i < repEnvInfo.length; i++) {
            repEnvInfo[i].closeEnv();
        }
    }

    @Test
    public void testShutdownTimeout()
        throws InterruptedException {

        new ShutdownSupport() {
            @Override
            void checkException(GroupShutdownException e){}
        }.shutdownBasic(500, 1);
    }

    @Test
    public void testShutdownBasic()
        throws InterruptedException {

        new ShutdownSupport() {
            @Override
            void checkException(GroupShutdownException e) {
                /*
                 * It's possible, in rare circumstances, for the exception to
                 * not contain the shutdown VLSN, that is, for it to be null,
                 * because the VLSNIndex range was not yet initialized. Ignore
                 * it in that circumstance.
                 */
                assertTrue((e.getShutdownVLSN() == null) ||
                           (ct.getVLSN() <=
                            e.getShutdownVLSN().getSequence()));
            }
        }.shutdownBasic(10000, 0);
    }

    /**
     * Test that shutting a node down while it is querying for group membership
     * still causes the node to exit.  Previously, the query failed to check
     * for shutdowns, and so the query continued, causing the node to fail to
     * shutdown.  [#24600]
     */
    @Test
    public void testShutdownDuringQueryForGroupMembership()
        throws Exception {

        /*
         * Number of minutes to wait for things to settle, to support testing
         * on slow machines.
         */
        final long wait = 2;

        createGroup();
        RepTestUtils.shutdownRepEnvs(repEnvInfo);

        final CountDownLatch hookCalled = new CountDownLatch(1);
        final CountDownLatch closeDone = new CountDownLatch(1);
        class Hook extends TestHookAdapter<String> {
            volatile Throwable exception;
            @Override
            public void doHook(String obj) {
                hookCalled.countDown();
                try {
                    assertTrue("Wait for close",
                               closeDone.await(wait, TimeUnit.MINUTES));
                    /*
                     * Wait for the soft shutdown to timeout, which happens
                     * after 4 seconds for RepNode, since it is the failure
                     * to detect the subsequent interrupt that was the bug.
                     * The interrupt should come during this sleep, but the
                     * situation we are testing is where the interrupt occurs
                     * not during a sleep, so ignore the interrupt here.
                     */
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                    }
                } catch (Throwable t) {
                    exception = t;
                }
            }
        };
        final Hook hook = new Hook();
        RepNode.queryGroupForMembershipBeforeSleepHook = hook;

        /*
         * Test using the secondary node, since it is easier to reproduce this
         * situation there.
         */
        final RepEnvInfo nodeInfo = repEnvInfo[repEnvInfo.length - 1];

        /* Reduce the environment setup timeout to make the test quicker */
        nodeInfo.getRepConfig().setConfigParam(
            RepParams.ENV_SETUP_TIMEOUT.getName(), "10 s");

        final OpenEnv openEnv = new OpenEnv(nodeInfo);
        openEnv.start();
        assertTrue("Wait for hook to be called",
                   hookCalled.await(wait, TimeUnit.MINUTES));
        nodeInfo.closeEnv();
        closeDone.countDown();
        openEnv.join(wait * 60 * 1000);
        assertFalse("OpenEnv thread exited", openEnv.isAlive());
        if (hook.exception != null) {
            throw new RuntimeException(
                "Unexpected exception: " + hook.exception);
        }
    }

    /**
     * Check for another EnvironmentWedgedException reported in [#25314].  In
     * this case, the shutdown request comes during the call by
     * RepNode.queryGroupForMembership to Learner.queryForMaster, which was
     * interrupted but did not exit for 10 seconds because it failed to check
     * for shutdowns.  Then the subsequent socket connection to the previous
     * master in RepNode.checkGroupMasterIsAlive was still underway when the
     * shutdown was reported wedged.
     */
    @Test
    public void testShutdownDuringQueryForGroupMembershipQueryLearners()
        throws Exception {

        /*
         * Number of minutes to wait for things to settle, to support testing
         * on slow machines.
         */
        final long wait = 2;

        createGroup();

        /*
         * Make sure that there are multiple helpers, which are needed to
         * cause Learner.queryForMaster to continue looping after an
         * interrupt.
         */
        updateHelperHostConfig();

        /* Shutdown all but the last secondary node */
        RepTestUtils.shutdownRepEnvs(
            Arrays.copyOf(repEnvInfo, repEnvInfo.length - 1));

        /* Track when the Learner.queryForMaster call starts */
        final CountDownLatch queryHookCalled = new CountDownLatch(1);
        class QueryHook extends TestHookAdapter<String> {
            @Override
            public void doHook(String x) {
                queryHookCalled.countDown();
            }
        };
        final QueryHook queryHook = new QueryHook();
        RepNode.queryGroupForMembershipBeforeQueryForMaster = queryHook;

        /*
         * Simulate socket connection timeouts that come from network
         * disconnections.
         */
        class OpenHook extends TestHookAdapter<ConnectOptions> {
            volatile Exception exception;
            @Override
            public void doHook(ConnectOptions options) {
                try {
                    Thread.sleep(options.getOpenTimeout());
                } catch (Exception e) {
                    exception = e;
                }
            }
        };
        final OpenHook openHook = new OpenHook();
        RepUtils.openSocketChannelHook = openHook;

        /*
         * Close the secondary node after the call to Learner.queryForMaster
         * starts
         */
        final RepEnvInfo nodeInfo = repEnvInfo[repEnvInfo.length - 1];
        class CloseEnv extends Thread {
            volatile Throwable exception;
            CloseEnv() { setDaemon(true); }
            @Override
            public void run() {
                try {
                    nodeInfo.closeEnv();
                } catch (Throwable t) {
                    exception = t;
                }
            }
        }
        CloseEnv closeEnv = new CloseEnv();
        assertTrue(queryHookCalled.await(wait, TimeUnit.MINUTES));
        closeEnv.start();

        /* Wait for the close to complete and check for success */
        closeEnv.join(wait * 60 * 1000);
        assertFalse("CloseEnv thread exited", closeEnv.isAlive());
        assertEquals("CloseEnv throws", null, closeEnv.exception);
        assertEquals("OpenHook throws",
                     InterruptedException.class,
                     openHook.exception.getClass());
    }

    /**
     * Test that the node shuts down in a timely manner while searching for
     * network restore suppliers.  Tests that that process checks for shutdown
     * between suppliers.  [#25314]
     */
    @Test
    public void testShutdownDuringFindRestoreSuppliers()
        throws Exception {

        /*
         * Number of minutes to wait for things to settle, to support testing
         * on slow machines.
         */
        final long wait = 2;

        createGroup();

        /* Make sure that there are multiple helpers */
        updateHelperHostConfig();

        /* Install a hook that will be called when finding restore suppliers */
        final CountDownLatch hookCalled = new CountDownLatch(1);
        RepNode.beforeFindRestoreSupplierHook = new TestHookAdapter<String>() {
            @Override
            public void doHook(String obj) {
                hookCalled.countDown();
                try {
                    /*
                     * Wait long enough that multiple waits for failing
                     * attempts to contact network restore suppliers after a
                     * shutdown has been requested will cause the shutdown to
                     * fail.  Ignore interrupts, since, although the interrupts
                     * could interrupt the connect attempts, the code would
                     * continue to contact the next possible supplier.
                     */
                    Thread.sleep(10 * 1000);
                } catch (Throwable t) {
                }
            }
        };

        /* Shutdown all nodes except for the secondary */
        final RepEnvInfo[] allButSecondary =
            Arrays.copyOf(repEnvInfo, repEnvInfo.length - 1);
        RepTestUtils.shutdownRepEnvs(allButSecondary);

        /*
         * Wait for the find restore supplier hook to be called, then close the
         * environment, which needs to succeed.
         */
        assertTrue("Wait for hook to be called",
                   hookCalled.await(wait, TimeUnit.MINUTES));
        final RepEnvInfo nodeInfo = repEnvInfo[repEnvInfo.length - 1];
        nodeInfo.closeEnv();
    }

    /* -- Utility classes and methods -- */

    abstract class ShutdownSupport {
        CommitToken ct;

        abstract void checkException(GroupShutdownException e);

        public void shutdownBasic(long timeoutMs,
                                  int testDelayMs)
            throws InterruptedException {

            createGroup();
            ReplicatedEnvironment mrep = repEnvInfo[0].getEnv();
            leaveGroupAllButMaster();

            ct = populateDB(mrep, TEST_DB_NAME, 1000);
            repEnvInfo[0].getRepNode().feederManager().
                setTestDelayMs(testDelayMs);
            restartReplicasNoWait();

            mrep.shutdownGroup(timeoutMs, TimeUnit.MILLISECONDS);

            for (int i=1; i < repEnvInfo.length; i++) {
                RepEnvInfo repi = repEnvInfo[i];
                final int retries = 100;
                for (int j=0; j < retries; j++) {
                    try {
                        /* Provoke exception */
                        repi.getEnv().getState();
                        if ((j+1) == retries) {
                            fail("expected exception from " +
                                 repi.getRepNode().getNameIdPair());
                        }
                        /* Give the replica time to react */
                        Thread.sleep(1000); /* a second between retries */
                    } catch (GroupShutdownException e) {
                        checkException(e);
                        break;
                    }
                }
                /* Close the handle. */
                repi.closeEnv();
            }
        }
    }

    /**
     * Start up replicas for existing master, but don't wait for any
     * consistency to be reached.
     */
    private void restartReplicasNoWait() {
        for (int i=1; i < repEnvInfo.length; i++) {
            RepEnvInfo ri = repEnvInfo[i];
            ri.openEnv(new NoConsistencyRequiredPolicy());
        }
    }

    private class OpenEnv extends Thread {
        final RepEnvInfo repEnvInfo;
        volatile Throwable exception;
        OpenEnv(RepEnvInfo repEnvInfo) {
            this.repEnvInfo = repEnvInfo;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                repEnvInfo.openEnv();
            } catch (Throwable t) {
                exception = t;
            }
        }
    }
}
