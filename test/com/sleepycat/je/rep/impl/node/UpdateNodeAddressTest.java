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
import static org.junit.Assert.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Tests for the {@code updateAddress()} feature.
 */
public class UpdateNodeAddressTest extends TestBase {
    /* Replication tests use multiple environments. */
    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;

    public UpdateNodeAddressTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Test that incorrect operations will throw correct exceptions.
     */
    @Test
    public void testUpdateExceptions()
        throws Throwable {

        try {
            /* Set up the replication group. */
            repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

            /* Create the ReplicationGroupAdmin. */
            HashSet<InetSocketAddress> helperSets =
                new HashSet<InetSocketAddress>();
            helperSets.add(master.getRepConfig().getNodeSocketAddress());
            ReplicationGroupAdmin groupAdmin =
                new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                          helperSets,
                                          RepTestUtils.readRepNetConfig());

            /*
             * Try to update an unexisted node, expect a
             * MemberNotFoundException.
             */
            try {
                groupAdmin.updateAddress("node4", "localhost", 5004);
                fail("Expect exceptions here");
            } catch (MemberNotFoundException e) {
                /* Expected exception. */
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            }

            /* Try to update the master, expect a MasterStateException. */
            try {
                groupAdmin.updateAddress
                    (master.getNodeName(), "localhost", 5004);
                fail("Expect exceptions here");
            } catch (MasterStateException e) {
                /* Expected exception. */
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            }

            /*
             * Try to update a node that is still alive, expect
             * ReplicaStateException.
             */
            try {
                groupAdmin.updateAddress
                    (repEnvInfo[2].getEnv().getNodeName(),
                     "localhost", 5004);
                fail("Expect exceptions here");
            } catch (ReplicaStateException e) {
                /* Expected exception. */
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /*
     * Test that the node after updating its address can work after removing
     * all its old log files.
     */
    @Test
    public void testUpdateAddressWithNoFormerLogs()
        throws Throwable {

        doTest(true);
    }

    /*
     * Do the test, if deleteLogFiles is true, the former environment home for
     * the node whose address updated will be deleted.
     */
    private void doTest(boolean deleteLogFiles)
        throws Throwable {

        try {

            /*
             * Disable the LocalCBVLSN changes so that no
             * InsufficientLogException will be thrown when restart the node
             * whose address has been updated.
             */
            LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);

            /* Create the replication group. */
            repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

            /* Create the ReplicationGroupAdmin. */
            HashSet<InetSocketAddress> helperSets =
                new HashSet<InetSocketAddress>();
            helperSets.add(master.getRepConfig().getNodeSocketAddress());
            ReplicationGroupAdmin groupAdmin =
                new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                          helperSets,
                                          RepTestUtils.readRepNetConfig());

            /* Shutdown node3. */
            InetSocketAddress nodeAddress =
                repEnvInfo[2].getRepConfig().getNodeSocketAddress();
            String nodeName = repEnvInfo[2].getEnv().getNodeName();
            File envHome = repEnvInfo[2].getEnv().getHome();
            repEnvInfo[2].closeEnv();

            /* Update the address for node3. */
            try {
                groupAdmin.updateAddress(nodeName,
                                         nodeAddress.getHostName(),
                                         nodeAddress.getPort() + 1);
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            }

            /* Restart node3 will get an EnvironmentFailureException. */
            try {
                repEnvInfo[2].openEnv();
                fail("Expect exceptions here.");
            } catch (EnvironmentFailureException e) {
                /* Expected exception. */
                assertEquals(e.getReason(),
                             EnvironmentFailureReason.HANDSHAKE_ERROR);
            } catch (Exception e) {
                fail("Unexpected excpetion: " + e);
            }

            /*
             * Delete all files in node3's environment home so that node3 can
             * restart as a fresh new node.
             */
            assertTrue(envHome.exists());

            /* Delete the former log files if we'd like to. */
            if (deleteLogFiles) {
                for (File file : envHome.listFiles()) {
                    /* Don't delete the je.properties. */
                    if (file.getName().contains("properties")) {
                        continue;
                    }

                    assertTrue(file.isFile());
                    assertTrue(file.delete());
                }
            }

            /* Reset the ReplicationConfig and restart again. */
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            ReplicationConfig repConfig = new ReplicationConfig();
            repConfig.setNodeName(nodeName);
            repConfig.setGroupName(RepTestUtils.TEST_REP_GROUP_NAME);
            repConfig.setNodeHostPort(nodeAddress.getHostName() + ":" +
                                      (nodeAddress.getPort() + 1));
            repConfig.setHelperHosts(master.getRepConfig().getNodeHostPort());

            ReplicatedEnvironment replica = null;
            try {
                replica =
                    new ReplicatedEnvironment(envHome, repConfig, envConfig);
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            } finally {
                if (replica != null) {
                    replica.close();
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /*
     * Test that address updates does work even the node reuses its old log
     * files.
     */
    @Test
    public void testUpdateAddressWithFormerLogs()
        throws Throwable {

        doTest(false);
    }
}
