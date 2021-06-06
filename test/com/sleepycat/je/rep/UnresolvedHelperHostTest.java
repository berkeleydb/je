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

import static com.sleepycat.je.rep.impl.RepParams.SKIP_HELPER_HOST_RESOLUTION;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.LocalAliasNameService;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Test using a replicated environment with a helper host whose host name
 * becomes not resolvable in DNS.
 */
public class UnresolvedHelperHostTest extends RepTestBase {

    private static final String dnsHostPrefix =
        "hostname-from-LocalAliasNameService-";
    private static final String originalSkipHelperHostResolution =
        System.getProperty(SKIP_HELPER_HOST_RESOLUTION, "false");
    private int[] originalDNSCachePolicy;

    @BeforeClass
    public static void setUpClass() {

        /*
         * Print a message if skipping helper host resolution has been
         * suppressed during testing, so that we can confirm that the
         * non-default setting was in effect during testing
         */
        if (Boolean.valueOf(originalSkipHelperHostResolution)) {
            System.err.println(SKIP_HELPER_HOST_RESOLUTION + " = true");
        }
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        RepTestUtils.removeRepEnvironments(envRoot);

        originalDNSCachePolicy = LocalAliasNameService.setDNSCachePolicy(0, 0);

        /* Confirm that DNS providers were installed properly */
        assertEquals("sun.net.spi.nameservice.provider.1",
                     "default",
                     System.getProperty("sun.net.spi.nameservice.provider.1"));
        assertEquals("sun.net.spi.nameservice.provider.2",
                     "dns,localalias",
                     System.getProperty("sun.net.spi.nameservice.provider.2"));
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        LocalAliasNameService.clearAllAliases();

        LocalAliasNameService.setDNSCachePolicy(
            originalDNSCachePolicy[0], originalDNSCachePolicy[1]);

        System.setProperty(SKIP_HELPER_HOST_RESOLUTION,
                           originalSkipHelperHostResolution);
    }

    /**
     * Test starting nodes when one of their helper hosts has a hostname that
     * is not resolvable.  [#23120]
     */
    @Test
    public void testBasic()
        throws Exception {

        try {
            InetAddress.getByName("this-is-an-unknown-hostname");
            assumeThat("Skip when running on systems that resolve unknown" +
                       " hostnames",
                       nullValue());
        } catch (UnknownHostException e) {
        }

        /* Start up the cluster */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        /*
         * Set LOG_FILE_MAX so that RepTestUtils.setupEnvInfo does not disable
         * parameter validation, since validation is what notices the
         * unresolvable helper host
         */
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000");

        File[] dirs = RepTestUtils.makeRepEnvDirs(envRoot, groupSize);
        repEnvInfo = new RepEnvInfo[groupSize];
        for (int i = 0; i < groupSize; i++) {
            EnvironmentConfig ec = envConfig.clone();
            ReplicationConfig rc = RepTestUtils.createRepConfig(i + 1);

            /*
             * Use made-up DNS names for each node so we can test later what
             * happens if we remove them from DNS
             */
            String hostname = dnsHostPrefix + i;
            LocalAliasNameService.addAlias(hostname);
            String hostPort = rc.getNodeHostPort();
            rc.setNodeHostPort(hostPort.replaceAll("localhost", hostname));

            repEnvInfo[i] =
                RepTestUtils.setupEnvInfo(dirs[i], ec, rc, repEnvInfo[0]);
        }

        /* Use the first two nodes as the helpers for the last three */
        String helpers = repEnvInfo[0].getRepConfig().getNodeHostPort() +
            "," +
            repEnvInfo[1].getRepConfig().getNodeHostPort();
        for (int i = 2; i < 5; i++) {
            repEnvInfo[i].getRepConfig().setHelperHosts(helpers);
        }

        RepTestUtils.joinGroup(repEnvInfo);
        RepEnvInfo masterInfo = findMaster(repEnvInfo);

        /* Stop a replica that is a helper and remove its DNS name */
        int replicaIndex = 1;
        RepEnvInfo replicaInfo = repEnvInfo[1];
        if (masterInfo == replicaInfo) {
            replicaIndex = 0;
            replicaInfo = repEnvInfo[0];
        }
        String replicaName = dnsHostPrefix + replicaIndex;
        replicaInfo.closeEnv();
        LocalAliasNameService.removeAlias(replicaName);

        /* Confirm that reopening the node whose DNS entry is missing fails */
        try {
            replicaInfo.openEnv();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            logger.info("Got expected exception: " + e);
        }

        /* Stop the remaining nodes */
        RepEnvInfo[] remainingRepEnvInfo = new RepEnvInfo[groupSize - 1];
        int j = 0;
        for (int i = 0; i < groupSize; i++) {
            if (i != replicaIndex) {
                remainingRepEnvInfo[j++] = repEnvInfo[i];
            }
        }
        RepTestUtils.shutdownRepEnvs(remainingRepEnvInfo);

        /*
         * Check that nodes 2 through 4 cannot start by default because of
         * hostname validation for their helper hosts
         */
        System.setProperty(SKIP_HELPER_HOST_RESOLUTION, "false");
        for (int i = 2; i < 5; i++) {
            try {
                repEnvInfo[i].openEnv();
                fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                logger.info("Got expected exception: " + e);
            }
        }

        /*
         * Set the system property to skip helper host resolution, and confirm
         * that the group starts successfully
         */
        System.setProperty(SKIP_HELPER_HOST_RESOLUTION, "true");
        RepTestUtils.restartGroup(remainingRepEnvInfo);

        /*
         * Close the current master and wait for a new master to be chosen, to
         * make sure that the bad hostname doesn't interfere with elections
         */
        RepEnvInfo prevMasterInfo = findMasterWait(0, repEnvInfo);
        prevMasterInfo.closeEnv();
        masterInfo = findMasterWait(30000, repEnvInfo);
        assertFalse(prevMasterInfo + " should not equal " + masterInfo,
                    prevMasterInfo.equals(masterInfo));
        prevMasterInfo.openEnv();
    }
}
