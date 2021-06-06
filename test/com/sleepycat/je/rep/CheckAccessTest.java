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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Check various modes of database access, including with/without SSL.
 */
public class CheckAccessTest extends TestBase {

    private File envRoot;
    private File[] envHomes;

    @Before
    public void setUp()
        throws Exception {

        envRoot = SharedTestUtils.getTestDir();
        envHomes = RepTestUtils.makeRepEnvDirs(envRoot, 2);
        super.setUp();
    }

    /**
     * Sanity check that no SSL works.
     */
    @Test
    public void testBasicConfig()
        throws Exception {

        checkAccess(null);
    }

    /**
     * Test that SSL works.
     */
    @Test
    public void testSSLOnlyConfig()
        throws Exception {

        Properties props = new Properties();
        setBasicSSLProperties(props);

        checkAccess(props);
    }

    /**
     * Set the basic SSL properties.  These rely on the build.xml configuration
     * that copies keystore and truststore files to the test environment.
     */
    public void setBasicSSLProperties(Properties props)
        throws Exception {

        RepTestUtils.setUnitTestSSLProperties(props);
    }

    /**
     * Check whether a particular access configuration works
     *
     * @param extraProperties Properties to be appended to the standard property
     *        file
     * @param servicePassword A service password to be used for authentication
     */
    private void checkAccess(Properties extraProperties)
        throws Exception {

        String propString = "\n";
        if (extraProperties != null) {
            Iterator<String> piter =
                extraProperties.stringPropertyNames().iterator();
            while (piter.hasNext()) {
                String key = piter.next();
                String value = extraProperties.getProperty(key);
                propString = propString + key + " = " + value + "\n";
            }
        }

        TestUtils.readWriteJEProperties(envHomes[0], propString);
        TestUtils.readWriteJEProperties(envHomes[1], propString);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        /*
         * masterFail and replicaFail are true if the master or replica
         * environment creation failed.
         */
        boolean masterFail = false;
        boolean replicaFail = false;

        ReplicatedEnvironment master = null;
        ReplicatedEnvironment replica = null;

        /* Create the ReplicationConfig for master and replica. */
        ReplicationConfig masterConfig = RepTestUtils.createRepConfig(1);
        masterConfig.setDesignatedPrimary(true);
        masterConfig.setHelperHosts(masterConfig.getNodeHostPort());

        ReplicationConfig replicaConfig = RepTestUtils.createRepConfig(2);
        replicaConfig.setHelperHosts(masterConfig.getNodeHostPort());

        /*
         * Attempt to create the master with the specified EnvironmentConfig.
         */
        master = new ReplicatedEnvironment(
            envHomes[0], masterConfig, envConfig);

        /* Check the specified EnvironmentConfig on the replica. */
        replica = new ReplicatedEnvironment(
            envHomes[1], replicaConfig, envConfig);

        assertTrue(master != null);
        assertTrue(replica != null);

        /*
         * If the specified EnvironmentConfig is correct, wait for
         * replication initialization to finish.
         */
        while (replica.getState() != ReplicatedEnvironment.State.REPLICA) {
            Thread.sleep(1000);
        }

        /* Make sure the test runs on both master and replica. */
        assertTrue(master.getState().isMaster());
        assertTrue(!replica.getState().isMaster());

        /* Close the replica and master. */
        replica.close();
        master.close();
    }
}
