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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseExistsException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Check that both master and replica nodes catch invalid environment and
 * database configurations.
 */
public class CheckConfigTest extends TestBase {

    private static final String originalSkipHelperHostResolution =
        System.getProperty(RepParams.SKIP_HELPER_HOST_RESOLUTION, "false");

    private final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

    private File envRoot;
    private File[] envHomes;
    private boolean useCorruptHelperHost = false;
    private boolean useLoopbackAddresses = true;

    @Override
    @Before
    public void setUp()
        throws Exception {

        envRoot = SharedTestUtils.getTestDir();
        envHomes = RepTestUtils.makeRepEnvDirs(envRoot, 2);
        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        System.setProperty(RepParams.SKIP_HELPER_HOST_RESOLUTION,
                           originalSkipHelperHostResolution);
    }

    /**
     * Replicated environments do not support non transactional mode.
     */
    @Test
    public void testEnvNonTransactionalConfig()
        throws Exception {

        EnvironmentConfig config = createConfig();
        config.setTransactional(false);
        expectRejection(config);
    }

    /**
     * A configuration of transactional + noLocking is invalid.
     */
    @Test
    public void testEnvNoLockingConfig()
        throws Exception {

        EnvironmentConfig config = createConfig();
        config.setLocking(false);
        expectRejection(config);
    }

    /**
     * ReadOnly = true should be accepted.
     *
     * Since setting environment read only is only possible when an Environment
     * exists, this test first creates a normal Environment and then reopens it
     * with read only configuration.
     */
    @Test
    public void testEnvReadOnlyConfig()
        throws Exception {

        EnvironmentConfig config = createConfig();
        expectAcceptance(config);
        config.setReadOnly(true);
        expectRejection(config);
    }

    /**
     * AllowCreate = false should be accepted.
     *
     * Since setting environment allowCreate to false is only possible when an
     * Environment exists, this test creates a normal Environment and then
     * reopens it with allowCreate=false configuration.
     */
    @Test
    public void testEnvAllowCreateFalseConfig()
        throws Exception {

        EnvironmentConfig config = createConfig();
        expectAcceptance(config);
        config.setAllowCreate(false);
        expectAcceptance(config);
    }

    /**
     * SharedCache = true should be accepted.
     */
    @Test
    public void testEnvSharedCacheConfig()
        throws Exception {

        EnvironmentConfig config = createConfig();
        config.setSharedCache(true);
        expectAcceptance(config);
    }

    /**
     * Serializable isolation = true should be accepted.
     */
    @Test
    public void testEnvSerializableConfig()
        throws Exception {

        EnvironmentConfig config = createConfig();
        config.setTxnSerializableIsolation(true);
        expectAcceptance(config);
    }

    /**
     * Check that a bad helper host is accepted.
     */
    @Test
    public void testBadHelperHost()
        throws Exception {
        EnvironmentConfig config = createConfig();
        useCorruptHelperHost = true;
        System.setProperty(RepParams.SKIP_HELPER_HOST_RESOLUTION, "true");

        /*
         * The replica should fail because of the mix of a loopback address for
         * node and a non-resolvable address in helpers
         */
        checkEnvConfig(config,
                       false /* masterInvalid */,
                       true /* replicaInvalid */);

        /*
         * Remove any existing environment files to avoid problems when
         * changing from loopback to local host names
         */
        RepTestUtils.removeRepEnv(envHomes[0]);
        RepTestUtils.removeRepEnv(envHomes[1]);

        /*
         * If the local host isn't a loopback address, arrange to use that
         * address and expect the non-resolvable host to be OK because it is
         * also a non-loopback address.  Just skip if the local host is a
         * loopback address.
         */
        if (!InetAddress.getLocalHost().isLoopbackAddress()) {
            useLoopbackAddresses = false;
            expectAcceptance(config);
        }

        /*
         * If unknown host names are not resolvable to IP addresses, then test
         * that the configuration fails when checking for unresolvable helper
         * addresses. On some systems, unknown host names are redirected to IP
         * addresses, so skip this test in that case.
         */
        boolean unknownHostnamesAreResolved = false;
        try {
            InetAddress.getByName("unknownhostfoobar");
            unknownHostnamesAreResolved = true;
        } catch (UnknownHostException e) {
        }
        if (!unknownHostnamesAreResolved) {
            System.setProperty(RepParams.SKIP_HELPER_HOST_RESOLUTION, "false");
            expectRejection(config);
        }
        useCorruptHelperHost = false;
        useLoopbackAddresses = true;
    }

    /**
     * Return a new transactional EnvironmentConfig for test use.
     */
    private EnvironmentConfig createConfig() {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);

        return config;
    }

    /**
     * Return a new transactional DatabaseConfig for test use.
     */
    private DatabaseConfig createDbConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);

        return config;
    }

    /**
     * Wrap checkEnvConfig in this method to make the intent of the test
     * obvious.
     */
    private void expectAcceptance(EnvironmentConfig envConfig)
        throws Exception {

        checkEnvConfig(envConfig,
                       false /* masterInvalid */,
                       false /* replicaInvalid */);
    }

    /**
     * Wrap checkEnvConfig in this method to make the intent of the test
     * obvious.
     */
    private void expectRejection(EnvironmentConfig envConfig)
            throws Exception {

        checkEnvConfig(envConfig,
                       true /* masterInvalid */,
                       true /* replicaInvalid */);
    }

    /**
     * Check whether an EnvironmentConfig is valid.
     *
     * @param envConfig the EnvironmentConfig to check
     * @param masterInvalid whether creating master should fail
     * @param replicaInvalid whether creating replica should fail
     */
    private void checkEnvConfig(EnvironmentConfig envConfig,
                                boolean masterInvalid,
                                boolean replicaInvalid)
        throws Exception {

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

        /*
         * Use non-loopback addresses if requested, so that we can test
         * unresolvable host names without a mix of loopback and non-loopback
         * addresses
         */
        final String localHost = InetAddress.getLocalHost().getHostName();
        if (!useLoopbackAddresses) {
            masterConfig.setNodeHostPort(
                localHost + ":" + masterConfig.getNodePort());
        }
        masterConfig.setHelperHosts(masterConfig.getNodeHostPort());
        ReplicationConfig replicaConfig = RepTestUtils.createRepConfig(2);
        if (!useLoopbackAddresses) {
            replicaConfig.setNodeHostPort(
                localHost + ":" + replicaConfig.getNodePort());
        }
        if (useCorruptHelperHost) {
            try {
                replicaConfig.setHelperHosts(masterConfig.getNodeHostPort() +
                                             ",unknownhostfoobar:1111");
            } catch (IllegalArgumentException e) {
                masterFail = true;
                logger.info("Unresolvable helper: " + e);
            }
            final boolean checkHelperHostResolution =
                !Boolean.getBoolean(RepParams.SKIP_HELPER_HOST_RESOLUTION);
            assertEquals(checkHelperHostResolution, masterFail);

            /*
             * If creating the configuration failed because of an unresolvable
             * helper host, then there is nothing more to test
             */
            if (checkHelperHostResolution) {
                return;
            }
        } else {
            replicaConfig.setHelperHosts(masterConfig.getNodeHostPort());
        }

        /*
         * Attempt to create the master with the specified EnvironmentConfig.
         */
        try {
            master = new ReplicatedEnvironment(envHomes[0],
                                               masterConfig,
                                               envConfig);
        } catch (IllegalArgumentException e) {
            logger.info("Create master: " + e);
            masterFail = true;
        }

        /*
         * If the master is expected to fail and the test tried to create a
         * replica in the following steps, it would actually try to create a
         * master.
         *
         * Since the test needs to test on both master and replica, create a
         * real master here.
         */
        if (masterInvalid) {
            EnvironmentConfig okConfig =
                RepTestUtils.createEnvConfig(RepTestUtils.DEFAULT_DURABILITY);
            master = new ReplicatedEnvironment(envHomes[0], masterConfig,
                                               okConfig);
        }

        /* Check the specified EnvironmentConfig on the replica. */
        try {
            replica = new ReplicatedEnvironment(envHomes[1],
                                                replicaConfig,
                                                envConfig);
        } catch (IllegalArgumentException e) {
            logger.info("Create replica: " + e);
            replicaFail = true;
        }

        /* Check whether the master and replica creations are as expected. */
        assertEquals("masterFail", masterInvalid, masterFail);
        assertEquals("replicaFail", replicaInvalid, replicaFail);

        /*
         * If the replica is expected to fail, close the master and return.
         */
        if (replicaInvalid) {
            if (master != null) {
                assertTrue(master.getState().isMaster());
                master.close();
            }

            return;
        }

        if (master != null && replica != null) {
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

    /**
     * AllowCreate = false should be accepted.
     *
     * Setting allowCreate to false is only possible when the database already
     * exists. Because of that, this test first creates a database and then
     * reopens it with allowCreate = false configuration.
     */
    @Test
    public void testDbAllowCreateFalseConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        expectDbAcceptance(dbConfig, true);
        dbConfig.setAllowCreate(false);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * Replicated datatabases do not support non transactional mode.
     */
    @Test
    public void testDbNonTransactionalConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setTransactional(false);
        expectDbRejection(dbConfig, false);
    }

    /**
     * A database configuration of transactional + deferredWrite is invalid.
     */
    @Test
    public void testDbDeferredWriteConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setDeferredWrite(true);
        expectDbRejection(dbConfig, false);
    }

    /**
     * A database configuration of transactional + temporary is invalid.
     */
    @Test
    public void testDbTemporaryConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setTemporary(true);
        expectDbRejection(dbConfig, false);
    }

    /**
     * ExclusiveCreate = true should be accepted on the master.
     *
     * Setting exclusiveCreate is expected to fail on the replica. It's because
     * when a database is created on master, replication will create the same
     * database on the replica. When the replica tries to create the database,
     * it will find the database already exists. When we set exclusiveCreate =
     * true, the replica will throw out a DatabaseExistException. The check
     * for this is done within the logic for expectDbAcceptance.
     */
    @Test
    public void testDbExclusiveCreateConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setExclusiveCreate(true);
        expectDbAcceptance(dbConfig, true);
    }

    /**
     * KeyPrefixing = true should be accpted.
     */
    @Test
    public void testDbKeyPrefixingConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setKeyPrefixing(true);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * ReadOnly = true should be accpted.
     *
     * Database read only is only possible when the database exists, so this
     * test first creates a database and then reopens it with read only
     * configuration.
     */
    @Test
    public void testDbReadOnlyConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        expectDbAcceptance(dbConfig, true);
        dbConfig.setReadOnly(true);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * SortedDuplicates = true should be accpted.
     */
    @Test
    public void testDbSortedDuplicatesConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setSortedDuplicates(true);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * OverrideBtreeComparator = true should be accepted.
     */
    @Test
    public void testDbOverideBtreeComparatorConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setOverrideBtreeComparator(true);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * OverrideDuplicatComparator = true should be accepted.
     */
    @Test
    public void testDbOverrideDuplicateComparatorConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        dbConfig.setOverrideDuplicateComparator(true);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * UseExistingConfig = true should be accepted.
     *
     * UseExistingConfig is only possible when the database exists, so this
     * test first creates a database and then reopens it with UseExistingConfig
     * configuration.
     */
    @Test
    public void testDbUseExistingConfig()
        throws Exception {

        DatabaseConfig dbConfig = createDbConfig();
        expectDbAcceptance(dbConfig, true);
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setUseExistingConfig(true);
        dbConfig.setReadOnly(true);
        expectDbAcceptance(dbConfig, false);
    }

    /**
     * Wrap checkDbConfig in this method to make the intent of the test
     * obvious.
     */
    private void expectDbAcceptance(DatabaseConfig dbConfig, boolean doSync)
        throws Exception {

        checkDbConfig(dbConfig, false /* isInvalid */, doSync);
    }

    /**
     * Wrap checkEnvConfig in this method to make the intent of the test
     * obvious.
     */
    private void expectDbRejection(DatabaseConfig dbConfig, boolean doSync)
            throws Exception {

        checkDbConfig(dbConfig, true /* isInvalid */, doSync);
    }

    /**
     * The main function checks whether a database configuration is valid.
     *
     * @param dbConfig The DatabaseConfig to check.
     * @param isInvalid if true, dbConfig represents an invalid configuration
     * and we expect database creation to fail.
     * @param doSync If true, the test should do a group sync after creating
     * the database on the master
     */
    public void checkDbConfig(DatabaseConfig dbConfig,
                              boolean isInvalid,
                              boolean doSync)
        throws Exception {

        /*
         * masterFail and replicaFail are true if the master or replica
         * database creation failed.
         */
        boolean masterFail = false;
        boolean replicaFail =false;

        /* Create an array of replicators successfully and join the group. */
        RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 2);
        repEnvInfo[0].getRepConfig().setDesignatedPrimary(true);
        RepTestUtils.joinGroup(repEnvInfo);

        /* Create the database with the specified configuration on master. */
        Database masterDb = null;
        try {
            masterDb = repEnvInfo[0].getEnv().openDatabase(null, "test",
                                                           dbConfig);
        } catch (IllegalArgumentException e) {
            masterFail = true;
        }

        /*
         * The test does a group sync when the tested configuration needs to
         * create a real database first.
         *
         * If a group sync isn't done, the replica would incorrectly try to
         * create the database since it hasn't seen it yet. Since write
         * operations on the replica are forbidden, the test would fail, which
         * is not expected.
         */
        if (doSync) {
            RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);
        }

        /* Open the database with the specified configuration on replica. */
        Database replicaDb = null;
        try {
            replicaDb = repEnvInfo[1].getEnv().openDatabase(null, "test",
                                                            dbConfig);
        } catch (IllegalArgumentException e) {
            replicaFail = true;
        } catch (ReplicaWriteException e) {
            /*
             * If the test throws a ReplicaStateException, it's because it
             * tries to create a new database on replica, but replica doesn't
             * allow create operation, it's thought to be valid.
             */
        } catch (DatabaseExistsException e) {
            replicaFail = true;
        }

        /* Check the validity here. */
        if (isInvalid) {
            assertTrue(masterFail && replicaFail);
        } else {

            /*
             * The exclusiveCreate config is checked explicitly here, because
             * it has different master/replica behavior.
             */
            if (dbConfig.getExclusiveCreate()) {
                assertFalse(masterFail);
                assertTrue(replicaFail);
            } else {
                assertFalse(masterFail || replicaFail);
            }
        }

        /* Shutdown the databases and environments. */
        if (masterDb != null) {
            masterDb.close();
        }

        if (replicaDb != null) {
            replicaDb.close();
        }

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }
}
