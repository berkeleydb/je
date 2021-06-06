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

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepParams.ChannelTypeConfigParam;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup.TestHook;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.PropUtil;

import org.junit.Test;

/*
 * TODO:
 * 1) Test null argument for repConfig
 *
 */
/**
 * Check ReplicatedEnvironment-specific functionality; environment-specific
 * functionality is covered via the DualTest infrastructure.
 */
public class ReplicatedEnvironmentTest extends RepTestBase {

    private static final String DEFAULT_NODEHOST = "localhost:5000";

    /**
     * Test to validate the code fragments contained in the javdoc for the
     * ReplicatedEnvironment class, or to illustrate statements made there.
     */
    @Test
    public void testClassJavadoc()
        throws DatabaseException {

        EnvironmentConfig envConfig = getEnvConfig();
        ReplicationConfig repEnvConfig = getRepEnvConfig();

        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        repEnv.close();

        repEnv = new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        RepImpl repImpl = repEnv.getNonNullRepImpl();
        assertTrue(repImpl != null);

        repEnv.close();
        /* It's ok to check after it's closed. */
        try {
            repEnv.getState();
            fail("expected exception");
        } catch (IllegalStateException e) {
            /* Expected. */
        }
    }

    /**
     * This is literally the snippet of code used as an
     * startup example. Test here to make sure it compiles.
     */
    @Test
    public void testExample() {
        File envHome = new File(".");
        try {
            /******* begin example *************/
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            // Identify the node
            ReplicationConfig repConfig = new ReplicationConfig();
            repConfig.setGroupName("PlanetaryRepGroup");
            repConfig.setNodeName("mercury");
            repConfig.setNodeHostPort("mercury.acme.com:5001");

            // This is the first node, so its helper is itself
            repConfig.setHelperHosts("mercury.acme.com:5001");

            ReplicatedEnvironment repEnv =
                new ReplicatedEnvironment(envHome, repConfig, envConfig);
            /******* end example *************/
            repEnv.close();
        } catch (IllegalArgumentException expected) {
        }

        try {
            /******* begin example *************/
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);

            // Identify the node
            ReplicationConfig repConfig =
                new ReplicationConfig("PlanetaryRepGroup", "Jupiter",
                                      "jupiter.acme.com:5002");

            // Use the node at mercury.acme.com:5001 as a helper to find the
            // rest of the group.
            repConfig.setHelperHosts("mercury.acme.com:5001");

            ReplicatedEnvironment repEnv =
                new ReplicatedEnvironment(envHome, repConfig, envConfig);

            /******* end example *************/
            repEnv.close();
        } catch (IllegalArgumentException expected) {
        }
    }

    private EnvironmentConfig getEnvConfig() {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        return envConfig;
    }

    private ReplicationConfig getRepEnvConfig() {
        /* ** DO NOT ** use localhost in javadoc. */
        ReplicationConfig repEnvConfig =
            new ReplicationConfig("ExampleGroup", "node1", DEFAULT_NODEHOST);

        /* Configure it to be the master. */
        repEnvConfig.setHelperHosts(repEnvConfig.getNodeHostPort());
        return repEnvConfig;
    }

    @Test
    public void testJoinGroupJavadoc()
        throws DatabaseException {

        EnvironmentConfig envConfig = getEnvConfig();
        ReplicationConfig repEnvConfig = getRepEnvConfig();

        ReplicatedEnvironment repEnv1 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        assertEquals(ReplicatedEnvironment.State.MASTER, repEnv1.getState());

        ReplicatedEnvironment repEnv2 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        assertEquals(ReplicatedEnvironment.State.MASTER, repEnv2.getState());

        repEnv1.close();
        repEnv2.close();
    }


    /**
     * Verify that an EFE will result in some other node being elected, even
     * if the EFE environment has not been closed.
     */
    @Test
    public void testEFE() throws InterruptedException {
        createGroup();
        final String masterName = repEnvInfo[0].getEnv().getNodeName();
        final CountDownLatch masterChangeLatch = new CountDownLatch(1);

        StateChangeListener listener = new StateChangeListener() {

            @Override
            public void stateChange(StateChangeEvent sce)
                throws RuntimeException {

               if (sce.getState().isActive() &&
                   !sce.getMasterNodeName().equals(masterName)) {
                   masterChangeLatch.countDown();
               }
            }
        };

        repEnvInfo[1].getEnv().setStateChangeListener(listener);

        /* Fail the master. */
        @SuppressWarnings("unused")
        EnvironmentFailureException efe =
            new EnvironmentFailureException(repEnvInfo[0].getRepImpl(),
                                            EnvironmentFailureReason.
                                            INSUFFICIENT_LOG);
        /* Validate that master has moved on. */
        boolean ok = masterChangeLatch.await(10, TimeUnit.SECONDS);

        try {
            assertTrue(ok);
        } finally {
            /* Now close the EFE environment */
            repEnvInfo[0].getEnv().close();
        }
    }

    /**
     * Verify exceptions resulting from timeouts due to slow syncups, or
     * because the Replica was too far behind and could not catch up in the
     * requisite time.
     */
    @Test
    public void testRepEnvTimeout()
        throws DatabaseException {

        createGroup();
        repEnvInfo[2].closeEnv();
        populateDB(repEnvInfo[0].getEnv(), "db", 10);

        /* Get past syncup for replica consistency exception. */
        repEnvInfo[2].getRepConfig().setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "1000 ms");

        TestHook<Object> syncupEndHook = new TestHook<Object>() {
            @Override
            public void doHook() throws InterruptedException {
                Thread.sleep(Integer.MAX_VALUE);
            }
        };

        ReplicaFeederSyncup.setGlobalSyncupEndHook(syncupEndHook);

        /* Syncup driven exception. */
        try {
            repEnvInfo[2].openEnv();
        } catch (ReplicaConsistencyException ume) {
            /* Expected exception. */
        } finally {
            ReplicaFeederSyncup.setGlobalSyncupEndHook(null);
        }

        repEnvInfo[2].getRepConfig().setConfigParam
            (RepParams.TEST_REPLICA_DELAY.getName(),
             Integer.toString(Integer.MAX_VALUE));
        repEnvInfo[2].getRepConfig().setConfigParam
            (ReplicationConfig.ENV_CONSISTENCY_TIMEOUT, "1000 ms");
        try {
            repEnvInfo[2].openEnv();
        } catch (ReplicaConsistencyException ume) {
            /* Expected exception. */
        }
    }

    /*
     * Ensure that default consistency policy can be overridden in the handle.
     */
    @Test
    public void testRepEnvConfig()
        throws DatabaseException {

        EnvironmentConfig envConfig = getEnvConfig();

        ReplicationConfig repEnvConfig = getRepEnvConfig();

        ReplicatedEnvironment repEnv1 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        /* Verify that default is used. */
        ReplicationConfig repConfig1 = repEnv1.getRepConfig();
        assertEquals(RepUtils.getReplicaConsistencyPolicy
                     (RepParams.CONSISTENCY_POLICY.getDefault()),
                     repConfig1.getConsistencyPolicy());

        /* Override the policy in the handle. */
        repEnvConfig.setConsistencyPolicy
            (NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        ReplicatedEnvironment repEnv2 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        ReplicationConfig repConfig2 = repEnv2.getRepConfig();
        /* New handle should have new policy. */
        assertEquals(NoConsistencyRequiredPolicy.NO_CONSISTENCY,
                     repConfig2.getConsistencyPolicy());

        /* Old handle should retain the old default policy. */
        assertEquals(RepUtils.getReplicaConsistencyPolicy
                     (RepParams.CONSISTENCY_POLICY.getDefault()),
                     repConfig1.getConsistencyPolicy());

        /* Default should be retained for new handles. */
        repEnvConfig = new ReplicationConfig();
        repEnvConfig.setGroupName("ExampleGroup");
        repEnvConfig.setNodeName("node1");
        repEnvConfig.setNodeHostPort(DEFAULT_NODEHOST);
        ReplicatedEnvironment repEnv3 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        ReplicationConfig repConfig3 = repEnv3.getRepConfig();
        assertEquals(RepUtils.getReplicaConsistencyPolicy
                     (RepParams.CONSISTENCY_POLICY.getDefault()),
                     repConfig3.getConsistencyPolicy());

        repEnv1.close();
        repEnv2.close();
        repEnv3.close();
    }

    /*
     * Ensure that channel configuration is set up properly.  In particular,
     * test that a ReplicationNetworkConfig object that is passed to a
     * ReplicatedEnvironment constructor (indirectly) is retained through the
     * RepEnvImpl construction process unless a channelType override says
     * otherwise.
     */
    @Test
    public void testRepEnvNetConfig()
        throws DatabaseException {

        Properties homeNetProps = RepTestUtils.readNetProps();
        String homeNetChanType = homeNetProps.getProperty("je.rep.channelType");

        EnvironmentConfig envConfig = getEnvConfig();

        ReplicationConfig repEnvConfig = getRepEnvConfig();

        ReplicatedEnvironment repEnv1 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        /* Verify that default is used if no file property specified. */
        ReplicationConfig repConfig1 = repEnv1.getRepConfig();

        if (homeNetChanType == null) {
            /* No je.properties override */
            assertEquals(RepParams.CHANNEL_TYPE.getDefault(),
                         repConfig1.getRepNetConfig().getChannelType());

        } else {
            assertEquals(homeNetChanType,
                         repConfig1.getRepNetConfig().getChannelType());
        }

        /* Try setting to basic in the config. */
        repEnvConfig.setRepNetConfig(new ReplicationBasicConfig());

        ReplicatedEnvironment repEnv2 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        ReplicationConfig repConfig2 = repEnv2.getRepConfig();

        if (homeNetChanType == null) {
            /* No je.properties override */
            assertEquals(ChannelTypeConfigParam.BASIC,
                         repConfig2.getRepNetConfig().getChannelType());

        } else {
            assertEquals(homeNetChanType,
                         repConfig2.getRepNetConfig().getChannelType());
        }

        /* Try setting to ssl in the config. */
        Properties sslProps = new Properties();
        RepTestUtils.setUnitTestSSLProperties(sslProps);

        /*
         * Remove the channelType property to verify that it gets
         * set automatically based on the created type.
         */
        sslProps.remove(ReplicationNetworkConfig.CHANNEL_TYPE);

        ReplicationSSLConfig repSSLConfig = new ReplicationSSLConfig(sslProps);
        PasswordSource pwdSrc =
            new PasswordSource() {
                @Override
                public char[] getPassword() {
                    return new char[0];
                }
            };
        repSSLConfig.setSSLKeyStorePasswordSource(pwdSrc);
        repEnvConfig.setRepNetConfig(repSSLConfig);

        ReplicatedEnvironment repEnv3 =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        ReplicationConfig repConfig3 = repEnv3.getRepConfig();

        if (homeNetChanType == null) {
            /* No je.properties override */
            assertEquals(ChannelTypeConfigParam.SSL,
                         repConfig3.getRepNetConfig().getChannelType());
            ReplicationSSLConfig envRepSSLConfig3 =
                (ReplicationSSLConfig) repConfig3.getRepNetConfig();
            assertEquals(pwdSrc,
                         envRepSSLConfig3.getSSLKeyStorePasswordSource());

        } else {
            assertEquals(homeNetChanType,
                         repConfig3.getRepNetConfig().getChannelType());
        }

        repEnv1.close();
        repEnv2.close();
        repEnv3.close();
    }

    @Test
    public void testRepEnvMutableConfig() {
        final EnvironmentConfig envConfig = getEnvConfig();
        final ReplicationConfig repEnvConfig = getRepEnvConfig();
        final ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        /* Test mutable change to REPLAY and HELPER_HOST config parameters. */
        final ReplicationMutableConfig mutableConfig =
                repEnv.getRepMutableConfig();

        final int defaultHandles =
                Integer.parseInt(RepParams.REPLAY_MAX_OPEN_DB_HANDLES.
                                 getDefault());

        assertEquals(defaultHandles,
                     ((RepImpl)DbInternal.getNonNullEnvImpl(repEnv)).
                     getRepNode().getReplica().getDbCache().getMaxEntries());

        mutableConfig.setConfigParam(ReplicationMutableConfig.
                                     REPLAY_MAX_OPEN_DB_HANDLES,
                                     Integer.toString(defaultHandles + 1));

        final int defaultTimeoutMs = PropUtil.parseDuration(
            RepParams.REPLAY_DB_HANDLE_TIMEOUT.getDefault());

        assertEquals(defaultTimeoutMs,
                     ((RepImpl)DbInternal.getNonNullEnvImpl(repEnv)).
                     getRepNode().getReplica().getDbCache().getTimeoutMs());

        mutableConfig.setConfigParam(ReplicationMutableConfig.
                                     REPLAY_DB_HANDLE_TIMEOUT,
                                     Integer.toString(defaultTimeoutMs + 1) +
                                     " ms");

        repEnv.setRepMutableConfig(mutableConfig);

        assertEquals(defaultHandles + 1,
                     ((RepImpl)DbInternal.getNonNullEnvImpl(repEnv)).
                     getRepNode().getReplica().getDbCache().getMaxEntries());


        assertEquals(defaultTimeoutMs + 1,
                     ((RepImpl)DbInternal.getNonNullEnvImpl(repEnv)).
                     getRepNode().getReplica().getDbCache().getTimeoutMs());

        /* Check the current value of helper hosts */
        String currentHelperHosts = mutableConfig.getHelperHosts();
        assertEquals(DEFAULT_NODEHOST, currentHelperHosts);

        /* Set a new helper host value */
        mutableConfig.setHelperHosts("localhost:13100");
        repEnv.setRepMutableConfig(mutableConfig);
        assertEquals("localhost:13100",
                     RepInternal.getNonNullRepImpl(repEnv).
                        getConfigManager().get(RepParams.HELPER_HOSTS));
        repEnv.close();
    }

    /*
     * Verify that configuring BIND_INADDR_ANY results in use of a wildcard
     * address by the ServiceDispatcher.
     */
    @Test
    public void testRepInaddrAnyConfig() {
        final EnvironmentConfig envConfig = getEnvConfig();
        final ReplicationConfig repEnvConfig = getRepEnvConfig();

        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        /* Verify specific address */
        assertTrue(!repEnv.getNonNullRepImpl().getRepNode().
                   getServiceDispatcher().getSocketBoundAddress().
                   isAnyLocalAddress());
        repEnv.close();

        repEnvConfig.setConfigParam(ReplicationConfig.BIND_INADDR_ANY,
            "true");

        repEnv = new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        /* Verify that it's a wildcard */
        assertTrue(repEnv.getNonNullRepImpl().getRepNode().
                   getServiceDispatcher().getSocketBoundAddress().
                   isAnyLocalAddress());

        repEnv.close();
    }

    /*
     * Ensure that only a r/o standalone Environment can open on a closed
     * replicated Environment home directory.
     */
    @Test
    public void testEnvOpenOnRepEnv()
        throws DatabaseException {

        final EnvironmentConfig envConfig = getEnvConfig();
        final ReplicationConfig repEnvConfig = getRepEnvConfig();
        final DatabaseConfig dbConfig = getDbConfig();

        final ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        Database db = repEnv.openDatabase(null, "db1", dbConfig);
        final DatabaseEntry dk = new DatabaseEntry(new byte[10]);
        final DatabaseEntry dv = new DatabaseEntry(new byte[10]);
        OperationStatus stat = db.put(null, dk, dv);
        assertEquals(OperationStatus.SUCCESS, stat);
        db.close();
        repEnv.close();

        try {
            Environment env = new Environment(envRoot, envConfig);
            env.close();
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            /* Expected. */
        }
        envConfig.setReadOnly(true);
        dbConfig.setReadOnly(true);

        /* Open the replicated environment as read-only. Should be OK. */
        Environment env = new Environment(envRoot, envConfig);
        Transaction txn = env.beginTransaction(null, null);
        db = env.openDatabase(txn, "db1", dbConfig);
        stat = db.get(txn, dk, dv, LockMode.READ_COMMITTED);
        assertEquals(OperationStatus.SUCCESS, stat);

        db.close();
        txn.commit();
        env.close();
    }

    /*
     * Check that JE throws an UnsupportedOperationException if we open a r/w
     * standalone Environment on a opened replicated Environment home
     * directory.
     */
    @Test
    public void testOpenEnvOnAliveRepEnv()
        throws DatabaseException {

        final EnvironmentConfig envConfig = getEnvConfig();
        final ReplicationConfig repConfig = getRepEnvConfig();

        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repConfig, envConfig);

        Environment env = null;
        try {
            env = new Environment(envRoot, envConfig);
            env.close();
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            /* Expected */
        }

        envConfig.setReadOnly(true);

        try {
            env = new Environment(envRoot, envConfig);
            env.close();
            fail("expected exception");
        } catch (IllegalArgumentException e) {

            /*
             * Expect IllegalArgumentException since the ReplicatedEnvironment
             * this Environment open is not read only.
             */
        }

        repEnv.close();
    }

    @Test
    public void testRepEnvUsingEnvHandle()
        throws DatabaseException {

        final EnvironmentConfig envConfig = getEnvConfig();
        final DatabaseConfig dbConfig = getDbConfig();
        final DatabaseEntry dk = new DatabaseEntry(new byte[10]);
        final DatabaseEntry dv = new DatabaseEntry(new byte[10]);

        {
            final ReplicationConfig repEnvConfig = getRepEnvConfig();
            final ReplicatedEnvironment repEnv1 =
                new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

            final Database db = repEnv1.openDatabase(null, "db1", dbConfig);

            OperationStatus stat = db.put(null, dk, dv);
            assertEquals(OperationStatus.SUCCESS, stat);
            db.close();
            repEnv1.close();
        }

        envConfig.setReadOnly(true);
        final Environment env = new Environment(envRoot, envConfig);
        dbConfig.setReadOnly(true);

        final Transaction t1 = env.beginTransaction(null, null);
        final Database db = env.openDatabase(null, "db1", dbConfig);

        try {
            /* Read operations ok. */
            OperationStatus stat = db.get(t1, dk, dv, LockMode.DEFAULT);
            assertEquals(OperationStatus.SUCCESS, stat);
        } catch (Exception e) {
            fail("Unexpected exception" + e);
        }
        t1.commit();

        /*
         * Iterate over all update operations that must fail, using auto and
         * explicit commit.
         */
        for (TryOp op : new TryOp[] {
                new TryOp(UnsupportedOperationException.class) {
                    @Override
                    void exec(Transaction t)
                        throws DatabaseException {

                        db.put(t, dk, dv);
                    }
                },

                new TryOp(UnsupportedOperationException.class) {
                    @Override
                    void exec(Transaction t)
                        throws DatabaseException {

                        db.delete(t, dk);
                    }
                },

                new TryOp(IllegalArgumentException.class) {
                    @Override
                    void exec(Transaction t)
                        throws DatabaseException {

                        env.openDatabase(t, "db2", dbConfig);
                    }
                },

                new TryOp(UnsupportedOperationException.class) {
                    @Override
                    void exec(Transaction t)
                        throws DatabaseException {

                        env.truncateDatabase(t, "db1", false);
                    }
                },

                new TryOp(UnsupportedOperationException.class) {
                    @Override
                    void exec(Transaction t)
                        throws DatabaseException {

                        env.renameDatabase(t, "db1", "db2");
                    }
                },

                new TryOp(UnsupportedOperationException.class) {
                    @Override
                    void exec(Transaction t)
                        throws DatabaseException {

                        env.removeDatabase(t, "db1");
                    }
                }}) {
            for (final Transaction t : new Transaction[] {
                    env.beginTransaction(null, null), null}) {
                try {
                    op.exec(t);
                    fail("expected exception");
                } catch (RuntimeException e) {
                    if (!op.expectedException.equals(e.getClass())) {
                        e.printStackTrace();
                        fail("unexpected exception." +
                             "Expected: " + op.expectedException +
                             "Threw: " + e.getClass());
                    }
                    if (t != null) {
                        t.abort();
                        continue;
                    }
                }
                if (t != null)  {
                    t.commit();
                }
            }
        }
        db.close();
        env.close();
    }

    /*
     * Ensure that an exception is thrown when we open a replicated env, put
     * some data in it, close the env, and then open a r/o replicated env.
     */
    @Test
    public void testReadOnlyRepEnvUsingEnvHandleSR17643()
        throws DatabaseException {

        final EnvironmentConfig envConfig = getEnvConfig();
        final DatabaseConfig dbConfig = getDbConfig();
        final DatabaseEntry dk = new DatabaseEntry(new byte[10]);
        final DatabaseEntry dv = new DatabaseEntry(new byte[10]);

        {
            final ReplicationConfig repEnvConfig = getRepEnvConfig();
            final ReplicatedEnvironment repEnv1 =
                new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

            final Database db = repEnv1.openDatabase(null, "db1", dbConfig);

            OperationStatus stat = db.put(null, dk, dv);
            assertEquals(OperationStatus.SUCCESS, stat);
            db.close();
            repEnv1.close();
        }

        try {
            final ReplicationConfig repEnvConfig = getRepEnvConfig();
            envConfig.setReadOnly(true);
            final ReplicatedEnvironment repEnv1 =
                new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
            repEnv1.close();
            fail("expected an exception");
        } catch (IllegalArgumentException IAE) {
            /* Expected. */
        }
    }

    /**
     * Check basic progress listener functionality.
     */
    @Test
    public void testRecoveryProgressListener()
        throws DatabaseException {

        final EnvironmentConfig envConfig = getEnvConfig();
        final DatabaseConfig dbConfig = getDbConfig();
        final DatabaseEntry dk = new DatabaseEntry(new byte[10]);
        final DatabaseEntry dv = new DatabaseEntry(new byte[10]);

        /* Check the phases seen by the recovery of a new environment. */
        TestProgress newEnvListener =
            new TestProgress(true,
                             RecoveryProgress.POPULATE_UTILIZATION_PROFILE,
                             RecoveryProgress.POPULATE_EXPIRATION_PROFILE,
                             RecoveryProgress.REMOVE_TEMP_DBS,
                             RecoveryProgress.CKPT,
                             RecoveryProgress.RECOVERY_FINISHED,
                             RecoveryProgress.BECOME_CONSISTENT);

        ReplicationConfig repEnvConfig = getRepEnvConfig();
        envConfig.setRecoveryProgressListener(newEnvListener);
        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);

        final Database db = repEnv.openDatabase(null, "db1", dbConfig);
        OperationStatus stat = db.put(null, dk, dv);
        assertEquals(OperationStatus.SUCCESS, stat);
        db.close();
        repEnv.close();
        newEnvListener.verify();

        /* Check the phases seen by the recovery of an existing environment. */
        TestProgress secondOpenListener =
            new TestProgress(true,
                             RecoveryProgress.FIND_END_OF_LOG,
                             RecoveryProgress.FIND_LAST_CKPT,
                             RecoveryProgress.READ_DBMAP_INFO,
                             RecoveryProgress.REDO_DBMAP_INFO,
                             RecoveryProgress.UNDO_DBMAP_RECORDS,
                             RecoveryProgress.REDO_DBMAP_RECORDS,
                             RecoveryProgress.READ_DATA_INFO,
                             RecoveryProgress.REDO_DATA_INFO,
                             RecoveryProgress.UNDO_DATA_RECORDS,
                             RecoveryProgress.REDO_DATA_RECORDS,
                             RecoveryProgress.POPULATE_UTILIZATION_PROFILE,
                             RecoveryProgress.POPULATE_EXPIRATION_PROFILE,
                             RecoveryProgress.REMOVE_TEMP_DBS,
                             RecoveryProgress.RECOVERY_FINISHED,
                             RecoveryProgress.BECOME_CONSISTENT);

        envConfig.setRecoveryProgressListener(secondOpenListener);
        repEnv = new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
        repEnv.close();

        /*
         * This listener returns false from progress() and should stop the
         * recovery.
         */
        TestProgress shutdownListener =
            new TestProgress(false, RecoveryProgress.FIND_END_OF_LOG,
                             RecoveryProgress.RECOVERY_FINISHED);
        envConfig.setRecoveryProgressListener(shutdownListener);
        try {
            repEnv = new ReplicatedEnvironment(envRoot, repEnvConfig,
                                               envConfig);
            fail("Should have failed due to false return from progress()");
        } catch (EnvironmentFailureException expected) {
            assertEquals(EnvironmentFailureReason.PROGRESS_LISTENER_HALT,
                         expected.getReason());
        }
    }

    /**
     * A test listener that checks that the phases come in the expected sequence
     */
    private class TestProgress implements ProgressListener<RecoveryProgress> {

        private final boolean progressReturnVal;
        private final LinkedList<RecoveryProgress> expected;

        /**
         * Seed the listener with the expected order of phases.
         */
        TestProgress(boolean progressReturnVal,
                     RecoveryProgress... progress) {
            this.progressReturnVal = progressReturnVal;
            expected =
                new LinkedList<RecoveryProgress>(Arrays.asList(progress));
        }

        /**
         * Check that we expect this phase.
         */
        @Override
        public boolean progress(RecoveryProgress phase, long n, long total) {
            System.err.println("phase=" + phase);
            assertEquals(expected.getFirst(), phase);
            expected.removeFirst();
            return progressReturnVal;
        }

        /** All phases should have been seen. */
        public void verify() {
            assertEquals(0, expected.size());
        }
    }

    private DatabaseConfig getDbConfig() {
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        return dbConfig;
    }

    abstract class TryOp {
        Class<?> expectedException;

        TryOp(Class<?> expectedException) {
            this.expectedException = expectedException;
        }

        abstract void exec(Transaction t) throws DatabaseException;
    }

    /*
     * Verify that requirements on the environment config imposed by
     * replication are enforced.
     */
    public void xtestEnvConfigRequirements()
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();

        ReplicationConfig repEnvConfig =
            new ReplicationConfig("ExampleGroup", "node1", "localhost:5000");
        ReplicatedEnvironment repEnv = null;
        try {
            repEnv =
                new ReplicatedEnvironment(envRoot, repEnvConfig, envConfig);
            repEnv.close();
            fail("expected exception saying env is not transactional");
        } catch (IllegalArgumentException e) {
            /* Expected. */
        }
    }

    /**
     * Test setting ENV_UNKNOWN_STATE_TIMEOUT and checking that individual
     * nodes can come up by themselves in the UNKNOWN state.
     */
    @Test
    public void testOpenUnknown()
        throws DatabaseException {

        /* Try a SECONDARY node */
        repEnvInfo[repEnvInfo.length - 1].getRepConfig().setNodeType(
            NodeType.SECONDARY);

        createGroup();
        closeNodes(repEnvInfo);
        for (final RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(
                ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "200 ms");
            info.openEnv();
            assertEquals(ReplicatedEnvironment.State.UNKNOWN,
                         info.getEnv().getState());
            info.closeEnv();
        }
    }

    /**
     * Verifies that a node can join the group, even in the absence of an
     * initial quorum thus preventing the master from updating the rep group
     * db, by retrying until a quorum eventually becomes available.
     */
    @Test
    public void testJoin() throws Exception {
        /* Speed the test along. */
        setRepConfigParam(RepParams.INSUFFICIENT_REPLICAS_TIMEOUT, "1 s");
        /* Set to avoid UnknownMasterException in case test is slow. */
        setRepConfigParam(RepParams.ENV_SETUP_TIMEOUT, "1000 s");
        createGroup(3);

        /* Create insufficient replicas, so node 4 cannot join. */
        repEnvInfo[1].closeEnv();
        repEnvInfo[2].closeEnv();

        /*
         * Node 4 will keep trying to join and will fail until a quorum is
         * established.
         */
        final EnvOpenThread asyncOpen = new EnvOpenThread(repEnvInfo[3]);
        asyncOpen.start();

        asyncOpen.join(5000);

        /* Verify that the open is still being tried. */
        assertTrue(asyncOpen.isAlive());

        /* Restore Quorum. */
        repEnvInfo[1].openEnv();
        repEnvInfo[2].openEnv();

        /* Wait for env open to complete successfully. */
        asyncOpen.join(60000);
        assertTrue(!asyncOpen.isAlive());
        assertTrue(asyncOpen.testException == null);
    }
}
