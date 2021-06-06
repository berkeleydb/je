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

package com.sleepycat.je.rep.utilint;

import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.cleaner.VerifyUtils;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.rep.CommitPointConsistencyPolicy;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.impl.PointConsistencyPolicy;
import com.sleepycat.je.rep.impl.RepGroupDB;
import com.sleepycat.je.rep.impl.RepGroupDB.GroupBinding;
import com.sleepycat.je.rep.impl.RepGroupDB.NodeBinding;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.FeederReader;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup.TestHook;
import com.sleepycat.je.rep.utilint.RepUtils.ConsistencyPolicyFormat;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;

import org.junit.Assert;

/**
 * Static utility methods and instances for replication unit tests.
 *
 * Examples of useful constructs here are methods that:
 * <ul>
 * <li>Create multiple environment directories suitable for unit testing
 * a set of replicated nodes.
 * <li>Create a router config that is initialized with exception and event
 * listeners that will dump asynchronous exceptions to stderr, and which
 * can be conditionalized to ignore exceptions at certain points when the
 * test expects a disconnected node or other error condition.
 * <li>Methods that compare two environments to see if they have a common
 * replication stream.
 * <li>etc ...
 * </ul>
 */
public class RepTestUtils {

    public static final String TEST_HOST = "localhost";
    private static final String REPDIR = "rep";
    public static final String TEST_REP_GROUP_NAME = "UnitTestGroup";
    private static final String[] BUP_SUFFIXES = { FileManager.BUP_SUFFIX };

    /*
     * If -DoverridePort=<val> is set, then replication groups will be
     * set up with this default port value.
     */
    public static final String OVERRIDE_PORT = "overridePort";

    /*
     * If -DlongTimeout is true, then this test will run with very long
     * timeouts, to make interactive debugging easier.
     */
    private static final boolean longTimeout =
        Boolean.getBoolean("longTimeout");

    public static final int MINUTE_MS = 60 * 1000;

    /* Time to wait for each node to start up and join the group. */
    private static final long JOIN_WAIT_TIME = 20000;

    /* The basis for varying log file size */
    private static int envCount = 1;

    /* Convenient constants */

    public final static Durability SYNC_SYNC_ALL_DURABILITY =
        new Durability(Durability.SyncPolicy.SYNC,
                       Durability.SyncPolicy.SYNC,
                       Durability.ReplicaAckPolicy.ALL);

    public final static Durability SYNC_SYNC_NONE_DURABILITY =
        new Durability(Durability.SyncPolicy.SYNC,
                       Durability.SyncPolicy.SYNC,
                       Durability.ReplicaAckPolicy.NONE);

    public final static Durability WNSYNC_NONE_DURABILITY =
        new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                       Durability.SyncPolicy.WRITE_NO_SYNC,
                       Durability.ReplicaAckPolicy.NONE);

    public static final Durability DEFAULT_DURABILITY =
        new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                       Durability.SyncPolicy.WRITE_NO_SYNC,
                       Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);

    public final static TransactionConfig SYNC_SYNC_ALL_TC =
        new TransactionConfig().setDurability(SYNC_SYNC_ALL_DURABILITY);

    public final static TransactionConfig SYNC_SYNC_NONE_TC =
        new TransactionConfig().setDurability(SYNC_SYNC_NONE_DURABILITY);

    public final static TransactionConfig WNSYNC_NONE_TC =
        new TransactionConfig().setDurability(WNSYNC_NONE_DURABILITY);

    public final static TransactionConfig DEFAULT_TC =
        new TransactionConfig().setDurability(DEFAULT_DURABILITY);

    public static final TransactionConfig NO_CONSISTENCY_TC =
        new TransactionConfig()
        .setConsistencyPolicy(NO_CONSISTENCY)
        .setDurability(SYNC_SYNC_NONE_DURABILITY);

    public static File[] getRepEnvDirs(File envRoot, int nNodes) {
        File envDirs[] = new File[nNodes];
        for (int i = 0; i < nNodes; i++) {
            envDirs[i] = new File(envRoot, RepTestUtils.REPDIR + i);
        }
        return envDirs;
    }

    /**
     * Create nNode directories within the envRoot directory nodes, for housing
     * a set of replicated environments. Each directory will be named
     * <envRoot>/rep#, i.e <envRoot>/rep1, <envRoot>/rep2, etc.
     */
    public static File[] makeRepEnvDirs(File envRoot, int nNodes)
        throws IOException {

        File[] envHomes = new File[nNodes];
        for (int i = 0; i < nNodes; i++) {
            envHomes[i] = makeRepEnvDir(envRoot, i);
        }
        return envHomes;
    }

    /**
     * Create a directory within the envRoot directory nodes for housing a
     * single replicated environment.  The directory will be named
     * <envRoot>/rep<i>
     */
    public static File makeRepEnvDir(File envRoot, int i)
        throws IOException {

        File jeProperties = new File(envRoot, "je.properties");
        File envHome = new File(envRoot, REPDIR + i);
        envHome.mkdir();

        /* Copy the test je.properties into the new directories. */
        File repProperties = new File(envHome, "je.properties");
        FileInputStream from = null;
        FileOutputStream to = null;
        try {
            try {
                from = new FileInputStream(jeProperties);
            } catch (FileNotFoundException e) {
                jeProperties.createNewFile();

                from = new FileInputStream(jeProperties);
            }
            to = new FileOutputStream(repProperties);
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = from.read(buffer)) != -1) {
                to.write(buffer, 0, bytesRead);
            }
        } finally {
            if (from != null) {
                try {
                    from.close();
                } catch (IOException ignore) {
                }
            }
            if (to != null) {
                try {
                    to.close();
                } catch (IOException ignore) {
                }
            }
        }

        return envHome;
    }

    /* Create the sub directories for replicated Environments. */
    public static void createRepSubDirs(RepEnvInfo[] repEnvInfo,
                                        int subDirNumber) {
        for (RepEnvInfo envInfo : repEnvInfo) {
            if (envInfo != null) {
                TestUtils.createEnvHomeWithSubDir
                    (envInfo.getEnvHome(), subDirNumber);
            }
        }
    }

    /* Remove the sub directories inside the replicated Environment home. */
    public static void removeRepSubDirs(RepEnvInfo[] repEnvInfo) {
        for (RepEnvInfo envInfo : repEnvInfo) {
            if (envInfo != null) {
                TestUtils.removeSubDirs(envInfo.getEnvHome());
            }
        }
    }

    /** Convenience method to {@link #removeRepEnv} multiple nodes. */
    public static void removeRepDirs(RepEnvInfo... repEnvInfo) {
        for (RepEnvInfo envInfo : repEnvInfo) {
            if (envInfo != null) {
                removeRepEnv(envInfo.getEnvHome());
            }
        }
    }

    /**
     * Remove all the log files in the <envRoot>/rep* directories directory.
     */
    public static void removeRepEnvironments(File envRoot) {
        File[] repEnvs = envRoot.listFiles();
        for (File repEnv : repEnvs) {
            if (repEnv.isDirectory()) {
                removeRepEnv(repEnv);
            }
        }
        removeRepEnv(envRoot);
    }

    /** Removes log/lck/bkp files from a single env home directory. */
    public static void removeRepEnv(File envHome) {
        TestUtils.removeLogFiles("removeRepEnv", envHome, false);
        new File(envHome, "je.lck").delete();
        removeBackupFiles(envHome);
    }

    private static void removeBackupFiles(File repEnv) {
        for (String fileName :
             FileManager.listFiles(repEnv, BUP_SUFFIXES, false)) {
            new File(repEnv, fileName).delete();
        }
    }

    /**
     * Create an array of environments, with basically the same environment
     * configuration.
     */

    public static RepEnvInfo[] setupEnvInfos(File envRoot, int nNodes)
        throws IOException {

        return setupEnvInfos(envRoot, nNodes, DEFAULT_DURABILITY);
    }

    /**
     * Fill in an array of environments, with basically the same environment
     * configuration. Only fill in the array slots which are null. Used to
     * initialize semi-populated set of environments.
     * @throws IOException
     */
    public static RepEnvInfo[] setupEnvInfos(File envRoot,
                                             int nNodes,
                                             Durability envDurability)
        throws IOException {

        File[] envHomes = makeRepEnvDirs(envRoot, nNodes);
        RepEnvInfo[] repEnvInfo = new RepEnvInfo[envHomes.length];

        for (int i = 0; i < repEnvInfo.length; i++) {
            repEnvInfo[i] = setupEnvInfo(envHomes[i],
                                         envDurability,
                                         (short) (i + 1), // nodeId
                                         repEnvInfo[0]);
        }
        return repEnvInfo;
    }

    public static RepEnvInfo[] setupEnvInfos(File envRoot,
                                             int nNodes,
                                             EnvironmentConfig envConfig)
        throws IOException {

        return setupEnvInfos(envRoot, nNodes, envConfig, null);
    }

    public static RepEnvInfo[] setupEnvInfos(File envRoot,
                                             int nNodes,
                                             EnvironmentConfig envConfig,
                                             ReplicationConfig repConfig)
        throws IOException {

        File[] envdirs = makeRepEnvDirs(envRoot, nNodes);
        RepEnvInfo[] repEnvInfo = new RepEnvInfo[envdirs.length];

        for (int i = 0; i < repEnvInfo.length; i++) {
            repEnvInfo[i] = setupEnvInfo(envdirs[i],
                                         envConfig.clone(),
                                         createRepConfig(repConfig, i + 1),
                                         repEnvInfo[0]);
        }
        return repEnvInfo;
    }

    /**
     * Adds an additional replicated environment to the specified list and
     * returns the extended list.  Uses the ReplicationConfig from the first
     * initial node as the basis for the new node.
     */
    public static RepEnvInfo[] setupExtendEnvInfo(
        final RepEnvInfo[] initialEnvInfo,
        final int nNodes)
        throws IOException {

        final int initialNodesCount = initialEnvInfo.length;
        final RepEnvInfo[] result =
            Arrays.copyOf(initialEnvInfo, initialNodesCount + nNodes);
        final File envRoot = initialEnvInfo[0].getEnvHome().getParentFile();
        final ReplicationConfig baseRepConfig =
            initialEnvInfo[0].getRepConfig();
        for (int i = 0; i < nNodes; i++) {
            final int pos = initialNodesCount + i;
            result[pos] = setupEnvInfo(makeRepEnvDir(envRoot, pos),
                                       createEnvConfig(DEFAULT_DURABILITY),
                                       createRepConfig(baseRepConfig, pos + 1),
                                       initialEnvInfo[0]);
        }
        return result;
    }

    /**
     * Create info for a single replicated environment.
     */
    public static RepEnvInfo setupEnvInfo(File envHome,
                                          Durability envDurability,
                                          int nodeId,
                                          RepEnvInfo helper) {

        EnvironmentConfig envConfig = createEnvConfig(envDurability);
        return setupEnvInfo(envHome, envConfig, nodeId, helper);
    }

    /**
     * Create info for a single replicated environment.
     */
    public static RepEnvInfo setupEnvInfo(File envHome,
                                          EnvironmentConfig envConfig,
                                          int nodeId,
                                          RepEnvInfo helper) {
        return setupEnvInfo(envHome,
                            envConfig,
                            createRepConfig(nodeId),
                            helper);
    }

    /**
     * Create info for a single replicated environment.
     */
    public static RepEnvInfo setupEnvInfo(File envHome,
                                          EnvironmentConfig envConfig,
                                          ReplicationConfig repConfig,
                                          RepEnvInfo helper) {

        /*
         * Give all the environments the same environment configuration.
         *
         * If the file size is not set by the calling test, stagger their log
         * file length to give them slightly different logs and VLSNs. Disable
         * parameter validation because we want to make the log file length
         * smaller than the minimums, for testing.
         */
        if (!envConfig.isConfigParamSet(EnvironmentConfig.LOG_FILE_MAX)) {
            DbInternal.disableParameterValidation(envConfig);
            /*  Vary the file size */
            long fileLen = ((envCount++ % 100) + 1) * 10000;
            envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                     Long.toString(fileLen));
        }

        repConfig.setHelperHosts((helper == null) ?
                                 repConfig.getNodeHostPort() :
                                 helper.getRepConfig().getNodeHostPort());

        /*
         * If -DlongTimeout is true, then this test will run with very long
         * timeouts, to make interactive debugging easier.
         */
        if (longTimeout) {
            setLongTimeouts(repConfig);
        }

        /*
         * If -DlongAckTimeout is true, then the test will set the
         * REPLICA_TIMEOUT to 50secs.
         */
        if (Boolean.getBoolean("longAckTimeout")) {
            repConfig.setReplicaAckTimeout(50, TimeUnit.SECONDS);
        }
        return new RepEnvInfo(envHome, repConfig, envConfig);
    }

    public static EnvironmentConfig createEnvConfig(Durability envDurability) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(envDurability);

        /*
         * Replicated tests use multiple environments, configure shared cache
         * to reduce the memory consumption.
         */
        envConfig.setSharedCache(true);

        return envConfig;
    }

    /**
     * Create a test RepConfig for the node with the specified id. Note that
     * the helper is not configured.
     */
    public static ReplicationConfig createRepConfig(int nodeId)
        throws NumberFormatException, IllegalArgumentException {

        return createRepConfig(null, nodeId);
    }

    private static int getDefaultPort() {
        return Integer.getInteger
            (OVERRIDE_PORT,
             Integer.parseInt(RepParams.DEFAULT_PORT.getDefault()));
    }

    /**
     * Create a test RepConfig for the node with the specified id, using the
     * specified repConfig. The repConfig may have other parameters set
     * already. Note that the helper is not configured.
     */
    public static
        ReplicationConfig createRepConfig(ReplicationConfig repConfig,
                                          int nodeId)
        throws NumberFormatException, IllegalArgumentException {

        ReplicationConfig filledInConfig =
            repConfig == null ? new ReplicationConfig() : repConfig.clone();

        final int firstPort = getDefaultPort();
        filledInConfig.setConfigParam
            (RepParams.ENV_SETUP_TIMEOUT.getName(), "60 s");
        filledInConfig.setConfigParam
            (ReplicationConfig.ENV_CONSISTENCY_TIMEOUT, "60 s");
        filledInConfig.setGroupName(TEST_REP_GROUP_NAME);
        filledInConfig.setNodeName("Node" + nodeId);
        String nodeHost = TEST_HOST + ":" + (firstPort + (nodeId - 1));
        filledInConfig.setNodeHostPort(nodeHost);

        /* Minimize socket bind exceptions in tests. */
        filledInConfig.setConfigParam(RepParams.SO_REUSEADDR.getName(),
                                      "true");
        filledInConfig.setConfigParam(RepParams.SO_BIND_WAIT_MS.getName(),
                                      "150000");
        return filledInConfig;
    }

    public static ReplicationNetworkConfig readRepNetConfig() {
        /* Call to force class loading and parameter registration */
        ReplicationNetworkConfig.registerParams();
        return ReplicationNetworkConfig.create(readNetProps());
    }

    public static Properties readNetProps() {
        final File propFile =
            new File(SharedTestUtils.getTestDir(), "je.properties");
        final Properties props = new Properties();
        RepUtils.populateNetProps(props, propFile);
        return props;
    }

    /**
     * Set timeouts to long intervals for debugging interactively
     */
    public static void setLongTimeouts(ReplicationConfig repConfig) {

        RepInternal.disableParameterValidation(repConfig);

        /* Wait an hour for this node to join the group.*/
        repConfig.setConfigParam(RepParams.ENV_SETUP_TIMEOUT.getName(),
                                 "1 h");
        repConfig.setConfigParam(ReplicationConfig.ENV_CONSISTENCY_TIMEOUT,
                                 "1 h");

        /* Wait an hour for replica acks. */
        repConfig.setConfigParam(ReplicationConfig.REPLICA_ACK_TIMEOUT,
                                 "1 h");

        /* Have a heartbeat every five minutes. */
        repConfig.setConfigParam(RepParams.HEARTBEAT_INTERVAL.getName(),
                                 "5 min");
    }

    /**
     * Shuts down the environments with a checkpoint at the end.
     *
     * @param repEnvInfo the environments to be shutdown
     */
    public static void shutdownRepEnvs(RepEnvInfo... repEnvInfo) {

        shutdownRepEnvs(repEnvInfo, true);
    }

    /**
     * Shut down the environment, with an optional checkpoint. It sequences the
     * shutdown so that all replicas are shutdown before the master.  This
     * sequencing avoids side-effects in the tests where shutting down the
     * master first results in elections and one of the "to be shutdown"
     * replicas becomes a master and so on.
     * <p>
     * If an exception occurs for any reason while closing one env, other envs
     * may be left open.  TODO: Determine if this behavior is really desired.
     *
     * @param repEnvInfo the environments to be shutdown
     *
     * @param doCheckpoint whether do a checkpoint at the end of close
     */
    public static void shutdownRepEnvs(RepEnvInfo[] repEnvInfo,
                                       boolean doCheckpoint) {

        if (repEnvInfo == null) {
            return;
        }

        RepEnvInfo master = null;
        for (RepEnvInfo ri : repEnvInfo) {
            if ((ri.repEnv == null) || !ri.repEnv.isValid()) {
                ri.repEnv = null;
                continue;
            }
            if (ri.repEnv.getState().isMaster()) {
                if (master != null) {
                    throw new IllegalStateException
                        ("Multiple masters: " + master.getEnv().getNodeName() +
                         " and " + ri.repEnv.getNodeName() +
                         " are both masters.");
                }
                master = ri;
            } else {
                try {
                    if (doCheckpoint) {
                        RepImpl repImpl =
                            RepInternal.getNonNullRepImpl(ri.repEnv);
                        ri.repEnv.close();
                        if (!repImpl.isClosed()) {
                            throw new IllegalStateException
                                ("Environment: " + ri.getEnvHome() +
                                 " not released");
                        }
                    } else {
                        RepInternal.getNonNullRepImpl(ri.repEnv).close(false);
                    }
                } finally {
                    ri.repEnv = null;
                }
            }
        }

        if (master != null) {
            try {
                if (doCheckpoint) {
                    master.getEnv().close();
                } else {
                    RepInternal.getNonNullRepImpl(master.getEnv()).
                        close(false);
                }
            } finally {
                master.repEnv = null;
            }
        }
    }

    /**
     * All the non-closed, non-null environments in the array join the group.
     * @return the replicator who is the master.
     */
    public static ReplicatedEnvironment
        openRepEnvsJoin(RepEnvInfo[] repEnvInfo) {

        return joinGroup(getOpenRepEnvs(repEnvInfo));
    }

    /* Get open replicated environments from an array. */
    public static RepEnvInfo[] getOpenRepEnvs(RepEnvInfo[] repEnvInfo) {
        Set<RepEnvInfo> repSet = new HashSet<RepEnvInfo>();
        for (RepEnvInfo ri : repEnvInfo) {
            if ((ri != null) &&
                (ri.getEnv() != null) &&
                ri.getEnv().isValid()) {
                repSet.add(ri);
            }
        }

        return repSet.toArray(new RepEnvInfo[repSet.size()]);
    }

    /**
     * Environment handles are created using the config information in
     * repEnvInfo. Note that since this method causes handles to be created
     * serially, it cannot be used to restart an existing group from scratch.
     * It can only be used to start a new group, or have nodes join a group
     * that is already active.
     *
     * @return the replicated environment associated with the master.
     */
    public static
        ReplicatedEnvironment joinGroup(RepEnvInfo ... repEnvInfo) {

        int retries = 10;
        final int retryWaitMillis = 5000;
        ReplicatedEnvironment master = null;
        List<RepEnvInfo> joinNotFinished =
            new LinkedList<RepEnvInfo>(Arrays.asList(repEnvInfo));

        while (joinNotFinished.size() != 0) {
            for (RepEnvInfo ri : joinNotFinished) {
                try {
                    ReplicatedEnvironment.State joinState;
                    if (ri.getEnv() != null) {

                        /*
                         * Handle exists, make sure it's not in UNKNOWN state.
                         */
                        RepImpl rimpl =
                            RepInternal.getNonNullRepImpl(ri.getEnv());
                        joinState = rimpl.getState();
                        Assert.assertFalse(
                            "Node " + ri.getEnv().getNodeName() +
                            " was detached",
                            joinState.equals(State.DETACHED));
                    } else {
                        joinState = ri.openEnv().getState();
                    }

                    if (joinState.equals(State.MASTER)) {
                        if (master != null) {
                            if (--retries > 0) {
                                Thread.sleep(retryWaitMillis);

                                /*
                                 * Start over. The master is slow making its
                                 * transition, one of them has not realized
                                 * that they are no longer the master.
                                 */
                                joinNotFinished = new LinkedList<RepEnvInfo>
                                    (Arrays.asList(repEnvInfo));
                                master = null;
                                break;
                            }
                            throw new RuntimeException
                                ("Dual masters: " + ri.getEnv().getNodeName() +
                                 " and " +
                                 master.getNodeName() + " despite retries");
                        }
                        master = ri.getEnv();
                    }
                    joinNotFinished.remove(ri);
                    if ((joinNotFinished.size() == 0) && (master == null)) {
                        if (--retries == 0) {
                            throw new RuntimeException
                            ("No master established despite retries");
                        }
                        Thread.sleep(retryWaitMillis);
                        /* Start over, an election is still in progress. */
                        joinNotFinished = new LinkedList<RepEnvInfo>
                        (Arrays.asList(repEnvInfo));
                    }
                    break;
                } catch (UnknownMasterException retry) {
                    /* Just retry. */
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return master;
    }

    /**
     * Used to ensure that the entire group is in sync, that is, all replicas
     * are consistent with the master's last commit. Note that it requires all
     * the nodes in the replication group to be available.
     *
     * @param repEnvInfo the array holding the environments
     * @param numSyncNodes the expected number of nodes to be synced; includes
     * the master
     * @throws InterruptedException
     */
    public static VLSN syncGroupToLastCommit(RepEnvInfo[] repEnvInfo,
                                             int numSyncNodes)
        throws InterruptedException {

        CommitToken masterCommitToken = null;

        /*
         * Create a transaction just to make sure all the replicas are awake
         * and connected.
         */
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (rep.getState().isMaster()) {
                try {
                    Transaction txn =
                        rep.
                        beginTransaction(null, RepTestUtils.SYNC_SYNC_ALL_TC);
                    txn.commit();
                } catch (InsufficientReplicasException e) {
                    if (e.getAvailableReplicas().size() != (numSyncNodes-1)) {
                        throw new IllegalStateException
                            ("Expected replicas: " + (numSyncNodes - 1) +
                             "available replicas: " +
                             e.getAvailableReplicas());
                    }
                }

                /*
                 * Handshakes with all replicas are now completed, if they were
                 * not before. Now get a token to represent the last committed
                 * point in the replication stream, from the master's
                 * perspective.
                 */
                RepNode repNode =
                    RepInternal.getNonNullRepImpl(rep).getRepNode();
                masterCommitToken = new CommitToken
                    (repNode.getUUID(),
                     repNode.getCurrentTxnEndVLSN().getSequence());
                break;
            }
        }

        if (masterCommitToken == null) {
            throw new IllegalStateException("No current master");
        }

        CommitPointConsistencyPolicy policy =
            new CommitPointConsistencyPolicy(masterCommitToken, MINUTE_MS,
                                             TimeUnit.MILLISECONDS);

        /*
         * Check that the environments are caught up with the last master
         * commit at the time of the call to this method.
         */
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if ((rep == null) ||
                !rep.isValid() ||
                rep.getState().isMaster() ||
                rep.getState().isDetached()) {
                continue;
            }
            policy.ensureConsistency(RepInternal.getNonNullRepImpl(rep));
        }
        return new VLSN(masterCommitToken.getVLSN());
    }

    /**
     * Used to ensure that the group is in sync with respect to a given
     * VLSN. If numSyncNodes == repEnvInfo.length, all the nodes in the
     * replication group must be alive and available. If numSyncNodes is less
     * than the size of the group, a quorum will need to be alive and
     * available.
     *
     * @param repEnvInfo the array holding the environments
     * @param numSyncNodes the expected number of nodes to be synced; includes
     * the master
     * @throws InterruptedException
     */
    public static void syncGroupToVLSN(RepEnvInfo[] repEnvInfo,
                                       int numSyncNodes,
                                       VLSN targetVLSN)
        throws InterruptedException {

        /*
         * Create a transaction just to make sure all the replicas are awake
         * and connected.
         */
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (rep == null) {
                continue;
            }

            if (rep.getState().isMaster()) {
                TransactionConfig txnConfig = null;
                if (numSyncNodes == repEnvInfo.length) {
                    txnConfig = RepTestUtils.SYNC_SYNC_ALL_TC;
                } else {
                    txnConfig = new TransactionConfig();
                    txnConfig.setDurability
                        (new Durability(SyncPolicy.SYNC,
                                        SyncPolicy.SYNC,
                                        ReplicaAckPolicy.SIMPLE_MAJORITY));
                }

                try {
                    Transaction txn = rep.beginTransaction(null, txnConfig);
                    txn.commit();
                } catch (InsufficientReplicasException e) {
                    if (e.getAvailableReplicas().size() !=
                        (numSyncNodes - 1)) {
                        throw new IllegalStateException
                            ("Expected replicas: " + (numSyncNodes - 1) +
                             ", available replicas: " +
                             e.getAvailableReplicas());
                    }
                }
            }
        }

        syncGroup(repEnvInfo, targetVLSN);
    }

    /* Syncs the group to the specific VLSN. */
    private static void syncGroup(RepEnvInfo[] repEnvInfo, VLSN targetVLSN)
        throws InterruptedException {
        PointConsistencyPolicy policy = new PointConsistencyPolicy(targetVLSN);

        /* Check that the environments are caught up with this VLSN. */
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (rep == null ||
                !rep.isValid() ||
                rep.getState().isMaster()) {
                continue;
            }
            policy.ensureConsistency(RepInternal.getNonNullRepImpl(rep));
        }
    }

    /**
     * Synchronizes the group to the current vlsn on the master. Used to ensure
     * that application level changes, even mid-transaction changes have been
     * replicated to all the nodes before the method returns.
     *
     * Note that since CBVLSN updates are asynchronous the vlsn may continue
     * moving forward, but the application level changes will have been
     * propagated.
     */
    public static VLSN syncGroup(RepEnvInfo[] repEnvInfo) {
        RepEnvInfo master = RepTestBase.findMaster(repEnvInfo);
        if (master == null) {
            throw new IllegalStateException("no master");
        }
        VLSN vlsn = master.getRepImpl().getVLSNIndex().getRange().getLast();
        try {
            syncGroup(repEnvInfo, vlsn);
        } catch (Exception e) {
            throw new IllegalStateException("unexpected exception");
        }
        return vlsn;
    }

    public static void checkUtilizationProfile(RepEnvInfo ... repEnvInfo) {
        checkUtilizationProfile(null, repEnvInfo);
    }

    /**
     * Run utilization profile checking on all databases in the set of
     * RepEnvInfo. The environment must be quiescent. The utility will lock
     * out any cleaning by using DbBackup, during the check.
     */
    public static void checkUtilizationProfile(PrintStream out,
                                               RepEnvInfo ... repEnvInfo) {
        for (RepEnvInfo info : repEnvInfo) {
            if (out != null) {
                out.println("checking " + info.getEnvHome());
            }

            Environment env = info.getEnv();

            /* Use DbBackup to prevent log file deletion. */
            DbBackup backup = new DbBackup(env);
            backup.startBackup();

            try {
                List<String> dbNames = env.getDatabaseNames();

                for (String dbName : dbNames) {
                    if (out != null) {
                        out.println("\tchecking " + dbName);
                    }
                    DatabaseConfig dbConfig = new DatabaseConfig();
                    DbInternal.setUseExistingConfig(dbConfig, true);
                    dbConfig.setTransactional(true);
                    Database db = env.openDatabase(null, dbName, dbConfig);

                    try {
                        VerifyUtils.checkLsns(db);
                    } finally {
                        db.close();
                    }
                }
            } finally {
                backup.endBackup();
            }
        }
    }

    /**
     * Confirm that all the nodes in this group match. Check number of
     * databases, names of databases, per-database count, per-database
     * records. Use the master node as the reference if it exists, else use the
     * first replicator.
     *
     * @param limit The replication stream portion of the equality check is
     * bounded at the upper end by this value. Limit is usually the commit sync
     * or vlsn sync point explicitly called by a test before calling
     * checkNodeEquality.  Each node must contain VLSNs up to and including the
     * limit, and may also include additional VSLNs due to heartbeats, etc.
     *
     * @throws InterruptedException
     *
     * @throws RuntimeException if there is an incompatibility
     */
    public static void checkNodeEquality(VLSN limit,
                                         boolean verbose,
                                         RepEnvInfo ... repEnvInfo)
        throws InterruptedException {

        int referenceIndex = -1;
        assert repEnvInfo.length > 0;
        for (int i = 0; i < repEnvInfo.length; i++) {
            if ((repEnvInfo[i] == null) ||
                (repEnvInfo[i].getEnv() == null)) {
                continue;
            }
            ReplicatedEnvironment repEnv = repEnvInfo[i].getEnv();
            if (repEnv.isValid() && repEnv.getState().isMaster()) {
                referenceIndex = i;
                break;
            }
        }
        assert referenceIndex != -1;

        ReplicatedEnvironment reference = repEnvInfo[referenceIndex].getEnv();
        for (int i = 0; i < repEnvInfo.length; i++) {
            if (i != referenceIndex) {
                if ((repEnvInfo[i] == null) ||
                    (repEnvInfo[i].getEnv() == null)) {
                    continue;
                }

                ReplicatedEnvironment repEnv = repEnvInfo[i].getEnv();
                if (verbose) {
                    System.out.println("Comparing master node " +
                                       reference.getNodeName() +
                                       " to node " +
                                       repEnv.getNodeName());
                }

                if (repEnv.isValid()) {
                    checkNodeEquality(reference, repEnv, limit, verbose);
                }
            }
        }
    }

    /* Enable or disable the log cleaning on a replica. */
    private static void enableCleanerFileDeletion(ReplicatedEnvironment repEnv,
                                                  boolean enable)
        throws InterruptedException {

        if (repEnv.isValid()) {
            RepImpl repImpl = RepInternal.getNonNullRepImpl(repEnv);
            repImpl.getCleaner().enableFileDeletion(enable);
            Thread.sleep(100);
        }
    }

    /**
     * Confirm that the contents of these two nodes match. Check number of
     * databases, names of databases, per-database count, per-database records.
     *
     * @throws InterruptedException
     * @throws RuntimeException if there is an incompatiblity
     */
    public static void checkNodeEquality(ReplicatedEnvironment replicatorA,
                                         ReplicatedEnvironment replicatorB,
                                         VLSN limit,
                                         boolean verbose)
        throws InterruptedException {

        enableCleanerFileDeletion(replicatorA, false);
        enableCleanerFileDeletion(replicatorB, false);

        String nodeA = replicatorA.getNodeName();
        String nodeB = replicatorB.getNodeName();

        Environment envA = replicatorA;
        Environment envB = replicatorB;

        RepImpl repImplA = RepInternal.getNonNullRepImpl(replicatorA);
        RepImpl repImplB = RepInternal.getNonNullRepImpl(replicatorB);

        try {

            /* Compare the replication related sequences. */
            if (verbose) {
                System.out.println("Comparing sequences");
            }

            /* replicated node id sequence. */

            /*
              long nodeIdA =
              envImplA.getNodeSequence().getLastReplicatedNodeId();
              long nodeIdB =
              envImplB.getNodeSequence().getLastReplicatedNodeId();

              // TEMPORARILY DISABLED: sequences not synced up. This may
              // actually apply right now to database and txn ids too,
              // but it's less likely to manifest itself.
              if (nodeIdA != nodeIdB) {
              throw new RuntimeException
              ("NodeId mismatch. " + nodeA +
              " lastRepNodeId=" + nodeIdA + " " + nodeB +
              " lastRepNodeId=" + nodeIdB);
              }
            */

            /* replicated txn id sequence. */

            /*
              long txnIdA = repImplA.getTxnManager().getLastReplicatedTxnId();
              long txnIdB = repmplB.getTxnManager().getLastReplicatedTxnId();
              if (txnIdA != txnIdB) {
              throw new RuntimeException
              ("TxnId mismatch. A.lastRepTxnId=" + txnIdA +
              " B.lastRepTxnId=" + txnIdB);
              }
            */

            /* Replicated database id sequence. */
            long dbIdA = repImplA.getDbTree().getLastReplicatedDbId();
            long dbIdB = repImplB.getDbTree().getLastReplicatedDbId();
            if (dbIdA != dbIdB) {
                throw new RuntimeException
                    ("DbId mismatch. A.lastRepDbId=" + dbIdA +
                     " B.lastRepDbId=" + dbIdB);
            }

            /* Check name and number of application databases first. */
            List<String> dbListA = envA.getDatabaseNames();
            List<String> dbListB = envB.getDatabaseNames();

            if (verbose) {
                System.out.println("envEquals: check db list: " + nodeA +
                                   "=" + dbListA + " " + nodeB + "=" +
                                   dbListB);
            }

            if (!dbListA.equals(dbListB)) {
                throw new RuntimeException("Mismatch: dbNameList " + nodeA +
                                           " =" + dbListA + " " +
                                           nodeB + " =" + dbListB);
            }

            /* Check record count and contents of each database. */
            DatabaseConfig checkConfig = new DatabaseConfig();
            checkConfig.setReadOnly(true);
            checkConfig.setTransactional(true);
            DbInternal.setUseExistingConfig(checkConfig, true);
            for (String dbName : dbListA) {

                Database dbA = null;
                Database dbB = null;
                try {
                    dbA = envA.openDatabase(null, dbName, checkConfig);
                    dbB = envB.openDatabase(null, dbName, checkConfig);

                    int count = checkDbContents(dbA, dbB);

                    if (verbose) {
                        System.out.println("compared " + count + " records");
                    }
                } finally {
                    if (dbA != null) {
                        dbA.close();
                    }
                    if (dbB != null) {
                        dbB.close();
                    }
                }
            }

            /*
             * Check the replication stream of each environment. The subset of
             * VLSN entries common to both nodes should match.
             */
            checkStreamIntersection(nodeA,
                                    nodeB,
                                    RepInternal.getNonNullRepImpl(replicatorA),
                                    RepInternal.getNonNullRepImpl(replicatorB),
                                    limit,
                                    verbose);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        enableCleanerFileDeletion(replicatorA, true);
        enableCleanerFileDeletion(replicatorB, true);
    }

    /**
     * @throws RuntimeException if dbA and dbB don't have the same contents.
     */
    private static int checkDbContents(Database dbA, Database dbB) {

        Cursor cursorA = null;
        Cursor cursorB = null;
        Transaction txnA = null;
        Transaction txnB = null;
        int debugCount = 0;
        boolean isGroupDB =
            dbA.getDatabaseName().equals(DbType.REP_GROUP.getInternalName());

        try {
            txnA = dbA.getEnvironment().beginTransaction(null, null);
            txnB = dbB.getEnvironment().beginTransaction(null, null);
            cursorA = dbA.openCursor(txnA, CursorConfig.READ_UNCOMMITTED);
            cursorB = dbB.openCursor(txnB, CursorConfig.READ_UNCOMMITTED);
            DatabaseEntry keyA = new DatabaseEntry();
            DatabaseEntry keyB = new DatabaseEntry();
            DatabaseEntry dataA = new DatabaseEntry();
            DatabaseEntry dataB = new DatabaseEntry();
            NodeBinding nodeBinding = null;

            OperationResult resultA;

            while ((resultA = cursorA.get(keyA, dataA, Get.NEXT, null))
                    != null) {
                debugCount++;

                OperationResult resultB = cursorB.get(
                    keyB, dataB, Get.NEXT, null);

                if (resultB == null) {
                    throw new RuntimeException("Mismatch: debugCount=" +
                                               debugCount + "bad resultB");
                }
                if (!Arrays.equals(keyA.getData(), keyB.getData())) {
                    throw new RuntimeException("Mismatch: debugCount=" +
                                               debugCount + " keyA=" +
                                               keyA + " keyB=" +
                                               keyB);

                }
                if (!Arrays.equals(dataA.getData(), dataB.getData())) {
                    if (isGroupDB &&
                        equalsNode(dataA.getData(), dataB.getData(),
                                   nodeBinding)) {
                        continue;
                    }
                    throw new RuntimeException("Mismatch: debugCount=" +
                                               debugCount + " dataA=" +
                                               dataA + " dataB=" +
                                               dataB);
                }

                if (resultA.getExpirationTime() !=
                    resultB.getExpirationTime()) {

                    throw new RuntimeException(
                        "Mismatch: debugCount=" + debugCount +
                        " expireA=" +
                        TTL.formatExpirationTime(resultA.getExpirationTime()) +
                        " expireB=" +
                        TTL.formatExpirationTime(resultB.getExpirationTime()));
                }

                if (isGroupDB &&
                    (nodeBinding == null) &&
                    RepGroupDB.GROUP_KEY.equals(
                        StringBinding.entryToString(keyA))) {
                    final RepGroupImpl group =
                        new GroupBinding().entryToObject(dataA);
                    nodeBinding = new NodeBinding(group.getFormatVersion());
                }
            }
            if (cursorB.get(keyB, dataB, Get.NEXT, null) != null) {
                throw new RuntimeException("Mismatch: debugCount=" +
                                           debugCount + " keyA is missing" +
                                           " keyB=" + keyB +
                                           " dataB=" + dataB);
            }
            return debugCount;
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (cursorA != null) {
                    cursorA.close();
                }
                if (cursorB != null) {
                    cursorB.close();
                }
                if (txnA != null) {
                    txnA.commit();
                }
                if (txnB != null) {
                    txnB.commit();
                }
            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /*
     * Implements a special check for group nodes which skips the syncup field.
     */
    private static boolean equalsNode(byte[] data1, byte[] data2,
                                      NodeBinding nodeBinding) {
        Assert.assertNotNull("Node binding", nodeBinding);
        RepNodeImpl n1 = nodeBinding.entryToObject(new TupleInput(data1));
        RepNodeImpl n2 = nodeBinding.entryToObject(new TupleInput(data2));
        return n1.equivalent(n2);
    }

    /**
     * @throws InterruptedException
     * @throws IOException
     * @throws RuntimeException if envA and envB don't have the same set of
     * VLSN mappings, VLSN-tagged log entries, and replication sequences.
     */
    @SuppressWarnings("unused")
    private static void checkStreamIntersection(String nodeA,
                                                String nodeB,
                                                RepImpl repA,
                                                RepImpl repB,
                                                VLSN limit,
                                                boolean verbose)
        throws IOException, InterruptedException {

        if (verbose) {
            System.out.println("Check intersection for " + nodeA +
                               " and " + nodeB);
        }

        VLSNIndex repAMap = repA.getVLSNIndex();
        VLSNRange repARange = repAMap.getRange();
        VLSNIndex repBMap = repB.getVLSNIndex();
        VLSNRange repBRange = repBMap.getRange();

        /*
         * Compare the vlsn ranges held on each environment and find the subset
         * common to both replicas.
         */
        VLSN firstA = repARange.getFirst();
        VLSN lastA = repARange.getLast();
        VLSN firstB = repBRange.getFirst();
        VLSN lastB = repBRange.getLast();
        VLSN lastSyncA = repARange.getLastSync();

        if (lastA.compareTo(limit) < 0) {
            throw new RuntimeException
                ("CheckRepStream error: repA (" + repA.getNameIdPair() +
                 ") lastVLSN = " + lastA +
                 " < limit = " + limit);
        }

        if (lastB.compareTo(limit) < 0) {
            throw new RuntimeException
                ("CheckRepStream error: repB (" + repB.getNameIdPair() +
                 ") lastVLSN = " + lastB +
                 " < limit = " + limit + ")");
        }

        /*
         * Calculate the largest VLSN range starting point and the smallest
         * VLSN range ending point for these two Replicators.
         */
        VLSN firstLarger = (firstA.compareTo(firstB) > 0) ? firstA : firstB;
        VLSN lastSmaller = (lastA.compareTo(lastB) < 0) ? lastA : lastB;

        try {
            /* The two replicas can read from the larger of the first VLSNs. */
            FeederReader readerA = new FeederReader(repA,
                                                    repAMap,
                                                    DbLsn.NULL_LSN,
                                                    100000);
            readerA.initScan(firstLarger);

            FeederReader readerB = new FeederReader(repB,
                                                    repBMap,
                                                    DbLsn.NULL_LSN,
                                                    100000);
            readerB.initScan(firstLarger);

            /* They should both find the smaller of the last VLSNs. */
            for (long vlsnVal = firstLarger.getSequence();
                 vlsnVal <= lastSmaller.getSequence();
                 vlsnVal++) {

                OutputWireRecord wireRecordA =
                    readerA.scanForwards(new VLSN(vlsnVal), 0);
                OutputWireRecord wireRecordB =
                    readerB.scanForwards(new VLSN(vlsnVal), 0);

                if (!(wireRecordA.match(wireRecordB))) {
                    throw new RuntimeException(nodeA + " at vlsn " + vlsnVal +
                                               " has " + wireRecordA + " " +
                                               nodeB  + " has " + wireRecordB);
                }

                /* Check that db id, node id, txn id are negative. */
                if (!repA.isRepConverted()) {
                    wireRecordA.verifyNegativeSequences(nodeA);
                }
                if (!repB.isRepConverted()) {
                    wireRecordB.verifyNegativeSequences(nodeB);
                }
            }

            if (verbose) {
                System.out.println("Checked from vlsn " + firstLarger +
                                   " to " + lastSmaller);
            }
        } catch (Exception e) {
            e.printStackTrace();

            System.err.println(nodeA + " vlsnMap=");
            repAMap.dumpDb(true);
            System.err.println(nodeB + " vlsnMap=");
            repBMap.dumpDb(true);

            throw new RuntimeException(e);
        }
    }

    /**
     * Return the number of nodes that constitute a quorum for this size
     * group. This should be replaced by ReplicaAckPolicy.requiredNodes;
     */
    public static int getQuorumSize(int groupSize) {
        assert groupSize > 0 : "groupSize = " + groupSize;
        if (groupSize == 1) {
            return 1;
        } else if (groupSize == 2) {
            return 1;
        } else {
            return (groupSize/2) + 1;
        }
    }

    /**
     * Create a rep group of a specified size on the local host, using the
     * default port configuration.
     *
     * @param electableNodes number of electable nodes in test group
     * @param monitorNodes number of monitor nodes in test group
     *
     * @return the simulated test RepGroup
     *
     * @throws UnknownHostException
     */
    public static RepGroupImpl createTestRepGroup(int electableNodes,
                                                  int monitorNodes)
        throws UnknownHostException {

        return createTestRepGroup(electableNodes, monitorNodes, 0);
    }

    /**
     * Create a rep group of a specified size on the local host, using the
     * default port configuration.
     *
     * @param electableNodes number of electable nodes in test group
     * @param monitorNodes number of monitor nodes in test group
     * @param secondaryNodes number of secondary nodes in the test group
     *
     * @return the simulated test RepGroup
     *
     * @throws UnknownHostException
     */
    public static RepGroupImpl createTestRepGroup(int electableNodes,
                                                  int monitorNodes,
                                                  int secondaryNodes)
        throws UnknownHostException {

        Map<Integer, RepNodeImpl> allNodeInfo =
            new HashMap<Integer, RepNodeImpl>();
        final InetAddress ia = InetAddress.getLocalHost();
        int port = getDefaultPort();
        RepGroupImpl repGroup = new RepGroupImpl("TestGroup", null);

        for (int i = 1; i <= electableNodes; i++) {
            allNodeInfo.put(i, new RepNodeImpl(new NameIdPair("node" + i,i),
                                               NodeType.ELECTABLE,
                                               true,
                                               false,
                                               ia.getHostName(),
                                               port,
                                               repGroup.getChangeVersion(),
                                               null));
            port++;
        }
        for (int i = (electableNodes + 1);
             i <= (electableNodes + monitorNodes);
             i++) {
            allNodeInfo.put(i, new RepNodeImpl(new NameIdPair("mon" + i,i),
                                               NodeType.MONITOR,
                                               true,
                                               false,
                                               ia.getHostName(),
                                               port,
                                               repGroup.getChangeVersion(),
                                               null));
            port++;
        }
        for (int i = electableNodes + monitorNodes + 1;
             i <= electableNodes + monitorNodes + secondaryNodes;
             i++) {
            allNodeInfo.put(i, new RepNodeImpl(new NameIdPair("sec" + i, i),
                                               NodeType.SECONDARY,
                                               true,
                                               false,
                                               ia.getHostName(),
                                               port,
                                               repGroup.getChangeVersion(),
                                               null));
            port++;
        }
        repGroup.setNodes(allNodeInfo);
        return repGroup;
    }

    public static class RepEnvInfo {
        private final File envHome;
        private final ReplicationConfig repConfig;
        private EnvironmentConfig envConfig;
        private QuorumPolicy initialElectionPolicy =
            QuorumPolicy.SIMPLE_MAJORITY;

        private ReplicatedEnvironment repEnv = null;

        public RepEnvInfo(File envHome,
                          ReplicationConfig repConfig,
                          EnvironmentConfig envConfig) {
            super();
            this.envHome = envHome;
            this.repConfig = repConfig;
            this.envConfig = envConfig;
        }

        public ReplicatedEnvironment openEnv() {
            if (repEnv != null) {
                throw new IllegalStateException("rep env already exists");
            }

            repEnv = new ReplicatedEnvironment(envHome,
                                               getRepConfig(),
                                               envConfig,
                                               null,
                                               initialElectionPolicy);
            return repEnv;
        }

        public ReplicatedEnvironment openEnv(ReplicaConsistencyPolicy cp) {

            if (repEnv != null) {
                throw new IllegalStateException("rep env already exists");
            }
            repEnv = new ReplicatedEnvironment
                (envHome, getRepConfig(), envConfig, cp,
                 initialElectionPolicy);
            return repEnv;
        }

        public ReplicatedEnvironment openEnv(RepEnvInfo helper) {

            repConfig.setHelperHosts((helper == null) ?
                                     repConfig.getNodeHostPort() :
                                     helper.getRepConfig().getNodeHostPort());
            return openEnv();
        }

        public ReplicatedEnvironment getEnv() {
            return repEnv;
        }

        public RepImpl getRepImpl() {
            return RepInternal.getNonNullRepImpl(repEnv);
        }

        public RepNode getRepNode() {
            return getRepImpl().getRepNode();
        }

        public ReplicationConfig getRepConfig() {
            return repConfig;
        }

        public File getEnvHome() {
            return envHome;
        }

        public void setEnvConfig(final EnvironmentConfig envConfig) {
            this.envConfig = envConfig;
        }

        public EnvironmentConfig getEnvConfig() {
            return envConfig;
        }

        public QuorumPolicy getInitialElectionPolicy() {
            return initialElectionPolicy;
        }

        public void setInitialElectionPolicy(
            final QuorumPolicy initialElectionPolicy) {
            this.initialElectionPolicy = initialElectionPolicy;
        }

        public void closeEnv() {
            try {
                if (repEnv != null) {
                   repEnv.close();
                }
            } finally {
                repEnv = null;
            }
        }

        /**
         * Convenience method that guards against a NPE when checking whether
         * the state of a node is MASTER.
         */
        public boolean isMaster() {
            return (repEnv != null) && repEnv.getState().isMaster();
        }

        /**
         * Convenience method that guards against a NPE when checking whether
         * the state of a node is REPLICA.
         */
        public boolean isReplica() {
            return (repEnv != null) && repEnv.getState().isReplica();
        }

        /**
         * Convenience method that guards against a NPE when checking whether
         * the state of a node is UNKNOWN.
         */
        public boolean isUnknown() {
            return (repEnv != null) && repEnv.getState().isUnknown();
        }

        /**
         * Simulate a crash of the environment, don't do a graceful close.
         */
        public void abnormalCloseEnv() {
            try {
                if (repEnv.isValid()) {

                    /*
                     * Although we want an abnormal close, we do want to flush.
                     * And if the env is valid, we expect it to work; so avoid
                     * ignoring exceptions from this call.
                     */
                    RepInternal.getNonNullRepImpl(repEnv).getLogManager().
                        flushNoSync();
                }
                try {
                    RepInternal.getNonNullRepImpl(repEnv).abnormalClose();
                } catch (DatabaseException ignore) {

                    /*
                     * The close will face problems like unclosed txns, ignore.
                     * We're trying to simulate a crash.
                     */
                }
            } finally {
                repEnv = null;
            }
        }

        @Override
        public String toString() {
            return (repEnv == null) ?
                    envHome.toString() : repEnv.getNodeName();
        }
    }

    public static String stackTraceString(final Throwable exception) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(bao);
        exception.printStackTrace(printStream);
        String stackTraceString = bao.toString();
        return stackTraceString;
    }

    /**
     * Restarts a group associated with an existing environment on disk.
     * Returns the environment associated with the master.
     */
    public static ReplicatedEnvironment
        restartGroup(RepEnvInfo ... repEnvInfo) {

        return restartGroup(false /*replicasOnly*/, false, repEnvInfo);
    }

    public static ReplicatedEnvironment
        restartGroup(boolean allowILE, RepEnvInfo ... repEnvInfo) {

        return restartGroup(false, allowILE, repEnvInfo);
    }

    /**
     * Restarts a group of replicas associated with an existing environment on
     * disk.
     */
    public static void restartReplicas(RepEnvInfo ... repEnvInfo) {

        restartGroup(true /*replicasOnly*/, false, repEnvInfo);
    }

    /**
     * Restarts a group associated with an existing environment on disk.
     * Returns the environment associated with the master.
     */
    private static ReplicatedEnvironment
        restartGroup(boolean replicasOnly,
                     boolean allowILE,
                     RepEnvInfo ... repEnvInfo) {

        /* To avoid the jdk bug: NullPointerException in Selector.open(). The
         * bug report can be found in
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        System.setProperty("sun.nio.ch.bugLevel",
                           System.getProperty("sun.nio.ch.bugLevel",""));

        /* Restart the group, a thread for each node. */
        JoinThread threads[] = new JoinThread[repEnvInfo.length];
        for (int i = 0; i < repEnvInfo.length; i++) {
            threads[i] = new JoinThread(repEnvInfo[i], allowILE);
            threads[i].start();
        }

        /*
         * Wait for each thread to have joined the group. The group must be
         * re-started in parallel to ensure that all nodes are up and elections
         * can be held.
         */
        RuntimeException firstFailure = null;
        for (int i = 0; i < repEnvInfo.length; i++) {
            JoinThread jt = threads[i];
            try {
                jt.join(JOIN_WAIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            final Throwable exception = jt.testException;
            RuntimeException failure = null;
            if (exception != null) {
                failure = new RuntimeException(
                    "Join thread exception for " + repEnvInfo[i] +
                    " still alive = " + jt.isAlive() + "\n" +
                    RepTestUtils.stackTraceString(exception));
            } else if (jt.isAlive()) {
                failure = new IllegalStateException(
                    "Join thread for " + repEnvInfo[i] +
                    " still alive after " + JOIN_WAIT_TIME + "ms," +
                    " and testException is null.");
            }
            if (failure != null) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    System.err.println(failure);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }

        /* All join threads are quiescent, now pick the master. */
        if (replicasOnly) {
            return null;
        }

        return getMaster(repEnvInfo, false /*openIfNeeded*/);
    }

    /**
     * Find the authoritative master (wait for election to quiesce).
     */
    public static ReplicatedEnvironment getMaster(RepEnvInfo[] repEnvInfo,
                                                  boolean openIfNeeded) {

        final int maxRetries = 100;
        int retries = maxRetries;
        while (true) {
            int masterId = -1;
            boolean multipleMasters = false;
            boolean nonAuthoritativeMaster = false;
            for (int i = 0; i < repEnvInfo.length; i++) {
                if (openIfNeeded && repEnvInfo[i].getEnv() == null) {
                    final boolean VERBOSE = false;
                    if (VERBOSE) {
                        System.out.println("Opening node " + (i + 1));
                    }
                    try {
                        repEnvInfo[i].openEnv();
                    } catch (RollbackException|UnknownMasterException e) {
                        /*
                         * This node was unable to join because it could not
                         * determine the master, or because a hard rollback is
                         * needed. If this prevents an authoritative master
                         * from being determined, we will retry below.
                         */
                        continue;
                    }
                }
                if (repEnvInfo[i].getEnv().getState().isMaster()) {
                    if (!repEnvInfo[i].getRepImpl().getRepNode().
                        isAuthoritativeMaster()) {
                        nonAuthoritativeMaster = true;
                    }
                    if (masterId >= 0) {
                        multipleMasters = true;
                    } else {
                        masterId = i;
                    }
                }
            }
            if (masterId >= 0 &&
                !multipleMasters &&
                !nonAuthoritativeMaster) {
                return repEnvInfo[masterId].getEnv();
            }
            if (--retries >= 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            if (nonAuthoritativeMaster) {
                throw new IllegalStateException(
                    "Non-authoritative master after " +
                    maxRetries + " retries.");
            }
            if (multipleMasters) {
                throw new IllegalStateException(
                    "More than one master in group after " +
                    maxRetries + " retries.");
            }
            if (masterId < 0) {
                throw new IllegalStateException
                    ("Node id of the elected master is invalid.");
            }
        }
    }

    /**
     * Threads used to simulate a parallel join group when multiple replication
     * nodes are first brought up for an existing environment.
     */
    private static class JoinThread extends Thread {

        final RepEnvInfo repEnvInfo;
        final boolean allowILE;

        /*
         * Captures any exception encountered in the process of joining.  The
         * presence of a non-null testException field indicates to the caller
         * that the join failed.
         */
        volatile Throwable testException = null;
        private static final int NUM_RETRIES = 100;

        /* The state of the node at the time of joining the group. */
        @SuppressWarnings("unused")
        ReplicatedEnvironment.State state =
            ReplicatedEnvironment.State.UNKNOWN;

        JoinThread(RepEnvInfo repEnvInfo, boolean allowILE) {
            this.repEnvInfo = repEnvInfo;
            this.allowILE = allowILE;
        }

        @Override
        public void run() {

            /*
             * The open of this environment may fail due to timing mishaps if
             * the environment has just been shutdown, as can happen in a
             * number of tests that repeatedly open and close
             * environments. Retry a few time to give the node a chance to
             * settle down.
             */
            int numRetries = 0;
            while (numRetries < NUM_RETRIES) {
                try {
                    state = repEnvInfo.openEnv().getState();
                    testException = null;
                    break;
                } catch (InsufficientLogException ile) {
                    if (allowILE) {
                        NetworkRestore restore = new NetworkRestore();
                        NetworkRestoreConfig nrc = new NetworkRestoreConfig();
                        nrc.setRetainLogFiles(false);
                        restore.execute(ile, nrc);
                        state = repEnvInfo.openEnv().getState();
                        testException = null;
                    } else {
                        testException = ile;
                    }
                    break;
                } catch (GroupShutdownException ge) {
                    /* Retry, this node is still shutting down. */
                    numRetries++;
                    testException = ge;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                    }
                } catch (Throwable e) {
                    testException = e;
                    break;
                }
            }
        }
    }

    /**
     * Issue DbSync on a group. All nodes are presumed to be closed.
     *
     * @param timeoutMs is the DbSync timeout (max time for replica to catch up
     * with master) as well as the join timeout for each thread calling DbSync.
     */
    public static void syncupGroup(long timeoutMs, RepEnvInfo ... repEnvInfo) {

        /*
         * The call to DbSync blocks until the sync is done, so it must
         * be executed concurrently by a set of threads.
         */
        SyncThread threads[] = new SyncThread[repEnvInfo.length];
        String helperHost = repEnvInfo[0].getRepConfig().getNodeHostPort();
        for (int i = 0; i < repEnvInfo.length; i++) {
            threads[i] = new SyncThread(timeoutMs, repEnvInfo[i], helperHost);
            threads[i].start();
        }

        /*
         * Wait for each thread to open, sync, and close the node.
         */
        for (int i = 0; i < repEnvInfo.length; i++) {
            SyncThread t = threads[i];
            try {
                t.join(timeoutMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (t.isAlive()) {
                throw new IllegalStateException("Expect SyncThread " + i +
                                                " dead, but it's alive.");
            }
            final Throwable exception = t.testException;
            if (exception != null) {
                throw new RuntimeException
                    ("Join thread exception.\n" +
                     RepTestUtils.stackTraceString(exception));
            }
        }
    }

    /**
     * Threads used to simulate a parallel join group when multiple replication
     * nodes are first brought up for an existing environment.
     */
    private static class SyncThread extends Thread {

        final RepEnvInfo repEnvInfo;
        final String helperHost;
        final long timeoutMs;

        /* Captures any exception encountered in the process of joining. */
        Throwable testException = null;

        SyncThread(long timeoutMs, RepEnvInfo repEnvInfo, String helperHost) {
            this.timeoutMs = timeoutMs;
            this.repEnvInfo = repEnvInfo;
            this.helperHost = helperHost;
        }

        @Override
        public void run() {
            try {
                ReplicationConfig config = repEnvInfo.getRepConfig();
                DbSync syncAgent =
                    new DbSync(repEnvInfo.getEnvHome().toString(),
                               repEnvInfo.getEnvConfig(),
                               config,
                               helperHost,
                               timeoutMs);
                syncAgent.sync();
            } catch (Throwable e) {
                testException = e;
            }
        }
    }

    /**
     * Disables network listening services, as a way of
     * simulating a network partition for testing.
     */
    public static void disableServices(final RepEnvInfo repEnvInfo) {
        final ServiceDispatcher sd1 =
            repEnvInfo.getRepNode().getServiceDispatcher();
        sd1.setSimulateIOException(Learner.SERVICE_NAME, true);
        sd1.setSimulateIOException(Acceptor.SERVICE_NAME, true);
        sd1.setSimulateIOException(FeederManager.FEEDER_SERVICE, true);
    }

    /**
     * Re-enables network services, to reverse the effect of a simulated
     * network partition.
     * @see #disableServices
     */
    public static void reenableServices(final RepEnvInfo repEnvInfo) {
        final ServiceDispatcher sd1 =
            repEnvInfo.getRepNode().getServiceDispatcher();
        sd1.setSimulateIOException(Learner.SERVICE_NAME, false);
        sd1.setSimulateIOException(Acceptor.SERVICE_NAME, false);
        sd1.setSimulateIOException(FeederManager.FEEDER_SERVICE, false);
    }

    public static void awaitCondition(Callable<Boolean> predicate)
        throws Exception {

        awaitCondition(predicate, 5000);
    }

    public static void awaitCondition(Callable<Boolean> predicate,
                                      long timeout)
        throws Exception {

        boolean done = false;
        long deadline = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < deadline) {
            if (predicate.call()) {
                done = true;
                break;
            }
            Thread.sleep(100);
        }
        Assert.assertTrue(done);
    }

    /**
     * Used for testing to force consistency checks to fail.
     */
    public static class AlwaysFail implements ReplicaConsistencyPolicy {

        public static final String NAME = "AlwaysFailConsistency";

        public AlwaysFail() {
        }

        @Override
        public void ensureConsistency(EnvironmentImpl repInstance)
            throws InterruptedException {

            throw new ReplicaConsistencyException("Always fails for testing",
                                                  this);
        }

        /**
         * Always returns 0, no timeout is needed for this policy.
         */
        @Override
        public long getTimeout(TimeUnit unit) {
            return 1;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * Set the basic SSL properties.  These rely on the build.xml configuration
     * that copies keystore and truststore files to the test environment.
     */
    public static void setUnitTestSSLProperties(Properties props) {
        File destDir = SharedTestUtils.getDestDir();
        String sslPath = new File(destDir.getPath(), "ssl").getPath();

        props.put("je.rep.channelType", "ssl");
        props.put("je.rep.ssl.keyStoreFile",
                  convertPath(new File(sslPath, "keys.store").getPath()));
        props.put("je.rep.ssl.keyStorePassword", "unittest");
        props.put("je.rep.ssl.trustStoreFile",
                  convertPath(new File(sslPath, "trust.store").getPath()));
        props.put("je.rep.ssl.clientKeyAlias", "mykey");
        props.put("je.rep.ssl.serverKeyAlias", "mykey");
    }

    /**
     * Converts path if run on windows.
     * @param path
     * @return
     */
    private static String convertPath(String path) {
        if (File.separator.equals("\\")) {
            return path.replace("\\", "\\\\");
        }
        return path;
    }

    /**
     * Used for testing to force consistency checks to fail.  Register the
     * format at the beginning of the test as follows:
     *
     *  // Register custom consistency policy format while quiescent.
     *  RepUtils.addConsistencyPolicyFormat
     *      (RepTestUtils.AlwaysFail.NAME,
     *       new RepTestUtils.AlwaysFailFormat());
     */
    public static class AlwaysFailFormat
        implements ConsistencyPolicyFormat<AlwaysFail> {

        @Override
        public String policyToString(final AlwaysFail policy) {
            return AlwaysFail.NAME;
        }

        @Override
        public AlwaysFail stringToPolicy(final String string) {
            return new AlwaysFail();
        }
    }

    /**
     * Wait until a replica/feeder syncup has been tried numSyncupAttempt times
     * on this node.
     */
    public static CountDownLatch setupWaitForSyncup
        (final ReplicatedEnvironment node, int numSyncupAttempts) {
        final CountDownLatch waiter = new CountDownLatch(numSyncupAttempts);

        TestHook<Object> syncupFinished = new TestHook<Object>() {
            @Override
            public void doHook() throws InterruptedException {
                waiter.countDown();
            }
        };

        RepInternal.getNonNullRepImpl(node).getRepNode().
            replica().setReplicaFeederSyncupHook(syncupFinished);
        return waiter;
    }

    /**
     * Modify the existing rep configuration with the new parameter value pair.
     */
    public static void setConfigParam(ConfigParam param,
                                      String value,
                                      RepEnvInfo repEnvInfo[]) {

        for (RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(param.getName(), value);
        }
    }
}
