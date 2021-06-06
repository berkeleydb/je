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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.dbi.DbEnvPool;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.monitor.Monitor;
import com.sleepycat.je.rep.monitor.MonitorConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public abstract class RepTestBase extends TestBase {

    protected final Logger logger =
        LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

    /**
     * Used to start up an existing group. Each environment must be opened in
     * its own thread, since the open of the environment does not return until
     * an election has been concluded and a Master is in place.
     */
    protected static class EnvOpenThread extends Thread {
        final RepEnvInfo threadRepInfo;
        public Throwable testException = null;

        public EnvOpenThread(RepEnvInfo info) {
            this.threadRepInfo = info;
        }

        @Override
        public void run() {
            try {
                threadRepInfo.openEnv();
            } catch (Throwable e) {
                testException = e;
            }
        }
    }

    /**
     * Listener used to determine when a Master becomes available, by tripping
     * the count down latch.
     */
    protected static class MasterListener implements StateChangeListener{
        final CountDownLatch masterLatch;

        public MasterListener(CountDownLatch masterLatch) {
            super();
            this.masterLatch = masterLatch;
        }

        @Override
        public void stateChange(StateChangeEvent stateChangeEvent)
            throws RuntimeException {

            if (stateChangeEvent.getState().isMaster()) {
                masterLatch.countDown();
            }
        }
    }

    protected final File envRoot = SharedTestUtils.getTestDir();
    protected int groupSize = 5;
    protected RepEnvInfo[] repEnvInfo = null;
    protected DatabaseConfig dbconfig;
    protected final DatabaseEntry key = new DatabaseEntry(new byte[]{1});
    protected final DatabaseEntry data = new DatabaseEntry(new byte[]{100});
    protected static final String TEST_DB_NAME = "TestDB";

    /* Max time to wait for consistency to be established. */
    protected static final int CONSISTENCY_TIMEOUT_MS = 10000;
    public static final int JOIN_WAIT_TIME = 10000;

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        dbconfig = new DatabaseConfig();
        dbconfig.setAllowCreate(true);
        dbconfig.setTransactional(true);
        dbconfig.setSortedDuplicates(false);
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, groupSize);
    }

    /**
     * @throws Exception in subclasses.
     */
    @Override
    @After
    public void tearDown()
        throws Exception {

        /*
         * Don't checkpoint on shutdown, we are not going to use these
         * environments again.
         */
        RepTestUtils.shutdownRepEnvs(repEnvInfo, false);

        /* Verify that all environments were indeed closed. */
        Collection<EnvironmentImpl> residualImpls =
            DbEnvPool.getInstance().getEnvImpls();
        if (residualImpls.size() != 0) {
            String implNames = "";
            for (EnvironmentImpl envImpl : residualImpls) {
                implNames += envImpl.getEnvironmentHome().toString() + " ";
            }

            /*
             * Clear the bad env state so that the next test is not
             * contaminated.
             */
            DbEnvPool.getInstance().clear();
            fail("residual environments:" + implNames);
        }
    }

    /**
     * Populates the master db using the specified transaction configuration.
     */
    protected CommitToken populateDB(ReplicatedEnvironment rep,
                                     String dbName,
                                     int startKey,
                                     int nRecords,
                                     TransactionConfig txnConfig)
        throws DatabaseException {

        Environment env = rep;
        Database db = null;
        boolean done = false;
        Transaction txn = env.beginTransaction(null, txnConfig);
        try {
            db = env.openDatabase(txn, dbName, dbconfig);
            txn.commit();
            txn = null;
            txn = env.beginTransaction(null, txnConfig);
            for (int i = 0; i < nRecords; i++) {
                IntegerBinding.intToEntry(startKey + i, key);
                LongBinding.longToEntry(i, data);
                db.put(txn, key, data);
            }
            txn.commit();
            done = true;
            return txn.getCommitToken();
        } finally {
            if (txn != null && !done) {
                txn.abort();
            }
            if (db != null) {
                db.close();
            }
        }
    }

    /**
     * Populates the master db using the specified keys and values
     * and transaction configuration.
     */
    protected CommitToken populateDB(ReplicatedEnvironment rep,
                                     String dbName,
                                     List<Integer> keys,
                                     TransactionConfig txnConfig)
            throws DatabaseException {

        Environment env = rep;
        Database db = null;
        boolean done = false;
        Transaction txn = env.beginTransaction(null, txnConfig);
        try {
            db = env.openDatabase(txn, dbName, dbconfig);
            txn.commit();
            txn = null;
            txn = env.beginTransaction(null, txnConfig);
            for (int i = 0; i < keys.size(); i++) {
                IntegerBinding.intToEntry(keys.get(i), key);
                LongBinding.longToEntry(i, data);
                db.put(txn, key, data);
            }
            txn.commit();
            done = true;
            return txn.getCommitToken();
        } finally {
            if (txn != null && !done) {
                txn.abort();
            }
            if (db != null) {
                db.close();
            }
        }
    }


    /**
     * Populates the master db without regard for the state of the replicas: It
     * uses ACK NONE to populate the database.
     */
    protected CommitToken populateDB(ReplicatedEnvironment rep,
                                     String dbName,
                                     int startKey,
                                     int nRecords)
        throws DatabaseException {

        return populateDB(rep, dbName, startKey, nRecords,
                          RepTestUtils.WNSYNC_NONE_TC);
    }


    /**
     * Populates the master db with a list of keys. It
     * uses ACK NONE to populate the database.
     */
    protected CommitToken populateDB(ReplicatedEnvironment rep,
                                     String dbName,
                                     List<Integer> keys)
            throws DatabaseException {

        return populateDB(rep, dbName, keys, RepTestUtils.WNSYNC_NONE_TC);
    }

    protected CommitToken populateDB(ReplicatedEnvironment rep,
                                     String dbName,
                                     int nRecords)
        throws DatabaseException {

        return populateDB(rep, dbName, 0, nRecords);
    }

    protected CommitToken populateDB(ReplicatedEnvironment rep, int nRecords)
        throws DatabaseException {

        return populateDB(rep, TEST_DB_NAME, 0, nRecords);
    }

    /** Read the db using the specified transaction configuration. */
    protected void readDB(final ReplicatedEnvironment rep,
                          final String dbName,
                          final int startKey,
                          final int nRecords,
                          final TransactionConfig txnConfig)
        throws DatabaseException {

        Environment env = rep;
        Database db = null;
        Transaction txn = env.beginTransaction(null, txnConfig);
        try {
            db = env.openDatabase(txn, dbName, dbconfig);
            txn.commit();
            txn = null;
            txn = env.beginTransaction(null, txnConfig);
            for (int i = 0; i < nRecords; i++) {
                IntegerBinding.intToEntry(startKey + i, key);
                db.get(txn, key, data, null /* lockMode */);
                final long value = LongBinding.entryToLong(data);
                assertEquals("Database value for key " + (startKey+i),
                             i, value);
            }
            txn.commit();
            txn = null;
        } finally {
            if (txn != null) {
                txn.abort();
            }
            if (db != null) {
                db.close();
            }
        }
    }

    /** Read the db using NO_CONSISTENCY. */
    protected void readDB(final ReplicatedEnvironment rep,
                          final String dbName,
                          final int startKey,
                          final int nRecords)
        throws DatabaseException {

        readDB(rep, dbName, startKey, nRecords,
               RepTestUtils.NO_CONSISTENCY_TC);
    }

    protected void readDB(final ReplicatedEnvironment rep,
                          final String dbName,
                          final int nRecords)
        throws DatabaseException {

        readDB(rep, dbName, 0, nRecords);
    }

    protected void readDB(final ReplicatedEnvironment rep, final int nRecords)
        throws DatabaseException {

        readDB(rep, TEST_DB_NAME, 0, nRecords);
    }

    protected void createGroup()
        throws UnknownMasterException, DatabaseException {

        createGroup(repEnvInfo.length);
    }

    protected void createGroup(int firstn)
        throws UnknownMasterException, DatabaseException {

        for (int i = 0; i < firstn; i++) {
            ReplicatedEnvironment rep = repEnvInfo[i].openEnv();
            State state = rep.getState();
            assertEquals((i == 0) ? State.MASTER : State.REPLICA, state);
        }

        logger.info("A replication group of " + groupSize +
                    " nodes has been created");
    }

    protected ReplicatedEnvironment leaveGroupAllButMaster()
        throws DatabaseException {

        ReplicatedEnvironment master = null;
        for (RepEnvInfo repi : repEnvInfo) {
            if (repi.getEnv() == null) {
                continue;
            }
            if (State.MASTER.equals(repi.getEnv().getState())) {
                master = repi.getEnv();
            } else {
                repi.closeEnv();
            }
        }

        assert(master != null);
        return master;
    }

    /**
     * Restarts the nodes in an existing group using the default join wait
     * time. Returns the info associated with the Master.
     *
     * @return the RepEnvInfo associated with the master, or null if there is
     * no master.  This could be because the election was not concluded within
     * JOIN_WAIT_TIME.
     */
    protected RepEnvInfo restartNodes(RepEnvInfo... nodes)
        throws InterruptedException {

        return restartNodes(JOIN_WAIT_TIME, nodes);
    }

    /**
     * Restarts the nodes in an existing group, waiting the specified amount of
     * time for the election to complete. Returns the info associated with the
     * Master.
     *
     * @return the RepEnvInfo associated with the master, or null if there is
     * no master.  This could be because the election was not concluded within
     * the join wait time.
     */
    protected RepEnvInfo restartNodes(final long joinWaitTime,
                                      final RepEnvInfo... nodes)
        throws InterruptedException {

        logger.info("Restarting " + nodes.length + " nodes");

        /* Restart the group. */
        EnvOpenThread threads[] = new EnvOpenThread[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            threads[i] = new EnvOpenThread(nodes[i]);
            threads[i].start();
        }

        RepEnvInfo mi = null;

        for (EnvOpenThread eot : threads) {
            eot.join(joinWaitTime);
            if (eot.isAlive()) {
                final String msg =
                    "Restart of node " +
                    eot.threadRepInfo.getRepConfig().getNodeName() +
                    " failed to complete within timeout " +
                    joinWaitTime + " ms";

                /*
                 * Print message in case assertion is masked by another failure
                 */
                System.err.println(msg);
                fail(msg);
            }

            if (eot.testException != null) {
                eot.testException.printStackTrace();
            }

            assertNull("test exception: " +
                       eot.testException, eot.testException);
            final ReplicatedEnvironment renv = eot.threadRepInfo.getEnv();
            if ((renv != null) &&
                renv.isValid() &&
                renv.getState().isMaster()) {
                mi = eot.threadRepInfo;
            }
        }

        return mi;
    }

    /**
     * Restarts the nodes in an existing group
     * but does, waiting the specified amount of
     * time for the election to complete. Returns the info associated with the
     * Master.
     *
     */
    protected void restartNodesNoWaitForReady(
                                      final RepEnvInfo... nodes) {

        /* Restart the group. */
        EnvOpenThread threads[] = new EnvOpenThread[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            threads[i] = new EnvOpenThread(nodes[i]);
            threads[i].start();
        }

    }

    /**
     * Find and return the Master from the set of nodes passed in.
     */
    static public RepEnvInfo findMaster(RepEnvInfo... nodes) {
        for (RepEnvInfo ri : nodes) {
            if ((ri.getEnv() == null) || !ri.getEnv().isValid()) {
                continue;
            }

            if (ri.getEnv().getState().isMaster()) {
                return ri;
            }
        }

        return null;
    }

    /**
     * Find the master, waiting for the specified number of milliseconds, and
     * failing if no master is found.
     */
    public static RepEnvInfo findMasterWait(final long timeout,
                                            final RepEnvInfo... nodes)
        throws InterruptedException {

        final long start = System.currentTimeMillis();
        while (true) {
            final RepEnvInfo masterInfo = findMaster(nodes);
            if (masterInfo != null) {
                return masterInfo;
            }
            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            Thread.sleep(500);
        }
        throw new AssertionError(
            "No master after " + (System.currentTimeMillis() - start) +
            " milliseconds");
    }

    /**
     * Find the master and specified number of acking replicas.
     * Fail if waiting for the specified number of milliseconds, and
     * no master or the specified number of replicas are found.
     */
    public static RepEnvInfo findMasterAndWaitForReplicas(final long timeout,
                                                          final int nAckingReplicas,
                                                          final RepEnvInfo... nodes)
        throws InterruptedException {

        final RepEnvInfo masterRepInfo = findMasterWait(timeout, nodes);
        final FeederManager feederManger =
            masterRepInfo.getRepNode().feederManager();

        final boolean replicasReady = new PollCondition(100, (int)timeout) {

            @Override
            protected boolean condition() {
                return feederManger.activeAckReplicaCount() >= nAckingReplicas;
            }
        }.await();

        if (replicasReady) {
            return masterRepInfo;
        }

        throw new AssertionError(
            "Found master but not the required number of replicas." +
            " Number of replicas needed " + nAckingReplicas + "found " +
            feederManger.activeReplicaCount() +
            " after " + timeout +" milliseconds");
    }

    /**
     * Close the nodes that were passed in. Close the master last to prevent
     * spurious elections, where intervening elections create Masters that are
     * immediately closed.
     */
    protected void closeNodes(RepEnvInfo... nodes) {
        RepEnvInfo mi = null;
        for (RepEnvInfo ri : nodes) {
            ReplicatedEnvironment env = ri.getEnv();
            if ((env == null) || !env.isValid()) {
                continue;
            }
            if (env.getState().isMaster()) {
                mi = ri;
                continue;
            }
            ri.closeEnv();
        }

        if (mi != null) {
            mi.closeEnv();
        }
    }

    /**
     * Create and return a {@link Monitor}.  The caller should make sure to
     * call {@link Monitor#shutdown} when it is done using the monitor.
     *
     * @param portDelta the increment past the default port for the monitor
     * port
     * @param monitorName the name of the monitor
     * @throws Exception if a problem occurs
     */
    protected Monitor createMonitor(final int portDelta,
                                    final String monitorName)
        throws Exception {

        final String nodeHosts =
            repEnvInfo[0].getRepConfig().getNodeHostPort() +
            "," + repEnvInfo[1].getRepConfig().getNodeHostPort();
        final int monitorPort =
            Integer.parseInt(RepParams.DEFAULT_PORT.getDefault()) + portDelta;
        final MonitorConfig monitorConfig = new MonitorConfig();
        monitorConfig.setGroupName(RepTestUtils.TEST_REP_GROUP_NAME);
        monitorConfig.setNodeName(monitorName);
        monitorConfig.setNodeHostPort
            (RepTestUtils.TEST_HOST + ":" + monitorPort);
        monitorConfig.setHelperHosts(nodeHosts);

        Properties accessProps = RepTestUtils.readNetProps();
        monitorConfig.setRepNetConfig(
            ReplicationNetworkConfig.create(accessProps));

        return new Monitor(monitorConfig);
    }

    protected void setRepConfigParam(ConfigParam param, String value) {

        for (RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(param.getName(), value);
        }
    }

    protected void setEnvConfigParam(ConfigParam param, String value) {

        for (RepEnvInfo info : repEnvInfo) {
            info.getEnvConfig().setConfigParam(param.getName(), value);
        }
    }

    protected void updateHelperHostConfig() {
        String helperHosts = "";
        for (RepEnvInfo rinfo : repEnvInfo) {
            helperHosts += (rinfo.getRepConfig().getNodeHostPort() + ",");
        }

        setRepConfigParam(RepParams.HELPER_HOSTS, helperHosts);
    }
}
