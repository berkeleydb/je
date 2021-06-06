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


package com.sleepycat.je.rep.subscription;

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.monitor.Monitor;
import com.sleepycat.je.rep.monitor.MonitorConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;

/**
 * Test base of subscription test
 */
class SubscriptionTestBase extends RepTestBase {

    /* polling interval and timeout to check if test is done */
    final static long TEST_POLL_INTERVAL_MS = 1000;
    final static long TEST_POLL_TIMEOUT_MS = 120000;

    /* test db */
    private static final int START_KEY = 1;
    final String dbName = "SUBSCRIPTION_UNIT_TEST_DB";

    /* test db with 10k keys */
    int numKeys = 1024*10;
    protected List<Integer> keys;

    /* a rep group with 1 master, 2 replicas and 1 monitor */
    int numReplicas = 2;
    boolean hasMonitor = true;

    Subscription subscription;
    Monitor monitor;
    Logger logger;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


    /**
     * Write garbage and clean the log until the lower end of VLSN on master
     * has been bumped up to some value greater than the very first VLSN (0).
     */
    void advanceVLSNRange(ReplicatedEnvironment master) {
        /* Need a disk limit to delete files and advance VLSN range begin. */
        master.setMutableConfig(
            master.getMutableConfig().setConfigParam(
                EnvironmentConfig.MAX_DISK,
                String.valueOf(20 * (1L << 20))));

        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(master).getVLSNIndex();

        for (int i = 0; i < 100; i += 1) {
            /* delete and update some keys, and clean the log */
            createObsoleteLogs(master, dbName, numKeys);
            cleanLog(master);

            /* check lower end of VLSN index on master */
            VLSNRange range = vlsnIndex.getRange();
            logger.info(master.getNodeName() + ": " + range);

            if (range.getFirst().compareTo(VLSN.FIRST_VLSN) > 0) {
                return;
            }
        }

        /* failed to move the VLSN lower end */
        fail("Lower end of VLSN index is not GT 0.");
    }

    /**
     * Create a test env and verify it is in good shape
     *
     * @throws InterruptedException if test fails
     */
    void prepareTestEnv() throws InterruptedException {

        createGroup(getNumDataNodes());
        if (hasMonitor) {
            /* monitor is the last node in group */
            ReplicationConfig rConfig =
                repEnvInfo[groupSize - 1].getRepConfig();
            rConfig.setNodeType(NodeType.MONITOR);
            MonitorConfig monConfig = new MonitorConfig();
            monConfig.setNodeName(rConfig.getNodeName());
            monConfig.setGroupName(rConfig.getGroupName());
            monConfig.setNodeHostPort(rConfig.getNodeHostPort());
            monConfig.setHelperHosts(rConfig.getHelperHosts());

            ReplicationConfig r0Config =
                repEnvInfo[0].getEnv().getRepConfig();
            monConfig.setRepNetConfig(r0Config.getRepNetConfig());
            monitor = new Monitor(monConfig);
            monitor.register();
        } else {
            monitor = null;
        }

        for (int i=0; i < getNumDataNodes(); i++) {
            final ReplicatedEnvironment env = repEnvInfo[i].getEnv();
            final boolean isMaster = (env.getState() == MASTER);
            final int targetGroupSize = groupSize;

            ReplicationGroup group = null;
            for (int j=0; j < 100; j++) {
                group = env.getGroup();
                if (group.getNodes().size() == targetGroupSize) {
                    break;
                }
                /* Wait for the replica to catch up. */
                Thread.sleep(1000);
            }
            assertEquals("Nodes", targetGroupSize, group.getNodes().size());
            assertEquals(RepTestUtils.TEST_REP_GROUP_NAME, group.getName());
            for (RepTestUtils.RepEnvInfo rinfo : repEnvInfo) {
                final ReplicationConfig repConfig = rinfo.getRepConfig();
                ReplicationNode member =
                    group.getMember(repConfig.getNodeName());

                /* only log the group and nodes in group for master */
                if (isMaster) {
                    logger.info("group: " + group.getName() +
                                " node: " + member.getName() +
                                " type: " + member.getType() +
                                " socket addr: " + member.getSocketAddress());
                }

                assertNotNull("Member", member);
                assertEquals(repConfig.getNodeName(), member.getName());
                assertEquals(repConfig.getNodeType(), member.getType());
                assertEquals(repConfig.getNodeSocketAddress(),
                             member.getSocketAddress());
            }

            /* verify monitor */
            final Set<ReplicationNode> monitorNodes = group.getMonitorNodes();
            for (final ReplicationNode n : monitorNodes) {
                assertEquals(NodeType.MONITOR, n.getType());
            }
            if (hasMonitor) {
                assertEquals("Monitor nodes", 1, monitorNodes.size());
                logger.info("monitor verified");
            }

            /* verify data nodes */
            final Set<ReplicationNode> dataNodes = group.getDataNodes();
            for (final ReplicationNode n : dataNodes) {
                assertEquals(NodeType.ELECTABLE, n.getType());
            }
            logger.info("data nodes verified");
        }
    }

    protected void cleanLog(ReplicatedEnvironment repEnv) {
        CheckpointConfig force = new CheckpointConfig();
        force.setForce(true);

        EnvironmentStats stats = repEnv.getStats(new StatsConfig());
        int numCleaned;
        int cleanedThisRun;
        long beforeNFileDeletes = stats.getNCleanerDeletions();

        /* clean logs */
        numCleaned = 0;
        while ((cleanedThisRun = repEnv.cleanLog()) > 0) {
            numCleaned += cleanedThisRun;
        }
        repEnv.checkpoint(force);

        while ((cleanedThisRun = repEnv.cleanLog()) > 0) {
            numCleaned += cleanedThisRun;
        }
        repEnv.checkpoint(force);

        stats = repEnv.getStats(new StatsConfig());
        long afterNFileDeletes = stats.getNCleanerDeletions();
        long actualDeleted = afterNFileDeletes - beforeNFileDeletes;
        repEnv.checkpoint(force);

        logger.info(repEnv.getNodeName() +
                    " deletedFiles=" + actualDeleted +
                    " numCleaned=" + numCleaned);
    }

    void failTimeout() {
        String error = "fail test due to timeout in " +
                       TEST_POLL_TIMEOUT_MS + " ms";
        logger.info(error);
        fail(error);
    }

    /* Populate data into test db and verify */
    void populateDataAndVerify(ReplicatedEnvironment masterEnv) {
        keys.addAll(createTestData(START_KEY, numKeys));
        populateDB(masterEnv, dbName, keys);
        readDB(masterEnv, dbName, START_KEY, numKeys);
        logger.info(numKeys + " records (start key: " +
                    START_KEY + ") have been populated into db " +
                    dbName + " and verified");
    }

    /*
     * Create a subscription configuration
     *
     * @param masterEnv     env of master node
     * @param useGroupUUID  true if use a valid group uuid
     *
     * @return a subscription configuration
     * @throws Exception
     */
    SubscriptionConfig createSubConfig(ReplicatedEnvironment masterEnv,
                                       boolean useGroupUUID)
        throws Exception {
        return createSubConfig(masterEnv, useGroupUUID, new Properties());
    }

    SubscriptionConfig createSubConfig(ReplicatedEnvironment masterEnv,
                                       boolean useGroupUUID, Properties sp)
        throws Exception {
        /* constants and parameters used in test */
        final String home = "./subhome/";
        final String subNodeName = "test-subscriber-node";
        final String nodeHostPortPair = "localhost:6001";

        String feederNode;
        int feederPort;
        String groupName;
        File subHome =
            new File(envRoot.getAbsolutePath() + File.separator + home);
        if (!subHome.exists()) {
            if (subHome.mkdir()) {
                logger.info("create test dir " + subHome.getAbsolutePath());
            } else {
                fail("unable to create test dir, fail the test");
            }
        }

        ReplicationGroup group = masterEnv.getGroup();
        ReplicationNode member = group.getMember(masterEnv.getNodeName());
        feederNode = member.getHostName();
        feederPort = member.getPort();
        groupName = group.getName();

        UUID uuid;
        if (useGroupUUID) {
            uuid = group.getRepGroupImpl().getUUID();
        } else {
            uuid = null;
        }

        String msg = "Feeder is on node " + feederNode + ":" + feederPort +
                     " in replication group "  + groupName +
                     " (group uuid: " + uuid + ")";
        logger.info(msg);

        final String feederHostPortPair = feederNode + ":" + feederPort;


        return new SubscriptionConfig(subNodeName, subHome.getAbsolutePath(),
                                      nodeHostPortPair, feederHostPortPair,
                                      groupName, uuid, NodeType.SECONDARY,
                                      null, sp);
    }

    /* Create a list of (k, v) pairs for testing */
    private static List<Integer> createTestData(int start, int num) {
        final List<Integer> ret = new ArrayList<>();
        for (int i = start; i < start + num; i++) {
            ret.add(i);
        }
        return ret;
    }

    private int getNumDataNodes() {
        return 1 + numReplicas;
    }

    /*
     * Put some keys multiple times and delete it, in order to create
     * obsolete entries in log files so cleaner can clean it
     *
     * @param master    master node
     * @param dbName    test db name
     * @param numKeys   number of keys to put and delete
     */
    private void createObsoleteLogs(ReplicatedEnvironment master,
                                    String dbName,
                                    int numKeys) {
        final int repeatPutTimes = 10;
        final int startKey = START_KEY;

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[100]);

        final TransactionConfig txnConf = new TransactionConfig();
        txnConf.setDurability
            (new Durability(Durability.SyncPolicy.NO_SYNC,
                            Durability.SyncPolicy.NO_SYNC,
                            Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));


        try (Database db = master.openDatabase(null, dbName, dbConfig)) {
            for (int i = startKey; i < numKeys; i++) {
                IntegerBinding.intToEntry(i, key);
                final Transaction txn = master.beginTransaction(null, txnConf);
                for (int repeat = 0; repeat < repeatPutTimes; repeat++) {
                    db.put(txn, key, data);
                }
                db.delete(txn, key);
                txn.commit();
            }

            /* One more synchronous one to flush the log files. */
            IntegerBinding.intToEntry(startKey, key);
            txnConf.setDurability
                (new Durability(Durability.SyncPolicy.SYNC,
                                Durability.SyncPolicy.SYNC,
                                Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));
            final Transaction txn = master.beginTransaction(null, txnConf);
            db.put(txn, key, data);
            db.delete(txn, key);
            txn.commit();
        }
    }

    class SubscriptionTestCallback implements SubscriptionCallback {


        private final int numKeysExpected;
        private final boolean allowException;

        private int numCommits;
        private int numAborts;
        private int numExceptions;
        private Exception firstException;
        private Exception lastException;
        private List<byte[]> recvKeys;
        private VLSN firstVLSN;
        private boolean testDone;

        SubscriptionTestCallback(int numKeysExpected, boolean allowException) {
            this.numKeysExpected = numKeysExpected;
            this.allowException = allowException;
            numAborts = 0;
            numCommits = 0;
            recvKeys = new ArrayList<>();
            firstVLSN = VLSN.NULL_VLSN;
            testDone = false;
        }

        /* callback does not allow exception */
        SubscriptionTestCallback(int numKeysExpected) {
            this(numKeysExpected, false);
        }

        @Override
        public void processPut(VLSN vlsn, byte[] key, byte[] value,
                               long txnId) {
            processPutAndDel(vlsn, key);
        }

        @Override
        public void processDel(VLSN vlsn, byte[] key, long txnId) {
            processPutAndDel(vlsn, key);
        }

        @Override
        public void processCommit(VLSN vlsn, long txnId) {
            numCommits++;
        }

        @Override
        public void processAbort(VLSN vlsn, long txnId) {
            numAborts++;
        }

        @Override
        public void processException(Exception exception) {
            assert (exception != null);

            if (allowException) {
                numExceptions++;
                if (firstException == null) {
                    firstException = exception;
                }
                lastException = exception;
            } else {
                /* fail test if we do not expect any exception*/
                fail(exception.getMessage());
            }
            testDone = true;
        }

        void resetTestDone() {
            testDone = false;
        }

        boolean isTestDone() {
            return testDone;
        }

        List<byte[]> getAllKeys() {
            return recvKeys;
        }

        VLSN getFirstVLSN() {
            return firstVLSN;
        }

        int getNumCommits() {
            return numCommits;
        }

        int getNumAborts() {
            return numAborts;
        }

        int getNumExceptions() {
            return numExceptions;
        }

        Exception getLastException() {
            return lastException;
        }

        Exception getFirstException() {
            return firstException;
        }

        private void processPutAndDel(VLSN vlsn, byte[] key) {
            /* record the first VLSN received from feeder */
            if (firstVLSN.isNull()) {
                firstVLSN = vlsn;
            }

            if (recvKeys.size() < numKeysExpected) {
                recvKeys.add(key);

                logger.finest("vlsn: " + vlsn +
                              "key " + Key.dumpString(key, 0) +
                              ", # of keys received: " + recvKeys.size() +
                              ", expected: " + numKeysExpected);

                if (recvKeys.size() == numKeysExpected) {
                    logger.info("received all " + numKeysExpected + " keys.");
                    testDone = true;
                }

            } else {
                /*
                 * we may receive more keys because in some tests,  the # of
                 * keys expected could be less than the size of database, but
                 * they are not interesting to us so ignore.
                 */
                logger.finest("keys beyond expected " + numKeysExpected +
                              " keys, vlsn: " + vlsn +
                              ", key: " + Key.dumpString(key, 0));
            }
        }

    }
}
