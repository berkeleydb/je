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
package com.sleepycat.je.rep.arb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.arbiter.Arbiter;
import com.sleepycat.je.rep.arbiter.ArbiterConfig;
import com.sleepycat.je.rep.arbiter.ArbiterMutableConfig;
import com.sleepycat.je.rep.arbiter.ArbiterStats;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.rep.monitor.GroupChangeEvent;
import com.sleepycat.je.rep.monitor.JoinGroupEvent;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.monitor.Monitor;
import com.sleepycat.je.rep.monitor.MonitorChangeListener;
import com.sleepycat.je.rep.monitor.MonitorConfig;
import com.sleepycat.je.rep.monitor.NewMasterEvent;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.util.DbGroupAdmin;
import com.sleepycat.je.rep.util.DbPing;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHookAdapter;

public class ArbiterTest extends RepTestBase {

    private final int portOffset = 20;
    private volatile int phase;
    private boolean useRepNetConfig;
    private Arbiter arb;
    private Arbiter arb2;
    private Monitor monitor;

    private final static long fiveSec = 5 * 1000;
    private final static long tenSec = 10 * 1000;
    private final static long UNKN_TIMEOUT = tenSec;
    private final static int MAX_PHASE_WAITS = 30;
    private final static int PHASE_ERROR = -1;
    private final static long ARB_SETUP_TIMEOUT = 60 * 1000;

    @Override
    public void setUp() throws Exception {
        groupSize = 4;
        super.setUp();
        phase = 0;

        /*
         * to set fine logging.
         * Logger parent = Logger.getLogger("com.sleepycat.je");
         * parent.setLevel(Level.FINE);
         * */
        ReplicationNetworkConfig repNetConfig =
            RepTestUtils.readRepNetConfig();
        useRepNetConfig = !repNetConfig.getChannelType().isEmpty();
        arb = null;
        arb2 = null;
        monitor = null;
    }

    /**
     * @throws Exception in subclasses.
     */
    @Override
    @After
    public void tearDown()
        throws Exception {
        Replica.setInitialReplayHook(null);
        phase = Integer.MAX_VALUE;
        if (arb != null) {
            arb.shutdown();
        }
        if (arb2 != null) {
            arb2.shutdown();
        }
        if (monitor != null) {
            monitor.shutdown();
        }
        super.tearDown();
    }

    /**
     * Test to insure there is an exception if
     *  the envHome parameter is null and if
     *  the arbiter home directory does not exist
     *
     * @throws DatabaseException
     */
    @Test
    public void testNegative()
        throws DatabaseException {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (arbHome.exists()) {
            arbHome.delete();
        }
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        boolean gotErrors = false;

        try {
            try {
                arb = getReadyArbiter(ac);
            } catch (IllegalArgumentException notFound) {
                gotErrors = true;
            }
            if (!gotErrors) {
                fail("Test should have failed due to null arg.");
            }

            gotErrors = false;
            ac.setArbiterHome(arbHome.getAbsolutePath());
            try {
                arb = getReadyArbiter(ac);
            } catch (IllegalArgumentException e) {
                gotErrors = true;
            }
            if (!gotErrors) {
                fail("Test should have failed due to missing directory.");
            }
            arbHome.mkdir();
        } finally {
            if (arb != null) {
                arb.shutdown();
            }
        }
    }

    /**
     * Test to make sure that an UnKnownMasterException is
     * thrown if an arbiter cannot communicate with the group.
     * @throws DatabaseException
     */
    @Test
    public void testTimeout()
        throws DatabaseException {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(1000, TimeUnit.MILLISECONDS);
        boolean gotUnMEx = false;
        try {
            arb = getReadyArbiter(ac);
        } catch (Exception e) {
            gotUnMEx = true;
        }
        assert(gotUnMEx);
    }

    /**
     * Test to insure that the Arbiter acks transactions when
     * the Replica node is down. Also tests that ack durability
     * of ALL does not succeed with a master and arb in a
     * two rep node and Arbiter group.
     * This test brings up two rep nodes and an arbiter node.
     * Adds data, stops the replica node. The group now consists
     * of the master and arbiter. More data is added.
     * @throws Exception
     */
    @Test
    public void testReplicaDown()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        Properties props = new Properties();
        props.setProperty(ReplicationConfig.GROUP_NAME, rc.getGroupName());
        props.setProperty(ReplicationConfig.NODE_NAME, nodeName);
        props.setProperty(ReplicationMutableConfig.HELPER_HOSTS,
                          rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        props.setProperty(ReplicationConfig.NODE_HOST_PORT, nodeHostPort);
        props.setProperty(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "5 s" );
        ArbiterConfig ac = new ArbiterConfig(props);
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              leaveGroupAllButMaster();
              populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);
              /* should not be able to do a write with ALL durability */
              TransactionConfig txncnf = new TransactionConfig();
              Durability d =
                  new Durability(SyncPolicy.NO_SYNC,      // localSync
                                 SyncPolicy.NO_SYNC,      // replicaSync
                                 ReplicaAckPolicy.ALL);       // replicaAck
              txncnf.setDurability(d);
              boolean gotException = false;
              try {
                  populateDB(repEnvInfo[0].getEnv(), "db", 21, 10, txncnf);
              } catch (InsufficientReplicasException e) {
                  gotException = true;
              }
              if (!gotException) {
                  fail("Insertion with ACK durabilty of " +
                       "ALL should have failed.");
              }
              phase++;
            }
        });

        /* Do some fail testing by starting arbiter and timeout */
        boolean gotException = false;
        try {
        arb = getReadyArbiter(ac);
        } catch (Exception e){
            gotException = true;
        }
        if (!gotException) {
            fail("Starting Arbiter should have failed" +
                 " due to no members in RepGroup up.");
        }
        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        arb.shutdown();
    }

    /*
     * Test to insure that multiple threads inserting data while the replica is
     * stopped still succeed. After the replica is stopped,
     * the Master and Arbiter handle the write requests.
     */
    @Test
    public void testReplicaDownActiveXact()
        throws Exception {
        String dbname = "db";
        int nInserters = 30;
        Thread[] insertThreads = new Thread[nInserters];
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_" + rc.getNodeName();
        String nodeHostPort = rc.getNodeHostname() + ":" + port;

        Properties props = new Properties();
        props.setProperty(ReplicationConfig.GROUP_NAME, rc.getGroupName());
        props.setProperty(ReplicationConfig.NODE_NAME, nodeName);
        props.setProperty(ReplicationMutableConfig.HELPER_HOSTS,
                          rc.getNodeHostPort() + "," + rc2.getNodeHostPort());
        props.setProperty(ReplicationConfig.NODE_HOST_PORT, nodeHostPort);
        props.setProperty(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "5 s" );
        ArbiterConfig ac = new ArbiterConfig(props);
        File arbHome =
            new File(envRoot.getAbsolutePath() + File.separator + "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        createGroup(2);

        arb = getReadyArbiter(ac);

        populateDB(repEnvInfo[0].getEnv(), dbname, 0, 10, null );

        /* Do some DML with master and arbiter */
        for (int i = 0; i < nInserters; i++) {
            insertThreads[i] =
                new Thread(new InsertRunnable(repEnvInfo[0].getEnv(),
                                              dbname,
                                              1,
                                              1000 * i,
                                              10));
        }

        for (int i = 0; i < nInserters; i++) {
            insertThreads[i].start();
        }
        Thread.sleep(1*1000);
        leaveGroupAllButMaster();
        Thread.sleep(1*1000);

        int startKey = 11;
        int nRecords = 100;
        Environment env = repEnvInfo[0].getEnv();
        Transaction txn = null;
        Database db = null;
        try {
            db = env.openDatabase(txn, dbname, dbconfig);
            txn = env.beginTransaction(null, null);
            for (int i = 0; i < nRecords; i++) {
                IntegerBinding.intToEntry(startKey + i, key);
                LongBinding.longToEntry(i, data);
                db.put(txn, key, data);
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
        phase++;
        waitForPhase(nInserters + 1);
        arb.shutdown();
    }

    /**
     * This tests that a slow responding ack from a replica will not
     * throw an InsufficientAckException if the Arbiter has acked the
     * transaction.
     */
    @Test
    public void testSlowReplica() throws Exception {
        String dbname = "db";
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_" + rc.getNodeName();
        String nodeHostPort = rc.getNodeHostname() + ":" + port;

        Properties props = new Properties();
        props.setProperty(ReplicationConfig.GROUP_NAME, rc.getGroupName());
        props.setProperty(ReplicationConfig.NODE_NAME, nodeName);
        props.setProperty(ReplicationMutableConfig.HELPER_HOSTS,
                          rc.getNodeHostPort() + "," + rc2.getNodeHostPort());
        props.setProperty(ReplicationConfig.NODE_HOST_PORT, nodeHostPort);
        props.setProperty(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "5 s" );
        ArbiterConfig ac = new ArbiterConfig(props);
        File arbHome =
            new File(envRoot.getAbsolutePath() + File.separator + "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        AtomicInteger delayFlag = new AtomicInteger(0);

        Replica.setInitialReplayHook(new DelayCommit(1100, delayFlag));
        int firstn = 2;
        for (int i = 0; i < firstn; i++) {
            ReplicationConfig repcfg = repEnvInfo[i].getRepConfig();
            repcfg.setReplicaAckTimeout(1, TimeUnit.SECONDS);
            ReplicatedEnvironment rep = repEnvInfo[i].openEnv();
            State state = rep.getState();
            assertEquals((i == 0) ? State.MASTER : State.REPLICA, state);
        }

        arb = getReadyArbiter(ac);

        delayFlag.incrementAndGet();

        populateDB(repEnvInfo[0].getEnv(), dbname, 0, 10, null );

        arb.shutdown();
    }

    /**
     * Test that two Arbiters can not join the same
     * replication group.
     * This test brings up two rep nodes and an arbiter node.
     * Adds data
     * A second arbiter node is attempted to be booted. This should
     * fail since there is an arbiter already in the rep group.
     * @throws Exception
     */
    @Test
    public void testTwoArbiters()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              phase++;
              waitForPhase(3);
            }
        });
        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        arbHome = new File(envRoot.getAbsolutePath()+File.separator+ "arb1");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        port++;
        nodeHostPort = (rc.getNodeHostname()+ ":" + port);
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName("arbiter_2");
        arb2 = null;
        boolean gotException = false;
        try {
            arb2 = getReadyArbiter(ac);
        } catch (EnvironmentFailureException e) {
            gotException = true;
        } finally {
            if (arb2 != null) {
                arb2.shutdown();
            }
            phase++;
        }
        if (!gotException) {
            fail("Second Arbiter should not " +
                "be allowed to be part of the group.");
        }

        arb.shutdown();
    }

    /**
     * Test that two Arbiter cannot be booted using the same
     * directory at the same time.
     * This test brings up two rep nodes and an arbiter node.
     * Attempts to bring up another arb using the same arb directory.
     * Note that this test exercises code running in the same JVM.
     * Check is made because the underlying envImpl is cached and we
     * check that this is illegal for arbiters.
     * Separate processes are required to test the file locking code.
     * @throws Exception
     */
    @Test
    public void testArbsSameDir()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              phase++;
              waitForPhase(3);
            }
        });
        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        /* configure to attempt to boot in same directory */
        port++;
        nodeHostPort = (rc.getNodeHostname()+ ":" + port);
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName("arbiter_2");
        arb2 = null;
        boolean gotError = false;
        try {
            arb2 = getReadyArbiter(ac);
        } catch (Exception e) {
            gotError = true;
        } finally {
            phase++;
            if (arb2 != null) {
                arb2.shutdown();
            }
        }

        if (!gotError) {
            fail("Starting arbiter in directory when another arbiter " +
                 "is running should have failed.");
        }
        arb.shutdown();
    }

    /**
     * Test that the rep group consisting of a master, replica and arbiter
     * can have write availablity when the master becomes unavailable.
     * This test brings up two rep nodes and an arbiter node.
     * Adds data, stops the master node add more data.
     * @throws Exception
     */
    @Test
    public void testMasterDown()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              closeMaster();
              waitForMaster(repEnvInfo[1]);
              populateDB(repEnvInfo[1].getEnv(), "db", 11, 10, null);
              phase++;
            }

        });
        testthread.start();
        arb = getReadyArbiter(ac);
        waitForReplica(arb, ARB_SETUP_TIMEOUT);
        phase++;
        waitForPhase(2);
        arb.shutdown();
    }

    /**
     * Test that the Arbiter prevents a node from becoming
     * Master due to its VLSN lower than the Arbiters.
     * This test brings up two rep nodes and an arbiter node.
     * Adds data, stops the replica node add more data.
     * stops the master
     * starts the node that was the Replica
     * waits a bit to allow time for node to attempt an election
     * start the node that was master
     * check that this node is the master
     * add  more data
     * @throws Exception
     */
    @Test
    public void testOneMaster()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              leaveGroupAllButMaster();
              populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);
              closeMaster();

              restartNodesNoWaitForReady(repEnvInfo[1]);

              try {
                  Thread.sleep(fiveSec);
              } catch (Exception e) {

              }

              /* the replica should not be able to become master */
              RepEnvInfo master = findMaster(repEnvInfo[1]);
              assert(master == null);

              try {
                  restartNodes(tenSec, repEnvInfo[0]);
              } catch (InterruptedException e) {
                  fail("error restarting node1.");
              }
              try {
                  Thread.sleep(tenSec);
              } catch (Exception e) {

              }
              master = findMaster(repEnvInfo[0]);
              assert(master != null);

              populateDB(repEnvInfo[0].getEnv(), "db", 21, 10, null);
              phase++;
            }

        });
        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        arb.shutdown();
    }

    /**
     * Test booting the Arbiter first followed by two
     * Replication nodes. The initial replication group
     * is empty.
     * @throws Exception
     */
    @Test
    public void testBootOrderArbFirst()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              phase++;
            }
        });

        ArbiterRunnable arun = new ArbiterRunnable(ac);
        Thread arbThread = new Thread(arun);
        arbThread.start();
        try {
            Thread.sleep(fiveSec);
        } catch (InterruptedException e) {
            fail("Thread sleep.");
        }
        try {
            testthread.start();
            waitForPhase(1);
        } finally {
            arun.shutdown();
        }
        arbThread.join();
    }

    /**
     * Test that a rep group with an Arbiter and one Rep Node
     * is provides write availability. Also tests that another
     * rep node can be added to the group and that this node
     * can become the master if the other (master) rep node is not
     * available.
     * Tests group database initialization and boot order with the Arbiter.
     * Start Arbiter
     * Start Node0
     * Add data
     * Start Node1
     * Add data
     * Stop Node0 - Arbiter and Node1 will allow Node1 to be elected master
     * Add data
     * @throws Exception
     */
    @Test
    public void testBootOrderOneRepTwoRep()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(1);
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );

              restartNodesNoWaitForReady(repEnvInfo[1]);
              try {
                  Thread.sleep(fiveSec);
              } catch (Exception e) {

              }
              populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null );
              closeMaster();
              try {
                  Thread.sleep(fiveSec);
              } catch (Exception e) {

              }
              populateDB(repEnvInfo[1].getEnv(), "db", 21, 10, null );

              phase++;
            }
        });

        ArbiterRunnable arun = new ArbiterRunnable(ac);
        Thread arbThread = new Thread(arun);
        arbThread.start();
        try {
            Thread.sleep(fiveSec);
        } catch (InterruptedException e) {
            fail("Thread sleep.");
        }
        try {
            testthread.start();
            waitForPhase(1);
        } finally {
            arun.shutdown();
        }
        arbThread.join();
    }

    /**
     * This test uses the arbiter to flip flop the Master between two
     * rep nodes.
     * This test brings up two rep nodes and an arbiter node.
     * Adds data
     * stops the master
     * Add more data
     * Start the old master
     * add more data
     * stop current master
     * add more data.
     * @throws Exception
     */
    @Test
    public void testFlipMaster()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              closeMaster();
              /* wait to allow for other node to become master */
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              RepEnvInfo master = findMaster(repEnvInfo[1]);
              assert(master != null);

              populateDB(repEnvInfo[1].getEnv(), "db", 11, 10, null);

              /* Restart the original master */
              try {
                  restartNodes(fiveSec, repEnvInfo[0]);
              } catch (InterruptedException e) {
                  fail("error restarting node1.");
              }

              master = findMaster(repEnvInfo[1]);
              assert(master != null);

              /* Stop the current master */
              closeMaster();
              /* wait to allow for other node to become master */
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              master = findMaster(repEnvInfo[0]);
              assert(master != null);
              populateDB(repEnvInfo[0].getEnv(), "db", 21, 10, null);
              /* Restart the original master */
              try {
                  restartNodes(fiveSec, repEnvInfo[1]);
              } catch (InterruptedException e) {
                  fail("error restarting node1.");
              }
              populateDB(repEnvInfo[0].getEnv(), "db", 31, 10, null);

              phase++;
            }
        });

        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        arb.shutdown();
    }

    /**
     * This test exercises the situation when a rep node is added to
     * a group consisting of two rep nodes and an arbiter. Also
     * tests that the Arbiter will prevent rep nodes from becoming
     * Master if the other rep nodes are not current. This is done
     * when the rep group consists of three rep nodes and an arbiter.
     *
     * This test brings up two rep nodes and an arbiter node.
     * Adds data (2 rep/1 arb rep size 2)
     * stops the replica
     * Add more data (1 rep/1 arb rep size 2)
     * Stop the master
     * Bring up old replica.
     * boot new replica node
     * check that there is no master
     *                (due to arb prevention) (2 rep/ 1 arb rep size 3)
     * bring up old master.
     * add more data (3 rep / 1 arb rep size 3)
     * stop arbiter
     * add more data (3 rep rep size 3)
     * stop rep node 1
     * add more data (2 rep) rep size 3
     * @throws Exception
     */
    @Test
    public void testQuad()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              leaveGroupAllButMaster();
              populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);
              closeMaster();
              try {
                  restartNodesNoWaitForReady(repEnvInfo[1]);
                  restartNodesNoWaitForReady(repEnvInfo[2]);
              } catch (Exception e) {
                  fail("test failed becasuse node could not be started. "+e);
              }
              /* wait to allow for other node to become master */
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              RepEnvInfo master = findMaster(repEnvInfo[1]);
              assert(master == null);
              master = findMaster(repEnvInfo[2]);
              assert(master == null);
              /* restart original master */
              try {
                  restartNodes(fiveSec, repEnvInfo[0]);
              } catch (InterruptedException e) {
                  fail("error restarting node1.");
              }
              populateDB(repEnvInfo[0].getEnv(), "db", 21, 10, null);
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              phase++;
              /* wait for arbiter to be shut down */
              waitForPhase(3);
              populateDB(repEnvInfo[0].getEnv(), "db", 31, 10, null);
              closeNodes(repEnvInfo[1]);
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              populateDB(repEnvInfo[0].getEnv(), "db", 41, 10, null);
              phase++;
            }
        });

        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        arb.shutdown();
        try {
            Thread.sleep(fiveSec);
        } catch (InterruptedException e) {
            fail("Thread sleep.");
        }
        phase++;
        waitForPhase(4);
    }

    /**
     * Tests that the Arbiter is a election participant when
     * the replication group consists of three rep nodes
     * and an Arbiter. See below for various rep nodes/arb
     * in the 3 rep node/arbiter configuration.
     *
     *
     * Creates group of 3 rep nodes. (3 rep) rep size 3
     * Create arbiter node. (3rep/1arb) rep size 3
     * Add data
     * Stop all rep nodes.
     * Stop arbiter node.
     * Start nodes 0 and 1. (2 rep)
     * Check to make sure there is no master
     * Start rep node 2 (3 rep)
     * Check to see there is a master.
     * add data
     * stop all rep nodes
     * Start arbiter
     * start rep node 0 and 1 (2 rep/1arb)
     * make sure there is a master
     * Add data
     * stop arbiter (2 rep)
     * Add data
     * start arbiter
     * stop replica (1 rep/1 arb)
     * attempt to insert data. check for isf rep exception
     *
     * @throws Exception
     */
    @Test
    public void testQuadElection()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(3);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              closeNodes(repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);
              phase++;
              waitForPhase(3);
              try {
                  restartNodesNoWaitForReady(repEnvInfo[0]);
                  restartNodesNoWaitForReady(repEnvInfo[1]);
              } catch (Exception e) {
                  fail("test failed becasuse node could not be started. "+e);
              }
              /* wait to allow for other node to become master */
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              RepEnvInfo master = findMaster(repEnvInfo[0]);
              assert(master == null);
              master = findMaster(repEnvInfo[1]);
              assert(master == null);

              try {
                  restartNodesNoWaitForReady(repEnvInfo[2]);
              } catch (Exception e) {
                  fail("test failed becasuse node could not be started. "+e);
              }
              /* wait to allow for other node to become master */
              try {
                  Thread.sleep(fiveSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              master =
                  findMaster(repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);
              assert(master != null);
              populateDB(master.getEnv(), "db", 11, 10, null );
              closeNodes(repEnvInfo[0], repEnvInfo[1], repEnvInfo[2]);
              phase++;
              waitForPhase(5);
              /* write with two rep nodes and an arbiter */
              try {
                  restartNodesNoWaitForReady(repEnvInfo[0]);
                  restartNodesNoWaitForReady(repEnvInfo[1]);
              } catch (Exception e) {
                  fail("test failed because node could not be started. "+e);
              }
              /* wait to allow for other node to become master */
              try {
                  Thread.sleep(tenSec);
              } catch (InterruptedException e) {
                  fail("Thread sleep.");
              }
              master = findMaster(repEnvInfo[0], repEnvInfo[1]);
              assert(master != null);
              populateDB(master.getEnv(), "db", 21, 10, null );
              phase++;
              waitForPhase(7);
              populateDB(master.getEnv(), "db", 31, 10, null );
              phase++;
              waitForPhase(9);
              leaveGroupAllButMaster();
              boolean gotISRException = false;
              try {
                  populateDB(master.getEnv(), "db", 41, 10, null );
              } catch (InsufficientReplicasException e) {
                  gotISRException = true;
              }
              assertTrue(gotISRException);
              phase++;

            }
        });

        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        arb.shutdown();
        phase++;
        waitForPhase(4);
        phase++;
        arb = getReadyArbiter(ac);
        waitForPhase(6);
        arb.shutdown();
        phase++;
        waitForPhase(8);
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(10);
        arb.shutdown();
    }

    /**
     * Tests that ping works on the Arbiter.
     * This test brings up two rep nodes and an arbiter node.
     * Adds data, stops the replica node add more data.
     * Uses DbPing to send ping request to the Arbiter.
     *
     * @throws Exception
     */
    @Test
    public void testPing()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              leaveGroupAllButMaster();
              populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);
              phase++;
              waitForPhase(3);
            }
        });
        testthread.start();
        arb = getReadyArbiter(ac);
        phase++;
        waitForPhase(2);
        String[] args = new String[] {
                "-groupName", RepTestUtils.TEST_REP_GROUP_NAME,
                "-nodeName", nodeName,
                "-nodeHost", nodeHostPort,
                "-socketTimeout", "5000" };

        /* Ping the node. */
        PrintStream original = System.out;
        try {
            /* Avoid polluting the test output. */
            System.setOut(new PrintStream(new ByteArrayOutputStream()));

            DbPing.main(args);

        } catch (Exception e) {
            fail("Unexpected exception: " + LoggerUtils.getStackTrace(e));
        } finally {
            System.setOut(original);
        }
        phase++;
        arb.shutdown();
    }

    /**
     * Test Arbiter Monitor events are fired and that the
     * correct transactions are being acked by the
     * Arbiter. Checks using Arbiter statistics.
     *
     * @throws Exception
     */
    @Test
    public void testGroupAckAndReJoin()
        throws Exception {
        int nInserters = 30;
        Thread[] insertThreads = new Thread[nInserters];

        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String monName = "monitor_" + rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        port++;
        MonitorConfig mc = new MonitorConfig();
        mc.setGroupName(rc.getGroupName());
        mc.setNodeName(monName);
        mc.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        mc.setNodeHostPort(rc.getNodeHostname() + ":" + port);
        TestListener listenUp = new TestListener();
        createGroup(2);

        /*
         * Do a check to insure that the monitor
         * event is sent by the arbiter.
         */
        monitor = new Monitor(mc);
        monitor.register();
        monitor.startListener(listenUp);
        arb = getReadyArbiter(ac);
        Thread.sleep(500);
        ArrayList<Object> events = listenUp.getEvents();
        if (events.size() == 0 ) {
            fail("did not get Arbiter join event.");
        }

        Object lastEvent = events.get(events.size() - 1);
        if (!(lastEvent instanceof GroupChangeEvent ||
            lastEvent instanceof JoinGroupEvent)) {
            fail("did not get Arbiter join group event." + lastEvent);
        }
        String eventNodeName;
        if (lastEvent instanceof GroupChangeEvent) {
            eventNodeName = ((GroupChangeEvent)lastEvent).getNodeName();
        } else {
            eventNodeName = ((JoinGroupEvent)lastEvent).getNodeName();
        }
        if (!eventNodeName.equals(ac.getNodeName())) {
            fail("did not get Arbiter join group event. "+
                 "Node name is not Arbiter." + lastEvent);
        }

        /* all nodes up */
        populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null);

        ArbiterStats stats = arb.getStats(new StatsConfig());
        String masterName = repEnvInfo[0].getRepConfig().getNodeName();
        assertTrue(stats.getMaster().startsWith(masterName));
        assertTrue(
            stats.getState().equals(
                ReplicatedEnvironment.State.REPLICA.toString()));
        leaveGroupAllButMaster();
        stats = arb.getStats(new StatsConfig());
        long preInsertAckCount = stats.getAcks();

        /* Do some DML with master and arbiter */
        for (int i = 0; i < nInserters; i++) {
            insertThreads[i] =
                new Thread(
                    new PopulateDeleteRunnable(
                        repEnvInfo[0].getEnv(),
                        1, 1000 * i,
                        10));
        }

        for (int i = 0; i < nInserters; i++) {
            insertThreads[i].start();
        }
        phase++;
        waitForPhase(nInserters + 1);

        /* Do some statistics checking */
        stats = arb.getStats(new StatsConfig());
        long ackCount = stats.getAcks();
        assertEquals( nInserters * 20, ackCount - preInsertAckCount);
        assertTrue(stats.getVLSN() >= stats.getDTVLSN());
        assertTrue(stats.getWrites() >= stats.getFSyncs());

        StatsConfig sc = new StatsConfig();
        sc.setClear(true);
        stats = arb.getStats(sc);
        assertEquals(ackCount, stats.getAcks());
        assertTrue(stats.getWrites() > 0);
        stats = arb.getStats(new StatsConfig());
        assertEquals(0, stats.getAcks());
        assertTrue(stats.getWrites() == 0);

        restartNodes(fiveSec, repEnvInfo[1]);

        /* do dml with all nodes up again */
        populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);
        closeMaster();
        waitForMaster(repEnvInfo[1]);

        /*
         * Do dml after the original master was closed and the
         * replica was elected the new master.
         */
        populateDB(repEnvInfo[1].getEnv(), "db", 21, 10, null);
        arb.shutdown();
        restartNodes(fiveSec, repEnvInfo[0]);
        Thread.sleep(500);

        /* check for the arbiter leave group event */
        if (!lookForEvent(ac.getNodeName(),
                          LeaveReason.NORMAL_SHUTDOWN,
                          listenUp.getEvents())) {
            fail("Did not get expected Arbiter shutdown monitor event.");
        }

        /*
         * do dml with the two rep nodes without the arbiter.
         */
        populateDB(repEnvInfo[1].getEnv(), "db", 31, 10, null);
    }

    /**
     * Tests that an Arbiter cannot be started if there
     * is an Arbiter with the same configuration already
     * running.
     * This test exercises the code when two arbiters are
     * started with the same configuration on the same
     * jvm. An error is expected on the second attempt to
     * start the arbiter.
     */
    @Test
    public void multiArbs() {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        createGroup(2);
        arb = getReadyArbiter(ac);
        populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null);
        Exception e = null;
        arb2 = null;
        try {
            arb2 = getReadyArbiter(ac);
        } catch (UnsupportedOperationException ex) {
            e = ex;
        } finally {
            if (arb2 != null) {
                arb2.shutdown();
                arb2 = null;
            }
        }
        if (e == null) {
            fail("The second arbiter creation should have failed but didn't.");
        }
        arb.shutdown();

        arb2 = getReadyArbiter(ac);
        populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null);
        arb2.shutdown();
    }

    /**
     * Tests online rep group configuration changes.
     * The replication group is altered by adding and
     * removing replication and arbiter nodes.
     *
     * starts with rep0,rep1 and arbiter.
     * shutdown rep1 and remove from group.
     * boot new node rep2
     * add rep2 node to arbiter helper hosts
     * shutdown rep1
     * check that rep2 has become master.
     *
     * @throws Exception
     */
    @Test
    public void testReconfigureRG()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        createGroup(2);
        arb = getReadyArbiter(ac);
        /* Construct a DbGroupAdmin instance. */
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        DbGroupAdmin dbAdmin =
            useRepNetConfig ?
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets(),
                             RepTestUtils.readRepNetConfig()) :
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             master.getRepConfig().getHelperSockets());

        /* Basic ReplicationGroup test with Arbiter */
        ReplicationGroupAdmin repGroupAdmin =
            useRepNetConfig ?
                    new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                     master.getRepConfig().getHelperSockets(),
                                     RepTestUtils.readRepNetConfig()) :
                    new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                     master.getRepConfig().getHelperSockets());
        ReplicationGroup rg = repGroupAdmin.getGroup();
        Set<ReplicationNode> arbNodes = rg.getArbiterNodes();
        assertTrue(arbNodes.size() == 1);


        populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null);
        /* stop replica node */
        leaveGroupAllButMaster();
        dbAdmin.removeMember(repEnvInfo[1].getRepConfig().getNodeName());

        /* boot new replica node */
        restartNodes(fiveSec, repEnvInfo[2]);
        ReplicationConfig rc3 = repEnvInfo[2].getRepConfig();
        ArbiterMutableConfig amc = arb.getArbiterMutableConfig();
        amc.setHelperHosts(rc.getNodeHostPort() + "," + rc3.getNodeHostPort());
        arb.setArbiterMutableConfig(amc);
        populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);

        /* stop master and wait for new node to become elected */
        closeMaster();
        RepEnvInfo newmaster = waitForMaster(repEnvInfo[2]);
        populateDB(repEnvInfo[2].getEnv(), "db", 21, 10, null);
        arb.shutdown();
        restartNodes(fiveSec, repEnvInfo[0]);
        newmaster = waitForMaster(repEnvInfo[2]);
        populateDB(newmaster.getEnv(), "db", 21, 10, null);
        dbAdmin.removeMember(ac.getNodeName());

        boolean removeFailed = false;
        try {
            dbAdmin.removeMember(ac.getNodeName());
        } catch (Exception e) {
            removeFailed = true;
        }
        if (!removeFailed) {
            fail("the remove of the arbiter should have failed.");
        }
    }


    /**
     * Tests that the Arbiter can be used as the port in order to
     * create a ReplicationGroupAdmin object.
     *
     * @throws Exception
     */
    @Test
    public void testRepGroupAdmin()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        createGroup(2);
        arb = getReadyArbiter(ac);

        Set<InetSocketAddress> arbAddr = HostPortPair.getSockets(nodeHostPort);

        /* Construct a DbGroupAdmin instance. */
        DbGroupAdmin dbAdmin =
            useRepNetConfig ?
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             arbAddr,
                             RepTestUtils.readRepNetConfig()) :
            new DbGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                             arbAddr);

        /* Basic ReplicationGroup test with Arbiter */
        ReplicationGroupAdmin repGroupAdmin =
            useRepNetConfig ?
                new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                          arbAddr,
                                          RepTestUtils.readRepNetConfig()) :
                new ReplicationGroupAdmin(RepTestUtils.TEST_REP_GROUP_NAME,
                                              arbAddr);
        ReplicationGroup rg = repGroupAdmin.getGroup();
        Set<ReplicationNode> arbNodes = rg.getArbiterNodes();
        assertTrue(arbNodes.size() == 1);
        leaveGroupAllButMaster();
        dbAdmin.removeMember(repEnvInfo[1].getRepConfig().getNodeName());
    }

    /**
     * Tests Arbiter configured with SSL.
     *
     */
    @Test
    public void testArbiterSSL()
        throws Exception {

        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();

        /* Try setting to ssl in the config. */
        Properties sslProps = new Properties();
        RepTestUtils.setUnitTestSSLProperties(sslProps);
        ReplicationSSLConfig repSSLConfig = new ReplicationSSLConfig(sslProps);
        repSSLConfig.setSSLKeyStorePasswordSource(null);
        rc.setRepNetConfig(repSSLConfig);
        rc2.setRepNetConfig(repSSLConfig);
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(tenSec, TimeUnit.MILLISECONDS);
        ac.setRepNetConfig(repSSLConfig);
        createGroup(2);
        arb = getReadyArbiter(ac);
        populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null);
        /* stop master and wait for new node to become elected */
        closeMaster();
        RepEnvInfo newmaster = waitForMaster(repEnvInfo[1]);
        populateDB(newmaster.getEnv(), "db", 21, 10, null);
        restartNodes(repEnvInfo[0]);
        populateDB(newmaster.getEnv(), "db", 31, 10, null);
        arb.shutdown();
        populateDB(newmaster.getEnv(), "db", 41, 10, null);
    }

    /**
     * Test Arbiter logging is mutable.
     * @throws Exception
     */
    @Test
    public void testArbiterLogging()
        throws Exception {
        ReplicationConfig rc = repEnvInfo[0].getRepConfig();
        ReplicationConfig rc2 = repEnvInfo[1].getRepConfig();
        int port = rc2.getNodePort() + portOffset;
        String nodeName = "arbiter_"+rc.getNodeName();
        String nodeHostPort =
        (rc.getNodeHostname()+ ":" + port);

        ArbiterConfig ac = new ArbiterConfig();
        File arbHome =
            new File(envRoot.getAbsolutePath()+File.separator+ "arb");
        if (!arbHome.exists()) {
            arbHome.mkdir();
        }
        ac.setArbiterHome(arbHome.getAbsolutePath());
        ac.setGroupName(rc.getGroupName());
        ac.setHelperHosts(rc.getNodeHostPort()+","+rc2.getNodeHostPort());
        ac.setNodeHostPort(nodeHostPort);
        ac.setNodeName(nodeName);
        ac.setUnknownStateTimeout(UNKN_TIMEOUT, TimeUnit.MILLISECONDS);

        Thread testthread = new Thread(new Runnable() {
            @Override
            public void run() {
              createGroup(2);
              waitForPhase(1);
              populateDB(repEnvInfo[0].getEnv(), "db", 0, 10, null );
              leaveGroupAllButMaster();
              populateDB(repEnvInfo[0].getEnv(), "db", 11, 10, null);
              phase++;
            }
        });

        try {
            Logger parent = Logger.getLogger("com.sleepycat.je");
            parent.setLevel(Level.FINE);
            testthread.start();
            arb = getReadyArbiter(ac);
            ArbiterMutableConfig amc = arb.getArbiterMutableConfig();
            amc.setFileLoggingLevel(Level.FINE.getName());
            arb.setArbiterMutableConfig(amc);
            phase++;
            waitForPhase(2);
        } finally {
            arb.shutdown();
        }
        File logFile =
            new File(arbHome.getAbsolutePath() + File.separator +
                     "je.info.0");
        if (!checkFile(logFile, "FINE")) {
            fail("The je.info.0 file does not have FINE logging messages");
        }
    }

    private boolean checkFile(File in,
                              String valueToLookFor)
        throws Exception {
        boolean found = false;
        String curRow;
        BufferedReader fr = new BufferedReader(new FileReader(in));
        try {
            while ((curRow = fr.readLine()) != null) {
                if (curRow.indexOf(valueToLookFor) >= 0) {
                    found = true;
                    break;
                }
            }
        } finally {
            fr.close();
        }
        return found;
    }

    private boolean lookForEvent(String nodeName,
                                 LeaveReason reason,
                                 ArrayList<Object> events) {
        for (int i = events.size(); i > 0; i--) {
            if (events.get(i - 1) instanceof LeaveGroupEvent) {
                LeaveGroupEvent lge = (LeaveGroupEvent)events.get(i - 1);
                if (nodeName.equals(lge.getNodeName()) &&
                    reason.equals(lge.getLeaveReason())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void waitForReplica(Arbiter an, long timeout)
        throws RuntimeException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + timeout) {
            State anState = an.getState();
            if (anState == State.REPLICA || anState == State.DETACHED) {
                return;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException  e) {

            }
        }
        throw new RuntimeException("Timed out waiting for Arbiter " +
                                   "to become a replica.");
    }

    private Arbiter getReadyArbiter(ArbiterConfig ac) {
        Arbiter arb = new Arbiter(ac);
        try {
            waitForReplica(arb, ARB_SETUP_TIMEOUT);
        } catch (RuntimeException e) {
            arb.shutdown();
            throw e;
        }
        return arb;
    }

    class TestListener implements MonitorChangeListener {
        private final ArrayList<Object> events = new ArrayList<Object>();
        @Override
        public void notify(NewMasterEvent newMasterEvent) {
            events.add(newMasterEvent);
        }
        @Override
        public void notify(GroupChangeEvent groupChangeEvent) {
            events.add(groupChangeEvent);
        }

        @Override
        public void notify(JoinGroupEvent joinGroupEvent) {
            events.add(joinGroupEvent);
        }

        @Override
        public void notify(LeaveGroupEvent leaveGroupEvent) {
            events.add(leaveGroupEvent);
        }

        public ArrayList<Object> getEvents() {
            return new ArrayList<Object>(events);
        }
    }

    class PopulateDeleteRunnable implements Runnable {
        private final int preCondition;
        private final int startVal;
        private final int nVals;
        private final ReplicatedEnvironment repEnv;
        private final TransactionConfig txnCfg;

        PopulateDeleteRunnable(ReplicatedEnvironment repEnv,
                               int preCondition,
                               int startVal,
                               int nVals) {
            this.preCondition = preCondition;
            this.startVal = startVal;
            this.nVals = nVals;
            this.repEnv = repEnv;
            txnCfg = new TransactionConfig();
            Durability d =
                new Durability(SyncPolicy.NO_SYNC,      // localSync
                               SyncPolicy.NO_SYNC,      // replicaSync
                               ReplicaAckPolicy.SIMPLE_MAJORITY);
            txnCfg.setDurability(d);
        }

        @Override
        public void run() {
            waitForPhase(preCondition);

            for (int i = 0; i < 20; i++) {
                boolean done = false;
                while(!done) {
                    try {
                        if ( (i % 2) == 0) {
                            populateDB(repEnv, "db", startVal, nVals, txnCfg);
                        } else {
                            deleteDB(repEnv, "db", startVal, nVals, txnCfg);
                        }
                    } catch (LockConflictException timeout) {
                        continue;
                    }
                    done = true;
                }
            }
            phase++;
        }
    }

    class InsertRunnable implements Runnable {
        private final int postCondition;
        private final int startVal;
        private final int nVals;
        private final ReplicatedEnvironment repEnv;
        private final TransactionConfig txnCfg;
        private final String dbname;

        InsertRunnable(ReplicatedEnvironment repEnv,
                       String dbname,
                       int postCondition,
                       int startVal,
                       int nVals) {
            this.postCondition = postCondition;
            this.startVal = startVal;
            this.nVals = nVals;
            this.repEnv = repEnv;
            this.dbname = dbname;
            txnCfg = new TransactionConfig();
            Durability d =
                new Durability(SyncPolicy.NO_SYNC,      // localSync
                               SyncPolicy.NO_SYNC,      // replicaSync
                               ReplicaAckPolicy.SIMPLE_MAJORITY);
            txnCfg.setDurability(d);
        }

        @Override
        public void run() {
            int insertCount = 0;
            Database db = null;
            Transaction txn = null;
            DatabaseEntry key = new DatabaseEntry(new byte[]{1});
            try {
                db = repEnv.openDatabase(null, dbname, dbconfig);
                boolean done = false;
                while(!done) {
                    insertCount++;
                    txn = repEnv.beginTransaction(null, null);
                    int startKey = insertCount % nVals + startVal;
                    IntegerBinding.intToEntry(startKey, key);
                    LongBinding.longToEntry(insertCount, data);
                    db.put(txn, key, data);
                    txn.commit();
                    txn = null;
                    if (phase >= postCondition) {
                        done = true;
                    }
                }
            } catch (DatabaseException de) {
                fail("Insert thread got exception "+de);
            }
            finally {
                if (txn != null) {
                    txn.abort();
                }
                if (db != null) {
                    db.close();
                }
            }
            phase++;
        }
    }

    class ArbiterRunnable implements Runnable {
        ArbiterConfig ac;
        Arbiter arbiter;
        volatile boolean shutdown = false;
        ArbiterRunnable(ArbiterConfig ac) {
            this.ac = ac;
        }
        @Override
        public void run() {
            arbiter = getReadyArbiter(ac);
            while (!shutdown) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                }
            }
        }

        public void shutdown() {
            if (arbiter != null) {
                arbiter.shutdown();
                arbiter = null;
                shutdown = true;
            }
        }

    }

    private void waitForPhase(int phaseToWaitFor) {
        if (phase == PHASE_ERROR) {
            throw new RuntimeException("Thread encountered phase error.");
        }
        int numberOfWaits = 0;
        while (phase < phaseToWaitFor) {
            try {
               Thread.sleep(1000);
               if (phase == PHASE_ERROR) {
                   throw new RuntimeException(
                                 "Thread encountered phase error.");
               }
               numberOfWaits++;
               if (numberOfWaits > MAX_PHASE_WAITS) {
                   phase = PHASE_ERROR;
                   throw new RuntimeException("Test failed due to phase " +
                       "timeout. Waiting for phase " + phaseToWaitFor +
                       " current phase " + phase +"." +
                       LoggerUtils.getStackTrace());
               }
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted sleep.");
            }
        }
    }

    private RepEnvInfo waitForMaster(RepEnvInfo... nodes) {
        if (phase == PHASE_ERROR) {
            throw new RuntimeException(
                "Thread encountered phase error waiting for master.");
        }
        int numberOfWaits = 0;
        while (true) {
            for (RepEnvInfo repi : nodes) {
                if (State.MASTER.equals(repi.getEnv().getState())) {
                    return repi;
                }
            }

            numberOfWaits++;
            if (numberOfWaits > MAX_PHASE_WAITS) {
                phase = PHASE_ERROR;
                throw new RuntimeException("Test failed due to timeout " +
                    "waiting for a master." + LoggerUtils.getStackTrace());
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted sleep.");
            }
        }
    }

    private void closeMaster()
            throws DatabaseException {

            for (RepEnvInfo repi : repEnvInfo) {
                if (repi.getEnv() == null) {
                    continue;
                }
                if (!repi.getEnv().isValid()) {
                    continue;
                }
                 if (State.MASTER.equals(repi.getEnv().getState())) {
                    repi.closeEnv();
                }
            }
        }

    protected CommitToken deleteDB(ReplicatedEnvironment rep,
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
                db.delete(txn, key);
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

    class DelayCommit extends TestHookAdapter<Message> {
        private final long delayTime;
        private final AtomicInteger flag;

        DelayCommit(long delayTime,
                    AtomicInteger flag) {
            this.delayTime = delayTime;
            this.flag = flag;
        }

        @Override
        public void doHook(Message m) {
            if (flag.get() == 1 && m.getOp() == Protocol.COMMIT) {
                try {
                   Thread.sleep(delayTime);
                } catch (Exception e)
                {
                }
            }
        }
    }

}
