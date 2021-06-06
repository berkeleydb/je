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
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Bring up a number of nodes with a fixed master and n clients. Perform
 * basic operations, test for correctness.
 */
public class ReplayWithBinDeltaInsertionsTest extends TestBase {

    private static final int ONE_MB = 1 << 20;

    private final boolean verbose = Boolean.getBoolean("VERBOSE");
    private static final String TEST_DB = "testdb";

    /* Replication tests use multiple environments. */
    private final File envRoot;
    private final int nNodes;

    public ReplayWithBinDeltaInsertionsTest() {
        envRoot = SharedTestUtils.getTestDir();
        nNodes = 3;
        customName = nNodes + "nodes";
    }

    /**
     * Create n nodes, startup.
     * - do some work, verify that all nodes have the same data.
     * - switch masters
     * - do more work, verify that all nodes have the same data.
     * - switch masters
     *   etc
     */
    @Test
    public void testRoundRobinMasters()
        throws Exception {

        RepEnvInfo[] repEnvInfo = null;
        Logger logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

        try {
            /* Create a replicator for each environment directory. */
            EnvironmentConfig envConfig =
                RepTestUtils.createEnvConfig
                (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                                Durability.SyncPolicy.WRITE_NO_SYNC,
                                Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_FILE_MAX,
                 EnvironmentParams.LOG_FILE_MAX.getDefault());

            envConfig.setConfigParam(
                EnvironmentParams.MAX_MEMORY.getName(),
                new Integer(ONE_MB).toString());

            // TODO: Is this needed now that hard recovery works?
            LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);
            envConfig.setConfigParam("je.env.runCleaner", "false");

            repEnvInfo =
                RepTestUtils.setupEnvInfos(envRoot, nNodes, envConfig);

            /* Increase the ack timeout, to deal with slow test machines. */
            RepTestUtils.setConfigParam(RepParams.REPLICA_ACK_TIMEOUT, "30 s",
                                        repEnvInfo);

            /* Start all members of the group. */
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
            assert(master != null);

            /* Do work */
            int startVal = 50000;
            doWork(master, startVal);

            VLSN commitVLSN =
                RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                   repEnvInfo.length);
            RepTestUtils.checkNodeEquality(commitVLSN, verbose , repEnvInfo);

            logger.fine("--> All nodes in sync");

            /*
             * Round robin through the group, letting each one have a turn
             * as the master.
             */
            for (int i = 0; i < nNodes; i++) {
                /*
                 * Shut just under a quorum of the nodes. Let the remaining
                 * nodes vote, and then do some work. Then bring
                 * the rest of the group back in a staggered fashion. Check for
                 * consistency among the entire group.
                 */
                logger.fine("--> Shutting down, oldMaster=" +
                            master.getNodeName());
                int activeNodes =
                    shutdownAllButQuorum(logger,
                                         repEnvInfo,
                                         RepInternal.getNodeId(master));

                master = RepTestUtils.openRepEnvsJoin(repEnvInfo);

                assertNotNull(master);
                logger.fine("--> New master = " +  master.getNodeName());

                if (i == 0) {
                    startVal = 0;
                } else if (i == 1) {
                    startVal = 70000;
                } else if (i == 2) {
                    startVal = 30000;
                } else {
                    assert(false);
                }

                /*
                 * This test is very timing dependent, so
                 * InsufficientReplicasException is allowed.
                 */
                int retries = 5;
                for (int retry = 0;; retry++) {
                    try{
                        doWork(master, startVal);
                        break;
                    } catch (InsufficientReplicasException e) {
                        if (retry >= retries) {
                            throw e;
                        }
                    }
                }

                /* Re-open the closed nodes and have them re-join the group. */
                logger.fine("--> Before closed nodes rejoin");
                ReplicatedEnvironment newMaster =
                    RepTestUtils.joinGroup(repEnvInfo);

                assertEquals("Round " + i +
                             " expected master to stay unchanged. ",
                             master.getNodeName(),
                             newMaster.getNodeName());
                VLSN vlsn =
                    RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                       activeNodes);
                RepTestUtils.checkNodeEquality(vlsn, verbose, repEnvInfo);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    private int shutdownAllButQuorum(Logger logger,
                                     RepEnvInfo[] replicators,
                                     int currentMasterId)
        throws DatabaseException, InterruptedException {

        /*
         * Shut all but a quorum of the nodes. Make sure that the master
         * is one of the shut down nodes.
         */
        int nShutdown = replicators.length -
                        RepTestUtils.getQuorumSize(replicators.length);

        /* Start by shutting down the master. */
        int shutdownIdx = currentMasterId - 1;
        int numSyncNodes = 0;
        for (RepEnvInfo ri : replicators) {
            if (ri.getEnv() != null) {
                numSyncNodes ++;
            }
        }

        RepTestUtils.syncGroupToLastCommit(replicators, numSyncNodes);
        while (nShutdown > 0) {
            logger.fine("Closing node " + (shutdownIdx+1));
            replicators[shutdownIdx].closeEnv();
            nShutdown--;
            shutdownIdx++;
            if (shutdownIdx == replicators.length) {
                shutdownIdx = 0;
            }

            /*
             * It is possible that, after shutting down the master, the new
             * selected master is also shut immediately, which will cause
             * InsufficientReplicasException. This check will largely avoid
             * such unexpected case.
             */
            if (replicators[shutdownIdx].isMaster()) {
                shutdownIdx++;
                if (shutdownIdx == replicators.length) {
                    shutdownIdx = 0;
                }
            }
        }
        return replicators.length - nShutdown;
    }

    private void doWork(ReplicatedEnvironment master, int startVal)
        throws DatabaseException {

        /* Now do some work. */
        Database testDb = openTestDb(master);
        insertData(testDb, startVal, startVal + 5000);
        modifyData(testDb, startVal + 2, startVal + 300);
        insertData(testDb, startVal + 5001, startVal + 8000);
        deleteData(testDb, startVal + 30, startVal + 4000);
        insertData(testDb, startVal + 8001, startVal + 9000);
        modifyData(testDb, startVal + 7500, startVal + 8500);
        deleteData(testDb, startVal + 8500, startVal + 9000);
        insertData(testDb, startVal + 8501, startVal + 9000);

        testDb.close();
    }

    /*
     * Create a database on the master.
     */
    private Database openTestDb(ReplicatedEnvironment master)
        throws DatabaseException {

        Environment env = master;
        DatabaseConfig config = new DatabaseConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        config.setSortedDuplicates(true);
        Database testDb = env.openDatabase(null, TEST_DB, config);
        return testDb;
    }

    private void insertData(Database testDb,
                            int startVal,
                            int endVal)
        throws DatabaseException {

        DatabaseEntry val = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[1024]);
        for (int i = startVal; i < endVal; i++) {
            IntegerBinding.intToEntry(i, val);
            assertEquals(OperationStatus.SUCCESS,
                         testDb.put(null, val /*key*/, data /*data*/));
        }
    }

    private void modifyData(Database testDb,
                            int startVal,
                            int endVal)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry newDataVal = new DatabaseEntry();
        for (int i = startVal; i < endVal; i++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i+1, newDataVal);
            assertEquals(OperationStatus.SUCCESS,
                         testDb.put(null, key, newDataVal));
        }
    }

    private void deleteData(Database testDb,
                            int startVal,
                            int endVal)
        throws DatabaseException {

        DatabaseEntry val = new DatabaseEntry();
        for (int i = startVal; i < endVal; i++) {
            IntegerBinding.intToEntry(i, val);
            assertEquals(OperationStatus.SUCCESS,
                         testDb.delete(null, val /*key*/));
        }
    }

}
