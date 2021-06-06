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
package com.sleepycat.je.rep.txn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.rep.LogOverwriteException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.txn.RollbackWorkload.DatabaseOpsStraddlesMatchpoint;
import com.sleepycat.je.rep.txn.RollbackWorkload.IncompleteTxnAfterMatchpoint;
import com.sleepycat.je.rep.txn.RollbackWorkload.IncompleteTxnBeforeMatchpoint;
import com.sleepycat.je.rep.txn.RollbackWorkload.IncompleteTxnStraddlesMatchpoint;
import com.sleepycat.je.rep.txn.RollbackWorkload.RemoveDatabaseAfterRollback;
import com.sleepycat.je.rep.txn.RollbackWorkload.RemoveSomeDatabasesAfterRollback;
import com.sleepycat.je.rep.txn.RollbackWorkload.SteadyWork;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test that a replica can rollback an replay active txn for syncup.
 * Test cases:
 * - Replay txn has only logged log entries that follow the syncup matchpoint
 * and must be entirely rolled back.
 *
 * - Replay txn has only logged log entries that precede the syncup matchpoint
 * and doesn not have to be rolled back at all.
 *
 * - Replay txn has logged log entries that both precede and follow the
 *   syncup matchpoint, and the txn must be partially rolled back.
 *
 * TRY: master fails
 *      replica fails
 *
 * The txn should have
 * - inserts
 * - delete
 * - reuse of a BIN slot
 * - intermediate versions within the same txn (the same record is modified
 * multiple times within the txn.
 */
public class RollbackTest extends TestBase {

    private final Logger logger;
    private final boolean verbose = Boolean.getBoolean("verbose");

    /* Replication tests use multiple environments. */
    private final File envRoot;

    public RollbackTest() {
        envRoot = SharedTestUtils.getTestDir();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");
    }

    @Override
    public void setUp() 
        throws Exception {
        super.setUp();
        RepTestUtils.removeRepEnvironments(envRoot);
    }

    @Override
    public void tearDown() {
        /* Restore static var in case other tests will run in this process. */
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(false);
    }

    @Test
    public void testDbOpsRollback() 
        throws Throwable {

        try {
            masterDiesAndRejoins(new DatabaseOpsStraddlesMatchpoint());
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;        
        }
    }

    @Test
    public void testTxnEndBeforeMatchpoint()
        throws Throwable {

        masterDiesAndRejoins(new IncompleteTxnBeforeMatchpoint());
    }

    @Test
    public void testTxnEndAfterMatchpoint()
        throws Throwable {

        masterDiesAndRejoins(new IncompleteTxnAfterMatchpoint());
    }

    @Test
    public void testTxnStraddleMatchpoint()
        throws Throwable {

        masterDiesAndRejoins(new IncompleteTxnStraddlesMatchpoint());
    }

    @Test
    public void testRemoveDatabaseAfterRollback()
        throws Throwable {

        masterDiesAndRejoins(new RemoveDatabaseAfterRollback());
    }

    @Test
    public void testRemoveSomeDatabasesAfterRollback()
        throws Throwable {

        masterDiesAndRejoins(new RemoveSomeDatabasesAfterRollback());
    }

    // TODO: why all the rollbacks when the master never changes?
    @Test
    public void testReplicasFlip()
        throws Throwable {

        replicasDieAndRejoin(new SteadyWork(), 10);
    }

    /*
     * Test the API: RepImpl.setBackupProhibited would disable the DbBackup in
     * DbBackup.startBackup, may be caused by Replay.rollback().
     */
    @Test
    public void testRollingBackDbBackupAPI()
        throws Throwable {

        RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 1);
        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        RepImpl repImpl = RepInternal.getNonNullRepImpl(master);

        DbBackup backupHelper = new DbBackup(master);
        repImpl.setBackupProhibited(true);

        try {
            backupHelper.startBackup();
            fail("Should throw out a LogOverwriteException here.");
        } catch (LogOverwriteException e) {
            /* Expect a LogOverwriteException here. */
        }

        repImpl.setBackupProhibited(false);
        try {
            backupHelper.startBackup();
            backupHelper.endBackup();
        } catch (Exception e) {
            fail("Shouldn't get an exception here.");
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /*
     * Test the API: RepImpl.invalidateDbBackups would disable the DbBackup
     * at endBackup, may be caused by Replay.rollback().
     */
    @Test
    public void testRollBackInvalidateDbBackup()
        throws Exception {

        RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 1);
        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        final RepImpl repImpl = RepInternal.getNonNullRepImpl(master);

        DbBackup backupHelper = new DbBackup(master);
        backupHelper.startBackup();

        backupHelper.setTestHook(new TestHook<Object>() {
            public void doHook() {
                repImpl.invalidateBackups(8L);
            }

            public Object getHookValue() {
                throw new UnsupportedOperationException();
            }

            public void doIOHook() {
                throw new UnsupportedOperationException();
            }

            public void hookSetup() {
                throw new UnsupportedOperationException();
            }

            public void doHook(Object obj) {
                throw new UnsupportedOperationException();                
            }
        });

        try {
            backupHelper.endBackup();
            fail("Should throw out a LogOverwriteException here.");
        } catch (LogOverwriteException e) {
            /* Expect to get a LogOverwriteException here. */
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /**
     * Create 3 nodes and replicate operations.
     * Kill off the master, and make the other two resume. This will require
     * a syncup and a rollback of any operations after the matchpoint.
     */
    private void masterDiesAndRejoins(RollbackWorkload workload)
        throws Throwable {

        RepEnvInfo[] repEnvInfo = null;

        try {
            /* Create a  3 node group */
            repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
            logger.severe("master=" + master);

            /* Run a workload against the master. */
            workload.beforeMasterCrash(master);

            /*
             * Disable group DB updates from this point onward, to ensure they
             * don't interfere with rollbacks and so we can easily check for an
             * uncommitted txn at the end of the log (this is done by
             * ensureDistinctLastAndSyncVLSN).
             */
            LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);

            /*
             * Sync up the group and check that all nodes have the same
             * contents. This first workload must end with in-progress,
             * uncommitted transactions.
             */
            VLSN lastVLSN = VLSN.NULL_VLSN;
            if (workload.noLockConflict()) {
                lastVLSN = checkIfWholeGroupInSync(master, repEnvInfo,
                                                    workload);
            }

            /*
             * Crash the master, find a new master.
             */
            RepEnvInfo oldMaster =
                repEnvInfo[RepInternal.getNodeId(master) - 1];
            master = crashMasterAndElectNewMaster(master, repEnvInfo);
            RepEnvInfo newMaster =
                repEnvInfo[RepInternal.getNodeId(master) - 1];
            logger.severe("newmaster=" + master);
            RepEnvInfo alwaysReplica = null;
            for (RepEnvInfo info : repEnvInfo) {
                if ((info != oldMaster) && (info != newMaster)) {
                    alwaysReplica = info;
                    break;
                }
            }

            /*
             * Check that the remaining two nodes only contain committed
             * updates.
             * TODO: check that the number of group members is 2.
             */
            assertTrue(workload.containsSavedData(master));
            RepTestUtils.checkNodeEquality(lastVLSN, verbose, repEnvInfo);

            /*
             * Do some work against the new master, while the old master is
             * asleep. Note that the first workload may have contained
             * in-flight transactions, so this may result in the rollback of
             * some transactions in the first workload.
             */
            workload.afterMasterCrashBeforeResumption(master);

            /*
             * The intent of this test is that the work after crash will end on
             * an incomplete txn. Check for that.
             */
            lastVLSN = ensureDistinctLastAndSyncVLSN(master, repEnvInfo);

            /* Now bring up the old master. */
            logger.info("Bring up old master");
            oldMaster.openEnv();

            logger.info("Old master joined");
            RepTestUtils.syncGroupToVLSN(repEnvInfo, repEnvInfo.length,
                                         lastVLSN);
            logger.info("Old master synced");

            /*
             * Check that all nodes only contain committed updates.
             */
            workload.releaseDbLocks();
            assertTrue(workload.containsSavedData(master));
            RepTestUtils.checkNodeEquality(lastVLSN, verbose, repEnvInfo);

            /*
             * Now crash the node that has never been a master. Do some work
             * without it, then recover that node, then do a verification
             * check.  This exercises the recovery of a log that has syncups in
             * it.
             */
            alwaysReplica.abnormalCloseEnv();
            workload.afterReplicaCrash(master);

            lastVLSN = RepInternal.getNonNullRepImpl(master).getVLSNIndex().
                getRange().getLast();
            RepTestUtils.syncGroupToVLSN(repEnvInfo, 2, lastVLSN);
            alwaysReplica.openEnv();
            RepTestUtils.syncGroupToVLSN(repEnvInfo, 3, lastVLSN);
            assertTrue(workload.containsSavedData(master));
            RepTestUtils.checkNodeEquality(lastVLSN, verbose, repEnvInfo);
            RepTestUtils.checkUtilizationProfile(repEnvInfo);

            workload.close();

            /*
             * We're done with the test. Bringing down these replicators
             * forcibly, without closing transactions and whatnot.
             */
            for (RepEnvInfo repi : repEnvInfo) {
                repi.abnormalCloseEnv();
            }

            /*
             * Open and verify the environments one last time, to ensure that
             * rollbacks in the recovery interval don't cause problems.
             */
            master = RepTestUtils.restartGroup(repEnvInfo);
            lastVLSN = RepInternal.getNonNullRepImpl(master).getVLSNIndex().
                getRange().getLast();
            RepTestUtils.syncGroupToVLSN(repEnvInfo, 3, lastVLSN);
            RepTestUtils.checkNodeEquality(lastVLSN, verbose, repEnvInfo);

            /* Final close. */
            for (RepEnvInfo repi : repEnvInfo) {
                repi.closeEnv();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Since the master never dies in this test, no rollbacks should occur,
     * but no data should be lost either.
     *
     * TODO: Should the workload param be of a different class (not a
     * RollbackWorkload), since its masterSteadyWork method is only called
     * here?
     */
    private void replicasDieAndRejoin(RollbackWorkload workload,
                                      int numIterations)
        throws Throwable {

        RepEnvInfo[] repEnvInfo = null;

        try {
            /* Create a  3 node group. Assign identities. */
            repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
            logger.severe("master=" + master);

            RepEnvInfo replicaA = null;
            RepEnvInfo replicaB = null;

            for (RepEnvInfo info : repEnvInfo) {
                if (info.getEnv().getState().isMaster()) {
                    continue;
                }

                if (replicaA == null) {
                    replicaA = info;
                } else {
                    replicaB = info;
                }
            }

            /*
             * For the sake of easy test writing, make sure numIterations is an
             * even number.
             */
            assertTrue((numIterations % 2) == 0);
            replicaA.abnormalCloseEnv();
            for (int i = 0; i < numIterations; i++) {
                workload.masterSteadyWork(master);
                waitForReplicaToSync(master, repEnvInfo);
                if ((i % 2) == 0) {
                    flushLogAndCrash(replicaB);
                    replicaA.openEnv();
                } else {
                    flushLogAndCrash(replicaA);
                    replicaB.openEnv();
                }
                waitForReplicaToSync(master, repEnvInfo);
            }
            replicaA.openEnv();

            VLSN lastVLSN =
                RepInternal.getNonNullRepImpl(master).getVLSNIndex().
                getRange().getLast();
            RepTestUtils.syncGroupToVLSN(repEnvInfo,
                                         repEnvInfo.length,
                                         lastVLSN);

            assertTrue(workload.containsAllData(master));
            RepTestUtils.checkNodeEquality(lastVLSN, verbose, repEnvInfo);

            workload.close();
            for (RepEnvInfo repi : repEnvInfo) {
                /*
                 * We're done with the test. Bringing down these replicators
                 * forcibly, without closing transactions and whatnot.
                 */
                repi.abnormalCloseEnv();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void flushLogAndCrash(RepEnvInfo replica) {
        DbInternal.getNonNullEnvImpl(replica.getEnv()).
            getLogManager().flushSync();
        replica.abnormalCloseEnv();
    }

    /**
     * Syncup the group and check for these requirements:
     *  - the master has all the data we expect
     *  - the replicas have all the data that is on the master.

     *  - the last VLSN is not a sync VLSN. We want to ensure that the
     * matchpoint is not the last VLSN, so the test will need to do rollback
     * @throws InterruptedException
     * @return lastVLSN on the master
     */
    private VLSN checkIfWholeGroupInSync(ReplicatedEnvironment master,
                                         RepEnvInfo[] repEnvInfo,
                                         RollbackWorkload workload)
        throws InterruptedException {

        /*
         * Make sure we're testing partial rollbacks, and that the replication
         * stream is poised at a place where the last sync VLSN != lastVLSN.
         */
        VLSN lastVLSN = ensureDistinctLastAndSyncVLSN(master, repEnvInfo);

        RepTestUtils.syncGroupToVLSN(repEnvInfo, repEnvInfo.length, lastVLSN);

        /*
         * All nodes in the group should have the same data, and it should
         * consist of committed and uncommitted updates.
         */
        assertTrue(workload.containsAllData(master));

        RepTestUtils.checkNodeEquality(lastVLSN, verbose, repEnvInfo);

        lastVLSN = ensureDistinctLastAndSyncVLSN(master, repEnvInfo);

        return lastVLSN;
    }

    /* Just check if the replica is in sync. */
    private void waitForReplicaToSync(ReplicatedEnvironment master,
                                      RepEnvInfo[] repEnvInfo)
        throws InterruptedException {

        VLSN lastVLSN = RepInternal.getNonNullRepImpl(master).getVLSNIndex().
            getRange().getLast();
        RepTestUtils.syncGroupToVLSN(repEnvInfo, 2, lastVLSN);
    }

    /**
     * Crash the current master, and wait until the group elects a new one.
     */
    private ReplicatedEnvironment
        crashMasterAndElectNewMaster(ReplicatedEnvironment master,
                                     RepEnvInfo[] repEnvInfo) {

        int masterIndex = RepInternal.getNodeId(master) - 1;

        logger.info("Crashing " + master.getNodeName());
        repEnvInfo[masterIndex].abnormalCloseEnv();

        logger.info("Rejoining");
        ReplicatedEnvironment newMaster =
            RepTestUtils.openRepEnvsJoin(repEnvInfo);

        logger.info("New master = " + newMaster.getNodeName());
        return newMaster;
    }

    /**
     * In this test, we often want to check that the last item in the
     * replicated stream is not a matchpoint candidate (that VLSNRange.lastVLSN
     * != VLSNRange.lastSync) There's nothing wrong intrinsically with that
     * being so, it's just that this test is trying to ensure that we test
     * partial rollbacks.
     * @return lastVLSN
     * @throws InterruptedException
     */
    private VLSN ensureDistinctLastAndSyncVLSN(ReplicatedEnvironment master,
                                               RepEnvInfo[] repEnvInfo)
        throws InterruptedException {

        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(master).getVLSNIndex();
        VLSNRange range = vlsnIndex.getRange();
        VLSN lastVLSN = range.getLast();
        VLSN syncVLSN = range.getLastSync();
        if (lastVLSN.equals(syncVLSN)) {
            master.flushLog(false); // for debugging using the data log
            fail("lastVLSN = " + lastVLSN + " syncVLSN = " + syncVLSN);
        }
        return lastVLSN;
    }
}
