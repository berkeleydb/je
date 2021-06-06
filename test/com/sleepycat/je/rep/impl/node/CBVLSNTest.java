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
package com.sleepycat.je.rep.impl.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.cbvlsn.GlobalCBVLSN;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.VLSN;

import org.junit.Before;
import org.junit.Test;

/**
 * Because the global CBVLSN is defunct as of JE 7.5, this test is only
 * maintained to exercise support for groups containing older nodes that rely
 * on the global CBVLSN. This test artificially enables CBVLSN maintenance.
 * See {@link GlobalCBVLSN}.
 */
public class CBVLSNTest extends RepTestBase {

    /* enable debugging printlns with -Dverbose=true */
    private final boolean verbose = Boolean.getBoolean("verbose");

    /**
     * Don't rely on RepTestBase setup, because we want to specify the
     * creation of the environment config.
     */
    @Before
    public void setUp()
        throws Exception {

        RepTestUtils.removeRepEnvironments(envRoot);
        dbconfig = new DatabaseConfig();
        dbconfig.setAllowCreate(true);
        dbconfig.setTransactional(true);
        dbconfig.setSortedDuplicates(false);
    }

    private void setupConfig(boolean allowCleaning) {

        for (RepEnvInfo i : repEnvInfo) {

            /* Enable maintenance of CBVLSN, regardless of JE version. */
            i.getRepConfig().setConfigParam(
                RepParams.TEST_CBVLSN.getName(), "true");

            /*
             * Disable MIN_RETAINED_VLSNS so that the group/global CBVLSN will
             * advance without regard to the number of records written.
             */
            i.getRepConfig().setConfigParam(
                RepParams.MIN_RETAINED_VLSNS.getName(), "0");

            /*
             * Turn off the cleaner because this test is doing scans of the log
             * file for test purposes, and those scans are not coordinated with
             * the cleaner.
             */
            if (!allowCleaning) {
                i.getEnvConfig().setConfigParam(
                    EnvironmentConfig.ENV_RUN_CLEANER, "false");
            }
        }
    }

    @Test
    public void testBasic()
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        File[] dirs = RepTestUtils.makeRepEnvDirs(envRoot, groupSize);
        repEnvInfo = new RepEnvInfo[groupSize];
        for (int i = 0; i < groupSize; i++) {

            /*
             * Mimic the default (variable per-env) log file sizing behavior
             * that would occur when this test case is the first one to be
             * executed.  (Since the order of execution is unpredictable, it's
             * unsound to rely on getting this behavior without explicitly
             * coding it here.)  It makes replicas have larger log files than
             * the master.  With five nodes, it tends to mean that the last
             * replica is still using a single log file, and its local CBVLSN
             * lags behind, and gates the global CBVLSN advancement.
             */ 
            long size = (i + 2) * 10000;
            EnvironmentConfig ec = envConfig.clone();
            DbInternal.disableParameterValidation(ec);
            ec.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                              Long.toString(size));
            ReplicationConfig rc =
                RepTestUtils.createRepConfig(i + 1);
            repEnvInfo[i] =
                RepTestUtils.setupEnvInfo(dirs[i],
                                          ec,
                                          rc,
                                          repEnvInfo[0]);
        }

        setupConfig(false /*allowCleaning*/);

        checkCBVLSNs(1000, 2);
    }

    /* 
     * Because this test case sets a very small log file size,
     * as a result, the GlobalCBVLSN would advance when the whole group 
     * starts up and an InsufficientLogException would be thrown when a node
     * tries to join the group at the start up, which is unexpected.
     *
     * To avoid this, disable the LocalCBVLSN updates on the master while 
     * starting up the group and enable it after the whole group is up.
     */
    @Test
    public void testSmallFiles()
        throws Exception {

        /*
         * Use uniformly small log files, which means that local CBVLSNs will
         * advance frequently, and the global CBVLSN will advance.
         */
        EnvironmentConfig smallFileConfig = new EnvironmentConfig();
        DbInternal.disableParameterValidation(smallFileConfig);
        smallFileConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000");
        smallFileConfig.setAllowCreate(true);
        smallFileConfig.setTransactional(true);

        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot,
                                                groupSize,
                                                smallFileConfig);

        setupConfig(false /*allowCleaning*/);

        checkCBVLSNs(60, 5, true);
    }

    private void checkCBVLSNs(int numRecords,
                              int filesToUse)
        throws Exception {

        checkCBVLSNs(numRecords, filesToUse, false);
    }

    private void checkCBVLSNs(int numRecords,
                              int filesToUse,
                              boolean disableLocalCBVLSNUpdate)
        throws Exception {

        if (!disableLocalCBVLSNUpdate) {
            RepTestUtils.joinGroup(repEnvInfo);
        } else {
            /* Open the master. */
            repEnvInfo[0].openEnv();
            /* Disable the LocalCBVLSN updates on the master. */
            RepInternal.getNonNullRepImpl(repEnvInfo[0].getEnv()).getRepNode().
                getCBVLSNTracker().setAllowUpdate(false);

            /* 
             * Open other replicas one by one, so that no 
             * InsufficientLogException will be thrown.
             */
            for (int i = 1; i < repEnvInfo.length; i++) {
                repEnvInfo[i].openEnv();
            }

            /* Enable the LocalCBVLSN updates again. */
            RepInternal.getNonNullRepImpl(repEnvInfo[0].getEnv()).getRepNode().
                getCBVLSNTracker().setAllowUpdate(true);
        }

        /* Master is the first node. */
        ReplicatedEnvironment mRepEnv = repEnvInfo[0].getEnv();
        RepImpl mRepImpl = repEnvInfo[0].getRepImpl();
        assertTrue(mRepEnv.getState().isMaster());

        Database db = mRepEnv.openDatabase(null, TEST_DB_NAME, dbconfig);

        try {

            /*
             * Do work on the master, and check that its local CBVLSN advances.
             */
            VLSN initialCBVLSN = workAndCheckLocalCBVLSN(
                mRepEnv, db, RepTestUtils.SYNC_SYNC_ALL_TC,
                numRecords, filesToUse);

            /* Make sure that all the replicas send in their local CBVLSNs. */
            doGroupWideChecks(
                mRepEnv, false /*oneStalledReplica*/, initialCBVLSN);

            /*
             * Crash one node, and resume execution. Make sure that the dead
             * node holds back the global CBVLSN. Run this set with quorum
             * acks.
             */
            if (verbose) {
                System.out.println("crash one node");
            }
            RepEnvInfo killInfo = repEnvInfo[repEnvInfo.length - 1];
            int killNodeId = killInfo.getRepNode().getNodeId();
            killInfo.getEnv().close();

            /*
             * The last CBVLSN received on the master from the dead replica
             * will (eventually) become the new group CBVLSN.
             */
            VLSN deadReplicaCBVLSN =
                mRepImpl.getRepNode().getGroup().getMember(killNodeId).
                getBarrierState().getLastCBVLSN();

            assertTrue(deadReplicaCBVLSN.compareTo(initialCBVLSN) > 0);

            TransactionConfig tConfig = new TransactionConfig();
            tConfig.setDurability(
                new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC,
                                ReplicaAckPolicy.SIMPLE_MAJORITY));

            workAndCheckLocalCBVLSN(
                mRepEnv, db, tConfig, numRecords, filesToUse);

            if (verbose) {
                System.out.println("group wide check");
            }

            doGroupWideChecks(
                mRepEnv, true /*oneStalledReplica*/, deadReplicaCBVLSN);

        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.close();
        }
    }

    /**
     * @return a VLSN beyond which the group CBVLSN should advance.  We return
     * the first VLSN written on the master, since we know that we'll write at
     * least one file after that (on all nodes).
     */
    private VLSN workAndCheckLocalCBVLSN(ReplicatedEnvironment mRepEnv,
                                         Database db,
                                         TransactionConfig tConfig,
                                         int numRecords,
                                         int numFilesToUse)
        throws InterruptedException {

        RepImpl mRepImpl = RepInternal.getNonNullRepImpl(mRepEnv);
        RepNode mNode = mRepImpl.getRepNode();
        final FileManager mFileManager = mRepImpl.getFileManager();
        String mName =  mRepEnv.getNodeName();
        long logFileNum = mFileManager.getLastFileNum().longValue();
        long startFileNum = logFileNum;
        VLSN barrierVLSN =
            mNode.getGroup().getMember(mName).getBarrierState().getLastCBVLSN();
        VLSN firstWrittenVLSN = VLSN.NULL_VLSN;

        /*
         * Tests that every log file switch results in a local CBVLSN update on
         * the master.
         */
        int mId = mRepImpl.getNodeId();
        for (int i=0; i < numRecords; i++) {
            Transaction txn = mRepEnv.beginTransaction(null, tConfig);
            IntegerBinding.intToEntry(i, key);
            LongBinding.longToEntry(i, data);
            db.put(txn, key, data);
            txn.commit();

            if (firstWrittenVLSN.isNull()) {
                firstWrittenVLSN =
                    mNode.getCBVLSNTracker().getLastSyncableVLSN();
            }

            long newFileNum = mFileManager.getLastFileNum().longValue();
            if (logFileNum < newFileNum) {
                logFileNum = newFileNum;

                /*
                 * We should have moved up to a new local CBVLSN.
                 * localCBVLSNVal is updated directly, so it's sure to have
                 * advanced.
                 */
                VLSN newLocalCBVLSN = 
                    mNode.getCBVLSNTracker().getBroadcastCBVLSN();

                if (verbose) {
                    System.out.println(
                        "master's global CBVLSN = " + mNode.getGlobalCBVLSN() +
                        " local CBVLSN = " + newLocalCBVLSN );
                }

                if (!barrierVLSN.isNull()) {

                    /* 
                     * if the barrier vlsn is null, we'd expect the first
                     * non-null vlsn to update the localcblvlsn state, so
                     * only to this check for if we're not starting out at
                     * null.
                     */
                    String info = "newLocal=" + newLocalCBVLSN +
                        " prevLocal=" + barrierVLSN;
                    assertTrue(info, 
                               newLocalCBVLSN.compareTo(barrierVLSN) >= 0);
                }

                barrierVLSN = newLocalCBVLSN;

                /*
                 * Check that the new local CBVLSN value is in the cached group
                 * info, and the database. The group info and the database are
                 * only updated by the master when it is running in the
                 * FeederManager loop, so we will retry once if needed.
                 */
                int retries = 2;
                VLSN indbVLSN = null;
                while (retries-- > 0) {
                    mNode.refreshCachedGroup();
                    indbVLSN = mNode.getGroup().getMember(mId).
                        getBarrierState().getLastCBVLSN();

                    if (!indbVLSN.equals(newLocalCBVLSN)) {
                        /* Wait one feederManager channel polling cycle. */
                        Thread.sleep(1000);
                        continue;
                    }
                }

                assertEquals("local=" + newLocalCBVLSN +
                             " inDbVLSN=" + indbVLSN,
                             newLocalCBVLSN, indbVLSN);
            }
        }

        /*
         * Test that at least two log files worth of data have been generated,
         * so the conditional above is exercised.
         */
        assertTrue(logFileNum > (startFileNum + 1));

        /*
         * Test that we exercised a certain number of log files on the
         * master, since local CBVLSNs are broadcast at file boundaries.
         */
        assertTrue("logFileNum=" + logFileNum +
                   " startFileNum=" + startFileNum +
                   " numFilesToUse=" + numFilesToUse,
                   (logFileNum - startFileNum) >= numFilesToUse);
        if (verbose) {
            System.out.println("logFileNum = " + logFileNum +
                               " start=" + startFileNum);
        }

        return firstWrittenVLSN;
    }

    /**
     * Check replicas for local CBVLSN value consistency wrt the current
     * master. Also check that the global CBVLSN has been advancing, if
     * appropriate.
     *
     * @param oneReplicaStalled true if a replica is down and is holding up
     * the group CBVLSN advancement and it is stuck at previousCBVLSN.  Or
     * false if all nodes are up and previousCBVLSN is the group CBVLSN before
     * writes were performed.
     *
     * @throws IOException
     */
    private void doGroupWideChecks(ReplicatedEnvironment mRepEnv,
                                   boolean oneReplicaStalled,
                                   VLSN previousCBVLSN)
        throws DatabaseException, InterruptedException, IOException {

        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (!rep.isValid()) {
                continue;
            }
        }

        RepNode mNode = RepInternal.getNonNullRepImpl(mRepEnv).getRepNode();

        /* Ensure that all replicas have reasonably current syncup values. */
        int replicaCount = 0;
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (!rep.isValid()) {
                continue;
            }
            replicaCount++;

            RepNode repNode = RepInternal.getNonNullRepImpl(rep).getRepNode();
            final int heartBeatMs =
                Integer.parseInt(rep.getRepConfig().
                                 getConfigParam(RepParams.
                                                HEARTBEAT_INTERVAL.getName()));
            final int retryWaitMs = 1000;

            /*
             * Each replicator will result in a roundtrip update, thus taking
             * more time for the group as a whole to reach stasis. So increase
             * the number of retries based on the number of replicators.
             */
            int retries = (repEnvInfo.length * heartBeatMs * 2) / retryWaitMs;

            if (verbose) {
                System.out.println("start retries = " + retries);
            }

            /*
             * Check to ensure they are reasonably current at the node itself.
             */
            while (true) {
                VLSN groupCBVLSN = repNode.getGlobalCBVLSN();
                VLSN groupMasterCBVLSN = mNode.getGlobalCBVLSN();

                VLSN localVLSN =
                    repNode.getCBVLSNTracker().getLastSyncableVLSN();
                VLSN localMasterVLSN =
                    mNode.getCBVLSNTracker().getLastSyncableVLSN();

                VLSN masterBroadcastVLSN =
                    mNode.getCBVLSNTracker().getBroadcastCBVLSN();

                String info =
                    rep.getNodeName() + " retries=" + retries + " time = " +
                    String.format("%tc", System.currentTimeMillis()) +
                    " groupCBVLSN = " + groupCBVLSN +
                    " groupMasterCBVLSN = " + groupMasterCBVLSN +
                    " localVLSN = " + localVLSN +
                    " localMasterVLSN = " + localMasterVLSN +
                    " masterBroadcastVLSN = " + masterBroadcastVLSN +
                    " oneReplicaStalled = " + oneReplicaStalled +
                    " previousCBVLSN = " + previousCBVLSN;

                if (retries-- <= 0) {
                    fail(info + dumpGroups());
                }

                /*
                 * There should be group CBVLSN agreement across the entire
                 * group.
                 */
                if (verbose) {
                    System.out.println(info);
                }

                if  (!localVLSN.equals(localMasterVLSN)) {

                    /*
                     * Replica is still processing the replication stream. See
                     * if it needs to catch up.
                     */
                    Thread.sleep(retryWaitMs);
                    continue; // retry
                }

                /*
                 * Now that the replica has broadcast a local CBVLSN near the
                 * end of its log, check that everyone agrees on the global
                 * CBVLSN value.
                 */
                if (oneReplicaStalled) {

                    /*
                     * The dead replica should prevent everyone from advancing
                     * past its CBVLSN, and everyone should agree on that
                     * global VLSN (eventually).
                     */
                    if (!previousCBVLSN.equals(groupCBVLSN) ||
                        !previousCBVLSN.equals(groupMasterCBVLSN)) {
                        Thread.sleep(retryWaitMs);
                        continue; // retry
                    }
                } else {

                    /*
                     * When no replica is stalled, unfortunately we can't
                     * guarantee the local and group CBVLSNs will ever match.
                     * This is because the CBVLSN is only broadcast when the
                     * file flips, and we broadcast the VLSN from the
                     * penultimate file,
                     *
                     * Because we always write multiple files since the last
                     * known point, we can confirm that the group CBVLSN has
                     * advanced.  But we can't predict how far.
                     */
                    if (groupMasterCBVLSN.compareTo(previousCBVLSN) <= 0) {
                        Thread.sleep(retryWaitMs);
                        continue; // retry
                    }
                }
                break;
            }
        }

        /* We should have checked all the live replicas. */
        assertEquals(replicaCount,
                     (oneReplicaStalled ? repEnvInfo.length - 1 :
                      repEnvInfo.length));
    }

    private String dumpGroups() {
        StringBuilder sb = new StringBuilder();
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (!rep.isValid()) {
                continue;
            }
            dumpGroup(rep, sb);
        }
        return sb.toString();
    }

    private void dumpGroup(ReplicatedEnvironment repEnv, StringBuilder sb) {
        RepNode repNode = RepInternal.getNonNullRepImpl(repEnv).getRepNode();
        sb.append("\n").append(repEnv.getNodeName()).append(" group members:");
        for (RepNodeImpl n :  repNode.getGroup().getAllElectableMembers()) {
            sb.append("\n  ").append(n.getName());
            sb.append(" ").append(n.getBarrierState());
        }
    }

    @Test
    public void testDbUpdateSuppression()
        throws DatabaseException, InterruptedException, IOException {

        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, groupSize);
        setupConfig(false /*allowCleaning*/);

        ReplicatedEnvironment mEnv = RepTestUtils.joinGroup(repEnvInfo);
        assertEquals(ReplicatedEnvironment.State.MASTER, mEnv.getState());
        RepImpl repInternal = RepInternal.getNonNullRepImpl(mEnv);
        RepNode masterNode = repInternal.getRepNode();
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);
        final FileManager masterFM = repInternal.getFileManager();
        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

        RepGroupImpl group1 = masterNode.getGroup();
        long fileNum1 = masterFM.getLastFileNum().longValue();

        Database db = mEnv.openDatabase(null, TEST_DB_NAME, dbconfig);

        /* Force two new log files. */
        for (int i=0; true; i++) {
            IntegerBinding.intToEntry(i, key);
            LongBinding.longToEntry(i, data);
            db.put(null, key, data);
            if (masterFM.getLastFileNum().longValue() > (fileNum1+1)) {
                break;
            }
        }
        RepGroupImpl group2 = masterNode.getGroup();
        for (RepNodeImpl n1 : group1.getAllElectableMembers()) {
            RepNodeImpl n2 = group2.getMember(n1.getNodeId());
            assertEquals(n1.getBarrierState(), n2.getBarrierState());
        }
        db.close();
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(false);
    }
}
