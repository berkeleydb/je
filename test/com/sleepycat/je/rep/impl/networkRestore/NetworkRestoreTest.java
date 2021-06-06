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

package com.sleepycat.je.rep.impl.networkRestore;

import static com.sleepycat.util.test.GreaterThan.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackup.RejectedServerException;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.VLSN;

import org.junit.Test;

public class NetworkRestoreTest extends RepTestBase {

    /*
     * Repeats of network restore operations, as the master advances the log
     * with modifications during each iteration.
     */
    private static int RESTORE_CYCLES = 5;

    @Test
    public void testLogProviders()
        throws Exception {

        configureForMaxCleaning(5);
        final RepEnvInfo minfo = repEnvInfo[0];

        /* Add a secondary node */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo sInfo = repEnvInfo[repEnvInfo.length - 1];
        sInfo.getRepConfig().setNodeType(NodeType.SECONDARY);

        createGroup();
        populateDB(minfo.getEnv(), TEST_DB_NAME, 100);

        /* The node that will be use for network restores. */
        RepEnvInfo nrInfo = repEnvInfo[1];

        /* restore from master. */
        doAndCheckRestore(nrInfo, minfo);
        /* Check restore from specific Replica. */
        doAndCheckRestore(nrInfo, repEnvInfo[2]);
        /* restore from self should fail. */
        try {
            doAndCheckRestore(nrInfo, repEnvInfo[1]);
            fail("exception expected");
        } catch (EnvironmentFailureException e) {
            // Expected. Cannot restore from just yourself.
        }

        /* Restore secondary */
        doAndCheckRestore(sInfo, minfo);

        /* Restore from secondary */
        doAndCheckRestore(nrInfo, sInfo);
    }

    @Test
    public void testConfigError() {
        configureForMaxCleaning(5);
        final RepEnvInfo minfo = repEnvInfo[0];

        createGroup();
        populateDB(minfo.getEnv(), TEST_DB_NAME, 100);
        RepEnvInfo nrInfo = repEnvInfo[1];
        nrInfo.closeEnv();
        shiftVLSNRight(repEnvInfo[0].getEnv());
        try {
            setExceptionListener(nrInfo);
            nrInfo.openEnv();
            fail("exception expected");
        } catch (InsufficientLogException e) {
            NetworkRestore networkRestore = new NetworkRestore();

            final ReplicationConfig repConfig = repEnvInfo[0].getRepConfig();
            // bad node name
            repConfig.setNodeName("badname");
            ReplicationNode restoreNode = new RepNodeImpl(repConfig);
            final NetworkRestoreConfig config = new NetworkRestoreConfig();
            config.setLogProviders(Arrays.asList(restoreNode));
            try {
                networkRestore.execute(e, config);
                fail("exception expected");
            } catch (IllegalArgumentException iae) {
                // Expected
            }
        }
    }

    /*
     * Creates conditions for a network restore at nrInfo and then restores the
     * node from a specific member.
     */
    private void doAndCheckRestore(RepEnvInfo nrInfo,
                                   RepEnvInfo restoreFromInfo) {
        nrInfo.closeEnv();
        shiftVLSNRight(repEnvInfo[0].getEnv());
        try {
            nrInfo.openEnv();
            fail("exception expected");
        } catch (InsufficientLogException e) {
            NetworkRestore networkRestore = new NetworkRestore();
            ReplicationNode restoreNode =
                new RepNodeImpl(restoreFromInfo.getRepConfig());
            final NetworkRestoreConfig config = new NetworkRestoreConfig();
            config.setLogProviders(Arrays.asList(restoreNode));
            networkRestore.execute(e, config);
            assertEquals(restoreNode, networkRestore.getLogProvider());
            final NetworkBackupStats stats =
                networkRestore.getNetworkBackupStats();
            assertThat(stats.getExpectedBytes(), greaterThan(0));
            assertThat(stats.getTransferredBytes(), greaterThan(0));
            nrInfo.openEnv();
        }
    }

    private class NetworkTestExceptionListener implements ExceptionListener {
        public void exceptionThrown(ExceptionEvent event) {
            if (event.getException() instanceof InsufficientLogException) {
                return;
            }

            System.err.println("NetworkRestoreTest caught: " +
                               event.getException() +
                               "\n  in thread: " +
                               event.getThreadName());
        }
    }

    private void setExceptionListener(final RepEnvInfo info) {
        EnvironmentConfig infoConfig = info.getEnvConfig();
        infoConfig.setExceptionListener(new NetworkTestExceptionListener());
        info.setEnvConfig(infoConfig);
    }

    /**
     * This is really multiple tests in one. It tests network restore with a
     * replica in each of the following three states:
     *
     * 1) A brand new node joining the group and needing a network restore.
     *
     * 2) An existing node with its own unique log needing a network restore.
     *
     * 3) Repeated network restores, reflecting a mature node.
     */
    @Test
    public void testBasic()
        throws DatabaseException, Exception {

        /*
         * The cleaner thread can see InsufficientLogExceptions so just stifle
         * those exceptions from stderr.
         */
        DaemonThread.stifleExceptionChatter = true;

        configureForMaxCleaning(2);

        final RepEnvInfo info1 = repEnvInfo[0];
        RepEnvInfo info2 = repEnvInfo[1];

        ReplicatedEnvironment masterRep = info1.openEnv();

        /*
         * Have just the master join first. We do this to test the special case
         * of a brand new node joining a group and needing VLSN 1. The same
         * node then rejoins with its VLSN > 1 to test subsequent rejoins
         * where the node has already participated in the replication.
         */
        populateDB(masterRep, TEST_DB_NAME, 100);

        File cenvDir = info2.getEnvHome();
        final int cid = 2;

        for (int i = 0; i < RESTORE_CYCLES; i++) {

            leaveGroupAllButMaster();
            shiftVLSNRight(masterRep);
            RepNodeImpl memberPrev =
                info1.getRepNode().getGroup().getMember
                 (info2.getRepConfig().getNodeName());
            /* Node1 is not known on the first iteration. */
            final VLSN prevSync = (i == 0) ? null :
                memberPrev.getBarrierState().getLastCBVLSN();
            try {
                /* Should force a network restore. */
                setExceptionListener(info2);
                info2.openEnv();
                fail("exception expected");
            } catch (InsufficientLogException e) {
                logger.info("Got " + e);
                RepNodeImpl member = info1.getRepNode().getGroup().getMember
                    (info2.getRepConfig().getNodeName());

                /*
                 * The sync state should have been advanced to help contribute
                 * to the global CBVLSN and prevent it from advancing.
                 */
                final VLSN currSync = member.getBarrierState().getLastCBVLSN();
                assertTrue((i == 0) || currSync.compareTo(prevSync) >= 0);

                NetworkRestore networkRestore = new NetworkRestore();
                networkRestore.execute(e, new NetworkRestoreConfig());
                final NetworkBackupStats stats =
                    networkRestore.getNetworkBackupStats();
                assertThat(stats.getExpectedBytes(), greaterThan(0));
                assertThat(stats.getTransferredBytes(), greaterThan(0));
                /* Create a replacement replicator. */
                info2 = RepTestUtils.setupEnvInfo
                    (cenvDir,
                     RepTestUtils.DEFAULT_DURABILITY,
                     cid,
                     info1);
                setExceptionListener(info2);
                info2.openEnv();
            }
            /* Verify that we can continue with the "restored" log files. */
            populateDB(masterRep, TEST_DB_NAME, 100, 100);
            VLSN commitVLSN =
                RepTestUtils.syncGroupToLastCommit(repEnvInfo, 2);
            RepTestUtils.checkNodeEquality(commitVLSN, false, repEnvInfo);
            info2.closeEnv();
        }
    }

    private void configureForMaxCleaning(int size) {
        for (int i = 0; i < size; i++) {
            /* Make it easy to create cleaner fodder. */
            repEnvInfo[i].getEnvConfig().setConfigParam(
                EnvironmentParams.LOG_FILE_MAX.getName(), "10000");
            repEnvInfo[i].getEnvConfig().setConfigParam(
                EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "90");
            repEnvInfo[i].getEnvConfig().setConfigParam(
                EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION.getName(),
                "90");
            /* Need a disk limit to cause VLSN index truncation. */
            repEnvInfo[i].getEnvConfig().setConfigParam(
                EnvironmentConfig.MAX_DISK, String.valueOf(50 * 10000));
        }
    }

    /*
     * Provoke sufficient log cleaning to move the entire VLSN right
     * sufficiently that the new VLSN range no longer overlaps the VLSN
     * range upon entry thus guaranteeing a LogFileRefreshException.
     */
    private void shiftVLSNRight(ReplicatedEnvironment menv) {
        /* Shift the vlsn range window. */

        RepImpl menvImpl = repEnvInfo[0].getRepImpl();
        final VLSNIndex vlsnIndex = menvImpl.getRepNode().getVLSNIndex();
        VLSN masterHigh = vlsnIndex.getRange().getLast();

        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setForce(true);

        do {

            /*
             * Populate just the master, leaving the replica behind Re-populate
             * with the same keys to create Cleaner fodder.
             */
            populateDB(menv, TEST_DB_NAME, 100);

            menv.cleanLog();
            menv.checkpoint(checkpointConfig);

        } while (masterHigh.compareTo(vlsnIndex.getRange().getFirst()) >= 0);
    }

    /*
     * Test the API: RepNode.shutdownNetworkBackup/restartNetworkBackup service
     * used to disable the service around a replica syncup operation.
     */
    @Test
    public void testLockout()
        throws IOException {

        setExceptionListener(repEnvInfo[0]);
        repEnvInfo[0].openEnv();
        RepNode repNode = repEnvInfo[0].getRepNode();
        leaveGroupAllButMaster();

        repNode.shutdownNetworkBackup();
        File backupDir =
            new File(repEnvInfo[1].getEnvHome().getCanonicalPath() +
                     ".backup");
        backupDir.mkdir();
        assertTrue(backupDir.exists());

        DataChannelFactory channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig());
        EnvironmentImpl envImpl = createEnvImpl(backupDir);
        try {
            NetworkBackup backup =
                new NetworkBackup(repNode.getSocket(),
                                  backupDir,
                                  new NameIdPair("n1", (short)1),
                                  true,
                                  envImpl.getFileManager(),
                                  envImpl.getLogManager(),
                                  channelFactory);
            backup.execute();
            fail("expected exception service should not have been available");
        } catch (ServiceConnectFailedException e) {
            /* Expected. */
        } catch (Exception e) {
            fail("unexpected exception" + e);
        }

        repNode.restartNetworkBackup();
        try {
            NetworkBackup backup =
                new NetworkBackup(repNode.getSocket(),
                                  backupDir,
                                  new NameIdPair("n1", (short)1),
                                  true,
                                  envImpl.getFileManager(),
                                  envImpl.getLogManager(),
                                  channelFactory);
            backup.execute();
        }  catch (Exception e) {
            fail("unexpected exception:" + e);
        }

        envImpl.abnormalClose();
    }

    private EnvironmentImpl createEnvImpl(File envDir) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        Environment backEnv = new Environment(envDir, envConfig);

        return DbInternal.getNonNullEnvImpl(backEnv);
    }

    /**
     * Verify that a NetworkBackup that's in progress is aborted by
     * repNode.shutdownNetworkRestore() and therefore during a rollback
     * operation.
     */
    @Test
    public void testNBAbortOnSyncup()
        throws IOException, DatabaseException, ServiceConnectFailedException,
                RejectedServerException, RestoreMarker.FileCreationException {

        setExceptionListener(repEnvInfo[0]);
        repEnvInfo[0].openEnv();
        final RepNode repNode = repEnvInfo[0].getRepNode();
        leaveGroupAllButMaster();
        File backupDir =
            new File(repEnvInfo[1].getEnvHome().getCanonicalPath() +
                     ".backup");
        backupDir.mkdir();
        DataChannelFactory channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig());
        EnvironmentImpl envImpl = createEnvImpl(backupDir);
        NetworkBackup backup =
            new NetworkBackup(repNode.getSocket(),
                              backupDir,
                              new NameIdPair("n1", (short)1),
                              true,
                              envImpl.getFileManager(),
                              envImpl.getLogManager(),
                              channelFactory);
        CyclicBarrier testBarrier =
            new CyclicBarrier(1, new Runnable() {
                public void run() {
                    /* The syncup should kill the NB */
                    repNode.shutdownNetworkBackup();
                }
            }
        );
        backup.setTestBarrier(testBarrier);
        try {
            backup.execute();
            fail("Expected exception");
        } catch(IOException e) {
            /* Expected exception as in progress service was terminated. */
        }

        envImpl.abnormalClose();
    }

    /**
     * Check that NumberFormatException is not thrown when parsing ILE
     * properties and VLSN GT max-int. [#26311]
     */
    @Test
    public void testLongVLSNInILE() {

        /* Use a dummy/shell env to speed up the test. */
        final EnvironmentConfig envConfig = new EnvironmentConfig()
            .setTransactional(true)
            .setAllowCreate(true);

        final ReplicatedEnvironment env = RepInternal.createInternalEnvHandle(
            envRoot, new ReplicationConfig(), envConfig);

        final RepImpl repImpl = RepInternal.getNonNullRepImpl(env);

        /* Create VLSN GT max-int. */
        final long vlsn = Long.MAX_VALUE - 100;

        /* Create ILE with properties containing the VLSN. */
        InsufficientLogException ile =
            new InsufficientLogException(repImpl, new VLSN(vlsn));

        /* We're done with the dummy env now. */
        env.close();

        /*
         * Create ILE that parses the properties. Prior to the bug fix, this
         * threw NumberFormatException.
         */
        ile = new InsufficientLogException(ile.getProperties(), null);

        assertEquals(
            String.valueOf(vlsn),
            ile.getProperties().getProperty("REFRESH_VLSN"));
    }
}
