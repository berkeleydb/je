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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test an interrupted network restore and the RestoreRequired mechanism.
 */
public class InterruptedNetworkRestoreTest extends RepTestBase {

    private Logger logger;

    @Override
    @Before
    public void setUp()
        throws Exception {

        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "FailoverTest");
        groupSize = 2;
        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * 1) Start with a two node group: rg1-rn1(M) and rg1-rn2(R)
     */
    @Test
    public void testBasic()
        throws InterruptedException {

        /* Need a disk limit to advance VLSN range head. */
        for (int i = 0; i < 2; i++) {
            repEnvInfo[i].getEnvConfig().setConfigParam(
                EnvironmentConfig.MAX_DISK, String.valueOf(50 * 10000));
        }
        createGroup(2);
        final RepEnvInfo mInfo1 = repEnvInfo[0];
        final RepEnvInfo mInfo2 = repEnvInfo[1];

        shiftVLSNRight(mInfo1.getEnv());

        /*
         * Shut down node2 and delete its files, so it will open with an
         * InsufficientLogException.
         */
        mInfo2.closeEnv();
        TestUtils.removeLogFiles("Setting up for network restore",
                                 mInfo2.getEnvHome(), false);
        logger.info("Removed files from " + mInfo2.getEnvHome());
        InsufficientLogException expectedILE = openEnvExpectILE(mInfo2);

        /*
         * Start a network restore on node 2, but intentionally kill
         * the network restore before the first transfer of a file
         * equal to or greater than 4.
         */
        NetworkRestore nr = new NetworkRestore();
        nr.setInterruptHook(new StopBackup(4, repEnvInfo[0].getRepImpl()));
        NetworkRestoreConfig config = new NetworkRestoreConfig();
        try {
            nr.execute(expectedILE, config);
            fail("should throw StopBackupException");
        } catch (StopBackupException expected) {
        }

        /* At this point, a marker file should exist. */
        assertTrue(markerFileExists(mInfo2.getEnvHome()));

        /*
         * Try to start up node2  multiple times. It should not be able to
         * recover, and should continue to throw the ILE until a network
         * restore is completed.
         */
        InsufficientLogException useException = null;
        for (int i = 0; i < 3; i++) {
            useException = openEnvExpectILE(mInfo2);
            assertTrue(markerFileExists(mInfo2.getEnvHome()));
        }

        /*
         * Reestablish a fresh network restore, still with the test hook, using
         * the new ILE generated from recovery. This will exercise the
         * path to create a new repImpl from the persisted ILE properties in
         * the RestoreRequired log entry.
         */
        nr = new NetworkRestore();
        nr.setInterruptHook(new StopBackup(2, repEnvInfo[0].getRepImpl()));
        try {
            nr.execute(useException, config);
            fail("should throw StopBackupException");
        } catch (StopBackupException expected) {
        }
        assertTrue(markerFileExists(mInfo2.getEnvHome()));

        /*
         * Reestablish another fresh network restore, with no test hook, using
         * the new ILE generated from recovery. This will exercise the
         * path to create a new repImpl from the persisted ILE properties in
         * the RestoreRequired log entry.
         */
        useException = openEnvExpectILE(mInfo2);
        nr = new NetworkRestore();
        nr.execute(useException, config);

        /* At this point, a marker file should not exist */
        assertFalse(markerFileExists(mInfo2.getEnvHome()));

        mInfo2.openEnv();
        VLSN commitVLSN = RepTestUtils.syncGroup(repEnvInfo);
        RepTestUtils.checkNodeEquality(commitVLSN, false, repEnvInfo);
    }

    private boolean markerFileExists(File envHome) {

        String [] jdbFiles =
            FileManager.listFiles(envHome,
                                  new String[]{FileManager.JE_SUFFIX},
                                  false);
        return jdbFiles[jdbFiles.length-1].equals
            (RestoreMarker.getMarkerFileName());
    }

    private InsufficientLogException openEnvExpectILE(RepEnvInfo rinfo) {
        try {
            rinfo.openEnv();
            fail("Expected ILE");
        } catch (InsufficientLogException ile) {
            return ile;
        }
        throw new IllegalStateException
            ("Should have seen an InsufficientLogException");
    }

    /*
     * Provoke sufficient log cleaning to move the entire VLSN right
     * sufficiently that the new VLSN range no longer overlaps the VLSN range
     * upon entry thus guaranteeing a InsufficientLogFileException.
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
            populateDB(menv, TEST_DB_NAME, 1000);

            /*
             * Sleep to permit the cbvlsn on the master to be updated. It's
             * done with the period: FeederManager.MASTER_CHANGE_CHECK_TIMEOUT
             */
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                fail("unexpected interrupt");
            }
            menv.cleanLog();
            menv.checkpoint(checkpointConfig);
        } while (masterHigh.compareTo(vlsnIndex.getRange().getFirst()) > 0);
    }

    private class StopBackupException extends RuntimeException {
        StopBackupException(String msg) {
            super(msg);
        }
    }

    private class StopBackup implements TestHook<File> {

        private final long interruptPoint;
        private final RepImpl repImpl;

        StopBackup(long interruptPoint, RepImpl repImpl) {
            this.interruptPoint = interruptPoint;
            this.repImpl = repImpl;
        }

        @Override
        public void hookSetup() {
        }

        @Override
        public void doIOHook() throws IOException {
        }

        @Override
        public void doHook(File f) {
            long fileNum = repImpl.getFileManager().getNumFromName(f.getName());
            if (fileNum >= interruptPoint) {
                throw new StopBackupException(
                    "Testhook: throwing exception  because we're at file " +
                        interruptPoint);
            }
        }

        @Override
        public File getHookValue() {
            return null;
        }

        @Override
        public void doHook() {
        }
    }

}
