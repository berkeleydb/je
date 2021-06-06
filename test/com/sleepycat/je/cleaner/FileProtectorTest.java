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
package com.sleepycat.je.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.Put;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.stream.FeederReplicaSyncup;
import com.sleepycat.je.rep.stream.ReplicaSyncupReader;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.util.DbSpace;
import com.sleepycat.je.utilint.TestHookAdapter;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests that deletion of cleaned/reserved log files is prohibited as
 * appropriate:
 * - during syncup
 * - by feeders
 * - during network restore, which uses DbBackup
 * - during regular DbBackup
 * - during Database.count
 * - while a DiskOrderedCursor is open
 */
public class FileProtectorTest extends RepTestBase {

    /*
     * If:
     *  - file size is FILE_SIZE
     *  - no files are protected
     *  - there is no max disk limit
     *  - RECORDS are written with DATA_SIZE and UPDATES are performed
     *  - cleaning and checkpointing are performed
     * Then:
     *  - at least RESERVED_FILES will be cleaned and reserved
     * After that, if:
     *  - MAX_DISK is configured
     *  - cleaner is woken up
     * Then:
     *  - total activeLogSize will be LTE ACTIVE_DATA_SIZE
     *  - VLSNS_REMAINING or less remain in the VLSNIndex range
     *
     * These values are approximate. If they become a little off due to other
     * changes, they can be adjusted to the actual values produced in
     * testBaselineCleaning.
     */
    private static final int FILE_SIZE = 10 * 1000;
    private static final int DATA_SIZE = 100;
    private static final int RECORDS = 500;
    private static final int UPDATES = 5;
    private static final int RESERVED_FILES = 12;
    private static final int VLSNS_REMAINING = (RECORDS * 2) + 200;
    private static final int ACTIVE_DATA_SIZE = (RECORDS * 150 * 5) / 3;
    private static final int ACTIVE_DATA_FILES =
        (ACTIVE_DATA_SIZE / FILE_SIZE) + 1;

    @Rule
    public TestName testName= new TestName();

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();

        for (int i = 0; i < groupSize; i += 1) {
            final RepEnvInfo info = repEnvInfo[i];
            final EnvironmentConfig envConfig = info.getEnvConfig();
            final ReplicationConfig repConfig = info.getRepConfig();

            /* Use small log files to quickly clean multiple files.  */
            DbInternal.disableParameterValidation(envConfig);
            envConfig.setConfigParam(
                EnvironmentConfig.LOG_FILE_MAX, String.valueOf(FILE_SIZE));

            envConfig.setConfigParam(
                EnvironmentConfig.CLEANER_MIN_AGE, "1");
            envConfig.setConfigParam(
                EnvironmentConfig.CLEANER_MIN_UTILIZATION, "60");
            envConfig.setConfigParam(
                EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "20");

            /* Allow the VLSNIndex to become small, to delete more files. */
            repConfig.setConfigParam(
                RepParams.MIN_VLSN_INDEX_SIZE.getName(), "10");

            /* Test requires explicit control over all operations. */
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_CLEANER, "false");
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_EVICTOR, "false");
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
            envConfig.setConfigParam(
                EnvironmentConfig.ENV_RUN_VERIFIER, "false");
        }
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        ReplicaSyncupReader.setFileGapHook(null);
        FeederReplicaSyncup.setAfterSyncupStartedHook(null);
        FeederReplicaSyncup.setAfterSyncupEndedHook(null);
        Feeder.setInitialWriteMessageHook(null);

        super.tearDown();
    }

    /**
     * Checks that test parameters cause cleaning, reserved files and deleted
     * files, as expected. Also checks barren file deletion.
     */
    @Test
    public void testBaselineCleaning() {

        RepTestUtils.joinGroup(repEnvInfo);
        final RepEnvInfo masterInfo = repEnvInfo[0];
        makeWaste(masterInfo);

        for (final RepEnvInfo info : repEnvInfo) {
            makeReservedFiles(info);
            expectFilesDeleted(info, 0);
        }

        /*
         * While we are here and the feeders are protecting one or more files
         * in the VLSNIndex range, check barren file deletion. The barren files
         * written and deleted by makeNonHAWaste will come after the files in
         * the VSLNIndex range.
         */
        for (final RepEnvInfo info : repEnvInfo) {
            makeNonHAWaste(info);
            makeReservedFiles(info);
            /* Allow two remaining reserved files due to checkpoints. */
            expectFilesDeleted(info, 2);
        }

        /*
         * Close one replica, write more data, then open the replica to check
         * that the syncup and feeder readers can skip over the gap due to the
         * deleted barren files.
         */
        final RepEnvInfo replicaInfo = repEnvInfo[2];
        replicaInfo.closeEnv();
        makeWaste(masterInfo);

        class Hook extends TestHookAdapter<Long> {
            private volatile boolean called = false;

            @Override
            public void doHook(Long prevFileNum) {
                called = true;
            }
        }

        final Hook hook = new Hook();
        ReplicaSyncupReader.setFileGapHook(hook);
        replicaInfo.openEnv();
        assertTrue(hook.called);

        verifyData();
    }

    /**
     * Tests that files are protected on a master as the result of syncup and
     * feeder position.
     *
     * 1. Create master, make waste and reserved files.
     * 2. Add a replica, but pause after syncup begins.
     * 3. Expect that no files can be deleted.
     * 4. Allow syncup to finish but pause before feeder starts.
     * 5. Expect that no files can be deleted.
     * 6. Allow feeder to catch up half way.
     * 7. Expect only half the files can be deleted.
     * 8. Allow feeder to completely catch up.
     * 9. Except the rest of the files can be deleted.
     */
    @Test
    public void testSyncupAndFeeder()
        throws Throwable {

        /* Create group of 2. Make waste and reserved files on both nodes. */
        RepTestUtils.joinGroup(Arrays.copyOf(repEnvInfo, 2));
        final RepEnvInfo masterInfo = repEnvInfo[0];
        final RepEnvInfo replicaInfo = repEnvInfo[1];
        makeWaste(masterInfo);
        makeReservedFiles(masterInfo);
        makeReservedFiles(replicaInfo);

        final VLSNIndex masterVlsnIndex =
            masterInfo.getRepImpl().getVLSNIndex();

        final VLSNRange initialRange = masterVlsnIndex.getRange();

        class ExpectNoDeletions extends TestHookAdapter<Feeder> {
            private volatile Throwable exception;

            @Override
            public void doHook(Feeder feeder) {
                try {
                    final VLSNRange prevRange = masterVlsnIndex.getRange();

                    /*
                     * Syncup has just started, or has just finished but
                     * feeding has not started. No files can be deleted on the
                     * master.
                     */
                    final int deleted = deleteReservedFiles(masterInfo);
                    assertEquals(0, deleted);

                    final VLSNRange curRange = masterVlsnIndex.getRange();

                    assertEquals(
                        "initialRange=" + initialRange +
                            " curRange=" + curRange +
                            " prevRange=" + prevRange,
                        initialRange.getFirst(), curRange.getFirst());

                    /*
                     * While we're here, check that files can be deleted on the
                     * first replica, which is not impacted by this syncup.
                     */
                    expectFilesDeleted(replicaInfo, 0);

                } catch (Throwable e) {
                    e.printStackTrace();
                    exception = e;
                    throw e;
                }
            }
        }

        final long halfwayMessages =
            initialRange.getLast().getSequence() -
                initialRange.getFirst().getSequence();

        final long halfwayVlsn = (halfwayMessages / 2) - VLSNS_REMAINING;

        final int someFiles = ACTIVE_DATA_FILES / 2;

        class ExpectSomeDeletions
            extends TestHookAdapter<BinaryProtocol.Message> {

            private volatile Throwable exception;
            private volatile int nMessages = 0;

            @Override
            public void doHook(BinaryProtocol.Message feeder) {

                nMessages += 1;

                if (nMessages != halfwayMessages) {
                    return;
                }

                /*
                 * After sending half the rep stream, expect that we can
                 * truncate the VLSNIndex and delete some files.
                 */
                try {
                    final VLSNRange prevRange = masterVlsnIndex.getRange();

                    final int deleted = deleteReservedFiles(masterInfo);

                    assertTrue(
                        "expected=" + someFiles +
                            " actual=" + deleted +
                            " nMessages=" + nMessages,
                        deleted >= someFiles);

                    final VLSNRange curRange = masterVlsnIndex.getRange();

                    assertTrue(
                        "expected=" + halfwayVlsn +
                            " initialRange=" + initialRange +
                            " curRange=" + curRange +
                            " prevRange=" + prevRange +
                            " nMessages=" + nMessages,
                        curRange.getFirst().getSequence() >= halfwayVlsn);

                } catch (Throwable e) {
                    e.printStackTrace();
                    exception = e;
                    throw e;
                }
            }
        }

        final ExpectNoDeletions expectNoDeletions = new ExpectNoDeletions();

        final ExpectSomeDeletions expectSomeDeletions =
            new ExpectSomeDeletions();

        FeederReplicaSyncup.setAfterSyncupStartedHook(expectNoDeletions);
        FeederReplicaSyncup.setAfterSyncupEndedHook(expectNoDeletions);
        Feeder.setInitialWriteMessageHook(expectSomeDeletions);

        /*
         * Add the third node (the second replica). Syncup and feeding of this
         * node will result in calling the test hooks.
         */
        RepTestUtils.joinGroup(repEnvInfo);
        RepTestUtils.syncGroup(repEnvInfo);

        if (expectNoDeletions.exception != null) {
            throw expectNoDeletions.exception;
        }

        if (expectSomeDeletions.exception != null) {
            throw expectSomeDeletions.exception;
        }

        FeederReplicaSyncup.setAfterSyncupStartedHook(null);
        FeederReplicaSyncup.setAfterSyncupEndedHook(null);
        Feeder.setInitialWriteMessageHook(null);

        /* The rest of the files can now be deleted. */
        makeReservedFiles(repEnvInfo[2]);
        for (final RepEnvInfo info : repEnvInfo) {
            expectFilesDeleted(info, 0);
        }

        verifyData();
    }

    /**
     * Tests that network restore protects the active files on the server plus
     * the two latest reserved files.
     */
    @Test
    public void testNetworkRestore()
        throws Throwable {

        /*
         * Create group of 2 and clean/delete files, so that syncup of the 3rd
         * node will fail.
         */
        RepTestUtils.joinGroup(Arrays.copyOf(repEnvInfo, 2));

        final RepEnvInfo masterInfo = repEnvInfo[0];
        makeWaste(masterInfo);
        makeReservedFiles(masterInfo);
        expectFilesDeleted(masterInfo, 0);

        final RepEnvInfo replicaInfo = repEnvInfo[1];
        makeReservedFiles(replicaInfo);

        /* Get stats before network restore. */
        final EnvironmentStats masterStats =
            masterInfo.getEnv().getStats(null);

        final EnvironmentStats replicaStats =
            replicaInfo.getEnv().getStats(null);

        /*
         * The replica (not the master) should be chosen as the server.
         * The master is lower priority, when current VLSNs are roughly equal.
         */
        final String expectNode = "Node2";

        /*
         * Two reserved files, plus the active files, should be restored.
         *
         * The original server active log size, plus the two reserved files,
         * should roughly equal the total bytes restored.
         */
        final int expectFiles =
            replicaInfo.getRepImpl().getFileProtector().getNActiveFiles() + 2;

        final long expectBytes =
            replicaStats.getActiveLogSize() + (2 * FILE_SIZE);

        class Hook extends TestHookAdapter<File> {
            private volatile Throwable exception;
            private volatile int nFiles = 0;
            private volatile long nBytes = 0;

            @Override
            public void doHook(File file) {
                try {
                    final FileManager fm =
                        replicaInfo.getRepImpl().getFileManager();

                    /* Convert client path to server path. */
                    file = new File(
                        fm.getFullFileName(
                            fm.getNumFromName(file.getName())));

                    nBytes += file.length();
                    nFiles += 1;

                    /*
                     * Make reserved files and delete files other than those
                     * that are protected because they are being restored.
                     * This checks that the network restore is protecting the
                     * files yet to be transferred, and that the VLSNIndex can
                     * advance at the same time.
                     *
                     * The number of reserved files will start at expectFiles
                     * and go down as files are transferred, since files are
                     * unprotected as they are transferred. Two files always
                     * remain reserved: one for the file just transferred and
                     * another is added as a fudge factor.
                     */
                    makeWaste(masterInfo);
                    makeReservedFiles(replicaInfo);
                    expectFilesDeleted(replicaInfo, expectFiles - nFiles + 2);

                    /*
                    final EnvironmentStats stats =
                        replicaInfo.getEnv().getStats(null);

                    System.out.format(
                        "expect reserved %d true reserved %,d %n",
                        (expectFiles - nFiles + 2),
                        stats.getReservedLogSize());

                    System.out.println("thread=" +
                        Thread.currentThread().getName() +
                        " activeSize=" + stats.getActiveLogSize() +
                        " reservedSize=" + stats.getReservedLogSize() +
                        " totalSize=" + stats.getTotalLogSize() +
                        " protectedSize=" + stats.getProtectedLogSize() +
                        " protectedSizeMap=" + stats.getProtectedLogSizeMap());
                    //*/
                } catch (Throwable e) {
                    exception = e;
                }
            }
        }

        /*
         * Add 3rd node to group, which will throw ILE. Then perform the
         * network restore, which will call the hook.
         */
        final NetworkRestore networkRestore = new NetworkRestore();
        final Hook hook = new Hook();

        try {
            RepTestUtils.joinGroup(repEnvInfo);
            fail("Expected ILE");
        } catch (final InsufficientLogException ile) {
            networkRestore.setInterruptHook(hook);
            networkRestore.execute(ile, new NetworkRestoreConfig());
        }

        if (hook.exception != null) {
            throw hook.exception;
        }

        final String nodeName = networkRestore.getLogProvider().getName();

        final String msg = "nodeName=" + nodeName +
            " hook.nFiles=" + hook.nFiles +
            " hook.nBytes=" + hook.nBytes +
            " masterActive=" + masterStats.getActiveLogSize() +
            " replicaActive=" + replicaStats.getActiveLogSize() +
            " expectNode=" + expectNode +
            " expectFiles=" + expectFiles +
            " expectBytes=" + expectBytes;

//        System.out.println(msg);

        assertEquals(msg, expectNode, nodeName);
        assertEquals(expectFiles, hook.nFiles);
        assertTrue(msg, Math.abs(hook.nBytes - expectBytes) < 5000);

        /* The restored node can now be opened. */
        final RepEnvInfo restoredReplicaInfo = repEnvInfo[2];
        restoredReplicaInfo.openEnv();

        verifyData();
    }

    /**
     * Tests that regular DbBackup protects files in the backup set, and that
     * protection can be removed as files are copied.
     */
    @Test
    public void testBackup() {

        RepTestUtils.joinGroup(repEnvInfo);
        final RepEnvInfo masterInfo = repEnvInfo[0];
        makeWaste(masterInfo);

        for (final RepEnvInfo info : repEnvInfo) {

            /* Make a data set, delete the waste. */
            makeReservedFiles(info);
            expectFilesDeleted(info, 0);
            assertEquals(0, deleteReservedFiles(info));

            /* The backup will protect all files currently active. */
            final DbBackup backup = new DbBackup(info.getEnv());
            backup.startBackup();
            final String[] files = backup.getLogFilesInBackupSet();

            /*
             * Create more waste which will make all files in the backup set
             * reserved, but they cannot be deleted yet.
             */
            makeWaste(masterInfo);
            makeReservedFiles(info);
            expectFilesDeleted(info, files.length);

            final Set<Long> reservedFiles = info.getRepImpl().
                getFileProtector().getReservedFileInfo().second();

            final FileManager fm = info.getRepImpl().getFileManager();

            /*
             * Simulate copying where protection is removed for each file after
             * it is copied, and then it can be deleted (if it is reserved).
             */
            int deleted = 0;

            for (final String file : files) {

                final File fileObj = new File(info.getEnvHome(), file);
                final EnvironmentStats stats = info.getEnv().getStats(null);

                final String msg = "file=" + file +
                    " protected=" + stats.getProtectedLogSize() +
                    " protectedMap=" + stats.getProtectedLogSizeMap();

                assertTrue(msg, fileObj.exists());
                backup.removeFileProtection(file);

                if (reservedFiles.contains(fm.getNumFromName(file))) {
                    assertEquals(msg, 1, deleteReservedFiles(info));
                    assertFalse(msg, fileObj.exists());
                    deleted += 1;
                } else {
                    assertEquals(msg, 0, deleteReservedFiles(info));
                }
            }

            /* At least some files should have been deleted. */
            final int someFiles = ACTIVE_DATA_FILES / 2;
            assertTrue(
                "expected=" + someFiles + " deleted=" + deleted,
                deleted >= someFiles);

            backup.endBackup();
            expectFilesDeleted(info, 0);
        }
    }

    /**
     * Tests that DiskOrderedCursor and Database.count protect all active
     * files, including files created during the scan. The test only checks
     * DiskOrderedCursor, and we assume protection works equally well for
     * Database.count, since they both rely on DiskOrderedScanner to implement
     * file protection.
     */
    @Test
    public void testDiskOrderedCursor() {

        RepTestUtils.joinGroup(repEnvInfo);
        final RepEnvInfo masterInfo = repEnvInfo[0];
        makeWaste(masterInfo);

        for (final RepEnvInfo info : repEnvInfo) {

            /* Make a bunch of reserved files. */
            makeReservedFiles(info);

            /*
             * The files currently reserved can be deleted even while the
             * cursor is open.
             */
            final Set<Long> reservedFiles = info.getRepImpl().
                getFileProtector().getReservedFileInfo().second();

            /* Set queue size to one to cause DOS producer to block. */
            final DiskOrderedCursorConfig config =
                new DiskOrderedCursorConfig().
                    setQueueSize(1);

            /*
             * Open the cursor, which will protect all active files, including
             * those that become reserved while the cursor is open.
             */
            final Database db = info.getEnv().openDatabase(
                null, "test",
                new DatabaseConfig().setTransactional(true));

            final DiskOrderedCursor cursor = db.openCursor(config);

            /*
             * Make more waste and reserved files, although these files cannot
             * be deleted while the cursor is open.
             */
            makeWaste(masterInfo);
            makeReservedFiles(info);

            /* Only the files reserved earlier can be deleted. */
            final int deleted = deleteReservedFiles(info);
            assertEquals(deleted, countDeletedFiles(info, reservedFiles));

            /* At least some files should have been deleted. */
            final int someFiles = ACTIVE_DATA_FILES / 2;
            assertTrue(
                "expected=" + someFiles + " deleted=" + deleted,
                deleted >= someFiles);

            /*
             * After the cursor is closed, the rest of the files can be
             * deleted.
             */
            cursor.close();

            /*
             * Wait for DOS producer thread to stop and remove file protection.
             */
            while (!DbInternal.getDiskOrderedCursorImpl(cursor).
                    isProcessorClosed()) {
                try {
                    Thread.sleep(5);
                } catch (Throwable e) {
                    /* Do nothing. */
                }
            }

            expectFilesDeleted(info, 0);
            db.close();
        }
    }

    /**
     * Generate waste that should later cause cleaning to generate at least
     * RESERVED_FILES reserved files.
     */
    private void makeWaste(final RepEnvInfo info) {

        final ReplicatedEnvironment master = info.getEnv();

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        final Database db = master.openDatabase(null, "test", dbConfig);
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[DATA_SIZE]);

        /* Insert records and then update them UPDATES*2 times. */
        for (int i = 0; i <= UPDATES * 2; i++) {
            for (int j = 0; j < RECORDS; j++) {
                IntegerBinding.intToEntry(j, key);
                Arrays.fill(data.getData(), (byte) j);
                db.put(null, key, data, Put.OVERWRITE, null);
            }
        }

        db.close();
        RepTestUtils.syncGroup(repEnvInfo);
    }

    private void verifyData() {

        for (final RepEnvInfo info : repEnvInfo) {
            final ReplicatedEnvironment env = info.getEnv();

            final Database db = env.openDatabase(
                null, "test",
                new DatabaseConfig().setTransactional(true));

            final Cursor cursor = db.openCursor(null, null);
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            int i = 0;
            while (cursor.get(key, data, Get.NEXT, null) != null) {
                assertEquals(i, IntegerBinding.entryToInt(key));
                assertEquals(DATA_SIZE, data.getSize());
                for (int j = 0; j < DATA_SIZE; j += 1) {
                    assertEquals(data.getData()[j], (byte) i);
                }
                i += 1;
            }

            assertEquals(RECORDS, i);
            cursor.close();
            db.close();
        }

        final VLSN lastVLSN =
            repEnvInfo[0].getRepImpl().getVLSNIndex().getRange().getLast();

        try {
            RepTestUtils.checkNodeEquality(lastVLSN, false, repEnvInfo);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs cleaning and a checkpoint. Checks that at least RESERVED_FILES
     * files are cleaned and made reserved.
     */
    private void makeReservedFiles(final RepEnvInfo info) {
        final Environment env = info.getEnv();

        final int cleaned = env.cleanLog();
        verifyMetadata(info);

        String msg = "cleaned=" + cleaned;
        assertTrue(msg, cleaned >= RESERVED_FILES);

        final int minUtil =
            info.getRepImpl().getCleaner().getUtilizationCalculator().
                getPredictedMinUtilization();

        assertTrue("minUtil=" + minUtil, minUtil >= 40);

        env.checkpoint(new CheckpointConfig().setForce(true));
        verifyMetadata(info);

        EnvironmentStats stats = env.getStats(null);

        final double reservedEst =
            stats.getReservedLogSize() / (double) FILE_SIZE;

        final int reserved =
            info.getRepImpl().getFileProtector().getNReservedFiles();

        msg += " reservedEst=" + reservedEst + " reserved=" + reserved;

        assertTrue(msg, reserved >= reservedEst);
        assertTrue(msg, reserved >= RESERVED_FILES);
    }

    /**
     * Sets the MAX_DISK limit temporarily to delete as many reserved files as
     * possible. Returns the number of files that were deleted.
     */
    private int deleteReservedFiles(final RepEnvInfo info) {

        final Environment env = info.getEnv();
        final EnvironmentStats stats = env.getStats(null);

        /*
         * Set MAX_DISK to less than the active data set size to delete as
         * many files as possible.
         */
        env.setMutableConfig(
            env.getMutableConfig().setConfigParam(
                EnvironmentConfig.MAX_DISK,
                String.valueOf(ACTIVE_DATA_SIZE / 2)));

        /*
         * Normally the cleaner will call manageDiskUsage periodically to
         * delete reserved files. We call it here directly to avoid
         * DiskLimitException and other side effects of attempting cleaning.
         */
        info.getRepImpl().getCleaner().manageDiskUsage();

        verifyMetadata(info);

        /*
         * Remove MAX_DISK limit and call manageDiskUsage to reset usage
         * violation state variables.
         */
        env.setMutableConfig(
            env.getMutableConfig().setConfigParam(
                EnvironmentConfig.MAX_DISK, "0"));

        info.getRepImpl().getCleaner().manageDiskUsage();

        verifyMetadata(info);

        return (int) (env.getStats(null).getNCleanerDeletions() -
            stats.getNCleanerDeletions());
    }

    /**
     * After deleting reserved files, expect ACTIVE_DATA_FILES or less
     * remaining and VLSNS_REMAINING or less in the VLSNIndex.
     *
     * @param expectReservedFiles no more than this amount should remain
     * reserved.
     */
    private void expectFilesDeleted(final RepEnvInfo info,
                                    final int expectReservedFiles) {

        deleteReservedFiles(info);

        final VLSNIndex vlsnIndex = info.getRepImpl().getVLSNIndex();
        final VLSNRange range = vlsnIndex.getRange();
        final long firstSeq = range.getLast().getSequence();
        final long lastSeq = range.getFirst().getSequence();

        assertTrue(
            "expected=" + VLSNS_REMAINING + " range=" + range,
            firstSeq - lastSeq <= VLSNS_REMAINING);

        final EnvironmentStats stats = info.getEnv().getStats(null);
        final long totalSize = stats.getTotalLogSize();
        final long reservedSize = stats.getReservedLogSize();

        /*
         * util * total == active
         * total == active / util
         */
        final int minUtil =
            info.getRepImpl().getCleaner().getUtilizationCalculator().
                getCurrentMinUtilization();

        final int expectTotal = ((ACTIVE_DATA_SIZE * 100) / minUtil);
        final int expectReserved = expectReservedFiles * FILE_SIZE;

        final String msg = "totalSize=" + totalSize +
            " reservedSize=" + reservedSize +
            " expectTotal=" + expectTotal +
            " expectReserved=" + expectReserved +
            " minUtil=" + minUtil;

        assertTrue(msg, totalSize <= expectTotal);
        assertTrue(msg, reservedSize <= expectReserved);
    }

    /*
     * For debugging.
     */
    @SuppressWarnings("unused")
    private void printDbSpace(final RepEnvInfo info, final String label) {
        System.out.println(label);
        DbSpace dbSpace = new DbSpace(info.getEnv(), false, false, false);
        dbSpace.print(System.out);
    }

    /**
     * Generate waste that is not replicated, to create "barren files" (having
     * no replicable entries). A non-replicated DB is used to create the waste.
     */
    private void makeNonHAWaste(final RepEnvInfo info) {

        final ReplicatedEnvironment env = info.getEnv();

        /* Create non-replicated DB. */
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setReplicated(false);
        dbConfig.setAllowCreate(true);

        final Database db = env.openDatabase(null, "non-ha", dbConfig);
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[DATA_SIZE]);

        /* Insert/update/delete records. */
        for (int i = 0; i <= UPDATES; i++) {
            for (int j = 0; j < RECORDS; j++) {
                IntegerBinding.intToEntry(j, key);
                db.put(null, key, data, Put.OVERWRITE, null);
                db.delete(null, key, null);
            }
        }

        db.close();
        env.flushLog(false);
    }

    /**
     * Checks integrity of various metadata.
     */
    private void verifyMetadata(final RepEnvInfo info) {

        /*
         * Check that all VLSNs in the VLSNIndex range are in existing files.
         */
        final RepImpl repImpl = info.getRepImpl();
        final FileManager fm = repImpl.getFileManager();
        final VLSNIndex vlsnIndex = repImpl.getVLSNIndex();
        final VLSNRange range = vlsnIndex.getRange();

        for (VLSN vlsn = range.getFirst();
             vlsn.compareTo(range.getLast()) <= 0;
             vlsn = vlsn.getNext()) {

            final long file = vlsnIndex.getGTEFileNumber(vlsn);
            assertEquals(file, vlsnIndex.getLTEFileNumber(vlsn));
            assertTrue("file=" + file, fm.isFileValid(file));
        }

        /* Sanity check of log size values. */
        final EnvironmentStats stats = info.getEnv().getStats(null);
        assertEquals(
            stats.getTotalLogSize(),
            stats.getActiveLogSize() + stats.getReservedLogSize());
        final long protectedSize = stats.getProtectedLogSize();
        assertTrue(protectedSize <= stats.getTotalLogSize());

        /* The largest protected map value should be LTE the total. */
        final Map<String, Long> map = stats.getProtectedLogSizeMap();
        final String msg = "protectedSize= " + protectedSize + " val=";
        for (final Long val : map.values()) {
            assertTrue(msg + val, val <= protectedSize);
        }
    }

    /**
     * Returns the number of files in the given set that do not exist.
     */
    private int countDeletedFiles(final RepEnvInfo info,
                                  final Set<Long> files) {

        final FileManager fm = info.getRepImpl().getFileManager();
        int count = 0;

        for (final long file : files) {
            if (!fm.isFileValid(file)) {
                count += 1;
            }
        }

        return count;
    }
}
