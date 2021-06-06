/*-
 * Copyright (c) 2002, 2017, oracle and/or its affiliates. all rights reserved.
 *
 * this file was distributed by oracle as part of a version of oracle berkeley
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.Put;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.TestHookAdapter;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the MAX_DISK and FREE_DISK limits and the available log size stat.
 *
 * Testing of concurrent activity (while hitting the disk limits) is done by
 * the DiskLimitStress standalone test.
 */
public class DiskLimitTest extends RepTestBase {

    private static final long FIVE_GB = 5L * 1024L * 1024L * 1024L;
    private static final long TEN_GB = 2 * FIVE_GB;

    private static final Durability ACK_ALL = new Durability(
        Durability.SyncPolicy.WRITE_NO_SYNC,
        Durability.SyncPolicy.WRITE_NO_SYNC,
        Durability.ReplicaAckPolicy.ALL);

    private static final Durability ACK_MAJORITY = new Durability(
        Durability.SyncPolicy.WRITE_NO_SYNC,
        Durability.SyncPolicy.WRITE_NO_SYNC,
        Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);

    private int nRecords = 100;

    private Environment standaloneEnv;

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();

        for (int i = 0; i < groupSize; i += 1) {
            final EnvironmentConfig envConfig = repEnvInfo[i].getEnvConfig();

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

            /* Disable shared cache to allow eviction testing. */
            envConfig.setConfigParam(
                EnvironmentConfig.SHARED_CACHE, "false");
        }
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        Checkpointer.setBeforeFlushHook(null);

        if (standaloneEnv != null) {
            try {
                standaloneEnv.close();
            } catch (DiskLimitException e) {
                /* Ignored. Disk limits may be set by test. */
            } finally {
                standaloneEnv = null;
            }
        }

        super.tearDown();
    }

    /**
     * Checks calculation of available log size with various input values.
     */
    @Test
    public void testAvailableLogSize() {

        standaloneEnv = new Environment(envRoot, repEnvInfo[0].getEnvConfig());

        /*
         * Use values from table in Cleaner.recalcLogSizeStats.
         *
         * params: freeDL, maxDL, diskFS, availableLS.
         */
        checkAvailableSize( 5,   0, 20,  35);
        checkAvailableSize(25,   0,  5,   0);
        checkAvailableSize(30,   0,  5,  -5);
        checkAvailableSize( 5, 100, 20,  35);
        checkAvailableSize(25, 100, 20,  15);
        checkAvailableSize( 5,  80, 20,  20);
        checkAvailableSize(25,  80, 20,   0);
        checkAvailableSize(25, 200,  5,   0);
        checkAvailableSize(25,  75, 20,  -5);
        checkAvailableSize(50,  80, 90, -25);

        /*
         * While a disk limit
         * is violated (availableLogSize is -5), make sure
         * Environment.close throws DLE.
         */
        try {
            standaloneEnv.close();
            fail("DLE expected");
        } catch (DiskLimitException e) {
            /* Expected. */
        }
    }

    private void checkAvailableSize(final int freeDL,
                                    final int maxDL,
                                    final int diskFS,
                                    final int availableLS) {

        /* Use values from table in Cleaner.recalcLogSizeStats. */
        final int activeLS = 50;
        final int reservedLS = 25;
        final int protectedLS = 5;

        final FileProtector.LogSizeStats logSizeStats =
            new FileProtector.LogSizeStats(
                activeLS, reservedLS, protectedLS, Collections.emptyMap());

        standaloneEnv.setMutableConfig(
            standaloneEnv.getMutableConfig().
                setMaxDisk(maxDL).
                setConfigParam(
                    EnvironmentConfig.FREE_DISK, String.valueOf(freeDL)));

        final Cleaner cleaner =
            DbInternal.getEnvironmentImpl(standaloneEnv).getCleaner();

        cleaner.recalcLogSizeStats(logSizeStats, diskFS);

        final EnvironmentStats stats = standaloneEnv.getStats(null);

        assertEquals(
            "freeDL=" + freeDL + " maxDL=" + maxDL + " diskFS=" + diskFS,
            availableLS, stats.getAvailableLogSize());

        if (availableLS > 0) {
            assertNull(
                cleaner.getDiskLimitViolation(),
                cleaner.getDiskLimitViolation());
        } else {
            assertNotNull(
                cleaner.getDiskLimitMessage(),
                cleaner.getDiskLimitViolation());

            assertTrue(
                "maxOverage=" + cleaner.getMaxDiskOverage() +
                    " freeShortage=" + cleaner.getFreeDiskShortage(),
                cleaner.getMaxDiskOverage() >= 0 ||
                    cleaner.getFreeDiskShortage() >= 0);
        }

        assertEquals(activeLS + reservedLS, stats.getTotalLogSize());

        assertEquals(maxDL, standaloneEnv.getConfig().getMaxDisk());
    }

    /**
     * Checks that FREE_DISK is subtracted from MAX_DISK iff:
     *  - MAX_DISK > 10GB, or
     *  - FREE_DISK is explicitly specified.
     */
    @Test
    public void testFreeDiskSubtraction() {

        standaloneEnv = new Environment(envRoot, repEnvInfo[0].getEnvConfig());

        /*
         * FREE_DISK is not specified, so it is subtracted only if MAX_DISK
         * is GT 10GB. Default FREE_DISK is 5GB.
         */
        checkFreeDiskSubtraction(1000, FIVE_GB, false);
        checkFreeDiskSubtraction(TEN_GB, FIVE_GB, false);
        checkFreeDiskSubtraction(TEN_GB + 1, FIVE_GB, true);

        final long FREE_DISK = 100;

        standaloneEnv.setMutableConfig(
            standaloneEnv.getMutableConfig().
                setConfigParam(
                    EnvironmentConfig.FREE_DISK,
                    String.valueOf(FREE_DISK)));


        /* FREE_DISK is specified so it is always subtracted. */
        checkFreeDiskSubtraction(1000, FREE_DISK, true);
        checkFreeDiskSubtraction(TEN_GB, FREE_DISK, true);
        checkFreeDiskSubtraction(TEN_GB + 1, FREE_DISK, true);
    }

    private void checkFreeDiskSubtraction(final long maxDisk,
                                          final long freeDisk,
                                          final boolean expectSubtracted) {

        /*
         * Use maxDisk-1 value for activeLogSize so that if freeDisk is
         * subtracted then availableLogSize will be zero and there will be
         * a disk limit violation.
         */
        final FileProtector.LogSizeStats logSizeStats =
            new FileProtector.LogSizeStats(
                maxDisk - 1, 0, 0, Collections.emptyMap());

        standaloneEnv.setMutableConfig(
            standaloneEnv.getMutableConfig().setMaxDisk(maxDisk));

        final Cleaner cleaner =
            DbInternal.getEnvironmentImpl(standaloneEnv).getCleaner();

        cleaner.recalcLogSizeStats(logSizeStats, freeDisk + 1);

        if (expectSubtracted) {
            assertNotNull(
                cleaner.getDiskLimitMessage(),
                cleaner.getDiskLimitViolation());
        } else {
            assertNull(
                cleaner.getDiskLimitViolation(),
                cleaner.getDiskLimitViolation());
        }
    }

    /**
     * Checks that a disk limit violation will prevent write operations.
     */
    @Test
    public void testWritesProhibited() {

        RepTestUtils.joinGroup(repEnvInfo);
        final RepEnvInfo masterInfo = repEnvInfo[0];
        write(masterInfo, 0, nRecords, ACK_MAJORITY);
        RepTestUtils.syncGroup(repEnvInfo);
        readAll();

        /*
         * Prohibit writes on master.
         * No records are inserted on any node.
         */
        prohibitWrites(masterInfo);
        try {
            insertOne(masterInfo, ACK_ALL);
            fail("Expected DLE");
        } catch (DiskLimitException e) {
            /* Expected. */
        }
        readAll();

        /* And then allowed again. */
        allowWrites(masterInfo);
        insertOne(masterInfo, ACK_ALL);
        readAll();

        /*
         * Prohibit writes on one replica.
         * Expect writes allowed with ack-majority but not ack-all.
         * Inserted records will only be present on two nodes.
         */
        prohibitWrites(repEnvInfo[1]);
        insertOne(masterInfo, ACK_MAJORITY);
        read(masterInfo, nRecords);
        read(repEnvInfo[2], nRecords);
        read(repEnvInfo[1], nRecords - 1);
        try {
            insertOne(masterInfo, ACK_ALL);
            fail("Expected IAE");
        } catch (InsufficientAcksException|InsufficientReplicasException e) {
            /* Expected. */
        }
        read(masterInfo, nRecords);
        read(repEnvInfo[2], nRecords);
        read(repEnvInfo[1], nRecords - 2);

        /*
         * Prohibit writes on both replicas.
         * Expect writes not allowed with ack-majority.
         * Inserted record will only be present on the master.
         */
        prohibitWrites(repEnvInfo[2]);
        try {
            insertOne(masterInfo, ACK_MAJORITY);
            fail("Expected IAE");
        } catch (InsufficientAcksException|InsufficientReplicasException e) {
            /* Expected. */
        }
        read(masterInfo, nRecords);
        read(repEnvInfo[2], nRecords - 1);
        read(repEnvInfo[1], nRecords - 3);

        /* And then allowed again. */
        allowWrites(repEnvInfo[1]);
        allowWrites(repEnvInfo[2]);
        insertOne(masterInfo, ACK_ALL);
        readAll();

        checkNodeEquality();
    }

    /**
     * Tests that checkpoints and cleaner runs are prohibited while a disk
     * limit is violated, and that eviction will not log dirty nodes.
     */
    @Test
    public void testCheckpointCleanEvict() {

        for (int i = 0; i < groupSize; i += 1) {
            final EnvironmentConfig envConfig = repEnvInfo[i].getEnvConfig();

            /* Use small files so we can clean with a small data set. */
            envConfig.setConfigParam(
                EnvironmentConfig.LOG_FILE_MAX, "1000");
        }

        RepTestUtils.joinGroup(repEnvInfo);
        final RepEnvInfo masterInfo = repEnvInfo[0];

        /* Test is not designed for non-default embedded LN param. */
        assumeTrue(masterInfo.getRepImpl().getMaxEmbeddedLN() >= 16);

        write(masterInfo, 0, nRecords, ACK_MAJORITY);
        RepTestUtils.syncGroup(repEnvInfo);
        readAll();

        for (final RepEnvInfo info : repEnvInfo) {
            /*
             * Prohibit writes and expect that a checkpoint will throw
             * DiskLimitException. Then allow writes and expect a complete
             * checkpoint.
             *
             * Prohibit writes after first CkptStart has been logged.
             */
            Checkpointer.setBeforeFlushHook(new TestHookAdapter<Object>() {
                @Override
                public void doHook() {
                    prohibitWrites(info);
                }
            });

            final CheckpointConfig config =
                new CheckpointConfig().setKBytes(1);

            final Environment env = info.getEnv();
            final long nCheckpoints = env.getStats(null).getNCheckpoints();

            for (int i = 0; i < 3; i += 1) {
                try {
                    env.checkpoint(config);
                    fail("Expected DLE");
                } catch (DiskLimitException e) {
                    /* Expected. */
                }
            }

            /* nCheckpoints is incremented at start of checkpoint. */
            assertEquals(
                nCheckpoints + 1,
                env.getStats(null).getNCheckpoints());

            allowWrites(info);
            Checkpointer.setBeforeFlushHook(null);
            env.checkpoint(config);

            assertEquals(
                nCheckpoints + 2,
                env.getStats(null).getNCheckpoints());

            /*
             * Prohibit writes and expect that a cleaner run will throw
             * DiskLimitException. Then allow writes and expect a complete
             * cleaner run.
             */
            final int minUtil = env.getStats(null).getCurrentMinUtilization();
            assertTrue("expect <= 40 minUtil=" + minUtil, minUtil <= 40);

            final long nCleanerRuns = env.getStats(null).getNCleanerRuns();

            /* Prohibit writes after first cleaner run has started. */
            final Cleaner cleaner = info.getRepImpl().getCleaner();
            cleaner.setFileChosenHook(new TestHookAdapter<Object>() {
                @Override
                public void doHook() {
                    prohibitWrites(info);
                }
            });

            for (int i = 0; i < 3; i += 1) {
                try {
                    env.cleanLog();
                    fail("Expected DLE");
                } catch (DiskLimitException e) {
                    /* Expected. */
                }
            }

            /* nCleanerRuns is incremented at start of cleaner run. */
            assertEquals(
                nCleanerRuns + 1,
                env.getStats(null).getNCleanerRuns());

            cleaner.setFileChosenHook(null);
            allowWrites(info);
            env.cleanLog();

            assertEquals(
                nCleanerRuns + 2,
                env.getStats(null).getNCleanerRuns());

            /*
             * Cleaning will have dirtied INs. Prohibit writes and expect that
             * eviction will not log dirty nodes. Then allow writes and expect
             * eviction of dirty nodes.
             */
            prohibitWrites(info);
            evictDirtyBINs(info, false /*expectEviction*/);

            allowWrites(info);
            evictDirtyBINs(info, true /*expectEviction*/);
        }

        readAll();
        checkNodeEquality();
    }

    private void insertOne(final RepEnvInfo info,
                           final Durability durability) {
        try {
            write(info, nRecords, 1, durability);
            nRecords += 1;
        } catch (InsufficientAcksException e) {
            nRecords += 1;
            throw e;
        }
    }

    private void write(final RepEnvInfo info,
                       final int startKey,
                       final int nKeys,
                       final Durability durability) {

        final ReplicatedEnvironment master = info.getEnv();
        final DatabaseEntry entry = new DatabaseEntry();

        final Database db = master.openDatabase(
            null, TEST_DB_NAME,
            new DatabaseConfig().setTransactional(true).setAllowCreate(true));

        final TransactionConfig txnConfig = new
            TransactionConfig().setDurability(durability);

        final Transaction txn = master.beginTransaction(null, txnConfig);
        try {
            for (int i = startKey; i < startKey + nKeys; i += 1) {
                IntegerBinding.intToEntry(i, entry);
                db.put(txn, entry, entry, Put.OVERWRITE, null);
            }
            txn.commit(durability);
            db.close();
        } catch (Throwable e) {
            txn.abort();
            db.close();
            throw e;
        }
    }

    private void read(final RepEnvInfo info,
                      final int nKeys) {

        final TransactionConfig txnConfig = new TransactionConfig().
            setConsistencyPolicy(new NoConsistencyRequiredPolicy()).
            setReadOnly(true);

        Transaction txn = info.getEnv().beginTransaction(null, txnConfig);

        final Database db = info.getEnv().openDatabase(
            txn, TEST_DB_NAME,
            new DatabaseConfig().setTransactional(true));

        txn.commit();

        txn = info.getEnv().beginTransaction(null, txnConfig);

        try {
            try (final Cursor cursor = db.openCursor(txn, null)) {
                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry data = new DatabaseEntry();

                int i = 0;
                while (cursor.get(key, data, Get.NEXT, null) != null) {
                    assertEquals(i, IntegerBinding.entryToInt(key));
                    assertEquals(i, IntegerBinding.entryToInt(data));
                    i += 1;
                }

                assertEquals(nKeys, i);
            }
        } finally {
            txn.commit();
        }

        db.close();
    }

    private void readAll() {
        for (final RepEnvInfo info : repEnvInfo) {
            read(info, nRecords);
        }
    }

    private void prohibitWrites(final RepEnvInfo info) {
        setMaxDisk(info, 1);
    }

    private void allowWrites(final RepEnvInfo info) {
        setMaxDisk(info, 0);
    }

    private void setMaxDisk(final RepEnvInfo info, final long size) {

        final Environment env = info.getEnv();

        env.setMutableConfig(env.getMutableConfig().setMaxDisk(size));

        try {
            info.getEnv().cleanLog();
        } catch (DiskLimitException e) {
            /* Do nothing. */
        }
    }

    private void checkNodeEquality() {

        final VLSN lastVLSN =
            repEnvInfo[0].getRepImpl().getVLSNIndex().getRange().getLast();

        try {
            RepTestUtils.checkNodeEquality(lastVLSN, false, repEnvInfo);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void evictDirtyBINs(final RepEnvInfo info,
                                final boolean expectEviction) {

        final EnvironmentImpl envImpl = info.getRepImpl();
        final StatsConfig sConfig = new StatsConfig();
        int dirtyBins = 0;

        for (final IN in : envImpl.getInMemoryINs()) {

            if (!in.isBIN() || !in.getDirty()) {
                continue;
            }

            dirtyBins += 1;

            final long origEvicted =
                envImpl.loadStats(sConfig).getNDirtyNodesEvicted();

            in.latch();

            /* Compress, strip LNs and compact. */
            in.partialEviction();
            in.partialEviction();

            final long bytes = envImpl.getEvictor().doTestEvict(
                in, Evictor.EvictionSource.MANUAL);

            final long evicted =
                envImpl.loadStats(sConfig).getNDirtyNodesEvicted() -
                    origEvicted;

            if (expectEviction) {
                assertTrue(bytes > 0);
                assertEquals(1, evicted);
            } else {
                assertEquals(0, bytes);
                assertEquals(0, evicted);
            }
        }

        assertTrue(dirtyBins > 0);
    }
}
