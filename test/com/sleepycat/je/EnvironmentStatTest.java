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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

public class EnvironmentStatTest extends TestBase {

    private final File envHome;
    private static final String DB_NAME = "foo";

    public EnvironmentStatTest() {
        envHome =  SharedTestUtils.getTestDir();
    }

    /**
     * Basic cache management stats.
     */
    @Test
    public void testCacheStats()
        throws Exception {

        /* Init the Environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        Environment env = new Environment(envHome, envConfig);

        EnvironmentStats stat = env.getStats(TestUtils.FAST_STATS);
        env.close();
        assertEquals(0, stat.getNCacheMiss());
        assertEquals(0, stat.getNNotResident());

        // Try to open and close again, now that the environment exists
        envConfig.setAllowCreate(false);
        env = new Environment(envHome, envConfig);

        /* Open a database and insert some data. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, DB_NAME, dbConfig);
        db.put(null, new DatabaseEntry(new byte[0]),
                     new DatabaseEntry(new byte[0]));
        Transaction txn = env.beginTransaction(null, null);
        db.put(txn, new DatabaseEntry(new byte[0]),
                    new DatabaseEntry(new byte[0]));

        /* Do the check. */
        stat = env.getStats(TestUtils.FAST_STATS);
        MemoryBudget mb =
            DbInternal.getNonNullEnvImpl(env).getMemoryBudget();

        assertEquals(mb.getCacheMemoryUsage(), stat.getCacheTotalBytes());

        /*
         * The size of each log buffer is calculated by:
         * mb.logBufferBudget/numBuffers, which is rounded down to the nearest
         * integer. The stats count the precise capacity of the log
         * buffers. Because of rounding down, the memory budget may be slightly
         * > than the stats buffer bytes, but the difference shouldn't be
         * greater than the numBuffers.
         */
        assertTrue((mb.getLogBufferBudget() - stat.getBufferBytes() <=
                    stat.getNLogBuffers()));
        assertEquals(mb.getTreeMemoryUsage() + mb.getTreeAdminMemoryUsage(),
                     stat.getDataBytes());
        assertEquals(mb.getLockMemoryUsage(), stat.getLockBytes());
        assertEquals(mb.getAdminMemoryUsage(), stat.getAdminBytes());

        assertTrue(stat.getBufferBytes() > 0);
        assertTrue(stat.getDataBytes() > 0);
        assertTrue(stat.getLockBytes() > 0);
        assertTrue(stat.getAdminBytes() > 0);

        /* Account for rounding down when calculating log buffer size. */
        assertTrue(stat.getCacheTotalBytes() -
                   (stat.getBufferBytes() +
                     stat.getDataBytes() +
                     stat.getLockBytes() +
                    stat.getAdminBytes()) <= stat.getNLogBuffers());

        assertTrue(stat.getNCacheMiss() > 10);
        assertTrue(stat.getNNotResident() > 10);

        /* Test deprecated getCacheDataBytes method. */
        final EnvironmentStats finalStat = stat;
        final long expectCacheDataBytes = mb.getCacheMemoryUsage() -
                                          mb.getLogBufferBudget();
        (new Runnable() {
            @Deprecated
            public void run() {
                assertTrue((expectCacheDataBytes -
                           finalStat.getCacheDataBytes()) <=
                               finalStat.getNLogBuffers());
            }
        }).run();

        txn.abort();
        db.close();
        env.close();
    }

    /**
     * Test that fetching a LN larger than LOG_FAULT_READ_SIZE does not cause a
     * repeat-fault-read, since the lastLoggedSize is known. Fetching large INs
     * however, will currently cause a cause a repeat-fault-read.
     */
    @Test
    public void testRepeatFaultReads()
        throws Exception {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        /* Use EVICT_LN to fetch LNs via Database.get(). */
        envConfig.setCacheMode(CacheMode.EVICT_LN);
        /* Do not use the off-heap cache to fetch from disk. */
        envConfig.setOffHeapCacheSize(0);
        /* Disable daemon threads for reliability. */
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        final Environment env = new Environment(envHome, envConfig);

        final int smallSize = 100;
        final int bigSize = 100 * 1024;

        final int readSize = Integer.parseInt(
            env.getConfig().getConfigParam(
                EnvironmentConfig.LOG_FAULT_READ_SIZE));

        assertTrue(readSize < bigSize);
        assertTrue(readSize > smallSize);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        final Database db = env.openDatabase(null, DB_NAME, dbConfig);

        final DatabaseEntry key = new DatabaseEntry(new byte[0]);
        final DatabaseEntry data = new DatabaseEntry();

        /*
         * With embedded data, no fetch is needed.
         */
        data.setData(new byte[0]);
        OperationResult result = db.put(null, key, data, Put.OVERWRITE, null);
        assertNotNull(result);

        clearLogBuffers(env);
        env.getStats(StatsConfig.CLEAR);
        result = db.get(null, key, data, Get.SEARCH, null);
        assertNotNull(result);
        EnvironmentStats stats = env.getStats(StatsConfig.CLEAR);

        assertEquals(0, stats.getNLNsFetch());
        assertEquals(0, stats.getNLNsFetchMiss());
        assertEquals(0, stats.getNRepeatFaultReads());

        /*
         * With data smaller than read size, the fetch is not a repeat fault.
         */
        data.setData(new byte[smallSize]);
        result = db.put(null, key, data, Put.OVERWRITE, null);
        assertNotNull(result);

        clearLogBuffers(env);
        env.getStats(StatsConfig.CLEAR);
        result = db.get(null, key, data, Get.SEARCH, null);
        assertNotNull(result);
        stats = env.getStats(StatsConfig.CLEAR);

        assertEquals(1, stats.getNLNsFetch());
        assertEquals(1, stats.getNLNsFetchMiss());
        assertEquals(0, stats.getNRepeatFaultReads());

        /*
         * With data larger than read size, the fetch is still not a repeat
         * fault because the lastLoggedSize is known for LNs.
         */
        data.setData(new byte[bigSize]);
        result = db.put(null, key, data, Put.OVERWRITE, null);
        assertNotNull(result);

        clearLogBuffers(env);
        env.getStats(StatsConfig.CLEAR);
        result = db.get(null, key, data, Get.SEARCH, null);
        assertNotNull(result);
        stats = env.getStats(StatsConfig.CLEAR);

        assertEquals(1, stats.getNLNsFetch());
        assertEquals(1, stats.getNLNsFetchMiss());
        assertEquals(0, stats.getNRepeatFaultReads());

        /*
         * Fetching an IN larger than read size will cause a repeat fault
         * because the lastLoggedSize is not known for INs.
         */
        key.setData(new byte[bigSize]);
        data.setData(new byte[0]);
        result = db.put(null, key, data, Put.OVERWRITE, null);
        assertNotNull(result);

        /* Flush BIN to disk, since a dirty BIN cannot be evicted. */
        env.checkpoint(
            new CheckpointConfig().
                setMinimizeRecoveryTime(true).
                setForce(true));

        /* Use EVICT_BIN to evict the non-dirty BIN. */
        result = db.get(
            null, key, data, Get.SEARCH,
            new ReadOptions().setCacheMode(CacheMode.EVICT_BIN));
        assertNotNull(result);

        clearLogBuffers(env);
        env.getStats(StatsConfig.CLEAR);
        result = db.get(null, key, data, Get.SEARCH, null);
        assertNotNull(result);
        stats = env.getStats(StatsConfig.CLEAR);

        assertEquals(0, stats.getNLNsFetch());
        assertEquals(0, stats.getNLNsFetchMiss());
        assertEquals(1, stats.getNBINsFetch());
        assertEquals(1, stats.getNBINsFetchMiss());
        assertEquals(1, stats.getNRepeatFaultReads());

        db.close();
        env.close();
    }

    /**
     * Use internal APIs to clear the log buffer pool, to ensure that a fetch
     * is performed from the file system.
     */
    private void clearLogBuffers(final Environment env) {
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        envImpl.getLogManager().resetPool(envImpl.getConfigManager());
    }

    /**
     * Check stats to see if we correctly record nLogFsyncs (any fsync of the
     * log) and nFSyncs(commit fsyncs)
     */
    @Test
    public void testFSyncStats()
        throws Exception {

        /* The usual env and db setup */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        Environment env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        Database db = env.openDatabase(null, "foo", dbConfig);
        DatabaseEntry value = new DatabaseEntry();
        Transaction txn = env.beginTransaction(null, null);
        IntegerBinding.intToEntry(10, value);
        db.put(txn, value, value);

        StatsConfig statConfig = new StatsConfig();
        statConfig.setClear(true);
        /* Get a snapshot of the stats, for use as the starting point. */
        EnvironmentStats start = env.getStats(statConfig);

        /*
         * The call to env.sync() provokes a ckpt, which does a group mgr type
         * commit, so both getNFsyncs and nLogFSyncs are incremented.
         */
        env.sync();
        EnvironmentStats postSync = env.getStats(statConfig);
        assertEquals(1, postSync.getNFSyncs());
        assertEquals(1, postSync.getNLogFSyncs());

        /* Should be a transaction related fsync */
        txn.commitSync();
        EnvironmentStats postCommit = env.getStats(statConfig);
        assertEquals(1, postCommit.getNFSyncs());
        assertEquals(1, postCommit.getNLogFSyncs());

        /* Should be a transaction related fsync */
        DbInternal.getNonNullEnvImpl(env).forceLogFileFlip();
        EnvironmentStats postFlip = env.getStats(statConfig);
        assertEquals(0, postFlip.getNFSyncs());
        assertEquals(1, postCommit.getNLogFSyncs());

        /* Call api to test that cast exception does not occur [#23060] */
        postFlip.getAvgBatchManual();

        db.close();
        env.close();
    }

    /*
     * Test that the Database.sync() and Database.close() won't do a fsync if
     * there is no updates on the database.
     */
    @Test
    public void testDbFSyncs()
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);

        Environment env = new Environment(envHome, envConfig);

        /* Check the normal database. */
        checkCloseFSyncs(env, false);

        /* Flush a new log file to make sure the next check would succeed. */
        DbInternal.getNonNullEnvImpl(env).forceLogFileFlip();

        /* Check the dw database. */
        checkCloseFSyncs(env, true);

        env.close();
    }

    private void checkCloseFSyncs(Environment env, boolean deferredWrite)
        throws Exception {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(deferredWrite);

        Database[] dbs = new Database[1000];

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < 1000; i++) {
            dbs[i] = env.openDatabase(null, "db" + i, dbConfig);
            IntegerBinding.intToEntry(i, key);
            StringBinding.stringToEntry("herococo", data);
            dbs[i].put(null, key, data);
        }

        StatsConfig stConfig = new StatsConfig();
        stConfig.setClear(true);

        EnvironmentStats envStats = env.getStats(stConfig);
        assertTrue(envStats.getNLogFSyncs() > 0);

        env.sync();

        envStats = env.getStats(stConfig);

        /*
         * The log file size is default 10M, 1000 records should be written in
         * the same log file.
         */
        assertTrue(envStats.getNLogFSyncs() == 1);

        if (deferredWrite) {
            for (int i = 0; i < 1000; i++) {
                dbs[i].sync();
            }
            envStats = env.getStats(stConfig);
            assertTrue(envStats.getNLogFSyncs() == 0);
        }

        for (int i = 0; i < 1000; i++) {
            dbs[i].close();
        }

        /*
         * Test no matter the database is deferred write or not, no log fsyncs
         * if no changes made before closing it.
         */
        envStats = env.getStats(stConfig);
        assert(envStats.getNLogFSyncs() == 0);
    }
}
