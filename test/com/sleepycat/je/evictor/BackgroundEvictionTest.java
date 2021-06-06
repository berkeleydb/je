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

package com.sleepycat.je.evictor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test that the background eviction threadpool can appropriately handle
 * maintaining the memory budget. Do basic eviction, but turn the critical
 * percentage way up so that the app threads do no eviction, leaving the
 * work to the eviction pool.
 * Disable daemon threads (which do in-line eviction) so they are not
 * available to help with the eviction.
 */
public class BackgroundEvictionTest extends TestBase {

    private static final int N_ENVS = 5;
    private static final int ONE_MB = 1 << 20;
    private static final int ENV_CACHE_SIZE = ONE_MB;
    private static final int TOTAL_CACHE_SIZE = N_ENVS * ENV_CACHE_SIZE;

    private static final int MIN_DATA_SIZE = 50 * 1024;
    private static final int ENTRY_DATA_SIZE = 500;
    private static final String TEST_PREFIX = "BackgroundEvictionTest_";
    private static final StatsConfig CLEAR_CONFIG = new StatsConfig();
    static {
        CLEAR_CONFIG.setClear(true);
    }

    private File envHome;
    private File[] dirs;
    private Environment[] envs;
    private Database[] dbs;
    private boolean sharedCache = true;

    public BackgroundEvictionTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        dirs = new File[N_ENVS];
        envs = new Environment[N_ENVS];
        dbs = new Database[N_ENVS];

        for (int i = 0; i < N_ENVS; i += 1) {
            dirs[i] = new File(envHome, TEST_PREFIX + i);
            dirs[i].mkdir();
            assertTrue(dirs[i].isDirectory());
        }
    }

    @Override
    @After
    public void tearDown() {

        for (int i = 0; i < N_ENVS; i += 1) {
            if (dbs[i] != null) {
                try {
                    dbs[i].close();
                } catch (Throwable e) {
                    System.out.println("tearDown: " + e);
                }
                dbs[i] = null;
            }
            if (envs[i] != null) {
                try {
                    envs[i].close();
                } catch (Throwable e) {
                    System.out.println("tearDown: " + e);
                }
                envs[i] = null;
            }
        }
        envHome = null;
        dirs = null;
        envs = null;
        dbs = null;
    }

    @Test
    public void testBaseline()
        throws DatabaseException {

        /* Open all DBs in the same environment. */
        final int N_DBS = N_ENVS;
        sharedCache = false;
        openOne(0);
        DatabaseConfig dbConfig = dbs[0].getConfig();
        for (int i = 1; i < N_DBS; i += 1) {
            dbs[i] = envs[0].openDatabase(null, "foo" + i, dbConfig);
        }

        for (int i = 0; i < N_DBS; i += 1) {
            write(i, ENV_CACHE_SIZE * 2);
        }

        for (int repeat = 0; repeat < 50; repeat += 1) {

            /* Read all DBs evenly. */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            boolean done = false;
            for (int i = 0; !done; i += 1) {
                IntegerBinding.intToEntry(i, key);
                for (int j = 0; j < N_DBS; j += 1) {
                    if (dbs[j].get(null, key, data, null) !=
                        OperationStatus.SUCCESS) {
                        done = true;
                    }
                }
            }
        }

        for (int i = 1; i < N_DBS; i += 1) {
            dbs[i].close();
            dbs[i] = null;
        }

        closeOne(0);
    }

    /**
     * Checks that the background pool works correctly with shared environments.
     */
    @Test
    public void testOpenClose()
        throws DatabaseException {

        openAll();
        int nRecs = 0;
        for (int i = 0; i < N_ENVS; i += 1) {
            int n = write(i, TOTAL_CACHE_SIZE);
            if (nRecs < n) {
                nRecs = n;
            }
        }

        closeAll();
        openAll();
        readEvenly(nRecs);
        /* Close only one. */
        for (int i = 0; i < N_ENVS; i += 1) {
            closeOne(i);
            readEvenly(nRecs);
            openOne(i);
            readEvenly(nRecs);
        }
        /* Close all but one. */
        for (int i = 0; i < N_ENVS; i += 1) {
            for (int j = 0; j < N_ENVS; j += 1) {
                if (j != i) {
                    closeOne(j);
                }
            }
            readEvenly(nRecs);
            for (int j = 0; j < N_ENVS; j += 1) {
                if (j != i) {
                    openOne(j);
                }
            }
            readEvenly(nRecs);
        }
        closeAll();
    }

    private void openAll()
        throws DatabaseException {

        for (int i = 0; i < N_ENVS; i += 1) {
            openOne(i);
        }
    }

    private void openOne(int i)
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setSharedCache(sharedCache);
        envConfig.setCacheSize(TOTAL_CACHE_SIZE);
        envConfig.setConfigParam(EnvironmentConfig.TREE_MIN_MEMORY,
                                 String.valueOf(MIN_DATA_SIZE));
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                                 "false");
        envConfig.setConfigParam
            (EnvironmentParams.EVICTOR_CRITICAL_PERCENTAGE.getName(), "900");

        envConfig.setConfigParam(EnvironmentParams.STATS_COLLECT.getName(),
                "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        envs[i] = new Environment(dirs[i], envConfig);
        /* Clear the eviction stats calculated during recovery. */
        envs[i].getStats(CLEAR_CONFIG);

        dbs[i] = envs[i].openDatabase(null, "foo", dbConfig);
    }

    private void closeAll()
        throws DatabaseException {

        for (int i = 0; i < N_ENVS; i += 1) {
            closeOne(i);
        }
    }

    private void closeOne(int i)
        throws DatabaseException {

        if (dbs[i] != null) {
            dbs[i].close();
            dbs[i] = null;
        }

        if (envs[i] != null) {
            envs[i].close();
            envs[i] = null;
        }

        /* Check stats only after system is quiescent. */
        checkStatsConsistency();
    }

    /**
     * Writes enough records in the given envIndex environment to cause at
     * least minSizeToWrite bytes to be used in the cache.
     */
    private int write(int envIndex, int minSizeToWrite)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[ENTRY_DATA_SIZE]);
        int i;
        for (i = 0; i < minSizeToWrite / ENTRY_DATA_SIZE; i += 1) {
            IntegerBinding.intToEntry(i, key);
            dbs[envIndex].put(null, key, data);
        }
        return i;
    }

    /**
     * Reads alternating records from each env, reading all records from each
     * env.  Checks that all environments use roughly equal portions of the
     * cache.
     */
    private void readEvenly(int nRecs)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Repeat reads twice to give the LRU a fighting chance. */
        for (int repeat = 0; repeat < 2; repeat += 1) {
            for (int i = 0; i < nRecs; i += 1) {
                IntegerBinding.intToEntry(i, key);
                for (int j = 0; j < N_ENVS; j += 1) {
                    if (dbs[j] != null) {
                        dbs[j].get(null, key, data, null);
                    }
                }
            }
        }
    }

    /**
     * Checks that all eviction was done with the background pool.
     */
    private void checkStatsConsistency()
        throws DatabaseException {

        EnvironmentStats stats = null;

        for (int i = 0; i < N_ENVS; i += 1) {
            if (envs[i] != null) {
                stats = envs[i].getStats(null);
                assertEquals(0, stats.getNBytesEvictedCritical());
                assertEquals(0, stats.getNBytesEvictedCacheMode());
                assertEquals(0, stats.getNBytesEvictedManual());
                assertTrue(stats.getNBytesEvictedEvictorThread() > 0);
                assertTrue("cacheTotalBytes=" + stats.getCacheTotalBytes() +
                           " maxMem=" + TOTAL_CACHE_SIZE,
                           stats.getCacheTotalBytes() < TOTAL_CACHE_SIZE);
            }
        }
    }
}
