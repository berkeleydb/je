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
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test that the background eviction threadpool state.
 */
public class EvictionThreadPoolTest extends TestBase {
    private static final String DB_NAME = "testDB";
    private static final int ONE_MB = 1 << 20;
    private static final int MIN_DATA_SIZE = 50 * 1024;
    private static final int ENTRY_DATA_SIZE = 500;

    private final File envHome;
    private Environment env;
    private Database db;

    public EvictionThreadPoolTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        if (db != null) {
            db.close();
        }

        if (env != null) {
            env.close();
        }
        db = null;
        env = null;
    }

    @Test
    public void testPoolState()
        throws Exception {

        final int corePoolSize = 3;

        openEnv(corePoolSize);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        ThreadPoolExecutor pool = envImpl.getEvictor().getThreadPool();
        DbConfigManager configManager = envImpl.getConfigManager();

        /* Check that the configurations to the pool are applied. */
        assertEquals
            (configManager.getInt(EnvironmentParams.EVICTOR_CORE_THREADS),
             pool.getCorePoolSize());
        assertEquals
            (configManager.getInt(EnvironmentParams.EVICTOR_MAX_THREADS),
             pool.getMaximumPoolSize());               
                     
        /* Invoke the eviction thread. */
        for (int i = 1; i <= corePoolSize; i++) {
            envImpl.getEvictor().alert();
            assertEquals(pool.getPoolSize(), i);
        }

        /* Do a checkpoint to invoke daemon eviction. */
        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);
        env.checkpoint(ckptConfig);

        /* The pool size shouldn't change because no eviction should happen. */
        assertEquals(pool.getPoolSize(), corePoolSize);

        /* Do database operations to invoke critical eviction. */
        int records = write(ONE_MB * 3);
        readEvenly(records);

        /* Do checkpoint to invoke daemon eviction. */
        env.checkpoint(ckptConfig);

        /* Because of heavy eviction work, the pool should be full. */
        assertEquals(pool.getPoolSize(), pool.getMaximumPoolSize());
        
        EnvironmentStats stats = env.getStats(null);
        /* There should be some threads rejected by the pool. */
        assertTrue(stats.getNThreadUnavailable() > 0);

        /* 
         * Most of the eviction should be done by the pool thread and critical. 
         */
        assertTrue(stats.getNBytesEvictedCritical() > 0 ||
                   stats.getNBytesEvictedEvictorThread() > 0);
    }

    /* Open the Environment and database for this test. */
    private void openEnv(int corePoolSize)
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setCacheSize(ONE_MB);
        envConfig.setConfigParam("je.tree.minMemory",
                                 String.valueOf(MIN_DATA_SIZE));
        /* Configure the core pool threads. */
        envConfig.setConfigParam("je.evictor.coreThreads",
                                 String.valueOf(corePoolSize));
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    /**
     * Writes enough records in the given envIndex environment to cause at
     * least minSizeToWrite bytes to be used in the cache.
     */
    private int write(int minSizeToWrite)
        throws Exception {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[ENTRY_DATA_SIZE]);
        int i;
        for (i = 0; i < minSizeToWrite / ENTRY_DATA_SIZE; i += 1) {
            IntegerBinding.intToEntry(i, key);
            db.put(null, key, data);
        }
        return i;
    }

    /**
     * Reads alternating records from each env, reading all records from each
     * env.  Checks that all environments use roughly equal portions of the
     * cache.
     */
    private void readEvenly(int nRecs)
        throws Exception {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Repeat reads twice to give the LRU a fighting chance. */
        for (int repeat = 0; repeat < 2; repeat += 1) {
            for (int i = 0; i < nRecs; i += 1) {
                IntegerBinding.intToEntry(i, key);
                db.get(null, key, data, null);
            }
        }
    }
}
