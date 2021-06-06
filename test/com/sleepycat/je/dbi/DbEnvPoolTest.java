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

package com.sleepycat.je.dbi;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHookAdapter;
import com.sleepycat.util.test.TestBase;

public class DbEnvPoolTest extends TestBase {
    private static final String dbName = "testDb";
    private static final String envHomeBName = "envB";

    private final File envHomeA;
    private final File envHomeB;

    public DbEnvPoolTest() {
        envHomeA = new File(System.getProperty(TestUtils.DEST_DIR));
        envHomeB = new File(envHomeA, envHomeBName);
    }

    @Before
    public void setUp() 
        throws Exception {

        TestUtils.removeLogFiles("Setup", envHomeA, false);
        if (envHomeB.exists()) {
            clearEnvHomeB();
        } else {
            envHomeB.mkdir();
        }
        super.setUp();
    }

    @After
    public void tearDown() {
        TestUtils.removeLogFiles("TearDown", envHomeA, false);
        clearEnvHomeB();
    }

    private void clearEnvHomeB() {
        File[] logFiles = envHomeB.listFiles();
        for (File logFile : logFiles) {
            assertTrue(logFile.delete());
        }
    }

    @Test
    public void testCanonicalEnvironmentName ()
        throws Throwable {

        try {
            /* Create an environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            Environment envA = new Environment(envHomeA, envConfig);

            /* Look in the environment pool with the relative path name. */
            File file2 = new File("build/test/classes");
            assertTrue(DbEnvPool.getInstance().isOpen(file2));

            envA.close();

        } catch (Throwable t) {
            /* Dump stack trace before trying to tear down. */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test that SharedCache Environments really shares cache.
     */
    @Test
    public void testSharedCacheEnv()
        throws Throwable {

        OpenEnvThread threadA = null;
        OpenEnvThread threadB = null;
        try {
            CountDownLatch awaitLatch = new CountDownLatch(1);
            AwaitHook hook = new AwaitHook(awaitLatch);

            threadA = new OpenEnvThread("threadA", envHomeA, hook, 10);
            threadB = new OpenEnvThread("threadB", envHomeB, hook, 1000);

            threadA.start();
            threadB.start();

            /* 
             * Make sure that all two threads have finished the first 
             * synchronization block. 
             */
            while (DbEnvPool.getInstance().getEnvImpls().size() != 2) {
            }

            /* Count down the latch so that Environment creation is done. */
            awaitLatch.countDown();

            threadA.finishTest();
            threadB.finishTest();

            Environment envA = threadA.getEnv();
            Environment envB = threadB.getEnv();

            /* Check the two Environments using the same SharedEvictor. */
            assertTrue(DbInternal.getNonNullEnvImpl(envA).getEvictor() ==
                       DbInternal.getNonNullEnvImpl(envB).getEvictor());
            
            StatsConfig stConfig = new StatsConfig();
            stConfig.setFast(true);
            assertTrue(envA.getConfig().getCacheSize() == 
                       envB.getConfig().getCacheSize());
            /* Check the shared cache total bytes are the same. */
            assertTrue(envA.getStats(stConfig).getSharedCacheTotalBytes() ==
                       envB.getStats(stConfig).getSharedCacheTotalBytes());
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (threadA != null) {
                threadA.closeEnv();
            }

            if (threadB != null) {
                threadB.closeEnv();
            }
        }
    }

    /* Thread used to opening two environments. */
    private static class OpenEnvThread extends JUnitThread {
        private final File envHome;
        private final AwaitHook awaitHook;
        private final int dbSize;
        private Environment env;
        private Database db;

        public OpenEnvThread(String threadName, 
                             File envHome, 
                             AwaitHook awaitHook,
                             int dbSize) {
            super(threadName);
            this.envHome = envHome;
            this.awaitHook = awaitHook;
            this.dbSize = dbSize;
        }

        @Override
        public void testBody()
            throws Throwable {

            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setSharedCache(true);

            DbEnvPool.getInstance().setBeforeFinishInitHook(awaitHook);

            env = new Environment(envHome, envConfig);                

            /* 
             * Write different data on different environments to check the
             * shared cache total bytes. 
             */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            db = env.openDatabase(null, dbName, dbConfig);

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = 1; i <= dbSize; i++) {
                IntegerBinding.intToEntry(i, key);
                StringBinding.stringToEntry("herococo", data);
                db.put(null, key, data);
            }
        }

        public Environment getEnv() {
            return env;
        }

        public void closeEnv() {
            if (db != null) {
                db.close();
            }
            
            if (env != null) {
                env.close();
            }
        }
    }

    private static class AwaitHook extends TestHookAdapter<EnvironmentImpl> {
        private final CountDownLatch awaitLatch;

        public AwaitHook(CountDownLatch awaitLatch) {
            this.awaitLatch = awaitLatch;
        }

        @Override
        public void doHook(EnvironmentImpl unused) {
            try {
                awaitLatch.await();
            } catch (InterruptedException e) {
                /* should never happen */
            }
        }
    }
}

