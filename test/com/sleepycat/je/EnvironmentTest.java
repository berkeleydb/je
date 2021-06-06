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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchFactory;
import com.sleepycat.je.txn.LockInfo;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DaemonRunner;
import com.sleepycat.util.test.SharedTestUtils;

import org.junit.Test;

public class EnvironmentTest extends DualTestCase {

    private Environment env1;
    private Environment env2;
    private Environment env3;
    private final File envHome;

    public EnvironmentTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Prints Java version as information for debugging.
     */
    @Test
    public void testDisplayJavaVersion() {
        System.out.println(
            "Java version: " + System.getProperty("java.version") +
            " Vendor: " + System.getProperty("java.vendor"));
    }

    /**
     * Test open and close of an environment.
     */
    @Test
    public void testBasic()
        throws Throwable {

        try {
            assertEquals("Checking version", "7.5.11",
                         JEVersion.CURRENT_VERSION.getVersionString());

            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setCacheSize(MemoryBudget.MIN_MAX_MEMORY_SIZE);
            /* Don't track detail with a tiny cache size. */
            envConfig.setConfigParam
                (EnvironmentParams.CLEANER_TRACK_DETAIL.getName(), "false");
            envConfig.setConfigParam
                (EnvironmentParams.NODE_MAX.getName(), "6");
            envConfig.setConfigParam
                (EnvironmentParams.LOG_MEM_SIZE.getName(),
                 EnvironmentParams.LOG_MEM_SIZE_MIN_STRING);
            envConfig.setConfigParam
                (EnvironmentParams.NUM_LOG_BUFFERS.getName(), "2");
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            close(env1);

            /* Try to open and close again, now that the environment exists. */
            envConfig.setAllowCreate(false);
            env1 = create(envHome, envConfig);
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test creation of a reserved name fails.
     */
    @Test
    public void testNoCreateReservedNameDB()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            try {
                env1.openDatabase(null, DbType.VLSN_MAP.getInternalName(),
                                  dbConfig);
                fail("expected DatabaseException since Environment not " +
                     "transactional");
            } catch (IllegalArgumentException IAE) {
            }

            close(env1);

            /* Try to open and close again, now that the environment exists. */
            envConfig.setAllowCreate(false);
            env1 = create(envHome, envConfig);
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test environment reference counting.
     */
    @Test
    public void testReferenceCounting()
        throws Throwable {

        try {

            /* Create two environment handles on the same environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setCacheSize(MemoryBudget.MIN_MAX_MEMORY_SIZE);
            /* Don't track detail with a tiny cache size. */
            envConfig.setConfigParam
                (EnvironmentParams.CLEANER_TRACK_DETAIL.getName(), "false");
            envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                     "6");
            envConfig.setConfigParam
            (EnvironmentParams.LOG_MEM_SIZE.getName(),
                    EnvironmentParams.LOG_MEM_SIZE_MIN_STRING);
            envConfig.setConfigParam
            (EnvironmentParams.NUM_LOG_BUFFERS.getName(), "2");
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);
            envConfig.setAllowCreate(false);
            env2 = create(envHome, envConfig);

            assertEquals("DbEnvironments should be equal",
                         env1.getNonNullEnvImpl(),
                         env2.getNonNullEnvImpl());

            /* Try to close one of them twice */
            EnvironmentImpl dbenv1 = env1.getNonNullEnvImpl();
            close(env1);
            try {
                close(env1);
            } catch (DatabaseException DENOE) {
                fail("Caught DatabaseException while re-closing " +
                     "an Environment.");
            }

            /*
             * Close both, open a third handle, should get a new
             * EnvironmentImpl.
             */
            close(env2);
            env1 = create(envHome, envConfig);
            assertTrue("EnvironmentImpl did not change",
                       dbenv1 != env1.getNonNullEnvImpl());
            try {
                close(env2);
            } catch (DatabaseException DENOE) {
                fail("Caught DatabaseException while re-closing " +
                     "an Environment.");
            }
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testTransactional()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            try {
                env1.beginTransaction(null, null);
                fail("should have thrown exception for non transactional "+
                     " environment");
            } catch (UnsupportedOperationException expected) {
            }

            String databaseName = "simpleDb";
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            try {
                env1.openDatabase(null, databaseName, dbConfig);
                fail("expected IllegalArgumentException since Environment " +
                     " not transactional");
            } catch (IllegalArgumentException expected) {
            }

            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Checks that Environment.flushLog writes buffered data to the log.
     * [#19111]
     */
    @Test
    public void testFlushLog() {

        /* Open transactional environment. */
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setConfigParam(EnvironmentConfig.LOG_USE_WRITE_QUEUE,
                                 "true");
        env1 = create(envHome, envConfig);
        EnvironmentImpl envImpl = env1.getNonNullEnvImpl();
        final boolean isReplicated = envImpl.isReplicated();

        /* Open transactional database. */
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setReplicated(true);
        Database db1 = env1.openDatabase(null, "db1", dbConfig);

        /* Insert into database without flushing. */
        final DatabaseEntry key = new DatabaseEntry(new byte[10]);
        final DatabaseEntry data = new DatabaseEntry(new byte[10]);
        OperationStatus status = db1.putNoOverwrite(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);

        /* Same for non-transactional database. */
        Database db2 = null;
        if (!isReplicated) {
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(false);
            dbConfig.setReplicated(false);
            db2 = env1.openDatabase(null, "db2", dbConfig);
            status = db2.putNoOverwrite(null, key, data);
            assertSame(OperationStatus.SUCCESS, status);
        }

        /* Flush. */
        env1.flushLog(false /*fsync*/);
        assertFalse(envImpl.getFileManager().hasQueuedWrites());
        env1.flushLog(true /*fsync*/);

        /* Crash, re-open and check. */
        abnormalClose(env1);
        env1 = create(envHome, envConfig);
        envImpl = env1.getNonNullEnvImpl();
        dbConfig.setTransactional(true);
        dbConfig.setReplicated(true);
        db1 = env1.openDatabase(null, "db1", dbConfig);
        status = db1.get(null, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        if (!isReplicated) {
            dbConfig.setTransactional(false);
            dbConfig.setReplicated(false);
            db2 = env1.openDatabase(null, "db2", dbConfig);
            status = db2.get(null, key, data, null);
            assertSame(OperationStatus.SUCCESS, status);
        }

        db1.close();
        if (!isReplicated) {
            db2.close();
        }
        close(env1);
    }

    @Test
    public void testReadOnly()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setReadOnly(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            String databaseName = "simpleDb";
            try {
                env1.openDatabase(null, databaseName, dbConfig);
                fail("expected DatabaseException since Environment is " +
                     "readonly");
            } catch (IllegalArgumentException expected) {
            }

            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testReadOnlyDbNameOps()
        throws DatabaseException {

        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        /* Create read-write env and DB, insert one record, close. */
        {
            final Environment envRW = create(envHome, envConfig);
            Database db1 = envRW.openDatabase(null, "db1", dbConfig);
            final DatabaseEntry dk = new DatabaseEntry(new byte[10]);
            final DatabaseEntry dv = new DatabaseEntry(new byte[10]);
            db1.put(null, dk,dv);
            assertEquals(1, db1.count());
            db1.close();
            close(envRW);
        }

        /* Open the env read-only. */
        envConfig.setReadOnly(true);
        envConfig.setAllowCreate(false);
        final Environment envRO = create(envHome, envConfig);
        assertTrue(envRO.getConfig().getReadOnly());

        /* Check that truncate, remove and rename are not supported. */
        try {
            envRO.truncateDatabase(null, "db1", true);
            fail();
        } catch (UnsupportedOperationException expected) {
        }
        try {
            envRO.removeDatabase(null, "db1");
            fail();
        } catch (UnsupportedOperationException expected) {
        }
        try {
            envRO.renameDatabase(null, "db1", "db2");
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        /* Make sure the DB is still intact. */
        dbConfig.setReadOnly(true);
        dbConfig.setAllowCreate(false);
        Database db1 = envRO.openDatabase(null, "db1", dbConfig);
        assertEquals(1, db1.count());
        db1.close();
        close(envRO);
    }

    /*
     * Tests memOnly mode with a home dir that does not exist. [#15255]
     */
    @Test
    public void testMemOnly()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam
            (EnvironmentParams.LOG_MEMORY_ONLY.getName(), "true");

        File noHome = new File("fileDoesNotExist");
        assertTrue(!noHome.exists());
        env1 = create(noHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db = env1.openDatabase(null, "foo", dbConfig);

        Transaction txn = env1.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        doSimpleCursorPutAndDelete(cursor, false);
        cursor.close();
        txn.commit();
        db.close();

        close(env1);
        assertTrue(!noHome.exists());
    }

    /**
     * Tests that opening an environment after a clean close does not add to
     * the log.
     */
    @Test
    public void testOpenWithoutCheckpoint()
        throws Throwable {

        /* Open, close, open. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        env1 = create(envHome, envConfig);
        close(env1);
        env1 = create(envHome, null);

        /* Check that no checkpoint was performed. */
        EnvironmentStats stats = env1.getStats(null);
        assertEquals(0, stats.getNCheckpoints());

        close(env1);
        env1 = null;
    }

    /**
     * Test environment configuration.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testConfig()
        throws Throwable {

        /* This tests assumes these props are immutable. */
        assertTrue(!isMutableConfig("je.lock.timeout"));
        assertTrue(!isMutableConfig("je.env.isReadOnly"));

        try {

            /*
             * Make sure that the environment keeps its own copy of the
             * configuration object.
             */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setReadOnly(true);
            envConfig.setAllowCreate(true);
            envConfig.setLockTimeout(7777);
            env1 = create(envHome, envConfig);

            /*
             * Change the environment config object, make sure the
             * environment cloned a copy when it was opened.
             */
            envConfig.setReadOnly(false);
            EnvironmentConfig retrievedConfig1 = env1.getConfig();
            assertTrue(envConfig != retrievedConfig1);
            assertEquals(true, retrievedConfig1.getReadOnly());
            assertEquals(true, retrievedConfig1.getAllowCreate());
            assertEquals(7000, retrievedConfig1.getLockTimeout());
            assertEquals
                (7, retrievedConfig1.getLockTimeout(TimeUnit.MILLISECONDS));

            /*
             * Make sure that the environment returns a cloned config
             * object when you call Environment.getConfig.
             */
            retrievedConfig1.setReadOnly(false);
            EnvironmentConfig retrievedConfig2 = env1.getConfig();
            assertEquals(true, retrievedConfig2.getReadOnly());
            assertTrue(retrievedConfig1 != retrievedConfig2);

            /*
             * Open a second environment handle, check that its attributes
             * are available.
             */
            env2 = create(envHome, null);
            EnvironmentConfig retrievedConfig3 = env2.getConfig();
            assertEquals(true, retrievedConfig3.getReadOnly());
            assertEquals(7000, retrievedConfig3.getLockTimeout());

            /*
             * Open an environment handle on an existing environment with
             * mismatching config params.
             */
            try {
                create(envHome, TestUtils.initEnvConfig());
                fail("Shouldn't open, config param has wrong number of params");
            } catch (IllegalArgumentException e) {
                /* expected */
            }

            try {
                envConfig.setLockTimeout(8888);
                create(envHome, envConfig);
                fail("Shouldn't open, cache size doesn't match");
            } catch (IllegalArgumentException e) {
                /* expected */
            }

            /*
             * Ditto for the mutable attributes.
             */
            EnvironmentMutableConfig mutableConfig =
                new EnvironmentMutableConfig();
            mutableConfig.setTxnNoSync(true);
            env1.setMutableConfig(mutableConfig);
            EnvironmentMutableConfig retrievedMutableConfig1 =
                env1.getMutableConfig();
            assertTrue(mutableConfig != retrievedMutableConfig1);
            retrievedMutableConfig1.setTxnNoSync(false);
            EnvironmentMutableConfig retrievedMutableConfig2 =
                env1.getMutableConfig();
            assertEquals(true, retrievedMutableConfig2.getTxnNoSync());
            assertTrue(retrievedMutableConfig1 != retrievedMutableConfig2);

            /*
             * Plus check that mutables can be retrieved via the main config.
             */
            EnvironmentConfig retrievedConfig4 = env1.getConfig();
            assertEquals(true, retrievedConfig4.getTxnNoSync());
            retrievedConfig4 = env2.getConfig();
            assertEquals(false, retrievedConfig4.getTxnNoSync());

            /*
             * Check that mutables can be passed to the ctor.
             */
            EnvironmentConfig envConfig3 = env2.getConfig();
            assertEquals(false, envConfig3.getTxnNoSync());
            envConfig3.setTxnNoSync(true);
            env3 = create(envHome, envConfig3);
            EnvironmentMutableConfig retrievedMutableConfig3 =
                env3.getMutableConfig();
            assertNotSame(envConfig3, retrievedMutableConfig3);
            assertEquals(true, retrievedMutableConfig3.getTxnNoSync());
            close(env1);
            close(env2);
            close(env3);
            env1 = null;
            env2 = null;
            env3 = null;
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test the semantics of env-wide mutable config properties.
     */
    @Test
    public void testMutableConfig()
        throws DatabaseException {

        /*
         * Note that during unit testing the shared je.properties is expected
         * to be empty, so we don't test the application of je.properties here.
         */
        final String P1 = EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName();
        final String P2 = EnvironmentParams.ENV_RUN_CLEANER.getName();
        final String P3 = EnvironmentParams.ENV_RUN_CHECKPOINTER.getName();

        assertTrue(isMutableConfig(P1));
        assertTrue(isMutableConfig(P2));
        assertTrue(isMutableConfig(P3));

        EnvironmentConfig config;
        EnvironmentMutableConfig mconfig;

        /*
         * Create env1, first handle.
         * P1 defaults to true.
         * P2 is set to true (the default).
         * P3 is set to false (not the default).
         */
        config = TestUtils.initEnvConfig();
        config.setAllowCreate(true);
        config.setConfigParam(P2, "true");
        config.setConfigParam(P3, "false");
        env1 = create(envHome, config);
        check3Params(env1, P1, "true", P2, "true", P3, "false");

        MyObserver observer = new MyObserver();
        env1.getNonNullEnvImpl().addConfigObserver(observer);
        assertEquals(0, observer.testAndReset());

        /*
         * Open env2, second handle, test that no mutable params can be
         * overridden.
         * P1 is set to false.
         * P2 is set to false.
         * P3 is set to true.
         */
        config = TestUtils.initEnvConfig();
        config.setConfigParam(P1, "false");
        config.setConfigParam(P2, "false");
        config.setConfigParam(P3, "true");
        env2 = create(envHome, config);
        assertEquals(0, observer.testAndReset());
        check3Params(env1, P1, "true", P2, "true", P3, "false");

        /*
         * Set mutable config explicitly.
         */
        mconfig = env2.getMutableConfig();
        mconfig.setConfigParam(P1, "false");
        mconfig.setConfigParam(P2, "false");
        mconfig.setConfigParam(P3, "true");
        env2.setMutableConfig(mconfig);
        assertEquals(1, observer.testAndReset());
        check3Params(env2, P1, "false", P2, "false", P3, "true");

        close(env1);
        env1 = null;
        close(env2);
        env2 = null;
    }

    /**
     * Checks that je.txn.deadlockStackTrace is mutable and takes effect.
     */
    @Test
    public void testTxnDeadlockStackTrace()
        throws DatabaseException {

        String name = EnvironmentParams.TXN_DEADLOCK_STACK_TRACE.getName();
        assertTrue(isMutableConfig(name));

        EnvironmentConfig config = TestUtils.initEnvConfig();
        config.setAllowCreate(true);
        config.setConfigParam(name, "true");
        env1 = create(envHome, config);
        assertTrue(LockInfo.getDeadlockStackTrace());

        EnvironmentMutableConfig mconfig = env1.getMutableConfig();
        mconfig.setConfigParam(name, "false");
        env1.setMutableConfig(mconfig);
        assertTrue(!LockInfo.getDeadlockStackTrace());

        mconfig = env1.getMutableConfig();
        mconfig.setConfigParam(name, "true");
        env1.setMutableConfig(mconfig);
        assertTrue(LockInfo.getDeadlockStackTrace());

        close(env1);
        env1 = null;
    }

    /**
     * Checks three config parameter values.
     */
    private void check3Params(Environment env,
                              String p1, String v1,
                              String p2, String v2,
                              String p3, String v3)
        throws DatabaseException {

        EnvironmentConfig config = env.getConfig();

        assertEquals(v1, config.getConfigParam(p1));
        assertEquals(v2, config.getConfigParam(p2));
        assertEquals(v3, config.getConfigParam(p3));

        EnvironmentMutableConfig mconfig = env.getMutableConfig();

        assertEquals(v1, mconfig.getConfigParam(p1));
        assertEquals(v2, mconfig.getConfigParam(p2));
        assertEquals(v3, mconfig.getConfigParam(p3));
    }

    /**
     * Returns whether a config parameter is mutable.
     */
    private boolean isMutableConfig(String name) {
        ConfigParam param = EnvironmentParams.SUPPORTED_PARAMS.get(name);
        assert param != null;
        return param.isMutable();
    }

    /**
     * Observes config changes and remembers how many times it was called.
     */
    private static class MyObserver implements EnvConfigObserver {

        private int count = 0;

        public void envConfigUpdate(DbConfigManager mgr,
                EnvironmentMutableConfig ignore) {
            count += 1;
        }

        int testAndReset() {
            int result = count;
            count = 0;
            return result;
        }
    }

    /**
     * Make sure that config param loading follows the right precedence.
     */
    @Test
    public void testParamLoading()
        throws Throwable {

        File testEnvHome = null;
        try {

            /*
             * A je.properties file has been put into
             * <testdestdir>/propTest/je.properties
             */
            StringBuilder testPropsEnv = new StringBuilder();
            testPropsEnv.append(SharedTestUtils.getTestDir().getParent());
            testPropsEnv.append(File.separatorChar);
            testPropsEnv.append("propTest");
            testEnvHome = new File(testPropsEnv.toString());
            TestUtils.removeLogFiles("testParamLoading start",
                                     testEnvHome, false);

            /*
             * Set some configuration params programatically.  Do not use
             * TestUtils.initEnvConfig since we're counting properties.
             */
            EnvironmentConfig appConfig = new EnvironmentConfig();
            appConfig.setConfigParam("je.log.numBuffers", "88");
            appConfig.setConfigParam
            ("je.log.totalBufferBytes",
                    EnvironmentParams.LOG_MEM_SIZE_MIN_STRING + 10);
            appConfig.setConfigParam("je.txn.durability",
                                     "sync,sync,simple_majority");
            appConfig.setAllowCreate(true);

            Environment appEnv = create(testEnvHome, appConfig);
            EnvironmentConfig envConfig = appEnv.getConfig();

            assertEquals(4, envConfig.getNumExplicitlySetParams());
            assertEquals("false",
                         envConfig.getConfigParam("je.env.recovery"));
            assertEquals("7001",
                         envConfig.getConfigParam("je.log.totalBufferBytes"));
            assertEquals("200",
                         envConfig.getConfigParam("je.log.numBuffers"));
            assertEquals("NO_SYNC,NO_SYNC,NONE",
                         envConfig.getConfigParam("je.txn.durability"));
            assertEquals(new Durability(SyncPolicy.NO_SYNC,
                                        SyncPolicy.NO_SYNC,
                                        ReplicaAckPolicy.NONE),
                         envConfig.getDurability());
            appEnv.close();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
        finally {
            TestUtils.removeLogFiles("testParamLoadingEnd",
                                     testEnvHome, false);
        }
    }

    @Test
    public void testDbRename()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            String databaseName = "simpleDb";
            String newDatabaseName = "newSimpleDb";

            /* Try to rename a non-existent db. */
            try {
                env1.renameDatabase(null, databaseName, newDatabaseName);
                fail("Rename on non-existent db should fail");
            } catch (DatabaseException e) {
                /* expect exception */
            }

            /* Now create a test db. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            Database exampleDb = env1.openDatabase(null, databaseName,
                    dbConfig);

            Transaction txn = env1.beginTransaction(null, null);
            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();
            txn.commit();
            exampleDb.close();

            dbConfig.setAllowCreate(false);
            env1.renameDatabase(null, databaseName, newDatabaseName);
            exampleDb = env1.openDatabase(null, newDatabaseName, dbConfig);

            if (DualTestCase.isReplicatedTest(getClass())) {
                txn = env1.beginTransaction(null, null);
            } else {
                txn = null;
            }

            cursor = exampleDb.openCursor(txn, null);
            // XXX doSimpleVerification(cursor);
            cursor.close();
            if (txn != null) {
                txn.commit();
            }

            /* Check debug name. */
            DatabaseImpl dbImpl = DbInternal.getDbImpl(exampleDb);
            assertEquals(newDatabaseName, dbImpl.getDebugName());
            exampleDb.close();
            try {
                exampleDb = env1.openDatabase(null, databaseName, dbConfig);
                fail("didn't get db not found exception");
            } catch (DatabaseNotFoundException expected) {
            }
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDbRenameCommit()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            String databaseName = "simpleRenameCommitDb";
            String newDatabaseName = "newSimpleRenameCommitDb";

            Transaction txn = env1.beginTransaction(null, null);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database exampleDb = env1.openDatabase(txn, databaseName,
                    dbConfig);

            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();
            exampleDb.close();

            dbConfig.setAllowCreate(false);
            env1.renameDatabase(txn, databaseName, newDatabaseName);
            exampleDb = env1.openDatabase(txn, newDatabaseName, dbConfig);
            cursor = exampleDb.openCursor(txn, null);
            cursor.close();
            exampleDb.close();
            try {
                exampleDb = env1.openDatabase(txn, databaseName, dbConfig);
                fail("didn't get db not found exception");
            } catch (DatabaseNotFoundException expected) {
            }
            txn.commit();

            try {
                exampleDb = env1.openDatabase(null, databaseName, null);
                fail("didn't catch DatabaseException opening old name");
            } catch (DatabaseNotFoundException expected) {
            }
            try {
                exampleDb = env1.openDatabase(null, newDatabaseName, null);
                exampleDb.close();
            } catch (DatabaseException DBE) {
                fail("caught unexpected exception");
            }

            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDbRenameAbort()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            /* Create a database. */
            String databaseName = "simpleRenameAbortDb";
            String newDatabaseName = "newSimpleRenameAbortDb";
            Transaction txn = env1.beginTransaction(null, null);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database exampleDb =
                env1.openDatabase(txn, databaseName, dbConfig);

            /* Put some data in, close the database, commit. */
            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();
            exampleDb.close();
            txn.commit();

            /*
             * Rename under another txn, shouldn't be able to open under the
             * old name.
             */
            txn = env1.beginTransaction(null, null);
            env1.renameDatabase(txn, databaseName, newDatabaseName);
            dbConfig.setAllowCreate(false);
            exampleDb = env1.openDatabase(txn, newDatabaseName, dbConfig);
            cursor = exampleDb.openCursor(txn, null);
            // XXX doSimpleVerification(cursor);
            cursor.close();
            exampleDb.close();
            try {
                exampleDb = env1.openDatabase(txn, databaseName, dbConfig);
                fail("didn't get db not found exception");
            } catch (DatabaseNotFoundException expected) {
            }

            /*
             * Abort the rename, should be able to open under the old name with
             * empty props (DB_CREATE not set)
             */
            txn.abort();
            exampleDb = new Database(env1);
            try {
                exampleDb = env1.openDatabase(null, databaseName, null);
                exampleDb.close();
            } catch (DatabaseException dbe) {
                fail("caught DatabaseException opening old name: " +
                     dbe.getMessage());
            }

            /* Shouldn't be able to open under the new name. */
            try {
                exampleDb = env1.openDatabase(null, newDatabaseName, null);
                fail("didn't catch DatabaseException opening new name");
            } catch (DatabaseNotFoundException expected) {
            }

            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDbRemove()
        throws Throwable {

        doDbRemove(true, false);
    }

    @Test
    public void testDbRemoveReadCommitted()
        throws Throwable {

        doDbRemove(true, true);
    }

    @Test
    public void testDbRemoveNonTxnl()
        throws Throwable {

        doDbRemove(false, false);
    }

    private void doDbRemove(boolean txnl, boolean readCommitted)
        throws Throwable {

        try {
            /* Set up an environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(txnl);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            String databaseName = "simpleDb";

            /* Try to remove a non-existent db */
            try {
                env1.removeDatabase(null, databaseName);
                fail("Remove of non-existent db should fail");
            } catch (DatabaseNotFoundException expected) {
            }

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(txnl);
            dbConfig.setAllowCreate(true);
            Transaction txn = null;
            if (txnl && readCommitted) {
                /* Create, close, then open with ReadCommitted. */
                Database db = env1.openDatabase(txn, databaseName, dbConfig);
                db.close();
                dbConfig.setAllowCreate(false);
                TransactionConfig txnConfig = new TransactionConfig();
                txnConfig.setReadCommitted(true);
                txn = env1.beginTransaction(null, txnConfig);
            }
            Database exampleDb =
                env1.openDatabase(txn, databaseName, dbConfig);
            if (txn != null) {
                txn.commit();
            }

            txn = null;
            if (txnl) {
                txn = env1.beginTransaction(null, null);
            }
            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();
            if (txn != null) {
                txn.commit();
            }

            /* Remove should fail because database is open. */
            try {
                env1.removeDatabase(null, databaseName);
                fail("didn't get db open exception");
            } catch (DatabaseException DBE) {
            }
            exampleDb.close();

            env1.removeDatabase(null, databaseName);

            /* Remove should fail because database does not exist. */
            try {
                exampleDb = env1.openDatabase(null, databaseName, null);
                fail("did not catch db does not exist exception");
            } catch (DatabaseNotFoundException expected) {
            }
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDbRemoveCommit()
        throws Throwable {

        try {
            /* Set up an environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            /* Make a database. */
            String databaseName = "simpleDb";
            Transaction txn = env1.beginTransaction(null, null);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database exampleDb =
                env1.openDatabase(txn, databaseName, dbConfig);

            /* Insert and delete data in it. */
            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();

            /*
             * Try a remove without closing the open Database handle.  Should
             * get an exception.
             */
            try {
                env1.removeDatabase(txn, databaseName);
                fail("didn't get db open exception");
            } catch (IllegalStateException e) {
            }
            exampleDb.close();

            /* Do a remove, try to open again. */
            env1.removeDatabase(txn, databaseName);
            try {
                dbConfig.setAllowCreate(false);
                exampleDb = env1.openDatabase(txn, databaseName, dbConfig);
                fail("did not catch db does not exist exception");
            } catch (DatabaseNotFoundException expected) {
            }
            txn.commit();

            /* Try to open, the db should have been removed. */
            try {
                exampleDb = env1.openDatabase(null, databaseName, null);
                fail("did not catch db does not exist exception");
            } catch (DatabaseNotFoundException expected) {
            }
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDbRemoveAbort()
        throws Throwable {

        try {
            /* Set up an environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);

            /* Create a database, commit. */
            String databaseName = "simpleDb";
            Transaction txn = env1.beginTransaction(null, null);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database exampleDb =
                env1.openDatabase(txn, databaseName, dbConfig);
            txn.commit();

            /* Start a new txn and put some data in the created db. */
            txn = env1.beginTransaction(null, null);
            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();

            /*
             * Try to remove, we should get an exception because the db is
             * open.
             */
            try {
                env1.removeDatabase(txn, databaseName);
                fail("didn't get db open exception");
            } catch (DatabaseException DBE) {
            }
            exampleDb.close();

            /*
             * txn can only be aborted at this point since the removeDatabase()
             * timed out.
             */
            txn.abort();
            txn = env1.beginTransaction(null, null);
            env1.removeDatabase(txn, databaseName);

            try {
                dbConfig.setAllowCreate(false);
                exampleDb = env1.openDatabase(txn, databaseName, dbConfig);
                fail("did not catch db does not exist exception");
            } catch (DatabaseNotFoundException expected) {
            }

            /* Abort, should rollback the db remove. */
            txn.abort();

            try {
                DatabaseConfig dbConfig2 = new DatabaseConfig();
                dbConfig2.setTransactional(true);
                exampleDb = env1.openDatabase(null, databaseName, dbConfig2);
            } catch (DatabaseException DBE) {
                fail("db does not exist anymore after delete/abort");
            }

            exampleDb.close();
            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Provides general testing of getDatabaseNames.  Additionally verifies a
     * fix for a bug that occurred when the first DB (lowest valued name) was
     * removed or renamed prior to calling getDatabaseNames.  A NPE occurred
     * in this case if the compressor had not yet deleted the BIN entry for
     * the removed/renamed name. [#13377]
     */
    @Test
    public void testGetDatabaseNames()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        /* Start with no databases. */
        Set<String> dbNames = new HashSet<String>();
        env1 = create(envHome, envConfig);
        checkDbNames(dbNames, env1.getDatabaseNames());

        /* Add DB1. */
        dbNames.add("DB1");
        Database db = env1.openDatabase(null, "DB1", dbConfig);
        db.close();
        checkDbNames(dbNames, env1.getDatabaseNames());

        /* Add DB2. */
        dbNames.add("DB2");
        db = env1.openDatabase(null, "DB2", dbConfig);
        db.close();
        checkDbNames(dbNames, env1.getDatabaseNames());

        /* Rename DB2 to DB3 (this caused NPE). */
        dbNames.remove("DB2");
        dbNames.add("DB3");
        env1.renameDatabase(null, "DB2", "DB3");
        checkDbNames(dbNames, env1.getDatabaseNames());

        /* Remove DB1. */
        dbNames.remove("DB1");
        dbNames.add("DB4");
        env1.renameDatabase(null, "DB1", "DB4");
        checkDbNames(dbNames, env1.getDatabaseNames());

        /* Add DB0. */
        dbNames.add("DB0");
        db = env1.openDatabase(null, "DB0", dbConfig);
        db.close();
        checkDbNames(dbNames, env1.getDatabaseNames());

        /* Remove DB0 (this caused NPE). */
        dbNames.remove("DB0");
        env1.removeDatabase(null, "DB0");
        checkDbNames(dbNames, env1.getDatabaseNames());

        close(env1);
        env1 = null;
    }

    /**
     * Checks that the expected set of names equals the list of names returned
     * from getDatabaseNames.  A list can't be directly compared to a set using
     * equals().
     */
    private void checkDbNames(Set<String> expected, List<String> actual) {
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, new HashSet<String>(actual));
    }

    /*
     * This little test case can only invoke the compressor, since the evictor,
     * cleaner and checkpointer are all governed by utilization metrics and are
     * tested elsewhere.
     */
    @Test
    public void testDaemonManualInvocation()
        throws Throwable {

        try {
            /* Set up an environment. */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            String testPropVal = "120000000";
            envConfig.setConfigParam
                (EnvironmentParams.COMPRESSOR_WAKEUP_INTERVAL.getName(),
                 testPropVal);
            envConfig.setConfigParam
                (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
            envConfig.setAllowCreate(true);
            envConfig.setConfigParam
            (EnvironmentParams.LOG_MEM_SIZE.getName(), "20000");
            envConfig.setConfigParam
            (EnvironmentParams.NUM_LOG_BUFFERS.getName(), "2");
            env1 = create(envHome, envConfig);

            String databaseName = "simpleDb";
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            Database exampleDb =
                env1.openDatabase(null, databaseName, dbConfig);

            Transaction txn = env1.beginTransaction(null, null);
            Cursor cursor = exampleDb.openCursor(txn, null);
            doSimpleCursorPutAndDelete(cursor, false);
            cursor.close();
            txn.commit();
            exampleDb.close();
            EnvironmentStats envStats = env1.getStats(TestUtils.FAST_STATS);
            env1.compress();

            envStats = env1.getStats(TestUtils.FAST_STATS);
            long compressorTotal =
                envStats.getSplitBins() +
                envStats.getDbClosedBins() +
                envStats.getCursorsBins() +
                envStats.getNonEmptyBins() +
                envStats.getProcessedBins() +
                envStats.getInCompQueueSize();
            assertTrue(compressorTotal > 0);

            close(env1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Tests that each daemon can be turned on and off dynamically.
     */
    @Test
    public void testDaemonRunPause()
        throws DatabaseException, InterruptedException {

        final String[] runProps = {
            EnvironmentParams.ENV_RUN_CLEANER.getName(),
            EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(),
            EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(),
        };

        EnvironmentConfig config = TestUtils.initEnvConfig();
        config.setAllowCreate(true);

        config.setConfigParam
            (EnvironmentParams.MAX_MEMORY.getName(),
             MemoryBudget.MIN_MAX_MEMORY_SIZE_STRING);
        /* Don't track detail with a tiny cache size. */
        config.setConfigParam
            (EnvironmentParams.CLEANER_TRACK_DETAIL.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.CLEANER_BYTES_INTERVAL.getName(),
             "100");
        config.setConfigParam
            (EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getName(),
             "100");
        config.setConfigParam
            (EnvironmentParams.COMPRESSOR_WAKEUP_INTERVAL.getName(),
             "1000000");
        config.setConfigParam(EnvironmentParams.LOG_MEM_SIZE.getName(),
                              EnvironmentParams.LOG_MEM_SIZE_MIN_STRING);
        config.setConfigParam
            (EnvironmentParams.NUM_LOG_BUFFERS.getName(), "2");
        setBoolConfigParams(config, runProps,
                            new boolean[] { false, false, false, false });

        env1 = create(envHome, config);
        EnvironmentImpl envImpl = env1.getNonNullEnvImpl();

        final DaemonRunner[] daemons = {
            envImpl.getCleaner(),
            envImpl.getCheckpointer(),
            envImpl.getINCompressor(),
        };

        //*
        doTestDaemonRunPause(env1, daemons, runProps,
                             new boolean[] { false, false, false, false });
        doTestDaemonRunPause(env1, daemons, runProps,
                             new boolean[] { true,  false, false, false });
        if (!envImpl.isNoLocking()) {
            doTestDaemonRunPause(env1, daemons, runProps,
                    new boolean[] { false, true,  false, false });
        }
        //*/
        doTestDaemonRunPause(env1, daemons, runProps,
                             new boolean[] { false, false, true,  false });
        //*
        doTestDaemonRunPause(env1, daemons, runProps,
                             new boolean[] { false, false, false, true  });
        doTestDaemonRunPause(env1, daemons, runProps,
                             new boolean[] { false, false, false, false });

        //*/
        close(env1);
        env1 = null;
    }

    /**
     * Tests a set of daemon on/off settings.
     */
    private void doTestDaemonRunPause(Environment env,
                                      DaemonRunner[] daemons,
                                      String[] runProps,
                                      boolean[] runValues)
        throws DatabaseException, InterruptedException {

        /* Set daemon run properties. */
        EnvironmentMutableConfig config = env.getMutableConfig();
        setBoolConfigParams(config, runProps, runValues);
        env.setMutableConfig(config);

        /* Allow previously running daemons to come to a stop. */
        for (int i = 0; i < 10; i += 1) {
            Thread.yield();
            Thread.sleep(10);
        }

        /* Get current wakeup counts. */
        int[] prevCounts = new int[daemons.length];
        for (int i = 0; i < prevCounts.length; i += 1) {
            prevCounts[i] = daemons[i].getNWakeupRequests();
        }

        /* Write some data to wakeup the checkpointer, cleaner and evictor. */
        String dbName = "testDaemonRunPause";
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        Database db = env1.openDatabase(null, dbName, dbConfig);
        Cursor cursor = db.openCursor(null, null);
        doSimpleCursorPutAndDelete(cursor, true);
        cursor.close();
        db.close();

        /* Sleep for a while to wakeup the compressor. */
        Thread.sleep(1000);

        /* Check that the expected daemons were woken. */
        for (int i = 0; i < prevCounts.length; i += 1) {
            int currNWakeups = daemons[i].getNWakeupRequests();
            boolean woken = prevCounts[i] < currNWakeups;
            assertEquals(daemons[i].getClass().getName() +
                         " prevNWakeups=" + prevCounts[i] +
                         " currNWakeups=" + currNWakeups,
                         runValues[i], woken);
        }
    }

    private void setBoolConfigParams(EnvironmentMutableConfig config,
                                     String[] names,
                                     boolean[] values) {
        for (int i = 0; i < names.length; i += 1) {
            config.setConfigParam(names[i],
                                  Boolean.valueOf(values[i]).toString());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testExceptions()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);
            close(env1);

            /* Test for exceptions on closed environments via public APIs */

            try {
                env1.openDatabase(null, null, null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.openSecondaryDatabase(null, null, null, null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.removeDatabase(null, "name");
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.renameDatabase(null, "old", "new");
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.truncateDatabase(null, "name", false);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.removeDatabase(null, "name");
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.beginTransaction(null, null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.checkpoint(null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.sync();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.cleanLog();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.evictMemory();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.compress();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getConfig();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.setMutableConfig(null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getMutableConfig();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getStats(null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getLockStats(null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getTransactionStats(null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getDatabaseNames();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.verify(null,null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.getThreadTransaction();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.setThreadTransaction(null);
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }

            try {
                env1.checkOpen();
                fail("Expected IllegalStateException for op on closed env");
            } catch (IllegalStateException expected) {
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testClose()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env1 = create(envHome, envConfig);
            close(env1);

            envConfig.setAllowCreate(false);
            env1 = create(envHome, envConfig);

            /* Create a transaction to prevent the close from succeeding */
            env1.beginTransaction(null, null);
            try {
                close(env1);
                fail("Expected IllegalStateException for open transactions");
            } catch (IllegalStateException expected) {
            }

            try {
                close(env1);
            } catch (DatabaseException DENOE) {
                fail("Caught DatabaseException while re-closing " +
                     "an Environment.");
            }

            env1 = create(envHome, envConfig);

            String databaseName = "simpleDb";
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            env1.openDatabase(null, databaseName, dbConfig);
            env1.openDatabase(null, databaseName + "2", dbConfig);
            try {
                close(env1);
                fail("Expected IllegalStateException for open dbs");
            } catch (IllegalStateException expected) {
            }
            try {
                close(env1);
            } catch (Exception e) {
                fail("Caught DatabaseException while re-closing " +
                     "an Environment.");
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    protected String[] simpleKeyStrings = {
        "foo", "bar", "baz", "aaa", "fubar",
        "foobar", "quux", "mumble", "froboy" };

    protected String[] simpleDataStrings = {
        "one", "two", "three", "four", "five",
        "six", "seven", "eight", "nine" };

    protected void doSimpleCursorPutAndDelete(Cursor cursor, boolean extras)
        throws DatabaseException {

        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();

        for (int i = 0; i < simpleKeyStrings.length; i++) {
            foundKey.setString(simpleKeyStrings[i]);
            foundData.setString(simpleDataStrings[i]);
            OperationStatus status =
                cursor.putNoOverwrite(foundKey, foundData);
            if (status != OperationStatus.SUCCESS) {
                fail("non-success return " + status);
            }
            /* Need to write some extra out to force eviction to run. */
            if (extras) {
                for (int j = 0; j < 500; j++) {
                    foundData.setString(Integer.toString(j));
                    status = cursor.put(foundKey, foundData);
                    if (status != OperationStatus.SUCCESS) {
                        fail("non-success return " + status);
                    }
                }
            }
        }

        OperationStatus status =
            cursor.getFirst(foundKey, foundData, LockMode.DEFAULT);

        while (status == OperationStatus.SUCCESS) {
            cursor.delete();
            status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
        }
    }

    protected void doSimpleVerification(Cursor cursor)
        throws DatabaseException {

        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();

        int count = 0;
        OperationStatus status = cursor.getFirst(foundKey, foundData,
                                                 LockMode.DEFAULT);

        while (status == OperationStatus.SUCCESS) {
            count++;
            status = cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
        }
        assertEquals(simpleKeyStrings.length, count);
    }

    /**
     * Test that a latch timeout occurs. We manually confirm that full stack
     * trace appears in the je.info log.
     *
     * This test is here rather than in LatchTest only because an
     * EnvironmentImpl is needed to create a thread dump.
     */
    @Test
    public void testLatchTimeout()
        throws Throwable {

        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);
            envConfig.setConfigParam(
                EnvironmentParams.ENV_LATCH_TIMEOUT.getName(), "1 ms");

            env1 = create(envHome, envConfig);

            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env1);

            final Latch latch = LatchFactory.createExclusiveLatch(
                envImpl, "test", false);

            final CountDownLatch latched = new CountDownLatch(1);

            Thread thread = new Thread() {
                @Override
                public void run() {
                    latch.acquireExclusive();
                    latched.countDown();
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException expected) {
                    }
                }
            };

            thread.start();

            latched.await(1000, TimeUnit.MILLISECONDS);

            try {
                latch.acquireExclusive();
                fail("Expected latch timeout");
            } catch (EnvironmentFailureException e) {
                assertTrue(
                    e.getMessage(), e.getMessage().contains("Latch timeout"));
            } finally {
                thread.interrupt();
            }

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (env1 != null) {
                try {
                    abnormalClose(env1);
                } catch (Throwable expected) {
                }
                env1 = null;
            }
        }
    }
}
