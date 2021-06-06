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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This tests exercises the act of eviction and determines whether the
 * expected nodes have been evicted properly.
 */
public class EvictActionTest extends TestBase {

    private static final boolean DEBUG = false;
    private static final int NUM_KEYS = 60;
    private static final int NUM_DUPS = 50;
    private static final int BIG_CACHE_SIZE = 500000;
    private static final int SMALL_CACHE_SIZE = (int)
        MemoryBudget.MIN_MAX_MEMORY_SIZE;

    private File envHome = null;
    private Environment env = null;
    private Database db = null;
    private int actualLNs = 0;
    private int actualINs = 0;

    public EvictActionTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {
        
        IN.ACCUMULATED_LIMIT = 0;
        Txn.ACCUMULATED_LIMIT = 0;
        super.setUp();
    }

    @After
    public void tearDown() {
        
        if (env != null) {
            try {
                env.close();
            } catch (Throwable e) {
                System.out.println("tearDown: " + e);
            }
        }
        envHome = null;
        env = null;
        db = null;
    }

    @Test
    public void testEvict()
        throws Throwable {

        doEvict(50, SMALL_CACHE_SIZE, true);
    }

    @Test
    public void testNoNeedToEvict()
        throws Throwable {

        doEvict(80, BIG_CACHE_SIZE, false);
    }

    /**
     * Evict in very controlled circumstances. Check that we first strip
     * BINs and later evict BINS.
     */
    private void doEvict(int floor,
                         int maxMem,
                         boolean shouldEvict)
        throws Throwable {

        openEnv(floor, maxMem);
        insertData(NUM_KEYS);

        /* Evict once after insert. */
        evictAndCheck(shouldEvict, NUM_KEYS);

        /* Evict again after verification. */
        evictAndCheck(shouldEvict, NUM_KEYS);

        closeEnv();
    }

    @Test
    public void testSetCacheSize()
        throws DatabaseException {

        /* Start with large cache size. */
        openEnv(80, BIG_CACHE_SIZE);
        EnvironmentMutableConfig config = env.getMutableConfig();
        insertData(NUM_KEYS);

        /* No need to evict. */
        verifyData(NUM_KEYS);
        evictAndCheck(false, NUM_KEYS);

        /* Set small cache size. */
        config.setCacheSize(SMALL_CACHE_SIZE);
        env.setMutableConfig(config);

        /* Expect eviction. */
        verifyData(NUM_KEYS);
        evictAndCheck(true, NUM_KEYS);

        /* Set large cache size. */
        config.setCacheSize(BIG_CACHE_SIZE);
        env.setMutableConfig(config);

        /* Expect no eviction. */
        verifyData(NUM_KEYS);
        evictAndCheck(false, NUM_KEYS);

        closeEnv();
    }

    @Test
    public void testSetCachePercent()
        throws DatabaseException {

        int nKeys = NUM_KEYS * 500;

        /* Start with large cache size. */
        openEnv(80, BIG_CACHE_SIZE);
        EnvironmentMutableConfig config = env.getMutableConfig();
        config.setCacheSize(0);
        config.setCachePercent(90);
        env.setMutableConfig(config);
        insertData(nKeys);

        /* No need to evict. */
        verifyData(nKeys);
        evictAndCheck(false, nKeys);

        /* Set small cache percent. */
        config.setCacheSize(0);
        config.setCachePercent(1);
        env.setMutableConfig(config);

        /* Expect eviction. */
        verifyData(nKeys);
        evictAndCheck(true, nKeys);

        /* Set large cache percent. */
        config.setCacheSize(0);
        config.setCachePercent(90);
        env.setMutableConfig(config);

        /* Expect no eviction. */
        verifyData(nKeys);
        evictAndCheck(false, nKeys);

        closeEnv();
    }

    @Test
    public void testThreadedCacheSizeChanges()
        throws DatabaseException {

        final int N_ITERS = 10;
        openEnv(80, BIG_CACHE_SIZE);
        insertData(NUM_KEYS);

        JUnitThread writer = new JUnitThread("Writer") {
            @Override
            public void testBody()
                throws DatabaseException {
                for (int i = 0; i < N_ITERS; i += 1) {
                    env.evictMemory();
                    /* insertData will update if data exists. */
                    insertData(NUM_KEYS);
                    env.evictMemory();
                    EnvironmentMutableConfig config = env.getMutableConfig();
                    config.setCacheSize(SMALL_CACHE_SIZE);
                    env.setMutableConfig(config);
                }
            }
        };

        JUnitThread reader = new JUnitThread("Reader") {
            @Override
            public void testBody()
                throws DatabaseException {
                for (int i = 0; i < N_ITERS; i += 1) {
                    env.evictMemory();
                    verifyData(NUM_KEYS);
                    env.evictMemory();
                    EnvironmentMutableConfig config = env.getMutableConfig();
                    config.setCacheSize(BIG_CACHE_SIZE);
                    env.setMutableConfig(config);
                }
            }
        };

        writer.start();
        reader.start();

        try {
            writer.finishTest();
        } catch (Throwable e) {
            try {
                reader.finishTest();
            } catch (Throwable ignore) { }
            e.printStackTrace();
            fail(e.toString());
        }

        try {
            reader.finishTest();
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.toString());
        }

        closeEnv();
    }

    @Test
    public void testSmallCacheSettings()
        throws DatabaseException {

        /*
         * With a cache size > 600 KB, the min tree usage should be the default
         * value.
         */
        openEnv(0, 1200 * 1024);
        EnvironmentMutableConfig config = env.getMutableConfig();
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        MemoryBudget mb = envImpl.getMemoryBudget();
        assertEquals(500 * 1024, mb.getMinTreeMemoryUsage());

        /*
         * With a cache size > 1000 KB, evict bytes may be > 500 KB but we
         * should not evict over half the cache size.
         */
        putLargeData(1200, 1024);
        
        EnvironmentStats stats = env.getStats(null);

        env.evictMemory();
        stats = env.getStats(null);
        assertTrue(stats.getCacheTotalBytes() >= 1200 * 1024 / 2);

        /*
         * With a cache size of 500 KB, the min tree usage should be the amount
         * available in the cache after the buffer bytes are subtracted.
         */
        config.setCacheSize(500 * 1024);
        env.setMutableConfig(config);
        stats = env.getStats(null);
        assertEquals(500 * 1024 - stats.getBufferBytes(),
                     mb.getMinTreeMemoryUsage());

        /*
         * With a cache size of 500 KB, evict bytes may be < 500 KB but we
         * should not evict over half the cache size.
         */
        putLargeData(500, 1024);
        env.evictMemory();
        stats = env.getStats(null);
        assertTrue(stats.getCacheTotalBytes() >= 500 * 1024 / 2);

        /*
         * Even when using a large amount of non-tree memory, the tree memory
         * usage should not go below the minimum.
         */
        mb.updateAdminMemoryUsage(500 * 1024);
        env.evictMemory();
        stats = env.getStats(null);
        long treeBytes = stats.getDataBytes()  +
                         50 * 1024 /* larger than any LN or IN */;
        assertTrue(treeBytes >= mb.getMinTreeMemoryUsage());
        mb.updateAdminMemoryUsage(-(500 * 1024));

        /* Allow changing the min tree usage explicitly. */
        config.setCacheSize(500 * 1024);
        config.setConfigParam("je.tree.minMemory", String.valueOf(50 * 1024));
        env.setMutableConfig(config);
        assertEquals(50 * 1024, mb.getMinTreeMemoryUsage());

        /* The min tree usage may not be larger than the cache. */
        config.setCacheSize(500 * 1024);
        config.setConfigParam("je.tree.minMemory", String.valueOf(900 * 1024));
        env.setMutableConfig(config);
        stats = env.getStats(null);
        assertEquals(500 * 1024 - stats.getBufferBytes(),
                     mb.getMinTreeMemoryUsage());

        closeEnv();
    }

    /**
     * We now allow eviction of the root IN of a DB, whether the DB is closed
     * or not.  Check that basic root eviction works.  [#13415]
     */
    @Test
    public void testRootINEviction()
        throws DatabaseException {

        DatabaseEntry entry = new DatabaseEntry(new byte[1]);
        OperationStatus status;

        openEnv(80, SMALL_CACHE_SIZE);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db1 = env.openDatabase(null, "db1", dbConfig);

        /* Root starts out null. */
        assertTrue(!isRootResident(db1));
        /* It is created when we insert the first record. */
        status = db1.put(null, entry, entry);
        assertSame(OperationStatus.SUCCESS, status);
        assertTrue(isRootResident(db1));
        /* It is evicted when necessary. */
        forceEviction();
        assertTrue(!isRootResident(db1));
        /* And fetched again when needed. */
        status = db1.get(null, entry, entry, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertTrue(isRootResident(db1));

        /* Deferred write DBs work in the same way. */
        dbConfig.setDeferredWrite(true);
        Database db2 = env.openDatabase(null, "db2", dbConfig);
        status = db2.put(null, entry, entry);
        assertSame(OperationStatus.SUCCESS, status);
        assertTrue(isRootResident(db2));
        /* It is evicted when necessary. */
        forceEviction();
        assertTrue(!isRootResident(db2));
        /* And fetched again when needed. */
        status = db2.get(null, entry, entry, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertTrue(isRootResident(db2));
        /* Deferred-write eviction also works when the root is not dirty. */
        db2.sync();
        forceEviction();
        assertTrue(!isRootResident(db2));

        db2.close();
        db1.close();
        closeEnv();
    }

    /**
     * We now allow eviction of the MapLN and higher level INs in the DB
     * mapping tree when DBs are closed.  Check that basic mapping tree IN
     * eviction works.  [#13415]
     */
    @Test
    public void testMappingTreeEviction()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        DatabaseEntry entry = new DatabaseEntry(new byte[1]);
        OperationStatus status;

        EnvironmentConfig envConfig =
            getEnvConfig(80, SMALL_CACHE_SIZE, false /*readonly*/);
        /* Can't use cleaner/expiration DBs in this sensitive test. */
        DbInternal.setCreateUP(envConfig, false);
        DbInternal.setCreateEP(envConfig, false);
        openEnv(envConfig);

        /* Baseline mapping tree LNs and INs. */
        final int baseLNs = 1; // Test DB
        final int baseINs = 2; // Root IN and BIN
        checkMappingTree(baseLNs, baseINs);
        forceEviction();
        checkMappingTree(baseLNs, baseINs);

        /*
         * Create enough DBs to fill up a BIN in the mapping DB.  NODE_MAX is
         * configured to be 4 in this test.  There are already 2 DBs open.
         */
        final int nDbs = 4;
        Database[] dbs = new Database[nDbs];
        for (int i = 0; i < nDbs; i += 1) {
            dbs[i] = env.openDatabase(null, "db" + i, dbConfig);
            status = dbs[i].put(null, entry, entry);
            assertSame(OperationStatus.SUCCESS, status);
            assertTrue(isRootResident(dbs[i]));
        }
        final int openLNs = baseLNs + nDbs; // Add 1 MapLN per open DB
        final int openINs = baseINs + 1;    // Add 1 BIN in the mapping tree
        checkMappingTree(openLNs, openINs);
        forceEviction();
        checkMappingTree(openLNs, openINs);

        /* Close DBs and force eviction. */
        for (int i = 0; i < nDbs; i += 1) {
            dbs[i].close();
        }
        forceEviction();
        checkMappingTree(baseLNs, baseINs);

        /* Re-open the DBs, opening each DB twice. */
        Database[] dbs2 = new Database[nDbs];
        for (int i = 0; i < nDbs; i += 1) {
            dbs[i] = env.openDatabase(null, "db" + i, dbConfig);
            dbs2[i] = env.openDatabase(null, "db" + i, dbConfig);
        }
        checkMappingTree(openLNs, openINs);
        forceEviction();
        checkMappingTree(openLNs, openINs);

        /* Close one handle only, MapLN eviction should not occur. */
        for (int i = 0; i < nDbs; i += 1) {
            dbs[i].close();
        }
        forceEviction();
        checkMappingTree(openLNs, openINs);

        /* Close the other handles, eviction should occur. */
        for (int i = 0; i < nDbs; i += 1) {
            dbs2[i].close();
        }
        forceEviction();
        checkMappingTree(baseLNs, baseINs);

        closeEnv();
    }

    /**
     * Checks that cleaning performs memory budget calculations correctly for
     * evicted databases (non-resident MapLNs).  [#21686]
     */
    public void testCleaningAfterMappingTreeEviction()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        DatabaseEntry key = new DatabaseEntry(new byte[1000]);
        DatabaseEntry data = new DatabaseEntry(new byte[10000]);
        OperationStatus status;

        EnvironmentConfig envConfig =
            getEnvConfig(0, SMALL_CACHE_SIZE, false /*readonly*/);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                 String.valueOf(1024 * 1024));
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                                 "75");
        openEnv(envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /*
         * Create enough DBs to fill several 10 or more files and close them so
         * they'll be evicted below.
         */
        final int nDbs = 1000;
        for (int i = 0; i < nDbs; i += 1) {
            Database db = env.openDatabase(null, "db" + i, dbConfig);
            status = db.put(null, key, data);
            assertSame(OperationStatus.SUCCESS, status);
            status = db.delete(null, key);
            assertSame(OperationStatus.SUCCESS, status);
            db.close();
        }
        env.checkpoint(new CheckpointConfig().setForce(true));
        forceEviction();

        /*
         * Clean and checkpoint repeatedly to create a scenario where a
         * non-resident MapLN is migrated by the cleaner [#21686].  With only a
         * single call to cleanLog, the MapLN will be resident due to the
         * nearby LNs for that database, since the cleaner does read-ahead
         * during LN processing.
         *
         * Prior to the bug fix, TestUtils.validateNodeMemUsage (called by
         * forceEviction below) would report a mismatch in the tree admin
         * memory usage.
         */
        for (int i = 0; i < 10; i += 1) {
            env.cleanLog();
            env.checkpoint(new CheckpointConfig().setForce(true));
            forceEviction();
        }

        /* Final check for good measure. */
        TestUtils.validateNodeMemUsage(envImpl, true);

        closeEnv();
    }

    /**
     * Checks that a dirty root IN is not evicted in a read-only environment.
     * [#16368]
     */
    @Test
    public void testReadOnlyRootINEviction()
        throws DatabaseException {

        OperationStatus status;

        openEnv(80, SMALL_CACHE_SIZE);

        /* Big record will be used to force eviction. */
        DatabaseEntry bigRecordKey = new DatabaseEntry(new byte[1]);
        status = db.put(null, bigRecordKey,
                        new DatabaseEntry(new byte[BIG_CACHE_SIZE]));
        assertSame(OperationStatus.SUCCESS, status);

        /* Open DB1 and insert a record to create the root IN. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db1 = env.openDatabase(null, "db1", dbConfig);

        DatabaseEntry smallRecordKey = new DatabaseEntry(new byte[1]);
        DatabaseEntry smallData = new DatabaseEntry(new byte[1]);
        status = db1.put(null, smallRecordKey, smallData);
        assertSame(OperationStatus.SUCCESS, status);

        /* Close environment and re-open it read-only. */
        db1.close();
        closeEnv();

        EnvironmentConfig envConfig =
            getEnvConfig(80, SMALL_CACHE_SIZE, true /*readOnly*/);
        openEnv(envConfig);

        dbConfig.setReadOnly(true);
        dbConfig.setAllowCreate(false);
        db1 = env.openDatabase(null, "db1", dbConfig);

        /* Load a record to load the root IN. */
        status = db1.get(null, smallRecordKey, new DatabaseEntry(), null);
        assertSame(OperationStatus.SUCCESS, status);
        assertTrue(isRootResident(db1));

        /*
         * Set the root dirty to prevent eviction.  In real life, this can only
         * be done by recovery in a read-only environment, but that's very
         * difficult to simulate precisely.
         */
        IN rootIN = DbInternal.getDbImpl(db1).
                               getTree().
                               getRootIN(CacheMode.DEFAULT);
        rootIN.setDirty(true);
        rootIN.releaseLatch();

        /* Root should not be evicted while dirty. */
        forceReadOnlyEviction(bigRecordKey);
        assertTrue(isRootResident(db1));
        forceReadOnlyEviction(bigRecordKey);
        assertTrue(isRootResident(db1));

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Evictor evictor = envImpl.getEvictor();

        /* When made non-dirty, it can be evicted. */
        rootIN.setDirty(false);
        evictor.addBack(rootIN);

        forceReadOnlyEviction(bigRecordKey);
        assertTrue(!isRootResident(db1));

        db1.close();
        closeEnv();
    }

    /**
     * Check that opening a database in a transaction and then aborting the
     * transaction will decrement the database use count.  [#13415]
     */
    @Test
    public void testAbortOpen()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_DB_EVICTION.getName(), "true");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);

        /* Abort the txn used to open a database. */
        Transaction txn = env.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db1 = env.openDatabase(txn, "db1", dbConfig);
        DatabaseImpl saveDb = DbInternal.getDbImpl(db1);
        txn.abort();

        /* DB should not be in use and does not have to be closed. */
        assertEquals(false, saveDb.isInUse());

        /*
         * Environment.close will not throw an exception, even though the DB
         * has not been closed.  The abort took care of cleaning up the handle.
         */
        closeEnv();

        /*
         * Try a non-transactional DB open that throws an exception because we
         * create it exclusively and it already exists.  The use count should
         * be decremented.
         */
        env = new Environment(envHome, envConfig);
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setTransactional(false);
        db1 = env.openDatabase(null, "db1", dbConfig);
        saveDb = DbInternal.getDbImpl(db1);
        try {
            env.openDatabase(null, "db1", dbConfig);
            fail();
        } catch (DatabaseException e) {
            assertTrue(e.getMessage().indexOf("already exists") >= 0);
        }
        db1.close();
        assertEquals(false, saveDb.isInUse());

        /*
         * Try a non-transactional DB open that throws an exception because we
         * change the duplicatesAllowed setting.  The use count should be
         * decremented.
         */
        dbConfig.setSortedDuplicates(true);
        dbConfig.setExclusiveCreate(false);
        try {
            env.openDatabase(null, "db1", dbConfig);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().indexOf("sortedDuplicates") >= 0);
        }
        assertEquals(false, saveDb.isInUse());

        closeEnv();
    }

    /**
     * Check for the expected number of nodes in the mapping DB.
     */
    private void checkMappingTree(int expectLNs, int expectINs)
        throws DatabaseException {

        IN root = DbInternal.getNonNullEnvImpl(env).
            getDbTree().getDb(DbTree.ID_DB_ID).getTree().
            getRootIN(CacheMode.UNCHANGED);
        actualLNs = 0;
        actualINs = 0;
        countMappingTree(root);
        root.releaseLatch();
        assertEquals("LNs", expectLNs, actualLNs);
        assertEquals("INs", expectINs, actualINs);
    }

    private void countMappingTree(IN parent) {
        actualINs += 1;
        for (int i = 0; i < parent.getNEntries(); i += 1) {
            if (parent.getTarget(i) != null) {
                if (parent.getTarget(i) instanceof IN) {
                    countMappingTree((IN) parent.getTarget(i));
                } else {
                    actualLNs += 1;
                }
            }
        }
    }

    /**
     * Returns whether the root IN is currently resident for the given DB.
     */
    private boolean isRootResident(Database dbParam) {
        return DbInternal.getDbImpl(dbParam)
                         .getTree()
                         .isRootResident();
    }

    /**
     * Force eviction by inserting a large record in the pre-opened DB.
     */
    private void forceEviction()
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        OperationStatus status;

        /*
         * Repeat twice to cause a 2nd pass over the INList.  The second pass
         * evicts BINs that were only stripped of LNs in the first pass.
         */
        for (int i = 0; i < 2; i += 1) {
            Cursor c = db.openCursor(null, null);
            status = c.put(new DatabaseEntry(new byte[1]),
                           new DatabaseEntry(new byte[BIG_CACHE_SIZE]));
            assertSame(OperationStatus.SUCCESS, status);

            /*
             * Evict while cursor pins LN memory, to ensure eviction of other
             * DB INs, including the DB root.  When lruOnly=false, root IN
             * eviction may not occur unless a cursor is used to pin the LN.
             */
            env.evictMemory();

            status = c.delete();
            assertSame(OperationStatus.SUCCESS, status);

            c.close();
        }

        TestUtils.validateNodeMemUsage(envImpl, true);
    }

    /**
     * Force eviction by reading a large record.
     */
    private void forceReadOnlyEviction(DatabaseEntry key)
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        OperationStatus status;

        /*
         * Repeat twice to cause a 2nd pass over the INList.  The second pass
         * evicts BINs that were only stripped of LNs in the first pass.
         */
        for (int i = 0; i < 2; i += 1) {
            Cursor c = db.openCursor(null, null);
            status = c.getSearchKey(key, new DatabaseEntry(), null);
            assertSame(OperationStatus.SUCCESS, status);

            /*
             * Evict while cursor pins LN memory, to ensure eviction of other
             * DB INs, including the DB root.  When lruOnly=false, root IN
             * eviction may not occur unless a cursor is used to pin the LN.
             */
            env.evictMemory();

            c.close();
        }

        TestUtils.validateNodeMemUsage(envImpl, true);
    }

    /**
     * Open an environment and database.
     */
    private void openEnv(int floor,
                         int maxMem)
        throws DatabaseException {

        EnvironmentConfig envConfig =
            getEnvConfig(floor, maxMem, false /*readonly*/);
        openEnv(envConfig);
    }

    /**
     * Open an environment and database.
     */
    private EnvironmentConfig getEnvConfig(int floor,
                                           int maxMem,
                                           boolean readOnly) {
        /* Convert floor percentage into bytes. */
        long evictBytes = 0;
        if (floor > 0) {
            evictBytes = maxMem - ((maxMem * floor) / 100);
        }

        /* Make a non-txnal env w/no daemons and small nodes. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(!readOnly);
        envConfig.setReadOnly(readOnly);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        if (evictBytes > 0) {
            envConfig.setConfigParam(EnvironmentParams.
                                     EVICTOR_EVICT_BYTES.getName(),
                                     (new Long(evictBytes)).toString());
        }
        envConfig.setConfigParam(EnvironmentParams.
                                 MAX_MEMORY.getName(),
                                 new Integer(maxMem).toString());
        /* Don't track detail with a tiny cache size. */
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_TRACK_DETAIL.getName(), "false");
        envConfig.setConfigParam(EnvironmentParams.LOG_MEM_SIZE.getName(),
                                 EnvironmentParams.LOG_MEM_SIZE_MIN_STRING);
        envConfig.setConfigParam(EnvironmentParams.NUM_LOG_BUFFERS.getName(),
                                 "2");
        /* Enable DB (MapLN) eviction for eviction tests. */
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_DB_EVICTION.getName(), "true");

        /*
         * Disable critical eviction, we want to test under controlled
         * circumstances.
         */
        envConfig.setConfigParam(EnvironmentParams.
                                 EVICTOR_CRITICAL_PERCENTAGE.getName(),
                                 "1000");

        /* Make small nodes */
        envConfig.setConfigParam(EnvironmentParams.
                                 NODE_MAX.getName(), "4");
        envConfig.setConfigParam(EnvironmentParams.
                                 NODE_MAX_DUPTREE.getName(), "4");

        return envConfig;
    }

    private void openEnv(EnvironmentConfig envConfig)
        throws DatabaseException {

        env = new Environment(envHome, envConfig);
        boolean readOnly = envConfig.getReadOnly();

        /* Open database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(!readOnly);
        dbConfig.setReadOnly(readOnly);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void closeEnv()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    private void insertData(int nKeys)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < nKeys; i++) {

            IntegerBinding.intToEntry(i, key);

            if ((i % 5) == 0) {
                for (int j = 10; j < (NUM_DUPS + 10); j++) {
                    IntegerBinding.intToEntry(j, data);
                    db.put(null, key, data);
                }
            } else {
                IntegerBinding.intToEntry(i+1, data);
                db.put(null, key, data);
            }
        }
    }

    private void putLargeData(int nKeys, int dataSize)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[dataSize]);
        for (int i = 0; i < nKeys; i++) {
            IntegerBinding.intToEntry(i, key);
            db.put(null, key, data);
        }
    }

    private void verifyData(int nKeys)
        throws DatabaseException {

        /* Full scan of data, make sure we can bring everything back in. */
        Cursor cursor = db.openCursor(null, null);
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();

        for (int i = 0; i < nKeys; i++) {
            if ((i % 5) ==0) {
                for (int j = 10; j < (NUM_DUPS + 10); j++) {
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor.getNext(key, data, LockMode.DEFAULT));
                    assertEquals(i, IntegerBinding.entryToInt(key));
                    assertEquals(j, IntegerBinding.entryToInt(data));
                }
            } else {
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getNext(key, data, LockMode.DEFAULT));
                assertEquals(i, IntegerBinding.entryToInt(key));
                assertEquals(i+1, IntegerBinding.entryToInt(data));
            }
        }

        assertEquals(OperationStatus.NOTFOUND,
                     cursor.getNext(key, data, LockMode.DEFAULT));
        cursor.close();
    }

    private void evictAndCheck(boolean shouldEvict, int nKeys)
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        MemoryBudget mb = envImpl.getMemoryBudget();

        /*
         * The following batches are run in a single evictMemory() call:
         * 1st eviction will strip DBINs.
         * 2nd will evict DBINs
         * 3rd will evict DINs
         * 4th will strip BINs
         * 5th will evict BINs
         * 6th will evict INs
         * 7th will evict INs
         */
        long preEvictMem = mb.getCacheMemoryUsage();
        TestUtils.validateNodeMemUsage(envImpl, true);
        env.evictMemory();
        long postEvictMem = mb.getCacheMemoryUsage();

        TestUtils.validateNodeMemUsage(envImpl, true);
        if (DEBUG) {
            System.out.println("preEvict=" + preEvictMem +
                               " postEvict=" + postEvictMem);
        }

        if (shouldEvict) {
            assertTrue("preEvict=" + preEvictMem +
                       " postEvict=" + postEvictMem +
                       " maxMem=" + mb.getMaxMemory(),
                       (preEvictMem > postEvictMem));
        } else {
            assertTrue("preEvict=" + preEvictMem +
                       " postEvict=" + postEvictMem,
                       (preEvictMem == postEvictMem));
        }

        verifyData(nKeys);
        TestUtils.validateNodeMemUsage(envImpl, true);
    }
}
