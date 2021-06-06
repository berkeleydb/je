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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Tests that the LRU algorithm is accurate.
 */
public class LRUTest extends TestBase {

    private static final int N_DBS = 5;
    private static final int ONE_MB = 1 << 20;
    private static final int DB_CACHE_SIZE = ONE_MB;
    private static final int ENV_CACHE_SIZE = N_DBS * DB_CACHE_SIZE;
    private static final int MIN_DATA_SIZE = 50 * 1024;
    private static final int LRU_ACCURACY_PCT = 70;
    private static final int ENTRY_DATA_SIZE = 500;

    private File envHome;
    private Environment env;
    private Database[] dbs = new Database[N_DBS];

    public LRUTest() {
        envHome = SharedTestUtils.getTestDir();
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
        dbs = null;
    }

    private void open() {
        open(null);
    }

    private void open(CacheMode cacheMode) {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setCacheSize(ENV_CACHE_SIZE);
        envConfig.setCacheMode(cacheMode);
        envConfig.setConfigParam(
            EnvironmentConfig.TREE_MIN_MEMORY, String.valueOf(MIN_DATA_SIZE));
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

        env = new Environment(envHome, envConfig);

        TestUtils.adjustCacheSizeForOffHeapCache(env);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        for (int i = 0; i < dbs.length; i += 1) {
            dbs[i] = env.openDatabase(null, "foo-" + i, dbConfig);
        }
    }

    private void close() {
        for (int i = 0; i < N_DBS; i += 1) {
            if (dbs[i] != null) {
                dbs[i].close();
                dbs[i] = null;
            }
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    @Test
    public void testBaseline() {
        open(CacheMode.DEFAULT);
        doTestBaseline();
        close();
    }

    private void doTestBaseline() {

        for (int i = 0; i < N_DBS; i += 1) {
            write(dbs[i], DB_CACHE_SIZE);
        }
        long[] results = new long[100];
        for (int repeat = 0; repeat < 100; repeat += 1) {

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

            /*
             * Check that each DB uses approximately equal portions of the
             * cache.
             */
            StringBuilder buf = new StringBuilder();
            long low = Long.MAX_VALUE;
            long high = 0;
            for (int i = 0; i < N_DBS; i += 1) {
                long val = getDatabaseCacheBytes(dbs[i], true);
                buf.append(" db=" + i + " bytes=" + val);
                if (low > val) {
                    low = val;
                }
                if (high < val) {
                    high = val;
                }
            }
            long pct = (low * 100) / high;
            if (repeat > 75) {
                assertTrue(
                    "failed repeat=" + repeat + " " +
                    " with pct=" + pct + buf,
                    pct >= LRU_ACCURACY_PCT);
            }
            results[repeat] = pct;
        }
        Arrays.sort(results);
//        System.out.println(Arrays.toString(results));
    }

    @Test
    public void testCacheMode_KEEP_HOT() {
        open(CacheMode.KEEP_HOT);
        doTestBaseline();
        close();
    }

    @Test
    public void testCacheMode_UNCHANGED() {

        open();
        doTestCacheMode_UNCHANGED(CacheMode.UNCHANGED);
        close();
    }

    private void doTestCacheMode_UNCHANGED(CacheMode cacheMode) {
        for (int i = 0; i < N_DBS; i += 1) {
            write(dbs[i], DB_CACHE_SIZE);
        }
        long[] results = new long[100];
        for (int repeat = 0; repeat < 100; repeat += 1) {

            /* Read all DBs evenly. */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            boolean done = false;
            Cursor[] cursors = new Cursor[N_DBS];
            for (int j = 0; j < N_DBS; j++) {
                cursors[j] = dbs[j].openCursor(null, null);
            }
            cursors[0].setCacheMode(cacheMode);
            cursors[1].setCacheMode(cacheMode);
            cursors[2].setCacheMode(CacheMode.DEFAULT);
            cursors[3].setCacheMode(CacheMode.DEFAULT);
            cursors[4].setCacheMode(CacheMode.DEFAULT);
            for (int i = 0; !done; i += 1) {
                IntegerBinding.intToEntry(i, key);
                for (int j = 0; j < N_DBS; j += 1) {
                    if (cursors[j].getSearchKey(key, data, null) !=
                        OperationStatus.SUCCESS) {
                        done = true;
                    }
                }
            }

            for (int j = 0; j < N_DBS; j++) {
                cursors[j].close();
            }

            /*
             * Check that db[0] and db[1] use less than the other three.
             */
            StringBuilder buf = new StringBuilder();
            long[] dbBytes = new long[N_DBS];
            for (int i = 0; i < N_DBS; i += 1) {
                dbBytes[i] = getDatabaseCacheBytes(dbs[i], true);
                buf.append(" db=" + i + " bytes=" + dbBytes[i]);
            }
//            System.out.println(buf);
            assertTrue(dbBytes[0] < dbBytes[2]);
            assertTrue(dbBytes[0] < dbBytes[3]);
            assertTrue(dbBytes[0] < dbBytes[4]);
            assertTrue(dbBytes[1] < dbBytes[2]);
            assertTrue(dbBytes[1] < dbBytes[3]);
            assertTrue(dbBytes[1] < dbBytes[4]);
        }
        Arrays.sort(results);
        //System.out.println(Arrays.toString(results));
    }

    @Test
    public void testCacheMode_MAKE_COLD() {
        open();
        doTestCacheMode_UNCHANGED(CacheMode.MAKE_COLD);
        close();
    }

    @Test
    public void testCacheMode_EVICT_LN() {

        open();
        for (int i = 0; i < N_DBS; i += 1) {
            write(dbs[i], DB_CACHE_SIZE);
        }
        long[] results = new long[100];
        for (int repeat = 0; repeat < 100; repeat += 1) {

            /* Read all DBs evenly. */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            boolean done = false;
            Cursor[] cursors = new Cursor[N_DBS];
            for (int j = 0; j < N_DBS; j++) {
                cursors[j] = dbs[j].openCursor(null, null);
            }
            cursors[0].setCacheMode(CacheMode.EVICT_LN);
            cursors[1].setCacheMode(CacheMode.EVICT_LN);
            cursors[2].setCacheMode(CacheMode.UNCHANGED);
            cursors[3].setCacheMode(CacheMode.UNCHANGED);
            cursors[4].setCacheMode(CacheMode.UNCHANGED);
            for (int i = 0; !done; i += 1) {
                IntegerBinding.intToEntry(i, key);
                for (int j = 0; j < N_DBS; j += 1) {
                    if (cursors[j].getSearchKey(key, data, null) !=
                        OperationStatus.SUCCESS) {
                        done = true;
                    }
                }
            }

            for (int j = 0; j < N_DBS; j++) {
                cursors[j].close();
            }

            /*
             * Check that db[0] and db[1] use less than the other three.
             */
            StringBuilder buf = new StringBuilder();
            long[] dbBytes = new long[N_DBS];
            for (int i = 0; i < N_DBS; i += 1) {
                dbBytes[i] = getDatabaseCacheBytes(dbs[i], false);
                buf.append(" db=" + i + " bytes=" + dbBytes[i]);
            }
            assertTrue(dbBytes[0] < dbBytes[2]);
            assertTrue(dbBytes[0] < dbBytes[3]);
            assertTrue(dbBytes[0] < dbBytes[4]);
            assertTrue(dbBytes[1] < dbBytes[2]);
            assertTrue(dbBytes[1] < dbBytes[3]);
            assertTrue(dbBytes[1] < dbBytes[4]);
            //System.out.println(buf);
        }
        Arrays.sort(results);
        //System.out.println(Arrays.toString(results));

        close();
    }

    @Test
    public void testCacheMode_EVICT_BIN() {

        open();
        for (int i = 0; i < N_DBS; i += 1) {
            write(dbs[i], DB_CACHE_SIZE);
        }
        long[] results = new long[100];
        for (int repeat = 0; repeat < 100; repeat += 1) {

            /* Read all DBs evenly. */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            boolean done = false;
            Cursor[] cursors = new Cursor[N_DBS];
            for (int j = 0; j < N_DBS; j++) {
                cursors[j] = dbs[j].openCursor(null, null);
            }
            cursors[0].setCacheMode(CacheMode.EVICT_BIN);
            cursors[1].setCacheMode(CacheMode.EVICT_BIN);
            cursors[2].setCacheMode(CacheMode.EVICT_LN);
            cursors[3].setCacheMode(CacheMode.EVICT_LN);
            cursors[4].setCacheMode(CacheMode.EVICT_LN);
            for (int i = 0; !done; i += 1) {
                IntegerBinding.intToEntry(i, key);
                for (int j = 0; j < N_DBS; j += 1) {
                    if (cursors[j].getSearchKey(key, data, null) !=
                        OperationStatus.SUCCESS) {
                        done = true;
                    }
                }
            }

            for (int j = 0; j < N_DBS; j++) {
                cursors[j].close();
            }

            /*
             * Check that db[0] and db[1] use less than the other three.
             */
            final StringBuilder buf = new StringBuilder("repeat=");
            buf.append(repeat);
            final long[] dbBytes = new long[N_DBS];
            for (int i = 0; i < N_DBS; i += 1) {
                dbBytes[i] = getDatabaseCacheBytes(dbs[i], false);
                buf.append(" db=").append(i).append(" bytes=").
                    append(dbBytes[i]);
            }
            final String msg = buf.toString();

            assertTrue(msg, dbBytes[0] < dbBytes[2]);
            assertTrue(msg, dbBytes[0] < dbBytes[3]);
            assertTrue(msg, dbBytes[0] < dbBytes[4]);
            assertTrue(msg, dbBytes[1] < dbBytes[2]);
            assertTrue(msg, dbBytes[1] < dbBytes[3]);
            assertTrue(msg, dbBytes[1] < dbBytes[4]);
            //System.out.println(buf);
        }
        Arrays.sort(results);
        //System.out.println(Arrays.toString(results));

        close();
    }

    private long getDatabaseCacheBytes(Database db, boolean addOffHeap) {

        final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        final OffHeapCache ohCache = dbImpl.getEnv().getOffHeapCache();
        final INList ins = dbImpl.getEnv().getInMemoryINs();

        long total = 0;

        for (final IN in : ins) {
            if (in.getDatabase() != dbImpl) {
                continue;
            }

            total += in.getInMemorySize();

            if (addOffHeap) {
                total += ohCache.getINSize(in);
            }
        }

        return total;
    }

    /**
     * Writes enough records in the given envIndex environment to cause at
     * least minSizeToWrite bytes to be used in the cache.
     */
    private int write(Database db, int minSizeToWrite) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[ENTRY_DATA_SIZE]);
        int i;
        for (i = 0; i < minSizeToWrite / ENTRY_DATA_SIZE; i += 1) {
            IntegerBinding.intToEntry(i, key);
            db.put(null, key, data);
        }
        return i;
    }
}
