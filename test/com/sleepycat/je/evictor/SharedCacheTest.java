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
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Tests the shared cache feature enabled via Environment.setSharedCache(true).
 */
public class SharedCacheTest extends TestBase {

    private static final int N_ENVS = 5;
    private static final int ONE_MB = 1 << 20;
    private static final int ENV_DATA_SIZE = ONE_MB;
    private static final int TOTAL_DATA_SIZE = N_ENVS * ENV_DATA_SIZE;
    private static final int LOG_BUFFER_SIZE = (ENV_DATA_SIZE * 7) / 100;
    private static final int MIN_DATA_SIZE = 50 * 1024;
    private static final int LRU_ACCURACY_PCT = 60;
    private static final int ENTRY_DATA_SIZE = 500;
    private static final String TEST_PREFIX = "SharedCacheTest_";
    private static final StatsConfig CLEAR_CONFIG = new StatsConfig();

    static {
        CLEAR_CONFIG.setClear(true);
    }

    private File envHome;
    private File[] dirs;
    private Environment[] envs;
    private Database[] dbs;
    private boolean sharedCache = true;
    private boolean offHeapCache = false;
    private long actualTotalCacheSize;

    public SharedCacheTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        dirs = new File[N_ENVS];
        envs = new Environment[N_ENVS];
        dbs = new Database[N_ENVS];

        for (int i = 0; i < N_ENVS; i += 1) {
            dirs[i] = new File(envHome, TEST_PREFIX + i);
            dirs[i].mkdir();
            assertTrue(dirs[i].isDirectory());
        }

        IN.ACCUMULATED_LIMIT = 0;
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

        IN.ACCUMULATED_LIMIT = IN.ACCUMULATED_LIMIT_DEFAULT;
    }

    @Test
    public void testBaselineOffHeap() {
        offHeapCache = true;
        testBaseline();
    }

    @Test
    public void testBaseline() {

        /* Open all DBs in the same environment. */
        final int N_DBS = N_ENVS;
        sharedCache = false;
        openOne(0);
        DatabaseConfig dbConfig = dbs[0].getConfig();
        for (int i = 1; i < N_DBS; i += 1) {
            dbs[i] = envs[0].openDatabase(null, "foo" + i, dbConfig);
        }
        for (int i = 0; i < N_DBS; i += 1) {
            write(i, ENV_DATA_SIZE);
        }

        envs[0].sync(); // So the separate LRU dirty list isn't used

        for (int iter = 0; iter < 50; iter += 1) {

            /* Read all DBs evenly. */
            for (int repeat = 0; repeat < 2; repeat += 1) {
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

            /*
             * Check that each DB uses approximately equal portions of the
             * cache.
             */
            StringBuilder buf = new StringBuilder();
            long low = Long.MAX_VALUE;
            long high = 0;
            for (int i = 0; i < N_DBS; i += 1) {
                long val = getDatabaseCacheBytes(dbs[i], false);
                buf.append(" db=").append(i).append(" bytes=").append(val);
                if (low > val) {
                    low = val;
                }
                if (high < val) {
                    high = val;
                }
            }

            final long pct = (low * 100) / high;

//            System.out.println("Baseline LRU accuracy pct=" + pct + buf);

            if (iter > 25) {
                assertTrue(
                    "failed with pct=" + pct + buf,
                    pct >= LRU_ACCURACY_PCT);
            }
        }

        for (int i = 1; i < N_DBS; i += 1) {
            dbs[i].close();
            dbs[i] = null;
        }
        closeOne(0);
    }

    private long getDatabaseCacheBytes(Database db, boolean offHeapOnly) {

        final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);

        final OffHeapCache ohCache = dbImpl.getEnv().getOffHeapCache();

        long total = 0;

        for (IN in : dbImpl.getEnv().getInMemoryINs()) {

            if (in.getDatabase() != dbImpl) {
                continue;
            }

            if (!offHeapOnly) {
                total += in.getInMemorySize();
            }

            if (offHeapCache) {
                total += ohCache.getINSize(in);
            }
        }

        return total;
    }

    @Test
    public void testWriteOneEnvAtATimeOffHeap() {
        offHeapCache = true;
        testWriteOneEnvAtATime();
    }

    /**
     * Writes to each env one at a time, writing enough data in each env to fill
     * the entire cache.  Each env in turn takes up a large majority of the
     * cache.
     */
    @Test
    public void testWriteOneEnvAtATime() {

        final int SMALL_DATA_SIZE = MIN_DATA_SIZE + (20 * 1024);
        final int SMALL_TOTAL_SIZE = SMALL_DATA_SIZE + LOG_BUFFER_SIZE;
        final int BIG_TOTAL_SIZE =
            ENV_DATA_SIZE - ((N_ENVS - 1) * SMALL_TOTAL_SIZE);

        openAll();

        for (int i = 0; i < N_ENVS; i += 1) {
            write(i, TOTAL_DATA_SIZE);

            final long sharedTotal = getSharedCacheTotal(i);
            final long localTotal = getLocalCacheTotal(i);

            String msg = "env=" + i +
                " total=" + localTotal +
                " shared=" + sharedTotal;

            assertTrue(msg, sharedTotal >= BIG_TOTAL_SIZE);
            assertTrue(msg, localTotal >= BIG_TOTAL_SIZE);
        }

        closeAll();
    }

    private long getLocalCacheTotal(final int i) {
        return getLocalCacheTotal(i, null);
    }

    private long getLocalCacheTotal(final int i, final StringBuilder buf) {

        final EnvironmentStats stats = envs[i].getStats(null);

        final long mainSize = stats.getCacheTotalBytes();

        final long offHeapSize = offHeapCache ?
            getDatabaseCacheBytes(dbs[i], true) : 0;

        final long total = mainSize + offHeapSize;

        if (buf != null) {
            buf.append("\nenv=").append(i);
            buf.append(" main=").append(mainSize);
            buf.append(" offHeap=").append(offHeapSize);
            buf.append(" total=").append(total);
        }

        return total;
    }

    private long getSharedCacheTotal(final int i) {
        final EnvironmentStats stats = envs[i].getStats(null);
        long size = stats.getSharedCacheTotalBytes();
        if (offHeapCache) {
            size += stats.getOffHeapTotalBytes();
        }
        return size;
    }

    @Test
    public void testWriteAllEnvsEvenlyOffHeap() {
        offHeapCache = true;
        testWriteAllEnvsEvenly();
    }

    /**
     * Writes alternating records to each env, writing enough data to fill the
     * entire cache.  Each env takes up roughly equal portions of the cache.
     */
    @Test
    public void testWriteAllEnvsEvenly() {

        openAll();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[ENTRY_DATA_SIZE]);
        for (int i = 0; i < 2 * (ENV_DATA_SIZE / ENTRY_DATA_SIZE); i += 1) {
            IntegerBinding.intToEntry(i, key);
            for (int j = 0; j < N_ENVS; j += 1) {
                dbs[j].put(null, key, data);
            }
            checkStatsConsistency();
        }
        checkEvenCacheUsage();
        closeAll();
    }

    @Test
    public void testOpenCloseOffHeap() {
        offHeapCache = true;
        testOpenClose();
    }

    /**
     * Checks that the cache usage changes appropriately as environments are
     * opened and closed.
     */
    @Test
    public void testOpenClose() {

        openAll();
        int nRecs = 0;
        for (int i = 0; i < N_ENVS; i += 1) {
            int n = write(i, TOTAL_DATA_SIZE);
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

    @Test
    public void testHotnessOffHeap() {
        offHeapCache = true;
        testHotness();
    }

    /**
     * Checks that an environment with hot data uses more of the cache.
     */
    @Test
    public void testHotness() {

        final int HOT_CACHE_SIZE = (int) (1.5 * ENV_DATA_SIZE);

        openAll();

        int nRecs = Integer.MAX_VALUE;

        for (int i = 0; i < N_ENVS; i += 1) {
            int n = write(i, TOTAL_DATA_SIZE);
            if (nRecs > n) {
                nRecs = n;
            }
            envs[i].sync();
        }

        readEvenly(nRecs);

        /* Keep one env "hot". */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int i = 0; i < N_ENVS; i += 1) {
            for (int j = 0; j < N_ENVS; j += 1) {
                for (int k = 0; k < nRecs; k += 1) {
                    IntegerBinding.intToEntry(k, key);
                    dbs[i].get(null, key, data, null);
                    dbs[j].get(null, key, data, null);
                }
                checkStatsConsistency();

                if (getLocalCacheTotal(i) < HOT_CACHE_SIZE ||
                    getLocalCacheTotal(j) < HOT_CACHE_SIZE) {

                    EnvironmentStats iStats = envs[i].getStats(null);
                    EnvironmentStats jStats = envs[j].getStats(null);

                    StringBuilder msg = new StringBuilder();
                    msg.append("Hot cache size is below " + HOT_CACHE_SIZE +
                        " for env " + i + " or " + j);
                    for (int k = 0; k < N_ENVS; k += 1) {
                        msg.append("\n**** ENV " + k + " ****\n");
                        msg.append(envs[k].getStats(null));
                    }
                    fail(msg.toString());
                }
            }
        }
        closeAll();
    }

    /**
     * Tests changing the cache size.
     */
    @Test
    public void testMutateCacheSize() {

        final int HALF_DATA_SIZE = TOTAL_DATA_SIZE / 2;
        openAll();

        int nRecs = 0;
        for (int i = 0; i < N_ENVS; i += 1) {
            int n = write(i, ENV_DATA_SIZE);
            if (nRecs < n) {
                nRecs = n;
            }
        }

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(envs[0]);
        DbConfigManager configManager = envImpl.getConfigManager();

        long evictBytes =
            configManager.getLong(EnvironmentParams.EVICTOR_EVICT_BYTES);

        long nodeMaxEntries =
            configManager.getInt(EnvironmentParams.NODE_MAX);

        long maxLNBytesPerBIN =
            (nodeMaxEntries - 1) *
            (MemoryBudget.LN_OVERHEAD +
             MemoryBudget.byteArraySize(ENTRY_DATA_SIZE));

        long maxFreeMem = (evictBytes * 2)  + maxLNBytesPerBIN;

        /* Full cache size. */
        readEvenly(nRecs);

        long memConsumed = envs[0].getStats(null).getSharedCacheTotalBytes();

//        System.out.println(
//            "Mem consumed = " + memConsumed +
//            " free mem = " + (actualTotalCacheSize - memConsumed) +
//            " max free allowed = " + maxFreeMem);

        assertTrue(Math.abs(actualTotalCacheSize - memConsumed) < maxFreeMem);

        /* Halve cache size. */
        EnvironmentMutableConfig config = envs[0].getMutableConfig();
        config.setCacheSize(HALF_DATA_SIZE);
        envs[0].setMutableConfig(config);
        final long actualHalfSize =
            TestUtils.adjustSharedCacheSize(envs, HALF_DATA_SIZE);

        readEvenly(nRecs);

        memConsumed = envs[0].getStats(null).getSharedCacheTotalBytes();

//        System.out.println(
//            "Mem consumed = " + memConsumed +
//            " free mem = " + (actualHalfSize - memConsumed) +
//            " max free allowed = " + maxFreeMem);

        assertTrue(Math.abs(actualHalfSize - memConsumed) < maxFreeMem);

        /* Full cache size. */
        config = envs[0].getMutableConfig();
        config.setCacheSize(TOTAL_DATA_SIZE);
        envs[0].setMutableConfig(config);
        actualTotalCacheSize =
            TestUtils.adjustSharedCacheSize(envs, TOTAL_DATA_SIZE);

        readEvenly(nRecs);

        memConsumed = envs[0].getStats(null).getSharedCacheTotalBytes();

//        System.out.println(
//            "Mem consumed = " + memConsumed +
//            " free mem = " + (actualTotalCacheSize - memConsumed) +
//            " max free allowed = " + maxFreeMem);

        assertTrue(Math.abs(actualTotalCacheSize - memConsumed) < maxFreeMem);

        closeAll();
    }

    private void openAll() {

        for (int i = 0; i < N_ENVS; i += 1) {
            openOne(i);
        }
    }

    private void openOne(int i) {

        IN.ACCUMULATED_LIMIT = 0;

        final long mainSize;
        final long offHeapSize;
        if (offHeapCache) {
            mainSize = ONE_MB;
            offHeapSize = TOTAL_DATA_SIZE;
        } else {
            mainSize = TOTAL_DATA_SIZE;
            offHeapSize = 0;
        }

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setSharedCache(sharedCache);
        envConfig.setCacheSize(mainSize);
        envConfig.setOffHeapCacheSize(offHeapSize);
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
        envConfig.setConfigParam(
            EnvironmentConfig.STATS_COLLECT, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.EVICTOR_EVICT_BYTES, "10240");
        envConfig.setConfigParam(
            EnvironmentConfig.OFFHEAP_EVICT_BYTES, "10240");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        /*
         * Because the evictors each have multiple LRU lists per LRUSet, the
         * accuracy of the LRU varies too much to be predictable in this test,
         * especially due to outliers on some machines. Use a single LRU list
         * per LRUSet.
         */
        envConfig.setConfigParam(
            EnvironmentConfig.OFFHEAP_N_LRU_LISTS, "1");
        envConfig.setConfigParam(
            EnvironmentConfig.EVICTOR_N_LRU_LISTS, "1");

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        envs[i] = new Environment(dirs[i], envConfig);
        dbs[i] = envs[i].openDatabase(null, "foo", dbConfig);

        if (offHeapCache) {
            actualTotalCacheSize =
                TestUtils.adjustSharedCacheSize(envs, ONE_MB) +
                TOTAL_DATA_SIZE;
        } else {
            actualTotalCacheSize =
                TestUtils.adjustSharedCacheSize(envs, TOTAL_DATA_SIZE);
        }
    }

    private void closeAll() {
        for (int i = 0; i < N_ENVS; i += 1) {
            closeOne(i);
        }
    }

    private void closeOne(int i) {
        if (dbs[i] != null) {
            dbs[i].close();
            dbs[i] = null;
        }
        if (envs[i] != null) {
            envs[i].close();
            envs[i] = null;
        }
    }

    /**
     * Writes enough records in the given envIndex environment to cause at
     * least minSizeToWrite bytes to be used in the cache.
     */
    private int write(int envIndex, int minSizeToWrite) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[ENTRY_DATA_SIZE]);
        int i;
        for (i = 0; i < minSizeToWrite / ENTRY_DATA_SIZE; i += 1) {
            IntegerBinding.intToEntry(i, key);
            dbs[envIndex].put(null, key, data);
        }
        checkStatsConsistency();
        return i;
    }

    /**
     * Reads alternating records from each env, reading all records from each
     * env.  Checks that all environments use roughly equal portions of the
     * cache.
     */
    private void readEvenly(int nRecs) {

        /*
        EnvironmentImpl firstEnvImpl = DbInternal.getNonNullEnvImpl(envs[0]);
        LRUEvictor evictor = (LRUEvictor)firstEnvImpl.getEvictor();

        ArrayList<LRUEvictor.LRUDebugStats> statsList =
            new ArrayList<LRUEvictor.LRUDebugStats>(N_ENVS);

        for (int i = 0; i < N_ENVS; i += 1) {
            statsList.add(new LRUEvictor.LRUDebugStats());
        }
        */

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Repeat reads twice to give the LRU a fighting chance. */
        for (int repeat = 0; repeat < 2; repeat += 1) {

            /*
            for (int k = 0; k < N_ENVS; k += 1) {
                EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(envs[k]);
                System.out.println("Before-read LRU stats for env " + k);
                evictor.getPri1LRUStats(envImpl, statsList.get(k));
                System.out.println("MIXED: " + statsList.get(k));
                evictor.getPri2LRUStats(envImpl, statsList.get(k));
                System.out.println("DIRTY: " + statsList.get(k));
                System.out.println("");
            }
            */

            for (int i = 0; i < nRecs; i += 1) {

                IntegerBinding.intToEntry(i, key);
                for (int j = 0; j < N_ENVS; j += 1) {
                    if (dbs[j] != null) {
                        dbs[j].get(null, key, data, null);
                    }
                }

                /*
                if (i % 512 == 0 || i == 1600) {
                    for (int k = 0; k < N_ENVS; k += 1) {
                        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(envs[k]);
                        System.out.println("LRU stats for env " + k +
                                           " at record " + i);
                        evictor.getPri1LRUStats(envImpl, statsList.get(k));
                        System.out.println("MIXED: " + statsList.get(k));
                        evictor.getPri2LRUStats(envImpl, statsList.get(k));
                        System.out.println("DIRTY: " + statsList.get(k));
                        System.out.println("");
                    }
                }
                */
                checkStatsConsistency();
            }
        }

        /*
        for (int i = 0; i < N_ENVS; i += 1) {
            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(envs[i]);
            System.out.println("After-read LRU stats for env " + i);
            evictor.getPri1LRUStats(envImpl, statsList.get(i));
            System.out.println("MIXED: " + statsList.get(i));
            evictor.getPri2LRUStats(envImpl, statsList.get(i));
            System.out.println("DIRTY: " + statsList.get(i));
            System.out.println("");
        }
        */

        checkEvenCacheUsage();
    }

    /**
     * Checks that each env uses approximately equal portions of the cache.
     * How equal the portions are depends on the accuracy of the LRU.
     */
    private void checkEvenCacheUsage() {

        final StringBuilder buf = new StringBuilder();

        long low = Long.MAX_VALUE;
        long high = 0;

        for (int i = 0; i < N_ENVS; i += 1) {
            if (envs[i] == null) {
                continue;
            }

            final long val = getLocalCacheTotal(i, buf);

            if (low > val) {
                low = val;
            }
            if (high < val) {
                high = val;
            }
        }

        long pct = (low * 100) / high;
        if (pct < LRU_ACCURACY_PCT) {
            fail("failed with pct=" + pct + buf);
        }
//        System.out.println("readEven LRU accuracy pct=" + pct + buf);
    }

    /**
     * Checks that the sum of all env cache usages is the total cache usage,
     * and other self-consistency checks.
     */
    private void checkStatsConsistency() {

        if (!sharedCache) {
            return;
        }

        long localTotal = 0;
        long sharedTotal = -1;
        int nShared = 0;
        EnvironmentStats stats = null;

        for (int i = 0; i < N_ENVS; i += 1) {
            if (envs[i] != null) {
                envs[i].evictMemory();
            }
        }

        for (int i = 0; i < N_ENVS; i += 1) {
            if (envs[i] == null) {
                continue;
            }

            if (sharedTotal == -1) {
                sharedTotal = getSharedCacheTotal(i);
            } else {
                assertEquals(sharedTotal, getSharedCacheTotal(i));
            }

            final long local = getLocalCacheTotal(i);

            localTotal += local;

//            System.out.println(
//                "Env= " + i +
//                " localTotal= " + localTotal +
//                " localTotal=" + local +
//                " shared=" + sharedTotal);

            nShared += 1;

            if (stats == null) {
                stats = envs[i].getStats(null);
            }
        }

        assertEquals(nShared, stats.getNSharedCacheEnvironments());
        assertEquals(sharedTotal, localTotal);

        final long expectMax =
            actualTotalCacheSize + (actualTotalCacheSize / 10);

        assertTrue(
            "sharedTotal= " + sharedTotal +
            " expectMax= " + expectMax,
            sharedTotal < expectMax);
    }
}
