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
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
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
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class EvictSelectionTest extends TestBase {
    private final File envHome;
    private final int scanSize = 5;
    private Environment env;
    private EnvironmentImpl envImpl;

    public EvictSelectionTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Override
    @After
    public void tearDown() {

        try {
            if (env != null) {
                env.close();
            }
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }
        env = null;
        envImpl = null;
    }

    static class EvictProfile implements TestHook<IN> {
        /* Keep a list of candidate nodes. */
        private final List<Long> candidates = new ArrayList<Long>();

        /* Remember that this node was targeted. */
        public void doHook(IN target) {
            candidates.add(Long.valueOf(target.getNodeId()));
        }

        public List<Long> getCandidates() {
            return candidates;
        }

        public void hookSetup() {
            candidates.clear();
        }

        public void doIOHook() {};
        public void doHook() {}

        public IN getHookValue() {
            return null;
        };
    }

    /*
     * We might call evict on an empty INList if the cache is set very low
     * at recovery time.
     */
    @Test
    public void testEmptyINList()
        throws Throwable {

        /* Create an environment, database, and insert some data. */
        initialize(true);

        env.close();
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setCacheSize(MemoryBudget.MIN_MAX_MEMORY_SIZE);
        env = new Environment(envHome, envConfig);
        env.close();
        env = null;
    }

    /*
     * Create an environment, database, and insert some data.
     */
    private void initialize(boolean makeDatabase)
        throws DatabaseException {

        /* Environment */

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_EVICTOR.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_CLEANER.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_CHECKPOINTER.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 ENV_RUN_INCOMPRESSOR.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.
                                 NODE_MAX.getName(), "4");

        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        if (makeDatabase) {
            /* Database */

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            Database db = env.openDatabase(null, "foo", dbConfig);

            /* Insert enough keys to get an odd number of nodes */

            DatabaseEntry keyAndData = new DatabaseEntry();
            for (int i = 0; i < 110; i++) {
                IntegerBinding.intToEntry(i, keyAndData);
                db.put(null, keyAndData, keyAndData);
            }

            db.close();
        }
    }

    /**
     * Tests a fix for an eviction bug that could cause an OOME in a read-only
     * environment.  [#17590]
     *
     * Before the bug fix, a dirty IN prevented eviction from working if the
     * dirty IN is returned by Evictor.selectIN repeatedly, only to be rejected
     * by Evictor.evictIN because it is dirty.  A dirty IN was considered as a
     * target and sometimes selected by selectIN as a way to avoid an infinite
     * loop when all INs are dirty.  This is unnecessary, since a condition was
     * added to cause the selectIN loop to terminate when all INs in the INList
     * have been iterated.  Now, with the fix, a dirty IN in a read-only
     * environment is never considered as a target or returned by selectIN.
     *
     * The OOME was reproduced with a simple test that uses a cursor to iterate
     * through 100k records, each 100k in size, in a read-only enviroment with
     * a 16m heap.  However, reproducing the problem in a fast-running unit
     * test is very difficult.  Instead, since the code change only impacts a
     * read-only environment, this unit test only ensures that the fix does not
     * cause an infinte loop when all nodes are dirty.
     */
    @Test
    public void testReadOnlyAllDirty()
        throws Throwable {

        /* Create an environment, database, and insert some data. */
        initialize(true /*makeDatabase*/);
        env.close();
        env = null;
        envImpl = null;

        /* Open the environment read-only. */
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setReadOnly(true);
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        /* Load everything into cache. */
        {
            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(true);
            final Database db = env.openDatabase(null, "foo", dbConfig);
            final Cursor cursor = db.openCursor(null, null);
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            OperationStatus status = cursor.getFirst(key, data, null);
            while (status == OperationStatus.SUCCESS) {
                status = cursor.getNext(key, data, null);
            }
            cursor.close();
            db.close();
        }

        /* Artificially make all nodes dirty in a read-only environment. */
        for (IN in : envImpl.getInMemoryINs()) {
            in.setDirty(true);
        }

        /*
         * Force an eviction.  No nodes will be selected for an eviction,
         * because all nodes are dirty.  If the (nIterated < maxNodesToIterate)
         * condition is removed from the selectIN loop, an infinite loop will
         * occur.
         */
        final EnvironmentMutableConfig mutableConfig = env.getMutableConfig();
        mutableConfig.setCacheSize(MemoryBudget.MIN_MAX_MEMORY_SIZE);
        env.setMutableConfig(mutableConfig);
        final StatsConfig clearStats = new StatsConfig();
        clearStats.setClear(true);
        EnvironmentStats stats = env.getStats(clearStats);
        env.evictMemory();
        stats = env.getStats(clearStats);
        assertEquals(0, stats.getNNodesSelected());

        env.close();
        env = null;
        envImpl = null;
    }
}
