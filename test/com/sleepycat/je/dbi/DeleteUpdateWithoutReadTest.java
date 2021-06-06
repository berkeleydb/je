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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.trigger.TestBase.DBT;
import com.sleepycat.je.trigger.Trigger;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Checks that deletions and updates can be performed without reading the old
 * record, when it is not in cache.
 */
public class DeleteUpdateWithoutReadTest extends DualTestCase {

    private static final int NUM_RECORDS = 5;
    private static final String DB_NAME = "foo";
    private static final StatsConfig CLEAR_STATS;
    static {
        CLEAR_STATS = new StatsConfig();
        CLEAR_STATS.setClear(true);
    }
    private final File envHome;
    private boolean dups;
    private Environment env;
    private Database db;
    private final boolean isSerializable =
        "serializable".equals(System.getProperty("isolationLevel"));

    public DeleteUpdateWithoutReadTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() 
        throws Exception {

        try {
            super.tearDown();
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }
        env = null;
        db = null;
    }

    /*
     * Delete/update not currently optimized to avoid fetching for dup DBs.
     */
    @Test
    public void testNoReadDups() {
        dups = true;
        testNoFetch();
    }

    /* Test that delete and updates don't need to fetch. */
    @Test
    public void testNoFetch() {
        open(false);
        env.getStats(CLEAR_STATS);
        EnvironmentStats stats;

        /* Insert */
        writeData(false, false /*update*/);
        stats = env.getStats(CLEAR_STATS);
        assertEquals(0, TestUtils.getNLNsLoaded(stats));
        assertEquals(0, stats.getNLNsFetch());

        /* Update */
        writeData(true, false /*update*/);
        stats = env.getStats(CLEAR_STATS);
        assertEquals(0, TestUtils.getNLNsLoaded(stats));
        assertEquals(0, stats.getNLNsFetch());

        /* Delete */
        deleteData();
        stats = env.getStats(CLEAR_STATS);
        assertEquals(0, TestUtils.getNLNsLoaded(stats));
        assertEquals(0, stats.getNLNsFetch());

        /* Compress */
        env.compress();
        stats = env.getStats(CLEAR_STATS);
        assertEquals(0, TestUtils.getNLNsLoaded(stats));
        /* Compressor does one fetch for MapLN. */
        assertEquals(1, stats.getNLNsFetch());

        /* Truncate the database. */
        db.close();
        stats = env.getStats(CLEAR_STATS);
        env.truncateDatabase(null, DB_NAME, false);
        assertEquals(0, TestUtils.getNLNsLoaded(stats));
        assertEquals(0, stats.getNLNsFetch());
        db = null;
        close();
    }

    /* 
     * Test the cases where updates and deletes are required to fetch.
     */
    @Test
    public void testFetch()
        throws Throwable {

        open(false);
        env.getStats(CLEAR_STATS);
        EnvironmentStats stats;

        /* Insert */
        writeData(false, false);
        stats = env.getStats(CLEAR_STATS);
        assertEquals(0, TestUtils.getNLNsLoaded(stats));
        assertEquals(0, stats.getNLNsFetch());

        /* Update with partial DatabaseEntry will fetch. */
        writeData(true, true);
        stats = env.getStats(CLEAR_STATS);
        assertEquals(5, TestUtils.getNLNsLoaded(stats));
        assertEquals(5, stats.getNLNsFetch());
        close();

        /* Configuring triggers will require fetching. */
        open(true);
        env.getStats(CLEAR_STATS);
        writeData(true, false);
        stats = env.getStats(CLEAR_STATS);
        assertEquals(5, TestUtils.getNLNsLoaded(stats));
        assertTrue(stats.getNLNsFetch() >= 5);
        close();
    }

    private void open(boolean useTriggers) {
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setCacheMode(CacheMode.EVICT_LN);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = create(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        if (useTriggers) {
            List<Trigger> triggers =
                new LinkedList<Trigger>(Arrays.asList((Trigger) new DBT("t1"),
                                        (Trigger) new DBT("t2")));
            dbConfig.setTriggers(triggers);
            dbConfig.setOverrideTriggers(true);
        }
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    private void close() {
        if (db != null) {
            db.close();
        }
        close(env);
    }

    private void writeData(boolean update, boolean partial) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[1000]);
        if (partial) {
            data.setPartial(10, 100, true);
        }
        for (int i = 0; i < NUM_RECORDS; i += 1) {
            IntegerBinding.intToEntry(i, key);
            final OperationStatus status;
            if (update) {
                status = db.put(null, key, data);
            } else {
                status = db.putNoOverwrite(null, key, data);
            }
            assertSame(OperationStatus.SUCCESS, status);
        }
    }

    private void deleteData() {
        final DatabaseEntry key = new DatabaseEntry();
        for (int i = 0; i < NUM_RECORDS; i += 1) {
            IntegerBinding.intToEntry(i, key);
            final OperationStatus status = db.delete(null, key);
            assertSame(OperationStatus.SUCCESS, status);
        }
    }
}
