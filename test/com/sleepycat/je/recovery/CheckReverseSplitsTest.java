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
package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.recovery.stepwise.TestData;
import com.sleepycat.je.util.TestUtils;

/*
 * Exercise reverse splits (deletes of subtrees). Add a comprehensive
 * "stepwise" approach, where we run the test repeatedly, truncating the log
 * at each log entry point. At recovery, we check that we have all expected
 * values. In particular, this approach was required to reproduce SR [#13501],
 * which only failed if the log was broken off at a given point, between
 * the logging of an IN and the update of a mapln.
 */
public class CheckReverseSplitsTest extends CheckBase {

    private static final String DB_NAME = "simpleDB";

    private int max = 12;
    private boolean useDups;
    private static CheckpointConfig FORCE_CONFIG = new CheckpointConfig();
    static {
        FORCE_CONFIG.setForce(true);
    }

    /**
     * SR #13501
     * Reverse splits require the same upward propagation as regular splits,
     * to avoid logging inconsistent versions of ancestor INs.
     */
    @Test
    public void testReverseSplit()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "4");
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(useDups);
        dbConfig.setAllowCreate(true);

        /* Run the full test case w/out truncating the log. */
        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator(true /* generate log description */){
                        void generateData(Database db)
                            throws DatabaseException {
                            setupReverseSplit(db);
                        }
                    },
                    envConfig, dbConfig);

        /*
         * Now run the test in a stepwise loop, truncate after each
         * log entry.
         */

        /* Establish the base set of records we expect. */
        HashSet<TestData> currentExpected = new HashSet<TestData>();
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        for (int i = 2; i < max; i++) {
            if (useDups) {
                IntegerBinding.intToEntry(0, keyEntry);
            } else {
                IntegerBinding.intToEntry(i, keyEntry);
            }
            IntegerBinding.intToEntry(i, dataEntry);
            currentExpected.add(new TestData(keyEntry, dataEntry));
        }

        stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected, 0);
    }

    @Test
    public void testReverseSplitDups()
        throws Throwable {

        useDups = true;
        testReverseSplit();
    }

    /**
     * Create this:
     * <p>
     * <pre>

                         INa                        level 3
                   /           \
                INb            INc                  level 2
             /   |    \        /  \
           BINs BINt  BINu   BINv  BINw             level 1
     * </pre>
     * <p>
     * First provoke an IN compression which removes BINs, and then
     * provoke a split of BINw which results in propagating the change
     * all the way up the tree. The bug therefore created a version of INa
     * on disk which did not include the removal of BINs.
     */
    private void setupReverseSplit(Database db)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so it grows to 3 levels. */
        for (int i = 0; i < max; i ++) {
            if (useDups) {
                IntegerBinding.intToEntry(0, key);
            } else {
                IntegerBinding.intToEntry(i, key);
            }
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        /* Empty out the leftmost bin */
        Cursor c = db.openCursor(null, null);
        try {
            assertEquals(OperationStatus.SUCCESS, c.getFirst(key, data,
                                                         LockMode.DEFAULT));
            assertEquals(OperationStatus.SUCCESS, c.delete());
            assertEquals(OperationStatus.SUCCESS,
                         c.getFirst(key, data, LockMode.DEFAULT));
            assertEquals(OperationStatus.SUCCESS, c.delete());
        } finally {
            c.close();
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After deletes");

        /* For log description start. */
        setStepwiseStart();

        /*
         * Checkpoint so that the deleted lns are not replayed, and recovery
         * relies on INs.
         */
        env.checkpoint(FORCE_CONFIG);

        /* Now remove the empty BIN. */
        env.compress();
        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After compress");

        /*
         * Add enough keys to split the level 2 IN on the right hand side.
         * This makes an INa which still references the obsolete BINs.
         * Truncate the log before the mapLN which refers to the new INa,
         * else the case will not fail, because recovery will first apply the
         * new INa, and then apply the INDelete of BINs. We want this case
         * to apply the INDelete of BINs, and then follow with a splicing in
         * of the new root.
         */
        for (int i = max; i < max+13; i ++) {
            if (useDups) {
                IntegerBinding.intToEntry(0, key);
            } else {
                IntegerBinding.intToEntry(i, key);
            }
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After data setup");

    }

    /**
     * Create a tree, remove it all, replace with new records.
     */
    @Test
    public void testCompleteRemoval()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "4");
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(useDups);
        dbConfig.setAllowCreate(true);

        /* Run the full test case w/out truncating the log. */
        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator(true /* generate log description. */){
                        void generateData(Database db)
                            throws DatabaseException {
                            setupCompleteRemoval(db);
                        }
                    },
                    envConfig, dbConfig);

        /*
         * Now run the test in a stepwise loop, truncate after each log entry.
         * Our baseline expected set is empty -- no records expected.
         */
        HashSet<TestData> currentExpected = new HashSet<TestData>();
        stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected, 0);
    }

    @Test
    public void testCompleteRemovalDups()
        throws Throwable {

        useDups = true;
        testCompleteRemoval();
    }

    /**
     * Create a populated tree, delete all records, then begin to insert again.
     */
    private void setupCompleteRemoval(Database db)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so it grows to 3 levels. */
        for (int i = 0; i < max; i ++) {
            if (useDups) {
                IntegerBinding.intToEntry(0, key);
            } else {
                IntegerBinding.intToEntry(i, key);
            }
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After inserts");

        /* Now delete it all. */
        Cursor c = db.openCursor(null, null);
        try {
            int count = 0;
            while (c.getNext(key, data, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {
                assertEquals(OperationStatus.SUCCESS, c.delete());
                count++;
            }
        } finally {
            c.close();
        }
        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After deletes");

        /* For log description start. */
        setStepwiseStart();

        /* Checkpoint before, so we don't simply replay all the  deleted LNs */
        env.checkpoint(FORCE_CONFIG);

        /* Compress, and make sure the subtree was removed. */
        env.compress();
        BtreeStats stats = (BtreeStats) db.getStats(new StatsConfig());
        if (useDups) {
            assertEquals(0, stats.getDuplicateInternalNodeCount());
        } else {
            assertEquals(1, stats.getBottomInternalNodeCount());
        }

        /* Insert new data. */
        for (int i = max*2; i < ((max*2) +5); i ++) {
            if (useDups) {
                IntegerBinding.intToEntry(0, key);
            } else {
                IntegerBinding.intToEntry(i, key);
            }
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
    }
}
