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
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.recovery.stepwise.TestData;
import com.sleepycat.je.util.TestUtils;

public class CheckSplitsTest extends CheckBase {

    private static final String DB_NAME = "simpleDB";
    private boolean useDups;

    /**
     * Test basic inserts.
     */
    @Test
    public void testBasicInsert()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "4");
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(useDups);

        DatabaseConfig validateDbConfig = new DatabaseConfig();
        validateDbConfig.setSortedDuplicates(useDups);

        testOneCase(DB_NAME,
                    envConfig,
                    dbConfig,
                    new TestGenerator(true /* generate log description */){
                        void generateData(Database db)
                            throws DatabaseException {

                            setupBasicInsertData(db);
                        }
                    },
                    envConfig,
                    validateDbConfig);

        /*
         * Now run the test in a stepwise loop, truncate after each
         * log entry. We start the steps before the inserts, so the base
         * expected set is empty.
         */
        HashSet<TestData> currentExpected = new HashSet<TestData>();
        stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected,  0);
    }

    @Test
    public void testBasicInsertDups()
        throws Throwable {

        useDups = true;
        testBasicInsert();
    }

    private void setupBasicInsertData(Database db)
        throws DatabaseException {

        setStepwiseStart();

        /* If using dups, create several dup trees. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < 21; i++) {
            if (useDups) {
                IntegerBinding.intToEntry(i%3, key);
            } else {
                IntegerBinding.intToEntry(i, key);
            }
            IntegerBinding.intToEntry(i, data);
            db.put(null, key, data);
        }
    }

    /**
     * SR #10715
     * Splits must propagate up the tree at split time to avoid logging
     * inconsistent versions of ancestor INs.
     */
    @Test
    public void testSplitPropagation()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "6");
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        EnvironmentConfig restartConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "6");
        envConfig.setTransactional(true);

        testOneCase(DB_NAME,
                    envConfig,
                    dbConfig,
                    new TestGenerator(true){
                        void generateData(Database db)
                            throws DatabaseException {

                            setupSplitData(db);
                        }
                    },
                    restartConfig,
                    new DatabaseConfig());

        /*
         * Now run the test in a stepwise loop, truncate after each
         * log entry. We start the steps before the inserts, so the base
         * expected set is empty.
         */
        HashSet<TestData> currentExpected = new HashSet<TestData>();
        if (TestUtils.runLongTests()) {
            stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected,  0);
        }
    }

    private void setupSplitData(Database db)
        throws DatabaseException {

        setStepwiseStart();

        int max = 120;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so it grows to 4 levels, then checkpoint. */

        for (int i = 0; i < max; i ++) {
            IntegerBinding.intToEntry(i*10, key);
            IntegerBinding.intToEntry(i*10, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);
        env.checkpoint(ckptConfig);

        /* Add enough keys to split the left hand branch again. */
        for (int i = 50; i < 100; i+=2) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        /* Add enough keys to split the right hand branch. */
        for (int i = 630; i < 700; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "before split");

        /* Add enough keys to split the left hand branch again. */
        for (int i = 58; i < 75; i++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
    }

    /**
     * [#13435]  Checks that a DIN can be replayed with a full BIN parent.
     * When a DIN is replayed, it may already be present in the parent BIN.
     * Before fixing this bug, we searched without allowing splits and then
     * called IN.insertEntry, which would throw InconsistentNodeException if
     * the BIN was full.  We now search with splits allowed, which avoids the
     * exception; however, it causes a split when one is not needed.
     *
     * Note that an alternate fix would be to revert to an earlier version of
     * RecoveryManager.replaceOrInsertDuplicateRoot (differences are between
     * version 1.184 and 1.185).  The older version searches for an existing
     * entry, and then inserts if necessary.  This would avoid the extra split.
     * However, we had to search with splits allowed anyway to fix another
     * problem -- see testBINSplitDuringDeletedDINReplay.
     */
    @Test
    public void testBINSplitDuringDINReplay()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);

        testOneCase(DB_NAME,
                    envConfig,
                    dbConfig,
                    new TestGenerator(true){
                        void generateData(Database db)
                            throws DatabaseException {

                            setupBINSplitDuringDINReplay(db);
                        }
                    },
                    envConfig,
                    dbConfig);

        /*
         * Now run the test in a stepwise loop, truncate after each
         * log entry. We start the steps before the inserts, so the base
         * expected set is empty.
         */
        HashSet<TestData> currentExpected = new HashSet<TestData>();
        if (TestUtils.runLongTests()) {
            stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected,  0);
        }
    }

    /**
     * Fill a BIN with entries, with a DIN in the first entry; then force the
     * BIN to be flushed, as might occur via eviction or checkpointing.
     */
    private void setupBINSplitDuringDINReplay(Database db)
        throws DatabaseException {

        setStepwiseStart();

        final int max = 128;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        IntegerBinding.intToEntry(1, key);
        IntegerBinding.intToEntry(0, data);
        assertEquals(OperationStatus.SUCCESS,
                     db.putNoOverwrite(null, key, data));
        IntegerBinding.intToEntry(1, data);
        assertEquals(OperationStatus.SUCCESS,
                     db.putNoDupData(null, key, data));

        Cursor cursor = db.openCursor(null, null);

        for (int i = 2; i <= max; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(0, data);
            assertEquals(OperationStatus.SUCCESS,
                         cursor.putNoOverwrite(key, data));
        }

        TestUtils.logBINAndIN(env, cursor);

        cursor.close();
    }

    /**
     * [#13435]  Checks that recovering a DIN causes a BIN split when needed.
     * This occurs when a DIN has been deleted and subsequently the BIN is
     * filled.  The DIN and the INDupDelete will be be replayed; we will insert
     * the DIN and then delete it.  In order to insert it, we may need to split
     * the BIN.  The sequence is:
     *
     * LN-a
     * (DupCountLN/) DIN (/DBIN/DupCountLN)
     * LN-b
     * DelDupLN-a (/DupCountLN)
     * DelDupLN-b (/DupCountLN)
     * INDupDelete compress
     * LN-c/etc to fill the BIN
     * BIN
     *
     * LN-a and LN-b are dups (same key).  After being compressed away, the
     * BIN is filled completely and flushed by the evictor or checkpointer.
     *
     * During recovery, when we replay the DIN and need to insert it into the
     * full BIN, therefore we need to split.  Before the bug fix, we did not
     * search with splits allowed, and got an InconsistentNodeException.
     */
    @Test
    public void testBINSplitDuringDeletedDINReplay()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);

        testOneCase(DB_NAME,
                    envConfig,
                    dbConfig,
                    new TestGenerator(true){
                        void generateData(Database db)
                            throws DatabaseException {

                            setupBINSplitDuringDeletedDINReplay(db);
                        }
                    },
                    envConfig,
                    dbConfig);

        /*
         * Now run the test in a stepwise loop, truncate after each
         * log entry. We start the steps before the inserts, so the base
         * expected set is empty.
         */
        HashSet<TestData> currentExpected = new HashSet<TestData>();
        if (TestUtils.runLongTests()) {
            stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected,  0);
        }
    }

    /**
     * Insert two dups, delete them, and compress to free the BIN entry;
     * then fill the BIN with LNs and flush the BIN.
     */
    private void setupBINSplitDuringDeletedDINReplay(Database db)
        throws DatabaseException {

        setStepwiseStart();

        int max = 128;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        IntegerBinding.intToEntry(0, key);
        IntegerBinding.intToEntry(0, data);
        assertEquals(OperationStatus.SUCCESS,
                     db.putNoOverwrite(null, key, data));
        IntegerBinding.intToEntry(1, data);
        assertEquals(OperationStatus.SUCCESS,
                     db.putNoDupData(null, key, data));

        assertEquals(OperationStatus.SUCCESS,
                     db.delete(null, key));

        env.compress();

        Cursor cursor = db.openCursor(null, null);

        for (int i = 1; i <= max; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(0, data);
            assertEquals(OperationStatus.SUCCESS,
                         cursor.putNoOverwrite(key, data));
        }

        TestUtils.logBINAndIN(env, cursor);

        cursor.close();
    }
}
