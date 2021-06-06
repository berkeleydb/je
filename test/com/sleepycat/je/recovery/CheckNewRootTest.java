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
import static org.junit.Assert.fail;

import java.util.HashSet;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.recovery.stepwise.TestData;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;

/**
 * Test situations where a new root is created
 */
public class CheckNewRootTest extends CheckBase {

    private static final boolean DEBUG = false;
    private static final String DB_NAME = "simpleDB";

    private final boolean useDups = false;
    private static CheckpointConfig FORCE_CONFIG = new CheckpointConfig();
    static {
        FORCE_CONFIG.setForce(true);
    }

    /**
     * Create a tree, make sure the root changes and is logged
     * before any checkpointing. The bug found in [#13897] was this:
     *
     * 100 BIN a
     * 110 RootIN b
     * 120 MapLN points to root IN at 110
     * 130 RootIN b written as part of compression
     * 140 ckpt start
     * 150 ckpt end
     *
     * Since the compression was writing a root IN w/out updating the mapLN,
     * the obsolete root at 110 was recovered instead of newer rootIN at 130.
     */
    @Test
    public void testWrittenByCompression()
        throws Throwable {

        EnvironmentConfig envConfig = setupEnvConfig();
        DatabaseConfig dbConfig = setupDbConfig();

        /* Run the full test case w/out truncating the log. */
        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator(true /* generate log description. */){
                        @Override
                        void generateData(Database db)
                            throws DatabaseException {
                            setupWrittenByCompression(db);
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

    /**
     * Create a populated tree, delete all records, then begin to insert again.
     */
    private void setupWrittenByCompression(Database db)
        throws DatabaseException {
        setStepwiseStart();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so it grows to 2 levels, with 2 BINs. */
        for (int i = 0; i < 10; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After inserts");
        env.checkpoint(FORCE_CONFIG);
        if (DEBUG) {
            System.out.println(db.getStats(new StatsConfig()));
        }

        /* Now delete all of 1 BIN. */
        for (int i = 0; i < 5; i ++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS, db.delete(null, key));
        }

        /* Compress, removing a BIN. */
        env.compress();
        if (DEBUG) {
            System.out.println("After compress");
            System.out.println(db.getStats(new StatsConfig()));
        }

        /* Checkpoint again. */
        env.checkpoint(FORCE_CONFIG);
    }

    /**
     * Create a tree, make sure the root changes and is logged
     * before any checkpointing. The bug found in [#13897] was this:
     *
     * 110 RootIN b
     * 120 MapLN points to root IN at 110
     * 130 BINb split
     * 140 RootIN b written as part of split
     * 150 ckpt start
     * 160 ckpt end
     *
     * Since the compression was writing a root IN w/out updating the mapLN,
     * the obsolete root at 110 was recovered instead of newer rootIN at 130.
     */
    @Test
    public void testWrittenBySplit()
        throws Throwable {

        EnvironmentConfig envConfig = setupEnvConfig();
        DatabaseConfig dbConfig = setupDbConfig();

        /* Run the full test case w/out truncating the log. */
        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator(true /* generate log description. */){
                        @Override
                        void generateData(Database db)
                            throws DatabaseException {
                            setupWrittenBySplits(db);
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

    /**
     */
    private void setupWrittenBySplits(Database db)
        throws DatabaseException {
        setStepwiseStart();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Create a tree and checkpoint. */
        IntegerBinding.intToEntry(0, key);
        IntegerBinding.intToEntry(0, data);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        env.checkpoint(FORCE_CONFIG);
        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After creation");

        /* Populate a tree so it splits. */
        for (int i = 1; i < 6; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After inserts");
        env.checkpoint(FORCE_CONFIG);
    }

    /*
     * Scenario from [#13897]: tree is created. Log looks like this
     *  provisional BIN
     *  root IN
     *  checkpoint start
     *  LN is logged but not yet attached to BIN
     *  checkpoint end
     *  BIN is dirtied, but is not part of checkpoint, because dirtying wasn't
     *  seen
     * In this case, getParentForBIN hangs, because there is no root.
     * This test is for debugging only, because it's not really possible to
     * run a real checkpoint in the small window when the bin is not dirty.
     * Attempts to run a checkpoint programmatically result in failing the
     * assert that no latches are held when the inlist latch is taken.
     * Instead, we do this pseudo checkpoint, to make the hang reproduce. But
     * this test will still fail even with the fixed code because the fix
     * now causes the rootIN to get re-logged, and the pseudo checkpoint
     * doesn't do that logging.
     */
    public void xxtestCreateNewTree() // This test for debugging only
        throws Throwable {

        EnvironmentConfig envConfig = setupEnvConfig();
        DatabaseConfig dbConfig = setupDbConfig();

        /* Run the full test case w/out truncating the log. */
        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator(true /* generate log description. */){
                        @Override
                        void generateData(Database db)
                            throws DatabaseException {
                            setupCreateNewTree(db);
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

    /**
     * Create a populated tree, delete all records, then begin to insert again.
     */
    private void setupCreateNewTree(Database db)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        TestHook ckptHook = new CheckpointHook(env);
        DbInternal.getDbImpl(db).getTree().setCkptHook(ckptHook);

        env.checkpoint(FORCE_CONFIG);

        /*
         * Create in the log
         *  provisional BIN, IN, ckpt start, LN
         */
        IntegerBinding.intToEntry(1, key);
        IntegerBinding.intToEntry(1, data);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
    }

    /*
     * Force a checkpoint into the log. Use another thread, lest the asserts
     * about held latches take effect.
     */
    private static class CheckpointHook implements TestHook {
        private final Environment env;

        CheckpointHook(Environment env) {
            this.env = env;
        }

        public void doHook() {
            try {
                EnvironmentImpl envImpl =
                    DbInternal.getNonNullEnvImpl(env);
                SingleItemEntry<CheckpointStart> startEntry =
                    SingleItemEntry.create(LogEntryType.LOG_CKPT_START,
                                           new CheckpointStart(100, "test"));
                long checkpointStart = envImpl.getLogManager().log
                    (startEntry,
                     ReplicationContext.NO_REPLICATE);
                CheckpointEnd ckptEnd = new CheckpointEnd
                    ("test",
                     checkpointStart,
                     envImpl.getRootLsn(),
                     envImpl.getTxnManager().getFirstActiveLsn(),
                     envImpl.getNodeSequence().getLastLocalNodeId(),
                     envImpl.getNodeSequence().getLastReplicatedNodeId(),
                     envImpl.getDbTree().getLastLocalDbId(),
                     envImpl.getDbTree().getLastReplicatedDbId(),
                     envImpl.getTxnManager().getLastLocalTxnId(),
                     envImpl.getTxnManager().getLastReplicatedTxnId(),
                     100,
                     true /*cleanedFilesToDelete*/);
                SingleItemEntry<CheckpointEnd> endEntry =
                    SingleItemEntry.create(LogEntryType.LOG_CKPT_END, ckptEnd);
                envImpl.getLogManager().logForceFlush
                    (endEntry,
                     true, // fsyncRequired
                     ReplicationContext.NO_REPLICATE);
            } catch (DatabaseException e) {
                fail(e.getMessage());
            }
        }

        public Object getHookValue() {
            throw new UnsupportedOperationException();
        }

        public void doIOHook() {
            throw new UnsupportedOperationException();
        }

        public void hookSetup() {
            throw new UnsupportedOperationException();
        }
        public void doHook(Object obj) {
            throw new UnsupportedOperationException();            
        }
    }

    /**
     * Make sure eviction doesn't evict roots. If it did, we'd need to
     * log the mapLN to be sure that recovery is correct.
     */
    @Test
    public void testChangeAndEvictRoot()
        throws Throwable {

        EnvironmentConfig envConfig = setupEnvConfig();
        DatabaseConfig dbConfig = setupDbConfig();

        /* Run the full test case w/out truncating the log. */
        testOneCase(DB_NAME, envConfig, dbConfig,
                    new TestGenerator(true /* generate log description. */){
                        @Override
                        void generateData(Database db)
                            throws DatabaseException {
                            setupEvictedRoot(db);
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

    /**
     * Create a populated tree, delete all records, then begin to insert again.
     */
    private void setupEvictedRoot(Database db)
        throws DatabaseException {
        setStepwiseStart();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so it grows to 2 levels, with 2 BINs. */
        for (int i = 0; i < 10; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After inserts");
        env.checkpoint(FORCE_CONFIG);

        /*
         * Add another record so that the eviction below will log
         * a different versions of the IN nodes.
         */
        IntegerBinding.intToEntry(10, key);
        IntegerBinding.intToEntry(10, data);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));

        /* Evict */
        TestHook<Boolean> evictHook = new TestHook<Boolean>() {
                public void doIOHook() {
                    throw new UnsupportedOperationException();
                }
                public void doHook() {
                    throw new UnsupportedOperationException();
                }
                public Boolean getHookValue() {
                    return Boolean.TRUE;
                }
                public void hookSetup() {
                    throw new UnsupportedOperationException();
                }
                public void doHook(Boolean obj) {
                    throw new UnsupportedOperationException();
                }
            };
        DbInternal.getNonNullEnvImpl(env).getEvictor().
                                           setRunnableHook(evictHook);
        env.evictMemory();

        /* Checkpoint again. */
        env.checkpoint(FORCE_CONFIG);
    }

    private EnvironmentConfig setupEnvConfig() {
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "4");
        envConfig.setAllowCreate(true);
        return envConfig;
    }

    private DatabaseConfig setupDbConfig() {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(useDups);
        dbConfig.setAllowCreate(true);
        return dbConfig;
    }
}
