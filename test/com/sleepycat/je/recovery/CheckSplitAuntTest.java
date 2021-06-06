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

/**
 * The split aunt problem [#14424] is described in LevelRecorder.
 * Also see [#23990] and [#24663].
 */
public class CheckSplitAuntTest extends CheckBase {

    private static final String DB_NAME = "simpleDB";

    @Test
    public void testSplitAunt()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "4");
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        EnvironmentConfig restartConfig = TestUtils.initEnvConfig();
        turnOffEnvDaemons(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "4");
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
        stepwiseLoop(DB_NAME, envConfig, dbConfig, currentExpected,  0);
    }

    private void setupSplitData(Database db)
        throws DatabaseException {

        setStepwiseStart();

        int max = 26;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Populate a tree so it grows to 4 levels, then checkpoint. */
        for (int i = 0; i < max; i ++) {
            IntegerBinding.intToEntry(i*10, key);
            IntegerBinding.intToEntry(i*10, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        CheckpointConfig ckptConfig = new CheckpointConfig();
        Trace.trace(DbInternal.getNonNullEnvImpl(env), "First sync");
        env.sync();

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "Second sync");
        env.sync();

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "Third sync");
        env.sync();

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "Fourth sync");
        env.sync();

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "Fifth sync");
        env.sync();

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "Sync6");
        env.sync();

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "After sync");

        /*
         * Add a key to dirty the left hand branch. 4 levels are needed to
         * create the problem scenario, because the single key added here,
         * followed by a checkpoint, will always cause at least 2 levels to be
         * logged -- that's the smallest maxFlushLevel for any checkpoint. And
         * we must not dirty the root, so 3 levels is not enough.
         */
        IntegerBinding.intToEntry(5, key);
        IntegerBinding.intToEntry(5, data);
        assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        Trace.trace
            (DbInternal.getNonNullEnvImpl(env), "After single key insert");

        /*
         * A normal checkpoint should log the BIN and its parent IN, but no
         * higher than level 2. The level 3 parent will be left dirty, but
         * level 4 (the root) will not be dirtied.
         */
        ckptConfig.setForce(true);
        env.checkpoint(ckptConfig);

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "before split");

        /* Add enough keys to split the right hand branch. */
        for (int i = max*10; i < max*10 + 7; i ++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        Trace.trace(DbInternal.getNonNullEnvImpl(env), "after split");
    }
}
