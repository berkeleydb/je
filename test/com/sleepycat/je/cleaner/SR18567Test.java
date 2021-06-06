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

package com.sleepycat.je.cleaner;

import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.TestUtils;

/**
 * Test concurrent checkpoint and Database.sync operations.
 *
 * Prior to the fix for [#18567], checkpoint and Database.sync both acquired a
 * shared latch on the parent IN while logging a child IN.  During concurrent
 * logging of provisional child BINs for same parent IN, problems can occur
 * when the utilization info in the parent IN is being updated by both threads.
 *
 * For example, the following calls could be concurrent in the two threads for
 * the same parent IN.
 *
 *  BIN.afterLog (two different BINs)
 *   |--IN.trackProvisionalObsolete (same parent)
 *      |-- PackedObsoleteInfo.copyObsoleteInfo (array copy)
 *
 * Concurrent array copies could corrupt the utilization info, and this could
 * result in the exception reported by the OTN user later on, when the parent
 * IN is logged and the obsolete info is copied to the UtilizationTracker:
 *
 *    java.lang.IndexOutOfBoundsException
 *    at com.sleepycat.bind.tuple.TupleInput.readBoolean(TupleInput.java:186)
 *    at com.sleepycat.je.cleaner.PackedObsoleteInfo.countObsoleteInfo(PackedObsoleteInfo.java:60)
 *    at com.sleepycat.je.log.LogManager.serialLogInternal(LogManager.java:671)
 *    at com.sleepycat.je.log.SyncedLogManager.serialLog(SyncedLogManager.java:40)
 *    at com.sleepycat.je.log.LogManager.multiLog(LogManager.java:388)
 *    at com.sleepycat.je.recovery.Checkpointer.logSiblings(Checkpointer.java:1285)
 *
 * Although we have not reproduced this particular exception, the test case
 * below, along with assertions in IN.beforeLog and afterLog, demonstrate that
 * we do allow concurrent logging of two child BINs for the same parent IN.
 * Since both threads are modifying the parent IN, it seems obvious that using
 * a shared latch could cause more than one type of problem and should be
 * disallowed.
 */
@RunWith(Parameterized.class)
public class SR18567Test extends CleanerTestBase {

    private static final String DB_NAME = "foo";

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private Database db;
    private JUnitThread junitThread1;
    private JUnitThread junitThread2;

    public SR18567Test(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }

    @After
    public void tearDown() 
        throws Exception {

        if (junitThread1 != null) {
            junitThread1.shutdown();
            junitThread1 = null;
        }
        if (junitThread2 != null) {
            junitThread2.shutdown();
            junitThread2 = null;
        }
        super.tearDown();
        db = null;
    }

    /**
     * Opens the environment and database.
     */
    private void openEnv()
        throws DatabaseException {

        EnvironmentConfig config = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(config);
        config.setAllowCreate(true);
        /* Do not run the daemons. */
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        config.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        if (envMultiSubDir) {
            config.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                  DATA_DIRS + "");
        }

        env = new Environment(envHome, config);

        openDb();
    }

    /**
     * Opens that database.
     */
    private void openDb()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    /**
     * Closes the environment and database.
     */
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

    @Test
    public void testSR18567()
        throws Throwable {

        openEnv();

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[0]);

        for (int i = 0; i < 100; i += 1) {
            for (int j = 0; j < 1000; j += 1) {
                IntegerBinding.intToEntry(j, key);
                db.put(null, key, data);
            }
            junitThread1 = new JUnitThread("Checkpoint") {
                @Override
                public void testBody() {
                    env.checkpoint(forceConfig);
                }
            };
            junitThread2 = new JUnitThread("Database.sync") {
                @Override
                public void testBody() {
                    db.sync();
                }
            };
            junitThread1.start();
            junitThread2.start();
            junitThread1.finishTest();
            junitThread2.finishTest();
        }

        closeEnv();
    }
}
