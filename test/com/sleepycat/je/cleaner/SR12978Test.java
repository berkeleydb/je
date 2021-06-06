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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

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
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.TestUtils;

/**
 * Tests a fix to 12978, which was a ClassCastException in the following
 * sequence.
 *
 * 1) A LN's BIN entry has the MIGRATE flag set.
 *
 * 2) Another LN with the same key is inserted (duplicates are allowed) and the
 * first LN is moved to a dup tree,
 *
 * 3) The MIGRATE flag on the BIN entry is not cleared, and this entry now
 * contains a DIN.
 *
 * 4) A split of the BIN occurs, logging the BIN with DIN entry.  During a
 * split we can't do migration, so we attempt to put the DIN onto the cleaner's
 * pending list.  We cast from DIN to LN, causing the exception.
 *
 * The fix was to clear the MIGRATE flag on the BIN entry at the time we update
 * it to contain the DIN.
 *
 * This bug also left latches unreleased if a runtime exception occurred during
 * a split, and that problem was fixed also.
 */
@RunWith(Parameterized.class)
public class SR12978Test extends CleanerTestBase {

    private static final String DB_NAME = "foo";

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private Database db;
    
    public SR12978Test(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir  ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }

    /**
     * Opens the environment and database.
     */
    private void open()
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
        /* Configure to make cleaning more frequent. */
        config.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(), "10240");
        config.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "90");
        if (envMultiSubDir) {
            config.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                  DATA_DIRS + "");
        }

        env = new Environment(envHome, config);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    /**
     * Closes the environment and database.
     */
    private void close()
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

    /**
     */
    @Test
    public void testSR12978()
        throws DatabaseException {

        open();

        final int COUNT = 800;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /*
         * Insert enough non-dup records to write a few log files.  Delete
         * every other so that cleaning will occur.  Leave key space so we can
         * insert below to cause splits.
         */
        IntegerBinding.intToEntry(0, data);
        for (int i = 0; i < COUNT; i += 4) {

            IntegerBinding.intToEntry(i + 0, key);
            status = db.putNoOverwrite(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);

            IntegerBinding.intToEntry(i + 1, key);
            status = db.putNoOverwrite(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);

            status = db.delete(null, key);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /* Clean to set the MIGRATE flag on some LN entries. */
        env.checkpoint(forceConfig);
        int nCleaned = env.cleanLog();
        assertTrue(nCleaned > 0);

        /* Add dups to cause the LNs to be moved to a dup tree. */
        IntegerBinding.intToEntry(1, data);
        for (int i = 0; i < COUNT; i += 4) {

            IntegerBinding.intToEntry(i + 0, key);
            status = db.putNoDupData(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /*
         * Insert more unique keys to cause BIN splits.  Before the fix to
         * 12978, a CastCastException would occur during a split.
         */
        IntegerBinding.intToEntry(0, data);
        for (int i = 0; i < COUNT; i += 4) {

            IntegerBinding.intToEntry(i + 2, key);
            status = db.putNoOverwrite(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);

            IntegerBinding.intToEntry(i + 3, key);
            status = db.putNoOverwrite(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        close();
    }
}
