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

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
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

@RunWith(Parameterized.class)
public class SR10597Test extends CleanerTestBase {

    private static final String DB_NAME = "foo";

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private Database db;

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
        /* Use a small log file size to make cleaning more frequent. */
        config.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                              Integer.toString(1024));
        if (envMultiSubDir) {
            config.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                  DATA_DIRS + "");
        }
        env = new Environment(envHome, config);

        openDb();
    }

    public SR10597Test(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }
    
    /**
     * Opens that database.
     */
    private void openDb()
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
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

    /**
     */
    @Test
    public void testSR10597()
        throws DatabaseException {

        openEnv();

        /* Put some duplicates, enough to fill a log file. */
        final int COUNT = 10;
        DatabaseEntry key = new DatabaseEntry(TestUtils.getTestArray(0));
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < COUNT; i += 1) {
            data.setData(TestUtils.getTestArray(i));
            db.put(null, key, data);
        }
        Cursor cursor = db.openCursor(null, null);
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchKey(key, data, null));
        assertEquals(COUNT, cursor.count());
        cursor.close();

        /* Delete everything, then compress to delete the DIN. */
        db.delete(null, key);
        env.compress();
        data.setData(TestUtils.getTestArray(0));

        /* Add a single record, which will not create a DIN. */
        db.put(null, key, data);

        /* Checkpoint and clean. */
        env.checkpoint(forceConfig);
        int cleaned = env.cleanLog();
        assertTrue("cleaned=" + cleaned, cleaned > 0);

        /*
         * Before the fix to 10597, when cleaning the log we would be looking
         * for an LN with containsDuplicates=true.  We assumed that when we
         * found the BIN entry, it must point to a DIN.  But because we
         * deleted and compressed above, the entry is actually an LN.  This
         * caused a ClassCastException at the bottom of
         * Tree.getParentBINForChildLN.
         */
        closeEnv();
    }
}
