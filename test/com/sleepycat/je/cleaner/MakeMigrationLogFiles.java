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

import java.io.File;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;

/**
 * Creates two small log files with close to 100% utilization for use by
 * FileSelectionTest.testLogVersionMigration.  This main program is with the
 * arguments: -h HOME_DIRECTORY
 *
 * This program was used to create two log files (stored in CVS as
 * migrate_f0.jdb and migrate_f1.jdb) running against JE 3.2.68, which writes
 * log version 5.  Testing with these files in testLogVersionUpgrade checks
 * that these files are migrated when je.cleaner.migrateToLogVersion is set.
 */
public class MakeMigrationLogFiles {

    private static final int FILE_SIZE = 1000000;

    public static void main(String[] args)
        throws DatabaseException {

        String homeDir = null;
        for (int i = 0; i < args.length; i += 1) {
            if (args[i].equals("-h")) {
                i += 1;
                homeDir = args[i];
            } else {
                throw new IllegalArgumentException("Unknown arg: " + args[i]);
            }
        }
        if (homeDir == null) {
            throw new IllegalArgumentException("Missing -h arg");
        }
        Environment env = openEnv(new File(homeDir), true /*allowCreate*/);
        makeMigrationLogFiles(env);
        env.close();
    }

    /**
     * Opens an Environment with a small log file size.
     */
    static Environment openEnv(File homeDir, boolean allowCreate)
        throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             String.valueOf(FILE_SIZE));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        return new Environment(homeDir, envConfig);
    }

    /**
     * Creates two log files.
     */
    static void makeMigrationLogFiles(Environment env)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, "foo", dbConfig);

        int nextKey = 0;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor c = db.openCursor(null, null);
        OperationStatus status = c.getLast(key, data, null);
        if (status == OperationStatus.SUCCESS) {
            nextKey = IntegerBinding.entryToInt(key);
        }
        c.close();

        byte[] dataBytes = new byte[1000];
        final int OVERHEAD = dataBytes.length + 100;
        data.setData(dataBytes);

        for (int size = 0; size < FILE_SIZE * 2; size += OVERHEAD) {
            nextKey += 1;
            IntegerBinding.intToEntry(nextKey, key);
            status = db.putNoOverwrite(null, key, data);
            assert status == OperationStatus.SUCCESS;
        }

        db.close();
    }
}
