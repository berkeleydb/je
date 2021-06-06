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

package com.sleepycat.je.util;

import java.io.File;
import java.util.logging.Level;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.utilint.StringUtils;

/**
 * KeySearch is a debugging aid that searches the database for a given
 * record.
 */
public class RecordSearch {

    public static void main(String[] argv) {
        try {
            int whichArg = 0;
            DatabaseEntry searchKey = null;
            String dbName = null;
            String keyVal = null;
            String levelVal = "SEVERE";
            boolean dumpAll = false;
            boolean searchKeyRange = false;

            /*
             * Usage: -h  <envHomeDir> (optional
             *        -db <db name>
             *        -ks <key to search for, as a string>
             *        -ksr <key to range search for, as a string>
             *        -a  <if true, dump the whole db>
             *        -l  <logging level>
             */
            String envHome = "."; // default to current directory
            while (whichArg < argv.length) {
                String nextArg = argv[whichArg];

                if (nextArg.equals("-h")) {
                    whichArg++;
                    envHome = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-db")) {
                    whichArg++;
                    dbName = CmdUtil.getArg(argv, whichArg);
                } else if (nextArg.equals("-ks")) {
                    whichArg++;
                    keyVal = CmdUtil.getArg(argv, whichArg);
                    searchKey = new DatabaseEntry(StringUtils.toUTF8(keyVal));
                } else if (nextArg.equals("-ksr")) {
                    whichArg++;
                    keyVal = CmdUtil.getArg(argv, whichArg);
                    searchKey = new DatabaseEntry(StringUtils.toUTF8(keyVal));
                    searchKeyRange = true;
                } else if (nextArg.equals("-l")) {
                    whichArg++;
                    levelVal = CmdUtil.getArg(argv, whichArg);
                    Level.parse(levelVal); // sanity check level
                } else if (nextArg.equals("-a")) {
                    whichArg++;
                    String dumpVal = CmdUtil.getArg(argv, whichArg);
                    dumpAll = Boolean.valueOf(dumpVal).booleanValue();
                } else {
                    throw new IllegalArgumentException
                        (nextArg + " is not a supported option.");
                }
                whichArg++;
            }

            if (dbName == null) {
                usage();
                System.exit(1);
            }

            /* Make a read only environment */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();

            // Don't debug log to the database log.
            envConfig.setConfigParam
                (EnvironmentParams.JE_LOGGING_DBLOG.getName(), "false");

            envConfig.setReadOnly(true);

            Environment envHandle = new Environment(new File(envHome),
                                                    envConfig);

            /* Open the db. */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(true);
            DbInternal.setUseExistingConfig(dbConfig, true);
            Database db = envHandle.openDatabase(null, dbName, dbConfig);

            DatabaseEntry foundData = new DatabaseEntry();
            if (dumpAll) {
                Cursor cursor = db.openCursor(null, null);
                DatabaseEntry foundKey = new DatabaseEntry();
                int i = 0;
                while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
                       OperationStatus.SUCCESS) {
                    System.out.println(i + ":key=" +
                                   StringUtils.fromUTF8(foundKey.getData()));
                    i++;
                }
                cursor.close();
            } else if (searchKeyRange) {
                /* Range Search for the key. */
                Cursor cursor = db.openCursor(null, null);
                OperationStatus status = cursor.getSearchKeyRange
                    (searchKey, foundData, LockMode.DEFAULT);
                cursor.close();
                System.out.println("Range Search for key " + keyVal +
                                   " status = " + status + " => " +
                                   StringUtils.fromUTF8(searchKey.getData()));
            } else {
                /* Search for the key. */
                OperationStatus status = db.get(null, searchKey, foundData,
                                                LockMode.DEFAULT);
                System.out.println("Search for key " + keyVal +
                                   " status = " + status);
            }
            db.close();
            envHandle.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            usage();
            System.exit(1);
        }
    }

    private static void usage() {
        System.out.println("Usage: RecordSearch");
        System.out.println("  -h <environment home> ");
        System.out.println("  -a <true if dump all>");
        System.out.println("  -db <db name>");
        System.out.println("  -l logging level");
        System.out.println("  -ks <key to search for, as a string");
        System.out.println("  -ksr <key to range search for, as a string");
    }
}
