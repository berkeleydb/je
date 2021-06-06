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

// File TxnGuide.java

package je.txn;

import java.io.File;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class TxnGuide {

    private static String myEnvPath = "./";
    private static String dbName = "mydb.db";
    private static String cdbName = "myclassdb.db";

    // DB handles
    private static Database myDb = null;
    private static Database myClassDb = null;
    private static Environment myEnv = null;

    private static int NUMTHREADS = 5;

    private static void usage() {
        System.out.println("TxnGuide [-h <env directory>]");
        System.exit(-1);
    }

    public static void main(String args[]) {
        try {
            // Parse the arguments list
            parseArgs(args);
            // Open the environment and databases
            openEnv();
            // Get our class catalog (used to serialize objects)
            StoredClassCatalog classCatalog =
                new StoredClassCatalog(myClassDb);

            // Start the threads
            DBWriter[] threadArray;
            threadArray = new DBWriter[NUMTHREADS];
            for (int i = 0; i < NUMTHREADS; i++) {
                threadArray[i] = new DBWriter(myEnv, myDb, classCatalog);
                threadArray[i].start();
            }

            for (int i = 0; i < NUMTHREADS; i++) {
                threadArray[i].join();
            }
        } catch (Exception e) {
            System.err.println("TxnGuide: " + e.toString());
            e.printStackTrace();
        } finally {
            closeEnv();
        }
        System.out.println("All done.");
    }

    private static void openEnv() throws DatabaseException {
        System.out.println("opening env");

        // Set up the environment.
        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        myEnvConfig.setAllowCreate(true);
        myEnvConfig.setTransactional(true);
        // Environment handles are free-threaded in JE,
        // so we do not have to do anything to cause the
        // environment handle to be free-threaded.

        // Set up the database
        DatabaseConfig myDbConfig = new DatabaseConfig();
        myDbConfig.setAllowCreate(true);
        myDbConfig.setTransactional(true);
        myDbConfig.setSortedDuplicates(true);
        // no DatabaseConfig.setThreaded() method available.
        // db handles in java are free-threaded so long as the
        // env is also free-threaded.

        // Open the environment
        myEnv = new Environment(new File(myEnvPath),    // Env home
                                myEnvConfig);

        // Open the database. Do not provide a txn handle. This open
        // is autocommitted because DatabaseConfig.setTransactional()
        // is true.
        myDb = myEnv.openDatabase(null,     // txn handle
                                  dbName,   // Database file name
                                  myDbConfig);

        // Used by the bind API for serializing objects
        // Class database must not support duplicates
        myDbConfig.setSortedDuplicates(false);
        myClassDb = myEnv.openDatabase(null,     // txn handle
                                       cdbName,  // Database file name
                                       myDbConfig);
    }

    private static void closeEnv() {
        System.out.println("Closing env and databases");
        if (myDb != null ) {
            try {
                myDb.close();
            } catch (DatabaseException e) {
                System.err.println("closeEnv: myDb: " +
                    e.toString());
                e.printStackTrace();
            }
        }

        if (myClassDb != null ) {
            try {
                myClassDb.close();
            } catch (DatabaseException e) {
                System.err.println("closeEnv: myClassDb: " +
                    e.toString());
                e.printStackTrace();
            }
        }

        if (myEnv != null ) {
            try {
                myEnv.close();
            } catch (DatabaseException e) {
                System.err.println("closeEnv: " + e.toString());
                e.printStackTrace();
            }
        }
    }

    private TxnGuide() {}

    private static void parseArgs(String args[]) {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("-")) {
                switch(args[i].charAt(1)) {
                    case 'h':
                        myEnvPath = new String(args[++i]);
                    break;
                    default:
                        usage();
                }
            }
        }
    }
}
