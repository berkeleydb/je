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

package je;

import java.io.File;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * SimpleExample creates a database environment, a database, and a database
 * cursor, inserts and retrieves data.
 */
class SimpleExample {
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FAILURE = 1;

    private int numRecords;   // num records to insert or retrieve
    private int offset;       // where we want to start inserting
    private boolean doInsert; // if true, insert, else retrieve
    private File envDir;

    public SimpleExample(int numRecords,
                         boolean doInsert,
                         File envDir,
                         int offset) {
        this.numRecords = numRecords;
        this.doInsert = doInsert;
        this.envDir = envDir;
        this.offset = offset;
    }

    /**
     * Usage string
     */
    public static void usage() {
        System.out.println("usage: java " +
                           "je.SimpleExample " +
                           "<dbEnvHomeDirectory> " +
                           "<insert|retrieve> <numRecords> [offset]");
        System.exit(EXIT_FAILURE);
    }

    /**
     * Main
     */
    public static void main(String argv[]) {

        if (argv.length < 2) {
            usage();
            return;
        }
        File envHomeDirectory = new File(argv[0]);

        boolean doInsertArg = false;
        if (argv[1].equalsIgnoreCase("insert")) {
            doInsertArg = true;
        } else if (argv[1].equalsIgnoreCase("retrieve")) {
            doInsertArg = false;
        } else {
            usage();
        }

        int startOffset = 0;
        int numRecordsVal = 0;

        if (doInsertArg) {

            if (argv.length > 2) {
                numRecordsVal = Integer.parseInt(argv[2]);
            } else {
                usage();
                return;
            }

            if (argv.length > 3) {
                startOffset = Integer.parseInt(argv[3]);
            }
        }

        try {
            SimpleExample app = new SimpleExample(numRecordsVal,
                                                  doInsertArg,
                                                  envHomeDirectory,
                                                  startOffset);
            app.run();
        } catch (DatabaseException e) {
            e.printStackTrace();
            System.exit(EXIT_FAILURE);
        }
        System.exit(EXIT_SUCCESS);
    }

    /**
     * Insert or retrieve data
     */
    public void run() throws DatabaseException {
        /* Create a new, transactional database environment */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        Environment exampleEnv = new Environment(envDir, envConfig);

        /*
         * Make a database within that environment
         *
         * Notice that we use an explicit transaction to
         * perform this database open, and that we
         * immediately commit the transaction once the
         * database is opened. This is required if we
         * want transactional support for the database.
         * However, we could have used autocommit to
         * perform the same thing by simply passing a
         * null txn handle to openDatabase().
         */
        Transaction txn = exampleEnv.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        Database exampleDb = exampleEnv.openDatabase(txn,
                                                     "simpleDb",
                                                     dbConfig);
        txn.commit();

        /*
         * Insert or retrieve data. In our example, database records are
         * integer pairs.
         */

        /* DatabaseEntry represents the key and data of each record */
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        if (doInsert) {

            /* put some data in */
            for (int i = offset; i < numRecords + offset; i++) {
                /*
                 * Note that autocommit mode, described in the Getting
                 * Started Guide, is an alternative to explicitly
                 * creating the transaction object.
                 */
                txn = exampleEnv.beginTransaction(null, null);

                /* Use a binding to convert the int into a DatabaseEntry. */

                IntegerBinding.intToEntry(i, keyEntry);
                IntegerBinding.intToEntry(i+1, dataEntry);
                OperationStatus status =
                    exampleDb.put(txn, keyEntry, dataEntry);

                /*
                 * Note that put will throw a DatabaseException when
                 * error conditions are found such as deadlock.
                 * However, the status return conveys a variety of
                 * information. For example, the put might succeed,
                 * or it might not succeed if the record alread exists
                 * and the database was not configured for duplicate
                 * records.
                 */
                if (status != OperationStatus.SUCCESS) {
                    throw new RuntimeException("Data insertion got status " +
                                               status);
                }
                txn.commit();
            }
        } else {
            /* retrieve the data */
            Cursor cursor = exampleDb.openCursor(null, null);

            while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {
                System.out.println("key=" +
                                   IntegerBinding.entryToInt(keyEntry) +
                                   " data=" +
                                   IntegerBinding.entryToInt(dataEntry));

            }
            cursor.close();
        }

        exampleDb.close();
        exampleEnv.close();

    }
}
