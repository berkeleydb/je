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
import java.io.Serializable;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;

/**
 * SecondaryExample operates in the same way as BindingExample, but adds a
 * SecondaryDatabase for accessing the primary database by a secondary key.
 */
class SecondaryExample {
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FAILURE = 1;

    private final int numRecords;   // num records to insert or retrieve
    private final int offset;       // where we want to start inserting
    private final boolean doInsert; // if true, insert, else retrieve
    private final File envDir;

    public SecondaryExample(int numRecords,
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
                           "je.SecondaryExample " +
                           "<dbEnvHomeDirectory> " +
                           "<insert|retrieve> <numRecords> [offset]");
        System.exit(EXIT_FAILURE);
    }

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
            SecondaryExample app = new SecondaryExample(numRecordsVal,
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
     * Insert or retrieve data.
     */
    public void run() throws DatabaseException {

        /* Create a new, transactional database environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        Environment exampleEnv = new Environment(envDir, envConfig);

        /*
         * Make a database within that environment. Because this will be used
         * as a primary database, it must not allow duplicates. The primary key
         * of a primary database must be unique.
         */
        Transaction txn = exampleEnv.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database exampleDb =
            exampleEnv.openDatabase(txn, "bindingsDb", dbConfig);

        /*
         * In our example, the database record is composed of an integer key
         * and and instance of the MyData class as data.
         *
         * A class catalog database is needed for storing class descriptions
         * for the serial binding used below.  This avoids storing class
         * descriptions redundantly in each record.
         */
        DatabaseConfig catalogConfig = new DatabaseConfig();
        catalogConfig.setTransactional(true);
        catalogConfig.setAllowCreate(true);
        Database catalogDb =
            exampleEnv.openDatabase(txn, "catalogDb", catalogConfig);
        StoredClassCatalog catalog = new StoredClassCatalog(catalogDb);

        /*
         * Create a serial binding for MyData data objects.  Serial
         * bindings can be used to store any Serializable object.
         */
        EntryBinding<MyData> dataBinding =
            new SerialBinding<MyData>(catalog, MyData.class);

        /*
         * Further below we'll use a tuple binding (IntegerBinding
         * specifically) for integer keys.  Tuples, unlike serialized
         * Java objects, have a well defined sort order.
         */

        /*
         * Define a String tuple binding for a secondary key.  The
         * secondary key is the msg field of the MyData object.
         */
        EntryBinding<String> secKeyBinding =
            TupleBinding.getPrimitiveBinding(String.class);

        /*
         * Open a secondary database to allow accessing the primary
         * database by the secondary key value.
         */
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setTransactional(true);
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);
        secConfig.setKeyCreator(new MyKeyCreator(secKeyBinding, dataBinding));
        SecondaryDatabase exampleSecDb =
            exampleEnv.openSecondaryDatabase(txn, "bindingsSecDb",
                                             exampleDb, secConfig);
        txn.commit();

        /* DatabaseEntry represents the key and data of each record. */
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        if (doInsert) {

            /*
             * Put some data in.  Note that the primary database is always used
             * to add data.  Adding or changing data in the secondary database
             * is not allowed; however, deleting through the secondary database
             * is allowed.
             */
            for (int i = offset; i < numRecords + offset; i++) {
                txn = exampleEnv.beginTransaction(null, null);
                StringBuilder stars = new StringBuilder();
                for (int j = 0; j < i; j++) {
                    stars.append('*');
                }
                MyData data = new MyData(i, stars.toString());

                IntegerBinding.intToEntry(i, keyEntry);
                dataBinding.objectToEntry(data, dataEntry);

                OperationStatus status =
                    exampleDb.put(txn, keyEntry, dataEntry);

                /*
                 * Note that put will throw a DatabaseException when error
                 * conditions are found such as deadlock.  However, the status
                 * return conveys a variety of information. For example, the
                 * put might succeed, or it might not succeed if the record
                 * exists and duplicates were not
                 */
                if (status != OperationStatus.SUCCESS) {
                    throw new RuntimeException("Data insertion got status " +
                                               status);
                }
                txn.commit();
            }
        } else {

            /*
             * Retrieve the data by secondary key by opening a cursor on the
             * secondary database.  The key parameter for a secondary cursor is
             * always the secondary key, but the data parameter is always the
             * data of the primary database.  You can cast the cursor to a
             * SecondaryCursor and use additional method signatures for
             * retrieving the primary key also.  Or you can call
             * openSecondaryCursor() to avoid casting.
             */
            txn = exampleEnv.beginTransaction(null, null);
            Cursor cursor = exampleSecDb.openCursor(txn, null);

            while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {

                String key = secKeyBinding.entryToObject(keyEntry);
                MyData data = dataBinding.entryToObject(dataEntry);

                System.out.println("key=" + key + " data=" + data);
            }
            cursor.close();
            txn.commit();
        }

        /*
         * Always close secondary databases before closing their associated
         * primary database.
         */
        catalogDb.close();
        exampleSecDb.close();
        exampleDb.close();
        exampleEnv.close();
    }

    @SuppressWarnings("serial")
    private static class MyData implements Serializable {

        private final int num;
        private final String msg;

        MyData(int number, String message) {
            this.num = number;
            this.msg = message;
        }

        String getMessage() {
            return msg;
        }

        @Override
        public String toString() {
            return String.valueOf(num) + ' ' + msg;
        }
    }

    /**
     * A key creator that knows how to extract the secondary key from the data
     * entry of the primary database.  To do so, it uses both the dataBinding
     * of the primary database and the secKeyBinding.
     */
    private static class MyKeyCreator implements SecondaryKeyCreator {

        private final EntryBinding<String> secKeyBinding;
        private final EntryBinding<MyData> dataBinding;

        MyKeyCreator(EntryBinding<String> secKeyBinding,
                     EntryBinding<MyData> dataBinding) {
            this.secKeyBinding = secKeyBinding;
            this.dataBinding = dataBinding;
        }

        public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                          DatabaseEntry keyEntry,
                                          DatabaseEntry dataEntry,
                                          DatabaseEntry resultEntry) {

            /*
             * Convert the data entry to a MyData object, extract the secondary
             * key value from it, and then convert it to the resulting
             * secondary key entry.
             */
            MyData data = dataBinding.entryToObject(dataEntry);
            String key = data.getMessage();
            if (key != null) {
                secKeyBinding.objectToEntry(key, resultEntry);
                return true;
            } else {

                /*
                 * The message property of MyData is optional, so if it is null
                 * then return false to prevent it from being indexed.  Note
                 * that if a required key is missing or an error occurs, an
                 * exception should be thrown by this method.
                 */
                return false;
            }
        }
    }
}
