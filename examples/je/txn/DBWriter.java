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

package je.txn;

import java.util.Random;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DBWriter extends Thread
{
    private Database myDb = null;
    private Environment myEnv = null;
    private EntryBinding dataBinding = null;
    private Random generator = new Random();

    private static int MAX_RETRY = 20;

    private static String[] keys = {"key 1", "key 2", "key 3",
                                    "key 4", "key 5", "key 6",
                                    "key 7", "key 8", "key 9",
                                    "key 10"};

    // Constructor. Get our DB handles from here
    DBWriter(Environment env, Database db, StoredClassCatalog scc) {
        myDb = db;
        myEnv = env;
        dataBinding = new SerialBinding(scc, PayloadData.class);
    }

    // Thread method that writes a series of records
    // to the database using transaction protection.
    // Deadlock handling is demonstrated here.
    public void run () {
        Transaction txn = null;

        // Perform 50 transactions
        for (int i=0; i<50; i++) {

           boolean retry = true;
           int retry_count = 0;
           // while loop is used for deadlock retries
           while (retry) {
                // try block used for deadlock detection and
                // general db exception handling
                try {

                    // Get a transaction
                    txn = myEnv.beginTransaction(null, null);

                    // Write 10 records to the db
                    // for each transaction
                    for (int j = 0; j < 10; j++) {
                        // Get the key
                        DatabaseEntry key = new DatabaseEntry();
                        StringBinding.stringToEntry(keys[j], key);

                        // Get the data
                        PayloadData pd = new PayloadData(i+j, getName(),
                            generator.nextDouble());
                        DatabaseEntry data = new DatabaseEntry();
                        dataBinding.objectToEntry(pd, data);

                        // Do the put
                        myDb.put(txn, key, data);
                    }

                    // commit
                    System.out.println(getName() + " : committing txn : " + i);

                    System.out.println(getName() + " : Found " +
                        countRecords(null) + " records in the database.");
                    try {
                        txn.commit();
                        txn = null;
                    } catch (DatabaseException e) {
                        System.err.println("Error on txn commit: " +
                            e.toString());
                    }
                    retry = false;

                } catch (LockConflictException de) {
                    System.out.println("################# " + getName() +
                        " : caught deadlock");
                    // retry if necessary
                    if (retry_count < MAX_RETRY) {
                        System.err.println(getName() +
                            " : Retrying operation.");
                        retry = true;
                        retry_count++;
                    } else {
                        System.err.println(getName() +
                            " : out of retries. Giving up.");
                        retry = false;
                    }
                } catch (DatabaseException e) {
                    // abort and don't retry
                    retry = false;
                    System.err.println(getName() +
                        " : caught exception: " + e.toString());
                    e.printStackTrace();
                } finally {
                    if (txn != null) {
                        try {
                            txn.abort();
                        } catch (Exception e) {
                            System.err.println("Error aborting transaction: " +
                                e.toString());
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    // This simply counts the number of records contained in the
    // database and returns the result. You can use this method
    // in three ways:
    //
    // First call it with an active txn handle.
    // Secondly, configure the cursor for dirty reads
    // Third, call count_records AFTER the writer has committed
    //    its transaction.
    //
    // If you do none of these things, the writer thread will
    // self-deadlock.
    //
    // Note that this method exists only for illustrative purposes.
    // A more straight-forward way to count the number of records in
    // a database is to use the Database.getStats() method.
    private int countRecords(Transaction txn)  throws DatabaseException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        int count = 0;
        Cursor cursor = null;

        try {
            // Get the cursor
            CursorConfig cc = new CursorConfig();
            cc.setReadUncommitted(true);
            cursor = myDb.openCursor(txn, cc);
            while (cursor.getNext(key, data, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS) {

                    count++;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return count;

    }
}
