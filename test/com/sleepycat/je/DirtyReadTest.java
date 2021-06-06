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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;

import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Check that the Database and Cursor classes properly use read-uncommitted
 * when specified.
 */
public class DirtyReadTest extends DualTestCase {
    private final File envHome;
    private Environment env;

    public DirtyReadTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testReadUncommitted()
        throws Throwable {

        Database db = null;
        Transaction txnA = null;
        Cursor cursor = null;
        try {
            /* Make an environment, a db, insert a few records */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setAllowCreate(true);
            env = create(envHome, envConfig);

            /* Now open for real, insert a record */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            db = env.openDatabase(null, "foo", dbConfig);

            StringDbt key = new StringDbt("key1");
            StringDbt data = new StringDbt("data1");
            txnA = env.beginTransaction(null, TransactionConfig.DEFAULT);
            OperationStatus status = db.put(txnA, key, data);
            assertEquals(OperationStatus.SUCCESS, status);

            /*
             * txnA should have a write lock on this record. Now try to read it
             * with read-uncommitted.
             */
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            Cursor nonTxnCursor =
                db.openCursor(null, CursorConfig.DEFAULT);
            try {
                nonTxnCursor.getSearchKey
                    (key, foundData, LockMode.READ_UNCOMMITTED);
                nonTxnCursor.getSearchKey
                    (key, foundData, LockMode.READ_UNCOMMITTED_ALL);

                /*
                 * Make sure we get a deadlock exception without
                 * read-uncommitted.
                 */
                try {
                    nonTxnCursor.getSearchKey
                        (key, foundData, LockMode.DEFAULT);
                    fail("Should throw LockConflict if non-txnl, " +
                         "non-readUnc.");
                } catch (LockConflictException expected) {
                }
            } finally {
                nonTxnCursor.close();
            }

            /*
             * Make sure we get a deadlock exception without read-uncommitted.
             */
            try {
                db.get(null, key, foundData, LockMode.DEFAULT);
                fail("Should deadlock");
            } catch (LockConflictException e) {
            }

            /*
             * Specify read-uncommitted as a lock mode.
             */
            status = db.get(null, key, foundData, LockMode.READ_UNCOMMITTED);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));

            status = db.get(
                null, key, foundData, LockMode.READ_UNCOMMITTED_ALL);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));

            status = db.getSearchBoth
                (null, key, data, LockMode.READ_UNCOMMITTED);
            assertEquals(OperationStatus.SUCCESS, status);

            status = db.getSearchBoth
                (null, key, data, LockMode.READ_UNCOMMITTED_ALL);
            assertEquals(OperationStatus.SUCCESS, status);

            Transaction txn = null;
            if (DualTestCase.isReplicatedTest(getClass())) {
                txn = txnA;
            }

            cursor = db.openCursor(txn, CursorConfig.DEFAULT);

            status = cursor.getFirst(
                foundKey, foundData, LockMode.READ_UNCOMMITTED);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(key.getData(), foundKey.getData()));
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));

            status = cursor.getFirst(
                foundKey, foundData, LockMode.READ_UNCOMMITTED_ALL);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(key.getData(), foundKey.getData()));
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));

            cursor.close();

            /*
             * Specify read-uncommitted through a read-uncommitted txn.
             */
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setReadUncommitted(true);
            Transaction readUncommittedTxn =
                env.beginTransaction(null, txnConfig);

            status = db.get
                (readUncommittedTxn, key, foundData, LockMode.DEFAULT);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));

            status = db.getSearchBoth
                (readUncommittedTxn, key, data,LockMode.DEFAULT);
            assertEquals(OperationStatus.SUCCESS, status);

            cursor = db.openCursor(readUncommittedTxn, CursorConfig.DEFAULT);
            status = cursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(key.getData(), foundKey.getData()));
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));
            cursor.close();
            readUncommittedTxn.abort();

            /*
             * Specify read-uncommitted through a read-uncommitted cursor
             */
            if (DualTestCase.isReplicatedTest(getClass())) {
                txn = txnA;
            } else {
                txn = null;
            }

            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            cursor = db.openCursor(txn, cursorConfig);
            status = cursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
            assertEquals(OperationStatus.SUCCESS, status);
            assertTrue(Arrays.equals(key.getData(), foundKey.getData()));
            assertTrue(Arrays.equals(data.getData(), foundData.getData()));

            /*
             * Open through the compatiblity method, should accept dirty
             * read (but ignores it)
             */
            // Database compatDb = new Database(env);
            // compatDb.open(null, null, "foo", DbConstants.DB_BTREE,
            //             DbConstants.DB_DIRTY_READ, DbConstants.DB_UNKNOWN);
            // compatDb.close();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (txnA != null) {
                txnA.abort();
            }

            if (db != null) {
                db.close();
            }
            close(env);
        }
    }
}
