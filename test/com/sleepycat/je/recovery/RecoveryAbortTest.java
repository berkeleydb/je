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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;

public class RecoveryAbortTest extends RecoveryTestBase {
    private static final boolean DEBUG = false;

    public RecoveryAbortTest() {
        super(true);
    }

    /**
     * Insert data into several dbs, then abort.
     */
    @Test
    public void testBasic()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = NUM_RECS * 3;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert all the data. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs - 1, expectedData, 1, false, NUM_DBS);
            txn.abort();
            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Test insert/abort with no duplicates.
     */
    @Test
    public void testInserts()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        EnvironmentImpl realEnv = DbInternal.getNonNullEnvImpl(env);

        int N = NUM_RECS;

        if (DEBUG) {
            System.out.println("<dump>");
        }
        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert 0 - N and commit. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, N - 1, expectedData, 1, true, NUM_DBS);
            txn.commit();
            verifyData(expectedData, false, NUM_DBS);

            /* Insert N - 3N and abort. */
            txn = env.beginTransaction(null, null);
            insertData(txn, N, (3 * N) - 1, expectedData, 1, false, NUM_DBS);
            txn.abort();
            verifyData(expectedData, false, NUM_DBS);

            /*
             * Wait for the incompressor queue to be processed, so we force the
             * recovery to run w/IN delete replays.
             */
            while (realEnv.getINCompressorQueueSize() > 0) {
                Thread.sleep(10000);
            }

            /* Insert 2N - 4N and commit. */
            txn = env.beginTransaction(null, null);
            insertData(txn, (2 * N), (4 * N) - 1, expectedData, 1, true,
                       NUM_DBS);
            txn.commit();
            verifyData(expectedData, false, NUM_DBS);

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);

        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        } finally {
            if (DEBUG) {
                System.out.println("</dump>");
            }
        }
    }

    @Test
    public void testMix()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);

        int numRecs = NUM_RECS;
        int numDups = 10;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert data without duplicates. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs, expectedData, 1, true, NUM_DBS);

            /* Insert more with duplicates, commit. */
            insertData(txn, numRecs+1, (2*numRecs), expectedData,
                       numDups, true, NUM_DBS);
            txn.commit();

            /* Delete all and abort. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, true, false, NUM_DBS);
            txn.abort();

            /* Delete every other and commit. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, true, NUM_DBS);
            txn.commit();

            /* Modify some and abort. */
            txn = env.beginTransaction(null, null);
            modifyData(txn, numRecs, expectedData, 3, false, NUM_DBS);
            txn.abort();

            /* Modify some and commit. */
            txn = env.beginTransaction(null, null);
            modifyData(txn, numRecs/2, expectedData, 2, true, NUM_DBS);
            txn.commit();

            if (DEBUG) {
                dumpData(NUM_DBS);
                dumpExpected(expectedData);
                com.sleepycat.je.tree.Key.DUMP_TYPE =
                    com.sleepycat.je.tree.Key.DumpType.BINARY;
                DbInternal.getDbImpl(dbs[0]).getTree().dump();
            }
            TestUtils.validateNodeMemUsage
                           (DbInternal.getNonNullEnvImpl(env),
                            false);
            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            // print stacktrace before trying to clean up files
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testSR13726()
        throws Throwable {

        int numDbs = 1;

        createEnvAndDbs(1 << 20, true, numDbs);

        try {
            /*
             * Insert data without duplicates, commit. This gets us a
             * DupCountLN.
             */
            Transaction txn = env.beginTransaction(null, null);
            Cursor c = dbs[0].openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            byte[] keyData = TestUtils.getTestArray(0);
            byte[] dataData = TestUtils.byteArrayCopy(keyData);
            key.setData(keyData);
            data.setData(dataData);
            for (int i = 0; i < 3; i++) {
                data.setData(TestUtils.getTestArray(i));
                assertEquals("insert some dups",
                             c.put(key, data),
                             OperationStatus.SUCCESS);
            }
            c.close();
            txn.commit();

            /* This gets us a DelDupLN in the slot in the BIN. */
            txn = env.beginTransaction(null, null);
            assertEquals("delete initial dups",
                         dbs[0].delete(txn, key),
                         OperationStatus.SUCCESS);
            txn.commit();

            /* This gets the dup tree cleaned up. */
            env.compress();

            /* Gets the BIN written out with knownDeleted=true. */
            closeEnv();
            recoverOnly();
            createDbs(null, numDbs);

            /*
             * Tree now has a BIN referring to a DelDupLN.  Add duplicates,
             * and abort.
             */
            txn = env.beginTransaction(null, null);
            c = dbs[0].openCursor(txn, null);
            for (int i = 0; i < 3; i++) {
                data.setData(TestUtils.getTestArray(i));
                assertEquals("insert later dups",
                             c.put(key, data),
                             OperationStatus.SUCCESS);
            }
            c.close();
            txn.abort();

            /*
             * Now add duplicates again and commit.
             */
            txn = env.beginTransaction(null, null);
            c = dbs[0].openCursor(txn, null);
            for (int i = 0; i < 3; i++) {
                data.setData(TestUtils.getTestArray(i));
                assertEquals("insert later dups",
                             c.put(key, data),
                             OperationStatus.SUCCESS);
            }
            c.close();
            txn.commit();

            txn = env.beginTransaction(null, null);
            c = dbs[0].openCursor(txn, null);
            int count = 0;
            while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
                count++;
            }
            c.getSearchKey(key, data, null);
            assertEquals("scanned count == count()", count, c.count());
            c.close();
            txn.commit();
            closeEnv();
        } catch (Throwable t) {
            // print stacktrace before trying to clean up files
            t.printStackTrace();
            throw t;
        }
    }

    /*
     * Test the sequence where we have an existing record in the
     * database; then in a separate transaction we delete that data
     * and reinsert it and then abort that transaction.  During the
     * undo, the insert will be undone first (by deleting the record
     * and setting knownDeleted true in the ChildReference); the
     * deletion will be undone second by adding the record back into
     * the database.  The entry needs to be present in the BIN when we
     * add it back in.  But the compressor may be running at the same
     * time and compress the entry out between the deletion and
     * re-insertion making the entry disappear from the BIN.  This is
     * prevented by a lock being taken by the compressor on the LN,
     * even if the LN is "knownDeleted". [#9465]
     */
    @Test
    public void testSR9465Part1()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = NUM_RECS;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert data without duplicates. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs, expectedData, 1, true, NUM_DBS);
            txn.commit();

            /* Delete all and abort. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, true, false, NUM_DBS);
            insertData(txn, 0, numRecs, expectedData, 1, false, NUM_DBS);
            txn.abort();

            txn = env.beginTransaction(null, null);
            verifyData(expectedData, NUM_DBS);
            txn.commit();

            if (DEBUG) {
                dumpData(NUM_DBS);
                dumpExpected(expectedData);
                com.sleepycat.je.tree.Key.DUMP_TYPE =
                    com.sleepycat.je.tree.Key.DumpType.BINARY;
                DbInternal.getDbImpl(dbs[0]).getTree().dump();
            }

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testSR9465Part2()
        throws Throwable {

        createEnvAndDbs(1 << 20, true, NUM_DBS);
        int numRecs = NUM_RECS;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert data without duplicates. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs, expectedData, 1, true, NUM_DBS);
            txn.commit();

            /* Delete all and abort. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, true, false, NUM_DBS);
            insertData(txn, 0, numRecs, expectedData, 1, false, NUM_DBS);
            deleteData(txn, expectedData, true, false, NUM_DBS);
            txn.abort();

            if (DEBUG) {
                dumpData(NUM_DBS);
                dumpExpected(expectedData);
                com.sleepycat.je.tree.Key.DUMP_TYPE =
                    com.sleepycat.je.tree.Key.DumpType.BINARY;
                DbInternal.getDbImpl(dbs[0]).getTree().dump();
            }

            txn = env.beginTransaction(null, null);
            verifyData(expectedData, NUM_DBS);
            txn.commit();

            if (DEBUG) {
                dumpData(NUM_DBS);
                dumpExpected(expectedData);
                com.sleepycat.je.tree.Key.DUMP_TYPE =
                    com.sleepycat.je.tree.Key.DumpType.BINARY;
                DbInternal.getDbImpl(dbs[0]).getTree().dump();
            }

            closeEnv();
            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testSR9752Part1()
        throws Throwable {

        createEnvAndDbs(1 << 20, false, NUM_DBS);
        int numRecs = NUM_RECS;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert data without duplicates. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs, expectedData, 1, true, NUM_DBS);
            txn.commit();

            /*
             * txn1 just puts a piece of data out to a database that won't
             * be seen by deleteData or insertData.  The idea is to hold
             * the transaction open across the env.sync() so that firstActive
             * comes before ckptStart.
             */
            Transaction txn1 = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });
            DatabaseEntry data = new DatabaseEntry(new byte[] { 4, 3, 2, 1 });
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(false);
            dbConfig.setTransactional(true);
            Database otherDb = env.openDatabase(txn1, "extradb", dbConfig);
            otherDb.put(txn1, key, data);

            /* Delete all and abort. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, false, NUM_DBS);
            txn.abort();

            /* Delete all and commit. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, true, NUM_DBS);
            txn.commit();

            env.sync(); /* env.checkpoint does not seem to be sufficient. */
            txn1.commit();
            otherDb.close();

            closeEnv();

            if (DEBUG) {
                dumpData(NUM_DBS);
                dumpExpected(expectedData);
                com.sleepycat.je.tree.Key.DUMP_TYPE =
                    com.sleepycat.je.tree.Key.DumpType.BINARY;
                DbInternal.getDbImpl(dbs[0]).getTree().dump();
            }

            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testSR9752Part2()
        throws Throwable {

        createEnvAndDbs(1 << 20, false, NUM_DBS);
        DbInternal.getNonNullEnvImpl(env).getCleaner().shutdown();
        int numRecs = NUM_RECS;

        try {
            /* Set up an repository of expected data. */
            Map<TestData, Set<TestData>> expectedData = 
                new HashMap<TestData, Set<TestData>>();

            /* Insert data without duplicates. */
            Transaction txn = env.beginTransaction(null, null);
            insertData(txn, 0, numRecs, expectedData, 1, true, NUM_DBS);
            txn.commit();

            /*
             * txn1 just puts a piece of data out to a database that won't
             * be seen by deleteData or insertData.  The idea is to hold
             * the transaction open across the env.sync() so that firstActive
             * comes before ckptStart.
             */
            Transaction txn1 = env.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry(new byte[] { 1, 2, 3, 4 });
            DatabaseEntry data = new DatabaseEntry(new byte[] { 4, 3, 2, 1 });
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(false);
            dbConfig.setTransactional(true);
            Database otherDb = env.openDatabase(txn1, "extradb", dbConfig);
            otherDb.put(txn1, key, data);

            /* Delete all and abort. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, false, NUM_DBS);
            txn.abort();

            /* Delete all and commit. */
            txn = env.beginTransaction(null, null);
            deleteData(txn, expectedData, false, true, NUM_DBS);
            txn.commit();

            env.sync(); /* env.checkpoint does not seem to be sufficient. */
            txn1.commit();
            otherDb.close();

            closeEnv();

            if (DEBUG) {
                dumpData(NUM_DBS);
                dumpExpected(expectedData);
                com.sleepycat.je.tree.Key.DUMP_TYPE =
                    com.sleepycat.je.tree.Key.DumpType.BINARY;
                DbInternal.getDbImpl(dbs[0]).getTree().dump();
            }

            recoverAndVerify(expectedData, NUM_DBS);
        } catch (Throwable t) {
            /* Print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Insert dbs, commit some, abort some. To do: add db remove, rename.
     */
    @Test
    public void testDbCreateRemove()
        throws Throwable {

        createEnv(1 << 20, true);
        int N1 = 10;
        int N2 = 50;
        int N3 = 60;
        int N4 = 70;
        int N5 = 100;

        String dbName1 = "foo";
        String dbName2 = "bar";

        try {
            /* Make Dbs, abort */
            Transaction txn = env.beginTransaction(null, null);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            for (int i = 0; i < N2; i++) {
                env.openDatabase(txn, dbName1 + i, dbConfig);
            }
            txn.abort();

            /* All dbs should not exist */
            checkForNoDb(dbName1, 0, N2);

            /* Make more dbs, overlapping with some of the aborted set. */
            txn = env.beginTransaction(null, null);
            for (int i = N1; i < N5; i++) {
                Database db = env.openDatabase(txn, dbName1 + i, dbConfig);
                db.close();
            }
            txn.commit();

            /*
             * Dbs 0  - N1-1 shouldn't exist
             * Dbs N1 - N5 should exist
             */
            checkForNoDb(dbName1, 0, N1);
            checkForDb(dbName1, N1, N5);

            /* Close and recover */
            env.close();

            EnvironmentConfig envConfig1 = TestUtils.initEnvConfig();
            envConfig1.setConfigParam
                (EnvironmentParams.NODE_MAX.getName(), "6");
            envConfig1.setConfigParam(EnvironmentParams.MAX_MEMORY.getName(),
                                     new Long(1 << 24).toString());
            envConfig1.setTransactional(true);
            envConfig1.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
            env = new Environment(envHome, envConfig1);

            /*
             * Dbs 0  - N1-1 shouldn't exist
             * Dbs N1 - N5 should exist
             */
            checkForNoDb(dbName1, 0, N1);
            checkForDb(dbName1, N1, N5);

            /* Remove some dbs, abort */
            txn = env.beginTransaction(null, null);
            for (int i = N2; i < N3; i++) {
                env.removeDatabase(txn, dbName1+i);
            }
            txn.abort();

            /* Remove some dbs, abort -- use READ_COMMITTED [#23821]. */
            txn = env.beginTransaction(
                null, new TransactionConfig().setReadCommitted(true));
            for (int i = N3; i < N4; i++) {
                env.removeDatabase(txn, dbName1+i);
            }
            txn.abort();

            /* Remove some dbs, commit */
            txn = env.beginTransaction(null, null);
            for (int i = N3; i < N4; i++) {
                env.removeDatabase(txn, dbName1+i);
            }
            txn.commit();

            /*
             * Dbs 0 - N1-1  should not exist
             * Dbs N1 - N3-1 should exist
             * Dbs N3 - N4-1 should not exist
             * Dbs N4 - N5-1 should exist
             */
            checkForNoDb(dbName1, 0, N1);
            checkForDb(dbName1, N1, N3);
            checkForNoDb(dbName1, N3, N4);
            checkForDb(dbName1, N4, N5);

            /* Close and recover */
            env.close();
            env = new Environment(envHome, envConfig1);

            /*
             * Dbs 0 - N1-1  should not exist
             * Dbs N1 - N3-1 should exist
             * Dbs N3 - N4-1 should not exist
             * Dbs N4 - N5-1 should exist
             */
            checkForNoDb(dbName1, 0, N1);
            checkForDb(dbName1, N1, N3);
            checkForNoDb(dbName1, N3, N4);
            checkForDb(dbName1, N4, N5);

            /* Rename some dbs, abort */
            txn = env.beginTransaction(null, null);
            for (int i = N1; i < N3; i++) {
                env.renameDatabase
                    (txn, dbName1+i, dbName2+i);
            }
            txn.abort();

            /* Remove some dbs, commit */
            txn = env.beginTransaction(null, null);
            for (int i = N2; i < N3; i++) {
                env.renameDatabase
                    (txn, dbName1+i, dbName2+i);
            }
            txn.commit();

            /*
             * Dbs 0 - N1-1  should not exist
             * Dbs N1 - N2-1 should exist with old name
             * Dbs N2 - N3-1 should exist with new name
             * Dbs N3 - N4 should not exist
             * Dbs N4 - N5-1 should exist with old name
             */
            checkForNoDb(dbName1, 0, N1);
            checkForDb(dbName1, N1, N2);
            checkForDb(dbName2, N2, N3);
            checkForNoDb(dbName1, N3, N4);
            checkForDb(dbName1, N4, N5);
        } catch (Throwable t) {
            /* print stacktrace before trying to clean up files. */
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Fail if any db from start - (end -1) exists
     */
    private void checkForNoDb(String dbName, int start, int end) {
        /* Dbs start - end -1  shouldn't exist */
        for (int i = start; i < end; i++) {
            try {
                env.openDatabase(null, dbName + i, null);
                fail(DB_NAME + i + " shouldn't exist");
            } catch (DatabaseException e) {
            }
        }
    }

    /**
     * Fail if any db from start - (end -1) doesn't exist
     */
    private void checkForDb(String dbName, int start, int end) {
        /* Dbs start - end -1  should exist. */
        for (int i = start; i < end; i++) {
            try {
                Database checkDb = env.openDatabase(null, dbName + i, null);
                checkDb.close();
            } catch (DatabaseException e) {
                fail(e.getMessage());
            }
        }
    }
}
