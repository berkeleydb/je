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

package com.sleepycat.je.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.utilint.StringUtils;

/**
 * Check maintenance of the memory size count within nodes.
 */
public class MemorySizeTest extends DualTestCase {
    private Environment env;
    private final File envHome;
    private Database db;

    public MemorySizeTest() {
        envHome = SharedTestUtils.getTestDir();
        /* Print keys as numbers */
        Key.DUMP_TYPE = DumpType.BINARY;
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        IN.ACCUMULATED_LIMIT = 0;
        Txn.ACCUMULATED_LIMIT = 0;

        /*
         * Properties for creating an environment.
         * Disable the evictor for this test, use larger BINS
         */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_EVICTOR.getName(),
                                 "false");
        envConfig.setConfigParam(
                       EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(),
                       "false");
        envConfig.setConfigParam(
                       EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(),
                       "false");
        envConfig.setConfigParam(
                       EnvironmentParams.ENV_RUN_CLEANER.getName(),
                       "false");

        /* Don't checkpoint utilization info for this test. */
        DbInternal.setCheckpointUP(envConfig, false);

        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "4");
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.setTransactional(true);
        env = create(envHome, envConfig);
    }

    @After
    public void tearDown()
        throws Exception {

        if (env != null) {
            close(env);
        }
        super.tearDown();
    }

    /* Test that the KeyPrefix changes should result in memory changes. */
    @Test
    public void testKeyPrefixChange()
        throws Throwable {

        final String dbName = "testDB";
        final String value = "herococo";

        /* Create the database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setKeyPrefixing(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, dbName, dbConfig);

        /* Insert some records with the same KeyPrefix. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= 9; i++) {
            StringBinding.stringToEntry(value + i, key);
            StringBinding.stringToEntry(value, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        /* Traverse the BIN. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = null;
        boolean success = false;
        try {
            cursor = db.openCursor(txn, null);
            int counter = 0;
            boolean hasKeyPrefix = false;
            while (cursor.getNext(key, data, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {
                counter++;

                final String keyString = StringUtils.fromUTF8(key.getData());
                final int keyLength = keyString.length();
                final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();

                /* Check that record is ordered by bytes. */
                assertEquals(keyString.substring(0, keyLength - 2), value);
                assertEquals(keyString.substring(keyLength - 2, keyLength - 1),
                             new Integer(counter).toString());

                /*
                 * If the BIN has KeyPrefix, reset the KeyPrefix and check the
                 * MemoryBudget. Note we don't do the recalculation here, so
                 * checking that the memory does change is sufficient.
                 */
                if (bin.getKeyPrefix() != null) {
                    assertEquals(StringUtils.fromUTF8(bin.getKeyPrefix()),
                                 value);
                    hasKeyPrefix = true;
                    long formerMemorySize = bin.getInMemorySize();
                    /* Change the KeyPrefix of this BIN. */
                    bin.latch();
                    bin.setKeyPrefix(StringUtils.toUTF8("hero"));
                    bin.releaseLatch();
                    assertEquals(StringUtils.fromUTF8(bin.getKeyPrefix()),
                                 "hero");
                    /* Check the MemorySize has changed. */
                    assertTrue(bin.getInMemorySize() != formerMemorySize);
                    break;
                }
            }
            assertTrue(hasKeyPrefix);
            success = true;
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (success) {
                txn.commit();
            } else {
                txn.abort();
            }
        }

        db.close();
    }

    /*
     * Do a series of these actions and make sure that the stored memory
     * sizes match the calculated memory size.
     * - create db
     * - insert records, no split
     * - cause IN split
     * - modify
     * - delete, compress
     * - checkpoint
     * - evict
     * - insert duplicates
     * - cause duplicate IN split
     * - do an abort
     *
     * After duplicate storage was redone in JE 5, this test no longer
     * exercises memory maintenance as thoroughly, since dup LNs are always
     * evicted immediately.  A non-dups test may be needed in the future.
     */
    @Test
    public void testMemSizeMaintenanceDups()
        throws Throwable {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        try {
            initDb(true);

            /* Insert one record. Adds two INs and an LN to our cost.*/
            insert((byte) 1, 10, (byte) 1, 100, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Fill out the node. */
            insert((byte) 2, 10, (byte) 2, 100, true);
            insert((byte) 3, 10, (byte) 3, 100, true);
            insert((byte) 4, 10, (byte) 4, 100, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Cause a split */
            insert((byte) 5, 10, (byte) 5, 100, true);
            insert((byte) 6, 10, (byte) 6, 100, true);
            insert((byte) 7, 10, (byte) 7, 100, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Modify data */
            modify((byte) 1, 10, (byte) 1, 1010, true);
            modify((byte) 7, 10, (byte) 7, 1010, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Delete data */
            delete((byte) 2, 10, true);
            delete((byte) 6, 10, true);
            checkCount(5);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Compress. */
            compress();
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Checkpoint */
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            env.checkpoint(ckptConfig);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /*
             * Check count, with side effect of fetching all records and
             * preventing further fetches that impact mem usage.
             */
            checkCount(5);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* insert duplicates */
            insert((byte) 3, 10, (byte) 30, 200, true);
            insert((byte) 3, 10, (byte) 31, 200, true);
            insert((byte) 3, 10, (byte) 32, 200, true);
            insert((byte) 3, 10, (byte) 33, 200, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* create duplicate split. */
            insert((byte) 3, 10, (byte) 34, 200, true);
            insert((byte) 3, 10, (byte) 35, 200, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* There should be 11 records. */
            checkCount(11);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* modify (use same data) and abort */
            modify((byte) 5, 10, (byte) 5, 100, false);
            /* Abort will evict LN.  Count to fetch LN back into tree. */
            checkCount(11);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* modify (use different data) and abort */
            modify((byte) 5, 10, (byte) 30, 1000, false);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* delete and abort */
            delete((byte) 1, 10, false);
            delete((byte) 7, 10, false);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Delete dup */
            delete((byte) 3, 10, (byte)34, 200, false);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* insert and abort */
            insert((byte) 2, 10, (byte) 5, 100, false);
            insert((byte) 6, 10, (byte) 6, 100, false);
            insert((byte) 8, 10, (byte) 7, 100, false);
            TestUtils.validateNodeMemUsage(envImpl, true);

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

    /*
     * Do a series of these actions and make sure that the stored memory
     * sizes match the calculated memory size.
     * - create db
     * - insert records, cause split
     * - delete
     * - insert and re-use slots.
     */
    @Test
    public void testSlotReuseMaintenance()
        throws Exception {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        try {

            initDb(true /*dups*/);

            /* Insert enough records to create one node. */
            insert((byte) 1, 10, (byte) 1, 100, true);
            insert((byte) 2, 10, (byte) 2, 100, true);
            insert((byte) 3, 10, (byte) 3, 100, true);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Delete  */
            delete((byte) 3, 10, true);
            checkCount(2);
            TestUtils.validateNodeMemUsage(envImpl, true);

            /* Insert again, reuse those slots */
            insert((byte) 3, 10, (byte) 2, 400, true);
            TestUtils.validateNodeMemUsage(envImpl, true);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

    private void initDb(boolean dups)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void insert(byte keyVal, int keySize,
                        byte dataVal, int dataSize,
                        boolean commit)
        throws DatabaseException {

        Transaction txn = null;
        if (!commit) {
            txn = env.beginTransaction(null, null);
        }
        assertEquals(OperationStatus.SUCCESS,
                     db.put(null, getEntry(keyVal, keySize),
                            getEntry(dataVal, dataSize)));
        if (!commit) {
            txn.abort();
        }
    }

    private void modify(byte keyVal, int keySize,
                        byte dataVal, int dataSize,
                        boolean commit)
        throws DatabaseException {

        Transaction txn = null;

        txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        DatabaseEntry data = new DatabaseEntry();
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchKey(getEntry(keyVal, keySize),
                                         data, LockMode.DEFAULT));
        /* To avoid changing memory sizes, do not delete unless necessary. */
        if (!data.equals(getEntry(dataVal, dataSize))) {
            assertEquals(OperationStatus.SUCCESS,
                         cursor.delete());
        }
        assertEquals(OperationStatus.SUCCESS,
                     cursor.put(getEntry(keyVal, keySize),
                                getEntry(dataVal, dataSize)));
        cursor.close();

        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
    }

    private void delete(byte keyVal, int keySize, boolean commit)
        throws DatabaseException {

        Transaction txn = null;
        if (!commit) {
            txn = env.beginTransaction(null, null);
        }
        assertEquals(OperationStatus.SUCCESS,
                     db.delete(txn, getEntry(keyVal, keySize)));
        if (!commit) {
            txn.abort();
        }
    }

    private void delete(byte keyVal, int keySize,
                        byte dataVal, int dataSize, boolean commit)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        assertEquals(OperationStatus.SUCCESS,
                     cursor.getSearchBoth(getEntry(keyVal, keySize),
                                          getEntry(dataVal, dataSize),
                                          LockMode.DEFAULT));
        assertEquals(OperationStatus.SUCCESS,  cursor.delete());
        cursor.close();

        if (commit) {
            txn.commit();
        } else {
            txn.abort();
        }
    }

    /*
     * Fake compressing daemon by call BIN.compress explicitly on all
     * BINS on the IN list.
     */
    private void compress()
        throws DatabaseException {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        INList inList = envImpl.getInMemoryINs();
        for (IN in : inList) {
            in.latch();
            envImpl.lazyCompress(in);
            in.releaseLatch();
        }
    }

    /*
     * Fake eviction daemon by call BIN.evictLNs explicitly on all
     * BINS on the IN list.
     *
     * Not currently used but may be needed later for a non-dups test.
     */
    private void evict()
        throws DatabaseException {

        INList inList = DbInternal.getNonNullEnvImpl(env).getInMemoryINs();
        for (IN in : inList) {
            if (in instanceof BIN &&
                !in.getDatabase().getDbType().isInternal()) {
                BIN bin = (BIN) in;
                bin.latch();
                try {
                    /* Expect to evict LNs. */
                    if ((bin.evictLNs() & ~IN.NON_EVICTABLE_IN) > 0) {
                        return;
                    }
                    fail("No LNs evicted.");
                } finally {
                    bin.releaseLatch();
                }
            }
        }
    }

    private DatabaseEntry getEntry(byte val, int size) {
        byte[] bArray = new byte[size];
        bArray[0] = val;
        return new DatabaseEntry(bArray);
    }

    private void checkCount(int expectedCount)
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);
        int count = 0;
        while (cursor.getNext(new DatabaseEntry(), new DatabaseEntry(),
                              LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            count++;
        }
        cursor.close();
        txn.commit();
        assertEquals(expectedCount, count);
    }

    private void dumpINList() {
        INList inList = DbInternal.getNonNullEnvImpl(env).getInMemoryINs();
        for (IN in : inList) {
            System.out.println("in nodeId=" + in.getNodeId());
        }
    }
}
