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

package com.sleepycat.je.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class SkipTest extends TestBase {

    private static final boolean DEBUG = false;

    private static final int[] nDupsArray =
        {2, 3, 20, 50, 500, 5000, 500, 50, 20, 3, 2};
    private static final int grandTotal;
    static {
        int total = 0;
        for (int i = 0; i < nDupsArray.length; i += 1) {
            total += nDupsArray[i];
        }
        grandTotal = total;
    }

    private final File envHome;
    private Environment env;
    private Database db;

    public SkipTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } catch (Throwable e) {
            System.out.println("during tearDown: " + e);
        }
    }

    private void openEnv() {
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                                 "false");
        env = new Environment(envHome, envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void closeEnv() {
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
     * Basic test that writes data and checks it.
     */
    @Test
    public void testSkip() {
        openEnv();
        insertData();
        checkData();
        closeEnv();
    }

    /**
     * Adds uncompressed deleted slots in between existing data records,
     * checks it.
     */
    @Test
    public void testUncompressedDeletions() {
        openEnv();
        insertData();
        insertAndDelete();
        checkData();
        closeEnv();
    }

    /**
     * Passes a larger number for maxCount than the number of records before
     * (after) the cursor position.  Includes testing a zero return.
     */
    @Test
    public void testSkipPastEnd() {

        openEnv();
        insertData();
        insertAndDelete();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final int lastKey = nDupsArray.length - 1;
        final int lastData = nDupsArray[lastKey] - 1;
        final int firstKey = 0;
        final int firstData = 0;

        long total = 0;
        final Cursor c = db.openCursor(null, null);
        while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
            total += 1;

            /* Skip forward to last record. */
            Cursor c2 = c.dup(true /*samePosition*/);
            key.setData(null);
            data.setData(null);
            long count = c2.skipNext(grandTotal, key, data, null);
            assertEquals(grandTotal - total, count);
            if (count == 0) {
                assertNull(key.getData());
                assertNull(data.getData());
            } else {
                assertEquals(lastKey, IntegerBinding.entryToInt(key));
                assertEquals(lastData, IntegerBinding.entryToInt(data));
            }
            c2.close();

            /* Skip backward to first record. */
            c2 = c.dup(true /*samePosition*/);
            key.setData(null);
            data.setData(null);
            count = c2.skipPrev(grandTotal, key, data, null);
            assertEquals(total - 1, count);
            if (count == 0) {
                assertNull(key.getData());
                assertNull(data.getData());
            } else {
                assertEquals(firstKey, IntegerBinding.entryToInt(key));
                assertEquals(firstData, IntegerBinding.entryToInt(data));
            }
            c2.close();
        }
        c.close();
        closeEnv();
    }

    /**
     * Tests exceptions.
     */
    @Test
    public void testExceptions() {
        openEnv();

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        key.setData(new byte[1]);
        data.setData(new byte[1]);
        OperationStatus status = db.putNoDupData(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        data.setData(new byte[2]);
        status = db.putNoDupData(null, key, data);
        assertSame(OperationStatus.SUCCESS, status);

        /* Cursor must be initialized to call skip. */
        Cursor c = db.openCursor(null, null);
        try {
            c.skipNext(1, key, data, null);
            fail();
        } catch (IllegalStateException expected) {
        }
        try {
            c.skipPrev(1, key, data, null);
            fail();
        } catch (IllegalStateException expected) {
        }

        /* Passing maxCount LTE zero not allowed. */
        status = c.getFirst(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        try {
            c.skipNext(0, key, data, null);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            c.skipPrev(0, key, data, null);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            c.skipNext(-1, key, data, null);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            c.skipPrev(-1, key, data, null);
            fail();
        } catch (IllegalArgumentException expected) {
        }

        c.close();
        closeEnv();
    }

    /**
     * Inserts duplicates according to nDupsArray, where the key is the index
     * into nDupsArray (0 to nDupsArray.length-1), and the data is 0 to N,
     * where N is the number of dups for that key (nDupsArray[index]).
     */
    private void insertData() {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        for (int i = 0; i < nDupsArray.length; i += 1) {
            final int nDups = nDupsArray[i];
            IntegerBinding.intToEntry(i, key);
            for (int j = 0; j < nDups; j += 1) {
                IntegerBinding.intToEntry(j, data);
                final OperationStatus status = db.putNoDupData(null, key,
                                                               data);
                assertSame(OperationStatus.SUCCESS, status);
            }
        }
    }

    /**
     * Inserts records between each two records inserted earlier, and then
     * deletes the new records.  This leaves uncompressed deleted slots, which
     * should be ignored by the skip methods.
     */
    private void insertAndDelete() {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        final Cursor c = db.openCursor(null, null);
        while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
            final byte[] a = new byte[data.getSize() + 1];
            System.arraycopy(data.getData(), 0, a, 0, data.getSize());
            data.setData(a);
            OperationStatus status = c.putNoDupData(key, data);
            assertSame(OperationStatus.SUCCESS, status);
            status = c.delete();
            assertSame(OperationStatus.SUCCESS, status);
        }
        c.close();
    }

    /**
     * Starts at the beginning (end) of the database and skips to the beginning
     * and end of each duplicate set.
     */
    private void checkData() {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        /* Skip in forward direction. */
        final Cursor c = db.openCursor(null, null);
        long total = nDupsArray[0];
        for (int i = 1; i < nDupsArray.length; i += 1) {

            /* Move to first record. */
            final OperationStatus status = c.getFirst(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);

            long skipInit = total;
            total += nDupsArray[i];

            for (int j = i; j < nDupsArray.length; j += 1) {
                final long dupsMinusOne = nDupsArray[j] - 1;

                /* Skip to first dup. */
                key.setData(null);
                data.setData(null);
                long count = c.skipNext(skipInit, key, data, null);
                assertEquals(skipInit, count);
                assertEquals(j, IntegerBinding.entryToInt(key));
                assertEquals(0, IntegerBinding.entryToInt(data));

                /* Skip to last dup. */
                key.setData(null);
                data.setData(null);
                count = c.skipNext(dupsMinusOne, key, data, null);
                assertEquals(dupsMinusOne, count);
                assertEquals(j, IntegerBinding.entryToInt(key));
                assertEquals(dupsMinusOne, IntegerBinding.entryToInt(data));

                skipInit = 1;
            }
        }

        /* Skip in reverse direction. */
        total = nDupsArray[nDupsArray.length - 1];
        for (int i = nDupsArray.length - 2; i >= 0; i -= 1) {

            /* Move to last record. */
            final OperationStatus status = c.getLast(key, data, null);
            assertSame(OperationStatus.SUCCESS, status);

            long skipInit = total;
            total += nDupsArray[i];

            for (int j = i; j >= 0; j -= 1) {
                final long dupsMinusOne = nDupsArray[j] - 1;

                /* Skip to last dup. */
                key.setData(null);
                data.setData(null);
                long count = c.skipPrev(skipInit, key, data, null);
                assertEquals(skipInit, count);
                assertEquals(j, IntegerBinding.entryToInt(key));
                assertEquals(dupsMinusOne, IntegerBinding.entryToInt(data));

                /* Skip to first dup. */
                key.setData(null);
                data.setData(null);
                count = c.skipPrev(dupsMinusOne, key, data, null);
                assertEquals(dupsMinusOne, count);
                assertEquals(j, IntegerBinding.entryToInt(key));
                assertEquals(0, IntegerBinding.entryToInt(data));

                skipInit = 1;
            }
        }
        c.close();
    }
}
