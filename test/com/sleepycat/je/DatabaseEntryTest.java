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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

public class DatabaseEntryTest extends DualTestCase {

    private final File envHome;
    private Environment env;
    private Database db;

    public DatabaseEntryTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testBasic() {
        /* Constructor that takes a byte array. */
        int size = 10;
        byte[] foo = new byte[size];
        byte val = 1;
        Arrays.fill(foo, val);

        DatabaseEntry dbtA = new DatabaseEntry(foo);
        assertEquals(foo.length, dbtA.getSize());
        assertTrue(Arrays.equals(foo, dbtA.getData()));

        /* Set the data to null */
        dbtA.setData(null);
        assertEquals(0, dbtA.getSize());
        assertFalse(Arrays.equals(foo, dbtA.getData()));

        /* Constructor that sets the data later */
        DatabaseEntry dbtLater = new DatabaseEntry();
        assertTrue(dbtLater.getData() == null);
        assertEquals(0, dbtLater.getSize());
        dbtLater.setData(foo);
        assertTrue(Arrays.equals(foo, dbtLater.getData()));

        /* Set offset, then reset data and offset should be reset. */
        DatabaseEntry dbtOffset = new DatabaseEntry(foo, 1, 1);
        assertEquals(1, dbtOffset.getOffset());
        assertEquals(1, dbtOffset.getSize());
        dbtOffset.setData(foo);
        assertEquals(0, dbtOffset.getOffset());
        assertEquals(foo.length, dbtOffset.getSize());
    }

    @Test
    public void testOffset()
        throws DatabaseException {

        final int N_BYTES = 30;

        openDb(false);

        DatabaseEntry originalKey = new DatabaseEntry(new byte[N_BYTES]);
        DatabaseEntry originalData = new DatabaseEntry(new byte[N_BYTES]);
        for (int i = 0; i < N_BYTES; i++) {
            originalKey.getData()[i] = (byte) i;
            originalData.getData()[i] = (byte) i;
        }

        originalKey.setSize(10);
        originalKey.setOffset(10);
        originalData.setSize(10);
        originalData.setOffset(10);

        db.put(null, originalKey, originalData);

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT);

        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(foundKey, foundData,
                                     LockMode.DEFAULT));

        assertEquals(0, foundKey.getOffset());
        assertEquals(0, foundData.getOffset());
        assertEquals(10, foundKey.getSize());
        assertEquals(10, foundData.getSize());
        for (int i = 0; i < 10; i++) {
            assertEquals(i + 10, foundKey.getData()[i]);
            assertEquals(i + 10, foundData.getData()[i]);
        }

        cursor.close();
        txn.commit();
        closeDb();
    }

    @Test
    public void testPartial()
        throws DatabaseException {

        openDb(false);

        DatabaseEntry originalKey = new DatabaseEntry(new byte[20]);
        DatabaseEntry originalData = new DatabaseEntry(new byte[20]);
        for (int i = 0; i < 20; i++) {
            originalKey.getData()[i] = (byte) i;
            originalData.getData()[i] = (byte) i;
        }

        originalData.setPartial(true);
        originalData.setPartialLength(10);
        originalData.setPartialOffset(10);

        db.put(null, originalKey, originalData);

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT);

        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(foundKey, foundData,
                                     LockMode.DEFAULT));

        assertEquals(0, foundKey.getOffset());
        assertEquals(20, foundKey.getSize());
        for (int i = 0; i < 20; i++) {
            assertEquals(i, foundKey.getData()[i]);
        }

        assertEquals(0, foundData.getOffset());
        assertEquals(30, foundData.getSize());
        for (int i = 0; i < 10; i++) {
            assertEquals(0, foundData.getData()[i]);
        }
        for (int i = 0; i < 20; i++) {
            assertEquals(i, foundData.getData()[i + 10]);
        }

        foundKey.setPartial(5, 10, true);
        foundData.setPartial(5, 20, true);

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(foundKey, foundData,
                                     LockMode.DEFAULT));
        assertEquals(0, foundKey.getOffset());
        assertEquals(10, foundKey.getSize());
        for (int i = 0; i < 10; i++) {
            assertEquals(i + 5, foundKey.getData()[i]);
        }

        assertEquals(0, foundData.getOffset());
        assertEquals(20, foundData.getSize());
        for (int i = 0; i < 5; i++) {
            assertEquals(0, foundData.getData()[i]);
        }
        for (int i = 0; i < 15; i++) {
            assertEquals(i, foundData.getData()[i + 5]);
        }

        /* Check that partial keys on put() is not allowed. */

        originalKey.setPartial(true);
        originalKey.setPartialLength(10);
        originalKey.setPartialOffset(10);

        try {
            db.put(null, originalKey, originalData);
            fail();
        } catch (IllegalArgumentException expected) {}
        try {
            db.putNoOverwrite(null, originalKey, originalData);
            fail();
        } catch (IllegalArgumentException expected) {}
        try {
            db.putNoDupData(null, originalKey, originalData);
            fail();
        } catch (IllegalArgumentException expected) {}

        try {
            cursor.put(originalKey, originalData);
            fail();
        } catch (IllegalArgumentException expected) {}
        try {
            cursor.putNoOverwrite(originalKey, originalData);
            fail();
        } catch (IllegalArgumentException expected) {}
        try {
            cursor.putNoDupData(originalKey, originalData);
            fail();
        } catch (IllegalArgumentException expected) {}

        cursor.close();
        txn.commit();
        closeDb();
    }

    @Test
    public void testPartialCursorPuts()
        throws DatabaseException {

        openDb(false);

        DatabaseEntry originalKey = new DatabaseEntry(new byte[20]);
        DatabaseEntry originalData = new DatabaseEntry(new byte[20]);
        for (int i = 0; i < 20; i++) {
            originalKey.getData()[i] = (byte) i;
            originalData.getData()[i] = (byte) i;
        }

        /* Put 20 bytes of key and data. */
        db.put(null, originalKey, originalData);

    Transaction txn = null;
    txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT);

        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(foundKey, foundData,
                                     LockMode.DEFAULT));

        assertEquals(0, foundKey.getOffset());
        assertEquals(20, foundKey.getSize());
        for (int i = 0; i < 20; i++) {
            assertEquals(i, foundKey.getData()[i]);
        }

        assertEquals(0, foundData.getOffset());
        assertEquals(20, foundData.getSize());

        for (int i = 0; i < 20; i++) {
            assertEquals(i, foundData.getData()[i]);
        }

        for (int i = 0; i < 10; i++) {
            foundData.getData()[i] = (byte) (i + 50);
        }

        foundData.setPartial(true);
        foundData.setPartialLength(10);
        foundData.setPartialOffset(10);

        cursor.putCurrent(foundData);

        foundData = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS,
                     cursor.getFirst(foundKey, foundData,
                                     LockMode.DEFAULT));
        assertEquals(0, foundKey.getOffset());
        assertEquals(20, foundKey.getSize());
        assertEquals(0, foundData.getOffset());
        assertEquals(30, foundData.getSize());
        for (int i = 0; i < 10; i++) {
            assertEquals(foundData.getData()[i], i);
            assertEquals(foundData.getData()[i + 10], (i + 50));
            assertEquals(foundData.getData()[i + 20], (i + 10));
        }

        cursor.close();
    txn.commit();
        closeDb();
    }

    @Test
    public void testToString() {
        DatabaseEntry entry = new DatabaseEntry(new byte[] {1, 2, 3}, 1, 2);
        String s1 = entry.toString();
        entry.setPartial(3, 4, true);
        String s2 = entry.toString();

        /*
         * Normally leave this disabled. Enable it to manually look at the
         * toString output and ensure it is valid XML.
         */
        if (false) {
            System.out.println(s1);
            System.out.println(s2);
        }
    }

    private void openDb(boolean dups)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 "1024");
        envConfig.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                                 "true");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 "6");
        env = create(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(dups);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    private void closeDb()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            close(env);
            env = null;
        }
    }
}
