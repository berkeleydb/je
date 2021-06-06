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

package com.sleepycat.je.dbi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.utilint.StringUtils;

/**
 * Various unit tests for CursorImpl.dup().
 */
public class DbCursorDupTest extends DbCursorTestBase {

    public DbCursorDupTest() {
        super();
    }

    @Test
    public void testCursorDupAndCloseDb()
        throws DatabaseException {

        initEnv(false);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database myDb = exampleEnv.openDatabase(null, "fooDb", dbConfig);

        myDb.put(null, new StringDbt("blah"), new StringDbt("blort"));
        Cursor cursor = myDb.openCursor(null, null);
        OperationStatus status = cursor.getNext(new DatabaseEntry(),
                                                new DatabaseEntry(),
                                                LockMode.DEFAULT);
        assertEquals(OperationStatus.SUCCESS, status);
        Cursor cursorDup = cursor.dup(true);
        cursor.close();
        cursorDup.close();
        myDb.close();
    }

    @Test
    public void testDupInitialized()
        throws DatabaseException {

        /* Open db. */
        initEnv(false);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database myDb = exampleEnv.openDatabase(null, "fooDb", dbConfig);

        /* Open uninitialized cursor. */
        Cursor c1 = myDb.openCursor(null, null);
        try {
            c1.getCurrent(new DatabaseEntry(), new DatabaseEntry(), null);
            fail();
        } catch (IllegalStateException expected) {}

        /* Dup uninitialized cursor with samePosition=false. */
        Cursor c2 = c1.dup(false);
        try {
            c2.getCurrent(new DatabaseEntry(), new DatabaseEntry(), null);
            fail();
        } catch (IllegalStateException expected) {}

        /* Dup uninitialized cursor with samePosition=true. */
        Cursor c3 = c1.dup(true);
        try {
            c3.getCurrent(new DatabaseEntry(), new DatabaseEntry(), null);
            fail();
        } catch (IllegalStateException expected) {}

        /* Ensure dup'ed cursors are usable. */
        assertEquals(OperationStatus.SUCCESS,
                     c1.put(new DatabaseEntry(new byte[0]),
                            new DatabaseEntry(new byte[0])));
        assertEquals(OperationStatus.SUCCESS,
                     c2.getFirst(new DatabaseEntry(), new DatabaseEntry(),
                                 null));
        assertEquals(OperationStatus.NOTFOUND,
                     c2.getNext(new DatabaseEntry(), new DatabaseEntry(),
                                 null));
        assertEquals(OperationStatus.SUCCESS,
                     c3.getFirst(new DatabaseEntry(), new DatabaseEntry(),
                                 null));
        assertEquals(OperationStatus.NOTFOUND,
                     c3.getNext(new DatabaseEntry(), new DatabaseEntry(),
                                 null));

        /* Close db. */
        c3.close();
        c2.close();
        c1.close();
        myDb.close();
    }

    /**
     * Create some duplicate data.
     *
     * Pass 1, walk over the data and with each iteration, dup() the
     * cursor at the same position.  Ensure that the dup points to the
     * same key/data pair.  Advance the dup'd cursor and ensure that
     * the data is different (key may be the same since it's a
     * duplicate set).  Then dup() the cursor without maintaining
     * position.  Ensure that getCurrent() throws a Cursor Not Init'd
     * exception.
     *
     * Pass 2, iterate through the data, and dup the cursor in the
     * same position.  Advance the original cursor and ensure that the
     * dup()'d points to the original data and the original cursor
     * points at new data.
     */
    @Test
    public void testCursorDupSamePosition()
        throws DatabaseException {

        initEnv(true);
        createRandomDuplicateData(null, false);

        DataWalker dw = new DataWalker(null) {
                void perData(String foundKey, String foundData)
                    throws DatabaseException {
                    DatabaseEntry keyDbt = new DatabaseEntry();
                    DatabaseEntry dataDbt = new DatabaseEntry();
                    Cursor cursor2 = cursor.dup(true);
                    cursor2.getCurrent(keyDbt, dataDbt, LockMode.DEFAULT);
                    String c2Key = StringUtils.fromUTF8(keyDbt.getData());
                    String c2Data = StringUtils.fromUTF8(dataDbt.getData());
                    assertTrue(c2Key.equals(foundKey));
                    assertTrue(c2Data.equals(foundData));
                    if (cursor2.getNext(keyDbt,
                                        dataDbt,
                                        LockMode.DEFAULT) ==
                        OperationStatus.SUCCESS) {
                        /* Keys can be the same because we have duplicates. */
                        /*
                          assertFalse(StringUtils.fromUTF8(keyDbt.getData()).
                          equals(foundKey));
                        */
                        assertFalse(StringUtils.fromUTF8(dataDbt.getData()).
                                    equals(foundData));
                    }
                    cursor2.close();
                    try {
                        cursor2 = cursor.dup(false);
                        cursor2.getCurrent(keyDbt, dataDbt, LockMode.DEFAULT);
                        fail("didn't catch Cursor not initialized exception");
                    } catch (IllegalStateException expected) {
                    }
                    cursor2.close();
                }
            };
        dw.setIgnoreDataMap(true);
        dw.walkData();

        dw = new DataWalker(null) {
                void perData(String foundKey, String foundData)
                    throws DatabaseException {
                    DatabaseEntry keyDbt = new DatabaseEntry();
                    DatabaseEntry dataDbt = new DatabaseEntry();
                    DatabaseEntry key2Dbt = new DatabaseEntry();
                    DatabaseEntry data2Dbt = new DatabaseEntry();
                    Cursor cursor2 = cursor.dup(true);

                    OperationStatus status =
                        cursor.getNext(keyDbt, dataDbt, LockMode.DEFAULT);

                    cursor2.getCurrent(key2Dbt, data2Dbt, LockMode.DEFAULT);
                    String c2Key = StringUtils.fromUTF8(key2Dbt.getData());
                    String c2Data = StringUtils.fromUTF8(data2Dbt.getData());
                    assertTrue(c2Key.equals(foundKey));
                    assertTrue(c2Data.equals(foundData));
                    if (status == OperationStatus.SUCCESS) {
                        assertFalse(StringUtils.fromUTF8(dataDbt.getData()).
                                    equals(foundData));
                        assertFalse(StringUtils.fromUTF8(dataDbt.getData()).
                                    equals(c2Data));
                    }
                    cursor2.close();
                }
            };
        dw.setIgnoreDataMap(true);
        dw.walkData();
    }
}
