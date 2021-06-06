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

import java.util.Hashtable;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.util.StringDbt;

/**
 * Various unit tests for CursorImpl.delete().
 */
public class DbCursorDeleteTest extends DbCursorTestBase {

    public DbCursorDeleteTest() {
        super();
    }

    /**
     * Put a small number of data items into the database in a specific order,
     * delete all the ones beginning with 'f', and then make sure they were
     * really deleted.
     */
    @Test
    public void testSimpleDelete()
        throws DatabaseException {

        initEnv(false);
        doSimpleCursorPuts();

        int deletedEntries = 0;
        DataWalker dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                try {
                    if (foundKey.charAt(0) == 'f') {
                        cursor.delete();
                        deletedEntries++;
                    }
                } catch (DatabaseException DBE) {
                    System.out.println("DBE " + DBE);
                }
            }
        };
        dw.walkData();
        deletedEntries = dw.deletedEntries;
        dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                assertTrue(foundKey.compareTo(prevKey) >= 0);
                assertTrue(foundKey.charAt(0) != 'f');
                prevKey = foundKey;
            }
        };
        dw.walkData();
        assertTrue(dw.nEntries == simpleKeyStrings.length - deletedEntries);
    }

    /**
     * Put a small number of data items into the database in a specific order.
     * For each one: delete, getCurrent (make sure failure), reinsert
     * (success), delete (success).  Once iterated through all of them,
     * reinsert and make sure successful.
     */
    @Test
    public void testSimpleDeleteInsert()
        throws DatabaseException {

        initEnv(false);
        doSimpleCursorPuts();
        DataWalker dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                try {
                    cursor.delete();
                    deletedEntries++;
                    assertEquals(OperationStatus.KEYEMPTY,
                                 cursor.getCurrent
                                 (new StringDbt(), new StringDbt(),
                                  LockMode.DEFAULT));
                    StringDbt newKey = new StringDbt(foundKey);
                    StringDbt newData = new StringDbt(foundData);
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor2.putNoOverwrite(newKey, newData));
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor2.delete());
                } catch (DatabaseException DBE) {
                    System.out.println("DBE " + DBE);
                }
            }
        };

        dw.walkData();
        doSimpleCursorPuts();

        dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                assertEquals(foundData,
                             (String) simpleDataMap.get(foundKey));
                simpleDataMap.remove(foundKey);
            }
        };
        dw.walkData();
        assertTrue(simpleDataMap.size() == 0);
    }

    /**
     * Put a small number of data items into the database in a specific order.
     * For each one: delete, getCurrent (make sure failure), reinsert
     * (success), delete (success).  Once iterated through all of them,
     * reinsert and make sure successful.
     */
    @Test
    public void testSimpleDeletePutCurrent()
        throws DatabaseException {

        initEnv(false);
        doSimpleCursorPuts();
        DataWalker dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                try {
                    cursor.delete();
                    deletedEntries++;
                    assertEquals(OperationStatus.KEYEMPTY,
                                 cursor.getCurrent
                                 (new StringDbt(), new StringDbt(),
                                  LockMode.DEFAULT));
                    StringDbt newData = new StringDbt(foundData);
                    assertEquals(OperationStatus.KEYEMPTY,
                                 cursor.putCurrent(newData));
                } catch (DatabaseException DBE) {
                    System.out.println("DBE " + DBE);
                }
            }
        };

        dw.walkData();
        doSimpleCursorPuts();

        dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                assertEquals(foundData,
                             (String) simpleDataMap.get(foundKey));
                simpleDataMap.remove(foundKey);
            }
        };
        dw.walkData();
        assertTrue(simpleDataMap.size() == 0);
    }

    /**
     * Similar to above test, but there was some question about whether this
     * tests new functionality or not.  Insert k1/d1 and d1/k1.  Iterate
     * through the data and delete k1/d1.  Reinsert k1/d1 and make sure it
     * inserts ok.
     */
    @Test
    public void testSimpleInsertDeleteInsert()
        throws DatabaseException {

        initEnv(true);
        StringDbt key = new StringDbt("k1");
        StringDbt data1 = new StringDbt("d1");

        assertEquals(OperationStatus.SUCCESS,
                     putAndVerifyCursor(cursor, key, data1, true));
        assertEquals(OperationStatus.SUCCESS,
                     putAndVerifyCursor(cursor, data1, key, true));

        DataWalker dw = new DataWalker(null) {
            @Override
            void perData(String foundKey, String foundData)
                throws DatabaseException {

                if (foundKey.equals("k1")) {
                    if (cursor.delete() == OperationStatus.SUCCESS) {
                        deletedEntries++;
                    }
                }
            }
        };
        dw.setIgnoreDataMap(true);
        dw.walkData();

        assertEquals(OperationStatus.SUCCESS,
                     putAndVerifyCursor(cursor, key, data1, true));
    }

    /**
     * Put a small number of data items into the database in a specific order,
     * delete all of them and then make sure they were really deleted.
     */
    @Test
    public void testSimpleDeleteAll()
        throws DatabaseException {

        initEnv(false);
        doSimpleCursorPuts();

        int deletedEntries = 0;
        DataWalker dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                try {
                    cursor.delete();
                    deletedEntries++;
                    assertEquals(OperationStatus.KEYEMPTY,
                                 cursor.getCurrent
                                 (new StringDbt(), new StringDbt(),
                                  LockMode.DEFAULT));
                } catch (DatabaseException DBE) {
                    System.out.println("DBE " + DBE);
                }
            }
        };
        dw.walkData();
        deletedEntries = dw.deletedEntries;
        dw = new DataWalker(simpleDataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                fail("didn't delete everything");
            }
        };
        dw.walkData();
        assertTrue(dw.nEntries == 0);
        assertTrue(simpleKeyStrings.length == deletedEntries);
    }

    /**
     * Insert N_KEYS data items into a tree.  Iterate through the tree in
     * ascending order deleting anything that has 'F' as the second character.
     * Iterate through the tree again and make sure they are all correctly
     * deleted.
     * @throws Exception 
     */
    @Test
    public void testLargeDelete()
        throws Exception {

        tearDown();
        for (int i = 0; i < N_ITERS; i++) {
            setUp();
            initEnv(false);
            doLargeDelete();
            tearDown();
        }
    }

    /**
     * Helper routine for above.
     */
    private void doLargeDeleteAll()
        throws DatabaseException {

        Hashtable dataMap = new Hashtable();
        int n_keys = 2000;
        doLargePut(dataMap, /* N_KEYS */ n_keys);

        int deletedEntries = 0;
        DataWalker dw = new DataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData)
                throws DatabaseException {
                cursor.delete();
                deletedEntries++;
                assertEquals(OperationStatus.KEYEMPTY,
                             cursor.getCurrent
                             (new StringDbt(), new StringDbt(),
                              LockMode.DEFAULT));
            }
        };
        dw.walkData();
        deletedEntries = dw.deletedEntries;
        dw = new DataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                fail("didn't delete everything");
            }
        };
        dw.walkData();
        assertTrue(dw.nEntries == 0);
        assertTrue(/* N_KEYS */ n_keys == deletedEntries);
    }

    /**
     * Insert N_KEYS data items into a tree.  Iterate through the tree in
     * ascending order deleting all entries.  Iterate through the tree again
     * and make sure they are all correctly deleted.
     * @throws Exception 
     */
    @Test
    public void testLargeDeleteAll()
        throws Exception {

        tearDown();
        for (int i = 0; i < N_ITERS; i++) {
            setUp();
            initEnv(false);
            doLargeDeleteAll();
            tearDown();
        }
    }

    /**
     * Helper routine for above.
     */
    private void doLargeDelete()
        throws DatabaseException {

        Hashtable dataMap = new Hashtable();
        doLargePut(dataMap, N_KEYS);

        int deletedEntries = 0;
        DataWalker dw = new DataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData)
                throws DatabaseException {
                if (foundKey.charAt(1) == 'F') {
                    cursor.delete();
                    deletedEntries++;
                }
            }
        };
        dw.walkData();
        deletedEntries = dw.deletedEntries;
        dw = new DataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                assertTrue(foundKey.compareTo(prevKey) >= 0);
                assertTrue(foundKey.charAt(1) != 'F');
                prevKey = foundKey;
            }
        };
        dw.walkData();
        assertTrue(dw.nEntries == N_KEYS - deletedEntries);
    }

    /**
     * Insert N_KEYS data items into a tree.  Iterate through the tree in
     * ascending order deleting the first entry.  Iterate through the tree
     * again and make sure only the first entry is deleted.
     * @throws Exception 
     */
    @Test
    public void testLargeDeleteFirst()
        throws Exception {

        tearDown();
        for (int i = 0; i < N_ITERS; i++) {
            setUp();
            initEnv(false);
            doLargeDeleteFirst();
            tearDown();
        }
    }

    /**
     * Helper routine for above.
     */
    private void doLargeDeleteFirst()
        throws DatabaseException {

        Hashtable dataMap = new Hashtable();
        doLargePut(dataMap, N_KEYS);

        DataWalker dw = new DataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData)
                throws DatabaseException {
                if (deletedEntry == null) {
                    deletedEntry = foundKey;
                    cursor.delete();
                }
            }
        };
        dw.walkData();

        String deletedEntry = dw.deletedEntry;

        dw = new DataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                assertFalse(deletedEntry.equals(foundKey));
            }
        };
        dw.deletedEntry = deletedEntry;
        dw.walkData();
        assertTrue(dw.nEntries == N_KEYS - 1);
    }

    /**
     * Insert N_KEYS data items into a tree.  Iterate through the tree in
     * ascending order deleting the last entry.  Iterate through the tree again
     * and make sure only the last entry is deleted.
     * @throws Exception 
     */
    @Test
    public void testLargeDeleteLast()
        throws Exception {

        tearDown();
        for (int i = 0; i < N_ITERS; i++) {
            setUp();
            initEnv(false);
            doLargeDeleteLast();
            tearDown();
        }
    }

    /**
     * Helper routine for above.
     */
    private void doLargeDeleteLast()
        throws DatabaseException {

        Hashtable dataMap = new Hashtable();
        doLargePut(dataMap, N_KEYS);

        DataWalker dw = new BackwardsDataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData)
                throws DatabaseException {
                if (deletedEntry == null) {
                    deletedEntry = foundKey;
                    cursor.delete();
                }
            }
        };
        dw.walkData();

        String deletedEntry = dw.deletedEntry;

        dw = new BackwardsDataWalker(dataMap) {
            @Override
            void perData(String foundKey, String foundData) {
                assertFalse(deletedEntry.equals(foundKey));
            }
        };
        dw.deletedEntry = deletedEntry;
        dw.walkData();
        assertTrue(dw.nEntries == N_KEYS - 1);
    }
}
