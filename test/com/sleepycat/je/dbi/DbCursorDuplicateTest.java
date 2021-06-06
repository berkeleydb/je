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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Hashtable;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PartialComparator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.utilint.StringUtils;

import org.junit.Test;

/**
 * Various unit tests for CursorImpl using duplicates.
 */
public class DbCursorDuplicateTest extends DbCursorTestBase {

    public DbCursorDuplicateTest() {
        super();
    }

    /**
     * Rudimentary insert/retrieve test.  Walk over the results forwards.
     */
    @Test
    public void testDuplicateCreationForward()
        throws Throwable {

        initEnv(true);
        try {
            doDuplicateTest(true, false);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Same as testDuplicateCreationForward except uses keylast.
     */
    @Test
    public void testDuplicateCreationForwardKeyLast()
        throws Throwable {

        initEnv(true);
        try {
            doDuplicateTest(true, true);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Rudimentary insert/retrieve test.  Walk over the results backwards.
     */
    @Test
    public void testDuplicateCreationBackwards()
        throws Throwable {

        initEnv(true);
        try {
            doDuplicateTest(false, false);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Insert N_KEYS data items into a tree.  Set a btreeComparison function.
     * Iterate through the tree in ascending order.  Ensure that the elements
     * are returned in ascending order.
     */
    @Test
    public void testLargeGetForwardTraverseWithNormalComparisonFunction()
        throws Throwable {

        try {
            tearDown();
            duplicateComparisonFunction = duplicateComparator;
            setUp();
            initEnv(true);
            doDuplicateTest(true, false);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Insert N_KEYS data items into a tree.  Set a reverse order
     * btreeComparison function. Iterate through the tree in ascending order.
     * Ensure that the elements are returned in ascending order.
     */
    @Test
    public void testLargeGetForwardTraverseWithReverseComparisonFunction()
        throws Throwable {

        try {
            tearDown();
            duplicateComparisonFunction = reverseDuplicateComparator;
            setUp();
            initEnv(true);
            doDuplicateTest(false, false);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Put a bunch of data items into the database in a specific order and
     * ensure that when read back that we can't putNoDupData without receiving
     * an error return code.
     */
    @Test
    public void testPutNoDupData()
        throws Throwable {

        try {
            initEnv(true);
            createRandomDuplicateData(null, false);

            DataWalker dw = new DataWalker(simpleDataMap) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        assertEquals
                            (OperationStatus.KEYEXIST,
                             cursor.putNoDupData(new StringDbt(foundKey),
                                                 new StringDbt(foundData)));
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testPutNoDupData2()
        throws Throwable {

        try {
            initEnv(true);
            for (int i = 0; i < simpleKeyStrings.length; i++) {
                OperationStatus status =
                    cursor.putNoDupData(new StringDbt("oneKey"),
                                        new StringDbt(simpleDataStrings[i]));
                assertEquals(OperationStatus.SUCCESS, status);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testAbortDuplicateTreeCreation()
        throws Throwable {

        try {
            initEnvTransactional(true);
            Transaction txn = exampleEnv.beginTransaction(null, null);
            Cursor c = exampleDb.openCursor(txn, null);
            OperationStatus status =
                c.put(new StringDbt("oneKey"),
                      new StringDbt("firstData"));
            assertEquals(OperationStatus.SUCCESS, status);
            c.close();
            txn.commit();
            txn = exampleEnv.beginTransaction(null, null);
            c = exampleDb.openCursor(txn, null);
            status =
                c.put(new StringDbt("oneKey"),
                      new StringDbt("secondData"));
            assertEquals(OperationStatus.SUCCESS, status);
            c.close();
            txn.abort();
            txn = exampleEnv.beginTransaction(null, null);
            c = exampleDb.openCursor(txn, null);
            DatabaseEntry keyRet = new DatabaseEntry();
            DatabaseEntry dataRet = new DatabaseEntry();
            assertEquals(OperationStatus.SUCCESS,
                         c.getFirst(keyRet, dataRet, LockMode.DEFAULT));
            assertEquals(1, c.count());
            assertEquals(OperationStatus.NOTFOUND,
                         c.getNext(keyRet, dataRet, LockMode.DEFAULT));
            c.close();
            txn.commit();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Create the usual random duplicate data.  Iterate back over it calling
     * count at each element.  Make sure the number of duplicates returned for
     * a particular key is N_DUPLICATE_PER_KEY.  Note that this is somewhat
     * inefficient, but cautious, in that it calls count for every duplicate
     * returned, rather than just once for each unique key returned.
     */
    @Test
    public void testDuplicateCount()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();

            createRandomDuplicateData(N_COUNT_TOP_KEYS,
                                      N_COUNT_DUPLICATES_PER_KEY,
                                      dataMap, false, true);

            DataWalker dw = new DataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        assertEquals(N_COUNT_DUPLICATES_PER_KEY,
                                     cursor.count());
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertEquals(N_COUNT_DUPLICATES_PER_KEY, dw.nEntries);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateDuplicates()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();

            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            assertTrue(cursor.put(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            dataString = "d2d2";
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            assertTrue(cursor.put(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            DataWalker dw = new DataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dw.nEntries == 2);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateDuplicatesWithComparators() //cwl
        throws Throwable {

        try {
            tearDown();
            duplicateComparisonFunction = invocationCountingComparator;
            btreeComparisonFunction = invocationCountingComparator;
            invocationCountingComparator.setInvocationCount(0);
            setUp();
            runBtreeVerifier = false;
            initEnv(true);

            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.put(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.put(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);

            InvocationCountingBtreeComparator bTreeICC =
                (InvocationCountingBtreeComparator)
                (exampleDb.getConfig().getBtreeComparator());

            InvocationCountingBtreeComparator dupICC =
                (InvocationCountingBtreeComparator)
                (exampleDb.getConfig().getDuplicateComparator());

            /*
             * Key and data are combined internally, so both comparators are
             * called twice.
             */
            assertEquals(2, bTreeICC.getInvocationCount());
            assertEquals(2, dupICC.getInvocationCount());
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateReplacement()
        throws Throwable {

        try {
            initEnv(true);
            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            dataString = "d2d2";
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            DataWalker dw = new DataWalker(null) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        StringDbt dataDbt = new StringDbt();
                        dataDbt.setString(foundData);
                        assertEquals(OperationStatus.SUCCESS,
                                     cursor.putCurrent(dataDbt));
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dw.nEntries == 2);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateReplacementFailure()
        throws Throwable {

        try {
            initEnv(true);
            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            dataString = "d2d2";
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            DataWalker dw = new DataWalker(null) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        StringDbt dataDbt = new StringDbt();
                        dataDbt.setString("blort");
                        try {
                            cursor.putCurrent(dataDbt);
                            fail("didn't catch DatabaseException");
                        } catch (DatabaseException DBE) {
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dw.nEntries == 2);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testDuplicateReplacementFailure1Dup()
        throws Throwable {

        try {
            initEnv(true);
            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            DataWalker dw = new DataWalker(null) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        StringDbt dataDbt = new StringDbt();
                        dataDbt.setString("blort");
                        try {
                            cursor.putCurrent(dataDbt);
                            fail("didn't catch DatabaseException");
                        } catch (DatabaseException DBE) {
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dw.nEntries == 1);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * When using a duplicate comparator that does not compare all bytes,
     * attempting to change the data for a duplicate data item should work when
     * a byte not compared is changed. [#15527] [#15704]
     */
    @Test
    public void testDuplicateReplacementFailureWithComparisonFunction1()
        throws Throwable {

        try {
            tearDown();
            duplicateComparisonFunction = truncatedComparator;
            setUp();
            initEnv(true);
            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            dataString = "d2d2";
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            DataWalker dw = new DataWalker(null) {
                    @Override
                    void perData(String foundKey, String foundData)
                        throws DatabaseException {

                        StringDbt dataDbt = new StringDbt();
                        StringBuilder sb = new StringBuilder(foundData);
                        sb.replace(3, 4, "3");
                        dataDbt.setString(sb.toString());
                        try {
                            cursor.putCurrent(dataDbt);
                        } catch (DatabaseException e) {
                            fail(e.toString());
                        }
                        StringDbt keyDbt = new StringDbt();
                        assertSame(OperationStatus.SUCCESS,
                                   cursor.getCurrent(keyDbt, dataDbt, null));
                        assertEquals(foundKey, keyDbt.getString());
                        assertEquals(sb.toString(), dataDbt.getString());
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * When using a duplicate comparator that compares all bytes, attempting to
     * change the data for a duplicate data item should cause an error.
     * [#15527]
     */
    @Test
    public void testDuplicateReplacementFailureWithComparisonFunction2()
        throws Throwable {

        try {
            tearDown();
            duplicateComparisonFunction = truncatedComparator;
            setUp();
            initEnv(true);

            String keyString = "aaaa";
            String dataString = "d1d1";
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            keyDbt.setData(StringUtils.toUTF8(keyString));
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            dataString = "d2d2";
            dataDbt.setData(StringUtils.toUTF8(dataString));
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) ==
                       OperationStatus.SUCCESS);
            assertTrue(cursor.putNoDupData(keyDbt, dataDbt) !=
                       OperationStatus.SUCCESS);
            DataWalker dw = new DataWalker(null) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        StringDbt dataDbt = new StringDbt();
                        StringBuilder sb = new StringBuilder(foundData);
                        sb.replace(2, 2, "3");
                        sb.setLength(4);
                        dataDbt.setString(sb.toString());
                        try {
                            cursor.putCurrent(dataDbt);
                            fail("didn't catch DatabaseException");
                        } catch (DatabaseException DBE) {
                        }
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertTrue(dw.nEntries == 2);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void doDuplicateTest(boolean forward, boolean useKeyLast)
        throws Throwable {

        Hashtable dataMap = new Hashtable();
        createRandomDuplicateData(dataMap, useKeyLast);

        DataWalker dw;
        if (forward) {
            dw = new DataWalker(dataMap) {
                    @Override
            void perData(String foundKey, String foundData) {
                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " + foundKey + "/" +
                                 foundData);
                        }

                        if (ht.get(foundData) != null) {
                            ht.remove(foundData);
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }
                        } else {
                            fail("didn't find " + foundKey + "/" + foundData);
                        }

                        assertTrue(foundKey.compareTo(prevKey) >= 0);

                        if (prevKey.equals(foundKey)) {
                            if (duplicateComparisonFunction == null) {
                                assertTrue(foundData.compareTo(prevData) >= 0);
                            } else {
                                assertTrue
                                    (duplicateComparisonFunction.compare
                                     (StringUtils.toUTF8(foundData),
                                      StringUtils.toUTF8(prevData)) >= 0);
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                    }
                };
        } else {
            dw = new BackwardsDataWalker(dataMap) {
                    @Override
            void perData(String foundKey, String foundData) {
                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " + foundKey + "/" +
                                 foundData);
                        }

                        if (ht.get(foundData) != null) {
                            ht.remove(foundData);
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }
                        } else {
                            fail("didn't find " + foundKey + "/" + foundData);
                        }

                        if (!prevKey.equals("")) {
                            assertTrue(foundKey.compareTo(prevKey) <= 0);
                        }

                        if (prevKey.equals(foundKey)) {
                            if (!prevData.equals("")) {
                                if (duplicateComparisonFunction == null) {
                                    assertTrue
                                        (foundData.compareTo(prevData) <= 0);
                                } else {
                                    assertTrue
                                        (duplicateComparisonFunction.compare
                                         (StringUtils.toUTF8(foundData),
                                          StringUtils.toUTF8(prevData)) <= 0);
                                }
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                    }
                };
        }
        dw.setIgnoreDataMap(true);
        dw.walkData();
        assertTrue(dataMap.size() == 0);
    }

    /**
     * Create a bunch of random duplicate data.  Iterate over it using
     * getNextDup until the end of the dup set.  At end of set, handleEndOfSet
     * is called to do a getNext onto the next dup set.  Verify that ascending
     * order is maintained and that we reach end of set the proper number of
     * times.
     */
    @Test
    public void testGetNextDup()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();

            createRandomDuplicateData(dataMap, false);

            DataWalker dw = new DupDataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        if (ht.get(foundData) != null) {
                            ht.remove(foundData);
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }
                        } else {
                            fail("didn't find " + foundKey + "/" + foundData);
                        }

                        assertTrue(foundKey.compareTo(prevKey) >= 0);

                        if (prevKey.equals(foundKey)) {
                            if (duplicateComparisonFunction == null) {
                                assertTrue(foundData.compareTo(prevData) >= 0);
                            } else {
                                assertTrue
                                    (duplicateComparisonFunction.compare
                                     (StringUtils.toUTF8(foundData),
                                      StringUtils.toUTF8(prevData)) >= 0);
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                    }

                    @Override
                    OperationStatus handleEndOfSet(OperationStatus status)
                        throws DatabaseException {

                        String foundKeyString = foundKey.getString();
                        Hashtable ht = (Hashtable) dataMap.get(foundKeyString);
                        assertNull(ht);
                        return cursor.getNext(foundKey, foundData,
                                              LockMode.DEFAULT);
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertEquals(N_TOP_LEVEL_KEYS, dw.nHandleEndOfSet);
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Create a bunch of random duplicate data.  Iterate over it using
     * getNextDup until the end of the dup set.  At end of set, handleEndOfSet
     * is called to do a getNext onto the next dup set.  Verify that descending
     * order is maintained and that we reach end of set the proper number of
     * times.
     */
    @Test
    public void testGetPrevDup()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();

            createRandomDuplicateData(dataMap, false);

            DataWalker dw = new BackwardsDupDataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        if (ht.get(foundData) != null) {
                            ht.remove(foundData);
                            if (ht.size() == 0) {
                                dataMap.remove(foundKey);
                            }
                        } else {
                            fail("didn't find " + foundKey + "/" + foundData);
                        }

                        if (!prevKey.equals("")) {
                            assertTrue(foundKey.compareTo(prevKey) <= 0);
                        }

                        if (prevKey.equals(foundKey)) {
                            if (!prevData.equals("")) {
                                if (duplicateComparisonFunction == null) {
                                    assertTrue(foundData.compareTo
                                               (prevData) <= 0);
                                } else {
                                    assertTrue
                                        (duplicateComparisonFunction.compare
                                         (StringUtils.toUTF8(foundData),
                                          StringUtils.toUTF8(prevData)) <= 0);
                                }
                            }
                            prevData = foundData;
                        } else {
                            prevData = "";
                        }

                        prevKey = foundKey;
                    }

                    @Override
                    OperationStatus handleEndOfSet(OperationStatus status)
                        throws DatabaseException {

                        String foundKeyString = foundKey.getString();
                        Hashtable ht = (Hashtable) dataMap.get(foundKeyString);
                        assertNull(ht);
                        return cursor.getPrev(foundKey, foundData,
                                              LockMode.DEFAULT);
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertEquals(N_TOP_LEVEL_KEYS, dw.nHandleEndOfSet);
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Create a bunch of random duplicate data.  Iterate over it using
     * getNextNoDup until the end of the top level set.  Verify that
     * ascending order is maintained and that we reach see the proper
     * number of top-level keys.
     */
    @Test
    public void testGetNextNoDup()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();

            createRandomDuplicateData(dataMap, false);

            DataWalker dw = new NoDupDataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        if (ht.get(foundData) != null) {
                            dataMap.remove(foundKey);
                        } else {
                            fail("saw " +
                                 foundKey + "/" + foundData + " twice.");
                        }

                        assertTrue(foundKey.compareTo(prevKey) > 0);
                        prevKey = foundKey;
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertEquals(N_TOP_LEVEL_KEYS, dw.nEntries);
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Create a bunch of random duplicate data.  Iterate over it using
     * getNextNoDup until the end of the top level set.  Verify that descending
     * order is maintained and that we reach see the proper number of top-level
     * keys.
     */
    @Test
    public void testGetPrevNoDup()
        throws Throwable {

        try {
            initEnv(true);
            Hashtable dataMap = new Hashtable();

            createRandomDuplicateData(dataMap, false);

            DataWalker dw = new NoDupBackwardsDataWalker(dataMap) {
                    @Override
                    void perData(String foundKey, String foundData) {
                        Hashtable ht = (Hashtable) dataMap.get(foundKey);
                        if (ht == null) {
                            fail("didn't find ht " +
                                 foundKey + "/" + foundData);
                        }

                        if (ht.get(foundData) != null) {
                            dataMap.remove(foundKey);
                        } else {
                            fail("saw " +
                                 foundKey + "/" + foundData + " twice.");
                        }

                        if (!prevKey.equals("")) {
                            assertTrue(foundKey.compareTo(prevKey) < 0);
                        }
                        prevKey = foundKey;
                    }
                };
            dw.setIgnoreDataMap(true);
            dw.walkData();
            assertEquals(N_TOP_LEVEL_KEYS, dw.nEntries);
            assertTrue(dataMap.size() == 0);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test
    public void testIllegalDuplicateCreation()
        throws Throwable {

        try {
            initEnv(false);
            Hashtable dataMap = new Hashtable();

            try {
                createRandomDuplicateData(dataMap, false);
                fail("didn't throw DuplicateEntryException");
            } catch (DuplicateEntryException DEE) {
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Just use the BtreeComparator that's already available.
     */
    private static Comparator duplicateComparator =
        new DuplicateAscendingComparator();

    private static Comparator reverseDuplicateComparator =
        new DuplicateReverseComparator();

    private static InvocationCountingBtreeComparator
        invocationCountingComparator =
        new InvocationCountingBtreeComparator();

    @SuppressWarnings("serial")
    public static class DuplicateAscendingComparator
        extends BtreeComparator {

        public DuplicateAscendingComparator() {
            super();
        }
    }

    @SuppressWarnings("serial")
    public static class DuplicateReverseComparator
        extends ReverseBtreeComparator {

        public DuplicateReverseComparator() {
            super();
        }
    }

    @SuppressWarnings("serial")
    public static class InvocationCountingBtreeComparator
        extends BtreeComparator {

        private int invocationCount = 0;

        @Override
        public int compare(Object o1, Object o2) {
            invocationCount++;
            return super.compare(o1, o2);
        }

        public int getInvocationCount() {
            return invocationCount;
        }

        public void setInvocationCount(int invocationCount) {
            this.invocationCount = invocationCount;
        }
    }

    /*
     * A special comparator that only looks at the first length-1 bytes of data
     * so that the last byte can be changed without affecting "equality".  Use
     * this for putCurrent tests of duplicates.
     */
    private static Comparator truncatedComparator = new TruncatedComparator();

    @SuppressWarnings("serial")
    private static class TruncatedComparator implements Comparator,
                                                        PartialComparator,
                                                        Serializable {
        protected TruncatedComparator() {
        }

        public int compare(Object o1, Object o2) {
            byte[] arg1;
            byte[] arg2;
            arg1 = (byte[]) o1;
            arg2 = (byte[]) o2;
            int a1Len = arg1.length - 1;
            int a2Len = arg2.length - 1;

            int limit = Math.min(a1Len, a2Len);

            for (int i = 0; i < limit; i++) {
                byte b1 = arg1[i];
                byte b2 = arg2[i];
                if (b1 == b2) {
                    continue;
                } else {
                    /*
                     * Remember, bytes are signed, so convert to
                     * shorts so that we effectively do an unsigned
                     * byte comparison.
                     */
                    short s1 = (short) (b1 & 0x7F);
                    short s2 = (short) (b2 & 0x7F);
                    if (b1 < 0) {
                        s1 |= 0x80;
                    }
                    if (b2 < 0) {
                        s2 |= 0x80;
                    }
                    return (s1 - s2);
                }
            }

            return (a1Len - a2Len);
        }
    }
}
