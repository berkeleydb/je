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

package com.sleepycat.je.incomp;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test that searches and cursor traversals execute correctly in the face of
 * a BIN with 0 entries, and with tree pruning at key points.
 */
@RunWith(Parameterized.class)
public class EmptyBINTest extends TestBase {
    private static final boolean DEBUG = false;

    private static final byte DEFAULT_VAL = 100;
    private final File envHome;
    private Environment env;
    private Database db;

    private final boolean useDups;
    private final boolean doPruningAtCursorLevel;
    private final boolean doPruningAtTreeLevel;

    /*
     * Run all tests in four combinations, using dups, and invoking bin
     * pruning.
     */
    @Parameters
    public static List<Object[]> genParams() {
        List<Object[]> list = new ArrayList<Object[]>();
        boolean[] combo = new boolean[] {true, false};
        
        for (boolean dup : combo) { 
            for (boolean pruneCursor : combo) {
                for (boolean pruneTree : combo) {
                    if (!(pruneCursor && pruneTree)) {
                        list.add(new Object[] {dup, pruneCursor, pruneTree});
                    }
                }
            }
        }
        return list;
    }
    
    public EmptyBINTest(boolean dup, boolean pruneCursor, boolean pruneTree) {
        this.useDups = dup;
        this.doPruningAtCursorLevel = pruneCursor;
        this.doPruningAtTreeLevel = pruneTree;
        if (DEBUG) {
            System.out.println("useDups=" + useDups +
                               " doPruningAtCursorLevel=" +
                               doPruningAtCursorLevel +
                               " doPruningAtTreeLevel=" +
                               doPruningAtTreeLevel);
        }
        this.envHome = SharedTestUtils.getTestDir();
        customName = (useDups ? "DUPS" : "!DUPS") +
                        "_" +
                     (doPruningAtCursorLevel ? "CURSORPRUNE" : "!CURSORPRUNE") 
                     + "_" +
                     (doPruningAtTreeLevel ? "TREEPRUNE" : "!TREEPRUNE");
    }

    @After
    public void tearDown()
        throws Exception {

        if (db != null) {
            try {
                db.close();
            } catch (DatabaseException ignore) {
            }
        }

        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException ignore) {
            }
        }
        env = null;
        db = null;
    }

    /* Non-dupes scans across an empty BIN. */
    @Test
    public void testScanFromEndOfFirstBin()
        throws DatabaseException {

        /*
         * Tree holds <0,1>  <2,3,4> <empty> <8,9,10>.
         *                        |
         *   fwd scan starts --- -+
         * Fwd scan starting at 4.  Expect 4, 8, 9, 10
         */
        doScanAcrossEmptyBin(true,      // forward
                             (byte) 4,  // start
                             new byte[] {4,8,9,10}); // expected
    }

    @Test
    public void testScanFromLeftSideOfEmptyBin()
        throws DatabaseException {

        /*
         * Tree holds <0,1>  <2,3,4> <empty> <8,9,10>.
         *                            |
         *   scan starts -------------+
         * Fwd scan starting at 5 (deleted).  Expect 8, 9, 10
         */
        doScanAcrossEmptyBin(true,       // forward
                             (byte) 5,   // start
                             new byte[] {8,9,10}); // expected
    }

    @Test
    public void testScanFromRightSideOfEmptyBin()
        throws DatabaseException {

        /*
         * Tree holds <0,1>  <2,3,4> <empty> <8,9,10>.
         *                                |
         *   backwards scan starts ------+
         * Backwards scan starting at 7 (deleted).  Expect 8,4,3,2,1,0
         */
        doScanAcrossEmptyBin(false,      // backwards
                             (byte) 7,   // start
                             new byte[] {8,4,3,2,1,0}); // expected
    }

    @Test
    public void testScanFromBeginningOfLastBin()
        throws DatabaseException {

        /*
         * Tree holds <0,1>  <2,3,4> <empty> <8,9,10>.
         *                                    |
         *   backwards scan starts -----------+
         */
        doScanAcrossEmptyBin(false,      // backwards
                             (byte) 8,   // start
                             new byte[] {8,4,3,2,1,0});  // expected vals
    }

    @Test
    public void testScanForward()
        throws DatabaseException {

        /*
         * Tree holds <0,1>  <2,3,4> <empty> <8,9,10>.
         * Fwd scan starting with first.  Expect 0, 1, 2, 4, 8, 9, 10.
         */
        doScanAcrossEmptyBin(true,    // forward
                             (byte) -1,
                             new byte[] {0,1,2,3,4,8,9,10});
    }

    @Test
    public void testScanBackwards()
        throws DatabaseException {

        /*
         * Tree holds <0,1>  <2,3,4> <empty> <8,9,10>.
         * Bwd scan starting with last. 10 -> 0
         */
        doScanAcrossEmptyBin(false,   // backwards
                             (byte) -1,
                             new byte[] {10,9,8,4,3,2,1,0});
    }

    /**
     * Scan over an empty BIN that is in the middle of the tree. [#11778]
     * The tree holds values from 0 - 10. Values 5, 6, 7 have been deleted.
     * @param forward indicates use getNext().
     * @param startKey >= 0 indicates do getSearchKeyRange to init cursor.
     * @param expectVals are the elements to expect find
     */
    private void doScanAcrossEmptyBin(boolean forward,
                                      byte startKey,
                                      byte[] expectVals)
        throws DatabaseException {

        int deleteStartVal = 5;
        int deleteEndVal = 7;
        openAndInitEmptyMiddleBIN(deleteStartVal, deleteEndVal);

        if (DEBUG) {
            DbInternal.getDbImpl(db).getTree().dump();
        }

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /*
         * Position a cursor and check that we get the expected values.
         */
        int cnt = 0;
        Cursor cursor = db.openCursor(null, null);
        CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        if (doPruningAtCursorLevel) {
            cursorImpl.setTestHook(new PruningHook(env));
        }

        if (doPruningAtTreeLevel) {
            DbInternal.getDbImpl(db).getTree().
                setSearchHook(new PruningHook(env));
        }

        int expectIndex = 0;
        if (startKey < 0) {
            if (forward) {
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getFirst(key, data, null));
            } else {
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getLast(key, data, null));
            }
        } else {
            if (useDups) {
                key.setData(new byte[] {DEFAULT_VAL});
                data.setData(new byte[] {startKey});
            } else {
                key.setData(new byte[] { startKey });
            }

            if ((startKey >= deleteStartVal) &&
                (startKey <= deleteEndVal)) {
                /* Test range query. */
                if (useDups) {
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor.getSearchBothRange(key, data, null));
                } else {
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor.getSearchKeyRange(key, data, null));
                }
            } else {
                /* Test from getSearchKey(). */
                if (useDups) {
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor.getSearchBoth(key, data, null));
                } else {
                    assertEquals(OperationStatus.SUCCESS,
                                 cursor.getSearchKey(key, data, null));
                }
            }
        }

        OperationStatus status;
        do {
            cnt++;

            /* check value. */
            if (DEBUG) {
                System.out.println("=>key=" + key.getData()[0] +
                                   " data=" + data.getData()[0]);
            }
            if (useDups) {
                assertEquals(expectVals[expectIndex++], data.getData()[0]);
            } else {
                assertEquals(expectVals[expectIndex++], key.getData()[0]);
            }

            if (forward) {
                status = cursor.getNext(key, data, null);
            } else {
                status = cursor.getPrev(key, data, null);
            }
        } while (status == OperationStatus.SUCCESS);

        assertEquals(expectVals.length, cnt);
        cursor.close();
        closeEnv();
    }

    /**
     * Create a tree with:
     *                         IN
     *                      /     \
     *                    IN       IN
     *                    / \     /   \
     *                BIN1 BIN2  BIN3 BIN4
     *
     * where BIN1 has values 0,1
     *       BIN2 has valus 2,3,4
     *       BIN3 has valus 5,6,7
     *       BIN4 has valus 8,9,10
     * Depending on configuration, the entries in BIN2 or BIN3
     */
    private void openAndInitEmptyMiddleBIN(int deleteStartVal,
                                           int deleteEndVal)
        throws DatabaseException {

        openEnv(false, "4");
        DatabaseEntry data = new DatabaseEntry();
        data.setData(new byte[] {DEFAULT_VAL});
        DatabaseEntry key = new DatabaseEntry();
        key.setData(new byte[] {DEFAULT_VAL});

        /* Create four BINs */
        OperationStatus status;
        for (int i = 0; i < 11; i++) {
            if (useDups) {
                data = new DatabaseEntry(new byte[] { (byte) i });
            } else {
                key = new DatabaseEntry(new byte[] { (byte) i });
            }
            status = db.put(null, key, data);
            assertEquals(OperationStatus.SUCCESS, status);
        }

        /* Empty out one of the middle ones. */
        if (useDups) {
            Cursor cursor = db.openCursor(null, null);
            data = new DatabaseEntry(new byte[] { (byte) deleteStartVal });
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchBoth(key, data, LockMode.DEFAULT));
            for (int i = deleteStartVal; i <= deleteEndVal; i++) {
                assertEquals(OperationStatus.SUCCESS,
                             cursor.delete());
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getNext(key, data, LockMode.DEFAULT));
            }
            cursor.close();
        } else {
            for (int i = deleteStartVal; i <= deleteEndVal; i++) {
                key = new DatabaseEntry(new byte[] { (byte) i });
                status = db.delete(null, key);
                assertEquals(OperationStatus.SUCCESS, status);
            }
        }

        CheckpointConfig config = new CheckpointConfig();
        config.setForce(true);
        env.checkpoint(config);
    }

    /**
     * Opens the environment and db.
     */
    private void openEnv(boolean transactional, String nodeMax)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(transactional);
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "true");
        if (nodeMax != null) {
            envConfig.setConfigParam
                (EnvironmentParams.NODE_MAX.getName(), nodeMax);
            envConfig.setConfigParam
                (EnvironmentParams.NODE_MAX_DUPTREE.getName(), nodeMax);
        }
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        /* Make a db and open it. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(transactional);
        dbConfig.setSortedDuplicates(useDups);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    /**
     * Closes the db and environment.
     */
    private void closeEnv()
        throws DatabaseException {

        db.close();
        db = null;
        env.close();
        env = null;
    }

    private static class PruningHook implements TestHook {
        Environment env;

        PruningHook(Environment env) {
            this.env = env;
        }

        public void doHook() {
            DbInternal.getNonNullEnvImpl(env).getINCompressor().
                wakeup();
            Thread.yield();
            try {
                Thread.sleep(100);
            } catch (Throwable T) {
            }
        }

        public Object getHookValue() {
            throw new UnsupportedOperationException();
        }

        public void doIOHook() {
            throw new UnsupportedOperationException();
        }

        public void hookSetup() {
            throw new UnsupportedOperationException();
        }

        public void doHook(Object obj) {
            throw new UnsupportedOperationException();            
        }
    }
}
