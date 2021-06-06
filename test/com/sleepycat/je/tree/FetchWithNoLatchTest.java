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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
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
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.TestHook;

public class FetchWithNoLatchTest extends TestBase {
    static private final boolean DEBUG = false;

    private final File envHome;
    private Environment env;
    private Database db1;
    private Database db2;
    private Database db3;

    public FetchWithNoLatchTest() {
        envHome = SharedTestUtils.getTestDir();

        Key.DUMP_TYPE = DumpType.BINARY;
    }

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        initEnv();
    }

    @After
    public void tearDown() {
        
        try {
            db1.close();
            db2.close();
            db3.close();
            env.close();
        } catch (DatabaseException E) {
        }
    }

    private void initEnv()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "4");
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

        env = new Environment(envHome, envConfig);

        String databaseName = "testDb1";
        Transaction txn = env.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db1 = env.openDatabase(txn, databaseName, dbConfig);
        txn.commit();

        databaseName = "testDb2";
        txn = env.beginTransaction(null, null);
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db2 = env.openDatabase(txn, databaseName, dbConfig);
        txn.commit();

        databaseName = "testDb3";
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db3 = env.openDatabase(null, databaseName, dbConfig);
    }

    @Test
    public void testSplit1()
        throws Exception {

        Key.DUMP_TYPE = DumpType.BINARY;
        final Database db = db1;

        try {
            /* Create the initial tree */
            Tree tree = createTree(db);

            /* Evict the BIN B containing the key 20 (BIN 14) */
            final Pair<IN, IN> pair = evictBIN(db, 20);
            final IN parent = pair.first();
            final IN bin = pair.second();

            /*
             * Refetch B (via getParentINForChildIN() below) while splitting
             * its sibling and parent (BINs 11 and 13) at the "same" time. The
             * split is done via the test hook below, which is executed right
             * after IN.fetchIN() releases the latch on B's parent in order to
             * fetch B. The split causes B's parent to change.
             */
            FetchINTestHook fetchINHook = new FetchINTestHook(
                db, parent, 66, true, false);

            parent.setFetchINHook(fetchINHook);
            tree.setFetchINHook(fetchINHook);

            bin.latch();
            SearchResult result = tree.getParentINForChildIN(
                bin, false, /*useTargetLevel*/ true, /*doFetch*/
                CacheMode.UNCHANGED);

            result.parent.releaseLatch();

            /* Make sure everything went fine. */
            assertTrue(fetchINHook.foundNullChild);
            assertTrue(result.exactParentFound);
            assertTrue(parent != result.parent);
            assertTrue(result.parent.getNodeId() > parent.getNodeId());

        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }


    @Test
    public void testSplit2()
        throws Exception {

        Key.DUMP_TYPE = DumpType.BINARY;
        final Database db = db2;

        try {
            /* Create the initial tree */
            Tree tree = createTree(db);

            /* Evict the BIN B containing the key 20 (BIN 14) */
            final Pair<IN, IN> pair = evictBIN(db, 30);
            final IN parent = pair.first();
            final IN bin = pair.second();

            /*
             * Refetch B (via getParentINForChildIN() below) while splitting B
             * itself and its parent (BIN 13) at the "same" time. The split is
             * done via the FetchINTestHook, which is executed right after
             * IN.fetchIN() releases the latch on B's parent in order to fetch
             * B. The split does not change B's parent. However, the version
             * B that fetchIN() is fetching becomes obsolete as a result of
             * the split, and should not be attached to the tree. This is 
             * indeed avaoided because fetchIN() will find B's new version
             * in the tree, and will return that one instead of the one just
             * fetched from disk.
             */
            FetchINTestHook fetchINHook = new FetchINTestHook(
                 db, parent, 33, true, false);

            parent.setFetchINHook(fetchINHook);
            tree.setFetchINHook(fetchINHook);

            bin.latch();
            SearchResult result = tree.getParentINForChildIN(
                bin, false, /*useTargetLevel*/ true, /*doFetch*/
                CacheMode.UNCHANGED);

            /* Make sure everything went fine. */
            assertTrue(fetchINHook.foundNullChild);
            assertTrue(result.exactParentFound);
            assertTrue(parent != result.parent);

            result.parent.releaseLatch();

            rangeSearch(db);

        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }


    @Test
    public void testSplit3()
        throws Exception {

        Key.DUMP_TYPE = DumpType.BINARY;
        final Database db = db3;

        try {
            /* Create the initial tree */
            Tree tree = createTree(db);

            /* Evict the BIN B containing the key 50 (BIN 11) */
            final Pair<IN, IN> pair = evictBIN(db, 50);
            final IN parent = pair.first();
            final IN bin = pair.second();

            /*
             * Refetch B (via getParentINForChildIN() below) while splitting B
             * itself and its parent (BIN 13) at the "same" time. The split is
             * done via the FetchINTestHook, which is executed right after
             * IN.fetchIN() releases the latch on B's parent in order to fetch
             * B. The split does not change B's parent. However, the version
             * B that fetchIN() is fetching becomes obsolete as a result of
             * the split, and should not be attached to the tree. In this test,
             * after B is split, we evict B before resuming with fetchIN().
             * fetchIN() will not attach the fetched (obsolete) version in the
             * tree because the parent slot has an LSN that is different than
             * the fetched LSN.
             */
            FetchINTestHook fetchINHook = new FetchINTestHook(
                db, parent, 66, true, true);

            parent.setFetchINHook(fetchINHook);
            tree.setFetchINHook(fetchINHook);

            bin.latch();
            SearchResult result = tree.getParentINForChildIN(
                bin, false, /*useTargetLevel*/ true, /*doFetch*/
                CacheMode.UNCHANGED);

            result.parent.releaseLatch();

            /* Make sure everything went fine. */
            assertTrue(fetchINHook.foundNullChild);
            assertTrue(result.exactParentFound);
            assertTrue(parent == result.parent);

            rangeSearch(db);

        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }

    @Test
    public void testSplit4()
        throws Exception {

        Key.DUMP_TYPE = DumpType.BINARY;
        final Database db = db2;

        try {
            /* Create the initial tree */
            Tree tree = createTree(db);

            /* Evict the BIN B containing the key 120 (BIN 9) */
            final Pair<IN, IN> pair = evictBIN(db, 120);
            final IN parent = pair.first();

            /*
             * Execute search(110) while splitting B (but not its parent) at
             * the "same" time. The split is done via the FetchINTestHook,
             * which is executed right after IN.fetchIN() (called from
             * search(110)) releases the latch on B's parent in order to
             * fetch B. The split does not change B's parent. However,
             * the key we are looking for is now in B's new sibling and
             * fetchIN() should return that sibling without causing a
             * restart of the search.
             */
            FetchINTestHook fetchINHook = new FetchINTestHook(
                 db, parent, 126, false, false);

            parent.setFetchINHook(fetchINHook);
            //parent.setIdentifierKey(new byte[]{(byte)40});

            Cursor cursor = db.openCursor(null, CursorConfig.DEFAULT);

            assertEquals(
                OperationStatus.SUCCESS,
                cursor.getSearchKey(
                    new DatabaseEntry(new byte[]{(byte)110}),
                    new DatabaseEntry(),
                    LockMode.DEFAULT));

            cursor.close();

        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }

    /*
     * A test hook assigned to a givan IN P and triggerred when P.fetchIN()
     * is called to fetch a missing child of P, after P is unlatched. The
     * doHook() method causes P to split by inserting a given key in one of
     * P's children.
     */
    class FetchINTestHook implements TestHook {

        Database db;
        IN parent;
        int newKey;
        boolean parentSplits;
        boolean evict;
        boolean foundNullChild = false;

        FetchINTestHook(
            Database db,
            IN parent,
            int newKey,
            boolean parentSplits,
            boolean evict) {

            this.db = db;
            this.parent = parent;
            this.newKey = newKey;
            this.parentSplits = parentSplits;
            this.evict = evict;
        }

        @Override
        public void doHook() {
            /* Only process the first call to the hook. */
            parent.setFetchINHook(null);

            int numEntries = parent.getNEntries();

            /* split the parent of the missing bin. */
            assertEquals(
                OperationStatus.SUCCESS,
                db.put(null,
                       new DatabaseEntry(new byte[]{(byte)newKey}),
                       new DatabaseEntry(new byte[] {1})));

            if (parentSplits) {
                assert(numEntries > parent.getNEntries());
            }

            if (evict) {
                evictBIN(db, newKey);
            }
        }

        @Override public void doHook(Object obj) {
            assertFalse(foundNullChild);
            foundNullChild = true;
        }

        @Override public void hookSetup() { }
        @Override public void doIOHook() { }
        
        @Override public Object getHookValue() { return foundNullChild; }
    };

    /*
     * Create a tree that looks like this:
     *
     *                                   ---------------------
     *                                   | nid: 12 - key: 70 |
     *                                   |...................|
     *                                   |    70    |   110  |
     *                                   ---------------------
     *                                        /          \
     *                                       /            \
     *                           ----------------------    .. Subtree shown
     *                           | nid: 13 - key: 70  |       below.
     *                           |....................|
     *                           |  40  |  50  |  80  |
     *                           ----------------------
     *                              /      |       \   80 <= k < 110
     *            ------------------       |        -----------------
     *            |                        |                        |
     *  ---------------------    ---------------------   ----------------------
     *  | nid: 14 - key: 40 |    | nid: 11 - key: 70 |   | nid: 10 - key: 100 |
     *  |...................|    |...................|   |....................|
     *  | 10 | 20 | 30 | 40 |    | 50 | 60 | 65 | 70 |   | 80 | 90 | 95 | 100 |
     *  ---------------------    ---------------------   ----------------------
     *
     *
     *            ---------------------- 
     *            | nid: 8 - key: 160  |
     *            |....................|
     *            |    110   |   140   |
     *            ----------------------
     *                /              \ 
     *               /                \
     *              /                  \
     *  -------------------------    ------------------------
     *  |   nid: 9 - key: 130   |    |   nid: 7 - key: 160  |
     *  |.......................|    |......................|
     *  | 110 | 120 | 125 | 130 |    | 140  |  150  |  160  |
     *  -------------------------    ------------------------
     */
    Tree createTree(Database db) {

        for (int i = 160; i > 0; i-= 10) {
            assertEquals(
                OperationStatus.SUCCESS,
                db.put(null,
                       new DatabaseEntry(new byte[] { (byte) i }),
                       new DatabaseEntry(new byte[] {1})));
        }

        assertEquals(
            OperationStatus.SUCCESS,
            db.put(null,
                   new DatabaseEntry(new byte[]{(byte)65}),
                   new DatabaseEntry(new byte[] {1})));

        assertEquals(
            OperationStatus.SUCCESS,
            db.put(null,
                   new DatabaseEntry(new byte[]{(byte)95}),
                   new DatabaseEntry(new byte[] {1})));

       assertEquals(
            OperationStatus.SUCCESS,
            db.put(null,
                   new DatabaseEntry(new byte[]{(byte)125}),
                   new DatabaseEntry(new byte[] {1})));

        Tree tree = DbInternal.getDbImpl(db).getTree();

        if (DEBUG) {
            System.out.println("<dump>");
            tree.dump();
        }

        return tree;
    }

    /*
     * Evict the BIN containing the given key and return the BIN and its parent.
     */
    Pair<IN, IN> evictBIN(Database db, int keyVal) {

        /* EVICT_BIN will not evict a dirty BIN. */
        env.sync();

        Tree tree = DbInternal.getDbImpl(db).getTree();

        Cursor cursor = db.openCursor(null, CursorConfig.DEFAULT);
        cursor.setCacheMode(CacheMode.EVICT_BIN);

        CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        DatabaseEntry key = new DatabaseEntry(new byte[] { (byte) keyVal });
        DatabaseEntry data = new DatabaseEntry();
        assertEquals(
            OperationStatus.SUCCESS,
            cursor.getSearchKey(key, data, LockMode.DEFAULT));

        IN bin = cursorImpl.getBIN();
        bin.latch();

        SearchResult result = tree.getParentINForChildIN(
            bin, false, /*useTargetLevel*/ true, /*doFetch*/
            CacheMode.UNCHANGED);

        assertTrue(result.exactParentFound);

        final IN parent = result.parent;
        parent.releaseLatch();

        /* evict the BIN */
        cursor.close();

        return new Pair<>(parent, bin);
    }

    /*
     * Do a range search for keys in [10, 100]
     */
    void rangeSearch(Database db) {

        Cursor cursor = db.openCursor(null, CursorConfig.DEFAULT);
        DatabaseEntry key = new DatabaseEntry(new byte[] { (byte) 10 });
        DatabaseEntry data = new DatabaseEntry();
        assertEquals(
            OperationStatus.SUCCESS,
            cursor.getSearchKeyRange(key, data, LockMode.DEFAULT));

        OperationStatus status = OperationStatus.SUCCESS;
        int keyVal = 0;
        int numKeys = 0;
        do {
            status = cursor.getNext(key, data, LockMode.DEFAULT);

            keyVal = (int)(key.getData()[0]);
            if (keyVal > 100) {
                break;
            }

            ++numKeys;
            System.out.println(keyVal);
        } while (status == OperationStatus.SUCCESS);
        
        cursor.close();

        assertEquals(numKeys, 12);
    }
}
