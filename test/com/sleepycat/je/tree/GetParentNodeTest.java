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
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

public class GetParentNodeTest extends TestBase {
    static private final boolean DEBUG = false;

    private final File envHome;
    private Environment env;
    private Database db;
    private IN rootIN;
    private IN firstLevel2IN;
    private BIN firstBIN;

    public GetParentNodeTest() {
        envHome = SharedTestUtils.getTestDir();
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
            db.close();
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
        env = new Environment(envHome, envConfig);

        String databaseName = "testDb";
        Transaction txn = env.beginTransaction(null, null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(txn, databaseName, dbConfig);
        txn.commit();
    }

    /**
     * Test getParentINForChildIN and GetParentBINForChildLN painstakingly on a
     * hand constructed tree.
     */
    @Test
    public void testBasic()
        throws Exception {

        try {
            /*
             * Make a tree w/3 levels in the main tree and a single dup
             * tree. The dupTree has two levels. The tree looks like this:
             *
             *            root(key=a)
             *             |
             *      +---------------------------+
             *    IN(key=a)                   IN(key=e)
             *     |                            |
             *  +------------------+       +--------+--------+
             * BIN(key=a)       BIN(c)    BIN(e)   BIN(g)  BIN(i)
             *   |  |             | |      | |       | |     | | |
             *  LNa,b           LNc,d    LNe,f     LNg,h   LNi,j,k
             */
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("a"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("b"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("c"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("d"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("e"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("f"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("g"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("h"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("i"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("j"),
                                new StringDbt("data1")));
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, new StringDbt("k"),
                                new StringDbt("data1")));

            /*
             * Test exact matches.
             */
            checkTreeUsingExistingNodes(true);
            checkTreeUsingExistingNodes(false);

            /* Test potential matches. */
            checkTreeUsingPotentialNodes();

            /* Should be no latches held. */
            assertEquals(0, LatchSupport.nBtreeLatchesHeld());
        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }

    private void checkTreeUsingExistingNodes(boolean requireExactMatch)
        throws DatabaseException {

        /* Start at the root. */
        DatabaseImpl database = DbInternal.getDbImpl(db);
        Tree tree = database.getTree();

        if (DEBUG) {
            tree.dump();
        }

        rootIN = tree.withRootLatchedShared
            (new GetRoot(DbInternal.getDbImpl(db)));
        assertEquals(rootIN.getNEntries(), 2);

        /* Second and third level. */
        firstBIN = null;
        for (int i = 0; i < rootIN.getNEntries(); i++) {
            /* Each level 2 IN. */
            IN in = (IN) rootIN.getTarget(i);

            if (i == 0) {
                firstLevel2IN = in;
            }
            checkMatch(tree, in, rootIN, i, requireExactMatch);

            /* For each BIN, find its parent, and then find its LNs. */
            for (int j = 0; j < in.getNEntries(); j++) {
                BIN bin = (BIN) in.getTarget(j);

                if (firstBIN == null) {
                    firstBIN = bin;
                }
                checkMatch(tree, bin, in, j, requireExactMatch);

                for (int k = 0; k < bin.getNEntries(); k++) {
                    checkMatch(tree, bin, bin.getKey(k), k, bin.getLsn(k));
                }
            }
        }
    }

    /*
     * Do a parent search, expect to find the parent, check that we do.
     */
    private void checkMatch(Tree tree,
                            IN target,
                            IN parent,
                            int index,
                            boolean requireExactMatch)
        throws DatabaseException {

        target.latch();

        long targetId = target.getNodeId();
        byte[] targetKey = target.getIdentifierKey();
        int targetLevel = -1;
        int exclusiveLevel = target.getLevel() + 1;

        target.releaseLatch();

        SearchResult result = tree.getParentINForChildIN(
            targetId, targetKey, targetLevel,
            exclusiveLevel, requireExactMatch, true,
            CacheMode.DEFAULT, null);

        assertTrue(result.exactParentFound);
        assertEquals("Target=" + target + " parent=" + parent,
                     index, result.index);
        assertEquals(parent, result.parent);
        parent.releaseLatch();
    }

    /*
     * Search for the BIN for this LN.
     */
    private void checkMatch(Tree tree,
                            BIN parent,
                            byte[] mainKey,
                            int index,
                            long expectedLsn)
        throws DatabaseException {
        TreeLocation location = new TreeLocation();

        assertTrue(tree.getParentBINForChildLN
                   (location, mainKey, false, false, CacheMode.DEFAULT));
        location.bin.releaseLatch();
        assertEquals(parent, location.bin);
        assertEquals(index, location.index);
        assertEquals(expectedLsn, location.childLsn);

        assertTrue(tree.getParentBINForChildLN
                   (location, mainKey, true, false, CacheMode.DEFAULT));
        location.bin.releaseLatch();
        assertEquals(parent, location.bin);
        assertEquals(index, location.index);
        assertEquals(expectedLsn, location.childLsn);

        assertTrue(tree.getParentBINForChildLN
                   (location, mainKey, true, false, CacheMode.DEFAULT));
        location.bin.releaseLatch();
        assertEquals(parent, location.bin);
        assertEquals(index, location.index);
        assertEquals(expectedLsn, location.childLsn);
    }

    private class GetRoot implements WithRootLatched {

        private final DatabaseImpl db;

        GetRoot(DatabaseImpl db) {
            this.db = db;
        }

        public IN doWork(ChildReference root)
            throws DatabaseException {

            return (IN) root.fetchTarget(db, null);
        }
    }

    /**
     * Make up non-existent nodes and see where they'd fit in. This exercises
     * recovery type processing and cleaning.
     */
    private void checkTreeUsingPotentialNodes()
        throws DatabaseException {

        DatabaseImpl database = DbInternal.getDbImpl(db);
        Tree tree = database.getTree();

        /*
         * Make an IN with the key "ab". Its potential parent should be the
         * first level 2 IN.
         */
        IN inAB = new IN(database, StringUtils.toUTF8("ab"), 4, 2);
        checkPotential(tree, inAB, firstLevel2IN);

        /*
         * Make an BIN with the key "x". Its potential parent should be the
         * first level 2 IN.
         */
        BIN binAB =
            new BIN(database, StringUtils.toUTF8("ab"), 4, 1);
        checkPotential(tree, binAB, firstLevel2IN);

        /*
         * Make an LN with the key "ab". It's potential parent should be the
         * BINa.
         */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        byte[] mainKey = StringUtils.toUTF8("ab");
        checkPotential(tree, firstBIN, mainKey, mainKey);
    }

    private void checkPotential(Tree tree, IN potential, IN expectedParent)
        throws DatabaseException {

        /* Try an exact match, expect a failure, then try an inexact match. */
        potential.latch();
        SearchResult result = tree.getParentINForChildIN(
            potential, false, /*useTargetLevel*/
            true, /*dofetch*/ CacheMode.DEFAULT);

        assertFalse(result.exactParentFound);
        assertTrue(result.parent == null);

        potential.latch();

        long targetId = potential.getNodeId();
        byte[] targetKey = potential.getIdentifierKey();
        int targetLevel = -1;
        int exclusiveLevel = potential.getLevel() + 1;

        potential.releaseLatch();

        result = tree.getParentINForChildIN(
            targetId, targetKey, targetLevel,
            exclusiveLevel, false, /*requireExactMatch*/ true, /*dofetch*/
            CacheMode.DEFAULT, null);

        assertFalse(result.exactParentFound);
        assertEquals("expected = " + expectedParent.getNodeId() +
                     " got" + result.parent.getNodeId(),
                     expectedParent, result.parent);
        result.parent.releaseLatch();
    }

    private void checkPotential(Tree tree,
                                BIN expectedParent,
                                byte[] mainKey,
                                byte[] expectedKey)
        throws DatabaseException {

        /* Try an exact match, expect a failure, then try an inexact match. */
        TreeLocation location = new TreeLocation();
        boolean found = tree.getParentBINForChildLN(
            location, mainKey, false, false, CacheMode.DEFAULT);

        assertTrue(!found || location.bin.isEntryKnownDeleted(location.index));

        location.bin.releaseLatch();
        assertEquals(location.bin, expectedParent);
        assertEquals(expectedKey, location.lnKey);
    }
}
