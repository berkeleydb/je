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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.NullCursor;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.utilint.StringUtils;

public class TreeTest extends TreeTestBase {

    public TreeTest() {
        super();
    }

    /**
     * Rudimentary insert/retrieve test.
     */
    @Test
    public void testSimpleTreeCreation()
        throws DatabaseException {
        initEnv(false);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn = BasicLocker.
            createBasicLocker(DbInternal.getNonNullEnvImpl(env));
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);
        insertAndRetrieve(cursor, StringUtils.toUTF8("aaaaa"),
                          new LN(new byte[0]));
        insertAndRetrieve(cursor, StringUtils.toUTF8("aaaab"),
                          new LN(new byte[0]));
        insertAndRetrieve(cursor, StringUtils.toUTF8("aaaa"),
                          new LN(new byte[0]));
        insertAndRetrieve(cursor, StringUtils.toUTF8("aaa"),
                          new LN(new byte[0]));
        txn.operationEnd();
    }

    /**
     * Slightly less rudimentary test inserting a handfull of keys and LN's.
     */
    @Test
    public void testMultipleInsertRetrieve0()
        throws DatabaseException {

        /*
         * Set the seed to reproduce a specific problem found while debugging:
         * IN.split was splitting with the identifier key being on the right
         * side.
         */
        initEnv(false);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);
        for (int i = 0; i < 21; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            TestUtils.generateRandomAlphaBytes(key);
            insertAndRetrieve(cursor, key, new LN(new byte[0]));
        }
        txn.operationEnd();
    }

    /**
     * Insert a bunch of keys and test that they retrieve back ok.  While we
     * insert, maintain the highest and lowest keys inserted.  Verify that
     * getFirstNode and getLastNode return those two entries.  Lather, rinse,
     * repeat.
     */
    @Test
    public void testMultipleInsertRetrieve1()
        throws DatabaseException {

        initEnv(false);
        doMultipleInsertRetrieve1();
    }

    /**
     * Helper routine for above.
     */
    private void doMultipleInsertRetrieve1()
        throws DatabaseException {

        byte[][] keys = new byte[N_KEYS][];
        LN[] lns = new LN[N_KEYS];

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);

        for (int i = 0; i < N_KEYS; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            keys[i] = key;
            lns[i] = new LN(new byte[0]);
            TestUtils.generateRandomAlphaBytes(key);
            insertAndRetrieve(cursor, key, lns[i]);
        }

        for (int i = 0; i < N_KEYS; i++) {
            LN foundLN = retrieveLN(keys[i]);
            assertTrue(foundLN == lns[i] || foundLN.logicalEquals(lns[i]));
        }

        TestUtils.checkLatchCount();
        IN leftMostNode = tree.getFirstNode(CacheMode.DEFAULT);

        assertTrue(leftMostNode instanceof BIN);
        BIN lmn = (BIN) leftMostNode;
        lmn.releaseLatch();
        TestUtils.checkLatchCount();
        assertTrue(Key.compareKeys(lmn.getKey(0), minKey, null) == 0);

        TestUtils.checkLatchCount();
        IN rightMostNode = tree.getLastNode(CacheMode.DEFAULT);

        assertTrue(rightMostNode instanceof BIN);
        BIN rmn = (BIN) rightMostNode;
        rmn.releaseLatch();
        TestUtils.checkLatchCount();
        assertTrue(Key.compareKeys
            (rmn.getKey(rmn.getNEntries() - 1), maxKey, null) == 0);
        assertTrue(tree.getTreeStats() > 1);

        txn.operationEnd();
    }

    /**
     * Create a tree.  After creation, walk the bins forwards using getNextBin
     * counting the keys and validating that the keys are being returned in
     * ascending order.  Ensure that the correct number of keys were returned.
     */
    @Test
    public void testCountAndValidateKeys()
        throws DatabaseException {

        initEnv(false);
        doCountAndValidateKeys();
    }

    /**
     * Helper routine for above test.
     */
    private void doCountAndValidateKeys()
        throws DatabaseException {
        byte[][] keys = new byte[N_KEYS][];
        LN[] lns = new LN[N_KEYS];
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);

        for (int i = 0; i < N_KEYS; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            keys[i] = key;
            lns[i] = new LN(new byte[0]);
            TestUtils.generateRandomAlphaBytes(key);
            insertAndRetrieve(cursor, key, lns[i]);
        }
        assertTrue(countAndValidateKeys(tree) == N_KEYS);
        txn.operationEnd();
    }

    /**
     * Create a tree.  After creation, walk the bins backwards using getPrevBin
     * counting the keys and validating that the keys are being returned in
     * descending order.  Ensure that the correct number of keys were returned.
     */
    @Test
    public void testCountAndValidateKeysBackwards()
        throws DatabaseException {

        initEnv(false);
        doCountAndValidateKeysBackwards();
    }

    /**
     * Helper routine for above test.
     */
    public void doCountAndValidateKeysBackwards()
        throws DatabaseException {

        byte[][] keys = new byte[N_KEYS][];
        LN[] lns = new LN[N_KEYS];
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);
        for (int i = 0; i < N_KEYS; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            keys[i] = key;
            lns[i] = new LN(new byte[0]);
            TestUtils.generateRandomAlphaBytes(key);
            insertAndRetrieve(cursor, key, lns[i]);
        }
        assertTrue(countAndValidateKeysBackwards(tree) == N_KEYS);
        txn.operationEnd();
    }

    @Test
    public void testAscendingInsertBalance()
        throws DatabaseException {

        initEnv(false);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);

        /* Fill up a db with data */
        for (int i = 0; i < N_KEYS; i++) {
            byte[] keyBytes = new byte[4];
            TestUtils.putUnsignedInt(keyBytes, TestUtils.alphaKey(i));
            insertAndRetrieve(cursor, keyBytes,
                              new LN(new byte[0]));
        }

        TestUtils.checkLatchCount();

        /* Count the number of levels on the left. */
        IN leftMostNode = tree.getFirstNode(CacheMode.DEFAULT);
        assertTrue(leftMostNode instanceof BIN);
        int leftSideLevels = 0;
        do {
            SearchResult result = tree.getParentINForChildIN(
                leftMostNode, false, /*useTargetLevel*/
                true, /*doFetch*/ CacheMode.DEFAULT);

            leftMostNode = result.parent;
            leftSideLevels++;

            if (leftMostNode != null && leftMostNode.isRoot()) {
                leftMostNode.releaseLatch();
                break;
            }

        } while (leftMostNode != null);

        TestUtils.checkLatchCount();

        /* Count the number of levels on the right. */
        IN rightMostNode = tree.getLastNode(CacheMode.DEFAULT);
        assertTrue(rightMostNode instanceof BIN);
        int rightSideLevels = 0;
        do {
            SearchResult result = tree.getParentINForChildIN(
                rightMostNode, false, /*useTargetLevel*/
                true, /*doFetch*/ CacheMode.DEFAULT);

            rightMostNode = result.parent;
            rightSideLevels++;

            if (rightMostNode != null && rightMostNode.isRoot()) {
                rightMostNode.releaseLatch();
                break;
            }

        } while (rightMostNode != null);

        TestUtils.checkLatchCount();

        if (leftSideLevels > 10 ||
            rightSideLevels > 10) {
            fail("Levels too high (" +
                 leftSideLevels +
                 "/" +
                 rightSideLevels +
                 ") on descending insert");
        }
        txn.operationEnd();
    }

    @Test
    public void testDescendingInsertBalance()
        throws DatabaseException {
        initEnv(false);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);

        for (int i = N_KEYS; i >= 0; --i) {
            byte[] keyBytes = new byte[4];
            TestUtils.putUnsignedInt(keyBytes, TestUtils.alphaKey(i));
            insertAndRetrieve(cursor, keyBytes,
                              new LN(new byte[0]));
        }

        TestUtils.checkLatchCount();
        IN leftMostNode = tree.getFirstNode(CacheMode.DEFAULT);

        assertTrue(leftMostNode instanceof BIN);
        int leftSideLevels = 0;
        do {
            SearchResult result = tree.getParentINForChildIN(
                leftMostNode, false, /*useTargetLevel*/
                true, CacheMode.DEFAULT);

            leftMostNode = result.parent;
            leftSideLevels++;

            if (leftMostNode != null && leftMostNode.isRoot()) {
                leftMostNode.releaseLatch();
                break;
            }
        } while (leftMostNode != null);

        TestUtils.checkLatchCount();

        IN rightMostNode = tree.getLastNode(CacheMode.DEFAULT);

        assertTrue(rightMostNode instanceof BIN);
        int rightSideLevels = 0;
        do {
            SearchResult result = tree.getParentINForChildIN(
                rightMostNode, false, /*useTargetLevel*/
                true, CacheMode.DEFAULT);

            rightMostNode = result.parent;
            rightSideLevels++;

            if (rightMostNode != null && rightMostNode.isRoot()) {
                rightMostNode.releaseLatch();
                break;
            }
        } while (rightMostNode != null);

        TestUtils.checkLatchCount();

        if (leftSideLevels > 10 ||
            rightSideLevels > 10) {
            fail("Levels too high (" +
                 leftSideLevels +
                 "/" +
                 rightSideLevels +
                 ") on descending insert");
        }
        txn.operationEnd();
    }

    /**
     * Insert a bunch of keys.  Call verify and validate the results.
     */
    @Test
    public void testVerify()
        throws DatabaseException {

        initEnv(false);
        byte[][] keys = new byte[N_KEYS][];
        LN[] lns = new LN[N_KEYS];
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        Locker txn = BasicLocker.createBasicLocker(envImpl);
        NullCursor cursor = new NullCursor(tree.getDatabase(), txn);

        for (int i = 0; i < N_KEYS; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            keys[i] = key;
            lns[i] = new LN((byte[]) new byte[1]);
            TestUtils.generateRandomAlphaBytes(key);
            insertAndRetrieve(cursor, key, lns[i]);
        }

        /*
         * Note that verify will attempt to continue past errors, so
         * assertTrue on the status return.
         */
        assertTrue(env.verify(new VerifyConfig(), System.err));
        DatabaseStats stats = db.verify(new VerifyConfig());
        BtreeStats btStats = (BtreeStats) stats;

        assertTrue(btStats.getInternalNodeCount() <
                   btStats.getBottomInternalNodeCount());
        assertTrue(btStats.getBottomInternalNodeCount() <
                   btStats.getLeafNodeCount() +
                   btStats.getDeletedLeafNodeCount());
        assertTrue(btStats.getLeafNodeCount() +
                   btStats.getDeletedLeafNodeCount() ==
                   N_KEYS);
        txn.operationEnd();

        /* Now intentionally create LogFileNotFoundExceptions */
        /*
          db.close();
          env.close();

          This is disabled until the method for flipping files is
          introduced. It's too hard to create a LogFileNotFoundException
          by brute force deleting a file; often recovery doesn't work.
          Instead, use a flipped file later on.

        String[] jeFiles =
            FileManager.listFiles(envHome,
                                  new String[] {FileManager.JE_SUFFIX});
        int targetIdx = jeFiles.length / 2;
        assertTrue(targetIdx > 0);
        File targetFile = new File(envHome, jeFiles[targetIdx]);
        assertTrue(targetFile.delete());

        initEnv(false);
        assertFalse(env.verify(new VerifyConfig(), System.err));
        */
    }
}
