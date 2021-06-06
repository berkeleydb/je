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

import java.io.File;

import org.junit.After;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.NullCursor;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class TreeTestBase extends TestBase {
    static protected final boolean DEBUG = true;

    static protected int N_KEY_BYTES = 10;
    static protected int N_ITERS = 1;
    static protected int N_KEYS = 10000;
    static protected int MAX_ENTRIES_PER_NODE = 6;

    protected Tree tree = null;
    protected byte[] minKey = null;
    protected byte[] maxKey = null;
    protected Database db = null;
    protected Environment env = null;
    protected File envHome = null;

    public TreeTestBase() {
        envHome = SharedTestUtils.getTestDir();
    }

    void initEnv(boolean duplicatesAllowed)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_EVICTOR.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 Integer.toString(MAX_ENTRIES_PER_NODE));
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(duplicatesAllowed);
        db = env.openDatabase(null, "foo", dbConfig);

        tree = DbInternal.getDbImpl(db).getTree();
        minKey = null;
        maxKey = null;
    }

    @After
    public void tearDown()
        throws Exception {

        db.close();
        if (env != null) {
            env.close();
        }
        env = null;
        db = null;
        tree = null;
        minKey = null;
        maxKey = null;
    }

    protected IN makeDupIN(IN old) {

        IN ret = new IN(
            DbInternal.getDbImpl(db),
            old.getIdentifierKey(), MAX_ENTRIES_PER_NODE, 2);

        ret.setNodeId(old.getNodeId());
        ret.setIsRoot(old.isRoot());

        for (int i = 0; i < old.getNEntries(); i++) {
            ret.appendEntryFromOtherNode(old, i);
        }

        return ret;
    }

    /**
     * Helper routine to insert a key and immediately read it back.
     */
    protected void insertAndRetrieve(NullCursor cursor, byte[] key, LN ln)
        throws DatabaseException {

        if (minKey == null) {
            minKey = key;
        } else if (Key.compareKeys(key, minKey, null) < 0) {
            minKey = key;
        }

        if (maxKey == null) {
            maxKey = key;
        } else if (Key.compareKeys(maxKey, key, null) < 0) {
            maxKey = key;
        }

        cursor.reset();

        TestUtils.checkLatchCount();

        assertTrue(cursor.insertRecord(
            key, ln, false, ReplicationContext.NO_REPLICATE));

        TestUtils.checkLatchCount();

        LN foundLN = retrieveLN(key);

        assertTrue(foundLN == ln || foundLN.logicalEquals(ln));
    }

    /**
     * Helper routine to read the LN referred to by key.
     */
    protected LN retrieveLN(byte[] key)
        throws DatabaseException {

        TestUtils.checkLatchCount();
        IN n = tree.search(key, Tree.SearchType.NORMAL, null,
                           CacheMode.DEFAULT, null /*keyComparator*/);
        if (!(n instanceof BIN)) {
            fail("search didn't return a BIN for key: " + key);
        }
        BIN bin = (BIN) n;
        try {
            int index = bin.findEntry(key, false, true);
            if (index == -1) {
                fail("Didn't read back key: " + key);
            } else {
                Node node = bin.getTarget(index);
                if (node instanceof LN) {
                    return (LN) node;
                } else if (bin.isEmbeddedLN(index)) {
                    return new LN(bin.getData(index));
                } else {
                    fail("Didn't read back LN for: " + key);
                }
            }
            return null;
        } finally {
            bin.releaseLatch();
            TestUtils.checkLatchCount();
        }
    }

    /**
     * Using getNextBin, count all the keys in the database.  Ensure that
     * they're returned in ascending order.
     */
    protected int countAndValidateKeys(Tree tree)
        throws DatabaseException {

        TestUtils.checkLatchCount();
        BIN nextBin = (BIN) tree.getFirstNode(CacheMode.DEFAULT);
        byte[] prevKey = { 0x00 };

        int cnt = 0;

        while (nextBin != null) {
            for (int i = 0; i < nextBin.getNEntries(); i++) {
                byte[] curKey = nextBin.getKey(i);
                if (Key.compareKeys(curKey, prevKey, null) <= 0) {
                    throw new RuntimeException
                        ("keys are out of order");
                }
                cnt++;
                prevKey = curKey;
            }
            nextBin = tree.getNextBin(nextBin, CacheMode.DEFAULT);
        }
        TestUtils.checkLatchCount();
        return cnt;
    }

    /**
     * Using getPrevBin, count all the keys in the database.  Ensure that
     * they're returned in descending order.
     */
    protected int countAndValidateKeysBackwards(Tree tree)
        throws DatabaseException {

        TestUtils.checkLatchCount();
        BIN nextBin = (BIN) tree.getLastNode(CacheMode.DEFAULT);
        byte[] prevKey = null;

        int cnt = 0;

        while (nextBin != null) {
            for (int i = nextBin.getNEntries() - 1; i >= 0; i--) {
                byte[] curKey = nextBin.getKey(i);
                if (prevKey != null &&
                    Key.compareKeys(prevKey, curKey, null) <= 0) {
                    throw new RuntimeException
                        ("keys are out of order");
                }
                cnt++;
                prevKey = curKey;
            }
            nextBin = tree.getPrevBin(nextBin, CacheMode.DEFAULT);
        }
        return cnt;
    }
}
