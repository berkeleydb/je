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

import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Random;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DbTestProxy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.util.StringDbt;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

import org.junit.After;

/**
 * Various unit tests for CursorImpl.
 */
public class DbCursorTestBase extends TestBase {
    protected File envHome;
    protected Cursor cursor;
    protected Cursor cursor2;
    protected Database exampleDb;
    protected Environment exampleEnv;
    protected Hashtable simpleDataMap;
    protected Comparator<byte[]> btreeComparisonFunction = null;
    protected Comparator<byte[]> duplicateComparisonFunction = null;
    protected StringDbt[] simpleKeys;
    protected StringDbt[] simpleData;
    protected boolean duplicatesAllowed;
    protected boolean keyPrefixing;

    protected static final int N_KEY_BYTES = 10;
    protected static final int N_ITERS = 2;
    protected static final int N_KEYS = 5000;
    protected static final int N_TOP_LEVEL_KEYS = 10;
    protected static final int N_DUPLICATES_PER_KEY = 2500;
    protected static final int N_COUNT_DUPLICATES_PER_KEY = 500;
    protected static final int N_COUNT_TOP_KEYS = 1;

    protected static int dbCnt = 0;
    protected boolean runBtreeVerifier = true;

    public DbCursorTestBase() {
        envHome = SharedTestUtils.getTestDir();
    }

    protected void initEnv(boolean duplicatesAllowed)
        throws DatabaseException {

        initEnvInternal(duplicatesAllowed, false);
    }

    protected void initEnvTransactional(boolean duplicatesAllowed)
        throws DatabaseException {

        initEnvInternal(duplicatesAllowed, true);
    }

    private void initEnvInternal(boolean duplicatesAllowed,
                                 boolean transactionalDatabase)
        throws DatabaseException {

        this.duplicatesAllowed = duplicatesAllowed;

        /* Set up sample data. */
        int nKeys = simpleKeyStrings.length;
        simpleKeys = new StringDbt[nKeys];
        simpleData = new StringDbt[nKeys];
        for (int i = 0; i < nKeys; i++) {
            simpleKeys[i] = new StringDbt(simpleKeyStrings[i]);
            simpleData[i] = new StringDbt(simpleDataStrings[i]);
        }

        /* Set up an environment. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam(EnvironmentParams.MAX_MEMORY.getName(),
                                 new Long(1 << 24).toString());
        envConfig.setAllowCreate(true);
        if (!runBtreeVerifier) {
            envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        }
        exampleEnv = new Environment(envHome, envConfig);

        /* Set up a database. */
        String databaseName = "simpleDb" + dbCnt++;
        DatabaseConfig dbConfig = new DatabaseConfig();
        if (btreeComparisonFunction != null) {
            dbConfig.setBtreeComparator(btreeComparisonFunction);
        }
        if (duplicateComparisonFunction != null) {
            dbConfig.setDuplicateComparator(duplicateComparisonFunction);
        }
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(duplicatesAllowed);
        dbConfig.setTransactional(transactionalDatabase);
        /* Prefixing cannot be false when sortedDuplicates is true. */
        dbConfig.setKeyPrefixing(keyPrefixing || duplicatesAllowed);
        exampleDb = exampleEnv.openDatabase(null, databaseName, dbConfig);

        /* Set up cursors. */
        cursor = exampleDb.openCursor(null, null);
        cursor2 = exampleDb.openCursor(null, null);
        simpleDataMap = new Hashtable();
    }

    void closeEnv() {
        simpleKeys = null;
        simpleData = null;
        simpleDataMap = null;

        try {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        } catch (DatabaseException ignore) {
        }

        try {
            if (cursor2 != null) {
                cursor2.close();
                cursor2 = null;
            }
        } catch (DatabaseException ignore) {
            /* Same as above. */
        }

        try {
            if (exampleDb != null) {
                exampleDb.close();
                exampleDb = null;
            }
        } catch (Exception ignore) {
        }

        try {
            if (exampleEnv != null) {
                exampleEnv.close();
                exampleEnv = null;
            }
        } catch (Exception ignore) {

            /*
             * Ignore this exception.  It's caused by us calling
             * tearDown() within the test.  Each tearDown() call
             * forces the database closed.  So when the call from
             * junit comes along, it's already closed.
             */
        }
    }

    @After
    public void tearDown() {
        closeEnv();
    }

    protected String[] simpleKeyStrings = {
        "foo", "bar", "baz", "aaa", "fubar",
        "foobar", "quux", "mumble", "froboy" };

    protected String[] simpleDataStrings = {
        "one", "two", "three", "four", "five",
        "six", "seven", "eight", "nine" };

    protected void doSimpleCursorPuts()
        throws DatabaseException {

        for (int i = 0; i < simpleKeyStrings.length; i++) {
            putAndVerifyCursor(cursor, simpleKeys[i], simpleData[i], true);
            simpleDataMap.put(simpleKeyStrings[i], simpleDataStrings[i]);
        }
    }

    /**
     * A class that performs cursor walking.  The walkData method iterates
     * over all data in the database and calls the "perData()" method on
     * each data item.  The perData() method is expected to be overridden
     * by the user.
     */
    protected class DataWalker {
        String prevKey = "";
        String prevData = "";
        int nEntries = 0;
        int deletedEntries = 0;
        int extraVisibleEntries = 0;
        int expectReadLocks = 1;
        protected int nHandleEndOfSet = 0;
        String whenFoundDoInsert;
        String newKey;
        String deletedEntry = null;
        Hashtable dataMap;
        Hashtable addedDataMap;
        Random rnd = new Random();
        /* True if the datamap processing should not happen in the walkData
           routine. */
        boolean ignoreDataMap = false;

        DataWalker(Hashtable dataMap) {
            this.dataMap = dataMap;
            this.addedDataMap = null;
        }

        DataWalker(Hashtable dataMap,
                   Hashtable addedDataMap) {
            this.dataMap = dataMap;
            this.addedDataMap = addedDataMap;
        }

        DataWalker() {
            this.dataMap = simpleDataMap;
            this.addedDataMap = null;
        }

        DataWalker(String whenFoundDoInsert,
                   String newKey,
                   Hashtable dataMap) {
            this.whenFoundDoInsert = whenFoundDoInsert;
            this.newKey = newKey;
            this.dataMap = dataMap;
            this.addedDataMap = null;
        }

        void setIgnoreDataMap(boolean ignoreDataMap) {
            this.ignoreDataMap = ignoreDataMap;
        }

        OperationStatus getFirst(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getFirst(foundKey, foundData,
                                   LockMode.DEFAULT);
        }

        OperationStatus getData(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getNext(foundKey, foundData,
                                  LockMode.DEFAULT);
        }

        StringDbt foundKey = new StringDbt();
        StringDbt foundData = new StringDbt();

        void walkData()
            throws DatabaseException {

            /* get some data back */
            OperationStatus status = getFirst(foundKey, foundData);

            while (status == OperationStatus.SUCCESS) {
                String foundKeyString = foundKey.getString();
                String foundDataString = foundData.getString();

                if (!ignoreDataMap) {
                    if (dataMap.get(foundKeyString) != null) {
                        assertEquals(dataMap.get(foundKeyString),
                                     foundDataString);
                    } else if (addedDataMap != null &&
                               addedDataMap.get(foundKeyString) != null) {
                        assertEquals(addedDataMap.get(foundKeyString),
                                     foundDataString);
                    } else {
                        fail("didn't find key in either map (" +
                             foundKeyString +
                             ")");
                    }
                }

                StatGroup stats =
                    DbTestProxy.dbcGetCursorImpl(cursor).getLockStats();
                assertEquals(expectReadLocks, stats.getInt(LOCK_READ_LOCKS));
                assertEquals(0, stats.getInt(LOCK_WRITE_LOCKS));
                perData(foundKeyString, foundDataString);
                nEntries++;
                status = getData(foundKey, foundData);
                if (status != OperationStatus.SUCCESS) {
                    nHandleEndOfSet++;
                    status = handleEndOfSet(status);
                }
            }
            TestUtils.validateNodeMemUsage(
                DbInternal.getNonNullEnvImpl(exampleEnv), false);
        }

        /**
         * @throws DatabaseException from subclasses.
         */
        void perData(String foundKey, String foundData)
            throws DatabaseException {

            /* to be overridden */
        }

        /**
         * @throws DatabaseException from subclasses.
         */
        OperationStatus handleEndOfSet(OperationStatus status)
            throws DatabaseException {

            return status;
        }

        void close()
            throws DatabaseException {

            cursor.close();
        }
    }

    protected class BackwardsDataWalker extends DataWalker {
        BackwardsDataWalker(Hashtable dataMap) {
            super(dataMap);
        }

        BackwardsDataWalker(Hashtable dataMap,
                            Hashtable addedDataMap) {
            super(dataMap, addedDataMap);
        }

        BackwardsDataWalker(String whenFoundDoInsert,
                            String newKey,
                            Hashtable dataMap) {
            super(whenFoundDoInsert, newKey, dataMap);
        }

        @Override
        OperationStatus getFirst(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getLast(foundKey, foundData,
                                  LockMode.DEFAULT);
        }

        @Override
        OperationStatus getData(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getPrev(foundKey, foundData,
                                  LockMode.DEFAULT);
        }
    }

    protected class DupDataWalker extends DataWalker {
        DupDataWalker(Hashtable dataMap) {
            super(dataMap);
        }

        DupDataWalker(Hashtable dataMap,
                      Hashtable addedDataMap) {
            super(dataMap, addedDataMap);
        }

        DupDataWalker(String whenFoundDoInsert,
                      String newKey,
                      Hashtable dataMap) {
            super(whenFoundDoInsert, newKey, dataMap);
        }

        @Override
        OperationStatus getData(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getNextDup(foundKey, foundData,
                                     LockMode.DEFAULT);
        }
    }

    protected class BackwardsDupDataWalker extends BackwardsDataWalker {
        BackwardsDupDataWalker(Hashtable dataMap) {
            super(dataMap);
        }

        BackwardsDupDataWalker(Hashtable dataMap,
                               Hashtable addedDataMap) {
            super(dataMap, addedDataMap);
        }

        BackwardsDupDataWalker(String whenFoundDoInsert,
                               String newKey,
                               Hashtable dataMap) {
            super(whenFoundDoInsert, newKey, dataMap);
        }

        @Override
        OperationStatus getData(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getPrevDup(foundKey, foundData,
                                     LockMode.DEFAULT);
        }
    }

    protected class NoDupDataWalker extends DataWalker {
        NoDupDataWalker(Hashtable dataMap) {
            super(dataMap);
        }

        NoDupDataWalker(Hashtable dataMap,
                        Hashtable addedDataMap) {
            super(dataMap, addedDataMap);
        }

        NoDupDataWalker(String whenFoundDoInsert,
                        String newKey,
                        Hashtable dataMap) {
            super(whenFoundDoInsert, newKey, dataMap);
        }

        @Override
        OperationStatus getData(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getNextNoDup(foundKey, foundData,
                                       LockMode.DEFAULT);
        }
    }

    protected class NoDupBackwardsDataWalker extends BackwardsDataWalker {
        NoDupBackwardsDataWalker(Hashtable dataMap) {
            super(dataMap);
        }

        NoDupBackwardsDataWalker(Hashtable dataMap,
                                 Hashtable addedDataMap) {
            super(dataMap, addedDataMap);
        }

        NoDupBackwardsDataWalker(String whenFoundDoInsert,
                                 String newKey,
                                 Hashtable dataMap) {
            super(whenFoundDoInsert, newKey, dataMap);
        }

        @Override
        OperationStatus getData(StringDbt foundKey, StringDbt foundData)
            throws DatabaseException {

            return cursor.getPrevNoDup(foundKey, foundData,
                                       LockMode.DEFAULT);
        }
    }

    /**
     * Construct the next highest key.
     */
    protected String nextKey(String key) {
        byte[] sb = StringUtils.toUTF8(key);
        sb[sb.length - 1]++;
        return StringUtils.fromUTF8(sb);
    }

    /**
     * Construct the next lowest key.
     */
    protected String prevKey(String key) {
        byte[] sb = StringUtils.toUTF8(key);
        sb[sb.length - 1]--;
        return StringUtils.fromUTF8(sb);
    }

    /**
     * Helper routine for testLargeXXX routines.
     */
    protected void doLargePut(Hashtable dataMap, int nKeys)
        throws DatabaseException {

        for (int i = 0; i < nKeys; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            TestUtils.generateRandomAlphaBytes(key);
            String keyString = StringUtils.fromUTF8(key);
            String dataString = Integer.toString(i);
            putAndVerifyCursor(cursor, new StringDbt(keyString),
                               new StringDbt(dataString), true);
            if (dataMap != null) {
                dataMap.put(keyString, dataString);
            }
        }
    }

    /**
     * Helper routine for testLargeXXX routines.
     */
    protected void doLargePutPerf(int nKeys)
        throws DatabaseException {

        byte[][] keys = new byte[nKeys][];
        for (int i = 0; i < nKeys; i++) {
            byte[] key = new byte[20];
            keys[i] = key;
            TestUtils.generateRandomAlphaBytes(key);
            String keyString = StringUtils.fromUTF8(key);
            byte[] dataBytes = new byte[120];
            TestUtils.generateRandomAlphaBytes(dataBytes);
            String dataString = StringUtils.fromUTF8(dataBytes);
            putAndVerifyCursor(cursor, new StringDbt(keyString),
                               new StringDbt(dataString), true);
        }
    }

    /**
     * Create some simple duplicate data.
     */
    protected void doSimpleDuplicatePuts()
        throws DatabaseException {

        for (int i = 0; i < simpleKeyStrings.length; i++) {
            for (int j = 0; j < simpleKeyStrings.length; j++) {
                putAndVerifyCursor(cursor, simpleKeys[i], simpleData[j], true);
            }
        }
    }

    /**
     * Create a tree with N_TOP_LEVEL_KEYS keys and N_DUPLICATES_PER_KEY
     * data items per key.
     *
     * @param dataMap A Hashtable of hashtables.  This routine adds entries
     * to the top level hash for each key created.  Secondary hashes contain
     * the duplicate data items for each key in the top level hash.
     *
     * @param putVariant a boolean for varying the way the data is put with the
     * cursor, currently unused..
     */
    protected void createRandomDuplicateData(Hashtable dataMap,
                                             boolean putVariant)
        throws DatabaseException {

        createRandomDuplicateData(N_TOP_LEVEL_KEYS,
                                  N_DUPLICATES_PER_KEY,
                                  dataMap,
                                  putVariant,
                                  false);
    }

    /**
     * Create a tree with a given number of keys and nDup
     * data items per key.
     *
     * @param nTopKeys the number of top level keys to create.  If negative,
     * create that number of top level keys with dupes underneath and the
     * same number of top level keys without any dupes.
     *
     * @param nDup The number of duplicates to create in the duplicate subtree.
     *
     * @param dataMap A Hashtable of hashtables.  This routine adds entries
     * to the top level hash for each key created.  Secondary hashes contain
     * the duplicate data items for each key in the top level hash.
     *
     * @param putVariant a boolean for varying the way the data is put with the
     * cursor, currently unused..
     */
    protected void createRandomDuplicateData(int nTopKeys,
                                             int nDup,
                                             Hashtable dataMap,
                                             boolean putVariant,
                                             boolean verifyCount)
        throws DatabaseException {

        boolean createSomeNonDupes = false;
        if (nTopKeys < 0) {
            nTopKeys = Math.abs(nTopKeys);
            nTopKeys <<= 1;
            createSomeNonDupes = true;
        }

        byte[][] keys = new byte[nTopKeys][];
        for (int i = 0; i < nTopKeys; i++) {
            byte[] key = new byte[N_KEY_BYTES];
            keys[i] = key;
            TestUtils.generateRandomAlphaBytes(key);
            String keyString = StringUtils.fromUTF8(key);
            Hashtable ht = new Hashtable();
            if (dataMap != null) {
                dataMap.put(keyString, ht);
            }
            int nDupesThisTime = nDup;
            if (createSomeNonDupes && (i % 2) == 0) {
                nDupesThisTime = 1;
            }
            for (int j = 1; j <= nDupesThisTime; j++) {
                byte[] data = new byte[N_KEY_BYTES];
                TestUtils.generateRandomAlphaBytes(data);
                OperationStatus status =
                    putAndVerifyCursor(cursor, new StringDbt(keyString),
                                       new StringDbt(data), putVariant);

                if (verifyCount) {
                    assertTrue(cursor.count() == j);
                }

                if (status != OperationStatus.SUCCESS) {
                    throw new DuplicateEntryException
                        ("Duplicate Entry");
                }
                String dataString = StringUtils.fromUTF8(data);
                ht.put(dataString, dataString);
            }
        }
    }

    /**
     * Debugging routine.  Iterate through the transient hashtable of
     * key/data pairs and ensure that each key can be retrieved from
     * the tree.
     */
    protected void verifyEntries(Hashtable dataMap)
        throws DatabaseException {

        Tree tree = DbInternal.getDbImpl(exampleDb).getTree();
        Enumeration e = dataMap.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (!retrieveData(tree, StringUtils.toUTF8(key))) {
                System.out.println("Couldn't find: " + key);
            }
        }
    }

    /* Throw assertion if the database is not valid. */
    protected void validateDatabase()
        throws DatabaseException {

        DatabaseImpl dbImpl = DbInternal.getDbImpl(exampleDb);
        dbImpl.verify(new VerifyConfig());
    }

    /**
     * Helper routine for above.
     */
    protected boolean retrieveData(Tree tree, byte[] key)
        throws DatabaseException {

        TestUtils.checkLatchCount();
        Node n = tree.search(key, Tree.SearchType.NORMAL, null,
                             CacheMode.DEFAULT, null /*keyComparator*/);
        if (!(n instanceof BIN)) {
            fail("search didn't return a BIN for key: " + key);
        }
        BIN bin = (BIN) n;
        try {
            int index = bin.findEntry(key, false, true);
            if (index == -1) {
                return false;
            }
            return true;
        } finally {
            bin.releaseLatch();
            TestUtils.checkLatchCount();
        }
    }

    protected OperationStatus putAndVerifyCursor(Cursor cursor,
                                                 StringDbt key,
                                                 StringDbt data,
                                                 boolean putVariant)
        throws DatabaseException {

        OperationStatus status;
        if (duplicatesAllowed) {
            status = cursor.putNoDupData(key, data);
        } else {
            status = cursor.putNoOverwrite(key, data);
        }

        if (status == OperationStatus.SUCCESS) {
            StringDbt keyCheck = new StringDbt();
            StringDbt dataCheck = new StringDbt();

            assertEquals(OperationStatus.SUCCESS, cursor.getCurrent
                         (keyCheck, dataCheck, LockMode.DEFAULT));
            assertEquals(key.getString(), keyCheck.getString());
            assertEquals(data.getString(), dataCheck.getString());
        }

        return status;
    }

    @SuppressWarnings("serial")
    protected static class BtreeComparator implements Comparator,
                                                      Serializable {
        protected boolean ascendingComparison = true;

        protected BtreeComparator() {
        }

        public int compare(Object o1, Object o2) {
            byte[] arg1;
            byte[] arg2;
            if (ascendingComparison) {
                arg1 = (byte[]) o1;
                arg2 = (byte[]) o2;
            } else {
                arg1 = (byte[]) o2;
                arg2 = (byte[]) o1;
            }
            int a1Len = arg1.length;
            int a2Len = arg2.length;

            int limit = Math.min(a1Len, a2Len);

            for (int i = 0; i < limit; i++) {
                byte b1 = arg1[i];
                byte b2 = arg2[i];
                if (b1 == b2) {
                    continue;
                } else {
                    /* Remember, bytes are signed, so convert to shorts so that
                       we effectively do an unsigned byte comparison. */
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

    @SuppressWarnings("serial")
    protected static class ReverseBtreeComparator extends BtreeComparator {
        protected ReverseBtreeComparator() {
            ascendingComparison = false;
        }
    }
}
