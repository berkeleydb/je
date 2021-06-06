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

package com.sleepycat.je.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.sleepycat.je.CacheMode;
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
import com.sleepycat.je.Transaction;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.StartupTracker;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;

public class RecoveryTestBase extends TestBase {
    private static final boolean DEBUG = false;

    protected static final int NUM_RECS = 257;
    protected static final int N_DUPLICATES_PER_KEY = 28;
    protected static final int NUM_DBS = 3;
    protected static final int TREE_FANOUT = 6;

    protected static final String DB_NAME = "testDb";

    protected File envHome;
    protected Environment env;
    protected Database[] dbs;
    protected EnvironmentConfig envConfig;
    protected CheckpointConfig forceConfig;
    protected Comparator<byte[]> btreeComparisonFunction = null;

    protected int treeFanout;

    public RecoveryTestBase() {
        init();
    }

    public RecoveryTestBase(boolean reduceMemory) {
        init();
        envConfig.setConfigParam(EnvironmentParams.MAX_MEMORY.getName(),
                                 new Long(1 << 24).toString());
    }

    private void init() {
        envHome = SharedTestUtils.getTestDir();
        Key.DUMP_TYPE = DumpType.BINARY;
        envConfig = TestUtils.initEnvConfig();
        forceConfig = new CheckpointConfig();
        forceConfig.setForce(true);
        treeFanout = TREE_FANOUT;
    }


    @After
    public void tearDown() {
        
        if (env != null) {
            try {
                env.close();
            } catch (RuntimeException E) {
            }
        }
        env = null;
        dbs = null;
        envConfig = null;
        forceConfig = null;
    }

    /**
     * Make an environment and databases, commit the db creation by default.
     * Running with or without the checkpoint daemon changes how recovery is
     * exercised.
     */
    protected void createEnv(int fileSize, boolean runCheckpointDaemon)
        throws DatabaseException {

        createEnvInternal(fileSize, runCheckpointDaemon, false, false);
    }

    private void createEnvInternal(int fileSize,
                                   boolean runCheckpointDaemon,
                                   boolean createXAEnv,
                                   boolean checkLeaks)
        throws DatabaseException {

        /* Make an environment and open it. */
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.
            setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                           Integer.toString(fileSize));
        envConfig.setConfigParam(EnvironmentParams.ENV_CHECK_LEAKS.getName(),
                                 (checkLeaks ? " true" : "false"));
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                 Integer.toString(treeFanout));
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_CLEANER.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_EVICTOR.getName(),
                                 "false");

        if (!runCheckpointDaemon) {
            envConfig.setConfigParam
                (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        }
        setExtraProperties();
        if (createXAEnv) {
            env = new XAEnvironment(envHome, envConfig);
        } else {
            env = new Environment(envHome, envConfig);
        }
    }

    /**
     * Overriden by using class.
     * @throws DatabaseException from subclasses.
     */
    protected void setExtraProperties()
        throws DatabaseException {
    }

    /**
     * Open/create databases.
     */
    protected void createDbs(Transaction txn, int numDbs)
        throws DatabaseException {

        /* Make a db and open it. */
        dbs = new Database[numDbs];

        DatabaseConfig dbConfig = new DatabaseConfig();
        if (btreeComparisonFunction != null) {
            dbConfig.setBtreeComparator
                ((Class<? extends Comparator<byte[]>>)
                 btreeComparisonFunction.getClass());
        }
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        for (int i = 0; i < numDbs; i++) {
            dbs[i] = env.openDatabase(txn, DB_NAME + i, dbConfig);
        }
    }

    protected void closeDbs() {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs) {
            if (db != null) {
                db.close();
            }
        }
        dbs = null;
    }

    /**
     * Make an environment and databases.
     */
    protected void  createEnvAndDbs(int fileSize,
                                   boolean runCheckpointerDaemon,
                                   int numDbs)
        throws DatabaseException {

        createEnvAndDbsInternal(fileSize, runCheckpointerDaemon,
                                numDbs, false, false);
    }

    protected void createXAEnvAndDbs(int fileSize,
                                     boolean runCheckpointerDaemon,
                                     int numDbs,
                                     boolean checkLeaks)
        throws DatabaseException {

        createEnvAndDbsInternal(fileSize, runCheckpointerDaemon,
                                numDbs, true, checkLeaks);
    }

    protected void createEnvAndDbsInternal(int fileSize,
                                           boolean runCheckpointerDaemon,
                                           int numDbs,
                                           boolean createXAEnv,
                                           boolean checkLeaks)
        throws DatabaseException {

        createEnvInternal(fileSize, runCheckpointerDaemon,
                          createXAEnv, checkLeaks);
        Transaction txn = env.beginTransaction(null, null);
        createDbs(txn, numDbs);
        txn.commit();
    }

    /**
     * Throw away the environment so the next open will cause a recovery.
     */
    protected void closeEnv()
        throws DatabaseException {

        TestUtils.validateNodeMemUsage(DbInternal.getNonNullEnvImpl(env),
                                      false);

        /* Close the environment. */
        closeDbs();
        forceCloseEnvOnly();
    }

    /* Force the environment to be closed even if with outstanding handles.*/
    protected void forceCloseEnvOnly()
        throws DatabaseException {

        /* Close w/out checkpointing, in order to exercise recovery better.*/
        DbInternal.getNonNullEnvImpl(env).close(false);
        env = null;
    }

    /*
     * Recover the databases and check the data. Return a list of the
     * RecoveryInfos generated by each recovery.
     */
    protected List<StartupTracker> recoverAndVerify(Map<TestData,
                                                  Set<TestData>> expectedData,
                                                  int numDbs)
        throws DatabaseException {

        return recoverAndVerifyInternal(expectedData, numDbs,
                                        false,  // XA
                                        false); // readOnly
    }

    protected List<StartupTracker>
        recoverROAndVerify(Map<TestData,
                           Set<TestData>> expectedData,
                           int numDbs)
        throws DatabaseException {

        return recoverAndVerifyInternal(expectedData, numDbs,
                                        false,  // XA
                                        true);  // readOnly
    }

    /*
     * Recover the databases and check the data. Return a list of the
     * RecoveryInfos generated by each recovery.
     */
    protected List<StartupTracker>
        xaRecoverAndVerify(Map<TestData,
                           Set<TestData>> expectedData,
                           int numDbs)
        throws DatabaseException {

        return recoverAndVerifyInternal(expectedData, 
                                        numDbs,
                                        true,   // XA
                                        false); // readOnly
    }

    private List<StartupTracker>
        recoverAndVerifyInternal(Map<TestData,
                                 Set<TestData>> expectedData,
                                 int numDbs,
                                 boolean createXAEnv,
                                 boolean readOnlyMode)
        throws DatabaseException {

        List<StartupTracker> infoList =
            recoverOnlyInternal(createXAEnv, readOnlyMode);
        verifyData(expectedData, numDbs);
        TestUtils.validateNodeMemUsage(DbInternal.getNonNullEnvImpl(env),
                                       false);
        /* Run verify again. */
        DbInternal.getNonNullEnvImpl(env).close(false);
        env = new Environment(envHome, getRecoveryConfig(readOnlyMode));
        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(env);
        infoList.add(envImpl.getStartupTracker());
        verifyData(expectedData, numDbs);
        TestUtils.validateNodeMemUsage(envImpl, false);
        env.close();
        return infoList;
    }

    private EnvironmentConfig getRecoveryConfig(boolean readOnlyMode) {
        EnvironmentConfig recoveryConfig = TestUtils.initEnvConfig();
        recoveryConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), "6");
        recoveryConfig.setConfigParam
            (EnvironmentParams.MAX_MEMORY.getName(),
             String.valueOf(10L << 24));
        recoveryConfig.setReadOnly(readOnlyMode);

        /*
         * Don't run checkLeaks, because verify is running while the system is
         * not quiescent. The other daemons are running.
         */
        recoveryConfig.setConfigParam
            (EnvironmentParams.ENV_CHECK_LEAKS.getName(), "false");
        recoveryConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        recoveryConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        recoveryConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");

        recoveryConfig.setTransactional(true);
        return recoveryConfig;
    }

    protected List<StartupTracker> recoverOnly()
        throws DatabaseException {

        return recoverOnlyInternal(false,   // XA
                                   false);  // read only
    }

    protected List<StartupTracker> xaRecoverOnly()
        throws DatabaseException {

        return recoverOnlyInternal(true,   // XA
                                   false); // read only
    }

    private List<StartupTracker> 
         recoverOnlyInternal(boolean createXAEnv,
                             boolean readOnlyMode)
        throws DatabaseException {

        List<StartupTracker> infoList = new ArrayList<StartupTracker>();

        /* Open it again, which will run recovery. */
        if (createXAEnv) {
            env = new XAEnvironment(envHome, getRecoveryConfig(readOnlyMode));
        } else {
            env = new Environment(envHome, getRecoveryConfig(readOnlyMode));
        }
        TestUtils.validateNodeMemUsage(DbInternal.getNonNullEnvImpl(env),
                                      false);

        infoList.add
            (DbInternal.getNonNullEnvImpl(env).getStartupTracker());

        return infoList;
    }

    /**
     * Compare the data in the databases agains the data in the expected data
     * set.
     */
    protected void verifyData(Map<TestData,
                              Set<TestData>> expectedData,
                              int numDbs)
        throws DatabaseException  {

        verifyData(expectedData, true, numDbs);
    }

    /**
     * Compare the data in the databases against the data in the expected data
     * set.
     */
    protected void verifyData(Map<TestData, Set<TestData>> expectedData,
                              boolean checkInList,
                              int numDbs)
        throws DatabaseException  {

        verifyData(expectedData, checkInList, 0, numDbs);
    }

    @SuppressWarnings("unchecked")
    protected void verifyData(Map<TestData, Set<TestData>> expectedData,
                              boolean checkInList,
                              int startDb,
                              int endDb)
        throws DatabaseException  {

        /* Run verify. */
        if (checkInList) {
            assertTrue(env.verify(null, System.err));
        } else {
            assertTrue(env.verify(null, System.err));
        }

        /*
         * Get a deep copy of expected data (cloning the data sets, not the
         * items within dataSet, since verifyData will remove items, and we
         * need to keep the expectedData set intact because we call verify
         * repeatedly.
         */
        Map<TestData, Set<TestData>> useData =
            new HashMap<TestData, Set<TestData>>();

        Iterator<Map.Entry<TestData, Set<TestData>>> iter =
            expectedData.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<TestData, Set<TestData>> entry = iter.next();
            Set<TestData> clone =
                (Set<TestData>) ((HashSet<TestData>)entry.getValue()).clone();
            useData.put(entry.getKey(), clone);
        }

        /* Generate an expected count map. */
        Map<TestData, Integer> countMap = generateCountMap(expectedData);

        /* Check each db in turn. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        if (btreeComparisonFunction != null) {
            dbConfig.setBtreeComparator
                ((Class<? extends Comparator<byte[]>>)
                 btreeComparisonFunction.getClass());
        }
        dbConfig.setTransactional(env.getConfig().getTransactional());
        dbConfig.setSortedDuplicates(true);
        dbConfig.setReadOnly(true);
        for (int d = startDb; d < endDb; d++) {
            Database checkDb = env.openDatabase(null, DB_NAME + d,
                                                dbConfig);
            Cursor myCursor = checkDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            OperationStatus status =
                myCursor.getFirst(key, data, LockMode.DEFAULT);
            DbInternal.getNonNullEnvImpl(env).verifyCursors();
            int numSeen = 0;

            while (status == OperationStatus.SUCCESS) {

                /* The key should have been in the expected data set. */
                removeExpectedData(useData, d, key, data, true);

                /* The count should be right. */
                int count = myCursor.count();
                assertEquals("Count not right for key " +
                             TestUtils.dumpByteArray(key.getData()),
                             getExpectedCount(countMap, d, key), count);

                status = myCursor.getNext(key, data, LockMode.DEFAULT);
                numSeen++;
            }

            myCursor.close();

            /* Should be nothing left in the expected data map. */
            if (DEBUG) {
                System.out.println("Finished db" + d + " numSeen=" +numSeen);
                dumpExpected(useData);
            }
            checkDb.close();
        }

        assertEquals(0, useData.size());
    }

    /**
     * Process the expected data map to generate expected counts. For each
     * database, make a map of key value to count.
     */
    private Map<TestData, Integer>
        generateCountMap(Map<TestData, Set<TestData>> expectedData) {

        Map<TestData, Integer> countMap = new HashMap<TestData, Integer>();

        Iterator<Set<TestData>> iter = expectedData.values().iterator();
        while (iter.hasNext()) {
            Set<TestData> dataSet = iter.next();
            Iterator<TestData> dataIter = dataSet.iterator();
            while (dataIter.hasNext()) {
                TestData t = dataIter.next();
                TestData countKey = new TestData(t.dbNum, t.key);
                Integer count = countMap.get(countKey);
                if (count == null) {
                    countMap.put(countKey, new Integer(1));
                } else {
                    countMap.put(countKey, new Integer(count.intValue()+1));
                }
            }
        }
        return countMap;
    }

    /**
     * @return the expected count value for a given key in a given db.
     */
    private int getExpectedCount(Map<TestData, Integer> countMap,
                                 int whichDb,
                                 DatabaseEntry key) {
        return countMap.get(new TestData(whichDb, key.getData())).intValue();
    }

    /**
     * Insert data over many databases.
     */
    protected void insertData(Transaction txn,
                              int startVal,
                              int endVal,
                              Map<TestData, Set<TestData>> expectedData,
                              int nDuplicatesPerKey,
                              boolean addToExpectedData,
                              int numDbs)
        throws DatabaseException {

        insertData(txn, startVal, endVal, expectedData,
                   nDuplicatesPerKey, false, addToExpectedData,
                   0, numDbs);
    }

    protected void insertData(Transaction txn,
                              int startVal,
                              int endVal,
                              Map<TestData, Set<TestData>> expectedData,
                              int nDuplicatesPerKey,
                              boolean addToExpectedData,
                              int startDb,
                              int endDb)
        throws DatabaseException {

        insertData(txn, startVal, endVal, expectedData,
                   nDuplicatesPerKey, false, addToExpectedData,
                   startDb, endDb);
    }

    /**
     * Insert data over many databases.
     *
     * @param toggle if true, insert every other value.
     */
    protected void insertData(Transaction txn,
                              int startVal,
                              int endVal,
                              Map<TestData, Set<TestData>> expectedData,
                              int nDuplicatesPerKey,
                              boolean toggle,
                              boolean addToExpectedData,
                              int numDbs)
        throws DatabaseException {

        insertData(txn, startVal, endVal, expectedData, nDuplicatesPerKey,
                   toggle, addToExpectedData, 0, numDbs);
    }

    /**
     * Insert data over many databases.
     *
     * @param toggle if true, insert every other value.
     */
    protected void insertData(Transaction txn,
                              int startVal,
                              int endVal,
                              Map<TestData, Set<TestData>> expectedData,
                              int nDuplicatesPerKey,
                              boolean toggle,
                              boolean addToExpectedData,
                              int startDb,
                              int endDb)
        throws DatabaseException {

        Cursor[] cursors = getCursors(txn, startDb, endDb);

        /* Make sure this test inserts something! */
        assertTrue(endVal - startVal > -1);

        /* Are we inserting in an ascending or descending way? */
        int incVal = (toggle) ? 2 : 1;
        if (startVal < endVal) {
            for (int i = startVal; i <= endVal; i += incVal) {
                insertOneRecord(cursors, i, expectedData,
                                nDuplicatesPerKey, addToExpectedData);
            }
        } else {
            for (int i = startVal; i >= endVal; i -= incVal) {
                insertOneRecord(cursors, i, expectedData,
                                nDuplicatesPerKey, addToExpectedData);
            }
        }

        for (int i = 0; i < cursors.length; i++) {
            cursors[i].close();
        }
    }

    /**
     * Insert data over many databases.
     */
    protected void insertRandomData(Transaction txn,
                                    Random rng,
                                    int numRecs,
                                    Map<TestData, Set<TestData>> expectedData,
                                    int nDuplicatesPerKey,
                                    boolean evictBINs,
                                    int startDb,
                                    int endDb)
        throws DatabaseException {

        Cursor[] cursors = getCursors(txn, startDb, endDb);

        if (evictBINs) {
            for (int i = 0; i < cursors.length; i += 1) {
                cursors[i].setCacheMode(CacheMode.EVICT_BIN);
            }
        }

        for (int i = 0; i < numRecs; i += 1) {
            int keyVal = rng.nextInt();
            insertOneRecord(cursors, keyVal, expectedData,
                            nDuplicatesPerKey, true);
        }

        for (int i = 0; i < cursors.length; i++) {
            cursors[i].close();
        }
    }

    /**
     * Add to the set of expected results. ExpectedData is keyed by a TestData
     * object that wraps db number and key, and points to sets of TestData
     * objects that wrap db number, key, and data.
     */
    protected void addExpectedData(Map<TestData, Set<TestData>> expectedData,
                                   int dbNum,
                                   DatabaseEntry key,
                                   DatabaseEntry data,
                                   boolean expectCommit) {
        if (expectCommit) {
            TestData keyTestData = new TestData(dbNum, key, null);
            Set<TestData> dataSet = expectedData.get(keyTestData);
            if (dataSet == null) {
                dataSet = new HashSet<TestData>();
                expectedData.put(keyTestData, dataSet);
            }

            dataSet.add(new TestData(dbNum, key, data));
        }
    }

    /**
     * Remove from the set of expected results.
     */
    private void removeExpectedData(Map<TestData, Set<TestData>> expectedData,
                                    int dbNum,
                                    DatabaseEntry key,
                                    DatabaseEntry data,
                                    boolean expectCommit) {
        if (expectCommit) {
            TestData keyTestData = new TestData(dbNum, key, null);
            Set<TestData> dataSet = expectedData.get(keyTestData);
            assertTrue("Should be a data set for " + keyTestData,
                       (dataSet != null));
            assertTrue("Should be able to remove key " + key +
                       " from expected data ",
                       dataSet.remove(new TestData(dbNum, key, data)));
            if (dataSet.size() == 0) {
                expectedData.remove(keyTestData);
            }
        }
    }

    /**
     * @return a set of cursors for the test databases.
     */
    private Cursor[] getCursors(Transaction txn, int startDb, int endDb)
        throws DatabaseException {

        Cursor[] cursors = new Cursor[endDb - startDb];
        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = dbs[startDb + i].openCursor(txn, null);
        }
        return cursors;
    }

    /**
     * Insert the given record into all databases.
     */
    private void insertOneRecord(Cursor[] cursors,
                                 int val,
                                 Map<TestData, Set<TestData>> expectedData,
                                 int nDuplicatesPerKey,
                                 boolean expectCommit)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int c = 0; c < cursors.length; c++) {

            int testVal = val + c;
            byte[] keyData = TestUtils.getTestArray(testVal);
            byte[] dataData = TestUtils.byteArrayCopy(keyData);
            key.setData(keyData);

            for (int d = 0; d < nDuplicatesPerKey; d++) {
                dataData = TestUtils.byteArrayCopy(dataData);
                dataData[1]++;
                data.setData(dataData);

                //System.out.println("Inserting record with key: " + testVal);

                assertEquals("Insertion of key " +
                             TestUtils.dumpByteArray(keyData),
                             OperationStatus.SUCCESS,
                             cursors[c].putNoDupData(key, data));

                addExpectedData(expectedData, c, key, data, expectCommit);
            }
        }
    }

    /**
     * Delete either every other or all data.
     */
    protected void deleteData(Transaction txn,
                              Map<TestData, Set<TestData>> expectedData,
                              boolean all,
                              boolean expectCommit,
                              int numDbs)
        throws DatabaseException {

        Cursor[] cursors = getCursors(txn, 0, numDbs);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int d = 0; d < cursors.length; d++) {
            OperationStatus status =
                cursors[d].getFirst(key, data, LockMode.DEFAULT);
            boolean toggle = true;
            int deleteCount = 0;
            while (status == OperationStatus.SUCCESS) {
                if (toggle) {
                    removeExpectedData(expectedData, d, key, data,
                                       expectCommit);
                    assertEquals(OperationStatus.SUCCESS, cursors[d].delete());
                    deleteCount++;
                    toggle = all;
                } else {
                    toggle = true;
                }
                status = cursors[d].getNext(key, data, LockMode.DEFAULT);
            }
            /* Make sure the test deletes something! */
            assertTrue(deleteCount > 0);
        }

        for (int i = 0; i < cursors.length; i++) {
            cursors[i].close();
        }
    }

    /**
     * Modify data
     * @param txn owning txn
     * @param endVal end point of the modification range
     * @param expectedData store of expected values for verification at end
     * @param increment used to modify the data.
     * @param expectCommit if true, reflect change in expected map. Sometimes
     *         we don't want to do this because we plan to make the txn abort.
     */
    protected void modifyData(Transaction txn,
                              int endVal,
                              Map<TestData, Set<TestData>> expectedData,
                              int increment,
                              boolean expectCommit,
                              int numDbs)
        throws DatabaseException {

        Cursor[] cursors = getCursors(txn, 0, numDbs);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int d = 0; d < cursors.length; d++) {

            /* Position cursor at the start value. */
            OperationStatus status =
                cursors[d].getFirst(key, data, LockMode.DEFAULT);

            /* For each record within the range, change the data. */
            int modCount = 0;
            int keyVal = TestUtils.getTestVal(key.getData());

            while ((status == OperationStatus.SUCCESS) && (keyVal <= endVal)) {

                /* Change the data. */
                removeExpectedData(expectedData, d, key, data, expectCommit);
                data.setData(TestUtils.getTestArray(keyVal + increment));
                cursors[d].delete();
                cursors[d].put(key, data);
                addExpectedData(expectedData, d, key, data, expectCommit);
                modCount++;

                status = cursors[d].getNext(key, data, LockMode.DEFAULT);

                if (status == OperationStatus.SUCCESS) {
                    keyVal = TestUtils.getTestVal(key.getData());
                }
            }
            /* Make sure we modify something! */
            assertTrue(modCount > 0);
        }

        for (int i = 0; i < cursors.length; i++) {
            cursors[i].close();
        }
    }

    /**
     * Print the contents of the databases out for debugging
     */
    protected void dumpData(int numDbs)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        if (btreeComparisonFunction != null) {
            dbConfig.setBtreeComparator
                ((Class<? extends Comparator<byte[]>>)
                 btreeComparisonFunction.getClass());
        }
        dbConfig.setSortedDuplicates(true);
        dbConfig.setTransactional(true);
        for (int d = 0; d < numDbs; d++) {
            Database checkDb = env.openDatabase(null, DB_NAME + d, dbConfig);
            Cursor myCursor = checkDb.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            OperationStatus status =
                myCursor.getFirst(key, data, LockMode.DEFAULT);
            while (status == OperationStatus.SUCCESS) {
                System.out.println("Database " + d +
                                   " seen = " +
                                   /*
                                      new String(key.getData(), UTF8) +
                                      "/" +
                                      new String(data.getData(), UTF8));
                                   */
                                   TestUtils.dumpByteArray(key.getData()) +
                                   "/" +
                                   TestUtils.dumpByteArray(data.getData()));
                status = myCursor.getNext(key, data, LockMode.DEFAULT);
            }
            myCursor.close();
        }
    }

    /**
     * Print the contents of the expected map for debugging.
     */
    protected void dumpExpected(Map<TestData, Set<TestData>> expectedData) {
        System.out.println("Expected = " );
        Iterator<Set<TestData>> iter = expectedData.values().iterator();
        while (iter.hasNext()) {
            Set<TestData> dataSet = iter.next();
            Iterator<TestData> dataIter = dataSet.iterator();
            while (dataIter.hasNext()) {
                TestData t = dataIter.next();
                System.out.println(t);
            }
        }
    }

    protected class TestData {
        public int dbNum;
        public byte[] key;
        public byte[] data;

        TestData(int dbNum, DatabaseEntry keyDbt, DatabaseEntry dataDbt) {
            this.dbNum = dbNum;
            key = keyDbt.getData();
            if (dataDbt == null) {
                dataDbt = new DatabaseEntry();
                dataDbt.setData(new byte[1]);
            }
            data = dataDbt.getData();
        }

        TestData(int dbNum, byte[] key) {
            this.dbNum = dbNum;
            this.key = key;
        }

        @Override
        public boolean equals(Object o ) {
            if (this == o)
                return true;
            if (!(o instanceof TestData))
                return false;

            TestData other = (TestData) o;
            if ((dbNum == other.dbNum) &&
                Arrays.equals(key, other.key) &&
                Arrays.equals(data, other.data)) {
                return true;
            } else
                return false;
        }

        @Override
        public String toString() {
            if (data == null) {
                return "db=" + dbNum +
                    " k=" + TestUtils.dumpByteArray(key);
            } else {
                return "db=" + dbNum +
                    " k=" + TestUtils.dumpByteArray(key) +
                    " d=" + TestUtils.dumpByteArray(data);
            }
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }
    }
}
