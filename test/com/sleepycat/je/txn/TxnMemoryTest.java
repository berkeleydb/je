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

package com.sleepycat.je.txn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

@RunWith(Parameterized.class)
public class TxnMemoryTest extends DualTestCase {
    private static final boolean DEBUG = false;
    private static final String DB_NAME = "foo";

    private static final String LOCK_AUTOTXN = "lock-autotxn";
    private static final String LOCK_USERTXN  = "lock-usertxn";
    private static final String LOCK_NOTXN  = "lock-notxn";
    private static final String COMMIT = "commit";
    private static final String ABORT = "abort";
    private static final String[] END_MODE = {COMMIT, ABORT};

    private final File envHome;
    private Environment env;
    private EnvironmentImpl envImpl;
    private MemoryBudget mb;
    private Database db;
    private final DatabaseEntry keyEntry = new DatabaseEntry();
    private final DatabaseEntry dataEntry = new DatabaseEntry();
    private final String lockMode;
    private final String endMode;

    private long beforeAction;
    private long afterTxnsCreated;
    private long afterAction;
    private Transaction[] txns;

    private final int numTxns = 2;
    private final int numRecordsPerTxn = 30;
    
    @Parameters
    public static List<Object[]> genParams() {
        return paramsHelper(false);
    }
    
    public static List<Object[]> paramsHelper(boolean rep) {
        String[] testModes = null;
        
        if (rep){
            testModes = new String[] {LOCK_USERTXN};
        } else {
            testModes = new String[] {LOCK_AUTOTXN, LOCK_USERTXN, LOCK_NOTXN};
        }
        List<Object[]> list = new ArrayList<Object[]>();
        
        for (String testMode : testModes) {
            for (String eMode : END_MODE) {
                list.add(new Object[] {testMode, eMode});
            }
        }
       
        return list;
    }
    
    public TxnMemoryTest(String testMode, String eMode) {
        envHome = SharedTestUtils.getTestDir();
        this.lockMode = testMode;
        this.endMode = eMode;
        customName = lockMode + '-' + endMode;
    }

    @Before
    public void setUp() 
        throws Exception {

        super.setUp();

        IN.ACCUMULATED_LIMIT = 0;
        Txn.ACCUMULATED_LIMIT = 0;

    }

    /**
     * Opens the environment and database.
     */
    private void openEnv()
        throws DatabaseException {

        EnvironmentConfig config = TestUtils.initEnvConfig();

        /*
         * ReadCommitted isolation is not allowed by this test because we
         * expect no locks/memory to be freed when using a transaction.
         */
        DbInternal.setTxnReadCommitted(config, false);

        /* Cleaner detail tracking adds to the memory budget; disable it. */
        config.setConfigParam
            (EnvironmentParams.CLEANER_TRACK_DETAIL.getName(), "false");

        config.setTransactional(true);
        config.setAllowCreate(true);
        env = create(envHome, config);
        envImpl = DbInternal.getNonNullEnvImpl(env);
        mb = envImpl.getMemoryBudget();

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(!lockMode.equals(LOCK_NOTXN));
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
    }

    /**
     * Closes the environment and database.
     */
    private void closeEnv(boolean doCheckpoint)
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            close(env);
            env = null;
        }
    }

    /**
     * Insert and then update some records. Measure memory usage at different
     * points in this sequence, asserting that the memory usage count is
     * properly decremented.
     */
    @Test
    public void testWriteLocks()
        throws DatabaseException {

        loadData();

        /*
         * Now update the database transactionally. This should not change
         * the node related memory, but should add txn related cache
         * consumption. If this is a user transaction, we should
         * hold locks and consume more memory.
         */
        for (int t = 0; t < numTxns; t++) {
            for (int i = 0; i < numRecordsPerTxn; i++) {
                int value = i + (t*numRecordsPerTxn);
                IntegerBinding.intToEntry(value, keyEntry);
                IntegerBinding.intToEntry(value+1, dataEntry);
                assertEquals(db.put(txns[t], keyEntry, dataEntry),
                             OperationStatus.SUCCESS);
            }
        }
        afterAction = mb.getLockMemoryUsage();

        closeTxns(true);
    }

    /**
     * Insert and then scan some records. Measure memory usage at different
     * points in this sequence, asserting that the memory usage count is
     * properly decremented.
     */
    @Test
    public void testReadLocks()
        throws DatabaseException {

        loadData();

        /*
         * Now scan the database. Make sure all locking overhead is
         * released.
         */
        for (int t = 0; t < numTxns; t++) {
            Cursor c = db.openCursor(txns[t], null);
            while (c.getNext(keyEntry, dataEntry, null) ==
                   OperationStatus.SUCCESS) {
            }
            c.close();
        }
        afterAction = mb.getLockMemoryUsage();

        closeTxns(false);
    }

    private void loadData()
        throws DatabaseException {

        openEnv();

        /* Build up a database to establish a given cache size. */
        for (int t = 0; t < numTxns; t++) {
            for (int i = 0; i < numRecordsPerTxn; i++) {

                int value = i + (t*numRecordsPerTxn);
                IntegerBinding.intToEntry(value, keyEntry);
                IntegerBinding.intToEntry(value, dataEntry);
                assertEquals(db.put(null, keyEntry, dataEntry),
                             OperationStatus.SUCCESS);
            }
        }

        beforeAction = mb.getLockMemoryUsage();

        /* Make some transactions. */
        txns = new Transaction[numTxns];
        if (lockMode.equals(LOCK_USERTXN)) {
            for (int t = 0; t < numTxns; t++) {
                txns[t] = env.beginTransaction(null, null);
            }

            afterTxnsCreated = mb.getLockMemoryUsage();
            assertTrue( "afterTxns=" + afterTxnsCreated +
                        "beforeUpdate=" + beforeAction,
                        (afterTxnsCreated > beforeAction));
        }
    }

    private void closeTxns(boolean writesDone)
        throws DatabaseException {

        assertTrue(afterAction > afterTxnsCreated);

        /*
         * If this is not a user transactional lock, we should be done
         * with all locking overhead. If it is a user transaction, we
         * only release memory after locks are released at commit or
         * abort.
         */
        if (lockMode.equals(LOCK_USERTXN)) {

            /*
             * Note: expectedLockUsage is annoyingly fragile. If we change
             * the lock implementation, this may not be the right number
             * to check.
             */
            long expectedLockUsage =
                   (numRecordsPerTxn * numTxns *
                    MemoryBudget.THINLOCKIMPL_OVERHEAD);

            assertTrue((afterAction - afterTxnsCreated) >= expectedLockUsage);

            for (int t = 0; t < numTxns; t++) {
                Transaction txn = txns[t];
                if (endMode.equals(COMMIT)) {
                    txn.commit();
                } else {
                    txn.abort();
                }
            }

            long afterTxnEnd = mb.getLockMemoryUsage();

            assertTrue("lockMode=" + lockMode +
                       " endMode=" + endMode +
                       " afterTxnEnd=" + afterTxnEnd +
                       " beforeAction=" + beforeAction,
                       (afterTxnEnd <= beforeAction));
        }
        if (DEBUG) {
            System.out.println("afterUpdate = " + afterAction +
                               " before=" + beforeAction);
        }

        closeEnv(true);
    }
}
