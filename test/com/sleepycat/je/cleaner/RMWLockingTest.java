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

package com.sleepycat.je.cleaner;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
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
import com.sleepycat.je.util.TestUtils;

/**
 * Use LockMode.RMW and verify that the FileSummaryLNs accurately reflect only
 * those LNs that have been made obsolete.
 */
@RunWith(Parameterized.class)
public class RMWLockingTest extends CleanerTestBase {

    private static final int NUM_RECS = 5;

    private Database db;
    private DatabaseEntry key;
    private DatabaseEntry data;

    public RMWLockingTest(boolean multiSubDir) {
        envMultiSubDir = multiSubDir;
        customName = envMultiSubDir ? "multi-sub-dir" : null ;
    }
    
    @Parameters
    public static List<Object[]> genParams() {
        
        return getEnv(new boolean[] {false, true});
    }

    @After
    public void tearDown() 
        throws Exception {

        if (db != null) {
            db.close();
        }

        super.tearDown();

        db = null;
    }

    @Test
    public void testBasic()
        throws DatabaseException {

        init();
        insertRecords();
        rmwModify();

        UtilizationProfile up =
            DbInternal.getNonNullEnvImpl(env).getUtilizationProfile();

        /*
         * Checkpoint the environment to flush all utilization tracking
         * information before verifying.
         */
        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);
        env.checkpoint(ckptConfig);

        assertTrue(up.verifyFileSummaryDatabase());
    }

    /**
     * Tests that we can load a log file containing offsets that correspond to
     * non-obsolete LNs.  The bad log file was created using testBasic run
     * against JE 2.0.54.  It contains version 1 FSLNs, one of which has an
     * offset which is not obsolete.
     */
    @Test
    public void testBadLog()
        throws DatabaseException, IOException {

        /* Copy a log file with bad offsets to log file zero. */
        String resName = "rmw_bad_offsets.jdb";
        File destDir = envHome;
        if (envMultiSubDir) {
            destDir = new File(envHome, "data001");
        }
        TestUtils.loadLog(getClass(), resName, destDir);

        /* Open the log we just copied. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);

        /*
         * Verify the UP of the bad log.  Prior to adding the code in
         * FileSummaryLN.postFetchInit that discards version 1 offsets, this
         * assertion failed.
         */
        UtilizationProfile up =
            DbInternal.getNonNullEnvImpl(env).getUtilizationProfile();
        assertTrue(up.verifyFileSummaryDatabase());

        env.close();
        env = null;
    }

    private void init()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        if (envMultiSubDir) {
            envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                     DATA_DIRS + "");
        }
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    /* Insert records. */
    private void insertRecords()
        throws DatabaseException {

        key = new DatabaseEntry();
        data = new DatabaseEntry();

        IntegerBinding.intToEntry(100, data);

        for (int i = 0; i < NUM_RECS; i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }
    }

    /* lock two records with RMW, only modify one. */
    private void rmwModify()
        throws DatabaseException {

        Transaction txn = env.beginTransaction(null, null);
        IntegerBinding.intToEntry(0, key);
        assertEquals(OperationStatus.SUCCESS,
                     db.get(txn, key, data, LockMode.RMW));
        IntegerBinding.intToEntry(1, key);
        assertEquals(OperationStatus.SUCCESS,
                     db.get(txn, key, data, LockMode.RMW));

        IntegerBinding.intToEntry(200, data);
        assertEquals(OperationStatus.SUCCESS,
                     db.put(txn, key, data));
        txn.commit();
    }
}
