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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class DupConvertTest extends TestBase {
    private File envHome;
    private Environment env;
    Database db;
    private static DatabaseEntry theKey = new DatabaseEntry();
    private static DatabaseEntry theData = new DatabaseEntry();
    DatabaseEntry findKey = new DatabaseEntry();
    DatabaseEntry findData = new DatabaseEntry();
    private static int N_ENTRIES = 4;
    private final String singletonLN_jdb = "je-4.1.7_logWithSingletonLN.jdb";
    private final String DIN_jdb = "je-4.1.7_logWithDIN.jdb";
    private final String DeletedLNCommit_jdb =
        "je-4.1.7_logWithDeletedLNCommit.jdb";
    private final String DeletedLNNoCommit_jdb = 
        "je-4.1.7_logWithDeletedLNNoCommit.jdb";
    private final String MixIN_jdb = "je-4.1.7_logWithMixIN.jdb";

    /*
     * Logs where the preupgrade utility has not been run, and an exception
     * should be thrown by recovery.
     */
    private final String NoPreUpgrade_Dups_jdb = "je-4.1.7_noPreUpgrade_dups";
    private final String NoPreUpgrade_Deltas_jdb =
        "je-4.1.7_noPreUpgrade_deltas";

    @Before
    public void setUp() 
        throws Exception {
        
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }
    
    @After
    public void tearDown() {
        try {
            if (env != null) {
                env.close();
            }
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }
        envHome = null;
        env = null;
    }
    
    private void openEnv(String logName, boolean readOnly, boolean loadLog)
        throws DatabaseException, IOException {
        /* Copy log file resource to log file zero. */
        if (loadLog) {
            TestUtils.loadLog(getClass(), logName, envHome);
        }

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(readOnly);
        envConfig.setTransactional(true);
        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), 
             Integer.toString(N_ENTRIES));
        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        TestUtils.validateNodeMemUsage(envImpl, true /*assertOnError*/);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(false);
        dbConfig.setReadOnly(readOnly);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    private void closeEnv()
        throws DatabaseException {

        db.close();
        db = null;
        env.close();
        env = null;
    }
    
    @Test
    public void testLogWithSingletonLN() 
        throws DatabaseException, IOException {

        openEnv(singletonLN_jdb, false, true);

        /* Verify the records are correctly read. */
        Cursor cur = db.openCursor(null, null);
        try {
            for(int i = 0; i < 100; i++) {
                theKey = makeEntry(i);
                theData = makeEntry(i);
                cur.getNext(findKey, findData, null);
                assertEquals(theKey, findKey);
                assertEquals(theData, findData);
            } 
        } finally {
            cur.close();
        }

        insertNoDupRecordTest();
        insertDupRecordTest();
        deleteRecordTest();

        closeEnv();
    }
    
    @Test
    public void testLogWithDIN() 
        throws DatabaseException, IOException {

        openEnv(DIN_jdb, false, true);

        /* Verify the records are correctly read. */
        Cursor cur = db.openCursor(null, null);
        try {
            for(int i = 0; i < 10; i++) {
                for (int j = 0; j < 10; j++) {
                    theKey = makeEntry(i);
                    theData = makeEntry(j * 10 + i);
                    cur.getNext(findKey, findData, null);
                    assertEquals(theKey, findKey);
                    assertEquals(theData, findData);
                }
            } 
        } finally {
            cur.close();
        }

        insertNoDupRecordTest();
        insertDupRecordTest();
        deleteRecordTest();

        closeEnv();
    }
    
    @Test
    public void testLogWithDeleteLNCommit() 
        throws DatabaseException, IOException {

        openEnv(DeletedLNCommit_jdb, false, true);

        /* Verify the records are correctly read. */
        Cursor cur = db.openCursor(null, null);
        try {
            for(int i = 0; i < 10; i++) {
                /* Have deleted half of the dup records. */
                for (int j = 5; j < 10; j++) {
                    theKey = makeEntry(i);
                    theData = makeEntry(j * 10 + i);
                    cur.getNext(findKey, findData, null);
                    assertEquals(theKey, findKey);
                    assertEquals(theData, findData);
                }
            } 
        } finally {
            cur.close();
        }

        insertNoDupRecordTest();
        insertDupRecordTest();
        deleteRecordTest();

        closeEnv();
    }
    
    @Test
    public void testLogWithDeleteLNNoCommit() 
        throws DatabaseException, IOException {

        openEnv(DeletedLNNoCommit_jdb, false, true);

        /* Verify the records are correctly read. */
        Cursor cur = db.openCursor(null, null);
        try {
            for(int i = 0; i < 10; i++) {
                /* The delete actions have not been committed. */
                for (int j = 0; j < 10; j++) {
                    theKey = makeEntry(i);
                    theData = makeEntry(j * 10 + i);
                    cur.getNext(findKey, findData, null);
                    assertEquals(theKey, findKey);
                    assertEquals(theData, findData);
                }
            } 
        } finally {
            cur.close();
        }

        insertNoDupRecordTest();
        insertDupRecordTest();
        deleteRecordTest();

        closeEnv();
    }
    
    @Test
    public void testLogWithMixIN() 
        throws DatabaseException, IOException {

        openEnv(MixIN_jdb, false, true);

        /* Verify the records are correctly read. */
        Cursor cur = db.openCursor(null, null);
        try {
            for(int i = 0; i < 10; i++) {
                /* Have deleted some of the dup records. */
                for (int j = 3; j < 7; j++) {
                    theKey = makeEntry(i);
                    theData = makeEntry(j * 10 + i);
                    cur.getNext(findKey, findData, null);
                    assertEquals(theKey, findKey);
                    assertEquals(theData, findData);
                }
            } 
            for(int i = 70; i < 100; i++) {
                theKey = makeEntry(i);
                theData = makeEntry(i);
                cur.getNext(findKey, findData, null);
                assertEquals(theKey, findKey);
                assertEquals(theData, findData);
            }
        } finally {
            cur.close();
        }
        
        insertNoDupRecordTest();
        insertDupRecordTest();
        deleteRecordTest();

        closeEnv();
    }
    
    /**
     * Open a log with DBINs/DINs in the last checkpoint.  The preupgrade
     * utility was not run, so an exception should be thrown when opening this
     * environment with JE 5.
     */
    @Test
    public void testNoPreUpgradeDups()
        throws IOException {

        try {
            openEnv(NoPreUpgrade_Dups_jdb, false, true);
            fail();
        } catch (EnvironmentFailureException e) {
            e.getMessage().contains("PreUpgrade");
        }
    }
    
    /**
     * Open a log with deltas in the last checkpoint.  The preupgrade utility
     * was not run, so an exception should be thrown when opening this
     * environment with JE 5.
     */
    @Test
    public void testNoPreUpgradeDeltas()
        throws IOException {

        try {
            openEnv(NoPreUpgrade_Deltas_jdb, false, true);
            fail();
        } catch (EnvironmentFailureException e) {
            e.getMessage().contains("PreUpgrade");
        }
    }
    
    private void insertNoDupRecordTest() {
        OperationStatus status;
        theKey = makeEntry(100);
        theData = makeEntry(100);
        status = db.putNoOverwrite(null, theKey, theData);
        assertEquals(OperationStatus.SUCCESS, status);
    }

    private void insertDupRecordTest() {
        OperationStatus status;
        theKey = makeEntry(5);
        theData = makeEntry(101);
        status = db.putNoOverwrite(null, theKey, theData);
        assertEquals(OperationStatus.KEYEXIST, status);
        status = db.put(null, theKey, theData);
        assertEquals(OperationStatus.SUCCESS, status);
    }

    private void deleteRecordTest() {
        OperationStatus status;
        status = db.delete(null, makeEntry(6));
        assertEquals(OperationStatus.SUCCESS, status);
        status = db.get(null, makeEntry(6), findData, null);
        assertEquals(OperationStatus.NOTFOUND, status);
    }
    
    private DatabaseEntry makeEntry(int val) {
        byte[] data = new byte[] { (byte) val }; 
        return new DatabaseEntry(data);
    }
}
