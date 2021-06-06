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

package com.sleepycat.je.logversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.TestUtilLogReader;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

/**
 * Tests that prior versions of log entries can be read.  This test is used in
 * conjunction with MakeLogEntryVersionData, a main program that was used once
 * to generate log files named je-x.y.z.jdb, where x.y.z is the version of JE
 * used to create the log.  When a new test log file is created with
 * MakeLogEntryVersionData, add a new test_x_y_z() method to this class.
 *
 * @see MakeLogEntryVersionData
 */
public class LogEntryVersionTest extends TestBase {

    private File envHome;
    private Environment env;
    private Database db1;
    private Database db2;

    public LogEntryVersionTest() {
        envHome = SharedTestUtils.getTestDir();
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
        db1 = null;
        db2 = null;
    }

    private void openEnv(String jeVersion, boolean readOnly)
        throws DatabaseException, IOException {

        /* Copy log file resource to log file zero. */
        String resName = "je-" + jeVersion + ".jdb";
        TestUtils.loadLog(getClass(), resName, envHome);

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(readOnly);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);

        /* Validate mem usage with daemons disabled, then enable them. */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        TestUtils.validateNodeMemUsage(envImpl, true /*assertOnError*/);
        envConfig = env.getConfig();
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "true");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "true");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "true");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                                 "true");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "true");
        env.setMutableConfig(envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(false);
        dbConfig.setReadOnly(readOnly);
        dbConfig.setSortedDuplicates(true);
        db1 = env.openDatabase(null, Utils.DB1_NAME, dbConfig);
        db2 = env.openDatabase(null, Utils.DB2_NAME, dbConfig);
    }

    private void closeEnv()
        throws DatabaseException {

        db1.close();
        db1 = null;
        db2.close();
        db2 = null;
        env.close();
        env = null;
    }

    //@Test
    public void test_1_5_4()
        throws DatabaseException, IOException {

        doTest("1.5.4");
    }

    //@Test
    public void test_1_7_0()
        throws DatabaseException, IOException {

        doTest("1.7.0");
    }

    /**
     * JE 2.0: FileHeader version 3.
     */
    @Test
    public void test_2_0_0()
        throws DatabaseException, IOException {

        doTest("2.0.0");
    }

    /**
     * JE 3.0.12: FileHeader version 4.
     */
    @Test
    public void test_3_0_12()
        throws DatabaseException, IOException {

        /*
         * The test was not run until JE 3.1.25, but no format changes were
         * made between 3.0.12 and 3.1.25.
         */
        doTest("3.1.25");
    }

    /**
     * JE 3.2.79: FileHeader version 5. Version 5 was actually introduced in
     * 3.2.22
     */
    @Test
    public void test_3_2_79()
        throws DatabaseException, IOException {

        doTest("3.2.79");
    }

    /**
     * JE 3.3.78: FileHeader version 5. Version 5 was actually introduced in
     * 3.2.22
     */
    @Test
    public void test_3_3_78()
        throws DatabaseException, IOException {

        doTest("3.3.78");
    }

    @Test
    public void test_4_0_51()
        throws DatabaseException, IOException {

        doTest("4.0.51");
    }

    @Test
    public void test_5_0_39()
        throws DatabaseException, IOException {

        doTest("5.0.39");
    }

    @Test
    public void test_6_0_13()
        throws DatabaseException, IOException {

        doTest("6.0.13");
    }

    @Test
    public void test_6_2_12()
        throws DatabaseException, IOException {

        doTest("6.2.12");
    }

    @Test
    public void test_6_4_14()
        throws DatabaseException, IOException {

        doTest("6.4.14");
    }

    @Test
    public void test_7_0_6()
        throws DatabaseException, IOException {

        doTest("7.0.6");
    }

    @Test
    public void test_7_1_9()
        throws DatabaseException, IOException {

        doTest("7.1.9");
    }

    private void doTest(String jeVersion)
        throws DatabaseException, IOException {

        openEnv(jeVersion, true /*readOnly*/);

        VerifyConfig verifyConfig = new VerifyConfig();
        verifyConfig.setAggressive(true);
        assertTrue(env.verify(verifyConfig, System.err));

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status;

        /* Database 1 is empty because the txn was aborted. */
        Cursor cursor = db1.openCursor(null, null);
        try {
            status = cursor.getFirst(key, data, null);
            assertEquals(OperationStatus.NOTFOUND, status);
        } finally {
            cursor.close();
        }

        /* Database 2 has one record: {3, 0} */
        cursor = db2.openCursor(null, null);
        try {
            status = cursor.getFirst(key, data, null);
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(3, Utils.value(key));
            assertEquals(0, Utils.value(data));
            status = cursor.getNext(key, data, null);
            assertEquals(OperationStatus.NOTFOUND, status);
        } finally {
            cursor.close();
        }

        /*
         * Database 3 should have one record (99,79) that was explicitly
         * committed. We only added this commit record and test case when
         * implementing JE 3.3, and only went to the trouble of backporting the
         * MakeLogEntryVersionData to file version 5. (It's just an additional
         * test, it should be fine for earlier versions.)
         */
        if (!((jeVersion.startsWith("1")) ||
              (jeVersion.startsWith("2")) ||
              (jeVersion.startsWith("3.1")))) {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(true);
            Database db3 = env.openDatabase(null, Utils.DB3_NAME, dbConfig);

            cursor = db3.openCursor(null, null);
            try {
                status = cursor.getFirst(key, data, null);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(99, Utils.value(key));
                assertEquals(79, Utils.value(data));
                status = cursor.getNext(key, data, null);
                assertEquals(OperationStatus.NOTFOUND, status);
            } finally {
                cursor.close();
                db3.close();
            }
        }

        /*
         * Verify log entry types using a log reader. Read both full and
         * partial items.
         */
        String resName = "je-" + jeVersion + ".txt";
        LineNumberReader textReader = new LineNumberReader
            (new InputStreamReader(getClass().getResourceAsStream(resName)));
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        TestUtilLogReader logReader = new TestUtilLogReader(envImpl);

        String expectedType = textReader.readLine();
        while (expectedType != null) {
                /* Read the full item. */
            assertTrue(logReader.readNextEntry());
            String foundType = logReader.getEntryType().toString();
            assertEquals
                ("At line " + textReader.getLineNumber(),
                 expectedType.substring(0, expectedType.indexOf('/')),
                 foundType);

            assertEquals
                ("At line " + textReader.getLineNumber(),
                 expectedType.substring(0, expectedType.indexOf('/')),
                 foundType);

            expectedType = textReader.readLine();
        }
        assertTrue("This test should be sure to read some lines",
                                textReader.getLineNumber() > 0);
        assertFalse("No more expected entries after line " +
                           textReader.getLineNumber() + " but found: " +
                           logReader.getEntry(),
                           logReader.readNextEntry());

        assertTrue(env.verify(verifyConfig, System.err));
        closeEnv();

        /*
         * Do enough inserts to cause a split and perform some other write
         * operations for good measure.
         */
        openEnv(jeVersion, false /*readOnly*/);
        for (int i = -127; i < 127; i += 1) {
            status = db2.put(null, Utils.entry(i), Utils.entry(0));
            assertEquals(OperationStatus.SUCCESS, status);
        }
        /* Do updates. */
        for (int i = -127; i < 127; i += 1) {
            status = db2.put(null, Utils.entry(i), Utils.entry(1));
            assertEquals(OperationStatus.SUCCESS, status);
        }
        /* Do deletes. */
        for (int i = -127; i < 127; i += 1) {
            status = db2.delete(null, Utils.entry(i));
            assertEquals(OperationStatus.SUCCESS, status);
        }
        /* Same for duplicates. */
        for (int i = -127; i < 127; i += 1) {
            status = db2.put(null, Utils.entry(0), Utils.entry(i));
            assertEquals(OperationStatus.SUCCESS, status);
        }
        for (int i = -127; i < 127; i += 1) {
            status = db2.put(null, Utils.entry(0), Utils.entry(i));
            assertEquals(OperationStatus.SUCCESS, status);
        }
        cursor = db2.openCursor(null, null);
        try {
            status = cursor.getFirst(key, data, null);
            while (status == OperationStatus.SUCCESS) {
                status = cursor.delete();
                assertEquals(OperationStatus.SUCCESS, status);
                status = cursor.getNext(key, data, null);
            }
        } finally {
            cursor.close();
        }

        assertTrue(env.verify(verifyConfig, System.err));
        closeEnv();
    }
}
