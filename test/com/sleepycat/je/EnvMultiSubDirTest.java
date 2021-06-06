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

package com.sleepycat.je;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test multiple environment data directories. 
 */
public class EnvMultiSubDirTest extends TestBase {
    private static final String DB_NAME = "testDb";
    private static final String keyPrefix = "herococo";
    private static final String dataValue = "abcdefghijklmnopqrstuvwxyz";

    private final File envHome;
    private final int N_DATA_DIRS = 3;

    public EnvMultiSubDirTest() {
        envHome = SharedTestUtils.getTestDir();
    }
    
    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        TestUtils.createEnvHomeWithSubDir(envHome, N_DATA_DIRS);
    }

    /* Test the basic CRUD operations with multiple data directories. */
    @Test
    public void testSubDirBasic()
        throws Throwable {

        doTestWork(false, false);
    }

    /* Test deferred write with multiple data directories. */
    @Test
    public void testSubDirDeferredWrite()
        throws Throwable {

        doTestWork(true, false);
    }

    /* Test transactional environment with multiple data directories. */
    @Test
    public void testSubDirTransactional()
        throws Throwable {

        doTestWork(false, true);
    }

    private void doTestWork(boolean deferredWrite, boolean transactional)
        throws Throwable {

        EnvironmentConfig envConfig = createEnvConfig(transactional);
        Environment env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(deferredWrite);
        dbConfig.setTransactional(transactional);
        Database db = env.openDatabase(null, DB_NAME, dbConfig);

        /* Do updates on the database. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= 2000; i++) {
            StringBinding.stringToEntry(keyPrefix + i, key);
            StringBinding.stringToEntry(dataValue, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        for (int i = 1; i <= 1000; i++) {
            StringBinding.stringToEntry(keyPrefix + i, key);
            assertEquals(OperationStatus.SUCCESS, db.delete(null, key));
        }

        for (int i = 1001; i <= 2000; i++) {
            StringBinding.stringToEntry(keyPrefix + i, key);
            StringBinding.stringToEntry(dataValue + dataValue, data);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        /* Check the contents of the current database. */
        assertEquals(1000, db.count());
        checkDbContents(db, 1001, 2000);
        db.close();
        env.close();

        /* Make sure reopen is OK. */
        env = new Environment(envHome, envConfig);
        db = env.openDatabase(null, DB_NAME, dbConfig);
        assertEquals(1000, db.count());
        checkDbContents(db, 1001, 2000);
        db.close();
        env.close();

        /* Check that log files in the sub directories are round-robin. */
        for (int i = 1; i <= N_DATA_DIRS; i++) {
            File subDir = new File(envHome, TestUtils.getSubDirName(i));
            File[] logFiles = subDir.listFiles();
            for (File logFile : logFiles) {
                if (logFile.getName().endsWith("jdb") && logFile.isFile()) {
                    String fileNumber = logFile.getName().substring
                        (0, logFile.getName().indexOf("."));
                    int number = 
                        Integer.valueOf(Integer.parseInt(fileNumber, 16));
                    assertTrue((number % N_DATA_DIRS) == (i - 1));
                }
            }
        }
    }

    private EnvironmentConfig createEnvConfig(boolean transactional) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(transactional);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                                 N_DATA_DIRS + "");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "10000");
        envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
                                 "20000");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_BYTES_INTERVAL,
                                 "10000");
        return envConfig;
    }

    /* Test that log files should stay in the correct sub directory. */
    @Test
    public void testLogFilesDirCheck()
        throws Throwable {

        /* Generating some log files. */
        doTestWork(false, false);

        /* Copy the log files from one sub directory to another. */
        File[] files = envHome.listFiles();
        String copySubName = null;
        for (File file : files) {
            if (file.isDirectory() && file.getName().startsWith("data")) {
                if (copySubName == null) {
                    copySubName = file.getName();
                } else {
                    assertTrue(!copySubName.equals(file.getName()));
                    SharedTestUtils.copyFiles
                        (new File(envHome, copySubName),
                         new File(envHome, file.getName()));
                    break;
                }
            }
        }           

        try {
            new Environment(envHome, createEnvConfig(false));
            fail("Expected to see exceptions.");
        } catch (EnvironmentFailureException e) {
            /* Expected exceptions. */
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
    }

    private void checkDbContents(Database db, int start, int end)
        throws Exception {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor c = db.openCursor(null, null);
        for (int i = start; i <= end; i++) {
            assertEquals("i=" + i,
                OperationStatus.SUCCESS,
                c.getNext(key, data, null));
            assertEquals(keyPrefix + i, StringBinding.entryToString(key));
            assertEquals(dataValue + dataValue,
                StringBinding.entryToString(data));
        }
        assertEquals(OperationStatus.NOTFOUND, c.getNext(key, data, null));
        c.close();
    }
}
