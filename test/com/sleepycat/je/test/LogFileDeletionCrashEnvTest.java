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

package com.sleepycat.je.test;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import com.sleepycat.bind.tuple.IntegerBinding;
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
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * Test the unexpected log file deletion detect code.
 * With this functionality, EnvironmentFailureException can be thrown asap.
 * Without this functionality, Read access can proceed even when log files
 * are deleted unexpectedly.
 */
public class LogFileDeletionCrashEnvTest extends TestBase {
    private Environment env;
    private Database db;
    private File envHome;
    private Cursor c;

    private final int recNum = 1000 * 50; //(1000 * 500) * 50 files
    private final int dataLen = 500;
    private static final int dirs = 3;

    private static final EnvironmentConfig envConfigWithDetectSingle
        = initConfig();
    private static final EnvironmentConfig envConfigWithoutDetectSingle
        = initConfig();
    private static final EnvironmentConfig envConfigWithDetectMulti
        = initConfig();
    private static final EnvironmentConfig envConfigWithoutDetectMulti
        = initConfig();

    static {
        envConfigWithoutDetectSingle.setConfigParam(
            EnvironmentParams.LOG_DETECT_FILE_DELETE.getName(), "false");

        envConfigWithDetectMulti.setConfigParam(
            EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(), dirs + "");

        envConfigWithoutDetectMulti.setConfigParam(
            EnvironmentParams.LOG_DETECT_FILE_DELETE.getName(), "false");
        envConfigWithoutDetectMulti.setConfigParam(
            EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(), dirs + "");
    }

    @Before
    public void setUp() 
        throws Exception {
        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @After
    public void tearDown() 
        throws Exception {

        if (c != null) {
            try {
                c.close(); 
            } catch (EnvironmentFailureException efe) {
                // do nothing
            }
            c = null;
        }

        if (db != null) {
            try {
                db.close(); 
            } catch (EnvironmentFailureException efe) {
                // do nothing
            }
            db = null;
        }

        if (env != null) {
            try {
                env.close();
            } catch (EnvironmentFailureException efe) {
                // do nothing
            }
            env = null;
        }

        super.tearDown();
    }

    private static EnvironmentConfig initConfig() {
        EnvironmentConfig config = TestUtils.initEnvConfig();
        config.setAllowCreate(true);
        config.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
            "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
            "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");
        config.setCacheSize(1000000);
        config.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000");
        return config;
    }

    /*
     * The following 4 test cases test log file deletion.
     */
    @Test
    public void testLogDeleteWithDetectForSingleEnv() {
        testLogFileDeletionInternal(envConfigWithDetectSingle);
    }

    @Test
    public void testLogDeleteWithoutDetectForSingleEnv() {
        testLogFileDeletionInternal(envConfigWithoutDetectSingle);
    }

    @Test
    public void testLogDeleteWithDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testLogFileDeletionInternal(envConfigWithDetectMulti);
    }

    @Test
    public void testLogDeleteWithoutDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testLogFileDeletionInternal(envConfigWithoutDetectMulti);
    }

    /*
     * The following 6 test cases test directly directory deletion.
     */
    @Test
    public void testRootDirDeleteWithDetectForSingleEnv() {
        testDirDeletionInternal(envConfigWithDetectSingle, "envHome", null);
    }

    @Test
    public void testRootDirDeleteWithoutDetectForSingleEnv() {
        testDirDeletionInternal(envConfigWithoutDetectSingle, "envHome", null);
    }

    @Test
    public void testRootDirDeleteWithDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithDetectMulti, "envHome", null);
    }

    @Test
    public void testRootDirDeleteWithoutDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithoutDetectMulti, "envHome", null);
    }

    @Test
    public void testDataDirDeleteWithDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithDetectMulti, "datadir", null);
    }

    @Test
    public void testDataDirDeleteWithoutDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithoutDetectMulti, "datadir", null);
    }

    /*
     * The following 6 test cases test directory rename.
     */
    @Test
    public void testRootRenameWithDetectForSingleEnv() {
        testDirDeletionInternal(envConfigWithDetectSingle, "rename", true);
    }

    @Test
    public void testRootRenameWithoutDetectForSingleEnv() {
        testDirDeletionInternal(envConfigWithoutDetectSingle, "rename", true);
    }

    @Test
    public void testDataDirRenameWithDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithDetectMulti, "rename", false);
    }

    @Test
    public void testDataDirRenameWithoutDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithoutDetectMulti, "rename", false);
    }

    @Test
    public void testRootRenameWithDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithDetectMulti, "rename", true);
    }

    @Test
    public void testRootRenameWithoutDetectForMultiEnv() {
        TestUtils.createEnvHomeWithSubDir(envHome, dirs);
        testDirDeletionInternal(envConfigWithoutDetectMulti, "rename", true);
    }

    private void testLogFileDeletionInternal(EnvironmentConfig config) {
        openEnvAndDb(config);
        initialDb();
        /* The first pass traverse to add file handles to fileCache. */
        tranverseDb(false);
        deleteFiles();
        tranverseDb(true);
    }

    private void testDirDeletionInternal(
        EnvironmentConfig config,
        String action,
        Boolean root) {

        openEnvAndDb(config);
        initialDb();
        /* The first pass traverse to add file handles to fileCache. */
        tranverseDb(false);
        deleteDir(action, root);
        tranverseDb(true);
    }

    public void openEnvAndDb(EnvironmentConfig config) {
        env = new Environment(envHome, config);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        final String dbName = "tempDB";
        db = env.openDatabase(null, dbName, dbConfig);

        c = db.openCursor(null, null);
    }

    public void initialDb() {
        try {
            for (int i = 0 ; i < recNum; i++) {
                final DatabaseEntry key = new DatabaseEntry();
                IntegerBinding.intToEntry(i, key);
                final DatabaseEntry data = new DatabaseEntry(new byte[dataLen]);
                db.put(null, key, data);
            }
        } catch (DatabaseException dbe) {
            throw new RuntimeException("Initiate Database fails.", dbe);
        }

        final int totalFiles =
            DbInternal.getEnvironmentImpl(env).getFileManager().                            
            getAllFileNumbers().length;
        assert totalFiles < 100 : "Total file number is " + totalFiles;
    }

    public void deleteFiles() {
        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
        final FileManager fm = envImpl.getFileManager();
        int nDirs = envImpl.getConfigManager().getInt(
            EnvironmentParams.LOG_N_DATA_DIRECTORIES);
        if (nDirs == 0) {
            nDirs = 1;
        }
        final File[] files = fm.listJDBFiles();
        for (int index = 0; index < files.length - nDirs; index++) {
            files[index].delete();
        }

        final int totalFiles =
            DbInternal.getEnvironmentImpl(env).getFileManager().                            
            getAllFileNumbers().length;
        assert totalFiles == nDirs : "Total file number is " + totalFiles;
    }

    public void deleteDir(String action, Boolean root) {
        String shellCmd = "";
        try {
            String envHomePath = envHome.getCanonicalPath();
            String dataDirPath = envHomePath + "/data001";
            String envHomePathNew = envHomePath + ".new";
            String dataDirPathNew = dataDirPath + ".new" ;

            if (action.equals("envHome")) {
                shellCmd = "rm -rf " + envHomePath;
            } else if (action.equals("datadir")) {
                shellCmd = "rm -rf " + dataDirPath;
            } else if (action.equals("rename")) {
                if (root.booleanValue()) {
                    shellCmd =
                        "mv " + envHomePath + " " + envHomePathNew +
                        " && " + "sleep 5" +
                        " && " + "rm -rf " + envHomePathNew;
                } else {
                    shellCmd = "mv " + dataDirPath + " " + dataDirPathNew;
                }
            }

            final ProcessBuilder pb =
                new ProcessBuilder("/bin/bash", "-c", shellCmd); 
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            if (p != null) {
                final int retvalue = p.waitFor();
                if (retvalue != 0) {
                    throw new IOException(
                        "The created process exit abnormally");
                }
            } else {
                throw new IOException("The created process is null");
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "Some error happens when executing " + shellCmd, e);
        }
    }

    public void tranverseDb(boolean check) {
        boolean detect = DbInternal.getEnvironmentImpl(env).getConfigManager().
            getBoolean(EnvironmentParams.LOG_DETECT_FILE_DELETE);
        try {
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            assert c.getFirst(key, data, null) == OperationStatus.SUCCESS :
                "The db should contain at least one record";
            /* Sleep at least 1s to let the TimerTask to execute. */
            try {Thread.sleep(1000);} catch (Exception e) {}
            while (c.getNext(key, data, null) == OperationStatus.SUCCESS) {
                //Do nothing
            }

            if (check) {
                if (detect) {
                    fail("With detecting log file deletion, we should catch" +
                        "EnvironmentFailureException.");
                }
            }
        } catch (EnvironmentFailureException efe) {
            if (check) {
                if (!detect) {
                    efe.printStackTrace();
                    fail("Without detecting log file deletion, we should" +
                        "not catch EnvironmentFailureException");
                }
            }
            // Leave tearDown() to close cursor, db and env.
        }
    }
}