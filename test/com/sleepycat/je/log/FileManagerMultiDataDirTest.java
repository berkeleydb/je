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

package com.sleepycat.je.log;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * File Manager
 */
public class FileManagerMultiDataDirTest extends TestBase {

    private static int FILE_SIZE = 120;

    private static int N_DATA_DIRS = 3;

    private Environment env;
    private FileManager fileManager;
    private final File envHome;

    public FileManagerMultiDataDirTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown()
        throws IOException, DatabaseException {

        if (fileManager != null) {
            fileManager.clear();
            fileManager.close();
        }
    }

    private void createEnvAndFileManager()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 new Integer(FILE_SIZE).toString());
        /* Yank the cache size way down. */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_CACHE_SIZE.getName(), "3");
        envConfig.setAllowCreate(true);

        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
             N_DATA_DIRS + "");
        for (int i = 1; i <= N_DATA_DIRS; i += 1) {
            new File(envHome, "data00" + i).mkdir();
        }

        env = new Environment(envHome, envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /* Make a standalone file manager for this test. */
        envImpl.close();
        envImpl.open(); /* Just sets state to OPEN. */
        fileManager = new FileManager(envImpl, envHome, false);

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }

    @Test
    public void testMultipleDataDirs1()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* Create 000, 001, 002, 003, expect failure because of 000 */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
             N_DATA_DIRS + "");
        for (int i = 1; i <= N_DATA_DIRS; i += 1) {
            new File(envHome, "data00" + i).mkdir();
        }
        new File(envHome, "data000").mkdir();

        try {
            env = new Environment(envHome, envConfig);
            fail("expected too many dirs exception");
        } catch (EnvironmentFailureException EFE) {
        }

        File dataDir = new File(envHome, "data000");
        TestUtils.removeFiles
            ("TearDown", dataDir, FileManager.JE_SUFFIX);
        dataDir.delete();

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }

    @Test
    public void testMultipleDataDirs2()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* Create 001, 002, 004, expect failure because 003 doesn't exist */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
             N_DATA_DIRS + "");
        new File(envHome, "data001").mkdir();
        new File(envHome, "data002").mkdir();
        new File(envHome, "data004").mkdir();

        try {
            env = new Environment(envHome, envConfig);
            fail("expected too many dirs exception");
        } catch (EnvironmentFailureException EFE) {
        }

        File dataDir = new File(envHome, "data004");
        TestUtils.removeFiles
            ("TearDown", dataDir, FileManager.JE_SUFFIX);
        dataDir.delete();

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }

    @Test
    public void testMultipleDataDirs3()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* Create 001, 002, expect failure because 003 doesn't exist */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
             N_DATA_DIRS + "");
        new File(envHome, "data001").mkdir();
        new File(envHome, "data002").mkdir();

        try {
            env = new Environment(envHome, envConfig);
            fail("expected too many dirs exception");
        } catch (EnvironmentFailureException EFE) {
        }

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }

    @Test
    public void testMultipleDataDirs4()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* Create 001, 002, 003, expect failure because 003 is a file */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
             N_DATA_DIRS + "");
        new File(envHome, "data001").mkdir();
        new File(envHome, "data002").mkdir();
        new File(envHome, "data003").createNewFile();

        try {
            env = new Environment(envHome, envConfig);
            fail("expected too many dirs exception");
        } catch (EnvironmentFailureException EFE) {
        }

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }

    @Test
    public void testMultipleDataDirs5()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* Create 001, 002, xxx, expect failure because xxx is not NNN */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
             N_DATA_DIRS + "");
        new File(envHome, "data001").mkdir();
        new File(envHome, "data002").mkdir();
        new File(envHome, "dataxxx").mkdir();

        try {
            env = new Environment(envHome, envConfig);
            fail("expected too many dirs exception");
        } catch (EnvironmentFailureException EFE) {
        }

        File dataDir = new File(envHome, "dataxxx");
        TestUtils.removeFiles
            ("TearDown", dataDir, FileManager.JE_SUFFIX);
        dataDir.delete();

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }

    @Test
    public void testMultipleDataDirs6()
        throws Throwable {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* Create 001, 002, xxx, expect failure because xxx is not NNN */
        envConfig.setConfigParam
            (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(), "0");
        new File(envHome, "data001").mkdir();
        new File(envHome, "data002").mkdir();
        new File(envHome, "data003").mkdir();

        try {
            env = new Environment(envHome, envConfig);
            fail("expected too many dirs exception");
        } catch (EnvironmentFailureException EFE) {
        }

        File dataDir = new File(envHome, "dataxxx");
        TestUtils.removeFiles
            ("TearDown", dataDir, FileManager.JE_SUFFIX);
        dataDir.delete();

        /*
         * Remove any files after the environment is created again!  We want to
         * remove the files made by recovery, so we can test the file manager
         * in controlled cases.
         */
        TestUtils.removeFiles("Setup", envHome, FileManager.JE_SUFFIX);
    }
}
