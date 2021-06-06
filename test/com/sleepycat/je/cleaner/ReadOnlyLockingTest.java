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


import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitProcessThread.OutErrReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.util.test.SharedTestUtils;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Verifies that opening an environment read-only will prevent cleaned files
 * from being deleted in a read-write environment.  Uses the ReadOnlyProcess
 * class to open the environment read-only in a separate process.
 */
@RunWith(Parameterized.class)
public class ReadOnlyLockingTest extends CleanerTestBase {

    private static final int FILE_SIZE = 4096;
    private static final int READER_STARTUP_SECS = 30;

    private static final CheckpointConfig forceConfig = new CheckpointConfig();
    static {
        forceConfig.setForce(true);
    }

    private EnvironmentImpl envImpl;
    private Database db;
    private Process readerProcess;

    private static File getProcessFile() {
        return new File(System.getProperty(TestUtils.DEST_DIR),
                        "ReadOnlyProcessFile");
    }

    private static void deleteProcessFile() {
        File file = getProcessFile();
        file.delete();
        assertTrue(!file.exists());
    }

    static void createProcessFile()
        throws IOException {

        File file = getProcessFile();
        assertTrue(file.createNewFile());
        assertTrue(file.exists());
    }

    public ReadOnlyLockingTest(boolean multiSubDir) {
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

        deleteProcessFile();
        try {
            stopReaderProcess();
        } catch (Throwable e) {
            System.out.println("tearDown: " + e);
        }

        super.tearDown();

        db = null;
        envImpl = null;
        readerProcess = null;
    }

    private void openEnv()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(Boolean.getBoolean(TestUtils.NO_SYNC));
        envConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "80");
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(),
             Integer.toString(FILE_SIZE));
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        if (envMultiSubDir) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, DATA_DIRS + "");
        }

        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "ReadOnlyLockingTest", dbConfig);
    }

    private void closeEnv()
        throws DatabaseException {

        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    /**
     * Tests that cleaned files are deleted when there is no reader process.
     */
    @Test
    public void testBaseline()
        throws DatabaseException {

        openEnv();
        writeAndDeleteData();
        env.checkpoint(forceConfig);

        int nFilesCleaned = env.cleanLog();
        assertTrue(nFilesCleaned > 0);
        assertTrue(listFiles(), !areAnyFilesDeleted());

        /* Files are deleted during the checkpoint. */
        env.checkpoint(forceConfig);
        assertTrue(listFiles(), areAnyFilesDeleted());

        closeEnv();
    }

    /**
     * Tests that cleaned files are not deleted when there is a reader process.
     */
    @Test
    public void testReadOnlyLocking()
        throws Exception {

        openEnv();
        writeAndDeleteData();
        env.checkpoint(forceConfig);
        int nFilesCleaned = env.cleanLog();
        assertTrue(nFilesCleaned > 0);
        assertTrue(listFiles(), !areAnyFilesDeleted());

        /*
         * No files are deleted after cleaning when the reader process is
         * running.
         */
        startReaderProcess();
        env.cleanLog();
        env.checkpoint(forceConfig);
        assertTrue(listFiles(), !areAnyFilesDeleted());

        /*
         * Files are deleted when a checkpoint occurs after the reader
         * process stops.
         */
        stopReaderProcess();
        env.cleanLog();
        env.checkpoint(forceConfig);
        assertTrue(listFiles(), areAnyFilesDeleted());

        closeEnv();
    }

    private void writeAndDeleteData()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        DatabaseEntry data = new DatabaseEntry(new byte[FILE_SIZE]);
        for (int i = 0; i < 5; i += 1) {
            db.put(null, key, data);
        }
    }

    private boolean areAnyFilesDeleted() {
        long lastNum = envImpl.getFileManager().getLastFileNum().longValue();
        for (long i = 0; i <= lastNum; i += 1) {
            String name = envImpl.getFileManager().getFullFileName
                (i, FileManager.JE_SUFFIX);
            if (!(new File(name).exists())) {
                return true;
            }
        }
        return false;
    }

    private String listFiles() {
        StringBuilder builder = new StringBuilder();
        builder.append("Files:");
        final String[] names = envHome.list();
        if (names != null) {
            for (String name : names) {
                builder.append(' ');
                builder.append(name);
            }
        }
        return builder.toString();
    }

    private void startReaderProcess()
        throws Exception {

        List<String> cmd = new ArrayList<>();
        cmd.add("java");
        JVMSystemUtils.addZingJVMArgs(cmd);

        cmd.addAll(Arrays.asList(
            "-cp",
            System.getProperty("java.class.path"),
            "-D" + SharedTestUtils.DEST_DIR + '=' +
                SharedTestUtils.getDestDir(),
            ReadOnlyProcess.class.getName(),
            Boolean.toString(envMultiSubDir),
            DATA_DIRS + ""));

        /* Start it and wait for it to open the environment. */
        readerProcess = new ProcessBuilder(cmd).start();
        InputStream error = readerProcess.getErrorStream();
        InputStream output = readerProcess.getInputStream();
        Thread err =
            new Thread(new OutErrReader(error, false /*ignoreOutput*/));
        err.start();
        Thread out =
            new Thread(new OutErrReader(output, false /*ignoreOutput*/));
        out.start();
        long startTime = System.currentTimeMillis();
        boolean running = false;
        while (!running &&
               ((System.currentTimeMillis() - startTime) <
                (READER_STARTUP_SECS * 1000))) {
            if (getProcessFile().exists()) {
                running = true;
            } else {
                Thread.sleep(10);
            }
        }
        //printReaderStatus();
        assertTrue("ReadOnlyProcess did not start after " +
                   READER_STARTUP_SECS + " + secs",
                   running);
    }

    private void stopReaderProcess()
        throws Exception {

        if (readerProcess != null) {
            readerProcess.destroy();
            readerProcess.waitFor();
            Thread.sleep(2000);
            readerProcess = null;
        }
    }

    private void printReaderStatus() {
        try {
            int status = readerProcess.exitValue();
            System.out.println("Process status=" + status);
        } catch (IllegalThreadStateException e) {
            System.out.println("Process is still running");
        }
    }
}
