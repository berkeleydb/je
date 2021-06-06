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

package com.sleepycat.je.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DbBackupTest extends TestBase {

    private static StatsConfig CLEAR_CONFIG = new StatsConfig();
    static {
        CLEAR_CONFIG.setClear(true);
    }

    private static CheckpointConfig FORCE_CONFIG = new CheckpointConfig();
    static {
        FORCE_CONFIG.setForce(true);
    }

    private static final String SAVE1 = "save1";
    private static final String SAVE2 = "save2";
    private static final String SAVE3 = "save3";
    private static final int NUM_RECS = 200;
    private static final int N_DATA_DIRS = 3;

    private final File envHome;
    private Environment env;
    private FileManager fileManager;
    private final boolean useMultiEnvDirs;

    @Parameters
    public static List<Object[]> genParams() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }
    
    public DbBackupTest(boolean multiEnv) {
        envHome = SharedTestUtils.getTestDir();
        useMultiEnvDirs = multiEnv;
        customName = useMultiEnvDirs ? ":multi-env-dirs" : "";
    }

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        deleteSaveDir(SAVE1);
        deleteSaveDir(SAVE2);
        deleteSaveDir(SAVE3);
    }

    @After
    public void tearDown() {
        if (env != null) {
            try {
                env.close();
            } finally {
                env = null;
            }
        }
        maybeDeleteDataDirs(envHome, "TearDown", false);

        TestUtils.removeLogFiles("TearDown", envHome, false);
        deleteSaveDir(SAVE1);
        deleteSaveDir(SAVE2);
        deleteSaveDir(SAVE3);
    }

    /**
     * Test basic backup, make sure log cleaning isn't running.
     */
    @Test
    public void testBackupVsCleaning()
        throws Throwable {

        env = createEnv(false, envHome); /* read-write env */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        fileManager = envImpl.getFileManager();

        /*
         * Grow files, creating obsolete entries to create cleaner
         * opportunity.
         */
        growFiles("db1", env, 8);

        /* Start backup. */
        DbBackup backupHelper = new DbBackup(env);
        backupHelper.startBackup();
        String[] backupFiles = backupHelper.getLogFilesInBackupSet();

        long lastFileNum =  backupHelper.getLastFileInBackupSet();
        long checkLastFileNum = lastFileNum;

        /* Copy the backup set. */
        saveFiles(backupHelper, -1, lastFileNum, SAVE1);

        /*
         * Try to clean and checkpoint. Check that the logs grew as
         * a result.
         */
        batchClean(0, backupFiles);
        long newLastFileNum = (fileManager.getLastFileNum()).longValue();
        assertTrue(checkLastFileNum < newLastFileNum);
        checkLastFileNum = newLastFileNum;

        /* Copy the backup set after attempting cleaning */
        saveFiles(backupHelper, -1, lastFileNum, SAVE2);

        /* Insert more data. */
        growFiles("db2", env, 8);

        /*
         * Try to clean and checkpoint. Check that the logs grew as
         * a result.
         */
        batchClean(0, backupFiles);
        newLastFileNum = fileManager.getLastFileNum();
        assertTrue(checkLastFileNum < newLastFileNum);
        checkLastFileNum = newLastFileNum;

        /* Copy the backup set after inserting more data */
        saveFiles(backupHelper, -1, lastFileNum, SAVE3);

        /* Check the membership of the saved set. */
        long lastFile =  backupHelper.getLastFileInBackupSet();
        String[] backupSet = backupHelper.getLogFilesInBackupSet();
        assertEquals((lastFile + 1), backupSet.length);

        /* End backup. */
        backupHelper.endBackup();

        /*
         * Run cleaning, and verify that quite a few files are deleted.
         */
        long numCleaned = batchClean(100, backupFiles);
        assertTrue(numCleaned > 5);
        env.close();
        env = null;

        /* Verify backups. */
        maybeDeleteDataDirs(envHome, "Verify", true);

        TestUtils.removeFiles("Verify", envHome, FileManager.JE_SUFFIX);
        verifyDb(SAVE1, true);

        maybeDeleteDataDirs(envHome, "Verify", true);

        TestUtils.removeFiles("Verify", envHome, FileManager.JE_SUFFIX);
        verifyDb(SAVE2, true);

        maybeDeleteDataDirs(envHome, "Verify", true);

        TestUtils.removeFiles("Verify", envHome, FileManager.JE_SUFFIX);
        verifyDb(SAVE3, true);
    }

    /**
     * Test basic backup, make sure environment can't be closed mid-stream.
     */
    @Test
    public void testBackupVsClose()
        throws Throwable {

        env = createEnv(false, envHome); /* read-write env */

        growFiles("db1", env, 8);

        /* Start backup. */
        DbBackup backupHelper = new DbBackup(env);
        backupHelper.startBackup();

        try {
            env.close();
            fail("expected whining about backup being in progress.");
        } catch (EnvironmentFailureException expected) {
            // expected
        }
        env = null;
        backupHelper.endBackup();

        /* Verify backups. */
        maybeDeleteDataDirs(envHome, "Verify", true);

        TestUtils.removeLogFiles("Verify", envHome, false);
        maybeDeleteDataDirs(envHome, "Verify", true);
    }

    /**
     * Test multiple backup passes
     */
    @Test
    public void testIncrementalBackup()
        throws Throwable {

        env = createEnv(false, envHome); /* read-write env */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        fileManager = envImpl.getFileManager();

        growFiles("db1", env, 8);

        /* Backup1. */
        DbBackup backupHelper1 = new DbBackup(env);
        backupHelper1.startBackup();
        long b1LastFile =  backupHelper1.getLastFileInBackupSet();
        saveFiles(backupHelper1, -1, b1LastFile, SAVE1);
        String lastName =
            fileManager.getFullFileName(b1LastFile, FileManager.JE_SUFFIX);
        long b1LastFileLen = new File(lastName).length();
        backupHelper1.endBackup();

        /*
         * Add more data. Check that the file did flip, and is not modified
         * by the additional data.
         */
        growFiles("db2", env, 8);
        checkFileLen(b1LastFile, b1LastFileLen);

        /* Backup2. */
        DbBackup backupHelper2 = new DbBackup(env, b1LastFile);
        backupHelper2.startBackup();
        long b2LastFile =  backupHelper2.getLastFileInBackupSet();
        saveFiles(backupHelper2, b1LastFile, b2LastFile, SAVE2);
        lastName =
            fileManager.getFullFileName(b2LastFile, FileManager.JE_SUFFIX);
        long b2LastFileLen = new File(lastName).length();
        backupHelper2.endBackup();

        /* Test deprecated getLogFilesInBackupSet(long) method. */
        DbBackup backupHelper3 = new DbBackup(env);
        backupHelper3.startBackup();
        String[] fileList3 =
            backupHelper3.getLogFilesInBackupSet(b1LastFile);
        assertEquals(b1LastFile + 1,
                     fileManager.getNumFromName(fileList3[0]).longValue());
        backupHelper3.endBackup();

        env.close();
        env = null;

        /* Verify backup 1. */
        maybeDeleteDataDirs(envHome, "Verify", true);
        TestUtils.removeFiles("Verify", envHome, FileManager.JE_SUFFIX);
        verifyDb(SAVE1, false);

        /* Last file should be immutable after a restore. */
        checkFileLen(b1LastFile, b1LastFileLen);

        /* Verify backup 2. */
        maybeDeleteDataDirs(envHome, "Verify", false);
        TestUtils.removeFiles("Verify", envHome, FileManager.JE_SUFFIX);
        verifyBothDbs(SAVE1, SAVE2);

        /* Last file should be immutable after a restore. */
        checkFileLen(b2LastFile, b2LastFileLen);
    }

    private void maybeDeleteDataDirs(File envDir,
                                     String comment,
                                     boolean subdirsOnly) {
        if (useMultiEnvDirs) {
            for (int i = 1; i <= N_DATA_DIRS; i += 1) {
                File dataDir = new File(envDir, "data00" + i);
                TestUtils.removeFiles
                    (comment, dataDir, FileManager.JE_SUFFIX);
                if (!subdirsOnly) {
                    dataDir.delete();
                }
            }
        }
    }

    @Test
    public void testBadUsage()
        throws Exception {

        env = createEnv(false, envHome); /* read-write env */

        DbBackup backup = new DbBackup(env);

        /* end can only be called after start. */
        try {
            backup.endBackup();
            fail("should fail");
        } catch (IllegalStateException expected) {
        }

        /* start can't be called twice. */
        backup.startBackup();
        try {
            backup.startBackup();
            fail("should fail");
        } catch (IllegalStateException expected) {
        }

        /*
         * You can only get the backup set when you're in between start
         * and end.
         */
        backup.endBackup();

        try {
            backup.getLastFileInBackupSet();
            fail("should fail");
        } catch (IllegalStateException expected) {
        }

        try {
            backup.getLogFilesInBackupSet();
            fail("should fail");
        } catch (IllegalStateException expected) {
        }

        try {
            backup.getLogFilesInBackupSet(0);
            fail("should fail");
        } catch (IllegalStateException expected) {
        }

        try {
            backup.getLogFilesInSnapshot();
            fail("should fail");
        } catch (IllegalStateException expected) {
        }

        env.close();
        env = null;
    }

    @Test
    public void testReadOnly()
        throws Exception {

        if (useMultiEnvDirs) {
            return; // not worth the trouble
        }

        /* Make a read-only handle on a read-write environment directory.*/
        env = createEnv(true, envHome);

        try {
            @SuppressWarnings("unused")
            DbBackup backup = new DbBackup(env);
            fail("Should fail because env is read/only.");
        } catch (IllegalArgumentException expected) {
        }

        env.close();
        env = null;

        /*
         * Make a read-only handle on a read-only environment directory. Use a
         * new environment directory because we're going to set it read0nly and
         * there doesn't seem to be a way of undoing that.
         */
        File tempEnvDir = new File(envHome, SAVE1);
        assertTrue(tempEnvDir.mkdirs());
        env = createEnv(false, tempEnvDir);
        growFiles("db1", env, 8);
        env.close();
        env = null;

        if (!tempEnvDir.setWritable(false)) {
            System.out.println(
                "Skipping testReadOnly because platform doesn't support " +
                "setting file permissions");
            return;
        }

        try {
            env = createEnv(true, tempEnvDir);

            DbBackup backupHelper = new DbBackup(env);
            backupHelper.startBackup();

            FileManager fileManager =
                DbInternal.getNonNullEnvImpl(env).getFileManager();
            long lastFile = fileManager.getLastFileNum().longValue();
            assertEquals(lastFile, backupHelper.getLastFileInBackupSet());

            backupHelper.endBackup();
            env.close();
            env = null;
        } finally {
            assertTrue(tempEnvDir.setWritable(true));
        }
    }

    private Environment createEnv(boolean readOnly, File envDir)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setReadOnly(readOnly);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                 "10000");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");

        if (useMultiEnvDirs) {
            envConfig.setConfigParam
                (EnvironmentParams.LOG_N_DATA_DIRECTORIES.getName(),
                 N_DATA_DIRS + "");
            for (int i = 1; i <= N_DATA_DIRS; i += 1) {
                new File(envDir, "data00" + i).mkdir();
            }
        }
        Environment env = new Environment(envDir, envConfig);

        return env;
    }

    private long growFiles(String dbName,
                           Environment env,
                           int minNumFiles)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, dbName, dbConfig);
        FileManager fileManager =
            DbInternal.getNonNullEnvImpl(env).getFileManager();
        long startLastFileNum =
            DbLsn.getFileNumber(fileManager.getLastUsedLsn());

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[1024]);
        /* Update twice, in order to create plenty of cleaning opportunity. */
        for (int i = 0; i < NUM_RECS; i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        for (int i = 0; i < NUM_RECS; i++) {
            IntegerBinding.intToEntry(i, key);
            assertEquals(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        db.close();

        long endLastFileNum =
            DbLsn.getFileNumber(fileManager.getLastUsedLsn());
        assertTrue((endLastFileNum -
                    startLastFileNum) >= minNumFiles);
        return endLastFileNum;
    }

    private long batchClean(int maxDeletions, String[] backupFiles)
        throws DatabaseException {

        EnvironmentStats stats = env.getStats(CLEAR_CONFIG);
        while (env.cleanLog() > 0) {
        }
        env.checkpoint(FORCE_CONFIG);

        int nDeletions = 0;
        for (String name : backupFiles) {
            if (!fileManager.isFileValid(fileManager.getNumFromName(name))) {
                nDeletions += 1;
            }
        }

        return nDeletions;
    }

    private void saveFiles(DbBackup backupHelper,
                           long lastFileFromPrevBackup,
                           long lastFileNum,
                           String saveDirName)
        throws DatabaseException {

        /* Check that the backup set contains only the files it should have. */
        String[] fileList = backupHelper.getLogFilesInBackupSet();
        assertEquals(lastFileFromPrevBackup + 1,
                     fileManager.getNumFromName(fileList[0]).
                     longValue());
        assertEquals(lastFileNum,
                     fileManager.getNumFromName(fileList[fileList.length - 1]).
                     longValue());

        final String[] snapshotFiles = backupHelper.getLogFilesInSnapshot();
        if (lastFileFromPrevBackup < 0) {
            /* In a full backup, the snapshot is the same as the backup set. */
            assertTrue(Arrays.equals(fileList, snapshotFiles));
        } else {
            /* In an incremental backup, the snapshot should be larger. */
            final HashSet<String> backupSet = new HashSet<String>();
            final HashSet<String> snapshotSet = new HashSet<String>();
            Collections.addAll(backupSet, fileList);
            Collections.addAll(snapshotSet, snapshotFiles);
            assertTrue(snapshotSet.containsAll(backupSet));
            assertTrue(snapshotFiles.length > fileList.length);
        }

        /* Make a new save directory. */
        File saveDir = new File(envHome, saveDirName);
        assertTrue(saveDir.mkdir());

        if (useMultiEnvDirs) {
            for (int i = 1; i <= N_DATA_DIRS; i += 1) {
                new File(saveDir, "data00" + i).mkdir();
            }
        }

        copyFiles(envHome, saveDir, fileList);
    }

    private void copyFiles(File sourceDir, File destDir, String[] fileList)
        throws DatabaseException {

        try {
            for (int i = 0; i < fileList.length; i++) {
                File source = new File(sourceDir, fileList[i]);
                FileChannel sourceChannel =
                    new FileInputStream(source).getChannel();
                File save = new File(destDir, fileList[i]);
                FileChannel saveChannel =
                    new FileOutputStream(save).getChannel();

                saveChannel.transferFrom(sourceChannel, 0,
                                         sourceChannel.size());

                /* Close the channels. */
                sourceChannel.close();
                saveChannel.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete all the contents and the directory itself.
     */
    private void deleteSaveDir(String saveDirName) {
        if (useMultiEnvDirs) {
            for (int i = 1; i <= N_DATA_DIRS; i += 1) {
                String saveSubdirName =
                    saveDirName + File.separator + "data00" + i;
                deleteSaveDir1(saveSubdirName);
            }
        }
        deleteSaveDir1(saveDirName);
    }

    private void deleteSaveDir1(String saveDirName) {
        File saveDir = new File(envHome, saveDirName);
        if (saveDir.exists()) {
            String[] savedFiles = saveDir.list();
            if (savedFiles != null) {
                for (int i = 0; i < savedFiles.length; i++) {
                    File f = new File(saveDir, savedFiles[i]);
                    assertTrue(f.delete());
                }

                assertTrue(saveDir.delete());
            }
        }
    }

    /**
     * Copy the saved files in, check values.
     */
    private void verifyDb(String saveDirName, boolean rename)
        throws DatabaseException {

        if (useMultiEnvDirs) {
            for (int i = 1; i <= N_DATA_DIRS; i += 1) {
                String saveSubdirName = File.separator + "data00" + i;
                verifyDbPart1(saveDirName, saveSubdirName, rename);
            }
            verifyDbPart2();
        } else {
            verifyDbPart1(saveDirName, "", rename);
            verifyDbPart2();
        }
    }

    private void verifyDbPart1(String saveDirName,
                               String subdirName,
                               boolean rename)
        throws DatabaseException {

        File saveDir = new File(envHome, saveDirName + subdirName);
        String[] savedFiles = saveDir.list();
        if (rename) {
            for (int i = 0; i < savedFiles.length; i++) {
                File saved = new File(saveDir, savedFiles[i]);
                File dest = new File(envHome + subdirName, savedFiles[i]);
                assertTrue(saved.renameTo(dest));
            }
        } else {
            /* copy. */
            copyFiles(saveDir, new File(envHome + subdirName), savedFiles);
        }
    }

    private void verifyDbPart2()
        throws DatabaseException {

        env = createEnv(false, envHome);
        checkDb("db1");

        /* Db 2 should not exist. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        try {
            @SuppressWarnings("unused")
            Database db = env.openDatabase(null, "db2", dbConfig);
            fail("db2 should not exist");
        } catch (DatabaseException expected) {
        }

        env.close();
        env = null;
    }

    /**
     * Copy the saved files in, check values.
     */
    private void verifyBothDbs(String saveDirName1, String saveDirName2)
        throws DatabaseException {

        File saveDir = new File(envHome, saveDirName1);
        String[] savedFiles = saveDir.list();
        for (int i = 0; i < savedFiles.length; i++) {
            File saved = new File(saveDir, savedFiles[i]);
            File dest = new File(envHome, savedFiles[i]);
            assertTrue(saved.renameTo(dest));
        }

        if (useMultiEnvDirs) {
            for (int j = 1; j <= N_DATA_DIRS; j += 1) {
                String saveSubDirName2 = File.separator + "data00" + j;

                saveDir = new File(envHome, saveDirName2 + saveSubDirName2);
                savedFiles = saveDir.list();
                for (int i = 0; i < savedFiles.length; i++) {
                    File saved = new File(saveDir, savedFiles[i]);
                    File dest =
                        new File(envHome + saveSubDirName2, savedFiles[i]);
                    assertTrue(saved.renameTo(dest));
                }
            }
        } else {
            saveDir = new File(envHome, saveDirName2);
            savedFiles = saveDir.list();
            for (int i = 0; i < savedFiles.length; i++) {
                File saved = new File(saveDir, savedFiles[i]);
                File dest = new File(envHome, savedFiles[i]);
                assertTrue(saved.renameTo(dest));
            }
        }

        env = createEnv(false, envHome);
        checkDb("db1");
        checkDb("db2");
        env.close();
        env = null;
    }

    private void checkDb(String dbName)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        Database db = env.openDatabase(null, dbName, dbConfig);
        Cursor c = null;
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            c = db.openCursor(null, null);

            for (int i = 0; i < NUM_RECS; i++) {
                assertEquals(OperationStatus.SUCCESS,
                             c.getNext(key, data, LockMode.DEFAULT));
                assertEquals(i, IntegerBinding.entryToInt(key));
            }
            assertEquals(OperationStatus.NOTFOUND,
                         c.getNext(key, data, LockMode.DEFAULT));
        } finally {
            if (c != null)
                c.close();
            db.close();
        }
    }

    private void checkFileLen(long fileNum, long length) {
        String fileName = fileManager.getFullFileName(fileNum,
                                                      FileManager.JE_SUFFIX);
        File f = new File(fileName);
        assertEquals(length, f.length());
    }

    /**
     * Tests EnvironmentConfig.ENV_RECOVERY_FORCE_NEW_FILE [#22834].
     */
    @Test
    public void testForceNewFile() {
        final EnvironmentConfig envConfig = TestUtils.initEnvConfig();

        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);
        envConfig.setAllowCreate(false);

        /* File is not flipped by default. */
        assertEquals(0, getLastFile());
        env.close();
        env = new Environment(envHome, envConfig);
        assertEquals(0, getLastFile());
        env.close();
        env = null;
        final long fileSize = getFileSize(0);

        /* File flips when ENV_RECOVERY_FORCE_NEW_FILE is true. */
        envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_NEW_FILE,
                                 "true");
        env = new Environment(envHome, envConfig);
        assertEquals(1, getLastFile());
        env.close();
        env = null;
        assertEquals(fileSize, getFileSize(0));

        /* File does not flip when ENV_RECOVERY_FORCE_NEW_FILE is false. */
        envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_NEW_FILE,
                                 "false");
        env = new Environment(envHome, envConfig);
        assertEquals(1, getLastFile());
        env.close();
        env = null;
        assertEquals(fileSize, getFileSize(0));
    }

    private long getLastFile() {
        return DbInternal.getNonNullEnvImpl(env).
                          getFileManager().
                          getCurrentFileNum();
    }

    private long getFileSize(long fileNum) {
        final File file = new File(envHome, FileManager.getFileName(fileNum));
        return file.length();
    }
}
