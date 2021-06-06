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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.util.test.SharedTestUtils;

public class VerifyLogTest extends DualTestCase {

    private static final String SAVE_DIR = "save";
    private static final int BUF_SIZE = 2048;
    private static final int NUM_RECS = 5000;

    private final File envHome;
    private final File tempDir;
    private Environment env;

    public VerifyLogTest() {
        envHome = SharedTestUtils.getTestDir();
        tempDir = new File(envHome, SAVE_DIR);
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        try {
            closeEnv();
        } catch (Throwable e) {
            System.out.println("During tearDown: " + e);
        }

        super.tearDown();
    }

    @Test
    public void testVerify()
        throws Throwable {

        openEnv();
        writeData();

        /* Use DbBackup to get a list of the log files. */
        final DbBackup backup = new DbBackup(env);
        backup.startBackup();
        final String[] fileNames = backup.getLogFilesInBackupSet();
        backup.endBackup();

        /* Verify files, copy while verifying, diff and verify the copy. */
        verifyFiles(fileNames, env.getHome());
        clearTempDir();
        copyFiles(env, fileNames, tempDir, BUF_SIZE);
        diffFiles(fileNames, tempDir, false /*allowShorterLastFile*/);
        verifyFiles(fileNames, tempDir);

        /*
         * Use NIO channels to copy while verifying, diff and verify the copy
         */
        clearTempDir();
        copyFilesNIO(env, fileNames, tempDir, BUF_SIZE);
        diffFiles(fileNames, tempDir, false /*allowShorterLastFile*/);
        verifyFiles(fileNames, tempDir);

        /*
         * Modify a byte at a time and expect a verification exception.  To
         * prevent this from running for a very long time, use the first file
         * only and limit the maximum file verifications to 5000.
         */
        final String fileName = fileNames[0];
        final File file = new File(tempDir, fileName);
        final long fileLen = file.length();
        final long maxIter = Math.min(5000, fileLen);
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        for (long offset = 0; offset < maxIter; offset += 1) {
            raf.seek(offset);
            int val = raf.read();
            raf.seek(offset);
            /* Replace byte with bitwise complement. */
            raf.write(~val);
            try {
                verifyFiles(new String[] {fileName}, tempDir);
                fail(String.format("Expected verify of %s to fail, " +
                     "offset: 0x%X, val: 0x%X", fileName, offset, val));
            } catch (LogVerificationException expected) {
            }
            /* Repair the damage we did above. */
            raf.seek(offset);
            raf.write(val);
        }

        /* Expect an exception when we append a byte at the end. */
        raf.seek(fileLen);
        raf.write(0);
        try {
            verifyFiles(new String[] {fileName}, tempDir);
            fail("Expected verify to fail after append: " + fileName);
        } catch (LogVerificationException expected) {
        }
        /* Expect an exception when we remove the last byte. */
        raf.seek(fileLen - 1);
        final int lastByte = raf.read();
        raf.setLength(fileLen - 1);
        try {
            verifyFiles(new String[] {fileName}, tempDir);
            fail("Expected verify to fail after truncate: " + fileName);
        } catch (LogVerificationException expected) {
        }
        /* Repair damage. */
        raf.seek(fileLen - 1);
        raf.write(lastByte);

        /* Ensure that the repairs above were successful. */
        verifyFiles(fileNames, tempDir);

        closeEnv();
        raf.close();
    }

    private void openEnv()
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setTxnNoSync(true);
        /* For simplicity, disable log file deletion. */
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        env = create(envHome, envConfig);
    }

    private void closeEnv()
        throws DatabaseException {

        if (env != null) {
            try {
                close(env);
            } finally {
                env = null;
            }
        }
    }

    private void clearTempDir() {
        deleteTempDir();
        assertTrue(tempDir.mkdir());
    }

    private void deleteTempDir() {
        if (tempDir.exists()) {
            final String[] fileNames = tempDir.list();
            if (fileNames != null) {
                for (final String fileName : fileNames) {
                    final File f = new File(tempDir, fileName);
                    assertTrue("Can't delete " + f, f.delete());
                }
            }
            assertTrue(tempDir.delete());
        }
    }

    /**
     * Add records of sizes varying from small to large, increasing the size
     * one byte at a time for each record.  This creates log entries with
     * varied sizes and buffer boundaries.
     */
    private void writeData()
        throws DatabaseException {

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        final Database db = env.openDatabase(null, "foo", dbConfig);

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        for (int i = 0; i < NUM_RECS; i += 1) {
            IntegerBinding.intToEntry(i, key);
            data.setData(new byte[i]);
            assertSame(OperationStatus.SUCCESS, db.put(null, key, data));
        }

        db.close();
    }

    /**
     * For every given file name in dir1, compare it to the same file name in
     * the environment home directory.
     *
     * @param allowShorterLastFile is true if the last file in the array in
     * dir1 may be shorter than the corresponding file in the environment home
     * directory, because writing is still active in the environment.
     */
    private void diffFiles(final String[] fileNames,
                           final File dir1,
                           final boolean allowShorterLastFile)
        throws IOException, DatabaseException {
  
        final File dir2 = env.getHome();
  
        for (final String fileName : fileNames) {
            final File file1 = new File(dir1, fileName);
            final FileInputStream is1 = new FileInputStream(file1);
            try {
                final File file2 = new File(dir2, fileName);
                final FileInputStream is2 = new FileInputStream(file2);
                try {
                    final byte[] buf1 = new byte[4096];
                    final byte[] buf2 = new byte[4096];
      
                    long offset = 0;
                    while (true) {
                        final int len1 = is1.read(buf1);
                        final int len2 = is2.read(buf2);
                        if (len1 < 0 && len2 < 0) {
                            break;
                        }
                        if (len1 != len2) {
                            fail(String.format("Length mismatch file: %s " +
                                 "offset: 0x%X len1: 0x%X len2: 0x%X",
                                 fileName, offset, len1, len2));
                        }
                        for (int i = 0; i < len1; i += 1) {
                            if (buf1[i] != buf2[i]) {
                                fail(String.format("Data mismatch file: %s " +
                                     "offset: 0x%X byte1: 0x%X byte2: 0x%X",
                                     fileName, offset + i, buf1[i], buf2[i]));
                            }
                        }
                        offset += len1;
                    }
                } finally {
                    is2.close();
                }
            } finally {
                is1.close();
            }
        }
    }

    /**
     * Copy specified log files to a given directory. This method is also
     * present in the class javadoc of LogVerificationInputStream.  This method
     * should be kept in sync with the documented method in order to test it.
     */
    void copyFiles(final Environment env,
                   final String[] fileNames,
                   final File destDir,
                   final int bufSize)
        throws IOException, DatabaseException {
  
        final File srcDir = env.getHome();
  
        for (final String fileName : fileNames) {
  
            final File destFile = new File(destDir, fileName);
            final FileOutputStream fos = new FileOutputStream(destFile);
  
            final File srcFile = new File(srcDir, fileName);
            final FileInputStream fis = new FileInputStream(srcFile);
            final LogVerificationInputStream vis =
                new LogVerificationInputStream(env, fis, fileName);
  
            final byte[] buf = new byte[bufSize];
  
            try {
                while (true) {
                    final int len = vis.read(buf);
                    if (len < 0) {
                        break;
                    }
                    fos.write(buf, 0, len);
                }
            } finally {
                fos.close();
                vis.close();
            }
        }
    }

    /**
     * Copy specified log files to a given directory using NIO channels.  This
     * method is also present in the class javadoc of
     * LogVerificationReadableByteChannel, and should be kept in sync with the
     * documented method in order to test it.
     */
    void copyFilesNIO(final Environment env,
                      final String[] fileNames,
                      final File destDir,
                      final int bufSize)
        throws IOException, DatabaseException {

        final File srcDir = env.getHome();

        for (final String fileName : fileNames) {

            final File destFile = new File(destDir, fileName);
            final FileOutputStream fos = new FileOutputStream(destFile);
            final FileChannel foc = fos.getChannel();

            final File srcFile = new File(srcDir, fileName);
            final FileInputStream fis = new FileInputStream(srcFile);
            final FileChannel fic = fis.getChannel();
            final LogVerificationReadableByteChannel vic =
                new LogVerificationReadableByteChannel(env, fic, fileName);

            final ByteBuffer buf = ByteBuffer.allocateDirect(bufSize);

            try {
                while (true) {
                    final int len = vic.read(buf);
                    if (len < 0) {
                        break;
                    }
                    buf.flip();
                    foc.write(buf);
                    buf.clear();
                }
            } finally {
                fos.close();
                vic.close();
            }
        }
    }

    /**
     * Verifies the given files without copying them.
     */
    private void verifyFiles(final String[] fileNames, final File dir)
        throws IOException {
  
        for (final String fileName : fileNames) {
            final File file = new File(dir, fileName);
            final FileInputStream fis = new FileInputStream(file);
            final LogVerificationInputStream vis =
                new LogVerificationInputStream(env, fis, fileName);
            final byte[] buf = new byte[BUF_SIZE];
            try {
                while (true) {
                    final int len = vis.read(buf);
                    if (len < 0) {
                        break;
                    }
                }
            } finally {
                vis.close();
            }
        }
    }
}
