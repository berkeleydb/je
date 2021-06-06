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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

public class FileEdgeCaseTest extends TestBase {

    private final File envHome;
    private Environment env;

    public FileEdgeCaseTest() {
        envHome = SharedTestUtils.getTestDir();
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
    }

    /**
     * SR #15133
     * Create a JE environment with a single log file and a checksum
     * exception in the second entry in the log file.
     *
     * When an application attempts to open this JE environment, JE truncates
     * the log file at the point before the bad checksum, because it assumes
     * that bad entries at the end of the log are the result of incompletely
     * flushed writes from the last environment use.  However, the truncated
     * log doesn't have a valid environment root, so JE complains and asks the
     * application to move aside the existing log file (via the exception
     * message). The resulting environment has a single log file, with
     * a single valid entry, which is the file header.
     *
     * Any subsequent attempts to open the environment should also fail at the
     * same point. In the error case reported by this SR, we didn't handle this
     * single log file/single file header case right, and subsequent opens
     * first truncated before the file header, leaving a 0 length log, and
     * then proceeded to write error trace messages into the log. This
     * resulted in a log file with no file header, (but with trace messages)
     * and any following opens got unpredictable errors like
     * ClassCastExceptions and BufferUnderflows.
     *
     * The correct situation is to continue to get the same exception.
     */
    @Test
    public void testPostChecksumError()
        throws IOException {

        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        config.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");
        env = new Environment(envHome, config);

        env.close();
        env = null;

        /* Intentionally corrupt the second entry. */
        corruptSecondEntry();

        /*
         * Attempts to open the environment should fail with a
         * EnvironmentFailureException
         */
        for (int i = 0; i < 3; i += 1) {
            try {
                env = new Environment(envHome, config);
            } catch (EnvironmentFailureException expected) {
                assertSame(EnvironmentFailureReason.LOG_INTEGRITY,
                    expected.getReason());
            }
        }
    }

    /**
     * [#18307]
     * Suppose we have LSN 1000, and the log entry there has a checksum 
     * exception.
     *
     * Case 1. if we manage to read past LSN 1000, but then hit a second 
     *         checksum exception, return false and truncate the log at the 
     *         first exception.
     * Case 2. if we manage to read past LSN 1000, and do not see any checksum 
     *         exceptions, and do not see any commits, return false and 
     *         truncate the log.
     * Case 3. if we manage to read past LSN 1000, and do not see any checksum 
     *         exceptions, but do see a txn commit, return true and throw 
     *         EnvironmentFailureException.
     *
     * Note the following comments:
     *
     * We should guarantee that, for case 3, the environment open
     * process will throw EFE. Otherwise, the envImpl will exist in DbEnvPool
     * for ever. This will cause the next test case will still use this
     * envImpl. Then the DataVerifier in this test case will throw exception
     * in next test case. This causes that it is very difficult to debug the
     * exception in next test case.
     */
    @Test
    public void testFindCommittedTxn()
        throws IOException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        envConfig.setConfigParam(
            EnvironmentConfig.HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION, "true");
        env = new Environment(envHome, envConfig);

        long[] offsets = writeTwoLNs();
        long lnOffset1 = offsets[0];
        long lnOffset2 = offsets[1];
        long postCommitOffset = offsets[2];

        /* 
         * Case 2: Intentionally pollute the entry checksum after all committed 
         * txns, so no committed txn will be found.
         */
        polluteEntryChecksum(postCommitOffset);

        /* 
         * When doing recovery, no committed txn will be found. So the recovery 
         * process just truncate the log file at the bad log point.
         */
        env = new Environment(envHome, envConfig);
        env.close();
        env = null;

        /*
         * Case 3: Intentionally pollute the entry checksum before the
         * committed txn.
         *
         * When the reader meets the first checksum error, it will step forward
         * to look for the committed txn. After finding the committed txn, 
         * EnvironmentFailureException will be thrown, the recovery process
         * will be stopped.
         */
        polluteEntryChecksum(lnOffset1);

        /*
         * Next attempt to open the environment should fail with a
         * EnvironmentFailureException.
         */
        try {
        
            /* 
             * When doing recovery, one committed txn will be found. So
             * EnvironmentFailureException will be thrown.
             */
            env = new Environment(envHome, envConfig);
            fail("Should caught exception while finding committed txn");
        } catch (EnvironmentFailureException expected) {
            assertSame(EnvironmentFailureReason.FOUND_COMMITTED_TXN,
                       expected.getReason());
        }   
        
        /* 
         * Case 1: Intentionally pollute two entries' checksums before the 
         * committed txn.
         *  
         * When the reader meets the first checksum error, it will step forward
         * to look for the committed txn. Before finding any committed txn, if 
         * the reader meets another checksum error, it will stop the search, 
         * and just truncate the log file at the first checksum error spot.
         */
        polluteEntryChecksum(lnOffset1);
        polluteEntryChecksum(lnOffset2);

        /* 
         * When doing recovery, no committed txn will be found. So the recovery 
         * process just truncate the log file at the corrupted log entry.
         */
        env = new Environment(envHome, envConfig);
        env.close();
        env = null;
    }

    /**
     * Writes two LNs in a txn, and closes the env.
     * @return an array of [lnOffset1, lnOffset2, pastCommitOffset].
     */
    private long[] writeTwoLNs() {

        FileManager fm = DbInternal.getEnvironmentImpl(env).getFileManager();

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db = env.openDatabase(null, "testDB", dbConfig);
        
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        IntegerBinding.intToEntry(0, key);
        IntegerBinding.intToEntry(0, data);

        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = db.openCursor(txn, null);

        long lnOffset1 = DbLsn.getFileOffset(fm.getNextLsn());
        cursor.put(key, data);
        long lnOffset2 = DbLsn.getFileOffset(fm.getNextLsn());
        cursor.put(key, data);

        cursor.close();
        txn.commit();
        long postCommitOffset = DbLsn.getFileOffset(fm.getNextLsn());

        db.close();
        env.close();
        env = null;

        return new long[] { lnOffset1, lnOffset2, postCommitOffset };
    }

    /**
     * Write junk into the second log entry, after the file header.
     */
    private void corruptSecondEntry()
        throws IOException {

        writeToFirstFile(FileManager.firstLogEntryOffset(), new byte[20]);
    }
    
    /**
     * Pollute a specified log entry's checksum.
     */
    private void polluteEntryChecksum(long entryOffset)
        throws IOException {

        /*
         * We just want to pollute the checksum bytes, so the junk has 4
         * bytes.
         */
        writeToFirstFile(entryOffset, new byte[4]);
    }

    private void writeToFirstFile(long entryOffset, byte[] junk)
        throws IOException {

        File firstFile = new File(
            envHome, FileManager.getFileName(0, FileManager.JE_SUFFIX));

        try (RandomAccessFile file = new RandomAccessFile(
            firstFile,
            FileManager.FileMode.READWRITE_MODE.getModeValue())) {

            file.seek(entryOffset);
            file.write(junk);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
