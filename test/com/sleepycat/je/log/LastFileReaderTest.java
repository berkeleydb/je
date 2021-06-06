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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.AbortLogEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.txn.TxnAbort;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

public class LastFileReaderTest extends TestBase {

    private DbConfigManager configManager;
    private FileManager fileManager;
    private LogManager logManager;
    private final File envHome;
    private Environment env;

    public LastFileReaderTest() {
        super();
        envHome = SharedTestUtils.getTestDir();
    }

    @Override
    @After
    public void tearDown() {

        /*
         * Pass false to skip checkpoint, since the file manager may hold
         * an open file that we've trashed in the tests, so we don't want to
         * write to it here.
         */
        try {
            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
            envImpl.close(false);
        } catch (DatabaseException e) {
        }

    }

    /* Create an environment, using the default log file size. */
    private void initEnv()
        throws Exception {

        initEnv(null);
    }

    /* Create an environment, specifying the log file size. */
    private void initEnv(String logFileSize)
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();

        /* Don't run daemons; we do some abrupt shutdowns. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");

        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), "6");
        if (logFileSize != null) {
            DbInternal.disableParameterValidation(envConfig);
            envConfig.setConfigParam
                (EnvironmentParams.LOG_FILE_MAX.getName(), logFileSize);
        }

        /* Disable noisy cleaner database usage. */
        DbInternal.setCreateEP(envConfig, false);
        DbInternal.setCreateUP(envConfig, false);
        DbInternal.setCheckpointUP(envConfig, false);

        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        env = new Environment(envHome, envConfig);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        configManager = envImpl.getConfigManager();
        fileManager = envImpl.getFileManager();
        logManager = envImpl.getLogManager();
    }

    /**
     * Run with an empty file that has a file header but no log entries.
     */
    @Test
    public void testEmptyAtEnd()
        throws Throwable {

        initEnv();

        /*
         * Make a log file with a valid header, but no data.
         */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        FileManagerTestUtils.createLogFile(fileManager, envImpl, 100);
        fileManager.clear();

        LastFileReader reader = new LastFileReader(envImpl, 1000);
        assertTrue(reader.readNextEntry());
        assertEquals(0, DbLsn.getFileOffset(reader.getLastLsn()));
    }

    /**
     * Run with an empty, 0 length file at the end.  This has caused a
     * BufferUnderflowException. [#SR 12631]
     */
    @Test
    public void testLastFileEmpty()
        throws Throwable {

        initEnv("1000");
        int numIters = 10;
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        List<Trace> testObjs = new ArrayList<Trace>();
        List<Long> testLsns = new ArrayList<Long>();

        /*
         * Create a log with one or more files. Use only Trace objects so we
         * can iterate through the entire log ... ?
         */
        for (int i = 0; i < numIters; i++) {
            /* Add a debug record. */
            Trace msg = new Trace("Hello there, rec " + (i+1));
            testObjs.add(msg);
            testLsns.add(new Long(Trace.trace(envImpl, msg)));
        }
        /* Flush the log, files. */
        logManager.flushSync();
        fileManager.clear();

        int lastFileNum = fileManager.getAllFileNumbers().length - 1;

        /*
         * Create an extra, totally empty file.
         */
        fileManager.syncLogEnd();
        fileManager.clear();
        String emptyLastFile =
            fileManager.getFullFileName(lastFileNum+1, FileManager.JE_SUFFIX);

        RandomAccessFile file =
            new RandomAccessFile(emptyLastFile, FileManager.FileMode.
                                 READWRITE_MODE.getModeValue());
        file.close();

        assertTrue(fileManager.getAllFileNumbers().length >= 2);

        /*
         * Try a LastFileReader. It should give us a end-of-log position in the
         * penultimate file.
         */
        LastFileReader reader = new LastFileReader(envImpl, 1000);
        while (reader.readNextEntry()) {
        }

        /*
         * The reader should be positioned at the last, valid file, skipping
         * this 0 length file.
         */
        assertEquals("lastValid=" + DbLsn.toString(reader.getLastValidLsn()),
                     lastFileNum,
                     DbLsn.getFileNumber(reader.getLastValidLsn()));
        assertEquals(lastFileNum, DbLsn.getFileNumber(reader.getEndOfLog()));
    }

    /**
     * Corrupt the file headers of the one and only log file.
     */
    @Test
    public void testBadFileHeader()
        throws Throwable {

        initEnv();

        /*
         * Handle a log file that has data and a bad header. First corrupt the
         * existing log file. We will not be able to establish log end, but
         * won't throw away the file because it has data.
         */
        long lastFileNum = fileManager.getLastFileNum().longValue();
        String lastFile =
            fileManager.getFullFileName(lastFileNum,
                                        FileManager.JE_SUFFIX);

        RandomAccessFile file =
            new RandomAccessFile(lastFile, FileManager.FileMode.
                                 READWRITE_MODE.getModeValue());

        file.seek(15);
        file.writeBytes("putting more junk in, mess up header");
        file.close();

        /*
         * We should see an exception on this one, because we made a file that
         * looks like it has a bad header and bad data.
         */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        try {
            LastFileReader reader = new LastFileReader(envImpl, 1000);
            fail("Should see exception when creating " + reader);
        } catch (EnvironmentFailureException e) {
            assertSame(EnvironmentFailureReason.
                       LOG_CHECKSUM, e.getReason());
            /* Eat exception, expected. */
        }

        /*
         * Now make a bad file header, but one that is less than the size of a
         * file header. This file ought to get moved aside.
         */
        file = new RandomAccessFile(lastFile, "rw");
        file.getChannel().truncate(0);
        file.writeBytes("bad");
        file.close();

        LastFileReader reader = new LastFileReader(envImpl, 1000);
        /* Nothing comes back from reader. */
        assertFalse(reader.readNextEntry());
        File movedFile = new File(envHome, "00000000.bad");
        assertTrue(movedFile.exists());

        /* Try a few more times, we ought to keep moving the file. */
        file = new RandomAccessFile(lastFile, "rw");
        file.getChannel().truncate(0);
        file.writeBytes("bad");
        file.close();

        reader = new LastFileReader(envImpl, 1000);
        assertTrue(movedFile.exists());
        File movedFile1 = new File(envHome, "00000000.bad.1");
        assertTrue(movedFile1.exists());
    }

    /**
     * Run with defaults.
     */
    @Test
    public void testBasic()
        throws Throwable {

        initEnv();
        int numIters = 50;
        List<Loggable> testObjs = new ArrayList<Loggable>();
        List<Long> testLsns = new ArrayList<Long>();

        fillLogFile(numIters, testLsns, testObjs);

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        LastFileReader reader =
            new LastFileReader(envImpl,
                               configManager.getInt
                               (EnvironmentParams.LOG_ITERATOR_READ_SIZE));

        checkLogEnd(reader, numIters, testLsns, testObjs);
    }

    /**
     * Run with very small read buffer.
     */
    @Test
    public void testSmallBuffers()
        throws Throwable {

        initEnv();
        int numIters = 50;
        List<Loggable> testObjs = new ArrayList<Loggable>();
        List<Long> testLsns = new ArrayList<Long>();

        fillLogFile(numIters, testLsns, testObjs);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        LastFileReader reader = new LastFileReader(envImpl, 10);
        checkLogEnd(reader, numIters, testLsns, testObjs);
    }

    /**
     * Run with medium buffers.
     */
    @Test
    public void testMedBuffers()
        throws Throwable {

        initEnv();
        int numIters = 50;
        List<Loggable> testObjs = new ArrayList<Loggable>();
        List<Long> testLsns = new ArrayList<Long>();

        fillLogFile(numIters, testLsns, testObjs);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        LastFileReader reader = new LastFileReader(envImpl, 100);
        checkLogEnd(reader, numIters, testLsns, testObjs);
    }

    /**
     * Put junk at the end of the file.
     */
    @Test
    public void testJunk()
        throws Throwable {

        initEnv();

        int numIters = 50;
        List<Loggable> testObjs = new ArrayList<Loggable>();
        List<Long> testLsns = new ArrayList<Long>();

        /* Write junk into the end of the file. */
        fillLogFile(numIters, testLsns, testObjs);
        long lastFileNum = fileManager.getLastFileNum().longValue();
        String lastFile =
            fileManager.getFullFileName(lastFileNum,
                                        FileManager.JE_SUFFIX);

        RandomAccessFile file =
            new RandomAccessFile(lastFile, FileManager.FileMode.
                                 READWRITE_MODE.getModeValue());
        file.seek(file.length());
        file.writeBytes("hello, some junk");
        file.close();

        /* Read. */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        LastFileReader reader = new LastFileReader(envImpl, 100);
        checkLogEnd(reader, numIters, testLsns, testObjs);
    }

    /**
     * Make a log, then make a few extra files at the end, one empty, one with
     * a bad file header.
     */
    @Test
    public void testExtraEmpty()
        throws Throwable {

        initEnv();
        int numIters = 50;
        List<Loggable> testObjs = new ArrayList<Loggable>();
        List<Long> testLsns = new ArrayList<Long>();
        int defaultBufferSize =
            configManager.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        /*
         * Make a valid log with data, then put a couple of extra files after
         * it. Make the file numbers non-consecutive. We should have three log
         * files.
         */
        /* Create a log */
        fillLogFile(numIters, testLsns, testObjs);

        /* First empty log file -- header, no data. */
        FileManagerTestUtils.bumpLsn(fileManager, 100000000);
        FileManagerTestUtils.bumpLsn(fileManager, 100000000);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        FileManagerTestUtils.createLogFile(fileManager, envImpl, 10);

        /* Second empty log file -- header, no data. */
        FileManagerTestUtils.bumpLsn(fileManager, 100000000);
        FileManagerTestUtils.bumpLsn(fileManager, 100000000);
        FileManagerTestUtils.createLogFile(fileManager, envImpl, 10);

        assertEquals(3, fileManager.getAllFileNumbers().length);

        /*
         * Corrupt the last empty file and then search for the correct last
         * file.
         */
        long lastFileNum = fileManager.getLastFileNum().longValue();
        String lastFile =
            fileManager.getFullFileName(lastFileNum,
                                        FileManager.JE_SUFFIX);
        RandomAccessFile file =
            new RandomAccessFile(lastFile, FileManager.FileMode.
                                 READWRITE_MODE.getModeValue());
        file.getChannel().truncate(10);
        file.close();
        fileManager.clear();

        /*
         * Make a reader, read the log. After the reader returns, we should
         * only have 2 log files.
         */
        LastFileReader reader = new LastFileReader(envImpl,
                                                   defaultBufferSize);
        checkLogEnd(reader, numIters, testLsns, testObjs);
        assertEquals(2, fileManager.getAllFileNumbers().length);

        /*
         * Corrupt the now "last" empty file and try again. This is actually
         * the first empty file we made.
         */
        lastFileNum = fileManager.getLastFileNum().longValue();
        lastFile = fileManager.getFullFileName(lastFileNum,
                                               FileManager.JE_SUFFIX);
        file = new RandomAccessFile(lastFile, FileManager.FileMode.
                                    READWRITE_MODE.getModeValue());
        file.getChannel().truncate(10);
        file.close();

        /*
         * Validate that we have the right number of log entries, and only one
         * valid log file.
         */
        reader = new LastFileReader(envImpl, defaultBufferSize);
        checkLogEnd(reader, numIters, testLsns, testObjs);
        assertEquals(1, fileManager.getAllFileNumbers().length);
    }

    /**
     * Create a marker file that has a RestoreRequired entry, and make
     * sure that the last file reader detects it.
     */
    @Test
    public void testRestoreMarkerFile()
        throws Throwable {

        initEnv();

        /*
         * Make a marker log file.
         */
        RestoreMarker marker = new RestoreMarker(fileManager, logManager);
        Properties props = new Properties();
        props.setProperty("prop1", "1");
        props.setProperty("prop1", "2");
        marker.createMarkerFile(RestoreRequired.FailureType.NETWORK_RESTORE,
                                props);

        /* 
         * We expect the marker file to have a file header, followed by
         * the RestoreRequired entry.
         */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        LastFileReader reader = new LastFileReader(envImpl, 1000);
        assertTrue(reader.readNextEntry());
        LogEntryType type = reader.getEntryType();
        assertEquals(LogEntryType.LOG_FILE_HEADER, type);

        assertTrue(reader.readNextEntry());
        type = reader.getEntryType();
        assertEquals(LogEntryType.LOG_RESTORE_REQUIRED, type);

        RestoreRequired rr = reader.getRestoreRequired();
        Properties rrProps = rr.getProperties();
        assertTrue(rrProps.entrySet().containsAll(props.entrySet()));
    }

    /**
     * Write a logfile of entries, then read the end.
     */
    private void fillLogFile(int numIters,
                             List<Long> testLsns,
                             List<Loggable> testObjs)
        throws Throwable {

        /*
         * Create a log file full of LNs and Debug Records.
         */
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        for (int i = 0; i < numIters; i++) {
            /* Add a debug record. */
            Trace msg = new Trace("Hello there, rec " + (i+1));
            testObjs.add(msg);
            testLsns.add(new Long(Trace.trace(envImpl, msg)));

            /* Add a txn abort */
            TxnAbort abort = new TxnAbort(10L, 200L,
                                          1234567 /* masterNodeId */, 
                                          1 /* DTVLSN */);
            final AbortLogEntry entry = new AbortLogEntry(abort);
            testObjs.add(abort);
            testLsns.add(new Long(logManager.log
                                  (entry,
                                   ReplicationContext.NO_REPLICATE)));
        }

        /* Flush the log, files. */
        logManager.flushSync();
        fileManager.clear();
    }

    /**
     * Use the LastFileReader to check this file, see if the log end is set
     * right.
     */
    private void checkLogEnd(LastFileReader reader,
                             int numIters,
                             List<Long> testLsns,
                             List<Loggable> testObjs)
        throws Throwable {

        reader.setTargetType(LogEntryType.LOG_DBTREE);
        reader.setTargetType(LogEntryType.LOG_TXN_COMMIT);
        reader.setTargetType(LogEntryType.LOG_TXN_ABORT);
        reader.setTargetType(LogEntryType.LOG_TRACE);

        for (LogEntryType entryType : LogEntryType.getAllTypes()) {
            if (entryType.isUserLNType()) {
                reader.setTargetType(entryType);
            }
        }

        /* Now ask the LastFileReader to read it back. */
        while (reader.readNextEntry()) {
        }

        /* Truncate the file. */
        reader.setEndOfFile();

        /*
         * In non-replicated environments, we should see numIters * 2 + 4
         * entries (the extra 4 are the root, debug records, checkpoints and
         * file header written by reocovery.
         */
        assertEquals("should have seen this many entries", numIters * 2 + 4,
                     reader.getNumRead());

        /* Check last used LSN. */
        int numLsns = testLsns.size();
        long lastLsn = DbLsn.longToLsn(testLsns.get(numLsns - 1));
        assertEquals("last LSN", lastLsn, reader.getLastLsn());

        /* Check last offset. */
        assertEquals("prev offset", DbLsn.getFileOffset(lastLsn),
                     reader.getPrevOffset());

        /* Check next available LSN. */
        int lastSize =
            testObjs.get(testObjs.size() - 1).getLogSize();

        long entryHeaderSize = LogEntryHeader.MIN_HEADER_SIZE;
        assertEquals("next available",
                     DbLsn.makeLsn(DbLsn.getFileNumber(lastLsn),
                     DbLsn.getFileOffset(lastLsn) +
                     entryHeaderSize + lastSize),
                     reader.getEndOfLog());

        /* The log should be truncated to just the right size. */
        FileHandle handle =  fileManager.getFileHandle(0L);
        RandomAccessFile file = handle.getFile();
        assertEquals(DbLsn.getFileOffset(reader.getEndOfLog()),
                     file.getChannel().size());
        handle.release();
        fileManager.clear();

        /* Check the last tracked LSNs. */
        assertTrue(reader.getLastSeen(LogEntryType.LOG_DBTREE) !=
                   DbLsn.NULL_LSN);
        assertTrue(reader.getLastSeen(LogEntryType.LOG_IN) == DbLsn.NULL_LSN);

        for (LogEntryType entryType : LogEntryType.getAllTypes()) {
            if (entryType.isUserLNType()) {
                assertTrue(reader.getLastSeen(entryType) == DbLsn.NULL_LSN);
            }
        }

        assertEquals(reader.getLastSeen(LogEntryType.LOG_TRACE),
                     DbLsn.longToLsn(testLsns.get(numLsns - 2)));
        assertEquals(reader.getLastSeen(LogEntryType.LOG_TXN_ABORT),
                     lastLsn);
    }
}
