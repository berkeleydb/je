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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test the LNFileReader
 */
public class LNFileReaderTest extends TestBase {
    static private final boolean DEBUG = false;

    private final File envHome;
    private Environment env;
    private EnvironmentImpl envImpl;
    private Database db;
    private List<CheckInfo> checkList;

    public LNFileReaderTest() {
        super();
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws DatabaseException {

        /*
         * Note that we use the official Environment class to make the
         * environment, so that everything is set up, but we then go a backdoor
         * route to get to the underlying EnvironmentImpl class so that we
         * don't require that the Environment.getDbEnvironment method be
         * unnecessarily public.
         */
        TestUtils.removeLogFiles("Setup", envHome, false);
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setConfigParam
            (EnvironmentParams.LOG_FILE_MAX.getName(), "1024");
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(envHome, envConfig);

        envImpl = DbInternal.getNonNullEnvImpl(env);
    }

    @After
    public void tearDown()
        throws DatabaseException {

        envImpl = null;
        env.close();
    }

    /**
     * Test no log file
     */
    @Test
    public void testNoFile()
        throws DatabaseException {

        /* Make a log file with a valid header, but no data. */
        LNFileReader reader =
            new LNFileReader(envImpl,
                             1000,             // read buffer size
                             DbLsn.NULL_LSN,   // start lsn
                             true,             // redo
                             DbLsn.NULL_LSN,   // end of file lsn
                             DbLsn.NULL_LSN,   // finish lsn
                             null,             // single file
                             DbLsn.NULL_LSN);  // ckpt end lsn
        addUserLNTargetTypes(reader);
        assertFalse("Empty file should not have entries",
                    reader.readNextEntry());
    }

    private void addUserLNTargetTypes(LNFileReader reader) {
        for (LogEntryType entryType : LogEntryType.getAllTypes()) {
            if (entryType.isUserLNType() && entryType.isTransactional()) {
                reader.addTargetType(entryType);
            }
        }
    }

    /**
     * Run with an empty file.
     */
    @Test
    public void testEmpty()
        throws IOException, DatabaseException {

        /* Make a log file with a valid header, but no data. */
        FileManager fileManager = envImpl.getFileManager();
        FileManagerTestUtils.createLogFile(fileManager, envImpl, 1000);
        fileManager.clear();

        LNFileReader reader =
            new LNFileReader(envImpl,
                             1000,             // read buffer size
                             DbLsn.NULL_LSN,   // start lsn
                             true,             // redo
                             DbLsn.NULL_LSN,   // end of file lsn
                             DbLsn.NULL_LSN,   // finish lsn
                             null,             // single file
                             DbLsn.NULL_LSN);  // ckpt end lsn
        addUserLNTargetTypes(reader);
        assertFalse("Empty file should not have entries",
                    reader.readNextEntry());
    }

    /**
     * Run with defaults, read whole log for redo, going forwards.
     */
    @Test
    public void testBasicRedo()
        throws Throwable {

        try {
            DbConfigManager cm =  envImpl.getConfigManager();
            doTest(50,
                   cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE),
                   0,
                   false,
                   true);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Run with defaults, read whole log for undo, going backwards.
     */
    @Test
    public void testBasicUndo()
        throws Throwable {

        try {
            DbConfigManager cm =  envImpl.getConfigManager();
            doTest(50,
                   cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE),
                   0,
                   false,
                   false);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Run with very small read buffer for redo, and track LNs.
     */
    @Test
    public void testSmallBuffersRedo()
        throws IOException, DatabaseException {

        doTest(50, 10, 0, true, true);
    }

    /**
     * Run with very small read buffer for undo and track LNs.
     */
    @Test
    public void testSmallBuffersUndo()
        throws IOException, DatabaseException {

        doTest(50, 10, 0, true, false);
    }

    /**
     * Run with medium buffers for redo.
     */
    @Test
    public void testMedBuffersRedo()
        throws IOException, DatabaseException {

        doTest(50, 100, 0, false, true);
    }

    /**
     * Run with medium buffers for undo.
     */
    @Test
    public void testMedBuffersUndo()
        throws IOException, DatabaseException {

        doTest(50, 100, 0, false, false);
    }

    /**
     * Start in the middle of the file for redo.
     */
    @Test
    public void testMiddleStartRedo()
        throws IOException, DatabaseException {

        doTest(50, 100, 20, true, true);
    }

    /**
     * Start in the middle of the file for undo.
     */
    @Test
    public void testMiddleStartUndo()
        throws IOException, DatabaseException {

        doTest(50, 100, 20, true, false);
    }

    /**
     * Create a log file, create the reader, read the log file
     * @param numIters each iteration makes 3 log entries (debug record, ln
     *           and mapLN
     * @param bufferSize to pass to reader
     * @param checkIndex where in the test data to start
     * @param trackLNs true if we're tracking LNS, false if we're tracking
     *           mapLNs
     */
    private void doTest(int numIters,
                        int bufferSize,
                        int checkIndex,
                        boolean trackLNs,
                        boolean redo)
        throws IOException, DatabaseException {

        checkList = new ArrayList<CheckInfo>();

        /* Fill up a fake log file. */
        long endOfFileLsn = createLogFile(numIters, trackLNs, redo);

        if (DEBUG) {
            System.out.println("eofLsn = " + endOfFileLsn);
        }

        /* Decide where to start. */
        long startLsn = DbLsn.NULL_LSN;
        long finishLsn = DbLsn.NULL_LSN;
        if (redo) {
            startLsn = checkList.get(checkIndex).lsn;
        } else {
            /* Going backwards. Start at last check entry. */
            int lastEntryIdx = checkList.size() - 1;
            startLsn = checkList.get(lastEntryIdx).lsn;
            finishLsn = checkList.get(checkIndex).lsn;
        }

        LNFileReader reader =
            new LNFileReader(envImpl, bufferSize, startLsn, redo, endOfFileLsn,
                             finishLsn, null, DbLsn.NULL_LSN);
        if (trackLNs) {
            addUserLNTargetTypes(reader);    
        } else {
            reader.addTargetType(LogEntryType.LOG_MAPLN);
        }

        if (!redo) {
            reader.addTargetType(LogEntryType.LOG_TXN_COMMIT);
        }

        /* read. */
        checkLogFile(reader, checkIndex, redo);
    }

    /**
     * Write a logfile of entries, put the entries that we expect to
     * read into a list for later verification.
     * @return end of file LSN.
     */
    private long createLogFile(int numIters, boolean trackLNs, boolean redo)
        throws IOException, DatabaseException {

        /*
         * Create a log file full of LNs, DeletedDupLNs, MapLNs and Debug
         * Records
         */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "foo", dbConfig);
        LogManager logManager = envImpl.getLogManager();
        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        DatabaseImpl mapDbImpl = envImpl.getDbTree().getDb(DbTree.ID_DB_ID);

        long lsn;
        Txn userTxn = Txn.createLocalTxn(envImpl, new TransactionConfig());
        long txnId = userTxn.getId();

        for (int i = 0; i < numIters; i++) {
            /* Add a debug record just to be filler. */
            Trace rec = new Trace("Hello there, rec " + (i+1));
            rec.trace(envImpl, rec);

            /* Make a transactional LN, we expect it to be there. */
            byte[] data = new byte[i+1];
            Arrays.fill(data, (byte)(i+1));
            LN ln = LN.makeLN(envImpl, data);
            byte[] key = new byte[i+1];
            Arrays.fill(key, (byte)(i+10));

            /*
             * Log an LN. If we're tracking LNs add it to the verification
             * list.
             */
            lsn = ln.log(
                envImpl, dbImpl,
                userTxn, new WriteLockInfo(),
                false, key,
                0 /*newExpiration*/, false /*newExpirationInHours*/,
                false, DbLsn.NULL_LSN, 0,
                true /*isInsertion*/,
                false, ReplicationContext.NO_REPLICATE).lsn;

            if (trackLNs) {
                checkList.add(new CheckInfo(lsn, ln, key, txnId));
            } 
            /*
             * Make a non-transactional LN. Shouldn't get picked up by reader.
             */
            data = Arrays.copyOf(data, data.length);
            LN nonTxnalLN = LN.makeLN(envImpl, data);
            nonTxnalLN.log(
                envImpl, dbImpl, null, null,
                false, key,
                0 /*newExpiration*/, false /*newExpirationInHours*/,
                false, DbLsn.NULL_LSN, 0,
                true /*isInsertion*/,
                false, ReplicationContext.NO_REPLICATE);

            /* Add a MapLN. */
            MapLN mapLN = new MapLN(dbImpl);

            lsn = mapLN.log(
                envImpl, mapDbImpl, null, null,
                false, key,
                0 /*newExpiration*/, false /*newExpirationInHours*/,
                false, DbLsn.NULL_LSN, 0,
                true /*isInsertion*/,
                false, ReplicationContext.NO_REPLICATE).lsn;

            if (!trackLNs) {
                checkList.add(new CheckInfo(lsn, mapLN, key, 0));
            }
        }

        long commitLsn = userTxn.commit(Durability.COMMIT_SYNC);

        /* We only expect checkpoint entries to be read in redo passes. */
        if (!redo) {
            checkList.add(new CheckInfo(commitLsn, null, null, txnId));
        }

        /* Make a marker log entry to pose as the end of file. */
        Trace rec = new Trace("Pretend this is off the file");
        long lastLsn = rec.trace(envImpl, rec);
        db.close();
        logManager.flushSync();
        envImpl.getFileManager().clear();
        return lastLsn;
    }

    private void checkLogFile(LNFileReader reader,
                              int checkIndex,
                              boolean redo)
        throws DatabaseException {

        LN lnFromLog;
        byte[] keyFromLog;

        /* Read all the LNs. */
        int i;
        if (redo) {
            /* start where indicated. */
            i = checkIndex;
        } else {
            /* start at the end. */
            i = checkList.size() - 1;
        }

        while (reader.readNextEntry()) {

            CheckInfo expected = checkList.get(i);

            /* Check LSN. */
            assertEquals("LSN " + i + " expected " +
                         DbLsn.getNoFormatString(expected.lsn) +
                         " but read " +
                         DbLsn.getNoFormatString(reader.getLastLsn()),
                         expected.lsn,
                         reader.getLastLsn());

            if (reader.isLN()) {

                /* Check the LN. */
                LNLogEntry<?> lnEntry = reader.getLNLogEntry();
                lnEntry.postFetchInit(false /*isDupDb*/);
                lnFromLog = lnEntry.getLN();
                LN expectedLN = expected.ln;
                assertEquals("Should be the same type of object",
                             expectedLN.getClass(),
                             lnFromLog.getClass());

                if (DEBUG) {
                    if (!expectedLN.toString().equals(lnFromLog.toString())) {
                        System.out.println("expected = " +
                                           expectedLN.toString()+
                                           "lnFromLog = " +
                                           lnFromLog.toString());
                    }
                }

                /*
                 * Don't expect MapLNs to be equal, since they change as
                 * logging occurs and utilization info changes.
                 */
                if (!(expectedLN instanceof MapLN)) {
                    assertEquals("LN " + i + " should match",
                                 expectedLN.toString(),
                                 lnFromLog.toString());
                }

                /* Check the key. */
                keyFromLog = lnEntry.getKey();
                byte[] expectedKey = expected.key;
                if (DEBUG) {
                    if (!Arrays.equals(expectedKey, keyFromLog)) {
                        System.out.println("expectedKey=" + expectedKey +
                                           " logKey=" + keyFromLog);
                    }
                }

                assertTrue("Key " + i + " should match",
                           Arrays.equals(expectedKey, keyFromLog));

                if (expected.txnId != 0) {
                    assertEquals(expected.txnId,
                                 reader.getTxnId().longValue());
                }

            } else {
                /* Should be a txn commit record. */
                assertEquals(expected.txnId,
                             reader.getTxnCommitId());
            }

            if (redo) {
                i++;
            } else {
                i--;
            }
        }
        int expectedCount = checkList.size() - checkIndex;
        assertEquals(expectedCount, reader.getNumRead());
    }

    private class CheckInfo {
        long lsn;
        LN ln;
        byte[] key;
        long txnId;

        CheckInfo(long lsn, LN ln, byte[] key, long txnId) {
            this.lsn = lsn;
            this.ln = ln;
            this.key = key;
            this.txnId = txnId;
        }
    }
}
