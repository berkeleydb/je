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

package com.sleepycat.je.rep.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.TraceLogEntry;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.stream.VLSNTestUtils.CheckReader;
import com.sleepycat.je.rep.stream.VLSNTestUtils.CheckWireRecord;
import com.sleepycat.je.rep.stream.VLSNTestUtils.LogPopulator;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Exercise the FeederReader, which is used for scanning for vlsn tagged
 * entries.
 */
public class FeederReaderTest extends TestBase {
    private static int STRIDE = 5;
    private static int MAX_MAPPINGS = 5;
    private static final String SPACER = "space" + new String(new byte[100]) +
        "space";

    private final boolean verbose = Boolean.getBoolean("verbose");
    private final File envHome;
    private ReplicatedEnvironment rep;
    private CheckReader checker;

    public FeederReaderTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Check that we can retrieve the correct results when doing forward
     * scans. Create a log, and start forward scans at each VLSN. (i.e. VLSN 1,
     * VLSN2, etc.)
     */
    @Test
    public void testForwardScans()
        throws Throwable {

        rep = null;
        try {
            ArrayList<CheckWireRecord> expected = setupLog(10 /* numFiles */);
            VLSN lastVLSN = expected.get(expected.size() - 1).getVLSN();

            /* Vary the read buffer size. */
            for (int readBufferSize = 100;
                 readBufferSize < 2000;
                 readBufferSize += 100) {

                /* And also vary the start point. */
                for (int i = 1; i <= lastVLSN.getSequence(); i++) {
                    checkForwardScan(checker.nScanned, expected, new VLSN(i),
                                     readBufferSize,
                                     false /* expectLogGrowth */);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (rep != null) {
                rep.close();
                rep = null;
            }
        }
    }

    /**
     * Check that we can wait for an upcoming VLSN when doing forward
     * scans. Create a log, and start forward scans at each VLSN. (i.e. VLSN 1,
     * VLSN2, etc.), and then ask for one more.
     */
    @Test
    public void testWait()
        throws Throwable {

        rep = null;
        try {
            ArrayList<CheckWireRecord> expected = setupLog(3 /* numFiles */);
            checkForwardScan(checker.nScanned, expected, new VLSN(1),
                             99,
                             true /* expectLogGrowth */);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (rep != null) {
                rep.close();
                rep = null;
            }
        }
    }

    /**
     * Check that we can retrieve the correct results when doing backwards
     * scans. Create a log, and start forward scans at each VLSN. (i.e. VLSN
     * 50, VLSN 49, etc.)
     */
    @Test
    public void testBackwardScans()
        throws Throwable {

        rep = null;
        try {
            ArrayList<CheckWireRecord> expected = setupLog(10 /* numFiles */);
            VLSN lastVLSN = expected.get(expected.size() - 1).getVLSN();

            for (long i = lastVLSN.getSequence(); i >= 1;  i--) {
                checkBackwardScan(checker.nScanned, expected, new VLSN(i),
                                  90909 /* readBufferSize */);
                checkBackwardScan(checker.nScanned, expected, new VLSN(i),
                                  1000 /* readBufferSize */);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (rep != null) {
                rep.close();
                rep = null;
            }
        }
    }

    /**
     * Check that we can find the sync-able matchpoints when doing backward
     * scans.
     */
    @Test
    public void testFindSyncableentries()
        throws Throwable {

        rep = null;
        try {
            ArrayList<CheckWireRecord> expected = setupLog(10 /* numFiles */);
            VLSN lastVLSN = expected.get(expected.size() - 1).getVLSN();

            for (long i = lastVLSN.getSequence(); i >= 1;  i--) {
                checkSyncScan(checker.nScanned, expected, new VLSN(i),
                              90909 /* readBufferSize */);
                checkSyncScan(checker.nScanned, expected, new VLSN(i),
                              1000 /* readBufferSize */);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (rep != null) {
                rep.close();
                rep = null;
            }
        }
    }

    private class Inserter implements LogPopulator {
        private final int desiredNumFiles;

        Inserter(int numFiles) {
            this.desiredNumFiles = numFiles;
        }

        @SuppressWarnings("hiding")
        @Override
        public void populateLog(ReplicatedEnvironment rep) {

            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(rep);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);

            FileManager fileManager = envImpl.getFileManager();

            Database db = rep.openDatabase(null, "test", dbConfig);
            try {
                DatabaseEntry value = new DatabaseEntry();
                for (int i = 0; 
                     fileManager.getLastFileNum() < desiredNumFiles; 
                     i++) {

                    Transaction txn = rep.beginTransaction(null, null);
                    IntegerBinding.intToEntry(i, value);
                    OperationStatus status =  db.put(txn, value, value);
                    if (status != OperationStatus.SUCCESS) {
                        throw new IllegalStateException("bad status of " + 
                                                        status);
                    }
                    Trace.trace(envImpl, SPACER);
                    txn.commit();
                }
            } finally {
                if (db != null){
                    db.close();
                    db = null;
                }
            }
            
        }
    }

    /** Start with an empty log. */
    private class NoInsert implements LogPopulator {
        public void populateLog(ReplicatedEnvironment rep) {
        }
    }

    private ArrayList<CheckWireRecord> setupLog(int numFiles)
        throws UnknownMasterException, DatabaseException,
               InterruptedException {
        return setupLog(new Inserter(numFiles));
    }

    private ArrayList<CheckWireRecord> setupLog(LogPopulator populator)
        throws UnknownMasterException, DatabaseException,
               InterruptedException {

        rep = VLSNTestUtils.setupLog(envHome, STRIDE, MAX_MAPPINGS, populator);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(rep);
        checker = new CheckReader(envImpl);
        return VLSNTestUtils.collectExpectedData(checker);
    }

    /**
     * Start a FeederReader, start scanning from startVLSN forwards.
     */
    private void checkForwardScan(long nCheckerScans,
                                  ArrayList<CheckWireRecord> expected,
                                  VLSN startVLSN,
                                  int readBufferSize,
                                  boolean expectLogGrowth)
        throws DatabaseException, IOException, InterruptedException {

        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(rep);
        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(rep).getVLSNIndex();
        FeederReader feederReader = new FeederReader(envImpl,
                                                     vlsnIndex,
                                                     DbLsn.NULL_LSN,
                                                     readBufferSize,
                                                     true /*bypassCache*/);
        if (verbose) {
            System.out.println("test forward scan starting at " + startVLSN +
                               " readBufferSize = " + readBufferSize +
                               " expectLogGrowth = " + expectLogGrowth);
        }
        feederReader.initScan(startVLSN);

        /* Read every replicated log entry with the feederReader. */
        for (CheckWireRecord w : expected){

            VLSN vlsn = w.getVLSN();
            if (vlsn.compareTo(startVLSN) < 0) {
                /* not checking yet, just go on. */
                continue;
            }

            /* Ask the feederReader for this. */
            OutputWireRecord feederRecord = feederReader.scanForwards(vlsn, 0);

            /*
             * Compare the contents. Can't use w.equals(), since equals
             * can only be used before the WireRecord's entryBuffer has been
             * reused.
             */
            assertTrue("check=" + w + " feederRecord= " + feederRecord,
                       w.exactMatch(feederRecord));
        }

        /* Check that the feeder reader has done some repositioning. */
        VLSN lastVLSN = expected.get(expected.size() - 1).getVLSN();
        int minimumMappings =
            (int)(lastVLSN.getSequence() - startVLSN.getSequence()) / STRIDE;
        assertTrue(feederReader.getNReposition() >= minimumMappings);
        assertTrue(feederReader.getNScanned() >= minimumMappings);
        assertTrue(nCheckerScans > feederReader.getNScanned());
        if (verbose) {
            System.out.println("repos=" + feederReader.getNReposition() +
                               " checkscan=" + nCheckerScans +
                               " feedscan=" + feederReader.getNScanned() +
                               " minMappings=" + minimumMappings);
        }

        /* Ask for one more vlsn. It should time out. */
        if (expectLogGrowth) {
            WireRecord notThere = feederReader.scanForwards(lastVLSN.getNext(),
                                                            5);
            assertEquals(null, notThere);
        }

    }

    private void checkBackwardScan(long nCheckerScans,
                                   ArrayList<CheckWireRecord> expected,
                                   VLSN startVLSN,
                                   int readBufferSize)
        throws DatabaseException, IOException, ChecksumException {

        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(rep);
        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(rep).getVLSNIndex();
        long lastLsn =  envImpl.getFileManager().getLastUsedLsn();

        /* Try both kinds of backwards readers */
        FeederSyncupReader feederSyncupReader =
            new FeederSyncupReader(envImpl,
                                   vlsnIndex,
                                   lastLsn,
                                   readBufferSize,
                                   startVLSN,
                                   DbLsn.NULL_LSN);
        ReplicaSyncupReader replicaSyncupReader =
            new ReplicaSyncupReader(envImpl,
                                    vlsnIndex,
                                    lastLsn,
                                    readBufferSize,
                                    startVLSN,
                                    DbLsn.NULL_LSN,
                                    new MatchpointSearchResults(envImpl));

        if (verbose) {
            System.out.println("->lastLsn = " +
                               DbLsn.getNoFormatString(lastLsn) +
                               " startVLSN = " + startVLSN);
        }

        /* Read every replicated log entry with the feederReader. */
        for (int i = expected.size() - 1; i >= 0; i--) {
            CheckWireRecord w = expected.get(i);
            VLSN vlsn = w.getVLSN();
            if (vlsn.compareTo(startVLSN) > 0) {
                /* not checking yet, just go on. */
                continue;
            }

            /* Ask the readers for this. */
            OutputWireRecord feederRecord =
                feederSyncupReader.scanBackwards(vlsn);
            OutputWireRecord replicaRecord =
                replicaSyncupReader.scanBackwards(vlsn);

            /*
             * Compare the contents. Can't use w.equals(), since equals
             * can only be used before the WireRecord's entryBuffer has been
             * reused, and in this case, the WireRecords are saved in a
             * collection.
             */
            assertTrue("feeder check=" + w + " feederRecord= " + feederRecord,
                       w.exactMatch(feederRecord));
            assertTrue("replica check=" + w + " replicaRecord= " +
                       replicaRecord, w.exactMatch(replicaRecord));

        }

        /*
         * Check that the feeder reader has done some repositioning.  The way
         * the mappings work, it's harder to assert as rigorously as the
         * forward scans that the number of repositions and scans are a certain
         * value.
         */
        int minimumMappings = (int)(startVLSN.getSequence()/ STRIDE);
        if (verbose) {
            System.out.println
                ("feeder repos=" + feederSyncupReader.getNReposition() +
                 "replica repos=" + replicaSyncupReader.getNReposition() +
                 " checkscan=" + nCheckerScans +
                 " feedscan=" + feederSyncupReader.getNScanned() +
                 " numVLSNs=" + minimumMappings);
        }

        assertTrue(nCheckerScans > feederSyncupReader.getNScanned());
        assertEquals(0, replicaSyncupReader.getNReposition());
        if (minimumMappings > 2) {
            assertTrue(feederSyncupReader.getNReposition() >= minimumMappings);
        }
    }

    private void checkSyncScan(@SuppressWarnings("unused") long nCheckerScans,
                               ArrayList<CheckWireRecord> expected,
                               VLSN startVLSN,
                               int readBufferSize)
        throws DatabaseException, IOException {

        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(rep);
        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(rep).getVLSNIndex();
        long lastLsn =  envImpl.getFileManager().getLastUsedLsn();
        ReplicaSyncupReader backwardsReader =
            new ReplicaSyncupReader(envImpl,
                                vlsnIndex,
                                lastLsn,
                                readBufferSize,
                                startVLSN,
                                DbLsn.NULL_LSN,
                                new MatchpointSearchResults(envImpl));

        /* Ask the feederReader for the start entry. */
        OutputWireRecord syncupRecord =
            backwardsReader.scanBackwards(startVLSN);

        int checkIndex = (int) (startVLSN.getSequence() - 1);
        CheckWireRecord check = expected.get(checkIndex);
        assertTrue("check=" + check + " syncupRecord= " + syncupRecord,
                   check.exactMatch(syncupRecord));

        /*
         * Now search backwards for syncable entries. Iterate through the
         * expected array, stopping at log entries of the right kind.
         * then check the feederReader's ability to also stop at that kind
         * of log entry.
         */
        if (verbose) {
            System.out.println("checking starting from  " + startVLSN);
        }
        for (int i = checkIndex - 1; i >= 0; i--) {
            check = expected.get(i);
            if (LogEntryType.isSyncPoint(check.getEntryType())) {
                syncupRecord = backwardsReader.findPrevSyncEntry(true);
                if (verbose) {
                    System.out.print("i= " + i);
                    System.out.println(check);
                }

                assertTrue("check=" + check + " syncupRecord= " + syncupRecord,
                           check.exactMatch(syncupRecord));
            }
        }
    }

    /**
     * Test that a feeder reader can read log entries that have not yet
     * been flushed to disk.
     * @throws Throwable
     */
    @Test
    public void testNonFlushedFetch()
        throws Throwable {
        rep = null;
        try {
            /* Create a replicator, and create a log. */
            ArrayList<CheckWireRecord> expected = setupLog(2 /* numFiles */);

            for (int i = 0; i < 4; i++) {
                logItem(expected, i, true /* replicate */, false /* sync */);
            }
            VLSN lastVLSN = expected.get(expected.size() - 1).getVLSN();

            for (int i = 1; i <= lastVLSN.getSequence(); i++) {
                checkForwardScan(checker.nScanned, expected, new VLSN(i),
                                 90909 /* readBufferSize */,
                                 true /* expectLogGrowth */);
                checkForwardScan(checker.nScanned, expected, new VLSN(i),
                                 1000 /* readBufferSize */,
                                 true /* expectLogGrowth */);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (rep != null) {
                rep.close();
                rep = null;
            }
        }
    }

    private void logItem(ArrayList<CheckWireRecord> expected,
                         int traceNumber,
                         boolean replicate,
                         boolean sync)
        throws DatabaseException {

        LogManager logManager =
            DbInternal.getNonNullEnvImpl(rep).
            getLogManager();

        Trace debugMsg = new Trace("Test " + traceNumber);
        LogEntry entry = new TraceLogEntry(debugMsg);

        ByteBuffer buffer = ByteBuffer.allocate(entry.getSize());
        entry.writeEntry(buffer);
        buffer.flip();
        if (replicate) {
            long lsn;
            if (sync) {
                lsn = logManager.logForceFlush(entry,
                                               false,
                                               ReplicationContext.MASTER);
            } else {
                lsn = logManager.log(entry, ReplicationContext.MASTER);
            }

            VLSNIndex vlsnIndex =
                RepInternal.getNonNullRepImpl(rep).getVLSNIndex();
            CheckWireRecord c =
                new CheckWireRecord(RepInternal.getNonNullRepImpl(rep), lsn,
                                    LogEntryType.LOG_TRACE.getTypeNum(),
                                    LogEntryType.LOG_VERSION,
                                    entry.getSize(),
                                    vlsnIndex.getRange().getLast(),
                                    buffer);
            expected.add(c);
        } else {
            if (sync) {
                logManager.logForceFlush(entry,
                                         false,
                                         ReplicationContext.NO_REPLICATE);
            } else {
                logManager.log(entry, ReplicationContext.NO_REPLICATE);
            }
        }
    }

    /**
     * Test that a feeder reader can switch between reading from the log
     * buffer pool, to the file, to the logBuffer pool, and back and forth.
     * @throws Throwable
     */
    @Test
    public void testSwitchFetch()
        throws Throwable {

        rep = null;
        for (int readBufferSize = 100;
             readBufferSize < 2000;
             readBufferSize += 100) {

            /*
             * The startIndex parameter for readAndGrow indicates where the
             * feeder reader should start. In different loops, we run the
             * reader starting at successively different location -- i.e
             * starting the scan at vlsn1, vlsn2, etc. That varies the location
             * of the items in the readBuffer. We need to call readAndGrow once
             * to establish how many items are created by the test.
             */
            int end;
            try {
                end = readAndGrow(0, readBufferSize);
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            } finally {
                if (rep != null) {
                    rep.close();
                    rep = null;
                }
                RepTestUtils.removeRepEnvironments(envHome);
            }

            /*
             * Now that we know how many test items are created, rerun the
             * test, starting the scan at different places.
             */
            for (int i = 1; i < end; i++) {
                try {
                    readAndGrow(i, readBufferSize);
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    if (rep != null) {
                        rep.close();
                        rep = null;
                    }
                    RepTestUtils.removeRepEnvironments(envHome);
                }
            }
        }
    }

    /**
     * Mimic a feeder that is reading as a log is growing.
     * @throws InterruptedException
     * @throws IOException
     * @throws DatabaseException
     * @throws UnknownMasterException
     */
    private int readAndGrow(int startIndex, int readBufferSize)
        throws DatabaseException, IOException, InterruptedException {

        if (verbose) {
            System.out.println("readAndGrow start at " + startIndex +
                               " readBufferSize =" + readBufferSize);
        }

        /*
         * Create a replicator. Even though we don't create any
         * application data, there will be a few replicated log entries
         * for the group db
         */
        ArrayList<CheckWireRecord> expected = setupLog(new NoInsert());

        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(rep).getVLSNIndex();
        EnvironmentImpl envImpl =
            DbInternal.getNonNullEnvImpl(rep);
        FeederReader feederReader = new FeederReader(envImpl,
                                                     vlsnIndex,
                                                     DbLsn.NULL_LSN,
                                                     readBufferSize,
                                                     true /*bypassCache*/);

        for (int i = 0; i < 4; i++) {
            logItem(expected, i, true /* replicate */, false /* sync */);
        }

        /* Read items that are still in the log buffer. */
        checkContinuedScan(feederReader, expected, 0, startIndex);
        int nextStart = expected.size();

        for (int i = 4; i < 8; i++) {
            logItem(expected, i, true /* replicate */, false /* sync */);
        }

        /* Read items that are still in the log buffer. */
        checkContinuedScan(feederReader, expected, nextStart, startIndex);
        nextStart = expected.size();

        for (int i = 8; i < 18; i++) {
            logItem(expected, i, true /* replicate */, true /* sync */);
        }

        /*
         * Read items that should have been flushed out of the log
         * buffers.
         */
        checkContinuedScan(feederReader, expected, nextStart, startIndex);
        nextStart = expected.size();

        for (int i = 18; i < 20; i++) {
            logItem(expected, i, true /* replicate */, false /* sync */);
        }

        /* Read items that should still be in log buffers. */
        checkContinuedScan(feederReader, expected, nextStart, startIndex);
        nextStart = expected.size();

        /*
         * Read items that should have been flushed out of the log
         * buffers.
         */
        for (int i = 20; i < 30; i++) {
            logItem(expected, i, true /* replicate */, true /* sync */);
        }
        checkContinuedScan(feederReader, expected, nextStart, startIndex);
        nextStart = expected.size();

        /*
         * Read items that should have been flushed out of the log
         * buffers.
         */
        for (int i = 30; i < 40; i++) {
            logItem(expected, i, true /* replicate */, false /* sync */);
        }
        checkContinuedScan(feederReader, expected, nextStart, startIndex);
        return expected.size();
    }

    /**
     * In this case, the FeederReader has been doing some reading already.
     * Ask it to read some more, and check the results.
     */
    private void checkContinuedScan(FeederReader feederReader,
                                    ArrayList<CheckWireRecord> expected,
                                    int newRecordsStart,
                                    int feederStart)
        throws InterruptedException, DatabaseException, IOException {

        for (int i = newRecordsStart; i < expected.size(); i++) {
            CheckWireRecord c = expected.get(i);
            if (i == feederStart)  {
                feederReader.initScan(c.getVLSN());
            }

            if (i >= feederStart) {
                OutputWireRecord feederRecord =
                    feederReader.scanForwards(c.getVLSN(), 0);
                assertTrue("check=" + c + " feederRecord= " + feederRecord,
                           c.exactMatch(feederRecord));

            }
        }
    }
}
