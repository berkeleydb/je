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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.util.DbVerifyLog;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Test that we can set invisible bits in the log, and that we can ignore them
 * appropriately. Excluded from dual mode testing, because we are manipulating
 * the log files explicitly.
 */
public class InvisibleTest extends TestBase {

    private static boolean verbose = Boolean.getBoolean("verbose");
    private final File envHome;

    public InvisibleTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * @throws FileNotFoundException 
     */
    @Test
    public void testBasic() 
        throws FileNotFoundException {

        final String filler = "--------------------------------------------";

        Environment env = setupEnvironment();
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        List<TestInfo> allData = new ArrayList<TestInfo>();
        Map<Long, List<Long>> invisibleEntriesByFile = 
            new HashMap<Long, List<Long>>();
        try {
            LogManager logManager = envImpl.getLogManager();
            
            /* 
             * Setup test data. Insert a number of records, then make all the
             * even entries invisible.
             */
            long currentFile = -1;
            List<Long> invisibleLsns = null;
            for (int i = 0; i < 50; i++) {
                Trace t = new Trace("debug " + filler + i);
                long lsn = t.trace(envImpl, t);
                boolean isInvisible = true;
                if ((i % 2) == 0) {
                    if (currentFile != DbLsn.getFileNumber(lsn)) {
                        currentFile = DbLsn.getFileNumber(lsn);
                        invisibleLsns = new ArrayList<Long>();
                        invisibleEntriesByFile.put(currentFile, invisibleLsns);
                    }
                    invisibleLsns.add(lsn);
                } else {
                    isInvisible = false;
                }
                allData.add(new TestInfo(lsn, t, isInvisible));
            }

            /*
             * We want to run this test on multiple files, so make sure that
             * the invisible entries occupy at least three distinct files.
             */
            assertTrue("size=" + invisibleEntriesByFile.size(),
                       invisibleEntriesByFile.size() > 3);

            if (verbose) {
                for (TestInfo info : allData) {
                    System.out.println(info);
                }
                System.out.println("------------------------");
            }

            /* 
             * Invisibility marking only works on log entries that are 
             * on disk. Flush everything to disk.
             */
            logManager.flushSync();

            /* Make the specified set of entries invisible */
            makeInvisible(envImpl, invisibleEntriesByFile);

            /* Check that only the right tracer log entries are visible */
            scanOnlyVisible(envImpl, allData);

            /* Check that we can read both visible and invisible log entries. */
            scanAll(envImpl, allData, false /* onlyReadVisible */);

            /* 
             * Check that we can fetch invisible log entries through the log
             * manager. This kind of fetch is done when executing rollbacks and
             * constructing the txn chain.
             * We want the fetch to go disk and not to fetch from the log
             * buffers, so write some more stuff to the log so that all
             * invisible entries are flushed out of the log buffers.
             */
            for (int i = 0; i < 50; i++) {
                Trace t = new Trace("debug " + filler + i);
                t.trace(envImpl, t);
            }

            fetchInvisible(envImpl, allData);

            try {
                DbVerifyLog verifier = new DbVerifyLog(env);
                verifier.verifyAll();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Don't expect exceptions here.");
            }
        } finally {
            env.close();
            env = null;
        }
    }

    private Environment setupEnvironment() {
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        /* 
         * Turn off cleaning so that it doesn't interfere with
         * what is in the log.
         */
        DbInternal.disableParameterValidation(envConfig);

        /* 
         * Use uniformly small log files, to make the invisible bit cross
         * files.  
         */
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000");
        envConfig.setConfigParam("je.env.runCleaner", "false");

        /*
         * When we test fetching of invisible log entries, we want to make sure
         * we fetch from the file, and not from the un-modified, non-invisible
         * entry in the log buffer. Because of that, make log buffers small.
         */
        envConfig.setConfigParam("je.log.bufferSize", "1024");

        return new Environment(envHome, envConfig);
    }

    private void makeInvisible(EnvironmentImpl envImpl,
                               Map<Long, List<Long>> invisibleEntries) {

        FileManager fileManager = envImpl.getFileManager();
        for (Map.Entry<Long, List<Long>> entry : invisibleEntries.entrySet()) {
            fileManager.makeInvisible(entry.getKey(), entry.getValue());
        }

        fileManager.force(invisibleEntries.keySet());
    }

    private void scanOnlyVisible(EnvironmentImpl envImpl,
                                 List<TestInfo> allData) {
        scanAll(envImpl, allData, true /* onlyReadVisible */);
    }

    private void scanAll(EnvironmentImpl envImpl,
                         List<TestInfo> allData,
                         boolean onlyReadVisible) {

        /* 
         * Make a list of expected Trace entries. If we are skipping invisible
         * entries, only include the visible ones.
         */
        List<Trace> expectedData = new ArrayList<Trace>();
        for (TestInfo info : allData) {
            if (info.isInvisible) {
                if (!onlyReadVisible) {
                    expectedData.add(info.trace);
                }
            } else {
                expectedData.add(info.trace);
            }
        }   
            
        TestReader reader = new TestReader(envImpl, onlyReadVisible);
        int i = (onlyReadVisible)? 1 : 0;
        while (reader.readNextEntry()) {
            assertEquals(allData.get(i).isInvisible,
                         reader.isInvisible());
            i += (onlyReadVisible) ? 2 : 1;
        }
        assertEquals(expectedData, reader.getTraces());
    }

    /** 
     * Check that invisible log entries throw a checksum exception by default,
     * and that they can be read only through the special log manager method
     * that expects and fixes invisibility.
     * @throws FileNotFoundException 
     */
    private void fetchInvisible(EnvironmentImpl envImpl, 
                                List<TestInfo> allData) 
        throws FileNotFoundException {

        LogManager logManager = envImpl.getLogManager();
        for (TestInfo info : allData) {

            if (!info.isInvisible) {
                continue;
            } 

            SingleItemEntry<?> okEntry = (SingleItemEntry<?>) 
                logManager.getLogEntryAllowInvisible(info.lsn).getEntry();
            assertEquals(info.trace, okEntry.getMainItem());

            try {
                logManager.getLogEntry(info.lsn);
                fail("Should have thrown exception for " + info);
            } catch(EnvironmentFailureException expected) {
                assertEquals(EnvironmentFailureReason.LOG_INTEGRITY,
                             expected.getReason());
            } 
        }   
    }

    /* 
     * Struct to package together test information.
     */
    private static class TestInfo {
        final long lsn;
        final Trace trace;
        final boolean isInvisible;
        
        TestInfo(long lsn, Trace trace, boolean isInvisible) {
            this.lsn = lsn;
            this.trace = trace;
            this.isInvisible = isInvisible;
        }

        @Override
        public String toString() {
            return DbLsn.getNoFormatString(lsn) + 
            (isInvisible ? " INVISIBLE " : " visible ") + trace;
        }
    }

    /* 
     * A FileReader that can read visible or invisible entries, upon command.
     */
    private static class TestReader extends FileReader {

        private final boolean readVisible;
        private final LogEntry entry;

        private List<Trace> tracers;
        private Trace currentTrace;

        public TestReader(EnvironmentImpl envImpl, boolean readVisible) {

            super(envImpl, 
                  1024 /* readBufferSize*/,
                  true /* forward */,
                  0L, 
                  null /* singleFileNumber */,
                  DbLsn.NULL_LSN /* endOfFileLsn */,
                  DbLsn.NULL_LSN /* finishLsn */);
            this.readVisible = readVisible;
            this.entry = LogEntryType.LOG_TRACE.getSharedLogEntry();
            tracers = new ArrayList<Trace>();
        }

        /**
         * @return true if this reader should process this entry, or just
         * skip over it.
         * @throws DatabaseException from subclasses.
         */
        @Override
        protected boolean isTargetEntry()
            throws DatabaseException {

            if (readVisible && currentEntryHeader.isInvisible()) {
                return false;
            }

            if (currentEntryHeader.getType() == 
                LogEntryType.LOG_TRACE.getTypeNum()) {
                return true;
            }

            return false;
        }

        @Override
        protected boolean processEntry(ByteBuffer entryBuffer)
                throws DatabaseException {

            entry.readEntry(envImpl, currentEntryHeader, entryBuffer);
            currentTrace = (Trace) entry.getMainItem();
            tracers.add(currentTrace);
            return true;
        }
        
        public List<Trace> getTraces() {
            return tracers;
        }
        
        boolean isInvisible() {
            return currentEntryHeader.isInvisible();
        }

        Trace getCurrentTrace() {
            return currentTrace;
        }
    }
}
