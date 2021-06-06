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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;

/**
 * Check our ability to adjust the file reader buffer size.
 */
public class FileReaderBufferingTest extends TestBase {

    private final File envHome;
    private Environment env;
    private EnvironmentImpl envImpl;
    private ArrayList<Long> expectedLsns;
    private ArrayList<String> expectedVals;

    public FileReaderBufferingTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    /**
     * Should overflow once and then grow.
     */
    @Test
    public void testBasic()
        throws Exception {

        readLog(1050,   // starting size of object in entry
                0,      // object growth increment
                100,    // starting read buffer size
                "3000", // max read buffer size
                0);     // expected number of overflows.
    }

    /**
     * Should overflow once and then grow.
     */
    @Test
    public void testCantGrow()
        throws Exception {

        readLog(2000,   // starting size of object in entry
                0,      // object growth increment
                100,    // starting read buffer size
                "1000", // max read buffer size
                10);    // expected number of overflows.
    }

    /**
     * Should overflow, grow, and then reach the max.
     */
    @Test
    public void testReachMax()
        throws Exception {

        readLog(1000,   // size of object in entry
                1000,      // object growth increment
                100,    // starting read buffer size
                "3500", // max read buffer size
                7);     // expected number of overflows.
    }
    /**
     *
     */
    private void readLog(int entrySize,
                         int entrySizeIncrement,
                         int readBufferSize,
                         String bufferMaxSize,
                         int expectedOverflows)
        throws Exception {

        try {

            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setAllowCreate(true);
            envConfig.setConfigParam
                (EnvironmentParams.LOG_ITERATOR_MAX_SIZE.getName(),
                 bufferMaxSize);
            env = new Environment(envHome, envConfig);

            envImpl = DbInternal.getNonNullEnvImpl(env);

            /* Make a log file */
            createLogFile(10, entrySize, entrySizeIncrement);
            SearchFileReader reader =
                new SearchFileReader(envImpl,
                                     readBufferSize,
                                     true,
                                     DbLsn.longToLsn
                                     (expectedLsns.get(0)),
                                     DbLsn.NULL_LSN,
                                     LogEntryType.LOG_TRACE);

            Iterator<Long> lsnIter = expectedLsns.iterator();
            Iterator<String> valIter = expectedVals.iterator();
            while (reader.readNextEntry()) {
                Trace rec = (Trace) reader.getLastObject();
                assertTrue(lsnIter.hasNext());
                assertEquals(reader.getLastLsn(),
                             DbLsn.longToLsn(lsnIter.next()));
                assertEquals(valIter.next(), rec.getMessage());
            }
            assertEquals(10, reader.getNumRead());
            assertEquals(expectedOverflows, reader.getNRepeatIteratorReads());

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            env.close();
        }
    }

    /**
     * Write a logfile of entries, put the entries that we expect to
     * read into a list for later verification.
     * @return end of file LSN.
     */
    private void createLogFile(int numItems, int size, int sizeIncrement)
        throws IOException, DatabaseException {

        LogManager logManager = envImpl.getLogManager();
        expectedLsns = new ArrayList<Long>();
        expectedVals = new ArrayList<String>();

        for (int i = 0; i < numItems; i++) {
            /* Add a debug record just to be filler. */
            int recordSize = size + (i * sizeIncrement);
            byte[] filler = new byte[recordSize];
            Arrays.fill(filler, (byte)i);
            String val = StringUtils.fromUTF8(filler);

            Trace rec = new Trace(val);
            long lsn = rec.trace(envImpl, rec);
            expectedLsns.add(new Long(lsn));
            expectedVals.add(val);
        }

        logManager.flushSync();
        envImpl.getFileManager().clear();
    }
}
