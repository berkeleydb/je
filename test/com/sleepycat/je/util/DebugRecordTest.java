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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.SearchFileReader;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TracerFormatter;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * This test originally ran in dual mode. After changes were made that started
 * making replication recovery run an different path, this expected entries
 * in the log began to differ substantially enough between a replicated
 * and non-replicated environment, and the dual mode version was removed.
 * If more test cases are eventually added to this test, we may want to
 * return to dual mode.
 */
public class DebugRecordTest extends DualTestCase {
    private File envHome;
    private Environment env;

    public DebugRecordTest() {
        envHome = SharedTestUtils.getTestDir();
        env = null;
    }

    @Test
    public void testDebugLogging()
        throws DatabaseException, IOException {

        try {

            /*
             * Turn on the txt file and db log logging, turn off the console.
             */
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setConfigParam
                (EnvironmentParams.NODE_MAX.getName(), "6");
            envConfig.setAllowCreate(true);
            /* Disable noisy cleaner DB creation. */
            DbInternal.setCreateUP(envConfig, false);
            DbInternal.setCreateEP(envConfig, false);
            /* Don't run the cleaner without a UtilizationProfile. */
            envConfig.setConfigParam
                (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
            envConfig.setTransactional(true);
        
            env = create(envHome, envConfig);
        
            EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

            List<Trace> expectedDbLogRecords = new ArrayList<Trace>();
            List<Trace> expectedFileRecords = new ArrayList<Trace>();

            /* Log a message. */
            Trace.trace(envImpl, "hi there");
            expectedDbLogRecords.add(new Trace("hi there"));

            /* 
             * Log an exception. The je.info file defaults to SEVERE, and will
             * only hold exceptions.
             */
            RuntimeException e = new RuntimeException("fake exception");
            LoggerUtils.traceAndLogException(envImpl, "DebugRecordTest", 
                                     "testException", "foo", e);
            Trace exceptionTrace = new Trace("foo\n" + 
                                             LoggerUtils.getStackTrace(e));
            expectedDbLogRecords.add(exceptionTrace);

            /* Log a split and flush the log to disk. */
            envImpl.getLogManager().flushSync();
            envImpl.getFileManager().clear();

            /* Verify. */
            checkDatabaseLog(expectedDbLogRecords);
            checkTextFile(expectedFileRecords);

        } finally {
            if (env != null) {
                close(env);
            }
        }
    }

    /**
     * Check what's in the database log.
     */
    private void checkDatabaseLog(List<Trace> expectedList)
        throws DatabaseException {

        SearchFileReader searcher =
            new SearchFileReader(DbInternal.getNonNullEnvImpl(env),
                    1000, true, DbLsn.NULL_LSN,
                                 DbLsn.NULL_LSN, LogEntryType.LOG_TRACE);

        int numSeen = 0;
        while (searcher.readNextEntry()) {
            Trace dRec = (Trace) searcher.getLastObject();
            assertEquals("Should see this as " + numSeen + " record: ",
                         expectedList.get(numSeen).getMessage(),
                         dRec.getMessage());
            numSeen++;
        }

        assertEquals("Should see this many debug records",
                     expectedList.size(), numSeen);
    }

    /**
     * Check what's in the text file.
     */
    private void checkTextFile(List<Trace> expectedList)
        throws IOException {

        FileReader fr = null;
        BufferedReader br = null;
        try {
            String textFileName = 
                DbInternal.getNonNullEnvImpl(env).getEnvironmentHome() +
                File.separator + "je.info.0";
            fr = new FileReader(textFileName);
            br = new BufferedReader(fr);

            String line = br.readLine();
            int numSeen = 0;

            /*
             * Read the file, checking only lines that start with valid Levels.
             */
            while (line != null) {
                try {
                    /* The line should start with a valid date. */
                    ParsePosition pp = new ParsePosition(0);
                    DateFormat ff = TracerFormatter.makeDateFormat();
                    ff.parse(line, pp);

                    /* There should be a java.util.logging.level next. */
                    int dateEnd = pp.getIndex();
                    int levelEnd = line.indexOf(" ", dateEnd + 1);
                    String possibleLevel = line.substring(dateEnd + 1,
                                                          levelEnd);
                    Level.parse(possibleLevel);

                    String expected =
                        expectedList.get(numSeen).getMessage();
                    StringBuilder seen = new StringBuilder();
                    seen.append(line.substring(levelEnd + 1));
                    /*
                     * Assemble the log message by reading the right number
                     * of lines
                     */
                    StringTokenizer st =
                        new StringTokenizer(expected,
                                            Character.toString('\n'), false);

                    for (int i = 1; i < st.countTokens(); i++) {
                        seen.append('\n');
                        String l = br.readLine();
                        seen.append(l);
                        if (i == (st.countTokens() -1)) {
                            seen.append('\n');
                        }
                    }
                    /* XXX, diff of multiline stuff isn't right yet. */
                    
                    /*
                     * The formatters for rep test and non-rep test 
                     * different, so ignore this check here.
                     */
                    if (!isReplicatedTest(getClass())) {
                        if (st.countTokens() == 1) {
                            assertEquals("Line " + numSeen +
                                         " should be the same",
                                         expected, seen.toString());
                        }
                    }
                    numSeen++;
                } catch (Exception e) {
                    /* Skip this line, not a message. */
                }
                line = br.readLine();
            }
            assertEquals("Should see this many debug records",
                         expectedList.size(), numSeen);
        } finally {
            if (br != null) {
                br.close();
            }

            if (fr != null) {
                fr.close();
            }
        }
    }
}
