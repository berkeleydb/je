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

package com.sleepycat.je.logversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.VersionMismatchException;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Tests log file header versioning.  This test is used in conjunction with
 * MakeLogHeaderVersionData, a main program that was used once to generate two
 * log files with maximum and minimum valued header version numbers.
 *
 * @see MakeLogHeaderVersionData
 */
public class LogHeaderVersionTest extends TestBase {

    private File envHome;

    public LogHeaderVersionTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {

        envHome = null;
    }

    /**
     * Tests that an exception is thrown when a log header is read with a newer
     * version than the current version.  The maxversion.jdb log file is loaded
     * as a resource by this test and written as a regular log file.  When the
     * environment is opened, we expect a VersionMismatchException.
     */
    @Test
    public void testGreaterVersionNotAllowed()
        throws IOException {

        TestUtils.loadLog(getClass(), Utils.MAX_VERSION_NAME, envHome);

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(false);
        envConfig.setTransactional(true);

        try {
            Environment env = new Environment(envHome, envConfig);
            try {
                env.close();
            } catch (Exception ignore) {}
        } catch (VersionMismatchException e) {
            /* Got VersionMismatchException as expected. */
            return;
        }
        fail("Expected VersionMismatchException");
    }

    /**
     * Tests that when a file is opened with a lesser version than the current
     * version, a new log file is started for writing new log entries.  This is
     * important so that the new header version is written even if no new log
     * file is needed.  If the new version were not written, an older version
     * of JE would not recognize that there had been a version change.
     */
    @Test
    public void testLesserVersionNotUpdated()
        throws DatabaseException, IOException {

        TestUtils.loadLog(getClass(), Utils.MIN_VERSION_NAME, envHome);
        File logFile = new File(envHome, TestUtils.LOG_FILE_NAME);
        long origFileSize = logFile.length();

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(false);
        envConfig.setTransactional(true);

        Environment env = new Environment(envHome, envConfig);
        env.sync();
        env.close();

        assertEquals(origFileSize, logFile.length());
    }
}
