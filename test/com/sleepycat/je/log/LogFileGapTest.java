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

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/*
 * Test whether JE can correctly find the FileHeader.lastEntryInPrevFileOffset
 * when the FileManager.truncateLog is invoked, also test that JE can throw out
 * an EnvironmentFailureException rather than hang when a log file gap is 
 * detected while reading backwards during recovery, see SR [#19463]. 
 */
public class LogFileGapTest extends TestBase {
    private static final String DB_NAME = "testDb";
    private final File envHome;
    private Environment env;

    public LogFileGapTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        
        if (env != null) {
            env.close();
        }
    }

    /* 
     * Test the case that we can recover after truncating some log entries on
     * an invalidated Environment, it will truncate to the FileHeader.
     */
    @Test
    public void testLogFileAllTruncate() 
        throws Throwable {

        createEnvAndData();
        createLogFileGapAndRecover(true, true, false);
    }

    /*
     * Test the case that we can recover after truncating some log entries on
     * an invalidated Environment, it won't truncate the FileHeader.
     */
    @Test
    public void testLogFileNotTruncateAll()
        throws Throwable {

        createEnvAndData();
        createLogFileGapAndRecover(true, false, false);
    }

    /*
     * Test that an EnvironmentFailureException will be thrown if a log file 
     * gap is detected during the recovery.
     */
    @Test
    public void testLogFileGapRecover()
        throws Throwable {

        createEnvAndData();
        createLogFileGapAndRecover(false, false, true);
    }

    /* Create some data in the databases. */
    private void createEnvAndData() {
        env = new Environment(envHome, createEnvConfig());

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database db = env.openDatabase(null, DB_NAME, dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[200]);
        for (int i = 1; i <= 50; i++) {
            IntegerBinding.intToEntry(i, key);
            assertTrue(OperationStatus.SUCCESS == db.put(null, key, data));
        }
        db.close();
    }

    private EnvironmentConfig createEnvConfig() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        /* Disable all daemon threads. */
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        envConfig.setConfigParam
            (EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        envConfig.setConfigParam
            (EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "2000");

        return envConfig;
    }

    /* 
     * Truncate the log file to create a log file gap and see if we correctly
     * throw out an EnvironmentFailureException.
     */
    private void createLogFileGapAndRecover(boolean invalidateEnvironment,
                                            boolean truncateFileHeader,
                                            boolean setLogFileEnd)
        throws Throwable {

        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        int readBufferSize = envImpl.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        LastFileReader fileReader =
            new LastFileReader(envImpl, readBufferSize);

        /*
         * If we want to truncate to the FileHeader, the reader will only read
         * once, otherwise, it will read two entries. 
         */
        int threshold = (truncateFileHeader ? 1 : 2);
        int counter = 0;
        while (fileReader.readNextEntry()) {
            counter++;
            if (counter == threshold) {
                break;
            }
        }

        /* Calculate the lsn that we want to truncate to. */
        long truncatedLsn = fileReader.getLastLsn();

        if (invalidateEnvironment) {
            envImpl.invalidate(EnvironmentFailureException.unexpectedState
                    ("Invalidate Environment for testing"));
            envImpl.abnormalClose();
        }

        if (setLogFileEnd) {
            envImpl.getFileManager().truncateSingleFile
                (DbLsn.getFileNumber(truncatedLsn),
                 DbLsn.getFileOffset(truncatedLsn));
        } else {
            envImpl.getFileManager().truncateLog
                (DbLsn.getFileNumber(truncatedLsn),
                 DbLsn.getFileOffset(truncatedLsn));
        }

        /* Do a log file flip. */
        if (!invalidateEnvironment) {
            envImpl.forceLogFileFlip();
            envImpl.abnormalClose();
        }

        /* Recover the Environment. */
        try {
            env = new Environment(envHome, createEnvConfig());
            env.close();
        } catch (EnvironmentFailureException e) {
            /* Expected exceptions if a log file gap is detected. */
            assertTrue(e.getMessage(), setLogFileEnd);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
}
