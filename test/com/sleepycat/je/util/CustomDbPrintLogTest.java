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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.junit.JUnitProcessThread;
import com.sleepycat.je.log.LNFileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @excludeDualMode
 * This test does not run in Replication Dual Mode. There are several
 * logistical issues.
 *
 * -It assumes that all log files are in the <envHome> directory, whereas
 * dual mode environments are in <envHome>/rep*
 */
public class CustomDbPrintLogTest extends TestBase {

    public static int COUNTER = 0;
    public static DatabaseId CHECK_ID;

    private Environment env;
    private final File envHome;

    public CustomDbPrintLogTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        deletePrintInfo();
    }

    @After
    public void tearDown() {

        /*
         * Close down environments in case the unit test failed so that the log
         * files can be removed.
         */
        try {
            if (env != null) {
                env.close();
                env = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        } 

        deletePrintInfo();
    }

    /* Delete the dumpLog file created by TestDumper. */
    private void deletePrintInfo() {
        for (File file : envHome.listFiles()) {
            if (file.isFile() && 
                file.getName().contains(TestDumper.SAVE_INFO_FILE)) {
                boolean deleted = file.delete();
                assertTrue(deleted);
            }
        }
    }

    private void createEnv() {
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, "foo", dbConfig);
        CHECK_ID = DbInternal.getDbImpl(db).getId();
        DatabaseEntry key = new DatabaseEntry(new byte[1000]);
        DatabaseEntry data = new DatabaseEntry(new byte[1000]);
        for (int i = 0; i < 10; i += 1) {
            db.put(null, key, data);
        }
        db.close();

        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);
        env.checkpoint(ckptConfig);
    }

    /*
     * Use the custom log printer to list types of log entries.  
     *
     * Note that we run the custom log printer in a separate process, whereas
     * it would have seemed more intuitive to merely call DbPrintLog
     * programmatically. It's done this way instead because the classpath
     * doesn't work out right within junit and it doesn't recognize the
     * custom dumper class.
     */
    @Test
    public void testCustom() 
        throws Throwable {

        createEnv();
        LNFileReader reader = 
            new LNFileReader(DbInternal.getNonNullEnvImpl(env),
                             10000, 0, true, DbLsn.NULL_LSN, DbLsn.NULL_LSN,
                             null, DbLsn.NULL_LSN);
        /* Specify the entry types looking for. */
        reader.addTargetType(LogEntryType.LOG_DEL_LN);
        reader.addTargetType(LogEntryType.LOG_INS_LN);
        reader.addTargetType(LogEntryType.LOG_UPD_LN);

        /* Check the LN count. */
        int count = 0;
        ArrayList<String> lnMessages = new ArrayList<String>();
        while(reader.readNextEntry()) {
            count++;
            lnMessages.add(reader.getLNLogEntry().getLogType() + " lsn=" + 
                           DbLsn.getNoFormatString(reader.getLastLsn()));
        }
        assertTrue("count: " + count, count == 10);

        TestDumper foo = new TestDumper(DbInternal.getNonNullEnvImpl(env),
                                        1000,
                                        0L, 0L, 0L, null, null, false, false, 
                                        true);
        
        /* Invoke process to call the DbPrintLog. */
        String[] commands = new String[5];
        commands[0] = "com.sleepycat.je.util.DbPrintLog";
        commands[1] = "-h";
        commands[2] = envHome.getAbsolutePath();
        commands[3] = "-c";
        commands[4] = foo.getClass().getName();
        
        JUnitProcessThread thread = 
            new JUnitProcessThread("TestDumper", 0, null, commands, true);
        thread.start();

        try {
            thread.finishTest();
        } catch (Throwable t) {
            fail("Unexpected exception: " + t);
        }

        /* Read from the info file and checks the size is the same. */
        ArrayList<String> messages = readMessages();
        assertTrue(messages.size() > count);

        /* 
         * Check messages read by the LNFileReader must exist in the list read
         * by TestDumper.
         */
        for (String message : lnMessages) {
            assertTrue("message: " + message + " is not in information " +
                       "read by TestDumper", messages.contains(message));
        }

        /* Close Environment. */
        env.close();
        env = null;
    }

    /* Read messages written by TestDumper. */
    private ArrayList<String> readMessages() 
        throws Exception {

        File file = new File(envHome, TestDumper.SAVE_INFO_FILE);
        assertTrue(file.exists());

        ArrayList<String> messages = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = reader.readLine()) != null) {
            messages.add(line);
        }
        reader.close();

        return messages;
    }
}
