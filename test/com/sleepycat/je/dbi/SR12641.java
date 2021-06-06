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

package com.sleepycat.je.dbi;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * This reproduces the bug described SR [#12641], also related to SR [#9543].
 *
 * Note that allthough this is a JUnit test case, it is not run as part of the
 * JUnit test suite.  It takes a long time, and when it fails it hangs.
 * Therefore, it was only used for debugging and is not intended to be a
 * regression test.
 *
 * For some reason the bug was not reproducible with a simple main program,
 * which is why a JUnit test was used.
 */
public class SR12641 extends TestBase {

    /* Use small NODE_MAX to cause lots of splits. */
    private static final int NODE_MAX = 6;

    private final File envHome;
    private Environment env;
    private Database db;
    private boolean dups;
    private boolean writerStopped;

    public SR12641() {
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        
        if (env != null) {
            try {
                env.close();
            } catch (Exception e) {
                System.err.println("TearDown: " + e);
            }
        }
        env = null;
        db = null;
    }

    @Test
    public void testSplitsWithScansDups()
        throws Throwable {

        dups = true;
        testSplitsWithScans();
    }

    @Test
    public void testSplitsWithScans()
        throws Throwable {

        open();

        /* Cause splits in the last BIN. */
        JUnitThread writer = new JUnitThread("writer") {
            @Override
            public void testBody() {
                try {
                    DatabaseEntry key = new DatabaseEntry(new byte[1]);
                    DatabaseEntry data = new DatabaseEntry(new byte[1]);
                    OperationStatus status;

                    Cursor cursor = db.openCursor(null, null);

                    for (int i = 0; i < 100000; i += 1) {
                        IntegerBinding.intToEntry(i, dups ? data : key);
                        if (dups) {
                            status = cursor.putNoDupData(key, data);
                        } else {
                            status = cursor.putNoOverwrite(key, data);
                        }
                        assertEquals(OperationStatus.SUCCESS, status);

                        if (i % 5000 == 0) {
                            System.out.println("Iteration: " + i);
                        }
                    }

                    cursor.close();
                    writerStopped = true;

                } catch (Exception e) {
                    try {
                        FileOutputStream os =
                            new FileOutputStream(new File("./err.txt"));
                        e.printStackTrace(new PrintStream(os));
                        os.close();
                    } catch (IOException ignored) {}
                    System.exit(1);
                }
            }
        };

        /* Move repeatedly from the last BIN to the prior BIN. */
        JUnitThread reader = new JUnitThread("reader") {
            @Override
            public void testBody() {
                try {
                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry data = new DatabaseEntry();

                    CursorConfig cursorConfig = new CursorConfig();
                    cursorConfig.setReadUncommitted(true);
                    Cursor cursor = db.openCursor(null, cursorConfig);

                    while (!writerStopped) {
                        cursor.getLast(key, data, null);
                        for (int i = 0; i <= NODE_MAX; i += 1) {
                            cursor.getPrev(key, data, null);
                        }
                    }

                    cursor.close();

                } catch (Exception e) {
                    try {
                        FileOutputStream os =
                            new FileOutputStream(new File("./err.txt"));
                        e.printStackTrace(new PrintStream(os));
                        os.close();
                    } catch (IOException ignored) {}
                    System.exit(1);
                }
            }
        };

        writer.start();
        reader.start();
        writer.finishTest();
        reader.finishTest();

        close();
        System.out.println("SUCCESS");
    }

    private void open()
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam
            (EnvironmentParams.NODE_MAX.getName(), String.valueOf(NODE_MAX));
        envConfig.setAllowCreate(true);
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setSortedDuplicates(dups);
        db = env.openDatabase(null, "testDb", dbConfig);
    }

    private void close()
        throws Exception {

        db.close();
        db = null;
        env.close();
        env = null;
    }
}
