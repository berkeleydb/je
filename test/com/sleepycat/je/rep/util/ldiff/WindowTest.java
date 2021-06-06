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

package com.sleepycat.je.rep.util.ldiff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.util.test.SharedTestUtils;

public class WindowTest {
    private static File envDir = SharedTestUtils.getTestDir();
    private static String dbName = "window.db";

    @Before
    public void setUp() 
        throws Exception {
        
        if (envDir.exists()) {
            for (File f : envDir.listFiles())
                f.delete();
            envDir.delete();
        }
        envDir.mkdir();
    }

    /**
     * Test that rolling the checksum yields the same value as calculating the
     * checksum directly.
     */
    @Test
    public void testRollingChecksum() {
        Cursor c1, c2;
        Database db;
        DatabaseEntry data, key;
        Environment env;
        Window w1, w2;
        byte[] dataarr =
            { (byte) 0xdb, (byte) 0xdb, (byte) 0xdb, (byte) 0xdb };
        byte[] keyarr = { 0, 0, 0, 0 };
        final int blockSize = 5;
        final int dbSize = 2 * blockSize;

        /* Open the database environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        /* envConfig.setTransactional(false); */
        envConfig.setAllowCreate(true);
        try {
            env = new Environment(envDir, envConfig);
        } catch (Exception e) {
            assertTrue(false);
            return;
        }

        /* Open a database within the environment. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setSortedDuplicates(true);
        try {
            db = env.openDatabase(null, dbName, dbConfig);
        } catch (Exception e) {
            assertTrue(false);
            return;
        }

        for (int i = 0; i < dbSize; i++) {
            key = new DatabaseEntry(keyarr);
            data = new DatabaseEntry(dataarr);
            db.put(null, key, data);
            keyarr[3]++;
        }

        c1 = db.openCursor(null, null);
        c2 = db.openCursor(null, null);
        try {
            w1 = new Window(c1, blockSize);
            w2 = new Window(c2, blockSize);
        } catch (Exception e) {
            c1.close();
            c2.close();
            db.close();
            env.close();
            assertTrue(false);
            return;
        }
        assertEquals(w1.getChecksum(), w2.getChecksum());
        key = new DatabaseEntry();
        key.setPartial(0, 0, true);
        data = new DatabaseEntry();
        data.setPartial(0, 0, true);
        for (int i = blockSize; i < dbSize; i++) {
            try {
                /* Advance w1 by one key/data pair. */
                w1.rollWindow();

                /* 
                 * Position c2 to the next key/data pair and get a new window
                 * (Constructing the window modifiers the cursor, so we need to
                 * reposition it.
                 */
                assertTrue(c2.getFirst(key, data, LockMode.DEFAULT) ==
                           OperationStatus.SUCCESS);
                for (int j = 0; j < i - blockSize; j++)
                    assertTrue(c2.getNext(key, data, LockMode.DEFAULT) ==
                               OperationStatus.SUCCESS);
                w2 = new Window(c2, blockSize);

                /* 
                 * The windows are referring to the same range of key/data
                 * pairs.
                 */
                assertEquals(w1.getChecksum(), w2.getChecksum());
            } catch (Exception e) {
                c1.close();
                c2.close();
                db.close();
                env.close();
                assertTrue(false);
                return;
            }
        }
        c1.close();
        c2.close();
        db.close();
        env.close();
    }
}
