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

package com.sleepycat.je.test;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test out-of-memory fix to DaemonThread [#10504].
 */
public class MultiEnvOpenCloseTest extends TestBase {

    private File envHome;

    @Before
    public void setUp() 
        throws Exception {

        envHome = SharedTestUtils.getTestDir();
        super.setUp();
    }

    @Test
    public void testMultiOpenClose()
        throws Exception {

        /*
         * Before fixing the bug in DaemonThread [#10504] this test would run
         * out of memory after 7 iterations.  The bug was, if we open an
         * environment read-only we won't start certain daemon threads, they
         * will not be GC'ed because they are part of a thread group, and they
         * will retain a reference to the Environment.  The fix was to not
         * create the threads until we need to start them.
         */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        final int DATA_SIZE = 1024 * 10;
        final int N_RECORDS = 1000;
        final int N_ITERS = 30;

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[DATA_SIZE]);

        Environment env = new Environment(envHome, envConfig);
        Database db = env.openDatabase(null, "MultiEnvOpenCloseTest",
                                       dbConfig);
        for (int i = 0; i < N_RECORDS; i += 1) {
            IntegerBinding.intToEntry(i, key);
            db.put(null, key, data);
        }

        db.close();
        env.close();

        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        dbConfig.setAllowCreate(false);
        dbConfig.setReadOnly(true);

        for (int i = 1; i <= N_ITERS; i += 1) {
            //System.out.println("MultiEnvOpenCloseTest iteration # " + i);
            env = new Environment(envHome, envConfig);
            db = env.openDatabase(null, "MultiEnvOpenCloseTest", dbConfig);
            for (int j = 0; j < N_RECORDS; j += 1) {
                IntegerBinding.intToEntry(j, key);
                db.get(null, key, data, null);
            }
            db.close();
            env.close();
        }
    }
}
