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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class RemoteProcessingTest extends TestBase {
    private File envHome;
    private final int dbCount = 25000;
    private final String DB_NAME = "testDb";

    /* The remote database environment. */
    private static Environment env;
    /* The list of blocks constituting RDB. */
    static final List<Block> rbList = new ArrayList<Block>();

    public RemoteProcessingTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        fillData();
    }

    private void fillData() throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);

        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);

        Database db = env.openDatabase(null, DB_NAME, dbConfig);
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 1; i <= dbCount; i++) {
            IntegerBinding.intToEntry(i, key);
            StringBinding.stringToEntry("bdb je", data);
            db.put(null, key, data);
        }
        db.close();
        env.close();
    }

    public static void configure(String envDir) {
        env = LDiffUtil.openEnv(envDir);
    }

    @Test
    public void testPlaceHolder() {
        /* 
         * A Junit test will fail if there are no tests cases at all, so
         * here is a placeholder test.
         */
    }
}
