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

package com.sleepycat.je.tree;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class ValidateSubtreeDeleteTest extends TestBase {

    private final File envHome;
    private Environment env;
    private Database testDb;

    public ValidateSubtreeDeleteTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(),
                                 "false");
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        testDb = env.openDatabase(null, "Test", dbConfig);
    }

    @After
    public void tearDown() 
        throws Exception {
        
        testDb.close();
        if (env != null) {
            try {
                env.close();
            } catch (DatabaseException E) {
            }
        }
    }

    @Test
    public void testBasic()
        throws Exception  {
        try {
            /* Make a 3 level tree full of data */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            byte[] testData = new byte[1];
            testData[0] = 1;
            data.setData(testData);

            Transaction txn = env.beginTransaction(null, null);
            for (int i = 0; i < 15; i ++) {
                key.setData(TestUtils.getTestArray(i));
                testDb.put(txn, key, data);
            }

            /* Should not be able to delete any of it */
            assertFalse(DbInternal.getDbImpl(testDb).getTree().validateDelete(0));
            assertFalse(DbInternal.getDbImpl(testDb).getTree().validateDelete(1));

            /*
             * Should be able to delete both, the txn is aborted and the data
             * isn't there.
             */
            txn.abort();
            assertTrue(DbInternal.getDbImpl(testDb).getTree().validateDelete(0));
            assertTrue(DbInternal.getDbImpl(testDb).getTree().validateDelete(1));

            /*
             * Try explicit deletes.
             */
            txn = env.beginTransaction(null, null);
            for (int i = 0; i < 15; i ++) {
                key.setData(TestUtils.getTestArray(i));
                testDb.put(txn, key, data);
            }
            for (int i = 0; i < 15; i ++) {
                key.setData(TestUtils.getTestArray(i));
                testDb.delete(txn, key);
            }
            assertFalse(DbInternal.getDbImpl(testDb).getTree().validateDelete(0));
            assertFalse(DbInternal.getDbImpl(testDb).getTree().validateDelete(1));

            // XXX, now commit the delete and compress and test that the
            // subtree is deletable. Not finished yet! Also must test deletes.
            txn.abort();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testDuplicates()
        throws Exception  {
        try {
            /* Make a 3 level tree full of data */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            byte[] testData = new byte[1];
            testData[0] = 1;
            key.setData(testData);

            Transaction txn = env.beginTransaction(null, null);
            for (int i = 0; i < 4; i ++) {
                data.setData(TestUtils.getTestArray(i));
                testDb.put(txn, key, data);
            }

            /* Should not be able to delete any of it */
            Tree tree = DbInternal.getDbImpl(testDb).getTree();
            assertFalse(tree.validateDelete(0));

            /*
             * Should be able to delete, the txn is aborted and the data
             * isn't there.
             */
            txn.abort();
            assertTrue(tree.validateDelete(0));

            /*
             * Try explicit deletes.
             */
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

}
