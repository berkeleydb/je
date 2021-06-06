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

import java.io.File;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.utilint.StringUtils;

public class MiniPerf {

    private File envHome;
    private Environment exampleEnv;
    private Database exampleDb;
    private Cursor cursor;

    static int nKeys;

    static public void main(String argv[])
        throws DatabaseException, NumberFormatException {

        boolean create = false;
        if (argv.length > 0) {
            nKeys = Integer.parseInt(argv[0]);
            create = true;
        } else {
            create = false;
        }
        new MiniPerf().doit(create);
    }

    void doit(boolean create)
        throws DatabaseException {

        envHome = SharedTestUtils.getTestDir();
        setUp(create);
        testIterationPerformance(create);
        tearDown();
    }

    public void setUp(boolean create)
        throws DatabaseException {

        if (create) {
            TestUtils.removeLogFiles("Setup", envHome, false);
        }

        // Set up an environment
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(create);
        exampleEnv = new Environment(envHome, envConfig);

        // Set up a database
        String databaseName = "simpleDb";
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        exampleDb = exampleEnv.openDatabase(null, databaseName, dbConfig);

        // Set up cursors
        cursor = exampleDb.openCursor(null, null);
    }

    public void tearDown()
        throws DatabaseException {

        exampleEnv.sync();

        if (exampleDb != null) {
            exampleDb.close();
            exampleDb = null;
        }
        if (exampleEnv != null) {
            try {
                exampleEnv.close();
            } catch (DatabaseException DE) {
                /*
                 * Ignore this exception.  It's caused by us calling
                 * tearDown() within the test.  Each tearDown() call
                 * forces the database closed.  So when the call from
                 * junit comes along, it's already closed.
                 */
            }
            exampleEnv = null;
        }

        cursor = null;
    }

    public void testIterationPerformance(boolean create)
        throws DatabaseException {

        final int N_KEY_BYTES = 10;
        final int N_DATA_BYTES = 20;

        if (create) {
            System.out.print("Creating...");
            for (int i = 0; i < nKeys; i++) {
                if (i % 100000 == 0) {
                    System.out.println(i);
                }
                byte[] key = new byte[N_KEY_BYTES];
                TestUtils.generateRandomAlphaBytes(key);
                String keyString = StringUtils.fromUTF8(key);

                byte[] data = new byte[N_DATA_BYTES];
                TestUtils.generateRandomAlphaBytes(data);
                String dataString = StringUtils.fromUTF8(data);
                cursor.put(new StringDbt(keyString),
                           new StringDbt(dataString));
            }
            System.out.print("done.");
        } else {
            String middleKey = null;
            int middleEntry = -1;
            int count = 0;
            for (int i = 0; i < 3; i++) {
                System.out.print("Iterating...");
                StringDbt foundKey = new StringDbt();
                StringDbt foundData = new StringDbt();

                long startTime = System.currentTimeMillis();
                OperationStatus status = cursor.getFirst(foundKey, foundData, LockMode.DEFAULT);

                count = 0;
                while (status == OperationStatus.SUCCESS) {
                    status =
                        cursor.getNext(foundKey, foundData, LockMode.DEFAULT);
                    count++;
                    if (count == middleEntry) {
                        middleKey = foundKey.getString();
                    }
                }
                long endTime = System.currentTimeMillis();
                System.out.println("done.");
                System.out.println(count + " records found.");
                middleEntry = count >> 1;
                System.out.println((endTime - startTime) + " millis");
            }

            System.out.println("Middle key: " + middleKey);

            StringDbt searchKey = new StringDbt(middleKey);
            StringDbt searchData = new StringDbt();
            for (int j = 0; j < 3; j++) {
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < count; i++) {
                    if (cursor.getSearchKey(searchKey,
                                            searchData,
                                            LockMode.DEFAULT) != OperationStatus.SUCCESS) {
                        System.out.println("non-0 return");
                    }
                }
                long endTime = System.currentTimeMillis();
                System.out.println((endTime - startTime) + " millis");
            }
        }
    }
}
