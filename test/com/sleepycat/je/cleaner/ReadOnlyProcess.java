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

package com.sleepycat.je.cleaner;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * @see ReadOnlyLockingTest
 */
public class ReadOnlyProcess {

    public static void main(String[] args) {

        /*
         * Don't write to System.out in this process because the parent
         * process only reads System.err.
         */
        try {
            EnvironmentConfig envConfig = TestUtils.initEnvConfig();
            envConfig.setTransactional(true);
            envConfig.setReadOnly(true);
            if (args[0].equals("true")) {
                envConfig.setConfigParam
                    (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, args[1]);
            }

            File envHome = SharedTestUtils.getTestDir();
            Environment env = new Environment(envHome, envConfig);

            //System.err.println("Opened read-only: " + envHome);
            //System.err.println(System.getProperty("java.class.path"));

            /* Notify the test that this process has opened the environment. */
            ReadOnlyLockingTest.createProcessFile();

            /* Sleep until the parent process kills me. */
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {

            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
