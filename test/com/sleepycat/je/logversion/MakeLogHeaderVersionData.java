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

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.TestUtils;

/**
 * This standalone command line program creates a single 00000000.jdb log file.
 * It was used to generate maxversion.jdb and minversion.jdb, and although it
 * may never need to be used again, below are instructions.
 *
 * <p>Before running this program change LogEntryType.LOG_VERSION to
 * Integer.MAX_VALUE or one temporarily, just for creating a file with the
 * maximum or minimum version number.  A single command line argument is
 * required for the home directory.  After running this program rename the
 * 00000000.jdb file to maxversion.jdb or minversion.jdb file in the directory
 * of this source package.  When adding it to CVS make sure to use -kb since it
 * is a binary file.  Don't forget to change LogEntryType.LOG_VERSION back to
 * the correct value.</p>
 *
 * @see LogHeaderVersionTest
 */
public class MakeLogHeaderVersionData {

    private MakeLogHeaderVersionData() {
    }

    public static void main(String[] args)
        throws Exception {

        if (args.length != 1) {
            throw new Exception("Home directory arg is required.");
        }

        File homeDir = new File(args[0]);
        File logFile = new File(homeDir, TestUtils.LOG_FILE_NAME);

        if (logFile.exists()) {
            throw new Exception("Home directory must be empty of log files.");
        }

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        /* Make as small a log as possible to save space in CVS. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");

        Environment env = new Environment(homeDir, envConfig);
        env.close();

        if (!logFile.exists()) {
            throw new Exception("Home directory does not contain: " + logFile);
        }

        System.out.println("Sucessfully created: " + logFile);
    }
}
