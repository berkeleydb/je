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

import java.io.File;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

/**
 * Run RepTestUtils.checkUtilizationProfile on a rep group.
 */
public class UtilizationChecker {

    private RepEnvInfo[] repEnvInfo;

    /* Environment home root for whole replication group. */
    private File envRoot;
    private int nNodes = 5;
    private int subDir = 0;

    public static void main(String args[]) {
        try {
            UtilizationChecker utilizationChecker = new UtilizationChecker();
            utilizationChecker.parseArgs(args);
            utilizationChecker.setup();
            utilizationChecker.check();
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * Grow the data store to the appropriate size for the steady state
     * portion of the test.
     */
    private void setup()
        throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
        }

        /*
         * We have a lot of environments open in a single process, so reduce
         * the cache size lest we run out of file descriptors.
         */
        envConfig.setConfigParam("je.log.fileCacheSize", "30");

        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, nNodes, envConfig);
    }

    private void check() {

        System.out.println("Check starting");

        RepTestUtils.restartGroup(repEnvInfo);
	RepTestUtils.checkUtilizationProfile(System.out, repEnvInfo);
        System.out.println("Check finishing");
        for (RepEnvInfo info : repEnvInfo) {
            info.closeEnv();
        }
    }

    private void parseArgs(String args[])
        throws Exception {

        for (int i = 0; i < args.length; i++) {
            boolean moreArgs = i < args.length - 1;
            if (args[i].equals("-h") && moreArgs) {
                envRoot = new File(args[++i]);
            } else if (args[i].equals("-repGroupSize") && moreArgs) {
                nNodes = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-subDir") && moreArgs) {
                subDir = Integer.parseInt(args[++i]);
            } else {
                usage("Unknown arg: " + args[i]);
            }
        }
    }

    private void usage(String error) {
        if (error != null) {
            System.err.println(error);
        }

        System.err.println
            ("java " + getClass().getName() + "\n" +
             "     [-h <replication group Environment home dir>]\n" +
             "     [-repGroupSize <replication group size>]\n" +
             "     [-subDir <num directories to use with je.log.nDataDirectories");
        System.exit(2);
    }
}
