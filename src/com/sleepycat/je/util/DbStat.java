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
import java.io.PrintStream;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.BasicLocker;

public class DbStat extends DbVerify {
    /*
    private String usageString =
        "usage: " + CmdUtil.getJavaCommand(DbStat.class) + "\n" +
        "               [-V] -s database -h dbEnvHome [-v progressInterval]\n";
    */

    private int progressInterval = 0;

    public static void main(String argv[])
        throws DatabaseException {

        DbStat stat = new DbStat();
        stat.parseArgs(argv);

        int ret = 1;
        try {
            stat.openEnv();
            if (stat.stats(System.err)) {
                ret = 0;
            }
            stat.closeEnv();
        } catch (Throwable T) {
            ret = 1;
            T.printStackTrace(System.err);
        }

        System.exit(ret);
    }

    DbStat() {
    }

    public DbStat(Environment env, String dbName) {
        super(env, dbName, false);
    }

    @Override
    void parseArgs(String argv[]) {

        int argc = 0;
        int nArgs = argv.length;
        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-V")) {
                System.out.println(JEVersion.CURRENT_VERSION);
                System.exit(0);
            } else if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(argv[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-s")) {
                if (argc < nArgs) {
                    dbName = argv[argc++];
                } else {
                    printUsage("-s requires an argument");
                }
            } else if (thisArg.equals("-v")) {
                if (argc < nArgs) {
                    progressInterval = Integer.parseInt(argv[argc++]);
                    if (progressInterval <= 0) {
                        printUsage("-v requires a positive argument");
                    }
                } else {
                    printUsage("-v requires an argument");
                }
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }

        if (dbName == null) {
            printUsage("-s is a required argument");
        }
    }

    public boolean stats(PrintStream out)
        throws DatabaseNotFoundException {

        final StatsConfig statsConfig = new StatsConfig();
        statsConfig.setShowProgressStream(out);
        if (progressInterval > 0) {
            statsConfig.setShowProgressInterval(progressInterval);
        }

        try {
            final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
            final DbTree dbTree = envImpl.getDbTree();
            BasicLocker locker =
                BasicLocker.createBasicLocker(envImpl, false /*noWait*/);
            DatabaseImpl dbImpl;

            try {
                dbImpl = dbTree.getDb(locker, dbName, null, false);
            } finally {
                locker.operationEnd();
            }

            if (dbImpl == null || dbImpl.isDeleted()) {
                return false;
            }

            try {
                final DatabaseStats stats = dbImpl.stat(statsConfig);
                out.println(stats);
            } finally {
                dbTree.releaseDb(dbImpl);
            }

        } catch (DatabaseException DE) {
            return false;
        }

        return true;
    }
}
