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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

/**
 * Utility methods to support replication standalone tests.
 */
public class Utils {
    public static final boolean VERBOSE = Boolean.getBoolean("verbose");
    public static final String DB_NAME = "testDb";

    /**
     * Create an EnvironmentConfig for ReplicatedEnvironments. 
     */
    public static EnvironmentConfig createEnvConfig(long fileSize,
                                                    long maxDisk,
                                                    long checkpointBytes,
                                                    int subDir,
                                                    long mainCacheSize,
                                                    long offHeapCacheSize) {
        EnvironmentConfig envConfig =
            RepTestUtils.createEnvConfig(RepTestUtils.DEFAULT_DURABILITY);

        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "true");

        /*
         * Set smaller log file max size and checkpointer interval bytes, so
         * that the log cleaning takes place in this test.
         */
        DbInternal.disableParameterValidation(envConfig);

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(fileSize));

        envConfig.setConfigParam(
            EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
            String.valueOf(checkpointBytes));

        if (maxDisk != 0) {
            envConfig.setConfigParam(
                EnvironmentConfig.MAX_DISK, String.valueOf(maxDisk));

            /* Set a small FREE_DISK since it is subtracted from MAX_DISK. */
            envConfig.setConfigParam(
                EnvironmentConfig.FREE_DISK, String.valueOf(1 << 20));
        }

        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
        }

        envConfig.setCacheSize(mainCacheSize);
        envConfig.setOffHeapCacheSize(offHeapCacheSize);

        return envConfig;
    }

    /* Start up the group and return the generated RepEnvInfo array. */
    static RepEnvInfo[] setupGroup(File envRoot,
                                   int nNodes,
                                   long fileSize,
                                   long maxDisk,
                                   long checkpointBytes,
                                   int subDir,
                                   long mainCacheSize,
                                   long offHeapCacheSize)
        throws Exception {

        RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(
            envRoot, nNodes,
            createEnvConfig(
                fileSize, maxDisk, checkpointBytes, subDir, mainCacheSize,
                offHeapCacheSize));

        if (subDir > 0) {
            RepTestUtils.createRepSubDirs(repEnvInfo, subDir);
        }

        if (nNodes == 2) {
            repEnvInfo[0].getRepConfig().setDesignatedPrimary(true);
        }

        return repEnvInfo;
    }

    /*
     * Join the ReplicatedEnvironments of a group of RepEnvInfo and return the
     * authoritative master (wait for election to quiesce).
     */
    public static ReplicatedEnvironment getMaster(RepEnvInfo[] repEnvInfo) {
        return RepTestUtils.getMaster(repEnvInfo, true /*openIfNeeded*/);
    }

    public static ReplicatedEnvironment assignMaster(RepEnvInfo[] repEnvInfo,
                                                     int masterId,
                                                     boolean restart)
        throws Exception {

        assert repEnvInfo.length == 2 :
            "This method can only be called by a replication group size of 2.";

        ReplicationMutableConfig newConfig = new ReplicationMutableConfig();
        newConfig.setDesignatedPrimary(true);

        if (restart) {
            repEnvInfo[masterId - 1].getRepConfig().setDesignatedPrimary(false);
            repEnvInfo[masterId - 1].openEnv();
        } else {
            repEnvInfo[2 - masterId].getEnv().setRepMutableConfig(newConfig);
        }

        assert repEnvInfo[2 - masterId].getEnv().getState().isMaster() :
            "Can't find out a master.";

        return repEnvInfo[2 - masterId].getEnv();
    }

    /* Create or open a database for test. */
    public static EntityStore openStore(ReplicatedEnvironment repEnv,
                                        String dbName)
        throws DatabaseException {

        StoreConfig config = new StoreConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);

        return new EntityStore(repEnv, dbName, config);
    }

    /**
     * Sync replicas to the master, and check that all nodes have the same
     * contents.
     */
    public static void doSyncAndCheck(RepEnvInfo[] replicators)
        throws Exception {

        /* Do the sync and check the node equality. */
        VLSN commitVLSN =
            RepTestUtils.syncGroupToLastCommit(replicators,
                                               replicators.length);
        RepTestUtils.checkNodeEquality(commitVLSN, VERBOSE, replicators);
    }

    /* Check the log cleaning and close the replicas. */
    public static void closeEnvAndCheckLogCleaning(RepEnvInfo[] repEnvInfo,
                                                   long[] fileDeletions,
                                                   boolean checkCleaning)
        throws Exception {

        /* Initiate an array for saving the largest log file number. */
        long [] lastFileNumbers = new long[repEnvInfo.length];

        if (checkCleaning) {
            /* Get the cleaner deletion stat for all replicas. */
            int index = 0;

            /* A stats config for getting stats. */
            StatsConfig stConfig = new StatsConfig();
            stConfig.setFast(true);
            stConfig.setClear(true);

            for (RepEnvInfo repInfo : repEnvInfo) {
                if (repInfo.getEnv() != null &&
                    repInfo.getEnv().isValid()) {
                    ReplicatedEnvironment repEnv = repInfo.getEnv();
                    fileDeletions[index] +=
                        repEnv.getStats(stConfig).getNCleanerDeletions();
                    /* Get largest log file number for each environment. */
                    lastFileNumbers[index] =
                        RepInternal.getNonNullRepImpl(repEnv).
                        getFileManager().getLastFileNum();
                }
                index++;
            }
        }

        /* Shut down the replicas. */
        RepTestUtils.shutdownRepEnvs(repEnvInfo);

        if (checkCleaning) {
            /* Check if there is replica doesn't do log cleaning. */
            for (int i = 0; i < fileDeletions.length; i++) {
                System.err.println("Deleted files on replica " + i + " = " +
                                   fileDeletions[i]);
                System.err.println("Total used log files on replica " + i +
                                   " = " + (lastFileNumbers[i] + 1));
                if ((fileDeletions[i] * 100) / (lastFileNumbers[i] + 1) < 40) {
                    throw new IllegalStateException
                        ("Expect to see log cleaning on replica " + i +
                         " exceeds 40%, but it doesn't.");
                }
            }
        }
    }

    public static void createSubDirs(File envHome, int subDirNumber) {
        createSubDirs(envHome, subDirNumber, false);
    }

    public static void createSubDirs(File envHome, 
                                     int subDirNumber, 
                                     boolean useExistEnvHome) {
        if (!envHome.exists()) {
            throw new IllegalStateException
                ("The environment home is not created yet.");
        }

        for (int i = 1; i <= subDirNumber; i++) {
            String fileName = null;
            if (i < 10) {
                fileName = "data00" + i;
            } else if (i < 100) {
                fileName = "data0" + i;
            } else if (i <= 256) {
                fileName = "data" + i;
            } else {
                throw new IllegalArgumentException
                    ("The number of sub directories is invalid.");
            }

            File subDir = new File(envHome, fileName);

            if (subDir.exists() && useExistEnvHome) {
                continue;
            }
     
            if (subDir.exists()) {
                throw new IllegalStateException
                    ("The sub directories shouldn't be created yet.");
            }

            if (!subDir.mkdir()) {
                throw new IllegalStateException
                    ("The sub directories are not created successfully.");
            }
        }
    }
}
