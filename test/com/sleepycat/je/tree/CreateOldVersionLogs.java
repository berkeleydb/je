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

import java.io.File;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
//import com.sleepycat.je.rep.util.DbRepPreUpgrade_4_1;
//import com.sleepycat.je.util.DbPreUpgrade_4_1;

/**
 * This program is used to generate the old data used for test dup convert on
 * JE 5.0. Before running this program, you have to uncomment those commented
 * codes, since there are some classes only exist in JE 4.1. Besure to run this
 * program on JE 4.1.
 *
 * Please ask Eric and Tao if you have more questions.
 */
public class CreateOldVersionLogs {
    private static File envHome = new File("data");
    private File[] envHomes;
    File logFile = new File(envHome, "00000000.jdb");
    Environment env;
    Database db;

    private static DatabaseEntry theKey = new DatabaseEntry();
    private static DatabaseEntry theData = new DatabaseEntry();
    private static int N_ENTRIES = 4;
    private static int repNodes = 3;
    ReplicatedEnvironment[] repEnvs = new ReplicatedEnvironment[repNodes];

    private String singletonLN_jdb = "je-4.1.7_logWithSingletonLN.jdb";
    private String DIN_jdb = "je-4.1.7_logWithDIN.jdb";
    private String DeletedLNCommit_jdb = "je-4.1.7_logWithDeletedLNCommit.jdb";
    private String DeletedLNNoCommit_jdb =
        "je-4.1.7_logWithDeletedLNNoCommit.jdb";
    private String MixIN_jdb = "je-4.1.7_logWithMixIN.jdb";

    private String singletonLNRep_jdb = "je-4.1.7_singletonLN";
    private String DINRep_jdb = "je-4.1.7_din";
    private String DeletedLNCommitRep_jdb =
        "je-4.1.7_deletedLNCommit";
    private String DeletedLNNoCommitRep_jdb =
        "je-4.1.7_deletedLNNoCommit";
    private String MixINRep_jdb = "je-4.1.7_mixIN";

    /*
     * Logs where the preupgrade utility has not been run, and an exception
     * should be thrown by recovery.
     */
    private String NoPreUpgrade_Dups_jdb = "je-4.1.7_noPreUpgrade_dups";
    private String NoPreUpgrade_Deltas_jdb = "je-4.1.7_noPreUpgrade_deltas";

    public static void main(String args[]) throws Exception {
        CreateOldVersionLogs covl = new CreateOldVersionLogs();

        /* Uncomment one or more methods before running. */

        /* Create standalone jdb. */
        //covl.createLogWithSingletonLN();
        //covl.createLogWithDIN();
        //covl.createLogWithDeletedLN(true);
        //covl.createLogWithDeletedLN(false);
        //covl.createLogWithMixIN();

        /* Create rep jdb. */
        //covl.createRepLogWithSingletonLN();
        //covl.createRepLogWithDIN();
        //covl.createRepLogWithDeletedLN(true);
        //covl.createRepLogWithDeletedLN(false);
        //covl.createRepLogWithMixIN();

        /* Create no-preupgrade jdb. */
        covl.logFile = new File(envHome, "00000000.jdb");
        covl.createNoPreUpgradeDups();
        //covl.createNoPreUpgradeDeltas();

        System.out.println("Finish all creation!");
    }

    void openStandaloneEnv() {
        env = new Environment(envHome, makeEnvConfig());
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    EnvironmentConfig makeEnvConfig() {
        return makeEnvConfig(true /*smallNodeMax*/);
    }

    EnvironmentConfig makeEnvConfig(boolean smallNodeMax) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        /* Disable all daemon threads. */
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_INCOMPRESSOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CHECKPOINTER.getName(), "false");

        /* Use a 100 MB log file size to ensure only one file is written. */
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 Integer.toString(100 * (1 << 20)));
        if (smallNodeMax) {
            envConfig.setConfigParam
                (EnvironmentParams.NODE_MAX.getName(),
                 Integer.toString(N_ENTRIES));
        }
        return envConfig;
    }

    void openRepEnv() throws InterruptedException {
        envHomes = new File[repNodes];
        for (int i = 0; i < repNodes; i++) {
            activateOneNode(i);
        }
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    void activateOneNode(int i) {
        envHomes[i] = new File(envHome, "rep" + i);
        envHomes[i].mkdir();
        ReplicationConfig repConfig;
        repConfig = new ReplicationConfig();
        repConfig.setGroupName("UnitTestGroup");
        repConfig.setNodeName("Node " + (i + 1));
        repConfig.setNodeHostPort("localhost:" + (5001 + i));
        repConfig.setHelperHosts("localhost:5001");
        repEnvs[i] = new ReplicatedEnvironment(envHomes[i], repConfig,
                                               makeEnvConfig());
        ReplicatedEnvironment.State joinState = repEnvs[i].getState();
        if (joinState.equals(State.MASTER)) {
            env = repEnvs[i];
        }
    }

    void close(boolean rep) {
        if (db != null) {
            db.close();
            db = null;
        }
        if (rep) {
            for (int i = repNodes - 1; i >= 0; i--) {
                if (repEnvs[i] != null) {
                    repEnvs[i].close();
                    repEnvs[i] = null;
                }
            }
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    void abnormalClose(boolean rep) {
        if (rep) {
            for (int i = repNodes - 1; i >= 0; i--) {
                if (repEnvs[i] != null) {
                    RepInternal.
                        getNonNullRepImpl((ReplicatedEnvironment) repEnvs[i]).
                        abnormalClose();
                    repEnvs[i] = null;
                }
            }
        } else {
            DbInternal.getNonNullEnvImpl(env).abnormalClose();
            env = null;
        }
    }

    void createLogWithSingletonLN() throws Exception {
        openStandaloneEnv();
        writeSingletonLNData(0, 100);
        close(false);
        upgradeJDB(envHome);
        File renamedLogFile = new File(envHome, singletonLN_jdb);
        renameJDB(renamedLogFile);
        System.out.println("Finish creation: " + singletonLN_jdb);
    }

    void createRepLogWithSingletonLN() throws Exception {
        openRepEnv();
        writeSingletonLNData(0, 100);
        upgradeAllRepJDB(false);
        for (int i = 0; i < repNodes; i++) {
            File renamedLogFile =
                new File(envHomes[i], singletonLNRep_jdb + "_" + i + ".jdb");
            logFile = new File(envHomes[i], "00000000.jdb");
            renameJDB(renamedLogFile);
        }
        System.out.println("Finish creation: " + singletonLNRep_jdb);
    }

    void writeSingletonLNData(int start, int end) {
        for (int i = start; i < end; i++) {
            theKey = makeEntry(i);
            theData = makeEntry(i);
            db.put(null, theKey, theData);
        }
    }

    void createLogWithDIN() throws Exception {
        openStandaloneEnv();
        writeDINData(0, 100, 10);
        close(false);
        upgradeJDB(envHome);
        File renamedLogFile = new File(envHome, DIN_jdb);
        renameJDB(renamedLogFile);
        System.out.println("Finish creation: " + DIN_jdb);
    }

    void createRepLogWithDIN() throws Exception {
        openRepEnv();
        writeDINData(0, 100, 10);
        upgradeAllRepJDB(false);
        for (int i = 0; i < repNodes; i++) {
            File renamedLogFile =
                new File(envHomes[i], DINRep_jdb + "_" + i + ".jdb");
            logFile = new File(envHomes[i], "00000000.jdb");
            renameJDB(renamedLogFile);
        }
        System.out.println("Finish creation: " + DINRep_jdb);
    }

    void writeDINData(int start, int end, int keys) {
        for(int i = start; i < end; i++) {
            /* Insert ten records for a given key. */
            theKey = makeEntry(i % keys);
            theData = makeEntry(i);
            db.put(null, theKey, theData);
        }
    }

    void createLogWithDeletedLN(boolean ifCommit)
        throws Exception {

        openStandaloneEnv();
        writeDINData(0, 100, 10);
        deleteDIN(10, 10, 5, ifCommit);
        abnormalClose(false);
        upgradeJDB(envHome);
        File renamedLogFile = new File(envHome, ifCommit ?
                                                DeletedLNCommit_jdb :
                                                DeletedLNNoCommit_jdb);
        renameJDB(renamedLogFile);
        System.out.println("Finish creation: " + (ifCommit ?
                                                  DeletedLNCommit_jdb :
                                                  DeletedLNNoCommit_jdb));
    }

    void createRepLogWithDeletedLN(boolean ifCommit)
        throws Exception {

        openRepEnv();
        writeDINData(0, 100, 10);

        deleteDIN(10, 10, 5, ifCommit);
        upgradeAllRepJDB(true);
        for (int i = 0; i < repNodes; i++) {
            File renamedLogFile = new File(envHomes[i],
                                           (ifCommit ?
                                           DeletedLNCommitRep_jdb :
                                           DeletedLNNoCommitRep_jdb) + "_" +
                                           i + ".jdb");
            logFile = new File(envHomes[i], "00000000.jdb");
            renameJDB(renamedLogFile);
        }
        System.out.println("Finish creation: " +
                           (ifCommit ?
                            DeletedLNCommitRep_jdb :
                            DeletedLNNoCommitRep_jdb));
    }

    void deleteDIN(int keys, int dups, int deleteDups, boolean ifCommit) {
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setSync(true);
        Transaction txn = env.beginTransaction(null, txnConfig);
        Cursor cur = db.openCursor(txn, null);
        try {
            for(int i = 0; i < keys; i++) {
                for (int j = 0; j < dups; j++) {
                    cur.getNext(theKey, theData, null);
                    /* Delete half of dup records. */
                    if (j < deleteDups) {
                        cur.delete();
                    }
                }
            }
        } finally {
            /* Do a checkpoint before closing the cursor. */
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            env.checkpoint(ckptConfig);

            if (ifCommit) {
                cur.close();
                txn.commit();
            }
        }
    }

    void createLogWithMixIN() throws Exception {
        openStandaloneEnv();
        writeDINData(0, 70, 10);
        writeSingletonLNData(70, 100);
        deleteDIN(10, 7, 3, true);
        deleteDIN(10, 7, 3, false);
        abnormalClose(false);
        upgradeJDB(envHome);
        File renamedLogFile = new File(envHome, MixIN_jdb);
        renameJDB(renamedLogFile);
        System.out.println("Finish creation: " + MixIN_jdb);
    }

    void createRepLogWithMixIN() throws Exception {
        openRepEnv();
        writeDINData(0, 70, 10);
        writeSingletonLNData(70, 100);
        deleteDIN(10, 7, 3, true);
        deleteDIN(10, 7, 3, false);
        upgradeAllRepJDB(true);
        for (int i = 0; i < repNodes; i++) {
            File renamedLogFile =
                new File(envHomes[i], MixINRep_jdb + "_" + i + ".jdb");
            logFile = new File(envHomes[i], "00000000.jdb");
            renameJDB(renamedLogFile);
        }
        System.out.println("Finish creation: " + MixINRep_jdb);
    }

    /**
     * Create a log with DBINs/DINs in the last checkpoint.  Do not run the
     * preupgrade utility.  An exception should be thrown when opening this
     * environment with JE 5.
     */
    void createNoPreUpgradeDups() throws Exception {
        openStandaloneEnv();
        writeDINData(0, 100, 10);

        /* Write DBINs/DINs in a checkpoint. */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /* Do not do a clean close. We want DBINs/DINs in last checkpoint. */
        abnormalClose(false);

        File renamedLogFile = new File(envHome, NoPreUpgrade_Dups_jdb);
        renameJDB(renamedLogFile);
        System.out.println("Finish creation: " + NoPreUpgrade_Dups_jdb);
    }

    /**
     * Create a log with regular deltas (no dups) in the last checkpoint.  Do
     * not run the preupgrade utility.  An exception should be thrown when
     * opening this environment with JE 5.
     */
    void createNoPreUpgradeDeltas() throws Exception {
        env = new Environment(envHome, makeEnvConfig(false /*smallNodeMax*/));

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "testDB", dbConfig);

        final int NUM_KEYS = 500;

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry(new byte[10]);

        /* Insert records. */
        for (int j = 0; j < NUM_KEYS; j += 1) {
            IntegerBinding.intToEntry(j, key);
            final OperationStatus status = db.putNoOverwrite(null, key, data);
            if (status != OperationStatus.SUCCESS) {
                throw new RuntimeException();
            }
        }

        /* Update records to create deltas. */
        data.setData(new byte[11]);
        for (int j = 0; j < NUM_KEYS; j += 10) {
            IntegerBinding.intToEntry(j, key);
            final OperationStatus status = db.put(null, key, data);
            if (status != OperationStatus.SUCCESS) {
                throw new RuntimeException();
            }
        }

        /* Write deltas in a checkpoint. */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /* Do not do a clean close. We want deltas in the last checkpoint. */
        abnormalClose(false);

        File renamedLogFile = new File(envHome, NoPreUpgrade_Deltas_jdb);
        renameJDB(renamedLogFile);
        System.out.println("Finish creation: " + NoPreUpgrade_Deltas_jdb);
    }

    DatabaseEntry makeEntry(int val) {
        byte[] data = new byte[] { (byte) val };
        return new DatabaseEntry(data);
    }

    void renameJDB(File newFile) throws Exception {
        if (!logFile.renameTo(newFile)) {
            throw new Exception
                ("Could not rename: " + logFile + " to " + newFile);
        }
    }

    void upgradeRepJDB(File envHome,
                       String groupName,
                       String nodeName,
                       String nodeHostPort,
                       String helperHosts) {
        /*DbRepPreUpgrade_4_1 upgrade =
            new DbRepPreUpgrade_4_1(envHome, groupName, nodeName, nodeHostPort,
                                    helperHosts);
        upgrade.preUpgrade();*/
    }

    /* Upgrade all the rep jdbs. */
    void upgradeAllRepJDB(boolean abnormalClose) {
        for (int i = repNodes - 1; i >= 0; i--) {
            if (repEnvs[i] != null) {
                if (abnormalClose) {
                    RepInternal.
                        getNonNullRepImpl((ReplicatedEnvironment) repEnvs[i]).
                        abnormalClose();
                } else {
                    if (i == 0) {
                        db.close();
                    }
                    repEnvs[i].close();
                }
                upgradeRepJDB(envHomes[i], "UnitTestGroup", "Node " + (i + 1),
                              "localhost:" + (5001 + i), "localhost:5001");
            }
        }
    }

    void upgradeJDB(File envHome) {
        /*DbPreUpgrade_4_1 upgrade = new DbPreUpgrade_4_1(envHome);
        upgrade.preUpgrade();*/
    }
}
