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

package com.sleepycat.je.rep.vlsn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Log cleaning can create gaps after the rightmost vlsn, due to the greater
 * effort spend in finding cleanable files that don't interfere with the 
 * replication stream. This test checks that we proactively clean files to the
 * right of the last VLSN, if possible, and that we can syncup when logs
 * have gaps due to this kind of proactive cleaning. [#21069]
 */
public class SyncupWithGapsTest extends TestBase {

    private static final String STUFF =  
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" +
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" +
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
           
    private final File envRoot;
    private final String dbName = "testDB";
    private RepEnvInfo[] repEnvInfo;
    private Database db;
    private final Logger logger;
    private final StatsConfig clearConfig;
    
    public SyncupWithGapsTest() {
        envRoot = SharedTestUtils.getTestDir();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");
        clearConfig = new StatsConfig();
        clearConfig.setClear(true);
        clearConfig.setFast(false);
    }

    private EnvironmentConfig makeEnvConfig() {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        DbInternal.disableParameterValidation(envConfig);
        envConfig.setConfigParam(EnvironmentParams.LOG_FILE_MAX.getName(),
                                 "10000");
        /* Control cleaning explicitly. */
        envConfig.setConfigParam(EnvironmentParams.ENV_RUN_CLEANER.getName(),
                                 "false");
        return envConfig;
    }

    private int findMasterIndex(Environment master) {
        for (int i = 0; i < repEnvInfo.length; i++) {
            if (repEnvInfo[i].getEnv().equals(master)) {
                return i;
            }
        }

        fail("Master should exist");
        return 0;
    }
        
    /** Write data into the database. */
    private void generateData(Environment master, 
                              int numTxns,
                              Durability durability,
                              boolean doCommit) {

        /* Write some data. */
        DatabaseEntry key = new DatabaseEntry();
        byte[] dataPadding = new byte[1000];
        DatabaseEntry data = new DatabaseEntry(dataPadding);

        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setDurability(durability);

        for (int i = 0; i < numTxns; i++) {
            final Transaction txn = 
                master.beginTransaction(null,txnConfig);
            //            long keyPrefix = i << 10;
            //            LongBinding.longToEntry(keyPrefix + i, key);
            LongBinding.longToEntry(i, key);
            db.put(txn, key, data);         

            if (doCommit) {
                txn.commit();
            } else {
                txn.abort();
            }
        }
    }
    
    private void readData(Environment env,
                          int lastRecordVal) {
        Database readDb = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        try {
            readDb = openDatabase(env); 

            for (int i = 0; i < lastRecordVal; i++) {
                LongBinding.longToEntry(i, key);
                assertEquals(OperationStatus.SUCCESS, 
                             db.get(null, key, data, LockMode.DEFAULT));
            }
        } finally {
            if (readDb != null) {
                readDb.close();
            }
        }
    }

    private Database openDatabase(Environment env) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        return env.openDatabase(null, dbName, dbConfig);        
    }

    private int cleanLog(Environment env) {
        int numCleaned = 0;
        int total = 0;
        do {
            numCleaned = env.cleanLog();
            total += numCleaned;
        } while (numCleaned > 0);

        logger.info("cleaned " + total);
        return total;
    }

    private void closeReplicas(int masterIndex) {
        for (int i = 0; i < repEnvInfo.length; i++) {
            if (i != masterIndex) {
                repEnvInfo[i].closeEnv();
            }
        }
    }

    private void openReplicas(int masterIndex) {
        RepEnvInfo[] restartList = new RepEnvInfo[2];
        int a = 0;
        for (int i = 0; i < repEnvInfo.length; i++) {
            if (i != masterIndex) {
                restartList[a++] = repEnvInfo[i];
            }
        }

        RepTestUtils.restartGroup(restartList);
    }

    /**
     * Create a log that will have swathes of cleaned files that follow the
     * replication stream, or are intermingled in the replication stream.
     * @return master
     */
    private Environment setupLogWithCleanedGaps(boolean multipleGaps) 
        throws Exception {

        db = null;
        repEnvInfo = 
            RepTestUtils.setupEnvInfos(envRoot, 3, makeEnvConfig());
        Environment master = RepTestUtils.joinGroup(repEnvInfo);
        int masterIdx = findMasterIndex(master);
        db = openDatabase(master);

        /* Write some data so there is a replication stream. */         
        generateData(master, 50, Durability.COMMIT_NO_SYNC, true);

        /*
         * Make the master have a low-utilization log, and gate cleaning
         * with a non-updating global cbvlsn. Shut down the replicas so the
         * global cbvlsn remains low, and then fill the master with junk. 
         * The junk will either entirely be to the right of the last VLSN,
         * or (since we can't predict RepGroupDB updates) at least within
         * the range of the active VLSN range.
         */
        closeReplicas(masterIdx);
        fillLogWithTraceMsgs(master, 50);

        if (multipleGaps) {
            Durability noAck = new Durability(SyncPolicy.NO_SYNC,
                                              SyncPolicy.NO_SYNC,
                                              ReplicaAckPolicy.NONE);
            /* Write more data */         
            generateData(master, 50, noAck, true);

            /* Make a second cleanup area of junk */
            fillLogWithTraceMsgs(master, 50);
        }

        CheckpointConfig cc = new CheckpointConfig();
        cc.setForce(true);
        master.checkpoint(cc);
                
        EnvironmentStats stats = master.getStats(clearConfig);
        stats = master.getStats(clearConfig);
            
        /* Clean the log */
        int totalCleaned = 0;
        int cleanedThisPass = 0;
        do {
            cleanedThisPass = cleanLog(master);
            totalCleaned += cleanedThisPass;
            master.checkpoint(cc);

            stats = master.getStats(clearConfig);
            logger.info("after cleaning, cleaner backlog = " + 
                        stats.getCleanerBacklog() +  " deletionBacklog=" +
                        stats.getFileDeletionBacklog());
        } while (cleanedThisPass > 0);

        assertTrue(totalCleaned > 0);
        
        return master;
    }
    @Test
    public void testReplicaHasOneGapNetworkRestore() 
        throws Throwable {
        doReplicaHasGapNetworkRestore(false);
    }

    @Test
    public void testReplicaHasMultipleGapsNetworkRestore() 
        throws Throwable {
        doReplicaHasGapNetworkRestore(true);
    }

    private void doReplicaHasGapNetworkRestore(boolean multiGaps) 
        throws Throwable {

        Durability noAck = new Durability(SyncPolicy.NO_SYNC,
                                          SyncPolicy.NO_SYNC,
                                          ReplicaAckPolicy.NONE);
        db = null;
        try {
            Environment master = setupLogWithCleanedGaps(multiGaps);
            int masterIdx = findMasterIndex(master);
            /* 
             * Write a record, so that we are sure that there will be a 
             * network restore, because we have to cross a checkpoint.
             */
            generateData(master, 1, noAck, false);
            CheckpointConfig cc = new CheckpointConfig();
            master.checkpoint(cc);
            EnvironmentStats stats = master.getStats(clearConfig);
            assertEquals(0, stats.getCleanerBacklog());
            if (multiGaps) {
                logger.info("Multigap: deletion backlog = " + 
                             stats.getFileDeletionBacklog());
            } else {
                assertEquals(0, stats.getFileDeletionBacklog());
            }

            db.close();
            db = null;
            repEnvInfo[masterIdx].closeEnv();

            /* Start up the two replicas */
            openReplicas(masterIdx);

            /* Start the node that had been the master */
            try {
                repEnvInfo[masterIdx].openEnv();
                fail("Should be a network restore");
            } catch(InsufficientLogException ile) {
                repEnvInfo[masterIdx].closeEnv();
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setRetainLogFiles(true);
                restore.execute(ile, config);
                repEnvInfo[masterIdx].openEnv();
            }

            /* Check its last VLSN and size. */
            
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (db != null) {
                db.close();
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    @Test
    public void testReplicaHasGap() 
        throws Throwable {

        db = null;
        try {
            Environment master = setupLogWithCleanedGaps(false);
            int masterIdx = findMasterIndex(master);
            db.close();
            db = null;
            repEnvInfo[masterIdx].closeEnv();

            /* Start up the two replicas */
            openReplicas(masterIdx);

            /* Start the master */
            try {
                repEnvInfo[masterIdx].openEnv();
            } catch(InsufficientLogException ile) {
                repEnvInfo[masterIdx].closeEnv();
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setRetainLogFiles(true);
                restore.execute(ile, config);
                repEnvInfo[masterIdx].openEnv();
            }
            
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (db != null) {
                db.close();
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /**
     * On the master, generate a log that has
     *  section A: a lot of records packed together
     *  section B: a lot of junk that gets cleaned away, creating a gap in 
     *             the log
     *  section C: a new section of data
     *
     * Bring the replicas down after A is replicated, but before C is written.
     * When the replicas come up, they will have to be fed by the feeder
     * from point A.
     */
    @Test
    public void testFeederHasGap() 
        throws Throwable {

        Durability noAck = new Durability(SyncPolicy.NO_SYNC,
                                          SyncPolicy.NO_SYNC,
                                          ReplicaAckPolicy.NONE);
        db = null;
        try {
            Environment master = setupLogWithCleanedGaps(false);
            int masterIdx = findMasterIndex(master);

            /* 
             * Write a single record, and then junk, so that we are sure there
             * is a new VLSN, and that the replicas will have to sync up to
             * this point, across the gap of cleaned junk.
             */
            generateData(master, 1, noAck, false);
            EnvironmentStats stats = master.getStats(clearConfig);
            assertEquals(0, stats.getCleanerBacklog());
            assertEquals(0, stats.getFileDeletionBacklog());

            /* Start up the two replicas */
            for (int i = 0; i < repEnvInfo.length; i++) {
                if (i != masterIdx) {
                    
                    repEnvInfo[i].openEnv();
                    /* make sure we have up to date data */
                    readData(repEnvInfo[i].getEnv(), 50);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            if (db != null) {
                db.close();
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    private void fillLogWithTraceMsgs(Environment env,
                                      int numToAdd) {
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        FileManager fileManager = envImpl.getFileManager();
        Long beforeTracing = getLastFileNum(env);
        logger.info("BeforeTracing end file = 0x" + 
                     Long.toHexString(beforeTracing));
        do {
            for (int i = 0; i <= 100; i++) {
                Trace.trace(envImpl, STUFF + i);
            }
        } while (fileManager.getLastFileNum() <= (beforeTracing + numToAdd));
        Long afterTracing = fileManager.getLastFileNum();
        logger.info("AfterTracing end file = 0x" + 
                     Long.toHexString(afterTracing));
        /* Check that we've grown the log by a good bit - at least 40 files */ 
        assertTrue((afterTracing - beforeTracing) > 40 );
    }

    private long getLastFileNum(Environment env) {
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        FileManager fileManager = envImpl.getFileManager();
        return fileManager.getLastFileNum();
    }
}

