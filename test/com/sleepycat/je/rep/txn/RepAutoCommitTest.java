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

package com.sleepycat.je.rep.txn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Check that autocommit txns in a replicated environment still obey
 * consistency policies.
 */
public class RepAutoCommitTest extends TestBase {

    /* Replication tests use multiple environments. */
    private final File envRoot;
    private RepEnvInfo[] repEnvInfo;

    public RepAutoCommitTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {

        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() != null) {
                info.closeEnv();
            }
        }
    }

    /**
     * @throws IOException
     */
    @Test
    public void testAutoCommit()
        throws IOException {

        /* Register custom consistency policy format while quiescent. */
        RepUtils.addConsistencyPolicyFormat
            (RepTestUtils.AlwaysFail.NAME,
             new RepTestUtils.AlwaysFailFormat());

        Logger logger = LoggerUtils.getLoggerFixedPrefix(getClass(),
                                                         "Test");
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
        /* Require all nodes to ack. */
        for (RepEnvInfo rei : repEnvInfo) {
            rei.getEnvConfig().
                setDurability(RepTestUtils.SYNC_SYNC_ALL_DURABILITY);
        }
        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        /* Create a db */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        Database masterDb = master.openDatabase(null, "Foo", dbConfig);
        Database replicaDb = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        ReplicatedEnvironment replica = null;
        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() != master) {
                replica = info.getEnv();
                break;
            }
        }

        try {
            /* Insert a record, should be successful. */
            IntegerBinding.intToEntry(1, key);
            IntegerBinding.intToEntry(1, data);
            assertEquals(OperationStatus.SUCCESS,
                         masterDb.put(null, key, data));

            /* Read on replica w/auto commit. */
            replicaDb = replica.openDatabase(null, "Foo", dbConfig);
            assertEquals(OperationStatus.SUCCESS,
                         replicaDb.get(null, key, data, LockMode.DEFAULT));

            /* Read on replica w/non-txnl cursor. */
            Cursor cursor = replicaDb.openCursor(null, null);
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchKey(key, data, LockMode.DEFAULT));
            cursor.close();

            /* Read on master w/auto commit. */
            assertEquals(OperationStatus.SUCCESS,
                         masterDb.get(null, key, data, LockMode.DEFAULT));

            /* Read on master w/non-txnl cursor. */
            cursor = masterDb.openCursor(null, null);
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchKey(key, data, LockMode.DEFAULT));
            cursor.close();

            /*
             * Write on replica w/autocommit should fail because writes are
             * prohibited.
             */
            try {
                OperationStatus status = replicaDb.put(null, key, data);
                fail("Should have gotten write exception. Status="
                     + status);
            } catch (ReplicaWriteException expected) {
                logger.info("expected " + expected);
            }

            /* Crash the replicas */
            replicaDb.close();
            replicaDb = null;
            for (RepEnvInfo info : repEnvInfo) {
                if (info.getEnv() != master) {
                    logger.info("closing" + info.getEnv().getNodeName());
                    info.abnormalCloseEnv();
                }
            }

            /* Insert a record. It should block w/insufficient acks. */
            IntegerBinding.intToEntry(2, key);
            IntegerBinding.intToEntry(2, data);
            try {
                OperationStatus status = masterDb.put(null, key, data);
                fail("Should have gotten insufficient replicas. Status="
                     + status);
            } catch (InsufficientReplicasException expected) {
                logger.info("expected " + expected);
            }

            /*
             * Read the successfully inserted record back on the master even
             * though all replicas are dead It should use an autocommit read
             * only txn, and should not block due to the dead replicas, because
             * this is read only.
             */
            IntegerBinding.intToEntry(1, key);
            assertEquals(OperationStatus.SUCCESS,
                         masterDb.get(null, key, data, LockMode.DEFAULT));
            logger.info("attempt to read on master after replica crash" +
                        IntegerBinding.entryToInt(data));

            /*
             * Open the replicas w/a consistency policy that won't be
             * satisfied.
             */
            for (RepEnvInfo info : repEnvInfo) {
                if (info.getEnv() == null) {
                    info.getRepConfig().setConsistencyPolicy
                        (new RepTestUtils.AlwaysFail());
                    replica = info.openEnv();
                    break;
                }
            }

            /* 
             * Test openDatabase with fail consistency check because 
             * env.openDatabase now requires a write lock. 
             */
            try {
                replicaDb = replica.openDatabase(null, "Foo", dbConfig);
                fail("Should have gotten consistency failure.");
            } catch (ReplicaConsistencyException expected) {
                logger.info("expected " + expected);
            }

            /* 
             * Open the database with a TimeConsistencyPolicy so that the 
             * operation succeeds. 
             */
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setConsistencyPolicy
                (new TimeConsistencyPolicy(Integer.MAX_VALUE,
                                           TimeUnit.MILLISECONDS, 0, null));
            Transaction txn = replica.beginTransaction(null, txnConfig);
            replicaDb = replica.openDatabase(txn, "Foo", dbConfig);
            txn.commit();

            /* Should fail consistency check: Read on replica w/auto commit. */
            try {
                OperationStatus status = replicaDb.get(null, key, data,
                                                       LockMode.DEFAULT);
                fail("Should have gotten consistency failure. Status="
                     + status);
            } catch (ReplicaConsistencyException expected) {
                logger.info("expected " + expected);
            }

            /*
             * Should fail consistency check : Open cursor on replica 
             * w/non-txnal cursor.
             */
            try {
                replicaDb.openCursor(null, null);
                fail("Should have gotten consistency failure.");
            } catch (ReplicaConsistencyException expected) {
                logger.info("expected " + expected);
            } 
        } finally {
            masterDb.close();

            if (replicaDb != null) {
                replicaDb.close();
            }
        }
        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() != null) {
                info.closeEnv();
            }
        }
    }
}
