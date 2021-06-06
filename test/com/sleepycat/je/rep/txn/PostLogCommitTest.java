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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.txn.MasterTxn.MasterTxnFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class PostLogCommitTest extends TestBase {

    private final File envRoot;

    public PostLogCommitTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Checks that the state of a txn is COMMITTED when a post-log-commit
     * exception (InsufficientAcksException) is thrown, and that abort can be
     * called after commit.
     *
     * Prior to a bug fix [#21598] the txn state was MUST_ABORT, and the abort
     * fired an assertion because the commitLsn was non-null.  Even worse, if
     * assertions were disabled the abort would be logged (as well as the
     * commit).
     */
    @Test
    public void testPostLogCommitException()
        throws IOException {

        /* Create 3 node group with 1 "dead" replica that we'll close below. */
        final RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 3);
        final ReplicatedEnvironment master =
            RepTestUtils.joinGroup(repEnvInfo);
        assertSame(master, repEnvInfo[0].getEnv());
        final ReplicatedEnvironment liveReplica = repEnvInfo[1].getEnv();
        final RepEnvInfo deadReplicaInfo = repEnvInfo[2];

        final Durability syncSyncAll = new Durability
            (Durability.SyncPolicy.SYNC, Durability.SyncPolicy.SYNC,
             Durability.ReplicaAckPolicy.ALL);

        /*
         * Open/create database on master.  Use syncSyncAll so DB can be opened
         * immediately on replica.
         */
        final DatabaseConfig dbConfig =
            new DatabaseConfig().setAllowCreate(true).setTransactional(true);
        Transaction txn = master.beginTransaction(null, null);
        Database masterDb = master.openDatabase(txn, "foo", dbConfig);
        txn.commit(syncSyncAll);
        txn = null;

        /* Open DB on replica. */
        dbConfig.setAllowCreate(false);
        Database replicaDb = liveReplica.openDatabase(null, "foo", dbConfig);

        /*
         * Write data and commit with ReplicaAckPolicy.ALL, expect
         * InsufficientAcksException.
         */
        final DatabaseEntry key = new DatabaseEntry(new byte[1]);
        final DatabaseEntry data = new DatabaseEntry(new byte[1]);
        try {
            MasterTxn.setFactory(new TxnFactory(deadReplicaInfo));
            txn = master.beginTransaction(null, null);
            final OperationStatus status = masterDb.put(txn, key, data);
            assertSame(OperationStatus.SUCCESS, status);
            verifyData(txn, masterDb);
            try {
                txn.commit(syncSyncAll);
                fail();
            } catch (InsufficientAcksException expected) {
            }
        } finally {
            MasterTxn.setFactory(null);
        }

        /* Before the fix, the txn state was MUST_ABORT here. */
        assertSame(Transaction.State.COMMITTED, txn.getState());
        verifyData(null, masterDb);

        /* Before the fix, abort would fire an assertion here. */
        txn.abort();
        assertSame(Transaction.State.COMMITTED, txn.getState());
        verifyData(null, masterDb);
        verifyData(null, replicaDb);

        masterDb.close();
        replicaDb.close();

        /* Shutdown environments without a checkpoint. */
        for (final RepEnvInfo repi : repEnvInfo) {
            if (repi.getEnv() != null) {
                repi.abnormalCloseEnv();
            }
        }

        /* Restart the environments and verify data for all. */
        RepTestUtils.restartGroup(repEnvInfo);
        dbConfig.setAllowCreate(false);
        for (final RepEnvInfo repi : repEnvInfo) {
            final ReplicatedEnvironment rep = repi.getEnv();
            final Database db = rep.openDatabase(null, "foo", dbConfig);
            verifyData(null, db);
            db.close();
        }
        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    private void verifyData(Transaction txn, Database checkDb) {
        final DatabaseEntry key = new DatabaseEntry(new byte[1]);
        final DatabaseEntry data = new DatabaseEntry();
        final OperationStatus status = checkDb.get(txn, key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals(1, checkDb.count());
        assertEquals(1, key.getData().length);
        assertEquals(1, data.getData().length);
        assertEquals(0, key.getData()[0]);
        assertEquals(0, data.getData()[0]);
    }

    /**
     * Factory for creating a TestMasterTxn.  The TestMasterTxn is only created
     * when called in this thread, to guard against it being called by
     * unrelated tasks.
     */
    private class TxnFactory implements MasterTxnFactory {

        private final RepEnvInfo deadReplicaInfo;
        private final Thread thread = Thread.currentThread();

        TxnFactory(RepEnvInfo deadReplicaInfo) {
            this.deadReplicaInfo = deadReplicaInfo;
        }

        @Override
        public MasterTxn create(EnvironmentImpl envImpl,
                                TransactionConfig config,
                                NameIdPair nameIdPair) {
            if (Thread.currentThread() != thread) {
                return new MasterTxn(envImpl, config, nameIdPair);
            }
            return new TestMasterTxn(envImpl, config, nameIdPair,
                                     deadReplicaInfo);
        }

        @Override
        public MasterTxn createNullTxn(EnvironmentImpl envImpl,
                                       TransactionConfig config,
                                       NameIdPair nameIdPair) {

            return new MasterTxn(envImpl, config, nameIdPair) {
                @Override
                protected boolean updateLoggedForTxn() {
                    return true;
                }
            };
        }
    }

    /**
     * MasterTxn that calls deadReplicaInfo.closeEnv after the pre-log-commit
     * check. If we were to close the replica env earlier,
     * InsufficientReplicasException would be thrown by the pre-log-commit
     * check.  We want InsufficientAcksException to be thrown instead, during
     * the post-log-commit check.
     */
    private class TestMasterTxn extends MasterTxn {

        private final RepEnvInfo deadReplicaInfo;

        public TestMasterTxn(EnvironmentImpl envImpl,
                             TransactionConfig config,
                             NameIdPair nameIdPair,
                             RepEnvInfo deadReplicaInfo) {
            super(envImpl, config, nameIdPair);
            this.deadReplicaInfo = deadReplicaInfo;
        }

        @Override
        protected void preLogCommitHook() {
            super.preLogCommitHook();
            deadReplicaInfo.closeEnv();
        }
    }
}
