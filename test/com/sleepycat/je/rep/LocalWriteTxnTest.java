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

package com.sleepycat.je.rep;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.RepTestUtils;

/**
 * Tests combinations of local-write and read-only transaction config settings.
 * Ensures that read/write of local/replicated DBs, as well as ack and
 * consistency policies, are enforced as specified in the API.
 */
public class LocalWriteTxnTest extends RepTestBase {

    private static final int N_RECORDS = 10;

    private static class Case {
        /* Following fields identify the test case. */
        final boolean localWrite;
        final boolean readOnly;
        /* Following fields determine the expected behavior. */
        final boolean localWriteAllowed;
        final boolean repWriteAllowed;
        final boolean acksEnforced;

        Case(final boolean localWrite,
             final boolean readOnly,
             final boolean localWriteAllowed,
             final boolean repWriteAllowed,
             final boolean acksEnforced) {
            this.localWrite = localWrite;
            this.readOnly = readOnly;
            this.localWriteAllowed = localWriteAllowed;
            this.repWriteAllowed = repWriteAllowed;
            this.acksEnforced = acksEnforced;
        }

        @Override
        public String toString() {
            return "localWrite=" + localWrite +
                " readOnly=" + readOnly +
                " localWriteAllowed=" + localWriteAllowed +
                " repWriteAllowed=" + repWriteAllowed +
                " acksEnforced=" + acksEnforced;
        }
    }

    private static Case[] CASES = new Case[] {
        /*
         *       localWrite
         *              readOnly
         *                     localWriteAllowed
         *                            repWriteAllowed
         *                                   acksEnforced
         */
        new Case(false, false, false, true,  true),
        new Case(false, true,  false, false, false),
        new Case(true,  false, true,  false, false),
    };

    ReplicatedEnvironment master;
    Database masterDb;
    Database[] repDbs;
    Database[] localNonTxnalDbs;
    Database[] localTxnalDbs;

    public LocalWriteTxnTest() {
        groupSize = 3;
        repDbs = new Database[groupSize];
        localNonTxnalDbs = new Database[groupSize];
        localTxnalDbs = new Database[groupSize];
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        /* Prevent OOME in a long test suite. */
        master = null;
        masterDb = null;
        repDbs = null;
        localNonTxnalDbs = null;
        localTxnalDbs = null;
    }

    /**
     * Expect exception when both localWrite and readOnly are set to true.
     */
    @Test
    public void testIllegalConfig() {
        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setLocalWrite(true);
        try {
            txnConfig.setReadOnly(true);
            fail();
        } catch (IllegalArgumentException expected) {
            assertEquals(false, txnConfig.getReadOnly());
        }

        /* Should fail setting params in either order. */
        txnConfig.setLocalWrite(false);
        txnConfig.setReadOnly(true);
        try {
            txnConfig.setLocalWrite(true);
            fail();
        } catch (IllegalArgumentException expected) {
            assertEquals(false, txnConfig.getLocalWrite());
        }
    }

    @Test
    public void testReadAndWrite() {

        openAndPopulateDBs();

        int n = 0;

        /*
         * Checks for auto-commit writes and non-txnal reads.
         */
        try {
            for (n = 0; n < groupSize; n += 1) {

                /*
                 * Auto-commit writes are always allowed (other than to a rep
                 * DB on a replica, which is handled by checkWriteAllowed).
                 */
                checkWriteAllowed(
                    repDbs[n], null, true);
                checkWriteAllowed(
                    localTxnalDbs[n], null, true);
                checkWriteAllowed(
                    localNonTxnalDbs[n], null, true);

                /* Reads are always allowed. */
                readRecordsNonTxnal(repDbs[n]);
                readRecordsNonTxnal(localTxnalDbs[n]);
                readRecordsNonTxnal(localNonTxnalDbs[n]);
            }

            /* Sync group before checking consistency enforcement. */
            syncGroupToLastCommit();

            for (n = 0; n < groupSize; n += 1) {

                /* Consistency is only enforced on replicas. */
                final boolean consistencyEnforced = (n > 0);

                /*
                 * Consistency of non-tnxal reads is always enforced for a rep
                 * DB, and never enforced for a local DB.
                 */
                checkConsistencyEnforced(
                    repDbs[n], null, consistencyEnforced);
                checkConsistencyEnforced(
                    localTxnalDbs[n], null, false);
                checkConsistencyEnforced(
                    localNonTxnalDbs[n], null, false);
            }
        } catch (final Throwable e) {
            throw new RuntimeException(
                String.valueOf(repEnvInfo[n].getEnv().getState()) +
                " NonTxnal", e);
        }

        /*
         * Checks for user txn writes and reads.
         */
        for (final Case c : CASES) {
            try {
                final TransactionConfig txnConfig =
                    makeTxnConfig(c.localWrite, c.readOnly);

                for (n = 0; n < groupSize; n += 1) {

                    /*
                     * Whether writes are allowed depends on the whether
                     * local-write or read-only are configured.
                     */
                    checkWriteAllowed(
                        repDbs[n], txnConfig, c.repWriteAllowed);
                    checkWriteAllowed(
                        localTxnalDbs[n], txnConfig, c.localWriteAllowed);

                    /* Reads are always allowed. */
                    readRecords(repDbs[n], txnConfig);
                    readRecords(localTxnalDbs[n], txnConfig);
                }

                /* Sync group before checking consistency enforcement. */
                syncGroupToLastCommit();

                for (n = 0; n < groupSize; n += 1) {

                    /* Consistency is only enforced on replicas. */
                    final boolean consistencyEnforced = (n > 0);

                    /* Consistency is always enforced for user txns. */
                    checkConsistencyEnforced(
                        repDbs[n], txnConfig, consistencyEnforced);
                    checkConsistencyEnforced(
                        repDbs[n], null, consistencyEnforced);
                    checkConsistencyEnforced(
                        localTxnalDbs[n], txnConfig, consistencyEnforced);
                }
            } catch (final Throwable e) {
                throw new RuntimeException(
                    String.valueOf(repEnvInfo[n].getEnv().getState()) +
                    " Case: " + c, e);
            }
        }

        /*
         * Stop one replica to check ack enforcement with ack policy ALL.
         */
        final int stopNode = groupSize - 1;
        repDbs[stopNode].close();
        localTxnalDbs[stopNode].close();
        localNonTxnalDbs[stopNode].close();
        repEnvInfo[stopNode].getEnv().close();

        /*
         * Ack enforcement checks for auto-commit writes.
         */
        try {
            for (n = 0; n < stopNode; n += 1) {

                /*
                 * Acks are enforced only for rep DBs on the master.  (If
                 * writeAllowed is false, then of course acks cannot be
                 * enforced.)
                 */
                if (n == 0) {
                    checkAcksEnforced(
                        repDbs[n], null, true /*acksEnforced*/,
                        true /*writeAllowed*/);
                } else {
                    checkAcksEnforced(
                        repDbs[n], null, false /*acksEnforced*/,
                        false /*writeAllowed*/);
                }
                checkAcksEnforced(
                    localTxnalDbs[n], null, false /*acksEnforced*/,
                    true /*writeAllowed*/);
                checkAcksEnforced(
                    localNonTxnalDbs[n], null, false /*acksEnforced*/,
                    true /*writeAllowed*/);
            }
        } catch (final Throwable e) {
            throw new RuntimeException(
                String.valueOf(repEnvInfo[n].getEnv().getState()) +
                " NonTxnal", e);
        }

        /*
         * Ack enforcement checks for user txn writes.
         */
        for (final Case c : CASES) {
            try {
                final TransactionConfig txnConfig =
                    makeTxnConfig(c.localWrite, c.readOnly);

                /*
                 * Whether acks are enforced depends on the whether local-write
                 * is configured.  (If writeAllowed is false, then of course
                 * acks cannot be enforced.)
                 */
                for (n = 0; n < stopNode; n += 1) {
                    final boolean acksEnforced = c.acksEnforced && (n == 0);

                    checkAcksEnforced(
                        repDbs[n], txnConfig, acksEnforced,
                        c.repWriteAllowed && (n == 0));
                    checkAcksEnforced(
                        localTxnalDbs[n], txnConfig, acksEnforced,
                        c.localWriteAllowed);
                }
            } catch (final Throwable e) {
                throw new RuntimeException(
                    String.valueOf(repEnvInfo[n].getEnv().getState()) +
                    " Case: " + c, e);
            }
        }

        for (n = 0; n < stopNode; n += 1) {
            repDbs[n].close();
            localTxnalDbs[n].close();
            localNonTxnalDbs[n].close();
        }
    }

    private TransactionConfig makeTxnConfig(final boolean localWrite,
                                            final boolean readOnly) {

        final TransactionConfig txnConfig = new TransactionConfig();

        txnConfig.setLocalWrite(localWrite);
        txnConfig.setReadOnly(readOnly);

        assertEquals(localWrite, txnConfig.getLocalWrite());
        assertEquals(readOnly, txnConfig.getReadOnly());

        return txnConfig;
    }

    /**
     * Open and populate test DBs using auto-commit writes, which works without
     * specifying local-write.  This, plus using non-transactional reads, also
     * provides more test coverage.
     */
    private void openAndPopulateDBs() {

        /* Start rep group. */
        master = RepTestUtils.joinGroup(repEnvInfo);
        assertSame(master, repEnvInfo[0].getEnv());

        /* Open/populate/read local DBs on all nodes. */
        for (int i = 0; i < groupSize; i += 1) {
            final ReplicatedEnvironment env = repEnvInfo[i].getEnv();

            localTxnalDbs[i] = openDb(
                env, false /*replicated*/, true /*txnal*/);

            writeRecordsAutoCommit(localTxnalDbs[i]);
            readRecordsNonTxnal(localTxnalDbs[i]);

            localNonTxnalDbs[i] = openDb(
                env, false /*replicated*/, false /*txnal*/);

            writeRecordsAutoCommit(localNonTxnalDbs[i]);
            readRecordsNonTxnal(localNonTxnalDbs[i]);
        }

        /* Open/populate/read master DB. */
        masterDb = openDb(master, true /*replicated*/, true /*txnal*/);
        repDbs[0] = masterDb;

        writeRecordsAutoCommit(masterDb);
        readRecordsNonTxnal(masterDb);

        /* Must sync group before reading from replicas. */
        syncGroupToLastCommit();

        /* Open/read replica DBs. */
        for (int i = 1; i < groupSize; i += 1) {
            final ReplicatedEnvironment env = repEnvInfo[i].getEnv();
            repDbs[i] = openDb(env, true /*replicated*/, true /*txnal*/);
            readRecordsNonTxnal(repDbs[i]);
        }
    }

    private Database openDb(final ReplicatedEnvironment env,
                            final boolean replicated,
                            final boolean txnal) {

        final DatabaseConfig config = new DatabaseConfig();
        config.setAllowCreate(true);
        config.setReplicated(replicated);
        config.setTransactional(txnal);

        final String dbName = "test-" +
            (replicated ? "rep" : "nonRep") + "-" +
            (txnal ? "txnal" : "nonTxnal");

        return env.openDatabase(null, dbName, config);
    }

    /**
     * Tries to write and checks behavior against the writeAllowed param value.
     *
     * @param txnConfig is non-null if an explicit txn should be used. Is null
     * to write with a null txn (auto-commit) and when the db is non-txnal.
     */
    private void checkWriteAllowed(final Database db,
                                   final TransactionConfig txnConfig,
                                   final boolean writeAllowed) {
        final ReplicatedEnvironment env =
            (ReplicatedEnvironment) db.getEnvironment();

        try {
            if (txnConfig != null) {
                writeRecords(db, txnConfig);
            } else {
                writeRecordsAutoCommit(db);
            }
            assertTrue("Write was allowed", writeAllowed);
        } catch (UnsupportedOperationException e) {
            assertFalse(
                "Write was not allowed - " + e.getMessage(), writeAllowed);
        } catch (ReplicaWriteException e) {
            assertTrue(
                "Expected UnsupportedOperation but got ReplicaWrite",
                writeAllowed);
            assertTrue(db.getConfig().getReplicated());
            assertEquals(ReplicatedEnvironment.State.REPLICA, env.getState());
        }
    }

    /**
     * Called with one replica down, so committing with ReplicaAckPolicy.ALL
     * should always fail.
     *
     * @param txnConfig is non-null if an explicit txn should be used.
     * Is null to use a null txn (auto-commit) and when the db is non-txnal.
     */
    private void checkAcksEnforced(final Database db,
                                   final TransactionConfig txnConfig,
                                   final boolean acksEnforced,
                                   final boolean writeAllowed) {

        final ReplicatedEnvironment env =
            (ReplicatedEnvironment) db.getEnvironment();

        final Durability ackPolicyAll = new Durability(
            Durability.SyncPolicy.NO_SYNC,
            Durability.SyncPolicy.NO_SYNC,
            Durability.ReplicaAckPolicy.ALL);

        final Durability saveDurability;

        if (txnConfig != null) {
            saveDurability = txnConfig.getDurability();
            txnConfig.setDurability(ackPolicyAll);
        } else {
            final EnvironmentConfig envConfig = env.getConfig();
            saveDurability = envConfig.getDurability();
            envConfig.setDurability(ackPolicyAll);
            env.setMutableConfig(envConfig);
        }

        try {
            if (txnConfig != null) {
                if (writeAllowed) {
                    writeRecords(db, txnConfig);
                } else {
                    readRecords(db, txnConfig);
                }
            } else {
                if (writeAllowed) {
                    writeRecordsAutoCommit(db);
                } else {
                    readRecordsNonTxnal(db);
                }
            }
            assertFalse("Acks were not enforced", acksEnforced);
        } catch (InsufficientReplicasException e) {
            assertTrue("Acks were enforced", acksEnforced);
        } finally {
            if (txnConfig != null) {
                txnConfig.setDurability(saveDurability);
            } else {
                final EnvironmentConfig envConfig = env.getConfig();
                envConfig.setDurability(saveDurability);
                env.setMutableConfig(envConfig);
            }
        }
    }

    private void writeRecordsAutoCommit(final Database db) {
        writeRecords(db, (Transaction) null);
    }

    private CommitToken writeRecords(final Database db,
                                     final TransactionConfig txnConfig) {
        assertNotNull(txnConfig);
        final Environment env = db.getEnvironment();
        final Transaction txn = env.beginTransaction(null, txnConfig);
        try {
            writeRecords(db, txn);
            txn.commit();
            return txn.getCommitToken();
        } catch (final Throwable e) {
            txn.abort();
            throw e;
        }
    }

    private void writeRecords(final Database db, final Transaction txn) {

        /* Cannot use a cursor to write to a txnal DB with no txn. */
        final Cursor cursor =
            (!db.getConfig().getTransactional() || (txn != null)) ?
            db.openCursor(txn, null) :
            null;

        try {
            for (int i = 0; i < N_RECORDS; i++) {
                IntegerBinding.intToEntry(i, key);
                IntegerBinding.intToEntry(i, data);
                final OperationStatus status;
                if ((cursor == null) || ((i & 1) == 0)) {
                    status = db.put(txn, key, data);
                } else {
                    status = cursor.put(key, data);
                }
                assertSame(OperationStatus.SUCCESS, status);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Tries to read with an impossible consistency policy that can never be
     * satisfied.
     *
     * @param txnConfig is non-null if an explicit txn should be used.
     * Is null to read non-transactionally and when the db is non-txnal.
     */
    private void checkConsistencyEnforced(
        final Database db,
        final TransactionConfig txnConfig,
        final boolean consistencyEnforced) {

        final ReplicatedEnvironment env =
            (ReplicatedEnvironment) db.getEnvironment();

        final RepImpl repImpl = RepInternal.getNonNullRepImpl(env);
        final RepNode repNode = repImpl.getRepNode();

        final CommitToken impossibleCommitToken = new CommitToken(
            repNode.getUUID(),
            repNode.getCurrentTxnEndVLSN().getSequence() + 100);

        final ReplicaConsistencyPolicy impossiblePolicy =
            new CommitPointConsistencyPolicy(
                impossibleCommitToken, 1, MILLISECONDS);

        final ReplicaConsistencyPolicy savePolicy;

        if (txnConfig != null) {
            savePolicy = txnConfig.getConsistencyPolicy();
            txnConfig.setConsistencyPolicy(impossiblePolicy);
        } else {
            savePolicy = repImpl.getDefaultConsistencyPolicy();
            repImpl.setDefaultConsistencyPolicy(impossiblePolicy);
        }

        try {
            if (txnConfig != null) {
                readRecords(db, txnConfig);
            } else {
                readRecordsNonTxnal(db);
            }
            assertFalse("Consistency was not enforced", consistencyEnforced);
        } catch (ReplicaConsistencyException e) {
            assertTrue("Consistency was enforced", consistencyEnforced);
        } finally {
            if (txnConfig != null) {
                txnConfig.setConsistencyPolicy(savePolicy);
            } else {
                repImpl.setDefaultConsistencyPolicy(savePolicy);
            }
        }
    }

    private void readRecordsNonTxnal(final Database db) {
        readRecords(db, (Transaction) null);
    }

    private void readRecords(final Database db,
                             final TransactionConfig txnConfig) {
        final Environment env = db.getEnvironment();
        final Transaction txn = env.beginTransaction(null, txnConfig);
        readRecords(db, txn);
        txn.commit();
    }

    private void readRecords(final Database db, final Transaction txn) {
        for (int i = 0; i < N_RECORDS; i++) {
            IntegerBinding.intToEntry(i, key);
            final OperationStatus status = db.get(txn, key, data, null);
            assertSame(OperationStatus.SUCCESS, status);
            assertEquals(i, IntegerBinding.entryToInt(data));
        }
        int i = 0;
        final Cursor cursor = db.openCursor(txn, null);
        while (cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
            assertEquals(i, IntegerBinding.entryToInt(key));
            assertEquals(i, IntegerBinding.entryToInt(data));
            i += 1;
        }
        assertEquals(N_RECORDS, i);
        cursor.close();
    }

    private void syncGroupToLastCommit() {
        try {
            RepTestUtils.syncGroupToLastCommit(repEnvInfo, groupSize);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
