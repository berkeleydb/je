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

package com.sleepycat.je.rep.impl.node;

import static com.sleepycat.je.Durability.ReplicaAckPolicy.NONE;
import static com.sleepycat.je.Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
import static com.sleepycat.je.OperationStatus.NOTFOUND;
import static com.sleepycat.je.OperationStatus.SUCCESS;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_ABORT;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;
import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.monitor.Monitor;
import com.sleepycat.je.rep.stream.InputWireRecord;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.util.DbGroupAdmin;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookAdapter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the Master Transfer operation.
 *
 * @see ReplicatedEnvironment#transferMaster
 */
public class MasterTransferTest extends RepTestBase {
    /**
     * Time duration used to control slow-paced tests, in milliseconds.
     * <p>
     * Several of these tests use a {@code TestHook} in replica Replay in order
     * to deliberately slow down the processing, so as to control the relative
     * timing of events in racing threads.  How much time is enough?  For the
     * tests to be reliable when run unattended as part of a full test suite,
     * the delays must be estimated fairly generously, in order to allow for a
     * range of processor speeds and other concurrent system activity.  On the
     * other hand, if you're debugging just this one test interactively and
     * repeatedly, a generous time estimate would make you impatient.
     * <p>
     * Therefore many of the time delay durations used here are computed in
     * terms of this "tick", which is defined to be 1 second by default, but
     * can be overridden as desired: values on the order of 100-200 msec seem to
     * work well for interactive use on a contemporary workstation that is not
     * otherwise overloaded with other activity.
     * <p>
     * (Note that some of the time limits used here that are not computed in
     * terms of this tick are merely fail-safe limits that should never come
     * into play, but are present only to prevent an infinite hang.  For
     * example, when threads are expected to finish "right away" we might
     * {@code join()} them with a 5000 msec limit, even though they should
     * usually finish much more quickly.  Reducing the limit in these cases
     * cannot normally help make the test complete more quickly.)
     */
    final static long TICK =
        Long.getLong("fasterPace", 1000).longValue();

    /** All potentially open databases, so we can close them during tearDown */
    private final List<Database> databases = new LinkedList<>();

    @Override
    @Before
    public void setUp()
        throws Exception {

        groupSize = 3;
        super.setUp();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        Replica.setInitialReplayHook(null);
        for (final Database db : databases) {
            try {
                db.close();
            } catch (Exception e) {
                /* Ignore during cleanup */
            }
        }
        databases.clear();
        super.tearDown();
    }

    /**
     * Tests a basic master transfer operation in a group of 3, with the master
     * idle at the time of the transfer.  Both replicas are named as candidate
     * targets for the transfer.
     */
    @Test
    public void testBasic() throws Exception {
        createGroup();
        /* env[0] is guaranteed to be the master. */
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        int nRecords = 3;             // arbitrary
        populateDB(master, nRecords);

        /* Setup hooks for state change and syncup. */
        final CountDownLatch becameMaster = new CountDownLatch(1);
        for (int i = 1; i < groupSize; i++) {
            repEnvInfo[i].getEnv().setStateChangeListener
                (new MasterListener(becameMaster));
        }
        CountDownLatch syncupWaiter = RepTestUtils.setupWaitForSyncup(master, 1);

        /* Transfer master to any of the replicas. */
        Set<String> replicas = new HashSet<>();
        for (int i = 1; i < groupSize; i++) {
            replicas.add(repEnvInfo[i].getEnv().getNodeName());
        }
        String newMaster =
            master.transferMaster(replicas, 10, TimeUnit.SECONDS);

        /* Expect that one of the replicas became the master. */
        boolean found = false;
        for (int i = 1; i < groupSize; i++) {
            if (newMaster.equals(repEnvInfo[i].getEnv().getNodeName())) {
                found = true;
                break;
            }
        }
        assertTrue("node name produced as result not in candidate set",
                   found);

        /* Expect that the state change event was also received. */
        boolean tookOver = becameMaster.await(10, TimeUnit.SECONDS);
        assertTrue("neither replica became master within 10 seconds of " +
                   "supposed completion of transfer",
                   tookOver);

        /*
         * The old master should sync up with the new master, without a
         * recovery.
         */
        assertTrue(syncupWaiter.await(10, TimeUnit.SECONDS));

        /* Master should now be a replica, and able to support reads. */
        assertEquals(master.getState(),
                     ReplicatedEnvironment.State.REPLICA);
    }

    /**
     * Tests a master transfer with a transaction in progress.
     */
    @Test
    public void testInFlightTxn() throws Exception {
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();

        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        Transaction txn = masterEnv.beginTransaction(null, null);
        try {
            for (int i = 0; i < 10; i++) {
                IntegerBinding.intToEntry(i, key1);
                LongBinding.longToEntry(i, value);
                db.put(txn, key1, value);
            }

            /* Leave the transaction uncommitted. */
            Set<String> replicas = new HashSet<>();
            replicas.add(replicaName);
            String result =
                masterEnv.transferMaster(replicas, 5, TimeUnit.SECONDS);
            assertEquals(replicaName, result);
            awaitSettle(master, replica);

            try {
                txn.commit();
                fail("expected exception from commit()");
            } catch (IllegalStateException expected) {
            }
            assertFalse(txn.isValid());
            txn.abort();
        } finally {
            safeAbort(txn);
        }
        db.close();

        masterEnv = repEnvInfo[1].getEnv();
        db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        IntegerBinding.intToEntry(1, key1);
        OperationStatus ret = db.get(null, key1, value, null);
        assertEquals(NOTFOUND, ret);
        db.close();
    }

    /** Abort a transaction for cleanup, ignoring any exceptions. */
    private static void safeAbort(final Transaction txn) {
        try {
            txn.abort();
        } catch (Exception e) {
        }
    }

    /**
     * Use test hooks to orchestrate a number of transactions that will be
     * inflight when the master transfer happens, and have the application
     * issue inserts or commits while the transfer is in play.
     * Check afterwards that locks are released appropriately, and the inflight
     * transactions have the proper state.
     */
    @Test
    public void testApplicationActivityDuringConversion() throws Exception {
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();

        /*
         * Populate the db with a committed records 100, 101, 102. We'll read
         * lock them so the in-progress txns have both read and write locks.
         */
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        DatabaseEntry val = new DatabaseEntry();
        DatabaseEntry readEntry = new DatabaseEntry();

        Transaction txn = masterEnv.beginTransaction(null, null);
        try {
            for (int i = 100; i <=102; i++) {
                IntegerBinding.intToEntry(i, val);
                db.putNoOverwrite(txn, val, val);
            }
            txn.commit();
        } finally {
            safeAbort(txn);
        }

        /*
         * Make several in-flight, uncommitted txns which write a record
         * and read record 100. The write locks will be on records 1-5.
         */
        List<Transaction> testTxns = new ArrayList<>();
        Transaction readLockTxn = null;
        try {
            int numInFlightTxns = 14;
            for (int i = 0; i < numInFlightTxns; i++) {
                txn = masterEnv.beginTransaction(null, null);
                testTxns.add(txn);
                IntegerBinding.intToEntry(i+1, val);
                /* Get a write lock on a new value */
                db.put(txn, val, val);

                /* get a read lock on an old value */
                IntegerBinding.intToEntry(100, val);
                assertEquals(OperationStatus.SUCCESS,
                             db.get(txn, val, readEntry, null));
            }

            /* Make an uncommitted txn that has read locks on 101 and 102 */
            readLockTxn = masterEnv.beginTransaction(null, null);

            IntegerBinding.intToEntry(101, val);
            assertEquals(OperationStatus.SUCCESS,
                         db.get(readLockTxn, val, readEntry, null));
            IntegerBinding.intToEntry(102, val);
            assertEquals(OperationStatus.SUCCESS,
                         db.get(readLockTxn, val, readEntry, null));

            /*
             * Generate 12 hooks.
             * - also have one txn with no write locks, should still be valid.
             */
            CountDownLatch done = new CountDownLatch(12);
            Set<CheckedHook> hooks = new HashSet<>();
            hooks.addAll(setupTxnEndHooks(testTxns.get(0),
                                          testTxns.get(1),
                                          testTxns.get(2),
                                          done));

            hooks.addAll(setupInsertHooks(testTxns.get(3),
                                          testTxns.get(4),
                                          testTxns.get(5),
                                          db,
                                          done));

            hooks.addAll(setupUpdateHooks(testTxns.get(6),
                                          testTxns.get(7),
                                          testTxns.get(8),
                                          db,
                                          done));

            hooks.addAll(
                setupRNConvertHooks(
                    RepInternal.getNonNullRepImpl(masterEnv).getRepNode(),
                    testTxns.get(9),
                    testTxns.get(10),
                    testTxns.get(11),
                    db,
                    done));

            /* issue a transfer. */
            Set<String> replicas = new HashSet<>();
            replicas.add(replicaName);
            String result = masterEnv.transferMaster(replicas, 5,
                                                     TimeUnit.SECONDS);
            assertEquals(replicaName, result);
            awaitSettle(master, replica);

            /* Check hooks */
            done.await();
            for (CheckedHook h : hooks) {
                assertTrue(h.toString() + " must run", h.ran);
                assertTrue(h + " " + h.problem, h.problem == null);
            }

            /* The inflight txns should be aborted and have no locks */
            for (Transaction t: testTxns) {
                logger.info("checking state on " + t + "/" + t.getState());
                assertFalse(t.isValid());
                assertFalse (t.getState().equals(Transaction.State.COMMITTED));
                //                if (t.getState().equals(Transaction.State.COMMITTED)) {
                //    logger.info("txn " + t.getId() + " is committed");
                //    continue;
                // }

                Txn internalT = DbInternal.getTxn(t);
                /* The read locks should be null or zeroed */
                if (internalT.getReadLockIds() != null) {
                    assertEquals("txn " + internalT.getId(), 0,
                                 internalT.getReadLockIds().size());
                }
                assertEquals("txn " + t.getId(), 0,
                             DbInternal.getTxn(t).getWriteLockIds().size());
                try {
                    t.commit();
                    fail("expected exception from commit, already closed()");
                } catch (IllegalStateException expected) {
                    logger.info("Expected ISE for txn " + t.getId());
                }

                t.abort();
            }
            db.close();

            /* The transaction that has read locks should still be good. */
            assertTrue("readLockTxn = " + readLockTxn.getId() + "/" +
                       readLockTxn.getState(), readLockTxn.isValid());
            assertEquals(2, DbInternal.getTxn(readLockTxn).
                         getReadLockIds().size());

            /*
             * Now that the transfer has happened, confirm that none of the
             * in-flight data exists on any of the environments.
             */

            /* Syncup the group */
            masterEnv = repEnvInfo[1].getEnv();
            Database checkDb = masterEnv.openDatabase(null, TEST_DB_NAME,
                                                      dbconfig);
            databases.add(checkDb);
            allAckWrite(repEnvInfo[1].getEnv(), checkDb, 1000);
            checkDb.close();

            /* Check that committed data is there, uncommitted is gone */
            for (RepEnvInfo info: repEnvInfo) {
                ReplicatedEnvironment env = info.getEnv();
                logger.info("reading after transfer for node " +
                            env.getNodeName());
                DatabaseEntry readKey = new DatabaseEntry();
                DatabaseEntry readVal = new DatabaseEntry();
                Database readDb = env.openDatabase(null, TEST_DB_NAME,
                                                   dbconfig);
                databases.add(readDb);
                for (int i = 100; i <= 102; i++) {
                    IntegerBinding.intToEntry(i, readKey);
                    txn = env.beginTransaction(null, null);
                    try {
                        assertEquals(OperationStatus.SUCCESS,
                                     readDb.get(txn, readKey, readVal, null));
                        txn.commit();
                    } finally {
                        safeAbort(txn);
                    }
                }
                for (int i = 1; i <= 5; i++) {
                    IntegerBinding.intToEntry(i, readKey);
                    txn = env.beginTransaction(null, null);
                    try {
                        assertEquals(OperationStatus.NOTFOUND,
                                     readDb.get(txn, readKey, readVal, null));
                        txn.commit();
                    } finally {
                        safeAbort(txn);
                    }
                }
                readDb.close();
            }

            readLockTxn.commit();
        } finally {
            for (Transaction t : testTxns) {
                safeAbort(t);
            }
            safeAbort(readLockTxn);
        }
    }

    private Set<CheckedHook> setupTxnEndHooks(Transaction txn1,
                                              Transaction txn2,
                                              Transaction txn3,
                                              CountDownLatch done) {

        Set<CheckedHook> hooks = new HashSet<>();
        /* Commit while frozen in MasterTxn.convertToReplayTxnAndClose */
        final TxnEndHook whileFrozen =
            new TxnEndHook(0, txn1, UnknownMasterException.class,
                           Transaction.State.MUST_ABORT, done,
                           "hook: commit while frozen");

        ((MasterTxn)DbInternal.getTxn(txn1)).setConvertHook(whileFrozen);
        hooks.add(whileFrozen);

        /*
         * Commit after the freeze in MasterTxn.convertToReplayTxnAndClose,
         * before the txn is closed.
         * Even though the txn has no write locks left, it still must abort,
         * because its writes are not committed.
         */
        final TxnEndHook afterUnfrozen =
            new TxnEndHook(1, txn2, UnknownMasterException.class,
                           Transaction.State.ABORTED,
                           done, "hook: commit after unfreeze");
        ((MasterTxn)DbInternal.getTxn(txn2)).setConvertHook(afterUnfrozen);
        hooks.add(afterUnfrozen);

        /* Commit after close in MasterTxn.convertToReplayTxnAndClose */
        final TxnEndHook afterClose =
            new TxnEndHook(2, txn3, IllegalStateException.class,
                           Transaction.State.ABORTED, done,
                           "hook: commit after close");
        ((MasterTxn)DbInternal.getTxn(txn3)).setConvertHook(afterClose);
        hooks.add(afterClose);
        return hooks;
    }

    private Set<CheckedHook> setupInsertHooks(Transaction txn4,
                                              Transaction txn5,
                                              Transaction txn6,
                                              Database db,
                                              CountDownLatch done) {

        Set<CheckedHook> hooks = new HashSet<>();
        /* Insert while frozen in MasterTxn.convertToReplayTxnAndClose */
        final AdditionalWriteHook whileFrozen =
            new AdditionalWriteHook(0, db, txn4,
                                    UnknownMasterException.class,
                                    Transaction.State.MUST_ABORT, done, 20,
                                    "hook: insert while frozen");

        ((MasterTxn)DbInternal.getTxn(txn4)).setConvertHook(whileFrozen);
        hooks.add(whileFrozen);

        /* Insert after the freeze in MasterTxn.convertToReplayTxnAndClose. */
        final AdditionalWriteHook afterUnfreeze =
            new AdditionalWriteHook(0, db, txn5,
                                    UnknownMasterException.class,
                                    Transaction.State.MUST_ABORT, done, 21,
                                    "hook: insert after unfreeze");
        ((MasterTxn)DbInternal.getTxn(txn5)).setConvertHook(afterUnfreeze);
        hooks.add(afterUnfreeze);

        /* Insert after close in MasterTxn.convertToReplayTxnAndClose */
        final AdditionalWriteHook afterClose =
            new AdditionalWriteHook(0, db, txn6,
                                    UnknownMasterException.class,
                                    Transaction.State.MUST_ABORT, done, 22,
                                    "hook: insert after close");
        ((MasterTxn)DbInternal.getTxn(txn6)).setConvertHook(afterClose);
        hooks.add(afterClose);


        return hooks;
    }

    private Set<CheckedHook> setupUpdateHooks(Transaction txn7,
                                              Transaction txn8,
                                              Transaction txn9,
                                              Database db,
                                              CountDownLatch done) {

        Set<CheckedHook> hooks = new HashSet<>();
        /* Insert before the freeze in MasterTxn.convertToReplayTxnAndClose */
        AdditionalWriteHook whileFrozen =
            new AdditionalWriteHook(0, db, txn7,
                                    UnknownMasterException.class,
                                    Transaction.State.MUST_ABORT, done, 7,
                                    "hook: update while frozen");

        ((MasterTxn)DbInternal.getTxn(txn7)).setConvertHook(whileFrozen);
        hooks.add(whileFrozen);

        /* Insert after the freeze in MasterTxn.convertToReplayTxnAndClose. */
        final AdditionalWriteHook afterUnfreeze =
            new AdditionalWriteHook(0, db, txn8,
                                    UnknownMasterException.class,
                                    Transaction.State.MUST_ABORT, done, 8,
                                    "hook: update after Unfreeze");
        ((MasterTxn)DbInternal.getTxn(txn8)).setConvertHook(afterUnfreeze);
        hooks.add(afterUnfreeze);

        /* Insert after close in MasterTxn.convertToReplayTxnAndClose */
        final AdditionalWriteHook afterClose =
            new AdditionalWriteHook(0, db, txn9,
                                    UnknownMasterException.class,
                                    Transaction.State.MUST_ABORT, done, 9,
                                    "hook: update after close");
        ((MasterTxn)DbInternal.getTxn(txn9)).setConvertHook(afterClose);
        hooks.add(afterClose);

        return hooks;
    }

    /* Setup hooks that execute just after the RepNode converts to master */
    private Set<CheckedHook> setupRNConvertHooks(RepNode repNode,
                                                 Transaction txn10,
                                                 Transaction txn11,
                                                 Transaction txn12,
                                                 Database db,
                                                 CountDownLatch done) {

        Set<CheckedHook> hooks = new HashSet<>();
        /* Try to do a write just after the node becomes unknown */
        AdditionalWriteHook update =
            new AdditionalWriteHook(0, db, txn10,
                                    UnknownMasterException.class,
                                    Transaction.State.OPEN, done, 10,
                                    "hook: update after unknown");
        repNode.setConvertHook(update);
        hooks.add(update);

        /* Commit after the node becomes unknown. */
        final TxnEndHook commitH =
            new TxnEndHook(0, txn11, null, Transaction.State.ABORTED,
                           done, "hook: commit after unknown");
        repNode.setConvertHook(commitH);
        hooks.add(commitH);

        /* Try to do an insert just after the node becomes unknown */
        AdditionalWriteHook insert =
            new AdditionalWriteHook(0, db, txn12,
                                    UnknownMasterException.class,
                                    Transaction.State.OPEN, done, 112,
                                    "hook: insert after unknown");
        repNode.setConvertHook(insert);
        hooks.add(insert);

        return hooks;
    }

    @Test
    public void testLateJoiner() throws Exception {
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();

        final RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();
        replica.closeEnv();
        Set<String> replicas = new HashSet<>();
        replicas.add(replicaName);
        try {
            masterEnv.transferMaster(replicas, 5, TimeUnit.SECONDS);
            fail("transfer to non-running node should have failed");
        } catch (MasterTransferFailureException e) {
            // expected
        }
        final Throwable[] th = new Throwable[1];
        Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(5000);
                        restartNodes(replica);
                    } catch (Throwable xcp) {
                        th[0] = xcp;
                    }
                }
            });
        t.start();
        String result =
            masterEnv.transferMaster(replicas, 10, TimeUnit.SECONDS);
        assertEquals(replicaName, result);

        t.join(60000);
        assertFalse(t.isAlive());
        if (th[0] != null) {
            throw new Exception("node-starter thread threw exception", th[0]);
        }

        awaitSettle(master, replica);
        master.closeEnv();
        master.openEnv();
    }

    @Test
    public void testStupidCodingMistakes() throws Exception {
        createGroup();
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        try {
            master.transferMaster(null,  1, TimeUnit.SECONDS);
            fail("null 'replicas' argument should be rejected");
        } catch (IllegalArgumentException e) {
            // expected
        }
        Set<String> replicas = new HashSet<>();
        try {
            master.transferMaster(replicas, 1, TimeUnit.SECONDS);
            fail("empty 'replicas' set should be rejected");
        } catch (IllegalArgumentException e) {
            // expected
        }
        replicas.add(repEnvInfo[1].getEnv().getNodeName());
        try {
            master.transferMaster(replicas, 1, null);
            fail("null TimeUnit should be rejected");
        } catch (IllegalArgumentException e) {
            // expected
        }
        for (int t = 0; t >= -2; t -= 2) {
            try {
                master.transferMaster(replicas, t, TimeUnit.SECONDS);
                fail("bogus timeout value " + t + " should be rejected");
            } catch (IllegalArgumentException e) {
                // expected;
            }
        }
        replicas.add("venus");
        try {
            master.transferMaster(replicas, 1, TimeUnit.SECONDS);
            fail("bogus replica name should be rejected");
        } catch (IllegalArgumentException e) {
            // expected
        }

        /* Transfer to monitor */
        final Monitor monitor = createMonitor(100, "monitor100");
        try {
            monitor.register();
            replicas = Collections.singleton("monitor100");
            try {
                master.transferMaster(replicas, 1, TimeUnit.SECONDS);
                fail("Operation should fail if invoked on a monitor");
            } catch (IllegalArgumentException e) {
                /* Expected */
            }
        } finally {
            monitor.shutdown();
        }

        /* Try to transfer to secondary */
        repEnvInfo = RepTestUtils.setupExtendEnvInfo(repEnvInfo, 1);
        final RepEnvInfo secondaryInfo = repEnvInfo[repEnvInfo.length - 1];
        secondaryInfo.getRepConfig().setNodeType(NodeType.SECONDARY);
        secondaryInfo.openEnv();
        replicas = Collections.singleton(secondaryInfo.getEnv().getNodeName());
        try {
            master.transferMaster(replicas, 1, TimeUnit.SECONDS);
            fail("Transfer to SECONDARY should throw" +
                 " IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            logger.info("Transfer to SECONDARY: " + e);
        }

        replicas = new HashSet<>();
        replicas.add(repEnvInfo[2].getEnv().getNodeName());
        try {
            repEnvInfo[1].getEnv().transferMaster(replicas,
                                                  1, TimeUnit.SECONDS);
            fail("operation should be rejected if invoked on replica");
        } catch (IllegalStateException e) {
            // expected
        }

        /*
         * If the master itself is in the list of candidate targets, the
         * operation is defined to complete immediately, successfully.  This
         * doesn't really quite qualify as a "stupid coding mistake".  But we
         * include it here on the grounds that it's similarly trivial.
         */
        replicas = new HashSet<>();
        for (int i = 0; i < groupSize; i++) {
            replicas.add(repEnvInfo[i].getEnv().getNodeName());
        }
        String result = master.transferMaster(replicas, 1,
                                              TimeUnit.MILLISECONDS);
        assertEquals(master.getNodeName(), result);

        /*
         * Existing master env handle should still remain active.  Check that
         * these operations do not throw exceptions.
         */
        assertTrue(master.isValid());
        Thread.sleep(2000);
        Transaction txn = master.beginTransaction(null, null);
        txn.abort();
    }

    /**
     * Tests recovery from thread interrupt.  Users are not supposed to
     * interrupt their threads when they're in a call to JE, so this should
     * never happen.  Still, we should do something reasonable.
     */
    @Test
    public void testInterruption() throws Exception {
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        RepEnvInfo replica = repEnvInfo[1];
        final String replicaName = replica.getEnv().getNodeName();
        final ReplicatedEnvironment masterEnv = master.getEnv();

        replica.closeEnv();
        final int pause = 10;
        final int timeout = (int)TICK * pause * 2;
        Transferer transferer =
            new Transferer(masterEnv, replicaName,
                             timeout, TimeUnit.MILLISECONDS);
        Thread thread = new Thread(transferer);
        thread.start();
        Thread.sleep(TICK * pause);
        thread.interrupt();
        thread.join(5000);
        assertFalse(thread.isAlive());
        assertNull(transferer.getUnexpectedException());
        assertFalse(masterEnv.isValid());

        master.closeEnv();
        restartNodes(master, replica);
    }

    /**
     * Tests interruption of a thread trying a commit, blocked in phase 2.
     * There was previously a bug where an interrupt would cause the thread to
     * be allowed to proceed with the commit.
     */
    @Test
    public void testTxnThreadInterrupt() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        final RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();
        final Database db =
            masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        replica.closeEnv();

        int backlog = 20;
        makeBacklog(masterEnv, db, backlog);
        repEnvInfo[2].closeEnv();

        final Set<Class<? extends Throwable> > expectedException =
            makeSet(UnknownMasterException.class,
                    ThreadInterruptedException.class);
        final int nThreads = 5;
        final TxnGenerator generators[] = new TxnGenerator[nThreads];
        final Thread threads[] = new Thread[nThreads];
        int extraStartKey = (nThreads + 1) * 1000;
        final TxnGenerator generator =
            new TxnGenerator(db, extraStartKey,
                             null, SIMPLE_MAJORITY, expectedException);
        final Thread thread = new Thread(generator);

        /*
         * Halfway through the backlog, start the 5 txn generator threads.
         */
        CyclicBarrier b1 =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     for (int i = 0; i < nThreads; i++) {
                         generators[i] =
                             new TxnGenerator(db, (i + 1) * 1000,
                                              null, SIMPLE_MAJORITY,
                                              expectedException);
                         threads[i] = new Thread(generators[i]);
                         threads[i].start();
                     }
                     try {
                         Thread.sleep(5 * TICK);
                     } catch (InterruptedException ie) {
                         // doesn't happen
                     }
                 }
             });

        /*
         * Once we've acked a couple of new txns (so that we know we're into
         * phase 2, and so commits should be blocked), start an extra txn
         * generator.
         */
        CyclicBarrier b2 =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     try {
                         /* Wait a bit to make sure node 2's acks get
                          * processed. */
                         Thread.sleep(2 * TICK);
                         thread.start();
                         Thread.sleep(4 * TICK);
                     } catch (InterruptedException ie) {
                         // shouldn't happen
                     }
                 }
             });

        CountDownLatch latch = new CountDownLatch(1);
        BarrierPacer pacer = new BarrierPacer(b1, b2, latch);
        Replica.setInitialReplayHook(pacer);
        replica.openEnv(NO_CONSISTENCY);

        Transferer mt = new Transferer(masterEnv, 60, replicaName);
        Thread transferThread = new Thread(mt);
        transferThread.start();
        Thread.sleep(5 * TICK);

        b1.await();
        b2.await();

        /*
         * Interrupt the txn generating thread; then finish the transfer and
         * make sure the thread was not allowed to proceed to a successful
         * commit.
         */
        logger.info("interrupt the extra thread");
        thread.interrupt();
        logger.info("allow the xfer to complete");
        latch.countDown();

        transferThread.join(10000);
        assertFalse(transferThread.isAlive());
        assertFalse(mt.didItFail());
        assertNull(mt.getUnexpectedException());
        assertEquals(replicaName, mt.getResult());
        RepTestUtils.awaitCondition(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return !master.getEnv().isValid();
            }}, 10000);
        try {
            db.close();
        } catch (DatabaseException de) {
            // ignored
        }

        for (int i = 0; i < nThreads; i++) {
            threads[i].join(5000);
            assertFalse(threads[i].isAlive());
            assertNull(generators[i].getUnexpectedException());
        }

        /*
         * The master was invalidated, the environment must be closed and
         * reopened.
         */
        master.closeEnv();
        master.openEnv();

        thread.join(5000);
        assertFalse(thread.isAlive());
        assertEquals(0, generator.getCommitCount());
        Throwable xcp = generator.getExpectedException();
        assertNotNull(xcp);
        assertTrue(xcp instanceof ThreadInterruptedException);

        Database db2 =
            replica.getEnv().openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db2);
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        IntegerBinding.intToEntry(extraStartKey, key1);
        OperationStatus ret = db2.get(null, key1, value, null);
        assertEquals(NOTFOUND, ret);
        db2.close();
    }

    /**
     * Closes the master env handle while a locally invoked Master Transfer
     * operation is in progress, and transaction threads are blocked in
     * commit.  Applications are not supposed to close the env handle while
     * other operations are in progress, so this shouldn't happen, so it's OK
     * if the result is a bit ugly.  But even in this case, it should
     * eventually be possible to get resources cleaned up enough so as to be
     * able to open a fresh env handle and start over again.
     */
    @Test
    public void testEnvCloseLocal() throws Exception {
        RepEnvInfo master = repEnvInfo[0];
        /* Disable ugly System.out warning messages. */
        master.getEnvConfig().setConfigParam
            (EnvironmentParams.ENV_CHECK_LEAKS.getName(), "false");

        ensureAssertions();
        long ackTimeout = 20 * TICK;    // 20 >= sum of sleep durations below
        bePatient(ackTimeout);
        createGroup();
        ReplicatedEnvironment masterEnv = master.getEnv();
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();
        final Database db =
            masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        replica.closeEnv();

        int backlog = 20;
        makeBacklog(masterEnv, db, backlog);
        repEnvInfo[2].closeEnv();

        final Set<Class<? extends Throwable> > expectedException =
            new HashSet< >();
        expectedException.add(ThreadInterruptedException.class);
        expectedException.add(NullPointerException.class);
        expectedException.add(InsufficientReplicasException.class);
        expectedException.add(InsufficientAcksException.class);
        expectedException.add(UnknownMasterException.class);
        expectedException.add(EnvironmentFailureException.class);
        final int nThreads = 5;
        final TxnGenerator generators[] = new TxnGenerator[nThreads];
        final Thread threads[] = new Thread[nThreads];
        int extraStartKey = (nThreads + 1) * 1000;
        final TxnGenerator generator =
            new TxnGenerator(db, extraStartKey,
                             null, SIMPLE_MAJORITY, expectedException);
        final Thread thread = new Thread(generator);

        /*
         * Halfway through the backlog, start the 5 txn generator threads.
         */
        CyclicBarrier b1 =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     for (int i = 0; i < nThreads; i++) {
                         generators[i] =
                             new TxnGenerator(db, (i + 1) * 1000,
                                              null, SIMPLE_MAJORITY,
                                              expectedException);
                         threads[i] = new Thread(generators[i]);
                         threads[i].start();
                     }
                     try {
                         Thread.sleep(5 * TICK);
                     } catch (InterruptedException ie) {
                         // doesn't happen
                     }
                 }
             });

        /*
         * Once we've acked a couple of new txns (so that we know we're into
         * phase 2, and so commits should be blocked), start an extra txn
         * generator.
         */
        CyclicBarrier b2 =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     try {
                         /* Wait a bit to make sure node 2's acks get
                          * processed. */
                         Thread.sleep(2 * TICK);
                         thread.start();
                         Thread.sleep(4 * TICK);
                     } catch (InterruptedException ie) {
                         // shouldn't happen
                     }
                 }
             });

        CountDownLatch latch = new CountDownLatch(1);
        BarrierPacer pacer = new BarrierPacer(b1, b2, latch);
        Replica.setInitialReplayHook(pacer);
        replica.openEnv(NO_CONSISTENCY);

        Transferer mt = new Transferer(masterEnv, 60, replicaName);
        Thread transferThread = new Thread(mt);
        transferThread.start();
        Thread.sleep(5 * TICK);

        b1.await();
        b2.await();

        try {
            master.closeEnv();
        } catch (DatabaseException de) {
            // expected
        }
        thread.join(10000);
        assertFalse(thread.isAlive());
        assertEquals(0, generator.getCommitCount());

        transferThread.join(10000);
        assertFalse(transferThread.isAlive());
        assertTrue(mt.didItFail());
        assertNull(mt.getUnexpectedException());

        latch.countDown();
        for (Thread t : threads) {
            t.join(60000);
            assertFalse(t.isAlive());
        }

        /* As usual, clean up, to placate tearDown(). */
        restartNodes(master, repEnvInfo[2]);
    }

    @Test
    public void testOverlappingAttempts() throws Exception {
        createGroup();

        // shut down Node 2
        // start a thread to transfer to node 2, with 20 second timeout?
        // start another thread to sleep a bit, and then try a transfer
        // it should fail, verify that
        // once that happens, start up Node 2
        // verify that the original transfer completed successfully
        //
        RepEnvInfo master = repEnvInfo[0];
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();
        replica.closeEnv();
        Transferer xfr1 = new Transferer(0, master.getEnv(),
                                         replicaName, 30, false);
        Thread t1 = new Thread(xfr1);
        t1.start();

        /*
         * This allows 10 seconds to make sure first thread has already started
         * the operation.
         */
        Transferer xfr2 = new Transferer(10, master.getEnv(),
                                         replicaName, 180, false);
        Thread t2 = new Thread(xfr2);
        t2.start();

        /*
         * Wait for the second thread, which should fail as soon as it tries
         * the operation.
         */
        t2.join(200 * 1000);
        assertFalse(t2.isAlive());
        assertTrue(xfr2.didItFail());
        assertNull(xfr2.getUnexpectedException());

        /*
         * Now start the target replica, which should cause the original
         * transfer to complete right away.
         */
         replica.openEnv();
         t1.join(5 * 1000);
         assertFalse(t1.isAlive());
         assertFalse(xfr1.didItFail());
         assertNull(xfr1.getUnexpectedException());
         awaitSettle(master, replica);
         master.closeEnv();
         master.openEnv();

         // part 2:
         // now the master is Node 2; shut down Node 3
         // start a thread to transfer to node 3 ("futile")
         // start another thread to sleep a bit, and then try transfer with
         // force, but to node 1!
         // verify that first thread got an exception
         // verify that second thread completed successfully, and that the
         // master indeed is now at Node 1.
         // (maybe we can do the second "forcing" attempt in our main thread
         // here)
         master = repEnvInfo[1];
         RepEnvInfo futile = repEnvInfo[2];
         replica = repEnvInfo[0];
         futile.closeEnv();

         xfr1 = new Transferer(0, master.getEnv(),
                               futile.getRepConfig().getNodeName(),
                               180, false);
         t1 = new Thread(xfr1);
         t1.start();
         xfr2 = new Transferer(10, master.getEnv(),
                               replica.getEnv().getNodeName(), 10, true);
         t2 = new Thread(xfr2);
         t2.start();
         t2.join(30 * 1000);
         assertFalse(t2.isAlive());
         assertFalse(xfr2.didItFail());
         assertNull(xfr2.getUnexpectedException());

         t1.join(5 * 1000);
         assertFalse(t1.isAlive());
         assertTrue(xfr1.didItFail());
         assertNull(xfr1.getUnexpectedException());
         awaitSettle(master, replica);

         /* Clean everything up as usual, to satisfy tearDown(). */
         master.closeEnv();
         master.openEnv();
         futile.openEnv();
    }

    /**
     * Tests a scenario in which completion of both phase 1 and phase 2 must be
     * triggered by acks to fresh txns from the replica.  In order to be sure
     * this is happening, we use a {@code TestHook} to slow down commit
     * processing at the replica to a pace of one per second.
     */
    @Test
    public void testConcurrentTxns() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        int nRecords = 5;             // arbitrary
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        populateDB(masterEnv, nRecords);
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);

        /* This replica will be the target of the transfer. */
        RepEnvInfo replica = repEnvInfo[1];
        replica.closeEnv();

        /* The replica will have to "catch up" with 20 transactions. */
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        long arbitraryData = 1732;
        LongBinding.longToEntry(arbitraryData, value);
        int backlog = 20;
        for (int i = 0; i < backlog; i++) {
            IntegerBinding.intToEntry(i, key1);
            db.put(null, key1, value);
        }

        /*
         * Shut down other replica, so that master will be forced to await
         * acks from our target replica.
         */
        repEnvInfo[2].closeEnv();

        /*
         * Create and install a test hook that throttles the rate of commit/ack
         * processing at the replica.
         */
        int maxLag = backlog / 2;
        Semaphore throttle = new Semaphore(maxLag);
        throttle.drainPermits();
        int initialTxns = backlog - maxLag;
        AckPacer pacer = new AckPacer(throttle, initialTxns, maxLag);
        Replica.setInitialReplayHook(pacer);

        int nThreads = 5;
        Thread threads[] = new Thread[nThreads];
        ThrottlingGenerator generators[] = new ThrottlingGenerator[nThreads];
        for (int i = 0; i < nThreads; i++) {
            generators[i] = new ThrottlingGenerator(db, throttle,
                                                    10000 * (i + 1));
            threads[i] = new Thread(generators[i]);
            threads[i].start();
        }
        logger.info("restart the replica, with a Replay hook installed");
        replica.openEnv();

        /* Reset, so as not to affect future env restarts. */
        Replica.setInitialReplayHook(null);

        logger.info("start Master Transfer with 60-second timeout " +
                    "(expected duration ~ 30 sec)");
        Set<String> target = new HashSet<>();
        String replicaName = replica.getEnv().getNodeName();
        target.add(replicaName);

        String result = masterEnv.transferMaster(target, 60, TimeUnit.SECONDS);
        assertEquals(replicaName, result);

        logger.info("transfer succeeded, now waiting (max 5 sec) " +
                "for old master to become replica");

        /* 5 seconds should be plenty of time for everything to settle down. */
        /* The replica will have become the new master. */
        awaitSettle(master, replica);

        logger.info("Master and replica have settled");

        /*
         * All the threads that are mimicking application transactions should
         * be finished. If we're unlucky, a thread could be waiting in a
         * post-commit hook for insufficient acks, having miraculously dodged
         * the commit block.  Since the ack timeout is rather long, we might
         * have to wait quite some time for this thread to finish. Fortunately
         * that doesn't happen often.
         */
        for (Thread t : threads) {
            t.join(65000);
            assertFalse(t.isAlive());
        }

        db.close();
        restartNodes(repEnvInfo[2]);

        /*
         * Examine each transaction generator to see that it terminated as
         * expected, and to find out how much it succeeded in writing.  Verify
         * that the expected records indeed survive at the new master.
         */
        key1 = new DatabaseEntry();
        value = new DatabaseEntry();
        ReplicatedEnvironment newMaster = replica.getEnv();
        db = newMaster.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        for (int i = 0; i < nThreads; i++) {
            Throwable unexpected = generators[i].getUnexpectedException();
            assertNull("" + unexpected, unexpected);
            int keyNumber = generators[i].getStartKey();
            int count = generators[i].getCommitCount();
            while (count-- > 0) {
                IntegerBinding.intToEntry(keyNumber++, key1);
                OperationStatus ret = db.get(null, key1, value, null);
                assertEquals(SUCCESS, ret);
            }
            /* Make sure no extra record snuck in there. */
            IntegerBinding.intToEntry(keyNumber, key1);
            OperationStatus ret = db.get(null, key1, value, null);
            assertEquals(NOTFOUND, ret);
        }
        db.close();
    }

    /*
     * Assertions must be enabled for many of these tests, because the test
     * hook is executed as a side effect in an assert statement.
     */
    private void ensureAssertions() throws Exception {
        boolean assertionsEnabled = false;
        assert (assertionsEnabled = true);
        if (!assertionsEnabled) {
            throw new Exception("this test requires assertions be enabled");
        }
    }

    /**
     * Sets a very long ack timeout, so that our pending commits remain in
     * place while the Master Transfer operation waits, even when we've
     * deliberately slowed things down so as to control the relative timing of
     * events.
     */
    private void bePatient() {
        bePatient(60000);
    }

    private void bePatient(long timeout) {
        for (int i = 0; i < groupSize; i++) {
            repEnvInfo[i].getRepConfig().
                setReplicaAckTimeout(timeout, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Tests transfer operations when the last stuff in the log is txn aborts.
     */
    @Test
    public void testLogEndAbort() throws Exception {
        ensureAssertions();
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);

        RepEnvInfo replica = repEnvInfo[1];
        replica.closeEnv();

        /* Generate a backlog of txns while the replica is down. */
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        long arbitraryData = 1732;
        LongBinding.longToEntry(arbitraryData, value);
        int n = 20;
        for (int i = 0; i < n; i++) {
            IntegerBinding.intToEntry(i, key1);
            db.put(null, key1, value);
        }
        for (int i = n; i < 2 * n; i++) {
            Transaction t = masterEnv.beginTransaction(null, null);
            try {
                IntegerBinding.intToEntry(i, key1);
                db.put(t, key1, value);
                t.abort();
            } finally {
                safeAbort(t);
            }
        }
        db.close();

        restartNodes(replica);

        /*
         * First try the simple case of a transfer when the replica has already
         * caught up.
         */
        Set<String> target = new HashSet<>();
        String replicaName = replica.getEnv().getNodeName();
        target.add(replicaName);
        String result = masterEnv.transferMaster(target, 10, TimeUnit.SECONDS);

        awaitSettle(master, replica);
        master.closeEnv();
        master.openEnv();

        /*
         * Now try the case where the replica is catching up at the time we
         * start the transfer operation.  To do so, install a test hook that
         * will force the replica to take a long time to process each commit or
         * abort.
         */
        replica = repEnvInfo[2];
        replica.closeEnv();
        /* Do some more txns, to create a backlog again. */
        master = repEnvInfo[1];
        masterEnv = master.getEnv();
        db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        key1 = new DatabaseEntry();
        value = new DatabaseEntry();
        arbitraryData = 314159;
        LongBinding.longToEntry(arbitraryData, value);
        n = 20;
        for (int i = 0; i < n; i++) {
            IntegerBinding.intToEntry(i, key1);
            db.put(null, key1, value);
        }
        for (int i = n; i < 2 * n; i++) {
            Transaction t = masterEnv.beginTransaction(null, null);
            try {
                IntegerBinding.intToEntry(i, key1);
                db.put(t, key1, value);
                t.abort();
            } finally {
                safeAbort(t);
            }
        }
        db.close();

        logger.info("set replay hook");
        Replica.setInitialReplayHook
            (new TestHookAdapter<Message>() {
                @Override
                public void doHook(Message m) {
                    if (m.getOp() != Protocol.SHUTDOWN_REQUEST &&
                        m.getOp() != Protocol.HEARTBEAT) {
                        Protocol.Entry e = (Protocol.Entry) m;
                        final InputWireRecord record = e.getWireRecord();
                        final byte entryType = record.getEntryType();
                        if (LOG_TXN_ABORT.equalsType(entryType)) {
                            try {
                                Thread.sleep(TICK);
                            } catch (InterruptedException ie) {
                                // doesn't happen
                            }
                        }
                    }
                }
            });
        replica.openEnv();
        Replica.setInitialReplayHook(null);

        logger.info("start Master Transfer with 60-second timeout " +
                    "(expected duration ~ 20 sec)");
        target = new HashSet<>();
        replicaName = replica.getEnv().getNodeName();
        target.add(replicaName);

        result = masterEnv.transferMaster(target, 60, TimeUnit.SECONDS);
        assertEquals(replicaName, result);
        awaitSettle(master, replica);

        master.closeEnv();
        master.openEnv();
    }

    /**
     * Wait for the former master to become a replica, and the new master to
     * get into the "MASTER" state.
     */
    public static void awaitSettle(final RepEnvInfo oldMaster, // former master
                            final RepEnvInfo oldReplica) // upcoming master
        throws Exception {

        try {
            RepTestUtils.awaitCondition(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    return
                        State.MASTER.equals(oldReplica.getEnv().getState()) &&
                        State.REPLICA.equals(oldMaster.getEnv().getState());
                }
            }, 10000);
        } catch (Throwable e) {
            throw new RuntimeException(
                "Failed waiting for mastership change" +
                " oldReplica.state=" + oldReplica.getEnv().getState() +
                " oldMaster.state=" + oldMaster.getEnv().getState(),
                e);
        }
    }

    /**
     * Tests a second Master Transfer attempt, after a previous attempt has
     * timed out.
     */
    @Test
    public void testAnotherTryAfterFailure() throws Exception {
        createGroup();

        RepEnvInfo replica = repEnvInfo[2];
        String replicaName = replica.getEnv().getNodeName();
        replica.closeEnv();

        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        makeTxns(masterEnv);

        Transferer xfr = new Transferer(masterEnv, 10, replicaName);
        Thread t = new Thread(xfr);
        t.start();
        makeTxns(masterEnv);
        t.join(20 * 1000);
        assertFalse(t.isAlive());
        assertTrue(xfr.didItFail());
        assertNull(xfr.getUnexpectedException());

        replica.openEnv();
        makeTxns(masterEnv);

        replica = repEnvInfo[1];
        replicaName = replica.getEnv().getNodeName();
        replica.closeEnv();

        xfr = new Transferer(10, masterEnv, replicaName, 20, false);
        t = new Thread(xfr);
        t.start();
        makeTxns(masterEnv);
        replica.openEnv();
        t.join(30 * 1000);
        assertFalse(t.isAlive());
        assertFalse(xfr.didItFail());
        assertNull(xfr.getUnexpectedException());

        master.closeEnv();
        master.openEnv();
    }

    private void makeTxns(Database db, int n) throws Exception {
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            IntegerBinding.intToEntry(i, key1);
            LongBinding.longToEntry(i, value);
            db.put(null, key1, value);
        }
    }

    private void makeTxns(ReplicatedEnvironment env) throws Exception {
        int n = 5;              // arbitrary
        Database db = env.openDatabase(null, TEST_DB_NAME, dbconfig);
        try {
            makeTxns(db, n);
        } finally {
            db.close();
        }
    }

    /**
     * Tests a scenario in which a Master Transfer operation times out while in
     * phase 2, freeing previously blocked transactions to complete
     * successfully, and allowing future transactions to execute freely.
     */
    @Test
    public void testPhase2Timeout() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();
        replica.closeEnv();

        // create a backlog, with node 2 down
        final int backlog = 20;
        makeTxns(db, backlog);

        logger.info("set replay hook");
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final CountDownLatch generatorLatch = new CountDownLatch(1);

        // start 5 txn generator threads.  Need to set large ack timeout
        // Need to add another latch, that the pacer hook trips when it first
        // runs, to let the generator threads know it's OK to start running.
        // (Or maybe it's better to do like the other test, and trip that one
        // half way through the backlog).

        final int nThreads = 5;
        TxnGenerator generators[] = new TxnGenerator[nThreads];
        Thread threads[] = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) {
            generators[i] = new TxnGenerator(db, 1000 * (i + 1),
                                               generatorLatch);
            threads[i] = new Thread(generators[i]);
            threads[i].start();
        }

        // The hook paces commits until it sees one needing an ack (which means
        // we've made it past the backlog.  It then acks only 2 txns (without
        // delay), and then sleeps indefinitely (until we deliberately release
        // it).
        Replica.setInitialReplayHook
            (new TwoAckPacer(backlog, generatorLatch) {
                @Override
                protected void tripOnSecondAck() throws InterruptedException {
                    finishLatch.await();
                }
            });
        replica.openEnv();
        Replica.setInitialReplayHook(null);

        // start the master transfer operation, with timeout
        Set<String> replicas = new HashSet<>();
        replicas.add(replicaName);
        try {
            int timeoutSecs = backlog + backlog/2;
            int timeout = (int)(timeoutSecs * TICK);
            masterEnv.transferMaster(replicas, timeout, TimeUnit.MILLISECONDS);
            fail("expected timeout");
        } catch (MasterTransferFailureException e) {
            // expected
        }
        finishLatch.countDown();

        // join generator threads and make sure they've been able to generate
        // their txns successfully.
        for (Thread t : threads) {
            t.join(10000);
            assertFalse(t.isAlive());
        }
        for (TxnGenerator g : generators) {
            assertNull(g.getUnexpectedException());
        }
        db.close();

        // make sure new txns can work too
        //
        makeTxns(masterEnv);

        // do a transfer to node 3
        replica = repEnvInfo[2];
        replicaName = replica.getEnv().getNodeName();
        replicas.clear();
        replicas.add(replicaName);
        String result =
            masterEnv.transferMaster(replicas, 10, TimeUnit.SECONDS);
        assertEquals(replicaName, result);
        awaitSettle(master, replica);

        master.closeEnv();
        master.openEnv();
    }

    /**
     * Tests the scenario in which one of the candidate replicas establishes
     * phase 2, but then a different replica becomes the winner.
     */
    @Test
    public void testFeederRace() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        Database db = masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);
        final RepEnvInfo node2 = repEnvInfo[1];
        final RepEnvInfo node3 = repEnvInfo[2];
        final String node2Name = node2.getEnv().getNodeName();
        final String node3Name = node3.getEnv().getNodeName();
        node2.closeEnv();
        node3.closeEnv();

        final int backlog = 20;
        makeBacklog(masterEnv, db, backlog);

        /* Restart node2 with a replay-pacing test hook. */
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final CountDownLatch generatorLatch = new CountDownLatch(1);
        final CountDownLatch node3Latch = new CountDownLatch(1);

        // start 5 txn generator threads
        Set<Class<? extends Throwable> > allowedExceptions =
            makeSet(UnknownMasterException.class,
                    InsufficientAcksException.class);
        final int nThreads = 5;
        TxnGenerator generators[] = new TxnGenerator[nThreads];
        Thread threads[] = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) {
            generators[i] = new TxnGenerator(db, 1000 * (i + 1), generatorLatch,
                                             SIMPLE_MAJORITY,
                                             allowedExceptions);
            threads[i] = new Thread(generators[i]);
            threads[i].start();
        }

        Replica.setInitialReplayHook
            (new TwoAckPacer(backlog, generatorLatch) {
                    @Override
                    protected void tripOnSecondAck()
                        throws InterruptedException {

                        logger.info("long sleep after 2nd ack");
                        Thread.sleep(5 * TICK);
                        logger.info("will start node3");
                        node3Latch.countDown();
                        finishLatch.await();
                    }
                });
        logger.info("restart node2 with replay hook in place");
        node2.openEnv();
        Replica.setInitialReplayHook(null);

        class NodeStarter implements Runnable {
            private Throwable exception;
            @Override
            public void run() {
                try {
                    node3Latch.await();
                    restartNodes(node3);
                } catch (Throwable t) {
                    t.printStackTrace();
                    exception = t;
                }
            }

            Throwable getUnexpectedException() {
                return exception;
            }
        }

        // start a thread to start node3
        NodeStarter nodeStarter = new NodeStarter();
        Thread nodeStarterThread = new Thread(nodeStarter);
        nodeStarterThread.start();

        // do a master transfer targeting node2 and node3
        Set<String> replicas = new HashSet<>();
        replicas.add(node2Name);
        replicas.add(node3Name);

        /*
         * Account for the time the pacer (q.v.) will introduce, plus a little
         * extra just in case.
         */
        int timeoutSecs = backlog + (2 * 5);
        int timeout = (int)(timeoutSecs * TICK);
        int extra = 5000;
        timeout += extra;
        String result = masterEnv.transferMaster(replicas,
                                                 timeout,
                                                 TimeUnit.MILLISECONDS);

        /*
         * Most of the work of this node-starter thread must have already been
         * done, if the transfer operation has completed.  But make sure every
         * last bit has completed, so that node 3's repEnvInfo env handle
         * reference is set before proceeding (since we pass it to
         * awaitSettle()).  And we may as well finally now allow node 2 to
         * finish up too, to avoid leaving it in the weird, artificial blocked
         * state.
         */
        nodeStarterThread.join(5000);
        assertFalse(nodeStarterThread.isAlive());
        assertNull(nodeStarter.getUnexpectedException());
        finishLatch.countDown();

        assertEquals(node3Name, result);
        awaitSettle(master, node3);

        // join threads and check for unexpected errors
        for (Thread t : threads) {
            t.join(5000);
            assertFalse(t.isAlive());
        }

        for (TxnGenerator g : generators) {
            assertNull("exception=" + g.getUnexpectedException(),
                       g.getUnexpectedException());
        }

        db.close();
        master.closeEnv();
        master.openEnv();
    }

    private void makeBacklog(Environment env, Database db, int n)
        throws Exception {

        TransactionConfig tc = new TransactionConfig();
        tc.setDurability(new Durability(SyncPolicy.NO_SYNC,
                                        SyncPolicy.NO_SYNC,
                                        ReplicaAckPolicy.NONE));
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        for (int i = 0; i < n; i++) {
            Transaction txn = env.beginTransaction(null, tc);
            try {
                IntegerBinding.intToEntry(i, key1);
                LongBinding.longToEntry(i, value);
                db.put(txn, key1, value);
                txn.commit();
            } finally {
                safeAbort(txn);
            }
        }
    }

    /**
     * Tests the scenario where a Feeder dies after having asserted the end of
     * phase 1.  Case #1: another Feeder has also "caught up" by the time the
     * first Feeder dies.
     */
    @Test
    public void testFeederDeath1() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        final Database db =
            masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);

        final RepEnvInfo node2 = repEnvInfo[1];
        String node2Name = node2.getEnv().getNodeName();
        RepEnvInfo node3 = repEnvInfo[2];
        String node3Name = node3.getEnv().getNodeName();
        node2.closeEnv();
        node3.closeEnv();

        int backlog = 20;
        makeBacklog(masterEnv, db, backlog);

        final int nThreads = 5;
        final TxnGenerator generators[] = new TxnGenerator[nThreads];
        final Thread threads[] = new Thread[nThreads];
        int extraStartKey = (nThreads + 1) * 1000;
        final TxnGenerator generator =
            new TxnGenerator(db, extraStartKey, SIMPLE_MAJORITY);
        final Thread thread = new Thread(generator);

        // get everything ready before we actually start stuff. ...

        /*
         * We'll first run node2, stopping at two rendezvous points: b2a and
         * b2b.  In the first rendezvous (when node 2 has gotten halfway
         * through the 20-txn backlog), we'll start the 5 txn generator
         * threads, and wait a moment for them to get going:
         */
        CyclicBarrier b2a =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     for (int i = 0; i < nThreads; i++) {
                         generators[i] = new TxnGenerator(db, (i + 1) * 1000);
                         threads[i] = new Thread(generators[i]);
                         threads[i].start();
                     }
                     try {
                         Thread.sleep(5 * TICK);
                     } catch (InterruptedException ie) {
                         // doesn't happen
                     }
                 }
             });

        /*
         * At the next rendezvous (when node 2 has acked 2 new txns), we'll
         * start one extra txn generating thread, and turn on node 2's "don't
         * process stream" test flag (in order to prevent it from completing
         * its phase 2).  (Later we'll kill the node.)
         */
        CyclicBarrier b2b =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     try {
                         /* Wait a bit to make sure node 2's acks get
                          * processed. */
                         Thread.sleep(2 * TICK);
                         thread.start();
                         Thread.sleep(4 * TICK);
                         node2.getRepNode().replica().setDontProcessStream();
                     } catch (InterruptedException ie) {
                         // shouldn't happen
                     }
                 }
             });

        /*
         * Finally, we'll start node 3 after all that, have it run until it has
         * acked 2 txns.  Once we've processed that, we'll kill node 2, and
         * then allow node 3 to proceed to the finish.
         */
        final CountDownLatch latch2 = new CountDownLatch(1);
        CyclicBarrier b3 =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     try {
                         Thread.sleep(2 * TICK);

                         /*
                          * Here, we know node 3 has asserted completion of
                          * phase 1, so we're ready to kill node 2.
                          */
                         latch2.countDown();
                         node2.closeEnv();
                         Thread.sleep(5 * TICK);
                     } catch (InterruptedException ie) {
                         // shouldn't happen
                     }
                 }
             });

        /*
         * Preparation is complete; now we can start the real fun.  Start node
         * 2, and start the Master Transfer thread.
         */
        BarrierPacer node2Pacer = new BarrierPacer(b2a, b2b, latch2);
        Replica.setInitialReplayHook(node2Pacer);
        node2.openEnv(NO_CONSISTENCY);

        Transferer mt = new Transferer(masterEnv, 60, node2Name, node3Name);
        Thread transferThread = new Thread(mt);
        transferThread.start();
        Thread.sleep(5 * TICK);

        b2a.await();            // complete 1st rendezvous point
        b2b.await();            // 2nd rendezvous point

        /*
         * Start node 3.  It doesn't need a barrier for the backlog halfway
         * point; just one after the first 2 acks.
         */
        BarrierPacer node3Pacer = new BarrierPacer(null, b3, null);
        Replica.setInitialReplayHook(node3Pacer);
        node3.openEnv(NO_CONSISTENCY);
        Replica.setInitialReplayHook(null);

        b3.await();             // wait for node 3 to "catch up"

        /* Wait for it all to end, and check the results. */
        transferThread.join(10000);
        assertFalse(transferThread.isAlive());
        assertFalse(mt.didItFail());
        assertNull(mt.getUnexpectedException());
        assertEquals(node3Name, mt.getResult());

        awaitSettle(master, node3);
        try {
            db.close();
        } catch (DatabaseException de) {
            // ignored
        }

        for (int i = 0; i < nThreads; i++) {
            threads[i].join(5000);
            assertFalse(threads[i].isAlive());
            assertNull(generators[i].getUnexpectedException());
        }
        master.closeEnv();
        master.openEnv();

        thread.join(5000);
        assertFalse(thread.isAlive());
        assertNull(generator.getUnexpectedException());

        Database db2 =
            node3.getEnv().openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db2);
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        IntegerBinding.intToEntry(extraStartKey, key1);
        OperationStatus ret = db2.get(null, key1, value, null);
        assertEquals(NOTFOUND, ret);
        db2.close();
    }

    /**
     * Tests the scenario where a Feeder dies after having asserted the end of
     * phase 1.  Case #2a: no other Feeder has also caught up at the time of
     * the death, and so we must rescind phase 1 completion and txn blockage;
     * in fact no other Feeder is even running at the time of the death.
     */
    @Test
    public void testFeederDeath2a() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        final Database db =
            masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);

        final RepEnvInfo node2 = repEnvInfo[1];
        String node2Name = node2.getEnv().getNodeName();
        RepEnvInfo node3 = repEnvInfo[2];
        String node3Name = node3.getEnv().getNodeName();
        node2.closeEnv();
        node3.closeEnv();

        int backlog = 20;
        makeBacklog(masterEnv, db, backlog);

        final int nThreads = 5;
        final TxnGenerator generators[] = new TxnGenerator[nThreads];
        final Thread threads[] = new Thread[nThreads];
        int extraStartKey = (nThreads + 1) * 1000;
        final TxnGenerator generator =
            new TxnGenerator(db, extraStartKey, NONE);
        final Thread thread = new Thread(generator);

        /*
         * We'll first run node2, stopping at two rendezvous points: b2a and
         * b2b.  In the first rendezvous (when node 2 has gotten halfway
         * through the 20-txn backlog), we'll start the 5 txn generator
         * threads, and wait a moment for them to get going:
         */
        final Set<Class<? extends Throwable> > allowedExceptions =
            makeSet(UnknownMasterException.class,
                    InsufficientReplicasException.class);
        CyclicBarrier b2a =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     for (int i = 0; i < nThreads; i++) {
                         generators[i] =
                             new TxnGenerator(db, (i + 1) * 1000, null,
                                              SIMPLE_MAJORITY,
                                              allowedExceptions);
                         threads[i] = new Thread(generators[i]);
                         threads[i].start();
                     }
                     try {
                         Thread.sleep(5 * TICK);
                     } catch (InterruptedException ie) {
                         // doesn't happen
                     }
                 }
             });

        /*
         * At the next rendezvous (when node 2 has acked 2 new txns), we'll
         * start one extra txn generating thread, and turn on node 2's "don't
         * process stream" test flag (in order to prevent it from completing
         * its phase 2).  (Later we'll kill the node.)
         */
        CyclicBarrier b2b =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     try {
                         /* Wait a bit to make sure node 2's acks get
                          * processed. */
                         Thread.sleep(2 * TICK);
                         thread.start();
                         Thread.sleep(4 * TICK);
                         node2.getRepNode().replica().setDontProcessStream();
                     } catch (InterruptedException ie) {
                         // shouldn't happen
                     }
                 }
             });

        /*
         * Preparation is complete.  Start node 2, and start the Master
         * Transfer thread.
         */
        BarrierPacer node2Pacer = new BarrierPacer(b2a, b2b, null);
        Replica.setInitialReplayHook(node2Pacer);
        node2.openEnv(NO_CONSISTENCY);
        Replica.setInitialReplayHook(null);

        Transferer mt = new Transferer(masterEnv, 60, node2Name, node3Name);
        Thread transferThread = new Thread(mt);
        transferThread.start();
        Thread.sleep(5 * TICK);

        b2a.await();            // complete 1st rendezvous point
        b2b.await();            // 2nd rendezvous point

        /*
         * Kill node 2; this should rescind the txn block.  Then start node 3,
         * with no test hook.
         */
        node2.closeEnv();
        Thread.sleep(5 * TICK);
        thread.join(5000);
        assertFalse(thread.isAlive());
        assertNull(generator.getUnexpectedException());

        node3.openEnv(NO_CONSISTENCY);

        /* Wait for it all to end, and check the results. */
        transferThread.join(10000);
        assertFalse(transferThread.isAlive());
        assertFalse(mt.didItFail());
        assertNull(mt.getUnexpectedException());
        assertEquals(node3Name, mt.getResult());

        awaitSettle(master, node3);
        try {
            db.close();
        } catch (DatabaseException de) {
            // ignored
        }

        for (int i = 0; i < nThreads; i++) {
            threads[i].join(5000);
            assertFalse(threads[i].isAlive());
            assertNull("Saw exception " +
                       generators[i].getUnexpectedException(),
                       generators[i].getUnexpectedException());
        }
        master.closeEnv();
        master.openEnv();


        Database db2 = node3.getEnv().openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db2);
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        IntegerBinding.intToEntry(extraStartKey, key1);
        OperationStatus ret = db2.get(null, key1, value, null);
        assertEquals(SUCCESS, ret);
        db2.close();
    }

    /**
     * Tests the scenario where a Feeder dies after having asserted the end of
     * phase 1.  Case #2b: no other Feeder has also caught up at the time of
     * the death, and so we must rescind phase 1 completion and txn blockage;
     * though the competing Feeder is at least running, working on its backlog.
     */
    @Test
    public void testFeederDeath2b() throws Exception {
        ensureAssertions();
        bePatient();
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        ReplicatedEnvironment masterEnv = master.getEnv();
        final Database db =
            masterEnv.openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db);

        final RepEnvInfo node2 = repEnvInfo[1];
        String node2Name = node2.getEnv().getNodeName();
        RepEnvInfo node3 = repEnvInfo[2];
        String node3Name = node3.getEnv().getNodeName();
        node2.closeEnv();
        node3.closeEnv();

        int backlog = 20;
        makeBacklog(masterEnv, db, backlog);

        final int nThreads = 5;
        final TxnGenerator generators[] = new TxnGenerator[nThreads];
        final Thread threads[] = new Thread[nThreads];
        int extraStartKey = (nThreads + 1) * 1000;
        final TxnGenerator generator =
            new TxnGenerator(db, extraStartKey, NONE);
        final Thread thread = new Thread(generator);

        /*
         * We'll first start both Feeders, allowing them to get halfway through
         * the 20-txn backlog.  At this first barrier's rendezvous point, we'll
         * then start 5 txn generator threads.
         */
        CyclicBarrier b23a =
            new CyclicBarrier
            (3,
             new Runnable() {
                 @Override
                public void run() {
                     for (int i = 0; i < nThreads; i++) {
                         generators[i] = new TxnGenerator(db, (i + 1) * 1000);
                         threads[i] = new Thread(generators[i]);
                         threads[i].start();
                     }
                     try {
                         Thread.sleep(5 * TICK);
                     } catch (InterruptedException ie) {
                         // doesn't happen
                     }
                 }
             });

        /*
         * At the next rendezvous (when node 2 has acked 2 new txns), we'll
         * start one extra txn generating thread, and turn on node 2's "don't
         * process stream" test flag (in order to prevent it from completing
         * its phase 2).  (Right after that we'll kill the node.)
         */
        CyclicBarrier b2b =
            new CyclicBarrier
            (2,
             new Runnable() {
                 @Override
                public void run() {
                     try {
                         /* Wait a bit to make sure node 2's acks get
                          * processed. */
                         Thread.sleep(2 * TICK);
                         thread.start();
                         Thread.sleep(4 * TICK);
                         node2.getRepNode().replica().setDontProcessStream();
                     } catch (InterruptedException ie) {
                         // shouldn't happen
                     }
                 }
             });

        CountDownLatch latch3 = new CountDownLatch(1);

        /* Preparation is complete. */
        BarrierPacer node2Pacer = new BarrierPacer(b23a, b2b, null);
        Replica.setInitialReplayHook(node2Pacer);
        node2.openEnv(NO_CONSISTENCY);

        BarrierPacer node3Pacer = new BarrierPacer(b23a, null, latch3);
        Replica.setInitialReplayHook(node3Pacer);
        node3.openEnv(NO_CONSISTENCY);
        Replica.setInitialReplayHook(null);

        Transferer mt = new Transferer(masterEnv, 60, node2Name, node3Name);
        Thread transferThread = new Thread(mt);
        transferThread.start();
        Thread.sleep(5 * TICK);

        b23a.await();            // complete 1st rendezvous point
        b2b.await();            // 2nd rendezvous point

        /*
         * Kill node 2; this should rescind the txn block.  Then release node 3,
         * to complete its processing
         */
        node2.closeEnv();
        Thread.sleep(5 * TICK);
        thread.join(5000);
        assertFalse(thread.isAlive());
        assertNull(generator.getUnexpectedException());

        latch3.countDown();

        /* Wait for it all to end, and check the results. */
        transferThread.join(10000);
        assertFalse(transferThread.isAlive());
        assertFalse(mt.didItFail());
        assertNull(mt.getUnexpectedException());
        assertEquals(node3Name, mt.getResult());

        awaitSettle(master, node3);
        try {
            db.close();
        } catch (DatabaseException de) {
            // ignored
        }

        for (int i = 0; i < nThreads; i++) {
            threads[i].join(5000);
            assertFalse(threads[i].isAlive());
            assertNull(generators[i].getUnexpectedException());
        }
        master.closeEnv();
        master.openEnv();

        Database db2 = node3.getEnv().openDatabase(null, TEST_DB_NAME, dbconfig);
        databases.add(db2);
        DatabaseEntry key1 = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        IntegerBinding.intToEntry(extraStartKey, key1);
        OperationStatus ret = db2.get(null, key1, value, null);
        assertEquals(SUCCESS, ret);
        db2.close();
    }

    static class Transferer implements Runnable {
        private final ReplicatedEnvironment env;
        private final Set<String> replicas;
        private final int delay;
        private final int timeout;
        private final TimeUnit units;
        private final boolean force;
        private boolean failed;
        private Throwable unexpectedException;
        private String result;

        Transferer(ReplicatedEnvironment env,
                   int timeout,
                   String ... targetReplicas) {
            this(0, env, makeSet(targetReplicas),
                 timeout, TimeUnit.SECONDS, false);
        }

        static private Set<String> makeSet(String ... items) {
            Set<String> set = new HashSet<>();
            set.addAll(Arrays.asList(items));
            return set;
        }

        Transferer(ReplicatedEnvironment env,
                   String replica,
                   int timeout,
                   TimeUnit units) {
            this(0, env, makeSet(replica), timeout, units, false);
        }

        Transferer(int delay,
                   ReplicatedEnvironment env,
                   String replica,
                   int timeout,
                   boolean flag) {
            this(delay, env, makeSet(replica),
                 timeout, TimeUnit.SECONDS, flag);
        }

        Transferer(int delay,
                   ReplicatedEnvironment env,
                   Set<String> replicas,
                   int timeout,
                   TimeUnit units,
                   boolean force) {
            this.delay = delay;
            this.env = env;
            this.replicas = replicas;
            this.timeout = timeout;
            this.units = units;
            this.force = force;
        }

        @Override
        public void run() {
            try {
                if (delay > 0) {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(delay));
                }
                result = env.transferMaster(replicas, timeout, units, force);
            } catch (MasterTransferFailureException mtfe) {
                failed = true;
            } catch (ThreadInterruptedException tie) {
                // expected in one case, doesn't happen in others
            } catch (Throwable t) {
                unexpectedException = t;
                t.printStackTrace();
            }
        }

        boolean didItFail() {
            return failed;
        }

        String getResult() {
            return result;
        }

        Throwable getUnexpectedException() {
            return unexpectedException;
        }
    }

    /**
     * Tests env close during remote invocation.
     */
    @Test
    public void testEnvCloseRemote() throws Exception {
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        RepEnvInfo replica = repEnvInfo[1];

        String groupName = RepTestUtils.TEST_REP_GROUP_NAME;
        ReplicationConfig rc = master.getRepConfig();
        String host = rc.getNodeHostname();
        int port = rc.getNodePort();
        Set<InetSocketAddress> helpers = new HashSet<>();
        helpers.add(new InetSocketAddress(host, port));
        final ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helpers,
                                      RepTestUtils.readRepNetConfig());
        final Set<String> replicas = new HashSet<>();
        String replicaName = replica.getEnv().getNodeName();
        replicas.add(replicaName);

        class RemoteInvoker implements Runnable {
            private Throwable unexpectedException;
            private boolean failed;

            @Override
            public void run() {
                try {
                    rga.transferMaster(replicas, 60, TimeUnit.SECONDS, false);
                } catch (EnvironmentFailureException efe) {
                    Pattern pat =
                        Pattern.compile(".*shutting down.*");
                    if (pat.matcher(efe.getMessage()).matches()) {
                        failed = true;
                    } else {
                        efe.printStackTrace();
                        unexpectedException = efe;
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    unexpectedException = t;
                }
            }

            boolean didItFail() {
                return failed;
            }

            Throwable getUnexpectedException() {
                return unexpectedException;
            }
        }

        replica.closeEnv();
        RemoteInvoker invoker = new RemoteInvoker();
        Thread thread = new Thread(invoker);
        thread.start();
        Thread.sleep(5 * TICK);
        master.closeEnv();

        thread.join(10000);
        assertFalse(thread.isAlive());
        assertTrue(invoker.didItFail());
        assertNull(invoker.getUnexpectedException());

        restartNodes(master, replica);
    }

    /**
     * Tests remove invocation.
     */
    @Test
    public void testRemoteInvocation() throws Exception {
        createGroup();

        RepEnvInfo master = repEnvInfo[0];
        RepEnvInfo replica = repEnvInfo[1];

        final String groupName = RepTestUtils.TEST_REP_GROUP_NAME;
        ReplicationConfig rc = replica.getRepConfig();
        String host = rc.getNodeHostname();
        int port = rc.getNodePort();
        Set<InetSocketAddress> helpers = new HashSet<>();
        helpers.add(new InetSocketAddress(host, port));
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helpers,
                                      RepTestUtils.readRepNetConfig());
        Set<String> replicas = new HashSet<>();
        String replicaName = replica.getEnv().getNodeName();
        replicas.add(replicaName);
        String result =
            rga.transferMaster(replicas, 10, TimeUnit.SECONDS, false);
        assertEquals(replicaName, result);
        awaitSettle(master, replica);
        master.closeEnv();
        master.openEnv();

        /* Node 2 is the new master. */
        master = repEnvInfo[1];
        replica = repEnvInfo[2];
        replicaName = replica.getEnv().getNodeName();
        replica.closeEnv();
        replicas.clear();
        replicas.add(replicaName);
        try {
            rga.transferMaster(replicas, 5, TimeUnit.SECONDS, false);
            fail("expected timeout failure from remote invocation");
        } catch (MasterTransferFailureException e) {
            // expected
        }

        /* Shut down master, so that only one replica remains running. */
        master.closeEnv();
        replica = repEnvInfo[0];
        rc = replica.getRepConfig();
        host = rc.getNodeHostname();
        port = rc.getNodePort();
        helpers.clear();
        helpers.add(new InetSocketAddress(host, port));
        rga = new ReplicationGroupAdmin(groupName, helpers,
                                        RepTestUtils.readRepNetConfig());
        replicaName = replica.getEnv().getNodeName();
        replicas.clear();
        replicas.add(replicaName);
        try {
            rga.transferMaster(replicas, 5, TimeUnit.SECONDS, false);
            fail("expected unknown-master exception");
        } catch (UnknownMasterException e) {
            // expected
        }

        restartNodes(repEnvInfo[1], repEnvInfo[2]);
    }

    @Test
    public void testCmdLineInvocation() throws Exception {

        PrintStream original = System.out;
        try {
            createGroup();
            RepEnvInfo master = repEnvInfo[0];
            RepEnvInfo replica = repEnvInfo[1];
            String replicaName = replica.getEnv().getNodeName();
            File propertyFile = new File(master.getEnvHome().getPath(),
                                         "je.properties");

            final String groupName = RepTestUtils.TEST_REP_GROUP_NAME;
            ReplicationConfig rc = replica.getRepConfig();
            String helper = rc.getNodeHostPort();
            System.setOut(new PrintStream(new ByteArrayOutputStream()));
            DbGroupAdmin.main("-groupName", groupName,
                              "-helperHosts", helper,
                              "-netProps", propertyFile.getPath(),
                              "-transferMaster", "-force", replicaName, "5 s");
            awaitSettle(master, replica);
            master.closeEnv();
            master.openEnv();

            /* Now Node 2 is the new master. */
            master = repEnvInfo[1];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos, true);
            System.setOut(ps);

            String candidate1 = repEnvInfo[0].getEnv().getNodeName();
            String candidate2 = repEnvInfo[2].getEnv().getNodeName();
            DbGroupAdmin.main("-groupName", groupName,
                              "-helperHosts", helper,
                              "-netProps", propertyFile.getPath(),
                              "-transferMaster",
                              candidate1 + "," + candidate2,
                              "10", "s");
            String output = baos.toString();
            /* Get the text sans line ending. */
            BufferedReader br = new BufferedReader(new StringReader(output));
            String line = br.readLine();
            assertNotNull(line);

            /* "The new master is: node_name" */
            Pattern pat = Pattern.compile(".*: (.*)");
            Matcher m = pat.matcher(line);
            assertTrue(m.matches());
            String result = m.group(1);
            assertTrue(result,
                       candidate1.equals(result) || candidate2.equals(result));
            RepEnvInfo newmaster =
                candidate1.equals(result) ?
                repEnvInfo[0] : repEnvInfo[2];
            awaitSettle(master, newmaster);
            master.closeEnv();
            master.openEnv();
        } finally {
            System.setOut(original);
        }
    }

    /**
     * Checks to see that a "forcing" operation leaves the "in progress" flag
     * set properly, but attempting a third (non-forcing) operation.
     */
    @Test
    public void testInProgressFlag() throws Exception {
        createGroup();
        RepEnvInfo master = repEnvInfo[0];
        RepEnvInfo replica = repEnvInfo[1];
        String replicaName = replica.getEnv().getNodeName();
        ReplicatedEnvironment masterEnv = master.getEnv();
        replica.closeEnv();

        Transferer mt1 =
            new Transferer(0, masterEnv, replicaName, 60, false);
        Thread t1 = new Thread(mt1);
        t1.start();

        /* Give mt1 a chance to get established, then supersede it with mt2. */
        Thread.sleep(5 * TICK);
        Transferer mt2 =
            new Transferer(0, masterEnv, replicaName, 60, true);
        Thread t2 = new Thread(mt2);
        t2.start();

        /*
         * Wait for mt1 to fail.  We've already tested elsewhere that it does.
         * But it's useful to wait for it here just so that we know when mt2
         * has become established.
         */
        t1.join(10000);
        assertFalse(t1.isAlive());
        assertTrue(mt1.didItFail());

        /* Here, a non-forcing MT op should be rejected immediately. */
        Transferer mt3 =
            new Transferer(0, masterEnv, replicaName, 3, false);
        mt3.run();
        assertTrue(mt3.didItFail());

        restartNodes(replica);
        t2.join(10000);
        assertFalse(t2.isAlive());
        assertFalse(mt2.didItFail());
        assertNull(mt2.getUnexpectedException());
        assertEquals(replicaName, mt2.getResult());

        /* As always, clean things up to placate our superclass. */
        awaitSettle(master, replica);
        master.closeEnv();
        master.openEnv();
    }

    abstract class Pacer extends TestHookAdapter<Message> {
        @Override
        public void doHook(Message m) {
            if (m.getOp() == Protocol.COMMIT) {
                Protocol.Entry e = (Protocol.Entry) m;
                final InputWireRecord record = e.getWireRecord();
                final byte entryType = record.getEntryType();
                if (LOG_TXN_COMMIT.equalsType(entryType)) {
                    Protocol.Commit commitEntry = (Protocol.Commit) e;
                    final boolean needsAck = commitEntry.getNeedsAck();
                    processCommit(needsAck);
                }
            }
        }
        protected abstract void processCommit(boolean ackable);
    }

    class AckPacer extends Pacer {
        private final Semaphore throttle;
        private final int initialTxns;
        private final int maxLag;
        private int count = 0;
        private boolean released = false;

        AckPacer(Semaphore t, int i, int m) {
            throttle = t;
            initialTxns = i;
            maxLag = m;
        }

        // throttle all commits, by making them take 1 second
        // once we've seen half the backlog go by, unleash the master
        // txn-writing threads by releasing all semaphore perms.
        // as we see ack-seeking commits, at the end of the sleeping time,
        // do another semaphore.release

        @Override
        protected void processCommit(boolean ackable) {
            try {
                Thread.sleep(TICK);
            } catch (InterruptedException ie) {
                // doesn't happen
            }
            if (!released) {
                if (++count >= initialTxns) {
                    throttle.release(maxLag);
                    released = true;
                }
            } else {
                if (ackable) {
                    throttle.release();
                }
            }
        }
    }

    /*
     * When two barriers are supplied, there are two phases to the processing
     * before a latch, if supplied.  If the first but not the second barrier is
     * supplied, along with a latch, then the waiting on the latch occurs on
     * completion of the first (only) phase.
     */
    class BarrierPacer extends Pacer {
        private final CyclicBarrier barrier1, barrier2;
        private final CountDownLatch latch;
        private int commitCount;
        private int ackCount;
        private int phase = 1;

        BarrierPacer(CyclicBarrier b1, CyclicBarrier b2, CountDownLatch l) {
            barrier1 = b1;
            barrier2 = b2;
            latch = l;
        }

        @Override
        protected void processCommit(boolean ackable) {
            try {
                switch (phase) {
                case 1:
                    if (++commitCount > 10) {
                        if (barrier1 != null) {
                            barrier1.await();
                        }
                        if (barrier2 == null && latch != null) {
                            latch.await();
                            phase = 3;
                        } else {
                            phase = 2;
                        }
                    }
                    break;
                case 2:
                    if (ackable && ++ackCount > 2) {
                        if (barrier2 != null) {
                            barrier2.await();
                        }
                        phase = 3;
                        if (latch != null) {
                            latch.await();
                        }
                    }
                    break;
                case 3:
                    break;
                }
            } catch (InterruptedException ie) {
                // shouldn't happen
            } catch (BrokenBarrierException bb) {
                // shouldn't happen
            }
        }
    }

    /**
     * An ack pacer that trips a latch when it's halfway through an initial
     * backlog, and then performs some specified action after it has seen and
     * acked the 2nd "live" txn.
     */
    abstract class TwoAckPacer extends Pacer {
        private int ackCount;
        private int txnCount;
        private final int backlog;
        private boolean seenAckable;
        private final CountDownLatch latch;

        TwoAckPacer(int backlog, CountDownLatch latch) {
            this.backlog = backlog;
            this.latch = latch;
        }

        @Override
        protected void processCommit(boolean ackable) {

            /*
             * Once we get half-way through the backlog, release the generator
             * threads to do their work.  (We want to make sure our hook is
             * installed, and the Feeder connected, before doing so.)
             */
            if (++txnCount >= backlog / 2) {
                latch.countDown();
            }
            if (ackable) {
                seenAckable = true;
            }
            try {
                if (seenAckable) {

                    /*
                     * Ack the first 2 "live" txns without delay; after that
                     * do whatever the subclass implements.
                     */
                    logger.info("seeing live txn");
                    if (++ackCount > 2) {
                        tripOnSecondAck();
                    }
                } else {
                    Thread.sleep(TICK);
                }
            } catch (InterruptedException ie) {
                // doesn't happen
            }
        }

        abstract protected void tripOnSecondAck() throws InterruptedException;
    }

    static class ThrottlingGenerator extends AbstractTxnGenerator {
        private final Semaphore throttle;

        ThrottlingGenerator(Database db, Semaphore throttle, int start) {

            /*
             * Insufficient Acks is rare, but it can occasionally happen during
             * the throes of everything settling down at the end of a transfer.
             */
            super(db, start, SIMPLE_MAJORITY,
                  makeSet(UnknownMasterException.class,
                          InsufficientAcksException.class));
            this.throttle = throttle;
        }

        @Override
        protected void throttle() throws InterruptedException {
            throttle.acquire();
        }
    }

    static class TxnGenerator extends AbstractTxnGenerator {
        private final CountDownLatch latch;

        TxnGenerator(Database db, int startKey) {
            this(db, startKey, SIMPLE_MAJORITY);
        }

        TxnGenerator(Database db, int startKey, ReplicaAckPolicy policy) {
            this(db, startKey, null, policy,
                 makeSet(UnknownMasterException.class));
        }

        TxnGenerator(Database db, int startKey, CountDownLatch latch) {
            this(db, startKey, latch, SIMPLE_MAJORITY,
                 makeSet(UnknownMasterException.class));
        }

        TxnGenerator(Database db,
                     int startKey,
                     CountDownLatch latch,
                     ReplicaAckPolicy policy,
                     Set<Class<? extends Throwable> > allowableExceptionTypes) {
            super(db, startKey, policy, allowableExceptionTypes);
            this.latch = latch == null ?
                         new CountDownLatch(0) : latch;
        }

        @Override
        protected void throttle() throws InterruptedException {
            latch.await();
        }
    }

    abstract static class AbstractTxnGenerator implements Runnable {
        private final Database db;
        private Throwable unexpectedException;
        private Throwable expectedException;
        private final int startKey;
        private final ReplicaAckPolicy policy;
        private int commitCount;
        private final Set<Class<? extends Throwable> > allowableExceptionTypes;

        AbstractTxnGenerator(Database db,
                             int startKey,
                             ReplicaAckPolicy policy,
                             Set<Class<? extends Throwable> >
                                 allowableExceptionTypes) {

            this.db = db;
            this.startKey = startKey;
            this.allowableExceptionTypes = allowableExceptionTypes;
            this.policy = policy;

            /*
             * Experiment to see if tests start passing when
             * IllegalStateException is allowed.
             */
            this.allowableExceptionTypes.add(IllegalStateException.class);
        }

        @Override
        public void run() {
            try {
                TransactionConfig tc = new TransactionConfig();
                tc.setDurability(new Durability(SyncPolicy.NO_SYNC,
                                                SyncPolicy.NO_SYNC,
                                                policy));
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                final int n = 10;
                for (int i = 0; i < n; i++) {
                    throttle();
                    Transaction txn =
                        db.getEnvironment().beginTransaction(null, tc);
                    try {
                        IntegerBinding.intToEntry(startKey + i, key);
                        LongBinding.longToEntry(i, value);
                        db.put(txn, key, value);
                        txn.commit();
                        commitCount++;
                    } finally {
                        safeAbort(txn);
                    }
                }
            } catch (Throwable t) {
                if (allowableExceptionTypes.contains(t.getClass())) {
                    expectedException = t;
                } else {
                    t.printStackTrace();
                    unexpectedException = t;
                }
            }
        }

        abstract protected void throttle() throws InterruptedException;

        Throwable getUnexpectedException() {
            return unexpectedException;
        }

        Throwable getExpectedException() {
            return expectedException;
        }

        int getCommitCount() {
            return commitCount;
        }

        int getStartKey() {
            return startKey;
        }
    }

    @SafeVarargs
    static private Set<Class<? extends Throwable> >
        makeSet(Class<? extends Throwable>... classes) {

        Set<Class<? extends Throwable>> set =
            new HashSet<>();

        Collections.addAll(set, classes);
        return set;
    }

    abstract private static class CheckedHook implements TestHook<Integer> {
        final Transaction txn;
        final Class<?> expectedException;
        final Transaction.State expectedState;
        final int triggerCount;
        public String problem;
        public boolean ran;
        public final CountDownLatch done;
        protected final String description;

        CheckedHook(int triggerCount,
                    Transaction txn,
                    Class<?> expectedException,
                    Transaction.State expectedState,
                    CountDownLatch done,
                    String description) {
            this.txn = txn;
            this.expectedException = expectedException;
            this.expectedState = expectedState;
            this.triggerCount = triggerCount;
            this.done = done;
            this.description = description;
        }

        @Override
        public void hookSetup() {
        }

        @Override
        public void doIOHook() throws IOException {
        }

        @Override
        public void doHook() {
        }

        @Override
        public void doHook(Integer val) {
            if (!val.equals(triggerCount)) {
                return;
            }

            Thread t = new Thread() {
               @Override
               public void run() {
                   executeAsyncTask();
               }
            };
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ran = true;
        }

        abstract void executeAsyncTask();

        @Override
        public Integer getHookValue() {
            return 0;
        }

        @Override
        public String toString() {
            return "Txn " + txn.getId() + "/trigger=" + triggerCount +
                " " + description;
        }
    }

    /* Try to commit while frozen */
    private class TxnEndHook extends CheckedHook {

        TxnEndHook(int triggerCount,
                   Transaction txn,
                   Class<?> expectedException,
                   Transaction.State expectedState,
                   CountDownLatch done,
                   String description) {
            super(triggerCount, txn, expectedException, expectedState, done,
                  description);
        }

        @Override
            void executeAsyncTask() {
            try {
                System.err.println("txn" + txn.getId() + " " + description +
                                   " for txnEndHook");
                txn.commit();
                if (expectedException != null) {
                    problem = "txn should not have committed" + txn;
                }
            } catch (Exception e) {
                if (expectedException != null) {
                    if (!e.getClass().equals(expectedException)) {
                        problem = "commit got unexpected " + e;
                    }
                }
            }
            if (!expectedState.equals(txn.getState())) {
                problem = "Saw " + txn.getState() + " instead of " +
                    expectedState;
            }

            done.countDown();
        };
    }

    /* Try to write while frozen */
    private class AdditionalWriteHook extends CheckedHook {
        private final Database db;
        private final int useKey;

        AdditionalWriteHook(int triggerCount,
                            Database db,
                            Transaction txn,
                            Class<?> expectedException,
                            Transaction.State expectedState,
                            CountDownLatch done,
                            int useKey,
                            String description) {
            super(triggerCount, txn, expectedException, expectedState, done,
                  description);
            this.db = db;
            this.useKey = useKey;
        }

        @Override
        void executeAsyncTask() {
            System.err.println("txn" + txn.getId() + " " + description +
                               " for writeHook");
            try {
                DatabaseEntry entry = new DatabaseEntry();
                DatabaseEntry found = new DatabaseEntry();
                IntegerBinding.intToEntry(useKey, entry);
                OperationStatus status = db.get(txn, entry, found, null);
                logger.info("get status = " + status +
                            " useKey = " + useKey);
                db.put(txn, entry, entry);
                ran = true;
                if (expectedException != null) {
                    problem = "txn should not have committed" + txn;
                }
            } catch (Exception e) {
                if (expectedException == null) {
                    problem = "commit got unexpected " + e;
                } else {
                    if (!e.getClass().equals(expectedException)) {
                        problem = "commit got unexpected " + e;
                    }
                }
            }
            if (!expectedState.equals(txn.getState())) {
                problem = "Saw " + txn.getState() + " instead of " +
                    expectedState;
            }
            done.countDown();
        }
    }

    /*
     * Write a record that requires an ack policy of ALL, to ensure that
     * the group is synced up.
     */
    private void allAckWrite(ReplicatedEnvironment masterEnv,
                             Database db,
                             int keyVal) {
        logger.info("Syncing up group with an all-ack write using master Env "
                    + masterEnv.getNodeName());
        Durability all = new Durability(SyncPolicy.NO_SYNC,
                                        SyncPolicy.NO_SYNC,
                                        ReplicaAckPolicy.ALL);
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setDurability(all);
        Transaction txn = masterEnv.beginTransaction(null, txnConfig);
        try {
            DatabaseEntry val = new DatabaseEntry();
            IntegerBinding.intToEntry(keyVal, val);
            db.put(txn, val, val);
            txn.commit();
        } finally {
            safeAbort(txn);
        }
    }
}
