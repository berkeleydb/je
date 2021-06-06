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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.DurabilityQuorum;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class ReplicatedTransactionTest extends TestBase {

    /* Convenience constants depicting variations in durability */
    static private final Durability SYNC_SYNC_ALL =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.SYNC,
                       ReplicaAckPolicy.ALL);

    static private final Durability SYNC_SYNC_QUORUM =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    static private final Durability SYNC_SYNC_NONE =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.SYNC,
                       ReplicaAckPolicy.NONE);

    private final File envRoot;
    /* min group size must be three */
    private final int groupSize = 3;

    /* The replicators used for each test. */
    RepEnvInfo[] repEnvInfo = null;
    DatabaseConfig dbconfig;
    final DatabaseEntry key = new DatabaseEntry(new byte[]{1});
    final DatabaseEntry data = new DatabaseEntry(new byte[]{100});

    public ReplicatedTransactionTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        dbconfig = new DatabaseConfig();
        dbconfig.setAllowCreate(true);
        dbconfig.setTransactional(true);
        dbconfig.setSortedDuplicates(false);

        super.setUp();
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, groupSize,
                                               SYNC_SYNC_ALL);
        /*
         * Avoid commits from group updates, that can result in assertion
         * failures in verifyReplicaStats.
         */
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);
    }

    @Override
    @After
    public void tearDown() {
        try {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @SuppressWarnings("unused")
    /* For future tests */
    private void waitForReplicaConnections(final ReplicatedEnvironment master)
        throws DatabaseException {

        assertTrue(master.getState().isMaster());
        Environment env = master;
        TransactionConfig tc = new TransactionConfig();
        tc.setDurability(SYNC_SYNC_ALL);
        Transaction td = env.beginTransaction(null, tc);
        td.commit();
    }

    @Test
    public void testAutoCommitDatabaseCreation()
        throws UnknownMasterException,
               DatabaseException,
               InterruptedException {

        ReplicatedEnvironment master = repEnvInfo[0].openEnv();
        State status = master.getState();
        assertEquals(status, State.MASTER);
        /* Create via auto txn. */
        Database mdb = master.openDatabase(null, "randomDB", dbconfig);

        /* Replicate the database. */
        ReplicatedEnvironment replica = repEnvInfo[1].openEnv();
        status = replica.getState();
        assertEquals(status, State.REPLICA);
        try {
            Database db = replica.openDatabase(null, "randomDB", dbconfig);
            db.close();
            mdb.close();
        } catch (Exception e) {
            fail("Unexpected exception");
            e.printStackTrace();
        }
        VLSN commitVLSN = RepTestUtils.syncGroupToLastCommit(repEnvInfo, 2);
        RepTestUtils.checkNodeEquality(master, replica, commitVLSN, false);
    }

    @Test
    public void testReadonlyTxnBasic()
        throws DatabaseException {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        final Environment menv = master;
        RepEnvInfo replicaInfo = findAReplica(repEnvInfo);
        createEmptyDB(menv);

        replicaInfo.closeEnv();
        final TransactionConfig mtc = new TransactionConfig();
        mtc.setDurability(SYNC_SYNC_QUORUM);
        final DatabaseEntry keyEntry = new DatabaseEntry();
        IntegerBinding.intToEntry(1, keyEntry);
        long lastTime = 0;
        for (int i=0; i < 100; i++) {
            Transaction mt = menv.beginTransaction(null, mtc);
            Database db = menv.openDatabase(mt, "testDB", dbconfig);
            IntegerBinding.intToEntry(i, keyEntry);
            DatabaseEntry value = new DatabaseEntry();
            lastTime = System.currentTimeMillis();
            LongBinding.longToEntry(lastTime, value);
            db.put(mt, keyEntry, value);
            mt.commit();
            db.close();
        }
        State state = replicaInfo.openEnv().getState();
        /* Slow down the replay on the replica, so the transaction waits. */
        RepImpl repImpl =  RepInternal.getNonNullRepImpl(replicaInfo.getEnv());
        repImpl.getRepNode().replica().setTestDelayMs(1);
        assertEquals(state, State.REPLICA);

        final Environment renv = replicaInfo.getEnv();
        final TransactionConfig rtc = new TransactionConfig();
        /* Ignore the lag */
        rtc.setConsistencyPolicy
            (new TimeConsistencyPolicy(Integer.MAX_VALUE,
                                       TimeUnit.MILLISECONDS, 0, null));

        Transaction rt = renv.beginTransaction(null, rtc);

        Database rdb = renv.openDatabase(rt, "testDB", dbconfig);

        rt.commit();
        /* Consistent within 2ms of master. */
        rtc.setConsistencyPolicy
            (new TimeConsistencyPolicy(2, TimeUnit.MILLISECONDS,
                                       RepTestUtils.MINUTE_MS,
                                       TimeUnit.MILLISECONDS));
        rt = renv.beginTransaction(null, rtc);
        DatabaseEntry val= new DatabaseEntry();
        OperationStatus status =
            rdb.get(rt, keyEntry, val, LockMode.READ_COMMITTED);
        assertEquals(OperationStatus.SUCCESS, status);
        long entryTime = LongBinding.entryToLong(val);
        assertEquals(lastTime, entryTime);
        rt.commit();
        rdb.close();
    }

    /**
     * Tests transaction begin on the master to make sure that the transaction
     * scope is only entered if the current Ack policy can be satisfied.
     */
    @Test
    public void testMasterTxnBegin()
        throws DatabaseException {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);

        final Environment env = master;
        final TransactionConfig tc = new TransactionConfig();

        ExpectNoException noException = new ExpectNoException() {
            @Override
            void test()
                throws DatabaseException {

                t = env.beginTransaction(null, tc);
            }
        };

        class ExpectInsufficientReplicasException
            extends ExpectException<InsufficientReplicasException> {
            final int requiredNodeCount;
            ExpectInsufficientReplicasException(int requiredNodeCount) {
                super(InsufficientReplicasException.class);
                this.requiredNodeCount = requiredNodeCount;
            }
            @Override
            void test()
                throws DatabaseException {

                t = env.beginTransaction(null, tc);
            }
            @Override
            void checkException(InsufficientReplicasException e) {
                assertEquals("Required node count",
                             requiredNodeCount, e.getRequiredNodeCount());
            }
        }

        tc.setDurability(SYNC_SYNC_ALL);
        noException.exec();

        shutdownAReplica(master, repEnvInfo);
        /* Timeout with database exception for Ack all with missing replica. */
        new ExpectInsufficientReplicasException(3).exec();

        tc.setDurability(SYNC_SYNC_QUORUM);
        /* No exception with one less replica since we still have a quorum. */
        noException.exec();

        DurabilityQuorum dq = RepInternal.getNonNullRepImpl(master).
            getRepNode().getDurabilityQuorum();
        final int quorumReplicas = dq.getCurrentRequiredAckCount
            (Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);
        int liveReplicas = groupSize - 2 /* master + shutdown replica */;

        /* Shut them down until we cross the quorum threshold. */
        while (liveReplicas-- >= quorumReplicas) {
            shutdownAReplica(master, repEnvInfo);
        }

        /* Timeout due to lack of quorum. */
        new ExpectInsufficientReplicasException(2).exec();

        /* No Acks -- no worries. */
        tc.setDurability(SYNC_SYNC_NONE);
        noException.exec();
    }

    /**
     * Test auto commit operations. They are all positive tests.
     */
    @Test
    public void testAutoTransactions()
        throws DatabaseException {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        final Environment env = master;
        new ExpectNoException() {
            @Override
            void test()
                throws DatabaseException {

                db = env.openDatabase(null, "testDB", dbconfig);
                db.put(null, key, data);
                DatabaseEntry val = new DatabaseEntry();
                OperationStatus status =
                    db.get(null, key, val, LockMode.READ_COMMITTED);
                assertEquals(OperationStatus.SUCCESS, status);
                assertEquals(data, val);
            }
        }.exec();
    }

    @Test
    public void testReplicaAckPolicy()
        throws UnknownMasterException,
               DatabaseException {

        final ReplicatedEnvironment master =
            RepTestUtils.joinGroup(repEnvInfo);
        final Environment env = master;
        final int repNodes = groupSize - 1;

        createEmptyDB(env);
        resetReplicaStats(repEnvInfo);
        new ExpectNoException() {
            @Override
            void test()
                throws DatabaseException {

                TransactionConfig tc = new TransactionConfig();
                tc.setDurability(SYNC_SYNC_ALL);
                t = env.beginTransaction(null, tc);
                db = env.openDatabase(t, "testDB", dbconfig);
                /* No changes, so it does not call for a replica commit. */
                t.commit(SYNC_SYNC_ALL);
                tc.setDurability(SYNC_SYNC_ALL);
                t = env.beginTransaction(null, tc);
                db.put(t, key, data);
                t.commit(SYNC_SYNC_ALL);
                t = null;
                /* Verify that all the replicas Ack'd the commit and synced. */
                int replicas = verifyReplicaStats(new long[] {1, 1, 1, 0, 0});
                assertEquals(repNodes, replicas);
            }
        }.exec();

        resetReplicaStats(repEnvInfo);

        DurabilityQuorum dq = RepInternal.getNonNullRepImpl(master).
            getRepNode().getDurabilityQuorum();
        final int quorumReplicas = dq.getCurrentRequiredAckCount
            (Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);

        new ExpectNoException() {
            @Override
            void test()
                throws DatabaseException {

                TransactionConfig tc = new TransactionConfig();
                tc.setDurability(SYNC_SYNC_ALL);
                t = env.beginTransaction(null, tc);
                db = env.openDatabase(t, "testDB", dbconfig);
                /* No changes, so it does not call for a replica commit. */
                t.commit(SYNC_SYNC_ALL);
                shutdownAReplica(master, repEnvInfo);
                tc.setDurability(SYNC_SYNC_QUORUM);
                t = env.beginTransaction(null, tc);
                db.put(t, key, data);
                t.commit(SYNC_SYNC_QUORUM);
                t = null;
                /* Verify that the replicas Ack'd the commit and synced. */
                int replicas = verifyReplicaStats(new long[] {1, 1, 1, 0, 0});
                assertTrue(replicas >= quorumReplicas);
            }
        }.exec();

        int liveReplicas = repNodes - 1 /* master + shutdown replica */;

        /* Shut them down until we cross the quorum threshold. */
        while (liveReplicas-- >= quorumReplicas) {
            shutdownAReplica(master, repEnvInfo);
        }

        resetReplicaStats(repEnvInfo);
        new ExpectNoException() {
            @Override
            void test()
                throws DatabaseException {

                TransactionConfig tc = new TransactionConfig();
                tc.setDurability(SYNC_SYNC_NONE);
                t = env.beginTransaction(null, tc);
                db = env.openDatabase(t, "testDB", dbconfig);
                /* No changes, so it does not call for a replica commit */
                t.commit(SYNC_SYNC_NONE);
                tc.setDurability(SYNC_SYNC_NONE);
                t = env.beginTransaction(null, tc);
                db.put(t, key, data);
                t.commit(SYNC_SYNC_NONE);
                t = null;
                /* We did not wait for any acks. */
            }
        }.exec();
    }

    /*
     * Simple test to create a database and make some changes on a master
     * with an explicit commit ACK policy.
     */
    @Test
    public void testReplicaCommitDurability()
        throws UnknownMasterException,
               DatabaseException {

        final ReplicatedEnvironment master =
            RepTestUtils.joinGroup(repEnvInfo);
        final Environment env = master;
        int repNodes = groupSize - 1;
        final Durability[] durabilityTest = new Durability[] {
            new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC,
                           ReplicaAckPolicy.ALL),
            new Durability(SyncPolicy.SYNC, SyncPolicy.NO_SYNC,
                           ReplicaAckPolicy.ALL),
            new Durability(SyncPolicy.SYNC, SyncPolicy.WRITE_NO_SYNC,
                           ReplicaAckPolicy.ALL)
        };

        /* The expected commit statistics, for the above durability config. */
        long[][] statistics = { {1, 1, 1, 0, 0},
                                {1, 1, 0, 1, 0},
                                {1, 1, 0, 0, 1}};
        createEmptyDB(env);
        for (int i=0; i < durabilityTest.length; i++) {
            resetReplicaStats(repEnvInfo);
            final int testNo = i;
            new ExpectNoException() {
                @Override
                void test()
                    throws DatabaseException {

                    t = env.beginTransaction(null, null);
                    db = env.openDatabase(t, "testDB", dbconfig);
                    /* No changes, so it does not call for a replica commit. */
                    t.commit(durabilityTest[testNo]);
                    t = env.beginTransaction(null, null);
                    db.put(t, key, data);

                    /*
                     * A modification requiring acknowledgment from the
                     * replicas.
                     */
                    t.commit(durabilityTest[testNo]);
                    t = null;
                }
            }.exec();
            /* Verify that all the replicas Ack'd the commit and synced. */
            int replicas = verifyReplicaStats(statistics[i]);
            assertEquals(repNodes, replicas);
        }

        /* Verify that the committed value was available on the Replica. */
        RepEnvInfo replicaInfo = findAReplica(repEnvInfo);
        final Environment renv = replicaInfo.getEnv();
        try {
            Transaction rt = renv.beginTransaction(null, null);
            Database replicaDb = renv.openDatabase(rt, "testDB", dbconfig);
            DatabaseEntry val = new DatabaseEntry();
            OperationStatus status =
                replicaDb.get(rt, key, val, LockMode.READ_COMMITTED);
            assertEquals(OperationStatus.SUCCESS, status);
            assertEquals(data, val);
            rt.commit();
            replicaDb.close();
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }

        /* Repeat for a Quorum. */

        resetReplicaStats(repEnvInfo);
        new ExpectNoException() {
            @Override
            void test()
                throws DatabaseException {

                t = env.beginTransaction(null, null);
                db = env.openDatabase(t, "testDB", dbconfig);
                t.commit(SYNC_SYNC_ALL);
                t = env.beginTransaction(null, null);
                shutdownAReplica(master, repEnvInfo);
                db.put(t, key, data);
                t.commit(SYNC_SYNC_QUORUM);
                t = null;
            }
        }.exec();
    }

    /*
     * A very basic test to ensure that "write" operations are disallowed on
     * the replica db.
     */
    /*
     * TODO: need a more comprehensive test enumerating every type of write
     * operation on the Env and database. Is there an easy way to do this?
     */
    @Test
    public void testReplicaReadonlyTransaction()
        throws DatabaseException {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        {   /* Create a database for use in subsequent tests */
            Environment env = master;
            try {
                Transaction t = env.beginTransaction(null, null);
                Database testDb = env.openDatabase(t, "testDB", dbconfig);
                t.commit(SYNC_SYNC_ALL);
                testDb.close();
                assertTrue(true);
            } catch (Throwable e) {
                e.printStackTrace();
                fail("Unexpected exception");
            }
        }

        RepEnvInfo replicaInfo = findAReplica(repEnvInfo);
        final Environment renv = replicaInfo.getEnv();
        new ExpectException<ReplicaWriteException>(ReplicaWriteException.class)
        {
            @Override
            void test()
                throws DatabaseException {

                t = renv.beginTransaction(null, null);
                db = renv.openDatabase(t, "testDB", dbconfig);
                db.put(t, key, data);
            }
        }.exec();

        new ExpectException<ReplicaWriteException>(ReplicaWriteException.class)
        {
            @Override
            void test()
                throws DatabaseException {

                t = renv.beginTransaction(null, null);
                db = renv.openDatabase(t, "testDBRep", dbconfig);
            }
        }.exec();

        /*
         * A delete operation is tested specially below.  At one time a bug
         * in LSN locking would allow a delete on a replica, due to a problem
         * with the uncontended lock optimization (see CursorImpl.lockLN).
         *
         * In one case (encountered in stress testing and reproduced here), a
         * NPE was thrown when logging the deletion, and the VLSNIndex
         * attempted to bump the VLSN.
         *
         * In another case (encountered in McStress), the delete was logged
         * successfully because the node transitioned to Master.  A
         * ReplicaWriteException was thrown after that, which caused an abort,
         * which threw an NPE because the undoDatabases field was not
         * initialized in Txn, because no write lock was taken.
         */
        final Transaction t = master.beginTransaction(null, null);
        final Database testDb = master.openDatabase(t, "testDB", dbconfig);
        final OperationStatus status = testDb.putNoOverwrite(t, key, data);
        assertSame(OperationStatus.SUCCESS, status);
        t.commit(SYNC_SYNC_ALL);
        testDb.close();

        new ExpectException<ReplicaWriteException>(ReplicaWriteException.class)
        {
            @Override
            void test()
                throws DatabaseException {

                t = renv.beginTransaction(null, null);
                db = renv.openDatabase(t, "testDB", dbconfig);
                db.delete(t, key);
            }
        }.exec();
    }

    @Test
    public void testTxnCommitException()
        throws UnknownMasterException,
               DatabaseException {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        Environment env = master;
        TransactionConfig tc = new TransactionConfig();
        tc.setDurability(SYNC_SYNC_ALL);
        Transaction td = env.beginTransaction(null, tc);
        td.commit();
        Database db = null;
        Transaction t = null;
        try {
            t = env.beginTransaction(null, null);
            shutdownAReplica(master, repEnvInfo);
            db = env.openDatabase(t, "testDB", dbconfig);

            /*
             * Should fail with ALL policy in place and a missing replica in
             * the preLogCommitHook.
             */
            t.commit(SYNC_SYNC_ALL);
            fail("expected CommitException");
        } catch (InsufficientReplicasException e) {
            if (t != null) {
                t.abort();
            }
            if (db != null) {
                db.close();
            }
            /* Make sure we get to this point successfully */
            assertTrue(true);
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }
    }

    /* Utility methods below. */

    /*
     * Create an empty database for test purposes.
     */
    private Database createEmptyDB(final Environment env)
        throws DatabaseException {

        ExpectNoException ene =
            new ExpectNoException() {
                @Override
                void test()
                    throws DatabaseException {

                    t = env.beginTransaction(null, null);
                    db = env.openDatabase(t, "testDB", dbconfig);
                    t.commit(SYNC_SYNC_ALL);
                    t = null;
                }
        };
        ene.exec();
        return ene.db;
    }

    /*
     * Shutdown some one replica and wait for the Master to shutdown its
     * associated feeder.
     */
    private ReplicatedEnvironment
        shutdownAReplica(ReplicatedEnvironment master,
                         RepEnvInfo[] replicators)
        throws DatabaseException {

        RepNode masterRepNode =
            RepInternal.getNonNullRepImpl(master).getRepNode();
        int replicaCount =
            masterRepNode.feederManager().activeReplicas().size();
        final RepEnvInfo shutdownReplicaInfo = findAReplica(replicators);
        assertNotNull(shutdownReplicaInfo);
        shutdownReplicaInfo.getEnv().close();

        /* Wait for feeder to recognize it's gone. */
        for (int i=0; i < 60; i++) {
            int currReplicaCount =
                masterRepNode.feederManager().activeReplicas().size();
            if (currReplicaCount == replicaCount) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    fail("unexpected interrupt exception");
                }
            }
        }
        assertTrue
        (masterRepNode.feederManager().activeReplicas().size() < replicaCount);

        return null;
    }

    /**
     * Select from one amongst the active replicas and return it.
     */
    private RepEnvInfo findAReplica(RepEnvInfo[] replicators)
        throws DatabaseException {

        for (RepEnvInfo repi : replicators) {
            ReplicatedEnvironment replicator = repi.getEnv();
            if (!replicator.isValid() ||
                replicator.getState().isMaster()) {
                continue;
            }
            return repi;
        }
        return null;
    }

    /**
     * Resets the statistics associated with a Replica
     * @param replicators
     * @throws DatabaseException
     */
    private void resetReplicaStats(RepEnvInfo[] replicators)
        throws DatabaseException {

        for (RepEnvInfo repi : replicators) {
            ReplicatedEnvironment replicator = repi.getEnv();
            if ((replicator == null) ||
                 !replicator.isValid() ||
                 replicator.getState().isMaster()) {
                continue;
            }
            RepInternal.getNonNullRepImpl(replicator).getReplay().resetStats();
        }
    }

    private int verifyReplicaStats(long[] expected)
        throws DatabaseException {

        int replicas = 0;
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment replicator = repi.getEnv();

            if (!replicator.isValid() ||
                replicator.getState().isMaster()) {
                continue;
            }
            replicas++;
            ReplicatedEnvironmentStats actual =
                replicator.getRepStats(StatsConfig.DEFAULT);
            assertEquals(expected[0], actual.getNReplayCommits());
            assertEquals(expected[1], actual.getNReplayCommitAcks());

            assertEquals(expected[2],
                         actual.getNReplayCommitSyncs() +
                         actual.getNReplayGroupCommitTxns());
            assertEquals(expected[3],
                         actual.getNReplayCommitNoSyncs() -
                         actual.getNReplayGroupCommitTxns());
            assertEquals(expected[4], actual.getNReplayCommitWriteNoSyncs());
        }

        return replicas;
    }

    /*
     * Helper classes for exception testing.
     */
    private abstract class ExpectException<T extends Throwable> {
        private final Class<T> exceptionClass;
        Transaction t = null;
        Database db = null;

        ExpectException(Class<T> exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

        abstract void test() throws Throwable;

        void exec()
            throws DatabaseException {

            try {
                test();
                try {
                    if (t != null) {
                        t.abort();
                    }
                    t = null;
                } catch (Exception ae) {
                    ae.printStackTrace();
                    fail("Spurious exception");
                }
                fail("Exception expected");
            } catch (Throwable th) {
                if (!exceptionClass.isInstance(th)) {
                    th.printStackTrace(System.err);
                    fail("unexpected exception");
                }
                checkException(exceptionClass.cast(th));
            } finally {
                if (t != null) {
                    t.abort();
                }
                if (db != null){
                    db.close();
                }
                t = null;
                db = null;
            }
        }

        void checkException(T th) { }
    }

    private abstract class ExpectNoException {
        Transaction t = null;
        Database db = null;
        abstract void test() throws Throwable;

        void exec()
            throws DatabaseException {

            try {
                test();
                if (t!= null) {
                    t.commit();
                }
                t = null;
            } catch (Throwable th) {
                th.printStackTrace(System.err);
                fail("unexpected exception");
            } finally {
                if (t != null) {
                    t.abort();
                }
                if (db != null){
                    db.close();
                }
            }
        }
    }
}
