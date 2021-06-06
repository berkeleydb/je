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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.SearchFileReader;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.SyncupProgress;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Test that uncommitted, unaborted, replicated transactions are recovered and
 * resurrected at recovery time.
 * - check that this happens both with and without a checkpoint.
 * - check that only replicated transactions are resurrected.
 * - check that rollbacks are honored and are rolled back at recovery.
 */
public class ReplayRecoveryTest extends TestBase {

    private final static boolean verbose = Boolean.getBoolean("verbose");
    private ReplicatedEnvironment master;
    private ReplicatedEnvironment replica;

    /* Replication tests use multiple environments. */
    private final File envRoot;

    public ReplayRecoveryTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        master = null;
        replica = null;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() {
        cleanup();
    }

    /**
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testRBRecoveryOneTxn()
        throws IOException, InterruptedException {

        doRollbackRecovery(new OneTransactionWorkload());
    }

    @Test
    public void testRBRecoveryMultiTxn()
        throws IOException, InterruptedException {

        doRollbackRecovery(new MultiTransactionWorkload());
    }

    @Test
    public void testRBRecoveryPostMatchpointTxn()
        throws IOException, InterruptedException {

        doRollbackRecovery(new PostMatchpointTransaction());
    }

    /**
     * Run two nodes.
     * Crash replica.
     * Restart, recovery, and syncup replica, requiring a partial rollback.
     * Crash replica again. Recover replica, requiring recovery w/partial
     *   rollback
     * Compare master and replica txns.
     */
    private void doRollbackRecovery(Workload workload)
        throws IOException, InterruptedException {

        Logger logger = LoggerUtils.getLoggerFixedPrefix(getClass(),
                                                         "Test");

        RepEnvInfo[] repEnvInfo = null;

        /* Create a 2 node group */
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 2);
        SyncupListenerTester tester = new SyncupListenerTester(repEnvInfo);
        master = RepTestUtils.joinGroup(repEnvInfo);

        /* Do some work */
        Set<Expected> unfinished = workload.doWork(master);

        /* Make sure both nodes are now up to the same VLSN */
        VLSN lastVLSN = RepInternal.getNonNullRepImpl(master).
            getVLSNIndex().getRange().getLast();
        RepTestUtils.syncGroupToVLSN(repEnvInfo, 2, lastVLSN);

        /*
         * Crash the replica and then sync up again. The replica will have a
         * partial rollback in its log from the syncup. Truncate the log so
         * that we remove the replay that has ensued from the syncup. That way,
         * we can test that a later recovery executed a redo of the partial
         * rollback.
         */
        logger.fine("Crash replica");
        RepEnvInfo crashed = crashReplica(repEnvInfo);
        logger.fine("Re-open replica");
        replica = crashed.openEnv();
        EnvironmentImpl replicaImpl = DbInternal.getNonNullEnvImpl(replica);

        long rollbackEndLsn = findRollbackEnd(replicaImpl);
        String fileName = replicaImpl.getFileManager().getFullFileName
            (DbLsn.getFileNumber(rollbackEndLsn), FileManager.JE_SUFFIX);

        /*
         * Bounce the replica again. We want to force a recovery that has to
         * process the RollbackEnd. The recovery is artificially stopped
         * before the replica does a handshake and a syncup, so that we
         * can check what active transactions have been created by recovery.
         */
        logger.fine("Crash replica again");
        crashed.abnormalCloseEnv();
        truncateLog(rollbackEndLsn, fileName);
        logger.fine("Recover with no syncup");
        replica = recoverWithoutSyncup(crashed);

        checkPostRecoveryReplicaTxns(unfinished, true /* checkToMatchpoint */);
        tester.checkForTwoSyncups();
    }

    @Test
    public void testResurrectionOneTxn()
        throws Throwable {

        doResurrection(new OneTransactionWorkload());
    }

    @Test
    public void testResurrectionMultiTxn()
        throws Throwable {

        doResurrection(new MultiTransactionWorkload());
    }

    @Test
    public void testResurrectionPostMatchpointTxn()
        throws Throwable {

        doResurrection(new PostMatchpointTransaction());
    }

    /**
     * Do work in a two node system, crash the replica and examine the
     * resurrected transactions.
     */
    /**
     * Run two nodes.
     * Crash replica.
     * Restart and recover replica.
     * Compare master and replica txns.
     */
    private void doResurrection(Workload workload)
        throws Throwable {

        RepEnvInfo[] repEnvInfo = null;

        /* Create a 2 node group */
        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 2);
        try {
            master = RepTestUtils.joinGroup(repEnvInfo);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        /* Do some work, make sure both nodes see all the work. */
        Set<Expected> unfinished = workload.doWork(master);

        /* Make sure both nodes are now up to the same VLSN */
        VLSN lastVLSN = RepInternal.getNonNullRepImpl(master).
            getVLSNIndex().getRange().getLast();
        RepTestUtils.syncGroupToVLSN(repEnvInfo, 2, lastVLSN);

        /* Crash the replica. */
        RepEnvInfo replicaInfo = crashReplica(repEnvInfo);

        /*
         * Bring up the replica again and check before joining that the
         * resurrected transactions are correct.
         */
        replica = recoverWithoutSyncup(replicaInfo);
        checkPostRecoveryReplicaTxns(unfinished,
                                     false /* checkToMatchpoint */);
    }

    private void checkPostRecoveryReplicaTxns(Set<Expected> unfinished,
                                              boolean rollbackInRecovery) {

        if (verbose) {
            System.out.println("comparing recovered transactions");
        }

        /*
         * Create a new set of the replay txns active on the replica. Make a
         * set because we're going to remove items from the set as part of the
         * verification.
         */
        Map<Long, ReplayTxn> testReplays =
            RepInternal.getNonNullRepImpl(replica).getReplay().
                getActiveTxns().getMap();

        /*
         * Expect the master and the recovered replica to have the same number
         * of replay and unfinished transactions, excluding those that
         * were part of a partial rollback.
         */
        int expectedCount = 0;
        for (Expected e : unfinished) {
            if (rollbackInRecovery && e.absentIfRollback) {
                continue;
            }
            expectedCount++;
        }
        assertEquals("Expected=" + expectedCount +
                     " actual=" + testReplays.size(),
                     expectedCount, testReplays.size());

        for (Expected info : unfinished) {
            if ((rollbackInRecovery) && (info.absentIfRollback)) {
                continue;
            }

            ReplayTxn replayTxn = testReplays.remove(info.transaction.getId());

            /* Check that the id is the same. */
            assertEquals(info.transaction.getId(), replayTxn.getId());

            /*
             * Check that the number of write locks is as expected. If this
             * test is checking partial rollbacks, compare to the matchpoint
             * locks. If not, compare to the locks currently in the master.
             */
            Set<Long> expectedLocks = null;
            Txn unfinishedTxn = DbInternal.getTxn(info.transaction);
            if (rollbackInRecovery) {
                expectedLocks = info.matchpointWriteLockIds;
            } else {
                expectedLocks = copyLocks(unfinishedTxn);
            }

            Set<Long> replayTxnWriteLocks = copyLocks(replayTxn);
            assertTrue(replayTxnWriteLocks.containsAll(expectedLocks));
            assertTrue("replay " + replayTxnWriteLocks +
                       " expected = " + expectedLocks,
                       expectedLocks.containsAll(replayTxnWriteLocks));

            assertEquals(0, replayTxn.getReadLockIds().size());

            if (verbose) {
                System.out.println("Compare " + replayTxn  +
                                   " to " + expectedLocks);
                System.out.println("replayWriteLocks= " +
                                   replayTxnWriteLocks);
            }

            unfinishedTxn.abort();
        }
        assertEquals(testReplays.size() + " txns left in the test set",
                     0, testReplays.size());
    }

    /**
     * Crash the replica node in a two node system.
     * @return the RepEnvInfo for the replica node
     */
    private RepEnvInfo crashReplica(RepEnvInfo[] repEnvInfo) {
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            if (rep.getState().isMaster()) {
                continue;
            }
            repi.abnormalCloseEnv();
            return repi;
        }
        return null;
    }

    /**
     * Recover the replica, but don't let it run syncup.
     */
    private ReplicatedEnvironment recoverWithoutSyncup(RepEnvInfo replicaInfo) {

        EnvironmentConfig replicaEnvConfig = replicaInfo.getEnvConfig();
        replicaEnvConfig.setConfigParam
            (EnvironmentParams.ENV_CHECK_LEAKS.getName(),"false");
        ReplicatedEnvironment rep =
            RepInternal.createDetachedEnv(replicaInfo.getEnvHome(),
                                          replicaInfo.getRepConfig(),
                                          replicaEnvConfig);

        /*
         * After a recovery, the vlsnIndex should have been entirely flushed
         * to disk.
         */
        VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(rep).getVLSNIndex();
        boolean isFlushed = vlsnIndex.isFlushedToDisk();
        if (!isFlushed) {
            vlsnIndex.dumpDb(true);
            fail("VLSNIndex should have been flushed to disk by recovery");
        }

        return rep;
    }

    private long findRollbackEnd(EnvironmentImpl envImpl) {

        /* Ensure that everything is out to disk. */
        FileManager fileManager = envImpl.getFileManager();

        long startLsn = fileManager.getLastUsedLsn();
        long endLsn = fileManager.getNextLsn();
        envImpl.getLogManager().flushSync();

        SearchFileReader searcher =
            new SearchFileReader(envImpl,
                                 10000,
                                 false,                        // forward
                                 startLsn,
                                 endLsn,
                                 LogEntryType.LOG_ROLLBACK_END);

        long targetLsn = 0;
        if (searcher.readNextEntry()) {
            targetLsn = searcher.getLastLsn();
        } else {
            fail("There should be some kind of rollback end in the log.");
        }

        assertTrue(targetLsn != 0);
        long truncateLsn = searcher.getLastEntrySize() + targetLsn;
        return truncateLsn;
    }

    /*
     * Find the last RollbackEnd and truncate the file directly after that.
     */
    private void truncateLog(long lsn, String fileName)
        throws IOException {

        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        long offset = DbLsn.getFileOffset(lsn);
        try {
            file.getChannel().truncate(offset);
        } finally {
            file.close();
        }
    }

    private void cleanup() {
        try {
            if (replica != null) {
                //DbInternal.getNonNullEnvImpl(replica).abnormalClose();
                replica.close();
            }
        } catch (DatabaseException ignore) {
            /* ignore txn close leaks. */
        } finally {
            replica = null;
        }

        try {
            if (master != null) {
                DbInternal.getNonNullEnvImpl(master).abnormalClose();
            }
        } catch (DatabaseException ignore) {
            /* ignore txn close leaks. */
        } finally {
             master = null;
        }
    }

    static abstract class Workload {
        EntityStore store;
        Environment env;

        Set<Expected> unfinished = new HashSet<Expected>();
        PrimaryIndex<Integer, TestData>  testIndex;

        /**
         * @return the set of unfinished transactions after doing work.
         */
        abstract Set<Expected> doWork(ReplicatedEnvironment master)
            throws DatabaseException;

        void setupStore(ReplicatedEnvironment master)
            throws DatabaseException {

            env = master;
            StoreConfig config = new StoreConfig();
            config.setAllowCreate(true);
            config.setTransactional(true);
            try {
                store = new EntityStore(env, "foo", config);
                testIndex =  store.getPrimaryIndex(Integer.class,
                                                   TestData.class);
            } catch (DatabaseException e) {
                if (store != null) {
                    store.close();
                }
            }
        }
    }

    /**
     * One unfinished transaction after the checkpoint.
     */
    static class OneTransactionWorkload extends Workload {

        @Override
        Set<Expected> doWork(ReplicatedEnvironment master)
            throws DatabaseException {

            setupStore(master);

            try {
                Transaction commitTxn = env.beginTransaction(null, null);
                Transaction unfinishedTxn = env.beginTransaction(null, null);

                testIndex.put(commitTxn, new TestData(1));
                testIndex.put(unfinishedTxn, new TestData(2));

                /* This is the matchpoint. */
                commitTxn.commit();
                Set<Long> matchpointLocks = copyLocks(unfinishedTxn);

                /* An insert after the matchpoint. */
                testIndex.put(unfinishedTxn, new TestData(3));

                unfinished.add(new Expected(unfinishedTxn, matchpointLocks));
            } finally {
                if (store != null) {
                    store.close();
                }
            }
            return unfinished;
        }
    }

    /**
     * Multiple unfinished transactions intermingled with the checkpoint and
     * aborts.
     */
    static class MultiTransactionWorkload extends Workload {

        @Override
        Set<Expected> doWork(ReplicatedEnvironment master)
            throws DatabaseException {

            setupStore(master);

            try {

                Transaction unfinishedA = env.beginTransaction(null, null);
                Transaction unfinishedB = env.beginTransaction(null, null);
                Transaction commitA = env.beginTransaction(null, null);
                Transaction commitB = env.beginTransaction(null, null);
                Transaction abortA = env.beginTransaction(null, null);
                Transaction abortB = env.beginTransaction(null, null);

                testIndex.put(unfinishedA, new TestData(1));
                testIndex.put(commitA, new TestData(2));
                commitA.commit();

                testIndex.put(unfinishedA, new TestData(3));
                testIndex.put(abortA, new TestData(4));
                abortA.abort();

                /* checkpoint ! */
                CheckpointConfig config = new CheckpointConfig();
                config.setForce(true);
                env.checkpoint(config);

                testIndex.put(unfinishedB, new TestData(5));


                testIndex.put(commitB, new TestData(6));
                commitB.commit();

                testIndex.put(abortB, new TestData(7));

                /* Matchpoint */
                abortB.abort();
                Set<Long> matchUnALocks = copyLocks(unfinishedA);
                Set<Long> matchUnBLocks = copyLocks(unfinishedB);

                testIndex.put(unfinishedA, new TestData(8));

                unfinished.add(new Expected(unfinishedA, matchUnALocks));
                unfinished.add(new Expected(unfinishedB, matchUnBLocks));

            } finally {
                if (store != null) {
                    store.close();
                }
            }
            return unfinished;
        }
    }

    /**
     * A transaction that is started after the matchpoint. It should be rolled
     * back at syncup, and not recovered. It will be replayed on the replica if
     * the master sends it.
     */
    static class PostMatchpointTransaction extends Workload {

        @Override
        Set<Expected> doWork(ReplicatedEnvironment master)
            throws DatabaseException {

            setupStore(master);

            try {
                Transaction commitTxn = env.beginTransaction(null, null);
                Transaction preMatch = env.beginTransaction(null, null);
                Transaction postMatch = env.beginTransaction(null, null);

                testIndex.put(commitTxn, new TestData(1));
                testIndex.put(preMatch, new TestData(2));

                /* This is the matchpoint. */
                commitTxn.commit();
                Set<Long> matchpointLocks = copyLocks(preMatch);

                testIndex.put(postMatch, new TestData(3));

                /*
                 * We expect the preMatch transaction to be visible after
                 * a non-rollback and a rollback recovery. The postMatch
                 * txn should only be visible in the recovery w/out a rollback
                 * period.
                 */
                unfinished.add(new Expected(preMatch, matchpointLocks));
                unfinished.add(new Expected(postMatch, true));
            } finally {
                if (store != null) {
                    store.close();
                }
            }
            return unfinished;
        }
    }

    private static Set<Long> copyLocks(Transaction trans) {
        return copyLocks(DbInternal.getTxn(trans));
    }

    /**
     * Convert each lock ID from LSN to VLSN to allow comparison of locks on
     * different HA nodes.
     */
    private static Set<Long> copyLocks(Txn txn) {
        final LogManager logManager = txn.getEnvironment().getLogManager();
        final Set<Long> lsns = txn.getWriteLockIds();
        final Set<Long> vlsns = new HashSet<Long>(lsns.size());
        for (long lsn : lsns) {
            final WholeEntry entry;
            try {
                entry = logManager.getLogEntryAllowInvisible(lsn);
            } catch (FileNotFoundException e) {
                throw new RuntimeException
                    ("LSN " + DbLsn.getNoFormatString(lsn) +
                     " may have been cleaned", e);
            }
            VLSN vlsn = entry.getHeader().getVLSN();
            assertNotNull(vlsn);
            vlsns.add(vlsn.getSequence());
        }
        return vlsns;
    }

    private class SyncupListenerTester {
        private final TestListener[] listeners;
        private final RepEnvInfo[] repEnvInfo;

        SyncupListenerTester(RepEnvInfo[] repEnvInfo) {
            this.repEnvInfo = repEnvInfo;
            listeners = new TestListener[repEnvInfo.length];
            for (int i = 0; i < repEnvInfo.length; i++) {
                listeners[i] = new TestListener();
                repEnvInfo[i].getRepConfig().
                    setSyncupProgressListener(listeners[i]);
            }
        }

        public void checkForTwoSyncups() {
            for (int i = 0; i < repEnvInfo.length; i++) {
               if (repEnvInfo[i].isMaster()) {
                   assertEquals(0, listeners[i].phasesSeen.size());
               } else {
                   List<SyncupProgress> seen = listeners[i].phasesSeen;
                   assertEquals(7, seen.size());
                   assertEquals(SyncupProgress.FIND_MATCHPOINT, seen.get(0));
                   assertEquals(SyncupProgress.CHECK_FOR_ROLLBACK, seen.get(1));
                   assertEquals(SyncupProgress.END, seen.get(2));
                   assertEquals(SyncupProgress.FIND_MATCHPOINT, seen.get(3));
                   assertEquals(SyncupProgress.CHECK_FOR_ROLLBACK, seen.get(4));
                   assertEquals(SyncupProgress.DO_ROLLBACK, seen.get(5));
                   assertEquals(SyncupProgress.END, seen.get(6));
               }
            }
        }

        private class TestListener implements ProgressListener<SyncupProgress> {
            List<SyncupProgress> phasesSeen;

            TestListener() {
                phasesSeen = new ArrayList<SyncupProgress>();
            }

            public boolean progress(SyncupProgress phase, long n, long total) {
               phasesSeen.add(phase);
               return true;
            }
        }
    }

    @Entity
    static class TestData {
        @PrimaryKey
        private int id;

        private int stuff;

        @SuppressWarnings("unused")
        private TestData() {
        }

        TestData(int id) {
            this.id = id;
            stuff = 10;
        }

        TestData(int id, int stuff) {
            this.id = id;
            this.stuff = stuff;
        }

        @Override
        public String toString() {
            return "id=" + id + " stuff=" + stuff;
        }
    }

    /**
     * Encapsulate which transactions and write locks are expected at the end
     * of the test.
     */
    private static class Expected {

        /*
         * absentIfRollback is true it this txn would be in a rollback period
         * if it were run in the doRollbackRecovery method. For example,
         * transactions that start after a syncup matchpoint are rolled back,
         * and are not recovered.
         */
        final boolean absentIfRollback;
        final Transaction transaction;
        final Set<Long> matchpointWriteLockIds;

        Expected(Transaction transaction,
                 Set<Long> matchpointWriteLockIds) {
            this.transaction = transaction;
            this.matchpointWriteLockIds = matchpointWriteLockIds;
            absentIfRollback = false;
        }

        Expected(Transaction transaction,
                 boolean absentIfRollback) {
            this.transaction = transaction;
            this.matchpointWriteLockIds = new HashSet<Long>();
            this.absentIfRollback = absentIfRollback;
        }
    }
}
