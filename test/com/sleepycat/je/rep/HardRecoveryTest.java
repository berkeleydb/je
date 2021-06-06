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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.impl.RepImplStatDefinition;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.ReplicaFeederSyncup.TestHook;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.WaitForMasterListener;
import com.sleepycat.je.util.DbTruncateLog;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Check the rollback past a commit or abort.
 */
public class HardRecoveryTest extends TestBase {
    private final boolean verbose = Boolean.getBoolean("verbose");
    private static final String DB_NAME = "testDb";
    private final File envRoot;
    private final Logger logger;

    public HardRecoveryTest() {
        envRoot = SharedTestUtils.getTestDir();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");
    }

    /**
     * HardRecovery as invoked via the ReplicatedEnvironment constructor.
     * Mimics the case where A was the master, has a commit in its log but that
     * commit has not been propagated.  A goes down, new master = B. When A
     * comes up, it has to roll back its commit and do a hard recovery. This
     * flavor of hard recovery is executed silently within the
     * ReplicatedEnvironment constructor.
     */
    @Test
    public void testHardRecoveryNoLimit()
        throws Exception {

        doHardRecovery(false, false, false);
    }

    /**
     * Same as {@link #testHardRecoveryNoLimit} but sets the rollback limit
     * to cause a RollbackProhibitedException.
     */
    @Test
    public void testHardRecoveryWithLimit()
        throws Exception {

        doHardRecovery(true, false, false);
    }

    /**
     * Same as {@link #testHardRecoveryNoLimit} but disables hard rollbacks
     * to cause a RollbackProhibitedException.
     */
    @Test
    public void testHardRecoveryDisabled()
        throws Exception {

        doHardRecovery(false, true, false);
    }

    /*
     * Test that a hard recovery which requires truncation in the penultimate
     * log file works, and that hard recovery processing does not mistakenly
     * cause a gap in the log file.
     *
     * This bug results in a recovery hang. [#19463]
     */
    @Test
    public void testMultipleLogFilesHardRecovery()
        throws Exception {

        doHardRecovery(false, false, true);
    }

    /**
     * If setRollbackLimit or disableHardRollback, we expect to get a
     * RollbackProhibitedException and to have to manually truncate the
     * environment.
     *
     * @param setRollbackLimit if true, set TXN_ROLLBACK_LIMIT to zero and
     * falsify the DTVLSN info to cause RollbackProhibitedException.
     *
     * @param disableHardRollback if true, set TXN_ROLLBACK_ENABLED to false
     * to cause RollbackProhibitedException.
     *
     * @param txnMultipleLogFiles make the rollback txn span multiple files.
     */
    private void doHardRecovery(boolean setRollbackLimit,
                                boolean disableHardRollback,
                                boolean txnMultipleLogFiles)
        throws Exception {

        assertFalse(setRollbackLimit && disableHardRollback);

        RepEnvInfo[] repEnvInfo = null;
        Database db = null;
        int numNodes = 3;
        try {
            EnvironmentConfig envConfig = RepTestUtils.createEnvConfig
               (RepTestUtils.SYNC_SYNC_NONE_DURABILITY);

            /*
             * Set a small log file size to make sure the rolled back
             * transaction log entries in mulitple log files.
             */
            if (txnMultipleLogFiles) {
                DbInternal.disableParameterValidation(envConfig);
                envConfig.setConfigParam
                    (EnvironmentConfig.LOG_FILE_MAX, "2000");
            }

            ReplicationConfig repConfig = new ReplicationConfig();

            if (setRollbackLimit) {
                repConfig.setConfigParam(
                    ReplicationConfig.TXN_ROLLBACK_LIMIT, "0");
            }

            if (disableHardRollback) {
                repConfig.setConfigParam(
                    ReplicationConfig.TXN_ROLLBACK_DISABLED, "true");
            }

            repEnvInfo = RepTestUtils.setupEnvInfos
                (envRoot, numNodes, envConfig, repConfig);

            ReplicatedEnvironment master = repEnvInfo[0].openEnv();
            assert master != null;

            db = createDb(master);

            logger.info("Created db on master");
            CommitToken commitToken = doInsert(master, db, 1, 1);
            CommitPointConsistencyPolicy cp =
                new CommitPointConsistencyPolicy(commitToken, 1000,
                                                 TimeUnit.SECONDS);
            for (int i = 1; i < numNodes; i++) {
                repEnvInfo[i].openEnv(cp);
            }

            /* Sync the group to make sure all replicas start up. */
            RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);

            /*
             * Shut down all replicas so that they don't see the next
             * commit.
             */
            for (int i = 1; i < numNodes; i++) {
                logger.info("shut down replica " +
                              repEnvInfo[i].getEnv().getNodeName());
                repEnvInfo[i].closeEnv();
            }

            /*
             * Do work on the sole node, which is the master, then close it.
             * This work was committed, and will have to be rolled back later
             * on.
             */
            logger.info("do master only insert");

            if (txnMultipleLogFiles) {

                /*
                 * A large transaction writes log entries in mulitple log
                 * files.
                 */
                DbInternal.getNonNullEnvImpl(master).forceLogFileFlip();
                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry data = new DatabaseEntry(new byte[200]);
                Transaction txn = master.beginTransaction(null, null);
                for (int i = 1; i <= 10; i++) {
                    IntegerBinding.intToEntry(i, key);
                    db.put(txn, key, data);
                }
                txn.commit();
                commitToken = txn.getCommitToken();
            } else {
                commitToken = doInsert(master, db, 2, 5);
                checkExists(master, db, 1, 2, 3, 4, 5);
            }

            db.close();
            db = null;


            /*
             * If setRollbackLimit is true, pretend that the preceding
             * transactions were made durable by a surreptitious update of
             * the DTVLSN used to determine hard rollback limits.
             *
             * Note that we do not need to do this when disableHardRollback is
             * true, because rollback is prohibited regardless of whether the
             * rolled back txns are considered durable.
             */
            if (setRollbackLimit) {
                final RepNode masterRepNode =
                    master.getNonNullRepImpl().getRepNode();

                final long commitTokenVLSN = commitToken.getVLSN();
                masterRepNode.updateDTVLSN(commitTokenVLSN);

                new PollCondition(100, 5000) {

                    @Override
                    protected boolean condition() {
                        /*
                         * Wait for the Null transaction to ensure that the
                         * DTVLSN has been made persistent
                         */
                        return masterRepNode.getCurrentTxnEndVLSN().
                                getSequence() > commitTokenVLSN;
                    };
                }.await();
            }

            if (txnMultipleLogFiles) {
                /* Close the Environment without doing a checkpoint. */
                repEnvInfo[0].abnormalCloseEnv();
            } else {
                repEnvInfo[0].closeEnv();
            }

            /*
             * Restart the group, make it do some other work which the
             * original master, which is down, won't see.
             */
            logger.info("restart group");
            master = RepTestUtils.restartGroup(repEnvInfo[1], repEnvInfo[2]);

            logger.info("group came up, new master = " +  master.getNodeName());
            db = openDb(master);
            commitToken = doInsert(master, db, 10,15);
            checkNotThere(master, db, 2, 3, 4, 5);
            checkExists(master, db, 1, 10, 11, 12, 13, 14, 15);

            /*
             * When we restart the master, it should transparently do a hard
             * recovery.
             */
            logger.info("restart old master");
            ReplicatedEnvironment oldMaster = null;
            try {
                repEnvInfo[0].openEnv
                    (new CommitPointConsistencyPolicy(commitToken, 1000,
                                                      TimeUnit.SECONDS));
                assertFalse(setRollbackLimit);
                assertFalse(disableHardRollback);
                oldMaster = repEnvInfo[0].getEnv();
                assertTrue(RepInternal.getNonNullRepImpl(oldMaster).
                           getNodeStats().
                           getBoolean(RepImplStatDefinition.HARD_RECOVERY));
                logger.info
                    (RepInternal.getNonNullRepImpl(oldMaster).getNodeStats().
                     getString(RepImplStatDefinition.HARD_RECOVERY_INFO));
            } catch (RollbackProhibitedException e) {

                /*
                 * If setRollback limit is set, we should get this exception
                 * with directions on how to truncate the log. If the
                 * limit was not set, the truncation should have been done
                 * by JE already.
                 */
                assertTrue(setRollbackLimit || disableHardRollback);
                assertEquals(0, e.getTruncationFileNumber());
                assertTrue(e.getEarliestTransactionId() != 0);
                assertTrue(e.getEarliestTransactionCommitTime() != null);

                /*
                 * Very test dependent, it should be 2657 at least, but should
                 * be larger if some internal replicated transaction commits.
                 * A change in log entry sizes could change this value. This
                 * should be set to the value in of the matchpoint.
                 */
                assertTrue(e.getTruncationFileOffset() >= 2657);

                DbTruncateLog truncator = new DbTruncateLog();
                truncator.truncateLog(repEnvInfo[0].getEnvHome(),
                                      e.getTruncationFileNumber(),
                                      e.getTruncationFileOffset());
                repEnvInfo[0].openEnv
                    (new CommitPointConsistencyPolicy(commitToken, 1000,
                                                      TimeUnit.SECONDS));
                oldMaster = repEnvInfo[0].getEnv();
            }

            Database replicaDb = openDb(oldMaster);
            checkNotThere(oldMaster, replicaDb, 2, 3, 4, 5);
            replicaDb.close();

            VLSN commitVLSN = RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                                 numNodes);
            RepTestUtils.checkNodeEquality(commitVLSN, verbose, repEnvInfo);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (db != null) {
                db.close();
                db = null;
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    private Database createDb(ReplicatedEnvironment master) {
        return openDb(master, true);
    }

    private Database openDb(ReplicatedEnvironment master) {
        return openDb(master, false);
    }

    private Database openDb(ReplicatedEnvironment master, boolean allowCreate) {

        Transaction txn = master.beginTransaction(null,null);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(allowCreate);
        dbConfig.setTransactional(true);
        Database db = master.openDatabase(txn, DB_NAME, dbConfig);
        txn.commit();
        return db;
    }

    /**
     * @return the commit token for the last txn used in the insert
     */
    private CommitToken doInsert(ReplicatedEnvironment master,
                                 Database db,
                                 int startVal,
                                 int endVal) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Transaction txn = null;

        for (int i = startVal; i <= endVal; i++) {
            txn = master.beginTransaction(null, null);
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            assertEquals(OperationStatus.SUCCESS, db.put(txn, key, data));
            if (verbose) {
                System.out.println("insert " + i);
            }
            txn.commit();
        }

        return (txn == null) ? null: txn.getCommitToken();
    }

    /**
     * Assert that these values are IN the database.
     */
    private void checkExists(ReplicatedEnvironment node,
                             Database db,
                             int ... values) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        if (verbose) {
            System.err.println("Entering checkThere: node=" +
                               node.getNodeName());
        }

        for (int i : values) {
            IntegerBinding.intToEntry(i, key);
            if (verbose) {
                System.err.println("checkThere: node=" + node.getNodeName() +
                                   " " + i);
            }
            assertEquals(OperationStatus.SUCCESS,
                         db.get(null, key, data, LockMode.DEFAULT));
            assertEquals(i, IntegerBinding.entryToInt(data));
        }
    }

    /**
     * Assert that these values are NOT IN the database.
     */
    private void checkNotThere(ReplicatedEnvironment node,
                               Database db,
                               int ... values) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        for (int i : values) {
            IntegerBinding.intToEntry(i, key);
            if (verbose) {
                System.out.println("checkNotThere: node=" + node.getNodeName()
                                   + " " + i);
            }
            assertEquals("for key " + i, OperationStatus.NOTFOUND,
                         db.get(null, key, data, LockMode.DEFAULT));
        }
    }

    /**
     * HardRecovery as invoked on a live replica. Can only occur with network
     * partitioning.
     *
     * Suppose we have nodes A,B,C. A is the master. C is partitioned, and
     * therefore misses part of the replication stream. Then, through a series
     * of timing accidents, C wins mastership over A and B. A and B then have
     * to discover the problem during a syncup, and throw hard recovery
     * exceptions.
     */
    @Test
    public void testHardRecoveryDeadHandle()
        throws Throwable {

        RepEnvInfo[] repEnvInfo = null;
        Database db = null;
        int numNodes = 3;
        try {
            repEnvInfo = RepTestUtils.setupEnvInfos
                (envRoot, numNodes, RepTestUtils.SYNC_SYNC_NONE_DURABILITY);

            /*
             * Start the master first, to ensure that it is the master, and then
             * start the rest of the group.
             */
            ReplicatedEnvironment master = repEnvInfo[0].openEnv();
            assert master != null;

            db = createDb(master);
            CommitToken commitToken = doInsert(master, db, 1, 1);
            CommitPointConsistencyPolicy cp =
                new CommitPointConsistencyPolicy(commitToken, 1000,
                                                 TimeUnit.SECONDS);

            for (int i = 1; i < numNodes; i++) {
                repEnvInfo[i].openEnv(cp);
            }

            /*
             * After node1 and node2 join, make sure that their presence in the
             * rep group db is propagated before we do a forceMaster. When a
             * node calls for an election, it must have its own id available to
             * itself from the rep group db on disk. If it doesn't, it will
             * send an election request with an illegal node id. In real life,
             * this can never happen, because a node that does not have its own
             * id won't win mastership, since others will be ahead of it.
             */
            commitToken = doInsert(master, db, 2, 2);
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setConsistencyPolicy
                (new CommitPointConsistencyPolicy(commitToken, 1000,
                                                  TimeUnit.SECONDS));

            for (int i = 1; i < numNodes; i++) {
                Transaction txn =
                    repEnvInfo[i].getEnv().beginTransaction(null, txnConfig);
                txn.commit();
            }

            /*
             * Mimic a network partition by forcing one replica to not see the
             * incoming LNs. Do some work, which that last replica doesn't see
             * and then force the laggard to become the master. This will
             * create a case where the current master has to do a hard
             * recovery.
             */
            int lastIndex = numNodes - 1;
            WaitForMasterListener masterWaiter = new WaitForMasterListener();
            ReplicatedEnvironment forcedMaster = repEnvInfo[lastIndex].getEnv();
            forcedMaster.setStateChangeListener(masterWaiter);
            RepNode lastRepNode =  repEnvInfo[lastIndex].getRepNode();
            lastRepNode.replica().setDontProcessStream();

            commitToken = doInsert(master, db, 3, 4);
            db.close();
            db = null;
            logger.info("Before force");

            /*
             * WaitForNodeX are latches that will help us know when node 1 and 2
             * have finished syncing up with node 3, after forcing node 3 to
             * become a master. There will be two syncup attempts, because after
             * the first syncup attempt, we do an election to make sure that
             * we are running with the most optimal master.
             */
            CountDownLatch waitForNode1 =
                setupWaitForSyncup(repEnvInfo[0].getEnv(), 2);
            CountDownLatch waitForNode2 =
                setupWaitForSyncup(repEnvInfo[1].getEnv(), 2);

            /*
             * Make node3 the master. Make sure that it did not see the
             * work done while it was in its fake network partitioned state.
             */
            lastRepNode.forceMaster(true);
            logger.info("After force");
            masterWaiter.awaitMastership();

            db = openDb(forcedMaster);
            checkNotThere(forcedMaster, db, 3, 4);
            checkExists(forcedMaster, db, 1, 2);
            db.close();

            /*
             * At this point, node 1 and 2 should have thrown a
             * RollbackException. Both will become invalid.
             */
            checkForHardRecovery(waitForNode1, repEnvInfo[0]);
            checkForHardRecovery(waitForNode2, repEnvInfo[1]);

            /*
             * Restart the group, make it do some other work and check
             * that the group has identical contents.
             */
            logger.info("restarting nodes which did hard recovery");
            RepTestUtils.restartReplicas(repEnvInfo[0]);
            RepTestUtils.restartReplicas(repEnvInfo[1]);
            logger.info("sync group");
            VLSN commitVLSN = RepTestUtils.syncGroupToLastCommit(repEnvInfo,
                                                                 numNodes);

            logger.info("run check");
            RepTestUtils.checkNodeEquality(commitVLSN, verbose, repEnvInfo);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (db != null) {
                db.close();
                db = null;
            }
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /**
     * Wait until a replica/feeder syncup has been tried numSyncupAttempt times
     * on this node.
     */
    private CountDownLatch setupWaitForSyncup
        (final ReplicatedEnvironment master, int numSyncupAttempts) {
        final CountDownLatch  waiter = new CountDownLatch(numSyncupAttempts);

        TestHook<Object> syncupFinished = new TestHook<Object>() {
            @Override
            public void doHook() throws InterruptedException {
                logger.info("----syncup countdown for " +
                               master.getNodeName() + " latch=" + waiter);
                waiter.countDown();
            }
        };

        RepInternal.getNonNullRepImpl(master).getRepNode().
            replica().setReplicaFeederSyncupHook(syncupFinished);
        return waiter;
    }

    /**
     * Make sure that this node has thrown a RollbackException.
     */
    private void checkForHardRecovery(CountDownLatch syncupFinished,
                                      RepEnvInfo envInfo)
        throws Throwable {

        syncupFinished.await();
        logger.info(envInfo.getEnv().getNodeName() + " becomes replica");

        try {
            ReplicatedEnvironment.State state = envInfo.getEnv().getState();
            fail("Should have seen rollback exception, got state of " +
                 state);
        } catch (RollbackException expected) {
            assertTrue(expected.getEarliestTransactionId() != 0);
            assertTrue(expected.getEarliestTransactionCommitTime() != null);
            logger.info("expected = " + expected.toString());
            envInfo.closeEnv();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
}
