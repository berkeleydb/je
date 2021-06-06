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

import java.io.File;

import org.junit.Test;

import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.util.test.TestBase;

/**
 * Dummy junit test captures illustrative code for HA exception handling
 * idioms.
 */
public class ExceptionIdiomsTest extends TestBase {

    /*
     * The number of seconds to wait between retries when a sufficient
     * number of replicas are not available for a transaction.
     */
    private static final int INSUFFICIENT_REPLICA_RETRY_SEC = 1;

    /* Amount of time to wait after a lock conflict. */
    private static final int LOCK_CONFLICT_RETRY_SEC = 1;

    /* Amount of time to wait to let a replica catch up before retrying. */
    private static final int CONSISTENCY_RETRY_SEC = 1;

    private static final int STATE_CHANGE_RETRY_SEC = 1;
    private static final int ENVIRONMENT_RETRY_SEC = 1;

    /* The maximum number of times to retry handle creation. */
    private static final int REP_HANDLE_RETRY_MAX = 10;

    /* The maximum number of times to retry the transaction. */
    static final int TRANSACTION_RETRY_MAX = 10;

    /* Exists only to satisfy junit. */
    @Test
    public void testNULL() {
    }

    /**
     * The method illustrates the outermost loop used to maintain a valid
     * environment handle. It handles all exceptions that invalidate the
     * environment, and creates a new environment handle so that the
     * application can recover and continue to make progress in the face of
     * errors that might result in environment invalidation.
     *
     * @param envHome the directory containing the replicated environment
     * @param repConfig the replication configuration
     * @param envConfig the environment configuration
     *
     * @throws InterruptedException if interrupted
     */
    void environmentLoop(File envHome,
                         ReplicationConfig repConfig,
                         EnvironmentConfig envConfig)
        throws InterruptedException {

        while (true) {
            ReplicatedEnvironment repEnv = null;
            try {
                repEnv = getEnvironmentHandle(envHome, repConfig, envConfig);
                transactionDispatchLoop(repEnv);
            } catch (InsufficientLogException insufficientLog) {
                /* Restore the log files from another node in the group. */
                NetworkRestore networkRestore = new NetworkRestore();
                networkRestore.execute(insufficientLog,
                                       new NetworkRestoreConfig());
                continue;
            } catch (RollbackException rollback) {

                /*
                 * Any transient state that is dependent on the environment
                 * must be re-synchronized to account for transactions that
                 * may have been rolled back.
                 */
                continue;
            } finally {
                if (repEnv != null) {
                    repEnv.close();
                }
            }
        }
    }

    /**
     * Creates the replicated environment handle and returns it. It will retry
     * indefinitely if a master could not be established because a sufficient
     * number of nodes were not available, or there were networking issues,
     * etc.
     *
     * @return the newly created replicated environment handle
     *
     * @throws InterruptedException if the operation was interrupted
     */
    ReplicatedEnvironment getEnvironmentHandle(File envHome,
                                               ReplicationConfig repConfig,
                                               EnvironmentConfig envConfig)
        throws InterruptedException {

        /*
         * In this example we retry REP_HANDLE_RETRY_MAX times, but a
         * production HA application may retry indefinitely.
         */
        for (int i = 0; i < REP_HANDLE_RETRY_MAX; i++) {
            try {
                return
                    new ReplicatedEnvironment(envHome, repConfig, envConfig);
            } catch (UnknownMasterException unknownMaster) {

                /*
                 * Indicates there is a group level problem: insufficient nodes
                 * for an election, network connectivity issues, etc. Wait and
                 * retry to allow the problem to be resolved.
                 */

                /*
                 * INSERT APP-SPECIFIC CODE HERE: Application would typically
                 * log this issue, page a sysadmin, etc., to get the underlying
                 * system issues resolved and retry.
                 */
                Thread.sleep(ENVIRONMENT_RETRY_SEC * 1000);
                continue;
            }
        }

        /*
         * INSERT APP-SPECIFIC CODE HERE: For example, an applications may
         * throw an exception or retry indefinitely in the above loop.
         */
        throw new IllegalStateException("Unable to open handle after " +
                                        REP_HANDLE_RETRY_MAX + " tries");
    }

    /**
     * Initiates transactions based upon the state of the node.
     * <p>
     * The code in the method is single threaded, but the dispatch could be
     * also be used to start threads to perform transactional work.
     *
     * @param repEnv the replicated environment
     *
     * @throws InterruptedException
     */
    private void transactionDispatchLoop(ReplicatedEnvironment repEnv)
        throws InterruptedException {

        while (true) {
           final State state = repEnv.getState();
           if (state.isUnknown()) {

                /*
                 * Typically means there is a group level problem, insufficient
                 * nodes for an election, network connectivity issues, etc.
                 * Wait and retry to allow the problem to be resolved.
                 */
                Thread.sleep(STATE_CHANGE_RETRY_SEC * 1000);
                continue;
            } else if (state.isDetached()) {

                /*
                 * The node is no longer in communication with the group.
                 * Reopen the environment, so the application can resume.
                 */
                return;
            }

            /*
             * The node is in the master or replica state. Assumes that a
             * higher level has sent it the appropriate type (read or write)
             * of transaction workload. If that isn't the case, the handler
             * for ReplicaWriteException will cause it to be redirected to the
             * correct node using an application-specific mechanism.
             */
           boolean readOnly = false; // Set based upon app-specific knowledge
           doTransaction(repEnv, readOnly);
        }
    }

    /**
     * Illustrates the handling of exceptions thrown in the process of
     * creating and executing a transaction. It retries the transaction if the
     * exception indicates that a retry is warranted.
     *
     * @param repEnv the replicated transactional environment
     * @param readOnly determines whether the transaction to be run is read
     * only.
     * @throws InterruptedException
     * @throws InsufficientLogException environment invalidating exception
     * handled at the outer level.
     * @throws RollbackException environment invalidation exception handled at
     * the the outer level.
     */
    private void doTransaction(ReplicatedEnvironment repEnv,
                               boolean readOnly)
        throws InterruptedException,
               InsufficientLogException,
               RollbackException {

        boolean success = false;
        long sleepMillis = 0;
        final TransactionConfig txnConfig = readOnly ?
            new TransactionConfig().setDurability(Durability.READ_ONLY_TXN) :
            null;

        for (int i = 0; i < TRANSACTION_RETRY_MAX; i++) {
            /* Sleep before retrying. */
            if (sleepMillis != 0) {
                Thread.sleep(sleepMillis);
                sleepMillis = 0;
            }
            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, txnConfig);

                /* INSERT APP-SPECIFIC CODE HERE: Do transactional work. */

                txn.commit();
                success = true;
                return;
            } catch (InsufficientReplicasException insufficientReplicas) {

                /*
                 * Give replicas a chance to contact this master, in case they
                 * have not had a chance to do so following an election.
                 */
                sleepMillis = INSUFFICIENT_REPLICA_RETRY_SEC * 1000;
                continue;
            } catch (InsufficientAcksException insufficientReplicas) {

                /*
                 * Transaction has been committed at this node. The other
                 * acknowledgments may be late in arriving, or may never arrive
                 * because the replica just went down.
                 */

                /*
                 * INSERT APP-SPECIFIC CODE HERE: For example, repeat
                 * idempotent changes to ensure they went through.
                 *
                 * Note that 'success' is false at this point, although some
                 * applications may consider the transaction to be complete.
                 */
                txn = null;
                return;
            } catch (ReplicaWriteException replicaWrite) {

                /*
                 * Attempted a modification while in the Replica state.
                 *
                 * INSERT APP-SPECIFIC CODE HERE: Cannot accomplish the changes
                 * on this node, redirect the write to the new master and retry
                 * the transaction there.  This could be done by forwarding the
                 * request to the master here, or by returning an error to the
                 * requester and retrying the request at a higher level.
                 */
                return;
            } catch (LockConflictException lockConflict) {

                /*
                 * Retry the transaction.  Note that LockConflictException
                 * covers the HA LockPreemptedException.
                 */
                sleepMillis = LOCK_CONFLICT_RETRY_SEC * 1000;
                continue;
            } catch (ReplicaConsistencyException replicaConsistency) {

                /*
                 * Retry the transaction. The timeout associated with the
                 * ReplicaConsistencyPolicy may need to be relaxed if it's too
                 * stringent.
                 */
                sleepMillis = CONSISTENCY_RETRY_SEC * 1000;
                continue;
            } finally {

                if (!success) {
                    if (txn != null) {
                        txn.abort();
                    }

                    /*
                     * INSERT APP-SPECIFIC CODE HERE: Perform any app-specific
                     * cleanup.
                     */
                }
            }
        }

        /*
         * INSERT APP-SPECIFIC CODE HERE: Transaction failed, despite retries.
         * Take some app-specific course of action.
         */
    }
}
