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

package je.rep.quote;

import java.io.PrintStream;

import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Utility class to begin and commit/abort a transaction and handle exceptions
 * according to this application's policies.  The doTransactionWork method is
 * abstract and must be implemented by callers.  The transaction is run and
 * doTransactionWork is called by the run() method of this class.  The
 * onReplicaWrite and onRetryFailure methods may optionally be overridden.
 */
public abstract class RunTransaction {

    /* The maximum number of times to retry the transaction. */
    private static final int TRANSACTION_RETRY_MAX = 10;

    /*
     * The number of seconds to wait between retries when a sufficient
     * number of replicas are not available for a transaction.
     */
    private static final int INSUFFICIENT_REPLICA_RETRY_SEC = 1;

    /* Amount of time to wait to let a replica catch up before retrying. */
    private static final int CONSISTENCY_RETRY_SEC = 1;

    /* Amount of time to wait after a lock conflict. */
    private static final int LOCK_CONFLICT_RETRY_SEC = 1;

    private final ReplicatedEnvironment env;
    private final PrintStream out;

    /**
     * Creates the runner.
     */
    RunTransaction(ReplicatedEnvironment repEnv, PrintStream printStream) {
        env = repEnv;
        out = printStream;
    }

    /**
     * Runs a transaction, calls the doTransactionWork method, and retries as
     * needed.
     * <p>
     * If the transaction is read only, it uses Durability.READ_ONLY_TXN for
     * the Transaction. Since this Durability policy does not call for any
     * acknowledgments, it eliminates the possibility of a {@link
     * InsufficientReplicasException} being thrown from the call to {@link
     * Environment#beginTransaction} for a read only transaction on a Master,
     * which is an overly stringent requirement. This makes the Master more
     * available for read operations.
     *
     * @param readOnly determines whether the transaction to be run is read
     * only.
     */
    public void run(boolean readOnly)
        throws InterruptedException, EnvironmentFailureException {

        OperationFailureException exception = null;
        boolean success = false;
        long sleepMillis = 0;
        TransactionConfig txnConfig = setupTxnConfig(readOnly);

        for (int i = 0; i < TRANSACTION_RETRY_MAX; i++) {
            /* Sleep before retrying. */
            if (sleepMillis != 0) {
                Thread.sleep(sleepMillis);
                sleepMillis = 0;
            }
            Transaction txn = null;
            try {
                txn = env.beginTransaction(null, txnConfig);
                doTransactionWork(txn); /* CALL APP-SPECIFIC CODE */
                txn.commit();
                success = true;
                return;
            } catch (InsufficientReplicasException insufficientReplicas) {

                /*
                 * Retry the transaction.  Give Replicas a chance to contact
                 * this Master, in case they have not had a chance to do so
                 * following an election.
                 */
                exception = insufficientReplicas;
                out.println(insufficientReplicas.toString() + 
                            "\n  Retrying ...");
                sleepMillis = INSUFFICIENT_REPLICA_RETRY_SEC * 1000;

                if (i > 1) {

                    /*
                     * As an example of a possible application choice, 
                     * elect to execute this operation with lower durability.
                     * That makes the node more available, but puts the 
                     * data at greater risk.
                     */
                    txnConfig = lowerDurability(txnConfig);
                }
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
                out.println(insufficientReplicas.toString());
                txn = null;
                return;
            } catch (ReplicaWriteException replicaWrite) {

                /*
                 * Attempted a modification while in the Replica state.
                 *
                 * CALL APP-SPECIFIC CODE HERE: Cannot accomplish the changes
                 * on this node, redirect the write to the new master and retry
                 * the transaction there.  This could be done by forwarding the
                 * request to the master here, or by returning an error to the
                 * requester and retrying the request at a higher level.
                 */
                onReplicaWrite(replicaWrite);
                return;
            } catch (LockConflictException lockConflict) {

                /*
                 * Retry the transaction.  Note that LockConflictException
                 * covers the HA LockPreemptedException.
                 */
                exception = lockConflict;
                out.println(lockConflict.toString() + "\n  Retrying ...");
                sleepMillis = LOCK_CONFLICT_RETRY_SEC * 1000;
                continue;
            } catch (ReplicaConsistencyException replicaConsistency) {

                /*
                 * Retry the transaction to see if the replica becomes
                 * consistent. If consistency couldn't be satisfied, we can
                 * choose to relax the timeout associated with the
                 * ReplicaConsistencyPolicy, or to do a read with
                 * NoConsistencyRequiredPolicy.
                 */
                exception = replicaConsistency;
                out.println(replicaConsistency.toString() + 
                            "\n  Retrying ...");
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
         * CALL APP-SPECIFIC CODE HERE: Transaction failed, despite retries.
         */
        onRetryFailure(exception);
    }

    /**
     * Must be implemented to perform operations using the given Transaction.
     */
    public abstract void doTransactionWork(Transaction txn);

    /**
     * May be optionally overridden to handle a ReplicaWriteException.  After
     * this method is called, the RunTransaction constructor will return.  By
     * default, this method throws the ReplicaWriteException.
     */
    public void onReplicaWrite(ReplicaWriteException replicaWrite) {
        throw replicaWrite;
    }

    /**
     * May be optionally overridden to handle a failure after the
     * TRANSACTION_RETRY_MAX has been exceeded.  After this method is called,
     * the RunTransaction constructor will return.  By default, this method
     * prints the last exception.
     */
    public void onRetryFailure(OperationFailureException lastException) {
        out.println("Failed despite retries." +
                            ((lastException == null) ?
                              "" :
                              " Encountered exception:" + lastException));
    }

    /**
     * Reduce the Durability level so that we don't require any
     * acknowledgments from replicas. An example of using lower durability
     * requirements.
     */
    private TransactionConfig lowerDurability(TransactionConfig txnConfig) {

        out.println("\nLowering durability, execute update although " +
                    "replicas not available. Update may not be durable.");
        TransactionConfig useTxnConfig = txnConfig;
        if (useTxnConfig == null) {
            useTxnConfig = new TransactionConfig();
        }

        useTxnConfig.setDurability(new Durability(SyncPolicy.WRITE_NO_SYNC,
                                                  SyncPolicy.NO_SYNC,
                                                  ReplicaAckPolicy.NONE));
        return useTxnConfig;
    }

    /**
     * Create an optimal transaction configuration.
     */
     private TransactionConfig setupTxnConfig(boolean readOnly) {

         if (!readOnly) {
             /* 
              * A read/write transaction can just use the default transaction
              * configuration. A null value for the configuration param means
              * defaults should be used.
              */
             return null;
         }

         if (env.getState().isUnknown()) {

             /* 
              * This node is not in touch with the replication group master and
              * because of that, can't fulfill any consistency checks. As an *
              * example of a possible application choice, change the
              * consistency * characteristics for this specific transaction and
              * avoid a * ReplicaConsistencyException by lowering the
              * consistency requirement now.
              */
             out.println("\nLowering consistency, permit access of data " +
                         " currently on this node.");
             return new TransactionConfig().setConsistencyPolicy
                 (NoConsistencyRequiredPolicy.NO_CONSISTENCY);
         } 

         /*
          * We can optimize a read operation by specifying a lower
          * durability. Since Durability.READ_ONLY_TXN does not call for any
          * acknowledgments, it eliminates the possibility of a {@link
          * InsufficientReplicasException} being thrown from the call to {@link
          * Environment#beginTransaction} for a read only transaction on a
          * Master. 
          */
         return new TransactionConfig().setDurability
             (Durability.READ_ONLY_TXN);
     }
}
