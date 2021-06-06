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

import static java.util.logging.Level.FINE;

import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.arbitration.Arbiter;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.stream.FeederTxns;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Provides information about quorums needed for durability decisions.
 */
public class DurabilityQuorum {

    private final RepImpl repImpl;
    private final Logger logger;

    public DurabilityQuorum(RepImpl repImpl) {

       this.repImpl = repImpl;
       logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * See if there are a sufficient number of replicas alive to support
     * the commit for this transaction. Used as an optimizing step before any
     * writes are executed, to reduce the number of outstanding writes that
     * suffer from insufficient ack problems.
     *
     * If this node is not the master, just return. A different kind of check
     * will catch the fact that this node cannot support writes.
     *
     * TODO: Read only transactions on the master should not have to wait.
     * In the future, introduce either a read-only attribute as part of
     * TransactionConfig or a read only transaction class to optimize this.
     *
     * @param insufficientReplicasTimeout timeout in ms
     * @throws InsufficientReplicasException if there are not enough replicas
     * connected to this feeder to be able to commit the transaction.
     */
    public void ensureReplicasForCommit(MasterTxn txn,
                                        int insufficientReplicasTimeout)
        throws DatabaseException, InterruptedException,
               InsufficientReplicasException {

        RepNode repNode = repImpl.getRepNode();
        if (!repNode.isMaster()) {
            return;
        }

        ReplicaAckPolicy ackPolicy =
            txn.getDefaultDurability().getReplicaAck();
        int requiredReplicaAckCount = getCurrentRequiredAckCount(ackPolicy);
        if (logger.isLoggable(FINE)) {
            LoggerUtils.fine(logger, repImpl,
                             "Txn " + txn + ": checking that " +
                             requiredReplicaAckCount +
                             " feeders exist before starting commit");
        }

        /* No need to wait for anyone else, only this node is needed. */
        if (requiredReplicaAckCount == 0) {
            return;
        }

        if (repNode.feederManager().awaitFeederReplicaConnections
            (requiredReplicaAckCount, insufficientReplicasTimeout)) {
            /* Wait was successful */
            return;
        }

        /*
         * Timed out, not enough replicas connected, or feeder was shutdown
         * normally, that is, without any exceptions while waiting.
         */
        if (!repNode.isMaster()) {

            /*
             * Continue if we are no longer the master after the wait. The
             * transaction will fail if it tries to acquire write locks, or
             * at commit.
             */
            return;
        }

        if (ackPolicy.equals(ReplicaAckPolicy.SIMPLE_MAJORITY) &&
            repNode.getArbiter().activateArbitration()) {
            return;
        }

        final boolean includeArbiters =
            !ackPolicy.equals(ReplicaAckPolicy.ALL);
        throw new InsufficientReplicasException(
            txn, ackPolicy, requiredReplicaAckCount,
            repNode.feederManager().activeAckReplicas(includeArbiters));
    }

    /**
     * Determine whether acknowledgments from the specified replica should be
     * counted against transaction durability requirements.
     *
     * @param replica the replica node
     * @return whether acknowledgments from the replica should be counted
     */
    public boolean replicaAcksQualify(final RepNodeImpl replica) {

        /* Only acknowledgments from electable nodes should be counted */
        return replica.getType().isElectable();
    }

    /**
     * Determine if this transaction has been adequately acknowledged.
     *
     * @throws InsufficientAcksException if the transaction's durability
     * requirements have not been met.
     */
    public void ensureSufficientAcks(FeederTxns.TxnInfo txnInfo,
                                     int timeoutMs)
        throws InsufficientAcksException {

        int pendingAcks = txnInfo.getPendingAcks();
        if (pendingAcks == 0) {
            return;
        }

        MasterTxn txn = txnInfo.getTxn();
        final int requiredAcks = getCurrentRequiredAckCount(
            txn.getCommitDurability().getReplicaAck());
        int requiredAckDelta = txn.getRequiredAckCount() - requiredAcks;
        if (requiredAckDelta >= pendingAcks) {

            /*
             * The group size was reduced while waiting for acks and the
             * acks received are sufficient given the new reduced group
             * size.
             */
            return;
        }

        /* Snapshot the state to be used in the error message */
        final String dumpState = repImpl.dumpAckFeederState();

        /*
         * Repeat the check to ensure that acks have not been received in
         * the time between the completion of the await() call above and
         * the creation of the exception message. This tends to happen when
         * there are lots of threads in the process thus potentially
         * delaying the resumption of this thread following the timeout
         * resulting from the await.
         *
         * It should be noted that some transactions may be setup to not
         * decrement the wait latch count for Arbiter acks. Checking an
         * Arbiters feeder VLSN here will account for the Arbiter ack.
         */
        final FeederManager feederManager =
            repImpl.getRepNode().feederManager();
        int currentFeederCount =
            feederManager.getNumCurrentAckFeeders(txn.getCommitVLSN());
        if (currentFeederCount >= requiredAcks) {
            String msg = "txn " + txn.getId() +
                " commit vlsn:" + txnInfo.getCommitVLSN() +
                " acknowledged after explicit feeder check" +
                " latch count:" + txnInfo.getPendingAcks() +
                " state:" + dumpState +
                " required acks:" + requiredAcks;

            LoggerUtils.info(logger, repImpl, msg);
            return;
        }

        /*
         * We can avoid the exception if it's possible for this node to enter
         * activate arbitration. It's useful to check for this again here in
         * case we happen to lose connections to replicas in the (brief)
         * period since the pre-log hook.  Note that in this case we merely
         * want to check; we don't want to switch into active arbitration
         * unless/until we actually lose the connection to the replica at
         * commit time. TODO: this doesn't seem right! Shouldn't we require
         * activation at this point!!!
         */
        if (repImpl.getRepNode().getArbiter().activationPossible()) {
            return;
        }
        throw new InsufficientAcksException(txn, pendingAcks, timeoutMs,
                                            dumpState);

    }

    /**
     * Returns the minimum number of acknowledgments required to satisfy the
     * ReplicaAckPolicy for a given group size. Does not include the master.
     * The method factors in considerations like the current arbitration status
     * of the environment and the composition of the replication group.
     *
     * TODO: it seems sufficient to return a number, as opposed to a set of
     * qualified ack nodes, as long as {@link #replicaAcksQualify} will only
     * count qualified acks against the required count. That does mean that
     * getCurrentRequiredAckCount and noteReplicaAcks for a transaction must be
     * kept consistent.
     *
     * @return the number of nodes that are needed, not including the master.
     */
    public int getCurrentRequiredAckCount(ReplicaAckPolicy ackPolicy) {

        /*
         * If the electableGroupSizeOverride is determining the size of the
         * election quorum, let it also influence the durability quorum.
         */
        RepNode repNode = repImpl.getRepNode();
        int electableGroupSizeOverride =
            repNode.getElectionQuorum().getElectableGroupSizeOverride();
        if (electableGroupSizeOverride > 0) {

            /*
             * Use the override-defined group size to determine the
             * number of acks.
             */
            return ackPolicy.minAckNodes(electableGroupSizeOverride) - 1;
        }

        Arbiter arbiter = repNode.getArbiter();
        if (arbiter.isApplicable(ackPolicy)) {
            return arbiter.getAckCount(ackPolicy);
        }

        return ackPolicy.minAckNodes
            (repNode.getGroup().getAckGroupSize()) - 1;
    }
}
