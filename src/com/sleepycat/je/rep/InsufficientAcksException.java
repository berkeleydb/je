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

import com.sleepycat.je.Durability;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.rep.txn.MasterTxn;

/**
 * <p>
 * This exception is thrown at the time of a commit in a Master, if the Master
 * could not obtain transaction commit acknowledgments from its Replicas in
 * accordance with the {@link ReplicaAckPolicy} currently in effect and within
 * the requested timeout interval. This exception will never be thrown when the
 * {@code ReplicaAckPolicy} of {@link ReplicaAckPolicy#NONE NONE} is in effect.
 * <p>
 * Note that an {@link InsufficientAcksException} means the transaction has
 * already committed at the master. The transaction may also have been
 * committed at one or more Replicas, but the lack of replica acknowledgments
 * means that the number of replicas that committed could not be
 * established. If the transaction was in fact committed by less than a simple
 * majority of the nodes, it could result in a {@link RollbackException} when
 * the node subsequently attempts to rejoin the group as a Replica.
 * <p>
 * The application can handle the exception and choose to respond in a number
 * of ways. For example, it can
 * <ul>
 * <li>do nothing, assuming that the transaction will eventually propagate to
 * enough replicas to become durable,
 * <li>retry the operation in a new transaction, which may succeed or fail
 * depending on whether the underlying problems have been resolved,
 * <li>retry using a larger timeout interval and return to the original
 * timeout interval at a later time,
 * <li>fall back temporarily to a read-only mode,
 * <li>increase the durability of the transaction on the Master by ensuring
 * that the changes are flushed to the operating system's buffers or to
 * the disk, or
 * <li>give up and report an error at a higher level, perhaps to allow an
 * administrator to check the underlying cause of the failure.
 * </ul>
 *
 * @see Durability
 */
public class InsufficientAcksException extends OperationFailureException {
    private static final long serialVersionUID = 1;

    private final int acksPending;
    private final int acksRequired;
    private final int ackTimeoutMs;
    private final String feederState;

    /**
     * @hidden
     * Creates a InsufficientAcksException.
     *
     * @param acksPending the number of missing acknowledgments
     * @param ackTimeoutMs the current acknowledgment timeout value in
     * milliseconds
     */
    public InsufficientAcksException(MasterTxn txn,
                                     int acksPending,
                                     int ackTimeoutMs,
                                     String feederState) {
        super(null, false /*abortOnly*/,
              "Transaction: " + txn.getId() +
              "  VLSN: " + txn.getCommitVLSN() +
              ", initiated at: " + String.format("%1tT. ", txn.getStartMs()) +
              " Insufficient acks for policy:" +
              txn.getCommitDurability().getReplicaAck() + ". " +
              "Need replica acks: " + txn.getRequiredAckCount() + ". " +
              "Missing replica acks: " + acksPending + ". " +
              "Timeout: " + ackTimeoutMs + "ms. " +
              "FeederState=" + feederState,
              null /*cause*/);
        assert(acksPending <= txn.getRequiredAckCount());
        this.acksPending = acksPending;
        this.acksRequired = txn.getRequiredAckCount();
        this.ackTimeoutMs = ackTimeoutMs;
        this.feederState = feederState;
    }

    /**
     * For testing only.
     * @hidden
     */
    public InsufficientAcksException(String message) {
        super(message);
        this.acksPending = 0;
        this.acksRequired = 0;
        this.ackTimeoutMs = 0;
        this.feederState = "Test feeder state";
    }

    /**
     * For internal use only.
     * @hidden
     */
    private InsufficientAcksException(String message,
                                      InsufficientAcksException cause) {
        super(message, cause);
        this.acksPending = cause.acksPending;
        this.acksRequired = cause.acksRequired;
        this.ackTimeoutMs = cause.ackTimeoutMs;
        this.feederState = cause.feederState;
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new InsufficientAcksException(msg, this);
    }

    /**
     * It returns the number of Replicas that did not respond with an
     * acknowledgment within the Replica commit timeout period.
     *
     * @return the number of missing acknowledgments
     */
    public int acksPending() {
        return acksPending;
    }

    /**
     * It returns the number of acknowledgments required by the commit policy.
     *
     * @return the number of acknowledgments required
     */
    public int acksRequired() {
        return acksRequired;
    }

    /**
     * Returns the acknowledgment timeout that was in effect at the time of the
     * exception.
     *
     * @return the acknowledgment timeout in milliseconds
     */
    public int ackTimeout() {
        return ackTimeoutMs;
    }
}
