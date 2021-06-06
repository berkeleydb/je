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

import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.MatchpointSearchResults;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * This exception may be thrown by a Replica during the {@link <a
 * href="{@docRoot}/../ReplicationGuide/lifecycle.html#lifecycle-nodestartup">
 * replication stream sync-up</a>} phase of startup. It indicates that a syncup
 * cannot proceed without undoing a number of committed transactions that
 * exceeds the limit defined by {@link ReplicationConfig#TXN_ROLLBACK_LIMIT}.
 * <p>
 * It is rare for committed transactions to be rolled back during a
 * sync-up. One way this can happen is if a replication group has been
 * executing with a {@link com.sleepycat.je.Durability} policy that specifies a
 * {@link com.sleepycat.je.Durability.ReplicaAckPolicy ReplicaAckPolicy} of
 * NONE.
 * <p>
 * When ReplicaAckPolicy.NONE is specified, transactions can commit on the
 * master without receiving any acknowledgments from replica nodes. Using that
 * policy, it is possible that if the master node crashes at a given time, and
 * the group fails over and continues on with a new master, the old master's
 * environment will have transactions on disk that were never replicated and
 * received by other nodes. When this old master comes back up and rejoins the
 * group as a replica, it will have committed transactions that need to be
 * rolled back.
 * <p>
 * If the number of committed transactions to be rolled back is less than or
 * equal to the limit specified by {@link
 * ReplicationConfig#TXN_ROLLBACK_LIMIT}, JE will automatically truncate the
 * environment log to remove the unreplicated transactions, and will throw a
 * {@link RollbackException}. The application only needs to reinstantiate the
 * ReplicatedEnvironment and proceed on. If the limit specified by {@link
 * ReplicationConfig#TXN_ROLLBACK_LIMIT} is exceeded, the application will
 * receive a RollbackProhibitedException to indicate that manual intervention 
 * is required.
 * <p>
 * The RollbackProhibitedException lets the user interject application specific
 * processing before the log is truncated. The exception message and getter
 * methods indicate the number of transactions that must be rolled back, and
 * the time and id of the earliest targeted transaction, and the user can use
 * this information to make any desired application adjustments. The
 * application may then manually truncate the log using {@link
 * com.sleepycat.je.util.DbTruncateLog}.
 * <p>
 * Note that any CommitTokens obtained before restarting this
 * <code>Replica</code> shouldn't be used after
 * {@link RollbackProhibitedException} is thrown because the token may no 
 * longer exist on the current <code>Master</code> node.
 */
public class RollbackProhibitedException extends RestartRequiredException {
    private static final long serialVersionUID = 1L;

    /* 
     * searchResults is only used by threads that catch the exception,
     * so the field is sure to be initialized. 
     */
    private final MatchpointSearchResults searchResults;

    /**
     * For internal use only.
     * @hidden
     */
    public RollbackProhibitedException(RepImpl repImpl,
                                       int rollbackTxnLimit,
                                       boolean rollbackDisabled,
                                       VLSN matchpointVLSN,
                                       MatchpointSearchResults searchResults) {

        super(repImpl, EnvironmentFailureReason.ROLLBACK_PROHIBITED,
              makeMessage(repImpl.getName(), searchResults, matchpointVLSN,
                          rollbackTxnLimit, rollbackDisabled));
        this.searchResults = searchResults;
    }

    private static String makeMessage(String nodeName,
                                      MatchpointSearchResults searchResults,
                                      VLSN matchpointVLSN,
                                      int rollbackTxnLimit,
                                      boolean rollbackDisabled) {
        long matchpointLSN = searchResults.getMatchpointLSN();
        long fileNumber = DbLsn.getFileNumber(matchpointLSN);
        long fileOffset = DbLsn.getFileOffset(matchpointLSN);
        StringBuilder str = new StringBuilder();

        str.append("Node ").append(nodeName).
            append(" must rollback ").append(searchResults.getRollbackMsg()).
            append(" in order to rejoin the replication group, but ");

        if (rollbackDisabled) {
            str.append("rollbacks are disabled because ").
                append("je.rep.txnRollbackDisabled has been set to true. ").
                append("Either set je.rep.txnRollbackDisabled to false to ").
                append("permit automatic rollback,");

        } else {
            str.append(" the transaction rollback limit of ").
                append(rollbackTxnLimit).append(" prohibits this. ").
                append("Either increase je.rep.txnRollbackLimit to a ").
                append("value larger than ").append(rollbackTxnLimit).
                append(" to permit automatic rollback,");
        }

        str.append(" or manually remove the problematic transactions. ").
            append("To do manual removal, truncate the log to file ").
            append(FileManager.getFileName(fileNumber)).
            append(", offset 0x").append(Long.toHexString(fileOffset)).
            append(", vlsn ").append(matchpointVLSN).
            append(" using the directions in ").
            append("com.sleepycat.je.util.DbTruncateLog.");

        return str.toString();
    }

    /**
     * For internal use only.
     * @hidden
     */
    public RollbackProhibitedException(String message,
                                       RollbackProhibitedException cause) {
        super(message + " " + cause.getMessage(), cause);
        this.searchResults = cause.searchResults;
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public RollbackProhibitedException wrapSelf(String msg) {
        return new RollbackProhibitedException(msg, this);
    }

    /*
     * The JE log must be truncated to this file in order for this node to
     * rejoin the group.
     */
    public long getTruncationFileNumber() {
        return DbLsn.getFileNumber(searchResults.getMatchpointLSN());
    }

    /**
     * The JE log must be truncated to this offset in the specified
     * file in order for this node to rejoin the group.
     */
    public long getTruncationFileOffset() {
        return DbLsn.getFileOffset(searchResults.getMatchpointLSN());
    }

    /**
     * Return the time in milliseconds of the earliest transaction commit that 
     * will be rolled back if the log is truncated to the location specified by
     * {@link #getTruncationFileNumber} and {@link #getTruncationFileOffset}
     */
    public Long getEarliestTransactionCommitTime() {
        return searchResults.getEarliestPassedTxn().time.getTime();
    }

    /**
     * Return the id of the earliest transaction commit that will be
     * rolled back if the log is truncated to the location specified by
     * {@link #getTruncationFileNumber} and {@link #getTruncationFileOffset}
     */
    public long getEarliestTransactionId() {
        return searchResults.getEarliestPassedTxn().id;
    }
}
