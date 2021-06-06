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

import static com.sleepycat.je.dbi.EnvironmentFailureReason.HARD_RECOVERY;

import com.sleepycat.je.Database;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.MatchpointSearchResults;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * This asynchronous exception indicates that a new master has been selected,
 * this <code>Replica</code>'s log is ahead of the current <code>Master</code>,
 * and in this case, the <code>Replica</code> was unable to rollback without a
 * recovery. As a consequence, it is possible that one or more of the most
 * recently committed transactions may need to be rolled back, before the
 * <code>Replica</code> can synchronize its state with that of the current
 * <code>Master</code>. Note that any CommitTokens obtained before restarting
 * this <code>Replica</code> shouldn't be used after {@link RollbackException}
 * is thrown because the token may no longer exist on the current
 * <code>Master</code> node, due to failover processing.
 * <p>
 * Existing {@link ReplicatedEnvironment}, and consequently {@link Database}
 * handles, are invalidated as a result of this exception. The application must
 * close all old handles and create new handles before it can proceed. The
 * actual rollback of any recently committed transactions is done when the
 * application re-instantiates and thereby reopens the {@link
 * ReplicatedEnvironment}.  The application is responsible for discarding and
 * recreating any transient state that may be associated with the committed
 * transactions that were rolled back. {@link #getEarliestTransactionId} and 
 * {@link #getEarliestTransactionCommitTime} provide information to help
 * determine which transactions might be rolled back. Note that it is possible
 * that no committed transactions have been rolled back and that the 
 * application need do no adjustments, in which case 
 * {@link #getEarliestTransactionCommitTime} will return null.
 * <p>
 * This exception should be encountered relatively infrequently in practice,
 * since the election mechanism favors nodes with the most advanced log when
 * deciding upon a master. The exception, due to its nature, can only be
 * encountered when the node is in the <code>Replica</code> state, or the node
 * is trying to transition to the <code>Replica</code> state.
 * <p>
 * Use of weak durability requirements like
 * {@link com.sleepycat.je.Durability.ReplicaAckPolicy#NONE} or a
 * {@link com.sleepycat.je.rep.ReplicationMutableConfig#NODE_PRIORITY} of zero
 * increases the likelihood of this exception.
 * @see RollbackProhibitedException
 */
public class RollbackException extends RestartRequiredException {
    private static final long serialVersionUID = 1;

    /* Testing support only */
    private final MatchpointSearchResults searchResults;
    /**
     * For internal use only.
     * @hidden
     */
    public RollbackException(RepImpl repImpl,
                             VLSN matchpointVLSN,
                             MatchpointSearchResults searchResults) {
        super(repImpl, HARD_RECOVERY, makeMessage(repImpl.getName(),
                                                  searchResults,
                                                  matchpointVLSN));
                                                  
        this.searchResults = searchResults;
              
    }

    private static String makeMessage
        (final String nodeName,
         final MatchpointSearchResults searchResults,
         final VLSN matchpointVLSN) {

        final long matchpointLSN = searchResults.getMatchpointLSN();
        return "Node " + nodeName + 
            " must rollback" +  searchResults.getRollbackMsg() +
            " in order to rejoin the replication group. All existing " +
            "ReplicatedEnvironment handles must be closed and " +
            "reinstantiated.  Log files were truncated to file 0x" + 
            DbLsn.getFileNumber(matchpointLSN) + ", offset 0x" +
            DbLsn.getFileOffset(matchpointLSN) + ", vlsn " +
            matchpointVLSN;
    }

    /**
     * Return the time in milliseconds of the earliest transaction commit that
     * has been rolled back. May return null if no commits have been rolled
     * back.  
     */
    public Long getEarliestTransactionCommitTime() {
        if (searchResults == null) {
            return null;
        }
        
        if (searchResults.getEarliestPassedTxn() == null) {
            return null;
        } 

        return searchResults.getEarliestPassedTxn().time.getTime();
    }

    /**
     * Return the id of the earliest transaction commit that has been
     * rolled back. 0 is returned if no commits have been rolled back.
     */
    public long getEarliestTransactionId() {
        if (searchResults == null) {
            return 0;
        }
        
        if (searchResults.getEarliestPassedTxn() == null) {
            return 0;
        }

        return searchResults.getEarliestPassedTxn().id;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public RollbackException(String message, RollbackException cause) {
        super(message + " " +  cause.getMessage(), cause);
        searchResults = cause.searchResults;
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public RollbackException wrapSelf(String msg) {
        return new RollbackException(msg, this);
    }
}
