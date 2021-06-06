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

package com.sleepycat.je.rep.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.txn.TxnAbort;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Timestamp;
import com.sleepycat.je.utilint.VLSN;

/**
 * Holds information seen by the ReplicaSyncupReader when
 * scanning a replica's log for a matchpoint.
 */
public class MatchpointSearchResults implements Serializable {
    private static final long serialVersionUID = 1L;

    private long matchpointLSN;

    /*
     * Track whether we passed a checkpoint which deleted cleaned log files.
     * If so, we cannot do a hard recovery.
     */
    private boolean passedCheckpointEnd;

    /*
     * If we skip a gap in the log file when searching for a matchpoint, we
     * no longer can be sure if we have passed checkpoints.
     */
    private boolean passedSkippedGap;

    /*
     * We save a set number of passed transactions for debugging information.
     * The list is limited in size in case we end up passing a large number
     * of transactions on our hunt for a matchpoint. Note that we save both
     * durable and non-durable transactions to aid with debugging.
     */
    private final List<PassedTxnInfo> passedTxns;
    private final int passedTxnLimit;

    /*
     * We need to keep the penultimate passed txn so we can readjust the
     * passed transaction information when the matchpoint is found. For
     * example, suppose the log contains:
     *   txn A commit
     *   txn B commit
     *   txn C commit
     * and txn A commit is the matchpoint. The way the loop works, we'll
     * have earliestPassedTxn = txnA, and penultimate = txn B. If the
     * matchpoint is txnA, the log will be truncated at txnB, and we
     * should reset earliestPassedTxn = txnB.
     *
     * Note that numPassedDurableCommits is a subset of numPassedCommits
     */
    private PassedTxnInfo earliestPassedTxn;
    private PassedTxnInfo penultimatePassedTxn;
    private int numPassedCommits;
    private int numPassedDurableCommits;

    /*
     * The DTVLSN established when scanning the log backwards from high to low
     * VLSNS
     */
    private VLSN dtvlsn = VLSN.NULL_VLSN;

    public MatchpointSearchResults(EnvironmentImpl envImpl) {

        /*
         * The matchpointLSN should be a non-null value, because we always have
         * to provide a valid truncation point.
         */
        matchpointLSN = DbLsn.makeLsn(0, 0);

        passedTxnLimit =
            envImpl.getConfigManager().getInt(RepParams.TXN_ROLLBACK_LIMIT);
        passedTxns = new ArrayList<PassedTxnInfo>();
        numPassedCommits = 0;
        numPassedDurableCommits = 0;
    }

    /**
     * If we see a checkpoint end record, see if it is a barrier to
     * rolling back, and advance the file reader position.
     */
    void notePassedCheckpointEnd() {
        passedCheckpointEnd = true;
    }

    /**
     * Keep track if we have jumped over a gap in the log files, and might have
     * missed a checkpoint end.
     */
    void noteSkippedGap() {
        passedSkippedGap = true;
    }

    /**
     * At the end of the search for a matchpoint, set the matchpointLSN and
     * adjust the debugging list of passed transactions. The matchpoint entry
     * is just before the truncation point, and does not get truncated.
     */
    void setMatchpoint(long match) {
        matchpointLSN = match;
        if ((earliestPassedTxn != null) &&
            (earliestPassedTxn.lsn == matchpointLSN)) {
            numPassedCommits--;

            if (passedTxns.size() > 0) {
               int lastSavedTxn = passedTxns.size() - 1;
               if (passedTxns.get(lastSavedTxn).lsn == match) {
                  passedTxns.remove(lastSavedTxn);
               }
               earliestPassedTxn = penultimatePassedTxn;
            }
        }
    }

    /**
     * The reader saw a transaction commit; record relevant information. Note
     * that we record all transactions (replicated and non-replicated) to avoid
     * cutting off a checkpoint. Note that the stream can be a combination of
     * log records from pre and post-dtvlsn masters. Note that we are scanning
     * the log backwards, so we are encountering commits in decreasing VLSN
     * sequence.
     *
     * @param commit the commit being passed. Note that this object is reused
     * and so should not be saved.
     *
     * @param commitVLSN the VLSN associated with the commit. It could be null
     * if the commit was local and not HA
     *
     * @param commitLSN the LSN at which the commit is located
     */
    void notePassedCommits(TxnCommit commit,
                           VLSN commitVLSN,
                           long commitLSN) {
        boolean durableCommit = false;
        final Timestamp commitTime = commit.getTime();
        final long txnId = commit.getId();
        final long commitDTVLSN = commit.getDTVLSN();

        if ((commitVLSN != null) && !commitVLSN.isNull()) {
            /* A replicated txn */
            processDTVLSN(commitVLSN, commitDTVLSN);

            if (commit.hasLoggedEntries() &&
                ((dtvlsn.getSequence() == VLSN.UNINITIALIZED_VLSN_SEQUENCE) ||
                 (commitVLSN.compareTo(dtvlsn) <= 0))) {
                numPassedDurableCommits++;
                durableCommit = true;
            }
        }

        numPassedCommits++;
        if (earliestPassedTxn != null) {
            penultimatePassedTxn = earliestPassedTxn;
        }
        earliestPassedTxn =
            new PassedTxnInfo(commitTime, txnId, commitVLSN, commitLSN,
                              durableCommit);

        /*
         * Save only a limited number of passed txns, for displaying to the log
         */
        if (numPassedCommits <= passedTxnLimit) {
            passedTxns.add(earliestPassedTxn);
        }
    }

    private void processDTVLSN(VLSN txnEndVLSN, final long txnEndDTVLSN) {
        if (dtvlsn.isNull()) {
            /*
             * The first commit/abort record, set the dtvlsn to a
             * non null value
             */
            dtvlsn = new VLSN(txnEndDTVLSN);
        } else {
            /*
             * Already set, verify sequencing. Make allowance for transitions
             * from pre to post-DTVLSN stream segments. Note that we are going
             * backwards in the log at this point.
             */
            if ((txnEndDTVLSN > dtvlsn.getSequence()) &&
                (dtvlsn.getSequence() != VLSN.UNINITIALIZED_VLSN_SEQUENCE)) {
                throw new IllegalStateException
                    ("DTVLSNs should only decrease with decreasing VLSNs." +
                     " prev:" + dtvlsn + " next:" + txnEndDTVLSN +
                     " commit VLSN:" + txnEndVLSN);
            }
        }
    }

    /**
     * Use the DTVLSN value from a replicated abort to set the dtvlsn.
     *
     * @param abort the abort log record
     *
     * @param abortVLSN the VLSN associated with the abort log record
     */
    void notePassedAborts(TxnAbort abort,
                          VLSN abortVLSN) {

        if (abortVLSN.isNull()) {
            /* A non-replicated abort. */
            return;
        }

        final long abortDTVLSN = abort.getDTVLSN();
        processDTVLSN(abortVLSN, abortDTVLSN);
    }

    boolean getPassedCheckpointEnd() {
        return passedCheckpointEnd;
    }

    boolean getSkippedGap() {
        return passedSkippedGap;
    }

    public long getMatchpointLSN() {
        return matchpointLSN;
    }

    public int getNumPassedCommits() {
        return numPassedCommits;
    }

    public int getNumPassedDurableCommits() {
        return numPassedDurableCommits;
    }

    public PassedTxnInfo getEarliestPassedTxn() {
        return earliestPassedTxn;
    }

    public VLSN getDTVLSN() {
        return dtvlsn;
    }

    /**
     * Display the saved transaction information.
     */
    public String dumpPassedTxns() {
        StringBuilder sb = new StringBuilder();
        for (PassedTxnInfo info : passedTxns) {
            sb.append(info);
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "matchpointLSN=" + DbLsn.getNoFormatString(matchpointLSN) +
            " passedCkpt=" + passedCheckpointEnd +
            " passedTxnLimit=" + passedTxnLimit +
            " passedTxns=" + passedTxns +
            " earliestTxn=" + earliestPassedTxn +
            " penultimateTxn=" + penultimatePassedTxn +
            " numPassedCommits=" + numPassedCommits +
            " numPassedDurableCommits=" + numPassedDurableCommits +
            " passedSkippedGap=" + passedSkippedGap;
    }

    /**
     * If 1 or more commits was passed, construct a message that can
     * be used by RollbackException and RollbackProhibitedException.
     */
    public String getRollbackMsg() {
        if (numPassedCommits == 0) {
            return " uncommitted operations";
        }

        if (numPassedDurableCommits == 0) {
            return " " + numPassedCommits +
                " total commits to the earliest point indicated by transaction " +
                earliestPassedTxn;
        }

        return " " + numPassedCommits + " total commits(" +
                numPassedDurableCommits + " of which were durable) " +
                "to the earliest point indicated by transaction " +
            earliestPassedTxn;
    }

    /* Struct to hold information about passed txns. */
    public static class PassedTxnInfo  implements Serializable {
        private static final long serialVersionUID = 1L;

        public final Timestamp time;
        public final long id;
        public final VLSN vlsn;
        public final long lsn;
        public final boolean durableCommit;

        PassedTxnInfo(Timestamp time, long id, VLSN vlsn, long lsn,
                      boolean durableCommit) {
            this.time = time;
            this.id = id;
            this.vlsn = vlsn;
            this.lsn = lsn;
            this.durableCommit = durableCommit;
        }

        @Override
        public String toString() {
            return "id=" + id +
                " time=" + time +
                " vlsn=" + vlsn +
                " lsn=" + DbLsn.getNoFormatString(lsn) +
                " durable=" + durableCommit;
        }
    }
}
