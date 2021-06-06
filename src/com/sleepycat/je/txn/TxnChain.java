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

package com.sleepycat.je.txn;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * TxnChain supports "txn rollback", which undoes the write operations for a
 * given txn to an arbitrary point. Txn rollback (and TxnChain construction)
 * is done in 2 occasions:
 * 1. During normal processing, when an ongoing txn must be rolled-back due to
 *    a syncup operation (see rep/txn/ReplayTxn.java).
 * 2. During recovery, to process a "rollback period" (see RollbackTracker.java)
 *
 * In the JE log, the logrecs that make up a txn are chained, but each logrec
 * contains undo info that refers to the pre-txn version of the associated
 * record, which may not be the immediately previous version, if the txn writes
 * the same record multiple times. For example, a log looks like this:
 *
 * lsn       key   data         abortlsn
 * 100       A      10          null_lsn (first instance of record A)
 * 150       B      100         null_lsn (first instance of record B)
 *  .....  txn begins .....
 * 200       A      20          100
 * 300       A    deleted       100
 * 400       B     200          150
 * 500       A      30          100
 * 600       C      10          null_lsn
 *
 * When reading the log, we can find all the records in the transaction. This
 * chain exists:
 * 500->400->300->200->null_lsn
 *
 * To rollback to an arbitrary entry in the transaction, we need a chain of all
 * the records that occupied a given BIN slot during the transaction.
 * chain. The key, data, and comparators are used to determine which records
 * hash to the same slot, mimicking the btree itself.
 *
 *
 *   300      400      500     600
 *    |        |        |       |
 *   \ /      \ /      \ /     \ /
 *    
 *   200      150      300    null_lsn   revertToLsn
 *                             true       revertKD
 */
public class TxnChain {

    private final EnvironmentImpl envImpl;

    /*
     * Null if we are in recovery. Otherwise, it points to the same map as the
     * undoDatabases field of the ReplayTxn.
     */
    private final Map<DatabaseId, DatabaseImpl> undoDatabases;

    /*
     * Set of LSNs that will not be undone (i.e. the preserved portion of the
     * txn's log chain).
     */
    private final Set<Long> remainingLockedNodes;

    /*
     * For each logrec that will be undone, revertList contains a RevertInfo
     * obj, which refers to the record version to revert to. The list is 
     * ordered in reverse LSN order.
     */
    private final LinkedList<RevertInfo> revertList;

    /* The last applied VLSN in this txn, after rollback has occurred. */
    private VLSN lastValidVLSN;

    /*
     * Find the previous version for all entries in this transaction. Used by
     * recovery. This differs from the constructor used by syncup rollback
     * which is instigated by the txn. In this case, there is no cache of
     * DatabaseImpls.
     */
    public TxnChain(
        long lastLoggedLsn,
        long txnId,
        long matchpoint,
        EnvironmentImpl envImpl)  {

        this(lastLoggedLsn, txnId, matchpoint, null, envImpl);
    }

    /*
     * Find the previous version for all entries in this transaction.
     * DatabaseImpls used during txn chain creation are taken from the
     * transaction's undoDatabases cache.
     */
    public TxnChain(
        long lastLoggedLsn,
        long txnId,
        long matchpoint,
        Map<DatabaseId, DatabaseImpl> undoDatabases,
        EnvironmentImpl envImpl)
        throws DatabaseException {

        LogManager logManager = envImpl.getLogManager();

        this.envImpl = envImpl;
        this.undoDatabases = undoDatabases;

        remainingLockedNodes = new HashSet<Long>();

        /*
         * A map that stores for each record R the revert info for the
         * latest R-logrec seen during the backwards traversal of the
         * txn chain done below.
         */
        TreeMap<CompareSlot, RevertInfo> recordsMap =
            new TreeMap<CompareSlot, RevertInfo>();

        revertList = new LinkedList<RevertInfo>();

        /*
         * Traverse this txn's entire logrec chain and record revert info
         * for each logrec in the chain. Start the traversal with the last
         * logrec generated by this txn and move backwards.
         */
        long currLsn = lastLoggedLsn;

        try {
            lastValidVLSN = VLSN.NULL_VLSN;

            while (currLsn != DbLsn.NULL_LSN) {

                WholeEntry wholeEntry =
                    logManager.getLogEntryAllowInvisible(currLsn);

                LNLogEntry<?> currLogrec =
                    (LNLogEntry<?>) wholeEntry.getEntry();

                DatabaseImpl dbImpl = getDatabaseImpl(currLogrec.getDbId());

                if (dbImpl == null) {

                    if (undoDatabases != null) {
                        throw EnvironmentFailureException.unexpectedState(
                            envImpl, // fatal error, this is a corruption
                            "DB missing during non-recovery rollback, dbId=" +
                            currLogrec.getDbId() + " txnId=" + txnId);
                    }

                    /*
                     * For recovery rollback, simply skip this entry when the
                     * DB has been deleted.  This has no impact on the chain
                     * for other LNs.  This LN will not be processed by
                     * recovery rollback, since RollbackTracker.rollback
                     * ignores LNs in deleted DBs.  [#22071] [#22052]
                     */
                    currLsn = currLogrec.getUserTxn().getLastLsn();
                    continue;
                }

                currLogrec.postFetchInit(dbImpl);

                try {
                    /*
                     * Let L be the current logrec, and let R and T be the
                     * record and txn associated with L. If T wrote R again
                     * after L, let Ln be the 1st R-logrec by T after L. 
                     */
                    CompareSlot recId = new CompareSlot(dbImpl, currLogrec);

                    RevertInfo ri = recordsMap.get(recId);

                    /*
                     * If Ln exists, update the RevertInfo created earlier for
                     * Ln so that it now refers to the L version of R.
                      */
                    if (ri != null) {
                        ri.revertLsn = currLsn;
                        ri.revertKD = false;
                        ri.revertPD = currLogrec.isDeleted();

                        ri.revertKey = 
                            (dbImpl.allowsKeyUpdates() ?
                             currLogrec.getKey() : null);

                        ri.revertData =
                            (currLogrec.isEmbeddedLN() ?
                             currLogrec.getData() : null);

                        ri.revertVLSN =
                            (currLogrec.isEmbeddedLN() ?
                             currLogrec.getLN().getVLSNSequence() :
                             VLSN.NULL_VLSN_SEQUENCE);

                        ri.revertExpiration = currLogrec.getExpiration();

                        ri.revertExpirationInHours =
                            currLogrec.isExpirationInHours();
                    }

                    /*
                     * If L will be rolled back, assume that it is the 1st
                     * R-logrec by T and thus set its revert info to refer
                     * to the pre-T version of R. 
                     */
                    if (DbLsn.compareTo(currLsn, matchpoint) > 0) {

                        ri = new RevertInfo(
                            currLogrec.getAbortLsn(),
                            currLogrec.getAbortKnownDeleted(),
                            currLogrec.getAbortKey(),
                            currLogrec.getAbortData(),
                            currLogrec.getAbortVLSN(),
                            currLogrec.getAbortExpiration(),
                            currLogrec.isAbortExpirationInHours());

                        revertList.add(ri);
                        recordsMap.put(recId, ri);

                    } else {

                        /*
                         * We are done with record R, so remove it from the
                         * map, if it is still there.
                         */
                        if (ri != null) {
                            recordsMap.remove(recId);
                        }

                        remainingLockedNodes.add(currLsn);

                        if (lastValidVLSN != null &&
                            lastValidVLSN.isNull() &&
                            wholeEntry.getHeader().getVLSN() != null &&
                            !wholeEntry.getHeader().getVLSN().isNull()) {

                            lastValidVLSN = wholeEntry.getHeader().getVLSN();
                        }
                    }

                    /* Move on to the previous logrec for this txn. */
                    currLsn = currLogrec.getUserTxn().getLastLsn();

                } finally {
                    releaseDatabaseImpl(dbImpl);
                }
            }
        } catch (FileNotFoundException e) {
            throw EnvironmentFailureException.promote(
                envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                "Problem finding intermediates for txn " + txnId +
                " at lsn " + DbLsn.getNoFormatString(currLsn), e);
        }
    }

    /**
     * Hide the details of whether we are getting a databaseImpl from the txn's
     * cache, or whether we're fetching it from the dbMapTree at recovery or 
     * during master->replica transition.
     */
    private DatabaseImpl getDatabaseImpl(DatabaseId dbId) {
        if (undoDatabases != null) {
            return undoDatabases.get(dbId);
        }

        return envImpl.getDbTree().getDb(dbId);
    }

    /** Only needed if we are in recovery, and fetched the DatabaseImpl. */
    private void releaseDatabaseImpl(DatabaseImpl dbImpl) {
        if (undoDatabases == null) {
            envImpl.getDbTree().releaseDb(dbImpl);
        }
    }

    /**
     * Returns LSNs for all nodes that should remain locked by the txn.  Note
     * that when multiple versions of a record were locked by the txn, the LSNs
     * of all versions are returned.  Only the latest version will actually be
     * locked.
     */
    public Set<Long> getRemainingLockedNodes() {
        return remainingLockedNodes;
    }

    /**
     * Return information about the next item on the transaction chain and
     * remove it from the chain.
     */
    public RevertInfo pop() {
        return revertList.remove();
    }

    public VLSN getLastValidVLSN() {
        return lastValidVLSN;
    }

    @Override
    public String toString() {
        return revertList.toString();
    }

    public static class RevertInfo {

        public long revertLsn;
        public boolean revertKD;
        public boolean revertPD;
        public byte[] revertKey;
        public byte[] revertData;
        public long revertVLSN;
        public int revertExpiration;
        public boolean revertExpirationInHours;

        RevertInfo(
            long revertLsn,
            boolean revertKD,
            byte[] revertKey,
            byte[] revertData,
            long revertVLSN,
            int revertExpiration,
            boolean revertExpirationInHours) {

            this.revertLsn = revertLsn;
            this.revertKD = revertKD;
            this.revertPD = false;
            this.revertKey = revertKey;
            this.revertData = revertData;
            this.revertVLSN = revertVLSN;
            this.revertExpiration = revertExpiration;
            this.revertExpirationInHours = revertExpirationInHours;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("revertLsn=");
            sb.append(DbLsn.getNoFormatString(revertLsn));
            sb.append(" revertKD=").append(revertKD);
            sb.append(" revertPD=").append(revertPD);
            if (revertKey != null) {
                sb.append(" revertKey=");
                sb.append(Key.getNoFormatString(revertKey));
            }
            if (revertData != null) {
                sb.append(" revertData=");
                sb.append(Key.getNoFormatString(revertData));
            }
            sb.append(" revertVLSN=").append(revertVLSN);
            sb.append(" revertExpires=");
            sb.append(TTL.formatExpiration(
                revertExpiration, revertExpirationInHours));
            return sb.toString();
        }
    }

    /**
     * Compare two keys using the appropriate comparator. Keys from different
     * databases should never be equal.
     */
    public static class CompareSlot implements Comparable<CompareSlot> {

        private final DatabaseImpl dbImpl;
        private final byte[] key;

        public CompareSlot(DatabaseImpl dbImpl, LNLogEntry<?> undoEntry) {
            this(dbImpl, undoEntry.getKey());
        }

        private CompareSlot(DatabaseImpl dbImpl, byte[] key) {
            this.dbImpl = dbImpl;
            this.key = key;
        }

        public int compareTo(CompareSlot other) {
            int dbCompare = dbImpl.getId().compareTo(other.dbImpl.getId());
            if (dbCompare != 0) {
                /* LNs are from different databases. */
                return dbCompare;
            }

            /* Compare keys. */
            return Key.compareKeys(key, other.key, dbImpl.getKeyComparator());
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CompareSlot)) {
                return false;
            }
            return compareTo((CompareSlot) other) == 0;
        }

        @Override
        public int hashCode() {

            /*
             * Disallow use of HashSet/HashMap/etc.  TreeSet/TreeMap/etc should
             * be used instead when a CompareSlot is used as a key.
             *
             * Because a comparator may be configured that compares only a part
             * of the key, a hash code cannot take into account the key or
             * data, because hashCode() must return the same value for two
             * objects whenever equals() returns true.  We could hash the DB ID
             * alone, but that would not produce an efficient hash table.
             */
            throw EnvironmentFailureException.unexpectedState
                ("Hashing not supported");
        }
    }
}
