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

package com.sleepycat.je;

import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE_TXNS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_BEGINS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_COMMITS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XAABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XACOMMITS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XAPREPARES;

import java.io.Serializable;

import com.sleepycat.je.utilint.StatGroup;

/**
 * Transaction statistics for a database environment.
 */
public class TransactionStats implements Serializable {

    private static final long serialVersionUID = 2136955031L;

    private StatGroup stats;

    /**
     * @hidden
     * Internal use only.
     */
    public TransactionStats(StatGroup stats) {
        this.stats = stats;
    }

    /**
     * The Active class represents an active transaction.
     */
    public static class Active implements Serializable {

        private static final long serialVersionUID = 1286693589L;

        /**
         * The transaction ID of the transaction.
         */
        private long txnId;

        /**
         * The transaction ID of the parent transaction (or 0, if no parent).
         */
        private long parentId;

        /**
         * The transaction name, including the thread name if available.
         */
        private String name;

        /**
         * The transaction ID of the transaction.
         */
        public long getId() {
            return txnId;
        }

        /**
         * The transaction ID of the parent transaction (or 0, if no parent).
         */
        public long getParentId() {
            return parentId;
        }

        /**
         * The transaction name, including the thread name if available.
         */
        public String getName() {
            return name;
        }

        /**
         * @hidden
         * Internal use only.
         */
        public Active(String name, long txnId, long parentId) {
            this.name = name;
            this.txnId = txnId;
            this.parentId = parentId;
        }

        @Override
        public String toString() {
            return "txnId = " + txnId + " txnName = " + name;
        }
    }

    /**
     * Return the array of active transactions.
     *
     * @return The array of active transactions.
     */
    public Active[] getActiveTxns() {
        return stats.getActiveTxnArray(TXN_ACTIVE_TXNS);
    }

    /**
     * The number of transactions that have aborted.
     */
    public long getNAborts() {
        return stats.getLong(TXN_ABORTS);
    }

    /**
     * The number of XA transactions that have aborted.
     */
    public long getNXAAborts() {
        return stats.getLong(TXN_XAABORTS);
    }

    /**
     * The number of XA transactions that have been prepared.
     */
    public long getNXAPrepares() {
        return stats.getLong(TXN_XAPREPARES);
    }

    /**
     * The number of transactions that are currently active.
     */
    public int getNActive() {
        return stats.getInt(TXN_ACTIVE);
    }

    /**
     * The number of transactions that have begun.
     */
    public long getNBegins() {
        return stats.getLong(TXN_BEGINS);
    }

    /**
     * The number of transactions that have committed.
     */
    public long getNCommits() {
        return stats.getLong(TXN_COMMITS);
    }

    /**
     * The number of XA transactions that have committed.
     */
    public long getNXACommits() {
        return stats.getLong(TXN_XACOMMITS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return stats.toString();
    }

    public String toStringVerbose() {
        return stats.toStringVerbose();
    }
}
