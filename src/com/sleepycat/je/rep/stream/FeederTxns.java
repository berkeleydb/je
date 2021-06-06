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

import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.ACK_WAIT_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LAST_COMMIT_TIMESTAMP;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LAST_COMMIT_VLSN;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TOTAL_TXN_MS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_NOT_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.VLSN_RATE;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.DurabilityQuorum;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.LongAvgRateStat;
import com.sleepycat.je.utilint.NoClearAtomicLongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;

/**
 * FeederTxns manages transactions that need acknowledgments.
 *
 * <p>The lastCommitVLSN, lastCommitTimestamp, and vlsnRate statistics provide
 * general information about committed transactions on the master, but are also
 * intended to be used programmatically along with other statistics for the
 * feeder to provide information about how up-to-date the replicas are.  See
 * the Feeder class for more details.
 */
public class FeederTxns {

    /** The moving average period in milliseconds */
    private static final long MOVING_AVG_PERIOD_MILLIS = 10000;

    /*
     * Tracks transactions that have not yet been acknowledged for the entire
     * replication node.
     */
    private final Map<Long, TxnInfo> txnMap;

    private final RepImpl repImpl;
    private final StatGroup statistics;
    private final AtomicLongStat txnsAcked;
    private final AtomicLongStat txnsNotAcked;
    private final AtomicLongStat ackWaitMs;
    private final AtomicLongStat totalTxnMs;
    private final NoClearAtomicLongStat lastCommitVLSN;
    private final NoClearAtomicLongStat lastCommitTimestamp;
    private final LongAvgRateStat vlsnRate;

    public FeederTxns(RepImpl repImpl) {

        txnMap = new ConcurrentHashMap<Long, TxnInfo>();
        this.repImpl = repImpl;
        statistics = new StatGroup(FeederTxnStatDefinition.GROUP_NAME,
                                   FeederTxnStatDefinition.GROUP_DESC);
        txnsAcked = new AtomicLongStat(statistics, TXNS_ACKED);
        txnsNotAcked = new AtomicLongStat(statistics, TXNS_NOT_ACKED);
        ackWaitMs = new AtomicLongStat(statistics, ACK_WAIT_MS);
        totalTxnMs = new AtomicLongStat(statistics, TOTAL_TXN_MS);
        lastCommitVLSN =
            new NoClearAtomicLongStat(statistics, LAST_COMMIT_VLSN);
        lastCommitTimestamp =
            new NoClearAtomicLongStat(statistics, LAST_COMMIT_TIMESTAMP);
        vlsnRate = new LongAvgRateStat(
            statistics, VLSN_RATE, MOVING_AVG_PERIOD_MILLIS, TimeUnit.MINUTES);
    }

    public AtomicLongStat getLastCommitVLSN() {
        return lastCommitVLSN;
    }

    public AtomicLongStat getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public LongAvgRateStat getVLSNRate() {
        return vlsnRate;
    }

    /**
     * Create a new TxnInfo so that transaction commit can wait on the latch it
     * sets up.
     *
     * @param txn identifies the transaction.
     */
    public void setupForAcks(MasterTxn txn) {
        if (txn.getRequiredAckCount() == 0) {
            /* No acks called for, no setup needed. */
            return;
        }
        TxnInfo txnInfo = new TxnInfo(txn);
        TxnInfo  prevInfo = txnMap.put(txn.getId(), txnInfo);
        assert(prevInfo == null);
    }

    /**
     * Returns the transaction if it's waiting for acknowledgments. Returns
     * null otherwise.
     */
    public MasterTxn getAckTxn(long txnId) {
        TxnInfo txnInfo = txnMap.get(txnId);
        return (txnInfo == null) ? null : txnInfo.txn;
    }

    /*
     * Clears any ack requirements associated with the transaction. It's
     * typically invoked on a transaction abort.
     */
    public void clearTransactionAcks(Txn txn) {
        txnMap.remove(txn.getId());
    }

    /**
     * Notes that an acknowledgment was received from a replica.
     *
     * @param replica the replica node
     * @param txnId the locally committed transaction that was acknowledged.
     * @param isArbiterFeeder true if feeder is an Arbiter false otherwise.
     *
     * @return the TxnInfo associated with the txnId, if txnId needs an ack,
     * null otherwise
     */
    public TxnInfo noteReplicaAck(final RepNodeImpl replica,
                                  final long txnId) {
        final DurabilityQuorum durabilityQuorum =
            repImpl.getRepNode().getDurabilityQuorum();
        if (!durabilityQuorum.replicaAcksQualify(replica)) {
            return null;
        }
        final TxnInfo txnInfo = txnMap.get(txnId);
        if (txnInfo == null) {
            return null;
        }
        txnInfo.countDown();
        return txnInfo;
    }

    /**
     * Waits for the required number of replica acks to come through.
     *
     * @param txn identifies the transaction to wait for.
     *
     * @param timeoutMs the amount of time to wait for the acknowledgments
     * before giving up.
     *
     * @throws InsufficientAcksException if the ack requirements were not met
     */
    public void awaitReplicaAcks(MasterTxn txn, int timeoutMs)
        throws InterruptedException {

        /* Record master commit information even if no acks are needed */
        final long vlsn = txn.getCommitVLSN().getSequence();
        final long ackAwaitStartMs = System.currentTimeMillis();
        lastCommitVLSN.set(vlsn);
        lastCommitTimestamp.set(ackAwaitStartMs);
        vlsnRate.add(vlsn, ackAwaitStartMs);

        TxnInfo txnInfo = txnMap.get(txn.getId());
        if (txnInfo == null) {
            return;
        }
        txnInfo.await(timeoutMs, ackAwaitStartMs);
        txnMap.remove(txn.getId());
        final RepNode repNode = repImpl.getRepNode();
        if (repNode != null) {
            repNode.getDurabilityQuorum().ensureSufficientAcks(
                txnInfo, timeoutMs);
        }
    }

    /**
     * Used to track the latch and the transaction information associated with
     * a transaction needing an acknowledgment.
     */
    public class TxnInfo {
        /* The latch used to track transaction acknowledgments. */
        final private CountDownLatch latch;
        final MasterTxn txn;

        private TxnInfo(MasterTxn txn) {
            assert(txn != null);
            final int numRequiredAcks = txn.getRequiredAckCount();
            this.latch = (numRequiredAcks == 0) ?
                null :
                new CountDownLatch(numRequiredAcks);
            this.txn = txn;
        }

        /**
         * Returns the VLSN associated with the committed txn, or null if the
         * txn has not yet been committed.
         */
        public VLSN getCommitVLSN() {
            return txn.getCommitVLSN();
        }

        private final boolean await(int timeoutMs, long ackAwaitStartMs)
            throws InterruptedException {

            boolean isZero = (latch == null) ||
                latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (isZero) {
                txnsAcked.increment();
                final long now = System.currentTimeMillis();
                ackWaitMs.add(now - ackAwaitStartMs);
                totalTxnMs.add(now - txn.getStartMs());
            } else {
                txnsNotAcked.increment();
            }
            return isZero;
        }

        public final void countDown() {
            if (latch == null) {
                return;
            }

            latch.countDown();
        }

        public final int getPendingAcks() {
            if (latch == null) {
                return 0;
            }

            return (int) latch.getCount();
        }

        public final MasterTxn getTxn() {
            return txn;
        }
    }

    public StatGroup getStats() {
        StatGroup ret = statistics.cloneGroup(false);

        return ret;
    }

    public void resetStats() {
        statistics.clear();
    }

    public StatGroup getStats(StatsConfig config) {

        StatGroup cloneStats = statistics.cloneGroup(config.getClear());

        return cloneStats;
    }
}
