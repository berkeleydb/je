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

package com.sleepycat.je.dbi;

import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.utilint.DbLsn;

/**
 * NodeSequence encapsulates the generation and maintenance of a sequence for
 * generating node IDs, transient LSNs and other misc sequences.
 */
public class NodeSequence {

    public static final int FIRST_LOCAL_NODE_ID = 1;
    public static final int FIRST_REPLICATED_NODE_ID = -10;

    /*
     * Node IDs: We need to ensure that local and replicated nodes use
     * different number spaces for their ids, so there can't be any possible
     * conflicts.  Local, non replicated nodes use positive values starting
     * with 1, replicated nodes use negative values starting with -10.
     *
     * Node ID values from 0 to -9 are reserved.  0 is not used and should be
     * avoided.  -1 is used to mean null or none, and should be used via the
     * Node.NULL_NODE_ID constant.  -2 through -9 are reserved for future use.
     *
     * The local and replicated node ID sequences are initialized by the first
     * pass of recovery, after the log has been scanned for the latest used
     * node ID.
     */
    private AtomicLong lastAllocatedLocalNodeId = null;
    private AtomicLong lastAllocatedReplicatedNodeId = null;

    /*
     * Transient LSNs are used for not-yet-logged DeferredWrite records and
     * for the EOF record used for Serializable isolation. Transient LSNs are
     * used to provide unique locks, and are only used during the life of an
     * environment, for non-persistent objects.
     */
    private final AtomicLong lastAllocatedTransientLsnOffset =
        new AtomicLong(0L);

    public final EnvironmentImpl envImpl;

    /* Transient sequences. */
    private final AtomicLong nextBackupId = new AtomicLong(0L);
    private final AtomicLong nextDatabaseCountId = new AtomicLong(0L);
    private final AtomicLong nextDiskOrderedCursorId = new AtomicLong(0L);
    private final AtomicLong nextNetworkRestoreId = new AtomicLong(0L);

    public NodeSequence(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
    }

    /**
     * Initialize the counters in these methods rather than a constructor
     * so we can control the initialization more precisely.
     */
    void initRealNodeId() {
        lastAllocatedLocalNodeId = new AtomicLong(FIRST_LOCAL_NODE_ID - 1);
        lastAllocatedReplicatedNodeId =
            new AtomicLong(FIRST_REPLICATED_NODE_ID + 1);
    }

    /**
     * The last allocated local and replicated node IDs are used for ckpts.
     */
    public long getLastLocalNodeId() {
        return lastAllocatedLocalNodeId.get();
    }

    public long getLastReplicatedNodeId() {
        return lastAllocatedReplicatedNodeId.get();
    }

    /**
     * We get a new node ID of the appropriate kind when creating a new node.
     */
    public long getNextLocalNodeId() {
        return lastAllocatedLocalNodeId.incrementAndGet();
    }

    /*
    public long getNextReplicatedNodeId() {
        return lastAllocatedReplicatedNodeId.decrementAndGet();
    }
    */

    /**
     * Initialize the node IDs, from recovery.
     */
    public void setLastNodeId(long lastReplicatedNodeId,
                              long lastLocalNodeId) {
        lastAllocatedReplicatedNodeId.set(lastReplicatedNodeId);
        lastAllocatedLocalNodeId.set(lastLocalNodeId);
    }

    /*
     * Tracks the lowest replicated node ID used during a replay of the
     * replication stream, so that it's available as the starting point if this
     * replica transitions to being the master.
     */
    public void updateFromReplay(long replayNodeId) {
        assert !envImpl.isMaster();
        if (replayNodeId > 0 && !envImpl.isRepConverted()) {
           throw EnvironmentFailureException.unexpectedState
               ("replay node id is unexpectedly positive " + replayNodeId);
        }

        if (replayNodeId < lastAllocatedReplicatedNodeId.get()) {
            lastAllocatedReplicatedNodeId.set(replayNodeId);
        }
    }

    /**
     * Assign the next available transient LSN.
     */
    public long getNextTransientLsn() {
        return DbLsn.makeTransientLsn
            (lastAllocatedTransientLsnOffset.getAndIncrement());
    }

    public long getNextBackupId() {
        return nextBackupId.getAndIncrement();
    }

    public long getNextDatabaseCountId() {
        return nextDatabaseCountId.getAndIncrement();
    }

    public long getNextDiskOrderedCursorId() {
        return nextDiskOrderedCursorId.getAndIncrement();
    }

    public long getNextNetworkRestoreId() {
        return nextNetworkRestoreId.getAndIncrement();
    }
}
