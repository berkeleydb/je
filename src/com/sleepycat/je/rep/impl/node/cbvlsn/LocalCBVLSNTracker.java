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

package com.sleepycat.je.rep.impl.node.cbvlsn;

import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * The methods in this class are disabled if the GlobalCBVLSN is defunct -- see
 * {@link GlobalCBVLSN}.
 *
 * <p>When GlobalCBVLSN} is not defunct, the LocalCBVLSNTracker tracks this
 * node's local CBVLSN. Each node has a single tracker instance.</p>
 *
 * <p>The GlobalCBVLSN must be durable. Since the GlobalCBVLSN is derived from
 * the LocalCBVLSN, we need to make the LocalCBVLSN durable too. [#18728]</p>
 * <ol>
 *     <li>For HA, the GlobalCbvlsn is supposed to ensure that the replication
 *     stream is always available for replay, across failovers.</li>
 *</ol>
 *
 * <p>The local CBVLSN is maintained by each node. Replicas periodically update
 * the Master with their current CBVLSN via a response to a heartbeat message
 * from the Master, where it is managed by the LocalCBVLSNUpdater and
 * flushed out to RepGroup database, whenever the updater notices that it
 * has changed. The change is then effectively broadcast to all the Replicas
 * including the originating Replica, via the replication stream. For this
 * reason, the CBVLSN for the node as represented in the RepGroup database
 * will generally lag the value contained in the tracker.</p>
 *
 * <p>Note that track() api is invoked in critical code with locks being
 * held and must be lightweight.</p>
 *
 * <p>Local CBVLSNs are used only to contribute to the calculation of the
 * global CBVLSN. The global CBVLSN acts as the cleaner throttle on old nodes.
 * Any invariants, such as the rule that the cleaner throttle cannot regress,
 * are applied when doing the global calculation. In addition, we enforce
 * the rule against regressing local CBVLSNs here.</p>
 */
public class LocalCBVLSNTracker {

    /*
     * Note that all reference fields below are null when then GlobalCBVLSN is
     * defunct.
     */

    /* Used to keep track of the last fsynced matchable VLSN. */
    private VLSN lastSyncableVLSN;

    /**
     * Final syncable VLSN from the penultimate log file.
     */
    private VLSN currentLocalCBVLSN;

    /*
     * We really only need to update the localCBVLSN once per file. currentFile
     * is used to determine if this is the first VLSN in the file.
     */
    private long currentFile;

    /* Test hook for disabling LocalCBVLSN changes. */
    private boolean allowUpdate = true;

    /* Same value as GlobalCBVLSN.defunct. */
    private final boolean defunct;

    public LocalCBVLSNTracker(RepNode repNode, GlobalCBVLSN globalCBVLSN) {
        defunct = globalCBVLSN.isDefunct();

        if (!defunct) {
            VLSNIndex vlsnIndex = repNode.getRepImpl().getVLSNIndex();

            /* Initialize the currentLocalCBVLSN and lastSyncableVLSN. */
            currentLocalCBVLSN = vlsnIndex.getRange().getLastSync();
            lastSyncableVLSN = currentLocalCBVLSN;

            /* Approximation good enough to start with. */
            currentFile = DbLsn.getFileNumber(DbLsn.NULL_LSN);
        }
    }

    /* Test hook, disable the LocalCBVLSN updates. */
    public void setAllowUpdate(boolean allowUpdate) {
        this.allowUpdate = allowUpdate;
    }

    /**
     * If GlobalCBVLSN is defunct, does nothing.
     *
     * <p>If GlobalCBVLSN is defunct, tracks barrier VLSNs, updating the
     * local CBVLSN if the associated log file has changed. When tracking is
     * done on a replica, the currentLocalCBVLSN value is ultimately sent
     * via heartbeat response to the master, which updates the RepGroupDb.
     * When tracking is done on a master, the update is done on this node.</p>
     *
     * <p>The update is only done once per file in order to decrease the cost
     * of tracking. Since we want the local cbvlsn to be durable, we use the
     * last vlsn in the penultimate log file as the local cbvlsn value. We know
     * the penultimate log file has been fsynced, and therefore the last vlsn
     * within that file has also been fsynced.</p>
     *
     * <p>Tracking can be called quite often, and should be lightweight.</p>
     *
     * @param newVLSN
     * @param lsn
     */
    public void track(VLSN newVLSN, long lsn) {
        if (defunct || !allowUpdate) {
            return;
        }

        synchronized (this) {
            if (newVLSN.compareTo(lastSyncableVLSN) > 0) {
                VLSN old = lastSyncableVLSN;
                lastSyncableVLSN = newVLSN;
                if (DbLsn.getFileNumber(lsn) != currentFile) {
                    currentFile = DbLsn.getFileNumber(lsn);
                    currentLocalCBVLSN = old;
                }
            }
        }
    }

    /**
     * If the GlobalVLSN is not defunct, initialize the local CBVLSN with the
     * syncup matchpoint, so that the heartbeat responses sent before the node
     * has replayed any log entries are still valid for saving a place in the
     * replication stream. If the GlobalVLSN is defunct, do nothing.
     *
     * @param matchpoint
     */
    public void registerMatchpoint(VLSN matchpoint) {
        if (defunct) {
            return;
        }
        this.currentLocalCBVLSN = matchpoint;
        this.lastSyncableVLSN = matchpoint;
    }

    /**
     * @return the local CBVLSN for broadcast from replica to master on the
     * heartbeat response, or a null VLSN if the GlobalVLSN is defunct.
     */
    public VLSN getBroadcastCBVLSN() {
        return defunct ? VLSN.NULL_VLSN : currentLocalCBVLSN;
    }

    /**
     * @return last syncable VLSN seen by this tracker, or a null VLSN if the
     * GlobalVLSN is defunct. Note that this VLSN has not yet been broadcast --
     * see {@link #track}.
     */
    public VLSN getLastSyncableVLSN() {
        return defunct ? VLSN.NULL_VLSN : lastSyncableVLSN;
    }
}
