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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;

/**
 * The methods in this class are disabled if the GlobalCBVLSN is defunct -- see
 * {@link GlobalCBVLSN}.
 *
 * <p>When the GlobalCBVLSN is not defunct, this class supports updating the
 * group database with each node's local CBVLSN when it is in the Master
 * state. There is one instance per feeder connection, plus one for the
 * Master. There is, logically, a LocalCBVLSNTracker instance associated
 * with each instance of the updater. The instance is local for an update
 * associated with a node in the Master state and is remote for each
 * Replica.</p>
 *
 * <p>The nodeCBVLSN can only increase during the lifetime of the
 * LocalCBVLSNUpdater instance. Note however that the value of the node's
 * CBVLSN as stored in the database, which represents the values from multiple
 * updaters associated with a node over its lifetime, may both decrease and
 * increase over its lifetime. The decreases are due primarily to rollbacks,
 * and should be relatively rare.</p>
 *
 * <p>The updaters used to maintain the Replica's local CBVLSNs are stored in
 * the Feeder.InputThread. The lifetime of such a LocalCBVLSNUpdater is
 * therefore determined by the lifetime of the connection between the Master
 * and the Replica. The node CBVLSN is updated each time a heart beat response
 * is processed by the FeederInput thread. It's also updated when an old Master
 * detects that a Replica needs a network restore. In this case, it updates
 * cbvlsn to the value expected from the node after a network restore so that
 * the global CBVLSN can continue to make forward progress and not hold up the
 * cleaner.</p>
 *
 * <p>The Master maintains an updater for its own CBVLSN in the FeederManager.
 * This updater exists as long as the node retains its Master state.</p>
 *
 * <p>Local CBVLSNs are used only to contribute to the calculation of the
 * global CBVLSN. The global CBVLSN acts as the cleaner throttle on old nodes.
 * Any invariants, such as the rule that the cleaner throttle cannot regress,
 * are applied when doing the global calculation.</p>
 *
 * <p>Note that CBVLSNs are not stored in the database for secondary nodes, but
 * transient information about them is still maintained.</p>
 */
public class LocalCBVLSNUpdater {

    private static final String MASTER_SOURCE = "master";
    private static final String HEARTBEAT_SOURCE = "heartbeat";

    /*
     * Note that all reference fields below are null when then GlobalCBVLSN is
     * defunct.
     */

    /*
     * The node ID of the node whose CBLVLSN is being tracked. If this updater
     * is working on the behalf of a replica node, the nameIdPair is not the
     * name of this node.
     */
    private final NameIdPair nameIdPair;

    /** The node type of the node whose CBVLSN is being tracked. */
    private final NodeType trackedNodeType;

    /* This node; note that its node ID may be different from nodeId above. */
    private final RepNode repNode;

    /*
     * The node's local CBVLSN is cached here, for use without reading the
     * group db.
     */
    private VLSN nodeCBVLSN;

    /*
     * True if this node's local CBVLSN  has changed, but the new value
     * has not been stored into the group db yet.
     */
    private boolean updatePending;

    /* Used to suppress database updates during unit testing. */
    private static boolean suppressGroupDBUpdates = false;

    private final Logger logger;

    /* Same value as GlobalCBVLSN.defunct. */
    private final boolean defunct;

    public LocalCBVLSNUpdater(final NameIdPair nameIdPair,
                              final NodeType trackedNodeType,
                              final RepNode repNode) {
        defunct = repNode.isGlobalCBVLSNDefunct();
        if (!defunct) {
            this.nameIdPair = nameIdPair;
            this.trackedNodeType = trackedNodeType;
            this.repNode = repNode;

            logger = LoggerUtils.getLogger(getClass());
        } else {
            this.nameIdPair = null;
            this.trackedNodeType = null;
            this.repNode = null;
            logger = null;
        }
        nodeCBVLSN = NULL_VLSN;
        updatePending = false;
    }

    /**
     * Sets the current CBVLSN for this node, and trips the updatePending
     * flag so that we know there is something to store to the RepGroupDB.
     *
     * @param syncableVLSN the new local CBVLSN
     */
    private void set(VLSN syncableVLSN, String source) {
        assert !defunct;

        assert repNode.isMaster() :
               "LocalCBVLSNUpdater.set() can only be called by the master";

        if (!nodeCBVLSN.equals(syncableVLSN)) {
            LoggerUtils.fine(logger, repNode.getRepImpl(),
                             "update local CBVLSN for " + nameIdPair +
                             " from nodeCBVLSN " + nodeCBVLSN + " to " +
                             syncableVLSN + " from " + source);
            if (nodeCBVLSN.compareTo(syncableVLSN) >= 0) {

                /*
                 * LCBVLSN must not decrease, since it can result in a GCBVLSN
                 * value that's outside a truncated VLSNIndex range. See SR
                 * [#17343]
                 */
                throw EnvironmentFailureException.unexpectedState
                    (repNode.getRepImpl(),
                     "nodeCBVLSN" + nodeCBVLSN + " >= " + syncableVLSN +
                     " attempted update local CBVLSN for " + nameIdPair +
                     " from " + source);
            }
            nodeCBVLSN = syncableVLSN;
            updatePending = true;
        }
    }

    /**
     * If the GlobalCBVLSN is not defunct, sets the current CBVLSN for this
     * node. Can only be used by the master. The new cbvlsn value comes from
     * an incoming heartbeat response message. If the GlobalCBVLSN is defunct,
     * does nothing.
     *
     * @param heartbeat The incoming heartbeat response message from the
     * replica containing its newest local cbvlsn.
     */
    public void updateForReplica(Protocol.HeartbeatResponse heartbeat) {
        if (defunct) {
            return;
        }
        doUpdate(heartbeat.getSyncupVLSN(), HEARTBEAT_SOURCE);
    }

    /**
     * If the GlobalCBVLSN is not defunct, as a master, update the database
     * with the local CBVLSN for this node. This call is needed because the
     * master's local CBVLSN will not be broadcast via a heartbeat, so it
     * needs to get to the updater another way. If the GlobalCBVLSN is not
     * defunct, do nothing.
     */
    public void updateForMaster(LocalCBVLSNTracker tracker) {
        if (defunct) {
            return;
        }
        doUpdate(tracker.getBroadcastCBVLSN(), MASTER_SOURCE);
    }

    private void doUpdate(VLSN vlsn, String source) {
        assert !defunct;
        set(vlsn, source);
        repNode.getRepImpl().updateCBVLSN(this);
    }

    /**
     * If the GlobalCBVLSN is not defunct, update the database, with the local
     * CBVLSN associated with the node ID if required. Note that updates can
     * only be invoked on the master. If the GlobalCBVLSN is defunct, do
     * nothing.
     */
    public void update() {

        if (defunct) {
            return;
        }

        if (!updatePending) {
            return;
        }

        if (suppressGroupDBUpdates) {
            /* Short circuit the database update. For testing only. */
            updatePending = false;
            return;
        }

        if (repNode.isShutdownOrInvalid()) {

            /*
             * Don't advance VLSNs after a shutdown request, to minimize the
             * need for a hard recovery.
             */
            return;
        }

        try {
            VLSN candidate = nodeCBVLSN;

            if (candidate.isNull()) {
                return;
            }

            if (candidate.compareTo(repNode.getGlobalCBVLSN()) < 0) {
                /* Don't let the group CBVLSN regress.*/
                return;
            }

            final boolean updated = repNode.getRepGroupDB().updateLocalCBVLSN(
                nameIdPair, candidate, trackedNodeType);
            /* If not updated, we'll try again later. */
            if (updated) {
                updatePending = false;
            }
        } catch (EnvironmentFailureException e) {

            /*
             * Propagate environment failure exception so that the master
             * can shut down.
             */
            throw e;
        } catch (LockNotAvailableException lnae) {
            /*
             * Expected exception, due to use of nowait transaction
             */
            LoggerUtils.info(repNode.getLogger(), repNode.getRepImpl(),
                             " lock not available without waiting. " +
                             "local cbvlsn update skipped for node: " +
                              nameIdPair + " Error: " + lnae.getMessage());
        } catch (DatabaseException e) {
            LoggerUtils.warning(repNode.getLogger(), repNode.getRepImpl(),
                                "local cbvlsn update failed for node: " +
                                nameIdPair + " Error: " + e.getMessage() +
                                "\n" + LoggerUtils.getStackTrace(e));
        }
    }

    /**
     * Used during testing to suppress CBVLSN updates at this node. Note that
     * the cleaner must also typically be turned off (first) in conjunction
     * with the suppression. If multiple nodes are running in the VM all nodes
     * will have the CBVLSN updates turned off.
     * @param suppressGroupDBUpdates If true, the group DB and the group CBVLSN
     * won't be updated at the master.
     */
    public static void setSuppressGroupDBUpdates(
        boolean suppressGroupDBUpdates) {

        LocalCBVLSNUpdater.suppressGroupDBUpdates = suppressGroupDBUpdates;
    }
}
