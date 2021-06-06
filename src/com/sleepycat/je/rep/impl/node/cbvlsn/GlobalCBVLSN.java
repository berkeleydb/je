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

import static com.sleepycat.je.rep.impl.RepParams.MIN_RETAINED_VLSNS;
import static com.sleepycat.je.rep.impl.RepParams.REP_STREAM_TIMEOUT;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.impl.MinJEVersionUnsupportedException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;

/**
 * The GlobalCBVLSN was used prior to JE 7.5 for ensuring that nodes do not
 * delete files needed for feeding other nodes. The methods in this class are
 * disabled if the GlobalCBVLSN is defunct -- see below.
 *
 * <p>In version 7.5 and greater we instead retain all files up to the disk
 * limits just in case they are needed for replication, and the GlobalCBVLSN
 * is no longer needed. The {@link com.sleepycat.je.cleaner.FileProtector} is
 * used for preventing file deletion on a local node. See [#25220] in the
 * JE 7.5 change log for a list of related external changes.</p>
 *
 * <p>The global and local CBVLSNs are completely defunct (no longer
 * maintained) in a rep group where all nodes are running JE 7.5 or greater.
 * In a mixed version group of new (7.5 or later) and old (pre-7.5) nodes,
 * we must continue to maintain the GlobalCBVLSN for the sake of the old
 * nodes. If we did not do this, for example, the following scenario could
 * occur:
 * <pre>
 *  - RF-3, N1 is new, N2 and N3 are old.
 *  - N1 is the master, N2 is an up-to-date replica, N3 is a lagging replica.
 *  - N2 deletes files that would be needed to feed N3, because the GlobalVLSN
 *    is not updated and therefore N2 doesn't know that N3 is lagging.
 *  - N1 goes down, N2 is elected master
 *  - N3 requires a network restore because N2 doesn't have the files that N3
 *    needs for syncup.
 *</pre>
 *
 * <p>The c.s.je.rep.impl.node.cbvlsn package contains most of the code for
 * maintaining the CBVLSN. This code is isolated here for two reasons:</p>
 * <ul>
 *     <li>To make it clear that it is not in play for groups of all new nodes.
 *     We don't have to think about the CBVLSN at all in this case.</li>
 *     <li>To make it easier to eventually remove this code entirely, when
 *     backward compatibility with JE 7.4 and earlier is dropped.</li>
 *</ul>
 *
 * <p>When it is not yet defunct, the GlobalCBVLSN class represents this
 * node's view of the global CBVLSN. Each node has its own view of the global
 * CBVLSN, based upon its local replicated copy of the rep group db. There is
 * a single instance of the GlobalCBVLSN and it exists for the lifetime of the
 * RepNode.</p>
 *
 * <p>A global CBVLSN is a per-environment value, and is safeguarded from
 * decreasing during the lifetime of a single RepImpl. Because nodes crash and
 * re-sync, and new nodes join, it's possible that the persisted local cbvlsns
 * can decrease, and drag the global cbvlsn value down, but those decreases
 * are ignored during the lifetime of the global CBVLSN instance.</p>
 *
 * <p>On pre-7.5 nodes, the global CBVLSN is used by:</p>
 * <ul>
 *     <li>As a barrier to file deletion by the Cleaner.</li>
 *     <li>The Feeder which only serves log records in the interval:
 *     [GlobalCBVLSN .. VLSNRange.last]</li>
 *     <li>The Replica which uses the interval [GlobalCBVLSN .. VLSNRange.last]
 *     at syncup time.</li>
 *</ul>
 */
public class GlobalCBVLSN {

    /*
     * The GlobalCBVLSN is defunct when all nodes in the group are running this
     * version or later.
     */
    private static final JEVersion DEFUNCT_JE_VERSION = new JEVersion("7.5.0");

    private final RepImpl repImpl;
    private final Logger logger;
    private final long streamTimeoutMs;
    private final int minRetainedVLSNs;
    private volatile boolean defunct = false;

    /* GroupCBVLSN can only be updated when synchronized. */
    private volatile VLSN groupCBVLSN = VLSN.NULL_VLSN;

    public GlobalCBVLSN(RepNode repNode) {

        this.repImpl = repNode.getRepImpl();
        streamTimeoutMs =
            repImpl.getConfigManager().getDuration(REP_STREAM_TIMEOUT);
        minRetainedVLSNs =
            repImpl.getConfigManager().getInt(MIN_RETAINED_VLSNS);
        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Uses minJEVersion to determine whether the global CBVLSN is defunct.
     *
     * Try to avoid any use of the GlobalCBVLSN when all nodes are
     * running new software:
     *
     * - For the master, this method should be called using the minJEVersion
     *   in the rep group DB.
     *
     * - For a replica, it should be called using the minJEVersion of the
     *   feeder, from the feeder handshake.
     *
     * See {@link #setDefunctJEVersion(RepNode)} for how the minJEVersion is
     * set for a new group (with new software) and for an upgraded group.
     *
     * @param minJEVersion the min JE version for the group, or null if
     * unknown.
     */
    public void init(RepNode repNode, JEVersion minJEVersion) {

        if (repNode.getRepImpl().getConfigManager().getBoolean(
            RepParams.TEST_CBVLSN)) {
            /* Should never transition from defunct to !defunct. */
            assert !defunct;
            return;
        }

        if (minJEVersion != null &&
            DEFUNCT_JE_VERSION.compareTo(minJEVersion) <= 0) {
            defunct = true;
        } else {
            /* Should never transition from defunct to !defunct. */
            assert !defunct;
        }
    }

    /**
     * Calls RepNode.setMinJEVersion to upgrade the min version to
     * DEFUNCT_JE_VERSION. This allows a group to treat the global CBLVSN as
     * defunct when all the electable nodes in the group have been upgraded.
     *
     * This method should be called when the first node in a a new group is
     * created to avoid all use of the GlobalVLSN. It should also be called
     * periodically on the master, as feeders become active, to update the min
     * JE version after all nodes in a group are upgraded.
     *
     * If the version update fails because not all nodes are upgraded, or
     * there is no quorum for the group DB update, or there is a lock
     * conflict, this method fails silently and should be called again later.
     */
    public void setDefunctJEVersion(RepNode repNode) {

        if (repNode.getRepImpl().getConfigManager().getBoolean(
            RepParams.TEST_CBVLSN)) {
            return;
        }

        try {
            repNode.setMinJEVersion(DEFUNCT_JE_VERSION);
            defunct = true;
        } catch (MinJEVersionUnsupportedException |
                 OperationFailureException e) {
            /* Fail silently -- will try again later. */
        }
    }

    /**
     * Returns true if all nodes in the group are running JE 7.5 or later, and
     * therefore the GlobalCBVLSN is not maintained. Returns false for a mixed
     * old/new version group (or when TEST_GLOBAL_CBVLSN is true).
     */
    public boolean isDefunct() {
        return defunct;
    }

    /**
     * Returns the global CBVLSN if it is not defunct. Returns a null if it is
     * defunct.
     *
     * <p>For sake of old nodes, the global CBVLSN is computed as the minimum
     * of CBVLSNs after discarding CBVLSNs that are obsolete. A CBVLSN is
     * considered obsolete if it has not been updated within a configurable
     * time interval relative to the time that the most recent CBVLSN was
     * updated.</p>
     */
    public VLSN getCBVLSN() {
        return defunct ? VLSN.NULL_VLSN : groupCBVLSN;
    }

    /**
     * Updates the cached group info for the node, avoiding a database read,
     * if the global CBVLSN is not defunct. If it is defunct, does nothing.
     *
     * @param updateNameIdPair the node whose localCBVLSN must be updated.
     * @param barrierState the new node syncup state
     */
    public void updateGroupInfo(NameIdPair updateNameIdPair,
                                CleanerBarrierState barrierState) {

        if (defunct) {
            return;
        }

        RepGroupImpl group = repImpl.getRepNode().getGroup();
        RepNodeImpl node = group.getMember(updateNameIdPair.getName());
        if (node == null) {
            /*  A subsequent refresh will get it, along with the new node. */
            return;
        }

        LoggerUtils.fine(logger, repImpl,
            "LocalCBVLSN for " + updateNameIdPair +
                " updated to " + barrierState +
                " from " + node.getBarrierState().getLastCBVLSN());
        node.setBarrierState(barrierState);
        recalculate(group);
    }

    /**
     * Recalculates the GlobalVLSN when it is not defunct. When it is defunct,
     * does nothing.
     *
     * <p>For sake of old nodes, the globalCBVLSN is computed as it was
     * earlier: the minimum of CBVLSNs after discarding CBVLSNs that are
     * obsolete. A CBVLSN is considered obsolete if it has not been updated
     * within a configurable time interval relative to the time that the
     * most recent CBVLSN was updated.</p>
     *
     * <p>Note that the read of GroupInfo is not protected, and that groupInfo
     * could be changing. That's okay, because we guarantee that none of the
     * local CBVLSNs can be LT globalCBVLSN. If a local CBVLSN is written, and
     * we miss it, it only means that this recalculation of global CBVLSN is
     * too pessimistic -- it's too low.</p>
     *
     * <p>Secondary nodes do not appear in the RepGroupDB, but the feeder has
     * local CBVLSN values for them which are part of this calculation.
     * Secondary nodes and new nodes have their VLSN ranges protected by the
     * mechanism which tries to ensure that files which may be needed by active
     * feeders are not candidates for deletion.</p>
     */
    public void recalculate(RepGroupImpl groupInfo) {

        if (defunct) {
            return;
        }

        /* Find the time the highest CBVLSN was computed. */
        VLSN maxCBVLSN = NULL_VLSN;
        long latestBarrierTime = 0;
        String nodeName = null;
        for (RepNodeImpl node : groupInfo.getDataMembers()) {

            CleanerBarrierState nodeBarrier = node.getBarrierState();
            VLSN cbvlsn = nodeBarrier.getLastCBVLSN();

            if ((cbvlsn == null) || cbvlsn.isNull()) {
                continue;
            }

            /*
             * Count all nodes when finding the max time, including old nodes
             * that are in the middle of syncup and have not established their
             * low point .
             */
            final long nodeBarrierTime = nodeBarrier.getBarrierTime();

            if (maxCBVLSN.compareTo(cbvlsn) <= 0) {

                /*
                 * Use min, since it represents the real change when they are
                 * equal.
                 */
                latestBarrierTime = cbvlsn.equals(maxCBVLSN) ?
                    Math.min(nodeBarrierTime, latestBarrierTime) :
                    nodeBarrierTime;
                maxCBVLSN = cbvlsn;

                /*
                 * Track the name of the node holding the maximum CBVLSN, since
                 * that node pins that VLSN
                 */
                nodeName = node.getName();
            }
        }

        if (latestBarrierTime == 0) {
            /* No cbvlsns entered yet, don't bother to recalculate. */
            return;
        }

        if (maxCBVLSN.isNull()) {
            /* No cbvlsns entered yet, don't bother to recalculate. */
            return;
        }

        /*
         * Now find the min CBVLSN that has not been timed out. This may mean
         * that the min CBVLSN == NULL_VLSN, for old nodes that have not yet
         * finished syncup.
         */
        VLSN newGroupCBVLSN = maxCBVLSN;
        long nodeBarrierTime = 0;
        for (RepNodeImpl node : groupInfo.getDataMembers()) {

            CleanerBarrierState nodeBarrier = node.getBarrierState();
            VLSN nodeCBVLSN = nodeBarrier.getLastCBVLSN();

            if ((nodeCBVLSN == null) || nodeCBVLSN.isNull()) {
                continue;
            }

            if (((latestBarrierTime - nodeBarrier.getBarrierTime()) <=
                 streamTimeoutMs) &&
                (newGroupCBVLSN.compareTo(nodeCBVLSN) > 0)) {
                newGroupCBVLSN = nodeCBVLSN;

                /*
                 * A node is pinning the CBVLSN because it is lagging and has
                 * not timed out
                 */
                nodeName = node.getName();
                nodeBarrierTime = nodeBarrier.getBarrierTime();
            }
        }

        /*
         * Adjust to retain min number of VLSNs, while ensuring we stay within
         * the current VLSN range.
         */
        newGroupCBVLSN =
            new VLSN(newGroupCBVLSN.getSequence() - minRetainedVLSNs);

        final VLSNIndex vlsnIndex = repImpl.getVLSNIndex();
        final VLSN rangeFirst = (vlsnIndex != null) ?
            vlsnIndex.getRange().getFirst() : VLSN.FIRST_VLSN;

        /*
         * Environments where the minRetainedVLSNs was expanded need to ensure
         * the global cbvlsn still stays within the vlsn range.
         */
        if (rangeFirst.compareTo(newGroupCBVLSN) > 0) {
            newGroupCBVLSN = rangeFirst;
        }

        updateGroupCBVLSN(groupInfo, newGroupCBVLSN, nodeName,
                          nodeBarrierTime, latestBarrierTime);
    }

    /*
     * Update the group CBVLSN, but only if the newGroupCBVLSN is more recent
     * This is to ensure that the group CBVLSN can only advance during the
     * lifetime of this instance.
     */
    private void updateGroupCBVLSN(RepGroupImpl groupInfo,
                                   VLSN newGroupCBVLSN,
                                   String nodeName,
                                   long nodeBarrierTime,
                                   long latestBarrierTime) {
        assert !defunct;
        boolean changed = false;
        String cbvlsnLoweredMessage = null;
        VLSN oldCBVLSN = VLSN.NULL_VLSN;

        synchronized(this) {

            /*
             * Be sure not to do anything expensive in this synchronized
             * section, such as logging.
             */
            if (newGroupCBVLSN.compareTo(groupCBVLSN) > 0) {
                VLSNRange currentRange = repImpl.getVLSNIndex().getRange();
                if (!currentRange.contains(newGroupCBVLSN) &&
                    logger.isLoggable(Level.FINE)) {
                    cbvlsnLoweredMessage =
                        "GroupCBVLSN: " + newGroupCBVLSN +
                        " is outside VLSN range: " + currentRange +
                        " Current group:" + groupInfo;
                } else {
                    oldCBVLSN = groupCBVLSN;
                    groupCBVLSN = newGroupCBVLSN;
                    changed = true;
                }
            }
        }

        if (logger.isLoggable(Level.FINE)) {
            if (cbvlsnLoweredMessage != null) {
                LoggerUtils.fine(logger, repImpl, cbvlsnLoweredMessage);
            }

            if (changed) {
                LoggerUtils.fine(logger, repImpl,
                                 "Global CBVLSN changed from " + oldCBVLSN +
                                 " to " + newGroupCBVLSN);
            }
        }
    }

    /**
     * Returns a VLSN appropriate for the RestoreResponse.cbvlsn field when the
     * GlobalCBVLSN is not defunct. When it is defunct, returns a null VLSN.
     *
     * <p>When sending a RestoreResponse to a (potentially) old node, we
     * supply a VLSN that will cause selection of reasonably current nodes as a
     * server (feeder) for the network restore. The return value is the VLSN
     * range end of this node (the master) minus a value that will allow
     * up-to-date servers to qualify (NETWORKBACKUP_MAX_LAG). Old nodes
     * will reject servers whose VLSN range does not cover this VLSN.</p>
     *
     * <p>In JE 7.4 and earlier, the "group CBVLSN" was used for this value.
     * This was incorrect, because it was the lower bound for lagging replicas
     * and reserved files. In JE 7.5 we improve on this by sending the
     * value described above, when older nodes must be supported.</p>
     *
     * <p>When GlobalCBLVSN is defunct, a null VLSN is returned because the
     * RestoreResponse.cbvlsn field is not used at all. See the updated
     * 'Algorithm' in {@link NetworkRestore}.</p>
     */
    public VLSN getRestoreResponseVLSN(final VLSNRange range) {

        if (defunct) {
            return VLSN.NULL_VLSN;
        }

        final long vlsn = range.getLast().getSequence() -
            repImpl.getConfigManager().getInt(RepParams.NETWORKBACKUP_MAX_LAG);

        return new VLSN(Math.max(0, vlsn));
    }
}
