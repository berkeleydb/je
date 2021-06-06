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

package com.sleepycat.je.rep.arbitration;

import java.util.logging.Logger;

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Designated Primary arbitration relies on the configuration parameter
 * je.rep.designatedPrimary. This form of arbitration is only effective when
 * the electable group size is 2. When one of the two electable nodes goes
 * down, the remaining node is permitted to win elections, retain authoritative
 * mastership, and commit transactions without any participation from its dead
 * sibling, if and only if it has been configured as designated primary.
 * <p>
 * The user is responsible for ensuring that only one node at any time is
 * annointed as the designated primary. There is some sanity checking that
 * designated primary is only set by one node by master/replica syncups.  The
 * parameter is mutable.
 */
public class DesignatedPrimaryProvider implements ArbiterProvider {

    private final RepImpl repImpl;

    private final Logger logger;

    DesignatedPrimaryProvider(RepImpl repImpl) {
        this.repImpl = repImpl;
        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Try to activate this node as a Primary, if it has been configured as
     * such and if the group size is two. This method is invoked when an
     * operation falls short of quorum requirements and is ready to trade
     * durability for availability. More specifically it's invoked when an
     * election fails, or there is an insufficient number of replicas during
     * a begin transaction or a transaction commit.
     *
     * Active arbitration ends when the Non-Primary contacts it.
     *
     * @return true if the primary was activated.
     */
    @Override
    public boolean attemptActivation() {
        if (checkDesignatedPrimary()) {
            LoggerUtils.info(logger, repImpl,
                             "Primary activated; quorum is one.");
            return true;
        }

        LoggerUtils.fine(logger, repImpl,
                         "Attempted unsuccessfully to activate designated " +
                         "primary");
        return false;
    }

    /**
     * Return true if this node is in a 2-node group and is configured as the
     * Designated Primary, and is therefore capable of entering active
     * arbitration.
     */
    @Override
    public boolean activationPossible() {
        return checkDesignatedPrimary();
    }

    /**
     * No cleannup is needed when exiting designated primary arbitration.
     */
    @Override
    public void endArbitration() {
        /* nothing to do. */
    }

    /**
     * Check the electable group size and the designated primary configuration
     * to see if this node has the option of becoming the primary.
     */
    private boolean checkDesignatedPrimary() {
        return (repImpl != null) && /* repImpl can be null in unit tests */
            repImpl.isDesignatedPrimary() &&
            repImpl.getRepNode().getGroup().getElectableGroupSize() == 2;
    }

    /**
     * When operating under designated primary arbitration, the election quorum
     * is 1 for a group with electable group size of 2,
     */
    @Override
    public int getElectionQuorumSize(QuorumPolicy quorumPolicy) {
        return 1;
    }

    /**
     * Always returns 0, no replica acks are needed when acting under
     * designated primary arbitration.
     * TODO: is this still true with non voting nodes?
     */
    @Override
    public int getAckCount(ReplicaAckPolicy ackPolicy) {
        return 0;
    }

    /**
     * Return true if this node is no longer configured as the designated
     * primary under the new configuration.
     */
    @Override
    public boolean shouldEndArbitration(ReplicationMutableConfig newConfig) {
        return (!newConfig.getDesignatedPrimary());
    }
}
