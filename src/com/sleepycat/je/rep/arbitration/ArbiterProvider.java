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

import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.ReplicationMutableConfig;

/**
 * Provides access to arbitration services provided by different arbitration
 * mechanisms.
 */
public interface ArbiterProvider {

    /**
     * Return true if the pre-requisites are in place to permit this node to
     * enter active arbitration. Different provider implementations have
     * different criteria. For example, the DesignatedPrimaryProvider requires
     * that a node's designated primary configuration parameter is true.
     */
    public boolean activationPossible();

    /**
     * Return true if this node has successfully entered active arbitration
     * state.
     */
    public boolean attemptActivation();

    /**
     * End active arbitration.
     */
    public void endArbitration();

    /**
     * Return the election quorum size that is dictated by arbitration, for
     * this quorum policy. The arbiter provider has the leeway to decide that
     * the quorum policy takes precedence, and that arbitration does not 
     * reduce the election quorum size.
     */
    public int getElectionQuorumSize(QuorumPolicy quorumPolicy);

    /**
     * Return the durability quorum size that is dictated by arbitration, for
     * this replica ack policy. The arbiter provider has the leeway to decide
     * that the ack policy takes precedence, and that arbitration does not
     * reduce the durabilty quorum size.
     */
    public int getAckCount(ReplicaAckPolicy ackPolicy);

    /**
     * Return true if the environment configuration parameters specified in
     * newConfig indicate that this node is not qualified to remain in active
     * arbitration
     */
    public boolean shouldEndArbitration(ReplicationMutableConfig newConfig);
}