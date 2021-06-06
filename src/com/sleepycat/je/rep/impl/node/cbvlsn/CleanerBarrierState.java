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

import java.io.Serializable;

import com.sleepycat.je.utilint.VLSN;

/**
 * This class is not used if the GlobalCBVLSN is defunct -- see
 * {@link GlobalCBVLSN}. Instances of this class are created in order to
 * initialize the RepNodeImpl.barrierState field or pass parameters, but these
 * instances are not actually used when the GlobalCBVLSN is defunct.
 *
 * Encapsulates the last known syncup state associated with a node.
 */
public class CleanerBarrierState implements Serializable {
    private static final long serialVersionUID = 1L;

    /*
     * The latest sync position of this node in the replication stream.
     * This position is approximate and is updated on some regular basis.
     * It is conservative in that the node is likely to have a newer sync
     * point. So it represents a lower bound for its sync point.
     */
    private final VLSN lastLocalCBVLSN;

    /*
     * The time that the sync point was last recorded. Note that clocks
     * should be reasonably synchronized.
     */
    private final long barrierTime;

    public CleanerBarrierState(VLSN lastLocalCBVLSN, long barrierTime) {
        super();
        this.lastLocalCBVLSN = lastLocalCBVLSN;
        this.barrierTime = barrierTime;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
                (lastLocalCBVLSN == null ? 0 : lastLocalCBVLSN.hashCode());
        result = prime * result +
                 (int) (barrierTime ^ (barrierTime >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CleanerBarrierState other = (CleanerBarrierState) obj;
        if (lastLocalCBVLSN == null) {
            if (other.lastLocalCBVLSN != null) {
                return false;
            }
        } else if (!lastLocalCBVLSN.equals(other.lastLocalCBVLSN)) {
            return false;
        }
        if (barrierTime != other.barrierTime) {
            return false;
        }
        return true;
    }

    public VLSN getLastCBVLSN() {
        return lastLocalCBVLSN;
    }

    public long getBarrierTime() {
        return barrierTime;
    }

    @Override
    public String toString() {
        return String.format("LocalCBVLSN:%,d at:%tc",
                             lastLocalCBVLSN.getSequence(), barrierTime);
    }
}
