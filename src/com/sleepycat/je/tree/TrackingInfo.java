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

package com.sleepycat.je.tree;

import com.sleepycat.je.utilint.DbLsn;

/**
 * Tracking info packages some tree tracing info.
 */
public class TrackingInfo {
    public final long lsn;
    public final long nodeId;
    public final int entries;
    public int index;

    TrackingInfo(long lsn, long nodeId, int entries) {
        this.lsn = lsn;
        this.nodeId = nodeId;
        this.entries = entries;
    }

    public TrackingInfo(long lsn, long nodeId, int entries, int index) {
        this.lsn = lsn;
        this.nodeId = nodeId;
        this.entries = entries;
        this.index = index;
    }

    void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "lsn=" + DbLsn.getNoFormatString(lsn) +
            " node=" + nodeId +
            " entries=" + entries +
            " index=" + index;
    }
}
