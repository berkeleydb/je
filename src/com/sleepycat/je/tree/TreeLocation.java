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

/*
 * TreeLocation is a cursor like object that keeps track of a location
 * in a tree. It's used during recovery.
 */
public class TreeLocation {

    public BIN bin;         // parent BIN for the target LN
    public int index;       // index of where the LN is or should go
    public byte[] lnKey;    // the key that represents this LN in this BIN
    public long childLsn = DbLsn.NULL_LSN; // current LSN value in that slot.
    public int childLoggedSize;
    public boolean isKD = false;
    public boolean isEmbedded = false;

    public void reset() {
        bin = null;
        index = -1;
        lnKey = null;
        childLsn = DbLsn.NULL_LSN;
        childLoggedSize = 0;
        isKD = false;
        isEmbedded = false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("<TreeLocation bin=\"");
        if (bin == null) {
            sb.append("null");
        } else {
            sb.append(bin.getNodeId());
        }
        sb.append("\" index=\"");
        sb.append(index);
        sb.append("\" lnKey=\"");
        sb.append(Key.dumpString(lnKey,0));
        sb.append("\" childLsn=\"");
        sb.append(DbLsn.toString(childLsn));
        sb.append("\" childLoggedSize=\"");
        sb.append(childLoggedSize);
        sb.append("\" isKD=\"");
        sb.append(isKD);
        sb.append("\" isEmbedded=\"");
        sb.append(isEmbedded);
        sb.append("\">");
        return sb.toString();
    }
}
