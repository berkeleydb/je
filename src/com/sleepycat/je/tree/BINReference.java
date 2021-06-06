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

import com.sleepycat.je.dbi.DatabaseId;

/**
 * A class that embodies a reference to a BIN that does not rely on a
 * Java reference to the actual BIN.
 */
public class BINReference {
    private final byte[] idKey;
    private final long nodeId;
    private final DatabaseId databaseId;

    BINReference(final long nodeId,
                 final DatabaseId databaseId,
                 final byte[] idKey) {
        this.nodeId = nodeId;
        this.databaseId = databaseId;
        this.idKey = idKey;
    }

    public long getNodeId() {
        return nodeId;
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public byte[] getKey() {
        return idKey;
    }

    /**
     * Compare two BINReferences.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof BINReference)) {
            return false;
        }

        return ((BINReference) obj).nodeId == nodeId;
    }

    @Override
    public int hashCode() {
        return (int) nodeId;
    }

    @Override
    public String toString() {
        return "idKey=" + Key.getNoFormatString(idKey) +
            " nodeId = " + nodeId +
            " db=" + databaseId;
    }
}
