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

package com.sleepycat.je.rep.util.ldiff;

/* 
 * An object used to present the range of a different area on local and remote
 * database. 
 *
 * It uses the [key-remote-begin/data-remote-begin, different area size] to
 * present a different area on the remote database and uses the
 * [key-local-begin/data-local-begin, different area size] to present a 
 * different area on the local database.
 */
public class MismatchedRegion {
    private byte[] remoteBeginKey;
    private byte[] remoteBeginData;
    private long remoteDiffSize;
    private byte[] localBeginKey;
    private byte[] localBeginData;
    private long localDiffSize;

    public void setRemoteBeginKey(byte[] remoteBeginKey) {
        this.remoteBeginKey = remoteBeginKey;
    }

    public void setRemoteBeginData(byte[] remoteBeginData) {
        this.remoteBeginData = remoteBeginData;
    }

    public void setRemoteDiffSize(long remoteDiffSize) {
        this.remoteDiffSize = remoteDiffSize;
    }

    public void setLocalBeginKey(byte[] localBeginKey) {
        this.localBeginKey = localBeginKey;
    }

    public void setLocalBeginData(byte[] localBeginData) {
        this.localBeginData = localBeginData;
    }

    public void setLocalDiffSize(long localDiffSize) {
        this.localDiffSize = localDiffSize;
    }

    public byte[] getRemoteBeginKey() {
        return remoteBeginKey;
    }

    public byte[] getRemoteBeginData() {
        return remoteBeginData;
    }

    public long getRemoteDiffSize() {
        return remoteDiffSize;
    }

    public byte[] getLocalBeginKey() {
        return localBeginKey;
    }

    public byte[] getLocalBeginData() {
        return localBeginData;
    }

    public long getLocalDiffSize() {
        return localDiffSize;
    }

    /* 
     * Return true if the different area is an additional block on remote 
     * database. 
     */
    public boolean isRemoteAdditional() {
        return (localDiffSize == 0) ? true : false;
    }

    /*
     * Return true if the different area is an additional block on local
     * database.
     */
    public boolean isLocalAdditional() {
        return (remoteDiffSize == 0) ? true : false;
    }

    /* Present a different area is meaningless. */
    public boolean isNull() {
        return (remoteBeginKey == null) && (remoteBeginData == null) &&
               (localBeginKey == null) && (localBeginData == null) &&
               (localDiffSize == 0) && (remoteDiffSize == 0);
    }
}
