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

package com.sleepycat.je.txn;

import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Timestamp;

/**
 * The base class for records that mark the end of a transaction.
 */
public abstract class TxnEnd implements Loggable {

    long id;
    Timestamp time;
    long lastLsn;

    /* For replication - master node which wrote this record. */
    int repMasterNodeId;

    /**
     * The txn commit VLSN that was acknowledged by at least a majority of the
     * nodes either at the time of this commit, or eventually via a heartbeat.
     * This VLSN must typically be less than the VLSN associated with the
     * TxnEnd itself, when it's written to the log. In cases of mixed mode
     * operation (when a pre-DTVLSN is serving as a feeder to a DTVLSN aware
     * replica) it may be equal to the VLSN associated with the TxnEnd.
     */
    long dtvlsn;

    TxnEnd(long id, long lastLsn, int repMasterNodeId, long dtvlsn) {
        this.id = id;
        time = new Timestamp(System.currentTimeMillis());
        this.lastLsn = lastLsn;
        this.repMasterNodeId = repMasterNodeId;
        this.dtvlsn = dtvlsn;
    }

    /**
     * For constructing from the log
     */
    public TxnEnd() {
        lastLsn = DbLsn.NULL_LSN;
    }

    /*
     * Accessors.
     */
    public long getId() {
        return id;
    }

    public Timestamp getTime() {
        return time;
    }

    long getLastLsn() {
        return lastLsn;
    }

    public int getMasterNodeId() {
        return repMasterNodeId;
    }

    @Override
    public long getTransactionId() {
        return id;
    }

    public long getDTVLSN() {
        return dtvlsn;
    }

    public void setDTVLSN(long dtvlsn) {
        this.dtvlsn = dtvlsn;
    }

    /**
     * Returns true if there are changes that have been logged for this entry.
     * It's unusual for such a record to not have associated changes, since
     * such commit/abort entries are typically optimized away. When present
     * they typically represent records used to persist uptodate DTVLSN
     * information as part of the entry.
     */
    public boolean hasLoggedEntries() {
        return (lastLsn != DbLsn.NULL_LSN);
    }

    protected abstract String getTagName();
}
