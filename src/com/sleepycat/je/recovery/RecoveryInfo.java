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

package com.sleepycat.je.recovery;

import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * RecoveryInfo keeps information about recovery processing.
 */
public class RecoveryInfo {

    /* Locations found during recovery. */
    public long lastUsedLsn = DbLsn.NULL_LSN;      // location of last entry
    /*  EOF, location of first unused spot. */
    public long nextAvailableLsn = DbLsn.NULL_LSN;
    public long firstActiveLsn = DbLsn.NULL_LSN;
    public long checkpointStartLsn = DbLsn.NULL_LSN;
    public long checkpointEndLsn = DbLsn.NULL_LSN;
    public long useRootLsn = DbLsn.NULL_LSN;

    /*
     * Represents the first CkptStart following the CkptEnd.  It is a CkptStart
     * with no CkptEnd, and is used for counting provisional INs obsolete.
     */
    public long partialCheckpointStartLsn = DbLsn.NULL_LSN;

    /* Checkpoint record used for this recovery. */
    public CheckpointEnd checkpointEnd;

    /* Ids */
    public long useMinReplicatedNodeId;
    public long useMaxNodeId;
    public long useMinReplicatedDbId;
    public long useMaxDbId;
    public long useMinReplicatedTxnId;
    public long useMaxTxnId;

    /* VLSN mappings seen during recovery processing, for replication. */
    public VLSNRecoveryProxy vlsnProxy;

    /*
     * The last reserved file that did not exist at recovery time, and its last
     * VLSN. The file could be missing because it was not included in a network
     * restore, or deleted by DbDeleteReservedFiles, or even manually deleted
     * when the environment was closed. The VLSN index must be truncated after
     * this file before being used.
     */
    public long lastMissingFileNumber = -1;
    public VLSN lastMissingFileVLSN = VLSN.NULL_VLSN;

    /**
     * ReplayTxns that are resurrected during recovery processing, for
     * replication. Txnid -> replayTxn
     */
    public final Map<Long, Txn> replayTxns = new HashMap<Long, Txn>();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Recovery Info ");
        appendLsn(sb, " firstActive=", firstActiveLsn);
        appendLsn(sb, " ckptStart=", checkpointStartLsn);
        appendLsn(sb, " ckptEnd=", checkpointEndLsn);
        appendLsn(sb, " lastUsed=", lastUsedLsn);
        appendLsn(sb, " nextAvail=", nextAvailableLsn);
        appendLsn(sb, " useRoot=", useRootLsn);
        sb.append(checkpointEnd);
        sb.append(" useMinReplicatedNodeId=").append(useMinReplicatedNodeId);
        sb.append(" useMaxNodeId=").append(useMaxNodeId);
        sb.append(" useMinReplicatedDbId=").append(useMinReplicatedDbId);
        sb.append(" useMaxDbId=").append(useMaxDbId);
        sb.append(" useMinReplicatedTxnId=").append(useMinReplicatedTxnId);
        sb.append(" useMaxTxnId=").append(useMaxTxnId);
        return sb.toString();
    }

    private void appendLsn(StringBuilder sb, String name, long lsn) {
        if (lsn != DbLsn.NULL_LSN) {
            sb.append(name).append(DbLsn.getNoFormatString(lsn));
        }
    }
}
