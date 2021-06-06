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
import com.sleepycat.je.utilint.VLSN;

/**
 * Transaction commit.
 */
public class TxnCommit extends VersionedWriteTxnEnd {

    public TxnCommit(long id, long lastLsn, int masterId, long dtvlsn) {
        super(id, lastLsn, masterId, dtvlsn);
        if ((masterId > 0) && (dtvlsn < VLSN.NULL_VLSN_SEQUENCE)) {
            /*
             * Note that the dtvln will be NULL when a Txn is created on a
             * master, so allow for it.
             */
            throw new IllegalStateException("DTVLSN value:" + dtvlsn);
        }
    }

    /**
     * For constructing from the log.
     */
    public TxnCommit() {
    }

    @Override
    protected String getTagName() {
        return "TxnCommit";
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof TxnCommit)) {
            return false;
        }

        TxnCommit otherCommit = (TxnCommit) other;

        return ((id == otherCommit.id) &&
                (repMasterNodeId == otherCommit.repMasterNodeId));
    }
}
