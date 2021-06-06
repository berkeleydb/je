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

/**
 * Transaction abort.
 */
public class TxnAbort extends VersionedWriteTxnEnd {

    public TxnAbort(long id, long lastLsn, int masterId, long dtvlsn) {
        super(id, lastLsn, masterId, dtvlsn);
    }

    /**
     * For constructing from the log.
     */
    public TxnAbort() {
    }

    @Override
    protected String getTagName() {
        return "TxnAbort";
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof TxnAbort))
            return false;

        TxnAbort otherAbort = (TxnAbort) other;

        return ((id == otherAbort.id) &&
                (repMasterNodeId == otherAbort.repMasterNodeId));
    }
}
