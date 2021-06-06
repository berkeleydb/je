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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ReplicationContext;

/**
 * A PreparedTxn is used at recovery for processing a TXN_PREPARE log entry. It
 * is provides essentially the same functionality as a TXN but lets the calling
 * code set the transaction id.
 */
public class PreparedTxn extends Txn {

    private PreparedTxn(EnvironmentImpl envImpl,
                       TransactionConfig config,
                       long mandatedId)
        throws DatabaseException {

        super(envImpl, config, ReplicationContext.NO_REPLICATE, mandatedId);
    }

    public static PreparedTxn createPreparedTxn(EnvironmentImpl envImpl,
                                                TransactionConfig config,
                                                long mandatedId)
        throws DatabaseException {

        PreparedTxn ret = null;
        try {
            ret = new PreparedTxn(envImpl, config, mandatedId);
        } catch (DatabaseException DE) {
            ret.close(false);
            throw DE;
        }
        return ret;
    }

    /**
     * PrepareTxns use the mandated id.
     */
    @Override
    protected long generateId(TxnManager txnManager, long mandatedId) {
        return mandatedId;
    }
}
