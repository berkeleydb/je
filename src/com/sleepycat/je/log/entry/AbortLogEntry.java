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

package com.sleepycat.je.log.entry;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.txn.TxnAbort;

/**
 * Log entry for a transaction abort.
 */
public class AbortLogEntry extends SingleItemReplicableEntry<TxnAbort> {

    /**
     * The log version number of the most recent change for this log entry,
     * including any changes to the format of the underlying {@link TxnAbort}
     * object.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 13;

    /** Construct a log entry for reading a {@link TxnAbort} object. */
    public AbortLogEntry() {
        super(TxnAbort.class);
    }

    /** Construct a log entry for writing a {@link TxnAbort} object. */
    public AbortLogEntry(final TxnAbort abort) {
        super(LogEntryType.LOG_TXN_ABORT, abort);
    }

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }
}
