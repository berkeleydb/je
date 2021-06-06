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

import java.nio.ByteBuffer;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;

/**
 * Holds a partial BIN that serves as a live BIN delta.
 *
 * A live delta (unlike a the obsolete OldBINDelta, which is contained in an
 * OldBINDeltaLogEntry) may appear in the Btree to serve as an incomplete BIN.
 */
public class BINDeltaLogEntry extends INLogEntry<BIN> {

    public BINDeltaLogEntry(Class<BIN> logClass) {
        super(logClass);
    }

    /**
     * When constructing an entry for writing to the log, use LOG_BIN_DELTA.
     */
    public BINDeltaLogEntry(BIN bin) {
        super(bin, true /*isBINDelta*/);
    }

    /**
     * Used to write a pre-serialized log entry.
     */
    public BINDeltaLogEntry(final ByteBuffer bytes,
                            final long lastFullLsn,
                            final long lastDeltaLsn,
                            final LogEntryType logEntryType,
                            final IN parent) {
        super(bytes, lastFullLsn, lastDeltaLsn, logEntryType, parent);
    }

    /*
     * Whether this LogEntry reads/writes a BIN-Delta logrec.
     */
    @Override
    public boolean isBINDelta() {
        return true;
    }
}
