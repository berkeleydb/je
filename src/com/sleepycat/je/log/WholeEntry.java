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

package com.sleepycat.je.log;

import com.sleepycat.je.log.entry.LogEntry;

/**
 * This class packages the log entry header and the log entry "contents"
 * together for the use of components that need information from both parts.
 */
public class WholeEntry {
    private final LogEntryHeader header;
    private final LogEntry entry;

    WholeEntry(LogEntryHeader header, LogEntry entry) {
        this.header = header;
        this.entry = entry;
    }

    public LogEntryHeader getHeader() {
        return header;
    }

    public LogEntry getEntry() {
        return entry;
    }
}
