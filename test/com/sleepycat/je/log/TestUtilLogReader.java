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

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Instantiates all log entries using the shared log entry instances.
 */
public class TestUtilLogReader extends FileReader {

    private LogEntryType entryType;
    private LogEntry entry;

    public TestUtilLogReader(EnvironmentImpl env)
        throws DatabaseException {

        super(env,
              4096,
              true,
              DbLsn.NULL_LSN,
              null,
              DbLsn.NULL_LSN,
              DbLsn.NULL_LSN);
    }

    public TestUtilLogReader(EnvironmentImpl env,
                             int readBufferSize,
                             boolean forward,
                             long startLsn,
                             Long singleFileNumber,
                             long endOfFileLsn,
                             long finishLsn)
        throws DatabaseException {

        super(env,
              readBufferSize,
              forward,
              startLsn,
              singleFileNumber,
              endOfFileLsn,
              finishLsn);
    }

    public LogEntryType getEntryType() {
        return entryType;
    }

    public int getEntryVersion() {
        return currentEntryHeader.getVersion();
    }

    public LogEntry getEntry() {
        return entry;
    }

    protected boolean isTargetEntry() {
        return true;
    }

    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        entryType = LogEntryType.findType(currentEntryHeader.getType());
        entry = entryType.getSharedLogEntry();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer);
        return true;
    }
}
