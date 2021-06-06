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
 * SearchFileReader searches for the a given entry type.
 */
public class SearchFileReader extends FileReader {

    private LogEntryType targetType;
    private LogEntry logEntry;

    /**
     * Create this reader to start at a given LSN.
     */
    public SearchFileReader(EnvironmentImpl env,
                            int readBufferSize,
                            boolean forward,
                            long startLsn,
                            long endOfFileLsn,
                            LogEntryType targetType)
        throws DatabaseException {

        super(env, readBufferSize, forward, startLsn, null,
              endOfFileLsn, DbLsn.NULL_LSN);

        this.targetType = targetType;
        logEntry = targetType.getNewLogEntry();
    }

    /**
     * @return true if this is a targeted entry.
     */
    @Override
    protected boolean isTargetEntry() {
        return (targetType.equalsType(currentEntryHeader.getType()));
    }

    /**
     * This reader instantiate the first object of a given log entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        logEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
        return true;
    }

    /**
     * @return the last object read.
     */
    public Object getLastObject() {
        return logEntry.getMainItem();
    }
}
