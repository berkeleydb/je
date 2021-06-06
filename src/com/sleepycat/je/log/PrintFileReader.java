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

/**
 * The PrintFileReader prints out the target log entries.
 */
public class PrintFileReader extends DumpFileReader {

    /**
     * Create this reader to start at a given LSN.
     */
    public PrintFileReader(EnvironmentImpl env,
                           int readBufferSize,
                           long startLsn,
                           long finishLsn,
                           long endOfFileLsn,
                           String entryTypes,
                           String dbIds,
                           String txnIds,
                           boolean verbose,
                           boolean repEntriesOnly,
                           boolean forwards)
        throws DatabaseException {

        super(env,
              readBufferSize,
              startLsn,
              finishLsn,
              endOfFileLsn,
              entryTypes,
              dbIds,
              txnIds,
              verbose,
              repEntriesOnly,
              forwards);
    }

    /**
     * This reader prints the log entry item.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        /* Figure out what kind of log entry this is */
        LogEntryType type =
            LogEntryType.findType(currentEntryHeader.getType());

        /* Read the entry. */
        LogEntry entry = type.getSharedLogEntry();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer);

        /* Match according to command line args. */
        if (!matchEntry(entry)) {
            return true;
        }

        /* Dump it. */
        StringBuilder sb = new StringBuilder();
        sb.append("<entry lsn=\"0x").append
            (Long.toHexString(window.currentFileNum()));
        sb.append("/0x").append(Long.toHexString(currentEntryOffset));
        sb.append("\" ");
        currentEntryHeader.dumpLogNoTag(sb, verbose);
        sb.append("\">");
        entry.dumpEntry(sb, verbose);
        sb.append("</entry>");
        System.out.println(sb.toString());

        return true;
    }
}
