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

package com.sleepycat.je.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.DumpFileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.DbLsn;

/**
 * TestDumper is a sample custom dumper, of the kind that can be specified by
 * DbPrintLog -c <custom dumper>.
 * 
 * To try the custom dumper:
 * java -cp "build/test/classes;build/classes" com.sleepycat.je.util.DbPrintLog
 *            -h <env> -c com.sleepycat.je.util.TestDumper
 * TestDumper will list a count of log entries, the log entry type, and the lsn.
 */
public class TestDumper extends DumpFileReader {
    public static final String SAVE_INFO_FILE = "dumpLog";

    private int counter = 0;

    public TestDumper(EnvironmentImpl env,
                      Integer readBufferSize,
                      Long startLsn,
                      Long finishLsn,
                      Long endOfFileLsn,
                      String entryTypes,
                      String txnIds,
                      Boolean verbose,
                      Boolean repEntriesOnly,
                      Boolean forwards) 
        throws DatabaseException {

        super(env, readBufferSize, startLsn, finishLsn, endOfFileLsn, 
            entryTypes, null /*dbIds*/, txnIds, verbose,  repEntriesOnly,
            forwards);
    }

    @Override
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        /* Figure out what kind of log entry this is */
        byte curType = currentEntryHeader.getType();
        LogEntryType lastEntryType = LogEntryType.findType(curType);

        /* Read the entry and dump it into a string buffer. */
        LogEntry entry = lastEntryType.getSharedLogEntry();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer); 

        writePrintInfo((lastEntryType + " lsn=" +
                       DbLsn.getNoFormatString(getLastLsn())));

        return true;
    }

    private void writePrintInfo(String message) {
        PrintWriter out = null;
        try {
            File savedFile = 
                new File(envImpl.getEnvironmentHome(), SAVE_INFO_FILE);
            out = new PrintWriter
                (new BufferedWriter(new FileWriter(savedFile, true)));
            out.println(message);
        } catch (Exception e) {
            throw new IllegalStateException("Exception happens while " +
                                            "writing information into the " + 
                                            "info file " + e.getMessage());
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    throw new IllegalStateException("Run into exception " +
                            "while closing the BufferedWriter: " + 
                            e.getMessage());
                }
            }
        }
    }
}
