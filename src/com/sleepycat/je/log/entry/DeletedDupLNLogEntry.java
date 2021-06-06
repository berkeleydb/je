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

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;

/**
 * DupDeletedLNEntry encapsulates a deleted dupe LN entry. This contains all
 * the regular transactional LN log entry fields and an extra key, which is the
 * nulled out data field of the LN (which becomes the key in the duplicate
 * tree.
 *
 * WARNING: Obsolete in version 8, only used by some log readers.
 *
 * TODO Move to dupConvert package, after testing is complete.
 */
public class DeletedDupLNLogEntry extends LNLogEntry<LN> {

    /*
     * Deleted duplicate LN must log an extra key in their log entries,
     * because the data field that is the "key" in a dup tree has been
     * nulled out because the LN is deleted.
     */
    private byte[] dataAsKey;

    /**
     * Constructor to read an entry.
     */
    public DeletedDupLNLogEntry() {
        super(com.sleepycat.je.tree.LN.class);
    }

    @Override
    byte[] combineDupKeyData() {
        return DupKeyData.combine(getKey(), dataAsKey);
    }

    /**
     * Extends its super class to read in the extra dup key.
     */
    @Override
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {

        readBaseLNEntry(envImpl, header, entryBuffer, 
                        false /*keyIsLastSerializedField*/);

        /* Key */
        int logVersion = header.getVersion();
        dataAsKey = LogUtils.readByteArray(entryBuffer, (logVersion < 6));
    }

    /**
     * Extends super class to dump out extra key.
     */
    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {
        super.dumpEntry(sb, verbose);
        sb.append(Key.dumpString(dataAsKey, 0));
        return sb;
    }

    /*
     * Writing support
     */

    @Override
    public boolean hasReplicationFormat() {
        return false;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return false;
    }

    @Override
    public int getSize(final int logVersion, final boolean forReplication) {
        throw EnvironmentFailureException.unexpectedState();
    }

    @Override
    public void writeEntry(final ByteBuffer destBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        throw EnvironmentFailureException.unexpectedState();
    }
}
