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

package com.sleepycat.je.tree.dupConvert;

import java.nio.ByteBuffer;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.tree.Key;

/**
 * INDeleteInfo encapsulates the information logged about the removal of a
 * child from an IN during IN compression.
 *
 * As of JE 3.3.87, INDelete is no longer logged becaue the root compression
 * feature has been removed.  However, INDelete must still be processed in log
 * files created with 3.3.87 and earlier. [#17546]
 *
 * Obsolete in log version 8, only used by DupConvert and some log readers.
 */
public class INDeleteInfo implements Loggable {

    private long deletedNodeId;
    private byte[] deletedIdKey;
    private final DatabaseId dbId;

    /**
     * Used by logging system only.
     */
    public INDeleteInfo() {
        dbId = new DatabaseId();
    }

    /*
     * Accessors.
     */
    public long getDeletedNodeId() {
        return deletedNodeId;
    }

    public byte[] getDeletedIdKey() {
        return deletedIdKey;
    }

    public DatabaseId getDatabaseId() {
        return dbId;
    }

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        deletedNodeId = LogUtils.readLong(itemBuffer, unpacked);
        deletedIdKey = LogUtils.readByteArray(itemBuffer, unpacked);
        dbId.readFromLog(itemBuffer, entryVersion);
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<INDeleteEntry node=\"").append(deletedNodeId);
        sb.append("\">");
        sb.append(Key.dumpString(deletedIdKey, 0));
        dbId.dumpLog(sb, verbose);
        sb.append("</INDeleteEntry>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }
}
