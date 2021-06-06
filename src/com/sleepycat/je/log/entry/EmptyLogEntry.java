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

import com.sleepycat.je.log.Loggable;

/**
 * Contains no information, implying that the LogEntryType is the only
 * information needed.
 * <p>
 * A single byte is actually written, but this is only to satisfy non-null
 * buffer dependencies in ChecksumValidator and file readers.
 */
public class EmptyLogEntry implements Loggable {

    public EmptyLogEntry() {
    }

    public int getLogSize() {
        return 1;
    }

    public void writeToLog(ByteBuffer logBuffer) {
        logBuffer.put((byte) 42);
    }

    public void readFromLog(ByteBuffer logBuffer, int entryVersion) {
        logBuffer.get();
    }

    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<Empty/>");
    }

    /**
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    public long getTransactionId() {
        return 0;
    }
}
