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

package com.sleepycat.je.dbi;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.BasicVersionedWriteLoggable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.utilint.StringUtils;

/**
 * DatabaseImpl Ids are wrapped in a class so they can be logged.
 */
public class DatabaseId extends BasicVersionedWriteLoggable
        implements Comparable<DatabaseId> {

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    /**
     * The unique id of this database.
     */
    private long id;

    /**
     *
     */
    public DatabaseId(long id) {
        this.id = id;
    }

    /**
     * Uninitialized database id, for logging.
     */
    public DatabaseId() {
    }

    /**
     * @return id value
     */
    public long getId() {
        return id;
    }

    /**
     * @return id as bytes, for use as a key
     */
    public byte[] getBytes()
        throws DatabaseException {

        return StringUtils.toUTF8(toString());
    }

    /**
     * Compare two DatabaseImpl Id's.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DatabaseId)) {
            return false;
        }

        return ((DatabaseId) obj).id == id;
    }

    @Override
    public int hashCode() {
        return (int) id;
    }

    @Override
    public String toString() {
        return Long.toString(id);
    }

    /**
     * see Comparable#compareTo
     */
    @Override
    public int compareTo(DatabaseId o) {
        if (o == null) {
            throw EnvironmentFailureException.unexpectedState("null arg");
        }

        if (id == o.id) {
            return 0;
        } else if (id > o.id) {
            return 1;
        } else {
            return -1;
        }
    }

    /*
     * Logging support.
     */

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Collections.emptyList();
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return LogUtils.getPackedLongLogSize(id);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        LogUtils.writePackedLong(logBuffer, id);
    }

    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        if (entryVersion < 6) {
            id = LogUtils.readInt(itemBuffer);
        } else {
            id = LogUtils.readPackedLong(itemBuffer);
        }
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<dbId id=\"");
        sb.append(id);
        sb.append("\"/>");
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof DatabaseId))
            return false;

        return id == ((DatabaseId) other).id;
    }
}
