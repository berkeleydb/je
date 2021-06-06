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

package com.sleepycat.je.utilint;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import com.sleepycat.je.log.BasicVersionedWriteLoggable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.VersionedWriteLoggable;

/**
 * This class writes out a log entry that can be used for replication syncup.
 * It can be issued arbitrarily by the master at any point, in order to bound
 * the syncup interval in much the way that a checkpoint bounds the recovery
 * interval. The entry will a replicated one, which means that it will be
 * tagged with a VLSN.
 *
 * Although this is a replication class, it resides in the utilint package
 * because it is referenced in LogEntryType.java.
 *
 * TODO: This is currently not used. When it is used, it will be the first
 * replicated log entry that does not have a real txn id. All replicated
 * entries are expected to have negative ids, and the matchpoint should be
 * exempt from Replay.updateSequences, or it should pass in a special reserved
 * negative id, so as not to incur the assertion in Replay.updateSequences,
 * that the txn id is <0.
 */
public class Matchpoint extends BasicVersionedWriteLoggable {

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    /* Time of issue. */
    private Timestamp time;

    /* For replication - master node which wrote this record. */
    private int repMasterNodeId;

    public Matchpoint(int repMasterNodeId) {
        this.repMasterNodeId = repMasterNodeId;
        time = new Timestamp(System.currentTimeMillis());
    }

    /**
     * For constructing from the log.
     */
    public Matchpoint() {
    }

    public int getMasterNodeId() {
        return repMasterNodeId;
    }

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
        return LogUtils.getTimestampLogSize(time) +
            LogUtils.getPackedIntLogSize(repMasterNodeId);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        LogUtils.writeTimestamp(logBuffer, time);
        LogUtils.writePackedInt(logBuffer, repMasterNodeId);
    }

    @Override
    public void readFromLog(ByteBuffer logBuffer, int entryVersion) {
        time = LogUtils.readTimestamp(logBuffer, false /* isUnpacked. */);
        repMasterNodeId = LogUtils.readInt(logBuffer, false /* unpacked */);
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<Matchpoint");
        sb.append("\" time=\"").append(time);
        sb.append("\" master=\"").append(repMasterNodeId);
        sb.append("\">");
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public boolean logicalEquals(Loggable other) {
        if (!(other instanceof Matchpoint)) {
            return false;
        }

        Matchpoint otherMatchpoint = (Matchpoint) other;
        return (otherMatchpoint.time.equals(time) &&
                (otherMatchpoint.repMasterNodeId == repMasterNodeId));
    }
}
