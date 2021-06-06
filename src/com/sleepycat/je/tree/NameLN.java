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

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.NameLNLogEntry;
import com.sleepycat.je.txn.Txn;

/**
 * A NameLN represents a Leaf Node in the name->database id mapping tree.
 */
public final class NameLN extends LN {

    private static final String BEGIN_TAG = "<nameLN>";
    private static final String END_TAG = "</nameLN>";

    private DatabaseId id;
    private boolean deleted;

    /**
     * In the ideal world, we'd have a base LN class so that this NameLN
     * doesn't have a superfluous data field, but we want to optimize the LN
     * class for size and speed right now.
     */
    public NameLN(DatabaseId id) {
        super(new byte[0]);
        this.id = id;
        deleted = false;
    }

    /**
     * Create an empty NameLN, to be filled in from the log.
     */
    public NameLN() {
        super();
        id = new DatabaseId();
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    void makeDeleted() {
        deleted = true;
    }

    public DatabaseId getId() {
        return id;
    }

    public void setId(DatabaseId id) {
        this.id = id;
    }

    /*
     * Dumping
     */

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.dumpString(nSpaces, dumpTags));
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<deleted val=\"").append(Boolean.toString(deleted));
        sb.append("\">");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<id val=\"").append(id);
        sb.append("\">");
        sb.append('\n');
        return sb.toString();
    }

    /*
     * Logging
     */

    /**
     * Return the correct log entry type for a NameLN depends on whether it's
     * transactional.
     */
    @Override
    protected LogEntryType getLogType(boolean isInsert,
                                      boolean isTransactional, DatabaseImpl db) {
        return isTransactional ? LogEntryType.LOG_NAMELN_TRANSACTIONAL : 
                                 LogEntryType.LOG_NAMELN;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        final Collection<VersionedWriteLoggable> list =
            new ArrayList<>(super.getEmbeddedLoggables());
        list.add(new DatabaseId());
        return list;
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return
            super.getLogSize(logVersion, forReplication) +
            id.getLogSize(logVersion, forReplication) +
            1;               // deleted flag
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        super.writeToLog(logBuffer, logVersion, forReplication);
        id.writeToLog(logBuffer, logVersion, forReplication);
        byte booleans = (byte) (deleted ? 1 : 0);
        logBuffer.put(booleans);
    }

    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        super.readFromLog(itemBuffer, entryVersion); // super class
        id.readFromLog(itemBuffer, entryVersion); // id
        byte booleans = itemBuffer.get();
        deleted = (booleans & 1) != 0;
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof NameLN)) {
            return false;
        }

        NameLN otherLN = (NameLN) other;

        if (!super.logicalEquals(otherLN)) {
            return false;
        }

        if (!(id.equals(otherLN.id))) {
            return false;
        }

        if (deleted != otherLN.deleted) {
            return false;
        }

        return true;
    }

    /**
     * Dump additional fields. Done this way so the additional info can be
     * within the XML tags defining the dumped log entry.
     */
    @Override
    protected void dumpLogAdditional(StringBuilder sb, boolean verbose) {
        id.dumpLog(sb, true);
    }

    /*
     * Each LN knows what kind of log entry it uses to log itself. Overridden
     * by subclasses.
     */
    @Override
    LNLogEntry<?> createLogEntry(
        LogEntryType entryType,
        DatabaseImpl dbImpl,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        byte[] newKey,
        boolean newEmbeddedLN,
        int newExpiration,
        boolean newExpirationInHours,
        ReplicationContext repContext) {

        return new NameLNLogEntry(entryType,
                                  dbImpl.getId(),
                                  txn,
                                  abortLsn,
                                  abortKD,
                                  newKey,
                                  this,
                                  repContext);
    }
}
