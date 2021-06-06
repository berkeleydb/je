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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.ReplicatedDatabaseConfig;
import com.sleepycat.je.log.DbOpReplicationContext;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.VLSN;

/**
 * NameLNLogEntry contains all the regular LNLogEntry fields and additional
 * information about the database operation which instigated the logging of
 * this NameLN. This additional information is used to support replication of
 * database operations in a replication group.
 *
 * Database operations pose a special problem for replication because unlike
 * data record put and get calls, they can result in multiple log entries that
 * are not all members of a single transaction.  Create and truncate are the
 * problem operations because they end up logging new MapLNs, and our
 * implementation does not treat MapLNs as transactional.  Database operations
 * challenge two replication assumptions: (a) that all logical operations can
 * be repeated on the client node based on the contents of a single log entry,
 * and (b) that non-txnal log entries like MapLNs need not be replicated.
 *
 * Specifically, here's what is logged for database operations.
 *
 * create:
 *
 *  1. new NameLN_TX
 *  2. new MapLN, which has the database config info.
 *  3. txn commit of autocommit or user txn.
 *
 * rename:
 *
 *  1. deleted NameLN_TX
 *  2. new NameLN_TX
 *  3. txn commit from autocommit or user txn
 *
 * truncate:
 *
 *  1. new MapLN w/new id
 *  2. modify the existing NameLN with new id (old database is deleted by
 *     usual commit-time processing)
 *  3. txn commit from autocommit or user txn
 *
 * delete
 *
 *  1. deleted NameLN_TX (old database gets deleted by usual commit-time
 *     processing)
 *  2. txn commit from autocommit or user txn
 *
 * Extra information is needed for create and truncate, which both log
 * information within the MapLN. Rename and delete only log NameLNs, so they
 * can be replicated on the client using the normal replication messages.  The
 * extra fields which follow the usual LNLogEntry fields are:
 *
 * operationType - the type of database operation. In a single node system,
 *                 this is local information implicit in the code path.
 * databaseConfig (optional) - For creates, database configuration info
 * databaseId (optional)- For truncates, the old db id, so we know which
 *                        MapLN to delete.
 */
public class NameLNLogEntry extends LNLogEntry<NameLN> {

    /**
     * The log version of the most recent format change for this entry,
     * including the superclass and any changes to the format of referenced
     * loggables.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 12;

    /*
     * operationType, truncateOldDbId and replicatedCreateConfig are
     * logged as part of the entry.
     */
    private DbOperationType operationType;
    private DatabaseId truncateOldDbId;
    private ReplicatedDatabaseConfig replicatedCreateConfig;

    /**
     * Constructor to read an entry.
     */
    public NameLNLogEntry() {
        super(com.sleepycat.je.tree.NameLN.class);
    }

    /**
     * Constructor to write this entry.
     */
    public NameLNLogEntry(
        LogEntryType entryType,
        DatabaseId dbId,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] key,
        NameLN nameLN,
        ReplicationContext repContext) {

        super(
            entryType, dbId, txn,
            abortLsn, abortKD,
            null/*abortKey*/, null/*abortData*/,
            VLSN.NULL_VLSN_SEQUENCE/*abortVLSN*/,
            0 /*abortExpiration*/, false /*abortExpirationInHours*/,
            key, nameLN, false/*newEmbeddedLN*/,
            0 /*expiration*/, false /*expirationInHours*/);

        ReplicationContext operationContext = repContext;

        operationType = repContext.getDbOperationType();
        if (DbOperationType.isWriteConfigType(operationType)) {
            replicatedCreateConfig =
                ((DbOpReplicationContext) operationContext).getCreateConfig();
        }

        if (operationType == DbOperationType.TRUNCATE) {
            truncateOldDbId =
              ((DbOpReplicationContext) operationContext).getTruncateOldDbId();
        }
    }

    /**
     * Extends its super class to read in database operation information.
     */
    @Override
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {

        readBaseLNEntry(envImpl, header, entryBuffer,
                        false /*keyIsLastSerializedField*/);

        /*
         * The NameLNLogEntry was introduced in version 6. Before, a LNLogEntry
         * was used for NameLNs, and there is no extra information in the log
         * entry.
         */
        int version = header.getVersion();
        if (version >= 6) {
            operationType = DbOperationType.readTypeFromLog(entryBuffer,
                                                            version);
            if (DbOperationType.isWriteConfigType(operationType)) {
                replicatedCreateConfig = new ReplicatedDatabaseConfig();
                replicatedCreateConfig.readFromLog(entryBuffer, version);
            }

            if (operationType == DbOperationType.TRUNCATE) {
                truncateOldDbId = new DatabaseId();
                truncateOldDbId.readFromLog(entryBuffer, version);
            }
        } else {
            operationType = DbOperationType.NONE;
        }
    }

    /**
     * Extends its super class to dump database operation information.
     */
    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {

        super.dumpEntry(sb, verbose);

        operationType.dumpLog(sb, verbose);
        if (replicatedCreateConfig != null ) {
            replicatedCreateConfig.dumpLog(sb, verbose);
        }
        if (truncateOldDbId != null) {
            truncateOldDbId.dumpLog(sb, verbose);
        }

        return sb;
    }

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        final Collection<VersionedWriteLoggable> list =
            new ArrayList<>(super.getEmbeddedLoggables());
        list.addAll(Arrays.asList(
            new NameLN(), DbOperationType.NONE,
            new ReplicatedDatabaseConfig()));
        return list;
    }

    @Override
    public int getSize(final int logVersion, final boolean forReplication) {

        int size = getBaseLNEntrySize(
            logVersion, false /*keyIsLastSerializedField*/,
            forReplication);

        size += operationType.getLogSize(logVersion, forReplication);

        if (DbOperationType.isWriteConfigType(operationType)) {
            size += replicatedCreateConfig.getLogSize(
                logVersion, forReplication);
        }

        if (operationType == DbOperationType.TRUNCATE) {
            size += truncateOldDbId.getLogSize(logVersion, forReplication);
        }
        return size;
    }

    @Override
    public void writeEntry(final ByteBuffer destBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        writeBaseLNEntry(
            destBuffer, logVersion,
            false /*keyIsLastSerializedField*/, forReplication);

        operationType.writeToLog(destBuffer, logVersion, forReplication);

        if (DbOperationType.isWriteConfigType(operationType)) {
            replicatedCreateConfig.writeToLog(
                destBuffer, logVersion, forReplication);
        }

        if (operationType == DbOperationType.TRUNCATE) {
            truncateOldDbId.writeToLog(destBuffer, logVersion, forReplication);
        }
    }

    @Override
    public boolean logicalEquals(LogEntry other) {

        if (!super.logicalEquals(other))
            return false;

        NameLNLogEntry otherEntry = (NameLNLogEntry) other;
        if (!operationType.logicalEquals(otherEntry.operationType)) {
            return false;
        }

        if ((truncateOldDbId != null) &&
            (!truncateOldDbId.logicalEquals(otherEntry.truncateOldDbId))) {
                return false;
        }

        if (replicatedCreateConfig != null) {
            if (!replicatedCreateConfig.logicalEquals
                (otherEntry.replicatedCreateConfig))
                return false;
        }
        return true;
    }

    public DbOperationType getOperationType() {
        return operationType;
    }

    public ReplicatedDatabaseConfig getReplicatedCreateConfig() {
        return replicatedCreateConfig;
    }

    public DatabaseId getTruncateOldDbId() {
        return truncateOldDbId;
    }

    @Override
    public void dumpRep(StringBuilder sb) {
        super.dumpRep(sb);
        sb.append(" dbop=").append(operationType);
    }
}
