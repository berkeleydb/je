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

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.ReplicatedDatabaseConfig;
import com.sleepycat.je.log.entry.DbOperationType;
import com.sleepycat.je.log.entry.NameLNLogEntry;
import com.sleepycat.je.utilint.VLSN;

/**
 * This subclass of ReplicationContext adds information specific to database
 * operations to the replication context passed from operation-aware code down
 * the the logging layer. It's a way to transport enough information though the
 * NameLNLogEntry to logically replicate database operations.
 */
public class DbOpReplicationContext extends ReplicationContext {

    /*
     * Convenience static instance used when you know this database operation
     * will not be replicated, either because it's executing on a
     * non-replicated node or it's a local operation for a local database.
     */
    public static DbOpReplicationContext NO_REPLICATE =
        new DbOpReplicationContext(false, // inReplicationStream
                                   DbOperationType.NONE);

    final private DbOperationType opType;
    private ReplicatedDatabaseConfig createConfig = null;
    private DatabaseId truncateOldDbId = null;

    /**
     * Create a replication context for logging a database operation NameLN on
     * the master.
     */
    public DbOpReplicationContext(boolean inReplicationStream,
                                  DbOperationType opType) {
        super(inReplicationStream);
        this.opType = opType;
    }

    /**
     * Create a repContext for executing a databaseOperation on the client.
     */
    public DbOpReplicationContext(VLSN vlsn,
                                  NameLNLogEntry nameLNEntry) {

        /*
         * Initialize the context with the VLSN that was shipped with the
         * replicated log entry.
         */

        super(vlsn);
        opType = nameLNEntry.getOperationType();

        if (DbOperationType.isWriteConfigType(opType)) {
            createConfig = nameLNEntry.getReplicatedCreateConfig();
        } else if (opType == DbOperationType.TRUNCATE) {
            truncateOldDbId = nameLNEntry.getTruncateOldDbId();
        }
    }

    @Override
    public DbOperationType getDbOperationType() {
        return opType;
    }

    public void setCreateConfig(ReplicatedDatabaseConfig createConfig) {
        assert(DbOperationType.isWriteConfigType(opType));
        this.createConfig = createConfig;
    }

    public ReplicatedDatabaseConfig getCreateConfig() {
        assert(DbOperationType.isWriteConfigType(opType));
        return createConfig;
    }

    public void setTruncateOldDbId(DatabaseId truncateOldDbId) {
        assert(opType == DbOperationType.TRUNCATE);
        this.truncateOldDbId = truncateOldDbId;
    }

    public DatabaseId getTruncateOldDbId() {
        assert(opType == DbOperationType.TRUNCATE);
        return truncateOldDbId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("opType=").append(opType);
        sb.append("truncDbId=").append(truncateOldDbId);
        return sb.toString();
    }
}
