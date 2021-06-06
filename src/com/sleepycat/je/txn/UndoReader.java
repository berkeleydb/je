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

package com.sleepycat.je.txn;

import java.util.Map;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LNFileReader;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Convenience class to package together the different steps and fields needed
 * for reading a log entry for undoing. Is used for both txn aborts and 
 * recovery undos.
 */
public class UndoReader {

    public final LNLogEntry<?> logEntry;
    public final LN ln;
    private final long lsn;
    public final int logEntrySize;
    public final DatabaseImpl db;
    
    private UndoReader(LNLogEntry<?> logEntry,
                       LN ln,
                       long lsn,
                       int logEntrySize,
                       DatabaseImpl db) {
        this.logEntry = logEntry;
        this.ln = ln;
        this.lsn = lsn;
        this.logEntrySize = logEntrySize;
        this.db = db;
    }

    /**
     * Set up an UndoReader when doing an undo or txn partial rollback for a
     * live txn.
     * <p>
     * Never returns null.  The DB ID of the LN must be present in
     * undoDatabases, or a fatal exception is thrown.
     */
    public static UndoReader create(
        EnvironmentImpl envImpl,
        long undoLsn,
        Map<DatabaseId, DatabaseImpl> undoDatabases) {

        final WholeEntry wholeEntry = envImpl.getLogManager().
            getWholeLogEntryHandleFileNotFound(undoLsn);
        final int logEntrySize = wholeEntry.getHeader().getEntrySize();
        final LNLogEntry<?> logEntry = (LNLogEntry<?>) wholeEntry.getEntry();
        final DatabaseId dbId = logEntry.getDbId();
        final DatabaseImpl db = undoDatabases.get(dbId);
        if (db == null) {
            throw EnvironmentFailureException.unexpectedState
                (envImpl,
                 "DB not found during non-recovery undo/rollback, id=" + dbId);
        }
        logEntry.postFetchInit(db);
        final LN ln = logEntry.getLN();
        final long lsn = undoLsn;
        ln.postFetchInit(db, undoLsn);

        return new UndoReader(logEntry, ln, lsn, logEntrySize, db);
    }

    /**
     * Set up an UndoReader when doing a recovery partial rollback. In that
     * case, we have a file reader positioned at the pertinent log entry.
     * <p>
     * This method calls DbTree.getDb.  The caller is responsible for calling
     * DbTree.releaseDb on the db field.
     * <p>
     * Null is returned if the DB ID of the LN has been deleted.
     */
    public static UndoReader createForRecovery(LNFileReader reader,
                                               DbTree dbMapTree) {
        final LNLogEntry<?> logEntry = reader.getLNLogEntry();
        final DatabaseId dbId = logEntry.getDbId();
        final DatabaseImpl db = dbMapTree.getDb(dbId);
        if (db == null) {
            return null;
        }
        logEntry.postFetchInit(db);
        final LN ln = logEntry.getLN();
        final long lsn = reader.getLastLsn();
        ln.postFetchInit(db, lsn);
        final int logEntrySize = reader.getLastEntrySize();

        return new UndoReader(logEntry, ln, lsn, logEntrySize, db);
    }

    @Override
    public String toString() {
        return ln + " lsn=" + DbLsn.getNoFormatString(lsn);
    }
}
