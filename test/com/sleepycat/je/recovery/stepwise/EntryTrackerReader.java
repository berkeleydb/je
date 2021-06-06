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

package com.sleepycat.je.recovery.stepwise;

import java.nio.ByteBuffer;
import java.util.List;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.utilint.DbLsn;

/**
 * EntryTrackerReader collects a list of EntryInfo describing all log entries
 * in the truncated portion of a log.  It lets the test know where to do a log
 * truncation and remembers whether an inserted or deleted record was seen, in
 * order to update the test's set of expected records.
 */
public class EntryTrackerReader extends FileReader {

    /*
     * EntryInfo is a list that corresponds to each entry in the truncated
     * area of the log.
     */
    private List<LogEntryInfo> entryInfo;
    private DatabaseEntry dbt = new DatabaseEntry();
    private LogEntry useLogEntry;
    private LogEntryType useLogEntryType;
    private boolean isCommit;

    /**
     * Create this reader to start at a given LSN.
     */
    public EntryTrackerReader(EnvironmentImpl env,
                              long startLsn,
                              List<LogEntryInfo> entryInfo) // EntryInfo
        throws DatabaseException {

        super(env, 2000, true, startLsn, null,
              -1, DbLsn.NULL_LSN);

        this.entryInfo = entryInfo;
    }

    /**
     * @return true if this is a targeted entry that should be processed.
     */
    protected boolean isTargetEntry() {
        LogEntryType entryType = 
            LogEntryType.findType(currentEntryHeader.getType());
        isCommit = false;
        boolean targeted = true;

        useLogEntryType = null;

        if (entryType.isUserLNType()) {
            useLogEntryType = entryType;
        } else if (entryType == LogEntryType.LOG_TXN_COMMIT) {
            useLogEntryType = LogEntryType.LOG_TXN_COMMIT;
            isCommit = true;
        } else {

            /*
             * Just make note, no need to process the entry, nothing to record
             * besides the LSN. Note that the offset has not been bumped by
             * the FileReader, so use nextEntryOffset.
             */
            entryInfo.add
                (new LogEntryInfo(DbLsn.makeLsn(window.currentFileNum(),
                                                nextEntryOffset), 0, 0));
            targeted = false;
        }

        if (useLogEntryType != null) {
            useLogEntry = useLogEntryType.getSharedLogEntry();
        }
        return targeted;
    }

    /**
     * This log entry has data which affects the expected set of records.
     * We need to save each lsn and determine whether the value of the
     * log entry should affect the expected set of records. For
     * non-transactional entries, the expected set is affected right away.
     * For transactional entries, we defer updates of the expected set until
     * a commit is seen.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        /*
         * Note that the offset has been bumped, so use currentEntryOffset
         * for the LSN.
         */
        long lsn = DbLsn.makeLsn(window.currentFileNum(), currentEntryOffset);
        useLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);

        boolean isTxnal = useLogEntryType.isTransactional();
        long txnId = useLogEntry.getTransactionId();

        if (isCommit) {

            /*
             * The txn id in a single item log entry is embedded within
             * the item.
             */
            txnId = ((TxnCommit) useLogEntry.getMainItem()).getId();
            entryInfo.add(new CommitEntry(lsn, txnId));
        } else {
            final LNLogEntry<?> lnLogEntry = (LNLogEntry<?>) useLogEntry;
            final boolean isDupDb =
                (lnLogEntry.getUnconvertedKeyLength() != 4);
            lnLogEntry.postFetchInit(isDupDb);
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();
            lnLogEntry.getUserKeyData(keyEntry, dataEntry);
            final int keyValue = IntegerBinding.entryToInt(keyEntry);
            final int dataValue;
            final boolean deleted = lnLogEntry.isDeleted();
            if (deleted) {
                dataValue = -1;
            } else {
                dataValue = IntegerBinding.entryToInt(dataEntry);
            }

            if (deleted) {
                if (isTxnal) {
                    entryInfo.add(new TxnalDeletedEntry(lsn, keyValue,
                                                        dataValue, txnId));
                } else {
                    entryInfo.add(new NonTxnalDeletedEntry(lsn, keyValue,
                                                           dataValue));
                }
            } else {
                if (isTxnal) {
                    entryInfo.add(new TxnalEntry(lsn, keyValue, dataValue,
                                                 txnId));
                } else {
                    entryInfo.add(new NonTxnalEntry(lsn, keyValue, dataValue));
                }
            }
        }

        return true;
    }
}
