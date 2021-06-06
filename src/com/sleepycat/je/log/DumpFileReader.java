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

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;

/**
 * The DumpFileReader prints every log entry to stdout.
 */
public abstract class DumpFileReader extends FileReader {

    /* A set of the entry type numbers that this DumpFileReader should dump. */
    private final Set<Byte> targetEntryTypes;

    /* A set of the txn ids that this DumpFileReader should dump. */
    private final Set<Long> targetTxnIds;

    /* A set of the db ids that this DumpFileReader should dump. */
    private final Set<Long> targetDbIds;

    /* If true, dump the long version of the entry. */
    protected final boolean verbose;

    /* If true, only dump entries that have a VLSN */
    private final boolean repEntriesOnly;

    /**
     * Create this reader to start at a given LSN.
     */
    public DumpFileReader(EnvironmentImpl env,
                          int readBufferSize,
                          long startLsn,
                          long finishLsn,
                          long endOfFileLsn,
                          String entryTypes,
                          String dbIds,
                          String txnIds,
                          boolean verbose,
                          boolean repEntriesOnly,
                          boolean forwards)
        throws DatabaseException {

        super(env,
              readBufferSize,
              forwards, 
              startLsn,
              null, // single file number
              endOfFileLsn, // end of file lsn
              finishLsn); // finish lsn

        /* If entry types is not null, record the set of target entry types. */
        targetEntryTypes = new HashSet<>();
        if (entryTypes != null) {
            StringTokenizer tokenizer = new StringTokenizer(entryTypes, ",");
            while (tokenizer.hasMoreTokens()) {
                String typeString = tokenizer.nextToken();
                targetEntryTypes.add(Byte.valueOf(typeString.trim()));
            }
        }
        /* If db ids is not null, record the set of target db ids. */
        targetDbIds = new HashSet<>();
        if (dbIds != null) {
            StringTokenizer tokenizer = new StringTokenizer(dbIds, ",");
            while (tokenizer.hasMoreTokens()) {
                String dbIdString = tokenizer.nextToken();
                targetDbIds.add(Long.valueOf(dbIdString.trim()));
            }
        }
        /* If txn ids is not null, record the set of target txn ids. */
        targetTxnIds = new HashSet<>();
        if (txnIds != null) {
            StringTokenizer tokenizer = new StringTokenizer(txnIds, ",");
            while (tokenizer.hasMoreTokens()) {
                String txnIdString = tokenizer.nextToken();
                targetTxnIds.add(Long.valueOf(txnIdString.trim()));
            }
        }
        this.verbose = verbose;
        this.repEntriesOnly = repEntriesOnly;
    }

    protected boolean needMatchEntry() {
        return !targetTxnIds.isEmpty() || !targetDbIds.isEmpty();
    }

    protected boolean matchEntry(LogEntry entry) {
        if (!targetTxnIds.isEmpty()) {
            LogEntryType type = entry.getLogType();
            if (!type.isTransactional()) {
                /* If -tx spec'd and not a transactional entry, don't dump. */
                return false;
            }
            if (!targetTxnIds.contains(entry.getTransactionId())) {
                /* Not in the list of txn ids. */
                return false;
            }
        }
        if (!targetDbIds.isEmpty()) {
            DatabaseId dbId = entry.getDbId();
            if (dbId == null) {
                /* If -db spec'd and not a db entry, don't dump. */
                return false;
            }
            if (!targetDbIds.contains(dbId.getId())) {
                /* Not in the list of db ids. */
                return false;
            }
        }

        return true;
    }

    /**
     * @return true if this reader should process this entry, or just skip over
     * it.
     */
    @Override
    protected boolean isTargetEntry() {
        if (repEntriesOnly && !currentEntryHeader.getReplicated()) {

            /* 
             * Skip this entry; we only want replicated entries, and this
             * one is not replicated.
             */
            return false;
        }

        if (targetEntryTypes.size() == 0) {
            /* We want to dump all entry types. */
            return true;
        }
        return targetEntryTypes.contains
            (Byte.valueOf(currentEntryHeader.getType()));
    }

    /**
     * @param ignore  
     */
    public void summarize(boolean ignore /*csvFile*/) {
    }
}
