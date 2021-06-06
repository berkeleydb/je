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

package com.sleepycat.je.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LastFileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.ScavengerFileReader;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.txn.TxnChain.CompareSlot;
import com.sleepycat.je.utilint.BitMap;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.utilint.StringUtils;

/**
 * Used to retrieve as much data as possible from a corrupted environment. 
 * This utility is meant to be used programmatically, and is the equivalent
 * to the -R or -r options for {@link DbDump}.
 * <p>
 * To scavenge a database:
 *<pre>
 *  DbScavenger scavenger =
 *      new DbScavenger(env, outputDirectory, <boolean>, <boolean>, <boolean>);
 *  scavenger.dump();
 *</pre> 
 *
 *<p>
 * The recovered databases will put placed in the outputDirectory with ".dump"
 * file suffixes.  The format of the .dump files will be suitable for use with
 * DbLoad.
 */

public class DbScavenger extends DbDump {
    private static final int FLUSH_INTERVAL = 100;
    private int readBufferSize;
    private EnvironmentImpl envImpl;

    /*
     * Set of committed txn ids that have been seen so far.  Positive IDs are
     * for non-replicated txns, and negative IDs are for replicated txns.
     */
    private BitMap positiveCommittedTxnIdsSeen;
    private BitMap negativeCommittedTxnIdsSeen;

    /*
     * Set of LN Node Ids that have been seen so far.
     */
    private Set<CompareSlot> lnNodesSeen;

    /*
     * Map of database id to database names.
     */
    private Map<Long, String> dbIdToName;

    /*
     * Map of database id to DatabaseImpl.
     */
    private Map<Long, DatabaseImpl> dbIdToImpl;

    /*
     * Map of database id to the .dump file output stream for that database.
     */
    private Map<Long, PrintStream> dbIdToOutputStream;

    private boolean dumpCorruptedBounds = false;

    private int flushCounter = 0;
    private long lastTime;

    /**
     * Create a DbScavenger object for a specific environment.
     * <p>
     * @param env The Environment containing the database to dump.
     * @param outputDirectory The directory to create the .dump files in.
     * @param formatUsingPrintable true if the dump should use printable 
     * characters.
     * @param doAggressiveScavengerRun true if true, then all data records are
     *  dumped, regardless of whether they are the latest version or not.
     * @param verbose true if status output should be written to System.out
     * during scavenging.
     */
    public DbScavenger(Environment env,
                       String outputDirectory,
                       boolean formatUsingPrintable,
                       boolean doAggressiveScavengerRun,
                       boolean verbose) {
        super(env, null, null, formatUsingPrintable);

        this.doAggressiveScavengerRun = doAggressiveScavengerRun;
        this.dbIdToName = new HashMap<Long, String>();
        this.dbIdToImpl = new HashMap<Long, DatabaseImpl>();
        this.dbIdToOutputStream = new HashMap<Long, PrintStream>();
        this.verbose = verbose;
        this.outputDirectory = outputDirectory;
    }

    /**
     * Set to true if corrupted boundaries should be dumped out.
     */
    public void setDumpCorruptedBounds(boolean dumpCorruptedBounds) {
        this.dumpCorruptedBounds = dumpCorruptedBounds;
    }

    /**
     * Start the scavenger run.
     */
    @Override
    public void dump()
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               IOException {

        openEnv(false);

        envImpl = DbInternal.getNonNullEnvImpl(env);
        DbConfigManager cm = envImpl.getConfigManager();
        readBufferSize = cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        /*
         * Find the end of the log.
         */
        LastFileReader reader = new LastFileReader(envImpl, readBufferSize);
        while (reader.readNextEntry()) {
        }

        /* Tell the fileManager where the end of the log is. */
        long lastUsedLsn = reader.getLastValidLsn();
        long nextAvailableLsn = reader.getEndOfLog();
        envImpl.getFileManager().setLastPosition(nextAvailableLsn,
                                                 lastUsedLsn,
                                                 reader.getPrevOffset());

        try {
            /* Pass 1: Scavenge the dbtree. */
            if (verbose) {
                System.out.println("Pass 1: " + new Date());
            }
            scavengeDbTree(lastUsedLsn, nextAvailableLsn);

            /* Pass 2: Scavenge the databases. */
            if (verbose) {
                System.out.println("Pass 2: " + new Date());
            }
            scavenge(lastUsedLsn, nextAvailableLsn);

            if (verbose) {
                System.out.println("End: " + new Date());
            }
        } finally {
            closeOutputStreams();
        }
    }

    /*
     * Scan the log looking for records that are relevant for scavenging the db
     * tree.
     */
    private void scavengeDbTree(long lastUsedLsn, long nextAvailableLsn)
        throws DatabaseException {

        positiveCommittedTxnIdsSeen = new BitMap();
        negativeCommittedTxnIdsSeen = new BitMap();
        lnNodesSeen = new TreeSet<CompareSlot>();

        final ScavengerFileReader scavengerReader =
            new ScavengerFileReader(envImpl, readBufferSize, lastUsedLsn,
                                    DbLsn.NULL_LSN, nextAvailableLsn) {
                protected void processEntryCallback(LogEntry entry,
                                                    LogEntryType entryType)
                    throws DatabaseException {

                    processDbTreeEntry(entry, entryType);
                }
            };

        scavengerReader.setTargetType(LogEntryType.LOG_MAPLN);
        scavengerReader.setTargetType(LogEntryType.LOG_NAMELN_TRANSACTIONAL);
        scavengerReader.setTargetType(LogEntryType.LOG_NAMELN);
        scavengerReader.setTargetType(LogEntryType.LOG_TXN_COMMIT);
        scavengerReader.setTargetType(LogEntryType.LOG_TXN_ABORT);
        lastTime = System.currentTimeMillis();
        long fileNum = -1;
        while (scavengerReader.readNextEntry()) {
            fileNum = reportProgress(fileNum,
                                     scavengerReader.getLastLsn());
        }
    }

    private long reportProgress(long fileNum, long lastLsn) {

        long currentFile = DbLsn.getFileNumber(lastLsn);
        if (verbose) {
            if (currentFile != fileNum) {
                long now = System.currentTimeMillis();
                System.out.println("processing file " +
                                   FileManager.getFileName(currentFile,
                                                           ".jdb  ") +
                                   (now-lastTime) + " ms");
                lastTime = now;
            }
        }

        return currentFile;
    }

    /*
     * Look at an entry and determine if it should be processed for scavenging.
     */
    private boolean checkProcessEntry(LogEntry entry,
                                      LogEntryType entryType,
                                      boolean pass2) {
        boolean isTransactional = entryType.isTransactional();

        /*
         * If entry is txnal...
         *  if a commit record, add to committed txn id set
         *  if an abort record, ignore it and don't process.
         *  if an LN, check if it's in the committed txn id set.
         *     If it is, continue processing, otherwise ignore it.
         */
        if (isTransactional) {
            final long txnId = entry.getTransactionId();
            if (entryType.equals(LogEntryType.LOG_TXN_COMMIT)) {
                setCommittedTxn(txnId);
                /* No need to process this entry further. */
                return false;
            }

            if (entryType.equals(LogEntryType.LOG_TXN_ABORT)) {
                /* No need to process this entry further. */
                return false;
            }

            if (!isCommittedTxn(txnId)) {
                return false;
            }
        }

        /*
         * Check the nodeId to see if we've already seen it or not.
         */
        if (entry instanceof LNLogEntry) {

            final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
            final long dbId = lnEntry.getDbId().getId();
            final DatabaseImpl db = dbIdToImpl.get(dbId);
            /* Must call postFetchInit if true is returned. */
            if (db != null) {
                lnEntry.postFetchInit(db);
            } else {
                lnEntry.postFetchInit(false /*isDupDb*/);
            }

            /*
             * If aggressive or if processing DbTree entries, don't worry about
             * whether this node has been processed already.
             */
            if (doAggressiveScavengerRun || !pass2) {
                return true;
            }

            if (db == null) {
                throw EnvironmentFailureException.unexpectedState
                    ("Database info not available for DB ID: " + dbId);
            }
            return lnNodesSeen.add(new CompareSlot(db, lnEntry));
        }

        return false;
    }

    /*
     * Called once for each log entry during the pass 1 (dbtree).
     */
    private void processDbTreeEntry(LogEntry entry, LogEntryType entryType)
        throws DatabaseException {

        boolean processThisEntry =
            checkProcessEntry(entry, entryType, false);

        if (processThisEntry &&
            (entry instanceof LNLogEntry)) {
            LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
            LN ln = lnEntry.getLN();
            if (ln instanceof NameLN) {
                String name = StringUtils.fromUTF8(lnEntry.getKey());
                Long dbId = Long.valueOf(((NameLN) ln).getId().getId());
                if (dbIdToName.containsKey(dbId) &&
                    !dbIdToName.get(dbId).equals(name)) {
                    throw EnvironmentFailureException.unexpectedState
                        ("Already name mapped for dbId: " + dbId +
                         " changed from " + dbIdToName.get(dbId) +
                         " to " + name);
                } else {
                    dbIdToName.put(dbId, name);
                }
            }

            if (ln instanceof MapLN) {
                DatabaseImpl db = ((MapLN) ln).getDatabase();
                Long dbId = db.getId().getId();
                /* Use latest version to get most recent comparators. */
                if (!dbIdToImpl.containsKey(dbId)) {
                    dbIdToImpl.put(dbId, db);
                }
            }
        }
    }

    /*
     * Pass 2: scavenge the regular (non-dbtree) environment.
     */
    private void scavenge(long lastUsedLsn, long nextAvailableLsn)
        throws DatabaseException {

        final ScavengerFileReader scavengerReader =
            new ScavengerFileReader(envImpl, readBufferSize, lastUsedLsn,
                                    DbLsn.NULL_LSN, nextAvailableLsn) {
                protected void processEntryCallback(LogEntry entry,
                                                    LogEntryType entryType)
                    throws DatabaseException {

                    processRegularEntry(entry, entryType);
                }
            };

        /*
         * Note: committed transaction id map has been created already, no
         * need to read TXN_COMMITS on this pass.
         */
        for (LogEntryType entryType : LogEntryType.getAllTypes()) {
            if (entryType.isUserLNType()) {
                scavengerReader.setTargetType(entryType);
            }
        }
        scavengerReader.setDumpCorruptedBounds(dumpCorruptedBounds);

        long progressFileNum = -1;
        while (scavengerReader.readNextEntry()) {
            progressFileNum = reportProgress(progressFileNum,
                                             scavengerReader.getLastLsn());
        }
    }

    /*
     * Process an entry during pass 2.
     */
    private void processRegularEntry(LogEntry entry, LogEntryType entryType)
        throws DatabaseException {

        boolean processThisEntry =
            checkProcessEntry(entry, entryType, true);

        if (processThisEntry) {
            LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
            Long dbId = Long.valueOf(lnEntry.getDbId().getId());
            LN ln = lnEntry.getLN();

            /* Create output file even if we don't process a deleted entry. */
            PrintStream out = getOutputStream(dbId);

            if (!ln.isDeleted()) {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                lnEntry.getUserKeyData(key, data);
                dumpOne(out, key.getData(), formatUsingPrintable);
                dumpOne(out, data.getData(), formatUsingPrintable);
                if ((++flushCounter % FLUSH_INTERVAL) == 0) {
                    out.flush();
                    flushCounter = 0;
                }
            }
        }
    }

    /*
     * Return the output stream for the .dump file for database with id dbId.
     * If an output stream has not already been created, then create one.
     */
    private PrintStream getOutputStream(Long dbId)
        throws DatabaseException {

        PrintStream ret = dbIdToOutputStream.get(dbId);
        if (ret != null) {
            return ret;
        }
        String name = dbIdToName.get(dbId);
        if (name == null) {
            name = "db" + dbId;
        }
        File file = new File(outputDirectory, name + ".dump");
        try {
            ret = new PrintStream(new FileOutputStream(file), false);
        } catch (FileNotFoundException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
        dbIdToOutputStream.put(dbId, ret);
        DatabaseImpl db = dbIdToImpl.get(dbId);
        boolean dupSort = (db != null) ? db.getSortedDuplicates() : false;
        printHeader(ret, dupSort, formatUsingPrintable);
        return ret;
    }

    private void closeOutputStreams() {

        Iterator<PrintStream> iter = dbIdToOutputStream.values().iterator();
        while (iter.hasNext()) {
            PrintStream s = iter.next();
            s.println("DATA=END");
            s.close();
        }
    }

    private void setCommittedTxn(final long txnId) {
        if (txnId >= 0) {
            positiveCommittedTxnIdsSeen.set(txnId);
        } else {
            negativeCommittedTxnIdsSeen.set(0 - txnId);
        }
    }

    private boolean isCommittedTxn(final long txnId) {
        if (txnId >= 0) {
            return positiveCommittedTxnIdsSeen.get(txnId);
        } else {
            return negativeCommittedTxnIdsSeen.get(0 - txnId);
        }
    }
}
