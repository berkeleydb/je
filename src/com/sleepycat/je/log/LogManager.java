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

import static com.sleepycat.je.log.LogStatDefinition.GROUP_DESC;
import static com.sleepycat.je.log.LogStatDefinition.GROUP_NAME;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_END_OF_LOG;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_REPEAT_FAULT_READS;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_TEMP_BUFFER_WRITES;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.DbFileSummary;
import com.sleepycat.je.cleaner.ExpirationTracker;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.util.verify.VerifierUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LSNStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * The LogManager supports reading and writing to the JE log.
 * The writing of data to the log is serialized via the logWriteMutex.
 * Typically space is allocated under the LWL. The client computes
 * the checksum and copies the data into the log buffer (not holding
 * the LWL).
 */
public class LogManager {

    /* No-op loggable object. */
    private static final String DEBUG_NAME = LogManager.class.getName();

    private final LogBufferPool logBufferPool; // log buffers
    private final Object logWriteMutex;           // synchronizes log writes
    private final boolean doChecksumOnRead;      // if true, do checksum on read
    private final FileManager fileManager;       // access to files
    private final FSyncManager grpManager;
    private final EnvironmentImpl envImpl;
    private final boolean readOnly;

    /* How many bytes to read when faulting in. */
    private final int readBufferSize;

    /* The last LSN in the log during recovery. */
    private long lastLsnAtRecovery = DbLsn.NULL_LSN;

    /* Stats */
    private final StatGroup stats;

    /*
     * Number of times we have to repeat a read when we fault in an object
     * because the initial read was too small.
     */
    private final LongStat nRepeatFaultReads;

    /*
     * Number of times we have to use the temporary marshalling buffer to
     * write to the log.
     */
    private final LongStat nTempBufferWrites;

    /* The location of the next entry to be written to the log. */
    private final LSNStat endOfLog;

    /*
     * Used to determine if we switched log buffers. For
     * NOSYNC durability, if we switched log buffers,
     * the thread will write the previous dirty buffers.
     */
    private LogBuffer prevLogBuffer = null;

    /* For unit tests */
    private TestHook readHook; // used for generating exceptions on log reads

    /* For unit tests. */
    private TestHook<Object> delayVLSNRegisterHook;
    private TestHook<CountDownLatch> flushHook;

    /* A queue to hold log entries which are to be logged lazily. */
    private final Queue<LazyQueueEntry> lazyLogQueue =
        new ConcurrentLinkedQueue<LazyQueueEntry>();

    /*
     * Used for tracking the current file. Is null if no tracking should occur.
     * Read/write of this field is protected by the LWL, but the tracking
     * actually occurs outside the LWL.
     */
    private ExpirationTracker expirationTracker = null;

    /*
     * An entry in the lazyLogQueue. A struct to hold the entry and repContext.
     */
    private static class LazyQueueEntry {
        private final LogEntry entry;
        private final ReplicationContext repContext;

        private LazyQueueEntry(LogEntry entry, ReplicationContext repContext) {
            this.entry = entry;
            this.repContext = repContext;
        }
    }

    /**
     * There is a single log manager per database environment.
     */
    public LogManager(EnvironmentImpl envImpl,
                      boolean readOnly)
        throws DatabaseException {

        /* Set up log buffers. */
        this.envImpl = envImpl;
        this.fileManager = envImpl.getFileManager();
        this.grpManager = new FSyncManager(this.envImpl);
        DbConfigManager configManager = envImpl.getConfigManager();
        this.readOnly = readOnly;
        logBufferPool = new LogBufferPool(fileManager, envImpl);

        /* See if we're configured to do a checksum when reading in objects. */
        doChecksumOnRead =
            configManager.getBoolean(EnvironmentParams.LOG_CHECKSUM_READ);

        logWriteMutex = new Object();
        readBufferSize =
            configManager.getInt(EnvironmentParams.LOG_FAULT_READ_SIZE);

        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        nRepeatFaultReads = new LongStat(stats, LOGMGR_REPEAT_FAULT_READS);
        nTempBufferWrites = new LongStat(stats, LOGMGR_TEMP_BUFFER_WRITES);
        endOfLog = new LSNStat(stats, LOGMGR_END_OF_LOG);
    }

    public boolean getChecksumOnRead() {
        return doChecksumOnRead;
    }

    public long getLastLsnAtRecovery() {
        return lastLsnAtRecovery;
    }

    public void setLastLsnAtRecovery(long lastLsnAtRecovery) {
        this.lastLsnAtRecovery = lastLsnAtRecovery;
    }

    /**
     * Called at the end of recovery to begin expiration tracking using the
     * given tracker. During recovery we are single threaded, so we can set
     * the field without taking the LWL.
     */
    public void initExpirationTracker(final ExpirationTracker tracker) {
        expirationTracker = tracker;
    }

    /**
     * Reset the pool when the cache is resized.  This method is called after
     * the memory budget has been calculated.
     */
    public void resetPool(DbConfigManager configManager)
            throws DatabaseException {
        synchronized (logWriteMutex) {
           logBufferPool.reset(configManager);
        }
    }

    /*
     * Writing to the log
     */

    /**
     * Log this single object and force a write of the log files.
     * @param entry object to be logged
     * @param fsyncRequired if true, log files should also be fsynced.
     * @return LSN of the new log entry
     */
    public long logForceFlush(LogEntry entry,
                              boolean fsyncRequired,
                              ReplicationContext repContext)
        throws DatabaseException {

        return log(entry,
                   Provisional.NO,
                   true,           // flush required
                   fsyncRequired,
                   false,          // forceNewLogFile
                   repContext);    // repContext
    }

    /**
     * Log this single object and force a flip of the log files.
     * @param entry object to be logged
     * @return LSN of the new log entry
     */
    public long logForceFlip(LogEntry entry)
        throws DatabaseException {

        return log(entry,
                   Provisional.NO,
                   true,           // flush required
                   false,          // fsync required
                   true,           // forceNewLogFile
                   ReplicationContext.NO_REPLICATE);
    }

    /**
     * Write a log entry.
     * @param entry object to be logged
     * @return LSN of the new log entry
     */
    public long log(LogEntry entry, ReplicationContext repContext)
        throws DatabaseException {

        return log(entry,
                   Provisional.NO,
                   false,           // flush required
                   false,           // fsync required
                   false,           // forceNewLogFile
                   repContext);
    }

    /**
     * Write a log entry lazily.
     * @param entry object to be logged
     */
    public void logLazily(LogEntry entry, ReplicationContext repContext) {

        lazyLogQueue.add(new LazyQueueEntry(entry, repContext));
    }

    /**
     * Translates individual log params to LogItem and LogContext fields.
     */
    private long log(final LogEntry entry,
                     final Provisional provisional,
                     final boolean flushRequired,
                     final boolean fsyncRequired,
                     final boolean forceNewLogFile,
                     final ReplicationContext repContext)
        throws DatabaseException {

        final LogParams params = new LogParams();

        params.entry = entry;
        params.provisional = provisional;
        params.repContext = repContext;
        params.flushRequired = flushRequired;
        params.fsyncRequired = fsyncRequired;
        params.forceNewLogFile = forceNewLogFile;

        final LogItem item = log(params);

        return item.lsn;
    }

    /**
     * Log an item, first logging any items on the lazyLogQueue, and finally
     * flushing and sync'ing (if requested).
     */
    public LogItem log(LogParams params)
        throws DatabaseException {

        final LogItem item = new LogItem();

        /*
         * In a read-only env we return NULL_LSN (the default value for
         * LogItem.lsn) for all entries.  We allow this to proceed, rather
         * than throwing an exception, to support logging INs for splits that
         * occur during recovery, for one reason.  Logging LNs in a read-only
         * env is not allowed, and this is checked in the LN class.
         */
        if (readOnly) {
            return item;
        }

        try {
            /* Flush any pending lazy entries. */
            for (LazyQueueEntry lqe = lazyLogQueue.poll();
                 lqe != null;
                 lqe = lazyLogQueue.poll()) {

                LogParams lqeParams = new LogParams();
                lqeParams.entry = lqe.entry;
                lqeParams.provisional = Provisional.NO;
                lqeParams.repContext = lqe.repContext;

                logItem(new LogItem(), lqeParams);
            }

            final LogEntry logEntry = params.entry;

            /*
             * If possible, marshall this entry outside the log write latch to
             * allow greater concurrency by shortening the write critical
             * section.  Note that the header may only be created during
             * marshalling because it calls entry.getSize().
             */
            if (logEntry.getLogType().marshallOutsideLatch()) {

                item.header = new LogEntryHeader(
                    logEntry, params.provisional, params.repContext);

                item.buffer = marshallIntoBuffer(item.header, logEntry);
            }

            logItem(item, params);

            if (params.fsyncRequired || params.flushRequired) {

                /* Flush log buffers and write queue, and optionally fsync. */
                grpManager.flushAndSync(params.fsyncRequired);

            } else if (params.switchedLogBuffer) {
                /*
                 * The operation does not require writing to the log file, but
                 * since we switched log buffers, this thread will write the
                 * previously dirty log buffers (not this thread's log entry
                 * though). This is done for NOSYNC durability so those types
                 * of transactions won't fill all the log buffers thus forcing
                 * to have to write the buffers under the log write latch.
                 */
                logBufferPool.writeDirty(false /*flushWriteQueue*/);
            }

            TestHookExecute.doHookIfSet(flushHook);

            /*
             * We've logged this log entry from the replication stream. Let the
             * Replicator know, so this node can create a VLSN->LSN mapping. Do
             * this before the ckpt so we have a better chance of writing this
             * mapping to disk.
             */
            if (params.repContext.inReplicationStream()) {

                assert (item.header.getVLSN() != null) :
                    "Unexpected null vlsn: " + item.header + " " +
                    params.repContext;

                /* Block the VLSN registration, used by unit tests. */
                TestHookExecute.doHookIfSet(delayVLSNRegisterHook);

                envImpl.registerVLSN(item);
            }

        } catch (EnvironmentFailureException e) {

            /*
             * Final checks are below for unexpected exceptions during the
             * critical write path.  Most should be caught by
             * serialLogInternal, but the catches here account for other
             * exceptions above.  Note that Errors must be caught here as well
             * as Exceptions.  [#21929]
             *
             * If we've already invalidated the environment, rethrow so as not
             * to excessively wrap the exception.
             */
            if (!envImpl.isValid()) {
                throw e;
            }
            throw EnvironmentFailureException.unexpectedException(envImpl, e);

        } catch (Exception e) {
            throw EnvironmentFailureException.unexpectedException(envImpl, e);

        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;
        }

        /*
         * Periodically, as a function of how much data is written, ask the
         * checkpointer or the cleaner to wake up.
         */
        envImpl.getCheckpointer().wakeupAfterWrite();
        envImpl.getCleaner().wakeupAfterWrite(item.size);

        /* Update background writes. */
        if (params.backgroundIO) {
            envImpl.updateBackgroundWrites(
                item.size, logBufferPool.getLogBufferSize());
        }

        return item;
    }

    private void logItem(final LogItem item, final LogParams params)
        throws IOException, DatabaseException {

        final UtilizationTracker tracker = envImpl.getUtilizationTracker();

        final LogWriteInfo lwi = serialLog(item, params, tracker);

        if (lwi != null) {

            /*
             * Add checksum, prev offset, and VLSN to the entry.
             * Copy data into the log buffer.
             */
            item.buffer = item.header.addPostMarshallingInfo(
                item.buffer, lwi.fileOffset, lwi.vlsn);

            lwi.lbs.put(item.buffer);
        }

        /* Update obsolete info under the LWL */
        updateObsolete(params, tracker);

        /* Expiration tracking is protected by the Btree latch, not the LWL. */
        if (params.expirationTrackerToUse != null) {
            params.expirationTrackerToUse.track(params.entry, item.size);
        }

        /* Queue flushing of expiration tracker after a file flip. */
        if (params.expirationTrackerCompleted != null) {
            envImpl.getExpirationProfile().addCompletedTracker(
                params.expirationTrackerCompleted);
        }
    }

    /**
     * This method handles exceptions to be certain that the Environment is
     * invalidated when any exception occurs in the critical write path, and it
     * checks for an invalid environment to be sure that no subsequent write is
     * allowed.  [#21929]
     *
     * Invalidation is necessary because a logging operation does not ensure
     * that the internal state -- correspondence of LSN pointer, log buffer
     * position and file position, and the integrity of the VLSN index [#20919]
     * -- is maintained correctly when an exception occurs.  Allowing a
     * subsequent write can cause log corruption.
     */
    private LogWriteInfo serialLog(
        final LogItem item,
        final LogParams params,
        final UtilizationTracker tracker)
        throws IOException {

        synchronized (logWriteMutex) {

            /* Do not attempt to write with an invalid environment. */
            envImpl.checkIfInvalid();

            try {
                return serialLogWork(item, params, tracker);

            } catch (EnvironmentFailureException e) {
                /*
                 * If we've already invalidated the environment, rethrow so
                 * as not to excessively wrap the exception.
                 */
                if (!envImpl.isValid()) {
                    throw e;
                }

                /* Otherwise, invalidate the environment. */
                throw EnvironmentFailureException.unexpectedException(
                    envImpl, e);

            } catch (Exception e) {
                throw EnvironmentFailureException.unexpectedException(
                    envImpl, e);

            } catch (Error e) {
                /* Errors must be caught here as well as Exceptions.[#21929] */
                envImpl.invalidate(e);
                throw e;
            }
        }
    }

    /**
     * This method is used as part of writing data to the log. Called
     * under the LogWriteLatch.
     * Data is either written to the LogBuffer or allocates space in the
     * LogBuffer. The LogWriteInfo object is used to save information about
     * the space allocate in the LogBuffer. The caller uses the object to
     * copy data into the underlying LogBuffer. A null value returned
     * indicates that the item was written to the log. This occurs when the
     * data item is too big to fit into an empty LogBuffer.
     *
     * @param params log params.
     * @param tracker utilization.
     * @return a LogWriteInfo object used to access allocated LogBuffer space.
     *           If null, the data was written to the log.
     * @throws IOException
     */
    private LogWriteInfo serialLogWork(
        final LogItem item,
        final LogParams params,
        final UtilizationTracker tracker)
        throws IOException {

        /*
         * Do obsolete tracking before marshalling a FileSummaryLN into the
         * log buffer so that a FileSummaryLN counts itself.
         * countObsoleteNode must be called before computing the entry
         * size, since it can change the size of a FileSummaryLN entry that
         * we're logging
         */
        final LogEntryType entryType = params.entry.getLogType();

        if (!DbLsn.isTransientOrNull(params.oldLsn)) {
            if (params.obsoleteDupsAllowed) {
                tracker.countObsoleteNodeDupsAllowed(
                    params.oldLsn, entryType, params.oldSize, params.nodeDb);
            } else {
                tracker.countObsoleteNode(
                    params.oldLsn, entryType, params.oldSize, params.nodeDb);
            }
        }

        /* Count auxOldLsn for same database; no specified size. */
        if (!DbLsn.isTransientOrNull(params.auxOldLsn)) {
            if (params.obsoleteDupsAllowed) {
                tracker.countObsoleteNodeDupsAllowed(
                    params.auxOldLsn, entryType, 0, params.nodeDb);
            } else {
                tracker.countObsoleteNode(
                    params.auxOldLsn, entryType, 0, params.nodeDb);
            }
        }

        /*
         * Compute the VLSNs and modify the DTVLSN in commit/abort entries
         * before the entry is marshalled or its size is required. At that
         * at this point we are committed to writing a log entry with  the
         * computed VLSN.
         */
        final VLSN vlsn;

        if (params.repContext.getClientVLSN() != null ||
            params.repContext.mustGenerateVLSN()) {

            if (params.repContext.mustGenerateVLSN()) {
                vlsn = envImpl.assignVLSNs(params.entry);
            } else {
                vlsn = params.repContext.getClientVLSN();
            }
        } else {
            vlsn = null;
        }

        /*
         * If an entry must be protected within the log write latch for
         * marshalling, take care to also calculate its size in the
         * protected section. Note that we have to get the size *before*
         * marshalling so that the currentLsn and size are correct for
         * utilization tracking.
         */
        final boolean marshallOutsideLatch = (item.buffer != null);
        final int entrySize;

        if (marshallOutsideLatch) {
            entrySize = item.buffer.limit();
            assert item.header != null;
        } else {
            assert item.header == null;
            item.header = new LogEntryHeader(
                params.entry, params.provisional, params.repContext);
            entrySize = item.header.getEntrySize();
        }

        /*
         * Get the next free slot in the log, under the log write latch.
         */
        if (params.forceNewLogFile) {
            fileManager.forceNewLogFile();
        }

        final boolean flippedFile = fileManager.shouldFlipFile(entrySize);
        final long currentLsn = fileManager.calculateNextLsn(flippedFile);

        /*
         * TODO: Count file header, since it is not logged via LogManager.
         * Some tests (e.g., INUtilizationTest) will need to be adjusted.
         *
        final int fileHeaderSize = FileManager.firstLogEntryOffset();
        if (DbLsn.getFileOffset(currentLsn) == fileHeaderSize) {
            final long fileNum = DbLsn.getFileNumber(currentLsn);

            tracker.countNewLogEntry(
                DbLsn.makeLsn(fileNum, 0), LogEntryType.LOG_FILE_HEADER,
                fileHeaderSize, null);
        }
        */

        /*
         * countNewLogEntry and countObsoleteNodeInexact cannot change
         * a FileSummaryLN size, so they are safe to call after
         * getSizeForWrite.
         */
        tracker.countNewLogEntry(
            currentLsn, entryType, entrySize, params.nodeDb);

        /*
         * LN deletions and dup DB LNs are obsolete immediately.  Inexact
         * counting is used to save resources because the cleaner knows
         * that all such LNs are obsolete.
         */
        if (params.entry.isImmediatelyObsolete(params.nodeDb)) {
            tracker.countObsoleteNodeInexact(
                currentLsn, entryType, entrySize, params.nodeDb);
        }

        /*
         * This entry must be marshalled within the log write latch.
         */
        if (!marshallOutsideLatch) {
            assert item.buffer == null;
            item.buffer = marshallIntoBuffer(item.header, params.entry);
        }

        /* Sanity check */
        if (entrySize != item.buffer.limit()) {
            throw EnvironmentFailureException.unexpectedState(
                "Logged entry entrySize= " + entrySize +
                " but marshalledSize=" + item.buffer.limit() +
                " type=" + entryType + " currentLsn=" +
                DbLsn.getNoFormatString(currentLsn));
        }

        /*
         * Ask for a log buffer suitable for holding this new entry. If
         * entrySize is larger than the LogBuffer capacity, this will flush
         * all dirty buffers and return the next empty (but too small) buffer.
         * The returned buffer is not latched.
         */
        final LogBuffer lastLogBuffer =
            logBufferPool.getWriteBuffer(entrySize, flippedFile);

        /*
         * Bump the LSN values, which gives us a valid previous pointer,
         * which is part of the log entry header. This must be done:
         *  - before logging the currentLsn.
         *  - after calling getWriteBuffer, to flush the prior file when
         *    flippedFile is true.
         */
        final long prevOffset = fileManager.advanceLsn(
            currentLsn, entrySize, flippedFile);

        if (lastLogBuffer != prevLogBuffer) {
            params.switchedLogBuffer = true;
        }
        prevLogBuffer = lastLogBuffer;

        final LogBufferSegment useBuffer;

        lastLogBuffer.latchForWrite();
        try {
            useBuffer = lastLogBuffer.allocate(entrySize);

            if (useBuffer != null) {
                /* Register the lsn while holding the buffer latch. */
                lastLogBuffer.registerLsn(currentLsn);
            } else {
                /*
                 * The item buffer is larger than the LogBuffer capacity, so
                 * write the item buffer to the file directly. Note that
                 * getWriteBuffer has flushed all dirty buffers.
                 *
                 * First add checksum, prev offset, and VLSN to the entry.
                 */
                item.buffer = item.header.addPostMarshallingInfo(
                    item.buffer, prevOffset, vlsn);

                final boolean flushWriteQueue =
                    params.flushRequired && !params.fsyncRequired;

                fileManager.writeLogBuffer(
                    new LogBuffer(item.buffer, currentLsn),
                    flushWriteQueue);

                assert lastLogBuffer.getDataBuffer().position() == 0;

                /* Leave a clue that the buffer size needs to be increased. */
                nTempBufferWrites.increment();
            }
        } finally {
            lastLogBuffer.release();
        }

        /*
         * If the txn is not null, the first entry is an LN. Update the txn
         * with info about the latest LSN. Note that this has to happen
         * within the log write latch.
         */
        params.entry.postLogWork(item.header, currentLsn, vlsn);

        item.lsn = currentLsn;
        item.size = entrySize;

        /* If the expirationTracker field is null, no tracking should occur. */
        if (expirationTracker != null) {
            /*
             * When logging to a new file, also flip the expirationTracker
             * under the LWL and return expirationTrackerCompleted so it will
             * be queued for flushing.
             */
            final long newFile = DbLsn.getFileNumber(item.lsn);
            if (flippedFile && newFile != expirationTracker.getFileNum()) {
                params.expirationTrackerCompleted = expirationTracker;
                expirationTracker = new ExpirationTracker(newFile);
            }
            /*
             * Increment the pending calls under the LWL, so we can determine
             * when we're finished.
             */
            expirationTracker.incrementPendingTrackCalls();
            params.expirationTrackerToUse = expirationTracker;
        }

        return (useBuffer == null ?
                null : new LogWriteInfo(useBuffer, vlsn, prevOffset));
    }

    /**
     * Serialize a loggable object into this buffer.
     */
    private ByteBuffer marshallIntoBuffer(LogEntryHeader header,
                                          LogEntry entry) {
        int entrySize = header.getSize() + header.getItemSize();

        ByteBuffer destBuffer = ByteBuffer.allocate(entrySize);
        header.writeToLog(destBuffer);

        /* Put the entry in. */
        entry.writeEntry(destBuffer);

        /* Set the limit so it can be used as the size of the entry. */
        destBuffer.flip();

        return destBuffer;
    }

    /**
     * Serialize a log entry into this buffer with proper entry header. Return
     * it ready for a copy.
     */
    ByteBuffer putIntoBuffer(LogEntry entry,
                             long prevLogEntryOffset) {
        LogEntryHeader header = new LogEntryHeader
            (entry, Provisional.NO, ReplicationContext.NO_REPLICATE);

        /*
         * Currently this method is only used for serializing the FileHeader.
         * Assert that we do not need the Txn mutex in case this method is used
         * in the future for other log entries. See LN.log. [#17204]
         */
        assert !entry.getLogType().isTransactional();

        ByteBuffer destBuffer = marshallIntoBuffer(header, entry);

        return header.addPostMarshallingInfo(destBuffer,
            prevLogEntryOffset,
            null);
    }

    /*
     * Reading from the log.
     */

    /**
     * Instantiate all the objects in the log entry at this LSN.
     */
    public LogEntry getLogEntry(long lsn)
        throws FileNotFoundException {

        return getLogEntry(lsn, 0, false /*invisibleReadAllowed*/).
            getEntry();
    }

    public WholeEntry getWholeLogEntry(long lsn)
        throws FileNotFoundException {

        return getLogEntry(lsn, 0, false /*invisibleReadAllowed*/);
    }

    /**
     * Instantiate all the objects in the log entry at this LSN. Allow the
     * fetch of invisible log entries if we are in recovery.
     */
    public WholeEntry getLogEntryAllowInvisibleAtRecovery(long lsn, int size)
        throws FileNotFoundException {

        return getLogEntry(
            lsn, size, envImpl.isInInit() /*invisibleReadAllowed*/);
    }

    /**
     * Instantiate all the objects in the log entry at this LSN. The entry
     * may be marked invisible.
     */
    public WholeEntry getLogEntryAllowInvisible(long lsn)
        throws FileNotFoundException {

        return getLogEntry(lsn, 0, true);
    }

    /**
     * Instantiate all the objects in the log entry at this LSN.
     * @param lsn location of entry in log.
     * @param invisibleReadAllowed true if it's expected that the target log
     * entry might be invisible. Correct the known-to-be-bad checksum before
     * proceeding.
     * @return log entry that embodies all the objects in the log entry.
     */
    private WholeEntry getLogEntry(
        long lsn,
        int lastLoggedSize,
        boolean invisibleReadAllowed)
        throws FileNotFoundException {

        /* Fail loudly if the environment is invalid. */
        envImpl.checkIfInvalid();

        LogSource logSource = null;
        try {

            /*
             * Get a log source for the log entry which provides an abstraction
             * that hides whether the entry is in a buffer or on disk. Will
             * register as a reader for the buffer or the file, which will take
             * a latch if necessary. Latch is released in finally block.
             */
            logSource = getLogSource(lsn);

            try {
                return getLogEntryFromLogSource(
                    lsn, lastLoggedSize, logSource, invisibleReadAllowed);

            } catch (ChecksumException ce) {

                /*
                 * When using a FileSource, a checksum error indicates a
                 * persistent corruption. An EFE with LOG_CHECKSUM is created
                 * in the catch below and EFE.isCorrupted will return true.
                 */
                if (!(logSource instanceof LogBuffer)) {
                    assert logSource instanceof FileSource;
                    throw ce;
                }

                /*
                 * When using a LogBuffer source, we must try to read the entry
                 * from disk to see if the corruption is persistent.
                 */
                final LogBuffer logBuffer = (LogBuffer) logSource;
                FileHandle fileHandle = null;
                long fileLength = -1;
                try {
                    fileHandle =
                        fileManager.getFileHandle(DbLsn.getFileNumber(lsn));
                    fileLength = fileHandle.getFile().length();
                } catch (IOException ioe) {
                    /* FileNotFound or another IOException was thrown. */
                }

                /*
                 * If the file does not exist (FileNotFoundException is thrown
                 * above) or the firstLsn in the buffer does not appear in the
                 * file (the buffer was not flushed), then the corruption is
                 * not persistent and we throw a EFE for which isCorrupted
                 * will return false.
                 */
                if (fileHandle == null ||
                    fileLength <=
                        DbLsn.getFileOffset(logBuffer.getFirstLsn())) {

                    throw EnvironmentFailureException.unexpectedException(
                        envImpl,
                        "Corruption detected in log buffer, " +
                            "but was not written to disk.",
                        ce);
                }

                /*
                 * The log entry should have been written to the file. Try
                 * getting the log entry from the FileSource. If a
                 * ChecksumException is thrown, the corruption is persistent
                 * and an EFE with LOG_CHECKSUM is thrown below.
                 */
                final FileSource fileSource = new FileHandleSource(
                    fileHandle, readBufferSize, fileManager);
                try {
                    return getLogEntryFromLogSource(
                        lsn, lastLoggedSize, fileSource,
                        invisibleReadAllowed);
                } finally {
                    fileSource.release();
                }
            }

        } catch (ChecksumException e) {
            /*
             * WARNING: EFE with LOG_CHECKSUM indicates a persistent corruption
             * and therefore LogSource.release must not be called until after
             * invalidating the environment (in the finally below). The buffer
             * latch prevents the corrupt buffer from being logged by another
             * thread.
             */
            throw VerifierUtils.createMarkerFileFromException(
                RestoreRequired.FailureType.LOG_CHECKSUM,
                e,
                envImpl,
                EnvironmentFailureReason.LOG_CHECKSUM);

        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;

        } finally {
            if (logSource != null) {
                logSource.release();
            }
        }
    }

    public LogEntry getLogEntryHandleFileNotFound(long lsn)
        throws DatabaseException {

        try {
            return getLogEntry(lsn);
        } catch (FileNotFoundException e) {
            throw new EnvironmentFailureException
                (envImpl,
                 EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
        }
    }

    public WholeEntry getWholeLogEntryHandleFileNotFound(long lsn)
        throws DatabaseException {

        try {
            return getWholeLogEntry(lsn);
        } catch (FileNotFoundException e) {
            throw new EnvironmentFailureException
                (envImpl,
                    EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
        }
    }

    /**
     * Throws ChecksumException rather than translating it to
     * EnvironmentFailureException and invalidating the environment.  Used
     * instead of getLogEntry when a ChecksumException is handled specially.
     */
    LogEntry getLogEntryAllowChecksumException(long lsn)
        throws ChecksumException, FileNotFoundException, DatabaseException {

        final LogSource logSource = getLogSource(lsn);

        try {
            return getLogEntryFromLogSource(
                lsn, 0, logSource, false /*invisibleReadAllowed*/).
                getEntry();
        } finally {
            logSource.release();
        }
    }

    LogEntry getLogEntryAllowChecksumException(long lsn,
                                               RandomAccessFile file,
                                               int logVersion)
        throws ChecksumException, DatabaseException {

        final LogSource logSource = new FileSource(
            file, readBufferSize, fileManager, DbLsn.getFileNumber(lsn),
            logVersion);

        try {
            return getLogEntryFromLogSource(
                lsn, 0, logSource,
                false /*invisibleReadAllowed*/).
                getEntry();
        } finally {
            logSource.release();
        }
    }

    /**
     * Gets log entry from the given source; the caller is responsible for
     * calling logSource.release and handling ChecksumException.
     *
     * Is non-private for unit testing.
     *
     * @param lsn location of entry in log
     * @param lastLoggedSize is the entry size if known, or zero if unknown.
     * @param invisibleReadAllowed if true, we will permit the read of invisible
     * log entries, and we will adjust the invisible bit so that the checksum
     * will validate
     * @return log entry that embodies all the objects in the log entry
     */
    WholeEntry getLogEntryFromLogSource(long lsn,
                                        int lastLoggedSize,
                                        LogSource logSource,
                                        boolean invisibleReadAllowed)
        throws ChecksumException, DatabaseException {

        /*
         * Read the log entry header into a byte buffer. If the
         * lastLoggedSize is available (non-zero), we can use it to avoid a
         * repeat-read further below. Otherwise we use the configured
         * LOG_FAULT_READ_SIZE, and a repeat-read may occur if the log
         * entry is larger than the buffer.
         *
         * Even when lastLoggedSize is non-zero, we do not assume that it
         * is always accurate, because this is not currently guaranteed
         * in corner cases such as transaction aborts. We do the initial
         * read with lastLoggedSize. If lastLoggedSize is larger than the
         * actual size, we will simply read more bytes than needed. If
         * lastLoggedSize is smaller than the actual size, we will do a
         * repeat-read further below.
         */
        long fileOffset = DbLsn.getFileOffset(lsn);

        ByteBuffer entryBuffer = (lastLoggedSize > 0) ?
            logSource.getBytes(fileOffset, lastLoggedSize) :
            logSource.getBytes(fileOffset);

        if (entryBuffer.remaining() < LogEntryHeader.MIN_HEADER_SIZE) {
            throw new ChecksumException(
                "Incomplete log entry header in " + logSource +
                " needed=" + LogEntryHeader.MIN_HEADER_SIZE +
                " remaining=" + entryBuffer.remaining() +
                " lsn=" + DbLsn.getNoFormatString(lsn));
        }

        /* Read the fixed length portion of the header. */
        LogEntryHeader header = new LogEntryHeader(
            entryBuffer, logSource.getLogVersion(), lsn);

        /* Read the variable length portion of the header. */
        if (header.isVariableLength()) {
            if (entryBuffer.remaining() <
                header.getVariablePortionSize()) {
                throw new ChecksumException(
                    "Incomplete log entry header in " + logSource +
                    " needed=" + header.getVariablePortionSize() +
                    " remaining=" + entryBuffer.remaining() +
                    " lsn=" + DbLsn.getNoFormatString(lsn));
            }
            header.readVariablePortion(entryBuffer);
        }

        ChecksumValidator validator = null;
        if (doChecksumOnRead) {
            int itemStart = entryBuffer.position();

            /*
             * We're about to read an invisible log entry, which has knowingly
             * been left on disk with a bad checksum. Flip the invisible bit in
             * the backing byte buffer now, so the checksum will be valid. The
             * LogEntryHeader object itself still has the invisible bit set,
             * which is useful for debugging.
             */
            if (header.isInvisible()) {
                LogEntryHeader.turnOffInvisible
                    (entryBuffer, itemStart - header.getSize());
            }

            /* Add header to checksum bytes */
            validator = new ChecksumValidator();
            int headerSizeMinusChecksum = header.getSizeMinusChecksum();
            entryBuffer.position(itemStart -
                                 headerSizeMinusChecksum);
            validator.update(entryBuffer, headerSizeMinusChecksum);
            entryBuffer.position(itemStart);
        }

        /*
         * Now that we know the size, read the rest of the entry if the first
         * read didn't get enough.
         */
        int itemSize = header.getItemSize();
        if (entryBuffer.remaining() < itemSize) {
            entryBuffer = logSource.getBytes(
                fileOffset + header.getSize(), itemSize);
            if (entryBuffer.remaining() < itemSize) {
                throw new ChecksumException(
                    "Incomplete log entry item in " + logSource +
                    " needed=" + itemSize +
                    " remaining=" + entryBuffer.remaining() +
                    " lsn=" + DbLsn.getNoFormatString(lsn));
            }
            nRepeatFaultReads.increment();
        }

        /*
         * Do entry validation. Run checksum before checking the entry type, it
         * will be the more encompassing error.
         */
        if (doChecksumOnRead) {
            /* Check the checksum first. */
            validator.update(entryBuffer, itemSize);
            validator.validate(header.getChecksum(), lsn);
        }

        /*
         * If invisibleReadAllowed == false, we should not be fetching an
         * invisible log entry.
         */
        if (header.isInvisible() && !invisibleReadAllowed) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                 "Read invisible log entry at " +
                 DbLsn.getNoFormatString(lsn) + " " + header);
        }

        assert LogEntryType.isValidType(header.getType()):
            "Read non-valid log entry type: " + header.getType();

        /* Read the entry. */
        LogEntry logEntry =
            LogEntryType.findType(header.getType()).getNewLogEntry();
        logEntry.readEntry(envImpl, header, entryBuffer);

        /* For testing only; generate a read io exception. */
        if (readHook != null) {
            try {
                readHook.doIOHook();
            } catch (IOException e) {
                /* Simulate what the FileManager would do. */
                throw new EnvironmentFailureException
                    (envImpl, EnvironmentFailureReason.LOG_READ, e);
            }
        }

        return new WholeEntry(header, logEntry);
    }

    /**
     * Fault in the first object in the log entry log entry at this LSN.
     * @param lsn location of object in log
     * @return the object in the log
     */
    public Object getEntry(long lsn)
        throws FileNotFoundException, DatabaseException {

        LogEntry entry = getLogEntry(lsn);
        return entry.getMainItem();
    }

    public Object getEntryHandleFileNotFound(long lsn) {
        LogEntry entry = getLogEntryHandleFileNotFound(lsn);
        return entry.getMainItem();
    }

    /**
     * Find the LSN, whether in a file or still in the log buffers.
     * Is public for unit testing.
     */
    public LogSource getLogSource(long lsn)
        throws FileNotFoundException, ChecksumException, DatabaseException {

        /*
         * First look in log to see if this LSN is still in memory.
         */
        LogBuffer logBuffer = logBufferPool.getReadBufferByLsn(lsn);

        if (logBuffer == null) {
            try {
                /* Not in the in-memory log -- read it off disk. */
                long fileNum = DbLsn.getFileNumber(lsn);
                return new FileHandleSource
                    (fileManager.getFileHandle(fileNum),
                     readBufferSize, fileManager);
            } catch (DatabaseException e) {
                /* Add LSN to exception message. */
                e.addErrorMessage("lsn= " + DbLsn.getNoFormatString(lsn));
                throw e;
            }
        }
        return logBuffer;
    }

    /**
     * Reads a log entry using a FileSource, and returns null (rather than
     * throwing a ChecksumException) if the entry exists in a log buffer that
     * was not flushed.
     *
     * Used to check whether an in-memory corruption is persistent.
     *
     * @return WholeEntry null means that this lsn does not exist in the file
     */
    public WholeEntry getLogEntryDirectFromFile(long lsn)
        throws ChecksumException {

        final LogSource logSource;
        try {
            logSource = getLogSource(lsn);
        } catch (FileNotFoundException fnfe) {
            return null;
        }

        final FileSource fileSource;

        if (logSource instanceof LogBuffer) {

            final FileHandle fileHandle;

            try {
                final LogBuffer logBuffer = (LogBuffer) logSource;
                final long fileLength;
                try {
                    fileHandle =
                        fileManager.getFileHandle(DbLsn.getFileNumber(lsn));
                    fileLength = fileHandle.getFile().length();
                } catch (IOException ioe) {
                    /* FileNotFound or another IOException was thrown. */
                    return null;
                }

                /*
                 * If the file does not exist (FileNotFoundException is thrown
                 * above) or the firstLsn in the buffer does not appear in the
                 * file (the buffer was not flushed), then the corruption is
                 * not persistent and later we will throw a EFE for which
                 * isCorrupted will return false.
                 */
                if (fileLength <=
                        DbLsn.getFileOffset(logBuffer.getFirstLsn())) {
                    return null;
                }
            } finally {
                logSource.release();
            }

            /*
             * The log entry should have been written to the file. Try
             * getting the log entry from the FileSource. If the log entry is
             * incomplete, ChecksumException is thrown below and later we will
             * throw an EFE with LOG_CHECKSUM.
             */
            fileSource = new FileHandleSource(
                fileHandle, readBufferSize, fileManager);
       } else {
            fileSource = (FileSource) logSource;
       }

       try {
           return getLogEntryFromLogSource(
               lsn, 0, fileSource, false /*invisibleReadAllowed*/);
       } finally {
           fileSource.release();
       }
    }

    /**
     * Return a log buffer locked for reading, or null if no log buffer
     * holds this LSN location.
     */
    public LogBuffer getReadBufferByLsn(long lsn) {

        assert DbLsn.getFileOffset(lsn) != 0 :
             "Read of lsn " + DbLsn.getNoFormatString(lsn)  +
            " is illegal because file header entry is not in the log buffer";

        return logBufferPool.getReadBufferByLsn(lsn);
    }

    /**
     * Flush all log entries, fsync the log file.
     */
    public void flushSync()
        throws DatabaseException {

        if (readOnly) {
            return;
        }

        /* The write queue is flushed by syncLogEnd. */
        flushInternal(false /*flushWriteQueue*/);
        fileManager.syncLogEnd();
    }

    /**
     * Flush all log entries and write to the log but do not fsync.
     */
    public void flushNoSync()
        throws DatabaseException {

        if (readOnly) {
            return;
        }

        flushInternal(true /*flushWriteQueue*/);
    }

    /**
     * Flush log buffers, but do not flush the write queue. This is used only
     * by FsyncManager, just prior to an fsync. When FsyncManager performs the
     * fsync, the write queue will be flushed by FileManager.fsyncLogEnd.
     */
    void flushBeforeSync()
        throws DatabaseException {

        if (readOnly) {
            return;
        }

        flushInternal(false /*flushWriteQueue*/);
    }

    /**
     * Flush the dirty log buffers, and optionally the write queue as well.
     *
     * Flushing logically means flushing all write buffers to the file system,
     * so flushWriteQueue should be false only when this method is called just
     * before an fsync (FileManager.syncLogEnd will flush the write queue).
     */
    private void flushInternal(boolean flushWriteQueue)
        throws DatabaseException {

        assert !readOnly;

        /*
         * If we cannot bump the current buffer because there are no
         * free buffers, the only recourse is to write all buffers
         * under the LWL.
         */
        synchronized (logWriteMutex) {
            if (!logBufferPool.bumpCurrent(0)) {
                logBufferPool.bumpAndWriteDirty(0, flushWriteQueue);
                return;
            }
        }

        /*
         * We bumped the current buffer but did not write any buffers above.
         * Write the dirty buffers now.  Hopefully this is the common case.
         */
        logBufferPool.writeDirty(flushWriteQueue);
    }

    public StatGroup loadStats(StatsConfig config)
        throws DatabaseException {

        endOfLog.set(fileManager.getLastUsedLsn());

        StatGroup copyStats = stats.cloneGroup(config.getClear());
        copyStats.addAll(logBufferPool.loadStats(config));
        copyStats.addAll(fileManager.loadStats(config));
        copyStats.addAll(grpManager.loadStats(config));

        return copyStats;
    }

    /**
     * Return the current number of cache misses in a lightweight fashion,
     * without incurring the cost of loading all the stats, and without
     * clearing any stats.
     */
    public long getNCacheMiss() {
        return logBufferPool.getNCacheMiss();
    }

    /**
     * For unit testing.
     */
    public StatGroup getBufferPoolLatchStats() {
        return logBufferPool.getBufferPoolLatchStats();
    }

    /**
     * Returns a tracked summary for the given file which will not be flushed.
     */
    public TrackedFileSummary getUnflushableTrackedSummary(long file) {
        synchronized (logWriteMutex) {
            return envImpl.getUtilizationTracker().
                    getUnflushableTrackedSummary(file);
        }
    }

    /**
     * Removes the tracked summary for the given file.
     */
    public void removeTrackedFile(TrackedFileSummary tfs) {
        synchronized (logWriteMutex) {
            tfs.reset();
        }
    }

    private void updateObsolete(
        LogParams params,
        UtilizationTracker tracker) {

        if (params.packedObsoleteInfo == null &&
            params.obsoleteWriteLockInfo == null) {
            return;
        }

        synchronized (logWriteMutex) {

            /* Count other obsolete info under the log write latch. */
            if (params.packedObsoleteInfo != null) {
                params.packedObsoleteInfo.countObsoleteInfo(
                    tracker, params.nodeDb);
            }

            if (params.obsoleteWriteLockInfo != null) {
                for (WriteLockInfo info : params.obsoleteWriteLockInfo) {
                    tracker.countObsoleteNode(info.getAbortLsn(),
                                              null /*type*/,
                                              info.getAbortLogSize(),
                                              info.getDb());
                }
            }
        }
    }

    /**
     * Count node as obsolete under the log write latch.  This is done here
     * because the log write latch is managed here, and all utilization
     * counting must be performed under the log write latch.
     */
    public void countObsoleteNode(long lsn,
                                  LogEntryType type,
                                  int size,
                                  DatabaseImpl nodeDb,
                                  boolean countExact) {
        synchronized (logWriteMutex) {
            UtilizationTracker tracker = envImpl.getUtilizationTracker();
            if (countExact) {
                tracker.countObsoleteNode(lsn, type, size, nodeDb);
            } else {
                tracker.countObsoleteNodeInexact(lsn, type, size, nodeDb);
            }
        }
    }

    /**
     * A flavor of countObsoleteNode which does not fire an assert if the
     * offset has already been counted. Called through the LogManager so that
     * this incidence of all utilization counting can be performed under the
     * log write latch.
     */
    public void countObsoleteNodeDupsAllowed(long lsn,
                                              LogEntryType type,
                                              int size,
                                              DatabaseImpl nodeDb) {
        synchronized (logWriteMutex) {
            UtilizationTracker tracker = envImpl.getUtilizationTracker();
            tracker.countObsoleteNodeDupsAllowed(lsn, type, size, nodeDb);
        }
    }

    /**
     * @see LocalUtilizationTracker#transferToUtilizationTracker
     */
    public void transferToUtilizationTracker(LocalUtilizationTracker
                                             localTracker)
        throws DatabaseException {
        synchronized (logWriteMutex) {
            UtilizationTracker tracker = envImpl.getUtilizationTracker();
            localTracker.transferToUtilizationTracker(tracker);
        }
    }

    /**
     * @see DatabaseImpl#countObsoleteDb
     */
    public void countObsoleteDb(DatabaseImpl db) {
        synchronized (logWriteMutex) {
            db.countObsoleteDb(envImpl.getUtilizationTracker(),
                               DbLsn.NULL_LSN /*mapLnLsn*/);
        }
    }

    public boolean removeDbFileSummaries(DatabaseImpl db,
                                         Collection<Long> fileNums) {
        synchronized (logWriteMutex) {
            return db.removeDbFileSummaries(fileNums);
        }
    }

    /**
     * @see DatabaseImpl#cloneDbFileSummaries
     */
    public Map<Long, DbFileSummary> cloneDbFileSummaries(DatabaseImpl db) {
        synchronized (logWriteMutex) {
            return db.cloneDbFileSummariesInternal();
        }
    }

    /* For unit testing only. */
    public void setReadHook(TestHook hook) {
        readHook = hook;
    }

    /* For unit testing only. */
    public void setDelayVLSNRegisterHook(TestHook<Object> hook) {
        delayVLSNRegisterHook = hook;
    }

    /* For unit testing only. */
    public void setFlushLogHook(TestHook<CountDownLatch> hook) {
        flushHook = hook;
        grpManager.setFlushLogHook(hook);
    }

    private class LogWriteInfo {
        final LogBufferSegment lbs;
        final VLSN vlsn;
        final long fileOffset;

        LogWriteInfo(final LogBufferSegment bs,
                     final VLSN vlsn,
                     final long fileOffset) {
            lbs = bs;
            this.vlsn = vlsn;
            this.fileOffset = fileOffset;
        }
    }
}
