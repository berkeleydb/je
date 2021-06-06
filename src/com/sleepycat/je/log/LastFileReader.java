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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * LastFileReader traverses the last log file, doing checksums and looking for
 * the end of the log. Different log types can be registered with it and it
 * will remember the last occurrence of targeted entry types.
 */
public class LastFileReader extends FileReader {

    /* Log entry types to track. */
    private Set<LogEntryType> trackableEntries;

    private long nextUnprovenOffset;
    private long lastValidOffset;
    private LogEntryType entryType;

    /*
     * Last LSN seen for tracked types. Key = LogEntryType, data is the offset
     * (Long).
     */
    private Map<LogEntryType, Long> lastOffsetSeen;

    /*
     * A marker log entry used to indicate that the log is physically or
     * semantically corrupt and can't be recovered. A restore of some form must 
     * take place.
     */
    private RestoreRequired restoreRequired;

    /**
     * This file reader is always positioned at the last file.
     *
     * If no valid files exist (and invalid files do not contain data and can
     * be moved away), we will not throw an exception.  We will return false
     * from the first call (all calls) to readNextEntry.
     *
     * @throws DatabaseException if the last file contains data and is invalid.
     */
    public LastFileReader(EnvironmentImpl envImpl,
                          int readBufferSize)
        throws DatabaseException {

        super(envImpl, readBufferSize, true, DbLsn.NULL_LSN, Long.valueOf(-1),
              DbLsn.NULL_LSN, DbLsn.NULL_LSN);

        try {
            startAtLastGoodFile(null /*singleFileNum*/);
        } catch (ChecksumException e) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_CHECKSUM, e);
        }

        trackableEntries = new HashSet<LogEntryType>();
        lastOffsetSeen = new HashMap<LogEntryType, Long>();

        lastValidOffset = 0;
        nextUnprovenOffset = nextEntryOffset;
    }

    /**
     * Ctor which allows passing in the file number we want to read to the end
     * of.  This is used by the ScavengerFileReader when it encounters a bad
     * log record in the middle of a file.
     *
     * @throws ChecksumException rather than wrapping it, to allow
     * ScavengerFileReader to handle it specially -- we should not invalidate
     * the environment with EnvironmentFailureException.
     */
    LastFileReader(EnvironmentImpl envImpl,
                   int readBufferSize,
                   Long specificFileNumber)
        throws ChecksumException, DatabaseException {

        super(envImpl, readBufferSize, true, DbLsn.NULL_LSN,
              specificFileNumber, DbLsn.NULL_LSN, DbLsn.NULL_LSN);

        startAtLastGoodFile(specificFileNumber);

        trackableEntries = new HashSet<LogEntryType>();
        lastOffsetSeen = new HashMap<LogEntryType, Long>();

        lastValidOffset = 0;
        nextUnprovenOffset = nextEntryOffset;
    }

    /**
     * Initialize starting position to the last file with a complete header
     * with a valid checksum.
     */
    private void startAtLastGoodFile(Long singleFileNum)
        throws ChecksumException {

        eof = false;
        window.initAtFileStart(DbLsn.makeLsn(0, 0));

        /*
         * Start at what seems like the last file. If it doesn't exist, we're
         * done.
         */
        Long lastNum = ((singleFileNum != null) &&
                        (singleFileNum.longValue() >= 0)) ?
            singleFileNum :
            fileManager.getLastFileNum();
        FileHandle fileHandle = null;

        long fileLen = 0;
        while ((fileHandle == null) && !eof) {
            if (lastNum == null) {
                eof = true;
            } else {
                try {
                    try {
                        window.initAtFileStart(DbLsn.makeLsn(lastNum, 0));
                        fileHandle = fileManager.getFileHandle(lastNum);

                        /*
                         * Check the size of this file. If it opened
                         * successfully but only held a header or is 0 length,
                         * backup to the next "last" file unless this is the
                         * only file in the log. Note that an incomplete header
                         * will end up throwing a checksum exception, but a 0
                         * length file will open successfully in read only
                         * mode.
                         */
                        fileLen = fileHandle.getFile().length();
                        if (fileLen <= FileManager.firstLogEntryOffset()) {
                            lastNum = fileManager.getFollowingFileNum
                                (lastNum, false);
                            if (lastNum != null) {
                                fileHandle.release();
                                fileHandle = null;
                            }
                        }
                    } catch (DatabaseException e) {
                        lastNum = attemptToMoveBadFile(e);
                        fileHandle = null;
                    } catch (ChecksumException e) {
                        lastNum = attemptToMoveBadFile(e);
                        fileHandle = null;
                    } finally {
                        if (fileHandle != null) {
                            fileHandle.release();
                        }
                    }
                } catch (IOException e) {
                    throw new EnvironmentFailureException
                        (envImpl, EnvironmentFailureReason.LOG_READ, e);
                }
            }
        }

        nextEntryOffset = 0;
    }

    /**
     * Something is wrong with this file. If there is no data in this file (the
     * header is <= the file header size) then move this last file aside and
     * search the next "last" file. If the last file does have data in it,
     * return null and throw an exception back to the application, since we're
     * not sure what to do now.
     *
     * @param cause is a DatabaseException or ChecksumException.
     */
    private Long attemptToMoveBadFile(Exception cause)
        throws IOException, ChecksumException, DatabaseException {

        String fileName = 
            fileManager.getFullFileNames(window.currentFileNum())[0];
        File problemFile = new File(fileName);

        if (problemFile.length() <= FileManager.firstLogEntryOffset()) {
            fileManager.clear(); // close all existing files
            /* Move this file aside. */
            Long lastNum = fileManager.getFollowingFileNum
                (window.currentFileNum(), false);
            if (!fileManager.renameFile(window.currentFileNum(),
                                        FileManager.BAD_SUFFIX)) {
                throw EnvironmentFailureException.unexpectedState
                    ("Could not rename file: 0x" +
                      Long.toHexString(window.currentFileNum()));
            }

            return lastNum;
        }
        /* There's data in this file, throw up to the app. */
        if (cause instanceof DatabaseException) {
            throw (DatabaseException) cause;
        }
        if (cause instanceof ChecksumException) {
            throw (ChecksumException) cause;
        }
        throw EnvironmentFailureException.unexpectedException(cause);
    }

    public void setEndOfFile()
        throws IOException, DatabaseException {

        fileManager.truncateSingleFile
            (window.currentFileNum(), nextUnprovenOffset);
    }

    /**
     * @return The LSN to be used for the next log entry.
     */
    public long getEndOfLog() {
        return DbLsn.makeLsn(window.currentFileNum(), nextUnprovenOffset);
    }

    public long getLastValidLsn() {
        return DbLsn.makeLsn(window.currentFileNum(), lastValidOffset);
    }

    public long getPrevOffset() {
        return lastValidOffset;
    }

    public LogEntryType getEntryType() {
        return entryType;
    }

    /**
     * Tell the reader that we are interested in these kind of entries.
     */
    public void setTargetType(LogEntryType type) {
        trackableEntries.add(type);
    }

    /**
     * @return The last LSN seen in the log for this kind of entry, or null.
     */
    public long getLastSeen(LogEntryType type) {
        Long typeNumber = lastOffsetSeen.get(type);
        if (typeNumber != null) {
            return DbLsn.makeLsn(window.currentFileNum(), 
                                 typeNumber.longValue());
        } else {
            return DbLsn.NULL_LSN;
        }
    }

    /**
     * Validate the checksum on each entry, see if we should remember the LSN
     * of this entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer) {

        /* 
         * If we're supposed to remember this LSN, record it. Although LSN 
         * recording is currently done for test purposes only, still do get a 
         * valid logEntyType, so it can be used for reading in a log entry 
         * further in this method.
         */
        entryType = LogEntryType.findType(currentEntryHeader.getType());
        if (trackableEntries.contains(entryType)) {
            lastOffsetSeen.put(entryType, Long.valueOf(currentEntryOffset));
        }

        /*
         * If this is a RestoreRequired entry, read it and
         * deserialize. Otherwise, skip over the data, we're not doing anything
         * with it.
         */
        if (entryType.equals(LogEntryType.LOG_RESTORE_REQUIRED)) {
            LogEntry logEntry = entryType.getSharedLogEntry();
            logEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
            restoreRequired = (RestoreRequired) logEntry.getMainItem();
        } else {
            entryBuffer.position(entryBuffer.position() +
                                 currentEntryHeader.getItemSize());
        }

        return true;
    }

    /**
     * readNextEntry will stop at a bad entry.
     * @return true if an element has been read.
     */
    @Override
    public boolean readNextEntry() {

        boolean foundEntry = false;

        try {

            /*
             * At this point,
             *  currentEntryOffset is the entry we just read.
             *  nextEntryOffset is the entry we're about to read.
             *  currentEntryPrevOffset is 2 entries ago.
             * Note that readNextEntry() moves all the offset pointers up.
             */

            foundEntry = super.readNextEntryAllowExceptions();

            /*
             * Note that initStartingPosition() makes sure that the file header
             * entry is valid.  So by the time we get to this method, we know
             * we're at a file with a valid file header entry.
             */
            lastValidOffset = currentEntryOffset;
            nextUnprovenOffset = nextEntryOffset;
        } catch (FileNotFoundException e) {
            throw new EnvironmentFailureException
                (envImpl,
                 EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
        } catch (ChecksumException e) {
            LoggerUtils.fine
                (logger, envImpl,  
                 "Found checksum exception while searching for end of log. " +
                 "Last valid entry is at " + DbLsn.toString
                 (DbLsn.makeLsn(window.currentFileNum(), lastValidOffset)) +
                 " Bad entry is at " +
                 DbLsn.makeLsn(window.currentFileNum(), nextUnprovenOffset));
                 
            DbConfigManager configManager = envImpl.getConfigManager();
            boolean findCommitTxn = 
                configManager.getBoolean
                (EnvironmentParams.HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION);
                
            /* Find the committed transactions at the rest the log file. */
            if (findCommitTxn) {
                boolean committedTxnFound = findCommittedTxn();  
                /* If we have found a committed txn. */
                if (committedTxnFound) {
                    throw new EnvironmentFailureException
                        (envImpl, EnvironmentFailureReason.FOUND_COMMITTED_TXN, 
                         "Found committed txn after the corruption point");
                }
            }
        }
        return foundEntry;
    }
    
    /* 
     * [#18307] Find the committed transaction after the corrupted log entry 
     * log file.
     *
     * Suppose we have LSN 1000, and the log entry there has a checksum 
     * exception.
     * Case 1. if the header at LSN 1000 is bad, findCommittedTxn() will not be
     *         called, and just truncate the log.
     *         Note: This case seems to not be handled. It seems we do call
     *         findCommittedTxn when the header is bad. Perhaps this comment
     *         is outdated.
     * Case 2. if the header at LSN 1000 says the log entry size is N, we 
     *         skip N bytes, which may let us go to the next log entry. If the 
     *         next log entry also has a checksum exception (because size N is 
     *         wrong, or because it really has a checksum problem), return 
     *         false, and truncate the log.
     * Case 3. if we manage to read past LSN 1000, but then hit a second 
     *         checksum exception, return false and truncate the log at the 
     *         first exception.
     * Case 4. if we manage to read past LSN 1000, and do not see any checksum 
     *         exceptions, and do not see any commits, return false and 
     *         truncate the log.
     * Case 5. if we manage to read past LSN 1000, and do not see any checksum 
     *         exceptions, but do see a txn commit, return true and throw 
     *         EnvironmentFailureException.
     */
    private boolean findCommittedTxn() {
        try {
        
            /* 
             * First we skip over the bad log entry, according to the item size
             * we get from the log entry's header.
             */
            skipData(currentEntryHeader.getItemSize());
            
            /* 
             * Begin searching for the committed txn from the next log entry to 
             * the end of the log file.
             */
            while (super.readNextEntryAllowExceptions()) {
                /* Case 5. */
                if (LogEntryType.LOG_TXN_COMMIT.equals(entryType)) {
                    return true;
                }
            }
        } catch (EOFException e) {
        } catch (FileNotFoundException e) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
        } catch (ChecksumException e) {
            /* Case 2 and 3. */
            LoggerUtils.fine
                (logger, envImpl,  
                 "Found checksum exception while searching for end of log. " +
                 "Last valid entry is at " + DbLsn.toString
                 (DbLsn.makeLsn(window.currentFileNum(), lastValidOffset)) + 
                 " Bad entry is at " +
                 DbLsn.makeLsn(window.currentFileNum(), nextUnprovenOffset));
        }
        
        /* Case 4. */
        return false;
    }

    public RestoreRequired getRestoreRequired() {
        return restoreRequired;
    }
}
