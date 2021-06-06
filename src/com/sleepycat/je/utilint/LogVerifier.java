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
import java.util.Arrays;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.ChecksumValidator;
import com.sleepycat.je.log.FileHeader;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.util.LogVerificationException;

/**
 * Verifies the checksums in the contents of a log file in a JE {@code
 * Environment}.
 *
 * <p>The caller supplies the contents of the log file by passing arrays of
 * bytes in a series of calls to the {@link #verify} method, which verifies the
 * checksums for log records, and by calling the {@link #verifyAtEof} when the
 * entire contents are complete, to detect incomplete entries at the end of the
 * file.  The primary intended use of this class is to verify the contents of
 * log files that are being copied as part of a programmatic backup.  It is
 * critical that invalid files are not added to a backup set, since then both
 * the live environment and the backup will be invalid.
 *
 * @see com.sleepycat.je.util.LogVerificationInputStream
 */
public class LogVerifier {

    private static final byte FILE_HEADER_TYPE_NUM =
                              LogEntryType.LOG_FILE_HEADER.getTypeNum();

    private final EnvironmentImpl envImpl;
    private final String fileName;
    private final long fileNum;

    /* Stream verification state information. */
    private enum State {
        INIT, FIXED_HEADER, VARIABLE_HEADER, ITEM, FILE_HEADER_ITEM, INVALID
    }
    private State state;
    private long entryStart;
    private long prevEntryStart;
    private final ChecksumValidator validator;
    private final ByteBuffer headerBuf;
    private LogEntryHeader header;
    private int itemPosition;
    private int logVersion;

    /**
     * Creates a log verifier.
     *
     * @param env the {@code Environment} associated with the log
     *
     * @param fileName the file name of the log, for reporting in the {@code
     * LogVerificationException}.  This should be a simple file name of the
     * form {@code NNNNNNNN.jdb}, where NNNNNNNN is the file number in
     * hexadecimal format.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    public LogVerifier(final Environment env, final String fileName) {
        this(DbInternal.getNonNullEnvImpl(env), fileName);
    }

    /**
     * Creates a log verifier.
     *
     * @param envImpl the {@code EnvironmentImpl} associated with the log
     *
     * @param fileName the file name of the log, for reporting in the {@code
     * LogVerificationException}.  This should be a simple file name of the
     * form {@code NNNNNNNN.jdb}, where NNNNNNNN is the file number in
     * hexadecimal format.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    public LogVerifier(final EnvironmentImpl envImpl, final String fileName) {
        this(envImpl, fileName, -1L);
    }

    /**
     * <p>Creates a log verifier for use with an internal environment.  If
     * {@code fileNum} is less than zero, it is derived from {@code fileName}.
     *
     * @param envImpl the {@code EnvironmentImpl} associated with the log
     *
     * @param fileName the file name of the log, for reporting in the {@code
     * LogVerificationException}.  This should be a simple file name of the
     * form {@code NNNNNNNN.jdb}, where NNNNNNNN is the file number in
     * hexadecimal format.
     *
     * @param fileNum the file number
     */
    public LogVerifier(final EnvironmentImpl envImpl,
                       final String fileName,
                       final long fileNum) {
        this.envImpl = envImpl;
        this.fileName = fileName;
        this.fileNum = (fileNum >= 0) ?
            fileNum : envImpl.getFileManager().getNumFromName(fileName);
        state = State.INIT;
        entryStart = 0L;
        prevEntryStart = 0L;
        validator = new ChecksumValidator();

        /*
         * The headerBuf is used to hold the fixed entry header, variable entry
         * header portion, and file header entry.
         */
        headerBuf = ByteBuffer.allocate
            (Math.max(LogEntryHeader.MAX_HEADER_SIZE, FileHeader.entrySize()));

        /* Initial log version for reading the file header. */
        logVersion = LogEntryType.UNKNOWN_FILE_HEADER_VERSION;
    }

    /**
     * Verifies the next portion of the log file.
     *
     * @param buf the buffer containing the log file bytes
     *
     * @param off the start offset of the log file bytes in the buffer
     *
     * @param len the number of log file bytes in the buffer
     *
     * @throws LogVerificationException if a checksum cannot be verified or a
     * log entry is determined to be invalid by examining its contents
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    public void verify(final byte[] buf, final int off, final int len)
        throws LogVerificationException {

        final int endOffset = off + len;
        int curOffset = off;
        while (curOffset < endOffset) {
            final int remaining = endOffset - curOffset;
            switch (state) {
            case INIT:
                processInit();
                break;
            case FIXED_HEADER:
                curOffset = processFixedHeader(buf, curOffset, remaining);
                break;
            case VARIABLE_HEADER:
                curOffset = processVariableHeader(buf, curOffset, remaining);
                break;
            case FILE_HEADER_ITEM:
                curOffset = processFileHeaderItem(buf, curOffset, remaining);
                break;
            case ITEM:
                curOffset = processItem(buf, curOffset, remaining);
                break;
            case INVALID:
                throw newVerifyException
                    ("May not read after LogVerificationException is thrown");
            default:
                assert false;
            }
        }
    }

    /**
     * Checks that the log file ends with a complete log entry, after having
     * completed verifying the log file contents through calls to {@link
     * #verify}.
     *
     * @throws LogVerificationException if the stream does not end with a
     * complete log entry
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    public void verifyAtEof()
        throws LogVerificationException {

        /* State should be INIT at EOF. */
        if (state == State.INIT) {
            return;
        }

        /* Ignore partial entry at end of last log file. */
        if (fileNum == envImpl.getFileManager().getLastFileNum()) {
            return;
        }

        /* Report partial entry at end of any other file. */
        throw newVerifyException("Entry is incomplete");
    }

    /**
     * Initializes all state variables before the start of a log entry.  Moves
     * the state to FIXED_HEADER, the first part of a log entry.
     */
    private void processInit() {
        validator.reset();
        headerBuf.clear();
        header = null;
        itemPosition = 0;
        state = State.FIXED_HEADER;
    }

    /**
     * Processes the fixed initial portion of a log entry.  After all bytes for
     * the fixed portion are read, moves the state to VARIABLE_HEADER if the
     * header contains a variable portion, or to ITEM if it does not.
     */
    private int processFixedHeader(final byte[] buf,
                                   final int curOffset,
                                   final int remaining)
        throws LogVerificationException {

        assert header == null;

        final int maxSize = LogEntryHeader.MIN_HEADER_SIZE;
        final int processSize =
            Math.min(remaining, maxSize - headerBuf.position());

        headerBuf.put(buf, curOffset, processSize);
        assert headerBuf.position() <= maxSize;

        if (headerBuf.position() == maxSize) {
            headerBuf.flip();
            try {
                header = new LogEntryHeader(
                    headerBuf, logVersion, DbLsn.makeLsn(fileNum, entryStart));
            } catch (ChecksumException e) {
                throw newVerifyException(
                    "Invalid header bytes=" +
                        Arrays.toString(headerBuf.array()),
                    e);
            }

            if (header.getPrevOffset() != prevEntryStart) {
                throw newVerifyException(
                    "Header prevOffset=0x" +
                    Long.toHexString(header.getPrevOffset()) +
                    " but prevEntryStart=0x" +
                    Long.toHexString(prevEntryStart));
            }

            /* If the header is invisible, turn off the invisible bit. */
            if (header.isInvisible()) {
                LogEntryHeader.turnOffInvisible(headerBuf, 0);
            }

            /* Do not validate the bytes of the checksum itself. */
            validator.update(headerBuf.array(),
                             LogEntryHeader.CHECKSUM_BYTES,
                             maxSize - LogEntryHeader.CHECKSUM_BYTES);

            if (header.isVariableLength()) {
                headerBuf.clear();
                state = State.VARIABLE_HEADER;
            } else if (header.getType() == FILE_HEADER_TYPE_NUM) {
                headerBuf.clear();
                state = State.FILE_HEADER_ITEM;
            } else {
                state = State.ITEM;
            }
        }

        return curOffset + processSize;
    }

    /**
     * Processes the variable portion of a log entry.  After all bytes for the
     * variable portion are read, moves the state to ITEM.
     */
    private int processVariableHeader(final byte[] buf,
                                      final int curOffset,
                                      final int remaining) {
        assert header != null;
        assert header.isVariableLength();

        final int maxSize = header.getVariablePortionSize();
        final int processSize =
            Math.min(remaining, maxSize - headerBuf.position());

        headerBuf.put(buf, curOffset, processSize);
        assert headerBuf.position() <= maxSize;

        if (headerBuf.position() == maxSize) {
            headerBuf.flip();
            header.readVariablePortion(headerBuf);
            validator.update(headerBuf.array(), 0, maxSize);

            if (header.getType() == FILE_HEADER_TYPE_NUM) {
                headerBuf.clear();
                state = State.FILE_HEADER_ITEM;
            } else {
                state = State.ITEM;
            }
        }

        return curOffset + processSize;
    }

    private int processFileHeaderItem(final byte[] buf,
                                      final int curOffset,
                                      final int remaining)
        throws LogVerificationException {

        assert header != null;
        assert logVersion == LogEntryType.UNKNOWN_FILE_HEADER_VERSION;

        final int maxSize = FileHeader.entrySize();
        final int processSize =
            Math.min(remaining, maxSize - headerBuf.position());

        headerBuf.put(buf, curOffset, processSize);
        assert headerBuf.position() <= maxSize;

        if (headerBuf.position() == maxSize) {
            validator.update(headerBuf.array(), 0, maxSize);
            try {
                validator.validate(header.getChecksum(), fileNum, entryStart);
            } catch (ChecksumException e) {
                throw newVerifyException(e);
            }

            headerBuf.flip();
            LogEntry fileHeaderEntry =
                LogEntryType.LOG_FILE_HEADER.getNewLogEntry();
            fileHeaderEntry.readEntry(envImpl, header, headerBuf);
            FileHeader fileHeaderItem =
                (FileHeader) fileHeaderEntry.getMainItem();

            /* Log version in the file header applies to all other entries. */
            logVersion = fileHeaderItem.getLogVersion();

            prevEntryStart = entryStart;
            entryStart += header.getSize() + maxSize;
            state = State.INIT;
        }

        return curOffset + processSize;
    }

    /**
     * Processes the item portion of a log entry.  After all bytes for the item
     * are read, moves the state back to INIT and bumps the entryStart.
     */
    private int processItem(final byte[] buf,
                            final int curOffset,
                            final int remaining)
        throws LogVerificationException {

        assert header != null;

        final int maxSize = header.getItemSize();
        final int processSize = Math.min(remaining, maxSize - itemPosition);

        validator.update(buf, curOffset, processSize);
        itemPosition += processSize;
        assert itemPosition <= maxSize;

        if (itemPosition == maxSize) {
            try {
                validator.validate(header.getChecksum(), fileNum, entryStart);
            } catch (ChecksumException e) {
                /*
                LogEntryType lastEntryType = LogEntryType.findType(header.getType());
                System.out.println();
                System.out.println(
                "Checksum error in logrec of tyoe " +
                lastEntryType.toStringNoVersion() + " log version: " + logVersion);
                System.out.println();
                */
                throw newVerifyException(e);
            }

            prevEntryStart = entryStart;
            entryStart += header.getSize() + maxSize;
            state = State.INIT;
        }

        return curOffset + processSize;
    }

    private LogVerificationException newVerifyException(String reason) {
        return newVerifyException(reason, null);
    }

    private LogVerificationException newVerifyException(Throwable cause) {
        return newVerifyException(cause.toString(), cause);
    }

    private LogVerificationException newVerifyException(String reason,
                                                        Throwable cause) {
        state = State.INVALID;

        final String logEntrySize;

        if (header != null) {
            logEntrySize =
                String.valueOf(header.getSize() + header.getItemSize());
        } else {
            logEntrySize = "unknown";
        }

        return new LogVerificationException
            ("Log is invalid, fileName: " + fileName +
             " fileNumber: 0x" + Long.toHexString(fileNum) +
             " logEntryOffset: 0x" + Long.toHexString(entryStart) +
             " logEntrySize: " + logEntrySize +
             " verifyState: " + state +
             " reason: " + reason,
             cause);
    }
}
