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

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * FileSource is used as a channel to a log file when faulting in objects
 * from the log.
 */
class FileSource implements LogSource {

    private final RandomAccessFile file;
    private final int readBufferSize;
    private final FileManager fileManager;
    private final long fileNum;
    private final int logVersion;

    FileSource(RandomAccessFile file,
               int readBufferSize,
               FileManager fileManager,
               long fileNum,
               int logVersion) {
        this.file = file;
        this.readBufferSize = readBufferSize;
        this.fileManager = fileManager;
        this.fileNum = fileNum;
        this.logVersion = logVersion;
    }

    /**
     * @throws DatabaseException in subclasses.
     * @see LogSource#release
     */
    public void release()
        throws DatabaseException {
    }

    /**
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset)
        throws DatabaseException {

        return getBytes(fileOffset, readBufferSize);
    }

    /**
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset, int numBytes)
        throws DatabaseException {

        /* Fill up buffer from file. */
        ByteBuffer destBuf = ByteBuffer.allocate(numBytes);
        fileManager.readFromFile(file, destBuf, fileOffset, fileNum);

        assert EnvironmentImpl.maybeForceYield();

        destBuf.flip();
        return destBuf;
    }

    public int getLogVersion() {
        return logVersion;
    }

    @Override
    public String toString() {
        return "[FileSource file=0x" + Long.toHexString(fileNum) + "]";
    }
}
