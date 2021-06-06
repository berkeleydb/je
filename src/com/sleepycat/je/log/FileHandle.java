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

import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchFactory;

/**
 * A FileHandle embodies a File and its accompanying latch.
 */
public class FileHandle {
    private RandomAccessFile file;
    private Latch fileLatch;
    private int logVersion;
    private long fileNum;

    /**
     * Creates a new handle but does not initialize it.  The init method must
     * be called before using the handle to access the file.
     */
    FileHandle(EnvironmentImpl envImpl, long fileNum, String label) {
        fileLatch = LatchFactory.createExclusiveLatch(
            envImpl, "file_" + label + "_fileHandle", false /*collectStats*/);
        this.fileNum = fileNum;
    }

    /**
     * Initializes the handle after opening the file and reading the header.
     */
    void init(RandomAccessFile file, int logVersion) {
        assert this.file == null;
        this.file = file;
        this.logVersion = logVersion;
    }

    RandomAccessFile getFile() {
        return file;
    }

    long getFileNum() {
        return fileNum;
    }

    public int getLogVersion() {
        return logVersion;
    }

    boolean isOldHeaderVersion() {
        return logVersion < LogEntryType.LOG_VERSION;
    }

    void latch()
        throws DatabaseException {

        fileLatch.acquireExclusive();
    }

    boolean latchNoWait()
        throws DatabaseException {

        return fileLatch.acquireExclusiveNoWait();
    }

    public void release()
        throws DatabaseException {

        fileLatch.release();
    }

    void close()
        throws IOException {

        if (file != null) {
            try {
                file.close();
            } finally {
                file = null;
            }
        }
    }
}
