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

import com.sleepycat.je.DatabaseException;

/**
 * FileHandleSource is a file source built on top of a cached file handle.
 */
class FileHandleSource extends FileSource {

    private FileHandle fileHandle;

    FileHandleSource(FileHandle fileHandle,
                     int readBufferSize,
                     FileManager fileManager) {
        super(fileHandle.getFile(), readBufferSize, fileManager,
              fileHandle.getFileNum(), fileHandle.getLogVersion());
        this.fileHandle = fileHandle;
    }

    /**
     * @see LogSource#release
     */
    @Override
    public void release()
        throws DatabaseException {

        fileHandle.release();
    }

    public int getLogVersion() {
        return fileHandle.getLogVersion();
    }
}
