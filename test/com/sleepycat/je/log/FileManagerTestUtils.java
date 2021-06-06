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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;

class FileManagerTestUtils {

    static long bumpLsn(FileManager fileManager, long size) {

        boolean flippedFile = fileManager.shouldFlipFile(size);

        return fileManager.advanceLsn(
            fileManager.calculateNextLsn(flippedFile),
            size, flippedFile);
    }

    static void createLogFile(FileManager fileManager,
                                         EnvironmentImpl envImpl,
                                         long logFileSize)
        throws DatabaseException, IOException {

        LogBuffer logBuffer = new LogBuffer(50, envImpl);
        logBuffer.latchForWrite();
        logBuffer.getDataBuffer().flip();
        long size = logFileSize - FileManager.firstLogEntryOffset();
        boolean flippedFile = fileManager.shouldFlipFile(size);
        long lsn = fileManager.calculateNextLsn(flippedFile);
        fileManager.advanceLsn(lsn, size, flippedFile);
        logBuffer.registerLsn(lsn);
        fileManager.writeLogBuffer(logBuffer, true);
        logBuffer.release();
        fileManager.syncLogEndAndFinishFile();
    }
}
