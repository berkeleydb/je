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

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileHeader;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;

/**
 * Contains a FileHeader entry.
 */
public class FileHeaderEntry extends SingleItemEntry<FileHeader> {

    /**
     * Construct a log entry for reading.
     */
    public FileHeaderEntry(Class<FileHeader> logClass) {
        super(logClass);
    }

    /**
     * Construct a log entry for writing.
     */
    public FileHeaderEntry(LogEntryType entryType, FileHeader item) {
        super(entryType, item);
    }

    /**
     * For a file header, the version is not available until after reading the
     * item.  Set the version in the entry header so it can be used by
     * FileReaders, etc.  [#16939]
     */
    @Override
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {
        super.readEntry(envImpl, header, entryBuffer);
        FileHeader entry = (FileHeader) getMainItem();
        header.setFileHeaderVersion(entry.getLogVersion());
    }
}
