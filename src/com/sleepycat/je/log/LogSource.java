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

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;

/**
 * A class that implements LogSource can return portions of the log.
 * Is public for unit testing.
 */
public interface LogSource {

    /**
     * We're done with this log source.
     */
    void release() throws DatabaseException;

    /**
     * Fill the destination byte array with bytes. The offset indicates the
     * absolute log file position.
     */
    ByteBuffer getBytes(long fileOffset) throws DatabaseException;

    /**
     * Fill the destination byte array with the requested number of bytes.  The
     * offset indicates the absolute position in the log file.
     */
    ByteBuffer getBytes(long fileOffset, int numBytes)
        throws DatabaseException;

    /**
     * Returns the log version of the log entries from this source.
     */
    int getLogVersion();
}
