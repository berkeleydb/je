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

/**
 * LogBufferSegment is used by a writer to access
 * a portion of a LogBuffer.
 *
 */
class LogBufferSegment {
    private final LogBuffer logBuffer;
    private final ByteBuffer data;

    public LogBufferSegment(LogBuffer lb, ByteBuffer bb) {
        logBuffer = lb;
        data = bb;
    }

    /**
     * Copies the data into the underlying LogBuffer
     * and decrements the LogBuffer pin count.
     * @param dataToCopy data to copy into the underlying
     *        LogBuffer.
     */
    public void put(ByteBuffer dataToCopy) {

        /*
         * The acquisition of the log buffer latch is
         * done to guarantee the java happens-before
         * semantic. There is no other reason to take the
         * latch here.
         */
        logBuffer.latchForWrite();
        data.put(dataToCopy);
        logBuffer.release();
        logBuffer.free();
    }
}
