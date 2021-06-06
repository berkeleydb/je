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

package com.sleepycat.je.rep.net;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SocketChannel;

/**
 * @hidden
 * An interface that associates a delegate socketChannel for network I/O, which
 * provides ByteChannel, GatheringByteChannel, and ScatteringByteChannel,
 * interfaces for callers.
 */
public interface DataChannel extends ByteChannel,
                                     GatheringByteChannel,
                                     ScatteringByteChannel {

    /**
     * Accessor for the underlying SocketChannel.
     * Callers may used the returned SocketChannel in order to query/modify
     * connections attributes, but may not directly close, read from or write
     * to the SocketChannel.
     *
     * @return the socket channel underlying this data channel instance
     */
    public SocketChannel getSocketChannel();

    /**
     * Checks whether the channel encrypted.
     *
     * @return true if the data channel provides network privacy
     */
    public boolean isSecure();

    /**
     * Checks whether  the channel capable of determining peer trust.
     *
     * @return true if the data channel implementation has the capability
     * to determine trust.
     */
    public boolean isTrustCapable();

    /**
     * Checks whether the channel peer is trusted.
     *
     * @return true if the channel has determined that the peer is trusted.
     */
    public boolean isTrusted();

    /**
     * The status of the flush method.
     */
    public enum FlushStatus {

        /** Flushes are not being used. */
        DISABLED,

        /** Nothing needs to be flushed. */
        DONE,

        /** Flush not complete because there is something left to flush. */
        AGAIN,

        /** Flush not complete because socket is busy for write. */
        WRITE_BUSY,

        /** Flush not complete due to read data dependency. */
        NEED_READ,

        /** Flush not complete due to task execution dependency. */
        NEED_TASK,
    }

    /**
     * Attempt to flush any pending writes to the underlying socket buffer.
     * The caller should ensure that it is the only thread accessing the
     * DataChannel in order that the return value be meaningful.
     *
     * @return the flush status
     */
    public FlushStatus flush() throws IOException;
}

