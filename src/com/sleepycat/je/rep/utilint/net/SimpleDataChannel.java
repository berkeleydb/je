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

package com.sleepycat.je.rep.utilint.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


/**
 * A basic concrete extension of DataChannel.
 * This simply delegates operations directly to the underlying SocketChannel
 */
public class SimpleDataChannel extends AbstractDataChannel {

    /**
     * Constructor for general use.
     *
     * @param socketChannel A SocketChannel, which should be connected.
     */
    public SimpleDataChannel(SocketChannel socketChannel) {
        super(socketChannel);
    }

    /*
     * The following ByteChannel implementation methods delegate to the wrapped
     * channel object.
     */

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return socketChannel.read(dsts);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
        throws IOException {

        return socketChannel.read(dsts, offset, length);
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return socketChannel.write(srcs);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
        throws IOException {

        return socketChannel.write(srcs, offset, length);
    }

    /**
     * Is the channel encrypted?
     */
    @Override
    public boolean isSecure() {
        return false;
    }

    /**
     * Is the channel peer trusted?
     */
    @Override
    public boolean isTrusted() {
        return false;
    }

    /**
     * Is the channel peer trust capable?
     */
    @Override
    public boolean isTrustCapable() {
        return false;
    }

    /**
     * Returns DISABLED, since this implementation does not do any flushing.
     */
    @Override
    public FlushStatus flush() {
        return FlushStatus.DISABLED;
    }
}

