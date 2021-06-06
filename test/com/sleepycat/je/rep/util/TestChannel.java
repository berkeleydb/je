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
package com.sleepycat.je.rep.util;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * This mimics the two part SocketChannel read done in real life when
 * assembling an incoming message.
 */
public class TestChannel implements ReadableByteChannel {

    ByteBuffer content;

    public TestChannel(ByteBuffer content) {
        this.content = content;
    }

    public int read(ByteBuffer fill) {
        int remaining = fill.remaining();
        for (int i = 0; i < remaining; i++) {
            fill.put(content.get());
        }

        return fill.limit();
    }

    public void close() {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen() {
        throw new UnsupportedOperationException();
    }
}
