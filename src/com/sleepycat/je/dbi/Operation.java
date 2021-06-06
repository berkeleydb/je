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

package com.sleepycat.je.dbi;

import java.nio.ByteBuffer;

/**
 * An enumeration of different api call sources for replication, currently for
 * debugging. This is also intended to support the future possibility of
 * providing application level visibility into the replication operation
 * stream.
 */
public class Operation {

    public static final Operation PUT =
        new Operation((byte) 1, "PUT");
    public static final Operation NO_OVERWRITE =
        new Operation((byte) 2, "NO_OVERWRITE");
    public static final Operation PLACEHOLDER =
        new Operation((byte) 3, "PLACEHOLDER");

    private static final Operation[] ALL_OPS =
    {PUT, NO_OVERWRITE, PLACEHOLDER };

    private static final byte MAX_OP = 3;
    private static final byte MIN_OP = 1;

    private byte op;
    private String name;

    public Operation() {
    }

    private Operation(byte op, String name) {
        this.op = op;
        this.name = name;
    }

    public int getContentSize() {
        return 1;
    }

    /**
     * Serialize this object into the buffer.
     * @param buffer is the destination buffer
     */
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.put(op);
    }

    public static Operation readFromBuffer(ByteBuffer buffer) {
        byte opNum = buffer.get();
        if (opNum >= MIN_OP &&
            opNum <= MAX_OP) {
            return ALL_OPS[opNum - 1];
        } else {
            return new Operation(opNum, "UNKNOWN " + opNum);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
