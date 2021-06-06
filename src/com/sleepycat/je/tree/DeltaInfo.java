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

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * DeltaInfo holds the delta for one BIN entry in a partial BIN log entry.
 * The data here is all that we need to update a BIN to its proper state.
 */
public class DeltaInfo implements Loggable {
    private byte[] key;
    private long lsn;
    private byte state;

    DeltaInfo(byte[] key, long lsn, byte state) {
        this.key = key;
        this.lsn = lsn;
        this.state = state;
    }

    /**
     * For reading from the log only.
     *
     * Is public for Sizeof.
     */
    public DeltaInfo() {
        lsn = DbLsn.NULL_LSN;
    }

    @Override
    public int getLogSize() {
        return
            LogUtils.getByteArrayLogSize(key) +
            LogUtils.getPackedLongLogSize(lsn) + // LSN
            1; // state
    }

    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeByteArray(logBuffer, key);
        LogUtils.writePackedLong(logBuffer, lsn);
        logBuffer.put(state);
    }

    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        key = LogUtils.readByteArray(itemBuffer, unpacked);
        lsn = LogUtils.readLong(itemBuffer, unpacked);
        state = itemBuffer.get();
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(Key.dumpString(key, 0));
        sb.append(DbLsn.toString(lsn));
        sb.append("<state kd=\"").append(IN.isStateKnownDeleted(state));
        sb.append("\" pd=\"").append(IN.isStatePendingDeleted(state));
        sb.append("\"/>");
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    /**
     * Always return false, this item should never be compared.
     */
    @Override
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    byte[] getKey() {
        return key;
    }

    byte getState() {
        return state;
    }

    boolean isKnownDeleted() {
        return IN.isStateKnownDeleted(state);
    }

    long getLsn() {
        return lsn;
    }

    /**
     * Returns the number of bytes occupied by this object.  Deltas are not
     * stored in the Btree, but they are budgeted during a SortedLSNTreeWalker
     * run.
     */
    long getMemorySize() {
        return MemoryBudget.DELTAINFO_OVERHEAD +
               MemoryBudget.byteArraySize(key.length);
    }
}
