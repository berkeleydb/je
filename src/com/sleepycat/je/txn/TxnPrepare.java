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

package com.sleepycat.je.txn;

import java.nio.ByteBuffer;

import javax.transaction.xa.Xid;

import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * This class writes out a transaction prepare record.
 */
public class TxnPrepare extends TxnEnd implements Loggable {

    private Xid xid;

    public TxnPrepare(long id, Xid xid) {
        /* LastLSN is never used. */
        super(id, DbLsn.NULL_LSN,
              0 /* masterNodeId, never replicated. */,
              0l /* dtvlsn, never replicated. */);
        this.xid = xid;
    }

    /**
     * For constructing from the log.
     */
    public TxnPrepare() {
    }

    public Xid getXid() {
        return xid;
    }

    /*
     * Log support
     */

    @Override
    protected String getTagName() {
        return "TxnPrepare";
    }

    @Override
    public int getLogSize() {
        return LogUtils.getPackedLongLogSize(id) +
            LogUtils.getTimestampLogSize(time) +
            LogUtils.getXidSize(xid);
    }

    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writePackedLong(logBuffer, id);
        LogUtils.writeTimestamp(logBuffer, time);
        LogUtils.writeXid(logBuffer, xid);
    }

    @Override
    public void readFromLog(ByteBuffer logBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        id = LogUtils.readLong(logBuffer, unpacked);
        time = LogUtils.readTimestamp(logBuffer, unpacked);
        xid = LogUtils.readXid(logBuffer);
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<").append(getTagName());
        sb.append(" id=\"").append(id);
        sb.append("\" time=\"").append(time);
        sb.append("\">");
        sb.append(xid); // xid already formatted as xml
        sb.append("</").append(getTagName()).append(">");
    }

    /**
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }
}
