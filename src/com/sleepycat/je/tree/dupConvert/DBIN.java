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

package com.sleepycat.je.tree.dupConvert;

import java.nio.ByteBuffer;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * A DBIN represents an Duplicate Bottom Internal Node in the JE tree.
 *
 * Obsolete in log version 8, only used by DupConvert and some log readers.
 */
public final class DBIN extends BIN implements Loggable {
    private static final String BEGIN_TAG = "<dbin>";
    private static final String END_TAG = "</dbin>";

    /**
     * Full key for this set of duplicates.
     */
    private byte[] dupKey;

    public DBIN() {
        super();
    }

    /**
     * For Sizeof, set all array fields to null, since they are not part of the
     * fixed overhead.
     */
    public DBIN(SizeofMarker marker) {
        super(marker);
        dupKey = null;
    }

    @Override
    public boolean isDBIN() {
        return true;
    }

    /**
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    @Override
    public boolean containsDuplicates() {
        return true;
    }

    @Override
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.DBIN_FIXED_OVERHEAD;
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    /**
     * For unit test support:
     * @return a string that dumps information about this IN, without
     */
    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append(beginTag());
        sb.append('\n');

        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<dupkey>");
        sb.append(dupKey == null ? "" : Key.dumpString(dupKey, 0));
        sb.append("</dupkey>");
        sb.append('\n');

        sb.append(super.dumpString(nSpaces, false));

        sb.append(TreeUtils.indent(nSpaces));
        sb.append(endTag());
        return sb.toString();
    }

    /**
     * @see IN#getLogType()
     */
    @Override
    public LogEntryType getLogType() {
        return LogEntryType.LOG_DBIN;
    }

    /*
     * Logging support
     */

    /**
     * @see Loggable#getLogSize
     */
    @Override
    public int getLogSize() {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see Loggable#writeToLog
     */
    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see BIN#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        super.readFromLog(itemBuffer, entryVersion);
        dupKey = LogUtils.readByteArray(itemBuffer, (entryVersion < 6));
    }

    /**
     * DBINS need to dump their dup key
     */
    @Override
    protected void dumpLogAdditional(StringBuilder sb) {
        super.dumpLogAdditional(sb);
        sb.append(Key.dumpString(dupKey, 0));
    }

    @Override
    public String shortClassName() {
        return "DBIN";
    }
}
