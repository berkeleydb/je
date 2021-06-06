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
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.TreeUtils;

/**
 * A DupCountLN represents the transactional part of the root of a
 * duplicate tree, specifically the count of dupes in the tree.
 *
 * Obsolete in log version 8, only used by DupConvert and some log readers.
 */
public final class DupCountLN extends LN {

    private static final String BEGIN_TAG = "<dupCountLN>";
    private static final String END_TAG = "</dupCountLN>";

    private int dupCount;

    /**
     * Create an empty DupCountLN, to be filled in from the log.
     */
    public DupCountLN() {
        super();
        dupCount = 0;
    }

    public int getDupCount() {
        return dupCount;
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
    public boolean isDeleted() {
        return false;
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        return MemoryBudget.DUPCOUNTLN_OVERHEAD;
    }

    /*
     * Dumping
     */

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<count v=\"").append(dupCount).append("\"/>").append('\n');
        sb.append(super.dumpString(nSpaces, false));
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    /*
     * Logging
     */

    /**
     * Return the correct log entry type for a DupCountLN depends on whether 
     * it's transactional.
     */
    @Override
    protected LogEntryType getLogType(boolean isInsert,
                                      boolean isTransactional,
                                      DatabaseImpl db) {
        return isTransactional ? LogEntryType.LOG_DUPCOUNTLN_TRANSACTIONAL :
                                 LogEntryType.LOG_DUPCOUNTLN;
    }

    /**
     * @see LN#getLogSize
     */
    @Override
    public int getLogSize() {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see LN#writeToLog
     */
    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see LN#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        super.readFromLog(itemBuffer, entryVersion);
        dupCount = LogUtils.readInt(itemBuffer, (entryVersion < 6));
    }

    /**
     * @see Loggable#logicalEquals
     * DupCountLNs are never replicated.
     */
    @Override
    public boolean logicalEquals(Loggable other) {

        return false;
    }

    /**
     * Dump additional fields
     */
    @Override
    protected void dumpLogAdditional(StringBuilder sb, boolean verbose) {
        super.dumpLogAdditional(sb, verbose);
        sb.append("<count v=\"").append(dupCount).append("\"/>");
    }
}
