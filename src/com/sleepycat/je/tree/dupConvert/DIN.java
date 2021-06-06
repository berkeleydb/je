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
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * An DIN represents an Duplicate Internal Node in the JE tree.
 *
 * Obsolete in log version 8, only used by DupConvert and some log readers.
 */
public final class DIN extends IN {

    private static final String BEGIN_TAG = "<din>";
    private static final String END_TAG = "</din>";

    /**
     * Full key for this set of duplicates. For example, if the tree
     * contains k1/d1, k1/d2, k1/d3, the dupKey = k1.
     */
    private byte[] dupKey;

    /**
     * Reference to DupCountLN which stores the count.
     */
    private ChildReference dupCountLNRef;

    /**
     * Create an empty DIN, with no node ID, to be filled in from the log.
     */
    public DIN() {
        super();

        dupCountLNRef = new ChildReference();
        init(null, Key.EMPTY_KEY, 0, 0);
    }

    /**
     * For Sizeof, set all array fields to null, since they are not part of the
     * fixed overhead.
     */
    public DIN(SizeofMarker marker) {
        super(marker);
        dupKey = null;
    }

    @Override
    public boolean isDIN() {
        return true;
    }

    public ChildReference getDupCountLNRef() {
        return dupCountLNRef;
    }

    /**
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    @Override
    public boolean containsDuplicates() {
        return true;
    }

    /**
     * Count up the memory usage attributable to this node alone. LNs children
     * are counted by their BIN/DIN parents, but INs are not counted by
     * their parents because they are resident on the IN list.
     */
    @Override
    public long computeMemorySize() {
        long size = super.computeMemorySize();
        if (dupCountLNRef != null) {
            size += MemoryBudget.byteArraySize(dupCountLNRef.getKey().length);
            if (dupCountLNRef.getTarget() != null) {
                size += dupCountLNRef.getTarget().
                    getMemorySizeIncludedByParent();
            }
        }
        return size;
    }

    /* Utility method used during unit testing. */
    @Override
    protected long printMemorySize() {
        final long inTotal = super.printMemorySize();
        long dupKeySize = 0;
        long dupLNSize = 0;

        if (dupCountLNRef != null) {
            dupKeySize = MemoryBudget.
                byteArraySize(dupCountLNRef.getKey().length);
            if (dupCountLNRef.getTarget() != null) {
                dupLNSize =
                    dupCountLNRef.getTarget().getMemorySizeIncludedByParent();
            }
        }

        final long dupTotal = inTotal + dupKeySize + dupLNSize;
        System.out.format("DIN: %d dkey: %d ln: %d %n",
                          dupTotal, dupKeySize, dupLNSize);
        return dupTotal;
    }

    @Override
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.DIN_FIXED_OVERHEAD;
    }

    /*
     * Logging Support
     */

    /**
     * @see IN#getLogType
     */
    @Override
    public LogEntryType getLogType() {
        return LogEntryType.LOG_DIN;
    }

    /**
     * @see IN#getLogSize
     */
    @Override
    public int getLogSize() {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see IN#writeToLog
     */
    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see IN#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        boolean unpacked = (entryVersion < 6);
        super.readFromLog(itemBuffer, entryVersion);
        dupKey = LogUtils.readByteArray(itemBuffer, unpacked);

        /* DupCountLN */
        boolean dupCountLNRefExists = false;
        byte booleans = itemBuffer.get();
        dupCountLNRefExists = (booleans & 1) != 0;
        if (dupCountLNRefExists) {
            dupCountLNRef.readFromLog(itemBuffer, entryVersion);
        } else {
            dupCountLNRef = null;
        }
    }

    /**
     * DINS need to dump their dup key
     */
    @Override
    protected void dumpLogAdditional(StringBuilder sb) {
        super.dumpLogAdditional(sb);
        sb.append(Key.dumpString(dupKey, 0));
        if (dupCountLNRef != null) {
            dupCountLNRef.dumpLog(sb, true);
        }
    }

    /*
     * Dumping
     */

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
     * @return a string that dumps information about this DIN, without
     */
    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }

        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<dupkey>");
        sb.append(dupKey == null ? "" :
                  Key.dumpString(dupKey, 0));
        sb.append("</dupkey>");
        sb.append('\n');
        if (dupCountLNRef == null) {
            sb.append(TreeUtils.indent(nSpaces+2));
            sb.append("<dupCountLN/>");
        } else {
            sb.append(dupCountLNRef.dumpString(nSpaces + 4, true));
        }
        sb.append('\n');
        sb.append(super.dumpString(nSpaces, false));

        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    @Override
    public String shortClassName() {
        return "DIN";
    }
}
