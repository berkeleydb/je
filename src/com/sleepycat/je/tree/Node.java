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

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Loggable;

/**
 * A Node contains all the common base information for any JE B-Tree node.
 */
public abstract class Node implements Loggable {

    /* Used to mean null or none.  See NodeSequence. */
    public static final long NULL_NODE_ID = -1L;

    protected Node() {
    }

    /**
     * Initialize a node that has been faulted in from the log.
     */
    public void postFetchInit(DatabaseImpl db, long sourceLsn)
        throws DatabaseException {

        /* Nothing to do. */
    }

    public void latch() {
    }

    public void latchShared()
        throws DatabaseException {
    }

    public void latchShared(CacheMode ignore)
        throws DatabaseException {
    }

    public void releaseLatch() {
    }

    /**
     * Since DIN/DBIN/DupCountLN are no longer used in the Btree, this method
     * should normally only be used by dup conversion or entities that do not
     * access records via the Btree.
     *
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    public boolean containsDuplicates() {
        return false;
    }

    /**
     * Cover for LN's and just return 0 since they'll always be at the bottom
     * of the tree.
     */
    public int getLevel() {
        return 0;
    }

    /**
     * Add yourself to the in memory list if you're a type of node that
     * should belong.
     */
    abstract void rebuildINList(INList inList)
        throws DatabaseException;

    /**
     * @return true if you're part of a deletable subtree.
     */
    abstract boolean isValidForDelete()
        throws DatabaseException;

    public boolean isLN() {
        return false;
    }

    public boolean isIN() {
        return false;
    }
    
    public boolean isUpperIN() {
        return false;
    }
    

    public boolean isBIN() {
        return false;
    }

    public boolean isBINDelta() {
        return false;
    }

    public boolean isBINDelta(boolean checkLatched) {
        return false;
    }

    public boolean isDIN() {
        return false;
    }

    public boolean isDBIN() {
        return false;
    }

    /**
     * Return the approximate size of this node in memory, if this size should
     * be included in its parents memory accounting.  For example, all INs
     * return 0, because they are accounted for individually. LNs must return a
     * count, they're not counted on the INList.
     */
    public long getMemorySizeIncludedByParent() {
        return 0;
    }

    /**
     * Default toString method at the root of the tree.
     */
    @Override
    public String toString() {
        return this.dumpString(0, true);
    }

    public void dump(int nSpaces) {
        System.out.print(dumpString(nSpaces, true));
    }

    String dumpString(int nSpaces, boolean dumpTags) {
        return "";
    }

    public String getType() {
        return getClass().getName();
    }

    /**
     * We categorize fetch stats by the type of node, so node subclasses
     * update different stats.
     */
    abstract void incFetchStats(EnvironmentImpl envImpl, boolean isMiss);

    /**
     * Returns the generic LogEntryType for this node. Returning the actual
     * type used to log the node is not always possible. Specifically, for LN 
     * nodes the generic type is less specific than the actual type used to log
     * the node:
     *  + A non-transactional type is always returned.
     *  + LOG_INS_LN is returned rather than LOG_UPD_LN.
     *  + LOG_DEL_LN is returned rather than LOG_DEL_DUPLN.
     */
    public abstract LogEntryType getGenericLogType();

    public long getTransactionId() {
        return 0;
    }
}
