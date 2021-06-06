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

package com.sleepycat.je;

import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BINS_BYLEVEL;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BIN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_DELETED_LN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.GROUP_NAME;
import static com.sleepycat.je.dbi.BTreeStatDefinition.GROUP_DESC;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_INS_BYLEVEL;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_IN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_LN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_MAINTREE_MAXDEPTH;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_RELATCHES_REQUIRED;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_ROOT_SPLITS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BIN_ENTRIES_HISTOGRAM;

import com.sleepycat.je.utilint.StatGroup;

/**
 * The BtreeStats object is used to return Btree database statistics.
 */
public class BtreeStats extends DatabaseStats {

    private static final long serialVersionUID = 298825033L;

    private StatGroup dbImplStats;
    private StatGroup treeStats;

    public BtreeStats() {
        dbImplStats = new StatGroup(GROUP_NAME, GROUP_DESC);
        treeStats = new StatGroup(GROUP_NAME, GROUP_DESC);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setDbImplStats(StatGroup stats) {
        dbImplStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setTreeStats(StatGroup tStats) {
        this.treeStats = tStats;
    }

    /**
     * Returns the number of Bottom Internal Nodes in the database tree.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return number of Bottom Internal Nodes in the database tree.
     */
    public long getBottomInternalNodeCount() {
        return dbImplStats.getLong(BTREE_BIN_COUNT);
    }

    /**
     * @deprecated as of 5.0, returns zero.
     */
    public long getDuplicateBottomInternalNodeCount() {
        return 0;
    }

    /**
     * Returns the number of deleted data records in the database tree that
     * are pending removal by the compressor.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return number of deleted data records in the database tree that are
     * pending removal by the compressor.
     */
    public long getDeletedLeafNodeCount() {
        return dbImplStats.getLong(BTREE_DELETED_LN_COUNT);
    }

    /**
     * @deprecated as of 5.0, returns zero.
     */
    public long getDupCountLeafNodeCount() {
        return 0;
    }

    /**
     * Returns the number of Internal Nodes in the database tree.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return number of Internal Nodes in the database tree.
     */
    public long getInternalNodeCount() {
        return dbImplStats.getLong(BTREE_IN_COUNT);
    }

    /**
     * @deprecated as of 5.0, returns zero.
     */
    public long getDuplicateInternalNodeCount() {
        return 0;
    }

    /**
     * Returns the number of leaf nodes in the database tree, which can equal
     * the number of records. This is calculated without locks or transactions,
     * and therefore is only an accurate count of the current number of records
     * when the database is quiescent.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return number of leaf nodes in the database tree, which can equal the
     * number of records. This is calculated without locks or transactions, and
     * therefore is only an accurate count of the current number of records
     * when the database is quiescent.
     */
    public long getLeafNodeCount() {
        return dbImplStats.getLong(BTREE_LN_COUNT);
    }

    /**
     * Returns the maximum depth of the main database tree.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return maximum depth of the main database tree.
     */
    public int getMainTreeMaxDepth() {
        return dbImplStats.getInt(BTREE_MAINTREE_MAXDEPTH);
    }

    /**
     * @deprecated as of 5.0, returns zero.
     */
    public int getDuplicateTreeMaxDepth() {
        return 0;
    }

    /**
     * Returns the count of Internal Nodes per level, indexed by level.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return count of Internal Nodes per level, indexed by level.
     */
    public long[] getINsByLevel() {
        return dbImplStats.getLongArray(BTREE_INS_BYLEVEL);
    }

    /**
     * Returns the count of Bottom Internal Nodes per level, indexed by level.
     *
     * <p>The information is included only if the {@link
     * com.sleepycat.je.Database#getStats Database.getStats} call was not
     * configured by the {@link com.sleepycat.je.StatsConfig#setFast
     * StatsConfig.setFast} method.</p>
     *
     * @return count of Bottom Internal Nodes per level, indexed by level.
     */
    public long[] getBINsByLevel() {
        return dbImplStats.getLongArray(BTREE_BINS_BYLEVEL);
    }

    /**
     * Returns an array representing a histogram of the number of Bottom
     * Internal Nodes with various percentages of non-deleted entry counts.
     * The array is 10 elements and each element represents a range of 10%.
     *
     * <pre>
     * element [0]: # BINs with 0% to 9% entries used by non-deleted values
     * element [1]: # BINs with 10% to 19% entries used by non-deleted values
     * element [2]: # BINs with 20% to 29% entries used by non-deleted values
     * ...
     * element [0]: # BINs with 90% to 100% entries used by non-deleted values
     * </pre>
     *
     * @return an array representing a histogram of the number of BINs with
     * various percentages of non-deleted entries.
     */
    public long[] getBINEntriesHistogram() {
        return dbImplStats.getLongArray(BTREE_BIN_ENTRIES_HISTOGRAM);
    }

    /**
     * @deprecated as of 5.0, returns an empty array.
     */
    public long[] getDINsByLevel() {
        return new long[0];
    }

    /**
     * @deprecated as of 5.0, returns an empty array.
     */
    public long[] getDBINsByLevel() {
        return new long[0];
    }

    /**
     * Returns the number of latch upgrades (relatches) required while
     * operating on this database's BTree.  Latch upgrades are required when an
     * operation assumes that a shared (read) latch will be sufficient but
     * later determines that an exclusive (write) latch will actually be
     * required.
     *
     * @return number of latch upgrades (relatches) required.
     */
    public long getRelatches() {
        return treeStats.getLong(BTREE_RELATCHES_REQUIRED);
    }

    /**
     * The number of times the root of the BTree was split.
     *
     * @return number of times the root was split.
     */
    public int getRootSplits() {
        return treeStats.getInt(BTREE_ROOT_SPLITS);
    }

    /**
     * For convenience, the BtreeStats class has a toString method that lists
     * all the data fields.
     */
    @Override
    public String toString() {
        return dbImplStats.toString() + "\n" + treeStats.toString();
    }

    public String toStringVerbose() {
        return
            dbImplStats.toStringVerbose() + "\n" + treeStats.toStringVerbose();
    }
}
