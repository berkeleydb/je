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

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE Btree statistics.
 */
public class BTreeStatDefinition {

    public static final String GROUP_NAME = "BTree";
    public static final String GROUP_DESC =
        "Composition of btree, types and counts of nodes.";

    public static final StatDefinition BTREE_BIN_COUNT =
        new StatDefinition("binCount",
                           "Number of bottom internal nodes in " +
                           "the database's btree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_DELETED_LN_COUNT =
        new StatDefinition("deletedLNCount",
                           "Number of deleted leaf nodes in the database's " +
                           "btree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_IN_COUNT =
        new StatDefinition("inCount",
                           "Number of internal nodes in database's btree. " +
                           "BINs are not included.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_LN_COUNT =
        new StatDefinition("lnCount",
                           "Number of leaf nodes in the database's btree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_MAINTREE_MAXDEPTH =
        new StatDefinition("mainTreeMaxDepth",
                           "Maximum depth of the in-memory tree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_INS_BYLEVEL =
        new StatDefinition("insByLevel",
                           "Histogram of internal nodes by level.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_BINS_BYLEVEL =
        new StatDefinition("binsByLevel",
                           "Histogram of bottom internal nodes by level.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BTREE_RELATCHES_REQUIRED =
        new StatDefinition("relatchesRequired",
                           "Number of latch upgrades (relatches) required.");

    public static final StatDefinition BTREE_ROOT_SPLITS =
        new StatDefinition("nRootSplits",
                           "Number of times the root was split.");

    public static final StatDefinition BTREE_BIN_ENTRIES_HISTOGRAM =
        new StatDefinition("binEntriesHistogram",
                           "Histogram of bottom internal nodes fill " +
                           "percentage.",
                           StatType.CUMULATIVE);
}
