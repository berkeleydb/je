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
 * Per-stat Metadata for JE EnvironmentImpl and MemoryBudget statistics.
 */
public class DbiStatDefinition {

    public static final String MB_GROUP_NAME = "Cache Layout";
    public static final String MB_GROUP_DESC =
        "Allocation of resources in the cache.";

    public static final String ENV_GROUP_NAME = "Environment";
    public static final String ENV_GROUP_DESC =
        "Miscellaneous environment wide statistics.";

    public static final String THROUGHPUT_GROUP_NAME = "Op";
    public static final String THROUGHPUT_GROUP_DESC =
        "Throughput statistics for JE calls.";

    /* The following stat definitions are used in MemoryBudget. */
    public static final String MB_SHARED_CACHE_TOTAL_BYTES_NAME =
        "sharedCacheTotalBytes";
    public static final String MB_SHARED_CACHE_TOTAL_BYTES_DESC =
        "Total amount of the shared JE main cache in use, in bytes.";
    public static final StatDefinition MB_SHARED_CACHE_TOTAL_BYTES =
        new StatDefinition(
            MB_SHARED_CACHE_TOTAL_BYTES_NAME,
            MB_SHARED_CACHE_TOTAL_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_TOTAL_BYTES_NAME =
        "cacheTotalBytes";
    public static final String MB_TOTAL_BYTES_DESC =
        "Total amount of JE main cache in use, in bytes.";
    public static final StatDefinition MB_TOTAL_BYTES =
        new StatDefinition(
            MB_TOTAL_BYTES_NAME,
            MB_TOTAL_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_DATA_BYTES_NAME =
        "dataBytes";
    public static final String MB_DATA_BYTES_DESC =
        "Amount of JE main cache used for holding data, keys and internal " +
            "Btree nodes, in bytes.";
    public static final StatDefinition MB_DATA_BYTES =
        new StatDefinition(
            MB_DATA_BYTES_NAME,
            MB_DATA_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_DATA_ADMIN_BYTES_NAME =
        "dataAdminBytes";
    public static final String MB_DATA_ADMIN_BYTES_DESC =
        "Amount of JE main cache used for holding per-database cleaner " +
            "utilization metadata, in bytes.";
    public static final StatDefinition MB_DATA_ADMIN_BYTES =
        new StatDefinition(
            MB_DATA_ADMIN_BYTES_NAME,
            MB_DATA_ADMIN_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_DOS_BYTES_NAME =
        "DOSBytes";
    public static final String MB_DOS_BYTES_DESC =
        "Amount of JE main cache consumed by disk-ordered cursor and " +
            "Database.count operations, in bytes.";
    public static final StatDefinition MB_DOS_BYTES =
        new StatDefinition(
            MB_DOS_BYTES_NAME,
            MB_DOS_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_ADMIN_BYTES_NAME =
        "adminBytes";
    public static final String MB_ADMIN_BYTES_DESC =
        "Number of bytes of JE main cache used for cleaner and checkpointer " +
            "metadata, in bytes.";
    public static final StatDefinition MB_ADMIN_BYTES =
        new StatDefinition(
            MB_ADMIN_BYTES_NAME,
            MB_ADMIN_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String MB_LOCK_BYTES_NAME =
        "lockBytes";
    public static final String MB_LOCK_BYTES_DESC =
        "Number of bytes of JE cache used for holding locks and transactions," +
            " in bytes.";
    public static final StatDefinition MB_LOCK_BYTES =
        new StatDefinition(
            MB_LOCK_BYTES_NAME,
            MB_LOCK_BYTES_DESC,
            StatType.CUMULATIVE);

    /* The following stat definitions are used in EnvironmentImpl. */
    public static final String ENV_RELATCHES_REQUIRED_NAME =
        "btreeRelatchesRequired";
    public static final String ENV_RELATCHES_REQUIRED_DESC =
        "Returns the number of btree latch upgrades required while operating " +
            "on this Environment. A measurement of contention.";
    public static final StatDefinition ENV_RELATCHES_REQUIRED =
        new StatDefinition(
            ENV_RELATCHES_REQUIRED_NAME,
            ENV_RELATCHES_REQUIRED_DESC);

    public static final String ENV_CREATION_TIME_NAME =
        "environmentCreationTime";
    public static final String ENV_CREATION_TIME_DESC =
        "Returns the time the Environment was created. ";
    public static final StatDefinition ENV_CREATION_TIME =
        new StatDefinition(
            ENV_CREATION_TIME_NAME,
            ENV_CREATION_TIME_DESC,
            StatType.CUMULATIVE);

    public static final String ENV_BIN_DELTA_GETS_NAME =
        "nBinDeltaGet";
    public static final String ENV_BIN_DELTA_GETS_DESC =
        "The number of gets performed in BIN deltas";
    public static final StatDefinition ENV_BIN_DELTA_GETS =
        new StatDefinition(
            ENV_BIN_DELTA_GETS_NAME,
            ENV_BIN_DELTA_GETS_DESC);

    public static final String ENV_BIN_DELTA_INSERTS_NAME =
        "nBinDeltaInsert";
    public static final String ENV_BIN_DELTA_INSERTS_DESC =
        "The number of insertions performed in BIN deltas";
    public static final StatDefinition ENV_BIN_DELTA_INSERTS =
        new StatDefinition(
            ENV_BIN_DELTA_INSERTS_NAME,
            ENV_BIN_DELTA_INSERTS_DESC);

    public static final String ENV_BIN_DELTA_UPDATES_NAME =
        "nBinDeltaUpdate";
    public static final String ENV_BIN_DELTA_UPDATES_DESC =
        "The number of updates performed in BIN deltas";
    public static final StatDefinition ENV_BIN_DELTA_UPDATES =
        new StatDefinition(
            ENV_BIN_DELTA_UPDATES_NAME,
            ENV_BIN_DELTA_UPDATES_DESC);

    public static final String ENV_BIN_DELTA_DELETES_NAME =
        "nBinDeltaDelete";
    public static final String ENV_BIN_DELTA_DELETES_DESC =
        "The number of deletions performed in BIN deltas";
    public static final StatDefinition ENV_BIN_DELTA_DELETES =
        new StatDefinition(
            ENV_BIN_DELTA_DELETES_NAME,
            ENV_BIN_DELTA_DELETES_DESC);

    /* The following stat definitions are used for throughput. */

    public static final String THROUGHPUT_PRI_SEARCH_NAME =
        "priSearch";
    public static final String THROUGHPUT_PRI_SEARCH_DESC =
        "Number of successful primary DB key search operations.";
    public static final StatDefinition THROUGHPUT_PRI_SEARCH =
        new StatDefinition(
            THROUGHPUT_PRI_SEARCH_NAME,
            THROUGHPUT_PRI_SEARCH_DESC);

    public static final String THROUGHPUT_PRI_SEARCH_FAIL_NAME =
        "priSearchFail";
    public static final String THROUGHPUT_PRI_SEARCH_FAIL_DESC =
        "Number of failed primary DB key search operations.";
    public static final StatDefinition THROUGHPUT_PRI_SEARCH_FAIL =
        new StatDefinition(
            THROUGHPUT_PRI_SEARCH_FAIL_NAME,
            THROUGHPUT_PRI_SEARCH_FAIL_DESC);

    public static final String THROUGHPUT_SEC_SEARCH_NAME =
        "secSearch";
    public static final String THROUGHPUT_SEC_SEARCH_DESC =
        "Number of successful secondary DB key search operations.";
    public static final StatDefinition THROUGHPUT_SEC_SEARCH =
        new StatDefinition(
            THROUGHPUT_SEC_SEARCH_NAME,
            THROUGHPUT_SEC_SEARCH_DESC);

    public static final String THROUGHPUT_SEC_SEARCH_FAIL_NAME =
        "secSearchFail";
    public static final String THROUGHPUT_SEC_SEARCH_FAIL_DESC =
        "Number of failed secondary DB key search operations.";
    public static final StatDefinition THROUGHPUT_SEC_SEARCH_FAIL =
        new StatDefinition(
            THROUGHPUT_SEC_SEARCH_FAIL_NAME,
            THROUGHPUT_SEC_SEARCH_FAIL_DESC);

    public static final String THROUGHPUT_PRI_POSITION_NAME =
        "priPosition";
    public static final String THROUGHPUT_PRI_POSITION_DESC =
        "Number of successful primary DB position operations.";
    public static final StatDefinition THROUGHPUT_PRI_POSITION =
        new StatDefinition(
            THROUGHPUT_PRI_POSITION_NAME,
            THROUGHPUT_PRI_POSITION_DESC);

    public static final String THROUGHPUT_SEC_POSITION_NAME =
        "secPosition";
    public static final String THROUGHPUT_SEC_POSITION_DESC =
        "Number of successful secondary DB position operations.";
    public static final StatDefinition THROUGHPUT_SEC_POSITION =
        new StatDefinition(
            THROUGHPUT_SEC_POSITION_NAME,
            THROUGHPUT_SEC_POSITION_DESC);

    public static final String THROUGHPUT_PRI_INSERT_NAME =
        "priInsert";
    public static final String THROUGHPUT_PRI_INSERT_DESC =
        "Number of successful primary DB insertion operations.";
    public static final StatDefinition THROUGHPUT_PRI_INSERT =
        new StatDefinition(
            THROUGHPUT_PRI_INSERT_NAME,
            THROUGHPUT_PRI_INSERT_DESC);

    public static final String THROUGHPUT_PRI_INSERT_FAIL_NAME =
        "priInsertFail";
    public static final String THROUGHPUT_PRI_INSERT_FAIL_DESC =
        "Number of failed primary DB insertion operations.";
    public static final StatDefinition THROUGHPUT_PRI_INSERT_FAIL =
        new StatDefinition(
            THROUGHPUT_PRI_INSERT_FAIL_NAME,
            THROUGHPUT_PRI_INSERT_FAIL_DESC);

    public static final String THROUGHPUT_SEC_INSERT_NAME =
        "secInsert";
    public static final String THROUGHPUT_SEC_INSERT_DESC =
        "Number of successful secondary DB insertion operations.";
    public static final StatDefinition THROUGHPUT_SEC_INSERT =
        new StatDefinition(
            THROUGHPUT_SEC_INSERT_NAME,
            THROUGHPUT_SEC_INSERT_DESC);

    public static final String THROUGHPUT_PRI_UPDATE_NAME =
        "priUpdate";
    public static final String THROUGHPUT_PRI_UPDATE_DESC =
        "Number of successful primary DB update operations.";
    public static final StatDefinition THROUGHPUT_PRI_UPDATE =
        new StatDefinition(
            THROUGHPUT_PRI_UPDATE_NAME,
            THROUGHPUT_PRI_UPDATE_DESC);

    public static final String THROUGHPUT_SEC_UPDATE_NAME =
        "secUpdate";
    public static final String THROUGHPUT_SEC_UPDATE_DESC =
        "Number of successful secondary DB update operations.";
    public static final StatDefinition THROUGHPUT_SEC_UPDATE =
        new StatDefinition(
            THROUGHPUT_SEC_UPDATE_NAME,
            THROUGHPUT_SEC_UPDATE_DESC);

    public static final String THROUGHPUT_PRI_DELETE_NAME =
        "priDelete";
    public static final String THROUGHPUT_PRI_DELETE_DESC =
        "Number of successful primary DB deletion operations.";
    public static final StatDefinition THROUGHPUT_PRI_DELETE =
        new StatDefinition(
            THROUGHPUT_PRI_DELETE_NAME,
            THROUGHPUT_PRI_DELETE_DESC);

    public static final String THROUGHPUT_PRI_DELETE_FAIL_NAME =
        "priDeleteFail";
    public static final String THROUGHPUT_PRI_DELETE_FAIL_DESC =
        "Number of failed primary DB deletion operations.";
    public static final StatDefinition THROUGHPUT_PRI_DELETE_FAIL =
        new StatDefinition(
            THROUGHPUT_PRI_DELETE_FAIL_NAME,
            THROUGHPUT_PRI_DELETE_FAIL_DESC);

    public static final String THROUGHPUT_SEC_DELETE_NAME =
        "secDelete";
    public static final String THROUGHPUT_SEC_DELETE_DESC =
        "Number of successful secondary DB deletion operations.";
    public static final StatDefinition THROUGHPUT_SEC_DELETE =
        new StatDefinition(
            THROUGHPUT_SEC_DELETE_NAME,
            THROUGHPUT_SEC_DELETE_DESC);
}
