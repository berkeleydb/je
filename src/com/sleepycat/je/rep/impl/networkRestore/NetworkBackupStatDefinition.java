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

package com.sleepycat.je.rep.impl.networkRestore;

import static com.sleepycat.je.utilint.StatDefinition.StatType.CUMULATIVE;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for each NetworkBackup statistics.
 */
public class NetworkBackupStatDefinition {

    public static final String GROUP_NAME = "NetworkBackup";
    public static final String GROUP_DESC = "NetworkBackup statistics";

    public static StatDefinition BACKUP_FILE_COUNT =
        new StatDefinition
        ("backupFileCount",
         "The total number of files.");

    public static StatDefinition SKIP_COUNT =
        new StatDefinition
        ("skipCount",
         "The number of files that were skipped because they were already " +
         "present and current in the local environment directory.");

    public static StatDefinition FETCH_COUNT =
        new StatDefinition
        ("fetchCount",
         "The number of files that were actually transferred from the " +
         "server");

    public static StatDefinition DISPOSED_COUNT =
        new StatDefinition
        ("disposedCount",
         "The number of files that were disposed (deleted or renamed) from " +
         "the local environment directory.");

    public static StatDefinition EXPECTED_BYTES =
        new StatDefinition(
            "expectedBytes",
            "The number of bytes that are expected to be transferred.",
            CUMULATIVE);

    public static StatDefinition TRANSFERRED_BYTES =
        new StatDefinition(
            "transferredBytes",
            "The number of bytes that have been transferred so far.",
            CUMULATIVE);

    public static StatDefinition TRANSFER_RATE =
        new StatDefinition(
            "transferRate",
            "The moving average of the rate, in bytes per second, at which" +
            " bytes have been transferred so far.");
}
