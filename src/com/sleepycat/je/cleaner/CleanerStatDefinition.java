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

package com.sleepycat.je.cleaner;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE cleaner statistics.
 */
public class CleanerStatDefinition {

    public static final String GROUP_NAME = "Cleaning";
    public static final String GROUP_DESC =
        "Log cleaning involves garbage collection of data " +
            "files in the append-only storage system.";

    public static final String CLEANER_RUNS_NAME =
        "nCleanerRuns";
    public static final String CLEANER_RUNS_DESC =
        "Number of cleaner runs, including two-pass runs.";
    public static final StatDefinition CLEANER_RUNS =
        new StatDefinition(
            CLEANER_RUNS_NAME,
            CLEANER_RUNS_DESC);

    public static final String CLEANER_TWO_PASS_RUNS_NAME =
        "nTwoPassRuns";
    public static final String CLEANER_TWO_PASS_RUNS_DESC =
        "Number of cleaner two-pass runs.";
    public static final StatDefinition CLEANER_TWO_PASS_RUNS =
        new StatDefinition(
            CLEANER_TWO_PASS_RUNS_NAME,
            CLEANER_TWO_PASS_RUNS_DESC);

    public static final String CLEANER_REVISAL_RUNS_NAME =
        "nRevisalRuns";
    public static final String CLEANER_REVISAL_RUNS_DESC =
        "Number of cleaner runs that ended in revising expiration info, but " +
            "not in any cleaning.";
    public static final StatDefinition CLEANER_REVISAL_RUNS =
        new StatDefinition(
            CLEANER_REVISAL_RUNS_NAME,
            CLEANER_REVISAL_RUNS_DESC);

    public static final String CLEANER_DELETIONS_NAME =
        "nCleanerDeletions";
    public static final String CLEANER_DELETIONS_DESC =
        "Number of cleaner file deletions.";
    public static final StatDefinition CLEANER_DELETIONS =
        new StatDefinition(
            CLEANER_DELETIONS_NAME,
            CLEANER_DELETIONS_DESC);

    public static final String CLEANER_PENDING_LN_QUEUE_SIZE_NAME =
        "pendingLNQueueSize";
    public static final String CLEANER_PENDING_LN_QUEUE_SIZE_DESC =
        "Number of LNs pending because they were locked and could not be " +
            "migrated.";
    public static final StatDefinition CLEANER_PENDING_LN_QUEUE_SIZE =
        new StatDefinition(
            CLEANER_PENDING_LN_QUEUE_SIZE_NAME,
            CLEANER_PENDING_LN_QUEUE_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_INS_OBSOLETE_NAME =
        "nINsObsolete";
    public static final String CLEANER_INS_OBSOLETE_DESC =
        "Accumulated number of INs obsolete.";
    public static final StatDefinition CLEANER_INS_OBSOLETE =
        new StatDefinition(
            CLEANER_INS_OBSOLETE_NAME,
            CLEANER_INS_OBSOLETE_DESC);

    public static final String CLEANER_INS_CLEANED_NAME =
        "nINsCleaned";
    public static final String CLEANER_INS_CLEANED_DESC =
        "Accumulated number of INs cleaned.";
    public static final StatDefinition CLEANER_INS_CLEANED =
        new StatDefinition(
            CLEANER_INS_CLEANED_NAME,
            CLEANER_INS_CLEANED_DESC);

    public static final String CLEANER_INS_DEAD_NAME =
        "nINsDead";
    public static final String CLEANER_INS_DEAD_DESC =
        "Accumulated number of INs that were not found in the tree anymore " +
            "(deleted).";
    public static final StatDefinition CLEANER_INS_DEAD =
        new StatDefinition(
            CLEANER_INS_DEAD_NAME,
            CLEANER_INS_DEAD_DESC);

    public static final String CLEANER_INS_MIGRATED_NAME =
        "nINsMigrated";
    public static final String CLEANER_INS_MIGRATED_DESC =
        "Accumulated number of INs migrated.";
    public static final StatDefinition CLEANER_INS_MIGRATED =
        new StatDefinition(
            CLEANER_INS_MIGRATED_NAME,
            CLEANER_INS_MIGRATED_DESC);

    public static final String CLEANER_BIN_DELTAS_OBSOLETE_NAME =
        "nBINDeltasObsolete";
    public static final String CLEANER_BIN_DELTAS_OBSOLETE_DESC =
        "Accumulated number of BIN-deltas obsolete.";
    public static final StatDefinition CLEANER_BIN_DELTAS_OBSOLETE =
        new StatDefinition(
            CLEANER_BIN_DELTAS_OBSOLETE_NAME,
            CLEANER_BIN_DELTAS_OBSOLETE_DESC);

    public static final String CLEANER_BIN_DELTAS_CLEANED_NAME =
        "nBINDeltasCleaned";
    public static final String CLEANER_BIN_DELTAS_CLEANED_DESC =
        "Accumulated number of BIN-deltas cleaned.";
    public static final StatDefinition CLEANER_BIN_DELTAS_CLEANED =
        new StatDefinition(
            CLEANER_BIN_DELTAS_CLEANED_NAME,
            CLEANER_BIN_DELTAS_CLEANED_DESC);

    public static final String CLEANER_BIN_DELTAS_DEAD_NAME =
        "nBINDeltasDead";
    public static final String CLEANER_BIN_DELTAS_DEAD_DESC =
        "Accumulated number of BIN-deltas that were not found in the tree " +
            "anymore (deleted).";
    public static final StatDefinition CLEANER_BIN_DELTAS_DEAD =
        new StatDefinition(
            CLEANER_BIN_DELTAS_DEAD_NAME,
            CLEANER_BIN_DELTAS_DEAD_DESC);

    public static final String CLEANER_BIN_DELTAS_MIGRATED_NAME =
        "nBINDeltasMigrated";
    public static final String CLEANER_BIN_DELTAS_MIGRATED_DESC =
        "Accumulated number of BIN-deltas migrated.";
    public static final StatDefinition CLEANER_BIN_DELTAS_MIGRATED =
        new StatDefinition(
            CLEANER_BIN_DELTAS_MIGRATED_NAME,
            CLEANER_BIN_DELTAS_MIGRATED_DESC);

    public static final String CLEANER_LNS_OBSOLETE_NAME =
        "nLNsObsolete";
    public static final String CLEANER_LNS_OBSOLETE_DESC =
        "Accumulated number of LNs obsolete.";
    public static final StatDefinition CLEANER_LNS_OBSOLETE =
        new StatDefinition(
            CLEANER_LNS_OBSOLETE_NAME,
            CLEANER_LNS_OBSOLETE_DESC);

    public static final String CLEANER_LNS_EXPIRED_NAME =
        "nLNsExpired";
    public static final String CLEANER_LNS_EXPIRED_DESC =
        "Accumulated number of obsolete LNs that were expired.";
    public static final StatDefinition CLEANER_LNS_EXPIRED =
        new StatDefinition(
            CLEANER_LNS_EXPIRED_NAME,
            CLEANER_LNS_EXPIRED_DESC);

    public static final String CLEANER_LNS_CLEANED_NAME =
        "nLNsCleaned";
    public static final String CLEANER_LNS_CLEANED_DESC =
        "Accumulated number of LNs cleaned.";
    public static final StatDefinition CLEANER_LNS_CLEANED =
        new StatDefinition(
            CLEANER_LNS_CLEANED_NAME,
            CLEANER_LNS_CLEANED_DESC);

    public static final String CLEANER_LNS_DEAD_NAME =
        "nLNsDead";
    public static final String CLEANER_LNS_DEAD_DESC =
        "Accumulated number of LNs that were not found in the tree anymore " +
            "(deleted).";
    public static final StatDefinition CLEANER_LNS_DEAD =
        new StatDefinition(
            CLEANER_LNS_DEAD_NAME,
            CLEANER_LNS_DEAD_DESC);

    public static final String CLEANER_LNS_LOCKED_NAME =
        "nLNsLocked";
    public static final String CLEANER_LNS_LOCKED_DESC =
        "Accumulated number of LNs encountered that were locked.";
    public static final StatDefinition CLEANER_LNS_LOCKED =
        new StatDefinition(
            CLEANER_LNS_LOCKED_NAME,
            CLEANER_LNS_LOCKED_DESC);

    public static final String CLEANER_LNS_MIGRATED_NAME =
        "nLNsMigrated";
    public static final String CLEANER_LNS_MIGRATED_DESC =
        "Accumulated number of LNs that were migrated forward in the log by " +
            "the cleaner.";
    public static final StatDefinition CLEANER_LNS_MIGRATED =
        new StatDefinition(
            CLEANER_LNS_MIGRATED_NAME,
            CLEANER_LNS_MIGRATED_DESC);

    public static final String CLEANER_LNS_MARKED_NAME =
        "nLNsMarked";
    public static final String CLEANER_LNS_MARKED_DESC =
        "Accumulated number of LNs in temporary DBs that  were dirtied by the" +
            " cleaner and subsequently  logging during checkpoint/eviction.";
    public static final StatDefinition CLEANER_LNS_MARKED =
        new StatDefinition(
            CLEANER_LNS_MARKED_NAME,
            CLEANER_LNS_MARKED_DESC);

    public static final String CLEANER_LNQUEUE_HITS_NAME =
        "nLNQueueHits";
    public static final String CLEANER_LNQUEUE_HITS_DESC =
        "Accumulated number of LNs processed without a tree lookup.";
    public static final StatDefinition CLEANER_LNQUEUE_HITS =
        new StatDefinition(
            CLEANER_LNQUEUE_HITS_NAME,
            CLEANER_LNQUEUE_HITS_DESC);

    public static final String CLEANER_PENDING_LNS_PROCESSED_NAME =
        "nPendingLNsProcessed";
    public static final String CLEANER_PENDING_LNS_PROCESSED_DESC =
        "Accumulated number of LNs processed because they were previously " +
            "locked.";
    public static final StatDefinition CLEANER_PENDING_LNS_PROCESSED =
        new StatDefinition(
            CLEANER_PENDING_LNS_PROCESSED_NAME,
            CLEANER_PENDING_LNS_PROCESSED_DESC);

    public static final String CLEANER_MARKED_LNS_PROCESSED_NAME =
        "nMarkLNsProcessed";
    public static final String CLEANER_MARKED_LNS_PROCESSED_DESC =
        "Accumulated number of LNs processed because they were previously " +
            "marked for migration.";
    public static final StatDefinition CLEANER_MARKED_LNS_PROCESSED =
        new StatDefinition(
            CLEANER_MARKED_LNS_PROCESSED_NAME,
            CLEANER_MARKED_LNS_PROCESSED_DESC);

    public static final String CLEANER_TO_BE_CLEANED_LNS_PROCESSED_NAME =
        "nToBeCleanedLNsProcessed";
    public static final String CLEANER_TO_BE_CLEANED_LNS_PROCESSED_DESC =
        "Accumulated number of LNs processed because they are soon to be " +
            "cleaned.";
    public static final StatDefinition CLEANER_TO_BE_CLEANED_LNS_PROCESSED =
        new StatDefinition(
            CLEANER_TO_BE_CLEANED_LNS_PROCESSED_NAME,
            CLEANER_TO_BE_CLEANED_LNS_PROCESSED_DESC);

    public static final String CLEANER_CLUSTER_LNS_PROCESSED_NAME =
        "nClusterLNsProcessed";
    public static final String CLEANER_CLUSTER_LNS_PROCESSED_DESC =
        "Accumulated number of LNs processed because they qualify for " +
            "clustering.";
    public static final StatDefinition CLEANER_CLUSTER_LNS_PROCESSED =
        new StatDefinition(
            CLEANER_CLUSTER_LNS_PROCESSED_NAME,
            CLEANER_CLUSTER_LNS_PROCESSED_DESC);

    public static final String CLEANER_PENDING_LNS_LOCKED_NAME =
        "nPendingLNsLocked";
    public static final String CLEANER_PENDING_LNS_LOCKED_DESC =
        "Accumulated number of pending LNs that could not be locked for " +
            "migration because of a long duration application lock.";
    public static final StatDefinition CLEANER_PENDING_LNS_LOCKED =
        new StatDefinition(
            CLEANER_PENDING_LNS_LOCKED_NAME,
            CLEANER_PENDING_LNS_LOCKED_DESC);

    public static final String CLEANER_ENTRIES_READ_NAME =
        "nCleanerEntriesRead";
    public static final String CLEANER_ENTRIES_READ_DESC =
        "Accumulated number of log entries read by the cleaner.";
    public static final StatDefinition CLEANER_ENTRIES_READ =
        new StatDefinition(
            CLEANER_ENTRIES_READ_NAME,
            CLEANER_ENTRIES_READ_DESC);

    public static final String CLEANER_DISK_READS_NAME =
        "nCleanerDisksReads";
    public static final String CLEANER_DISK_READS_DESC =
        "Number of disk reads by the cleaner.";
    public static final StatDefinition CLEANER_DISK_READS =
        new StatDefinition(
            CLEANER_DISK_READS_NAME,
            CLEANER_DISK_READS_DESC);

    public static final String CLEANER_REPEAT_ITERATOR_READS_NAME =
        "nRepeatIteratorReads";
    public static final String CLEANER_REPEAT_ITERATOR_READS_DESC =
        "Number of attempts to read a log entry larger than the read buffer " +
            "size during which the log buffer couldn't be grown enough to " +
            "accommodate the object.";
    public static final StatDefinition CLEANER_REPEAT_ITERATOR_READS =
        new StatDefinition(
            CLEANER_REPEAT_ITERATOR_READS_NAME,
            CLEANER_REPEAT_ITERATOR_READS_DESC);

    public static final String CLEANER_ACTIVE_LOG_SIZE_NAME =
        "activeLogSize";
    public static final String CLEANER_ACTIVE_LOG_SIZE_DESC =
        "Bytes used by all active data files: files required " +
            "for basic JE operation.";
    public static final StatDefinition CLEANER_ACTIVE_LOG_SIZE =
        new StatDefinition(
            CLEANER_ACTIVE_LOG_SIZE_NAME,
            CLEANER_ACTIVE_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_RESERVED_LOG_SIZE_NAME =
        "reservedLogSize";
    public static final String CLEANER_RESERVED_LOG_SIZE_DESC =
        "Bytes used by all reserved data files: files that have been" +
            "cleaned and can be deleted if they are not protected.";
    public static final StatDefinition CLEANER_RESERVED_LOG_SIZE =
        new StatDefinition(
            CLEANER_RESERVED_LOG_SIZE_NAME,
            CLEANER_RESERVED_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PROTECTED_LOG_SIZE_NAME =
        "protectedLogSize";
    public static final String CLEANER_PROTECTED_LOG_SIZE_DESC =
        "Bytes used by all protected data files: the subset of reserved " +
            "files that are temporarily protected and cannot be deleted.";
    public static final StatDefinition CLEANER_PROTECTED_LOG_SIZE =
        new StatDefinition(
            CLEANER_PROTECTED_LOG_SIZE_NAME,
            CLEANER_PROTECTED_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_PROTECTED_LOG_SIZE_MAP_NAME =
        "protectedLogSizeMap";
    public static final String CLEANER_PROTECTED_LOG_SIZE_MAP_DESC =
        "A breakdown of protectedLogSize as a map of protecting " +
            "entity name to protected size in bytes.";
    public static final StatDefinition CLEANER_PROTECTED_LOG_SIZE_MAP =
        new StatDefinition(
            CLEANER_PROTECTED_LOG_SIZE_MAP_NAME,
            CLEANER_PROTECTED_LOG_SIZE_MAP_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_AVAILABLE_LOG_SIZE_NAME =
        "availableLogSize";
    public static final String CLEANER_AVAILABLE_LOG_SIZE_DESC =
        "Bytes available for write operations when unprotected reserved " +
            "files are deleted: " +
            "free space + reservedLogSize - protectedLogSize.";
    public static final StatDefinition CLEANER_AVAILABLE_LOG_SIZE =
        new StatDefinition(
            CLEANER_AVAILABLE_LOG_SIZE_NAME,
            CLEANER_AVAILABLE_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_TOTAL_LOG_SIZE_NAME =
        "totalLogSize";
    public static final String CLEANER_TOTAL_LOG_SIZE_DESC =
        "Total bytes used by data files on disk: " +
            "activeLogSize + reservedLogSize.";
    public static final StatDefinition CLEANER_TOTAL_LOG_SIZE =
        new StatDefinition(
            CLEANER_TOTAL_LOG_SIZE_NAME,
            CLEANER_TOTAL_LOG_SIZE_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_MIN_UTILIZATION_NAME =
        "minUtilization";
    public static final String CLEANER_MIN_UTILIZATION_DESC =
        "The current minimum (lower bound) log utilization as a percentage.";
    public static final StatDefinition CLEANER_MIN_UTILIZATION =
        new StatDefinition(
            CLEANER_MIN_UTILIZATION_NAME,
            CLEANER_MIN_UTILIZATION_DESC,
            StatType.CUMULATIVE);

    public static final String CLEANER_MAX_UTILIZATION_NAME =
        "maxUtilization";
    public static final String CLEANER_MAX_UTILIZATION_DESC =
        "The current maximum (upper bound) log utilization as a percentage.";
    public static final StatDefinition CLEANER_MAX_UTILIZATION =
        new StatDefinition(
            CLEANER_MAX_UTILIZATION_NAME,
            CLEANER_MAX_UTILIZATION_DESC,
            StatType.CUMULATIVE);
}
