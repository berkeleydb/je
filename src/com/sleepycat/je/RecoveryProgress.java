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

/**
 * Describes the different phases of initialization that 
 * be executed when an Environment is instantiated. Meant to be used in
 * conjunction with a {@link ProgressListener} that is configured through
 * {@link EnvironmentConfig#setRecoveryProgressListener} to monitor
 * the cost of environment startup
 * @since 5.0
 */
public enum RecoveryProgress {
        /**
         * Find the last valid entry in the database log.
         */
        FIND_END_OF_LOG, 

        /**
         * Find the last complete checkpoint in the database log.
         */
        FIND_LAST_CKPT, 

        /**
         * Read log entries that pertain to the database map, which is an
         * internal index of all databases.
         */
        READ_DBMAP_INFO, 

        /**
         * Redo log entries that pertain to the database map, which is an
         * internal index of all databases.
         */
        REDO_DBMAP_INFO, 

        /**
         * Rollback uncommitted database creation, deletion and truncations.
         */
        UNDO_DBMAP_RECORDS,

        /**
         * Redo committed database creation, deletion and truncations.
         */
        REDO_DBMAP_RECORDS,

        /**
         * Read log entries that pertain to the database indices.
         */
        READ_DATA_INFO,

        /**
         * Redo log entries that pertain to the database indices.
         */
        REDO_DATA_INFO, 

        /**
         * Rollback uncommitted data operations, such as inserts, updates
         * and deletes.
         */
        UNDO_DATA_RECORDS, 

        /**
         * Repeat committed data operations, such as inserts, updates
         * and deletes.
         */
        REDO_DATA_RECORDS, 

        /**
         * Populate internal metadata which stores information about the
         * utilization level of each log file, for efficient log cleaning.
         */
        POPULATE_UTILIZATION_PROFILE,

        /**
         * Populate internal metadata which stores information about the
         * expiration time/data windows (histogram) of each log file, for
         * efficient log cleaning.
         *
         * @since 6.5
         */
        POPULATE_EXPIRATION_PROFILE,

        /**
         * Remove temporary databases created by the application that
         * are no longer valid.
         */
        REMOVE_TEMP_DBS, 

        /**
         * Perform a checkpoint to make all the work of this environment
         * startup persistent, so it is not repeated in future startups.
         */
        CKPT, 

        /** 
         * Basic recovery is completed, and the environment is able to
         * service operations.
         */
        RECOVERY_FINISHED,

        /**
         * For replicated systems only: locate the master of the 
         * replication group by querying others in the group, and holding an
         * election if necessary.
         */
        FIND_MASTER,

        /**
         * For replicated systems only: if a replica, process enough of the
         * replication stream so that the environment fulfills the required
         * consistency policy, as defined by parameters passed to the
         * ReplicatedEnvironment constructor.
         */
        BECOME_CONSISTENT
        }