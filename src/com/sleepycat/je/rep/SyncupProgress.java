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
package com.sleepycat.je.rep;

/**
 * Describes the different phases of replication stream syncup that are
 * executed when a replica starts working with a new replication group master.
 * Meant to be used in conjunction with a 
 * {@link com.sleepycat.je.ProgressListener} that is configured through
 * {@link ReplicationConfig#setSyncupProgressListener}, to monitor the
 * occurrence and cost of replica sync-ups.
 * @see <a href="{@docRoot}/../ReplicationGuide/progoverviewlifecycle.html" 
 * target="_top">Replication Group Life Cycle</a>
 * @since 5.0
 */
public enum SyncupProgress {

    /** 
     * Syncup is starting up. The replica and feeder are searching for the
     * most recent common shared point in the replication stream.
     */
    FIND_MATCHPOINT, 
    
    /**
     * A matchpoint has been found, and the replica is determining whether it
     * has to rollback any uncommitted replicated records applied from the
     * previous master.
     */
    CHECK_FOR_ROLLBACK,
    
    /**
     * The replica is rolling back uncommitted replicated records applied from 
     * the previous master. 
     */
    DO_ROLLBACK,
    
    /** Replication stream syncup has ended. */
    END
}
