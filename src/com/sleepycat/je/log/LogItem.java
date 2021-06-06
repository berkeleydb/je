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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Values returned when a item is logged.
 *
 * This class is used as a simple struct for returning multiple values, and
 * does not need getters and setters.
 */
public class LogItem {

    /**
     * LSN of the new log entry.  Is NULL_LSN if a BIN-delta is logged.  If
     * not NULL_LSN for a tree node, is typically used to update the slot in
     * the parent IN.
     */
    public long lsn = DbLsn.NULL_LSN;

    /**
     * Size of the new log entry.  Is used to update the LN slot in the BIN.
     */
    public int size = 0;

    /**
     * The header of the new log entry. Used by HA to do VLSN tracking and
     * implement a tip cache.
     */
    public LogEntryHeader header = null;

    /**
     * The bytes of new log entry. Used by HA to implement a tip cache.
     */
    public ByteBuffer buffer = null;

    /**
     * Used for saving the materialized form of the buffer in LogItemCache.
     */
    public volatile ReplicableLogEntry cachedEntry = null;
}
