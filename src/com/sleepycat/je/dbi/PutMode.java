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

/**
 * Used to distinguish Cursor put operations.
 */
public enum PutMode {

    /**
     * User operation: Cursor.putCurrent. Replace data at current position.
     * Return KEYEMPTY if record at current position is deleted.
     */
    CURRENT,

    /**
     * User operation: Cursor.putNoDupData. Applies only to databases with
     * duplicates. Insert key/data pair if it does not already exist;
     * otherwise, return KEYEXIST.
     */
    NO_DUP_DATA,

    /**
     * User operation: Cursor.putNoOverwrite. Insert key/data pair if key
     * does not already exist; otherwise, return KEYEXIST.
     */
    NO_OVERWRITE,

    /**
     * User operation: Cursor.put. Insert if key (for non-duplicates DBs) or
     * key/data (for duplicates DBs) does not already exist; otherwise,
     * overwrite key and data.
     */
    OVERWRITE,
}
