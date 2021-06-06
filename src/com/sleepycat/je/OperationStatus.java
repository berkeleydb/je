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
 * Status values from database operations.
 */
public enum OperationStatus {

    /**
     * The operation was successful.
     */
    SUCCESS,

    /**
     * The operation to insert data was configured to not allow overwrite and
     * the key already exists in the database.
     */
    KEYEXIST,

    /**
     * The cursor operation was unsuccessful because the current record was
     * deleted. This can only occur if a Cursor is positioned to an existing
     * record, then the record is deleted, and then the getCurrent, putCurrent,
     * or delete methods is called.
     */
    KEYEMPTY,

    /**
     * The requested key/data pair was not found.
     */
    NOTFOUND;

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "OperationStatus." + name();
    }
}
