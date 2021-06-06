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
 * Holds the result of a database truncate operation.
 */
public class TruncateResult {

    private DatabaseImpl db;
    private int count;

    TruncateResult(DatabaseImpl db, int count) {
        this.db = db;
        this.count = count;
    }

    public DatabaseImpl getDatabase() {
        return db;
    }

    public int getRecordCount() {
        return count;
    }
}
