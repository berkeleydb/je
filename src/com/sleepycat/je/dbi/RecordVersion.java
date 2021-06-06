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
 * Used to return the VLSN and LSN for a record.  The VLSN is a unique version
 * for a rep group, and the LSN is unique for a single node.
 */
public class RecordVersion {

    private final long vlsn;
    private final long lsn;

    RecordVersion(long vlsn, long lsn) {
        this.vlsn = vlsn;
        this.lsn = lsn;
    }

    public long getVLSN() {
        return vlsn;
    }

    public long getLSN() {
        return lsn;
    }
}
