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

package com.sleepycat.je.rep.vlsn;

import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * Just a struct for testing convenience.
 */
class VLPair {
    final VLSN vlsn;
    final long lsn;

    VLPair(VLSN vlsn, long lsn) {
        this.vlsn = vlsn;
        this.lsn = lsn;
    }

    VLPair(int vlsnSequence, long fileNumber, long offset) {
        this.vlsn = new VLSN(vlsnSequence);
        this.lsn = DbLsn.makeLsn(fileNumber, offset);
    }

    @Override
        public String toString() {
        return vlsn + "/" + DbLsn.getNoFormatString(lsn);
    }
}
