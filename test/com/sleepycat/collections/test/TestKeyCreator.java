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

package com.sleepycat.collections.test;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

/**
 * Unused until secondaries are available.
 * @author Mark Hayes
 */
class TestKeyCreator implements SecondaryKeyCreator {

    private final boolean isRecNum;

    TestKeyCreator(boolean isRecNum) {

        this.isRecNum = isRecNum;
    }

    public boolean createSecondaryKey(SecondaryDatabase db,
                                      DatabaseEntry primaryKeyData,
                                      DatabaseEntry valueData,
                                      DatabaseEntry indexKeyData) {
        if (valueData.getSize() == 0) {
            return false;
        }
        if (valueData.getSize() != 1) {
            throw new IllegalStateException();
        }
        byte val = valueData.getData()[valueData.getOffset()];
        if (val == 0) {
            return false; // fixed-len pad value
        }
        val -= 100;
        if (isRecNum) {
            RecordNumberBinding.recordNumberToEntry(val, indexKeyData);
        } else {
            indexKeyData.setData(new byte[] { val }, 0, 1);
        }
        return true;
    }

    public void clearIndexKey(DatabaseEntry valueData) {

        throw new RuntimeException("not supported");
    }
}
