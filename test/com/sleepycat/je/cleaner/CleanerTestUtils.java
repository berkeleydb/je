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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DbTestProxy;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Package utilities.
 */
public class CleanerTestUtils {

    /**
     * Gets the file of the LSN at the cursor position, using internal methods.
     */
    static long getLogFile(Cursor cursor) {
        CursorImpl impl = DbTestProxy.dbcGetCursorImpl(cursor);
        BIN bin = impl.getBIN();
        assertNotNull(bin);
        int index = impl.getIndex();
        long lsn = bin.getLsn(index);
        assertTrue(lsn != DbLsn.NULL_LSN);
        long file = DbLsn.getFileNumber(lsn);
        return file;
    }
}
