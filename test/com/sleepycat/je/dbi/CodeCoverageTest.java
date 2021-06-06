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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.util.StringDbt;

/**
 * Various unit tests for CursorImpl to enhance code coverage.
 */
public class CodeCoverageTest extends DbCursorTestBase {

    public CodeCoverageTest() {
        super();
    }

    /**
     * Test the internal CursorImpl.delete() deleted LN code..
     */
    @Test
    public void testDeleteDeleted()
        throws Throwable {

        try {
            initEnv(false);
            doSimpleCursorPuts();

            StringDbt foundKey = new StringDbt();
            StringDbt foundData = new StringDbt();

            OperationStatus status = cursor.getFirst(foundKey, foundData,
                                                     LockMode.DEFAULT);
            assertEquals(OperationStatus.SUCCESS, status);

            cursor.delete();
            cursor.delete();

            /*
             * While we've got a cursor in hand, call CursorImpl.dumpToString()
             */
            DbInternal.getCursorImpl(cursor).dumpToString(true);
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
}
