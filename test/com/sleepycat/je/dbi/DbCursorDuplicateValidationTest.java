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

import static org.junit.Assert.assertFalse;

import java.util.Enumeration;
import java.util.Hashtable;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbTestProxy;
import com.sleepycat.je.tree.BIN;

public class DbCursorDuplicateValidationTest extends DbCursorTestBase {

    public DbCursorDuplicateValidationTest() {
        super();
    }

    @Test
    public void testValidateCursors()
        throws Throwable {

        initEnv(true);
        Hashtable dataMap = new Hashtable();
        createRandomDuplicateData(10, 1000, dataMap, false, false);

        Hashtable bins = new Hashtable();

        DataWalker dw = new DataWalker(bins) {
                void perData(String foundKey, String foundData)
                    throws DatabaseException {
                    CursorImpl cursorImpl =
                        DbTestProxy.dbcGetCursorImpl(cursor);
                    BIN lastBin = cursorImpl.getBIN();
                    if (rnd.nextInt(10) < 8) {
                        cursor.delete();
                    }
                    dataMap.put(lastBin, lastBin);
                }
            };
        dw.setIgnoreDataMap(true);
        dw.walkData();
        dw.close();
        Enumeration e = bins.keys();
        while (e.hasMoreElements()) {
            BIN b = (BIN) e.nextElement();
            assertFalse(b.getCursorSet().size() > 0);
        }
    }
}
