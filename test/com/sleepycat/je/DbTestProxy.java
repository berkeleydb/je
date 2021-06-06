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

import com.sleepycat.je.dbi.CursorImpl;

/**
 * DbTestProxy is for internal use only. It serves to shelter methods that must
 * be public to be used by JE unit tests that but are not part of the
 * public api available to applications.
 */
public class DbTestProxy {
    /**
     * Proxy to Cursor.getCursorImpl
     */
    public static CursorImpl dbcGetCursorImpl(Cursor dbc) {
        return dbc.getCursorImpl();
    }
}
