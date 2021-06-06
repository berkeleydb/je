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
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.txn.Locker;

/**
 * A NullCursor is used as a no-op object by tree unit tests, which
 * wish to speak directly to Tree methods.
 */
public class NullCursor extends CursorImpl {
    /**
     * Cursor constructor.
     */
    public NullCursor(DatabaseImpl database, Locker txn) {
        super(database, txn);
    }

    @Override
    void addCursor(BIN bin) {}
    @Override
    void addCursor() {}
}
