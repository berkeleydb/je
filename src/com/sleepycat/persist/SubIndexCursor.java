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

package com.sleepycat.persist;

import com.sleepycat.je.DatabaseException;
/* <!-- begin JE only --> */
import com.sleepycat.je.Get;
/* <!-- end JE only --> */
import com.sleepycat.je.LockMode;
/* <!-- begin JE only --> */
import com.sleepycat.je.ReadOptions;
/* <!-- end JE only --> */
import com.sleepycat.util.keyrange.RangeCursor;

/**
 * The cursor for a SubIndex treats Dup and NoDup operations specially because
 * the SubIndex never has duplicates -- the keys are primary keys.  So a
 * next/prevDup operation always returns null, and a next/prevNoDup operation
 * actually does next/prev.
 *
 * @author Mark Hayes
 */
class SubIndexCursor<V> extends BasicCursor<V> {

    SubIndexCursor(RangeCursor cursor, ValueAdapter<V> adapter) {
        super(cursor, adapter, false/*updateAllowed*/);
    }

    @Override
    public EntityCursor<V> dup()
        throws DatabaseException {

        return new SubIndexCursor<V>(cursor.dup(true), adapter);
    }

    @Override
    public V nextDup(LockMode lockMode) {
        checkInitialized();
        return null;
    }

    @Override
    public V nextNoDup(LockMode lockMode)
        throws DatabaseException {

        return next(lockMode);
    }

    @Override
    public V prevDup(LockMode lockMode) {
        checkInitialized();
        return null;
    }

    @Override
    public V prevNoDup(LockMode lockMode)
        throws DatabaseException {

        return prev(lockMode);
    }

    /* <!-- begin JE only --> */
    public EntityResult<V> get(Get getType, ReadOptions options)
        throws DatabaseException {

        switch (getType) {
        case NEXT_DUP:
            return null;
        case NEXT_NO_DUP:
            return super.get(Get.NEXT, options);
        case PREV_DUP:
            return null;
        case PREV_NO_DUP:
            return super.get(Get.PREV, options);
        default:
            return super.get(getType, options);
        }
    }
    /* <!-- end JE only --> */
}
