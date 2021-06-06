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

import java.util.Iterator;

import com.sleepycat.compat.DbCompat;
import com.sleepycat.compat.DbCompat.OpReadOptions;
import com.sleepycat.compat.DbCompat.OpResult;
/* <!-- begin JE only --> */
import com.sleepycat.je.CacheMode;
/* <!-- end JE only --> */
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
/* <!-- begin JE only --> */
import com.sleepycat.je.Get;
/* <!-- end JE only --> */
import com.sleepycat.je.LockMode;
/* <!-- begin JE only --> */
import com.sleepycat.je.OperationResult;
/* <!-- end JE only --> */
import com.sleepycat.je.OperationStatus;
/* <!-- begin JE only --> */
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.WriteOptions;
/* <!-- end JE only --> */
import com.sleepycat.util.keyrange.RangeCursor;

/**
 * Implements EntityCursor and uses a ValueAdapter so that it can enumerate
 * either keys or entities.
 *
 * @author Mark Hayes
 */
class BasicCursor<V> implements EntityCursor<V> {

    RangeCursor cursor;
    ValueAdapter<V> adapter;
    boolean updateAllowed;
    DatabaseEntry key;
    DatabaseEntry pkey;
    DatabaseEntry data;

    BasicCursor(RangeCursor cursor,
                ValueAdapter<V> adapter,
                boolean updateAllowed) {
        this.cursor = cursor;
        this.adapter = adapter;
        this.updateAllowed = updateAllowed;
        key = adapter.initKey();
        pkey = adapter.initPKey();
        data = adapter.initData();
    }

    public V first()
        throws DatabaseException {

        return first(null);
    }

    public V first(LockMode lockMode)
        throws DatabaseException {

        return returnValue(
            cursor.getFirst(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V last()
        throws DatabaseException {

        return last(null);
    }

    public V last(LockMode lockMode)
        throws DatabaseException {

        return returnValue(
            cursor.getLast(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V next()
        throws DatabaseException {

        return next(null);
    }

    public V next(LockMode lockMode)
        throws DatabaseException {

        return returnValue(
            cursor.getNext(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V nextDup()
        throws DatabaseException {

        return nextDup(null);
    }

    public V nextDup(LockMode lockMode)
        throws DatabaseException {

        checkInitialized();
        return returnValue(
            cursor.getNextDup(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V nextNoDup()
        throws DatabaseException {

        return nextNoDup(null);
    }

    public V nextNoDup(LockMode lockMode)
        throws DatabaseException {

        return returnValue(
            cursor.getNextNoDup(
                key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V prev()
        throws DatabaseException {

        return prev(null);
    }

    public V prev(LockMode lockMode)
        throws DatabaseException {

        return returnValue(
            cursor.getPrev(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V prevDup()
        throws DatabaseException {

        return prevDup(null);
    }

    public V prevDup(LockMode lockMode)
        throws DatabaseException {

        checkInitialized();
        return returnValue(
            cursor.getPrevDup(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V prevNoDup()
        throws DatabaseException {

        return prevNoDup(null);
    }

    public V prevNoDup(LockMode lockMode)
        throws DatabaseException {

        return returnValue(
            cursor.getPrevNoDup(
                key, pkey, data, OpReadOptions.make(lockMode)));
    }

    public V current()
        throws DatabaseException {

        return current(null);
    }

    public V current(LockMode lockMode)
        throws DatabaseException {

        checkInitialized();
        return returnValue(
            cursor.getCurrent(key, pkey, data, OpReadOptions.make(lockMode)));
    }

    /* <!-- begin JE only --> */
    public EntityResult<V> get(Get getType, ReadOptions options)
        throws DatabaseException {

        OpReadOptions opOptions = OpReadOptions.make(options);

        switch (getType) {
        case CURRENT:
            return returnResult(
                cursor.getCurrent(key, pkey, data, opOptions));
        case FIRST:
            return returnResult(
                cursor.getFirst(key, pkey, data, opOptions));
        case LAST:
            return returnResult(
                cursor.getLast(key, pkey, data, opOptions));
        case NEXT:
            return returnResult(
                cursor.getNext(key, pkey, data, opOptions));
        case NEXT_DUP:
            return returnResult(
                cursor.getNextDup(key, pkey, data, opOptions));
        case NEXT_NO_DUP:
            return returnResult(
                cursor.getNextNoDup(key, pkey, data, opOptions));
        case PREV:
            return returnResult(
                cursor.getPrev(key, pkey, data, opOptions));
        case PREV_DUP:
            return returnResult(
                cursor.getPrevDup(key, pkey, data, opOptions));
        case PREV_NO_DUP:
            return returnResult(
                cursor.getPrevNoDup(key, pkey, data, opOptions));
        default:
            throw new IllegalArgumentException(
                "getType not allowed: " + getType);
        }
    }
    /* <!-- end JE only --> */

    public int count()
        throws DatabaseException {

        checkInitialized();
        return cursor.count();
    }

    /* <!-- begin JE only --> */
    public long countEstimate()
        throws DatabaseException {

        checkInitialized();
        return cursor.getCursor().countEstimate();
    }
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /* for FUTURE use
    public long skipNext(long maxCount) {
        return skipNext(maxCount, null);
    }

    public long skipNext(long maxCount, LockMode lockMode) {
        checkInitialized();
        return cursor.getCursor().skipNext
            (maxCount, BasicIndex.NO_RETURN_ENTRY, BasicIndex.NO_RETURN_ENTRY,
             lockMode);
    }

    public long skipPrev(long maxCount) {
        return skipPrev(maxCount, null);
    }

    public long skipPrev(long maxCount, LockMode lockMode) {
        checkInitialized();
        return cursor.getCursor().skipPrev
            (maxCount, BasicIndex.NO_RETURN_ENTRY, BasicIndex.NO_RETURN_ENTRY,
             lockMode);
    }
    */
    /* <!-- end JE only --> */

    public Iterator<V> iterator() {
        return iterator(null);
    }

    public Iterator<V> iterator(LockMode lockMode) {
        return new BasicIterator(this, lockMode);
    }

    public boolean update(V entity)
        throws DatabaseException {

        /* <!-- begin JE only --> */
        if (DbCompat.IS_JE) {
            return update(entity, null) != null;
        }
        /* <!-- end JE only --> */

        if (!updateAllowed) {
            throw new UnsupportedOperationException(
                "Update not allowed on a secondary index");
        }
        checkInitialized();
        adapter.valueToData(entity, data);

        return cursor.getCursor().putCurrent(data) == OperationStatus.SUCCESS;
    }

    /* <!-- begin JE only --> */
    public OperationResult update(V entity, WriteOptions options)
        throws DatabaseException {

        if (!updateAllowed) {
            throw new UnsupportedOperationException(
                "Update not allowed on a secondary index");
        }
        checkInitialized();
        adapter.valueToData(entity, data);

        return cursor.getCursor().put(null, data, Put.CURRENT, options);
    }
    /* <!-- end JE only --> */

    public boolean delete()
        throws DatabaseException {

        /* <!-- begin JE only --> */
        if (DbCompat.IS_JE) {
            return delete(null) != null;
        }
        /* <!-- end JE only --> */

        checkInitialized();
        return cursor.getCursor().delete() == OperationStatus.SUCCESS;
    }

    /* <!-- begin JE only --> */
    public OperationResult delete(WriteOptions options)
        throws DatabaseException {

        checkInitialized();
        return cursor.getCursor().delete(options);
    }
    /* <!-- end JE only --> */

    public EntityCursor<V> dup()
        throws DatabaseException {

        return new BasicCursor<V>(cursor.dup(true), adapter, updateAllowed);
    }

    public void close()
        throws DatabaseException {

        cursor.close();
    }

    /* <!-- begin JE only --> */
    public void setCacheMode(CacheMode cacheMode) {
        cursor.getCursor().setCacheMode(cacheMode);
    }
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    public CacheMode getCacheMode() {
        return cursor.getCursor().getCacheMode();
    }
    /* <!-- end JE only --> */

    void checkInitialized()
        throws IllegalStateException {

        if (!cursor.isInitialized()) {
            throw new IllegalStateException
                ("Cursor is not initialized at a valid position");
        }
    }

    V returnValue(OpResult opResult) {
        V value;
        if (opResult.isSuccess()) {
            value = adapter.entryToValue(key, pkey, data);
        } else {
            value = null;
        }
        /* Clear entries to save memory. */
        adapter.clearEntries(key, pkey, data);
        return value;
    }

    /* <!-- begin JE only --> */
    EntityResult<V> returnResult(OpResult opResult) {
        V value = returnValue(opResult);
        return (value != null) ?
            new EntityResult<>(value, opResult.jeResult) :
            null;
    }
    /* <!-- end JE only --> */
}
