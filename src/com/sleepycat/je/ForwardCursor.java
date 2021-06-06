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

import java.io.Closeable;

/**
 * The interface for forward-moving Cursor operations.  Specific implementations
 * may modify the documented behavior on each of these methods.
 * @since 5.0
 */
public interface ForwardCursor extends Closeable {

    /**
     * Returns the Database handle associated with this ForwardCursor.
     *
     * @return The Database handle associated with this ForwardCursor.
     */
    Database getDatabase();

    /**
     * Discards the cursor.
     *
     * <p>The cursor handle may not be used again after this method has been
     * called, regardless of the method's success or failure.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    void close();

    /**
     * Moves the cursor to a record according to the specified {@link Get}
     * type.
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param getType is {@link Get#NEXT} or {@link Get#CURRENT}.
     * interface. {@code Get.CURRENT} is permitted only if the cursor is
     * initialized (positioned on a record).
     *
     * @param options the ReadOptions, or null to use default options.
     *
     * @return the OperationResult if the record requested is found, else null.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * the cursor is uninitialized (not positioned on a record) and this is not
     * permitted (see above), or the non-transactional cursor was created in a
     * different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null getType, a null input key/data parameter,
     * an input key/data parameter with a null data array, a partial key/data
     * input parameter, and specifying a {@link ReadOptions#getLockMode()
     * lock mode} of READ_COMMITTED.
     *
     * @since 7.0
     */
    OperationResult get(
        DatabaseEntry key,
        DatabaseEntry data,
        Get getType,
        ReadOptions options);

    /**
     * Returns the key/data pair to which the cursor refers.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#CURRENT}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    OperationStatus getCurrent(DatabaseEntry key,
                               DatabaseEntry data,
                               LockMode lockMode);

    /**
     * Moves the cursor to the next key/data pair and returns that pair.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#NEXT}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    OperationStatus getNext(DatabaseEntry key,
                            DatabaseEntry data,
                            LockMode lockMode);
}
