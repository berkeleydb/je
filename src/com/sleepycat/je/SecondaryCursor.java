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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.SearchMode;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * A database cursor for a secondary database. Cursors are not thread safe and
 * the application is responsible for coordinating any multithreaded access to
 * a single cursor object.
 *
 * <p>Secondary cursors are returned by {@link SecondaryDatabase#openCursor
 * SecondaryDatabase.openCursor} and {@link
 * SecondaryDatabase#openSecondaryCursor
 * SecondaryDatabase.openSecondaryCursor}.  The distinguishing characteristics
 * of a secondary cursor are:</p>
 *
 * <ul> <li>Direct calls to <code>put()</code> methods on a secondary cursor
 * are prohibited.
 *
 * <li>The {@link #delete} method of a secondary cursor will delete the primary
 * record and as well as all its associated secondary records.
 *
 * <li>Calls to all get methods will return the data from the associated
 * primary database.
 *
 * <li>Additional get method signatures are provided to return the primary key
 * in an additional pKey parameter.
 *
 * <li>Calls to {@link #dup} will return a {@link SecondaryCursor}.
 *
 * </ul>
 *
 * <p>To obtain a secondary cursor with default attributes:</p>
 *
 * <blockquote><pre>
 *     SecondaryCursor cursor = myDb.openSecondaryCursor(txn, null);
 * </pre></blockquote>
 *
 * <p>To customize the attributes of a cursor, use a CursorConfig object.</p>
 *
 * <blockquote><pre>
 *     CursorConfig config = new CursorConfig();
 *     config.setReadUncommitted(true);
 *     SecondaryCursor cursor = myDb.openSecondaryCursor(txn, config);
 * </pre></blockquote>
 */
public class SecondaryCursor extends Cursor {

    private final SecondaryDatabase secondaryDb;

    /**
     * Cursor constructor. Not public. To get a cursor, the user should call
     * SecondaryDatabase.cursor();
     */
    SecondaryCursor(final SecondaryDatabase dbHandle,
                    final Transaction txn,
                    final CursorConfig cursorConfig) {
        super(dbHandle, txn, cursorConfig);
        secondaryDb = dbHandle;
    }

    /**
     * Cursor constructor. Not public. To get a cursor, the user should call
     * SecondaryDatabase.cursor();
     */
    SecondaryCursor(final SecondaryDatabase dbHandle,
                    final Locker locker,
                    final CursorConfig cursorConfig) {
        super(dbHandle, locker, cursorConfig);
        secondaryDb = dbHandle;
    }

    /**
     * Copy constructor.
     */
    private SecondaryCursor(final SecondaryCursor cursor,
                            final boolean samePosition) {
        super(cursor, samePosition);
        secondaryDb = cursor.secondaryDb;
    }

    boolean isSecondaryCursor() {
        return true;
    }

    /**
     * Returns the Database handle associated with this Cursor.
     *
     * @return The Database handle associated with this Cursor.
     */
    @Override
    public SecondaryDatabase getDatabase() {
        return secondaryDb;
    }

    /**
     * Returns the primary {@link com.sleepycat.je.Database Database}
     * associated with this cursor.
     *
     * <p>Calling this method is the equivalent of the following
     * expression:</p>
     *
     * <blockquote><pre>
     *         getDatabase().getPrimaryDatabase()
     * </pre></blockquote>
     *
     * @return The primary {@link com.sleepycat.je.Database Database}
     * associated with this cursor.
     */

    /*
     * To be added when SecondaryAssociation is published:
     * If a {@link SecondaryAssociation} is {@link
     * SecondaryCursor#setSecondaryAssociation configured}, this method returns
     * null.
     */
    public Database getPrimaryDatabase() {
        return secondaryDb.getPrimaryDatabase();
    }

    /**
     * Returns a new <code>SecondaryCursor</code> for the same transaction as
     * the original cursor.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public SecondaryCursor dup(final boolean samePosition) {
        checkOpenAndState(false);
        return new SecondaryCursor(this, samePosition);
    }

    /**
     * Returns a new copy of the cursor as a <code>SecondaryCursor</code>.
     *
     * <p>Calling this method is the equivalent of calling {@link #dup} and
     * casting the result to {@link SecondaryCursor}.</p>
     *
     * @see #dup
     *
     * @deprecated As of JE 4.0.13, replaced by {@link Cursor#dup}.</p>
     */
    public SecondaryCursor dupSecondary(final boolean samePosition) {
        return dup(samePosition);
    }

    /**
     * Delete the record to which the cursor refers from the primary database
     * and all secondary indices.
     *
     * <p>This method behaves as if {@link Database#delete(Transaction,
     * DatabaseEntry, WriteOptions)} were called for the primary database,
     * using the primary key associated with this cursor position.</p>
     *
     * <p>The cursor position is unchanged after a delete, and subsequent calls
     * to cursor functions expecting the cursor to refer to an existing record
     * will fail.</p>
     *
     * <p>WARNING: Unlike read operations using a SecondaryCursor, write
     * operations like this one are deadlock-prone.</p>
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationResult delete(final WriteOptions options) {

        checkOpenAndState(true);

        trace(Level.FINEST, "SecondaryCursor.delete: ", null);

        final CacheMode cacheMode =
            options != null ? options.getCacheMode() : null;

        /* Read the primary key (the data of a secondary). */
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry pKey = new DatabaseEntry();

        /*
         * Currently we write-lock the secondary before deleting the primary,
         * which reverses the normal locking order and is deadlock-prone.
         *
         * FUTURE: To avoid deadlocks we could use dirty-read-all here, and
         * then perform a special delete-if-has-secondary-key operation on the
         * primary. We must be careful not to delete the primary record if,
         * after locking it, it does not reference this secondary key.
         */
        final OperationResult secResult =
            getCurrentInternal(key, pKey, LockMode.RMW, cacheMode);

        if (secResult == null) {
            return null;
        }

        final Locker locker = cursorImpl.getLocker();
        final Database primaryDb = secondaryDb.getPrimary(pKey);

        if (primaryDb == null) {
            /* Primary was removed from the association. */
            deleteNoNotify(cacheMode, getDatabaseImpl().getRepContext());
            return secResult;
        }

        /* Delete the primary and all secondaries (including this one). */
        final OperationResult priResult =
            primaryDb.deleteInternal(locker, pKey, cacheMode);

        if (priResult != null) {
            return priResult;
        }

        /* The primary record may have expired after locking the secondary. */
        if (cursorImpl.isProbablyExpired()) {
            return null;
        }

        throw secondaryDb.secondaryRefersToMissingPrimaryKey(
            locker, key, pKey, secResult.getExpirationTime());
    }

    /**
     * Delete the record to which the cursor refers from the primary database
     * and all secondary indices.
     *
     * <p>This method behaves as if {@link Database#delete(Transaction,
     * DatabaseEntry, WriteOptions)} were called for the primary database,
     * using the primary key associated with this cursor position.</p>
     *
     * <p>The cursor position is unchanged after a delete, and subsequent calls
     * to cursor functions expecting the cursor to refer to an existing record
     * will fail.</p>
     *
     * <p>Calling this method is equivalent to calling {@link
     * #delete(WriteOptions)}.</p>
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus delete() {
        final OperationResult result = delete(null);
        return result == null ?
            OperationStatus.KEYEMPTY : OperationStatus.SUCCESS;
    }

    /**
     * This operation is not allowed on a secondary cursor. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary cursor should be used instead.
     */
    @Override
    public OperationResult put(
        DatabaseEntry key,
        DatabaseEntry data,
        Put putType,
        WriteOptions options) {

        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary cursor. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary cursor should be used instead.
     */
    @Override
    public OperationStatus put(final DatabaseEntry key,
                               final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary cursor. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary cursor should be used instead.
     */
    @Override
    public OperationStatus putNoOverwrite(final DatabaseEntry key,
                                          final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary cursor. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary cursor should be used instead.
     */
    @Override
    public OperationStatus putNoDupData(final DatabaseEntry key,
                                        final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary cursor. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary cursor should be used instead.
     */
    @Override
    public OperationStatus putCurrent(final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * Moves the cursor to a record according to the specified {@link Get}
     * type.
     *
     * <p>The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     * In addition, two operations are not supported by this method:
     * {@link Get#SEARCH_BOTH} and {@link Get#SEARCH_BOTH_GTE}.</p>
     */
    @Override
    public OperationResult get(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Get getType,
        final ReadOptions options) {

        return get(key, null, data, getType, options);
    }

    /**
     * Moves the cursor to a record according to the specified {@link Get}
     * type.
     *
     * <p>If the operation succeeds, the record at the resulting cursor
     * position will be locked according to the {@link
     * ReadOptions#getLockMode() lock mode} specified, the key, primary key,
     * and/or data will be returned via the (non-null) DatabaseEntry
     * parameters, and a non-null OperationResult will be returned. If the
     * operation fails because the record requested is not found, null is
     * returned.</p>
     *
     * <p>The following table lists each allowed operation and whether the key,
     * pKey and data parameters are <a href="DatabaseEntry.html#params">input
     * or output parameters</a>. Also specified is whether the cursor must be
     * initialized (positioned on a record) before calling this method. See the
     * individual {@link Get} operations for more information.</p>
     *
     * <div><table border="1" summary="">
     * <tr>
     *     <th>Get operation</th>
     *     <th>Description</th>
     *     <th>'key' parameter</th>
     *     <th>'pKey' parameter</th>
     *     <th>'data' parameter</th>
     *     <th>Cursor position<br/>must be initialized?</th>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH}</td>
     *     <td>Searches using an exact match by key.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_BOTH}</td>
     *     <td>Searches using an exact match by key and pKey.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_GTE}</td>
     *     <td>Searches using a GTE match by key.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input/output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_BOTH_GTE}</td>
     *     <td>Searches using an exact match by key and a GTE match by pKey.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#inParam">input/output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#CURRENT}</td>
     *     <td>Accesses the current record</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#FIRST}</td>
     *     <td>Finds the first record in the database.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#LAST}</td>
     *     <td>Finds the last record in the database.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT}</td>
     *     <td>Moves to the next record.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT_DUP}</td>
     *     <td>Moves to the next record with the same key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT_NO_DUP}</td>
     *     <td>Moves to the next record with a different key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV}</td>
     *     <td>Moves to the previous record.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV_DUP}</td>
     *     <td>Moves to the previous record with the same key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV_NO_DUP}</td>
     *     <td>Moves to the previous record with a different key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * </table></div>
     *
     * <p>** - For these 'next' and 'previous' operations the cursor may be
     * uninitialized, in which case the cursor will be moved to the first or
     * last record, respectively.</p>
     *
     * @param key the secondary key input or output parameter, depending on
     * getType.
     *
     * @param pKey the primary key input or output parameter, depending on
     * getType.
     *
     * @param data the primary data output parameter.
     *
     * @param getType the Get operation type. May not be null.
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
     * This includes passing a null getType, a null input key/pKey parameter,
     * an input key/pKey parameter with a null data array, a partial key/pKey
     * input parameter, and specifying a {@link ReadOptions#getLockMode()
     * lock mode} of READ_COMMITTED.
     *
     * @since 7.0
     */
    public OperationResult get(
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        final DatabaseEntry data,
        final Get getType,
        ReadOptions options) {

        try {
            checkOpen();

            if (options == null) {
                options = DEFAULT_READ_OPTIONS;
            }

            final LockMode lockMode = options.getLockMode();

            trace(
                Level.FINEST, "SecondaryCursor.get: ", String.valueOf(getType),
                key, data, lockMode);

            return getInternal(
                key, pKey, data, getType, options, lockMode);

        } catch (Error E) {
            getDatabaseImpl().getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Performs the get() operation except for state checking and tracing.
     *
     * The LockMode is passed because for Database operations it is sometimes
     * different than ReadOptions.getLockMode.
     *
     * Allows passing a throughput stat index so it can be called for Database
     * and SecondaryCursor operations.
     */
    OperationResult getInternal(
        DatabaseEntry key,
        DatabaseEntry pKey,
        DatabaseEntry data,
        Get getType,
        final ReadOptions options,
        final LockMode lockMode) {

        DatabaseUtil.checkForNullParam(getType, "getType");

        if (data == null) {
            data = NO_RETURN_DATA;
        }

        final CacheMode cacheMode = options.getCacheMode();
        final SearchMode searchMode = getType.getSearchMode();

        if (searchMode != null) {
            checkState(false /*mustBeInitialized*/);

            DatabaseUtil.checkForNullDbt(key, "key", true);
            DatabaseUtil.checkForPartial(key, "key");

            if (searchMode.isDataSearch()) {
                DatabaseUtil.checkForNullDbt(pKey, "pKey", true);
                DatabaseUtil.checkForPartial(pKey, "pKey");
            } else {
                if (pKey == null) {
                    pKey = new DatabaseEntry();
                }
            }

            return search(key, pKey, data, lockMode, cacheMode, searchMode);
        }

        if (key == null) {
            key = NO_RETURN_DATA;
        }
        if (pKey == null) {
            pKey = new DatabaseEntry();
        }

        GetMode getMode = getType.getGetMode();

        if (getType.getAllowNextPrevUninitialized() &&
            cursorImpl.isNotInitialized()) {

            assert getMode != null;
            getType = getMode.isForward() ? Get.FIRST : Get.LAST;
            getMode = null;
        }

        if (getMode != null) {
            checkState(true /*mustBeInitialized*/);

            return retrieveNext(
                key, pKey, data, lockMode, cacheMode, getMode,
                getLockPrimaryOnly(lockMode, data));
        }

        if (getType == Get.CURRENT) {
            checkState(true /*mustBeInitialized*/);

            return getCurrentInternal(key, pKey, data, lockMode, cacheMode);
        }

        assert getType == Get.FIRST || getType == Get.LAST;
        checkState(false /*mustBeInitialized*/);

        return position(
            key, pKey, data, lockMode, cacheMode, getType == Get.FIRST,
            getLockPrimaryOnly(lockMode, data));
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getCurrent(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        return getCurrent(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Returns the key/data pair to which the cursor refers.
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
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
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getCurrent(final DatabaseEntry key,
                                      final DatabaseEntry pKey,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.CURRENT, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.KEYEMPTY : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getFirst(final DatabaseEntry key,
                                    final DatabaseEntry data,
                                    final LockMode lockMode) {
        return getFirst(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the first key/data pair of the database, and return
     * that pair.  If the first key has duplicate values, the first data item
     * in the set of duplicates is returned.
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getFirst(final DatabaseEntry key,
                                    final DatabaseEntry pKey,
                                    final DatabaseEntry data,
                                    final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.FIRST, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getLast(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode) {
        return getLast(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the last key/data pair of the database, and return
     * that pair.  If the last key has duplicate values, the last data item in
     * the set of duplicates is returned.
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getLast(final DatabaseEntry key,
                                   final DatabaseEntry pKey,
                                   final DatabaseEntry data,
                                   final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.LAST, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getNext(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode) {
        return getNext(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the next key/data pair and return that pair.  If the
     * matching key has duplicate values, the first data item in the set of
     * duplicates is returned.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the first
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next key/data pair of the database, and that pair
     * is returned.  In the presence of duplicate key values, the value of the
     * key may not change.</p>
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getNext(final DatabaseEntry key,
                                   final DatabaseEntry pKey,
                                   final DatabaseEntry data,
                                   final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.NEXT, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getNextDup(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        return getNextDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * If the next key/data pair of the database is a duplicate data record for
     * the current key/data pair, move the cursor to the next key/data pair of
     * the database and return that pair.
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getNextDup(final DatabaseEntry key,
                                      final DatabaseEntry pKey,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.NEXT_DUP,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getNextNoDup(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode) {
        return getNextNoDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the next non-duplicate key/data pair and return that
     * pair.  If the matching key has duplicate values, the first data item in
     * the set of duplicates is returned.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the first
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next non-duplicate key of the database, and that
     * key/data pair is returned.</p>
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getNextNoDup(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data,
                                        final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.NEXT_NO_DUP,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getPrev(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode) {
        return getPrev(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the previous key/data pair and return that pair. If
     * the matching key has duplicate values, the last data item in the set of
     * duplicates is returned.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the last
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the previous key/data pair of the database, and that
     * pair is returned. In the presence of duplicate key values, the value of
     * the key may not change.</p>
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getPrev(final DatabaseEntry key,
                                   final DatabaseEntry pKey,
                                   final DatabaseEntry data,
                                   final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.PREV, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getPrevDup(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        return getPrevDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * If the previous key/data pair of the database is a duplicate data record
     * for the current key/data pair, move the cursor to the previous key/data
     * pair of the database and return that pair.
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getPrevDup(final DatabaseEntry key,
                                      final DatabaseEntry pKey,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.PREV_DUP,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getPrevNoDup(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode) {
        return getPrevNoDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the previous non-duplicate key/data pair and return
     * that pair.  If the matching key has duplicate values, the last data item
     * in the set of duplicates is returned.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the last
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the previous non-duplicate key of the database, and
     * that key/data pair is returned.</p>
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getPrevNoDup(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data,
                                        final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.PREV_NO_DUP,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getSearchKey(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode) {
        return getSearchKey(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the given key of the database, and return the datum
     * associated with the given key.  If the matching key has duplicate
     * values, the first data item in the set of duplicates is returned.
     *
     * @param key the secondary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchKey(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data,
                                        final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.SEARCH, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * {@inheritDoc}
     *
     * The difference between this method and the method it overrides in
     * {@link Cursor} is that the key here is defined as the secondary
     * records's key, and the data is defined as the primary record's data.
     */
    @Override
    public OperationStatus getSearchKeyRange(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode) {
        return getSearchKeyRange(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the closest matching key of the database, and return
     * the data item associated with the matching key.  If the matching key has
     * duplicate values, the first data item in the set of duplicates is
     * returned.
     *
     * <p>The returned key/data pair is for the smallest key greater than or
     * equal to the specified key (as determined by the key comparison
     * function), permitting partial key matches and range searches.</p>
     *
     * @param key the secondary key used as input and returned as output.  It
     * must be initialized with a non-null byte array by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchKeyRange(final DatabaseEntry key,
                                             final DatabaseEntry pKey,
                                             final DatabaseEntry data,
                                             final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.SEARCH_GTE,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * This operation is not allowed with this method signature. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method with the <code>pKey</code> parameter should be
     * used instead.
     */
    @Override
    public OperationStatus getSearchBoth(final DatabaseEntry key,
                                         final DatabaseEntry data,
                                         final LockMode lockMode) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * Move the cursor to the specified secondary and primary key, where both
     * the primary and secondary key items must match.
     *
     * @param key the secondary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param pKey the primary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchBoth(final DatabaseEntry key,
                                         final DatabaseEntry pKey,
                                         final DatabaseEntry data,
                                         final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.SEARCH_BOTH,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * This operation is not allowed with this method signature. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method with the <code>pKey</code> parameter should be
     * used instead.
     */
    @Override
    public OperationStatus getSearchBothRange(final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final LockMode lockMode) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * Move the cursor to the specified secondary key and closest matching
     * primary key of the database.
     *
     * <p>In the case of any database supporting sorted duplicate sets, the
     * returned key/data pair is for the smallest primary key greater than or
     * equal to the specified primary key (as determined by the key comparison
     * function), permitting partial matches and range searches in duplicate
     * data sets.</p>
     *
     * @param key the secondary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param pKey the primary key used as input and returned as output.  It
     * must be initialized with a non-null byte array by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchBothRange(final DatabaseEntry key,
                                              final DatabaseEntry pKey,
                                              final DatabaseEntry data,
                                              final LockMode lockMode) {
        final OperationResult result = get(
            key, pKey, data, Get.SEARCH_BOTH_GTE,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Returns the current key and data.
     *
     * When a secondary key is found, but the primary cannot be read for one of
     * the following reasons, this method returns KEYEMPTY.
     *
     *  1) lock mode is read-uncommitted and the primary record was deleted in
     *     the middle of the operation
     *
     *  2) the primary DB has been removed from the SecondaryAssocation
     */
    private OperationResult getCurrentInternal(final DatabaseEntry key,
                                               final DatabaseEntry pKey,
                                               final DatabaseEntry data,
                                               final LockMode lockMode,
                                               final CacheMode cacheMode) {
        final boolean lockPrimaryOnly = getLockPrimaryOnly(lockMode, data);

        final LockMode searchLockMode =
            lockPrimaryOnly ? LockMode.READ_UNCOMMITTED_ALL : lockMode;

        final OperationResult result = getCurrentInternal(
            key, pKey, searchLockMode, cacheMode);

        if (result == null) {
            return null;
        }

        return readPrimaryAfterGet(
            key, pKey, data, lockMode, isReadUncommittedMode(searchLockMode),
            lockPrimaryOnly, result);
    }

    /**
     * Calls search() and retrieves primary data.
     *
     * When the primary record cannot be read (see readPrimaryAfterGet),
     * advance over the unavailable record, according to the search type.
     */
    OperationResult search(final DatabaseEntry key,
                           final DatabaseEntry pKey,
                           final DatabaseEntry data,
                           final LockMode lockMode,
                           final CacheMode cacheMode,
                           final SearchMode searchMode) {

        final boolean lockPrimaryOnly = getLockPrimaryOnly(lockMode, data);

        final LockMode searchLockMode =
            lockPrimaryOnly ? LockMode.READ_UNCOMMITTED_ALL : lockMode;

        final OperationResult result1 = search(
            key, pKey, searchLockMode, cacheMode, searchMode, true);
        if (result1 == null) {
            return null;
        }

        final OperationResult result2 = readPrimaryAfterGet(
            key, pKey, data, lockMode, isReadUncommittedMode(searchLockMode),
            lockPrimaryOnly, result1);

        if (result2 != null) {
            return result2;
        }

        /* Advance over the unavailable record. */
        switch (searchMode) {
        case BOTH:
            /* Exact search on sec and pri key. */
            return null;
        case SET:
        case BOTH_RANGE:
            /* Find exact sec key and next primary key. */
            return retrieveNext(
                key, pKey, data, lockMode, cacheMode, GetMode.NEXT_DUP,
                lockPrimaryOnly);
        case SET_RANGE:
            /* Find next sec key or primary key. */
            return retrieveNext(
                key, pKey, data, lockMode, cacheMode, GetMode.NEXT,
                lockPrimaryOnly);
        default:
            throw EnvironmentFailureException.unexpectedState();
        }
    }

    /**
     * Calls position() and retrieves primary data.
     *
     * When the primary record cannot be read (see readPrimaryAfterGet),
     * advance over the unavailable record.
     */
    private OperationResult position(final DatabaseEntry key,
                                     final DatabaseEntry pKey,
                                     final DatabaseEntry data,
                                     final LockMode lockMode,
                                     final CacheMode cacheMode,
                                     final boolean first,
                                     final boolean lockPrimaryOnly) {

        final LockMode searchLockMode =
            lockPrimaryOnly ? LockMode.READ_UNCOMMITTED_ALL : lockMode;

        final OperationResult result1 =
            position(key, pKey, searchLockMode, cacheMode, first);

        if (result1 == null) {
            return null;
        }

        final OperationResult result2 = readPrimaryAfterGet(
            key, pKey, data, lockMode, isReadUncommittedMode(searchLockMode),
            lockPrimaryOnly, result1);

        if (result2 != null) {
            return result2;
        }

        /* Advance over the unavailable record. */
        return retrieveNext(
            key, pKey, data, lockMode, cacheMode,
            first ? GetMode.NEXT : GetMode.PREV, lockPrimaryOnly);
    }

    /**
     * Calls retrieveNext() and retrieves primary data.
     *
     * When the primary record cannot be read (see readPrimaryAfterGet),
     * advance over the unavailable record.
     */
    private OperationResult retrieveNext(
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        final DatabaseEntry data,
        final LockMode lockMode,
        final CacheMode cacheMode,
        final GetMode getMode,
        final boolean lockPrimaryOnly) {

        final LockMode searchLockMode =
            lockPrimaryOnly ? LockMode.READ_UNCOMMITTED_ALL : lockMode;

        while (true) {
            final OperationResult result1 = retrieveNext(
                key, pKey, searchLockMode, cacheMode, getMode);

            if (result1 == null) {
                return null;
            }

            final OperationResult result2 = readPrimaryAfterGet(
                key, pKey, data, lockMode,
                isReadUncommittedMode(searchLockMode),
                lockPrimaryOnly, result1);

            if (result2 != null) {
                return result2;
            }

            /* Continue loop to advance over the unavailable record. */
        }
    }

    /**
     * Returns whether to use dirty-read for the secondary read and rely on
     * the primary record lock alone.
     *
     * False is returned in the following cases, and true otherwise.
     *
     * + When the user specifies dirty-read, since there is no locking.
     *
     * + For serializable isolation because this would likely require other
     *   changes to the serializable algorithms. Currently we live with the
     *   fact that secondary access with serializable isolation is deadlock
     *   prone.
     *
     * + When the primary data is not requested we must lock the secondary
     *   because we do not read or lock the primary.
     */
    private boolean getLockPrimaryOnly(final LockMode lockMode,
                                       final DatabaseEntry data) {

        final boolean dataRequested =
            data != null &&
            (!data.getPartial() || data.getPartialLength() != 0);

        return dataRequested &&
               !isSerializableIsolation(lockMode) &&
               !isReadUncommittedMode(lockMode);
    }

    /**
     * Reads the primary record associated with a secondary record.
     *
     * An approach is used for secondary DB access that avoids deadlocks that
     * would occur if locks were acquired on primary and secondary DBs in
     * different orders for different operations.  The primary DB lock must
     * always be acquired first when doing a write op; for example, when
     * deleting a primary record, we don't know what the secondary keys are
     * until we read (and lock) the primary record.  However, the natural way
     * to read via a secondary DB would be to read (and lock) the secondary
     * record first to obtain the primary key, and then read (and lock) the
     * primary record. Because this would obtain locks in the reverse order as
     * write ops, a different approach is used for secondary reads.
     *
     * In order to avoid deadlocks, for non-serializable isolation we change
     * the natural lock order for reads -- we only lock the primary record and
     * then check the secondary record's reference to primary record. The
     * initial read of the secondary DB is performed without acquiring locks
     * (dirty-read). The primary key is then used to read and lock the
     * associated primary record. At this point only the primary record is
     * locked.
     *
     * Then, the secondary reference is checked (see checkReferenceToPrimary in
     * Cursor). Note that there is no need to lock the secondary before
     * checking its reference to the primary, because during the check the
     * secondary is protected from changes by the lock on the primary. If we
     * discover that the secondary record has been deleted (for example, due to
     * an update to the primary after the secondary dirty-read and before the
     * primary locking read), the record will not be returned to the caller (it
     * will be skipped) and we will advance to the next record according to the
     * operation type. In this case the lock on the primary record is released.
     *
     * In addition, the READ_UNCOMMITTED_ALL mode is used for the dirty-read
     * of the secondary DB.  This ensures that we do not skip uncommitted
     * deleted records.  See LockMode.READ_UNCOMMITTED_ALL and
     * Cursor.readPrimaryAfterGet for further details.
     *
     * For a secondary DB with dups, READ_UNCOMMITTED_ALL will return a deleted
     * record for an open txn, and we'll discover the deletion when reading
     * (and locking) the primary record. The primary lookup is wasted in that
     * case, but this should be infrequent. For a secondary DB without dups,
     * READ_UNCOMMITTED_ALL will block during the secondary read in this case
     * (a deleted record for an open txn) in order to obtain the data (the
     * primary key).
     *
     * @return null if the primary record has been deleted or updated (when
     * using read-uncommitted), or the primary database has been removed from
     * the association. Otherwise, returns the result that should be returned
     * to the API caller, which may or may not be origResult (see below).
     */
    private OperationResult readPrimaryAfterGet(
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean secDirtyRead,
        final boolean lockPrimaryOnly,
        final OperationResult origResult) {

        final Database primaryDb = secondaryDb.getPrimary(pKey);
        if (primaryDb == null) {
            /* Primary was removed from the association. */
            return null;
        }

        if (!readPrimaryAfterGet(
            primaryDb, key, pKey, data, lockMode, secDirtyRead,
            lockPrimaryOnly, false /*verifyPrimary*/,
            cursorImpl.getLocker() /*locker*/, secondaryDb, null)) {
            return null;
        }

        if (!secDirtyRead) {
            return origResult;
        }

        /*
         * The expiration time may have changed after the secondary dirty-read
         * and before locking the primary.
         */
        return DbInternal.makeResult(cursorImpl.getExpirationTime());
    }

    /**
     * @see Cursor#checkForPrimaryUpdate
     */
    @Override
    boolean checkForPrimaryUpdate(final DatabaseEntry key,
                                  final DatabaseEntry pKey,
                                  final DatabaseEntry data) {

        final SecondaryConfig conf = secondaryDb.getPrivateSecondaryConfig();
        boolean possibleIntegrityError = false;

        /*
         * If the secondary key is immutable, or the key creators are
         * null (the database is read only), then we can skip this
         * check.
         */
        if (conf.getImmutableSecondaryKey()) {
            /* Do nothing. */
        } else if (conf.getKeyCreator() != null) {

            /*
             * Check that the key we're using is equal to the key
             * returned by the key creator.
             */
            final DatabaseEntry secKey = new DatabaseEntry();
            if (!conf.getKeyCreator().createSecondaryKey
                    (secondaryDb, pKey, data, secKey) ||
                !secKey.equals(key)) {
                possibleIntegrityError = true;
            }
        } else if (conf.getMultiKeyCreator() != null) {

            /*
             * Check that the key we're using is in the set returned by
             * the key creator.
             */
            final Set<DatabaseEntry> results = new HashSet<DatabaseEntry>();
            conf.getMultiKeyCreator().createSecondaryKeys
                (secondaryDb, pKey, data, results);
            if (!results.contains(key)) {
                possibleIntegrityError = true;
            }
        }

        return possibleIntegrityError;
    }
}
