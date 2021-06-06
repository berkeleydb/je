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
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Level;

import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.SearchMode;

/**
 * A specialized join cursor for use in performing equality or natural joins on
 * secondary indices.
 *
 * <p>A join cursor is returned when calling {@link Database#join
 * Database.join}.</p>
 *
 * <p>To open a join cursor using two secondary cursors:</p>
 *
 * <pre>
 *     Transaction txn = ...
 *     Database primaryDb = ...
 *     SecondaryDatabase secondaryDb1 = ...
 *     SecondaryDatabase secondaryDb2 = ...
 *     <p>
 *     SecondaryCursor cursor1 = null;
 *     SecondaryCursor cursor2 = null;
 *     JoinCursor joinCursor = null;
 *     try {
 *         DatabaseEntry key = new DatabaseEntry();
 *         DatabaseEntry data = new DatabaseEntry();
 *         <p>
 *         cursor1 = secondaryDb1.openSecondaryCursor(txn, null);
 *         cursor2 = secondaryDb2.openSecondaryCursor(txn, null);
 *         <p>
 *         key.setData(...); // initialize key for secondary index 1
 *         OperationStatus status1 =
 *         cursor1.getSearchKey(key, data, LockMode.DEFAULT);
 *         key.setData(...); // initialize key for secondary index 2
 *         OperationStatus status2 =
 *         cursor2.getSearchKey(key, data, LockMode.DEFAULT);
 *         <p>
 *         if (status1 == OperationStatus.SUCCESS &amp;&amp;
 *                 status2 == OperationStatus.SUCCESS) {
 *             <p>
 *             SecondaryCursor[] cursors = {cursor1, cursor2};
 *             joinCursor = primaryDb.join(cursors, null);
 *             <p>
 *             while (true) {
 *                 OperationStatus joinStatus = joinCursor.getNext(key, data,
 *                     LockMode.DEFAULT);
 *                 if (joinStatus == OperationStatus.SUCCESS) {
 *                      // Do something with the key and data.
 *                 } else {
 *                     break;
 *                 }
 *             }
 *         }
 *     } finally {
 *         if (cursor1 != null) {
 *             cursor1.close();
 *         }
 *         if (cursor2 != null) {
 *             cursor2.close();
 *         }
 *         if (joinCursor != null) {
 *             joinCursor.close();
 *         }
 *     }
 * </pre>
 *
 * <p>The join algorithm is described here so that its cost can be estimated and
 * compared to other approaches for performing a query.  Say that N cursors are
 * provided for the join operation. According to the order they appear in the
 * array the cursors are labeled C(1) through C(n), and the keys at each cursor
 * position are labeled K(1) through K(n).</p>
 *
 * <ol>
 *
 * <li>Using C(1), the join algorithm iterates sequentially through all records
 * having K(1).  This iteration is equivalent to a {@link Cursor#getNextDup
 * Cursor.getNextDup} operation on the secondary index.  The primary key of a
 * candidate record is determined in this manner.  The primary record itself is
 * not retrieved and the primary database is not accessed.</li>
 *
 * <li>For each candidate primary key found in step 1, a Btree lookup is
 * performed using C(2) through C(n), in that order.  The Btree lookups are
 * exact searches to determine whether the candidate record also contains
 * secondary keys K(2) through K(n).  The lookups are equivalent to a {@link
 * Cursor#getSearchBoth Cursor.getSearchBoth} operation on the secondary index.
 * The primary record itself is not retrieved and the primary database is not
 * accessed.</li>
 *
 * <li>If any lookup in step 2 fails, the algorithm advances to the next
 * candidate record using C(1).  Lookups are performed in the order of the
 * cursor array, and the algorithm proceeds to the next C(1) candidate key as
 * soon as a single lookup fails.</li>
 *
 * <li>If all lookups in step 2 succeed, then the matching key and/or data is
 * returned by the {@code getNext} method.  If the {@link
 * #getNext(DatabaseEntry,DatabaseEntry,LockMode)} method signature is used,
 * then the primary database is read to obtain the record data, as if {@link
 * Cursor#getSearchKey Cursor.getSearchKey} were called for the primary
 * database.  If the {@link #getNext(DatabaseEntry,LockMode)} method signature
 * is used, then only the primary key is returned and the primary database is
 * not accessed.</li>
 *
 * <li>The algorithm ends when C(1) has no more candidate records with K(1),
 * and the {@code getNext} method will then return {@link
 * com.sleepycat.je.OperationStatus#NOTFOUND OperationStatus.NOTFOUND}.</li>
 *
 * </ol>
 */
public class JoinCursor implements ForwardCursor, Closeable {

    private JoinConfig config;
    private Database priDb;
    private Cursor[] secCursors;
    private DatabaseEntry[] cursorScratchEntries;
    private DatabaseEntry scratchEntry;
    private DatabaseEntry firstSecKey;
    private boolean[] cursorFetchedFirst;

    /**
     * Creates a join cursor without parameter checking.
     */
    JoinCursor(final Database primaryDb,
               final Cursor[] cursors,
               final JoinConfig configParam)
        throws DatabaseException {

        priDb = primaryDb;
        config = (configParam != null) ? configParam.clone()
                                       : JoinConfig.DEFAULT;
        scratchEntry = new DatabaseEntry();
        firstSecKey = new DatabaseEntry();
        cursorScratchEntries = new DatabaseEntry[cursors.length];
        for (int i = 0; i < cursors.length; i += 1) {
            cursorScratchEntries[i] = new DatabaseEntry();
        }
        cursorFetchedFirst = new boolean[cursors.length];
        Cursor[] sortedCursors = new Cursor[cursors.length];
        System.arraycopy(cursors, 0, sortedCursors, 0, cursors.length);

        if (!config.getNoSort()) {

            /*
             * Sort ascending by duplicate count.  Collect counts before
             * sorting so that countEstimate is called only once per cursor.
             */
            final long[] counts = new long[cursors.length];
            for (int i = 0; i < cursors.length; i += 1) {
                counts[i] = cursors[i].countEstimateInternal();
                assert counts[i] >= 0;
            }
            Arrays.sort(sortedCursors, new Comparator<Cursor>() {
                public int compare(Cursor o1, Cursor o2) {
                    long count1 = -1;
                    long count2 = -1;

                    /*
                     * Scan for objects in cursors not sortedCursors since
                     * sortedCursors is being sorted in place.
                     */
                    for (int i = 0; i < cursors.length &&
                                    (count1 < 0 || count2 < 0); i += 1) {
                        if (cursors[i] == o1) {
                            count1 = counts[i];
                        } else if (cursors[i] == o2) {
                            count2 = counts[i];
                        }
                    }
                    assert count1 >= 0 && count2 >= 0;
                    long cmp = count1 - count2;
                    return (cmp < 0) ? (-1) : ((cmp > 0) ? 1 : 0);
                }
            });
        }

        /*
         * Dup cursors last.  If an error occurs before the constructor is
         * complete, close them and ignore exceptions during close.
         */
        try {
            secCursors = new Cursor[cursors.length];
            for (int i = 0; i < cursors.length; i += 1) {
                secCursors[i] = sortedCursors[i].dup(true);
            }
        } catch (DatabaseException e) {
            close(e); /* will throw e */
        }
    }

    /**
     * Closes the cursors that have been opened by this join cursor.
     *
     * <p>The cursors passed to {@link Database#join Database.join} are not
     * closed by this method, and should be closed by the caller.</p>
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
    public void close()
        throws DatabaseException {

        if (priDb == null) {
            return;
        }
        close(null);
    }

    /**
     * Close all cursors we own, throwing only the first exception that occurs.
     *
     * @param firstException an exception that has already occured, or null.
     */
    private void close(DatabaseException firstException)
        throws DatabaseException {

        priDb = null;
        for (int i = 0; i < secCursors.length; i += 1) {
            if (secCursors[i] != null) {
                try {
                    secCursors[i].close();
                } catch (DatabaseException e) {
                    if (firstException == null) {
                        firstException = e;
                    }
                }
                secCursors[i] = null;
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * For unit testing.
     */
    Cursor[] getSortedCursors() {
        return secCursors;
    }

    /**
     * Returns the primary database handle associated with this cursor.
     *
     * @return the primary database handle associated with this cursor.
     */
    public Database getDatabase() {

        return priDb;
    }

    /**
     * Returns this object's configuration.
     *
     * @return this object's configuration.
     */
    public JoinConfig getConfig() {

        return config.clone();
    }

    /**
     * Returns the next primary key and data resulting from the join operation.
     *
     * @param getType is {@link Get#NEXT}.
     */
    @Override
    public OperationResult get(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Get getType,
        final ReadOptions options) {

        if (getType != Get.NEXT) {
            throw new IllegalArgumentException(
                "Get type not allowed: " + getType);
        }

        final LockMode lockMode =
            (options != null) ? options.getLockMode() : null;

        final CacheMode cacheMode =
            (options != null) ? options.getCacheMode() : null;

        try {
            secCursors[0].checkEnv();
            secCursors[0].trace(Level.FINEST, getType.toString(), lockMode);

            return retrieveNext(key, data, lockMode, cacheMode);

        } catch (Error E) {
            priDb.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * This operation is not allowed on a join cursor. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     */
    @Override
    public OperationStatus getCurrent(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the next primary key resulting from the join operation.
     *
     * <p>An entry is returned by the join cursor for each primary key/data
     * pair having all secondary key values that were specified using the array
     * of secondary cursors passed to {@link Database#join Database.join}.</p>
     *
     * @param key the key returned as
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
    public OperationStatus getNext(final DatabaseEntry key,
                                   final LockMode lockMode) {
        return getNext(key, null, lockMode);
    }

    /**
     * Returns the next primary key and data resulting from the join operation.
     *
     * <p>An entry is returned by the join cursor for each primary key/data
     * pair having all secondary key values that were specified using the array
     * of secondary cursors passed to {@link Database#join Database.join}.</p>
     */
    public OperationStatus getNext(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.NEXT, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Internal version of getNext(), with an optional data param.
     * <p>
     * Since duplicates are always sorted and duplicate-duplicates are not
     * allowed, a natural join can be implemented by simply traversing through
     * the duplicates of the first cursor to find candidate keys, and then
     * looking for each candidate key in the duplicate set of the other
     * cursors, without ever reseting a cursor to the beginning of the
     * duplicate set.
     * <p>
     * This only works when the same duplicate comparison method is used for
     * all cursors.  We don't check for that, we just assume the user won't
     * violate that rule.
     * <p>
     * A future optimization would be to add a SearchMode.BOTH_DUPS operation
     * and use it instead of using SearchMode.BOTH.  This would be the
     * equivalent of the undocumented DB_GET_BOTHC operation used by DB core's
     * join() implementation.
     */
    private OperationResult retrieveNext(final DatabaseEntry keyParam,
                                         final DatabaseEntry dataParam,
                                         final LockMode lockMode,
                                         final CacheMode cacheMode) {
        boolean readUncommitted =
            secCursors[0].isReadUncommittedMode(lockMode);

        outerLoop: while (true) {

            /* Process the first cursor to get a candidate key. */
            Cursor secCursor = secCursors[0];
            DatabaseEntry candidateKey = cursorScratchEntries[0];
            OperationResult result;
            if (!cursorFetchedFirst[0]) {
                /* Get first duplicate at initial cursor position. */
                result = secCursor.getCurrentInternal(
                    firstSecKey, candidateKey, lockMode, cacheMode);
                if (readUncommitted && result == null) {
                    /* Deleted underneath read-uncommitted cursor; skip it. */
                    cursorFetchedFirst[0] = true;
                    continue;
                }
                cursorFetchedFirst[0] = true;
            } else {
                /* Already initialized, move to the next candidate key. */
                result = secCursor.retrieveNext(
                    firstSecKey, candidateKey, lockMode, cacheMode,
                    GetMode.NEXT_DUP);
            }
            if (result == null) {
                /* No more candidate keys. */
                return null;
            }

            /* Process the second and following cursors. */
            for (int i = 1; i < secCursors.length; i += 1) {
                secCursor = secCursors[i];
                DatabaseEntry secKey = cursorScratchEntries[i];
                if (!cursorFetchedFirst[i]) {
                    result = secCursor.getCurrentInternal(
                        secKey, scratchEntry, lockMode, cacheMode);
                    if (readUncommitted &&
                        result == null) {
                        /* Deleted underneath read-uncommitted; skip it. */
                        result = secCursor.retrieveNext(
                            secKey, scratchEntry, lockMode, cacheMode,
                            GetMode.NEXT_DUP);
                        if (result == null) {
                            /* All keys were deleted; no possible match. */
                            return null;
                        }
                    }
                    cursorFetchedFirst[i] = true;
                }
                scratchEntry.setData(secKey.getData(), secKey.getOffset(),
                                     secKey.getSize());
                result = secCursor.search(
                    scratchEntry, candidateKey, lockMode, cacheMode,
                    SearchMode.BOTH, true);
                if (result == null) {
                    /* No match, get another candidate key. */
                    continue outerLoop;
                }
            }

            /* The candidate key was found for all cursors. */
            if (dataParam != null) {
                if (!secCursors[0].readPrimaryAfterGet(
                        priDb, firstSecKey, candidateKey, dataParam, lockMode,
                        readUncommitted, false /*lockPrimaryOnly*/,
                        false /*verifyPrimary*/,
                        secCursors[0].getCursorImpl().getLocker(),
                        secCursors[0].getDatabase(), null)) {
                    /* Deleted underneath read-uncommitted cursor; skip it. */
                    continue;
                }

                /*
                 * Copy primary info to all secondary cursors. The 0th cursor
                 * was updated above with the primary info.
                 */
                final CursorImpl firstSecCursor = secCursors[0].cursorImpl;
                for (int i = 1; i < secCursors.length; i += 1) {
                    secCursors[i].cursorImpl.setPriInfo(firstSecCursor);
                }
            }
            keyParam.setData(candidateKey.getData(), candidateKey.getOffset(),
                             candidateKey.getSize());
            return result;
        }
    }
}
