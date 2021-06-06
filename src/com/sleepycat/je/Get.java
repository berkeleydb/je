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

import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.SearchMode;

/**
 * The operation type passed to "get" methods on databases and cursors.
 */
public enum Get {

    /**
     * Searches using an exact match by key.
     *
     * <p>Returns, or moves the cursor to, the record having a key exactly
     * matching the given key parameter.</p>
     *
     * <p>If the database has duplicate keys, the record with the matching key
     * and lowest data value (or the lowest primary key, for secondary
     * databases) is selected.</p>
     *
     * <p>The operation does not succeed if no record matches.</p>
     */
    SEARCH(SearchMode.SET),

    /**
     * Searches using an exact match by key and data (or pKey).
     *
     * <p>Returns, or moves the cursor to, the record having a key exactly
     * matching the given key parameter, and having a data value (or primary
     * key) exactly matching the given data (or pKey) parameter. The data is
     * matched for Database and Cursor operations, while the primary key is
     * matched for SecondaryDatabase and SecondaryCursor operations.</p>
     *
     * <p>If the database has duplicate keys, the search is performed by key
     * and data (or pKey) using the database Btree. If the database has does
     * not have duplicate keys, the search is performed by key alone using the
     * Btree, and then the data (or primary key) of the matching record is
     * simply compared to the data (pKey) parameter. In other words, using
     * this operation has no performance advantage over {@link #SEARCH} when
     * the database does not have duplicates.</p>
     *
     * <p>The operation does not succeed (null is returned) if no record
     * matches.</p>
     */
    SEARCH_BOTH(SearchMode.BOTH),

    /**
     * Searches using a GTE match by key.
     *
     * <p>Returns, or moves the cursor to, the record with a key that is
     * greater than or equal to (GTE) the given key parameter.</p>
     *
     * <p>If the database has duplicate keys, the record with the lowest data
     * value (or the lowest primary key, for a secondary database) is selected
     * among the duplicates with the matching key.</p>
     *
     * <p>The operation does not succeed (null is returned) if no record
     * matches.</p>
     */
    SEARCH_GTE(SearchMode.SET_RANGE),

    /**
     * Searches using an exact match by key and a GTE match by data (or pKey).
     *
     * <p>Returns, or moves the cursor to, the record with a key exactly
     * matching the given key parameter, and having a data value (or primary
     * key) that is greater than or equal to (GTE) the given data (or pKey)
     * parameter. The data is matched for Database and Cursor operations, while
     * the primary key is matched for SecondaryDatabase and SecondaryCursor
     * operations.</p>
     *
     * <p>If the database does not have duplicate keys, the data (or pKey) is
     * matched exactly and this operation is equivalent to {@link
     * #SEARCH_BOTH}.</p>
     *
     * <p>The operation does not succeed (null is returned) if no record
     * matches.</p>
     */
    SEARCH_BOTH_GTE(SearchMode.BOTH_RANGE),

    /**
     * Accesses the current record.
     *
     * <p>Accesses the record at the current cursor position. If the cursor is
     * uninitialized (not positioned on a record), {@link
     * IllegalStateException} is thrown.</p>
     *
     * <p>The operation does not succeed (null is returned) if the record at
     * the current position has been deleted. This can occur in two cases: 1.
     * If the record was deleted using this cursor and then accessed. 2. If the
     * record was not locked by this cursor or transaction, and was deleted by
     * another thread or transaction after this cursor was positioned on
     * it.</p>
     */
    CURRENT(),

    /**
     * Finds the first record in the database.
     *
     * <p>Moves the cursor to the record in the database with the lowest valued
     * key.</p>
     *
     * <p>If the database has duplicate keys, the record with the lowest data
     * value (or the lowest primary key, for a secondary database) is selected
     * among the duplicates for the lowest key.</p>
     *
     * <p>The operation does not succeed (null is returned) if the database is
     * empty.</p>
     */
    FIRST(),

    /**
     * Finds the last record in the database.
     *
     * <p>Moves the cursor to the record in the database with the highest
     * valued key.</p>
     *
     * <p>If the database has duplicate keys, the record with the highest data
     * value (or the highest primary key, for a secondary database) is selected
     * among the duplicates for the highest key.</p>
     *
     * <p>The operation does not succeed (null is returned) if the database is
     * empty.</p>
     */
    LAST(),

    /**
     * Moves to the next record.
     *
     * <p>Moves the cursor to the record following the record at the current
     * cursor position. If the cursor is uninitialized (not positioned on a
     * record), moves to the first record and this operation is equivalent to
     * {@link #FIRST}.</p>
     *
     * <p>If the database does not have duplicate keys, the following record is
     * defined as the record with the next highest key. If the database does
     * have duplicate keys, the following record is defined as the record with
     * the same key and the next highest data value (or the next highest
     * primary key, for a secondary database) among the duplicates for that
     * key; or if there are no more records with the same key, the following
     * record is the record with the next highest key and the lowest data value
     * (or the lowest primary key, for a secondary database) among the
     * duplicates for that key.</p>
     *
     * <p>The operation does not succeed (null is returned) if the record at
     * the cursor position is the last record in the database.</p>
     */
    NEXT(GetMode.NEXT, true /*allowNexPrevUninitialized*/),

    /**
     * Moves to the next record with the same key.
     *
     * <p>Moves the cursor to the record following the record at the current
     * cursor position and having the same key. If the cursor is uninitialized
     * (not positioned on a record), {@link IllegalStateException} is
     * thrown.</p>
     *
     * <p>If the database has duplicate keys, moves to the record with the same
     * key and the next highest data value (or the next highest primary key,
     * for a secondary database) among the duplicates for that key.</p>
     *
     * <p>The operation does not succeed (null is returned) if there are no
     * following records with the same key. This is always the case when
     * database does not have duplicate keys.</p>
     */
    NEXT_DUP(GetMode.NEXT_DUP, false /*allowNexPrevUninitialized*/),

    /**
     * Moves to the next record with a different key.
     *
     * <p>Moves the cursor to the record following the record at the current
     * cursor position and having the next highest key. If the cursor is
     * uninitialized (not positioned on a record), moves to the first record
     * and this operation is equivalent to {@link #FIRST}.</p>
     *
     * <p>If the database has duplicate keys, moves to the record with the next
     * highest key and the lowest data value (or the lowest primary key, for a
     * secondary database) among the duplicates for that key; this effectively
     * skips over records having the same key and a higher data value (or a
     * higher primary key, for a secondary database). If the database does not
     * have duplicate keys, this operation is equivalent to {@link #NEXT}.</p>
     *
     * <p>The operation does not succeed (null is returned) if there are no
     * following records with a different key.</p>
     */
    NEXT_NO_DUP(GetMode.NEXT_NODUP, true /*allowNexPrevUninitialized*/),

    /**
     * Moves to the previous record.
     *
     * <p>Moves the cursor to the record preceding the record at the current
     * cursor position. If the cursor is uninitialized (not positioned on a
     * record), moves to the last record and this operation is equivalent to
     * {@link #LAST}.</p>
     *
     * <p>If the database does not have duplicate keys, the preceding record is
     * defined as the record with the next lowest key. If the database does
     * have duplicate keys, the preceding record is defined as the record with
     * the same key and the next lowest data value (or the next lowest primary
     * key, for a secondary database) among the duplicates for that key; or if
     * there are no preceding records with the same key, the preceding record
     * is the record with the next lowest key and the highest data value (or
     * the highest primary key, for a secondary database) among the duplicates
     * for that key.</p>
     *
     * <p>The operation does not succeed (null is returned) if the record at
     * the cursor position is the first record in the database.</p>
     */
    PREV(GetMode.PREV, true /*allowNexPrevUninitialized*/),

    /**
     * Moves to the previous record with the same key.
     *
     * <p>Moves the cursor to the record preceding the record at the current
     * cursor position and having the same key. If the cursor is uninitialized
     * (not positioned on a record), {@link IllegalStateException} is
     * thrown.</p>
     *
     * <p>If the database has duplicate keys, moves to the record with the same
     * key and the next lowest data value (or the next lowest primary key, for
     * a secondary database) among the duplicates for that key.</p>
     *
     * <p>The operation does not succeed (null is returned) if there are no
     * preceding records with the same key. This is always the case when
     * database does not have duplicate keys.</p>
     */
    PREV_DUP(GetMode.PREV_DUP, false /*allowNexPrevUninitialized*/),

    /**
     * Moves to the previous record with a different key.
     *
     * <p>Moves the cursor to the record preceding the record at the current
     * cursor position and having the next lowest key. If the cursor is
     * uninitialized (not positioned on a record), moves to the last record
     * and this operation is equivalent to {@link #LAST}.</p>
     *
     * <p>If the database has duplicate keys, moves to the record with the next
     * lowest key and the highest data value (or the highest primary key, for a
     * secondary database) among the duplicates for that key; this effectively
     * skips over records having the same key and a lower data value (or a
     * lower primary key, for a secondary database). If the database does not
     * have duplicate keys, this operation is equivalent to {@link #PREV}.</p>
     *
     * <p>The operation does not succeed (null is returned) if there are no
     * preceding records with a different key.</p>
     */
    PREV_NO_DUP(GetMode.PREV_NODUP, true /*allowNexPrevUninitialized*/);

    private final SearchMode searchMode;
    private final GetMode getMode;
    private final boolean allowNexPrevUninitialized;

    Get() {
        this(null, null, false);
    }

    Get(final SearchMode searchMode) {
        this(searchMode, null, false);
    }

    Get(final GetMode getMode, final boolean allowNexPrevUninitialized) {
        this(null, getMode, allowNexPrevUninitialized);
    }

    Get(final SearchMode searchMode,
        final GetMode getMode,
        final boolean allowNexPrevUninitialized) {

        this.searchMode = searchMode;
        this.getMode = getMode;
        this.allowNexPrevUninitialized = allowNexPrevUninitialized;
    }

    SearchMode getSearchMode() {
        return searchMode;
    }

    GetMode getGetMode() {
        return getMode;
    }

    boolean getAllowNextPrevUninitialized() {
        return allowNexPrevUninitialized;
    }
}
