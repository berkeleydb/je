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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
/* <!-- begin JE only --> */
import com.sleepycat.je.Get;
/* <!-- end JE only --> */
import com.sleepycat.je.LockMode;
/* <!-- begin JE only --> */
import com.sleepycat.je.OperationResult;
/* <!-- end JE only --> */
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
/* <!-- begin JE only --> */
import com.sleepycat.je.WriteOptions;
/* <!-- end JE only --> */
import com.sleepycat.util.keyrange.KeyRange;
import com.sleepycat.util.keyrange.RangeCursor;

/**
 * Implements EntityIndex using a ValueAdapter.  This class is abstract and
 * does not implement get()/map()/sortedMap() because it doesn't have access
 * to the entity binding.
 *
 * @author Mark Hayes
 */
abstract class BasicIndex<K, E> implements EntityIndex<K, E> {

    static final DatabaseEntry NO_RETURN_ENTRY;
    static {
        NO_RETURN_ENTRY = new DatabaseEntry();
        NO_RETURN_ENTRY.setPartial(0, 0, true);
    }

    Database db;
    boolean transactional;
    boolean sortedDups;
    boolean locking;
    boolean concurrentDB;
    Class<K> keyClass;
    EntryBinding keyBinding;
    KeyRange emptyRange;
    ValueAdapter<K> keyAdapter;
    ValueAdapter<E> entityAdapter;

    BasicIndex(Database db,
               Class<K> keyClass,
               EntryBinding keyBinding,
               ValueAdapter<E> entityAdapter)
        throws DatabaseException {

        this.db = db;
        DatabaseConfig config = db.getConfig();
        transactional = config.getTransactional();
        sortedDups = config.getSortedDuplicates();
        locking =
            DbCompat.getInitializeLocking(db.getEnvironment().getConfig());
        Environment env = db.getEnvironment();
        concurrentDB = DbCompat.getInitializeCDB(env.getConfig());
        this.keyClass = keyClass;
        this.keyBinding = keyBinding;
        this.entityAdapter = entityAdapter;

        emptyRange = new KeyRange(config.getBtreeComparator());
        keyAdapter = new KeyValueAdapter(keyClass, keyBinding);
    }

    public Database getDatabase() {
        return db;
    }

    /*
     * Of the EntityIndex methods only get()/map()/sortedMap() are not
     * implemented here and therefore must be implemented by subclasses.
     */

    public boolean contains(K key)
        throws DatabaseException {

        return contains(null, key, null);
    }

    public boolean contains(Transaction txn, K key, LockMode lockMode)
        throws DatabaseException {

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = NO_RETURN_ENTRY;
        keyBinding.objectToEntry(key, keyEntry);

        OperationStatus status = db.get(txn, keyEntry, dataEntry, lockMode);
        return (status == OperationStatus.SUCCESS);
    }

    public long count()
        throws DatabaseException {

        if (DbCompat.DATABASE_COUNT) {
            return DbCompat.getDatabaseCount(db);
        } else {
            long count = 0;
            DatabaseEntry key = NO_RETURN_ENTRY;
            DatabaseEntry data = NO_RETURN_ENTRY;
            CursorConfig cursorConfig = locking ?
                CursorConfig.READ_UNCOMMITTED : null;
            Cursor cursor = db.openCursor(null, cursorConfig);
            try {
                OperationStatus status = cursor.getFirst(key, data, null);
                while (status == OperationStatus.SUCCESS) {
                    if (sortedDups) {
                        count += cursor.count();
                    } else {
                        count += 1;
                    }
                    status = cursor.getNextNoDup(key, data, null);
                }
            } finally {
                cursor.close();
            }
            return count;
        }
    }

    /* <!-- begin JE only --> */

    public long count(long memoryLimit)
        throws DatabaseException {

        return db.count(memoryLimit);
    }

    /* <!-- end JE only --> */

    public boolean delete(K key)
        throws DatabaseException {

        return delete(null, key);
    }

    public boolean delete(Transaction txn, K key)
        throws DatabaseException {

        /* <!-- begin JE only --> */
        if (DbCompat.IS_JE) {
            return delete(txn, key, null) != null;
        }
        /* <!-- end JE only --> */

        DatabaseEntry keyEntry = new DatabaseEntry();
        keyBinding.objectToEntry(key, keyEntry);

        OperationStatus status = db.delete(txn, keyEntry);
        return (status == OperationStatus.SUCCESS);
    }

    /* <!-- begin JE only --> */
    public OperationResult delete(Transaction txn, K key, WriteOptions options)
        throws DatabaseException {

        DatabaseEntry keyEntry = new DatabaseEntry();
        keyBinding.objectToEntry(key, keyEntry);

        return db.delete(txn, keyEntry, options);
    }
    /* <!-- end JE only --> */

    public EntityCursor<K> keys()
        throws DatabaseException {

        return keys(null, null);
    }

    public EntityCursor<K> keys(Transaction txn, CursorConfig config)
        throws DatabaseException {

        return cursor(txn, emptyRange, keyAdapter, config);
    }

    public EntityCursor<E> entities()
        throws DatabaseException {

        return cursor(null, emptyRange, entityAdapter, null);
    }

    public EntityCursor<E> entities(Transaction txn,
                                    CursorConfig config)
        throws DatabaseException {

        return cursor(txn, emptyRange, entityAdapter, config);
    }

    public EntityCursor<K> keys(K fromKey, boolean fromInclusive,
                                K toKey, boolean toInclusive)
        throws DatabaseException {

        return cursor(null, fromKey, fromInclusive, toKey, toInclusive,
                      keyAdapter, null);
    }

    public EntityCursor<K> keys(Transaction txn,
                                K fromKey,
                                boolean fromInclusive,
                                K toKey,
                                boolean toInclusive,
                                CursorConfig config)
        throws DatabaseException {

        return cursor(txn, fromKey, fromInclusive, toKey, toInclusive,
                      keyAdapter, config);
    }

    public EntityCursor<E> entities(K fromKey, boolean fromInclusive,
                                    K toKey, boolean toInclusive)
        throws DatabaseException {

        return cursor(null, fromKey, fromInclusive, toKey, toInclusive,
                      entityAdapter, null);
    }

    public EntityCursor<E> entities(Transaction txn,
                                    K fromKey,
                                    boolean fromInclusive,
                                    K toKey,
                                    boolean toInclusive,
                                    CursorConfig config)
        throws DatabaseException {

        return cursor(txn, fromKey, fromInclusive, toKey, toInclusive,
                      entityAdapter, config);
    }

    private <V> EntityCursor<V> cursor(Transaction txn,
                                       K fromKey,
                                       boolean fromInclusive,
                                       K toKey,
                                       boolean toInclusive,
                                       ValueAdapter<V> adapter,
                                       CursorConfig config)
        throws DatabaseException {

        DatabaseEntry fromEntry = null;
        if (fromKey != null) {
            fromEntry = new DatabaseEntry();
            keyBinding.objectToEntry(fromKey, fromEntry);
        }
        DatabaseEntry toEntry = null;
        if (toKey != null) {
            toEntry = new DatabaseEntry();
            keyBinding.objectToEntry(toKey, toEntry);
        }
        KeyRange range = emptyRange.subRange
            (fromEntry, fromInclusive, toEntry, toInclusive);
        return cursor(txn, range, adapter, config);
    }

    private <V> EntityCursor<V> cursor(Transaction txn,
                                       KeyRange range,
                                       ValueAdapter<V> adapter,
                                       CursorConfig config)
        throws DatabaseException {

        Cursor cursor = db.openCursor(txn, config);
        RangeCursor rangeCursor =
            new RangeCursor(range, null/*pkRange*/, sortedDups, cursor);
        return new BasicCursor<V>(rangeCursor, adapter, isUpdateAllowed());
    }

    abstract boolean isUpdateAllowed();

    /* <!-- begin JE only --> */
    static void checkGetType(Get getType) {

        if (getType != Get.SEARCH) {
            throw new IllegalArgumentException(
                "getType not allowed: " + getType);
        }
    }
    /* <!-- end JE only --> */
}
