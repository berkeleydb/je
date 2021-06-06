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

import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.compat.DbCompat;
import com.sleepycat.compat.DbCompat.OpResult;
import com.sleepycat.compat.DbCompat.OpWriteOptions;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
/* <!-- begin JE only --> */
import com.sleepycat.je.DbInternal;
/* <!-- end JE only --> */
import com.sleepycat.je.Environment;
/* <!-- begin JE only --> */
import com.sleepycat.je.Get;
/* <!-- end JE only --> */
import com.sleepycat.je.LockMode;
/* <!-- begin JE only --> */
import com.sleepycat.je.OperationResult;
/* <!-- end JE only --> */
import com.sleepycat.je.OperationStatus;
/* <!-- begin JE only --> */
import com.sleepycat.je.ReadOptions;
/* <!-- end JE only --> */
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
/* <!-- begin JE only --> */
import com.sleepycat.je.WriteOptions;
/* <!-- end JE only --> */
import com.sleepycat.util.keyrange.KeyRange;
import com.sleepycat.util.keyrange.RangeCursor;

/**
 * The EntityIndex returned by SecondaryIndex.subIndex.  A SubIndex, in JE
 * internal terms, is a duplicates btree for a single key in the main btree.
 * From the user's viewpoint, the keys are primary keys.  This class implements
 * that viewpoint.  In general, getSearchBoth and getSearchBothRange are used
 * where in a normal index getSearchKey and getSearchRange would be used.  The
 * main tree key is always implied, not passed as a parameter.
 *
 * @author Mark Hayes
 */
class SubIndex<PK, E> implements EntityIndex<PK, E> {

    private SecondaryIndex<?,PK,E> secIndex;
    private SecondaryDatabase db;
    private boolean transactional;
    private boolean sortedDups;
    private boolean locking;
    private boolean concurrentDB;
    private DatabaseEntry keyEntry;
    private Object keyObject;
    private KeyRange singleKeyRange;
    private EntryBinding pkeyBinding;
    private KeyRange emptyPKeyRange;
    private EntityBinding entityBinding;
    private ValueAdapter<PK> keyAdapter;
    private ValueAdapter<E> entityAdapter;
    private SortedMap<PK, E> map;

    <SK> SubIndex(SecondaryIndex<SK, PK, E> secIndex,
                  EntityBinding entityBinding,
                  SK key)
        throws DatabaseException {

        this.secIndex = secIndex;
        db = secIndex.getDatabase();
        transactional = secIndex.transactional;
        sortedDups = secIndex.sortedDups;
        locking =
            DbCompat.getInitializeLocking(db.getEnvironment().getConfig());
        Environment env = db.getEnvironment();
        concurrentDB = DbCompat.getInitializeCDB(env.getConfig());
        keyObject = key;
        keyEntry = new DatabaseEntry();
        secIndex.keyBinding.objectToEntry(key, keyEntry);
        singleKeyRange = secIndex.emptyRange.subRange(keyEntry);

        PrimaryIndex<PK, E> priIndex = secIndex.getPrimaryIndex();
        pkeyBinding = priIndex.keyBinding;
        emptyPKeyRange = priIndex.emptyRange;
        this.entityBinding = entityBinding;

        keyAdapter = new PrimaryKeyValueAdapter<PK>
            (priIndex.keyClass, priIndex.keyBinding);
        entityAdapter = secIndex.entityAdapter;
    }

    public Database getDatabase() {
        return db;
    }

    public boolean contains(PK key)
        throws DatabaseException {

        return contains(null, key, null);
    }

    public boolean contains(Transaction txn, PK key, LockMode lockMode)
        throws DatabaseException {

        DatabaseEntry pkeyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = BasicIndex.NO_RETURN_ENTRY;
        pkeyBinding.objectToEntry(key, pkeyEntry);

        OperationStatus status =
            db.getSearchBoth(txn, keyEntry, pkeyEntry, dataEntry, lockMode);
        return (status == OperationStatus.SUCCESS);
    }

    public E get(PK key)
        throws DatabaseException {

        return get(null, key, null);
    }

    public E get(Transaction txn, PK key, LockMode lockMode)
        throws DatabaseException {

        /* <!-- begin JE only --> */
        if (DbCompat.IS_JE) {
            EntityResult<E> result = get(
                txn, key, Get.SEARCH, DbInternal.getReadOptions(lockMode));
            return result != null ? result.value() : null;
        }
        /* <!-- end JE only --> */

        DatabaseEntry pkeyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        pkeyBinding.objectToEntry(key, pkeyEntry);

        OperationStatus status =
            db.getSearchBoth(txn, keyEntry, pkeyEntry, dataEntry, lockMode);

        if (status == OperationStatus.SUCCESS) {
            return (E) entityBinding.entryToObject(pkeyEntry, dataEntry);
        } else {
            return null;
        }
    }

    /* <!-- begin JE only --> */
    public EntityResult<E> get(Transaction txn,
                               PK key,
                               Get getType,
                               ReadOptions options)
        throws DatabaseException {

        BasicIndex.checkGetType(getType);

        DatabaseEntry pkeyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        pkeyBinding.objectToEntry(key, pkeyEntry);

        OperationResult result = db.get(
            txn, keyEntry, pkeyEntry, dataEntry, Get.SEARCH_BOTH, options);

        if (result != null) {
            return new EntityResult<>(
                (E) entityBinding.entryToObject(pkeyEntry, dataEntry),
                result);
        } else {
            return null;
        }
    }
    /* <!-- end JE only --> */

    public long count()
        throws DatabaseException {

        CursorConfig cursorConfig = locking ?
            CursorConfig.READ_UNCOMMITTED : null;
        EntityCursor<PK> cursor = keys(null, cursorConfig);
        try {
            if (cursor.next() != null) {
                return cursor.count();
            } else {
                return 0;
            }
        } finally {
            cursor.close();
        }
    }

    /* <!-- begin JE only --> */

    public long count(long memoryLimit)
        throws DatabaseException {

        return count();
    }

    /* <!-- end JE only --> */

    public boolean delete(PK key)
        throws DatabaseException {

        return delete(null, key);
    }

    public boolean delete(Transaction txn, PK key)
        throws DatabaseException {

        return deleteInternal(txn, key, OpWriteOptions.EMPTY).isSuccess();
    }

    /* <!-- begin JE only --> */
    public OperationResult delete(Transaction txn,
                                  PK key,
                                  WriteOptions options)
        throws DatabaseException {

        return deleteInternal(txn, key, OpWriteOptions.make(options)).jeResult;
    }
    /* <!-- end JE only --> */

    private OpResult deleteInternal(Transaction txn,
                                    PK key,
                                    OpWriteOptions options)
        throws DatabaseException {

        DatabaseEntry pkeyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = BasicIndex.NO_RETURN_ENTRY;
        pkeyBinding.objectToEntry(key, pkeyEntry);

        boolean autoCommit = false;
        Environment env = db.getEnvironment();
        if (transactional &&
            txn == null &&
            DbCompat.getThreadTransaction(env) == null) {
            txn = env.beginTransaction
                (null, secIndex.getAutoCommitTransactionConfig());
            autoCommit = true;
        }

        boolean failed = true;
        CursorConfig cursorConfig = null;
        if (concurrentDB) {
            cursorConfig = new CursorConfig();
            DbCompat.setWriteCursor(cursorConfig, true);
        } 
        SecondaryCursor cursor = db.openSecondaryCursor(txn, cursorConfig);
        try {
            /* <!-- begin JE only --> */
            if (DbCompat.IS_JE) {
                ReadOptions readOptions;
                if (options.jeOptions != null &&
                    options.jeOptions.getCacheMode() != null) {
                    readOptions = new ReadOptions();
                    readOptions.setLockMode(LockMode.RMW);
                    readOptions.setCacheMode(options.jeOptions.getCacheMode());
                } else {
                    readOptions = LockMode.RMW.toReadOptions();
                }
                OperationResult result = cursor.get(
                    keyEntry, pkeyEntry, dataEntry, Get.SEARCH_BOTH,
                    readOptions);
                if (result != null) {
                    result = cursor.delete(options.jeOptions);
                }
                failed = false;
                return OpResult.make(result);
            }
            /* <!-- end JE only --> */
            OperationStatus status = cursor.getSearchBoth
                (keyEntry, pkeyEntry, dataEntry,
                 locking ? LockMode.RMW : null);
            if (status == OperationStatus.SUCCESS) {
                status = cursor.delete();
            }
            failed = false;
            return OpResult.make(status);
        } finally {
            cursor.close();
            if (autoCommit) {
                if (failed) {
                    txn.abort();
                } else {
                    txn.commit();
                }
            }
        }
    }

    public EntityCursor<PK> keys()
        throws DatabaseException {

        return keys(null, null);
    }

    public EntityCursor<PK> keys(Transaction txn, CursorConfig config)
        throws DatabaseException {

        return cursor(txn, null, keyAdapter, config);
    }

    public EntityCursor<E> entities()
        throws DatabaseException {

        return cursor(null, null, entityAdapter, null);
    }

    public EntityCursor<E> entities(Transaction txn,
                                    CursorConfig config)
        throws DatabaseException {

        return cursor(txn, null, entityAdapter, config);
    }

    public EntityCursor<PK> keys(PK fromKey,
                                 boolean fromInclusive,
                                 PK toKey,
                                 boolean toInclusive)
        throws DatabaseException {

        return cursor(null, fromKey, fromInclusive, toKey, toInclusive,
                      keyAdapter, null);
    }

    public EntityCursor<PK> keys(Transaction txn,
                                 PK fromKey,
                                 boolean fromInclusive,
                                 PK toKey,
                                 boolean toInclusive,
                                 CursorConfig config)
        throws DatabaseException {

        return cursor(txn, fromKey, fromInclusive, toKey, toInclusive,
                      keyAdapter, config);
    }

    public EntityCursor<E> entities(PK fromKey,
                                    boolean fromInclusive,
                                    PK toKey,
                                    boolean toInclusive)
        throws DatabaseException {

        return cursor(null, fromKey, fromInclusive, toKey, toInclusive,
                      entityAdapter, null);
    }

    public EntityCursor<E> entities(Transaction txn,
                                    PK fromKey,
                                    boolean fromInclusive,
                                    PK toKey,
                                    boolean toInclusive,
                                    CursorConfig config)
        throws DatabaseException {

        return cursor(txn, fromKey, fromInclusive, toKey, toInclusive,
                      entityAdapter, config);
    }

    private <V> EntityCursor<V> cursor(Transaction txn,
                                       PK fromKey,
                                       boolean fromInclusive,
                                       PK toKey,
                                       boolean toInclusive,
                                       ValueAdapter<V> adapter,
                                       CursorConfig config)
        throws DatabaseException {

        DatabaseEntry fromEntry = null;
        if (fromKey != null) {
            fromEntry = new DatabaseEntry();
            pkeyBinding.objectToEntry(fromKey, fromEntry);
        }
        DatabaseEntry toEntry = null;
        if (toKey != null) {
            toEntry = new DatabaseEntry();
            pkeyBinding.objectToEntry(toKey, toEntry);
        }
        KeyRange pkeyRange = emptyPKeyRange.subRange
            (fromEntry, fromInclusive, toEntry, toInclusive);
        return cursor(txn, pkeyRange, adapter, config);
    }

    private <V> EntityCursor<V> cursor(Transaction txn,
                                       KeyRange pkeyRange,
                                       ValueAdapter<V> adapter,
                                       CursorConfig config)
        throws DatabaseException {

        Cursor cursor = db.openCursor(txn, config);
        RangeCursor rangeCursor =
            new RangeCursor(singleKeyRange, pkeyRange, sortedDups, cursor);
        return new SubIndexCursor<V>(rangeCursor, adapter);
    }

    public Map<PK, E> map() {
        return sortedMap();
    }

    public synchronized SortedMap<PK, E> sortedMap() {
        if (map == null) {
            map = (SortedMap) ((StoredSortedMap) secIndex.sortedMap()).
                duplicatesMap(keyObject, pkeyBinding);
        }
        return map;
    }
}
