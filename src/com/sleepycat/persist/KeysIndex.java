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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
/* <!-- begin JE only --> */
import com.sleepycat.je.DbInternal;
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
import com.sleepycat.je.Transaction;

/**
 * The EntityIndex returned by SecondaryIndex.keysIndex().  This index maps
 * secondary key to primary key.  In Berkeley DB internal terms, this is a
 * secondary database that is opened without associating it with a primary.
 *
 * @author Mark Hayes
 */
class KeysIndex<SK, PK> extends BasicIndex<SK, PK> {

    private EntryBinding pkeyBinding;
    private SortedMap<SK, PK> map;

    KeysIndex(Database db,
              Class<SK> keyClass,
              EntryBinding keyBinding,
              Class<PK> pkeyClass,
              EntryBinding pkeyBinding)
        throws DatabaseException {

        super(db, keyClass, keyBinding,
              new DataValueAdapter<PK>(pkeyClass, pkeyBinding));
        this.pkeyBinding = pkeyBinding;
    }

    /*
     * Of the EntityIndex methods only get()/map()/sortedMap() are implemented
     * here.  All other methods are implemented by BasicIndex.
     */

    public PK get(SK key)
        throws DatabaseException {

        return get(null, key, null);
    }

    public PK get(Transaction txn, SK key, LockMode lockMode)
        throws DatabaseException {

        /* <!-- begin JE only --> */
        if (DbCompat.IS_JE) {
            EntityResult<PK> result = get(
                txn, key, Get.SEARCH, DbInternal.getReadOptions(lockMode));
            return result != null ? result.value() : null;
        }
        /* <!-- end JE only --> */

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry pkeyEntry = new DatabaseEntry();
        keyBinding.objectToEntry(key, keyEntry);

        OperationStatus status = db.get(txn, keyEntry, pkeyEntry, lockMode);

        if (status == OperationStatus.SUCCESS) {
            return (PK) pkeyBinding.entryToObject(pkeyEntry);
        } else {
            return null;
        }
    }

    /* <!-- begin JE only --> */
    public EntityResult<PK> get(Transaction txn,
                                SK key,
                                Get getType,
                                ReadOptions options)
        throws DatabaseException {

        checkGetType(getType);

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry pkeyEntry = new DatabaseEntry();
        keyBinding.objectToEntry(key, keyEntry);

        OperationResult result = db.get(
            txn, keyEntry, pkeyEntry, getType, options);

        if (result != null) {
            return new EntityResult<>(
                (PK) pkeyBinding.entryToObject(pkeyEntry),
                result);
        } else {
            return null;
        }
    }
    /* <!-- end JE only --> */

    public Map<SK, PK> map() {
        return sortedMap();
    }

    public synchronized SortedMap<SK, PK> sortedMap() {
        if (map == null) {
            map = new StoredSortedMap(db, keyBinding, pkeyBinding, false);
        }
        return map;
    }

    boolean isUpdateAllowed() {
        return false;
    }
}
