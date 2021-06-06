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
import com.sleepycat.je.DatabaseEntry;

/**
 * A ValueAdapter where the "value" is the primary key.
 *
 * @author Mark Hayes
 */
class PrimaryKeyValueAdapter<V> implements ValueAdapter<V> {

    private EntryBinding keyBinding;

    PrimaryKeyValueAdapter(Class<V> keyClass, EntryBinding keyBinding) {
        this.keyBinding = keyBinding;
    }

    public DatabaseEntry initKey() {
        return new DatabaseEntry();
    }

    public DatabaseEntry initPKey() {
        return new DatabaseEntry();
    }

    public DatabaseEntry initData() {
        return BasicIndex.NO_RETURN_ENTRY;
    }

    public void clearEntries(DatabaseEntry key,
                             DatabaseEntry pkey,
                             DatabaseEntry data) {
        key.setData(null);
        pkey.setData(null);
    }

    public V entryToValue(DatabaseEntry key,
                          DatabaseEntry pkey,
                          DatabaseEntry data) {
        return (V) keyBinding.entryToObject(pkey);
    }

    public void valueToData(V value, DatabaseEntry data) {
        throw new UnsupportedOperationException
            ("Cannot change the data in a key-only index");
    }
}
