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

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.je.DatabaseEntry;

/**
 * A ValueAdapter where the "value" is the entity.
 *
 * @author Mark Hayes
 */
class EntityValueAdapter<V> implements ValueAdapter<V> {

    private EntityBinding entityBinding;
    private boolean isSecondary;

    EntityValueAdapter(Class<V> entityClass,
                       EntityBinding entityBinding,
                       boolean isSecondary) {
        this.entityBinding = entityBinding;
        this.isSecondary = isSecondary;
    }

    public DatabaseEntry initKey() {
        return new DatabaseEntry();
    }

    public DatabaseEntry initPKey() {
        return isSecondary ? (new DatabaseEntry()) : null;
    }

    public DatabaseEntry initData() {
        return new DatabaseEntry();
    }

    public void clearEntries(DatabaseEntry key,
                             DatabaseEntry pkey,
                             DatabaseEntry data) {
        key.setData(null);
        if (isSecondary) {
            pkey.setData(null);
        }
        data.setData(null);
    }

    public V entryToValue(DatabaseEntry key,
                          DatabaseEntry pkey,
                          DatabaseEntry data) {
        return (V) entityBinding.entryToObject(isSecondary ? pkey : key, data);
    }

    public void valueToData(V value, DatabaseEntry data) {
        entityBinding.objectToData(value, data);
    }
}
