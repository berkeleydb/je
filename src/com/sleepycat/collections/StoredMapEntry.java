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

package com.sleepycat.collections;

/**
 * @author Mark Hayes
 */
final class StoredMapEntry extends MapEntryParameter {

    private BaseIterator iter;
    private StoredCollection coll;

    StoredMapEntry(Object key,
                   Object value,
                   StoredCollection coll,
                   BaseIterator iter) {

        super(key, value);
        this.coll = coll;
        this.iter = iter;
    }

    public Object setValue(Object newValue) {

        Object oldValue;
        if (iter != null && iter.isCurrentData(this)) {
            oldValue = getValue();
            iter.set(newValue);
        } else {
            if (coll.view.dupsAllowed) {
                throw new IllegalStateException("May not insert duplicates");
            }
            oldValue = coll.putKeyValue(getKey(), newValue);
        }
        setValueInternal(newValue);
        return oldValue;
    }
}
