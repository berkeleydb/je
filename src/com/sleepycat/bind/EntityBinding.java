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

package com.sleepycat.bind;

import com.sleepycat.je.DatabaseEntry;

/**
 * A binding between a key-value entry pair and an entity object.
 *
 * <p><em>WARNING:</em> Binding instances are typically shared by multiple
 * threads and binding methods are called without any special synchronization.
 * Therefore, bindings must be thread safe.  In general no shared state should
 * be used and any caching of computed values must be done with proper
 * synchronization.</p>
 *
 * @author Mark Hayes
 */
public interface EntityBinding<E> {

    /**
     * Converts key and data entry buffers into an entity Object.
     *
     * @param key is the source key entry.
     *
     * @param data is the source data entry.
     *
     * @return the resulting Object.
     */
    E entryToObject(DatabaseEntry key, DatabaseEntry data);

    /**
     * Extracts the key entry from an entity Object.
     *
     * @param object is the source Object.
     *
     * @param key is the destination entry buffer.
     */
    void objectToKey(E object, DatabaseEntry key);

    /**
     * Extracts the data entry from an entity Object.
     *
     * @param object is the source Object.
     *
     * @param data is the destination entry buffer.
     */
    void objectToData(E object, DatabaseEntry data);
}
