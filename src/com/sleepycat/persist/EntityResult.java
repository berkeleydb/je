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

import com.sleepycat.je.OperationResult;

/**
 * Used to return an entity value from a 'get' operation along with an
 * OperationResult. If the operation fails, null is returned. If the operation
 * succeeds and a non-null EntityResult is returned, the contained entity value
 * and OperationResult are guaranteed to be non-null.
 */
public class EntityResult<V> {

    private final V value;
    private final OperationResult result;

    EntityResult(V value, OperationResult result) {
        assert value != null;
        assert result != null;

        this.value = value;
        this.result = result;
    }

    /**
     * Returns the entity value resulting from the operation.
     *
     * @return the non-null entity value.
     */
    public V value() {
        return value;
    }

    /**
     * Returns the OperationResult resulting from the operation.
     *
     * @return the non-null OperationResult.
     */
    public OperationResult result() {
        return result;
    }
}
