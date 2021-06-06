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

/**
 * The result of an operation that successfully reads or writes a record.
 * <p>
 * An OperationResult does not contain any failure information. Methods that
 * perform unsuccessful reads or writes return null or throw an exception. Null
 * is returned if the operation failed for commonly expected reasons, such as a
 * read that fails because the key does not exist, or an insertion that fails
 * because the key does exist.
 * <p>
 * Methods that return OperationResult can be compared to methods that return
 * {@link OperationStatus} as follows: If {@link OperationStatus#SUCCESS} is
 * returned by the latter methods, this is equivalent to returning a non-null
 * OperationResult by the former methods.
 *
 * @since 7.0
 */
public class OperationResult {

    private final long expirationTime;
    private final boolean update;

    OperationResult(final long expirationTime, final boolean update) {
        this.expirationTime = expirationTime;
        this.update = update;
    }

    /**
     * Returns whether the operation was an update, for distinguishing inserts
     * and updates performed by a {@link Put#OVERWRITE} operation.
     *
     * @return whether an existing record was updated by this operation.
     */
    public boolean isUpdate() {
        return update;
    }

    /**
     * Returns the expiration time of the record, in milliseconds, or zero
     * if the record has no TTL and does not expire.
     * <p>
     * For 'get' operations, this is the expiration time of the current record.
     * For 'put operations, this is the expiration time of the newly written
     * record. For 'delete' operation, this is the expiration time of the
     * record that was deleted.
     * <p>
     * The return value will always be evenly divisible by the number of
     * milliseconds in one hour. If {@code TimeUnit.Days} was specified
     * when the record was written, the return value will also be evenly
     * divisible by the number of milliseconds in one day.
     *
     * @return the expiration time in milliseconds, or zero.
     */
    public long getExpirationTime() {
        return expirationTime;
    }
}
