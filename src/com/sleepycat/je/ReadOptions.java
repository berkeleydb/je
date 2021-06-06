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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedException;

import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * Options for calling methods that read records.
 *
 * @since 7.0
 */
public class ReadOptions implements Cloneable {

    private CacheMode cacheMode = null;
    private LockMode lockMode = LockMode.DEFAULT;

    /**
     * Constructs a ReadOptions object with default values for all properties.
     */
    public ReadOptions() {
    }

    @Override
    public ReadOptions clone() {
        try {
            return (ReadOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw unexpectedException(e);
        }
    }

    /**
     * Sets the {@code CacheMode} to be used for the operation.
     * <p>
     * By default this property is null, meaning that the default specified
     * using {@link Cursor#setCacheMode},
     * {@link DatabaseConfig#setCacheMode} or
     * {@link EnvironmentConfig#setCacheMode} will be used.
     *
     * @param cacheMode is the {@code CacheMode} used for the operation, or
     * null to use the Cursor, Database or Environment default.
     *
     * @return 'this'.
     */
    public ReadOptions setCacheMode(final CacheMode cacheMode) {
        this.cacheMode = cacheMode;
        return this;
    }

    /**
     * Returns the {@code CacheMode} to be used for the operation, or null
     * if the Cursor, Database or Environment default will be used.
     *
     * @see #setCacheMode(CacheMode)
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the {@code LockMode} to be used for the operation.
     * <p>
     * By default this property is {@link LockMode#DEFAULT}.
     *
     * @param lockMode the locking attributes. Specifying null or
     * {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return 'this'.
     */
    public ReadOptions setLockMode(final LockMode lockMode) {
        DatabaseUtil.checkForNullParam(lockMode, "lockMode");
        this.lockMode = lockMode;
        return this;
    }

    /**
     * Returns the {@code LockMode} to be used for the operation.
     *
     * @see #setLockMode(LockMode)
     */
    public LockMode getLockMode() {
        return lockMode;
    }
}
