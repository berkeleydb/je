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

package com.sleepycat.je.dbi;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.WriteOptions;

/**
 * A struct for passing record expiration info to a 'put' operation, and
 * returning the old expiration time plus whether it was updated/changed.
 */
public class ExpirationInfo {

    public static final ExpirationInfo DEFAULT =
        new ExpirationInfo(0, false, false);

    public final int expiration;
    public final boolean expirationInHours;
    public final boolean updateExpiration;
    private boolean expirationUpdated = false;
    private long oldExpirationTime = 0;

    public ExpirationInfo(
        final int expiration,
        final boolean expirationInHours,
        final boolean updateExpiration) {

        this.expiration = expiration;
        this.expirationInHours = expirationInHours;
        this.updateExpiration = updateExpiration;
    }

    /**
     * Creates an ExpirationInfo struct from the WriteOptions TTL params, for
     * the current system time.
     *
     * @param options WriteOptions, may not be null.
     *
     * @return ExpirationInfo, or null if WriteOptions.getTTL is zero and
     * WriteOptions.getUpdateTTL is false, meaning we will not add or update
     * the TTL.
     */
    public static ExpirationInfo getInfo(final WriteOptions options) {

        if (options.getTTL() == 0 && !options.getUpdateTTL()) {
            return null;
        }

        return new ExpirationInfo(
            TTL.ttlToExpiration(options.getTTL(), options.getTTLUnit()),
            options.getTTLUnit() == TimeUnit.HOURS,
            options.getUpdateTTL());
    }

    public void setExpirationUpdated(boolean val) {
        expirationUpdated = val;
    }

    public boolean getExpirationUpdated() {
        return expirationUpdated;
    }

    public void setOldExpirationTime(long val) {
        oldExpirationTime = val;
    }

    public long getOldExpirationTime() {
        return oldExpirationTime;
    }
}
