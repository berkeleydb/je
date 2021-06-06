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

package com.sleepycat.je.config;

/**
 * A JE configuration parameter with an boolean value.
 */
public class BooleanConfigParam extends ConfigParam {

    private static final String DEBUG_NAME =
        BooleanConfigParam.class.getName();

    /**
     * Set a boolean parameter w/default.
     * @param configName
     * @param defaultValue
     * @param forReplication true if param is for replication
     */
    public BooleanConfigParam(String configName,
                              boolean defaultValue,
                              boolean mutable,
                              boolean forReplication) {
        /* defaultValue must not be null. */
        super(configName,
              Boolean.valueOf(defaultValue).toString(),
              mutable,
              forReplication);
    }

    /**
     * Make sure that value is a valid string for booleans.
     */
    @Override
    public void validateValue(String value)
        throws IllegalArgumentException {

        if (!value.trim().equalsIgnoreCase(Boolean.FALSE.toString()) &&
            !value.trim().equalsIgnoreCase(Boolean.TRUE.toString())) {
            throw new IllegalArgumentException
                (DEBUG_NAME + ": " +  value + " not valid boolean " + name);
        }
    }
}
