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
 * A JE configuration parameter with an short value.
 */
public class ShortConfigParam extends ConfigParam {
    
    private static final String DEBUG_NAME =
        ShortConfigParam.class.getName();

    private Short min;
    private Short max;

    public ShortConfigParam(String configName,
                     Short minVal,
                     Short maxVal,
                     Short defaultValue,
                     boolean mutable,
                     boolean forReplication) {
        /* defaultValue must not be null. */
        super(configName, defaultValue.toString(), mutable, forReplication);

        min = minVal;
        max = maxVal;
    }

    /**
     * Self validate. Check mins and maxs.
     */
    private void validate(Short value)
        throws IllegalArgumentException {

        if (value != null) {
            if (min != null) {
                if (value.compareTo(min) < 0) {
                    throw new IllegalArgumentException
                        (DEBUG_NAME + ":" +
                         " param " + name +
                         " doesn't validate, " + value +
                         " is less than min of " + min);
                }
            }
            if (max != null) {
                if (value.compareTo(max) > 0) {
                    throw new IllegalArgumentException
                        (DEBUG_NAME + ":" +
                         " param " + name +
                         " doesn't validate, " + value +
                         " is greater than max of " +
                         max);
                }
            }
        }
    }

    @Override
    public void validateValue(String value)
        throws IllegalArgumentException {

        try {
            validate(new Short(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
                (DEBUG_NAME + ": " +  value +
                 " not valid value for " + name);
        }
    }

    public Short getMin() {
        return min;
    }
}
