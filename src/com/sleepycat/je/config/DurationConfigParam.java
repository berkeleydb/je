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

import com.sleepycat.je.utilint.PropUtil;

/**
 * A JE configuration parameter with a duration integer value in milliseconds.
 * The String format is described under Time Duration Properties in the
 * EnvironmentConfig javadoc.
 */
public class DurationConfigParam extends ConfigParam {

    private static final String DEBUG_NAME =
        DurationConfigParam.class.getName();

    private String minString;
    private int minMillis;
    private String maxString;
    private int maxMillis;

    public DurationConfigParam(String configName,
                               String minVal,
                               String maxVal,
                               String defaultValue,
                               boolean mutable,
                               boolean forReplication) {
        super(configName, defaultValue, mutable, forReplication);
        if (minVal != null) {
            minString = minVal;
            minMillis = PropUtil.parseDuration(minVal);
        }
        if (maxVal != null) {
            maxString = maxVal;
            maxMillis = PropUtil.parseDuration(maxVal);
        }
    }

    @Override
    public void validateValue(String value)
        throws IllegalArgumentException {

        final int millis;
        try {
            /* Parse for validation side-effects. */
            millis = PropUtil.parseDuration(value);
        } catch (IllegalArgumentException e) {
            /* Identify this property in the exception message. */
            throw new IllegalArgumentException
                (DEBUG_NAME + ":" +
                 " param " + name +
                 " doesn't validate, " +
                 value +
                 " fails validation: " + e.getMessage());
        }
        /* Check min/max. */
        if (minString != null) {
            if (millis < minMillis) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ":" +
                     " param " + name +
                     " doesn't validate, " +
                     value +
                     " is less than min of "+
                     minString);
            }
        }
        if (maxString != null) {
            if (millis > maxMillis) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ":" +
                     " param " + name +
                     " doesn't validate, " +
                     value +
                     " is greater than max of " +
                     maxString);
            }
        }
    }
}
