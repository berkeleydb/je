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

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for JE sequence statistics.
 */
public class SequenceStatDefinition {

    public static final String GROUP_NAME = "Sequence";
    public static final String GROUP_DESC = "Sequence statistics";

    public static final StatDefinition SEQUENCE_GETS =
        new StatDefinition("nGets", 
                           "Number of times that Sequence.get was called " +
                           "successfully.");

    public static final StatDefinition SEQUENCE_CACHED_GETS =
        new StatDefinition("nCachedGets",
                           "Number of times that Sequence.get was called " +
                           "and a cached value was returned.");

    public static final StatDefinition SEQUENCE_STORED_VALUE =
        new StatDefinition("current",
                           "The current value of the sequence in the " +
                           "database.");

    public static final StatDefinition SEQUENCE_CACHE_VALUE =
        new StatDefinition("value",
                           "The current cached value of the sequence.");

    public static final StatDefinition SEQUENCE_CACHE_LAST =
        new StatDefinition("lastValue", 
                           "The last cached value of the sequence.");

    public static final StatDefinition SEQUENCE_RANGE_MIN =
        new StatDefinition("min", 
                           "The minimum permitted value of the sequence.");

    public static final StatDefinition SEQUENCE_RANGE_MAX =
        new StatDefinition("max", 
                           "The maximum permitted value of the sequence.");
    
    public static final StatDefinition SEQUENCE_CACHE_SIZE =
        new StatDefinition("cacheSize", 
                           "The mumber of values that will be cached in " +
                           "this handle.");
}
