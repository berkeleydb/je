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

package com.sleepycat.je.rep.utilint;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for each SizeAwaitMap statistics.
 */
public class SizeAwaitMapStatDefinition {

    public static final String GROUP_NAME = "SizeAwaitMap";
    public static final String GROUP_DESC = "SizeAwaitMap statistics";

    public static StatDefinition N_NO_WAITS = 
        new StatDefinition
        ("nNoWaits", 
         "Number of times the map size requirement was met, and the thread " +
         "did not need to wait.");

    public static StatDefinition N_REAL_WAITS = 
        new StatDefinition
        ("nRealWaits", 
         "Number of times the map size was less than the required size, and " +
         "the thread had to wait to reach the map size.");

    public static StatDefinition N_WAIT_TIME = 
        new StatDefinition
        ("nWaitTime", 
         "Totla time (in ms) spent waiting for the map to reach the " +
         "required size.");
}
