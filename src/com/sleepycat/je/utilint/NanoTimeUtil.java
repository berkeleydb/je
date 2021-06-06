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

package com.sleepycat.je.utilint;


/**
 * Utility class for dealing with special cases of System.nanoTime
 */
public class NanoTimeUtil {

    /**
     * Special compare function for comparing times returned by
     * System.nanoTime() to protect against numerical overflows.
     *
     * @return a negative integer, zero, or a positive integer as the
     * first argument is less than, equal to, or greater than the second.
     *
     * @see System#nanoTime
     */
    public static long compare(long t1, long t2) {
        return t1 - t2;
    }
}
