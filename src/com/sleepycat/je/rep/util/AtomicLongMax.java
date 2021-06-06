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

package com.sleepycat.je.rep.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An Atomic long that maintains a max value
 */
public class AtomicLongMax {

    private final AtomicLong value;

    public AtomicLongMax(long initialValue) {
        value = new AtomicLong(initialValue);
    }

    /**
     * Updates the max value if the argument is greater than the current max.
     */
    public long updateMax(long newMax) {
        long currMax = value.get();
        for (; newMax > currMax; currMax = value.get()) {
            if (value.compareAndSet(currMax, newMax)) {
                return newMax;
            }
        }

        /* Higher or equal value already present. */
        return currMax;
    }

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    public long get() {
        return value.get();
    }

    /**
     * Set the value to newValue and returns the old value.
     */
    public long set(long newValue) {
        return value.getAndSet(newValue);
    }
}
