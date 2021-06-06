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

import java.io.Serializable;

/**
 * The basic interface for accessing and clearing statistics for use in both
 * standalone statistics and component statistics contained in a {@link
 * MapStat}.
 *
 * @param <T> the type of the statistic value
 */
public abstract class BaseStat<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Returns the value of the statistic.
     *
     * @return the value
     */
    public abstract T get();

    /** Resets the statistic to its initial state. */
    public abstract void clear();

    /**
     * Returns a copy of this statistic.
     *
     * @return a copy
     */
    public abstract BaseStat<T> copy();

    /**
     * Returns the value of the statistic as a formatted string.
     *
     * @return the value as a formatted string
     */
    protected abstract String getFormattedValue();

    /**
     * Returns whether the statistic is in its initial state.
     *
     * @return if the statistic is in its initial state
     */
    public abstract boolean isNotSet();
}
