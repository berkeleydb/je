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
 * The interface for individual stat components included in a {@link MapStat}.
 *
 * @param <T> the type of the statistic value
 * @param <C> the type of the component
 */
public abstract class MapStatComponent<T, C extends MapStatComponent<T, C>>
        extends BaseStat<T> {

    /**
     * Returns the value of the statistic as a formatted string, either using
     * or not using commas as requested.  Implementations should make sure that
     * the result does not contain commas when useCommas is false, because the
     * value will be used in a comma-separated value file, where embedded
     * commas would cause problems.
     *
     * @param useCommas whether to use commas
     * @return the value as a formatted string
     */
    protected abstract String getFormattedValue(boolean useCommas);

    /** Implement this overloading to use commas. */
    @Override
    protected String getFormattedValue() {
        return getFormattedValue(true);
    }

    /** Narrow the return type to the component type. */
    @Override
    public abstract C copy();
}
