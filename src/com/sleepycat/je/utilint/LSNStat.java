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

import com.sleepycat.je.EnvironmentFailureException;

/**
 * A long JE stat.
 */
public class LSNStat extends LongStat{
    private static final long serialVersionUID = 1L;

    public LSNStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public LSNStat(StatGroup group, StatDefinition definition, long counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    public void add(Stat<Long> other) {
        throw EnvironmentFailureException.unexpectedState(
            "LongArrayStat doesn't support the add operation.");
    }

    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        return copy();
    }

    @Override
    public void negate() {
    }

    @Override
    protected String getFormattedValue() {
        return DbLsn.getNoFormatString(counter);
    }
}
