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

import com.sleepycat.je.utilint.LongMinStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * For stats where the min value in the range is zero, so that sums, averages,
 * etc. based on positive ranges just work.
 */
public class LongMinZeroStat extends LongMinStat {

    private static final long serialVersionUID = 1L;

    public LongMinZeroStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    @Override
    public Long get() {
        Long value = super.get();
        return (value == Long.MAX_VALUE) ? 0 : value;
    }
}
