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

import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Used to create running totals across the lifetime of the StatGroup. They
 * cannot be cleared.
 */
public class IntRunningTotalStat extends IntStat {

    private static final long serialVersionUID = 1L;

    public IntRunningTotalStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    @Override
    public void clear() {
       /* Don't clear it because it's a running total. */
    }
}
