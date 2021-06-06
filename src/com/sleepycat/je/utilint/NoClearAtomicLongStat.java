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
 * A version of {@link AtomicLongStat} that does not reset its value when
 * cleared.
 */
public class NoClearAtomicLongStat extends AtomicLongStat {
    private static final long serialVersionUID = 1L;

    public NoClearAtomicLongStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    NoClearAtomicLongStat(StatDefinition definition, long value) {
        super(definition, value);
    }

    /** Never clear this stat. */
    @Override
    public void clear() { }

    @Override
    public AtomicLongStat copy() {
        return new NoClearAtomicLongStat(definition, get());
    }

    /** Never clear this stat. */
    @Override
    public AtomicLongStat copyAndClear() {
        return copy();
    }
}
