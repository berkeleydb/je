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

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * A JE stat that maintains a map of individual {@link LongDiffStat} values
 * which can be looked up with a String key, and that returns results as a
 * formatted string.  Only supports CUMULATIVE stats.
 */
public final class LongDiffMapStat extends MapStat<Long, LongDiffStat> {
    private static final long serialVersionUID = 1L;

    /**
     * The maximum time, in milliseconds, that a computed difference is
     * valid.
     */
    private final long validityMillis;

    /**
     * Creates an instance of this class.  The definition type must be
     * CUMULATIVE.
     *
     * @param group the owning group
     * @param definition the associated definition
     * @param validityMillis the amount of time, in milliseconds, which a
     * computed difference remains valid
     */
    public LongDiffMapStat(StatGroup group,
                           StatDefinition definition,
                           long validityMillis) {
        super(group, definition);
        assert definition.getType() == StatType.CUMULATIVE;
        assert validityMillis > 0;
        this.validityMillis = validityMillis;
    }

    private LongDiffMapStat(LongDiffMapStat other) {
        super(other);
        validityMillis = other.validityMillis;
    }

    /**
     * Creates, stores, and returns a new stat for the specified key and base
     * stat.
     *
     * @param key the new key
     * @param base the base stat
     * @return the new stat
     */
    public synchronized LongDiffStat createStat(String key, Stat<Long> base) {
        final LongDiffStat stat = new LongDiffStat(base, validityMillis);
        statMap.put(key, stat);
        return stat;
    }

    @Override
    public LongDiffMapStat copy() {
        return new LongDiffMapStat(this);
    }

    /** Ignores base for a non-additive stat. */
    @Override
    public LongDiffMapStat computeInterval(Stat<String> base) {
        return copy();
    }

    /** Does nothing for a non-additive stat. */
    @Override
    public synchronized void negate() { }
}
