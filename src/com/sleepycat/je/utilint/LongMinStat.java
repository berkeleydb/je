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
 * A long stat which maintains a minimum value. It is intialized to
 * Long.MAX_VALUE. The setMin() method assigns the counter to
 * MIN(counter, new value).
 */
public class LongMinStat extends LongStat {
    private static final long serialVersionUID = 1L;

    public LongMinStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        clear();
    }

    public LongMinStat(StatGroup group,
                       StatDefinition definition,
                       long counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    public void clear() {
        set(Long.MAX_VALUE);
    }

    /**
     * Set stat to MIN(current stat value, newValue).
     */
    public void setMin(long newValue) {
        counter = (counter > newValue) ? newValue : counter;
    }

    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        return (counter > base.get() ? base.copy() : copy());
    }

    @Override
    public void negate() {
    }

    @Override
    protected String getFormattedValue() {
        if (counter == Long.MAX_VALUE) {
            return "NONE";
        }

        return Stat.FORMAT.format(counter);
    }
}
