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
 * A long JE stat.
 */
public class LongStat extends Stat<Long> {
    private static final long serialVersionUID = 1L;

    protected long counter;

    public LongStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public LongStat(StatGroup group, StatDefinition definition, long counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    public Long get() {
        return counter;
    }

    @Override
    public void set(Long newValue) {
        counter = newValue;
    }

    public void increment() {
        counter++;
    }

    public void add(long count) {
        counter += count;
    }

    public void max(long count) {
        if (count > counter) {
            count = counter;
        }
    }

    @Override
    public void add(Stat<Long> other) {
        counter += other.get();
    }

    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        Stat<Long> ret = copy();
        if (definition.getType() == StatType.INCREMENTAL) {
            ret.set(counter - base.get());
        }
        return ret;
    }

    @Override
    public void negate () {
        if (definition.getType() == StatType.INCREMENTAL) {
            counter = -counter;
        }
    }

    @Override
    public void clear() {
        counter = 0L;
    }

    @Override
    protected String getFormattedValue() {
        return Stat.FORMAT.format(counter);
    }

    @Override
    public boolean isNotSet() {
       return (counter == 0);
    }
}
