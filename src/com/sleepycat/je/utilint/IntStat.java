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
 * An integer JE stat.
 */
public class IntStat extends Stat<Integer> {
    private static final long serialVersionUID = 1L;

    private int counter;

    public IntStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public IntStat(StatGroup group, StatDefinition definition, int counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    public Integer get() {
        return counter;
    }

    @Override
    public void set(Integer newValue) {
        counter = newValue;
    }

    public void increment() {
        counter++;
    }

    public void add(int count) {
        counter += count;
    }

    @Override
    public void add(Stat<Integer> otherStat) {
        counter += otherStat.get();
    }

    @Override
    public Stat<Integer> computeInterval(Stat<Integer> base) {
        Stat<Integer> ret = copy();
        if (definition.getType() == StatType.INCREMENTAL) {
            ret.set(counter - base.get());
        }
        return ret;
    }

    @Override
    public void negate() {
        counter = -counter;
    }

    @Override
    public void clear() {
        counter = 0;
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
