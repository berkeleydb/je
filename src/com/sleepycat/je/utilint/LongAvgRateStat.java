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

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * A long JE stat generated from an exponential moving average over a
 * specified time period of the rate of change in a value over time.
 */
public class LongAvgRateStat extends Stat<Long> {
    private static final long serialVersionUID = 1L;

    /** The underlying average rate. */
    private final LongAvgRate avg;

    /**
     * Creates an instance of this class.  The definition type must be
     * INCREMENTAL.
     *
     * @param group the statistics group
     * @param definition the statistics definition
     * @param periodMillis the averaging period in milliseconds
     * @param reportTimeUnit the time unit for reporting the rate
     */
    public LongAvgRateStat(StatGroup group,
                           StatDefinition definition,
                           long periodMillis,
                           TimeUnit reportTimeUnit) {
        super(group, definition);
        assert definition.getType() == StatType.INCREMENTAL;
        avg = new LongAvgRate(
            definition.getName(), periodMillis, reportTimeUnit);
    }

    private LongAvgRateStat(StatDefinition definition, LongAvgRate avg) {
        super(definition);
        this.avg = avg;
    }

    /**
     * Adds a new value to the average.
     *
     * @param value the new value
     * @param time the current time
     */
    public void add(long value, long time) {
        avg.add(value, time);
    }

    @Override
    public Long get() {
        return avg.get();
    }

    @Override
    public void clear() {
        avg.clear();
    }

    @Override
    public LongAvgRateStat copy() {
        return new LongAvgRateStat(definition, avg.copy());
    }

    @Override
    protected String getFormattedValue() {
        return avg.getFormattedValue();
    }

    @Override
    public boolean isNotSet() {
        return avg.isNotSet();
    }

    /** @throws UnsupportedOperationException always */
    @Override
    public void set(Long newValue) {
        throw new UnsupportedOperationException();
    }

    /** @throws UnsupportedOperationException always */
    @Override
    public void add(Stat<Long> other) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a stat that includes the newest entries from this stat and the
     * base stat.  This method does not use negative intervals, since negation
     * does not work properly for this non-additive stat.  The base argument
     * must be a LongAvgRateStat.
     */
    @Override
    public LongAvgRateStat computeInterval(Stat<Long> base) {
        assert base instanceof LongAvgRateStat;
        final LongAvgRate baseAvg = ((LongAvgRateStat) base).avg;
        return new LongAvgRateStat(definition, avg.copyLatest(baseAvg));
    }

    /** Do nothing for this non-additive stat. */
    @Override
    public void negate() { }
}
