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
 * A long stat which represents a average whose value is Integral.
 */
public class IntegralLongAvgStat extends Stat<IntegralLongAvg> {

    private static final long serialVersionUID = 1L;
    private IntegralLongAvg value;

    public IntegralLongAvgStat(StatGroup group,
                               StatDefinition definition,
                               long numerator,
                               long denominator,
                               long factor) {
        super(group, definition);
        value = new IntegralLongAvg(numerator, denominator, factor);
    }

    public IntegralLongAvgStat(StatGroup group,
                               StatDefinition definition,
                               long numerator,
                               long denominator) {
        super(group, definition);
        value = new IntegralLongAvg(numerator, denominator);
    }

    @Override
    public IntegralLongAvg get() {
        return value;
    }

    @Override
    public void set(IntegralLongAvg newValue) {
        value = newValue;
    }

    @Override
    public void add(Stat<IntegralLongAvg> otherStat) {
        value.add(otherStat.get());
    }

    @Override
    public Stat<IntegralLongAvg> computeInterval(Stat<IntegralLongAvg> base) {
        IntegralLongAvgStat ret = copy();
        ret.value.subtract(base.get());
        return ret;
    }

    @Override
    public void negate() {
        if (value != null) {
            value.setDenominator(-value.getDenominator());
            value.setNumerator(-value.getNumerator());
        }
    }

    @Override
    public IntegralLongAvgStat copy() {
        try {
            IntegralLongAvgStat ret = (IntegralLongAvgStat) super.clone();
            ret.value = new IntegralLongAvg(value);
            return ret;
        } catch (CloneNotSupportedException unexpected) {
            throw EnvironmentFailureException.unexpectedException(unexpected);
        }
    }

    @Override
    public void clear() {
        value = null;
    }

    @Override
    protected String getFormattedValue() {
        return (value != null) ?
                Stat.FORMAT.format(get()) :
                Stat.FORMAT.format(0);
    }

    @Override
    public boolean isNotSet() {
        return (value == null);
    }
}
