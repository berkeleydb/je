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

public class IntegralLongAvg extends Number {

    private static final long serialVersionUID = 1L;
    private long numerator;
    private long denominator;
    private long factor = 1;

    public IntegralLongAvg (long numerator, long denominator, long factor) {
        this.numerator = numerator;
        this.denominator = denominator;
        this.factor = factor;
    }

    public IntegralLongAvg (long numerator, long denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
    }

    public IntegralLongAvg (IntegralLongAvg val) {
        this.numerator = val.numerator;
        this.denominator = val.denominator;
        this.factor = val.factor;
    }

    public void add(IntegralLongAvg other) {
        numerator += other.numerator;
        denominator += other.denominator;
    }

    public void subtract(IntegralLongAvg other) {
        numerator -= other.numerator;
        denominator -= other.denominator;
    }

    public long compute() {
        return (denominator != 0) ?
                (numerator * factor) / denominator :
                0;
    }

    public long getNumerator() {
        return numerator;
    }

    public void setNumerator(long numerator) {
        this.numerator = numerator;
    }

    public long getDenominator() {
        return denominator;
    }

    public void setDenominator(long denominator) {
        this.denominator = denominator;
    }

    @Override
    public int intValue() {
        return (int)compute();
    }

    @Override
    public long longValue() {
        return compute();
    }

    @Override
    public float floatValue() {
        return compute();
    }

    @Override
    public double doubleValue() {
        return compute();
    }
}
