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
 * A long stat which represents a rate whose value is Integral.
 */
public class IntegralRateStat extends LongStat {
    private static final long serialVersionUID = 1L;

    private final long factor;
    
    public IntegralRateStat(StatGroup group, 
                            StatDefinition definition, 
                            Stat<? extends Number> divisor, 
                            Stat<? extends Number> dividend,
                            long factor) {
        super(group, definition);
        this.factor = factor;

        calculateRate(divisor, dividend);
    }

    /* Calculate the rate based on the two stats. */
    private void calculateRate(Stat<? extends Number> divisor, 
                               Stat<? extends Number> dividend) {
        if (divisor == null || dividend == null) {
            counter = 0;
        } else {
            counter = (dividend.get().longValue() != 0) ?
                (divisor.get().longValue() * factor) / 
                 dividend.get().longValue() :
                 0;
        }
    }
}
