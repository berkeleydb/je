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
 * A stat that saves a string; a way to save general information for later
 * display and access.
 */
public class StringStat extends Stat<String> {
    private static final long serialVersionUID = 1L;

    private String value;

    public StringStat(StatGroup group,
                      StatDefinition definition) {
        super(group, definition);
    }

    public StringStat(StatGroup group,
                      StatDefinition definition,
                      String initialValue) {
        super(group, definition);
        value = initialValue;
    }

    @Override
    public String get() {
        return value;
    }

    @Override
    public void set(String newValue) {
        value = newValue;
    }

    @Override
    public void add(Stat<String> otherStat) {
        value += otherStat.get();
    }

    @Override
    public Stat<String> computeInterval(Stat<String> base) {
       return copy();
    }

    @Override
    public void negate() {
    }

    @Override
    public void clear() {
        value = null;
    }

    @Override
    protected String getFormattedValue() {
        return value;
    }

    @Override
    public boolean isNotSet() {
        return (value == null);
    }
}
