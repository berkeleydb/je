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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A stat component based on an AtomicLong.
 */
public class AtomicLongComponent
        extends MapStatComponent<Long, AtomicLongComponent> {

    final AtomicLong val;

    /** Creates an instance of this class. */
    AtomicLongComponent() {
        val = new AtomicLong();
    }

    private AtomicLongComponent(long val) {
        this.val = new AtomicLong(val);
    }

    /**
     * Sets the stat to the specified value.
     *
     * @param newValue the new value
     */
    public void set(long newValue) {
        val.set(newValue);
    }

    /**
     * Adds the specified value.
     *
     * @param inc the value to add.
     */
    public void add(long inc) {
        val.addAndGet(inc);
    }

    @Override
    public Long get() {
        return val.get();
    }

    @Override
    public void clear() {
        val.set(0);
    }

    @Override
    public AtomicLongComponent copy() {
        return new AtomicLongComponent(val.get());
    }

    @Override
    protected String getFormattedValue(boolean useCommas) {
        if (useCommas) {
            return Stat.FORMAT.format(val.get());
        } else {
            return val.toString();
        }
    }

    @Override
    public boolean isNotSet() {
        return val.get() == 0;
    }

    @Override
    public String toString() {
        return val.toString();
    }
}
