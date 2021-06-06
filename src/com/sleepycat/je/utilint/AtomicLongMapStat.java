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

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * A JE stat that maintains a map of individual values based on AtomicLong
 * which can be looked up with a String key, and that returns results as a
 * formatted string.
 */
public final class AtomicLongMapStat
        extends MapStat<Long, AtomicLongComponent> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates an instance of this class.
     *
     * @param group the owning group
     * @param definition the associated definition
     */
    public AtomicLongMapStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    private AtomicLongMapStat(AtomicLongMapStat other) {
        super(other);
    }

    /**
     * Creates, stores, and returns a new stat for the specified key.
     *
     * @param key the key
     * @return the new stat
     */
    public synchronized AtomicLongComponent createStat(String key) {
        assert key != null;
        final AtomicLongComponent stat = new AtomicLongComponent();
        statMap.put(key, stat);
        return stat;
    }

    @Override
    public AtomicLongMapStat copy() {
        return new AtomicLongMapStat(this);
    }

    /** The base argument must be an instance of AtomicLongMapStat. */
    @Override
    public AtomicLongMapStat computeInterval(Stat<String> base) {
        assert base instanceof AtomicLongMapStat;
        final AtomicLongMapStat copy = copy();
        if (definition.getType() != StatType.INCREMENTAL) {
            return copy;
        }
        final AtomicLongMapStat baseMapStat = (AtomicLongMapStat) base;
        synchronized (copy) {
            for (final Entry<String, AtomicLongComponent> entry :
                     copy.statMap.entrySet()) {

                final AtomicLongComponent baseValue;
                synchronized (baseMapStat) {
                    baseValue = baseMapStat.statMap.get(entry.getKey());
                }
                if (baseValue != null) {
                    final AtomicLongComponent entryValue = entry.getValue();
                    entryValue.val.getAndAdd(-baseValue.get());
                }
            }
        }
        return copy;
    }

    @Override
    public synchronized void negate() {
        if (definition.getType() == StatType.INCREMENTAL) {
            for (final AtomicLongComponent stat : statMap.values()) {
                final AtomicLong atomicVal = stat.val;

                /*
                 * Negate the value atomically, retrying if another change
                 * intervenes.  This loop emulates the behavior of
                 * AtomicLong.getAndIncrement.
                 */
                while (true) {
                    final long val = atomicVal.get();
                    if (atomicVal.compareAndSet(val, -val)) {
                        break;
                    }
                }
            }
        }
    }
}
