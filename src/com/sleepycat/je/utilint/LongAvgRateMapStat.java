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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * A JE stat that maintains a map of individual {@link LongAvgRate} values
 * which can be looked up with a String key, and that returns results as a
 * formatted string.
 */
public final class LongAvgRateMapStat extends MapStat<Long, LongAvgRate> {

    private static final long serialVersionUID = 1L;

    /** The averaging period in milliseconds. */
    protected final long periodMillis;

    /** The time unit for reporting rates. */
    private final TimeUnit reportTimeUnit;

    /**
     * The time the last stat was removed.  This value is used to determine
     * which entries should be included when calling computeInterval.
     * Synchronize on this instance when accessing this field.
     */
    private long removeStatTimestamp;

    /**
     * Creates an instance of this class.  The definition type must be
     * INCREMENTAL.
     *
     * @param group the owning group
     * @param definition the associated definition
     * @param periodMillis the sampling period in milliseconds
     * @param reportTimeUnit the time unit for reporting rates
     */
    public LongAvgRateMapStat(StatGroup group,
                              StatDefinition definition,
                              long periodMillis,
                              TimeUnit reportTimeUnit) {
        super(group, definition);
        assert definition.getType() == StatType.INCREMENTAL;
        assert periodMillis > 0;
        assert reportTimeUnit != null;
        this.periodMillis = periodMillis;
        this.reportTimeUnit = reportTimeUnit;
    }

    private LongAvgRateMapStat(LongAvgRateMapStat other) {
        super(other);
        periodMillis = other.periodMillis;
        reportTimeUnit = other.reportTimeUnit;
        synchronized (this) {
            synchronized (other) {
                removeStatTimestamp = other.removeStatTimestamp;
            }
        }
    }

    /**
     * Creates, stores, and returns a new stat for the specified key.
     *
     * @param key the key
     * @return the new stat
     */
    public synchronized LongAvgRate createStat(String key) {
        assert key != null;
        final LongAvgRate stat = new LongAvgRate(
            definition.getName() + ":" + key, periodMillis, reportTimeUnit);
        statMap.put(key, stat);
        return stat;
    }

    /**
     * Note the removal time, so that computeInterval can tell if an empty map
     * is newer than a non-empty one.
     */
    @Override
    public synchronized void removeStat(String key) {
        removeStat(key, System.currentTimeMillis());
    }

    /** Remove a stat and specify the time of the removal -- for testing. */
    synchronized void removeStat(String key, long time) {
        removeStatTimestamp = time;
        super.removeStat(key);
    }

    @Override
    public LongAvgRateMapStat copy() {
        return new LongAvgRateMapStat(this);
    }

    /**
     * Creates a new map that contains entries for all keys that appear in
     * whichever of this map or the argument is newer, with those entries
     * updated with any values from both maps.  Treats this map as newest if
     * both have the same timestamp.  This method does not compute negative
     * intervals, since negation does not work properly for this non-additive
     * stat.  The base argument must be a LongAvgRateMapStat.
     */
    @Override
    public LongAvgRateMapStat computeInterval(Stat<String> base) {
        assert base instanceof LongAvgRateMapStat;
        final LongAvgRateMapStat copy = copy();
        final LongAvgRateMapStat baseCopy =
            (LongAvgRateMapStat) base.copy();
        if (copy.getLatestTime() < baseCopy.getLatestTime()) {
            return copy.updateLatest(baseCopy);
        }
        return baseCopy.updateLatest(copy);
    }

    /**
     * Update this map to reflect changes from the argument, including merging
     * latest changes, removing entries not in the argument, and adding ones
     * not in this instance.
     */
    private synchronized LongAvgRateMapStat updateLatest(
        final LongAvgRateMapStat latest) {

        synchronized (latest) {
            for (final Iterator<Entry<String, LongAvgRate>> i =
                     statMap.entrySet().iterator();
                 i.hasNext(); ) {
                final Entry<String, LongAvgRate> e = i.next();
                final LongAvgRate latestStat =
                    latest.statMap.get(e.getKey());
                if (latestStat != null) {
                    e.getValue().add(latestStat);
                } else {
                    i.remove();
                }
            }

            for (final Entry<String, LongAvgRate> e :
                     latest.statMap.entrySet()) {
                final String key = e.getKey();
                if (!statMap.containsKey(key)) {
                    statMap.put(key, e.getValue());
                }
            }
        }
        return this;
    }

    /**
     * Returns the most recent time any component stat was modified, including
     * the time of the latest stat removal.
     */
    private synchronized long getLatestTime() {
        long latestTime = removeStatTimestamp;
        for (final LongAvgRate stat : statMap.values()) {
            latestTime = Math.max(latestTime, stat.getPrevTime());
        }
        return latestTime;
    }

    /** Do nothing for this non-additive stat. */
    @Override
    public synchronized void negate() { }
}
