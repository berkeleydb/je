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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sleepycat.je.TransactionStats.Active;

/**
 * The Stats infrastructure provides context for JE statistics. Each statistic
 * has these attributes:
 * - metadata - specifically, a name and description
 * - each statistic is associated with a parent stat group, which itself has
 *   a name and description.
 * - support for the StatsConfig.clear semantics
 * - a way to print statistics in a user friendly way.
 *
 * To create a statistic variable, instantiate one of the concrete subclasses
 * of Stat. Each concrete subclass should hold the methods that are needed to
 * best set and display the value. For example, instead of using LongStat to
 * hold a timestamp or LSN value, use TimestampStat or LSNStat. A Stat instance
 * needs to specify a StatDefinition. There may be multiple Stat variables in
 * different components that share a StatDefinition. They are differentiated
 * when displayed by their parent StatGroup.
 *
 * Each Stat instance is associated with a StatGroup, which holds the
 * collection of stats that belong to a given component. Each member of the
 * StatGroup has a unique StatDefinition. StatGroups can be combined, in order
 * to accumulate values. For example, the LockManager may have multiple lock
 * tables. Each lock table keeps its own latch statistics. When LockStats are
 * generated, the StatsGroup for each latch is collected and rolled up into a
 * single StatGroup, using the addAll(StatGroup) method.
 *
 * The Stats infrastructure is for internal use only. Public API classes like
 * EnvironmentStats, LockStats, etc, contain StatGroups. A call to retrieve
 * stats is implemented by getting a clone of the StatGroups held by the
 * components like the cleaner, the incompressor, the LockManager, etc.  The
 * public API classes provide getter methods that reach into the StatGroups to
 * return the specific stat value.
 *
 * To add a statistic, create the Stat variable in the component where it is
 * being used and associate it with a StatGroup. The Stat infrastructure does
 * the rest of the work for plumbing that statistic up to the public API
 * class. Each API class must provide a getter method to access the specific
 * statistic. Currently, this is done manually.
 */
public class StatGroup implements Serializable {
    private static final long serialVersionUID = 1L;

    /*
     * User understandable description of the grouping. The description may
     * indicate that these stats are meant for internal use.
     */
    private final String groupName;
    private final String groupDescription;
    private final Map<StatDefinition, Stat<?>> stats;

    public StatGroup(String groupName, String groupDescription) {
        this(groupName, groupDescription,
             new HashMap<StatDefinition, Stat<?>>());
    }

    private StatGroup(String groupName,
                      String groupDescription,
                      Map<StatDefinition, Stat<?>> values) {
        this.groupName = groupName;
        this.groupDescription = groupDescription;
        this.stats = Collections.synchronizedMap(values);
    }

    /**
     * Returns a synchronized, unmodifiable view of the stats in this group.
     * Note that the returned set can still be modified by other threads, so
     * the caller needs to take that into account.
     */
    public Map<StatDefinition, Stat<?>> getStats() {
        return Collections.unmodifiableMap(stats);
    }

    /**
     * Add a stat to the group.
     */
    void register(Stat<?> oneStat) {
        Stat<?> prev = stats.put(oneStat.getDefinition(), oneStat);
        assert (prev == null) : "prev = " + prev + " oneStat=" +
            oneStat.getDefinition();
    }

    /**
     * Add all the stats from the other group into this group. If both groups
     * have the same stat, add the values.  The caller must make sure that no
     * stats are added to or removed from the argument during this call.
     *
     * @throws ConcurrentModificationException if the addition or removal of
     * stats in the argument is detected
     */
    @SuppressWarnings("unchecked")
    public void addAll(StatGroup other) {

        for (Entry<StatDefinition, Stat<?>> entry :
             other.stats.entrySet()) {

            StatDefinition definition = entry.getKey();
            Stat<?> localStat;
            synchronized (stats) {
                localStat = stats.get(definition);
                if (localStat == null) {
                    stats.put(definition, entry.getValue());
                    continue;
                }
            }

            /*
             * Cast to get around type problem. We know it's the same stat type
             * because the definition is the same, but the compiler doesn't
             * know that.
             */
            @SuppressWarnings("rawtypes")
            Stat additionalValue = entry.getValue();
            localStat.add(additionalValue);
        }
    }

    /**
     * The caller must make sure that no stats are added to or removed from
     * this stat group while this method is being called.
     *
     * @throws ConcurrentModificationException if the addition or removal of
     * stats in this group is detected
     */
    @SuppressWarnings("unchecked")
    public StatGroup computeInterval(StatGroup baseGroup) {
        Map<StatDefinition, Stat<?>> intervalValues =
                new HashMap<StatDefinition, Stat<?>>();

        for (Entry<StatDefinition, Stat<?>> entry :
             stats.entrySet()) {
            StatDefinition definition = entry.getKey();
            Stat<?> statValue = entry.getValue();
            @SuppressWarnings("rawtypes")
            Stat baseStat = baseGroup.stats.get(definition);
            if (baseStat == null) {
                intervalValues.put(definition, statValue.copy());
            } else {
                intervalValues.put(definition,
                                   statValue.computeInterval(baseStat));
            }
        }
        return new StatGroup(groupName, groupDescription, intervalValues);
    }

    /**
     * Clear all stats in a StatGroup.
     */
    public void clear() {
        synchronized (stats) {
            for (Stat<?> s : stats.values()) {
                s.clear();
            }
        }
    }

    /**
     * Negates all stats in a StatGroup.
     */
    public void negate() {
        synchronized (stats) {
            for (Stat<?> s : stats.values()) {
                s.negate();
            }
        }
    }

    public String getName() {
        return groupName;
    }

    public String getDescription() {
        return groupDescription;
    }

    /**
     * @return a Stats class that copies the value of all stats in the group
     */
    public StatGroup cloneGroup(boolean clear) {

        Map<StatDefinition, Stat<?>> copyValues =
            new HashMap<StatDefinition, Stat<?>>();

        synchronized (stats) {
            for (Stat<?> s : stats.values()) {
                if (clear) {
                    copyValues.put(s.getDefinition(), s.copyAndClear());
                } else {
                    copyValues.put(s.getDefinition(), s.copy());
                }
            }
        }
        return new StatGroup(groupName, groupDescription, copyValues);
    }

    /**
     * Return the stat associated with the specified definition, or null if not
     * found.
     *
     * @return the stat or null
     */
    public Stat<?> getStat(StatDefinition definition) {
        return stats.get(definition);
    }

    public int getInt(StatDefinition definition) {
        int retval;
        Stat<?> s = stats.get(definition);
        if (s == null) {
            retval = 0;
        } else if (s instanceof IntStat) {
            retval = ((IntStat) s).get();
        } else if (s instanceof AtomicIntStat) {
            retval = ((AtomicIntStat) s).get();
        } else {
            assert false : "Internal error calling getInt with" +
                " unexpected stat type: " + s.getClass().getName();
            retval = 0;
        }
        return retval;
    }

    public LongStat getLongStat(StatDefinition definition) {
        return (LongStat) stats.get(definition);
    }

    public long getLong(StatDefinition definition) {
        long retval = 0;
        Stat<?> s = stats.get(definition);
        if (s == null) {
            retval= 0;
        } else if (s instanceof LongStat) {
            retval = ((LongStat)s).get();
        } else if (s instanceof AtomicLongStat) {
            retval = ((AtomicLongStat)s).get();
        } else if (s instanceof IntegralLongAvgStat) {
            retval = ((IntegralLongAvgStat)s).get().compute();
        } else {
            assert false: "Internal error calling getLong() with "+
                 "unknown stat type.";
        }
        return retval;
    }

    public IntegralLongAvgStat getIntegralLongAvgStat(
        StatDefinition definition) {
        return (IntegralLongAvgStat) stats.get(definition);
    }

    public LongMinStat getLongMinStat(StatDefinition definition) {
        return (LongMinStat) stats.get(definition);
    }

    public LongMaxStat getLongMaxStat(StatDefinition definition) {
        return (LongMaxStat) stats.get(definition);
    }

    public AtomicLongStat getAtomicLongStat(StatDefinition definition) {
        return (AtomicLongStat) stats.get(definition);
    }

    public Long getAtomicLong(StatDefinition definition) {
        AtomicLongStat s = (AtomicLongStat) stats.get(definition);
        if (s == null) {
            return 0L;
        } else {
            return s.get();
        }
    }

    public Active[] getActiveTxnArray(StatDefinition definition) {
        ActiveTxnArrayStat s = (ActiveTxnArrayStat) stats.get(definition);
        if (s == null) {
            return null;
        } else {
            return s.get();
        }
    }

    public long[] getLongArray(StatDefinition definition) {
        LongArrayStat s = (LongArrayStat) stats.get(definition);
        if (s == null) {
            return null;
        } else {
            return s.get();
        }
    }

    public float getFloat(StatDefinition definition) {
        FloatStat s = (FloatStat) stats.get(definition);
        if (s == null) {
            return 0;
        } else {
            return s.get();
        }
    }

    public boolean getBoolean(StatDefinition definition) {
        BooleanStat s = (BooleanStat) stats.get(definition);
        if (s == null) {
            return false;
        } else {
            return s.get();
        }
    }

    public String getString(StatDefinition definition) {
        StringStat s = (StringStat) stats.get(definition);
        if (s == null) {
            return null;
        } else {
            return s.get();
        }
    }

    @SuppressWarnings("unchecked")
    public <V> SortedMap<String, V> getMap(StatDefinition definition) {
        MapStat<V, ?> s = (MapStat<V, ?>) stats.get(definition);
        if (s == null) {
            return null;
        } else {
            return s.getMap();
        }
    }

    /*
     * Add this group's information to the jconsole tip map.
     */
    public void addToTipMap(Map<String, String> tips) {
        tips.put(getName(), getDescription());
        for (StatDefinition d: stats.keySet()) {
            tips.put(d.getName(), d.getDescription());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(groupName).append(": ");
        sb.append(groupDescription).append("\n");

        /* Order the stats for consistent display. */
        Map<StatDefinition, Stat<?>> sortedStats;
        synchronized (stats) {
            sortedStats = new TreeMap<StatDefinition, Stat<?>>(stats);
        }
        for (Stat<?> s : sortedStats.values()) {
            sb.append("\t").append(s).append("\n");
        }

        return sb.toString();
    }

    /**
     * Includes the per-stat description in the output string.
     */
    public String toStringVerbose() {
        StringBuilder sb = new StringBuilder();
        sb.append(groupName).append(": ");
        sb.append(groupDescription).append("\n");

        /* Order the stats for consistent display.*/
        Map<StatDefinition, Stat<?>> sortedStats;
        synchronized (stats) {
            sortedStats = new TreeMap<StatDefinition, Stat<?>>(stats);
        }
        for (Stat<?> s : sortedStats.values()) {
            sb.append("\t").append(s.toStringVerbose()).append("\n");
        }
        return sb.toString();
    }

    /**
     * Only print values that are not null or zero.
     */
    public String toStringConcise() {

        boolean headerPrinted = false;
        StringBuilder sb = new StringBuilder();

        /* Order the stats for consistent display.*/
        Map<StatDefinition, Stat<?>> sortedStats;
        synchronized (stats) {
            sortedStats = new TreeMap<StatDefinition, Stat<?>>(stats);
        }

        for (Stat<?> s : sortedStats.values()) {

            if (s.isNotSet()) {
                continue;
            }

            /*
             * Print the group name lazily, in case no fields in this group are
             * set at all. In that case, this method will not print anything.
             */
            if (!headerPrinted) {
                sb.append(groupName + "\n");
                headerPrinted = true;
            }
            sb.append("\t").append(s).append("\n");
        }
        return sb.toString();
    }

    /**
     * Return a string suitable for using as the header for a .csv file.
     */
    public String getCSVHeader() {
        StringBuilder sb = new StringBuilder();
        synchronized (stats) {
            for (StatDefinition def : stats.keySet()) {
                sb.append(groupName + "_" + def.getName() + ",");
            }
        }
        return sb.toString();
    }

    /**
     * Return a string suitable for using as the data for a .csv file.
     */
    public String getCSVData() {
        StringBuilder sb = new StringBuilder();
        synchronized (stats) {
            for (Stat<?> s : stats.values()) {
                sb.append(s.getFormattedValue() + ",");
            }
        }
        return sb.toString();
    }
}
