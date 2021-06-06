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

package com.sleepycat.je.statcap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.LongMaxStat;
import com.sleepycat.je.utilint.LongMinStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;

/**
 * The StatManager provides functionality to acquire incremental statistics.
 * A client registers itself and is returned a key. The key is used in
 * subsequent calls to acquire statistics. The key is associated with a base
 * set of statistic values. The base set is used to compute incremental
 * statistics. Incremental statistics are computed interval by subtracting
 * the base from the current set of values. The base values for the
 * registered contexts are updated when statistics are cleared.
 *
 * For instance if you have a counter named X. The initial value is zero.
 * Suppose there are two statistic contexts registered S1
 * (say for statcapture)and S2 (for the public api loadStats).   The counter
 * gets incremented to 10. S1 loads stats with clear=true. The statistic base
 * for the other stat contexts, S2  is updated. The value in the base for X
 * is set to (current value in base - current stat value) or (0 - 10). The
 * value returned for stat X with respect to context S1
 * (the caller of loadStat) is (current value of X - base value) or 10-0.
 * The value of X is cleared since getClear() =true. Later the value of X is
 * incremented (value is now 1). Statistics are loaded for stat context S2.
 * The value returned is current value - base value, or 1 - (-10) or 11.
 */
public class StatManager {

    /* Registered statistics base contexts */
    protected final Map<Integer, StatContext> statContextMap =
        new HashMap<Integer, StatContext>();

    private final UpdateMinMax updateMinMaxStat =
        new UpdateMinMax(StatCaptureDefinitions.minStats,
                         StatCaptureDefinitions.maxStats);

    protected final EnvironmentImpl env;

    public StatManager(EnvironmentImpl env) {
        this.env = env;
    }

    public synchronized Integer registerStatContext() {
        StatContext sctx = new StatContext(null);
        int max = 0;
        for (Integer key : statContextMap.keySet()) {
            if (key > max) {
                max = key;
            }
        }
        Integer newkey = max + 1;
        statContextMap.put(newkey, sctx);
        return newkey;
    }

    public synchronized EnvironmentStats loadStats(StatsConfig config,
                                                   Integer contextKey) {
        StatContext sc = statContextMap.get(contextKey);
        if (sc == null) {
            throw EnvironmentFailureException.unexpectedState(
                "Internal error stat context is not registered");
        }
        /* load current statistics */
        EnvironmentStats curstats = env.loadStatsInternal(config);
        Map<String, StatGroup> cur = curstats.getStatGroupsMap();

        /* compute statistics by using the base values from the context */
        Map<String, StatGroup> base = sc.getBase();
        EnvironmentStats intervalStats;
        if (base != null) {
            intervalStats = computeIntervalStats(cur, base);
        } else {
            intervalStats = curstats;
        }

        if (config.getClear()) {

            /* The underlying statistics were cleared so the base values
             * for the registered contexts are updated to reflect the
             * current statistic values.
             */
            for (StatContext context : statContextMap.values()) {
                if (context.getBase() != null) {
                    updateMinMaxStat.updateBase(context.getBase(), cur);
                }
            }

            for (StatContext context : statContextMap.values()) {
                if (context == sc) {
                    context.setBase(null);
                } else {
                    if (context.getBase() == null) {
                        context.setBase(cloneAndNegate(cur));
                    } else {
                        // reset base
                        context.setBase(
                            computeIntervalStats(
                                context.getBase(), cur).getStatGroupsMap());
                    }
                }
            }
        }
        return intervalStats;
    }

    private EnvironmentStats computeIntervalStats(
        Map<String, StatGroup> current,
        Map<String, StatGroup> base) {

        EnvironmentStats envStats = new EnvironmentStats();

        for (StatGroup cg : current.values()) {
            StatGroup bg = base.get(cg.getName());
            envStats.setStatGroup(cg.computeInterval(bg));
        }
        return envStats;
    }

    protected Map<String, StatGroup> cloneAndNegate(Map<String, StatGroup> in) {
        HashMap<String, StatGroup> retval = new HashMap<String, StatGroup>();
        for (Entry<String, StatGroup>e : in.entrySet()) {
            StatGroup negatedGroup = e.getValue().cloneGroup(false);
            negatedGroup.negate();
            retval.put(e.getKey(), negatedGroup);
        }
        return retval;
    }

    protected class StatContext {
        private Map<String, StatGroup> base;
        private Map<String, StatGroup> repbase = null;

        StatContext(Map<String, StatGroup> base) {
            this.base = base;
        }

        void setBase(Map<String, StatGroup> base) {
            this.base = base;
        }

        Map<String, StatGroup> getBase() {
            return base;
        }

        public void setRepBase(Map<String, StatGroup> base) {
            this.repbase = base;
        }

        public Map<String, StatGroup> getRepBase() {
            return repbase;
        }
    }

    public static class SDef {
        private final String groupName;
        private final StatDefinition definition;

        public SDef(String groupname, StatDefinition sd) {
            definition = sd;
            groupName = groupname;
        }

        public String getGroupName() {
            return groupName;
        }

        public StatDefinition getDefinition() {
            return definition;
        }
    }

    public class UpdateMinMax {
        private final ArrayList<SDef> minStats = new ArrayList<SDef>();
        private final ArrayList<SDef> maxStats = new ArrayList<SDef>();

        public UpdateMinMax(SDef[] minStatistics, SDef[] maxStatistics) {
            for (SDef min : minStatistics) {
                minStats.add(min);
            }

            for (SDef max : maxStatistics) {
                maxStats.add(max);
            }
        }

        public void updateBase(Map<String, StatGroup> base,
                               Map<String, StatGroup> other) {
            for (SDef sd : minStats) {
                StatGroup group = other.get(sd.groupName);
                if (group == null) {
                    continue;
                }
                LongStat otherValue =
                    group.getLongStat(sd.definition);
                if (otherValue == null) {
                    continue;
                }

                LongMinStat baseStat =
                        base.get(sd.groupName).getLongMinStat(sd.definition);

                /* Check is stat is not yet in the base */
                if (baseStat == null) {
                    StatGroup sg = base.get(sd.groupName);
                    baseStat = (LongMinStat)otherValue.copyAndAdd(sg);
                }

                baseStat.setMin(otherValue.get());
            }
            for (SDef sd : maxStats) {
                StatGroup group = other.get(sd.groupName);
                if (group == null) {
                    continue;
                }
                LongStat otherValue =
                    group.getLongStat(sd.definition);
                if (otherValue == null) {
                    continue;
                }
                LongMaxStat baseStat =
                        base.get(sd.groupName).getLongMaxStat(sd.definition);

                /* Check is stat is not yet in the base */
                if (baseStat == null) {
                    StatGroup sg = base.get(sd.groupName);
                    baseStat = (LongMaxStat)otherValue.copyAndAdd(sg);
                }

                baseStat.setMax(otherValue.get());
            }
        }
    }
}
