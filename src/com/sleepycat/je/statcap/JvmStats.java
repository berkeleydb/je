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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;
import com.sleepycat.je.utilint.StatGroup;

class JvmStats {

    private final List<GarbageCollectorMXBean> gcBeans =
        ManagementFactory.getGarbageCollectorMXBeans();

    private final MemoryMXBean memoryBean =
        ManagementFactory.getMemoryMXBean();
    private final String GROUPNAME = "Jvm";
    private final String GROUPDEF = "Statistics capture jvm statistics.";
    private final String GC_COUNT_DESC = "GC collection count.";
    private final String GC_COLLECTION_TIME_DESC = "GC collection time.";
    private final String GC_COUNT_NAME_SUFFIX = ".count";
    private final String GC_TIME_NAME_SUFFIX = ".time";

    public static final StatDefinition LOAD_AVERAGE =
        new StatDefinition("loadAverage",
                           "Average JVM system load.",
                           StatType.CUMULATIVE);

    public static final StatDefinition HEAP_MEMORY_USAGE =
        new StatDefinition("heap",
                           "Heap memory usage.",
                           StatType.CUMULATIVE);

    private StatGroup prev = null;

    private final Map<String, StatDefinition> statdefmap =
        new HashMap<String, StatDefinition>();

    public JvmStats() {
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            String name = gcBean.getName();
            String statname = name + GC_COUNT_NAME_SUFFIX;
            StatDefinition sd = new StatDefinition(statname, GC_COUNT_DESC);
            statdefmap.put(statname, sd);
            statname = name + GC_TIME_NAME_SUFFIX;
            sd = new StatDefinition(statname, GC_COLLECTION_TIME_DESC);
            statdefmap.put(statname, sd);
        }
        statdefmap.put(LOAD_AVERAGE.getName(), LOAD_AVERAGE);
        statdefmap.put(HEAP_MEMORY_USAGE.getName(), HEAP_MEMORY_USAGE);
    }

    public StatGroup loadStats(StatsConfig sc) {
        StatGroup retgroup;

        StatGroup sg = new StatGroup(GROUPNAME, GROUPDEF);
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            String name = gcBean.getName();
            String statname = name + GC_COUNT_NAME_SUFFIX;
            new LongStat(
                sg, statdefmap.get(statname), gcBean.getCollectionCount());
            statname = name + GC_TIME_NAME_SUFFIX;
            new LongStat(
                sg, statdefmap.get(statname), gcBean.getCollectionTime());
        }
        new LongStat(sg, LOAD_AVERAGE, (long) JVMSystemUtils.getSystemLoad());
        new LongStat(
            sg, HEAP_MEMORY_USAGE, memoryBean.getHeapMemoryUsage().getUsed());

        if (prev != null) {
            retgroup = sg.computeInterval(prev);
        } else {
            retgroup = sg;
        }
        prev = sg;
        return retgroup;
    }

    public void addVMStatDefs(SortedSet<String> projections) {
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            projections.add(
                GROUPNAME + ":" + gcBean.getName() + GC_COUNT_NAME_SUFFIX);
            projections.add(
                GROUPNAME + ":" + gcBean.getName() + GC_TIME_NAME_SUFFIX);
        }
        projections.add(GROUPNAME + ":" + LOAD_AVERAGE.getName());
        projections.add(GROUPNAME + ":" + HEAP_MEMORY_USAGE.getName());
    }
}
