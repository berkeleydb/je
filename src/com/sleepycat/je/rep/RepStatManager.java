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

package com.sleepycat.je.rep;

import java.util.Map;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.utilint.StatCaptureRepDefinitions;
import com.sleepycat.je.statcap.StatManager;
import com.sleepycat.je.utilint.StatGroup;

/**
 * @hidden
 * For internal use only.
 */
public class RepStatManager extends StatManager {

    private final UpdateMinMax updateRepMinMaxStat =
        new UpdateMinMax(StatCaptureRepDefinitions.minStats,
                         StatCaptureRepDefinitions.maxStats);

    public RepStatManager(RepImpl env) {
        super(env);
    }

    public synchronized ReplicatedEnvironmentStats getRepStats(
        StatsConfig config,
        Integer contextKey) {

        StatContext sc = statContextMap.get(contextKey);
        if (sc == null) {
            throw EnvironmentFailureException.unexpectedState(
                "Internal error stat context is not registered");
        }
        ReplicatedEnvironmentStats rstat =
            ((RepImpl)env).getStatsInternal(config);
        if (rstat == null) {
            return null;
        }
        Map<String, StatGroup> cur = rstat.getStatGroupsMap();
        Map<String, StatGroup> base = sc.getRepBase();

        ReplicatedEnvironmentStats intervalStats;
        if (base != null) {
            intervalStats = computeRepIntervalStats(cur, base);
        } else {
            intervalStats = rstat;
        }

        if (config.getClear()) {

            for (StatContext context : statContextMap.values()) {
                if (context.getRepBase() != null) {
                    updateRepMinMaxStat.updateBase(context.getRepBase(), cur);
                }
            }

            for (StatContext context : statContextMap.values()) {
                if (context == sc) {
                    context.setRepBase(null);
                } else {
                    if (context.getRepBase() == null) {
                        context.setRepBase(cloneAndNegate(cur));
                    } else {
                        // reset base
                        context.setRepBase(
                            computeRepIntervalStats(
                                context.getRepBase(),cur).getStatGroupsMap());
                    }
                }
            }
        }

        return intervalStats;
    }

    private ReplicatedEnvironmentStats computeRepIntervalStats(
        Map<String, StatGroup>current,
        Map<String, StatGroup> base) {

        ReplicatedEnvironmentStats envStats = new ReplicatedEnvironmentStats();
        for (StatGroup cg : current.values()) {
            if (base != null) {
                StatGroup bg = base.get(cg.getName());
                envStats.setStatGroup(cg.computeInterval(bg));
            } else {
                envStats.setStatGroup(cg.cloneGroup(false));
            }
        }
        return envStats;
    }
}
