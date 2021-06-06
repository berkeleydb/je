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

package com.sleepycat.je.rep.jmx.plugin;

import java.util.HashMap;

import javax.management.MBeanServerConnection;

import com.sleepycat.je.jmx.plugin.Stats;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.jmx.RepJEMonitor;

public class RepJEStats extends Stats {
    private static final long serialVersionUID = 4240112567440108407L;

    public RepJEStats(MBeanServerConnection connection) {
        super(connection);
    }

    @Override
    protected void initVariables() {
        statsTitles = ReplicatedEnvironmentStats.getStatGroupTitles(); 
        opName = RepJEMonitor.OP_DUMP_REPSTATS;
        mBeanNamePrefix = RepJEStatsPlugin.mBeanNamePrefix;
    }
   
    @SuppressWarnings("unchecked") 
    @Override
    protected void generateTips() {
        try {
            tips = (HashMap) connection.invoke
                (objName, RepJEMonitor.OP_GET_REP_TIPS, 
                 new Object[] {}, new String[] {});
            updateTips();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
