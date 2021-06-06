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

package com.sleepycat.je.jmx.plugin;

import java.util.HashMap;

import javax.management.MBeanServerConnection;

import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.jmx.JEMonitor;

public class JEStats extends Stats {
    private static final long serialVersionUID = 2327923744424679603L;

    public JEStats(MBeanServerConnection connection) {
        super(connection);
    }

    @Override
    protected void initVariables() {
        statsTitles = EnvironmentStats.getStatGroupTitles();
        opName = JEMonitor.OP_ENV_STAT;
        mBeanNamePrefix = JEStatsPlugin.mBeanNamePrefix;
    }
   
    @SuppressWarnings("unchecked") 
    @Override
    protected void generateTips() {
        try {
            tips = (HashMap) connection.invoke
                (objName, JEMonitor.OP_GET_TIPS, 
                 new Object[] {}, new String[] {});
            updateTips();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
