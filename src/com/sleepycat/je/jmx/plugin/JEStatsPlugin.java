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

import java.util.LinkedHashMap;

import javax.management.ObjectName;
import javax.swing.JPanel;

public class JEStatsPlugin extends StatsPlugin {
    public static String mBeanNamePrefix = 
        "com.sleepycat.je.jmx:name=*JEMonitor(*";

    @Override
    protected void initTabs() {
        if (tabs == null) {
            tabs = new LinkedHashMap<String, JPanel>();
            try {
                ObjectName name = new ObjectName(mBeanNamePrefix);
                int count = getContext().getMBeanServerConnection().
                    queryNames(name, null).size();

                if (count > 0) {
                    Stats status =
                        new JEStats(getContext().getMBeanServerConnection());
                    tabs.put("JE Statistics", status);
                    stats.add(status);
                } else {
                    tabs.put("JE Statistics", new JPanel());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
