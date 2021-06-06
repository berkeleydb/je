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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.utilint.StatLogger;

public class EnvStatsLogger implements EnvConfigObserver {

    private final EnvironmentImpl env;
    private StatLogger stlog;
    public static final String STATFILENAME = "je.config";
    public static final String STATFILEEXT = "csv";
    private static final String DELIMITER = ",";
    private static final String QUOTE = "\"";
    private static final int MAXROWCOUNT = 1000;
    private static final int MAXFILECOUNT = 2;
    private final StringBuffer sb = new StringBuffer();
    private final StringBuffer valb = new StringBuffer();
    private final Logger logger;

    public EnvStatsLogger(EnvironmentImpl env) {

        File statdirf;

        this.env = env;
        logger = LoggerUtils.getLogger(getClass());
        String statdir = env.getConfigManager().get(
                EnvironmentParams.STATS_FILE_DIRECTORY);
        if (statdir == null || statdir.equals("")) {
            statdirf = env.getEnvironmentHome();
        } else {
            statdirf = new File(statdir);
        }

        try {
            stlog = new StatLogger(statdirf,
                                   STATFILENAME,
                                   STATFILEEXT,
                                   MAXFILECOUNT,
                                   MAXROWCOUNT);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Error accessing statistics capture file "+
                STATFILENAME + "." + STATFILEEXT +
                " IO Exception: " + e);
        }
    }

    public void log() {
        SortedMap<String, String> envConfigMap = new TreeMap<String, String>();
        EnvironmentConfig mc = env.cloneConfig();
        for (String colname :
             EnvironmentParams.SUPPORTED_PARAMS.keySet()) {
            envConfigMap.put("envcfg:" + colname, mc.getConfigParam(colname));
        }
        addSystemStats(envConfigMap);
        sb.setLength(0);
        valb.setLength(0);
        sb.append("time");
        valb.append(StatUtils.getDate(System.currentTimeMillis()));
        for (Entry<String, String> e : envConfigMap.entrySet()) {
            if (sb.length() != 0) {
                sb.append(DELIMITER);
                valb.append(DELIMITER);
            }
            sb.append(e.getKey());
            valb.append(QUOTE + e.getValue() + QUOTE);
        }
        try {
            stlog.setHeader(sb.toString());
            stlog.logDelta(valb.toString());
        } catch (IOException e) {
            LoggerUtils.warning(logger, env,
                " Error accessing environment statistics file " +
                STATFILENAME + "." + STATFILEEXT +
                " IO Exception: " + e);
        }
        sb.setLength(0);
        valb.setLength(0);
    }

    @Override
    public void envConfigUpdate(DbConfigManager configMgr,
                                EnvironmentMutableConfig newConfig) {
        log();
    }

    private void addSystemStats(Map<String, String> statmap) {
        OperatingSystemMXBean osbean =
            ManagementFactory.getOperatingSystemMXBean();
        MemoryMXBean memoryBean =
            ManagementFactory.getMemoryMXBean();

        statmap.put("je:version",
                    JEVersion.CURRENT_VERSION.getVersionString());
        statmap.put("java:version", System.getProperty("java.version"));
        statmap.put("java:vendor", System.getProperty("java.vendor"));
        statmap.put("os:name", osbean.getName());
        statmap.put("os:version", osbean.getVersion());
        statmap.put("mc:arch", osbean.getArch());
        statmap.put("mc:processors",
                    Integer.toString(osbean.getAvailableProcessors()));
        statmap.put("java:minMemory" ,
                    Long.toString(memoryBean.getHeapMemoryUsage().getInit()));
        statmap.put("java:maxMemory" ,
                Long.toString(memoryBean.getHeapMemoryUsage().getMax()));
        List<String> args =
                     ManagementFactory.getRuntimeMXBean().getInputArguments();
        sb.setLength(0);
        for (String arg : args) {
            sb.append(" " + arg);
        }
        statmap.put("java:args", sb.toString());
        sb.setLength(0);
    }
}
