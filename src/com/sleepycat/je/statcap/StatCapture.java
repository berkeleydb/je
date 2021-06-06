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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.sleepycat.je.CustomStats;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Stat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.utilint.StatLogger;

public class StatCapture extends DaemonThread implements EnvConfigObserver {

    public static final String STATFILENAME = "je.stat";
    public static final String STATFILEEXT = "csv";
    private static final String CUSTOMGROUPNAME = "Custom";
    private static final String DELIMITER = ",";
    private static final String DELIMITERANDSPACE = ", ";

    private final StatManager statMgr;

    private final SortedSet<String> statProjection;

    private final StatsConfig statsConfig;

    private final Integer statKey;

    private volatile StatLogger stlog = null;
    private final StringBuffer values = new StringBuffer();
    private String currentHeader = null;

    private final JvmStats jvmstats = new JvmStats();
    private final CustomStats customStats;
    private final String[] customStatHeader;

    private final Logger logger;

    /*
     * Exception of last outputStats() call or null if call was successful.
     * Used to limit the number of errors logged.
     */
    private Exception lastCallException = null;

    public StatCapture(EnvironmentImpl environment,
                       String name,
                       long waitTime,
                       CustomStats customStats,
                       SortedSet<String> statProjection,
                       StatManager statMgr) {

        super(waitTime, name, environment);

        logger = LoggerUtils.getLogger(getClass());
        environment.addConfigObserver(this);

        this.statMgr = statMgr;
        statKey = statMgr.registerStatContext();

        this.customStats = customStats;
        this.statProjection = statProjection;

        /*
         * Note that we fetch all stats, not just fast stats. Since the stat
         * retrieval frequency is one minute and this is done by a background
         * thread, there is no reason not to include all stats.
         */
        statsConfig = new StatsConfig();
        statsConfig.setClear(true);

        /* Add jvm and custom statistics to the projection list. */
        jvmstats.addVMStatDefs(statProjection);

        if (customStats != null) {
            final String[] customFldNames = customStats.getFieldNames();
            customStatHeader = new String[customFldNames.length];
            for (int i = 0; i < customFldNames.length; i++) {
                customStatHeader[i] =
                    CUSTOMGROUPNAME + ":" + customFldNames[i];
                statProjection.add(customStatHeader[i]);
            }
        } else {
            customStatHeader = null;
        }

        envConfigUpdate(envImpl.getConfigManager(), null);
    }

    private boolean collectStats() {
        return stlog != null;
    }

    /**
     * Called whenever the DaemonThread wakes up from a sleep.
     */
    @Override
    protected void onWakeup() {

        if (!envImpl.isValid() || !collectStats()) {
            return;
        }

        outputStats();
    }

    @Override
    public void requestShutdown() {
        super.requestShutdown();

        /*
         * Check if env is valid outside of synchronized call to
         * outputStats(). It is possible that a call to outputStats
         * caused the invalidation and we would deadlock since that
         * thread is holding the lock for this object and waiting for
         * this thread to shutdown.
         */
        if (!collectStats() || !envImpl.isValid()) {
            return;
        }
        outputStats();
    }

    private synchronized void outputStats() {

        if (!collectStats() || !envImpl.isValid()) {
            return;
        }

        try {
            SortedMap<String, String> stats = getStats();

            if (stats != null) {
                if (currentHeader == null) {
                    values.setLength(0);
                    values.append("time");

                    for (Iterator<String> nameit = statProjection.iterator();
                        nameit.hasNext();) {
                        String statname = nameit.next();
                        values.append(DELIMITER + statname);
                    }
                    stlog.setHeader(values.toString());
                    currentHeader = values.toString();
                }
                values.setLength(0);
                values.append(StatUtils.getDate(System.currentTimeMillis()));

                for (Iterator<String> nameit = statProjection.iterator();
                    nameit.hasNext();) {
                    String statname = nameit.next();
                    String val = stats.get(statname);
                    if (val != null) {
                        values.append(DELIMITER + val);
                    } else {
                        values.append(DELIMITERANDSPACE);
                    }
                }
                stlog.log(values.toString());
                values.setLength(0);
                lastCallException = null;
            }
        }
        catch (IOException e) {
            if (lastCallException == null) {
                LoggerUtils.warning(logger, envImpl,
                    "Error accessing statistics capture file " +
                    STATFILENAME + "." + STATFILEEXT +
                    " IO Exception: " + e.getMessage());
            }
            lastCallException = e;
        }
        catch (Exception e) {
            if (lastCallException == null) {
                LoggerUtils.warning(logger, envImpl,
                    "Error accessing or writing statistics capture file  " +
                    STATFILENAME + "." + STATFILEEXT + e + "\n" +
                    LoggerUtils.getStackTrace(e));
            }
            lastCallException = e;
        }
    }

    private SortedMap<String, String> getStats() {
        final Collection<StatGroup> envStats = new ArrayList<StatGroup>(
            statMgr.loadStats(statsConfig, statKey).getStatGroups());

        if (envImpl.isReplicated()) {
            Collection<StatGroup> rsg =
                envImpl.getRepStatGroups(statsConfig, statKey);
            if (rsg != null) {
                envStats.addAll(rsg);
            }
        }

        envStats.add(jvmstats.loadStats(statsConfig));

        SortedMap<String, String> statsMap = new TreeMap<String, String>();

        for (StatGroup sg : envStats) {

            for (Entry<StatDefinition, Stat<?>> e :
                 sg.getStats().entrySet()) {

                final String mapName =
                    (sg.getName() + ":" + e.getKey().getName()).intern();
                final Stat<?> stat = e.getValue();
                if (stat.isNotSet()) {
                    statsMap.put(mapName, " ");
                    continue;
                }

                final Object val = stat.get();

                /* get stats back as strings. */
                final String str;
                if ((val instanceof Float) || (val instanceof Double)) {
                    str = String.format("%.2f", val);
                } else if (val instanceof Number) {
                    str = Long.toString(((Number) val).longValue());
                } else if (val != null) {
                    str = String.valueOf(val);
                } else {
                    str = " ";
                }
                statsMap.put(mapName, str);
            }
        }

        if (customStats != null) {
            String vals[] = customStats.getFieldValues();
            for (int i = 0; i < vals.length; i++) {
                statsMap.put(customStatHeader[i], vals[i]);
            }
        }
        return statsMap;
    }

    public void envConfigUpdate(DbConfigManager configMgr,
                                EnvironmentMutableConfig unused)
                                throws DatabaseException {

        setWaitTime(configMgr.getDuration(
            EnvironmentParams.STATS_COLLECT_INTERVAL));

        if (envImpl.isReadOnly() || envImpl.isMemOnly() ||
            !configMgr.getBoolean(EnvironmentParams.STATS_COLLECT)) {
            stlog = null;
            return;
        }

        final int maxFiles =
            configMgr.getInt(EnvironmentParams.STATS_MAX_FILES);

        final int fileRowCount =
            configMgr.getInt(EnvironmentParams.STATS_FILE_ROW_COUNT);

        if (stlog == null) {

            final String statdir =
                configMgr.get(EnvironmentParams.STATS_FILE_DIRECTORY);

            final File statDir;

            if (statdir == null || statdir.equals("")) {
                statDir = envImpl.getEnvironmentHome();
            } else {
                statDir = new File(statdir);

                if (!statDir.exists()) {
                    /* TODO: require the user to create the directory. */
                    statDir.mkdirs();
                } else if (!statDir.isDirectory()) {
                    throw new IllegalArgumentException(
                        "Specified statistic log directory " +
                        statDir.getAbsolutePath() + " is not a directory.");
                }
            }

            try {
                stlog = new StatLogger(
                    statDir, STATFILENAME, STATFILEEXT,
                    maxFiles, fileRowCount);

            } catch (IOException e) {
                throw new IllegalStateException(
                    " Error accessing statistics capture file " +
                    STATFILENAME + "." + STATFILEEXT +
                    " IO Exception: " + e.getMessage());
            }
        } else {
            stlog.setFileCount(maxFiles);
            stlog.setRowCount(fileRowCount);
        }
    }
}
