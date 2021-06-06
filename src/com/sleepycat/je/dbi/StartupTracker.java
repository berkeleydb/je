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

package com.sleepycat.je.dbi;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.EnumMap;
import java.util.Formatter;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.recovery.CheckpointEnd;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Store and calculate elapsed time, counts, and other statistics about
 * environment open. No synchronization is used, which generally works because
 * with the exception of replication, environment startup is currently a
 * serial, single threaded event. Replicated environments must be sure to
 * record startup times only at thread safe points.
 */
public class StartupTracker {

    /* 
     * Statistics are kept about startup phases, defined below. Phases can
     * be nested, so the child and root fields are used to express this
     * relationship. For example:
     *  TotalEnvOpen
     *      TotalRecovery
     *         FindEndOfLog
     *          ..
     *         BuildTree
     *             ReadMapIN 
     *             ..
     *         Ckpt
     *  TotalJoinGroup encompasses the following two phases. 
     *      FindMaster
     *      BecomeConsistent
     * Keep these enums in order of execution, so that the display is easier to
     * comprehend. Of course, some phases subsume other phases, but in general,
     * this enum order follows the order of execution.
     */
    public enum Phase {
            TOTAL_ENV_OPEN("Environment Open"),
            TOTAL_RECOVERY,
            FIND_END_OF_LOG,
            FIND_LAST_CKPT, 
            BUILD_TREE,
            READ_MAP_INS,
            REDO_MAP_INS,
            UNDO_MAP_LNS,
            REDO_MAP_LNS,
            READ_INS,
            REDO_INS,
            UNDO_LNS,
            REDO_LNS,
            POPULATE_UP, 
            POPULATE_EP,
            REMOVE_TEMP_DBS,
            CKPT,
            TOTAL_JOIN_GROUP("Replication Join Group"),
            FIND_MASTER,
            BECOME_CONSISTENT;

        private Phase[] children;
        private Phase root;
        private String reportLabel;

        private Phase() {
        }

        private Phase(String reportLabel) {
            this.reportLabel = reportLabel;
        }

        static {
            TOTAL_ENV_OPEN.children = new Phase[] 
                {TOTAL_RECOVERY};
            TOTAL_RECOVERY.children = new Phase[] 
                {FIND_END_OF_LOG,
                 FIND_LAST_CKPT,
                 BUILD_TREE,
                 POPULATE_UP,
                 POPULATE_EP,
                 REMOVE_TEMP_DBS,
                 CKPT};
            BUILD_TREE.children = new Phase[] 
                {READ_MAP_INS,
                 REDO_MAP_INS,
                 UNDO_MAP_LNS,
                 REDO_MAP_LNS,
                 READ_INS,
                 REDO_INS,
                 UNDO_LNS,
                 REDO_LNS};
            TOTAL_JOIN_GROUP.children = new Phase[] 
                {FIND_MASTER, 
                 BECOME_CONSISTENT};

            TOTAL_RECOVERY.root = TOTAL_ENV_OPEN;
            FIND_END_OF_LOG.root = TOTAL_ENV_OPEN;
            FIND_LAST_CKPT.root = TOTAL_ENV_OPEN; 
            BUILD_TREE.root = TOTAL_ENV_OPEN;
            READ_MAP_INS.root = TOTAL_ENV_OPEN;
            REDO_MAP_INS.root = TOTAL_ENV_OPEN;
            UNDO_MAP_LNS.root = TOTAL_ENV_OPEN;
            REDO_MAP_LNS.root = TOTAL_ENV_OPEN;
            READ_INS.root = TOTAL_ENV_OPEN;
            REDO_INS.root = TOTAL_ENV_OPEN;
            UNDO_LNS.root = TOTAL_ENV_OPEN;
            REDO_LNS.root = TOTAL_ENV_OPEN;
            POPULATE_UP.root = TOTAL_ENV_OPEN; 
            POPULATE_EP.root = TOTAL_ENV_OPEN;
            REMOVE_TEMP_DBS.root = TOTAL_ENV_OPEN;
            CKPT.root = TOTAL_ENV_OPEN;

            FIND_MASTER.root = TOTAL_JOIN_GROUP;
            BECOME_CONSISTENT.root = TOTAL_JOIN_GROUP;
        }
    }

    private final Map<Phase, Elapsed> elapsed;
    private final Map<Phase, Counter> counters;
    private final Map<Phase, StatGroup> stats;
    private final Logger logger;
    private final EnvironmentImpl envImpl;
    private RecoveryInfo info;
    private long lastDumpMillis;

    public StartupTracker(EnvironmentImpl envImpl) {

        elapsed = new EnumMap<Phase, Elapsed>(Phase.class);
        counters = new EnumMap<Phase, Counter>(Phase.class);
        stats = new EnumMap<Phase, StatGroup>(Phase.class);
        for (Phase p : Phase.values()){
            elapsed.put(p, new Elapsed());
        }

        this.envImpl = envImpl;

        logger = LoggerUtils.getLogger(getClass());
        lastDumpMillis = System.currentTimeMillis();
    }

    public void setRecoveryInfo(RecoveryInfo rInfo) {
        info = rInfo;
    }

    /**
     * Note that a particular phase is starting. 
     */
    public void start(Phase phase) {
        String msg = "Starting " + phase;
        if (info != null) {
            msg += " " + info;
        }
        LoggerUtils.logMsg(logger, envImpl, Level.CONFIG, msg);

        elapsed.get(phase).start();
        Counter c = new Counter();
        counters.put(phase, c);       
        if (!phase.equals(Phase.TOTAL_ENV_OPEN)) {

            /* 
             * LogManager does not exist yet so we can't reference it. Anyway,
             * cache misses are 0 to start with, so TOTAL_ENV_OPEN does not 
             * have to set the starting cache miss count.
             */
            c.setCacheMissStart(envImpl.getLogManager().getNCacheMiss());
        }
    }
    
    /**
     * Note that a particular phase is ending.
     */
    public void stop(Phase phase) {
        Elapsed e = elapsed.get(phase);
        e.end();
        Counter c = getCounter(phase);
        c.setCacheMissEnd(envImpl.getLogManager().getNCacheMiss());

        /* Log this phase to the je.info file. */
        String msg = "Stopping " + phase;
        if (info != null) {
            msg += " " + info;
        }
        LoggerUtils.logMsg(logger, envImpl, Level.CONFIG, msg);

        /*
         * Conditionally log the whole report to the je.info file, either 
         * because this family of phases has ended, or because this startup
         * is taking a very long time.
         *
         * Take care to only check the value of dumpThreshold here, rather than
         * setting it in the StartupTracker constructor, because StartupTracker
         * is instantiated before the DbConfigManager and the
         * STARTUP_DUMP_THRESHOLD param cannot be read.
         */
        int dumpThreshold = envImpl.getConfigManager().getDuration
            (EnvironmentParams.STARTUP_DUMP_THRESHOLD);

        /* We're at the end of a family of phases. */
        if (phase.root == null) {
            if ((e.getEnd() - e.getStart()) > dumpThreshold) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream p = new PrintStream(baos);
                displayStats(p, phase);
                LoggerUtils.logMsg(logger, envImpl, Level.INFO,
                                   baos.toString());
                return;
            }
        }
         
        /* 
         * It's not the ending phase, but this has been taking a very long
         * time, so dump some information.
         */
         if ((System.currentTimeMillis() - lastDumpMillis) > dumpThreshold) {
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             PrintStream p = new PrintStream(baos);
             displayInterim(p, phase);
             LoggerUtils.logMsg(logger, envImpl, Level.INFO, baos.toString());
         }
    }

    /**
     * Record new progress states for any registered environment progress
     * listener.
     */
    public void setProgress(RecoveryProgress progress) {
        ProgressListener<RecoveryProgress> progressListener = 
            envImpl.getRecoveryProgressListener();

        if (progressListener == null) {
            return;
        }
        if (!progressListener.progress(progress, -1, -1)) {
            throw new EnvironmentFailureException
              (envImpl, EnvironmentFailureReason.PROGRESS_LISTENER_HALT,
              "EnvironmentConfig.recoveryProgressListener: ");      
        }
    }

    /**
     * Return the counter for this phase so we can update one of the detail
     * values stored there.
     */
    public Counter getCounter(Phase phase) {
        return counters.get(phase);
    }

    /** 
     * Save stats for a given phase. 
     */
    public void setStats(Phase phase, StatGroup sg) {
        stats.put(phase, sg);
    }

    /** 
     * Generate a description of the four recovery locations (firstActive,
     * ckptStart, ckptend, end of Log) and the distance inbetween.
     */
    private String displayRecoveryInterval() {
        StringBuilder returnInfo = new StringBuilder();

        CheckpointEnd cEnd = info.checkpointEnd;
        if (cEnd != null) {
            returnInfo.append("checkpointId = ");
            returnInfo.append(cEnd.getId());
            if (cEnd.getInvoker() == null) {
                returnInfo.append(" ");
            } else {
                returnInfo.append("[").append(cEnd.getInvoker());
                returnInfo.append("] ");
            }
        }

        long fileMax =
            envImpl.getConfigManager().getLong(EnvironmentParams.LOG_FILE_MAX);

        long useStart = info.checkpointStartLsn == DbLsn.NULL_LSN ?
            0 : info.checkpointStartLsn;
        long head = DbLsn.getNoCleaningDistance(useStart,
                                                info.firstActiveLsn,
                                                fileMax);

        long useEnd = info.checkpointEndLsn == DbLsn.NULL_LSN ?
            0 : info.checkpointEndLsn;
        long ckpt = DbLsn.getNoCleaningDistance(useEnd,
                                                info.checkpointStartLsn,
                                                fileMax);

        long useLast = info.lastUsedLsn == DbLsn.NULL_LSN ?
            0 : info.lastUsedLsn;
        long tail = DbLsn.getNoCleaningDistance(useLast,
                                                info.checkpointEndLsn,
                                                fileMax);
        returnInfo.append(
            "firstActive[" + 
            DbLsn.getNoFormatString(info.firstActiveLsn) +
            "], ckptStart[" +
            DbLsn.getNoFormatString(info.checkpointStartLsn) +
            "], ckptEnd[" +
            DbLsn.getNoFormatString(info.checkpointEndLsn) +
            "], lastUsed[" +
            DbLsn.getNoFormatString(info.lastUsedLsn) +
            "]\n");
        StringBuilder sb = new StringBuilder();
        Formatter f = new Formatter(sb);
        f.format("%24s bytes = %,d\n%24s bytes = %,d\n%24s bytes = %,d",
                 "firstActive->ckptStart", head,
                 "ckptStart->ckptEnd", ckpt,
                 "ckptEnd->end bytes", tail);
        
        return returnInfo.toString() + "\nApproximate distances:\n" + 
            sb.toString();
    }

    private String displayTimestamp(Long time) {
        StringBuilder sb = new StringBuilder();
        Formatter timestampFormatter = new Formatter(sb);
        timestampFormatter.format("%tD,%tH:%tM:%tS:%tL", 
                                  time, time, time, time, time);
        return sb.toString();
    }

    /**
     * Display a phase and its children, showing elapsed time as a  
     * percentage of the phases' root.
     */
    private void displayPhaseSubtree(PrintStream stream,
                                     Phase parent,
                                     Elapsed parentTime,
                                     Elapsed rootElapsed) {

        String headerFormat = "%24s  %% of total  %s\n";
        String parentFormat = "%20s             %3d %s\n";
        String dataFormat   = "%24s         %3d %s\n";
        String divider = "                         "+
            "-------------------------";

        if (parent.children == null) {
            return;
        }

        if ((parentTime.getEnd() - parentTime.getStart()) ==0) {
            return;
        }

        stream.println("\n");
        stream.printf(headerFormat, " ", Elapsed.DISPLAY_COLUMNS);
        stream.printf(parentFormat, parent, 
                      parentTime.getPercentage(rootElapsed), parentTime);
        stream.println(divider);

        for (Phase child : parent.children) {
            Elapsed time = elapsed.get(child);
            if (time.getStart() == 0) {
                continue;
            }
            stream.printf(dataFormat, 
                          child,
                          time.getPercentage(rootElapsed),
                          time);
        }
    }

    private void displayCounters(PrintStream stream, Phase root) {
        String basicFormat = "%20s   %s\n";
        boolean headerNotPrinted = true;
        for (Map.Entry<Phase, Counter> c : counters.entrySet()) {
            Phase p = c.getKey();
            if (p.root != root) {
                continue;
            }
            Counter counter = c.getValue();
            if (counter.isEmpty()) {
                continue;
            }

            if (headerNotPrinted) {
                stream.println();
                stream.printf(basicFormat, " " , Counter.DISPLAY_COLUMNS);
                headerNotPrinted = false;
            }
            stream.printf(basicFormat, c.getKey(), counter);
        }
    }
    
    /**
     * Display all information that has been tracked for this family of
     * phases.
     */
    public void displayStats(PrintStream stream, Phase root ) {
        lastDumpMillis = System.currentTimeMillis();
        Elapsed rootTime = elapsed.get(root);

        stream.println("\n=== " + root.reportLabel + " Report  ===");
        stream.println("start = " + displayTimestamp(rootTime.getStart()));
        stream.println("end   = " + displayTimestamp(rootTime.getEnd()));
        if (root == Phase.TOTAL_ENV_OPEN) {
            stream.print(displayRecoveryInterval());
        }

        /* Elapsed time. */
        for (Map.Entry<Phase, Elapsed> x : elapsed.entrySet()) {
            Phase p = x.getKey();
            if (p.root == null) {
               if (p != root) {
                   continue;
               }
            } else if (p.root != root) {
               continue;
            }
            
            displayPhaseSubtree(stream, x.getKey(),x.getValue(), rootTime);
        }

        /* Counters */
        displayCounters(stream, root);

        /* Stats */
        for (Map.Entry<Phase, StatGroup> s : stats.entrySet()) {
            Phase p = s.getKey();
            if (p.root != root) {
                continue;
            }
            stream.println(s.getKey() + " stats:");
            stream.println(s.getValue());
        }
    }

    /**
     * Display all information available so far.
     */
    private void displayInterim(PrintStream stream, Phase phase ) {
        lastDumpMillis = System.currentTimeMillis();

        stream.println("\n=== Interim " + phase + " Report  ===");

        stream.println(displayRecoveryInterval());

        /* Elapsed time. */
        boolean headerNotPrinted = true;
        for (Map.Entry<Phase, Elapsed> x : elapsed.entrySet()) {
            Phase p = x.getKey();
            Elapsed e = x.getValue();
            if (e.start == 0) {
                continue;
            }
            if (headerNotPrinted) {
                stream.println("                             Elapsed(ms)");
                headerNotPrinted = false;
            }
            stream.printf("%20s : %s\n", p, e);
        }

        /* Counters */
        displayCounters(stream, phase.root);

        /* Stats */
        for (Map.Entry<Phase, StatGroup> s : stats.entrySet()) {
            stream.println(s.getKey() + " stats:");
            stream.println(s.getValue());
        }
    }

    /** Measures elapsed time in millisecond granularity. */
    static private class Elapsed {

        /* For dumping elapsed values in a column */
        static String DISPLAY_COLUMNS = " Elapsed(ms)";

        private long start;
        private long end;

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        /* Mark the start of a phase. */
        private void start() {
            start = System.currentTimeMillis();
        }

        /* Mark the end of a phase. */
        private void end() {
            end = System.currentTimeMillis();
        }

        private int getPercentage(Elapsed rootTime) {
            if (rootTime == null) {
                return 0;
            } 

            long rootTotal = rootTime.end-rootTime.start;
            if (rootTotal <= 0) {
                return 0;
            }

            if (end == 0) {
                return 0;
            }
            return (int)(((float) (end-start)/ rootTotal) * 100);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            Formatter f = new Formatter(sb);
            if (end != 0) {
                f.format("%,13d", (end-start));
            } else {
                if (start != 0) {
                    f.format("%13s  %tD,%tH:%tM:%tS:%tL",
                             "started at", start, start, start, start, start);
                } else {
                    f.format("%13s", "none");
                }
            }
            return sb.toString(); 
        }
    }

    /**
     * Record number of log entries processed during a given recovery phase.
     */
    static public class Counter {
        private int numRead;
        private int numProcessed;
        private int numDeleted;
        private int numAux;
        private long numRepeatIteratorReads;
        private long startCacheMiss;
        private long endCacheMiss;

        /* If nothing is set, don't print this one. */
        private boolean isEmpty() {
            return((numRead==0) &&
                   (numProcessed==0) &&
                   (numDeleted==0) &&
                   (numAux==0) &&
                   (numRepeatIteratorReads==0) &&
                   ((endCacheMiss-startCacheMiss)==0));
        }

        public void incNumRead() {
            numRead++;
        }

        public void incNumProcessed() {
            numProcessed++;
        }

        public void incNumDeleted() {
            numDeleted++;
        }

        /**
         * Keep track of auxiliary log entries processed during this pass. 
         * For example, LNs are the main target of the undoLN pass, but we
         * also read aborts and commits.
         */
        public void incNumAux() {
            numAux++;
        }

        public void setRepeatIteratorReads(long repeats) {
            numRepeatIteratorReads = repeats;
        }

        public void setCacheMissStart(long miss) {
            startCacheMiss = miss;
        }

        public void setCacheMissEnd(long miss) {
            endCacheMiss = miss;
        }

        static String DISPLAY_COLUMNS =
"      nRead nProcessed   nDeleted       nAux  nRepeatRd nCacheMiss";
 
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            Formatter f = new Formatter(sb);
            f.format("%,11d%,11d%,11d%,11d%,11d%,11d",
                     numRead, numProcessed, numDeleted, numAux, 
                     numRepeatIteratorReads, (endCacheMiss - startCacheMiss));
            return sb.toString();
        }

        public int getNumProcessed() {
            return numProcessed;
        }
    }
}
