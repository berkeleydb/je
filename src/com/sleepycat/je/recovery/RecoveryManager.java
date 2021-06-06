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

package com.sleepycat.je.recovery;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.cleaner.RecoveryUtilizationTracker;
import com.sleepycat.je.cleaner.ReservedFileInfo;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.StartupTracker;
import com.sleepycat.je.dbi.StartupTracker.Counter;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.log.CheckpointFileReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.INFileReader;
import com.sleepycat.je.log.LNFileReader;
import com.sleepycat.je.log.LastFileReader;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.NameLNLogEntry;
import com.sleepycat.je.recovery.RollbackTracker.Scanner;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.TrackingInfo;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.PreparedTxn;
import com.sleepycat.je.txn.RollbackEnd;
import com.sleepycat.je.txn.RollbackStart;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnChain.RevertInfo;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Performs recovery when an Environment is opened.
 *
 * TODO: Need a description of the recovery algorithm here.  For some related
 * information, see the Checkpointer class comments.
 *
 * Recovery, the INList and Eviction
 * =================================
 * There are two major steps in recovery: 1) recover the mapping database and
 * the INs for all other databases, 2) recover the LNs for the other databases.
 * In the buildTree method, step 1 comes before the call to buildINList and
 * step 2 comes after that.  The INList is not maintained in step 1.
 *
 * The INList is not maintained in the step 1 because there is no benefit -- we
 * cannot evict anyway as explained below -- and there are potential drawbacks
 * to maintaining it: added complexity and decreased performance.  The
 * drawbacks are described in more detail further below.
 *
 * Even if the INList were maintained in step 1, eviction could not be enabled
 * until step 2, because logging is not allowed until all the INs are in place.
 * In principle we could evict non-dirty nodes in step 1, but since recovery is
 * dirtying the tree as it goes, there would be little or nothing that is
 * non-dirty and could be evicted.
 *
 * Therefore, the INList has an 'enabled' mode that is initially false (in step
 * 1) and is set to true by buildINList, just before step 2.  The mechanism for
 * adding nodes to the INList is skipped when it is disabled.  In addition to
 * enabling it, buildINList populates it from the contents of the Btrees that
 * were constructed in step 1.  In step 2, eviction is invoked explicitly by
 * calling EnvironmentImpl.invokeEvictor often during recovery.  This is
 * important since the background evictor thread is not yet running.
 *
 * An externally visible limitation created by this situation is that the nodes
 * placed in the Btree during step 1 must all fit in memory, since no eviction
 * is performed.  So memory is a limiting factor in how large a recovery can be
 * performed.  Since eviction is allowed in step 2, and step 2 is where the
 * bulk of the recovery is normally performed, this limitation of step 1 hasn't
 * been a critical problem.
 *
 * Maintaining the INList
 * ----------------------
 * In this section we consider the impact of maintaining the INList in step 1,
 * if this were done in a future release.  It is being considered for a future
 * release so we can rely on the INList to reference INs by node ID in the
 * in-memory representation of an IN (see the Big Memory SR [#22292]).
 *
 * To maintain the INList in step 1, when a branch of a tree (a parent IN) is
 * spliced in, the previous branch (all of the previous node's children) would
 * have to be removed from the INList.  Doing this incorrectly could cause an
 * OOME, and it may also have a performance impact.
 *
 * The performance impact of removing the previous branch from the INList is
 * difficult to estimate.  In the normal case (recovery after normal shutdown),
 * very few nodes will be replaced because normally only nodes at the max flush
 * level are replayed, and the slots they are placed into will be empty (not
 * resident).  Here is description of a worst case scenario, which is when
 * there is a crash near the end of a checkpoint:
 *
 *  + The last checkpoint is large, includes all nodes in the tree, is mostly
 *    complete, but was not finished (no CkptEnd).  The middle INs (above BIN
 *    and below the max flush level) must be replayed (see Checkpointer and
 *    Provisional.BEFORE_CKPT_END).
 *
 *  + For these middle INs, the INs at each level are placed in the tree and
 *    replace any IN present in the slot.  For the bottom-most level of middle
 *    INs (level 2), these don't replace a node (the slot will be empty because
 *    BINs are never replayed).  But for the middle INs in all levels above
 *    that, they replace a node that was fetched earlier; it was fetched
 *    because it is the parent of a node at a lower level that was replayed.
 *
 *  + In the worst case, all INs from level 3 to R-1, where R is the root
 *    level, would be replayed and replace a node.  However, it seems the
 *    replaced node would not have resident children in the scenario described,
 *    so the cost of removing it from the INList does not seem excessive.
 *
 *  + Here's an example illustrating this scenario.  The BINs and their parents
 *    (as a sub-tree) are logged first, followed by all dirty INs at the next
 *    level, etc.
 *
 *    0050 CkptStart
 *    0100 BIN level 1
 *    0200 BIN level 1
 *    ...
 *    1000 IN level 2, parent of 0100, 0200, etc.
 *    1100 BIN level 1
 *    1200 BIN level 1
 *    ...
 *    2000 IN level 2, parent of 1100, 1200, etc.
 *    ...
 *    7000 IN level 2, last level 2 IN logged
 *    8000 IN level 3, parent of 1000, 2000, etc.
 *    ...
 *    9000 IN level 4, parent of 8000, etc.
 *    ...
 *    
 *                  9000       level 4
 *                 /
 *            ----8000----     level 3
 *           /  /         \
 *       1000  2000    ......  level 2
 *
 *                             BINs not shown
 *
 *    Only the root (if it happens to be logged right before the crash) is
 *    non-provisional.  We'll assume in this example that the root was not
 *    logged.  Level 2 through R-1 are logged as Provisional.BEFORE_CKPT_END,
 *    and treated as non-provisional (replayed) by recovery because there is no
 *    CkptEnd.
 *
 *    When 1000 (and all other nodes at level 2) is replayed, it is placed into
 *    an empty slot.
 *
 *    When 8000 (and all other INs at level 3 and higher, below the root) is
 *    replayed, it will replace a resident node that was fetched and placed in
 *    the slot when replaying its children.  The replaced node is one not
 *    shown, and assumed to have been logged sometime prior to this checkpoint.
 *    The replaced node will have all the level 2 nodes that were replayed
 *    earlier (1000, 2000, etc.) as its resident children, and these are the
 *    nodes that would have to be removed from the INList, if recovery were
 *    changed to place INs on the INList in step 1.
 *
 *    So if the INs were placed on the INList, in this worst case scenario, all
 *    INs from level 3 to R-1 will be replayed, and all their immediate
 *    children would need to be removed from the INList.  Grandchildren would
 *    not be resident.  In other words, all nodes at level 2 and above (except
 *    the root) would be removed from the INList and replaced by a node being
 *    replayed.
 *
 * When there is a normal shutdown, we don't know of scenarios that would cause
 * this sort of INList thrashing.  So perhaps maintaining the INList in step 1
 * could be justified, if the additional recovery cost after a crash is
 * acceptable.
 *
 * Or, a potential solution for the worst case scenario above might be to place
 * the resident child nodes in the new parent, rather than discarding them and
 * removing them from the INList.  This would have the benefit of populating
 * the cache and not wasting the work done to read and replay these nodes.
 * OTOH, it may cause OOME if too much of the tree is loaded in step 1.
 */
public class RecoveryManager {
    private static final String TRACE_LN_REDO = "LNRedo:";
    private static final String TRACE_LN_UNDO = "LNUndo";
    private static final String TRACE_IN_REPLACE = "INRecover:";
    private static final String TRACE_ROOT_REPLACE = "RootRecover:";

    private final EnvironmentImpl envImpl;
    private final int readBufferSize;
    private final RecoveryInfo info;                // stat info
    /* Committed txn ID to Commit LSN */
    private final Map<Long, Long> committedTxnIds;
    private final Set<Long> abortedTxnIds;             // aborted txns
    private final Map<Long, PreparedTxn> preparedTxns; // txnid -> prepared Txn

    /*
     * A set of lsns for log entries that will be resurrected is kept so that
     * we can correctly redo utilization. See redoUtilization()
     */
    private final Set<Long> resurrectedLsns;

    /* dbs for which we have to build the in memory IN list. */
    private final Set<DatabaseId> inListBuildDbIds;

    private final Set<DatabaseId> tempDbIds;        // temp DBs to be removed

    private final Set<DatabaseId> expectDeletedMapLNs;

    /*
     * Reserved file db records in the recovery interval are tracked in order
     * to redo MapLN updates at the end of recovery.
     */
    private final Set<Long> reservedFiles;
    private final Set<DatabaseId> reservedFileDbs;

    /* Handles rollback periods created by HA syncup. */
    private final RollbackTracker rollbackTracker;

    private final RecoveryUtilizationTracker tracker;
    private final StartupTracker startupTracker;
    private final Logger logger;

    /* DBs that may violate the rule for upgrading to log version 8. */
    private final Set<DatabaseId> logVersion8UpgradeDbs;

    /* Whether deltas violate the rule for upgrading to log version 8. */
    private final AtomicBoolean logVersion8UpgradeDeltas;

    /* Used to recalc disk usage to prevent eviction from violating limits. */
    private int nOpsSinceDiskLimitRecalc = 0;

    /**
     * Make a recovery manager
     */
    public RecoveryManager(EnvironmentImpl env)
        throws DatabaseException {

        this.envImpl = env;
        DbConfigManager cm = env.getConfigManager();
        readBufferSize =
            cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        committedTxnIds = new HashMap<>();
        abortedTxnIds = new HashSet<>();
        preparedTxns = new HashMap<>();
        resurrectedLsns = new HashSet<>();
        inListBuildDbIds = new HashSet<>();
        tempDbIds = new HashSet<>();
        expectDeletedMapLNs = new HashSet<>();
        reservedFiles = new HashSet<>();
        reservedFileDbs = new HashSet<>();
        tracker = new RecoveryUtilizationTracker(env);
        logger = LoggerUtils.getLogger(getClass());
        rollbackTracker = new RollbackTracker(envImpl);
        info = new RecoveryInfo();
        logVersion8UpgradeDbs = new HashSet<>();
        logVersion8UpgradeDeltas = new AtomicBoolean(false);

        startupTracker = envImpl.getStartupTracker();
        startupTracker.setRecoveryInfo(info);
    }

    /**
     * Look for an existing log and use it to create an in memory structure for
     * accessing existing databases. The file manager and logging system are
     * only available after recovery.
     * @return RecoveryInfo statistics about the recovery process.
     */
    public RecoveryInfo recover(boolean readOnly)
        throws DatabaseException {

        startupTracker.start(Phase.TOTAL_RECOVERY);
        try {
            FileManager fileManager = envImpl.getFileManager();
            DbConfigManager configManager = envImpl.getConfigManager();
            boolean forceCheckpoint;

            /*
             * After a restore from backup we must flip the file on the first
             * write.  The lastFileInBackup must be immutable.  [#22834]
             */
            if (configManager.getBoolean(
                EnvironmentParams.ENV_RECOVERY_FORCE_NEW_FILE)) {
                fileManager.forceNewLogFile();
                /* Must write something to create new file.*/
                forceCheckpoint = true;
            } else {
                forceCheckpoint = configManager.getBoolean(
                    EnvironmentParams.ENV_RECOVERY_FORCE_CHECKPOINT);
            }

            if (fileManager.filesExist()) {

                /* 
                 * Check whether log files are correctly located in the sub 
                 * directories. 
                 */
                fileManager.getAllFileNumbers();

                /*
                 * Establish the location of the end of the log.  Log this
                 * information to the java.util.logging logger, but delay
                 * tracing this information in the .jdb file, because the
                 * logging system is not yet initialized. Because of that, be
                 * sure to use lazy logging, and do not use
                 * LoggerUtils.logAndTrace().
                 */
                findEndOfLog(readOnly);

                String endOfLogMsg = "Recovery underway, valid end of log = " +
                    DbLsn.getNoFormatString(info.nextAvailableLsn);

                Trace.traceLazily(envImpl, endOfLogMsg);

                /*
                 * Establish the location of the root, the last checkpoint, and
                 * the first active LSN by finding the last checkpoint.
                 */
                findLastCheckpoint();

                envImpl.getLogManager().setLastLsnAtRecovery
                    (fileManager.getLastUsedLsn());

                /* Read in the root. */
                envImpl.readMapTreeFromLog(info.useRootLsn);

                /* Build the in memory tree from the log. */
                buildTree();
            } else {

                /*
                 * Nothing more to be done. Enable publishing of debug log
                 * messages to the database log.
                 */
                LoggerUtils.logMsg
                    (logger, envImpl, Level.CONFIG, "Recovery w/no files.");

                /* Enable the INList and log the root of the mapping tree. */
                envImpl.getInMemoryINs().enable();
                envImpl.getEvictor().setEnabled(true);
                /* Do not write LNs in a read-only environment. */
                if (!readOnly) {
                    envImpl.logMapTreeRoot();
                }

                /* Add shared cache environment when buildTree is not used. */
                if (envImpl.getSharedCache()) {
                    envImpl.getEvictor().addEnvironment(envImpl);
                }

                /*
                 * Always force a checkpoint during creation.
                 */
                forceCheckpoint = true;
            }

            int ptSize = preparedTxns.size();
            if (ptSize > 0) {
                boolean singular = (ptSize == 1);
                LoggerUtils.logMsg(logger, envImpl, Level.INFO,
                                   "There " + (singular ? "is " : "are ") +
                                   ptSize + " prepared but unfinished " +
                                   (singular ? "txn." : "txns."));

                /*
                 * We don't need this set any more since these are all
                 * registered with the TxnManager now.
                 */
                preparedTxns.clear();
            }

            final EnvironmentConfig envConfig =
                envImpl.getConfigManager().getEnvironmentConfig();

            /* Use of cleaner DBs may be disabled for unittests. */
            if (DbInternal.getCreateUP(envConfig)) {

                /*
                 * Open the file summary DB and populate the cache before the
                 * first checkpoint so that the checkpoint may flush file
                 * summary information.
                 */
                startupTracker.start(Phase.POPULATE_UP);

                startupTracker.setProgress(
                    RecoveryProgress.POPULATE_UTILIZATION_PROFILE);

                forceCheckpoint |=
                    envImpl.getUtilizationProfile().populateCache(
                        startupTracker.getCounter(Phase.POPULATE_UP),
                        info, reservedFiles, reservedFileDbs);

                startupTracker.stop(Phase.POPULATE_UP);
            }
            if (DbInternal.getCreateEP(envConfig)) {
                /*
                 * Open the file expiration DB, populate the expiration
                 * profile, and initialize the current expiration tracker.
                 */
                startupTracker.start(Phase.POPULATE_EP);

                startupTracker.setProgress(
                    RecoveryProgress.POPULATE_EXPIRATION_PROFILE);

                envImpl.getExpirationProfile().populateCache(
                    startupTracker.getCounter(Phase.POPULATE_EP),
                    envImpl.getRecoveryProgressListener());

                startupTracker.stop(Phase.POPULATE_EP);
            }

            /* Transfer recovery utilization info to the global tracker. */
            tracker.transferToUtilizationTracker(
                envImpl.getUtilizationTracker());

            /*
             * After utilization info is complete and prior to the checkpoint,
             * remove all temporary databases encountered during recovery.
             */
            removeTempDbs();

            /*
             * For truncate/remove NameLNs with no corresponding deleted MapLN
             * found, delete the MapLNs now.
             */
            deleteMapLNs();

            /*
             * Execute any replication initialization that has to happen before
             * the checkpoint.
             */
            envImpl.preRecoveryCheckpointInit(info);

            /*
             * At this point, we've recovered (or there were no log files at
             * all). Write a checkpoint into the log.
             */
            if (!readOnly &&
                ((envImpl.getLogManager().getLastLsnAtRecovery() !=
                  info.checkpointEndLsn) ||
                 forceCheckpoint)) {

                CheckpointConfig config = new CheckpointConfig();
                config.setForce(true);
                config.setMinimizeRecoveryTime(true);

                startupTracker.setProgress(RecoveryProgress.CKPT);
                startupTracker.start(Phase.CKPT);
                try {
                    envImpl.invokeCheckpoint(config, "recovery");
                } catch (DiskLimitException e) {
                    LoggerUtils.logMsg(logger, envImpl, Level.WARNING,
                        "Recovery checkpoint failed due to disk limit" +
                        " violation but environment can still service reads: "
                        + e);
                }
                startupTracker.setStats
                    (Phase.CKPT,
                     envImpl.getCheckpointer().loadStats(StatsConfig.DEFAULT));
                startupTracker.stop(Phase.CKPT);
            } else {
                /* Initialize intervals when there is no initial checkpoint. */
                envImpl.getCheckpointer().initIntervals
                    (info.checkpointStartLsn, info.checkpointEndLsn,
                     System.currentTimeMillis());
            }
        } catch (IOException e) {
            LoggerUtils.traceAndLogException(envImpl, "RecoveryManager",
                                             "recover", "Couldn't recover", e);
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_READ, e);
        } finally {
            startupTracker.stop(Phase.TOTAL_RECOVERY);
        }

        return info;
    }

    /**
     * Find the end of the log, initialize the FileManager. While we're
     * perusing the log, return the last checkpoint LSN if we happen to see it.
     */
    private void findEndOfLog(boolean readOnly)
        throws IOException, DatabaseException {

        startupTracker.start(Phase.FIND_END_OF_LOG);
        startupTracker.setProgress(RecoveryProgress.FIND_END_OF_LOG);
        Counter counter = startupTracker.getCounter(Phase.FIND_END_OF_LOG);

        LastFileReader reader = new LastFileReader(envImpl, readBufferSize);

        /*
         * Tell the reader to iterate through the log file until we hit the end
         * of the log or an invalid entry.  Remember the last seen CkptEnd, and
         * the first CkptStart with no following CkptEnd.
         */
        while (reader.readNextEntry()) {
            counter.incNumRead();
            counter.incNumProcessed();

            LogEntryType type = reader.getEntryType();

            if (LogEntryType.LOG_CKPT_END.equals(type)) {
                info.checkpointEndLsn = reader.getLastLsn();
                info.partialCheckpointStartLsn = DbLsn.NULL_LSN;
            } else if (LogEntryType.LOG_CKPT_START.equals(type)) {
                if (info.partialCheckpointStartLsn == DbLsn.NULL_LSN) {
                    info.partialCheckpointStartLsn = reader.getLastLsn();
                }
            } else if (LogEntryType.LOG_DBTREE.equals(type)) {
                info.useRootLsn = reader.getLastLsn();
            } else if (LogEntryType.LOG_IMMUTABLE_FILE.equals(type)) {
                envImpl.getFileManager().forceNewLogFile();
            } else if (LogEntryType.LOG_RESTORE_REQUIRED.equals(type)) {
                /* 
                 * This log entry is a marker that indicates that the log is
                 * considered corrupt in some way, and recovery should not
                 * proceed. Some external action has to happen to obtain new
                 * log files that are coherent and can be recovered.
                 */
                envImpl.handleRestoreRequired(reader.getRestoreRequired());
            }
        }

        /*
         * The last valid LSN should point to the start of the last valid log
         * entry, while the end of the log should point to the first byte of
         * blank space, so these two should not be the same.
         */
        assert (reader.getLastValidLsn() != reader.getEndOfLog()):
        "lastUsed=" + DbLsn.getNoFormatString(reader.getLastValidLsn()) +
            " end=" + DbLsn.getNoFormatString(reader.getEndOfLog());

        /* Now truncate if necessary. */
        if (!readOnly) {
            reader.setEndOfFile();
        }

        /* Tell the fileManager where the end of the log is. */
        info.lastUsedLsn = reader.getLastValidLsn();
        info.nextAvailableLsn = reader.getEndOfLog();
        counter.setRepeatIteratorReads(reader.getNRepeatIteratorReads());
        envImpl.getFileManager().setLastPosition(info.nextAvailableLsn,
            info.lastUsedLsn,
            reader.getPrevOffset());
        startupTracker.stop(Phase.FIND_END_OF_LOG);
    }

    /**
     * Find the last checkpoint and establish the firstActiveLsn point,
     * checkpoint start, and checkpoint end.
     */
    private void findLastCheckpoint()
        throws IOException, DatabaseException {

        startupTracker.start(Phase.FIND_LAST_CKPT);
        startupTracker.setProgress(RecoveryProgress.FIND_LAST_CKPT);
        Counter counter = startupTracker.getCounter(Phase.FIND_LAST_CKPT);

        /*
         * The checkpointLsn might have been already found when establishing
         * the end of the log.  If it was found, then partialCheckpointStartLsn
         * was also found.  If it was not found, search backwards for it now
         * and also set partialCheckpointStartLsn.
         */
        if (info.checkpointEndLsn == DbLsn.NULL_LSN) {

            /*
             * Search backwards though the log for a checkpoint end entry and a
             * root entry.
             */
            CheckpointFileReader searcher =
                new CheckpointFileReader(envImpl, readBufferSize, false,
                                         info.lastUsedLsn, DbLsn.NULL_LSN,
                                         info.nextAvailableLsn);

            while (searcher.readNextEntry()) {
                counter.incNumRead();
                counter.incNumProcessed();

                /*
                 * Continue iterating until we find a checkpoint end entry.
                 * While we're at it, remember the last root seen in case we
                 * don't find a checkpoint end entry.
                 */
                if (searcher.isCheckpointEnd()) {

                    /*
                     * We're done, the checkpoint end will tell us where the
                     * root is.
                     */
                    info.checkpointEndLsn = searcher.getLastLsn();
                    break;
                } else if (searcher.isCheckpointStart()) {

                    /*
                     * Remember the first CkptStart following the CkptEnd.
                     */
                    info.partialCheckpointStartLsn = searcher.getLastLsn();

                } else if (searcher.isDbTree()) {

                    /*
                     * Save the last root that was found in the log in case we
                     * don't see a checkpoint.
                     */
                    if (info.useRootLsn == DbLsn.NULL_LSN) {
                        info.useRootLsn = searcher.getLastLsn();
                    }
                }
            }
            counter.setRepeatIteratorReads(searcher.getNRepeatIteratorReads());
        }

        /*
         * If we haven't found a checkpoint, we'll have to recover without
         * one. At a minimium, we must have found a root.
         */
        if (info.checkpointEndLsn == DbLsn.NULL_LSN) {
            info.checkpointStartLsn = DbLsn.NULL_LSN;
            info.firstActiveLsn = DbLsn.NULL_LSN;
        } else {
            /* Read in the checkpoint entry. */
            CheckpointEnd checkpointEnd = (CheckpointEnd)
                (envImpl.getLogManager().getEntry(info.checkpointEndLsn));
            info.checkpointEnd = checkpointEnd;
            info.checkpointStartLsn = checkpointEnd.getCheckpointStartLsn();
            info.firstActiveLsn = checkpointEnd.getFirstActiveLsn();

            /*
             * Use the last checkpoint root only if there is no later root.
             * The latest root has the correct per-DB utilization info.
             */
            if (checkpointEnd.getRootLsn() != DbLsn.NULL_LSN &&
                info.useRootLsn == DbLsn.NULL_LSN) {
                info.useRootLsn = checkpointEnd.getRootLsn();
            }

            /* Init the checkpointer's id sequence.*/
            envImpl.getCheckpointer().setCheckpointId(checkpointEnd.getId());
        }

        /*
         * Let the rollback tracker know where the checkpoint start is.
         * Rollback periods before the checkpoint start do not need to be
         * processed.
         */
        rollbackTracker.setCheckpointStart(info.checkpointStartLsn);

        startupTracker.stop(Phase.FIND_LAST_CKPT);

        if (info.useRootLsn == DbLsn.NULL_LSN) {
            throw new EnvironmentFailureException
                (envImpl,
                 EnvironmentFailureReason.LOG_INTEGRITY,
                 "This environment's log file has no root. Since the root " +
                 "is the first entry written into a log at environment " +
                 "creation, this should only happen if the initial creation " +
                 "of the environment was never checkpointed or synced. " +
                 "Please move aside the existing log files to allow the " +
                 "creation of a new environment");
        }
    }

    /**
     * Should be called when performing operations that may add to the cache,
     * but only after all INs are in place and buildINList has been called.
     */
    private void invokeEvictor() {

        /*
         * To prevent eviction from violating disk limits we must periodically
         * freshen the log size stats. (Since the cleaner isn't running.)
         */
        nOpsSinceDiskLimitRecalc += 1;
        if (nOpsSinceDiskLimitRecalc == 1000) {
            envImpl.getCleaner().freshenLogSizeStats();
            nOpsSinceDiskLimitRecalc = 0;
        }

        envImpl.invokeEvictor();
    }

    /**
     * Use the log to recreate an in memory tree.
     */
    private void buildTree()
        throws DatabaseException {

        startupTracker.start(Phase.BUILD_TREE);

        try {

            /*
             * Read all map database INs, find largest node ID before any
             * possibility of splits, find largest txn Id before any need for a
             * root update (which would use an AutoTxn)
             */
            buildINs(true /*mappingTree*/, 
                     Phase.READ_MAP_INS,
                     Phase.REDO_MAP_INS, 
                     RecoveryProgress.READ_DBMAP_INFO,
                     RecoveryProgress.REDO_DBMAP_INFO);

            /*
             * Undo all aborted map LNs. Read and remember all committed,
             * prepared, and replicated transaction ids, to prepare for the
             * redo phases.
             */
            startupTracker.start(Phase.UNDO_MAP_LNS);
            startupTracker.setProgress(RecoveryProgress.UNDO_DBMAP_RECORDS);

            Set<LogEntryType> mapLNSet = new HashSet<>();
            mapLNSet.add(LogEntryType.LOG_TXN_COMMIT);
            mapLNSet.add(LogEntryType.LOG_TXN_ABORT);
            mapLNSet.add(LogEntryType.LOG_TXN_PREPARE);
            mapLNSet.add(LogEntryType.LOG_ROLLBACK_START);
            mapLNSet.add(LogEntryType.LOG_ROLLBACK_END);

            undoLNs(mapLNSet, true /*firstUndoPass*/,
                    startupTracker.getCounter(Phase.UNDO_MAP_LNS));

            startupTracker.stop(Phase.UNDO_MAP_LNS);

            /*
             * Start file cache warmer after we have read the log from
             * firstActiveLsn forward. From here forward, recovery should be
             * reading from the file system cache, so another reading thread
             * should not cause disk head movement.
             */
            envImpl.getFileManager().startFileCacheWarmer(info.firstActiveLsn);

            /*
             * Replay all mapLNs, mapping tree in place now. Use the set of
             * committed txns, replicated and prepared txns found from the undo
             * pass.
             */
            startupTracker.start(Phase.REDO_MAP_LNS);
            startupTracker.setProgress(RecoveryProgress.REDO_DBMAP_RECORDS);

            mapLNSet.clear();
            mapLNSet.add(LogEntryType.LOG_MAPLN);

            redoLNs(mapLNSet, startupTracker.getCounter(Phase.REDO_MAP_LNS));

            startupTracker.stop(Phase.REDO_MAP_LNS);

            /*
             * When the mapping DB is complete, check for log version 8 upgrade
             * violations. Will throw an exception if there is a violation.
             */
            checkLogVersion8UpgradeViolations();

            /*
             * Reconstruct the internal nodes for the main level trees.
             */
            buildINs(false /*mappingTree*/, 
                     Phase.READ_INS, 
                     Phase.REDO_INS,
                     RecoveryProgress.READ_DATA_INFO,
                     RecoveryProgress.REDO_DATA_INFO);

            /*
             * Build the in memory IN list.  Now that the INs are complete we
             * can add the environment to the evictor (for a shared cache) and
             * invoke the evictor.  The evictor will also be invoked during the
             * undo and redo passes.
             */
            buildINList();
            if (envImpl.getSharedCache()) {
                envImpl.getEvictor().addEnvironment(envImpl);
            }
            invokeEvictor();

            /*
             * Undo aborted LNs. No need to include TxnAbort, TxnCommit,
             * TxnPrepare, RollbackStart and RollbackEnd records again, since
             * those were scanned during the the undo of all aborted MapLNs.
             */
            startupTracker.start(Phase.UNDO_LNS);
            startupTracker.setProgress(RecoveryProgress.UNDO_DATA_RECORDS);

            Set<LogEntryType> lnSet = new HashSet<>();
            for (LogEntryType entryType : LogEntryType.getAllTypes()) {
                if (entryType.isLNType() && entryType.isTransactional() &&
                    !entryType.equals(LogEntryType.LOG_MAPLN_TRANSACTIONAL)) {
                    lnSet.add(entryType);
                }
            }

            undoLNs(lnSet, false /*firstUndoPass*/,
                    startupTracker.getCounter(Phase.UNDO_LNS));

            startupTracker.stop(Phase.UNDO_LNS);

            /* Replay LNs. Also read non-transactional LNs. */
            startupTracker.start(Phase.REDO_LNS);
            startupTracker.setProgress(RecoveryProgress.REDO_DATA_RECORDS);

            for (LogEntryType entryType : LogEntryType.getAllTypes()) {
                if (entryType.isLNType() && !entryType.isTransactional() &&
                    !entryType.equals(LogEntryType.LOG_MAPLN)) {
                    lnSet.add(entryType);
                }
            }

            redoLNs(lnSet, startupTracker.getCounter(Phase.REDO_LNS));

            startupTracker.stop(Phase.REDO_LNS);

            rollbackTracker.recoveryEndFsyncInvisible();
        } finally {
            startupTracker.stop(Phase.BUILD_TREE);
        }
    }

    /**
     * Perform two passes for the INs of a given level. Root INs must be
     * processed first to account for splits/compressions that were done
     * during/after a checkpoint [#14424] [#24663].
     *
     * Splits and compression require logging up to the root of the tree, to
     * ensure that all INs are properly returned to the correct position at
     * recovery. In other words, splits and compression ensure that the
     * creation and deletion of all nodes is promptly logged.
     *
     * However, checkpoints are not propagated to the top of the tree, in
     * order to conserve on logging. Because of that, a great-aunt situation
     * can occur, where an ancestor of a given node can be logged without
     * referring to the latest on-disk position of the node, because that
     * ancestor was part of a split or compression.
     *
     * Take this scenario:
     *     Root-A
     *      /    \
     *    IN-B   IN-C
     *   /      / | \
     *  BIN-D
     *  /
     * LN-E
     *
     * 1) LN-E  is logged, BIN-D is dirtied
     * 2) BIN-D is logged during a checkpoint, IN-B is dirtied
     * 3) IN-C  is split and Root-A is logged
     * 4) We recover using Root-A and the BIN-D logged at (2) is lost
     *
     * At (3) when Root-A is logged, it points to an IN-B on disk that does not
     * point to the most recent BIN-D
     *
     * At (4) when we recover, although we will process the BIN-D logged at (2)
     * and splice it into the tree, the Root-A logged at (3) is processed last
     * and overrides the entire subtree containing BIN-D
     *
     * This could be addressed by always logging to the root at every
     * checkpoint. Barring that, we address it by replaying the root INs first,
     * and then all non-root INs.
     *
     * It is important that no IN is replayed that would cause a fetch of an
     * older IN version which has been replaced by a newer version in the
     * checkpoint interval. If the newer version were logged as the result of
     * log cleaning, and we attempt to fetch the older version, this would
     * cause a LOG_FILE_NOT_FOUND exception. The replay of the root INs in the
     * first pass is safe, because it won't cause a fetch [#24663]. The replay
     * of INs in the second pass is safe because only nodes at maxFlushLevel
     * were logged non-provisionally and only these nodes are replayed.
     *
     * @param mappingTree if true, we're building the mapping tree
     */
    private void buildINs(
        boolean mappingTree, 
        StartupTracker.Phase phaseA,
        StartupTracker.Phase phaseB,
        RecoveryProgress progressA,
        RecoveryProgress progressB)
        throws DatabaseException {

        /*
         * Pass a: Replay root INs.
         */
        startupTracker.start(phaseA);
        startupTracker.setProgress(progressA);

        if (mappingTree) {
            readRootINsAndTrackIds(startupTracker.getCounter(phaseA));
        } else {
            readRootINs(startupTracker.getCounter(phaseA));
        }
        startupTracker.stop(phaseA);

        /*
         * Pass b: Replay non-root INs.
         */
        startupTracker.start(phaseB);
        startupTracker.setProgress(progressB);

        readNonRootINs(mappingTree, startupTracker.getCounter(phaseB));

        startupTracker.stop(phaseB);
    }

    /*
     * Read root INs in the mapping tree DB and place in the in-memory tree.
     *
     * Also peruse all pertinent log entries in order to update our knowledge
     * of the last used database, transaction and node ids, and to to track
     * utilization profile and VLSN->LSN mappings.
     */
    private void readRootINsAndTrackIds(
        StartupTracker.Counter counter)
        throws DatabaseException {

        INFileReader reader = new INFileReader(
            envImpl, readBufferSize,
            info.checkpointStartLsn,         // start lsn
            info.nextAvailableLsn,           // finish lsn
            true,                            // track ids
            info.partialCheckpointStartLsn,  // partialCkptStart
            info.checkpointEndLsn,           // ckptEnd
            tracker,
            logVersion8UpgradeDbs,
            logVersion8UpgradeDeltas);

        reader.addTargetType(LogEntryType.LOG_IN);

        /* Validate all entries in at least one full recovery pass. */
        reader.setAlwaysValidateChecksum(true);

        try {
            DbTree dbMapTree = envImpl.getDbTree();

            /* Process every IN and BIN in the mapping tree. */
            while (reader.readNextEntry()) {

                counter.incNumRead();

                DatabaseId dbId = reader.getDatabaseId();

                if (!dbId.equals(DbTree.ID_DB_ID)) {
                    continue;
                }

                DatabaseImpl db = dbMapTree.getDb(dbId);

                assert db != null; // mapping DB is always available

                try {
                    if (!reader.getIN(db).isRoot()) {
                        continue;
                    }

                    replayOneIN(reader, db);

                    counter.incNumProcessed();

                } finally {
                    dbMapTree.releaseDb(db);
                }
            }

            counter.setRepeatIteratorReads(reader.getNRepeatIteratorReads());

            /*
             * Update node ID, database ID, and txn ID sequences. Use either
             * the maximum of the IDs seen by the reader vs. the IDs stored in
             * the checkpoint.
             */
            info.useMinReplicatedNodeId = reader.getMinReplicatedNodeId();
            info.useMaxNodeId = reader.getMaxNodeId();

            info.useMinReplicatedDbId = reader.getMinReplicatedDbId();
            info.useMaxDbId = reader.getMaxDbId();

            info.useMinReplicatedTxnId = reader.getMinReplicatedTxnId();
            info.useMaxTxnId = reader.getMaxTxnId();

            if (info.checkpointEnd != null) {
                CheckpointEnd ckptEnd = info.checkpointEnd;

                if (info.useMinReplicatedNodeId >
                    ckptEnd.getLastReplicatedNodeId()) {
                    info.useMinReplicatedNodeId =
                        ckptEnd.getLastReplicatedNodeId();
                }
                if (info.useMaxNodeId < ckptEnd.getLastLocalNodeId()) {
                    info.useMaxNodeId = ckptEnd.getLastLocalNodeId();
                }

                if (info.useMinReplicatedDbId >
                    ckptEnd.getLastReplicatedDbId()) {
                    info.useMinReplicatedDbId =
                        ckptEnd.getLastReplicatedDbId();
                }
                if (info.useMaxDbId < ckptEnd.getLastLocalDbId()) {
                    info.useMaxDbId = ckptEnd.getLastLocalDbId();
                }

                if (info.useMinReplicatedTxnId >
                    ckptEnd.getLastReplicatedTxnId()) {
                    info.useMinReplicatedTxnId =
                        ckptEnd.getLastReplicatedTxnId();
                }
                if (info.useMaxTxnId < ckptEnd.getLastLocalTxnId()) {
                    info.useMaxTxnId = ckptEnd.getLastLocalTxnId();
                }
            }

            envImpl.getNodeSequence().
                setLastNodeId(info.useMinReplicatedNodeId, info.useMaxNodeId);
            envImpl.getDbTree().setLastDbId(info.useMinReplicatedDbId,
                                            info.useMaxDbId);
            envImpl.getTxnManager().setLastTxnId(info.useMinReplicatedTxnId,
                                                 info.useMaxTxnId);

            info.vlsnProxy = reader.getVLSNProxy();
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "readMapIns", e);
        }
    }

    /**
     * Read root INs for DBs other than the mapping tree, and process.
     */
    private void readRootINs(
        StartupTracker.Counter counter)
        throws DatabaseException {

        /* Don't need to track IDs. */
        INFileReader reader = new INFileReader(
            envImpl, readBufferSize,
            info.checkpointStartLsn,        // start lsn
            info.nextAvailableLsn,          // finish lsn
            false,                          // track ids
            info.partialCheckpointStartLsn, // partialCkptStart
            info.checkpointEndLsn,          // ckptEnd
            null);                          // tracker
 
        reader.addTargetType(LogEntryType.LOG_IN);

        try {

            /*
             * Read all non-provisional INs, and process if they don't belong
             * to the mapping tree.
             */
            DbTree dbMapTree = envImpl.getDbTree();

            while (reader.readNextEntry()) {

                counter.incNumRead();

                DatabaseId dbId = reader.getDatabaseId();

                if (dbId.equals(DbTree.ID_DB_ID)) {
                    continue;
                }

                DatabaseImpl db = dbMapTree.getDb(dbId);

                if (db == null) {
                    /* This db has been deleted, ignore the entry. */
                    counter.incNumDeleted();
                    continue;
                }

                try {
                    if (!reader.getIN(db).isRoot()) {
                        continue;
                    }

                    replayOneIN(reader, db);

                    counter.incNumProcessed();

                } finally {
                    dbMapTree.releaseDb(db);
                }
            }

            counter.setRepeatIteratorReads(reader.getNRepeatIteratorReads());
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "readNonMapIns", e);
        }
    }

    /**
     * Read non-root INs and process.
     */
    private void readNonRootINs(
        boolean mappingTree,
        StartupTracker.Counter counter)
        throws DatabaseException {

        /* Don't need to track IDs. */
        INFileReader reader = new INFileReader(
            envImpl, readBufferSize,
            info.checkpointStartLsn,        // start lsn
            info.nextAvailableLsn,          // finish lsn
            false,                          // track ids
            info.partialCheckpointStartLsn, // partialCkptStart
            info.checkpointEndLsn,          // ckptEnd
            null);                          // tracker

        reader.addTargetType(LogEntryType.LOG_IN);
        reader.addTargetType(LogEntryType.LOG_BIN);
        reader.addTargetType(LogEntryType.LOG_BIN_DELTA);
        reader.addTargetType(LogEntryType.LOG_OLD_BIN_DELTA);

        try {

            /* Read all non-provisional INs that are in the repeat set. */
            DbTree dbMapTree = envImpl.getDbTree();

            while (reader.readNextEntry()) {

                counter.incNumRead();

                DatabaseId dbId = reader.getDatabaseId();

                if (mappingTree != dbId.equals(DbTree.ID_DB_ID)) {
                    continue;
                }

                DatabaseImpl db = dbMapTree.getDb(dbId);

                if (db == null) {
                    /* This db has been deleted, ignore the entry. */
                    counter.incNumDeleted();
                    continue;
                }

                try {
                    if (reader.getIN(db).isRoot()) {
                        continue;
                    }

                    replayOneIN(reader, db);

                    counter.incNumProcessed();

                } finally {
                    dbMapTree.releaseDb(db);
                }
            }

            counter.setRepeatIteratorReads(reader.getNRepeatIteratorReads());
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "readNonMapIns", e);
        }
    }

    /**
     * Get an IN from the reader, set its database, and fit into tree.
     */
    private void replayOneIN(
        INFileReader reader,
        DatabaseImpl db)
        throws DatabaseException {

        /*
         * Last entry is a node, replay it. Now, we should really call
         * IN.postFetchInit, but we want to do something different from the
         * faulting-in-a-node path, because we don't want to put the IN on the
         * in memory list, and we don't want to search the db map tree, so we
         * have a IN.postRecoveryInit.
         */
        final long logLsn = reader.getLastLsn();
        final IN in = reader.getIN(db);
        in.postRecoveryInit(db, logLsn);
        in.latch();

        recoverIN(db, in, logLsn);

        /*
         * Add any db that we encounter IN's for because they will be part of
         * the in-memory tree and therefore should be included in the INList
         * build.
         */
        inListBuildDbIds.add(db.getId());
    }

    /**
     * Recover an internal node.
     *
     * inFromLog should be latched upon entering this method and it will
     * not be latched upon exiting.
     *
     * @param inFromLog - the new node to put in the tree.  The identifier key
     * and node ID are used to find the existing version of the node.
     * @param logLsn - the location of log entry in in the log.
     */
    private void recoverIN(DatabaseImpl db, IN inFromLog, long logLsn)
        throws DatabaseException {

        List<TrackingInfo> trackingList = null;
        try {

            /*
             * We must know a priori if this node is the root. We can't infer
             * that status from a search of the existing tree, because
             * splitting the root is done by putting a node above the old root.
             * A search downward would incorrectly place the new root below the
             * existing tree.
             */
            if (inFromLog.isRoot()) {
                recoverRootIN(db, inFromLog, logLsn);

            } else {

                /*
                 * Look for a parent. The call to getParentNode unlatches node.
                 * Then place inFromLog in the tree if appropriate.
                 */
                trackingList = new ArrayList<>();
                recoverChildIN(db, inFromLog, logLsn, trackingList);
            }
        } catch (EnvironmentFailureException e) {
            /* Pass through untouched. */
            throw e;
        } catch (Exception e) {
            String trace = printTrackList(trackingList);
            LoggerUtils.traceAndLogException(
                db.getEnv(), "RecoveryManager", "recoverIN",
                " lsnFromLog: " + DbLsn.getNoFormatString(logLsn) +
                " " + trace, e);

            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                "lsnFromLog=" + DbLsn.getNoFormatString(logLsn), e);
        } finally {
            if (LatchSupport.TRACK_LATCHES) {
                LatchSupport.expectBtreeLatchesHeld(
                    0, "LSN = " + DbLsn.toString(logLsn) +
                        " inFromLog = " + inFromLog.getNodeId());
            }
        }
    }

    /**
     * If the root of this tree is null, use this IN from the log as a root.
     * Note that we should really also check the LSN of the mapLN, because
     * perhaps the root is null because it's been deleted. However, the replay
     * of all the LNs will end up adjusting the tree correctly.
     *
     * If there is a root, check if this IN is a different LSN and if so,
     * replace it.
     */
    private void recoverRootIN(DatabaseImpl db, IN inFromLog, long lsn)
        throws DatabaseException {

        boolean success = true;
        Tree tree = db.getTree();
        RootUpdater rootUpdater = new RootUpdater(tree, inFromLog, lsn);
        try {
            /* Run the root updater while the root latch is held. */
            tree.withRootLatchedExclusive(rootUpdater);

            /* Update the mapLN if necessary */
            if (rootUpdater.updateDone()) {

                /*
                 * Dirty the database to call DbTree.modifyDbRoot later during
                 * the checkpoint.  We should not log a DatabaseImpl until its
                 * utilization info is correct.
                 */
                db.setDirty();
            }
        } catch (Exception e) {
            success = false;
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                "lsnFromLog=" + DbLsn.getNoFormatString(lsn), e);
        } finally {
            if (rootUpdater.getInFromLogIsLatched()) {
                inFromLog.releaseLatch();
            }

            trace(logger,
                  db, TRACE_ROOT_REPLACE, success, inFromLog,
                  lsn,
                  null,
                  true,
                  rootUpdater.getReplaced(),
                  rootUpdater.getInserted(),
                  rootUpdater.getOriginalLsn(),
                  DbLsn.NULL_LSN,
                  -1);
        }
    }

    /*
     * RootUpdater lets us replace the tree root within the tree root latch.
     */
    private static class RootUpdater implements WithRootLatched {
        private final Tree tree;
        private final IN inFromLog;
        private long lsn = DbLsn.NULL_LSN;
        private boolean inserted = false;
        private boolean replaced = false;
        private long originalLsn = DbLsn.NULL_LSN;
        private boolean inFromLogIsLatched = true;

        RootUpdater(Tree tree, IN inFromLog, long lsn) {
            this.tree = tree;
            this.inFromLog = inFromLog;
            this.lsn = lsn;
        }

        boolean getInFromLogIsLatched() {
            return inFromLogIsLatched;
        }

        public IN doWork(ChildReference root)
            throws DatabaseException {

            ChildReference newRoot =
                tree.makeRootChildReference(inFromLog, new byte[0], lsn);
            inFromLog.releaseLatch();
            inFromLogIsLatched = false;

            if (root == null) {
                tree.setRoot(newRoot, false);
                inserted = true;
            } else {
                originalLsn = root.getLsn(); // for debugLog

                /*
                 * The current in-memory root IN is older than the root IN from
                 * the log.
                 */
                if (DbLsn.compareTo(originalLsn, lsn) < 0) {
                    tree.setRoot(newRoot, false);
                    replaced = true;
                }
            }
            return null;
        }

        boolean updateDone() {
            return inserted || replaced;
        }

        boolean getInserted() {
            return inserted;
        }

        boolean getReplaced() {
            return replaced;
        }

        long getOriginalLsn() {
            return originalLsn;
        }
    }

    /**
     * Recovers a non-root IN.  See algorithm below.
     *
     * Note that this method never inserts a slot for an IN, it only replaces
     * the node in a slot under certain conditions.  Insertion of slots is
     * unnecessary because splits are logged all the way to the root, and
     * therefore inserted slots are always visible via the parent node.  In
     * fact, it is critical that splits are not allowed during this phase of
     * recovery, because that might require splits and logging is not allowed
     * until the INs are all in place.
     */
    private void recoverChildIN(
        DatabaseImpl db,
        IN inFromLog,
        long logLsn,
        List<TrackingInfo> trackingList)
        throws DatabaseException {

        boolean replaced = false;
        long treeLsn = DbLsn.NULL_LSN;
        boolean finished = false;
        SearchResult result = new SearchResult();
        
        try {
            long targetNodeId = inFromLog.getNodeId();
            byte[] targetKey = inFromLog.getIdentifierKey();
            int exclusiveLevel = inFromLog.getLevel() + 1;

            inFromLog.releaseLatch();

            result = db.getTree().getParentINForChildIN(
                targetNodeId, targetKey, -1, /*targetLevel*/
                exclusiveLevel, true /*requireExactMatch*/, true, /*doFetch*/
                CacheMode.UNCHANGED, trackingList);

            /*
             * Does inFromLog exist in this parent?
             *
             * 1. IN is not in the current tree. Skip this IN; it's represented
             *    by a parent that's later in the log or it has been deleted.
             *    This works because splits and IN deleteions are logged
             *    immediately when they occur all the way up to the root.
             * 2. physical match: (LSNs same) this LSN is already in place,
             *    do nothing.
             * 3. logical match: another version of this IN is in place.
             *    Replace child with inFromLog if inFromLog's LSN is greater.
             */
            if (result.parent == null) {
                finished = true;
                return;  // case 1,
            }

            IN parent = result.parent;
            int idx = result.index;

            assert(result.exactParentFound);
            assert(result.index >= 0);
            assert(targetNodeId == ((IN)parent.getTarget(idx)).getNodeId());

            /* Get the key that will locate inFromLog in this parent. */
            if (parent.getLsn(idx) == logLsn) {
                /* case 2: do nothing */
            } else {

                /* Not an exact physical match, now need to look at child. */
                treeLsn = parent.getLsn(idx);

                /* case 3: It's a logical match, replace */
                if (DbLsn.compareTo(treeLsn, logLsn) < 0) {

                    /*
                     * It's a logical match, replace. Put the child
                     * node reference into the parent, as well as the
                     * true LSN of the IN or BIN-delta.
                     */
                    parent.recoverIN(
                        idx, inFromLog, logLsn, 0 /*lastLoggedSize*/);

                    replaced = true;
                }
            }

            finished = true;

        } finally {
            if (result.parent != null) {
                result.parent.releaseLatch();
            }

            trace(logger, db,
                TRACE_IN_REPLACE, finished, inFromLog,
                logLsn, result.parent,
                result.exactParentFound, replaced, false /*inserted*/,
                treeLsn, DbLsn.NULL_LSN, result.index);
        }
    }

    /**
     * Undo all LNs that belong to aborted transactions. These are LNs in the
     * log that
     * (1) don't belong to a committed txn AND
     * (2) aren't part of a prepared txn AND
     * (3) shouldn't be resurrected as part of a replication ReplayTxn.
     *
     * LNs that are part of a rollback period do need to be undone, but in
     * a different way from the other LNs. They are rolledback and take a
     * different path.
     *
     * To find these LNs, walk the log backwards, using log entry commit record
     * to create a collection of committed txns. If we see a log entry that
     * doesn't fit the criteria above, undo it.
     *
     * @param firstUndoPass is true if this is the first time that undoLNs is
     * called. This is a little awkward, but is done to explicitly indicate to
     * the rollback tracker that this is the tracker's construction phase.
     * During this first pass, RollbackStart and RollbackEnd are in the target
     * log types, and the rollback period map is created.
     * We thought that it was better to be explicit than to reply on checking
     * the logTypes parameter to see if RollbackStart/RollbackEnd is included.
     */
    private void undoLNs(
        Set<LogEntryType> logTypes,
        boolean firstUndoPass,
        StartupTracker.Counter counter)
        throws DatabaseException {

        long firstActiveLsn = info.firstActiveLsn;
        long lastUsedLsn = info.lastUsedLsn;
        long endOfFileLsn = info.nextAvailableLsn;

        /* Set up a reader to pick up target log entries from the log. */
        LNFileReader reader = new LNFileReader(
            envImpl, readBufferSize, lastUsedLsn,
            false, endOfFileLsn, firstActiveLsn, null,
            info.checkpointEndLsn);

        for (LogEntryType lt: logTypes) {
            reader.addTargetType(lt);
        }

        DbTree dbMapTree = envImpl.getDbTree();

        /*
         * See RollbackTracker.java for details on replication rollback
         * periods.  Standalone recovery must handle replication rollback at
         * recovery, because we might be opening a replicated environment in a
         * read-only, non-replicated way for use by a command line utility.
         * Even though the utility will not write invisible bits, it will need
         * to ensure that all btree nodes are in the proper state, and reflect
         * any rollback related changes.
         *
         * Note that when opening a read-only environment, because we cannot
         * write invisible bits, we may end up redo'ing LNs in rolled back txns
         * that should be marked invisible.  This is very unlikely, but should
         * be fixed at some point by using the LSNs collected by
         * RollbackTracker to determine whether a log entry should be treated
         * as invisible by redo.  See [#23708].
         *
         * The rollbackScanner is a sort of cursor that acts with the known
         * state of the rollback period detection.
         *
         * We let the tracker know if it is the first pass or not, in order
         * to support some internal tracker assertions.
         */
        rollbackTracker.setFirstPass(firstUndoPass);
        final Scanner rollbackScanner =  rollbackTracker.getScanner();

        try {

            /*
             * Iterate over the target LNs and commit records, constructing the
             * tree.
             */
            while (reader.readNextEntry()) {
                counter.incNumRead();
                if (reader.isLN()) {

                    /* Get the txnId from the log entry. */
                    Long txnId = reader.getTxnId();

                    /* Skip past this, no need to undo non-txnal LNs. */
                    if (txnId == null) {
                        continue;
                    }

                    if (rollbackScanner.positionAndCheck(reader.getLastLsn(),
                                                         txnId)) {
                        /*
                         * If an LN is in the rollback period and was part of a
                         * rollback, let the rollback scanner decide how it
                         * should be handled. This does not include LNs that
                         * were explicitly aborted.
                         */
                        rollbackScanner.rollback(txnId, reader, tracker);
                        continue;
                    }

                    /* This LN is part of a committed txn. */
                    if (committedTxnIds.containsKey(txnId)) {
                        continue;
                    }

                    /* This LN is part of a prepared txn. */
                    if (preparedTxns.get(txnId) != null) {
                        resurrectedLsns.add(reader.getLastLsn());
                        continue;
                    }

                    /*
                     * This LN is part of a uncommitted, unaborted
                     * replicated txn.
                     */
                    if (isReplicatedUncommittedLN(reader, txnId)) {
                        createReplayTxn(txnId);
                        resurrectedLsns.add(reader.getLastLsn());
                        continue;
                    }

                    undoUncommittedLN(reader, dbMapTree);
                    counter.incNumProcessed();

                } else if (reader.isPrepare()) {
                    handlePrepare(reader);
                    counter.incNumAux();

                } else if (reader.isAbort()) {
                    /* The entry just read is an abort record. */
                    abortedTxnIds.add(reader.getTxnAbortId());
                    counter.incNumAux();

                } else if (reader.isCommit()) {

                    /*
                     * Sanity check that the commit does not interfere with the
                     * rollback period. Since the reader includes commits only
                     * on the first pass, the cost of the check is confined to
                     * that pass, and is very low if there is no rollback
                     * period.
                     */
                    rollbackTracker.checkCommit(reader.getLastLsn(),
                                                reader.getTxnCommitId());

                    committedTxnIds.put(reader.getTxnCommitId(),
                                        reader.getLastLsn());
                    counter.incNumAux();

                } else if (reader.isRollbackStart()) {
                    rollbackTracker.register(
                        (RollbackStart) reader.getMainItem(),
                        reader.getLastLsn());
                    counter.incNumAux();

                } else if (reader.isRollbackEnd()) {
                    rollbackTracker.register(
                        (RollbackEnd) reader.getMainItem(),
                        reader.getLastLsn());
                    counter.incNumAux();

                } else {
                    throw EnvironmentFailureException.unexpectedState(
                        envImpl,
                        "LNreader should not have picked up type " +
                        reader.dumpCurrentHeader());
                }
            } /* while */
            counter.setRepeatIteratorReads(reader.getNRepeatIteratorReads());
            rollbackTracker.singlePassSetInvisible();

        } catch (RuntimeException e) {
            traceAndThrowException(reader.getLastLsn(), "undoLNs", e);
        }
    }

    /**
     * Uncommitted, unaborted LNs that belong to a replicated txn are
     * resurrected rather than undone. This means that the LN is also
     * replicated.
     */
    private boolean isReplicatedUncommittedLN(LNFileReader reader, Long txnId) {

        /*
         * This only applies if the environment is replicated AND the entry is
         * in a replicated txn. If a replicated environment is opened by a read
         * only command line utility, it will be opened in a non-replicated
         * way, and we don't want to resurrect the txn and acquire write locks.
         */
        if (!envImpl.isReplicated()) {
            return false;
        }

        if (abortedTxnIds.contains(txnId)) {
            return false;
        }

        if (reader.entryIsReplicated()) {
            return true;
        }

        return false;
    }

    /**
     * When recovering a replicated environment, all uncommitted, replicated
     * transactions are resurrected much the same way as a prepared
     * transaction. If the node turns out to be a new master, by definition
     * those txns won't resume, and the code path for new master setup will
     * abort these transactions. If the node is a replica, the transactions
     * will either resume or abort depending on whether the originating master
     * is alive or not.
     */
    private void createReplayTxn(long txnId)
        throws DatabaseException {

        /*
         * If we didn't see this transaction yet, create a ReplayTxn
         * to use in the later redo stage, when we redo and resurrect
         * this transaction.
         */
        if (info.replayTxns.get(txnId) == null) {
            info.replayTxns.put(txnId, envImpl.createReplayTxn(txnId));
        }
    }

    /**
     * The entry just read is a prepare record. Setup a PrepareTxn that will
     * exempt any of its uncommitted LNs from undo. Instead, uncommitted LNs
     * that belong to a PrepareTxn are redone.
     */
    private void handlePrepare(LNFileReader reader)
        throws DatabaseException {

        long prepareId = reader.getTxnPrepareId();
        Long prepareIdL = prepareId;
        if (!committedTxnIds.containsKey(prepareIdL) &&
            !abortedTxnIds.contains(prepareIdL)) {
            TransactionConfig txnConf = new TransactionConfig();
            PreparedTxn preparedTxn = PreparedTxn.createPreparedTxn
                (envImpl, txnConf, prepareId);

            /*
             * There should be no lock conflicts during recovery, but just in
             * case there are, we set the locktimeout to 0.
             */
            preparedTxn.setLockTimeout(0);
            preparedTxns.put(prepareIdL, preparedTxn);
            preparedTxn.setPrepared(true);
            envImpl.getTxnManager().registerXATxn
                (reader.getTxnPrepareXid(), preparedTxn, true);
            LoggerUtils.logMsg(logger, envImpl, Level.INFO,
                               "Found unfinished prepare record: id: " +
                               reader.getTxnPrepareId() +
                               " Xid: " + reader.getTxnPrepareXid());
        }
    }

    /**
     * Found an uncommitted LN, set up the work to undo the LN.
     */
    private void undoUncommittedLN(LNFileReader reader, DbTree dbMapTree)
        throws DatabaseException {

        /* Invoke the evictor to reduce memory consumption. */
        invokeEvictor();

        DatabaseId dbId = reader.getDatabaseId();
        DatabaseImpl db = dbMapTree.getDb(dbId);

        /* Database may be null if it's been deleted. */
        if (db == null) {
            return;
        }

        LNLogEntry<?> lnEntry = reader.getLNLogEntry();
        lnEntry.postFetchInit(db);

        LN ln = lnEntry.getLN();
        TreeLocation location = new TreeLocation();
        long logLsn = reader.getLastLsn();

        try {

            ln.postFetchInit(db, logLsn);

            recoveryUndo(location, db, lnEntry, logLsn);

            /* Undo utilization info. */
            undoUtilizationInfo(lnEntry, db, logLsn, reader.getLastEntrySize());

            /*
             * Add any db that we encounter LN's for because they'll be
             * part of the in-memory tree and therefore should be included
             * in the INList build.
             */
            inListBuildDbIds.add(dbId);

            /*
             * For temporary DBs that are encountered as MapLNs, add them
             * to the set of databases to be removed.
             */
            if (ln instanceof MapLN) {
                MapLN mapLN = (MapLN) ln;
                if (mapLN.getDatabase().isTemporary()) {
                    tempDbIds.add(mapLN.getDatabase().getId());
                }
            }
        } finally {
            dbMapTree.releaseDb(db);
        }
    }

    /**
     * Undo the changes to this node. Here are the rules that govern the action
     * taken.
     *
     * <pre>
     *
     * found LN in  | abortLsn is | logLsn ==       | action taken
     *    tree      | null        | LSN in tree     | by undo
     * -------------+-------------+----------------------------------------
     *      Y       |     N       |      Y          | replace w/abort LSN
     * ------------ +-------------+-----------------+-----------------------
     *      Y       |     Y       |      Y          | remove from tree
     * ------------ +-------------+-----------------+-----------------------
     *      Y       |     N/A     |      N          | no action
     * ------------ +-------------+-----------------+-----------------------
     *      N       |     N/A     |    N/A          | no action (*)
     * (*) If this key is not present in the tree, this record doesn't
     * reflect the IN state of the tree and this log entry is not applicable.
     *
     * </pre>
     * @param location holds state about the search in the tree. Passed
     *  in from the recovery manager to reduce objection creation overhead.
     * @param logLsn is the LSN from the just-read log entry
     *
     * Undo can take place for regular recovery, for aborts, and for recovery
     * rollback processing. Each flavor has some slight differences, and are
     * factored out below.
     */
    private void recoveryUndo(
        TreeLocation location,
        DatabaseImpl db,
        LNLogEntry<?> lnEntry,
        long logLsn) {

        undo(logger, Level.FINE, location, db, lnEntry, logLsn,
             lnEntry.getAbortLsn(), lnEntry.getAbortKnownDeleted(),
             false/*revertPD*/,
             lnEntry.getAbortKey(), lnEntry.getAbortData(),
             lnEntry.getAbortVLSN(),
             lnEntry.getAbortExpiration(), lnEntry.isAbortExpirationInHours());
    }

    public static void abortUndo(
        Logger logger,
        Level traceLevel,
        TreeLocation location,
        DatabaseImpl db,
        LNLogEntry<?> lnEntry,
        long logLsn) {

        undo(logger, traceLevel, location, db, lnEntry, logLsn,
             lnEntry.getAbortLsn(), lnEntry.getAbortKnownDeleted(),
             false/*revertPD*/,
             lnEntry.getAbortKey(), lnEntry.getAbortData(),
             lnEntry.getAbortVLSN(),
             lnEntry.getAbortExpiration(), lnEntry.isAbortExpirationInHours());
    }

    public static void rollbackUndo(
        Logger logger,
        Level traceLevel,
        TreeLocation location,
        DatabaseImpl db,
        LNLogEntry<?> lnEntry,
        long undoLsn,
        RevertInfo revertTo) {

        undo(logger, traceLevel, location,
             db, lnEntry, undoLsn,
             revertTo.revertLsn, revertTo.revertKD, revertTo.revertPD,
             revertTo.revertKey, revertTo.revertData, revertTo.revertVLSN,
             revertTo.revertExpiration, revertTo.revertExpirationInHours);
    }

    private static void undo(
        Logger logger,
        Level traceLevel,
        TreeLocation location,
        DatabaseImpl db,
        LNLogEntry<?> lnEntry,
        long logLsn,
        long revertLsn,
        boolean revertKD,
        boolean revertPD,
        byte[] revertKey,
        byte[] revertData,
        long revertVLSN,
        int revertExpiration,
        boolean revertExpirationInHours)
        throws DatabaseException {

        boolean found = false;
        boolean replaced = false;
        boolean success = false;

        try {

            /* Find the BIN which is the parent of this LN. */
            location.reset();

            found = db.getTree().getParentBINForChildLN(
                location, lnEntry.getKey(), false /*splitsAllowed*/,
                false /*blindDeltaOps*/, CacheMode.DEFAULT);

            if (found) {

                /* This LN is in the tree. See if it needs replacing. */
                BIN bin = location.bin;
                int slotIdx = location.index;
                long slotLsn = location.childLsn;

                if (slotLsn == DbLsn.NULL_LSN) {

                    /*
                     * Slots can exist and have a NULL_LSN as the result of an
                     * undo of an insertion that was done without slot reuse.
                     *
                     * We must be sure not to compare the NULL_LSN against the
                     * valid LSN, lest we get a NPE.  [#17427] [#17578]
                     *
                     * To be really sure, check that the location is truly
                     * empty, just as an assertion safety check.
                     */
                    if (!(bin.isEntryKnownDeleted(slotIdx) ||
                          bin.isEntryPendingDeleted(slotIdx))) {
                        throw EnvironmentFailureException.unexpectedState(
                            location + " has a NULL_LSN but the " +
                            "slot is not empty. KD=" +
                            bin.isEntryKnownDeleted(slotIdx) +
                            " PD=" +
                            bin.isEntryPendingDeleted(slotIdx));
                    }

                    bin.queueSlotDeletion(slotIdx);
                    success = true;
                    return;
                }

                boolean updateEntry = DbLsn.compareTo(logLsn, slotLsn) == 0;

                if (updateEntry) {

                    int revertLogrecSize = 0;
                    if (revertLsn != DbLsn.NULL_LSN &&
                        !bin.isEmbeddedLN(slotIdx) &&
                        revertData == null) { 
                        revertLogrecSize = fetchLNSize(db, 0, revertLsn);
                    }

                    bin.recoverRecord(
                        slotIdx, revertLsn, revertKD, revertPD,
                        revertKey, revertData, revertVLSN, revertLogrecSize,
                        revertExpiration, revertExpirationInHours);

                    replaced = true;
                }

                /*
                 * Because we undo before we redo, the record may not be in
                 * the tree, even if it were in the tree when the write op
                 * reflected by the current logrec was performed. 
                    } else {
                        assert(revertLsn == DbLsn.NULL_LSN || revertKD);
                    }
                 */
            }

            success = true;

        } finally {

            if (location.bin != null) {
                location.bin.releaseLatch();
            }

            trace(logger, traceLevel, db, TRACE_LN_UNDO, success,
                lnEntry.getLN(), logLsn, location.bin, found, replaced,
                false, location.childLsn, revertLsn, location.index);
        }
    }

    /**
     * Redo all LNs that should be revived. That means
     *  - all committed LNs
     *  - all prepared LNs
     *  - all uncommitted, replicated LNs on a replicated node.
     */
    private void redoLNs(
        Set<LogEntryType> lnTypes, 
        StartupTracker.Counter counter)
        throws DatabaseException {

        long endOfFileLsn = info.nextAvailableLsn;
        long firstActiveLsn = info.firstActiveLsn;

        /*
         * Set up a reader to pick up target log entries from the log.
         *
         * There are 2 RedoLNs passes.
         *
         * The 1st pass applies to the DbIdMap BTree only. Only LOG_MAPLN
         * logrecs are  returned by the reader during this pass. All such
         * logrecs are eligible for redo and will be processed further here.
         *
         * The 2nd pass applies to all other DB BTrees. The logrecs returned by
         * the reader during this pass are all user-LN logrecs (transactional
         * or not) plus LOG_NAMELN, LOG_NAMELN_TRANSACTIONAL, and
         * LOG_FILESUMMARYLN. During this pass, for most logrecs we should only
         * redo them if they start after the ckpt start LSN. However, there are
         * two categories of logrecs that are not committed, but still need
         * to be redone. These are logrecs that belong to 2PC txns that have
         * been prepared, but still not committed) and logrecs that belong to
         * replicated, uncommitted txns. These logrecs still need to be
         * processed and can live in the log between the firstActive LSN and
         * the checkpointStart LSN, so we start the LNFileReader at the First
         * Active LSN.
         */
        LNFileReader reader = new LNFileReader(
            envImpl, readBufferSize, firstActiveLsn,
            true/*forward*/, DbLsn.NULL_LSN, endOfFileLsn, null,
            info.checkpointEndLsn);

        for (LogEntryType lt : lnTypes) {
            reader.addTargetType(lt);
        }

        DbTree dbMapTree = envImpl.getDbTree();
        TreeLocation location = new TreeLocation();

        try {

            /*
             * Iterate over the target LNs and construct in-memory tree.
             */
            while (reader.readNextEntry()) {

                counter.incNumRead();

                RedoEligible eligible = eligibleForRedo(reader);

                if (!eligible.isEligible) {
                    continue;
                }

                /*
                 * We're doing a redo. Invoke the evictor in this loop to
                 * reduce memory consumption.
                 */
                invokeEvictor();

                DatabaseId dbId = reader.getDatabaseId();
                DatabaseImpl db = dbMapTree.getDb(dbId);

                long logrecLsn = reader.getLastLsn();

                /*
                 * Database may be null if it's been deleted. Only redo for
                 * existing databases.
                 */
                if (db == null) {
                    counter.incNumDeleted();

                    tracker.countObsoleteIfUncounted(
                        logrecLsn, logrecLsn, null, 
                        reader.getLastEntrySize(), dbId,
                        false/*trackOffset*/);
                    
                    continue;
                }

                try {
                    LNLogEntry<?> logrec = reader.getLNLogEntry();
                    logrec.postFetchInit(db);

                    counter.incNumProcessed();

                    redoOneLN(
                        reader, logrec, logrecLsn, dbId, db, eligible, location);
                } finally {
                    dbMapTree.releaseDb(db);
                }
            }

            counter.setRepeatIteratorReads(reader.getNRepeatIteratorReads());

        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "redoLns", e);
        }
    }

    /**
     * These categories of LNs are redone:
     *  - LNs from committed txns between the ckpt start and end of log
     *  - non-transactional LNs between the ckpt start and end of log
     *  - LNs from prepared txns between the first active LSN and end of log
     *  - LNs from replicated, uncommitted, unaborted txns between the first
     *  active LSN and end of log that are NOT invisible.
     *
     * LNs that are in a rollback part of the log are invisible and will not be
     * redone.
     */
    private RedoEligible eligibleForRedo(LNFileReader reader) {

        if (!reader.isLN()) {
            return RedoEligible.NOT;
        }

        if (reader.isInvisible()) {
            return RedoEligible.NOT;
        }

        /*
         * afterCheckpointStart indicates that we're processing log entries
         * after the checkpoint start LSN.  We only process prepared or
         * replicated resurrected txns before checkpoint start. After
         * checkpoint start, we evaluate everything.  If there is no
         * checkpoint, the beginning of the log is really the checkpoint start,
         * and all LNs should be evaluated.
         */
        boolean afterCheckpointStart =
            info.checkpointStartLsn == DbLsn.NULL_LSN ||
            DbLsn.compareTo(reader.getLastLsn(), info.checkpointStartLsn) >= 0;

        /*
         * A transaction will be either prepared OR replayed OR will be a
         * regular committed transaction. A transaction would never be in more
         * than one of these sets.
         */
        Long txnId = reader.getTxnId();
        Txn preparedTxn = preparedTxns.get(txnId);
        Txn replayTxn = info.replayTxns.get(txnId);

        if (preparedTxn != null) {
            return new RedoEligible(preparedTxn);
        } else if (replayTxn != null) {
            return new RedoEligible(replayTxn);
        } else {
            if (afterCheckpointStart) {
                if (txnId == null) {
                    /* A non-txnal LN  after ckpt start is always redone. */
                    return RedoEligible.ELIGIBLE_NON_TXNAL;
                }
                Long commitLongLsn = committedTxnIds.get(txnId);
                if (commitLongLsn != null) {
                    /* A committed LN after ckpt start is always redone. */
                    return new RedoEligible(commitLongLsn);
                }
            }
        }
        return RedoEligible.NOT;
    }

    /* Packages eligibility info. */
    private static class RedoEligible{
        final boolean isEligible;
        final Txn resurrectTxn;  // either a prepared or a replay txn
        final long commitLsn;

        static RedoEligible NOT = new RedoEligible(false);
        static RedoEligible ELIGIBLE_NON_TXNAL = new RedoEligible(true);

        /* Used for eligible prepared and replicated, resurrected txns. */
        RedoEligible(Txn resurrectTxn) {
            this.isEligible = true;
            this.resurrectTxn = resurrectTxn;
            this.commitLsn = DbLsn.NULL_LSN;
        }

        /* Used for eligible, committed LNs. */
        RedoEligible(long commitLsn) {
            this.isEligible = true;
            this.resurrectTxn = null;
            this.commitLsn = commitLsn;
        }

        RedoEligible(boolean eligible) {
            this.isEligible = eligible;
            this.resurrectTxn = null;
            this.commitLsn = DbLsn.NULL_LSN;
        }

        boolean isNonTransactional() {
            return isEligible && 
                   commitLsn == DbLsn.NULL_LSN &&
                   resurrectTxn == null;
        }

        boolean isCommitted() {
            return commitLsn != DbLsn.NULL_LSN || isNonTransactional();
        }
    }

    /*
     * Redo the LN and utilization info. LNs from prepared and replay txns are
     * "resurrected" and also need to re-establish its write locks and undo
     * information.
     */
    private void redoOneLN(
        LNFileReader reader,
        LNLogEntry<?> logrec,
        long logrecLsn,
        DatabaseId dbId,
        DatabaseImpl db,
        RedoEligible eligible,
        TreeLocation location)
        throws DatabaseException {

        int logrecSize = reader.getLastEntrySize();
        LN ln = logrec.getLN();

        ln.postFetchInit(db, logrecLsn);

        if (eligible.resurrectTxn != null) {

            /*
             * This is a prepared or replay txn, so we need to reacquire the
             * write lock as well as redoing the operation, in order to end up
             * with an active txn.
             */
            relock(eligible.resurrectTxn, logrecLsn, logrec, db);
        }

        long treeLsn = redo(
            db, location, logrec, logrecLsn, logrecSize, eligible);

        /*
         * Add any db that we encounter LN's for because they'll be part of the
         * in-memory tree and therefore should be included in the INList build.
         */
        inListBuildDbIds.add(dbId);

        /*
         * Further processing of MapLNs:
         * - For temporary DBs that are encountered as MapLNs, add them to the
         *   set of databases to be removed.
         * - For deleted MapLNs (truncated or removed DBs), redo utilization
         *   counting by counting the entire database as obsolete.
         */
        if (ln instanceof MapLN) {
            MapLN mapLN = (MapLN) ln;

            if (mapLN.getDatabase().isTemporary()) {
                tempDbIds.add(mapLN.getDatabase().getId());
            }

            if (mapLN.isDeleted()) {
                mapLN.getDatabase().countObsoleteDb(tracker, logrecLsn);
            }
        }

        /*
         * For committed truncate/remove NameLNs, we expect a deleted MapLN
         * after it.  Maintain expectDeletedMapLNs to contain all DB IDs for
         * which a deleted MapLN is not found.  [#20816]
         * <p>
         * Note that we must use the new NameLNLogEntry operationType to
         * identify truncate and remove ops, and to distinguish them from a
         * rename (which also deletes the old NameLN) [#21537].
         */
        if (eligible.resurrectTxn == null) {
            NameLNLogEntry nameLNEntry = reader.getNameLNLogEntry();
            if (nameLNEntry != null) {
                switch (nameLNEntry.getOperationType()) {
                case REMOVE:
                    assert nameLNEntry.isDeleted();
                    NameLN nameLN = (NameLN) nameLNEntry.getLN();
                    expectDeletedMapLNs.add(nameLN.getId());
                    break;
                case TRUNCATE:
                    DatabaseId truncateId =
                        nameLNEntry.getTruncateOldDbId();
                    assert truncateId != null;
                    expectDeletedMapLNs.add(truncateId);
                    break;
                default:
                    break;
                }
            }
        }

        boolean treeLsnIsImmediatelyObsolete = db.isLNImmediatelyObsolete();
        
        if (!treeLsnIsImmediatelyObsolete && treeLsn != DbLsn.NULL_LSN) {
            treeLsnIsImmediatelyObsolete = 
                (location.isEmbedded || location.isKD);
        }

        redoUtilizationInfo(
            logrec, reader.getLastEntrySize(), logrecLsn,
            treeLsn, treeLsnIsImmediatelyObsolete, location.childLoggedSize,
            eligible.commitLsn, eligible.isCommitted(),
            db);

        trackReservedFileRecords(logrec);
    }

    private void trackReservedFileRecords(LNLogEntry<?> logrec) {

        if (!LogEntryType.LOG_RESERVED_FILE_LN.equals(logrec.getLogType()) ||
            logrec.isDeleted()) {
            return;
        }

        Long file = ReservedFileInfo.entryToKey(
            new DatabaseEntry(logrec.getKey()));

        ReservedFileInfo info = ReservedFileInfo.entryToObject(
            new DatabaseEntry(logrec.getData()));

        reservedFiles.add(file);
        reservedFileDbs.addAll(info.dbIds);
    }

    /*
     * Reacquire the write lock for the given LN, so we can end up with an
     * active txn.
     */
    private void relock(
        Txn txn,
        long logLsn,
        LNLogEntry<?> logrec,
        DatabaseImpl db)
        throws DatabaseException {

        txn.addLogInfo(logLsn);

        /*
         * We're reconstructing an unfinished transaction.  We know that there
         * was a write lock on this LN since it exists in the log under this
         * txnId.
         */
        final LockResult result = txn.nonBlockingLock(
            logLsn, LockType.WRITE, false /*jumpAheadOfWaiters*/, db);

        if (result.getLockGrant() == LockGrantType.DENIED) {
            throw EnvironmentFailureException.unexpectedState(
               "Resurrected lock denied txn=" + txn.getId() +
               " logLsn=" + DbLsn.getNoFormatString(logLsn) +
               " abortLsn=" + DbLsn.getNoFormatString(logrec.getAbortLsn()));
        }

        /*
         * Set abortLsn and database for utilization tracking.  We don't know
         * lastLoggedSize, so a default will be used for utilization counting.
         * This should not be common.
         */
        result.setAbortInfo(
            logrec.getAbortLsn(), logrec.getAbortKnownDeleted(),
            logrec.getAbortKey(), logrec.getAbortData(), logrec.getAbortVLSN(),
            logrec.getAbortExpiration(), logrec.isAbortExpirationInHours(),
            db);

        final WriteLockInfo wli = result.getWriteLockInfo();

        if (wli == null) {
            throw EnvironmentFailureException.unexpectedState(
               "Resurrected lock has no write info txn=" + txn.getId() +
               " logLsn=" + DbLsn.getNoFormatString(logLsn) +
               " abortLsn=" + DbLsn.getNoFormatString(logrec.getAbortLsn()));
        }

        wli.setAbortLogSize(0 /*lastLoggedSize*/);
    }

    /**
     * Redo a committed LN for recovery.
     *
     * Let R and T be the record and locker associated with the current logrec
     * L. Let TL be the logrec pointed to by the BIN slot for R (if any) just
     * before the call to the redo() method on R. Let TT be the locker that
     * wrote TL.
     *
     * R slot found  | L vs TL | L is     | action
     *   in tree     |         | deletion |
     * --------------+-------- +----------+------------------------
     *     Y         | L <= TL |          | no action
     * --------------+---------+----------+------------------------
     *     Y         | L > TL  |     N    | replace w/log LSN
     * --------------+---------+----------+------------------------
     *     Y         | L > TL  |     Y    | replace w/log LSN, put
     *               |         |          | on compressor queue
     * --------------+---------+----------+------------------------
     *     N         |    n/a  |     N    | insert into tree
     * --------------+---------+----------+------------------------
     *     N         |    n/a  |     Y    | no action
     * --------------+---------+----------+------------------------
     *
     * @param location Used to return to the caller info about the R slot found
     * in the tree (if any) before the slot is updated by this method. It is
     * allocated once by the caller and reset by this method; this way the
     * overhead of object creation is avoided.
     *
     * @param logrec the LN logrec L that is being redone.
     *
     * @param logrecLsn the LSN of L.
     *
     * @param logrecSize the on-disk size of L.
     *
     * @return the LSN found in the tree, or NULL_LSN if the tree did not
     * contain any slot for the record.
     */
    private long redo(
        DatabaseImpl db,
        TreeLocation location,
        LNLogEntry<?> logrec,
        long logrecLsn,
        int logrecSize,
        RedoEligible eligible)
        throws DatabaseException {

        boolean found;
        boolean foundNotKD = false;
        boolean replaced = false;
        boolean inserted = false;
        boolean success = false;

        DbConfigManager configManager = db.getEnv().getConfigManager();

        LogEntryType logrecType = logrec.getLogType();
        LN logrecLN = logrec.getLN();
        long logrecVLSN = logrecLN.getVLSNSequence();
        boolean isDeletion = logrecLN.isDeleted();
        byte[] logrecKey = logrec.getKey();
        byte[] logrecData = logrec.getEmbeddedData();
        long abortLsn = logrec.getAbortLsn();
        boolean abortKD = logrec.getAbortKnownDeleted();
        int expiration = logrec.getExpiration();
        boolean expirationInHours = logrec.isExpirationInHours();

        long treeLsn = DbLsn.NULL_LSN;

        /*
         * Let RL be the logrec being replayed here. Let R and T be
         * the record and the txn associated with RL.
         *
         * We say that RL is replayed "blindly" if the search for
         * R's key in the tree lands on a BIN-delta, this delta does
         * not contain R's key, and we don't mutate the delta to a
         * full BIN to check if R is indeed in the tree or not;
         * instead we just insert R in the delta.
         *
         * RL can be applied blindly only if RL is a "pure" insertion,
         * i.e. RL is an insertion and R did not exist prior to T.
         *
         * A non-pure insertion (where R existed before T, it was
         * deleted by T, and then reinserted by T) cannot be applied
         * blindly, because if it were, it would generate a logrec
         * with abortLSN == NULL, and if T were aborted, undoing the
         * logrec with the NULL abortLSN would cause the loss of the
         * pre-T version of R. So, to replay a non-pure insertion,
         * we must check if a slot for R exists in the tree already,
         * and if so, generate a new logrec with an abortLSN pointing
         * to the pre-T version of R.
         *
         * Updates and deletes cannot be replayed blindly either,
         * because we wouldn't be able to generate logrecs with the
         * correct abortLsn, nor count the previous version of R as
         * obsolete.
         *
         * The condition (abortLsn == DbLsn.NULL_LSN || abortKD)
         * guarantees that LN is a pure insertion.
         */
        boolean blindInsertions =
            (configManager.getBoolean(
                 EnvironmentParams.BIN_DELTA_BLIND_OPS) &&
             eligible.isCommitted() &&
             (db.isLNImmediatelyObsolete() ||
              ((abortLsn == DbLsn.NULL_LSN || abortKD) &&
               (logrecType.equals(LogEntryType.LOG_INS_LN_TRANSACTIONAL) ||
                logrecType.equals(LogEntryType.LOG_INS_LN)))));

        try {

            /*
             * Find the BIN which is the parent of this LN.
             */
            location.reset();

            found = db.getTree().getParentBINForChildLN(
                location, logrecKey, true /*splitsAllowed*/,
                blindInsertions /*blindDeltaOps*/, CacheMode.DEFAULT);

            if (!found && (location.bin == null)) {

                /*
                 * There is no possible parent for this LN. This tree was
                 * probably compressed away.
                 */
                success = true;
                return DbLsn.NULL_LSN;
            }

            BIN bin = location.bin;
            int index = location.index;

            foundNotKD = found && !bin.isEntryKnownDeleted(index);

            if (foundNotKD) {

                treeLsn = location.childLsn;

                int lsnCmp = DbLsn.compareTo(logrecLsn, treeLsn);

                /*
                 * TL <= L 
                 */
                if (lsnCmp >= 0) {

                    /*
                     * If L is a deletion, make sure the KD and PD flags in the
                     * slot are set correctly. Specifically, if T is committed,
                     * set KD and clear PD (if set); otherwise set PD (we know
                     * KD is not set here). 
                     */
                    boolean redoKD = false;
                    boolean redoPD = false;
                    if (isDeletion) {
                        if (eligible.resurrectTxn != null) {
                            redoPD = true;
                        } else {
                            redoKD = true;
                        }
                    }

                    /*
                     * If TL < L apply L, i.e., replace the TL version with the
                     * newer L version. Do not attach the LN as a resident LN
                     * would consume too much memory.
                     *
                     * If TL == L we only need to take act if L is a committed
                     * deletion; in this case, we want to set the KD and clear
                     * the PD flag.
                     */
                    if (lsnCmp > 0) {
                        bin.recoverRecord(
                            index, logrecLsn, redoKD, redoPD,
                            logrecKey, logrecData, logrecVLSN, logrecSize,
                            expiration, expirationInHours);
                        
                        replaced = true;

                    } else if (isDeletion) {
                        if (redoKD) {
                            if (!bin.isEntryKnownDeleted(index)) {
                                bin.setKnownDeleted(index);
                            }
                        } else {
                            assert(bin.isEntryPendingDeleted(index));
                            assert(!bin.isEntryKnownDeleted(index));
                        }
                    }

                    /*
                     * If L is a deletion, put its slot on the compressor
                     * queue. Even when there is a resurrectTxn, it will
                     * not contain delete info ????.
                     */
                    if (isDeletion) { 
                        bin.queueSlotDeletion(index);
                    }
                }

            } else if (found) {

                treeLsn = bin.getLsn(index);

                /*
                 * There is a KD slot with the record's key. If the current
                 * logrec is not a deletion, insert the record in the existing
                 * slot.
                 */
                if (!isDeletion) {

                    if (treeLsn == DbLsn.NULL_LSN ||
                        DbLsn.compareTo(logrecLsn, treeLsn) > 0) {
                        bin.recoverRecord(
                            index, logrecLsn, false/*KD*/, false/*PD*/,
                            logrecKey, logrecData, logrecVLSN, logrecSize,
                            expiration, expirationInHours);

                        inserted = true;
                    }
                } else {
                    bin.queueSlotDeletion(index);
                    /*
                     * logecLsn cannot be > treeLsn, because the record must
                     * have been re-inserted between treeLsn and logrecLsn,
                     * in which case, the slot could not have been KD.
                     */
                    assert(treeLsn == DbLsn.NULL_LSN ||
                           DbLsn.compareTo(logrecLsn, treeLsn) <= 0);
                }

            } else if (bin.isBINDelta()) {

                assert(blindInsertions);

                index = bin.insertEntry1(
                    null/*ln*/, logrecKey, logrecData, logrecLsn,
                    true/*blindInsertion*/);

                assert((index & IN.INSERT_SUCCESS) != 0);

                inserted = true;
                index &= ~IN.INSERT_SUCCESS;
                location.index = index;

                bin.setLastLoggedSize(index, logrecSize);
                bin.setExpiration(index, expiration, expirationInHours);

                if (bin.isEmbeddedLN(index)) {
                    bin.setCachedVLSN(index, logrecVLSN);
                }

                /*
                 * If current logrec is a deletion set the KD flag to prevent
                 * fetching a cleaned LN (we know that the logrec is comitted).
                 */
                if (isDeletion) {
                    assert(eligible.resurrectTxn == null);
                    bin.setKnownDeleted(index);
                }

            } else {

                /*
                 * This LN's key is not in the tree. If the current logrec is
                 * not a deletion, insert the LN to the tree.
                 */
                if (!isDeletion) {

                    index = bin.insertEntry1(
                        null, logrecKey, logrecData, logrecLsn,
                        false/*blindInsertion*/);

                    assert((index & IN.INSERT_SUCCESS) != 0);

                    inserted = true;
                    index &= ~IN.INSERT_SUCCESS;
                    location.index = index;

                    bin.setLastLoggedSize(index, logrecSize);
                    bin.setExpiration(index, expiration, expirationInHours);

                    if (bin.isEmbeddedLN(index)) {
                        bin.setCachedVLSN(index, logrecVLSN);
                    }
                }
            }

            /*
             * We're about to cast away this instantiated LN. It may have
             * registered for some portion of the memory budget, so free
             * that now. Specifically, this would be true for the
             * DbFileSummaryMap in a MapLN.
             */
            logrecLN.releaseMemoryBudget();

            success = true;
            return treeLsn;

        } finally {
            if (location.bin != null) {
                location.bin.releaseLatch();
            }

            trace(logger, db,
                  TRACE_LN_REDO, success, logrecLN,
                  logrecLsn, location.bin, foundNotKD,
                  replaced, inserted,
                  location.childLsn, DbLsn.NULL_LSN, location.index);
        }
    }

    /**
     * Update utilization info during redo.
     *
     * Let R and T be the record and txn associated with the current logrec L.
     * Let TL be the logrec pointed to by the BIN slot for R (if any) just
     * before the call to the redo() method on R. Let TT be the txn that wrote
     * TL. Let AL by the logrec whose LSN is stored as the abortLSN in L (if
     * L.abortLsn != NULL).
     *
     * This method considers whether L, TL, or AL should be counted as
     * obsolete, and if so, it does the counting.
     *
     * @param logrec The deserialized logrec L that is being processed.
     *
     * @param logrecSize The on-disk size of L.
     *
     * @param logrecLsn The LSN of L.
     *
     * @param treeLsn The LSN of TL. Will be NULL_LSN if there was no R slot
     * in the BTree, or the R slot was a KD slot with a NULL_LSN. 
     *
     * @param treeLsnIsImmediatelyObsolete True if (a) the DB is a dups DB
     * with all immediatelly obsolete LNs, or (b) treeLsn is NULL_LSN, or (c)
     * TL is an embedded logrec (as indicated by the embedded flag in the
     * slot).
     *
     * @param treeLNLoggedSize The on-disk size of TL
     *
     * @param commitLsn The commitLSN of T, if T is a Txn that did commit.
     *
     * @param isCommitted True if T is non-transactional or a Txn that did
     * commit.
     *
     * @param db The DatabaseImpl obj for the DB containing R.
     *
     * There are cases where we do not count the previous version of an LN as
     * obsolete when that obsolete LN occurs prior to the recovery interval.
     * This happens when a later version of the LN is current in the tree
     * because its parent IN has been flushed non-provisionally after it.  The
     * old version of the LN is not in the tree so we never encounter it during
     * recovery and cannot count it as obsolete.  For example:
     *
     * 100 LN-A
     * checkpoint occurred (ckpt begin, flush, ckpt end)
     * 200 LN-A
     * 300 BIN parent of 200
     * 400 IN parent of 300, non-provisional via a sync
     *
     * no utilization info is flushed
     * no checkpoint
     * crash and recover
     *
     * 200 is the current LN-A in the tree.  When we redo 200 we do not count
     * anything as obsolete because the log and tree LSNs are equal.  100 is
     * never counted obsolete because it is not in the recovery interval.
     *
     * The same thing occurs when a deleted LN is replayed and the old version
     * is not found in the tree because it was compressed and the IN was
     * flushed non-provisionally.
     *
     * In these cases we may be able to count the abortLsn as obsolete but that
     * would not work for non-transactional entries.
     */
    private void redoUtilizationInfo(
        LNLogEntry<?> logrec,
        int logrecSize,
        long logrecLsn,
        long treeLsn,
        boolean treeLsnIsImmediatelyObsolete,
        int treeLNLoggedSize,
        long commitLsn,
        boolean isCommitted,
        DatabaseImpl db) {

        /*
         * If the logrec is "immediately obsolete", L was counted as obsolete
         * during normal processing and it should be counted here only if its
         * obsoleteness was not made durable before the crash, i.e., if it is
         * after the latest FSLN for the containing log file. No need to record
         * L's LSN in the tracker, because the cleaner already knows that all
         * immediately-obsolete logrecs are obsolete.
         */
        if (logrec.isImmediatelyObsolete(db)) {
            tracker.countObsoleteIfUncounted(
                logrecLsn, logrecLsn, null, logrecSize, db.getId(),
                 false /*trackOffset*/);
        }

        /*
         * Nothing more to be done of immediatelly obsolete DBs. If the treeLsn
         * or the abortLsn are before the ckpt start, then they are already
         * counted as obsolete because utilization info is flushed just before
         * ckpt end. And if they are after the ckpt start, then they have been
         * processed earlier in this RedoLNs pass and as a result counted by
         * the countObsoleteIfUncounted() call above.
         */
        if (db.isLNImmediatelyObsolete()) {
            return;
        }

        /* Determine whether to count the treeLsn or the logrecLsn obsolete. */
        if (treeLsn != DbLsn.NULL_LSN) {

            final int cmpLogLsnToTreeLsn = DbLsn.compareTo(logrecLsn, treeLsn);

            if (cmpLogLsnToTreeLsn < 0) {

                /*
                 * L < TL.
                 *
                 * In normal standalone recovery, if L < TL, we can assume
                 * that TL belongs to a committed txn. This is because the
                 * undo phase happened first, and any uncommitted lsns would
                 * have been removed from the tree. But for replicated and
                 * prepared txns, this assumption is not true; such txns
                 * may be committed or aborted later on. So, TL may belong to
                 * a later, uncommitted txn. [#17710] [#17022]
                 *
                 * L may be obsolete. It is obsolete iff:
                 *
                 * 1. it is immediately obsolete, or
                 * 2. TT is committed (we can check this by checking whether
                 *    TL is not among the resurrected LSNs), or
                 * 3. L is not the last logrec on R by T.
                 *
                 * L is not obsolete if TT is not committed and L is the last
                 * logrec on R by T. These conditions, together with the fact
                 * that L < TL imply that T != TT and L is the abortLsn of TT.
                 *
                 * We have already handled case 1 above. We cannot check for
                 * case 3, so we will conservatively assume L is indeed
                 * obsolete but record its LSN in the tracker only if we know
                 * for sure that it is obsolete, ie, if TT is committed.
                 *
                 * Notice also that if L is indeed obsolete, we cannot be
                 * sure which logrec caused L to become obsolete during
                 * normal processing. So, here we conservatively assume
                 * that it was TL that made L obsolete, and pass TL as the
                 * "lsn" param to countObsoleteIfUncounted(). This will
                 * result in double counting if (a) another logrec L',
                 * with L < L' < TL, made L obsolete, an FSLN for L's
                 * logfile was logged after L' and before TL, and no other
                 * FSLN for the that logfile was logged after TL. 
                 */
                if (!logrec.isImmediatelyObsolete(db)) {
                    tracker.countObsoleteIfUncounted(
                        logrecLsn, treeLsn, null,
                        fetchLNSize(db, logrecSize, logrecLsn), db.getId(),
                        !resurrectedLsns.contains(treeLsn) /*trackOffset*/);
                }

            } else if (cmpLogLsnToTreeLsn > 0) {

                /*
                 * TL < L. 
                 *
                 * Basically, the above discussion for the L < TL case applies
                 * here as well, with the roles of L and TL reversed.
                 *
                 * Notice that in this case, there cannot be another R logrec
                 * L' such that TL < L' < L. To see why, assume L' exists and
                 * consider these 2 cases: (a) L' > ckpt_start. Then L' was
                 * replayed earlier during the current RedoLNs pass, so at this
                 * time, TL must be L'. (b)  L' < ckpt_start. Then L' <= TL,
                 * because the ckpt writes all dirty BINs to the log and
                 * RedoINs is done before RedoLNs.
                 *
                 * The above observation implies that either T == TT or TL is
                 * the abortLSN of L. If TL == AL, then it is T's commit logrec
                 * that made TL obsolete during normal processing. However, here
                 * we pass L as the lsn param of countObsoleteIfUncounted().
                 * As explained below this can result in missing counting for
                 * a real obsolete logrec.
                 *
                 * If TL is immediatelly obsolete, it has been counted already,
                 * for the same reason described above in the case of 
                 * db.isLNImmediatelyObsolete(). So, to avoid double counting,
                 * we don't attempt to count it here again.
                 */
                if (!treeLsnIsImmediatelyObsolete) {
                    tracker.countObsoleteIfUncounted(
                        treeLsn, logrecLsn, null,
                        fetchLNSize(db, treeLNLoggedSize, treeLsn), db.getId(),
                        isCommitted/*trackOffset*/);
                }
            }
        }

        /*
         * The abortLSN is obsolete iff T is a committed txn. In fact, it is
         * the commit logrec of T that makes abortLSN obsolete. So, we pass
         * commitLSN as the "lsn" param to the countObsoleteIfUncounted()
         * call below. However, to avoid excessive double-counting, we don't
         * always call countObsoleteIfUncounted(AL, T-cmt). In relatively
         * rare scenarios, this can result in failing to count AL as obsolete.
         * Consider the following cases:
         *
         * TL < L
         * -------
         *
         * If TL < L we don't call countObsoleteIfUncounted() on AL, because
         * in most cases this has been done already during the current RedoLNs
         * pass or AL was counted durably as obsolete during normal processing.
         * The reasoning is as follows. As explained above, if TL < L, one of
         * the following is true:
         *
         * (a) TL == AL.
         *
         *     TL/AL --- TT-cmt --- L --- (FSLN)? --- T-cmt
         *
         *     countObsoleteIfUncounted(TL, L) was called above. If FSLN
         *     exists, this call did not count TL. However, FSLN does not
         *     contain TL, because it is T-cmt that recorded TL as obsolete.
         *     Therefore, assuming no other FSLN exists after T-cmt, we miss
         *     counting TL by not calling countObsoleteIfUncounted(TL, T-cmt).
         *
         *     However, in most cases, there won't be any FSLN between L and
         *     T-cmt, or there will be another FLSN after T-cmt. As a result,
         *     the countObsoleteIfUncounted(TL, L) call suffices. Therefore,
         *     we prefer to occasionally miss an abortLSN than doing too much
         *     double counting.
         *
         * (b1) T == TT and TL < ckpt_start. In
         *
         *      AL --- TL --- ckpt-start --- L --- T-cmt --- (FSLN)?
         *
         *      TL and L have the same abortLSN and the same commitLSN. TL is
         *      not processed during this RedoLNs pass, so unless an FSLN was
         *      logged during normal processing after T-cmt, we miss counting
         *      AL. Notice that an FSLN will exist if T-cmt occurs before
         *      ckpt-end, which is the more common case.
         *
         * (b2) T == TT and TL > ckpt_start
         *
         *      ckpt-start --- TL --- L --- (FSLN-1)? --- T-cmt --- (FSLN)?
         *
         *      TL and L have the same abortLSN and the same commitLSN.
         *      Furthermore, TL was processed during the current RedoLNs pass.
         *      We assume that what ever was done about AL during the
         *      processing of TL was correct and we don't repeat it.
         *
         * L < TL
         * -------
         *
         * (a) ckpt_start --- AL --- L --- T-cmt --- (FSLN)? --- TL --- (FSLN)?
         *
         *     AL was processed earlier in this RedoLNs pass. When it was
         *     processed, it was < TL, so countObsoleteIfUncounted(AL, TL)
         *     was called. There is no reason to count again.
         *
         * (b) ckpt_start --- AL --- L --- TL --- (FSLN))? --- T-cmt --- (FSLN)?
         *
         *     AL was processed earlier in this RedoLNs pass. When it was
         *     processed, it was < TL, so countObsoleteIfUncounted(AL, TL)
         *     was called. To avoid double counting, we will not call
         *     countObsoleteIfUncounted(AL, t-cmt). However, in this case
         *     we will fail counting AL as obsolete if there is an FSLN
         *     between TL and T-cmt and no FSLN after T-cmt.
         *
         * (c) AL --- ckpt_start --- L --- TL
         *
         *     In this case we call countObsoleteIfUncounted(AL, t-cmt)
         *
         * L == TL
         * -------
         *
         * (a) ckpt_start --- AL --- L/TL --- (FSLN)? --- T-cmt --- (FSLN)?
         *
         *     Same as L < TL, case (b).
         *
         * (c) AL --- ckpt_start --- L --- TL
         *
         *     Same as L < TL, case (c).
         *
         * As usual, we should not count abortLSN as obsolete here if it is
         * an immediatelly obsolete logrec (i.e. if abortKD == true or
         * abortData != null).
         */

        long abortLsn = logrec.getAbortLsn();
        boolean abortKD = logrec.getAbortKnownDeleted();

        if (commitLsn != DbLsn.NULL_LSN &&
            abortLsn != DbLsn.NULL_LSN &&
            !abortKD &&
            logrec.getAbortData() == null) {

            if (treeLsn == DbLsn.NULL_LSN || 
                (DbLsn.compareTo(logrecLsn, treeLsn) <= 0 &&
                 DbLsn.compareTo(abortLsn, info.checkpointStartLsn) < 0)) {
                tracker.countObsoleteIfUncounted(
                    abortLsn, commitLsn, null, 0, db.getId(),
                    true/*trackOffset*/);
            }
        }
    }

    /**
     * Update utilization info during recovery undo (not abort undo).
     *
     * Let R and T be the record and txn associated with the current logrec L.
     * L is for sure obsolete. It may or may have been counted as such already.
     * Consider the following cases:
     *
     * 1. L is an immediately obsolete logrec. Then, L was counted as obsolete
     * during normal processing and it should be counted here only if its
     * obsoleteness was not made durable before the crash, i.e., if it is
     * after the latest FSLN for the containing log file. No need to record
     * L's LSN in the tracker, because L is immediately obsolete.
     *
     * 2. L is not an immediately obsolete logrec.
     *
     * 2.1. L is the last logrec for R by T. In this case, L was not counted
     * as obsolete during normal processing. L is made obsolete here by the 
     * fact that it is undone.
     *
     * 2.2 L is not the last logrec for R by T. In this case, L was counted
     * as obsolete during normal processing it should be counted here only
     * if its obsoleteness was not made durable before the crash.
     *
     * Unfortunately, we cannot differentiate between cases 2.1 and 2.2,
     * so the code below calls tracker.countObsoleteUnconditional() for
     * both of those cases, which can result to some double counting in
     * case 2.2. However, it is not very common for a txn to update the
     * same record multiple times, so this should not be a big issue.
     */
    private void undoUtilizationInfo(
        LNLogEntry<?> logrec,
        DatabaseImpl db,
        long logrecLsn,
        int logrecSize) {

        if (logrec.isImmediatelyObsolete(db)) {
            tracker.countObsoleteIfUncounted(
                logrecLsn, logrecLsn, null/*logEntryType*/,
                logrecSize, db.getId(), false /*trackOffset*/);
        } else {
            tracker.countObsoleteUnconditional(
                logrecLsn, null/*logEntryType*/,
                logrecSize, db.getId(), true /*trackOffset*/);
        }
    }

    /**
     * Fetches the LN to get its size only if necessary and so configured.
     */
    private static int fetchLNSize(DatabaseImpl db, int size, long lsn)
        throws DatabaseException {

        if (size != 0) {
            return size;
        }
        final EnvironmentImpl envImpl = db.getEnv();
        if (!envImpl.getCleaner().getFetchObsoleteSize(db)) {
            return 0;
        }
        try {
            final LogEntryHeader header =
                envImpl.getLogManager().getWholeLogEntry(lsn).getHeader();
            return header.getEntrySize();
        } catch (FileNotFoundException e) {
            /* Ignore errors if the file was cleaned. */
        }
        return 0;
    }

    /**
     * Build the in memory inList with INs that have been made resident by the
     * recovery process.
     */
    private void buildINList()
        throws DatabaseException {

        envImpl.getInMemoryINs().enable();           // enable INList
        envImpl.getEvictor().setEnabled(true);
        envImpl.getDbTree().rebuildINListMapDb();    // scan map db

        /* For all the dbs that we read in recovery, scan for resident INs. */
        for (DatabaseId dbId : inListBuildDbIds) {
            /* We already did the map tree, don't do it again. */
            if (!dbId.equals(DbTree.ID_DB_ID)) {
                DatabaseImpl db = envImpl.getDbTree().getDb(dbId);
                try {
                    if (db != null) {
                        /* Temp DBs will be removed, skip build. */
                        if (!db.isTemporary()) {
                            db.getTree().rebuildINList();
                        }
                    }
                } finally {
                    envImpl.getDbTree().releaseDb(db);
                }
            }
        }
    }

    /**
     * Remove all temporary databases that were encountered as MapLNs during
     * recovery undo/redo.  A temp DB needs to be removed when it is not closed
     * (closing a temp DB removes it) prior to a crash.  We ensure that the
     * MapLN for every open temp DBs is logged each checkpoint interval.
     */
    private void removeTempDbs()
        throws DatabaseException {

        startupTracker.start(Phase.REMOVE_TEMP_DBS);
        startupTracker.setProgress(RecoveryProgress.REMOVE_TEMP_DBS);
        Counter counter = startupTracker.getCounter(Phase.REMOVE_TEMP_DBS);
        
        DbTree dbMapTree = envImpl.getDbTree();
        BasicLocker locker =
            BasicLocker.createBasicLocker(envImpl, false /*noWait*/);
        boolean operationOk = false;
        try {
            for (DatabaseId tempDbId : tempDbIds) {
                counter.incNumRead();
                DatabaseImpl db = dbMapTree.getDb(tempDbId);
                dbMapTree.releaseDb(db); // Decrement use count.
                if (db != null) {
                    assert db.isTemporary();
                    if (!db.isDeleted()) {
                        try {
                            counter.incNumProcessed();
                            envImpl.getDbTree().dbRemove(locker,
                                                         db.getName(),
                                                         db.getId());
                        } catch (DbTree.NeedRepLockerException e) {
                            /* Should never happen; db is never replicated. */
                            throw EnvironmentFailureException.
                                unexpectedException(envImpl, e);
                        } catch (DatabaseNotFoundException e) {
                            throw EnvironmentFailureException.
                                unexpectedException(e);
                        }
                    } else {
                        counter.incNumDeleted();
                    }
                }
            }
            operationOk = true;
        } catch (Error E) {
            envImpl.invalidate(E);
            throw E;
        } finally {
            locker.operationEnd(operationOk);
            startupTracker.stop(Phase.REMOVE_TEMP_DBS);

        }
    }

    /**
     * For committed truncate/remove NameLNs with no corresponding deleted
     * MapLN found, delete the MapLNs now.  MapLNs are deleted by
     * truncate/remove operations after logging the Commit, so it is possible
     * that a crash occurs after logging the Commit of the NameLN, and before
     * deleting the MapLN.  [#20816]
     */
    private void deleteMapLNs() {
        for (final DatabaseId id : expectDeletedMapLNs) {
            final DatabaseImpl dbImpl = envImpl.getDbTree().getDb(id);
            if (dbImpl == null) {
                continue;
            }

            /*
             * Delete the MapLN, count the DB obsolete, set the deleted
             * state, and call releaseDb.
             */
            dbImpl.finishDeleteProcessing();
        }
    }

    /*
     * Throws an EnvironmentFailureException if there is any Node entry that
     * meets these qualifications:
     * 1. It is in the recovery interval.
     * 2. it belongs to a duplicate DB.
     * 3. Its log version is less than 8.
     */
    private void checkLogVersion8UpgradeViolations()
        throws EnvironmentFailureException {

        /*
         * Previously during the initial INFileReader pass (for ID tracking) we
         * collected a set of database IDs for every Node log entry in the 
         * recovery interval that has a log version less than 8. Now that the
         * DbTree is complete we can check to see if any of these are in a
         * duplicates DB.
         */
        boolean v8DupNodes = false;
        for (DatabaseId dbId : logVersion8UpgradeDbs) {
            final DbTree dbTree = envImpl.getDbTree();
            final DatabaseImpl db = dbTree.getDb(dbId);
            try {

                /*
                 * If DB is null (deleted in the recovery interval), no
                 * conversion is needed because all entries for the DB will be
                 * discarded.  [#22203]
                 */
                if (db == null) {
                    continue;
                }

                if (db.getSortedDuplicates()) {
                    v8DupNodes = true;
                    break;
                }
            } finally {
                dbTree.releaseDb(db);
            }
        }

        boolean v8Deltas = logVersion8UpgradeDeltas.get();

        if (v8DupNodes || v8Deltas) {
            final String illegalEntries = v8DupNodes ?
                "JE 4.1 duplicate DB entries" :
                "JE 4.1 BINDeltas";
            throw EnvironmentFailureException.unexpectedState
                (illegalEntries + " were found in the recovery interval. " +
                 "Before upgrading to JE 5.0, the following utility " +
                 "must be run using JE 4.1 (4.1.20 or later): " +
                 (envImpl.isReplicated() ? 
                  "DbRepPreUpgrade_4_1 " : "DbPreUpgrade_4_1 ") +
                 ". See the change log.");
        }
    }

    /**
     * Dump a tracking list into a string.
     */
    private String printTrackList(List<TrackingInfo> trackingList) {
        if (trackingList != null) {
            StringBuilder sb = new StringBuilder();
            Iterator<TrackingInfo> iter = trackingList.iterator();
            sb.append("Trace list:");
            sb.append('\n');
            while (iter.hasNext()) {
                sb.append(iter.next());
                sb.append('\n');
            }
            return sb.toString();
        }
        return null;
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled. This is used to
     * construct verbose trace messages for individual log entry processing.
     */
    private static void trace(Logger logger,
                              DatabaseImpl database,
                              String debugType,
                              boolean success,
                              Node node,
                              long logLsn,
                              IN parent,
                              boolean found,
                              boolean replaced,
                              boolean inserted,
                              long replacedLsn,
                              long abortLsn,
                              int index) {
        trace(logger, Level.FINE, database, debugType, success, node, logLsn,
              parent, found, replaced, inserted, replacedLsn, abortLsn, index);
    }

    private static void trace(Logger logger,
                              Level level,
                              DatabaseImpl database,
                              String debugType,
                              boolean success,
                              Node node,
                              long logLsn,
                              IN parent,
                              boolean found,
                              boolean replaced,
                              boolean inserted,
                              long replacedLsn,
                              long abortLsn,
                              int index) {
        Level useLevel = level;
        if (!success) {
            useLevel = Level.SEVERE;
        }
        if (logger.isLoggable(useLevel)) {
            StringBuilder sb = new StringBuilder();
            sb.append(debugType);
            sb.append(" success=").append(success);
            if (node instanceof IN) {
                sb.append(" node=");
                sb.append(((IN) node).getNodeId());
            }
            sb.append(" lsn=");
            sb.append(DbLsn.getNoFormatString(logLsn));
            if (parent != null) {
                sb.append(" parent=").append(parent.getNodeId());
            }
            sb.append(" found=");
            sb.append(found);
            sb.append(" replaced=");
            sb.append(replaced);
            sb.append(" inserted=");
            sb.append(inserted);
            if (replacedLsn != DbLsn.NULL_LSN) {
                sb.append(" replacedLsn=");
                sb.append(DbLsn.getNoFormatString(replacedLsn));
            }
            if (abortLsn != DbLsn.NULL_LSN) {
                sb.append(" abortLsn=");
                sb.append(DbLsn.getNoFormatString(abortLsn));
            }
            sb.append(" index=").append(index);
            if (useLevel.equals(Level.SEVERE)) {
                LoggerUtils.traceAndLog(
                    logger, database.getEnv(), useLevel, sb.toString());
            } else {
                LoggerUtils.logMsg(
                    logger, database.getEnv(), useLevel, sb.toString());
            }
        }
    }

    private void traceAndThrowException(long badLsn,
                                        String method,
                                        Exception originalException)
        throws DatabaseException {

        String badLsnString = DbLsn.getNoFormatString(badLsn);
        LoggerUtils.traceAndLogException(envImpl, "RecoveryManager", method,
                                 "last LSN = " + badLsnString,
                                 originalException);
        throw new EnvironmentFailureException
            (envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
             "last LSN=" + badLsnString, originalException);
    }
}
