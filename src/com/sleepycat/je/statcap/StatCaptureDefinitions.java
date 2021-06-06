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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.sleepycat.je.cleaner.CleanerStatDefinition;
import com.sleepycat.je.dbi.DbiStatDefinition;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.evictor.EvictorStatDefinition;
import com.sleepycat.je.evictor.OffHeapStatDefinition;
import com.sleepycat.je.incomp.INCompStatDefinition;
import com.sleepycat.je.latch.LatchStatDefinition;
import com.sleepycat.je.log.LogStatDefinition;
import com.sleepycat.je.recovery.CheckpointStatDefinition;
import com.sleepycat.je.txn.LockStatDefinition;
import com.sleepycat.je.utilint.StatDefinition;

/**
 * Used to define the statistics that are projected into the
 * statistics file.
 *
 */
public class StatCaptureDefinitions {

    protected Map<String, StatDefinition> nameToDef;

    private static StatDefinition[] cleanerStats = {
        CleanerStatDefinition.CLEANER_RUNS,
        CleanerStatDefinition.CLEANER_TWO_PASS_RUNS,
        CleanerStatDefinition.CLEANER_REVISAL_RUNS,
        CleanerStatDefinition.CLEANER_DELETIONS,
        CleanerStatDefinition.CLEANER_PENDING_LN_QUEUE_SIZE,
        CleanerStatDefinition.CLEANER_INS_OBSOLETE,
        CleanerStatDefinition.CLEANER_INS_CLEANED,
        CleanerStatDefinition.CLEANER_INS_DEAD,
        CleanerStatDefinition.CLEANER_INS_MIGRATED,
        CleanerStatDefinition.CLEANER_BIN_DELTAS_OBSOLETE,
        CleanerStatDefinition.CLEANER_BIN_DELTAS_CLEANED,
        CleanerStatDefinition.CLEANER_BIN_DELTAS_DEAD,
        CleanerStatDefinition.CLEANER_BIN_DELTAS_MIGRATED,
        CleanerStatDefinition.CLEANER_LNS_OBSOLETE,
        CleanerStatDefinition.CLEANER_LNS_CLEANED,
        CleanerStatDefinition.CLEANER_LNS_DEAD,
        CleanerStatDefinition.CLEANER_LNS_EXPIRED,
        CleanerStatDefinition.CLEANER_LNS_LOCKED,
        CleanerStatDefinition.CLEANER_LNS_MIGRATED,
        CleanerStatDefinition.CLEANER_LNS_MARKED,
        CleanerStatDefinition.CLEANER_LNQUEUE_HITS,
        CleanerStatDefinition.CLEANER_PENDING_LNS_PROCESSED,
        CleanerStatDefinition.CLEANER_MARKED_LNS_PROCESSED,
        CleanerStatDefinition.CLEANER_TO_BE_CLEANED_LNS_PROCESSED,
        CleanerStatDefinition.CLEANER_CLUSTER_LNS_PROCESSED,
        CleanerStatDefinition.CLEANER_PENDING_LNS_LOCKED,
        CleanerStatDefinition.CLEANER_ENTRIES_READ,
        CleanerStatDefinition.CLEANER_DISK_READS,
        CleanerStatDefinition.CLEANER_REPEAT_ITERATOR_READS,
        CleanerStatDefinition.CLEANER_TOTAL_LOG_SIZE,
        CleanerStatDefinition.CLEANER_ACTIVE_LOG_SIZE,
        CleanerStatDefinition.CLEANER_RESERVED_LOG_SIZE,
        CleanerStatDefinition.CLEANER_AVAILABLE_LOG_SIZE,
        CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE,
        CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE_MAP,
        CleanerStatDefinition.CLEANER_MIN_UTILIZATION,
        CleanerStatDefinition.CLEANER_MAX_UTILIZATION,
    };

    private static StatDefinition[] dbiStats = {
        DbiStatDefinition.MB_SHARED_CACHE_TOTAL_BYTES,
        DbiStatDefinition.MB_TOTAL_BYTES,
        DbiStatDefinition.MB_DATA_BYTES,
        DbiStatDefinition.MB_DATA_ADMIN_BYTES,
        DbiStatDefinition.MB_DOS_BYTES,
        DbiStatDefinition.MB_ADMIN_BYTES,
        DbiStatDefinition.MB_LOCK_BYTES,
    };

    private static StatDefinition[] environmentStats = {
        DbiStatDefinition.ENV_RELATCHES_REQUIRED,
        DbiStatDefinition.ENV_CREATION_TIME,
        DbiStatDefinition.ENV_BIN_DELTA_GETS,
        DbiStatDefinition.ENV_BIN_DELTA_INSERTS,
        DbiStatDefinition.ENV_BIN_DELTA_UPDATES,
        DbiStatDefinition.ENV_BIN_DELTA_DELETES,
    };

    private static StatDefinition[] evictorStats = {
        EvictorStatDefinition.EVICTOR_EVICTION_RUNS,
        EvictorStatDefinition.EVICTOR_NODES_TARGETED,
        EvictorStatDefinition.EVICTOR_NODES_EVICTED,
        EvictorStatDefinition.EVICTOR_NODES_STRIPPED,
        EvictorStatDefinition.EVICTOR_NODES_MUTATED,
        EvictorStatDefinition.EVICTOR_NODES_PUT_BACK,
        EvictorStatDefinition.EVICTOR_NODES_MOVED_TO_PRI2_LRU,
        EvictorStatDefinition.EVICTOR_NODES_SKIPPED,
        EvictorStatDefinition.EVICTOR_ROOT_NODES_EVICTED,
        EvictorStatDefinition.EVICTOR_DIRTY_NODES_EVICTED,
        EvictorStatDefinition.EVICTOR_LNS_EVICTED,

        EvictorStatDefinition.EVICTOR_SHARED_CACHE_ENVS,

        EvictorStatDefinition.LN_FETCH,
        EvictorStatDefinition.LN_FETCH_MISS,
        EvictorStatDefinition.UPPER_IN_FETCH,
        EvictorStatDefinition.UPPER_IN_FETCH_MISS,
        EvictorStatDefinition.BIN_FETCH,
        EvictorStatDefinition.BIN_FETCH_MISS,
        EvictorStatDefinition.BIN_DELTA_FETCH_MISS,
        EvictorStatDefinition.BIN_FETCH_MISS_RATIO,
        EvictorStatDefinition.FULL_BIN_MISS,

        EvictorStatDefinition.BIN_DELTA_BLIND_OPS,

        EvictorStatDefinition.CACHED_UPPER_INS,
        EvictorStatDefinition.CACHED_BINS,
        EvictorStatDefinition.CACHED_BIN_DELTAS,

        EvictorStatDefinition.THREAD_UNAVAILABLE,

        EvictorStatDefinition.CACHED_IN_SPARSE_TARGET,
        EvictorStatDefinition.CACHED_IN_NO_TARGET,
        EvictorStatDefinition.CACHED_IN_COMPACT_KEY,

        EvictorStatDefinition.PRI1_LRU_SIZE,
        EvictorStatDefinition.PRI2_LRU_SIZE,

        EvictionSource.CACHEMODE.getNumBytesEvictedStatDef(),
        EvictionSource.CRITICAL.getNumBytesEvictedStatDef(),
        EvictionSource.DAEMON.getNumBytesEvictedStatDef(),
        EvictionSource.EVICTORTHREAD.getNumBytesEvictedStatDef(),
        EvictionSource.MANUAL.getNumBytesEvictedStatDef(),
    };

    private static StatDefinition[] offHeapStats = {
        OffHeapStatDefinition.ALLOC_FAILURE,
        OffHeapStatDefinition.ALLOC_OVERFLOW,
        OffHeapStatDefinition.THREAD_UNAVAILABLE,
        OffHeapStatDefinition.NODES_TARGETED,
        OffHeapStatDefinition.CRITICAL_NODES_TARGETED,
        OffHeapStatDefinition.NODES_EVICTED,
        OffHeapStatDefinition.DIRTY_NODES_EVICTED,
        OffHeapStatDefinition.NODES_STRIPPED,
        OffHeapStatDefinition.NODES_MUTATED,
        OffHeapStatDefinition.NODES_SKIPPED,
        OffHeapStatDefinition.LNS_EVICTED,
        OffHeapStatDefinition.LNS_LOADED,
        OffHeapStatDefinition.LNS_STORED,
        OffHeapStatDefinition.BINS_LOADED,
        OffHeapStatDefinition.BINS_STORED,
        OffHeapStatDefinition.CACHED_LNS,
        OffHeapStatDefinition.CACHED_BINS,
        OffHeapStatDefinition.CACHED_BIN_DELTAS,
        OffHeapStatDefinition.TOTAL_BYTES,
        OffHeapStatDefinition.TOTAL_BLOCKS,
        OffHeapStatDefinition.LRU_SIZE,
    };

    private static StatDefinition[] inCompStats = {
        INCompStatDefinition.INCOMP_SPLIT_BINS,
        INCompStatDefinition.INCOMP_DBCLOSED_BINS,
        INCompStatDefinition.INCOMP_CURSORS_BINS,
        INCompStatDefinition.INCOMP_NON_EMPTY_BINS,
        INCompStatDefinition.INCOMP_PROCESSED_BINS,
        INCompStatDefinition.INCOMP_QUEUE_SIZE
    };

    private static StatDefinition[] latchStats = {
        LatchStatDefinition.LATCH_NO_WAITERS,
        LatchStatDefinition.LATCH_SELF_OWNED,
        LatchStatDefinition.LATCH_CONTENTION,
        LatchStatDefinition.LATCH_NOWAIT_SUCCESS,
        LatchStatDefinition.LATCH_NOWAIT_UNSUCCESS,
        LatchStatDefinition.LATCH_RELEASES
    };

    private static StatDefinition[] logStats = {
        LogStatDefinition.FILEMGR_RANDOM_READS,
        LogStatDefinition.FILEMGR_RANDOM_WRITES,
        LogStatDefinition.FILEMGR_SEQUENTIAL_READS,
        LogStatDefinition.FILEMGR_SEQUENTIAL_WRITES,
        LogStatDefinition.FILEMGR_RANDOM_READ_BYTES,
        LogStatDefinition.FILEMGR_RANDOM_WRITE_BYTES,
        LogStatDefinition.FILEMGR_SEQUENTIAL_READ_BYTES,
        LogStatDefinition.FILEMGR_SEQUENTIAL_WRITE_BYTES,
        LogStatDefinition.FILEMGR_FILE_OPENS,
        LogStatDefinition.FILEMGR_OPEN_FILES,
        LogStatDefinition.FILEMGR_BYTES_READ_FROM_WRITEQUEUE,
        LogStatDefinition.FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE,
        LogStatDefinition.FILEMGR_READS_FROM_WRITEQUEUE,
        LogStatDefinition.FILEMGR_WRITES_FROM_WRITEQUEUE,
        LogStatDefinition.FILEMGR_WRITEQUEUE_OVERFLOW,
        LogStatDefinition.FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES,
        LogStatDefinition.FSYNCMGR_FSYNCS,
        LogStatDefinition.FSYNCMGR_FSYNC_REQUESTS,
        LogStatDefinition.FSYNCMGR_TIMEOUTS,
        LogStatDefinition.FILEMGR_LOG_FSYNCS,
        LogStatDefinition.GRPCMGR_FSYNC_TIME,
        LogStatDefinition.GRPCMGR_FSYNC_MAX_TIME,
        LogStatDefinition.GRPCMGR_N_GROUP_COMMIT_REQUESTS,
        LogStatDefinition.GRPCMGR_N_GROUP_COMMIT_WAITS,
        LogStatDefinition.GRPCMGR_N_LOG_INTERVAL_EXCEEDED,
        LogStatDefinition.GRPCMGR_N_LOG_MAX_GROUP_COMMIT,
        LogStatDefinition.LOGMGR_REPEAT_FAULT_READS,
        LogStatDefinition.LOGMGR_TEMP_BUFFER_WRITES,
        LogStatDefinition.LOGMGR_END_OF_LOG,
        LogStatDefinition.LBFP_NO_FREE_BUFFER,
        LogStatDefinition.LBFP_NOT_RESIDENT,
        LogStatDefinition.LBFP_MISS,
        LogStatDefinition.LBFP_LOG_BUFFERS,
        LogStatDefinition.LBFP_BUFFER_BYTES
    };

    private static StatDefinition[] checkpointStats = {
        CheckpointStatDefinition.CKPT_CHECKPOINTS,
        CheckpointStatDefinition.CKPT_LAST_CKPTID,
        CheckpointStatDefinition.CKPT_FULL_IN_FLUSH,
        CheckpointStatDefinition.CKPT_FULL_BIN_FLUSH,
        CheckpointStatDefinition.CKPT_DELTA_IN_FLUSH,
        CheckpointStatDefinition.CKPT_LAST_CKPT_INTERVAL,
        CheckpointStatDefinition.CKPT_LAST_CKPT_START,
        CheckpointStatDefinition.CKPT_LAST_CKPT_END
    };

    private static StatDefinition[] throughputStats = {

        DbiStatDefinition.THROUGHPUT_PRI_SEARCH,
        DbiStatDefinition.THROUGHPUT_PRI_SEARCH_FAIL,
        DbiStatDefinition.THROUGHPUT_SEC_SEARCH,
        DbiStatDefinition.THROUGHPUT_SEC_SEARCH_FAIL,
        DbiStatDefinition.THROUGHPUT_PRI_POSITION,
        DbiStatDefinition.THROUGHPUT_SEC_POSITION,
        DbiStatDefinition.THROUGHPUT_PRI_INSERT,
        DbiStatDefinition.THROUGHPUT_PRI_INSERT_FAIL,
        DbiStatDefinition.THROUGHPUT_SEC_INSERT,
        DbiStatDefinition.THROUGHPUT_PRI_UPDATE,
        DbiStatDefinition.THROUGHPUT_SEC_UPDATE,
        DbiStatDefinition.THROUGHPUT_PRI_DELETE,
        DbiStatDefinition.THROUGHPUT_PRI_DELETE_FAIL,
        DbiStatDefinition.THROUGHPUT_SEC_DELETE,
    };

    private static StatDefinition[] lockStats = {
        LockStatDefinition.LOCK_REQUESTS,
        LockStatDefinition.LOCK_WAITS,
    };

    /*
     * Define min/max stats using the group name returned by loadStats not
     * necessarily what is defined in the underlying statistic. Some groups are
     * combined into a super group.
     */
    public static StatManager.SDef[] minStats = {};

    public static StatManager.SDef[] maxStats = {
        new StatManager.SDef(LogStatDefinition.GROUP_NAME,
                             LogStatDefinition.GRPCMGR_FSYNC_MAX_TIME)
    };

    public StatCaptureDefinitions() {
        nameToDef = new HashMap<String, StatDefinition>();
        String groupname = EvictorStatDefinition.GROUP_NAME;
        for (StatDefinition stat : evictorStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        for (StatDefinition stat : dbiStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = OffHeapStatDefinition.GROUP_NAME;
        for (StatDefinition stat : offHeapStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = CheckpointStatDefinition.GROUP_NAME;
        for (StatDefinition stat : checkpointStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = CleanerStatDefinition.GROUP_NAME;
        for (StatDefinition stat : cleanerStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = LogStatDefinition.GROUP_NAME;
        for (StatDefinition stat : logStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = LockStatDefinition.GROUP_NAME;
        for (StatDefinition stat : lockStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        for (StatDefinition stat : latchStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = DbiStatDefinition.ENV_GROUP_NAME;
        for (StatDefinition stat : environmentStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = INCompStatDefinition.GROUP_NAME;
        for (StatDefinition stat : inCompStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = DbiStatDefinition.THROUGHPUT_GROUP_NAME;
        for (StatDefinition stat : throughputStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
    }

    public SortedSet<String> getStatisticProjections() {
        SortedSet<String> retval = new TreeSet<String>();
        getProjectionsInternal(retval);
        return retval;
    }

    protected void getProjectionsInternal(SortedSet<String> pmap) {
        String groupname = EvictorStatDefinition.GROUP_NAME;
        for (StatDefinition stat : evictorStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        for (StatDefinition stat : dbiStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = OffHeapStatDefinition.GROUP_NAME;
        for (StatDefinition stat : offHeapStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = CheckpointStatDefinition.GROUP_NAME;
        for (StatDefinition stat : checkpointStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = CleanerStatDefinition.GROUP_NAME;
        for (StatDefinition stat : cleanerStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = LogStatDefinition.GROUP_NAME;
        for (StatDefinition stat : logStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = LockStatDefinition.GROUP_NAME;
        for (StatDefinition stat : lockStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        for (StatDefinition stat : latchStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = DbiStatDefinition.ENV_GROUP_NAME;
        for (StatDefinition stat : environmentStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = INCompStatDefinition.GROUP_NAME;
        for (StatDefinition stat : inCompStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
        groupname = DbiStatDefinition.THROUGHPUT_GROUP_NAME;
        for (StatDefinition stat : throughputStats) {
            pmap.add(groupname + ":" + stat.getName());
        }
    }

    /**
     * Used to get a statistics definition. This method is used
     * for testing purposes only.
     * @param colname in format groupname:statname.
     * @return statistics definition or null of not defined.
     */
    public StatDefinition getDefinition(String colname) {
        return nameToDef.get(colname);
    }
}
