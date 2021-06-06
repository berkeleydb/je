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

package com.sleepycat.je;

import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_ACTIVE_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_AVAILABLE_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_BIN_DELTAS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_CLUSTER_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_DELETIONS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_DISK_READS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_ENTRIES_READ;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_INS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNQUEUE_HITS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_CLEANED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_DEAD;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_EXPIRED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_LOCKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_MARKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_MIGRATED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_LNS_OBSOLETE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MARKED_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MAX_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_MIN_UTILIZATION;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LNS_LOCKED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PENDING_LN_QUEUE_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_PROTECTED_LOG_SIZE_MAP;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_REPEAT_ITERATOR_READS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_RESERVED_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_REVISAL_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_RUNS;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TOTAL_LOG_SIZE;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TO_BE_CLEANED_LNS_PROCESSED;
import static com.sleepycat.je.cleaner.CleanerStatDefinition.CLEANER_TWO_PASS_RUNS;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_BIN_DELTA_DELETES;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_BIN_DELTA_GETS;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_BIN_DELTA_INSERTS;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_BIN_DELTA_UPDATES;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_CREATION_TIME;
import static com.sleepycat.je.dbi.DbiStatDefinition.ENV_RELATCHES_REQUIRED;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_ADMIN_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_DATA_ADMIN_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_DATA_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_DOS_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_LOCK_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_SHARED_CACHE_TOTAL_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.MB_TOTAL_BYTES;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_DELETE;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_DELETE_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_INSERT;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_INSERT_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_POSITION;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_SEARCH;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_SEARCH_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_PRI_UPDATE;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_DELETE;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_INSERT;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_POSITION;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_SEARCH;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_SEARCH_FAIL;
import static com.sleepycat.je.dbi.DbiStatDefinition.THROUGHPUT_SEC_UPDATE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_DELTA_BLIND_OPS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_DELTA_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS_RATIO;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_BINS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_BIN_DELTAS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_COMPACT_KEY;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_NO_TARGET;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_IN_SPARSE_TARGET;
import static com.sleepycat.je.evictor.EvictorStatDefinition.CACHED_UPPER_INS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_DIRTY_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_EVICTION_RUNS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_LNS_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_MOVED_TO_PRI2_LRU;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_MUTATED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_PUT_BACK;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_SKIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_STRIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_TARGETED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_ROOT_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_SHARED_CACHE_ENVS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.FULL_BIN_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.PRI1_LRU_SIZE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.PRI2_LRU_SIZE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.THREAD_UNAVAILABLE;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH_MISS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_CURSORS_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_DBCLOSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_NON_EMPTY_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_PROCESSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_QUEUE_SIZE;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_SPLIT_BINS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_CONTENTION;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_NOWAIT_SUCCESS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_NOWAIT_UNSUCCESS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_NO_WAITERS;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_RELEASES;
import static com.sleepycat.je.latch.LatchStatDefinition.LATCH_SELF_OWNED;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_BYTES_READ_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_FILE_OPENS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_LOG_FSYNCS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_OPEN_FILES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_READS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_READ_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_WRITES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_RANDOM_WRITE_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_READS_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_READS;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_READ_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_WRITES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_SEQUENTIAL_WRITE_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_WRITEQUEUE_OVERFLOW;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES;
import static com.sleepycat.je.log.LogStatDefinition.FILEMGR_WRITES_FROM_WRITEQUEUE;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_FSYNCS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_FSYNC_REQUESTS;
import static com.sleepycat.je.log.LogStatDefinition.FSYNCMGR_TIMEOUTS;
import static com.sleepycat.je.log.LogStatDefinition.GRPCMGR_FSYNC_MAX_TIME;
import static com.sleepycat.je.log.LogStatDefinition.GRPCMGR_FSYNC_TIME;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_BUFFER_BYTES;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_LOG_BUFFERS;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_MISS;
import static com.sleepycat.je.log.LogStatDefinition.LBFP_NOT_RESIDENT;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_END_OF_LOG;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_REPEAT_FAULT_READS;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_TEMP_BUFFER_WRITES;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_CHECKPOINTS;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_DELTA_IN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_FULL_BIN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_FULL_IN_FLUSH;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPTID;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_END;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_INTERVAL;
import static com.sleepycat.je.recovery.CheckpointStatDefinition.CKPT_LAST_CKPT_START;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_OWNERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_REQUESTS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_TOTAL;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.je.cleaner.CleanerStatDefinition;
import com.sleepycat.je.dbi.DbiStatDefinition;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.evictor.EvictorStatDefinition;
import com.sleepycat.je.evictor.OffHeapStatDefinition;
import com.sleepycat.je.incomp.INCompStatDefinition;
import com.sleepycat.je.log.LogStatDefinition;
import com.sleepycat.je.recovery.CheckpointStatDefinition;
import com.sleepycat.je.txn.LockStatDefinition;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.util.DbCacheSize;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Statistics for a single environment. Statistics provide indicators for
 * system monitoring and performance tuning.
 *
 * <p>Each statistic has a name and a getter method in this class. For example,
 * the {@code cacheTotalBytes} stat is returned by the {@link
 * #getCacheTotalBytes()} method. Statistics are categorized into several
 * groups, for example, {@code cacheTotalBytes} is in the {@code Cache}
 * group. Each stat and group has a name and a description.</p>
 *
 * <p>Viewing the statistics through {@link #toString()} shows the stat names
 * and values organized by group. Viewing the stats with {@link
 * #toStringVerbose()} additionally shows the description of each stat and
 * group.</p>
 *
 * <p>Statistics are periodically output in CSV format to the je.stat.csv file
 * (see {@link EnvironmentConfig#STATS_COLLECT}). The column header in the .csv
 * file has {@code group:stat} format, where 'group' is the group name and
 * 'stat' is the stat name. In Oracle NoSQL DB, in the addition to the .csv
 * file, JE stats are output in the .stat files.</p>
 *
 * <p>Stat values may also be obtained via JMX using the {@link <a
 * href="{@docRoot}/../jconsole/JConsole-plugin.html">JEMonitor mbean</a>}.
 * In Oracle NoSQL DB, JE stats are obtained via a different JMX interface in
 * JSON format. The JSON format uses property names of the form {@code
 * group_stat} where 'group' is the group name and 'stat' is the stat name.</p>
 *
 * <p>The stat groups are listed below. Each group name links to a summary of
 * the statistics in the group.</p>
 *
 * <table>
 *     <tr>
 *         <th>Group Name</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td><a href="#cache">{@value
 *         com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#cache">{@value
 *         com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#log">{@value
 *         com.sleepycat.je.log.LogStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.log.LogStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#cleaner">{@value
 *         com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#incomp">{@value
 *         com.sleepycat.je.incomp.INCompStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.incomp.INCompStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#ckpt">{@value
 *         com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.recovery.CheckpointStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#lock">{@value
 *         com.sleepycat.je.txn.LockStatDefinition#GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.txn.LockStatDefinition#GROUP_DESC}
 *         </td>
 *     </tr>
 *     <tr>
 *         <td><a href="#env">{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_NAME}
 *         </a></td>
 *         <td>{@value
 *         com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_DESC}
 *         </td>
 *     </tr>
 * </table>
 *
 * <p>The following sections describe each group of stats along with some
 * common strategies for using them for monitoring and performance tuning.</p>
 *
 * <h3><a name="cache">Cache Statistics</a></h3>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
 * <br/>Description: {@value
 * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_DESC}</p>
 *
 * <p style="margin-left: 2em">Group Name: {@value
 * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
 * <br/>Description: {@value
 * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_DESC}</p>
 *
 * <p>The JE cache consists of the main (in-heap) cache and and optional
 * off-heap cache. The vast majority of the cache is occupied by Btree nodes,
 * including internal nodes (INs) and leaf nodes (LNs). INs contain record keys
 * while LNs contain record data.</p>
 *
 * <p>Each IN refers to a configured maximum number of child nodes ({@link
 * EnvironmentConfig#NODE_MAX_ENTRIES}). The INs form a Btree of at least 2
 * levels. With a large data set the Btree will normally have 4 or 5 levels.
 * The top level is a single node, the root IN. Levels are numbered from the
 * bottom up, starting with level 1 for bottom level INs (BINs). Levels are
 * added at the top when the root IN splits.</p>
 *
 * <p>When an off-heap cache is configured, it serves as an overflow for the
 * main cache. See {@link EnvironmentConfig#MAX_OFF_HEAP_MEMORY}.</p>
 *
 * <h4><a name="cacheSizing">Cache Statistics: Sizing</a></h4>
 *
 * <p>Operation performance is often directly proportional to how much of the
 * active data set is cached. BINs and LNs form the vast majority of the cache.
 * Caching of BINs and LNs have different performance impacts, and behavior
 * varies depending on whether an off-heap cache is configured and which {@link
 * CacheMode} is used.</p>
 *
 * <p>Main cache current usage is indicated by the following stats. Note that
 * there is currently no stat for the number of LNs in the main cache.</p>
 * <ul>
 *     <li>{@link #getCacheTotalBytes()}</li>
 *     <li>{@link #getNCachedBINs()}</li>
 *     <li>{@link #getNCachedBINDeltas()}</li>
 *     <li>{@link #getNCachedUpperINs()}</li>
 * </ul>
 *
 * <p>Off-heap cache current usage is indicated by:</p>
 * <ul>
 *     <li>{@link #getOffHeapTotalBytes()}</li>
 *     <li>{@link #getOffHeapCachedLNs()}</li>
 *     <li>{@link #getOffHeapCachedBINs()}</li>
 *     <li>{@link #getOffHeapCachedBINDeltas()}</li>
 * </ul>
 * </p>
 *
 * <p>A cache miss is considered a miss only when the object is not found in
 * either cache. Misses often result in file I/O and are a primary indicator
 * of cache performance. Fetches (access requests) and misses are indicated
 * by:</p>
 * <ul>
 *     <li>{@link #getNLNsFetch()}</li>
 *     <li>{@link #getNLNsFetchMiss()}</li>
 *     <li>{@link #getNBINsFetch()}</li>
 *     <li>{@link #getNBINsFetchMiss()}</li>
 *     <li>{@link #getNBINDeltasFetchMiss()}</li>
 *     <li>{@link #getNFullBINsMiss()}</li>
 *     <li>{@link #getNUpperINsFetch()}</li>
 *     <li>{@link #getNUpperINsFetchMiss()}</li>
 * </ul>
 *
 * <p>When the number of LN misses ({@code nLNsFetchMiss}) or the number of
 * BIN misses ({@code nBINsFetchMiss + nFullBINsMiss}) are significant, the
 * JE cache may be undersized, as discussed below. But note that it is not
 * practical to correlate the number of fetches and misses directly to
 * application operations, because LNs are sometimes
 * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}, BINs are sometimes
 * accessed multiple times per operation, and internal Btree accesses are
 * included in the stat values.</p>
 *
 * <p>Ideally, all BINs and LNs for the active data set should fit in cache so
 * that operations do not result in fetch misses, which often perform random
 * read I/O. When this is not practical, which is often the case for large
 * data sets, the next best thing is to ensure that all BINs fit in cache,
 * so that an operation will perform at most one random read I/O to fetch
 * the LN. The {@link DbCacheSize} javadoc describes how to size the cache
 * to ensure that all BINs and/or LNs fit in cache.</p>
 *
 * <p>Normally {@link EnvironmentConfig#MAX_MEMORY_PERCENT} determines the JE
 * cache size as a value relative to the JVM heap size, i.e., the heap size
 * determines the cache size.</p>
 *
 * <p>For configuring cache size and behavior, see:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#MAX_MEMORY_PERCENT}</li>
 *     <li>{@link EnvironmentConfig#MAX_MEMORY}</li>
 *     <li>{@link EnvironmentConfig#MAX_OFF_HEAP_MEMORY}</li>
 *     <li>{@link EnvironmentConfig#setCacheMode(CacheMode)}</li>
 *     <li>{@link CacheMode}</li>
 *     <li>{@link DbCacheSize}</li>
 * </ul>
 *
 * <p>When using Oracle NoSQL DB, a sizing exercise and {@link DbCacheSize} are
 * used to determine the cache size needed to hold all BINs in memory. The
 * memory available to each node is divided between a 32 GB heap for the JVM
 * process (so that CompressedOops may be used) and the off-heap cache (when
 * more than 32 GB of memory is available).</p>
 *
 * <p>It is also important not to configured the cache size too large, relative
 * to the JVM heap size. If there is not enough free space in the heap, Java
 * GC pauses may become a problem. Increasing the default value for {@code
 * MAX_MEMORY_PERCENT}, or setting {@code MAX_MEMORY} (which overrides {@code
 * MAX_MEMORY_PERCENT}), should be done carefully.</p>
 *
 * <p>Java GC performance may also be improved by using {@link
 * CacheMode#EVICT_LN}. Record data sizes should also be kept below 1 MB to
 * avoid "humongous objects" (see Java GC documentation).</p>
 *
 * <p>When using Oracle NoSQL DB, by default, {@code MAX_MEMORY_PERCENT} is
 * set to 70% and {@link CacheMode#EVICT_LN} is used. The LOB (large object)
 * API is implemented using multiple JE records per LOB where the data size of
 * each record is 1 MB or less.</p>
 *
 * <p>When a shared cache is configured, the main and off-heap cache may be
 * shared by multiple JE Environments in a single JVM process. See:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#SHARED_CACHE}</li>
 *     <li>{@link #getSharedCacheTotalBytes()}</li>
 *     <li>{@link #getNSharedCacheEnvironments()}</li>
 * </ul>
 *
 * <p>When using Oracle NoSQL DB, the JE shared cache feature is not used
 * because each node only uses a single JE Environment.</p>
 *
 * <h4><a name="cacheSizeOptimizations">Cache Statistics: Size
 * Optimizations</a></h4>
 *
 * <p>Since a large portion of an IN consists of record keys, JE uses
 * {@link DatabaseConfig#setKeyPrefixing(boolean) key prefix compression}.
 * Ideally, key suffixes are small enough to be stored using the {@link
 * EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH compact key format}. The
 * following stat indicates the number of INs using this compact format:</p>
 * <ul>
 *     <li>{@link #getNINCompactKeyIN()}</li>
 * </ul>
 *
 * <p>Configuration params impacting key prefixing and the compact key format
 * are:</p>
 * <ul>
 *     <li>{@link DatabaseConfig#setKeyPrefixing(boolean)}</li>
 *     <li>{@link EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH}</li>
 * </ul>
 *
 * <p>Enabling key prefixing for all databases is strongly recommended. When
 * using Oracle NoSQL DB, key prefixing is always enabled.</p>
 *
 * <p>Another configuration param impacting BIN cache size is {@code
 * TREE_MAX_EMBEDDED_LN}. There is currently no stat indicating the number of
 * embedded LNs. See:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN}</li>
 * </ul>
 *
 * <h4><a name="cacheUnexpectedSizes">Cache Statistics: Unexpected
 * Sizes</a></h4>
 *
 * <p>Although the Btree normally occupies the vast majority of the cache, it
 * is possible that record locks occupy unexpected amounts of cache when
 * large transactions are used, or when cursors or transactions are left open
 * due to application bugs. The following stat indicates the amount of cache
 * used by record locks:</p>
 * <ul>
 *     <li>{@link #getLockBytes()}</li>
 * </ul>
 *
 * <p>To reduce the amount of memory used for record locks:</p>
 * <ul>
 *     <li>Use a small number of write operations per transaction. Write
 *     locks are held until the end of a transaction.</li>
 *     <li>For transactions using Serializable isolation or RepeatableRead
 *     isolation (the default), use a small number of read operations per
 *     transaction.</li>
 *     <li>To read large numbers of records, use {@link
 *     LockMode#READ_COMMITTED} isolation or use a null Transaction (which
 *     implies ReadCommitted). With ReadCommitted isolation, locks are
 *     released after each read operation. Using {@link
 *     LockMode#READ_UNCOMMITTED} will also avoid record locks, but does not
 *     provide any transactional guarantees.</li>
 *     <li>Ensure that all cursors and transactions are closed
 *     promptly.</li>
 * </ul>
 *
 * <p>Note that the above guidelines are also important for reducing contention
 * when records are accessed concurrently from multiple threads and
 * transactions. When using Oracle NoSQL DB, the application should avoid
 * performing a large number of write operations in a single request. For read
 * operations, NoSQL DB uses ReadCommitted isolation to avoid accumulation of
 * locks.</p>
 *
 * <p>Another unexpected use of cache is possible when using a {@link
 * DiskOrderedCursor} or when calling {@link Database#count()}. The amount of
 * cache used by these operations is indicated by:</p>
 * <ul>
 *     <li>{@link #getDOSBytes()}</li>
 * </ul>
 *
 * <p>{@code DiskOrderedCursor} and {@code Database.count} should normally be
 * explicitly constrained to use a maximum amount of cache memory. See:</p>
 * <ul>
 *     <li>{@link DiskOrderedCursorConfig#setInternalMemoryLimit(long)}</li>
 *     <li>{@link Database#count(long)}</li>
 * </ul>
 *
 * <p>Oracle NoSQL DB does not currently use {@code DiskOrderedCursor} or
 * {@code Database.count}.</p>
 *
 * <h4><a name="cacheEviction">Cache Statistics: Eviction</a></h4>
 *
 * <p>Eviction is removal of Btree node from the cache in order to make room
 * for newly added nodes. See {@link CacheMode} for a description of
 * eviction.</p>
 *
 * <p>Normally eviction is performed via background threads in the eviction
 * thread pools. Disabling the eviction pool threads is not recommended.</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#ENV_RUN_EVICTOR}</li>
 *     <li>{@link EnvironmentConfig#ENV_RUN_OFFHEAP_EVICTOR}</li>
 * </ul>
 *
 * <p>Eviction stats are important indicator of cache efficiency and provide a
 * deeper understanding of cache behavior. Main cache eviction is indicated
 * by:</p>
 * <ul>
 *     <li>{@link #getNLNsEvicted()}</li>
 *     <li>{@link #getNNodesMutated()}</li>
 *     <li>{@link #getNNodesEvicted()}</li>
 *     <li>{@link #getNDirtyNodesEvicted()}</li>
 * </ul>
 *
 * <p>Note that objects evicted from the main cache are moved to the off-heap
 * cache whenever possible.</p>
 *
 * <p>Off-heap cache eviction is indicated by:</p>
 * <ul>
 *     <li>{@link #getOffHeapLNsEvicted()}</li>
 *     <li>{@link #getOffHeapNodesMutated()}</li>
 *     <li>{@link #getOffHeapNodesEvicted()}</li>
 *     <li>{@link #getOffHeapDirtyNodesEvicted()}</li>
 * </ul>
 *
 * <p>When analyzing Java GC performance, the most relevant stats are {@code
 * NLNsEvicted}, {@code NNodesMutated} and {@code NNodesEvicted}, which all
 * indicate eviction from the main cache based on LRU. Large values for these
 * stats indicate that many old generation Java objects are being GC'd, which
 * is often a cause of GC pauses.</p>
 *
 * <p>Note that {@link CacheMode#EVICT_LN} is used or when LNs are {@link
 * EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}, {@code NLNsEvicted} will
 * be close to zero because LNs are not evicted based on LRU. And if an
 * off-heap cache is configured, {@code NNodesMutated} will be close to zero
 * because BIN mutation takes place in the off-heap cache. If any of the three
 * values are large, this points to a potential GC performance problem. The GC
 * logs should be consulted to confirm this.</p>
 *
 * <p>Large values for {@code NDirtyNodesEvicted} or {@code
 * OffHeapDirtyNodesEvicted} indicate that the cache is severely undersized and
 * there is a risk of using all available disk space and severe performance
 * problems. Dirty nodes are evicted last (after evicting all non-dirty nodes)
 * because they must be written to disk. This causes excessive writing and JE
 * log cleaning may be unproductive.</p>
 *
 * <p>Note that when an off-heap cache is configured, {@code
 * NDirtyNodesEvicted} will be zero because dirty nodes in the main cache are
 * moved to the off-heap cache if they don't fit in the main cache, and are
 * evicted completely and written to disk only when they don't fit in the
 * off-heap cache.</p>
 *
 * <p>Another type of eviction tuning for the main cache involves changing the
 * number of bytes evicted each time an evictor thread is awoken:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_EVICT_BYTES}</li>
 * </ul>
 *
 * <p>If the number of bytes is too large, it may cause a noticeable spike in
 * eviction activity, reducing resources available to other threads. If the
 * number of bytes is too small, the overhead of waking the evictor threads
 * more often may be noticeable. The default values for this parameter is
 * generally a good compromise. This parameter also impacts critical eviction,
 * which is described next.</p>
 *
 * <p>Note that the corresponding parameter for the off-heap cache, {@link
 * EnvironmentConfig#OFFHEAP_EVICT_BYTES}, works differently and is described
 * in the next section.</p>
 *
 * <h4><a name="cacheCriticalEviction">Cache Statistics: Critical
 * Eviction</a></h4>
 *
 * <p>The following stats indicate that critical eviction is occurring:</p>
 * <ul>
 *     <li>{@link #getNBytesEvictedCritical()}</li>
 *     <li>{@link #getNBytesEvictedCacheMode()}</li>
 *     <li>{@link #getNBytesEvictedDeamon()}</li>
 *     <li>{@link #getNBytesEvictedEvictorThread()}</li>
 *     <li>{@link #getNBytesEvictedManual()}</li>
 *     <li>{@link #getOffHeapCriticalNodesTargeted()}</li>
 *     <li>{@link #getOffHeapNodesTargeted()}</li>
 * </ul>
 *
 * <p>Eviction is performed by eviction pool threads, calls to {@link
 * Environment#evictMemory()} in application background threads, or via {@link
 * CacheMode#EVICT_LN} or {@link CacheMode#EVICT_BIN}. If these mechanisms are
 * not sufficient to evict memory from cache as quickly as CRUD operations are
 * adding memory to cache, then critical eviction comes into play. Critical
 * eviction is performed in-line in the thread performing the CRUD operation,
 * which is very undesirable since it increases operation latency.</p>
 *
 * <p>Critical eviction in the main cache is indicated by large values for
 * {@code NBytesEvictedCritical}, as compared to the other {@code
 * NBytesEvictedXXX} stats. Critical eviction in the off-heap cache is
 * indicated by large values for {@code OffHeapCriticalNodesTargeted} compared
 * to {@code OffHeapNodesTargeted}.</p>
 *
 * <p>Additional stats indicating that background eviction threads may be
 * insufficient are:</p>
 * <ul>
 *     <li>{@link #getNThreadUnavailable()}</li>
 *     <li>{@link #getOffHeapThreadUnavailable()}</li>
 * </ul>
 *
 * <p>Critical eviction can sometimes be reduced by changing {@link
 * EnvironmentConfig#EVICTOR_CRITICAL_PERCENTAGE} or modifying the eviction
 * thread pool parameters.</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_CRITICAL_PERCENTAGE}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_CORE_THREADS}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_MAX_THREADS}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_KEEP_ALIVE}</li>
 *     <li>{@link EnvironmentConfig#OFFHEAP_CORE_THREADS}</li>
 *     <li>{@link EnvironmentConfig#OFFHEAP_MAX_THREADS}</li>
 *     <li>{@link EnvironmentConfig#OFFHEAP_KEEP_ALIVE}</li>
 * </ul>
 *
 * <p>When using Oracle NoSQL DB, {@code EVICTOR_CRITICAL_PERCENTAGE} is set to
 * 20% rather than using the JE default of 0%.</p>
 *
 * <p>In the main cache, critical eviction uses the same parameter as
 * background eviction for determining how many bytes to evict at one
 * time:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_EVICT_BYTES}</li>
 * </ul>
 *
 * <p>Be careful when increasing this value, since this will cause longer
 * operation latencies when critical eviction is occurring in the main
 * cache.</p>
 *
 * <p>The corresponding parameter for the off-heap cache, {@code
 * OFFHEAP_EVICT_BYTES}, works differently:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#OFFHEAP_EVICT_BYTES}</li>
 * </ul>
 *
 * <p>Unlike in the main cache, {@code OFFHEAP_EVICT_BYTES} defines the goal
 * for background eviction to be below {@code MAX_OFF_HEAP_MEMORY}. The
 * background evictor threads for the off-heap cache attempt to maintain the
 * size of the off-heap cache at {@code MAX_OFF_HEAP_MEMORY -
 * OFFHEAP_EVICT_BYTES}. If the off-heap cache size grows larger than {@code
 * MAX_OFF_HEAP_MEMORY}, critical off-heap eviction will occur. The default
 * value for {@code OFFHEAP_EVICT_BYTES} is fairly large to ensure that
 * critical eviction does not occur. Be careful when lowering this value.</p>
 *
 * <p>This approach is intended to prevent the off-heap cache from exceeding
 * its maximum size. If the maximum is exceeded, there is a danger that the
 * JVM process will be killed by the OS. See {@link
 * #getOffHeapAllocFailures()}.</p>
 *
 * <h4><a name="cacheLRUListContention">Cache Statistics: LRU List
 * Contention</a></h4>
 *
 * <p>Another common tuning issue involves thread contention on the cache LRU
 * lists, although there is no stat to indicate such contention. Since each
 * time a node is accessed it must be moved to the end of the LRU list, a
 * single LRU list would cause contention among threads performing CRUD
 * operations. By default there are 4 LRU lists for each cache. If contention
 * is noticeable on internal Evictor.LRUList or OffHeapCache.LRUList methods,
 * consider increasing the number of LRU lists:</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#EVICTOR_N_LRU_LISTS}</li>
 *     <li>{@link EnvironmentConfig#OFFHEAP_N_LRU_LISTS}</li>
 * </ul>
 *
 * <p>However, note that increasing the number of LRU lists will decrease the
 * accuracy of the LRU.</p>
 *
 * <h4><a name="cacheDebugging">Cache Statistics: Debugging</a></h4>
 *
 * <p>The following cache stats are unlikely to be needed for monitoring or
 * tuning, but are sometimes useful for debugging and testing.</p>
 * <ul>
 *     <li>{@link #getDataBytes()}</li>
 *     <li>{@link #getAdminBytes()}</li>
 *     <li>{@link #getDataAdminBytes()}</li>
 *     <li>{@link #getNNodesTargeted()}</li>
 *     <li>{@link #getNNodesStripped()}</li>
 *     <li>{@link #getNNodesPutBack()}</li>
 *     <li>{@link #getNNodesMovedToDirtyLRU()}</li>
 *     <li>{@link #getNNodesSkipped()}</li>
 *     <li>{@link #getNRootNodesEvicted()}</li>
 *     <li>{@link #getNBINsFetchMissRatio()}</li>
 *     <li>{@link #getNINSparseTarget()}</li>
 *     <li>{@link #getNINNoTarget()}</li>
 *     <li>{@link #getMixedLRUSize()}</li>
 *     <li>{@link #getDirtyLRUSize()}</li>
 *     <li>{@link #getOffHeapAllocFailures()}</li>
 *     <li>{@link #getOffHeapAllocOverflows()}</li>
 *     <li>{@link #getOffHeapNodesStripped()}</li>
 *     <li>{@link #getOffHeapNodesSkipped()}</li>
 *     <li>{@link #getOffHeapLNsLoaded()}</li>
 *     <li>{@link #getOffHeapLNsStored()}</li>
 *     <li>{@link #getOffHeapBINsLoaded()}</li>
 *     <li>{@link #getOffHeapBINsStored()}</li>
 *     <li>{@link #getOffHeapTotalBlocks()}</li>
 *     <li>{@link #getOffHeapLRUSize()}</li>
 * </ul>
 *
 * <p>Likewise, the following cache configuration params are unlikely to be
 * needed for tuning, but are sometimes useful for debugging and testing.</p>
 * <ul>
 *     <li>{@link EnvironmentConfig#ENV_DB_EVICTION}</li>
 *     <li>{@link EnvironmentConfig#TREE_MIN_MEMORY}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_FORCED_YIELD}</li>
 *     <li>{@link EnvironmentConfig#EVICTOR_ALLOW_BIN_DELTAS}</li>
 *     <li>{@link EnvironmentConfig#OFFHEAP_CHECKSUM}</li>
 * </ul>
 *
 * <!--
 * For Cleaning section:
 * See this <a href="{@docRoot}/../GettingStartedGuide/logfilesrevealed.html">
 * overview</a> of the logging system. Log files are cleaned and deleted as
 * their contents become obsolete.
 *
 * For Env section:
 *     <li>{@link #getNBINDeltaBlindOps()}</li>
 * -->
 *
 * @see <a href="{@docRoot}/../jconsole/JConsole-plugin.html">Viewing
 * Statistics with JConsole</a>
 */
public class EnvironmentStats implements Serializable {

    /*
    find/replace:
    public static final StatDefinition\s+(\w+)\s*=\s*new StatDefinition\(\s*(".*"),\s*(".*")(,?\s*\w*\.?\w*)\);
    public static final String $1_NAME =\n        $2;\n    public static final String $1_DESC =\n        $3;\n    public static final StatDefinition $1 =\n        new StatDefinition(\n            $1_NAME,\n            $1_DESC$4);
    */

    private static final long serialVersionUID = 1734048134L;

    private StatGroup incompStats;
    private StatGroup cacheStats;
    private StatGroup offHeapStats;
    private StatGroup ckptStats;
    private StatGroup cleanerStats;
    private StatGroup logStats;
    private StatGroup lockStats;
    private StatGroup envImplStats;
    private StatGroup throughputStats;

    /**
     * @hidden
     * Internal use only.
     */
    public EnvironmentStats() {
        incompStats = new StatGroup(INCompStatDefinition.GROUP_NAME,
                                    INCompStatDefinition.GROUP_DESC);

        cacheStats = new StatGroup(EvictorStatDefinition.GROUP_NAME,
                                   EvictorStatDefinition.GROUP_DESC);
        offHeapStats = new StatGroup(OffHeapStatDefinition.GROUP_NAME,
                                     OffHeapStatDefinition.GROUP_DESC);
        ckptStats = new StatGroup(CheckpointStatDefinition.GROUP_NAME,
                                  CheckpointStatDefinition.GROUP_DESC);
        cleanerStats = new StatGroup(CleanerStatDefinition.GROUP_NAME,
                                     CleanerStatDefinition.GROUP_DESC);
        logStats = new StatGroup(LogStatDefinition.GROUP_NAME,
                                 LogStatDefinition.GROUP_DESC);
        lockStats = new StatGroup(LockStatDefinition.GROUP_NAME,
                                  LockStatDefinition.GROUP_DESC);
        envImplStats = new StatGroup(DbiStatDefinition.ENV_GROUP_NAME,
                                     DbiStatDefinition.ENV_GROUP_DESC);
        throughputStats =
            new StatGroup(DbiStatDefinition.THROUGHPUT_GROUP_NAME,
                          DbiStatDefinition.THROUGHPUT_GROUP_DESC);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public List<StatGroup> getStatGroups() {
        return Arrays.asList(
            logStats, cacheStats, offHeapStats, cleanerStats, incompStats,
            ckptStats, envImplStats, lockStats, throughputStats);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public Map<String, StatGroup> getStatGroupsMap() {
        final HashMap<String, StatGroup> map = new HashMap<>();
        for (StatGroup group : getStatGroups()) {
            map.put(group.getName(), group);
        }
        return map;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setStatGroup(StatGroup sg) {

        if (sg.getName().equals(INCompStatDefinition.GROUP_NAME)) {
            incompStats = sg;
        } else if (sg.getName().equals(EvictorStatDefinition.GROUP_NAME)) {
            cacheStats = sg;
        } else if (sg.getName().equals(OffHeapStatDefinition.GROUP_NAME)) {
            offHeapStats = sg;
        } else if (sg.getName().equals(CheckpointStatDefinition.GROUP_NAME)) {
            ckptStats = sg;
        } else if (sg.getName().equals(CleanerStatDefinition.GROUP_NAME)) {
            cleanerStats = sg;
        } else if (sg.getName().equals(LogStatDefinition.GROUP_NAME)) {
            logStats = sg;
        } else if (sg.getName().equals(LockStatDefinition.GROUP_NAME)) {
            lockStats = sg;
        } else if (sg.getName().equals(DbiStatDefinition.ENV_GROUP_NAME)) {
            envImplStats = sg;
        } else if (sg.getName().equals(
            DbiStatDefinition.THROUGHPUT_GROUP_NAME)) {
            throughputStats = sg;
        } else {
            throw EnvironmentFailureException.unexpectedState
            ("Invalid stat group name in setStatGroup " +
            sg.getName());
        }
    }

    /**
     * @hidden
     * Internal use only
     * For JConsole plugin support.
     */
    public static String[] getStatGroupTitles() {
        List<StatGroup> groups = new EnvironmentStats().getStatGroups();
        final String[] titles = new String[groups.size()];
        for (int i = 0; i < titles.length; i += 1) {
            titles[i] = groups.get(i).getName();
        }
        return titles;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setThroughputStats(StatGroup stats) {
        throughputStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setINCompStats(StatGroup stats) {
        incompStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setCkptStats(StatGroup stats) {
        ckptStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setCleanerStats(StatGroup stats) {
        cleanerStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setLogStats(StatGroup stats) {
        logStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setMBAndEvictorStats(StatGroup clonedMBStats,
                                     StatGroup clonedEvictorStats){
        cacheStats = clonedEvictorStats;
        cacheStats.addAll(clonedMBStats);
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setOffHeapStats(StatGroup stats) {
        offHeapStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setLockStats(StatGroup stats) {
        lockStats = stats;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setEnvStats(StatGroup stats) {
        envImplStats = stats;
    }

    /* INCompressor stats. */

    /**
     * The number of BINs encountered by the INCompressor that had cursors
     * referring to them when the compressor ran.
     */
    public long getCursorsBins() {
        return incompStats.getLong(INCOMP_CURSORS_BINS);
    }

    /**
     * The time the Environment was created.
     */
    public long getEnvironmentCreationTime() {
        return envImplStats.getLong(ENV_CREATION_TIME);
    }

    /**
     * The number of BINs encountered by the INCompressor that had their
     * database closed between the time they were put on the compressor queue
     * and when the compressor ran.
     */
    public long getDbClosedBins() {
        return incompStats.getLong(INCOMP_DBCLOSED_BINS);
    }

    /**
     * The number of entries in the INCompressor queue when the getStats()
     * call was made.
     */
    public long getInCompQueueSize() {
        return incompStats.getLong(INCOMP_QUEUE_SIZE);
    }

    /**
     * The number of BINs encountered by the INCompressor that were not
     * actually empty when the compressor ran.
     */
    public long getNonEmptyBins() {
        return incompStats.getLong(INCOMP_NON_EMPTY_BINS);
    }

    /**
     * The number of BINs that were successfully processed by the IN
     * Compressor.
     */
    public long getProcessedBins() {
        return incompStats.getLong(INCOMP_PROCESSED_BINS);
    }

    /**
     * The number of BINs encountered by the INCompressor that were split
     * between the time they were put on the compressor queue and when the
     * compressor ran.
     */
    public long getSplitBins() {
        return incompStats.getLong(INCOMP_SPLIT_BINS);
    }

    /* Checkpointer stats. */

    /**
     * The Id of the last checkpoint.
     */
    public long getLastCheckpointId() {
        return ckptStats.getLong(CKPT_LAST_CKPTID);
    }

    /**
     * The total number of checkpoints run so far.
     */
    public long getNCheckpoints() {
        return ckptStats.getLong(CKPT_CHECKPOINTS);
    }

    /**
     * The accumulated number of full INs flushed to the log.
     */
    public long getNFullINFlush() {
        return ckptStats.getLong(CKPT_FULL_IN_FLUSH);
    }

    /**
     * The accumulated number of full BINs flushed to the log.
     */
    public long getNFullBINFlush() {
        return ckptStats.getLong(CKPT_FULL_BIN_FLUSH);
    }

    /**
     * The accumulated number of Delta INs flushed to the log.
     */
    public long getNDeltaINFlush() {
        return ckptStats.getLong(CKPT_DELTA_IN_FLUSH);
    }

    /**
     * Byte length from last checkpoint start to the previous checkpoint start.
     */
    public long getLastCheckpointInterval() {
        return ckptStats.getLong(CKPT_LAST_CKPT_INTERVAL);
    }

    /**
     * The location in the log of the last checkpoint start.
     */
    public long getLastCheckpointStart() {
        return ckptStats.getLong(CKPT_LAST_CKPT_START);
    }

    /**
     * The location in the log of the last checkpoint end.
     */
    public long getLastCheckpointEnd() {
        return ckptStats.getLong(CKPT_LAST_CKPT_END);
    }

    /* Cleaner stats. */

    /**
     * @deprecated in 7.0, always returns zero. Use {@link
     * #getCurrentMinUtilization()} and {@link #getCurrentMaxUtilization()} to
     * monitor cleaner behavior.
     */
    public int getCleanerBacklog() {
        return 0;
    }

    /**
     * @deprecated in 7.5, always returns zero. Use {@link
     * #getProtectedLogSize()} {@link #getProtectedLogSizeMap()} to monitor
     * file protection.
     */
    public int getFileDeletionBacklog() {
        return 0;
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MIN_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MIN_UTILIZATION_NAME}</p>
     *
     * The last known log minimum utilization as a percentage.  This statistic
     * provides a cheap way of checking the log utilization without having to
     * run the DbSpace utility.
     * <p>
     * The log utilization is the percentage of the total log size (all .jdb
     * files) that is utilized or active.  The remaining portion of the log
     * is obsolete.  The log cleaner is responsible for keeping the log
     * utilization below the configured threshold,
     * {@link EnvironmentConfig#CLEANER_MIN_UTILIZATION}.
     * <p>
     * This statistic is computed every time the log cleaner examines the
     * utilization of the log, in order to determine whether cleaning is
     * needed.  The frequency can be configured using
     * {@link EnvironmentConfig#CLEANER_BYTES_INTERVAL}.
     * <p>
     * The obsolete portion of the log includes data that has expired at the
     * time the statistic was last computed. An expiration histogram is stored
     * for each file and used to compute the expired size. The minimum and
     * maximum utilization are the lower and upper bounds of computed
     * utilization, which may be different when some data has expired. See
     * {@link #getNCleanerTwoPassRuns()} for more information.
     * <p>
     * Note that the size of the utilized data in the log is always greater
     * than the amount of user data (total size of keys and data).  The active
     * Btree internal nodes and other metadata are also included.
     * <p>
     *
     * @return the current minimum utilization, or -1 if the utilization has
     * not been calculated for this environment since it was last opened.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 6.5
     */
    public int getCurrentMinUtilization() {
        return cleanerStats.getInt(CLEANER_MIN_UTILIZATION);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MAX_UTILIZATION_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MAX_UTILIZATION_NAME}</p>
     *
     * The last known log maximum utilization as a percentage.  This statistic
     * provides a cheap way of checking the log utilization without having to
     * run the DbSpace utility.
     * <p>
     * The log utilization is the percentage of the total log size (all .jdb
     * files) that is utilized or active.  The remaining portion of the log
     * is obsolete.  The log cleaner is responsible for keeping the log
     * utilization below the configured threshold,
     * {@link EnvironmentConfig#CLEANER_MIN_UTILIZATION}.
     * <p>
     * This statistic is computed every time the log cleaner examines the
     * utilization of the log, in order to determine whether cleaning is
     * needed.  The frequency can be configured using
     * {@link EnvironmentConfig#CLEANER_BYTES_INTERVAL}.
     * <p>
     * The obsolete portion of the log includes data that has expired at the
     * time the statistic was last computed. An expiration histogram is stored
     * for each file and used to compute the expired size. The minimum and
     * maximum utilization are the lower and upper bounds of computed
     * utilization, which may be different when some data has expired. See
     * {@link #getNCleanerTwoPassRuns()} for more information.
     * <p>
     * Note that the size of the utilized data in the log is always greater
     * than the amount of user data (total size of keys and data).  The active
     * Btree internal nodes and other metadata are also included.
     * <p>
     *
     * @return the current maximum utilization, or -1 if the utilization has
     * not been calculated for this environment since it was last opened.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 6.5
     */
    public int getCurrentMaxUtilization() {
        return cleanerStats.getInt(CLEANER_MAX_UTILIZATION);
    }

    /**
     * @deprecated in JE 6.5, use {@link #getCurrentMinUtilization()} or
     * {@link #getCurrentMaxUtilization()} instead.
     */
    public int getLastKnownUtilization() {
        return getCurrentMinUtilization();
    }

    /**
     * @deprecated in JE 6.3. Adjustments are no longer needed because LN log
     * sizes have been stored in the Btree since JE 6.0.
     */
    public float getLNSizeCorrectionFactor() {
        return 1;
    }

    /**
     * @deprecated in JE 5.0.56, use {@link #getCorrectedAvgLNSize} instead.
     */
    public float getCorrectedAvgLNSize() {
        return Float.NaN;
    }

    /**
     * @deprecated in JE 5.0.56, use {@link #getCorrectedAvgLNSize} instead.
     */
    public float getEstimatedAvgLNSize() {
        return Float.NaN;
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RUNS_NAME}</p>
     *
     * Total number of cleaner runs, including {@link #getNCleanerTwoPassRuns()
     * two-pass runs} but not including {@link #getNCleanerRevisalRuns()
     * revisal runs}. The {@link #getCurrentMinUtilization() minimum} and
     * {@link #getCurrentMaxUtilization() maximum} utilization values are used
     * to drive cleaning.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNCleanerRuns() {
        return cleanerStats.getLong(CLEANER_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TWO_PASS_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TWO_PASS_RUNS_NAME}</p>
     *
     * Number of cleaner two-pass runs, which are a subset of the
     * {@link #getNCleanerRuns() total cleaner runs}. The {@link
     * #getCurrentMinUtilization() minimum} and {@link
     * #getCurrentMaxUtilization() maximum} utilization values are used to
     * drive cleaning.
     * <p>
     * The obsolete portion of the log includes data that has expired. An
     * expiration histogram is stored for each file and used to compute the
     * expired size. The minimum and maximum utilization are the lower and
     * upper bounds of computed utilization. They are different only when the
     * TTL feature is used, and some data in the file has expired while other
     * data has become obsolete for other reasons, such as record updates,
     * record deletions or checkpoints. In this case the strictly obsolete size
     * and the expired size may overlap because they are maintained separately.
     * <p>
     * If they overlap completely then the minimum utilization is correct,
     * while if there is no overlap then the maximum utilization is correct.
     * Both utilization values trigger cleaning, but when there is significant
     * overlap, the cleaner will perform two-pass cleaning.
     * <p>
     * In the first pass of two-pass cleaning, the file is read to recompute
     * obsolete and expired sizes, but the file is not cleaned. As a result of
     * recomputing the expired sizes, the strictly obsolete and expired sizes
     * will no longer overlap, and the minimum and maximum utilization will be
     * equal. If the file should still be cleaned, based on the recomputed
     * utilization, it is cleaned as usual, and in this case the number of
     * two-pass runs (this statistic) is incremented.
     * <p>
     * If the file should not be cleaned because its recomputed utilization is
     * higher than expected, the file will not be cleaned. Instead, its
     * recomputed expiration histogram, which has size information that now
     * does not overlap with the strictly obsolete data, is stored for future
     * use. By storing the revised histogram, the cleaner can select the most
     * appropriate files for cleaning in the future. In this case the number of
     * {@link #getNCleanerRevisalRuns() revisal runs} is incremented, and the
     * number of {@link #getNCleanerRuns() total runs} is not incremented.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 6.5.0
     */
    public long getNCleanerTwoPassRuns() {
        return cleanerStats.getLong(CLEANER_TWO_PASS_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REVISAL_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REVISAL_RUNS_NAME}</p>
     *
     * Number of cleaner runs that ended in revising expiration info, but not
     * in any cleaning.
     *
     * @see #getNCleanerTwoPassRuns()
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 6.5.0
     */
    public long getNCleanerRevisalRuns() {
        return cleanerStats.getLong(CLEANER_REVISAL_RUNS);
    }

    /**
     * @deprecated in JE 6.3, always returns zero.
     */
    public long getNCleanerProbeRuns() {
        return 0;
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DELETIONS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DELETIONS_NAME}</p>
     *
     * The number of cleaner file deletions this session.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNCleanerDeletions() {
        return cleanerStats.getLong(CLEANER_DELETIONS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LN_QUEUE_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LN_QUEUE_SIZE_NAME}</p>
     *
     * The number of LNs pending because they were locked and could not be
     * migrated.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public int getPendingLNQueueSize() {
        return cleanerStats.getInt(CLEANER_PENDING_LN_QUEUE_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DISK_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_DISK_READS_NAME}</p>
     *
     * The number of disk reads performed by the cleaner.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNCleanerDiskRead() {
        return cleanerStats.getLong(CLEANER_DISK_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ENTRIES_READ_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ENTRIES_READ_NAME}</p>
     *
     * The accumulated number of log entries read by the cleaner.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNCleanerEntriesRead() {
        return cleanerStats.getLong(CLEANER_ENTRIES_READ);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_OBSOLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_OBSOLETE_NAME}</p>
     *
     * The accumulated number of INs obsolete.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNINsObsolete() {
        return cleanerStats.getLong(CLEANER_INS_OBSOLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_CLEANED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_CLEANED_NAME}</p>
     *
     * The accumulated number of INs cleaned.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNINsCleaned() {
        return cleanerStats.getLong(CLEANER_INS_CLEANED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_DEAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_DEAD_NAME}</p>
     *
     * The accumulated number of INs that were not found in the tree anymore
     * (deleted).
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNINsDead() {
        return cleanerStats.getLong(CLEANER_INS_DEAD);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_MIGRATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_INS_MIGRATED_NAME}</p>
     *
     * The accumulated number of INs migrated.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNINsMigrated() {
        return cleanerStats.getLong(CLEANER_INS_MIGRATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_OBSOLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_OBSOLETE_NAME}</p>
     *
     * The accumulated number of BIN-deltas obsolete.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNBINDeltasObsolete() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_OBSOLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_CLEANED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_CLEANED_NAME}</p>
     *
     * The accumulated number of BIN-deltas cleaned.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNBINDeltasCleaned() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_CLEANED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_DEAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_DEAD_NAME}</p>
     *
     * The accumulated number of BIN-deltas that were not found in the tree
     * anymore (deleted).
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNBINDeltasDead() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_DEAD);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_MIGRATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_BIN_DELTAS_MIGRATED_NAME}</p>
     *
     * The accumulated number of BIN-deltas migrated.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNBINDeltasMigrated() {
        return cleanerStats.getLong(CLEANER_BIN_DELTAS_MIGRATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_OBSOLETE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_OBSOLETE_NAME}</p>
     *
     * The accumulated number of LNs obsolete.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsObsolete() {
        return cleanerStats.getLong(CLEANER_LNS_OBSOLETE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXPIRED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_EXPIRED_NAME}</p>
     *
     * The accumulated number of obsolete LNs that were expired. Note that
     * this does not included embedded LNs (those having a data size less than
     * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN}), because embedded LNs
     * are always considered obsolete.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsExpired() {
        return cleanerStats.getLong(CLEANER_LNS_EXPIRED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_CLEANED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_CLEANED_NAME}</p>
     *
     * The accumulated number of LNs cleaned.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsCleaned() {
        return cleanerStats.getLong(CLEANER_LNS_CLEANED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_DEAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_DEAD_NAME}</p>
     *
     * The accumulated number of LNs that were not found in the tree anymore
     * (deleted).
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsDead() {
        return cleanerStats.getLong(CLEANER_LNS_DEAD);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_LOCKED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_LOCKED_NAME}</p>
     *
     * The accumulated number of LNs encountered that were locked.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsLocked() {
        return cleanerStats.getLong(CLEANER_LNS_LOCKED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MIGRATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MIGRATED_NAME}</p>
     *
     * The accumulated number of LNs encountered that were migrated forward in
     * the log by the cleaner.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsMigrated() {
        return cleanerStats.getLong(CLEANER_LNS_MIGRATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MARKED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNS_MARKED_NAME}</p>
     *
     * The accumulated number of LNs in temporary DBs that were dirtied by the
     * cleaner and subsequently logging during checkpoint/eviction.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNsMarked() {
        return cleanerStats.getLong(CLEANER_LNS_MARKED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNQUEUE_HITS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_LNQUEUE_HITS_NAME}</p>
     *
     * The accumulated number of LNs processed without a tree lookup.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNLNQueueHits() {
        return cleanerStats.getLong(CLEANER_LNQUEUE_HITS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_PROCESSED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_PROCESSED_NAME}</p>
     *
     * The accumulated number of LNs processed because they were previously
     * locked.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNPendingLNsProcessed() {
        return cleanerStats.getLong(CLEANER_PENDING_LNS_PROCESSED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MARKED_LNS_PROCESSED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_MARKED_LNS_PROCESSED_NAME}</p>
     *
     * The accumulated number of LNs processed because they were previously
     * marked for migration.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNMarkedLNsProcessed() {
        return cleanerStats.getLong(CLEANER_MARKED_LNS_PROCESSED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TO_BE_CLEANED_LNS_PROCESSED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TO_BE_CLEANED_LNS_PROCESSED_NAME}</p>
     *
     * The accumulated number of LNs processed because they are soon to be
     * cleaned.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNToBeCleanedLNsProcessed() {
        return cleanerStats.getLong(CLEANER_TO_BE_CLEANED_LNS_PROCESSED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_CLUSTER_LNS_PROCESSED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_CLUSTER_LNS_PROCESSED_NAME}</p>
     *
     * The accumulated number of LNs processed because they qualify for
     * clustering.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNClusterLNsProcessed() {
        return cleanerStats.getLong(CLEANER_CLUSTER_LNS_PROCESSED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_LOCKED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PENDING_LNS_LOCKED_NAME}</p>
     *
     * The accumulated number of pending LNs that could not be locked for
     * migration because of a long duration application lock.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNPendingLNsLocked() {
        return cleanerStats.getLong(CLEANER_PENDING_LNS_LOCKED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REPEAT_ITERATOR_READS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_REPEAT_ITERATOR_READS_NAME}</p>
     *
     * The number of times we tried to read a log entry larger than the read
     * buffer size and couldn't grow the log buffer to accommodate the large
     * object. This happens during scans of the log during activities like
     * environment open or log cleaning. Implies that the read chunk size
     * controlled by je.log.iteratorReadSize is too small.
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getNRepeatIteratorReads() {
        return cleanerStats.getLong(CLEANER_REPEAT_ITERATOR_READS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ACTIVE_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_ACTIVE_LOG_SIZE_NAME}</p>
     *
     * <p>The {@link #getCurrentMinUtilization() log utilization} is the
     * percentage of activeLogSize that is currently referenced or active.</p>
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 7.5
     */
    public long getActiveLogSize() {
        return cleanerStats.getLong(CLEANER_ACTIVE_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RESERVED_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_RESERVED_LOG_SIZE_NAME}</p>
     *
     * <p>Deletion of reserved files may be postponed for several reasons.
     * This occurs if an active file is protected (by a backup, for example),
     * and then the file is cleaned and becomes a reserved file. See
     * {@link #getProtectedLogSizeMap()} for more information. In a
     * standalone JE environment, reserved files are normally deleted very
     * soon after being cleaned.</p>
     *
     * <p>In an HA environment, reserved files are retained because they might
     * be used for replication to electable nodes that have been offline
     * for the {@link com.sleepycat.je.rep.ReplicationConfig#FEEDER_TIMEOUT}
     * interval or longer, or to offline secondary nodes. The replication
     * stream position of these nodes is unknown, so whether these files could
     * be used to avoid a network restore, when bringing these nodes online,
     * is also unknown. The files are retained just in case they can be used
     * for such replication. Files are reserved for replication on both master
     * and replicas, since a replica may become a master at a future time.
     * Such files will be deleted (oldest file first) to make room for a
     * write operation, if the write operation would have caused a disk limit
     * to be violated.</p>
     *
     * <p>In NoSQL DB, this retention of reserved files has the additional
     * benefit of supplying the replication stream to subscribers of the
     * Stream API, when such subscribers need to replay the stream from an
     * earlier point in time.</p>
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 7.5
     */
    public long getReservedLogSize() {
        return cleanerStats.getLong(CLEANER_RESERVED_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_NAME}</p>
     *
     * <p>Reserved files are protected for reasons described by {@link
     * #getProtectedLogSizeMap()}.</p>
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 7.5
     */
    public long getProtectedLogSize() {
        return cleanerStats.getLong(CLEANER_PROTECTED_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_MAP_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_PROTECTED_LOG_SIZE_MAP_NAME}</p>
     *
     * <p>{@link #getReservedLogSize() Reserved} data files are temporarily
     * {@link #getProtectedLogSize() protected} for a number of reasons. The
     * keys in the protected log size map are the names of the protecting
     * entities, and the values are the number of bytes protected by each
     * entity. The type and format of the entity names are as follows:</p>
     *
     * <pre>
     *    Backup-N
     *    DatabaseCount-N
     *    DiskOrderedCursor-N
     *    Syncup-N
     *    Feeder-N
     *    NetworkRestore-N
     * </pre>
     *
     * <p>Where:</p>
     * <ul>
     *     <li>
     *         {@code Backup-N} represents a {@link DbBackup} in progress,
     *         i.e., for which {@link DbBackup#startBackup()} has been called
     *         and {@link DbBackup#endBackup()} has not yet been called. All
     *         active files are initially protected by the backup, but these
     *         are not reserved files ond only appear in the map if they are
     *         cleaned and become reserved after the backup starts. Files
     *         are not protected if they have been copied and
     *         {@link DbBackup#removeFileProtection(String)} has been called.
     *         {@code N} is a sequentially assigned integer.
     *     </li>
     *     <li>
     *         {@code DatabaseCount-N} represents an outstanding call to
     *         {@link Database#count()}.
     *         All active files are initially protected by this method, but
     *         these are not reserved files ond only appear in the map if
     *         they are cleaned and become reserved during the execution of
     *         {@code Database.count}.
     *         {@code N} is a sequentially assigned integer.
     *     </li>
     *     <li>
     *         {@code DiskOrderedCursor-N} represents a
     *         {@link DiskOrderedCursor} that has not yet been closed by
     *         {@link DiskOrderedCursor#close()}.
     *         All active files are initially protected when the cursor is
     *         opened, but these are not reserved files ond only appear in
     *         the map if they are cleaned and become reserved while the
     *         cursor is open.
     *         {@code N} is a sequentially assigned integer.
     *     </li>
     *     <li>
     *         {@code Syncup-N} represents an in-progress negotiation between
     *         a master and replica node in an HA replication group to
     *         establish a replication stream. This is a normally a very short
     *         negotiation and occurs when a replica joins the group or after
     *         an election is held. During syncup, all reserved files are
     *         protected.
     *         {@code N} is the node name of the other node involved in the
     *         syncup, i.e, if this node is a master then it is the name of
     *         the replica, and vice versa.
     *     </li>
     *     <li>
     *         {@code Feeder-N} represents an HA master node that is supplying
     *         the replication stream to a replica. Normally data in active
     *         files is being supplied and this data is not in the reserved
     *         or protected categories. But if the replica is lagging, data
     *         from reserved files may be supplied, and in that case will be
     *         protected and appear in the map.
     *         {@code N} is the node name of the replica receiving the
     *         replication stream.
     *     </li>
     *     <li>
     *         {@code NetworkRestore-N} represents an HA replica or master
     *         node that is supplying files to a node that is performing a
     *         {@link com.sleepycat.je.rep.NetworkRestore}. The files supplied
     *         are all active files plus the two most recently written
     *         reserved files. The two reserved files will appear in the map,
     *         as well as any of the active files that were cleaned and became
     *         reserved during the network restore. Files that have already
     *         been copied by the network restore are not protected.
     *         {@code N} is the name of the node performing the
     *         {@link com.sleepycat.je.rep.NetworkRestore}.
     *     </li>
     * </ul>
     *
     * <p>When more than one entity is included in the map, in general the
     * largest value points to the entity primarily responsible for
     * preventing reclamation of disk space. Note that the values normally
     * sum to more than {@link #getProtectedLogSize()}, since protection often
     * overlaps.</p>
     *
     * <p>The string format of this stat consists of {@code name=size} pairs
     * separated by semicolons, where name is the entity name described
     * above and size is the number of protected bytes.</p>
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @since 7.5
     */
    public SortedMap<String, Long> getProtectedLogSizeMap() {
        return cleanerStats.getMap(CLEANER_PROTECTED_LOG_SIZE_MAP);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_AVAILABLE_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_AVAILABLE_LOG_SIZE_NAME}</p>
     *
     * <p>This is the amount that can be logged by write operations, and
     * other JE activity such as checkpointing, without violating a disk
     * limit. The files making up {@code reservedLogSize} can be deleted to
     * make room for these write operations, so {@code availableLogSize} is
     * the sum of the current disk free space and the reserved size that is not
     * protected ({@code reservedLogSize} - {@code protectedLogSize}). The
     * current disk free space is calculated using the disk volume's free
     * space, {@link EnvironmentConfig#MAX_DISK} and {@link
     * EnvironmentConfig#FREE_DISK}.</p>
     *
     * <p>Note that when a record is written, the number of bytes includes JE
     * overheads for the record. Also, this causes Btree metadata to be
     * written during checkpoints, and other metadata is also written by JE.
     * So the space occupied on disk by a given set of records cannot be
     * calculated by simply summing the key/data sizes.</p>
     *
     * <p>Also note that {@code availableLogSize} will be negative when a disk
     * limit has been violated, representing the amount that needs to be freed
     * before write operations are allowed.</p>
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     * @see EnvironmentConfig#MAX_DISK
     * @see EnvironmentConfig#FREE_DISK
     * @since 7.5
     */
    public long getAvailableLogSize() {
        return cleanerStats.getLong(CLEANER_AVAILABLE_LOG_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TOTAL_LOG_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.cleaner.CleanerStatDefinition#CLEANER_TOTAL_LOG_SIZE_NAME}</p>
     *
     * @see <a href="#cleaner">Cleaner Statistics</a>
     */
    public long getTotalLogSize() {
        return cleanerStats.getLong(CLEANER_TOTAL_LOG_SIZE);
    }

    /* LogManager stats. */

    /**
     * The total number of requests for database objects which were not in
     * memory.
     */
    public long getNCacheMiss() {
        return logStats.getAtomicLong(LBFP_MISS);
    }

    /**
     * The location of the next entry to be written to the log.
     *
     * <p>Note that the log entries prior to this position may not yet have
     * been flushed to disk.  Flushing can be forced using a Sync or
     * WriteNoSync commit, or a checkpoint.</p>
     */
    public long getEndOfLog() {
        return logStats.getLong(LOGMGR_END_OF_LOG);
    }

    /**
     * The number of fsyncs issued through the group commit manager. A subset
     * of nLogFsyncs.
     */
    public long getNFSyncs() {
        return logStats.getAtomicLong(FSYNCMGR_FSYNCS);
    }

    /**
     * The number of fsyncs requested through the group commit manager.
     */
    public long getNFSyncRequests() {
        return logStats.getLong(FSYNCMGR_FSYNC_REQUESTS);
    }

    /**
     * The number of fsync requests submitted to the group commit manager which
     * timed out.
     */
    public long getNFSyncTimeouts() {
        return logStats.getLong(FSYNCMGR_TIMEOUTS);
    }

    /**
     * The total number of milliseconds used to perform fsyncs.
     *
     * @since 7.0, although the stat was output by {@link #toString} and
     * appeared in the je.stat.csv file in earlier versions.
     */
    public long getFSyncTime() {
        return logStats.getLong(GRPCMGR_FSYNC_TIME);
    }

    /**
     * The maximum number of milliseconds used to perform a single fsync.
     *
     * @since 7.0
     */
    public long getFSyncMaxTime() {
        return logStats.getLong(GRPCMGR_FSYNC_MAX_TIME);
    }

    /**
     * The total number of fsyncs of the JE log. This includes those fsyncs
     * issued on behalf of transaction commits.
     */
    public long getNLogFSyncs() {
        return logStats.getLong(FILEMGR_LOG_FSYNCS);
    }

    /**
     * The number of log buffers currently instantiated.
     */
    public int getNLogBuffers() {
        return logStats.getInt(LBFP_LOG_BUFFERS);
    }

    /**
     * The number of disk reads which required repositioning the disk head
     * more than 1MB from the previous file position.  Reads in a different
     * *.jdb log file then the last IO constitute a random read.
     * <p>
     * This number is approximate and may differ from the actual number of
     * random disk reads depending on the type of disks and file system, disk
     * geometry, and file system cache size.
     */
    public long getNRandomReads() {
        return logStats.getLong(FILEMGR_RANDOM_READS);
    }

    /**
     * The number of bytes read which required repositioning the disk head
     * more than 1MB from the previous file position.  Reads in a different
     * *.jdb log file then the last IO constitute a random read.
     * <p>
     * This number is approximate vary depending on the type of disks and file
     * system, disk geometry, and file system cache size.
     */
    public long getNRandomReadBytes() {
        return logStats.getLong(FILEMGR_RANDOM_READ_BYTES);
    }

    /**
     * The number of disk writes which required repositioning the disk head by
     * more than 1MB from the previous file position.  Writes to a different
     * *.jdb log file (i.e. a file "flip") then the last IO constitute a random
     * write.
     * <p>
     * This number is approximate and may differ from the actual number of
     * random disk writes depending on the type of disks and file system, disk
     * geometry, and file system cache size.
     */
    public long getNRandomWrites() {
        return logStats.getLong(FILEMGR_RANDOM_WRITES);
    }

    /**
     * The number of bytes written which required repositioning the disk head
     * more than 1MB from the previous file position.  Writes in a different
     * *.jdb log file then the last IO constitute a random write.
     * <p>
     * This number is approximate vary depending on the type of disks and file
     * system, disk geometry, and file system cache size.
     */
    public long getNRandomWriteBytes() {
        return logStats.getLong(FILEMGR_RANDOM_WRITE_BYTES);
    }

    /**
     * The number of disk reads which did not require repositioning the disk
     * head more than 1MB from the previous file position.  Reads in a
     * different *.jdb log file then the last IO constitute a random read.
     * <p>
     * This number is approximate and may differ from the actual number of
     * sequential disk reads depending on the type of disks and file system,
     * disk geometry, and file system cache size.
     */
    public long getNSequentialReads() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_READS);
    }

    /**
     * The number of bytes read which did not require repositioning the disk
     * head more than 1MB from the previous file position.  Reads in a
     * different *.jdb log file then the last IO constitute a random read.
     * <p>
     * This number is approximate vary depending on the type of disks and file
     * system, disk geometry, and file system cache size.
     */
    public long getNSequentialReadBytes() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_READ_BYTES);
    }

    /**
     * The number of disk writes which did not require repositioning the disk
     * head by more than 1MB from the previous file position.  Writes to a
     * different *.jdb log file (i.e. a file "flip") then the last IO
     * constitute a random write.
     * <p>
     * This number is approximate and may differ from the actual number of
     * sequential disk writes depending on the type of disks and file system,
     * disk geometry, and file system cache size.
     */
    public long getNSequentialWrites() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_WRITES);
    }

    /**
     * The number of bytes written which did not require repositioning the
     * disk head more than 1MB from the previous file position.  Writes in a
     * different *.jdb log file then the last IO constitute a random write.
     * <p>
     * This number is approximate vary depending on the type of disks and file
     * system, disk geometry, and file system cache size.
     */
    public long getNSequentialWriteBytes() {
        return logStats.getLong(FILEMGR_SEQUENTIAL_WRITE_BYTES);
    }

    /**
     * The number of bytes read to fulfill file read operations by reading out
     * of the pending write queue.
     */
    public long getNBytesReadFromWriteQueue() {
        return logStats.getLong(FILEMGR_BYTES_READ_FROM_WRITEQUEUE);
    }

    /**
     * The number of bytes written from the pending write queue.
     */
    public long getNBytesWrittenFromWriteQueue() {
        return logStats.getLong(FILEMGR_BYTES_WRITTEN_FROM_WRITEQUEUE);
    }

    /**
     * The number of file read operations which were fulfilled by reading out
     * of the pending write queue.
     */
    public long getNReadsFromWriteQueue() {
        return logStats.getLong(FILEMGR_READS_FROM_WRITEQUEUE);
    }

    /**
     * The number of file writes operations executed from the pending write
     * queue.
     */
    public long getNWritesFromWriteQueue() {
        return logStats.getLong(FILEMGR_WRITES_FROM_WRITEQUEUE);
    }

    /**
     * The number of writes operations which would overflow the Write Queue.
     */
    public long getNWriteQueueOverflow() {
        return logStats.getLong(FILEMGR_WRITEQUEUE_OVERFLOW);
    }

    /**
     * The number of writes operations which would overflow the Write Queue
     * and could not be queued.
     */
    public long getNWriteQueueOverflowFailures() {
        return logStats.getLong(FILEMGR_WRITEQUEUE_OVERFLOW_FAILURES);
    }

    /**
     * The total memory currently consumed by log buffers, in bytes.  If this
     * environment uses the shared cache, this method returns only the amount
     * used by this environment.
     */
    public long getBufferBytes() {
        return logStats.getLong(LBFP_BUFFER_BYTES);
    }

    /**
     * The number of requests for database objects not contained within the
     * in memory data structures.
     */
    public long getNNotResident() {
        return logStats.getAtomicLong(LBFP_NOT_RESIDENT);
    }

    /**
     * The number of reads which had to be repeated when faulting in an object
     * from disk because the read chunk size controlled by je.log.faultReadSize
     * is too small.
     */
    public long getNRepeatFaultReads() {
        return logStats.getLong(LOGMGR_REPEAT_FAULT_READS);
    }

    /**
     * The number of writes which had to be completed using the temporary
     * marshalling buffer because the fixed size log buffers specified by
     * je.log.totalBufferBytes and je.log.numBuffers were not large enough.
     */
    public long getNTempBufferWrites() {
        return logStats.getLong(LOGMGR_TEMP_BUFFER_WRITES);
    }

    /**
     * The number of times a log file has been opened.
     */
    public int getNFileOpens() {
        return logStats.getInt(FILEMGR_FILE_OPENS);
    }

    /**
     * The number of files currently open in the file cache.
     */
    public int getNOpenFiles() {
        return logStats.getInt(FILEMGR_OPEN_FILES);
    }

    /* Return Evictor stats. */

    /**
     * @deprecated The method returns 0 always.
     */
    public long getRequiredEvictBytes() {
        return 0;
    }

    /**
     * @deprecated This statistic has no meaning after the implementation
     * of the new evictor in JE 6.0. The method returns 0 always.
     */
    public long getNNodesScanned() {
        return 0;
    }

    /**
     * @deprecated Use {@link #getNEvictionRuns()} instead.
     */
    public long getNEvictPasses() {
        return cacheStats.getLong(EVICTOR_EVICTION_RUNS);
    }

    /**
     * @deprecated use {@link #getNNodesTargeted()} instead.
     */
    public long getNNodesSelected() {
        return cacheStats.getLong(EVICTOR_NODES_TARGETED);
    }

    /**
     * @deprecated Use {@link #getNNodesEvicted()} instead.
     */
    public long getNNodesExplicitlyEvicted() {
        return cacheStats.getLong(EVICTOR_NODES_EVICTED);
    }

    /**
     * @deprecated Use {@link #getNNodesStripped()} instead.
     */
    public long getNBINsStripped() {
        return cacheStats.getLong(EVICTOR_NODES_STRIPPED);
    }

    /**
     * @deprecated Use {@link #getNNodesMutated()} instead.
     */
    public long getNBINsMutated() {
        return cacheStats.getLong(EVICTOR_NODES_MUTATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_EVICTION_RUNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_EVICTION_RUNS_NAME}</p>
     *
     * <p>When an evictor thread is awoken it performs eviction until
     * {@link #getCacheTotalBytes()} is at least
     * {@link EnvironmentConfig#EVICTOR_EVICT_BYTES} less than the
     * {@link EnvironmentConfig#MAX_MEMORY_PERCENT total cache size}.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cache">Cache Statistics</a>
     */
    public long getNEvictionRuns() {
        return cacheStats.getLong(EVICTOR_EVICTION_RUNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_TARGETED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_TARGETED_NAME}</p>
     *
     * <p>An eviction target may actually be evicted, or skipped, or put back
     * to the LRU, potentially after partial eviction (stripping) or
     * BIN-delta mutation is done on it.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesTargeted() {
        return cacheStats.getLong(EVICTOR_NODES_TARGETED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_EVICTED_NAME}</p>
     *
     * <p>Does not include {@link #getNLNsEvicted() LN eviction} or
     * {@link #getNNodesMutated() BIN-delta mutation}.
     * Includes eviction of {@link #getNDirtyNodesEvicted() dirty nodes} and
     * {@link #getNRootNodesEvicted() root nodes}.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNNodesEvicted() {
        return cacheStats.getLong(EVICTOR_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_ROOT_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_ROOT_NODES_EVICTED_NAME}</p>
     *
     * <p>The root node of a Database is only evicted after all other nodes in
     * the Database, so this implies that the entire Database has fallen out of
     * cache and is probably closed.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNRootNodesEvicted() {
        return cacheStats.getLong(EVICTOR_ROOT_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_DIRTY_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_DIRTY_NODES_EVICTED_NAME}</p>
     *
     * <p>When a dirty IN is evicted from main cache and no off-heap cache is
     * configured, the IN must be logged. When an off-heap cache is configured,
     * dirty INs can be moved from main cache to off-heap cache based on LRU,
     * but INs are only logged when they are evicted from off-heap cache.
     * Therefore, this stat is always zero when an off-heap cache is configured.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNDirtyNodesEvicted() {
        return cacheStats.getLong(EVICTOR_DIRTY_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_LNS_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_LNS_EVICTED_NAME}</p>
     *
     * <p>When a BIN is considered for eviction based on LRU, if the BIN
     * contains resident LNs in main cache, it is stripped of the LNs rather
     * than being evicted. This stat reflects LNs evicted in this manner, but
     * not LNs evicted as a result of using {@link CacheMode#EVICT_LN}. Also
     * note that {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded} LNs
     * are evicted immediately and are not reflected in this stat value.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNLNsEvicted() {
        return cacheStats.getLong(EVICTOR_LNS_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_STRIPPED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_STRIPPED_NAME}</p>
     *
     * <p>BINs are stripped in order to {@link #getNLNsEvicted() evict LNs}.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesStripped() {
        return cacheStats.getLong(EVICTOR_NODES_STRIPPED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MUTATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MUTATED_NAME}</p>
     *
     * <p>When a BIN is considered for eviction based on LRU, if the BIN
     * can be mutated to a BIN-delta, it is mutated rather than being evicted.
     * Note that when an off-heap cache is configured, this stat value will be
     * zero because BIN mutation will take place only in the off-heap cache;
     * see {@link #getOffHeapNodesMutated()}.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getNNodesMutated() {
        return cacheStats.getLong(EVICTOR_NODES_MUTATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_PUT_BACK_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_PUT_BACK_NAME}</p>
     *
     * <p>Reasons for putting back a target IN are:</p>
     * <ul>
     *     <li>The IN was accessed by an operation while the evictor was
     *     processing it.</li>
     *     <li>To prevent the cache usage for Btree objects from falling below
     *     {@link EnvironmentConfig#TREE_MIN_MEMORY}.</li>
     * </ul>
     *
     * <p>See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesPutBack() {
        return cacheStats.getLong(EVICTOR_NODES_PUT_BACK);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MOVED_TO_PRI2_LRU_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_MOVED_TO_PRI2_LRU_NAME}</p>
     *
     * <p>When an off-cache is not configured, dirty nodes are evicted last
     * from the main cache by moving them to a 2nd priority LRU list. When an
     * off-cache is configured, level-2 INs that reference off-heap BINs are
     * evicted last from the main cache, using the same approach.
     * See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesMovedToDirtyLRU() {
        return cacheStats.getLong(EVICTOR_NODES_MOVED_TO_PRI2_LRU);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_SKIPPED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_NODES_SKIPPED_NAME}</p>
     *
     * <p>Reasons for skipping a target IN are:</p>
     * <ul>
     *     <li>It has already been evicted by another thread.</li>
     *     <li>It cannot be evicted because concurrent activity added resident
     *     child nodes.</li>
     *     <li>It cannot be evicted because it is dirty and the environment is
     *     read-only.</li>
     * </ul>
     * <p>See {@link CacheMode} for a description of eviction.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNNodesSkipped() {
        return cacheStats.getLong(EVICTOR_NODES_SKIPPED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#THREAD_UNAVAILABLE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#THREAD_UNAVAILABLE_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNThreadUnavailable() {
        return cacheStats.getAtomicLong(THREAD_UNAVAILABLE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_SHARED_CACHE_ENVS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#EVICTOR_SHARED_CACHE_ENVS_NAME}</p>
     *
     * <p>This method says nothing about whether this environment is using
     * the shared cache or not.</p>
     *
     */
    public int getNSharedCacheEnvironments() {
        return cacheStats.getInt(EVICTOR_SHARED_CACHE_ENVS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_NAME}</p>
     *
     * <p>Note that the number of LN fetches does not necessarily correspond
     * to the number of records accessed, since some LNs may be
     * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNLNsFetch() {
        return cacheStats.getAtomicLong(LN_FETCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_NAME}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNBINsFetch() {
        return cacheStats.getAtomicLong(BIN_FETCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_NAME}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNUpperINsFetch() {
        return cacheStats.getAtomicLong(UPPER_IN_FETCH);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#LN_FETCH_MISS_NAME}</p>
     *
     * <p>Note that the number of LN fetches does not necessarily correspond
     * to the number of records accessed, since some LNs may be
     * {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN embedded}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNLNsFetchMiss() {
        return cacheStats.getAtomicLong(LN_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_NAME}</p>
     *
     * <p>This is the portion of {@link #getNBINsFetch()} that resulted in a
     * fetch miss. The fetch may be for a full BIN or BIN-delta
     * ({@link #getNBINDeltasFetchMiss()}), depending on whether a BIN-delta
     * currently exists (see {@link EnvironmentConfig#TREE_BIN_DELTA}).
     * However, additional full BIN fetches occur when mutating a BIN-delta to
     * a full BIN ({@link #getNFullBINsMiss()}) whenever this is necessary for
     * completing an operation.</p>
     *
     * <p>Therefore, the total number of BIN fetch misses
     * (including BIN-deltas) is:</p>
     *
     * <p style="margin-left: 2em">{@code nFullBINsMiss + nBINsFetchMiss}</p>
     *
     * <p>And the total number of full BIN (vs BIN-delta) fetch misses is:</p>
     *
     * <p style="margin-left: 2em">{@code nFullBINsMiss + nBINsFetchMiss -
     * nBINDeltasFetchMiss}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNBINsFetchMiss() {
        return cacheStats.getAtomicLong(BIN_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_FETCH_MISS_NAME}</p>
     *
     * <p>This represents the portion of {@code nBINsFetchMiss()} that fetched
     * BIN-deltas rather than full BINs. See {@link #getNBINsFetchMiss()}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNBINDeltasFetchMiss() {
        return cacheStats.getAtomicLong(BIN_DELTA_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#FULL_BIN_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#FULL_BIN_MISS_NAME}</p>
     *
     * <p>Note that this stat does not include full BIN misses that are
     * <i>not</i> due to BIN-delta mutations. See
     * {@link #getNBINsFetchMiss()}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNFullBINsMiss() {
        return cacheStats.getAtomicLong(FULL_BIN_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_MISS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#UPPER_IN_FETCH_MISS_NAME}</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNUpperINsFetchMiss() {
        return cacheStats.getAtomicLong(UPPER_IN_FETCH_MISS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_RATIO_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_FETCH_MISS_RATIO_NAME}</p>
     *
     * <p>This stat can be misleading because it does not include the number
     * of full BIN fetch misses resulting from BIN-delta mutations ({@link
     * #getNFullBINsMiss()}. It may be improved, or perhaps deprecated, in a
     * future release.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public float getNBINsFetchMissRatio() {
        return cacheStats.getFloat(BIN_FETCH_MISS_RATIO);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_BLIND_OPS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#BIN_DELTA_BLIND_OPS_NAME}</p>
     *
     * <p>Note that this stat is misplaced. It should be in the
     * {@value com.sleepycat.je.dbi.DbiStatDefinition#ENV_GROUP_NAME} group
     * and will probably be moved there in a future release.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     * @see EnvironmentConfig#TREE_BIN_DELTA
     */
    public long getNBINDeltaBlindOps() {
        return cacheStats.getAtomicLong(BIN_DELTA_BLIND_OPS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_UPPER_INS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_UPPER_INS_NAME}</p>
     *
     * <p>When used on shared environment caches, zero is returned when {@link
     * StatsConfig#setFast fast stats} are requested.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNCachedUpperINs() {
        return cacheStats.getLong(CACHED_UPPER_INS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BINS_NAME}</p>
     *
     * <p>When used on shared environment caches, zero is returned when {@link
     * StatsConfig#setFast fast stats} are requested.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNCachedBINs() {
        return cacheStats.getLong(CACHED_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BIN_DELTAS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_BIN_DELTAS_NAME}</p>
     *
     * <p>When used on shared environment caches, zero is returned when {@link
     * StatsConfig#setFast fast stats} are requested.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getNCachedBINDeltas() {
        return cacheStats.getLong(CACHED_BIN_DELTAS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_SPARSE_TARGET_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_SPARSE_TARGET_NAME}</p>
     *
     * <p>Each IN contains an array of references to child INs or LNs. When
     * there are between one and four children resident, the size of the array
     * is reduced to four. This saves a significant amount of cache memory for
     * BINs when {@link CacheMode#EVICT_LN} is used, because there are
     * typically only a small number of LNs resident in main cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNINSparseTarget() {
        return cacheStats.getLong(CACHED_IN_SPARSE_TARGET);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_NO_TARGET_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_NO_TARGET_NAME}</p>
     *
     * <p>Each IN contains an array of references to child INs or LNs. When
     * there are no children resident, no array is allocated. This saves a
     * significant amount of cache memory for BINs when {@link
     * CacheMode#EVICT_LN} is used, because there are typically only a small
     * number of LNs resident in main cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getNINNoTarget() {
        return cacheStats.getLong(CACHED_IN_NO_TARGET);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_COMPACT_KEY_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#CACHED_IN_COMPACT_KEY_NAME}</p>
     *
     * @see <a href="#cacheSizeOptimizations">Cache Statistics: Size
     * Optimizations</a>
     *
     * @see EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH
     */
    public long getNINCompactKeyIN() {
        return cacheStats.getLong(CACHED_IN_COMPACT_KEY);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI2_LRU_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI2_LRU_SIZE_NAME}</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     * @see #getNNodesMovedToDirtyLRU()
     */
    public long getDirtyLRUSize() {
        return cacheStats.getLong(PRI2_LRU_SIZE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI1_LRU_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#PRI1_LRU_SIZE_NAME}</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     * @see #getNNodesMovedToDirtyLRU()
     */
    public long getMixedLRUSize() {
        return cacheStats.getLong(PRI1_LRU_SIZE);
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBINsEvictedEvictorThread() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBINsEvictedManual() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBINsEvictedCritical() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBINsEvictedCacheMode() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBINsEvictedDaemon() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNUpperINsEvictedEvictorThread() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNUpperINsEvictedManual() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNUpperINsEvictedCritical() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNUpperINsEvictedCacheMode() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNUpperINsEvictedDaemon() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBatchesEvictorThread() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBatchesManual() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBatchesCacheMode() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBatchesCritical() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getNBatchesDaemon() {
        return 0;
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_EVICTORTHREAD_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_EVICTORTHREAD_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedEvictorThread() {
        return cacheStats.getLong(
            EvictionSource.EVICTORTHREAD.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_MANUAL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_MANUAL_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedManual() {
        return cacheStats.getLong(
            EvictionSource.MANUAL.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CACHEMODE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CACHEMODE_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedCacheMode() {
        return cacheStats.getLong(
            EvictionSource.CACHEMODE.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CRITICAL_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_CRITICAL_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedCritical() {
        return cacheStats.getLong(
            EvictionSource.CRITICAL.getNumBytesEvictedStatDef());
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_DAEMON_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#N_BYTES_EVICTED_DAEMON_NAME}</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getNBytesEvictedDeamon() {
        return cacheStats.getLong(
            EvictionSource.DAEMON.getNumBytesEvictedStatDef());
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getAvgBatchEvictorThread() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getAvgBatchManual() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getAvgBatchCacheMode() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getAvgBatchCritical() {
        return 0;
    }

    /**
     * @deprecated This statistic has been removed. The method returns 0
     * always.
     */
    public long getAvgBatchDaemon() {
        return 0;
    }

    /* MemoryBudget stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_SHARED_CACHE_TOTAL_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_SHARED_CACHE_TOTAL_BYTES_NAME}</p>
     *
     * <p>If this
     * environment uses the shared cache, this method returns the total size of
     * the shared cache, i.e., the sum of the {@link #getCacheTotalBytes()} for
     * all environments that are sharing the cache.  If this environment does
     * <i>not</i> use the shared cache, this method returns zero.</p>
     *
     * <p>To get the configured maximum cache size, see {@link
     * EnvironmentMutableConfig#getCacheSize}.</p>
     */
    public long getSharedCacheTotalBytes() {
        return cacheStats.getLong(MB_SHARED_CACHE_TOTAL_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_TOTAL_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_TOTAL_BYTES_NAME}</p>
     *
     * <p>This method returns the sum of {@link #getDataBytes}, {@link
     * #getAdminBytes}, {@link #getLockBytes} and {@link #getBufferBytes}.</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * <p>To get the configured maximum cache size, see {@link
     * EnvironmentMutableConfig#getCacheSize}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getCacheTotalBytes() {
        return cacheStats.getLong(MB_TOTAL_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_BYTES_NAME}</p>
     *
     * <p>The value returned by this method includes the amount returned by
     * {@link #getDataAdminBytes}.</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getDataBytes() {
        return cacheStats.getLong(MB_DATA_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_ADMIN_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DATA_ADMIN_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getDataAdminBytes() {
        return cacheStats.getLong(MB_DATA_ADMIN_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DOS_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_DOS_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheUnexpectedSizes">Cache Statistics: Unexpected
     * Sizes</a>
     */
    public long getDOSBytes() {
        return cacheStats.getLong(MB_DOS_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_ADMIN_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_ADMIN_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getAdminBytes() {
        return cacheStats.getLong(MB_ADMIN_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_LOCK_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.EvictorStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.dbi.DbiStatDefinition#MB_LOCK_BYTES_NAME}</p>
     *
     * <p>If this environment uses the shared cache, this method returns only
     * the amount used by this environment.</p>
     *
     * @see <a href="#cacheUnexpectedSizes">Cache Statistics: Unexpected
     * Sizes</a>
     */
    public long getLockBytes() {
        return cacheStats.getLong(MB_LOCK_BYTES);
    }

    /**
     * @deprecated Please use {@link #getDataBytes} to get the amount of cache
     * used for data and use {@link #getAdminBytes}, {@link #getLockBytes} and
     * {@link #getBufferBytes} to get other components of the total cache usage
     * ({@link #getCacheTotalBytes}).
     */
    public long getCacheDataBytes() {
        return getCacheTotalBytes() - getBufferBytes();
    }

    /* OffHeapCache stats. */

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#ALLOC_FAILURE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#ALLOC_FAILURE_NAME}</p>
     *
     * <p>Currently, with the default off-heap allocator, an allocation
     * failure occurs only when OutOfMemoryError is thrown by {@code
     * Unsafe.allocateMemory}. This might be considered a fatal error, since it
     * means that no memory is available on the machine or VM. In practice,
     * we have not seen this occur because Linux will automatically kill
     * processes that are rapidly allocating memory when available memory is
     * very low.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapAllocFailures() {
        return cacheStats.getLong(OffHeapStatDefinition.ALLOC_FAILURE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#ALLOC_OVERFLOW_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#ALLOC_OVERFLOW_NAME}</p>
     *
     * <p>Currently, with the default off-heap allocator, this never happens
     * because the allocator will perform the allocation as long as any memory
     * is available. Even so, the off-heap evictor normally prevents
     * overflowing of the off-heap cache by freeing memory before it is
     * needed.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapAllocOverflows() {
        return cacheStats.getLong(OffHeapStatDefinition.ALLOC_OVERFLOW);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#THREAD_UNAVAILABLE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#THREAD_UNAVAILABLE_NAME}</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getOffHeapThreadUnavailable() {
        return cacheStats.getLong(OffHeapStatDefinition.THREAD_UNAVAILABLE);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_TARGETED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_TARGETED_NAME}</p>
     *
     * <p>Nodes are selected as targets by the evictor based on LRU, always
     * selecting from the cold end of the LRU list. First, non-dirty nodes and
     * nodes referring to off-heap LNs are selected based on LRU. When there
     * are no more such nodes then dirty nodes with no off-heap LNs are
     * selected, based on LRU.</p>
     *
     * <p>An eviction target may actually be evicted, or skipped, or put
     * back to the LRU, potentially after stripping child LNs or mutation to
     * a BIN-delta.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getOffHeapNodesTargeted() {
        return offHeapStats.getLong(OffHeapStatDefinition.NODES_TARGETED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CRITICAL_NODES_TARGETED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CRITICAL_NODES_TARGETED_NAME}</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheCriticalEviction">Cache Statistics: Critical
     * Eviction</a>
     */
    public long getOffHeapCriticalNodesTargeted() {
        return cacheStats.getLong(
            OffHeapStatDefinition.CRITICAL_NODES_TARGETED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_EVICTED_NAME}</p>
     *
     * <p>An evicted BIN is completely removed from the off-heap cache and LRU
     * list. If it is dirty, it must be logged. A BIN is evicted only if it has
     * no off-heap child LNs and it cannot be mutated to a BIN-delta.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getOffHeapNodesEvicted() {
        return cacheStats.getLong(OffHeapStatDefinition.NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#DIRTY_NODES_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#DIRTY_NODES_EVICTED_NAME}</p>
     *
     * <p>This stat value is a subset of {@link #getOffHeapNodesEvicted()}.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getOffHeapDirtyNodesEvicted() {
        return cacheStats.getLong(OffHeapStatDefinition.DIRTY_NODES_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_STRIPPED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_STRIPPED_NAME}</p>
     *
     * <p>When a BIN is stripped, all off-heap LNs that the BIN refers to are
     * evicted. The {@link #getOffHeapLNsEvicted()} stat is incremented
     * accordingly.</p>
     *
     * <p>A stripped BIN could be a BIN in main cache that is stripped of
     * off-heap LNs, or a BIN that is off-heap and also refers to off-heap
     * LNs. When a main cache BIN is stripped, it is removed from the
     * off-heap LRU. When an off-heap BIN is stripped, it is either modified
     * in place to remove the LN references (this is done when a small
     * number of LNs are referenced and the wasted space is small), or is
     * copied to a new, smaller off-heap block with no LN references.</p>
     *
     * <p>After stripping an off-heap BIN, it is moved to the hot end of the
     * LRU list. Off-heap BINs are only mutated to BIN-deltas or evicted
     * completely when they do not refer to any off-heap LNs. This gives
     * BINs precedence over LNs in the cache.
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapNodesStripped() {
        return cacheStats.getLong(OffHeapStatDefinition.NODES_STRIPPED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_MUTATED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_MUTATED_NAME}</p>
     *
     * <p>Mutation to a BIN-delta is performed for full BINs that do not
     * refer to any off-heap LNs and can be represented as BIN-deltas in
     * cache and on disk (see {@link EnvironmentConfig#TREE_BIN_DELTA}).
     * When a BIN is mutated, it is is copied to a new, smaller off-heap
     * block. After mutating an off-heap BIN, it is moved to the hot end of
     * the LRU list.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getOffHeapNodesMutated() {
        return cacheStats.getLong(OffHeapStatDefinition.NODES_MUTATED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_SKIPPED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#NODES_SKIPPED_NAME}</p>
     *
     * <p>For example, a node will be skipped if it has been moved to the
     * hot end of the LRU list by another thread, or more rarely, already
     * processed by another evictor thread. This can occur because there is
     * a short period of time where a targeted node has been removed from
     * the LRU by the evictor thread, but not yet latched.</p>
     *
     * <p>The number of skipped nodes is normally very small, compared to the
     * number of targeted nodes.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapNodesSkipped() {
        return cacheStats.getLong(OffHeapStatDefinition.NODES_SKIPPED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LNS_EVICTED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LNS_EVICTED_NAME}</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheEviction">Cache Statistics: Eviction</a>
     */
    public long getOffHeapLNsEvicted() {
        return offHeapStats.getLong(OffHeapStatDefinition.LNS_EVICTED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LNS_LOADED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LNS_LOADED_NAME}</p>
     *
     * <p>LNs are loaded when requested by CRUD operations or other internal
     * btree operations.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapLNsLoaded() {
        return offHeapStats.getLong(OffHeapStatDefinition.LNS_LOADED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LNS_STORED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LNS_STORED_NAME}</p>
     *
     * <p>LNs are stored off-heap when they are evicted from the main cache.
     * Note that when {@link CacheMode#EVICT_LN} is used, the LN resides in
     * the main cache for a very short period since it is evicted after the
     * CRUD operation is complete.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapLNsStored() {
        return offHeapStats.getLong(OffHeapStatDefinition.LNS_STORED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#BINS_LOADED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#BINS_LOADED_NAME}</p>
     *
     * <p>BINs are loaded when needed by CRUD operations or other internal
     * btree operations.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapBINsLoaded() {
        return offHeapStats.getLong(OffHeapStatDefinition.BINS_LOADED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#BINS_STORED_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#BINS_STORED_NAME}</p>
     *
     * <p>BINs are stored off-heap when they are evicted from the main cache.
     * Note that when {@link CacheMode#EVICT_BIN} is used, the BIN resides
     * in the main cache for a very short period since it is evicted after
     * the CRUD operation is complete.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapBINsStored() {
        return offHeapStats.getLong(OffHeapStatDefinition.BINS_STORED);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CACHED_LNS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CACHED_LNS_NAME}</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public int getOffHeapCachedLNs() {
        return offHeapStats.getInt(OffHeapStatDefinition.CACHED_LNS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CACHED_BINS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CACHED_BINS_NAME}</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public int getOffHeapCachedBINs() {
        return offHeapStats.getInt(OffHeapStatDefinition.CACHED_BINS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CACHED_BIN_DELTAS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#CACHED_BIN_DELTAS_NAME}</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public int getOffHeapCachedBINDeltas() {
        return offHeapStats.getInt(OffHeapStatDefinition.CACHED_BIN_DELTAS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#TOTAL_BYTES_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#TOTAL_BYTES_NAME}</p>
     *
     * <p>This includes the estimated overhead for off-heap memory blocks, as
     * well as their contents.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * <p>To get the configured maximum off-heap cache size, see {@link
     * EnvironmentMutableConfig#getOffHeapCacheSize()}.</p>
     *
     * @see <a href="#cacheSizing">Cache Statistics: Sizing</a>
     */
    public long getOffHeapTotalBytes() {
        return offHeapStats.getLong(OffHeapStatDefinition.TOTAL_BYTES);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#TOTAL_BLOCKS_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#TOTAL_BLOCKS_NAME}</p>
     *
     * <p>There is one block for each off-heap BIN and one for each off-heap
     * LN. So the total number of blocks is the sum of
     * {@link #getOffHeapCachedLNs} and {@link #getOffHeapCachedBINs}.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapTotalBlocks() {
        return offHeapStats.getInt(OffHeapStatDefinition.TOTAL_BLOCKS);
    }

    /**
     * <p>{@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LRU_SIZE_DESC}</p>
     *
     * <p style="margin-left: 2em">Group: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#GROUP_NAME}
     * <br/>Name: {@value
     * com.sleepycat.je.evictor.OffHeapStatDefinition#LRU_SIZE_NAME}</p>
     *
     * <p>The off-heap LRU list is stored in the Java heap. Each entry occupies
     * 20 bytes of memory when compressed oops are used, or 24 bytes otherwise.
     * This memory is not considered part of the JE main cache, and is not
     * included in main cache statistics.</p>
     *
     * <p>There is one LRU entry for each off-heap BIN, and one for each BIN in
     * main cache that refers to one or more off-heap LNs. The latter approach
     * avoids an LRU entry per off-heap LN, which would use excessive amounts
     * of space in the Java heap. Similarly, when an off-heap BIN refers to
     * off-heap LNs, only one LRU entry (for the BIN) is used.</p>
     *
     * <p>If this environment uses the shared cache, the return value is the
     * total for all environments that are sharing the cache.</p>
     *
     * @see <a href="#cacheDebugging">Cache Statistics: Debugging</a>
     */
    public long getOffHeapLRUSize() {
        return offHeapStats.getInt(OffHeapStatDefinition.LRU_SIZE);
    }

    /* EnvironmentImpl stats. */

    /**
     * Returns the number of latch upgrades (relatches) required while
     * operating on this Environment.  Latch upgrades are required when an
     * operation assumes that a shared (read) latch will be sufficient but
     * later determines that an exclusive (write) latch will actually be
     * required.
     *
     * @return number of latch upgrades (relatches) required.
     */
    public long getRelatchesRequired() {
        return envImplStats.getLong(ENV_RELATCHES_REQUIRED);
    }

    /* TxnManager stats. */

    /**
     * Total lock owners in lock table.  Only provided when {@link
     * com.sleepycat.je.Environment#getStats Environment.getStats} is
     * called in "slow" mode.
     */
    public int getNOwners() {
        return lockStats.getInt(LOCK_OWNERS);
    }

    /**
     * Total read locks currently held.  Only provided when {@link
     * com.sleepycat.je.Environment#getStats Environment.getStats} is
     * called in "slow" mode.
     */
    public int getNReadLocks() {
        return lockStats.getInt(LOCK_READ_LOCKS);
    }

    /**
     * Total locks currently in lock table.  Only provided when {@link
     * com.sleepycat.je.Environment#getStats Environment.getStats} is
     * called in "slow" mode.
     */
    public int getNTotalLocks() {
        return lockStats.getInt(LOCK_TOTAL);
    }

    /**
     * Total transactions waiting for locks.  Only provided when {@link
     * com.sleepycat.je.Environment#getStats Environment.getStats} is
     * called in "slow" mode.
     */
    public int getNWaiters() {
        return lockStats.getInt(LOCK_WAITERS);
    }

    /**
     * Total write locks currently held.  Only provided when {@link
     * com.sleepycat.je.Environment#getStats Environment.getStats} is
     * called in "slow" mode.
     */
    public int getNWriteLocks() {
        return lockStats.getInt(LOCK_WRITE_LOCKS);
    }

    /**
     * Total number of lock requests to date.
     */
    public long getNRequests() {
        return lockStats.getLong(LOCK_REQUESTS);
    }

    /**
     * Total number of lock waits to date.
     */
    public long getNWaits() {
        return lockStats.getLong(LOCK_WAITS);
    }

    /**
     * Number of acquires of lock table latch with no contention.
     */
    public int getNAcquiresNoWaiters() {
        return lockStats.getInt(LATCH_NO_WAITERS);
    }

    /**
     * Number of acquires of lock table latch when it was already owned
     * by the caller.
     */
    public int getNAcquiresSelfOwned() {
        return lockStats.getInt(LATCH_SELF_OWNED);
    }

    /**
     * Number of acquires of lock table latch when it was already owned by
     * another thread.
     */
    public int getNAcquiresWithContention() {
        return lockStats.getInt(LATCH_CONTENTION);
    }

    /**
     * Number of successful no-wait acquires of the lock table latch.
     */
    public int getNAcquiresNoWaitSuccessful() {
        return lockStats.getInt(LATCH_NOWAIT_SUCCESS);
    }

    /**
     * Number of unsuccessful no-wait acquires of the lock table latch.
     */
    public int getNAcquiresNoWaitUnSuccessful() {
        return lockStats.getInt(LATCH_NOWAIT_UNSUCCESS);
    }

    /**
     * Number of releases of the lock table latch.
     */
    public int getNReleases() {
        return lockStats.getInt(LATCH_RELEASES);
    }

    /**
     * The number of user (non-internal) Cursor and Database get operations
     * performed in BIN deltas.
     */
    public long getNBinDeltaGetOps() {
        return envImplStats.getAtomicLong(ENV_BIN_DELTA_GETS);
    }

    /**
     * The number of user (non-internal) Cursor and Database insert operations
     * performed in BIN deltas (these are insertions performed via the various
     * put methods).
     */
    public long getNBinDeltaInsertOps() {
        return envImplStats.getAtomicLong(ENV_BIN_DELTA_INSERTS);
    }

    /**
     * The number of user (non-internal) Cursor and Database update operations
     * performed in BIN deltas (these are updates performed via the various
     * put methods).
     */
    public long getNBinDeltaUpdateOps() {
        return envImplStats.getAtomicLong(ENV_BIN_DELTA_UPDATES);
    }

    /**
     * The number of user (non-internal) Cursor and Database delete operations
     * performed in BIN deltas.
     */
    public long getNBinDeltaDeleteOps() {
        return envImplStats.getAtomicLong(ENV_BIN_DELTA_DELETES);
    }

    /**
     * Number of successful primary DB key search operations.
     * <p>
     * This operation corresponds to one of the following API calls:
     * <ul>
     *     <li>
     *         A successful {@link Cursor#get(DatabaseEntry, DatabaseEntry,
     *         Get, ReadOptions) Cursor.get} or {@link
     *         Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
     *         ReadOptions) Database.get} call with {@link Get#SEARCH}, {@link
     *         Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or {@link
     *         Get#SEARCH_BOTH_GTE}.
     *     </li>
     *     <li>
     *         A successful {@link SecondaryCursor#get(DatabaseEntry,
     *         DatabaseEntry, DatabaseEntry, Get, ReadOptions)
     *         SecondaryCursor.get} or {@link
     *         SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     *         DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} call
     *         when the primary data is requested (via the {@code data} param).
     *         This call internally performs a key search operation in the
     *         primary DB in order to return the data.
     *     </li>
     * </ul>
     */
    public long getPriSearchOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_SEARCH);
    }

    /**
     * Number of failed primary DB key search operations.
     * <p>
     * This operation corresponds to a call to {@link Cursor#get(DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) Cursor.get} or {@link
     * Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) Database.get} with {@link Get#SEARCH}, {@link
     * Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or {@link
     * Get#SEARCH_BOTH_GTE}, when the specified key is not found in the DB.
     */
    public long getPriSearchFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_SEARCH_FAIL);
    }

    /**
     * Number of successful secondary DB key search operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * SecondaryCursor#get(DatabaseEntry, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) SecondaryCursor.get} or {@link
     * SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} with
     * {@link Get#SEARCH}, {@link Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or
     * {@link Get#SEARCH_BOTH_GTE}.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     */
    public long getSecSearchOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_SEARCH);
    }

    /**
     * Number of failed secondary DB key search operations.
     * <p>
     * This operation corresponds to a call to {@link
     * SecondaryCursor#get(DatabaseEntry, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) SecondaryCursor.get} or {@link
     * SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} with {@link
     * Get#SEARCH}, {@link Get#SEARCH_GTE}, {@link Get#SEARCH_BOTH}, or {@link
     * Get#SEARCH_BOTH_GTE}, when the specified key is not found in the DB.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     */
    public long getSecSearchFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_SEARCH_FAIL);
    }

    /**
     * Number of successful primary DB position operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#get(DatabaseEntry, DatabaseEntry, Get, ReadOptions) Cursor.get}
     * or {@link Database#get(Transaction, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) Database.get} with {@link Get#FIRST}, {@link Get#LAST},
     * {@link Get#NEXT}, {@link Get#NEXT_DUP}, {@link Get#NEXT_NO_DUP},
     * {@link Get#PREV}, {@link Get#PREV_DUP} or {@link Get#PREV_NO_DUP}.
     */
    public long getPriPositionOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_POSITION);
    }

    /**
     * Number of successful secondary DB position operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * SecondaryCursor#get(DatabaseEntry, DatabaseEntry, DatabaseEntry, Get,
     * ReadOptions) SecondaryCursor.get} or {@link
     * SecondaryDatabase#get(Transaction, DatabaseEntry, DatabaseEntry,
     * DatabaseEntry, Get, ReadOptions) SecondaryDatabase.get} with
     * {@link Get#FIRST}, {@link Get#LAST},
     * {@link Get#NEXT}, {@link Get#NEXT_DUP}, {@link Get#NEXT_NO_DUP},
     * {@link Get#PREV}, {@link Get#PREV_DUP} or {@link Get#PREV_NO_DUP}.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     */
    public long getSecPositionOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_POSITION);
    }

    /**
     * Number of successful primary DB insertion operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put} in one of the following cases:
     * <ul>
     *     <li>
     *         When {@link Put#NO_OVERWRITE} or {@link Put#NO_DUP_DATA} is
     *         specified.
     *     </li>
     *     <li>
     *         When {@link Put#OVERWRITE} is specified and the key was inserted
     *         because it previously did not exist in the DB.
     *     </li>
     * </ul>
     */
    public long getPriInsertOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_INSERT);
    }

    /**
     * Number of failed primary DB insertion operations.
     * <p>
     * This operation corresponds to a call to {@link Cursor#put(DatabaseEntry,
     * DatabaseEntry, Put, WriteOptions) Cursor.put} or {@link
     * Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put} with {@link Put#NO_OVERWRITE} or {@link
     * Put#NO_DUP_DATA}, when the key could not be inserted because it
     * previously existed in the DB.
     */
    public long getPriInsertFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_INSERT_FAIL);
    }

    /**
     * Number of successful secondary DB insertion operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put}, for a primary DB with an associated
     * secondary DB. A secondary record is inserted when inserting a primary
     * record with a non-null secondary key, or when updating a primary record
     * and the secondary key is changed to to a non-null value that is
     * different than the previously existing value.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     */
    public long getSecInsertOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_INSERT);
    }

    /**
     * Number of successful primary DB update operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put} in one of the following cases:
     * <ul>
     *     <li>
     *         When {@link Put#OVERWRITE} is specified and the key previously
     *         existed in the DB.
     *     </li>
     *     <li>
     *         When calling {@code Cursor.put} with {@link Put#CURRENT}.
     *     </li>
     * </ul>
     */
    public long getPriUpdateOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_UPDATE);
    }

    /**
     * Number of successful secondary DB update operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#put(DatabaseEntry, DatabaseEntry, Put, WriteOptions) Cursor.put}
     * or {@link Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     * WriteOptions) Database.put}, when a primary record is updated and its
     * TTL is changed. The associated secondary records must also be updated to
     * reflect the change in the TTL.
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     */
    public long getSecUpdateOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_UPDATE);
    }

    /**
     * Number of successful primary DB deletion operations.
     * <p>
     * This operation corresponds to a successful call to {@link
     * Cursor#delete() Cursor.delete}, {@link Database#delete(Transaction,
     * DatabaseEntry, WriteOptions) Database.delete}, {@link
     * SecondaryCursor#delete() SecondaryCursor.delete} or {@link
     * SecondaryDatabase#delete(Transaction, DatabaseEntry, WriteOptions)
     * SecondaryDatabase.delete}.
     */
    public long getPriDeleteOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_DELETE);
    }

    /**
     * Number of failed primary DB deletion operations.
     * <p>
     * This operation corresponds to a call to {@link
     * Database#delete(Transaction, DatabaseEntry,
     * WriteOptions) Database.delete} or {@link
     * SecondaryDatabase#delete(Transaction, DatabaseEntry, WriteOptions)
     * SecondaryDatabase.delete}, when the key could not be deleted because it
     * did not previously exist in the DB.
     */
    public long getPriDeleteFailOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_PRI_DELETE_FAIL);
    }

    /**
     * Number of successful secondary DB deletion operations.
     * <p>
     * This operation corresponds to one of the following API calls:
     * <ul>
     *     <li>
     *         A successful call to {@link Cursor#delete() Cursor.delete} or
     *         {@link Database#delete(Transaction, DatabaseEntry,
     *         WriteOptions) Database.delete}, that deletes a primary record
     *         containing a non-null secondary key.
     *     </li>
     *     <li>
     *         A successful call to {@link SecondaryCursor#delete()
     *         SecondaryCursor.delete} or {@link
     *         SecondaryDatabase#delete(Transaction, DatabaseEntry,
     *         WriteOptions) SecondaryDatabase.delete}.
     *     </li>
     *     <li>
     *         A successful call to {@link Cursor#put(DatabaseEntry,
     *         DatabaseEntry, Put, WriteOptions) Cursor.put} or {@link
     *         Database#put(Transaction, DatabaseEntry, DatabaseEntry, Put,
     *         WriteOptions) Database.put} that updates a primary record and
     *         changes its previously non-null secondary key to null.
     *     </li>
     * </ul>
     * <p>
     * Note: Operations are currently counted as secondary DB (rather than
     * primary DB) operations only if the DB has been opened by the application
     * as a secondary DB. In particular the stats may be confusing on an HA
     * replica node if a secondary DB has not been opened by the application on
     * the replica.
     */
    public long getSecDeleteOps() {
        return throughputStats.getAtomicLong(THROUGHPUT_SEC_DELETE);
    }

    /**
     * Returns a String representation of the stats in the form of
     * &lt;stat&gt;=&lt;value&gt;
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (StatGroup group : getStatGroups()) {
            sb.append(group.toString());
        }
        return sb.toString();
    }

    /**
     * Returns a String representation of the stats which includes stats
     * descriptions in addition to &lt;stat&gt;=&lt;value&gt;
     */
    public String toStringVerbose() {
        StringBuilder sb = new StringBuilder();
        for (StatGroup group : getStatGroups()) {
            sb.append(group.toStringVerbose());
        }
        return sb.toString();
    }

    /**
     * @hidden
     * Internal use only.
     * JConsole plugin support: Get tips for stats.
     */
    public Map<String, String> getTips() {
        Map<String, String> tipsMap = new HashMap<String, String>();
        for (StatGroup group : getStatGroups()) {
            group.addToTipMap(tipsMap);
        }
        return tipsMap;
    }
}
