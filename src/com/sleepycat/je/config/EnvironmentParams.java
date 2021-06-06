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

package com.sleepycat.je.config;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;

/**
 */
public class EnvironmentParams {

    /* The prefix for all JE replication parameters. */
    public static final String REP_PARAM_PREFIX = "je.rep.";

    /*
     * The map of supported environment parameters where the key is parameter
     * name and the data is the configuration parameter object. Put first,
     * before any declarations of ConfigParams.
     */
    public final static Map<String, ConfigParam> SUPPORTED_PARAMS =
        new HashMap<String, ConfigParam>();

    /*
     * Only environment parameters that are part of the public API are
     * represented by String constants in EnvironmentConfig.
     */
    public static final LongConfigParam MAX_MEMORY =
        new LongConfigParam(EnvironmentConfig.MAX_MEMORY,
                            null,           // min
                            null,           // max
                            0L,             // default uses je.maxMemoryPercent
                            true,           // mutable
                            false);         // forReplication

    public static final IntConfigParam MAX_MEMORY_PERCENT =
        new IntConfigParam(EnvironmentConfig.MAX_MEMORY_PERCENT,
                           1,               // min
                           90,              // max
                           60,              // default
                           true,            // mutable
                           false);          // forReplication

    public static final BooleanConfigParam ENV_SHARED_CACHE =
        new BooleanConfigParam(EnvironmentConfig.SHARED_CACHE,
                               false,         // default
                               false,         // mutable
                               false);        // forReplication

    public static final LongConfigParam MAX_DISK =
        new LongConfigParam(EnvironmentConfig.MAX_DISK,
            0L,             // min
            null,           // max
            0L,             // default
            true,           // mutable
            false);         // forReplication

    public static final LongConfigParam FREE_DISK =
        new LongConfigParam(EnvironmentConfig.FREE_DISK,
            0L,             // min
            null,           // max
            5368709120L,    // default
            true,           // mutable
            false);         // forReplication

    /**
     * Used by utilities, not exposed in the API.
     *
     * If true, even when recovery is not run (see ENV_RECOVERY) by a utility,
     * the btree and dup comparators will be instantiated.  Set to true by
     * utilities such as DbScavenger that need comparators in spite of not
     * needing recovery.
     */
    public static final BooleanConfigParam ENV_COMPARATORS_REQUIRED =
        new BooleanConfigParam("je.env.comparatorsRequired",
                               false,         // default
                               false,         // mutable
                               false);        // forReplication

    /**
     * Used by utilities, not exposed in the API.
     *
     * If true, an environment is created with recovery and the related daemon
     * threads are enabled.
     */
    public static final BooleanConfigParam ENV_RECOVERY =
        new BooleanConfigParam("je.env.recovery",
                               true,          // default
                               false,         // mutable
                               false);        // forReplication

    public static final BooleanConfigParam ENV_RECOVERY_FORCE_CHECKPOINT =
        new BooleanConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT,
                               false,         // default
                               false,         // mutable
                               false);        // forReplication

    public static final BooleanConfigParam ENV_RECOVERY_FORCE_NEW_FILE =
        new BooleanConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_NEW_FILE,
                               false,         // default
                               false,         // mutable
                               false);        // forReplication

    public static final BooleanConfigParam
        HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION =
            new BooleanConfigParam(
                EnvironmentConfig.HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION,
                false,         // default
                false,         // mutable
                false);        // forReplication

    public static final BooleanConfigParam ENV_RUN_INCOMPRESSOR =
        new BooleanConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
                               true,          // default
                               true,          // mutable
                               false);        // forReplication

    public static final BooleanConfigParam ENV_RUN_EVICTOR =
        new BooleanConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR,
                               true,         // default
                               true,         // mutable
                               false);       // forReplication

    public static final BooleanConfigParam ENV_DUP_CONVERT_PRELOAD_ALL =
        new BooleanConfigParam(EnvironmentConfig.ENV_DUP_CONVERT_PRELOAD_ALL,
                               true,         // default
                               false,        // mutable
                               false);       // forReplication

    /**
     * @deprecated as of JE 4.1
     */
    private static final DurationConfigParam EVICTOR_WAKEUP_INTERVAL =
        new DurationConfigParam("je.evictor.wakeupInterval",
                                "1 s",                 // min
                                "75 min",              // max
                                "5 s",                 // default
                                false,                 // mutable
                                false);

    public static final IntConfigParam EVICTOR_CORE_THREADS =
        new IntConfigParam(EnvironmentConfig.EVICTOR_CORE_THREADS,
                            0,                      // min
                            Integer.MAX_VALUE,      // max
                            1,                      // default
                            true,                   // mutable
                            false);                 // forReplication

    public static final IntConfigParam EVICTOR_MAX_THREADS =
        new IntConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS,
                            1,                      // min
                            Integer.MAX_VALUE,      // max
                            10,                     // default
                            true,                   // mutable
                            false);                 // forReplication

    public static final DurationConfigParam EVICTOR_KEEP_ALIVE =
        new DurationConfigParam(EnvironmentConfig.EVICTOR_KEEP_ALIVE,
                                "1 s",          // min
                                "24 h",         // max
                                "10 min",       // default
                                true,           // mutable
                                false);         // forReplication

    /**
     * The amount of time to wait for the eviction pool to terminate, in order
     * to create a clean shutdown. An intentionally unadvertised parameter, of
     * use mainly for unit test cleanup.
     */
    public static final DurationConfigParam EVICTOR_TERMINATE_TIMEOUT =
        new DurationConfigParam("je.env.terminateTimeout",
                                "1 ms",         // min
                                "60 s",         // max
                                "10 s",         // default
                                true,           // mutable
                                false);         // forReplication

    public static final BooleanConfigParam EVICTOR_ALLOW_BIN_DELTAS =
        new BooleanConfigParam(EnvironmentConfig.EVICTOR_ALLOW_BIN_DELTAS,
                               true,         // default
                               false,        // mutable
                               false);       // forReplication

    /*
     * Not exposed in the API because we expect that BIN mutation will
     * always be beneficial. Intended only for debugging and testing.
     */
    public static final BooleanConfigParam EVICTOR_MUTATE_BINS =
        new BooleanConfigParam("je.evictor.mutateBins",
                               true,         // default
                               false,        // mutable
                               false);       // forReplication

    public static final BooleanConfigParam ENV_RUN_CHECKPOINTER =
        new BooleanConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                               true,        // default
                               true,        // mutable
                               false);      // forReplication

    public static final BooleanConfigParam ENV_RUN_CLEANER =
        new BooleanConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                               true,        // default
                               true,        // mutable
                               false);      // forReplication

    public static final IntConfigParam ENV_BACKGROUND_READ_LIMIT =
        new IntConfigParam(EnvironmentConfig.ENV_BACKGROUND_READ_LIMIT,
                            0,                 // min
                            Integer.MAX_VALUE, // max
                            0,                 // default
                            true,              // mutable
                            false);            // forReplication

    public static final IntConfigParam ENV_BACKGROUND_WRITE_LIMIT =
        new IntConfigParam(EnvironmentConfig.ENV_BACKGROUND_WRITE_LIMIT,
                            0,                 // min
                            Integer.MAX_VALUE, // max
                            0,                 // default
                            true,              // mutable
                            false);            // forReplication

    public static final DurationConfigParam ENV_BACKGROUND_SLEEP_INTERVAL =
        new DurationConfigParam(
                               EnvironmentConfig.ENV_BACKGROUND_SLEEP_INTERVAL,
                               "1 ms",          // min
                               null,            // max
                               "1 ms",          // default
                               true,            // mutable
                               false);          // forReplication

    public static final BooleanConfigParam ENV_CHECK_LEAKS =
        new BooleanConfigParam(EnvironmentConfig.ENV_CHECK_LEAKS,
                               true,              // default
                               false,             // mutable
                               false);            // forReplication

    public static final BooleanConfigParam ENV_FORCED_YIELD =
        new BooleanConfigParam(EnvironmentConfig.ENV_FORCED_YIELD,
                               false,             // default
                               false,             // mutable
                               false);            // forReplication

    public static final BooleanConfigParam ENV_INIT_TXN =
        new BooleanConfigParam(EnvironmentConfig.ENV_IS_TRANSACTIONAL,
                               false,             // default
                               false,             // mutable
                               false);            // forReplication

    public static final BooleanConfigParam ENV_INIT_LOCKING =
        new BooleanConfigParam(EnvironmentConfig.ENV_IS_LOCKING,
                               true,              // default
                               false,             // mutable
                               false);            // forReplication

    public static final BooleanConfigParam ENV_RDONLY =
        new BooleanConfigParam(EnvironmentConfig.ENV_READ_ONLY,
                               false,             // default
                               false,             // mutable
                               false);            // forReplication

    public static final BooleanConfigParam ENV_FAIR_LATCHES =
        new BooleanConfigParam(EnvironmentConfig.ENV_FAIR_LATCHES,
                               false,             // default
                               false,             // mutable
                               false);            // forReplication

    /**
     * Not part of the public API. As of 3.3, is true by default.  As of 6.0,
     * it is no longer used (and latches are always shared when possible).
     * The param is left in place just to avoid errors from config settings.
     */
    private static final BooleanConfigParam ENV_SHARED_LATCHES =
        new BooleanConfigParam("je.env.sharedLatches",
                               true,             // default
                               false,            // mutable
                               false);           // forReplication

    public static final BooleanConfigParam ENV_SETUP_LOGGER =
            new BooleanConfigParam("je.env.setupLogger",
                                   false,             // default
                                   false,             // mutable
                                   false);            // forReplication

    public static final DurationConfigParam ENV_LATCH_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.ENV_LATCH_TIMEOUT,
                                "1 ms",            // min
                                null,              // max
                                "5 min",           // default
                                false,             // mutable
                                false);            // forReplication

    public static final DurationConfigParam ENV_TTL_CLOCK_TOLERANCE =
        new DurationConfigParam(EnvironmentConfig.ENV_TTL_CLOCK_TOLERANCE,
                                "1 ms",            // min
                                null,              // max
                                "2 h",             // default
                                false,             // mutable
                                false);            // forReplication

    /**
     * Hidden (for now) parameter to control the assumed maximum length that a
     * lock may be held. It is used to determine when a record might expire
     * during a transaction, so we can avoid extra locking or checking for
     * locks when a record should not expire during the current transaction.
     */
    public static final DurationConfigParam ENV_TTL_MAX_TXN_TIME =
        new DurationConfigParam("je.env.ttlMaxTxnTime",
                                null,              // min
                                null,              // max
                                "24 h",            // default
                                false,             // mutable
                                false);            // forReplication

    /**
     * Hidden (for now) parameter to determine the amount added to the
     * expirationTime of a record to determine when to purge it, in the
     * cleaner. The goal is to ensure (disregarding clock changes) that when a
     * record is locked, its LN will not be purged. We lock the record before
     * fetching the LN, so delaying the purge of the LN a little should prevent
     * purging a locked LN due to thread scheduling issues.
     */
    public static final DurationConfigParam ENV_TTL_LN_PURGE_DELAY =
        new DurationConfigParam("je.env.ttlLnPurgeDelay",
                                null,              // min
                                null,              // max
                                "5 s",             // default
                                false,             // mutable
                                false);            // forReplication

    /**
     * Hidden (for now) parameter to allow user key/data values to be included
     * in exception messages, log messages, etc. For example, when this is set
     * to true, the SecondaryReferenceException message will include the
     * primary key and secondary key.
     */
    public static final BooleanConfigParam ENV_EXPOSE_USER_DATA =
        new BooleanConfigParam("je.env.exposeUserData",
                               false,            // default
                               true,             // mutable
                               false);           // forReplication

    public static final BooleanConfigParam ENV_DB_EVICTION =
        new BooleanConfigParam(EnvironmentConfig.ENV_DB_EVICTION,
                               true,             // default
                               false,            // mutable
                               false);           // forReplication

    public static final IntConfigParam ADLER32_CHUNK_SIZE =
        new IntConfigParam(EnvironmentConfig.ADLER32_CHUNK_SIZE,
                           0,       // min
                           1 << 20, // max
                           0,       // default
                           true,    // mutable
                           false);  // forReplication

    /*
     * Database Logs
     */
    /* default: 2k * NUM_LOG_BUFFERS */
    public static final int MIN_LOG_BUFFER_SIZE = 2048;
    public static final int NUM_LOG_BUFFERS_DEFAULT = 3;
    public static final long LOG_MEM_SIZE_MIN =
        NUM_LOG_BUFFERS_DEFAULT * MIN_LOG_BUFFER_SIZE;
    public static final String LOG_MEM_SIZE_MIN_STRING =
        Long.toString(LOG_MEM_SIZE_MIN);

    public static final LongConfigParam LOG_MEM_SIZE =
        new LongConfigParam(EnvironmentConfig.LOG_TOTAL_BUFFER_BYTES,
                            LOG_MEM_SIZE_MIN,  // min
                            null,              // max
                            0L,                // by default computed
                                               // from je.maxMemory
                            false,             // mutable
                            false);            // forReplication

    public static final IntConfigParam NUM_LOG_BUFFERS =
        new IntConfigParam(EnvironmentConfig.LOG_NUM_BUFFERS,
                           2,                  // min
                           null,               // max
                           NUM_LOG_BUFFERS_DEFAULT, // default
                           false,              // mutable
                           false);             // forReplication

    public static final IntConfigParam LOG_BUFFER_MAX_SIZE =
        new IntConfigParam(EnvironmentConfig.LOG_BUFFER_SIZE,
                           1 << 10,  // min
                           null,     // max
                           1 << 20,  // default
                           false,    // mutable
                           false);   // forReplication

    public static final IntConfigParam LOG_FAULT_READ_SIZE =
        new IntConfigParam(EnvironmentConfig.LOG_FAULT_READ_SIZE,
                           32,     // min
                           null,   // max
                           2048,   // default
                           false,  // mutable
                           false); // forReplication

    public static final IntConfigParam LOG_ITERATOR_READ_SIZE =
        new IntConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE,
                           128,    // min
                           null,   // max
                           8192,   // default
                           false,  // mutable
                           false); // forReplication

    public static final IntConfigParam LOG_ITERATOR_MAX_SIZE =
        new IntConfigParam(EnvironmentConfig.LOG_ITERATOR_MAX_SIZE,
                           128,      // min
                           null,     // max
                           16777216, // default
                           false,    // mutable
                           false);   // forReplication

    public static final LongConfigParam LOG_FILE_MAX =
        new LongConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                            1000000L,    // min
                            1073741824L, // max
                            10000000L,   // default
                            false,       // mutable
                            false);      // forReplication

    public static final IntConfigParam LOG_N_DATA_DIRECTORIES =
        new IntConfigParam(EnvironmentConfig.LOG_N_DATA_DIRECTORIES,
                           0,      // min
                           256,    // max
                           0,      // default
                           false,  // mutable
                           false); // forReplication

    public static final BooleanConfigParam LOG_CHECKSUM_READ =
        new BooleanConfigParam(EnvironmentConfig.LOG_CHECKSUM_READ,
                               true,               // default
                               false,              // mutable
                               false);             // forReplication

    public static final BooleanConfigParam LOG_VERIFY_CHECKSUMS =
        new BooleanConfigParam(EnvironmentConfig.LOG_VERIFY_CHECKSUMS,
                               false,              // default
                               false,              // mutable
                               false);             // forReplication

    public static final BooleanConfigParam LOG_MEMORY_ONLY =
        new BooleanConfigParam(EnvironmentConfig.LOG_MEM_ONLY,
                               false,              // default
                               false,              // mutable
                               false);             // forReplication

    public static final IntConfigParam LOG_FILE_CACHE_SIZE =
        new IntConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE,
                           3,      // min
                           null,   // max
                           100,    // default
                           false,  // mutable
                           false); // forReplication

    /**
     * This is experimental and pending performance tests. Javadoc and change
     * log are commented out below, and can be used if we decide to use this.
     */
    public static final IntConfigParam LOG_FILE_WARM_UP_SIZE =
        new IntConfigParam("je.log.fileWarmUpSize",
            0,      // min
            null,   // max
            0,      // default
            false,  // mutable
            false); // forReplication

    /**
     * This is experimental and pending performance tests. Javadoc and change
     * log are commented out below, and can be used if we decide to use this.
     */
    public static final IntConfigParam LOG_FILE_WARM_UP_BUF_SIZE =
        new IntConfigParam("je.log.fileWarmUpReadSize",
            128,      // min
            null,     // max
            10485760, // default
            false,    // mutable
            false);   // forReplication

    /* 
     * Whether detect unexpected log file deletion.
     */
    public static final BooleanConfigParam LOG_DETECT_FILE_DELETE =
        new BooleanConfigParam(EnvironmentConfig.LOG_DETECT_FILE_DELETE,
            true,     // default
            false,    // mutable
            false);   // forReplication

    /*
     * The interval used to check for unexpected file deletions.
     */
    public static final DurationConfigParam LOG_DETECT_FILE_DELETE_INTERVAL =
        new DurationConfigParam("je.log.detectFileDeleteInterval",
            "1 ms",     // min
            null,       // max
            "1000 ms",  // default
            false,      // mutable
            false);     // forReplication

    /**
     * The size in MiB to be read sequentially at the end of the log in order
     * to warm the file system cache.
     * <p>
     * Making use of sequential reads to warm the file system cache has the
     * benefit of reducing random reads caused by CRUD operations, and thereby
     * increasing throughput and latency for these operations. This is
     * especially true during the initial period after opening an Environment,
     * when CRUD operations must fetch Btree internal nodes from the file
     * system in order to populate the JE cache. The fetches due to JE cache
     * misses typically cause random reads. Often the Btree internal nodes that
     * are needed appear close to the end of the log because they were written
     * fairly recently by checkpoints, and this is why warming the cache with
     * the data at the end of the log is often beneficial.
     * <p>
     * The warm-up occurs concurrently with recovery when an Environment is
     * opened. It may finish before recovery finishes, or continue after
     * recovery finishes when recovery is brief. In the latter case, the
     * warm-up is concurrent with the application's CRUD operations. A
     * dedicated thread is used for the warm-up, and this thread is destroyed
     * when warm-up is complete.
     * <p>
     * Recovery itself will perform at least a partial warm-up implicitly,
     * since it reads the log (sequentially), and in fact it may read more than
     * the configured warm-up size. The warm-up thread will only read the
     * portion of the log not being read by recovery, and only when the warm-up
     * size is larger than the size read by recovery (i.e., it reads the
     * difference between these two sizes).
     * <p>
     * The size read by recovery is dependent on whether the Environment was
     * previously closed cleanly (a crash did not occur and the application
     * called Environment.close), and on the size of the last complete
     * checkpoint. When the environment is closed cleanly with a small
     * checkpoint, recovery will only read a small portion of the log, and in
     * this case the additional reads performed by the warm-up thread can be
     * very beneficial.
     * <p>
     * If the warm-up size is larger than the amount of memory available to the
     * file system cache, then the warm-up may be counter productive, although
     * TODO: change text below or change default to 1024
     * the default warm-up size (1 GiB) was chosen to avoid this problem in
     * most cases. Applications are advised to change the warm-up size based on
     * knowledge of the amount of physical memory on the machine and how much
     * is expected to be available as file system cache. The warm-up may be
     * disabled by setting the warm-up size to zero, although of course
     * recovery will continue to do some amount of warm-up implicitly.
     * <p>
     * The warm-up thread performs read operations using a single buffer and it
     * reads as much as will fit in the buffer at a time. The size of the
     * buffer, and therefore the maximum size of each read, is {@link
     * #LOG_FILE_WARM_UP_READ_SIZE}. Files are read in the reverse of the order
     * they were written.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>0</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table></p>
    public static final String LOG_FILE_WARM_UP_SIZE = "je.log.fileWarmUpSize";
     */

    /**
     * The read buffer size for warming the file system cache; see {@link
     * #LOG_FILE_WARM_UP_SIZE}.
     *
     * Because the warm-up can be concurrent with application CRUD operations,
     * it is important that a large buffer size be used for reading the data
     * files during the warm-up. That way, the warm-up is performed using
     * sequential reads to a large degree, even though CRUD operations may
     * cause some random I/O. Sequential reads are required to obtain the
     * performance benefit of the warm-up.
     * <p>
     * Note that this buffer is allocated outside of the JE cache, so the Java
     * heap size must be set accordingly.
     * <p>
     * The default value, 10 MiB, is designed to reduce random I/O to some
     * degree. It should be made larger to perform the warm-up more quickly,
     * especially if there are many application threads performing CRUD
     * operations. In our tests, using a value of 100 MiB minimized the time to
     * complete the warm-up while 20 threads performed CRUD operations.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>10485760 (10 MiB)</td>
     * <td>128</td>
     * <td>-none-</td>
     * </tr>
     * </table></p>
    public static final String LOG_FILE_WARM_UP_READ_SIZE =
        "je.log.fileWarmUpReadSize";
     */

    /* Future change log entry for above feature: (adjust for default value)
    <li>
    JE now warms the file system cache at startup by sequentially reading at least
    1 GiB (by default) at the end of the data log, even if this amount is not read
    by recovery.
    <p>
    Making use of sequential reads to warm the file system cache has the
    benefit of reducing random reads caused by CRUD operations, and thereby
    increasing throughput and latency for these operations. This is
    especially true during the initial period after opening an Environment,
    when CRUD operations must fetch Btree internal nodes from the file
    system in order to populate the JE cache. The fetches due to JE cache
    misses typically cause random reads. Often the Btree internal nodes that
    are needed appear close to the end of the log because they were written
    fairly recently by checkpoints, and this is why warming the cache with
    the data at the end of the log is often beneficial.
    <p>
    A new config param, EnvironmentConfig.LOG_FILE_WARM_UP_SIZE, can be modified to
    change the size of the log read during warm-up, or to disable the warm-up. See
    the javadoc for this parameter for details on the warm-up behavior.  Another
    new parameter, EnvironmentConfig.LOG_FILE_WARM_UP_READ_SIZE, provides control
    over the buffer size for the warm-up. Applications running with very small
    heaps or very little memory available to the file system should disable the
    warm-up or reduce these param values from their default settings.
    <p>
    [#23893] (6.2.27)
    </li><br>
    */

    public static final DurationConfigParam LOG_FSYNC_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.LOG_FSYNC_TIMEOUT,
                                "10 ms",           // min
                                null,              // max
                                "500 ms",          // default
                                false,             // mutable
                                false);            // forReplication

    public static final DurationConfigParam LOG_FSYNC_TIME_LIMIT =
        new DurationConfigParam(EnvironmentConfig.LOG_FSYNC_TIME_LIMIT,
                                "0",               // min
                                "30 s",            // max
                                "5 s",             // default
                                false,             // mutable
                                false);            // forReplication

    public static final DurationConfigParam LOG_GROUP_COMMIT_INTERVAL =
        new DurationConfigParam(EnvironmentConfig.LOG_GROUP_COMMIT_INTERVAL,
                                    "0 ns",        // min
                                    null,          // max
                                    "0 ns",        // default
                                    false,         // mutable
                                    false);        // forReplication

    public static final IntConfigParam LOG_GROUP_COMMIT_THRESHOLD =
        new IntConfigParam(EnvironmentConfig.LOG_GROUP_COMMIT_THRESHOLD,
                           0,      // min
                           null,   // max
                           0,      // default
                           false,  // mutable
                           false); // forReplication

    /**
     * @see EnvironmentConfig#LOG_FLUSH_SYNC_INTERVAL
     */
    public static final DurationConfigParam LOG_FLUSH_SYNC_INTERVAL =
        new DurationConfigParam(
            EnvironmentConfig.LOG_FLUSH_SYNC_INTERVAL,
            "0",             // min
            null,            // max
            "20 s",          // default
            true,            // mutable
            false);          // forReplication

    /**
     * @see EnvironmentConfig#LOG_FLUSH_NO_SYNC_INTERVAL
     */
    public static final DurationConfigParam LOG_FLUSH_NO_SYNC_INTERVAL =
        new DurationConfigParam(
            EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL,
            "0",             // min
            null,            // max
            "5 s",           // default
            true,            // mutable
            false);          // forReplication

    /**
     * Deprecated but still supported for backward compatibility.
     */
    public static final BooleanConfigParam OLD_REP_RUN_LOG_FLUSH_TASK =
        new BooleanConfigParam(
            EnvironmentParams.REP_PARAM_PREFIX + "runLogFlushTask",
            true,             // default
            true,             // mutable
            true);            // forReplication

    /**
     * Deprecated but still supported for backward compatibility.
     */
    public static final DurationConfigParam OLD_REP_LOG_FLUSH_TASK_INTERVAL =
        new DurationConfigParam(
            EnvironmentParams.REP_PARAM_PREFIX + "logFlushTaskInterval",
            "1 s",           // min
            null,            // max
            "5 min",         // default
            true,            // mutable
            true);           // forReplication

    public static final BooleanConfigParam LOG_USE_ODSYNC =
        new BooleanConfigParam(EnvironmentConfig.LOG_USE_ODSYNC,
                               false,          // default
                               false,          // mutable
                               false);         // forReplication

    public static final BooleanConfigParam LOG_USE_NIO =
        new BooleanConfigParam(EnvironmentConfig.LOG_USE_NIO,
                               false,          // default
                               false,          // mutable
                               false);         // forReplication

    public static final BooleanConfigParam LOG_USE_WRITE_QUEUE =
        new BooleanConfigParam(EnvironmentConfig.LOG_USE_WRITE_QUEUE,
                               true,           // default
                               false,          // mutable
                               false);         // forReplication

    public static final IntConfigParam LOG_WRITE_QUEUE_SIZE =
        new IntConfigParam(EnvironmentConfig.LOG_WRITE_QUEUE_SIZE,
                           1 << 12,    // min (4KB)
                           1 << 28,    // max (32MB)
                           1 << 20,    // default (1MB)
                           false,      // mutable
                           false);     // forReplication

    /**
     * @deprecated
     */
    private static final BooleanConfigParam LOG_DIRECT_NIO =
        new BooleanConfigParam(EnvironmentConfig.LOG_DIRECT_NIO,
                               false,          // default
                               false,          // mutable
                               false);         // forReplication

    /**
     * @deprecated
     */
    private static final LongConfigParam LOG_CHUNKED_NIO =
        new LongConfigParam(EnvironmentConfig.LOG_CHUNKED_NIO,
                            0L,         // min
                            1L << 26,   // max (64M)
                            0L,         // default (no chunks)
                            false,      // mutable
                            false);     // forReplication

    /**
     * @deprecated As of 3.3, no longer used
     *
     * Optimize cleaner operation for temporary deferred write DBs.
     */
    public static final BooleanConfigParam LOG_DEFERREDWRITE_TEMP =
        new BooleanConfigParam("je.deferredWrite.temp",
                               false,          // default
                               false,          // mutable
                               false);         // forReplication

    /*
     * @see EnvironmentConfig#ENV_RUN_VERIFIER
     */
    public static final BooleanConfigParam ENV_RUN_VERIFIER =
        new BooleanConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER,
            true,     // default
            true,     // mutable
            false);   // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_SCHEDULE
     */
    public static final ConfigParam VERIFY_SCHEDULE =
        new ConfigParam(EnvironmentConfig.VERIFY_SCHEDULE,
            "0 0 * * *",         // default
            true,                // mutable
            false);              // forReplication

    /*
     * The max accepted tardiness to allow the scheduled run of verifier to
     * execute.
     * <p>
     * Normally, the verifier runs at most once per scheduled interval. If the
     * complete verification (log verification followed by Btree verification)
     * takes longer than the scheduled interval, then the next verification
     * will start at the next increment of the interval. For example, if the
     * default schedule is used (one per day at midnight), and verification
     * takes 25 hours, then verification will occur once every two
     * days (48 hours), starting at midnight.
     * <p>
     * But sometimes, some degree of tardiness may be tolerated. For example,
     * if the default schedule is used (one per day at midnight) and if the
     * verification takes 24 hours and 5 minutes, the left 23 hours and
     * 55 minutes may be considered to be enough for the scheduled run.
     * <p>
     * VERIFY_MAX_TARDINESS is just used to constraint the tardiness.
     * If the tardiness caused by the current long-time verification exceeds
     * VERIFY_MAX_TARDINESS, then the scheduled run will be skipped.
     */
    public static final DurationConfigParam VERIFY_MAX_TARDINESS =
        new DurationConfigParam("je.env.verifyMaxTardiness",
            "1 s",       // min
            null,        // max
            "5 min",     // default
            true,        // mutable
            false);      // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_LOG
     */
    public static final BooleanConfigParam VERIFY_LOG =
        new BooleanConfigParam(EnvironmentConfig.VERIFY_LOG,
            true,     // default
            true,     // mutable
            false);   // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_LOG_READ_DELAY
     */
    public static final DurationConfigParam VERIFY_LOG_READ_DELAY =
        new DurationConfigParam(EnvironmentConfig.VERIFY_LOG_READ_DELAY,
            "0 ms",      // min
            "10 s",      // max
            "100 ms",    // default
            true,        // mutable
            false);      // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_BTREE
     */
    public static final BooleanConfigParam VERIFY_BTREE =
        new BooleanConfigParam(EnvironmentConfig.VERIFY_BTREE,
            true,     // default
            true,     // mutable
            false);   // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_SECONDARIES
     */
    public static final BooleanConfigParam VERIFY_SECONDARIES =
        new BooleanConfigParam(EnvironmentConfig.VERIFY_SECONDARIES,
            true,     // default
            true,     // mutable
            false);   // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_DATA_RECORDS
     */
    public static final BooleanConfigParam VERIFY_DATA_RECORDS =
        new BooleanConfigParam(EnvironmentConfig.VERIFY_DATA_RECORDS,
            false,    // default
            true,     // mutable
            false);   // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_OBSOLETE_RECORDS
     */
    public static final BooleanConfigParam VERIFY_OBSOLETE_RECORDS =
        new BooleanConfigParam(EnvironmentConfig.VERIFY_OBSOLETE_RECORDS,
            false,    // default
            true,     // mutable
            false);   // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_BTREE_BATCH_SIZE
     */
    public static final IntConfigParam VERIFY_BTREE_BATCH_SIZE =
        new IntConfigParam(EnvironmentConfig.VERIFY_BTREE_BATCH_SIZE,
            1,      // min
            10000,  // max
            1000,   // default
            true,  // mutable
            false); // forReplication

    /*
     * @see EnvironmentConfig#VERIFY_BTREE_BATCH_DELAY
     */
    public static final DurationConfigParam VERIFY_BTREE_BATCH_DELAY =
        new DurationConfigParam(EnvironmentConfig.VERIFY_BTREE_BATCH_DELAY,
            "0 ms",      // min
            "10 s",      // max
            "10 ms",     // default
            true,       // mutable
            false);      // forReplication

    /*
     * Tree
     */
    public static final IntConfigParam NODE_MAX =
        new IntConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES,
                           4,      // min
                           32767,  // max
                           128,    // default
                           false,  // mutable
                           false); // forReplication

    public static final IntConfigParam NODE_MAX_DUPTREE =
        new IntConfigParam(EnvironmentConfig.NODE_DUP_TREE_MAX_ENTRIES,
                           4,      // min
                           32767,  // max
                           128,    // default
                           false,  // mutable
                           false); // forReplication

    public static final IntConfigParam TREE_MAX_EMBEDDED_LN =
        new IntConfigParam(EnvironmentConfig.TREE_MAX_EMBEDDED_LN,
                           0,      // min
                           null,   // max
                           16,     // default
                           false,  // mutable
                           false); // forReplication

    /**
     * @deprecated as of JE 6.0
     */
    private static final IntConfigParam BIN_MAX_DELTAS =
        new IntConfigParam(EnvironmentConfig.TREE_MAX_DELTA,
                           0,      // min
                           100,    // max
                           10,     // default
                           false,  // mutable
                           false); // forReplication

    public static final IntConfigParam BIN_DELTA_PERCENT =
        new IntConfigParam(EnvironmentConfig.TREE_BIN_DELTA,
                           0,      // min
                           75,     // max
                           25,     // default
                           false,  // mutable
                           false); // forReplication

    /*
     * Whether blind insertions are allowed in BIN-deltas (it is also used to
     * determine the max number of slots when a delta is created).
     */
    public static final BooleanConfigParam BIN_DELTA_BLIND_OPS =
        new BooleanConfigParam("je.tree.binDeltaBlindOps",
                               true,         // default
                               false,        // mutable
                               false);       // forReplication

    /*
     * Whether blind puts are allowed in BIN-deltas. Blind puts imply
     * the storage of bloom filters in BIN-deltas.
     */
    public static final BooleanConfigParam BIN_DELTA_BLIND_PUTS =
        new BooleanConfigParam("je.tree.binDeltaBlindPuts",
                               true,         // default
                               false,        // mutable
                               false);       // forReplication

    public static final LongConfigParam MIN_TREE_MEMORY =
        new LongConfigParam(EnvironmentConfig.TREE_MIN_MEMORY,
                            50L * 1024,   // min
                            null,         // max
                            500L * 1024,  // default
                            true,         // mutable
                            false);       // forReplication

    public static final IntConfigParam TREE_COMPACT_MAX_KEY_LENGTH =
        new IntConfigParam(EnvironmentConfig.TREE_COMPACT_MAX_KEY_LENGTH,
                           0,      // min
                           255,    // max
                           16,     // default
                           false,  // mutable
                           false); // forReplication

    /*
     * IN Compressor
     */
    public static final DurationConfigParam COMPRESSOR_WAKEUP_INTERVAL =
        new DurationConfigParam(EnvironmentConfig.COMPRESSOR_WAKEUP_INTERVAL,
                                "1 s",                 // min
                                "75 min",              // max
                                "5 s",                 // default
                                false,                 // mutable
                                false);                // forReplication

    public static final IntConfigParam COMPRESSOR_RETRY =
        new IntConfigParam(EnvironmentConfig.COMPRESSOR_DEADLOCK_RETRY,
                           0,                 // min
                           Integer.MAX_VALUE, // max
                           3,                 // default
                           false,             // mutable
                           false);            // forReplication

    public static final DurationConfigParam COMPRESSOR_LOCK_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.COMPRESSOR_LOCK_TIMEOUT,
                                null,                  // min
                                "75 min",              // max
                                "500 ms",              // default
                                false,                 // mutable
                                false);                // forReplication

    /*
     * Evictor
     */
    public static final LongConfigParam EVICTOR_EVICT_BYTES =
        new LongConfigParam(EnvironmentConfig.EVICTOR_EVICT_BYTES,
                            1024L,       // min
                            null,        // max
                            524288L,     // default
                            false,       // mutable
                            false);      // forReplication

    /**
     * @deprecated As of 2.0, this is replaced by je.evictor.evictBytes
     *
     * When eviction happens, the evictor will push memory usage to this
     * percentage of je.maxMemory.
     */
    private static final IntConfigParam EVICTOR_USEMEM_FLOOR =
        new IntConfigParam("je.evictor.useMemoryFloor",
                           50,        // min
                           100,       // max
                           95,        // default
                           false,     // mutable
                           false);    // forReplication

    /**
     * @deprecated As of 1.7.2, this is replaced by je.evictor.nodesPerScan
     *
     * The evictor percentage of total nodes to scan per wakeup.
     */
    private static final IntConfigParam EVICTOR_NODE_SCAN_PERCENTAGE =
        new IntConfigParam("je.evictor.nodeScanPercentage",
                           1,          // min
                           100,        // max
                           10,         // default
                           false,      // mutable
                           false);     // forReplication

    /**
     * @deprecated As of 1.7.2, 1 node is chosen per scan.
     *
     * The evictor percentage of scanned nodes to evict per wakeup.
     */
    private static final
        IntConfigParam EVICTOR_EVICTION_BATCH_PERCENTAGE =
        new IntConfigParam("je.evictor.evictionBatchPercentage",
                           1,          // min
                           100,        // max
                           10,         // default
                           false,      // mutable
                           false);     // forReplication

    /**
     * @deprecated as of JE 6.0
     */
    private static final IntConfigParam EVICTOR_NODES_PER_SCAN =
        new IntConfigParam(EnvironmentConfig.EVICTOR_NODES_PER_SCAN,
                           1,           // min
                           1000,        // max
                           10,          // default
                           false,       // mutable
                           false);      // forReplication

    public static final IntConfigParam EVICTOR_CRITICAL_PERCENTAGE =
        new IntConfigParam(EnvironmentConfig.EVICTOR_CRITICAL_PERCENTAGE,
                           0,           // min
                           1000,        // max
                           0,           // default
                           false,       // mutable
                           false);      // forReplication

    /**
     * @deprecated as of JE 4.1
     */
    private static final IntConfigParam EVICTOR_RETRY =
        new IntConfigParam(EnvironmentConfig.EVICTOR_DEADLOCK_RETRY,
                            0,                 // min
                            Integer.MAX_VALUE, // max
                            3,                 // default
                           false,              // mutable
                           false);             // forReplication

    /**
     * @deprecated as of JE 6.0
     */
    private static final BooleanConfigParam EVICTOR_LRU_ONLY =
        new BooleanConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY,
                               true,                  // default
                               false,                 // mutable
                               false);                // forReplication

    /**
     * If true (the default), use a 2-level LRU policy that aims to keep
     * dirty BTree nodes in memory at the expense of potentially hotter
     * clean nodes. Specifically, a node that is selected for eviction from
     * level-1 will be moved to level-2 if it is dirty. Nodes in level-2 are
     * considered for eviction only after all nodes in level-1 have been
     * considered. Dirty nodes that are in level-2 are moved back to level-1
     * when they get cleaned.
     * <p>
     * This parameter applies to the new evictor only.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table></p>
     */
    public static final BooleanConfigParam EVICTOR_USE_DIRTY_LRU =
        new BooleanConfigParam("je.evictor.useDirtyLRU",
                               true,                  // default
                               false,                 // mutable
                               false);                // forReplication

    public static final IntConfigParam EVICTOR_N_LRU_LISTS =
        new IntConfigParam(EnvironmentConfig.EVICTOR_N_LRU_LISTS,
                           1,       // min
                           32,      // max
                           4,       // default
                           false,   // mutable
                           false);  // forReplication

    public static final BooleanConfigParam EVICTOR_FORCED_YIELD =
        new BooleanConfigParam(EnvironmentConfig.EVICTOR_FORCED_YIELD,
                               false,             // default
                               false,             // mutable
                               false);            // forReplication

    /* Off-heap cache. */

    public static final LongConfigParam MAX_OFF_HEAP_MEMORY =
        new LongConfigParam(EnvironmentConfig.MAX_OFF_HEAP_MEMORY,
            0L,             // min
            null,           // max
            0L,             // default
            true,           // mutable
            false);         // forReplication

    public static final LongConfigParam OFFHEAP_EVICT_BYTES =
        new LongConfigParam(EnvironmentConfig.OFFHEAP_EVICT_BYTES,
            1024L,        // min
            null,         // max
            50 * 1024 * 1024L, // default
            false,        // mutable
            false);       // forReplication

    public static final BooleanConfigParam OFFHEAP_CHECKSUM =
        new BooleanConfigParam(EnvironmentConfig.OFFHEAP_CHECKSUM,
            false,                 // default
            false,                 // mutable
            false);                // forReplication

    /**
     */
    public static final BooleanConfigParam ENV_RUN_OFFHEAP_EVICTOR =
        new BooleanConfigParam(EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR,
            true,         // default
            true,         // mutable
            false);       // forReplication

    public static final BooleanConfigParam ENV_EXPIRATION_ENABLED =
        new BooleanConfigParam(EnvironmentConfig.ENV_EXPIRATION_ENABLED,
            true,          // default
            true,          // mutable
            false);        // forReplication

    public static final IntConfigParam OFFHEAP_CORE_THREADS =
        new IntConfigParam(EnvironmentConfig.OFFHEAP_CORE_THREADS,
            0,                      // min
            Integer.MAX_VALUE,      // max
            1,                      // default
            true,                   // mutable
            false);                 // forReplication

    public static final IntConfigParam OFFHEAP_MAX_THREADS =
        new IntConfigParam(EnvironmentConfig.OFFHEAP_MAX_THREADS,
            1,                      // min
            Integer.MAX_VALUE,      // max
            3,                      // default
            true,                   // mutable
            false);                 // forReplication

    public static final DurationConfigParam OFFHEAP_KEEP_ALIVE =
        new DurationConfigParam(EnvironmentConfig.OFFHEAP_KEEP_ALIVE,
            "1 s",          // min
            "24 h",         // max
            "10 min",       // default
            true,           // mutable
            false);         // forReplication

    public static final IntConfigParam OFFHEAP_N_LRU_LISTS =
        new IntConfigParam(EnvironmentConfig.OFFHEAP_N_LRU_LISTS,
            1,       // min
            32,      // max
            4,       // default
            false,   // mutable
            false);  // forReplication

    /*
     * Checkpointer
     */
    public static final LongConfigParam CHECKPOINTER_BYTES_INTERVAL =
        new LongConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
                            0L,              // min
                            Long.MAX_VALUE,  // max
                            20000000L,       // default
                            false,           // mutable
                            false);          // forReplication

    public static final DurationConfigParam CHECKPOINTER_WAKEUP_INTERVAL =
        new DurationConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
                                "1 s",                 // min
                                "75 min",              // max
                                "0",                   // default
                                false,                 // mutable
                                false);                // forReplication

    public static final IntConfigParam CHECKPOINTER_RETRY =
        new IntConfigParam(EnvironmentConfig.CHECKPOINTER_DEADLOCK_RETRY,
                           0,                 // min
                           Integer.MAX_VALUE, // max
                           3,                 // default
                           false,             // mutable
                           false);            // forReplication

    public static final BooleanConfigParam CHECKPOINTER_HIGH_PRIORITY =
        new BooleanConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY,
                               false, // default
                               true,  // mutable
                               false);// forReplication

    /*
     * Cleaner
     */
    public static final IntConfigParam CLEANER_MIN_UTILIZATION =
        new IntConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                           0,           // min
                           90,          // max
                           50,          // default
                           true,        // mutable
                           false);      // forReplication

    public static final IntConfigParam CLEANER_MIN_FILE_UTILIZATION =
        new IntConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION,
                           0,           // min
                           50,          // max
                           5,           // default
                           true,        // mutable
                           false);      // forReplication

    public static final LongConfigParam CLEANER_BYTES_INTERVAL =
        new LongConfigParam(EnvironmentConfig.CLEANER_BYTES_INTERVAL,
                            0L,             // min
                            Long.MAX_VALUE, // max
                            0L,             // default
                            true,           // mutable
                            false);         // forReplication

    public static final DurationConfigParam CLEANER_WAKEUP_INTERVAL =
        new DurationConfigParam(EnvironmentConfig.CLEANER_WAKEUP_INTERVAL,
                            "0",           // min
                            "1 h",         // max
                            "10 s",        // default
                            true,          // mutable
                            false);        // forReplication

    public static final BooleanConfigParam CLEANER_FETCH_OBSOLETE_SIZE =
        new BooleanConfigParam(EnvironmentConfig.CLEANER_FETCH_OBSOLETE_SIZE,
                               false, // default
                               true,  // mutable
                               false);// forReplication

    /**
     * @deprecated in JE 6.3. Adjustments are no longer needed because LN log
     * sizes have been stored in the Btree since JE 6.0.
     */
    private static final BooleanConfigParam CLEANER_ADJUST_UTILIZATION =
        new BooleanConfigParam(EnvironmentConfig.CLEANER_ADJUST_UTILIZATION,
                               false, // default
                               true,  // mutable
                               false);// forReplication

    public static final IntConfigParam CLEANER_DEADLOCK_RETRY =
        new IntConfigParam(EnvironmentConfig.CLEANER_DEADLOCK_RETRY,
                            0,                 // min
                            Integer.MAX_VALUE, // max
                            3,                 // default
                           true,               // mutable
                           false);             // forReplication

    public static final DurationConfigParam CLEANER_LOCK_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.CLEANER_LOCK_TIMEOUT,
                                "0",                // min
                                "75 min",           // max
                                "500 ms",           // default
                                true,               // mutable
                                false);             // forReplication

    public static final BooleanConfigParam CLEANER_REMOVE =
        new BooleanConfigParam(EnvironmentConfig.CLEANER_EXPUNGE,
                               true,                 // default
                               true,                 // mutable
                               false);               // forReplication

    public static final BooleanConfigParam CLEANER_USE_DELETED_DIR =
        new BooleanConfigParam(EnvironmentConfig.CLEANER_USE_DELETED_DIR,
                               false,                // default
                               true,                 // mutable
                               false);               // forReplication

    /**
     * @deprecated As of 1.7.1, no longer used.
     */
    private static final IntConfigParam CLEANER_MIN_FILES_TO_DELETE =
        new IntConfigParam("je.cleaner.minFilesToDelete",
                           1,           // min
                           1000000,     // max
                           5,           // default
                           false,       // mutable
                           false);      // forReplication

    /**
     * @deprecated As of 2.0, no longer used.
     */
    private static final IntConfigParam CLEANER_RETRIES =
        new IntConfigParam("je.cleaner.retries",
                           0,           // min
                           1000,        // max
                           10,          // default
                           false,       // mutable
                           false);      // forReplication

    /**
     * @deprecated As of 2.0, no longer used.
     */
    private static final IntConfigParam CLEANER_RESTART_RETRIES =
        new IntConfigParam("je.cleaner.restartRetries",
                           0,           // min
                           1000,        // max
                           5,           // default
                           false,       // mutable
                           false);      // forReplication

    public static final IntConfigParam CLEANER_MIN_AGE =
        new IntConfigParam(EnvironmentConfig.CLEANER_MIN_AGE,
                           1,           // min
                           1000,        // max
                           2,           // default
                           true,        // mutable
                           false);      // forReplication

    /**
     * @deprecated in JE 6.3.
     */
    private final IntConfigParam CLEANER_CALC_RECENT_LN_SIZES =
        new IntConfigParam("je.cleaner.calc.recentLNSizes",
                           1,        // min
                           100,      // max
                           10,       // default
                           false,    // mutable
                           false);   // forReplication

    /**
     * @deprecated in JE 6.3.
     */
    private static final IntConfigParam CLEANER_CALC_MIN_UNCOUNTED_LNS =
        new IntConfigParam("je.cleaner.calc.minUncountedLNs",
                           0,        // min
                           1000000,  // max
                           1000,     // default
                           false,    // mutable
                           false);   // forReplication

    /**
     * @deprecated in JE 6.3.
     */
    private static final IntConfigParam CLEANER_CALC_INITIAL_ADJUSTMENTS =
        new IntConfigParam("je.cleaner.calc.initialAdjustments",
                           1,        // min
                           100,      // max
                           5,        // default
                           false,    // mutable
                           false);   // forReplication

    /**
     * @deprecated in JE 6.3.
     */
    private static final IntConfigParam CLEANER_CALC_MIN_PROBE_SKIP_FILES =
        new IntConfigParam("je.cleaner.calc.minProbeSkipFiles",
                           1,        // min
                           100,      // max
                           5,        // default
                           false,    // mutable
                           false);   // forReplication

    /**
     * @deprecated in JE 6.3.
     */
    private static final IntConfigParam CLEANER_CALC_MAX_PROBE_SKIP_FILES =
        new IntConfigParam("je.cleaner.calc.maxProbeSkipFiles",
                           1,        // min
                           100,      // max
                           20,       // default
                           false,    // mutable
                           false);   // forReplication

    /**
     * @deprecated
     * Retained here only to avoid errors in old je.properties files.
     */
    private static final BooleanConfigParam CLEANER_CLUSTER =
        new BooleanConfigParam("je.cleaner.cluster",
                               false,               // default
                               true,                // mutable
                               false);              // forReplication

    /**
     * @deprecated
     * Retained here only to avoid errors in old je.properties files.
     */
    private static final BooleanConfigParam CLEANER_CLUSTER_ALL =
        new BooleanConfigParam("je.cleaner.clusterAll",
                               false,              // default
                               true,               // mutable
                               false);             // forReplication

    /**
     * @deprecated
     * Retained here only to avoid errors in old je.properties files.
     */
    public static final IntConfigParam CLEANER_MAX_BATCH_FILES =
        new IntConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES,
                           0,         // min
                           100000,    // max
                           0,         // default
                           true,      // mutable
                           false);    // forReplication

    public static final IntConfigParam CLEANER_READ_SIZE =
        new IntConfigParam(EnvironmentConfig.CLEANER_READ_SIZE,
                           128,    // min
                           null,   // max
                           0,      // default
                           true,   // mutable
                           false); // forReplication

    /**
     * DiskOrderedScan
     */
    public static final DurationConfigParam DOS_PRODUCER_QUEUE_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.DOS_PRODUCER_QUEUE_TIMEOUT,
                                "0",                // min
                                "75 min",           // max
                                "10 seconds",       // default
                                true,               // mutable
                                false);             // forReplication

    /**
     * Not part of public API.
     *
     * If true, the cleaner tracks and stores detailed information that is used
     * to decrease the cost of cleaning.
     */
    public static final BooleanConfigParam CLEANER_TRACK_DETAIL =
        new BooleanConfigParam("je.cleaner.trackDetail",
                               true,          // default
                               false,         // mutable
                               false);        // forReplication

    /**
     * Not part of public API.
     *
     * If true (the default), data expires gradually over an hour or day time
     * period, preventing spikes in cleaning after hour/day boundaries. This
     * might be set to false for debugging.
     */
    public static final BooleanConfigParam CLEANER_GRADUAL_EXPIRATION =
        new BooleanConfigParam("je.cleaner.gradualExpiration",
                               true,          // default
                               true,          // mutable
                               false);        // forReplication

    /**
     * Not part of public API.
     *
     * Used to determine when to perform two-pass cleaning.
     *
     * @see #CLEANER_TWO_PASS_THRESHOLD
     * @see EnvironmentStats#getNCleanerTwoPassRuns()
     */
    public static final IntConfigParam CLEANER_TWO_PASS_GAP =
        new IntConfigParam("je.cleaner.twoPassGap",
                       1,      // min
                       100,    // max
                       10,     // default
                       true,   // mutable
                       false); // forReplication

    /**
     * Not part of public API.
     *
     * Used to determine when to perform two-pass cleaning.
     *
     * Two-pass cleaning is used when:
     * 1. the file's maximum utilization is greater than
     *    {@link #CLEANER_TWO_PASS_THRESHOLD}, and
     * 2. the difference between the minimum and maximum utilization of a file
     *    is greater than or equal to than
     *    {@link #CLEANER_TWO_PASS_GAP}.
     *
     * After pass one, pass two is performed only if the recalculated
     * utilization is greater than or equal to
     * {@link #CLEANER_TWO_PASS_THRESHOLD}.
     *
     * When this parameter is zero, the default, the value used is
     * {@link EnvironmentConfig#CLEANER_MIN_UTILIZATION} minus five.
     *
     * @see EnvironmentStats#getNCleanerTwoPassRuns()
     */
    public static final IntConfigParam CLEANER_TWO_PASS_THRESHOLD =
        new IntConfigParam("je.cleaner.twoPassThreshold",
                       0,      // min
                       100,    // max
                       0,      // default
                       true,   // mutable
                       false); // forReplication

    public static final IntConfigParam CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE =
    new IntConfigParam(EnvironmentConfig.CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE,
                       1,      // min
                       90,     // max
                       2,      // default
                       true,   // mutable
                       false); // forReplication

    /**
     * Not part of public API, since it applies to a very old bug.
     *
     * If true, detail information is discarded that was added by earlier
     * versions of JE (specifically 2.0.42 and 2.0.54) if it may be invalid.
     * This may be set to false for increased performance when those version of
     * JE were used but LockMode.RMW was never used.
     */
    public static final BooleanConfigParam CLEANER_RMW_FIX =
        new BooleanConfigParam("je.cleaner.rmwFix",
                               true,          // default
                               false,         // mutable
                               false);        // forReplication

    public static final ConfigParam CLEANER_FORCE_CLEAN_FILES =
        new ConfigParam(EnvironmentConfig.CLEANER_FORCE_CLEAN_FILES,
                        "",                  // default
                        true,                // mutable
                        false);              // forReplication

    public static final IntConfigParam CLEANER_UPGRADE_TO_LOG_VERSION =
        new IntConfigParam(EnvironmentConfig.CLEANER_UPGRADE_TO_LOG_VERSION,
                           -1,     // min
                           null,   // max
                           0,      // default
                           false,  // mutable
                           false); // forReplication

    public static final IntConfigParam CLEANER_THREADS =
        new IntConfigParam(EnvironmentConfig.CLEANER_THREADS,
                           1,      // min
                           null,   // max
                           1,      // default
                           true,   // mutable
                           false); // forReplication

    public static final IntConfigParam CLEANER_LOOK_AHEAD_CACHE_SIZE =
        new IntConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
                           0,      // min
                           null,   // max
                           8192,   // default
                           true,   // mutable
                           false); // forReplication

    /**
     * @deprecated
     * Retained here only to avoid errors in old je.properties files.
     */
    private static final BooleanConfigParam
        CLEANER_FOREGROUND_PROACTIVE_MIGRATION = new BooleanConfigParam(
            EnvironmentConfig.CLEANER_FOREGROUND_PROACTIVE_MIGRATION,
            false,                // default
            true,                 // mutable
            false);               // forReplication

    /**
     * @deprecated
     * Retained here only to avoid errors in old je.properties files.
     */
    public static final BooleanConfigParam
        CLEANER_BACKGROUND_PROACTIVE_MIGRATION = new BooleanConfigParam(
            EnvironmentConfig.CLEANER_BACKGROUND_PROACTIVE_MIGRATION,
            false,                // default
            true,                 // mutable
            false);               // forReplication

    /**
     * @deprecated
     * Retained here only to avoid errors in old je.properties files.
     */
    private static final BooleanConfigParam CLEANER_LAZY_MIGRATION =
        new BooleanConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION,
                           false,             // default
                           true,              // mutable
                           false);            // forReplication

    /* Processed entry count after which we clear the database cache. */
    public static final IntConfigParam ENV_DB_CACHE_CLEAR_COUNT =
        new IntConfigParam("je.env.dbCacheClearCount",
                           1,      // min
                           null,   // max
                           100,    // default
                           true,   // mutable
                           false); // forReplication

    /*
     * Transactions
     */
    public static final IntConfigParam N_LOCK_TABLES =
        new IntConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES,
                           1,      // min
                           32767,  // max
                           1,      // default
                           false,  // mutable
                           false); // forReplication

    public static final DurationConfigParam LOCK_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.LOCK_TIMEOUT,
                                null,              // min
                                "75 min",          // max
                                "500 ms",          // default
                                false,             // mutable
                                false);            // forReplication
    
    /* "mutable" aims to do some test in DeadlockStress.java. */
    public static final BooleanConfigParam LOCK_DEADLOCK_DETECT =
        new BooleanConfigParam(EnvironmentConfig.LOCK_DEADLOCK_DETECT,
            true,               // default
            true,              // mutable
            false);             // forReplication

    public static final DurationConfigParam LOCK_DEADLOCK_DETECT_DELAY =
        new DurationConfigParam(EnvironmentConfig.LOCK_DEADLOCK_DETECT_DELAY,
            "0",               // min
            "75 min",          // max
            "0",               // default
            false,             // mutable
            false);            // forReplication

    public static final BooleanConfigParam LOCK_OLD_LOCK_EXCEPTIONS =
        new BooleanConfigParam(EnvironmentConfig.LOCK_OLD_LOCK_EXCEPTIONS,
                               false,              // default
                               false,              // mutable
                               false);             // forReplication

    public static final DurationConfigParam TXN_TIMEOUT =
        new DurationConfigParam(EnvironmentConfig.TXN_TIMEOUT,
                                null,              // min
                                "75 min",          // max
                                "0",               // default
                                false,             // mutable
                                false);            // forReplication

    public static final BooleanConfigParam TXN_SERIALIZABLE_ISOLATION =
        new BooleanConfigParam(EnvironmentConfig.TXN_SERIALIZABLE_ISOLATION,
                               false,              // default
                               false,              // mutable
                               false);             // forReplication

    public static final BooleanConfigParam TXN_DEADLOCK_STACK_TRACE =
        new BooleanConfigParam(EnvironmentConfig.TXN_DEADLOCK_STACK_TRACE,
                               false,              // default
                               true,               // mutable
                               false);             // forReplication

    public static final BooleanConfigParam TXN_DUMPLOCKS =
        new BooleanConfigParam(EnvironmentConfig.TXN_DUMP_LOCKS,
                               false,              // default
                               true,               // mutable
                               false);             // forReplication

    /*
     * If true, exceptions and critical cleaner and recovery event tracing
     * is written into the .jdb files.
     */
    public static final BooleanConfigParam JE_LOGGING_DBLOG =
        new BooleanConfigParam("je.env.logTrace",
                               true,               // default
                               false,              // mutable
                               false);             // forReplication

    /*
     * The level for JE ConsoleHandler.
     */
    public static final ConfigParam JE_CONSOLE_LEVEL =
        new ConfigParam(EnvironmentConfig.CONSOLE_LOGGING_LEVEL,
                        "OFF",                     // default
                        true,                      // mutable
                        false) {                   // for Replication

            @Override
            public void validateValue(String level)
                throws NullPointerException, IllegalArgumentException {

                /* Parse the level. */
                Level.parse(level);
            }
    };

    /*
     * The level for JE FileHandler.
     */
    public static final ConfigParam JE_FILE_LEVEL =
        new ConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL,
                        "INFO",                    // default
                        true,                      // mutable
                        false) {                   // for Replication

            @Override
            public void validateValue(String level)
                throws NullPointerException, IllegalArgumentException {

                /* Parse the level. */
                Level.parse(level);
            }
    };

    /*
     * The default below for JE_DURABILITY is currently null to avoid mixed
     * mode durability API exceptions. Once the "sync" API has been removed, we
     * can provide a default like: sync,sync,simple majority that's compatible
     * with the current sync default stand alone behavior and is safe, though
     * not the best performing setup, wrt HA.
     */
    public static final ConfigParam JE_DURABILITY =
        new ConfigParam(EnvironmentConfig.TXN_DURABILITY,
                        null,                  // default
                        true,                  // mutable
                        false) {               // forReplication

        @Override
        public void validateValue(String durabilityString)
            throws IllegalArgumentException {
            // Parse the string to determine whether it's valid
            Durability.parse(durabilityString);
        }
    };

    /**
     * If environment startup exceeds this duration, startup statistics are
     * logged and can be found in the je.info file.
     */
    public static final DurationConfigParam STARTUP_DUMP_THRESHOLD =
        new DurationConfigParam(EnvironmentConfig.STARTUP_DUMP_THRESHOLD,
                                "0",               // min
                                null,              // max
                                "5 min",           // default
                                false,             // mutable
                                false);            // forReplication
    public static final BooleanConfigParam STATS_COLLECT =
            new BooleanConfigParam(EnvironmentConfig.STATS_COLLECT,
                    true,         // default
                    true,         // mutable
                    false);        // forReplication

    public static final IntConfigParam STATS_FILE_ROW_COUNT =
            new IntConfigParam(EnvironmentConfig.STATS_FILE_ROW_COUNT,
                               2,                 // min
                               Integer.MAX_VALUE, // max
                               1440,              // default
                               true,              // mutable
                               false);            // forReplication

    public static final IntConfigParam STATS_MAX_FILES =
            new IntConfigParam(EnvironmentConfig.STATS_MAX_FILES,
                               1,                 // min
                               Integer.MAX_VALUE, // max
                               10,                // default
                               true,              // mutable
                               false);            // forReplication

    public static final DurationConfigParam STATS_COLLECT_INTERVAL =
            new DurationConfigParam(EnvironmentConfig.STATS_COLLECT_INTERVAL,
                                    "1 s",           // min
                                    null,            // max
                                    "1 min",           // default
                                    true,            // mutable
                                    false);          // forReplication

    public static final ConfigParam STATS_FILE_DIRECTORY =
            new ConfigParam(EnvironmentConfig.STATS_FILE_DIRECTORY,
                            "",                  // default
                            false,               // mutable
                            false);              // forReplication

    /*
     * Replication params are in com.sleepycat.je.rep.impl.RepParams
     */

    /*
     * Add a configuration parameter to the set supported by an environment.
     */
    public static void addSupportedParam(ConfigParam param) {
        SUPPORTED_PARAMS.put(param.getName(), param);
    }
}
