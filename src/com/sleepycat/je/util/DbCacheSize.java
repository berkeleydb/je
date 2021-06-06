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

package com.sleepycat.je.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.PreloadStatus;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.evictor.OffHeapCache;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbCacheSizeRepEnv;
import com.sleepycat.util.RuntimeExceptionWrapper;

/**
 * Estimates the in-memory cache size needed to hold a specified data set.
 *
 * To get an estimate of the in-memory footprint for a given database,
 * specify the number of records and database characteristics and DbCacheSize
 * will return an estimate of the cache size required for holding the
 * database in memory. Based on this information a JE main cache size can be
 * chosen and then configured using {@link EnvironmentConfig#setCacheSize} or
 * using the {@link EnvironmentConfig#MAX_MEMORY} property. An off-heap cache
 * may also be optionally configured using {@link
 * EnvironmentConfig#setOffHeapCacheSize} or using the {@link
 * EnvironmentConfig#MAX_OFF_HEAP_MEMORY} property.
 *
 * <h4>Importance of the JE Cache</h4>
 *
 * The JE cache is not an optional cache. It is used to hold the metadata for
 * accessing JE data.  In fact the JE cache size is probably the most critical
 * factor to JE performance, since Btree nodes will have to be fetched during a
 * database read or write operation if they are not in cache. During a single
 * read or write operation, at each level of the Btree that a fetch is
 * necessary, an IO may be necessary at a different disk location for each
 * fetch.  In addition, if internal nodes (INs) are not in cache, then write
 * operations will cause additional copies of the INs to be written to storage,
 * as modified INs are moved out of the cache to make room for other parts of
 * the Btree during subsequent operations.  This additional fetching and
 * writing means that sizing the cache too small to hold the INs will result in
 * lower operation performance.
 * <p>
 * For best performance, all Btree nodes should fit in the JE cache, including
 * leaf nodes (LNs), which hold the record data, and INs, which hold record
 * keys and other metadata.  However, because system memory is limited, it is
 * sometimes necessary to size the cache to hold all or at least most INs, but
 * not the LNs.  This utility estimates the size necessary to hold only INs,
 * and the size to hold INs and LNs.
 * <p>
 * In addition, a common problem with large caches is that Java GC overhead
 * can become significant. When a Btree node is evicted from the JE main
 * cache based on JE's LRU algorithm, typically the node will have been
 * resident in the JVM heap for an extended period of time, and will be
 * expensive to GC. Therefore, when most or all LNs do <em>not</em> fit in
 * the main cache, using {@link CacheMode#EVICT_LN} can be beneficial to
 * reduce the Java GC cost of collecting the LNs as they are moved out of the
 * main cache. With EVICT_LN, the LNs only reside in the JVM heap for a short
 * period and are cheap to collect. A recommended approach is to size the JE
 * main cache to hold only INs, and size the Java heap to hold that amount plus
 * the amount needed for GC working space and application objects, leaving
 * any additional memory for use by the file system cache or the off-heap
 * cache. Tests show this approach results in lower GC overhead and more
 * predictable latency.
 * <p>
 * Another issue is that 64-bit JVMs store object references using less space
 * when the heap size is slightly less than 32GiB. When the heap size is 32GiB
 * or more, object references are larger and less data can be cached per GiB of
 * memory. This JVM feature is enabled with the
 * <a href="http://download.oracle.com/javase/7/docs/technotes/guides/vm/performance-enhancements-7.html#compressedOop">Compressed Oops</a>
 * (<code>-XX:+UseCompressedOops</code>) option, although in modern JVMs it is
 * on by default. Because of this factor, and because Java GC overhead is
 * usually higher with larger heaps, a maximum heap size slightly less than
 * 32GiB is recommended, along with Compressed Oops option.
 * <p>
 * Of course, the JE main cache size must be less than the heap size since the
 * main cache is stored in the heap. In fact, around 30% of free space should
 * normally be reserved in the heap for use by Java GC, to avoid high GC
 * overheads. For example, if the application uses roughly 2GiB of the heap,
 * then with a 32GiB heap the JE main cache should normally be no more than
 * 20GiB.
 * <p>
 * As of JE 6.4, an optional off-heap cache may be configured in addition to
 * the main JE cache. See {@link EnvironmentConfig#setOffHeapCacheSize} for
 * information about the trade-offs in using an off-heap cache. When the
 * {@code -offheap} argument is specified, this utility displays sizing
 * information for both the main and off-heap caches. The portion of the data
 * set that fits in the main cache, and the off-heap size needed to hold the
 * rest of the data set, will be shown. The main cache size can be specified
 * with the {@code -maincache} argument, or is implied to be the amount needed
 * to hold all internal nodes if this argument is omitted. Omitting this
 * argument is appropriate when {@link CacheMode#EVICT_LN} is used, since only
 * internal nodes will be stored in the main cache.
 * <p>
 * To reduce Java GC overhead, sometimes a small main cache is used along
 * with an off-heap cache. Note that it is important that the size the main
 * cache is at least large enough to hold all the upper INs (the INs at level
 * 2 and above). This is because the off-heap cache does not contain upper
 * INs, it only contains LNs and bottom internal nodes (BINs). When a level 2
 * IN is evicted from the main cache, its children (BINs and LNs) in the
 * off-heap cache, if any, must also be evicted, which can be undesirable,
 * especially if the off-heap cache is not full. This utility displays the
 * main cache size needed to hold all upper INs, and displays a warning if
 * this is smaller than the main cache size specified.
 *
 * <h4>Estimating the JE Cache Size</h4>
 *
 * Estimating JE in-memory sizes is not straightforward for several reasons.
 * There is some fixed overhead for each Btree internal node, so fanout
 * (maximum number of child entries per parent node) and degree of node
 * sparseness impacts memory consumption. In addition, JE uses various compact
 * in-memory representations that depend on key sizes, data sizes, key
 * prefixing, how many child nodes are resident, etc. The physical proximity
 * of node children also allows compaction of child physical address values.
 * <p>
 * Therefore, when running this utility it is important to specify all {@link
 * EnvironmentConfig} and {@link DatabaseConfig} settings that will be used in
 * a production system.  The {@link EnvironmentConfig} settings are specified
 * by command line options for each property, using the same names as the
 * {@link EnvironmentConfig} parameter name values.  For example, {@link
 * EnvironmentConfig#LOG_FILE_MAX}, which influences the amount of memory used
 * to store physical record addresses, can be specified on the command line as:
 * <p>
 * {@code -je.log.fileMax LENGTH}
 * <p>
 * To be sure that this utility takes into account all relevant settings,
 * especially as the utility is enhanced in future versions, it is best to
 * specify all {@link EnvironmentConfig} settings used by the application.
 * <p>
 * The {@link DatabaseConfig} settings are specified using command line options
 * defined by this utility.
 * <ul>
 *   <li>{@code -nodemax ENTRIES} corresponds to {@link
 *   DatabaseConfig#setNodeMaxEntries}.</li>
 *   <li>{@code -duplicates} corresponds to passing true to {@link
 *   DatabaseConfig#setSortedDuplicates}.  Note that duplicates are configured
 *   for DPL MANY_TO_ONE and MANY_TO_MANY secondary indices.</li>
 *   <li>{@code -keyprefix LENGTH} corresponds to passing true {@link
 *   DatabaseConfig#setKeyPrefixing}.  Note that key prefixing is always used
 *   when duplicates are configured.</li>
 * </ul>
 * <p>
 * This utility estimates the JE cache size by creating an in-memory
 * Environment and Database.  In addition to the size of the Database, the
 * minimum overhead for the Environment is output.  The Environment overhead
 * shown is likely to be smaller than actually needed because it doesn't take
 * into account use of memory by JE daemon threads (cleaner, checkpointer, etc)
 * the memory used for locks that are held by application operations and
 * transactions, the memory for HA network connections, etc. An additional
 * amount should be added to account for these factors.
 * <p>
 * This utility estimates the cache size for a single JE Database, or a logical
 * table spread across multiple databases (as in the case of Oracle NoSQL DB,
 * for example).  To estimate the size for multiple databases/tables with
 * different configuration parameters or different key and data sizes, run
 * this utility for each database/table and sum the sizes. If you are summing
 * multiple runs for multiple databases/tables that are opened in a single
 * Environment, the overhead size for the Environment should only be added once.
 * <p>
 * In some applications with databases/tables having variable key and data
 * sizes, it may be difficult to determine the key and data size input
 * parameters for this utility.  If a representative data set can be created,
 * one approach is to use the {@link DbPrintLog} utility with the {@code -S}
 * option to find the average key and data size for all databases/tables, and
 * use these values as input parameters, as if there were only a single
 * database/tables.  With this approach, it is important that the {@code
 * DatabaseConfig} parameters are the same, or at least similar, for all
 * databases/tables.
 *
 * <h4>Key Prefixing and Compaction</h4>
 *
 * Key prefixing deserves special consideration.  It can significantly reduce
 * the size of the cache and is generally recommended; however, the benefit can
 * be difficult to predict.  Key prefixing, in turn, impacts the benefits of
 * key compaction, and the use of the {@link
 * EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH} parameter.
 * <p>
 * For a given data set, the impact of key prefixing is determined by how many
 * leading bytes are in common for the keys in a single bottom internal node
 * (BIN).  For example, if keys are assigned sequentially as long (8 byte)
 * integers, and the {@link DatabaseConfig#setNodeMaxEntries maximum entries
 * per node} is 128 (the default value) then 6 or 7 of the 8 bytes of the key
 * will have a common prefix in each BIN.  Of course, when records are deleted,
 * the number of prefixed bytes may be reduced because the range of key values
 * in a BIN will be larger.  For this example we will assume that, on average,
 * 5 bytes in each BIN are a common prefix leaving 3 bytes per key that are
 * unprefixed.
 * <p>
 * Key compaction is applied when the number of unprefixed bytes is less than a
 * configured value; see {@link EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH}.
 * In the example, the 3 unprefixed bytes per key is less than the default used
 * for key compaction (16 bytes).  This means that each key will use 16 bytes
 * of memory, in addition to the amount used for the prefix for each BIN.  The
 * per-key overhead could be reduced by changing the {@code
 * TREE_COMPACT_MAX_KEY_LENGTH} parameter to a smaller value, but care should
 * be taken to ensure the compaction will be effective as keys are inserted and
 * deleted over time.
 * <p>
 * Because key prefixing depends so much on the application key format and the
 * way keys are assigned, the number of expected prefix bytes must be estimated
 * by the user and specified to DbCacheSize using the {@code -keyprefix}
 * argument.
 *
 * <h4>Key Prefixing and Duplicates</h4>
 * 
 * When {@link DatabaseConfig#setSortedDuplicates duplicates} are configured
 * for a Database (including DPL MANY_TO_ONE and MANY_TO_MANY secondary
 * indices), key prefixing is always used.  This is because the internal key in
 * a duplicates database BIN is formed by concatenating the user-specified key
 * and data.  In secondary databases with duplicates configured, the data is
 * the primary key, so the internal key is the concatenation of the secondary
 * key and the primary key.
 * <p>
 * Key prefixing is always used for duplicates databases because prefixing is
 * necessary to store keys efficiently.  When the number of duplicates per
 * unique user-specified key is more than the number of entries per BIN, the
 * entire user-specified key will be the common prefix.
 * <p>
 * For example, a database that stores user information may use email address
 * as the primary key and zip code as a secondary key.  The secondary index
 * database will be a duplicates database, and the internal key stored in the
 * BINs will be a two part key containing zip code followed by email address.
 * If on average there are more users per zip code than the number of entries
 * in a BIN, then the key prefix will normally be at least as long as the zip
 * code key.  If there are less (more than one zip code appears in each BIN),
 * then the prefix will be shorter than the zip code key.
 * <p>
 * It is also possible for the key prefix to be larger than the secondary key.
 * If for one secondary key value (one zip code) there are a large number of
 * primary keys (email addresses), then a single BIN may contain concatenated
 * keys that all have the same secondary key (same zip code) and have primary
 * keys (email addresses) that all have some number of prefix bytes in common.
 * Therefore, when duplicates are specified it is possible to specify a prefix
 * size that is larger than the key size.
 *
 * <h4>Small Data Sizes and Embedded LNs</h4>
 *
 * Another special data representation involves small data sizes. When the
 * data size of a record is less than or equal to {@link
 * EnvironmentConfig#TREE_MAX_EMBEDDED_LN} (16 bytes, by default), the data
 * is stored (embedded) in the BIN, and the LN is not stored in cache at all.
 * This increases the size needed to hold all INs in cache, but it decreases
 * the size needed to hold the complete data set. If the data size specified
 * when running this utility is less than or equal to TREE_MAX_EMBEDDED_LN,
 * the size displayed for holding INs only will be the same as the size
 * displayed for holdings INs and LNs.
 * <p>
 * See {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN} for information about
 * the trade-offs in using the embedded LNs feature.
 *
 * <h4>Record Versions and Oracle NoSQL Database</h4>
 *
 * This note applies only to when JE is used with Oracle NoSQL DB.  In Oracle
 * NoSQL DB, an internal JE environment configuration parameter is always
 * used: {@code -je.rep.preserveRecordVersion true}.  This allows using record
 * versions in operations such as "put if version", "delete if version", etc.
 * This feature performs best when the cache is sized large enough to hold the
 * record versions.
 * <p>
 * When using JE with Oracle NoSQL DB, always add {@code
 * -je.rep.preserveRecordVersion true} to the command line.  This ensures that
 * the cache sizes calculated are correct, and also outputs an additional line
 * showing how much memory is required to hold the internal nodes and record
 * versions (but not the leaf nodes).  This is the minimum recommended size
 * when the "... if version" operations are used.
 *
 * <h4>Running the DbCacheSize utility</h4>
 *
 * Usage:
 * <pre>
 * java { com.sleepycat.je.util.DbCacheSize |
 *        -jar je-&lt;version&gt;.jar DbCacheSize }
 *  -records COUNT
 *      # Total records (key/data pairs); required
 *  -key BYTES
 *      # Average key bytes per record; required
 *  [-data BYTES]
 *      # Average data bytes per record; if omitted no leaf
 *      # node sizes are included in the output; required with
 *      # -duplicates, and specifies the primary key length
 *  [-offheap]
 *      # Indicates that an off-heap cache will be used.
 *  [-maincache BYTES]
 *      # The size of the main cache (in the JVM heap).
 *      # The size of the off-heap cache displayed is the
 *      # additional amount needed to hold the data set.
 *      # If omitted, the main cache size is implied to
 *      # be the amount needed to hold all internal nodes.
 *      # Ignored if -offheap is not also specified.
 *  [-keyprefix BYTES]
 *      # Expected size of the prefix for the keys in each
 *      # BIN; default: key prefixing is not configured;
 *      # required with -duplicates
 *  [-nodemax ENTRIES]
 *      # Number of entries per Btree node; default: 128
 *  [-orderedinsertion]
 *      # Assume ordered insertions and no deletions, so BINs
 *      # are 100% full; default: unordered insertions and/or
 *      # deletions, BINs are 70% full
 *  [-duplicates]
 *      # Indicates that sorted duplicates are used, including
 *      # MANY_TO_ONE and MANY_TO_MANY secondary indices;
 *      # default: false
 *  [-ttl]
 *      # Indicates that TTL is used; default: false
 *  [-replicated]
 *      # Use a ReplicatedEnvironment; default: false
 *  [-ENV_PARAM_NAME VALUE]...
 *      # Any number of EnvironmentConfig parameters and
 *      # ReplicationConfig parameters (if -replicated)
 *  [-btreeinfo]
 *      # Outputs additional Btree information
 *  [-outputproperties]
 *      # Writes Java properties file to System.out
 * </pre>
 * <p>
 * You should run DbCacheSize on the same target platform and JVM for which you
 * are sizing the cache, as cache sizes will vary.  You may also need to
 * specify -d32 or -d64 depending on your target, if the default JVM mode is
 * not the same as the mode to be used in production.
 * <p>
 * To take full advantage of JE cache memory, it is strongly recommended that
 * <a href="http://download.oracle.com/javase/7/docs/technotes/guides/vm/performance-enhancements-7.html#compressedOop">compressed oops</a>
 * (<code>-XX:+UseCompressedOops</code>) is specified when a 64-bit JVM is used
 * and the maximum heap size is less than 32 GB.  As described in the
 * referenced documentation, compressed oops is sometimes the default JVM mode
 * even when it is not explicitly specified in the Java command.  However, if
 * compressed oops is desired then it <em>must</em> be explicitly specified in
 * the Java command when running DbCacheSize or a JE application.  If it is not
 * explicitly specified then JE will not aware of it, even if it is the JVM
 * default setting, and will not take it into account when calculating cache
 * memory sizes.
 * <p>
 * For example:
 * <pre>
 * $ java -jar je-X.Y.Z.jar DbCacheSize -records 554719 -key 16 -data 100
 *
 *  === Environment Cache Overhead ===
 *
 *  3,157,213 minimum bytes
 *
 * To account for JE daemon operation, record locks, HA network connections, etc,
 * a larger amount is needed in practice.
 *
 *  === Database Cache Size ===
 *
 *  Number of Bytes  Description
 *  ---------------  -----------
 *       23,933,736  Internal nodes only
 *      107,206,616  Internal nodes and leaf nodes
 * </pre>
 * <p>
 * This indicates that the minimum memory size to hold only the internal nodes
 * of the Database Btree is approximately 24MB. The maximum size to hold the
 * entire database, both internal nodes and data records, is approximately
 * 107MB.  To either of these amounts, at least 3MB (plus more for locks and
 * daemons) should be added to account for the environment overhead.
 * <p>
 * The following example adds the use of an off-heap cache, where the main
 * cache size is specified to be 30MB.
 * <pre>
 * $ java -jar je-X.Y.Z.jar DbCacheSize -records 554719 -key 16 -data 100 \
 *      -offheap -maincache 30000000
 *
 *  === Environment Cache Overhead ===
 *
 *  5,205,309 minimum bytes
 *
 * To account for JE daemon operation, record locks, HA network connections, etc,
 * a larger amount is needed in practice.
 *
 *  === Database Cache Size ===
 *
 *  Number of Bytes  Description
 *  ---------------  -----------
 *       23,933,736  Internal nodes only: MAIN cache
 *                0  Internal nodes only: OFF-HEAP cache
 *       24,794,691  Internal nodes and leaf nodes: MAIN cache
 *       70,463,604  Internal nodes and leaf nodes: OFF-HEAP cache
 * </pre>
 * There are several things of interest in the output.
 * <ul>
 *     <li>The environment overhead is larger because of memory used for the
 *     off-heap LRU.</li>
 *     <li>To cache only internal nodes, an off-heap cache is not needed since
 *     the internal nodes take around 24MB, which when added to the 5MB
 *     overhead is less than the 30MB main cache specified. This is why the
 *     number of bytes on the second line is zero.</li>
 *     <li>To cache all nodes, the main cache size specified should be used
 *     (25MB added to the 5MB overhead is 30MB), and an off-heap cache of
 *     around 71MB should be configured.</li>
 * </ul>
 *
 * <h4>Output Properties</h4>
 *
 * <p>
 * When {@code -outputproperties} is specified, a list of properties in Java
 * properties file format will be written to System.out, instead of the output
 * shown above. The properties and their meanings are listed below.
 * <ul>
 *     <li>The following properties are always output (except allNodes, see
 *     below). They describe the estimated size of the main cache.
 *     <ul>
 *         <li><strong>overhead</strong>: The environment overhead, as shown
 *         under Environment Cache Overhead above.</li>
 *         <li><strong>internalNodes</strong>: The Btree size in the main
 *         cache for holding the internal nodes. This is the "Internal nodes
 *         only" line above (followed by "MAIN cache" when {@code -offheap} is
 *         specified).</li>
 *         <li><strong>internalNodesAndVersions</strong>: The Btree size needed
 *         to hold the internal nodes and record versions in the main cache.
 *         This value is zero when {@code -offheap} is specified; currently JE
 *         does not cache record versions off-heap unless their associated LNs
 *         are also cached off-heap, so there is no way to calculate this
 *         property.</li>
 *         <li><strong>allNodes</strong>: The Btree size in the main cache
 *         needed to hold all nodes. This is the "Internal nodes and leaf
 *         nodes" line above (followed by "MAIN cache" when {@code -offheap} is
 *         specified). This property is not output unless {@code -data} is
 *         specified.</li>
 *     </ul>
 *     <li>The following properties are output only when {@code -offheap} is
 *     specified. They describe the estimated size of the off-heap cache.
 *     <ul>
 *         <li><strong>minMainCache</strong>: The minimum size of the main
 *         cache needed to hold all upper INs. When the {@code -maincache}
 *         value specified is less than this minimum, not all internal nodes
 *         can be cached. See the discussion further above.</li>
 *         <li><strong>offHeapInternalNodes</strong>: The size of the off-heap
 *         cache needed to hold the internal nodes. This is the "Internal nodes
 *         only: OFF_HEAP cache" line above.</li>
 *         <li><strong>offHeapAllNodes</strong>: The size of the off-heap cache
 *         needed to hold all nodes. This is the "Internal nodes and leaf
 *         nodes: OFF_HEAP cache" line above. This property is not output
 *         unless {@code -data} is specified.</li>
 *     </ul>
 *     <li>The following properties are deprecated but are output for
 *     compatibility with earlier releases.
 *     <ul>
 *         <li> minInternalNodes, maxInternalNodes, minAllNodes, and (when
 *         {@code -data} is specified) maxAllNodes</li>
 *     </ul>
 * </ul>
 *
 * @see EnvironmentConfig#setCacheSize
 * @see EnvironmentConfig#setOffHeapCacheSize
 * @see CacheMode
 *
 * @see <a href="../EnvironmentStats.html#cacheSizing">Cache Statistics:
 * Sizing</a>
 */
public class DbCacheSize {

    /*
     * Undocumented command line options, used for comparing calculated to
     * actual cache sizes during testing.
     *
     *  [-measure]
     *      # Causes main program to write a database to find
     *      # the actual cache size; default: do not measure;
     *      # without -data, measures internal nodes only
     *
     * Only use -measure without -orderedinsertion when record count is 100k or
     * less, to avoid endless attempts to find an unused key value via random
     * number generation.  Also note that measured amounts will be slightly
     * less than calculated amounts because the number of prefix bytes is
     * larger for smaller key values, which are sequential integers from zero
     * to max records minus one.
     */

    private static final NumberFormat INT_FORMAT =
        NumberFormat.getIntegerInstance();

    private static final String MAIN_HEADER =
        "   Number of Bytes  Description\n" +
        "   ---------------  -----------";
    //   123456789012345678
    //                     12
    private static final int MIN_COLUMN_WIDTH = 18;
    private static final String COLUMN_SEPARATOR = "  ";

    /* IN density for non-ordered insertion. */
    private static final int DEFAULT_DENSITY = 70;
    /* IN density for ordered insertion. */
    private static final int ORDERED_DENSITY = 100;

    /* Parameters. */
    private final EnvironmentConfig envConfig = new EnvironmentConfig();
    private final Map<String, String> repParams = new HashMap<>();
    private long records = 0;
    private int keySize = 0;
    private int dataSize = -1;
    private boolean offHeapCache = false;
    private boolean assumeEvictLN = false;
    private long mainCacheSize = 0;
    private long mainDataSize = 0;
    private int nodeMaxEntries = 128;
    private int binMaxEntries = -1;
    private int keyPrefix = 0;
    private boolean orderedInsertion = false;
    private boolean duplicates = false;
    private boolean replicated = false;
    private boolean useTTL = false;
    private boolean outputProperties = false;
    private boolean doMeasure = false;
    private boolean btreeInfo = false;

    /* Calculated values. */
    private long envOverhead;
    private long uinWithTargets;
    private long uinNoTargets;
    private long uinOffHeapBINIds;
    private long binNoLNsOrVLSNs;
    private long binNoLNsWithVLSNs;
    private long binWithLNsAndVLSNs;
    private long binOffHeapWithLNIds;
    private long binOffHeapNoLNIds;
    private long binOffHeapLNs;
    private long binOffHeapLNIds;
    private long mainMinDataSize;
    private long mainNoLNsOrVLSNs;
    private long mainNoLNsWithVLSNs;
    private long mainWithLNsAndVLSNs;
    private long offHeapNoLNsOrVLSNs;
    private long offHeapWithLNsAndVLSNs;
    private long nMainBINsNoLNsOrVLSNs;
    private long nMainBINsWithLNsAndVLSNs;
    private long nMainLNsWithLNsAndVLSNs;
    private long measuredMainNoLNsOrVLSNs;
    private long measuredMainNoLNsWithVLSNs;
    private long measuredMainWithLNsAndVLSNs;
    private long measuredOffHeapNoLNsOrVLSNs;
    private long measuredOffHeapWithLNsAndVLSNs;
    private long preloadMainNoLNsOrVLSNs;
    private long preloadMainNoLNsWithVLSNs;
    private long preloadMainWithLNsAndVLSNs;
    private int nodeAvg;
    private int binAvg;
    private int btreeLevels;
    private long nBinNodes;
    private long nUinNodes;
    private long nLevel2Nodes;

    private File tempDir;

    DbCacheSize() {
    }

    void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i += 1) {
            String name = args[i];
            String val = null;
            if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                i += 1;
                val = args[i];
            }
            if (name.equals("-records")) {
                if (val == null) {
                    usage("No value after -records");
                }
                try {
                    records = Long.parseLong(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (records <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-key")) {
                if (val == null) {
                    usage("No value after -key");
                }
                try {
                    keySize = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (keySize <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-data")) {
                if (val == null) {
                    usage("No value after -data");
                }
                try {
                    dataSize = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (dataSize < 0) {
                    usage(val + " is not a non-negative integer");
                }
            } else if (name.equals("-offheap")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                offHeapCache = true;
            } else if (name.equals("-maincache")) {
                if (val == null) {
                    usage("No value after -maincache");
                }
                try {
                    mainCacheSize = Long.parseLong(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (mainCacheSize <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-keyprefix")) {
                if (val == null) {
                    usage("No value after -keyprefix");
                }
                try {
                    keyPrefix = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (keyPrefix < 0) {
                    usage(val + " is not a non-negative integer");
                }
            } else if (name.equals("-orderedinsertion")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                orderedInsertion = true;
            } else if (name.equals("-duplicates")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                duplicates = true;
            } else if (name.equals("-ttl")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                useTTL = true;
            } else if (name.equals("-replicated")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                replicated = true;
            } else if (name.equals("-nodemax")) {
                if (val == null) {
                    usage("No value after -nodemax");
                }
                try {
                    nodeMaxEntries = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (nodeMaxEntries <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-binmax")) {
                if (val == null) {
                    usage("No value after -binmax");
                }
                try {
                    binMaxEntries = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (binMaxEntries <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-density")) {
                usage
                    ("-density is no longer supported, see -orderedinsertion");
            } else if (name.equals("-overhead")) {
                usage("-overhead is no longer supported");
            } else if (name.startsWith("-je.")) {
                if (val == null) {
                    usage("No value after " + name);
                }
                if (name.startsWith("-je.rep.")) {
                    repParams.put(name.substring(1), val);
                } else {
                    envConfig.setConfigParam(name.substring(1), val);
                }
            } else if (name.equals("-measure")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                doMeasure = true;
            } else if (name.equals("-outputproperties")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                outputProperties = true;
            } else if (name.equals("-btreeinfo")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                btreeInfo = true;
            } else {
                usage("Unknown arg: " + name);
            }
        }

        if (records == 0) {
            usage("-records not specified");
        }
        if (keySize == 0) {
            usage("-key not specified");
        }
    }

    void cleanup() {
        if (tempDir != null) {
            emptyTempDir();
            tempDir.delete();
        }
    }

    long getMainNoLNsOrVLSNs() {
        return mainNoLNsOrVLSNs;
    }

    long getMainNoLNsWithVLSNs() {
        return mainNoLNsWithVLSNs;
    }

    long getOffHeapWithLNsAndVLSNs() {
        return offHeapWithLNsAndVLSNs;
    }

    long getOffHeapNoLNsOrVLSNs() {
        return offHeapNoLNsOrVLSNs;
    }

    long getMainWithLNsAndVLSNs() {
        return mainWithLNsAndVLSNs;
    }

    long getMeasuredMainNoLNsOrVLSNs() {
        return measuredMainNoLNsOrVLSNs;
    }

    long getMeasuredMainNoLNsWithVLSNs() {
        return measuredMainNoLNsWithVLSNs;
    }

    long getMeasuredMainWithLNsAndVLSNs() {
        return measuredMainWithLNsAndVLSNs;
    }

    long getMeasuredOffHeapNoLNsOrVLSNs() {
        return measuredOffHeapNoLNsOrVLSNs;
    }

    long getMeasuredOffHeapWithLNsAndVLSNs() {
        return measuredOffHeapWithLNsAndVLSNs;
    }

    long getPreloadMainNoLNsOrVLSNs() {
        return preloadMainNoLNsOrVLSNs;
    }

    long getPreloadMainNoLNsWithVLSNs() {
        return preloadMainNoLNsWithVLSNs;
    }

    long getPreloadMainWithLNsAndVLSNs() {
        return preloadMainWithLNsAndVLSNs;
    }

    /**
     * Runs DbCacheSize as a command line utility.
     * For command usage, see {@link DbCacheSize class description}.
     */
    public static void main(final String[] args)
        throws Throwable {

        final DbCacheSize dbCacheSize = new DbCacheSize();
        try {
            dbCacheSize.parseArgs(args);
            dbCacheSize.calculateCacheSizes();
            if (dbCacheSize.outputProperties) {
                dbCacheSize.printProperties(System.out);
            } else {
                dbCacheSize.printCacheSizes(System.out);
            }
            if (dbCacheSize.doMeasure) {
                dbCacheSize.measure(System.out);
            }
        } finally {
            dbCacheSize.cleanup();
        }
    }

    /**
     * Prints usage and calls System.exit.
     */
    private static void usage(final String msg) {

        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println
            ("usage:" +
             "\njava "  + CmdUtil.getJavaCommand(DbCacheSize.class) +
             "\n   -records <count>" +
             "\n      # Total records (key/data pairs); required" +
             "\n   -key <bytes> " +
             "\n      # Average key bytes per record; required" +
             "\n  [-data <bytes>]" +
             "\n      # Average data bytes per record; if omitted no leaf" +
             "\n      # node sizes are included in the output; required with" +
             "\n      # -duplicates, and specifies the primary key length" +
             "\n  [-offheap]" +
             "\n      # Indicates that an off-heap cache will be used." +
             "\n  [-maincache <bytes>]" +
             "\n      # The size of the main cache (in the JVM heap)." +
             "\n      # The size of the off-heap cache displayed is the" +
             "\n      # additional amount needed to hold the data set." +
             "\n      # If omitted, the main cache size is implied to" +
             "\n      # be the amount needed to hold all internal nodes." +
             "\n      # Ignored if -offheap is not also specified." +
             "\n  [-keyprefix <bytes>]" +
             "\n      # Expected size of the prefix for the keys in each" +
             "\n      # BIN; default: zero, key prefixing is not configured;" +
             "\n      # required with -duplicates" +
             "\n  [-nodemax <entries>]" +
             "\n      # Number of entries per Btree node; default: 128" +
             "\n  [-orderedinsertion]" +
             "\n      # Assume ordered insertions and no deletions, so BINs" +
             "\n      # are 100% full; default: unordered insertions and/or" +
             "\n      # deletions, BINs are 70% full" +
             "\n  [-duplicates]" +
             "\n      # Indicates that sorted duplicates are used, including" +
             "\n      # MANY_TO_ONE and MANY_TO_MANY secondary indices;" +
             "\n      # default: false" +
             "\n  [-ttl]" +
             "\n      # Indicates that TTL is used; default: false" +
             "\n  [-replicated]" +
             "\n      # Use a ReplicatedEnvironment; default: false" +
             "\n  [-ENV_PARAM_NAME VALUE]..." +
             "\n      # Any number of EnvironmentConfig parameters and" +
             "\n      # ReplicationConfig parameters (if -replicated)" +
             "\n  [-btreeinfo]" +
             "\n      # Outputs additional Btree information" +
             "\n  [-outputproperties]" +
             "\n      # Writes Java properties to System.out");

        System.exit(2);
    }

    /**
     * Calculates estimated cache sizes.
     */
    void calculateCacheSizes() {

        if (binMaxEntries <= 0) {
            binMaxEntries = nodeMaxEntries;
        }

        final Environment env = openCalcEnvironment(true);
        boolean success = false;
        try {
            IN.ACCUMULATED_LIMIT = 0;

            envOverhead = env.getStats(null).getCacheTotalBytes();

            if (offHeapCache) {

                assumeEvictLN = (mainCacheSize == 0);

                if (mainCacheSize > 0 &&
                    mainCacheSize - envOverhead <= 1024 * 1024) {

                    throw new IllegalArgumentException(
                        "The -maincache value must be at least 1 MiB larger" +
                        " than the environment overhead (" +
                        INT_FORMAT.format(envOverhead) + ')');
                }
            }

            final int density =
                orderedInsertion ? ORDERED_DENSITY : DEFAULT_DENSITY;

            nodeAvg = (nodeMaxEntries * density) / 100;
            binAvg = (binMaxEntries * density) / 100;

            calcTreeSizes(env);
            calcNNodes();
            calcMainCacheSizes();

            /*
             * With an off-heap cache, if all UINs don't fit in main then we
             * can't fit all internal nodes, much less all nodes, in both
             * caches. We adjust the number of records downward so all UINs do
             * fit in main (there is no point in configuring a cache that can
             * never be filled) and then recalculate the number of nodes.
             */
            if (offHeapCache) {

                if (mainCacheSize == 0) {
                    mainCacheSize = mainNoLNsOrVLSNs + envOverhead;
                }

                mainDataSize = mainCacheSize - envOverhead;
                mainMinDataSize = calcLevel2AndAboveSize();

                if (mainMinDataSize > mainDataSize) {
                    records *= ((double) mainDataSize) / mainMinDataSize;
                    calcNNodes();
                    calcMainCacheSizes();
                }

                calcOffHeapNoLNsOrVLSNs();
                calcOffHeapWithLNsAndVLSNs();
            }

            success = true;
        } finally {

            IN.ACCUMULATED_LIMIT = IN.ACCUMULATED_LIMIT_DEFAULT;

            /*
             * Do not propagate exception thrown by Environment.close if
             * another exception is currently in flight.
             */
            try {
                env.close();
            } catch (RuntimeException e) {
                if (success) {
                    throw e;
                }
            }
        }
    }

    private long calcLevel2AndAboveSize() {
        assert offHeapCache;

        return ((nUinNodes - nLevel2Nodes) * uinWithTargets) +
               (nLevel2Nodes * (uinNoTargets + uinOffHeapBINIds));
    }

    private void calcNNodes() {

        nBinNodes = (records + binAvg - 1) / binAvg;
        btreeLevels = 1;
        nUinNodes = 0;
        nLevel2Nodes = 0;

        for (long nodes = nBinNodes / nodeAvg;; nodes /= nodeAvg) {

            if (nodes == 0) {
                nodes = 1; // root
            }

            if (btreeLevels == 2) {
                assert nLevel2Nodes == 0;
                nLevel2Nodes = nodes;
            }

            nUinNodes += nodes;
            btreeLevels += 1;

            if (nodes == 1) {
                break;
            }
        }
    }

    /**
     * Calculates main cache sizes as if there were no off-heap cache. During
     * off-heap cache size calculations, these numbers may be revised.
     */
    private void calcMainCacheSizes() {

        final long mainUINs = nUinNodes * uinWithTargets;

        mainNoLNsOrVLSNs =
            (nBinNodes * binNoLNsOrVLSNs) + mainUINs;

        mainNoLNsWithVLSNs =
            (nBinNodes * binNoLNsWithVLSNs) + mainUINs;

        mainWithLNsAndVLSNs =
            (nBinNodes * binWithLNsAndVLSNs) + mainUINs;
    }

    private void calcOffHeapNoLNsOrVLSNs() {
        assert offHeapCache;

        mainNoLNsWithVLSNs = 0;

        /*
         * If all INs fit in main, then no off-heap cache is needed.
         */
        if (mainNoLNsOrVLSNs <= mainDataSize) {
            offHeapNoLNsOrVLSNs = 0;
            nMainBINsNoLNsOrVLSNs = nBinNodes;
            return;
        }

        mainNoLNsOrVLSNs = mainDataSize;

        /*
         * If not all BINs fit in main, then put as many BINs in main as
         * possible, and the rest off-heap.
         */
        final long mainSpare = (mainDataSize > calcLevel2AndAboveSize()) ?
            (mainDataSize - calcLevel2AndAboveSize()) : 0;

        final long nMainBINs = mainSpare / binNoLNsOrVLSNs;
        final long nOffHeapBins = nBinNodes - nMainBINs;

        offHeapNoLNsOrVLSNs = nOffHeapBins * binOffHeapNoLNIds;
        nMainBINsNoLNsOrVLSNs = nMainBINs;
    }

    private void calcOffHeapWithLNsAndVLSNs() {
        assert offHeapCache;

        /*
         * If everything fits in main, then no off-heap cache is needed.
         */
        if (mainWithLNsAndVLSNs <= mainDataSize) {
            offHeapWithLNsAndVLSNs = 0;
            nMainBINsWithLNsAndVLSNs = nBinNodes;
            nMainLNsWithLNsAndVLSNs = (binOffHeapLNs == 0) ? 0 : records;
            return;
        }

        mainWithLNsAndVLSNs = mainDataSize;

        /*
         * If LNs are not stored separately (they are embedded or duplicates
         * are configured), then only internal nodes are relevant.
         */
        if (binOffHeapLNs == 0) {
            offHeapWithLNsAndVLSNs = offHeapNoLNsOrVLSNs;
            nMainBINsWithLNsAndVLSNs = nMainBINsNoLNsOrVLSNs;
            nMainLNsWithLNsAndVLSNs = 0;
            return;
        }

        /*
         * If all BINs fit in main, then compute how many BINs will have main
         * LNs and how many off-heap LNs. The number that have main LNs is
         * the amount of main cache to spare (if all BINs had off-heap LNs)
         * divided by the added size required to hold the LNs in one BIN.
         */
        final long mainWithOffHeapLNIds =
            mainNoLNsOrVLSNs + (nBinNodes * binOffHeapLNIds);

        if (mainWithOffHeapLNIds <= mainDataSize) {

            final long mainSpare = (mainDataSize > mainNoLNsOrVLSNs) ?
                (mainDataSize - mainNoLNsOrVLSNs) : 0;

            final long nBINsWithMainLNs = mainSpare /
                (binWithLNsAndVLSNs - binNoLNsOrVLSNs);

            final long nBINsWithOffHeapLNs = nBinNodes - nBINsWithMainLNs;

            offHeapWithLNsAndVLSNs = nBINsWithOffHeapLNs * binOffHeapLNs;
            nMainBINsWithLNsAndVLSNs = nMainBINsNoLNsOrVLSNs;
            nMainLNsWithLNsAndVLSNs = nBINsWithMainLNs * nodeAvg;
            return;
        }

        /*
         * If not all BINs fit in main, then put as many BINs in main as
         * possible, and the rest off-heap. Put all LNs off-heap.
         */
        final long mainSpare = (mainDataSize > calcLevel2AndAboveSize()) ?
            (mainDataSize - calcLevel2AndAboveSize()) : 0;

        final long nMainBINs = mainSpare / (binNoLNsOrVLSNs + binOffHeapLNIds);
        final long nOffHeapBins = nBinNodes - nMainBINs;

        offHeapWithLNsAndVLSNs =
            (nOffHeapBins * binOffHeapWithLNIds) +
            (nBinNodes * binOffHeapLNs);

        nMainBINsWithLNsAndVLSNs = nMainBINs;
        nMainLNsWithLNsAndVLSNs = 0;
    }

    private void calcTreeSizes(final Environment env) {

        if (nodeMaxEntries != binMaxEntries) {
            throw new IllegalArgumentException(
                "-binmax not currently supported because a per-BIN max is" +
                " not implemented in the Btree, so we can't measure" +
                " an actual BIN node with the given -binmax value");
        }
        assert nodeAvg == binAvg;

        if (nodeAvg > 0xFFFF) {
            throw new IllegalArgumentException(
                "Entries per node (" + nodeAvg + ") is greater than 0xFFFF");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /*
         * Either a one or two byte key is used, depending on whether a single
         * byte can hold the key for nodeAvg entries.
         */
        final byte[] keyBytes = new byte[(nodeAvg <= 0xFF) ? 1 : 2];
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        final WriteOptions options = new WriteOptions();
        if (useTTL) {
            options.setTTL(30, TimeUnit.DAYS);
        }

        /* Insert nodeAvg records into a single BIN. */
        final Database db = openDatabase(env, true);
        for (int i = 0; i < nodeAvg; i += 1) {

            if (keyBytes.length == 1) {
                keyBytes[0] = (byte) i;
            } else {
                assert keyBytes.length == 2;
                keyBytes[0] = (byte) (i >> 8);
                keyBytes[1] = (byte) i;
            }

            setKeyData(keyBytes, keyPrefix, keyEntry, dataEntry);

            final OperationResult result = db.put(
                null, keyEntry, dataEntry,
                duplicates ? Put.NO_DUP_DATA : Put.NO_OVERWRITE,
                options);

            if (result == null) {
                throw new IllegalStateException();
            }
        }

        /* Position a cursor at the first record to get the BIN. */
        final Cursor cursor = db.openCursor(null, null);
        OperationStatus status = cursor.getFirst(keyEntry, dataEntry, null);
        assert status == OperationStatus.SUCCESS;
        final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        cursor.close();
        bin.latchNoUpdateLRU();

        /*
         * Calculate BIN size including LNs. The recalcKeyPrefix and
         * compactMemory methods are called to simulate normal operation.
         * Normally prefixes are recalculated when a IN is split, and
         * compactMemory is called after fetching a IN or evicting an LN.
         */
        bin.recalcKeyPrefix();
        bin.compactMemory();
        binWithLNsAndVLSNs = bin.getInMemorySize();

        /*
         * Evict all LNs so we can calculate BIN size without LNs.  This is
         * simulated by calling partialEviction directly.
         */
        if (offHeapCache) {
            final long prevSize = getOffHeapCacheSize(envImpl);

            bin.partialEviction();

            binOffHeapLNs = 0;
            for (int i = 0; i < nodeAvg; i += 1) {
                binOffHeapLNs += getOffHeapLNSize(bin, 0);
            }

            assert getOffHeapCacheSize(envImpl) - prevSize == binOffHeapLNs;

            binOffHeapLNIds = bin.getOffHeapLNIdsMemorySize();

        } else {
            bin.partialEviction();

            binOffHeapLNs = 0;
            binOffHeapLNIds = 0;
        }

        assert !bin.hasCachedChildren();

        binNoLNsWithVLSNs = bin.getInMemorySize() - binOffHeapLNIds;

        /*
         * Another variant is when VLSNs are cached, since they are evicted
         * after the LNs in a separate step.  This is simulated by calling
         * partialEviction a second time.
         */
        if (duplicates || !envImpl.getCacheVLSN()) {
            assert bin.getVLSNCache().getMemorySize() == 0;

        } else {
            assert bin.getVLSNCache().getMemorySize() > 0;

            bin.partialEviction();

            if (dataSize <= bin.getEnv().getMaxEmbeddedLN()) {
                assert bin.getVLSNCache().getMemorySize() > 0;
            } else {
                assert bin.getVLSNCache().getMemorySize() == 0;
            }
        }

        /* There are no LNs or VLSNs remaining. */
        binNoLNsOrVLSNs = bin.getInMemorySize() - binOffHeapLNIds;

        /*
         * To calculate IN size, get parent/root IN and artificially fill the
         * slots with nodeAvg entries.
         */
        final IN in = DbInternal.getDbImpl(db).
                                 getTree().
                                 getRootINLatchedExclusive(CacheMode.DEFAULT);
        assert bin == in.getTarget(0);

        for (int i = 1; i < nodeAvg; i += 1) {

            final int result = in.insertEntry1(
                bin, bin.getKey(i), null, bin.getLsn(i),
                false/*blindInsertion*/);

            assert (result & IN.INSERT_SUCCESS) != 0;
            assert i == (result & ~IN.INSERT_SUCCESS);
        }

        in.recalcKeyPrefix();
        in.compactMemory();
        uinWithTargets = in.getInMemorySize();
        uinNoTargets = uinWithTargets - in.getTargets().calculateMemorySize();

        if (offHeapCache) {

            in.releaseLatch();

            long bytesFreed = envImpl.getEvictor().doTestEvict(
                bin, Evictor.EvictionSource.CACHEMODE);

            assert bytesFreed > 0;

            in.latchNoUpdateLRU();

            final int binId = in.getOffHeapBINId(0);
            assert binId >= 0;

            binOffHeapWithLNIds = getOffHeapBINSize(in, 0);

            bytesFreed = envImpl.getOffHeapCache().stripLNs(in, 0);

            binOffHeapNoLNIds = getOffHeapBINSize(in, 0);

            assert bytesFreed ==
                binOffHeapLNs + (binOffHeapWithLNIds - binOffHeapNoLNIds);

            for (int i = 1; i < nodeAvg; i += 1) {
                in.setOffHeapBINId(i, binId, false, false);
            }

            uinOffHeapBINIds = in.getOffHeapBINIdsMemorySize();

            /* Cleanup to avoid assertions during env close. */
            for (int i = 1; i < nodeAvg; i += 1) {
                in.clearOffHeapBINId(i);
            }

            in.releaseLatch();

        } else {
            binOffHeapWithLNIds = 0;
            uinOffHeapBINIds = 0;

            bin.releaseLatch();
            in.releaseLatch();
        }

        db.close();
    }

    private long getMainDataSize(final Environment env) {
        return DbInternal.getNonNullEnvImpl(env).
            getMemoryBudget().getTreeMemoryUsage();
    }

    private long getOffHeapCacheSize(final EnvironmentImpl envImpl) {
        assert offHeapCache;
        return envImpl.getOffHeapCache().getAllocator().getUsedBytes();
    }

    private long getOffHeapLNSize(final BIN bin, final int i) {
        assert offHeapCache;

        final OffHeapCache ohCache = bin.getEnv().getOffHeapCache();

        final long memId = bin.getOffHeapLNId(i);
        if (memId == 0) {
            return 0;
        }

        return ohCache.getAllocator().totalSize(memId);
    }

    private long getOffHeapBINSize(final IN parent, final int i) {
        assert offHeapCache;

        final OffHeapCache ohCache = parent.getEnv().getOffHeapCache();

        final int lruId = parent.getOffHeapBINId(0);
        assert lruId >= 0;

        final long memId = ohCache.getMemId(lruId);
        assert memId != 0;

        return ohCache.getAllocator().totalSize(memId);
    }

    private void setKeyData(final byte[] keyBytes,
                            final int keyOffset,
                            final DatabaseEntry keyEntry,
                            final DatabaseEntry dataEntry) {
        final byte[] fullKey;
        if (duplicates) {
            fullKey = new byte[keySize + dataSize];
        } else {
            fullKey = new byte[keySize];
        }

        if (keyPrefix + keyBytes.length > fullKey.length) {
            throw new IllegalArgumentException(
                "Key doesn't fit, allowedLen=" + fullKey.length +
                " keyLen=" + keyBytes.length + " prefixLen=" + keyPrefix);
        }

        System.arraycopy(keyBytes, 0, fullKey, keyOffset, keyBytes.length);

        final byte[] finalKey;
        final byte[] finalData;
        if (duplicates) {
            finalKey = new byte[keySize];
            finalData = new byte[dataSize];
            System.arraycopy(fullKey, 0, finalKey, 0, keySize);
            System.arraycopy(fullKey, keySize, finalData, 0, dataSize);
        } else {
            finalKey = fullKey;
            finalData = new byte[Math.max(0, dataSize)];
        }

        keyEntry.setData(finalKey);
        dataEntry.setData(finalData);
    }

    /**
     * Prints Java properties for information collected by calculateCacheSizes.
     * Min/max sizes are output for compatibility with earlier versions; in the
     * past, min and max were different values.
     */
    private void printProperties(final PrintStream out) {
        out.println("overhead=" + envOverhead);
        out.println("internalNodes=" + mainNoLNsOrVLSNs);
        out.println("internalNodesAndVersions=" + mainNoLNsWithVLSNs);
        if (dataSize >= 0) {
            out.println("allNodes=" + mainWithLNsAndVLSNs);
        }
        if (offHeapCache) {
            out.println("minMainCache=" + (mainMinDataSize + envOverhead));
            out.println("offHeapInternalNodes=" + offHeapNoLNsOrVLSNs);
            if (dataSize >= 0) {
                out.println("offHeapAllNodes=" + offHeapWithLNsAndVLSNs);
            }
        }
        out.println("# Following are deprecated");
        out.println("minInternalNodes=" + mainNoLNsOrVLSNs);
        out.println("maxInternalNodes=" + mainNoLNsOrVLSNs);
        if (dataSize >= 0) {
            out.println("minAllNodes=" + mainWithLNsAndVLSNs);
            out.println("maxAllNodes=" + mainWithLNsAndVLSNs);
        }
    }

    /**
     * Prints information collected by calculateCacheSizes.
     */
    void printCacheSizes(final PrintStream out) {

        final String mainSuffix = offHeapCache ? ": MAIN cache" : "";
        final String offHeapSuffix = ": OFF-HEAP cache";

        out.println();
        out.println("=== Environment Cache Overhead ===");
        out.println();
        out.print(INT_FORMAT.format(envOverhead));
        out.println(" minimum bytes");
        out.println();
        out.println(
            "To account for JE daemon operation, record locks, HA network " +
            "connections, etc,");
        out.println("a larger amount is needed in practice.");
        out.println();
        out.println("=== Database Cache Size ===");
        out.println();
        out.println(MAIN_HEADER);

        out.println(line(
            mainNoLNsOrVLSNs, "Internal nodes only" + mainSuffix));

        if (offHeapCache) {
            out.println(line(
                offHeapNoLNsOrVLSNs, "Internal nodes only" + offHeapSuffix));
        }

        if (dataSize >= 0) {
            if (!offHeapCache && mainNoLNsWithVLSNs != mainNoLNsOrVLSNs) {
                out.println(line(
                    mainNoLNsWithVLSNs,
                    "Internal nodes and record versions" + mainSuffix));
            }

            out.println(line(
                mainWithLNsAndVLSNs,
                "Internal nodes and leaf nodes" + mainSuffix));

            if (offHeapCache) {
                out.println(line(
                    offHeapWithLNsAndVLSNs,
                    "Internal nodes and leaf nodes" + offHeapSuffix));
            }

            if (mainNoLNsOrVLSNs == mainWithLNsAndVLSNs &&
                offHeapNoLNsOrVLSNs == offHeapWithLNsAndVLSNs){

                if (duplicates) {
                    out.println(
                        "\nNote that leaf nodes do not use additional memory" +
                        " because the database is" +
                        "\nconfigured for duplicates. In addition, record" +
                        " versions are not applicable.");
                } else {
                    out.println(
                        "\nNote that leaf nodes do not use additional memory" +
                        " because with a small" +
                        "\ndata size, the LNs are embedded in the BINs." +
                        " In addition, record versions" +
                        "\n(if configured) are always cached in this mode.");
                }

            }
        } else {
            if (!duplicates) {
                out.println("\nTo get leaf node sizing specify -data");
            }
        }

        if (offHeapCache && mainMinDataSize > mainDataSize) {
            out.println(
                "\nWARNING: The information above applies to a data set of " +
                INT_FORMAT.format(records) + " records," +
                "\nnot the number of records specified, because the main" +
                " cache size specified is " +
                "\ntoo small to hold all upper INs. This prevents all" +
                " internal nodes (or leaf" +
                "\nnodes) from fitting into cache, and the data set was" +
                " reduced accordingly. To" +
                "\nfit all internal nodes in cache with the specified " +
                " number of records, specify" +
                "\na main cache size of at least " +
                INT_FORMAT.format(mainMinDataSize + envOverhead) + " bytes.");
        }

        if (btreeInfo) {
            out.println();
            out.println("=== Calculated Btree Information ===");
            out.println();
            out.println(line(btreeLevels, "Btree levels"));
            out.println(line(nUinNodes, "Upper internal nodes"));
            out.println(line(nBinNodes, "Bottom internal nodes"));

            if (offHeapCache) {
                out.println();
                out.println("--- BINs and LNs in Main Cache vs Off-heap ---");
                out.println();
                out.println(line(
                    nMainBINsNoLNsOrVLSNs,
                    "Internal nodes only, BINs" + mainSuffix));
                out.println(line(
                    nBinNodes - nMainBINsNoLNsOrVLSNs,
                    "Internal nodes only, BINs" + offHeapSuffix));
                out.println(line(
                    nMainBINsWithLNsAndVLSNs,
                    "Internal nodes and leaf nodes, BINs" + mainSuffix));
                out.println(line(
                    nBinNodes - nMainBINsWithLNsAndVLSNs,
                    "Internal nodes and leaf nodes, BINs" + offHeapSuffix));
                out.println(line(
                    nMainLNsWithLNsAndVLSNs,
                    "Internal nodes and leaf nodes, LNs" + mainSuffix));
                out.println(line(
                    records - nMainLNsWithLNsAndVLSNs,
                    "Internal nodes and leaf nodes, LNs" + offHeapSuffix));
            }
        }

        out.println();
        out.println("For further information see the DbCacheSize javadoc.");
    }

    private String line(final long num, final String comment) {

        final StringBuilder buf = new StringBuilder(100);

        column(buf, INT_FORMAT.format(num));
        buf.append(COLUMN_SEPARATOR);
        buf.append(comment);

        return buf.toString();
    }

    private void column(final StringBuilder buf, final String str) {

        int start = buf.length();

        while (buf.length() - start + str.length() < MIN_COLUMN_WIDTH) {
            buf.append(' ');
        }

        buf.append(str);
    }

    /**
     * For testing, insert the specified data set and initialize
     * measuredMainNoLNsWithVLSNs and measuredMainWithLNsAndVLSNs.
     */
    void measure(final PrintStream out) {

        Environment env = openMeasureEnvironment(
            true /*createNew*/, false /*setMainSize*/);
        try {
            IN.ACCUMULATED_LIMIT = 0;

            Database db = openDatabase(env, true);

            if (out != null) {
                out.println(
                    "Measuring with maximum cache size: " +
                    INT_FORMAT.format(env.getConfig().getCacheSize()) +
                    " and (for off-heap) main data size: " +
                    INT_FORMAT.format(mainDataSize));
            }

            insertRecords(out, env, db);

            if (offHeapCache) {
                db.close();
                env.close();
                env = null;
                env = openMeasureEnvironment(
                    false /*createNew*/, false /*setMainSize*/);
                db = openDatabase(env, false);

                readRecords(out, env, db, false /*readData*/);
                evictMainToDataSize(db, mainDataSize);

                measuredMainNoLNsOrVLSNs = getStats(
                    out, env, "After read keys only, evict main to size");

                measuredOffHeapNoLNsOrVLSNs =
                    getOffHeapCacheSize(DbInternal.getNonNullEnvImpl(env));

                readRecords(out, env, db, true /*readData*/);
                evictMainToDataSize(db, mainDataSize);

                measuredMainWithLNsAndVLSNs = getStats(
                    out, env, "After read all, evict main to size");

                measuredOffHeapWithLNsAndVLSNs =
                    getOffHeapCacheSize(DbInternal.getNonNullEnvImpl(env));

            } else {
                measuredMainWithLNsAndVLSNs = getStats(
                    out, env, "After insert");

                trimLNs(db);

                measuredMainNoLNsWithVLSNs = getStats(
                    out, env, "After trimLNs");

                trimVLSNs(db);

                measuredMainNoLNsOrVLSNs = getStats(
                    out, env, "After trimVLSNs");
            }

            db.close();
            env.close();
            env = null;

            env = openMeasureEnvironment(
                false /*createNew*/, offHeapCache /*setMainSize*/);
            db = openDatabase(env, false);

            PreloadStatus status = preloadRecords(out, db, false /*loadLNs*/);

            preloadMainNoLNsOrVLSNs = getStats(
                out, env,
                "Internal nodes only after preload (" +
                status + ")");

            if (assumeEvictLN) {
                preloadMainWithLNsAndVLSNs = preloadMainNoLNsOrVLSNs;
            } else {
                status = preloadRecords(out, db, true /*loadLNs*/);

                preloadMainWithLNsAndVLSNs = getStats(
                    out, env,
                    "All nodes after preload (" +
                        status + ")");
            }

            if (!offHeapCache) {
                trimLNs(db);

                preloadMainNoLNsWithVLSNs = getStats(
                    out, env,
                    "Internal nodes plus VLSNs after preload (" +
                    status + ")");
            }

            db.close();
            env.close();
            env = null;

        } finally {

            IN.ACCUMULATED_LIMIT = IN.ACCUMULATED_LIMIT_DEFAULT;

            /*
             * Do not propagate exception thrown by Environment.close if
             * another exception is currently in flight.
             */
            if (env != null) {
                try {
                    env.close();
                } catch (RuntimeException ignore) {
                }
            }
        }
    }

    private Environment openMeasureEnvironment(final boolean createNew,
                                               final boolean setMainSize) {

        final EnvironmentConfig config = envConfig.clone();

        if (setMainSize) {
            config.setCacheSize(mainCacheSize);

            /*
             * Normally the main cache size is left "unlimited", meaning that
             * log buffers will be maximum sized (1 MB each). Here we limit the
             * main cache size in order to use the off-heap cache. But with a
             * smaller main cache, the log buffers will be smaller. Use maximum
             * sized log buffers so we can compare totals with the case where
             * we don't set the cache size.
             */
            config.setConfigParam(
                EnvironmentConfig.LOG_TOTAL_BUFFER_BYTES,
                String.valueOf(3 << 20));
        } else {
            config.setCachePercent(90);
        }

        if (offHeapCache) {
            config.setOffHeapCacheSize(1024 * 1024 * 1024);
        } else {
            config.setOffHeapCacheSize(0);
        }

        return openEnvironment(config, createNew);
    }

    private Environment openCalcEnvironment(final boolean createNew) {

        final EnvironmentConfig config = envConfig.clone();

        if (offHeapCache) {
            config.setOffHeapCacheSize(1024 * 1024 * 1024);
        } else {
            config.setOffHeapCacheSize(0);
        }

        /* The amount of disk space needed is quite small. */
        config.setConfigParam(
            EnvironmentConfig.FREE_DISK, String.valueOf(1L << 20));

        return openEnvironment(config, createNew);
    }

    private Environment openEnvironment(final EnvironmentConfig config,
                                        final boolean createNew) {
        mkTempDir();

        if (createNew) {
            emptyTempDir();
        }

        config.setTransactional(true);
        config.setDurability(Durability.COMMIT_NO_SYNC);
        config.setAllowCreate(createNew);

        /* Daemons interfere with cache size measurements. */
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_OFFHEAP_EVICTOR, "false");
        config.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");

        /* Evict in small chunks. */
        config.setConfigParam(
            EnvironmentConfig.EVICTOR_EVICT_BYTES, "1024");

        final Environment newEnv;

        if (replicated) {
            try {
                final Class repEnvClass = Class.forName
                    ("com.sleepycat.je.rep.utilint.DbCacheSizeRepEnv");
                final DbCacheSizeRepEnv repEnv =
                    (DbCacheSizeRepEnv) repEnvClass.newInstance();
                newEnv = repEnv.open(tempDir, config, repParams);
            } catch (ClassNotFoundException |
                     InstantiationException |
                     IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        } else {
            if (!repParams.isEmpty()) {
                throw new IllegalArgumentException(
                    "Cannot set replication params in a standalone " +
                    "environment.  May add -replicated.");
            }
            newEnv = new Environment(tempDir, config);
        }

        /*
         * LSN compaction is typically effective (in a realistic data set) only
         * when the file size fits in 3 bytes and sequential keys are written.
         * Since a tiny data set is use for estimating, and a small data set
         * for testing, we disable the compact representation when it is
         * unlikely to be effective.
         */
        final long fileSize = Integer.parseInt(
            newEnv.getConfig().getConfigParam(EnvironmentConfig.LOG_FILE_MAX));

        if ((fileSize > IN.MAX_FILE_OFFSET) || !orderedInsertion) {
            IN.disableCompactLsns = true;
        }

        /*
         * Preallocate 1st chunk of LRU entries, so it is counted in env
         * overhead.
         */
        if (offHeapCache) {
            DbInternal.getNonNullEnvImpl(newEnv).
                getOffHeapCache().preallocateLRUEntries();
        }

        return newEnv;
    }

    private void mkTempDir() {
        if (tempDir == null) {
            try {
                tempDir = File.createTempFile("DbCacheSize", null);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            /* createTempFile creates a file, but we want a directory. */
            tempDir.delete();
            tempDir.mkdir();
        }
    }

    private void emptyTempDir() {
        if (tempDir == null) {
            return;
        }
        final File[] children = tempDir.listFiles();
        if (children != null) {
            for (File child : children) {
                child.delete();
            }
        }
    }

    private Database openDatabase(final Environment env,
                                  final boolean createNew) {
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(createNew);
        dbConfig.setExclusiveCreate(createNew);
        dbConfig.setNodeMaxEntries(nodeMaxEntries);
        dbConfig.setKeyPrefixing(keyPrefix > 0);
        dbConfig.setSortedDuplicates(duplicates);
        return env.openDatabase(null, "foo", dbConfig);
    }

    /**
     * Inserts records and ensures that no eviction occurs.  LNs (and VLSNs)
     * are left intact.
     */
    private void insertRecords(final PrintStream out,
                               final Environment env,
                               final Database db) {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        final int lastKey = (int) (records - 1);
        final byte[] lastKeyBytes = BigInteger.valueOf(lastKey).toByteArray();
        final int maxKeyBytes = lastKeyBytes.length;

        final int keyOffset;
        if (keyPrefix == 0) {
            keyOffset = 0;
        } else {

            /*
             * Calculate prefix length for generated keys and adjust key offset
             * to produce the desired prefix length.
             */
            final int nodeAvg = orderedInsertion ?
                nodeMaxEntries :
                ((nodeMaxEntries * DEFAULT_DENSITY) / 100);
            final int prevKey = lastKey - (nodeAvg * 2);
            final byte[] prevKeyBytes =
                padLeft(BigInteger.valueOf(prevKey).toByteArray(),
                        maxKeyBytes);
            int calcPrefix = 0;
            while (calcPrefix < lastKeyBytes.length &&
                   calcPrefix < prevKeyBytes.length &&
                   lastKeyBytes[calcPrefix] == prevKeyBytes[calcPrefix]) {
                calcPrefix += 1;
            }
            keyOffset = keyPrefix - calcPrefix;
        }

        /* Generate random keys. */
        List<Integer> rndKeys = null;
        if (!orderedInsertion) {
            rndKeys = new ArrayList<Integer>(lastKey + 1);
            for (int i = 0; i <= lastKey; i += 1) {
                rndKeys.add(i);
            }
            Collections.shuffle(rndKeys, new Random(123));
        }

        final WriteOptions options = new WriteOptions();
        if (useTTL) {
            options.setTTL(30, TimeUnit.DAYS);
        }

        final Transaction txn = env.beginTransaction(null, null);
        final Cursor cursor = db.openCursor(txn, null);
        boolean success = false;
        try {
            for (int i = 0; i <= lastKey; i += 1) {
                final int keyVal = orderedInsertion ? i : rndKeys.get(i);
                final byte[] keyBytes = padLeft(
                    BigInteger.valueOf(keyVal).toByteArray(), maxKeyBytes);
                setKeyData(keyBytes, keyOffset, keyEntry, dataEntry);

                final OperationResult result = cursor.put(
                    keyEntry, dataEntry,
                    duplicates ? Put.NO_DUP_DATA : Put.NO_OVERWRITE,
                    options);

                if (result == null && !orderedInsertion) {
                    i -= 1;
                    continue;
                }
                if (result == null) {
                    throw new IllegalStateException("Could not insert");
                }

                if (i % 10000 == 0) {
                    checkForEviction(env, i);
                    if (out != null) {
                        out.print(".");
                        out.flush();
                    }
                }
            }
            success = true;
        } finally {
            cursor.close();
            if (success) {
                txn.commit();
            } else {
                txn.abort();
            }
        }

        checkForEviction(env, lastKey);

        /* Checkpoint to speed recovery and reset the memory budget. */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /* Let's be sure the memory budget is updated. */
        iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.updateMemoryBudget();
                return true;
            }
        });
    }

    /**
     * Reads all keys, optionally reading the data.
     */
    private void readRecords(final PrintStream out,
                             final Environment env,
                             final Database db,
                             final boolean readData) {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        if (!readData) {
            dataEntry.setPartial(0, 0, true);
        }

        final ReadOptions options = new ReadOptions();

        if (assumeEvictLN) {
            options.setCacheMode(CacheMode.EVICT_LN);
        }

        try (final Cursor cursor = db.openCursor(null, null)) {
            while (cursor.get(keyEntry, dataEntry, Get.NEXT, options) !=
                    null) {
            }
        }
    }


    private void checkForEviction(Environment env, int recNum) {
        final EnvironmentStats stats = env.getStats(null);
        if (stats.getOffHeapNodesTargeted() > 0) {
            getStats(System.out, env, "Out of off-heap cache");
            throw new IllegalStateException(
                "*** Ran out of off-heap cache at record " + recNum +
                " -- try increasing off-heap cache size ***");
        }
        if (stats.getNNodesTargeted() > 0) {
            getStats(System.out, env, "Out of main cache");
            throw new IllegalStateException(
                "*** Ran out of main cache at record " + recNum +
                " -- try increasing Java heap size ***");
        }
    }

    private void trimLNs(final Database db) {
        iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.evictLNs();
                bin.updateMemoryBudget();
                return true;
            }
        });
    }

    private void trimVLSNs(final Database db) {
        iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.discardVLSNCache();
                bin.updateMemoryBudget();
                return true;
            }
        });
    }

    private void evictMainToDataSize(final Database db,
                                     final long dataSize) {

        if (getMainDataSize(db.getEnvironment()) <= dataSize) {
            return;
        }

        boolean keepGoing = iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.evictLNs();
                bin.discardVLSNCache();
                bin.updateMemoryBudget();
                return getMainDataSize(db.getEnvironment()) > dataSize;
            }
        });

        if (!keepGoing) {
            return;
        }

        final Evictor evictor =
            DbInternal.getNonNullEnvImpl(db.getEnvironment()).getEvictor();

        keepGoing = iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                evictor.doTestEvict(bin, Evictor.EvictionSource.CACHEMODE);
                return getMainDataSize(db.getEnvironment()) > dataSize;
            }
        });

        assert !keepGoing;
    }

    private interface BINVisitor {
        boolean visitBIN(BIN bin);
    }

    private boolean iterateBINs(final Database db, final BINVisitor visitor) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(0, 0, true);

        final Cursor c = db.openCursor(null, null);
        BIN prevBin = null;
        boolean keepGoing = true;

        while (keepGoing &&
               c.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
               OperationStatus.SUCCESS) {

            final BIN bin = DbInternal.getCursorImpl(c).getBIN();

            if (bin == prevBin) {
                continue;
            }

            if (prevBin != null) {
                prevBin.latch();
                keepGoing = visitor.visitBIN(prevBin);
                prevBin.releaseLatchIfOwner();
            }

            prevBin = bin;
        }

        c.close();

        if (keepGoing && prevBin != null) {
            prevBin.latch();
            visitor.visitBIN(prevBin);
            prevBin.releaseLatch();
        }

        return keepGoing;
    }

    /**
     * Pads the given array with zeros on the left, and returns an array of
     * the given size.
     */
    private byte[] padLeft(byte[] data, int size) {
        assert data.length <= size;
        if (data.length == size) {
            return data;
        }
        final byte[] b = new byte[size];
        System.arraycopy(data, 0, b, size - data.length, data.length);
        return b;
    }

    /**
     * Preloads the database.
     */
    private PreloadStatus preloadRecords(final PrintStream out,
                                         final Database db,
                                         final boolean loadLNs) {
        Thread thread = null;
        if (out != null) {
            thread = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            out.print(".");
                            out.flush();
                            Thread.sleep(5 * 1000);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            };
            thread.start();
        }
        final PreloadStats stats;
        try {
            stats = db.preload(new PreloadConfig().setLoadLNs(loadLNs));
        } finally {
            if (thread != null) {
                thread.interrupt();
            }
        }
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeExceptionWrapper(e);
            }
        }

        /*
         * When preloading with an off-heap cache, the main cache will overflow
         * a little by design. We evict here to bring it down below the
         * maximum, and clear the stats so that the getStats method in this
         * class doesn't complain about the eviction later on.
         */
        final Environment env = db.getEnvironment();
        if (offHeapCache) {
            env.evictMemory();
            env.getStats(StatsConfig.CLEAR);
        }

        return stats.getStatus();
    }

    /**
     * Returns the Btree size, and prints a few other stats for testing.
     */
    private long getStats(final PrintStream out,
                          final Environment env,
                          final String msg) {
        if (out != null) {
            out.println();
            out.println(msg + ':');
        }

        final EnvironmentStats stats = env.getStats(null);

        final long dataSize = getMainDataSize(env);

        if (out != null) {
            out.println(
                "MainCache= " + INT_FORMAT.format(stats.getCacheTotalBytes()) +
                " Data= " + INT_FORMAT.format(dataSize)  +
                " BINs= " + INT_FORMAT.format(stats.getNCachedBINs()) +
                " UINs= " + INT_FORMAT.format(stats.getNCachedUpperINs()) +
                " CacheMiss= " + INT_FORMAT.format(stats.getNCacheMiss()) +
                " OffHeapCache= " +
                INT_FORMAT.format(stats.getOffHeapTotalBytes()) +
                " OhLNs= " + INT_FORMAT.format(stats.getOffHeapCachedLNs()) +
                " OhBIN= " + INT_FORMAT.format(stats.getOffHeapCachedBINs()) +
                " OhBINDeltas= " +
                INT_FORMAT.format(stats.getOffHeapCachedBINDeltas()));
        }

        if (stats.getNNodesTargeted() > 0) {
            throw new IllegalStateException(
                "*** All records did not fit in the cache ***");
        }
        if (stats.getOffHeapNodesTargeted() > 0) {
            throw new IllegalStateException(
                "*** All records did not fit in the off-heap cache ***");
        }
        return dataSize;
    }
}
