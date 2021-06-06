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

package com.sleepycat.je.evictor;

import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * The off-heap stats were put in a separate group rather than being combined
 * with the main cache evictor stats, simply because there were so many evictor
 * stats already.
 */
public class OffHeapStatDefinition {
    public static final String GROUP_NAME = "OffHeap";
    public static final String GROUP_DESC =
        "The optional off-heap cache resides outside the " +
            "Java heap and serves as an overflow area for the main cache.";

    public static final String ALLOC_FAILURE_NAME =
        "offHeapAllocFailure";
    public static final String ALLOC_FAILURE_DESC =
        "Number of off-heap allocation failures due to lack of system memory.";
    public static final StatDefinition ALLOC_FAILURE =
        new StatDefinition(
            ALLOC_FAILURE_NAME,
            ALLOC_FAILURE_DESC);

    public static final String ALLOC_OVERFLOW_NAME =
        "offHeapAllocOverflow";
    public static final String ALLOC_OVERFLOW_DESC =
        "Number of off-heap allocation attempts that exceeded the cache size.";
    public static final StatDefinition ALLOC_OVERFLOW =
        new StatDefinition(
            ALLOC_OVERFLOW_NAME,
            ALLOC_OVERFLOW_DESC);

    public static final String THREAD_UNAVAILABLE_NAME =
        "offHeapThreadUnavailable";
    public static final String THREAD_UNAVAILABLE_DESC =
        "Number of eviction tasks that were submitted to the background " +
            "off-heap evictor pool, but were refused because all eviction " +
            "threads were busy.";
    public static final StatDefinition THREAD_UNAVAILABLE =
        new StatDefinition(
            THREAD_UNAVAILABLE_NAME,
            THREAD_UNAVAILABLE_DESC);

    public static final String NODES_TARGETED_NAME =
        "offHeapNodesTargeted";
    public static final String NODES_TARGETED_DESC =
        "Number of BINs selected as off-heap eviction targets.";
    public static final StatDefinition NODES_TARGETED =
        new StatDefinition(
            NODES_TARGETED_NAME,
            NODES_TARGETED_DESC);

    public static final String CRITICAL_NODES_TARGETED_NAME =
        "offHeapCriticalNodesTargeted";
    public static final String CRITICAL_NODES_TARGETED_DESC =
        "Number of nodes targeted in 'critical eviction' mode.";
    public static final StatDefinition CRITICAL_NODES_TARGETED =
        new StatDefinition(
            CRITICAL_NODES_TARGETED_NAME,
            CRITICAL_NODES_TARGETED_DESC);

    public static final String NODES_EVICTED_NAME =
        "offHeapNodesEvicted";
    public static final String NODES_EVICTED_DESC =
        "Number of target BINs (including BIN-deltas) evicted from the " +
            "off-heap cache.";
    public static final StatDefinition NODES_EVICTED =
        new StatDefinition(
            NODES_EVICTED_NAME,
            NODES_EVICTED_DESC);

    public static final String DIRTY_NODES_EVICTED_NAME =
        "offHeapDirtyNodesEvicted";
    public static final String DIRTY_NODES_EVICTED_DESC =
        "Number of target BINs evicted from the off-heap cache that were " +
            "dirty and therefore were logged.";
    public static final StatDefinition DIRTY_NODES_EVICTED =
        new StatDefinition(
            DIRTY_NODES_EVICTED_NAME,
            DIRTY_NODES_EVICTED_DESC);

    public static final String NODES_STRIPPED_NAME =
        "offHeapNodesStripped";
    public static final String NODES_STRIPPED_DESC =
        "Number of target BINs whose off-heap child LNs were evicted " +
            "(stripped).";
    public static final StatDefinition NODES_STRIPPED =
        new StatDefinition(
            NODES_STRIPPED_NAME,
            NODES_STRIPPED_DESC);

    public static final String NODES_MUTATED_NAME =
        "offHeapNodesMutated";
    public static final String NODES_MUTATED_DESC =
        "Number of off-heap target BINs mutated to BIN-deltas.";
    public static final StatDefinition NODES_MUTATED =
        new StatDefinition(
            NODES_MUTATED_NAME,
            NODES_MUTATED_DESC);

    public static final String NODES_SKIPPED_NAME =
        "offHeapNodesSkipped";
    public static final String NODES_SKIPPED_DESC =
        "Number of off-heap target BINs on which no action was taken.";
    public static final StatDefinition NODES_SKIPPED =
        new StatDefinition(
            NODES_SKIPPED_NAME,
            NODES_SKIPPED_DESC);

    public static final String LNS_EVICTED_NAME =
        "offHeapLNsEvicted";
    public static final String LNS_EVICTED_DESC =
        "Number of LNs evicted from the off-heap cache as a result of BIN " +
            "stripping.";
    public static final StatDefinition LNS_EVICTED =
        new StatDefinition(
            LNS_EVICTED_NAME,
            LNS_EVICTED_DESC);

    public static final String LNS_LOADED_NAME =
        "offHeapLNsLoaded";
    public static final String LNS_LOADED_DESC =
        "Number of LNs loaded from the off-heap cache.";
    public static final StatDefinition LNS_LOADED =
        new StatDefinition(
            LNS_LOADED_NAME,
            LNS_LOADED_DESC);

    public static final String LNS_STORED_NAME =
        "offHeapLNsStored";
    public static final String LNS_STORED_DESC =
        "Number of LNs stored into the off-heap cache.";
    public static final StatDefinition LNS_STORED =
        new StatDefinition(
            LNS_STORED_NAME,
            LNS_STORED_DESC);

    public static final String BINS_LOADED_NAME =
        "offHeapBINsLoaded";
    public static final String BINS_LOADED_DESC =
        "Number of BINs loaded from the off-heap cache.";
    public static final StatDefinition BINS_LOADED =
        new StatDefinition(
            BINS_LOADED_NAME,
            BINS_LOADED_DESC);

    public static final String BINS_STORED_NAME =
        "offHeapBINsStored";
    public static final String BINS_STORED_DESC =
        "Number of BINs stored into the off-heap cache.";
    public static final StatDefinition BINS_STORED =
        new StatDefinition(
            BINS_STORED_NAME,
            BINS_STORED_DESC);

    public static final String CACHED_LNS_NAME =
        "offHeapCachedLNs";
    public static final String CACHED_LNS_DESC =
        "Number of LNs residing in the off-heap cache.";
    public static final StatDefinition CACHED_LNS =
        new StatDefinition(
            CACHED_LNS_NAME,
            CACHED_LNS_DESC,
            StatType.CUMULATIVE);

    public static final String CACHED_BINS_NAME =
        "offHeapCachedBINs";
    public static final String CACHED_BINS_DESC =
        "Number of BINs (full BINs and BIN-deltas) residing in the off-heap " +
            "cache.";
    public static final StatDefinition CACHED_BINS =
        new StatDefinition(
            CACHED_BINS_NAME,
            CACHED_BINS_DESC,
            StatType.CUMULATIVE);

    public static final String CACHED_BIN_DELTAS_NAME =
        "offHeapCachedBINDeltas";
    public static final String CACHED_BIN_DELTAS_DESC =
        "Number of BIN-deltas residing in the off-heap cache.";
    public static final StatDefinition CACHED_BIN_DELTAS =
        new StatDefinition(
            CACHED_BIN_DELTAS_NAME,
            CACHED_BIN_DELTAS_DESC,
            StatType.CUMULATIVE);

    public static final String TOTAL_BYTES_NAME =
        "offHeapTotalBytes";
    public static final String TOTAL_BYTES_DESC =
        "Total number of estimated bytes in off-heap cache.";
    public static final StatDefinition TOTAL_BYTES =
        new StatDefinition(
            TOTAL_BYTES_NAME,
            TOTAL_BYTES_DESC,
            StatType.CUMULATIVE);

    public static final String TOTAL_BLOCKS_NAME =
        "offHeapTotalBlocks";
    public static final String TOTAL_BLOCKS_DESC =
        "Total number of memory blocks in off-heap cache.";
    public static final StatDefinition TOTAL_BLOCKS =
        new StatDefinition(
            TOTAL_BLOCKS_NAME,
            TOTAL_BLOCKS_DESC,
            StatType.CUMULATIVE);

    public static final String LRU_SIZE_NAME =
        "offHeapLruSize";
    public static final String LRU_SIZE_DESC =
        "Number of LRU entries used for the off-heap cache.";
    public static final StatDefinition LRU_SIZE =
        new StatDefinition(
            LRU_SIZE_NAME,
            LRU_SIZE_DESC,
            StatType.CUMULATIVE);
}
