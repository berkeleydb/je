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
package com.sleepycat.je.rep.vlsn;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;

/**
 * A no-wait cache used to retain the most recent VLSNs. The Feeders check this
 * cache first for log entries to send out to the Replicas. Feeders that are
 * feeding at the most up to date portion of the replication stream will likely
 * hit in the cache, preventing a lookup in the log buffers or log files.
 *
 * The log item cache is made up of weak references so there is never any
 * guarantee that even the most recent 32 entries are in there.
 */
class LogItemCache {

    /*
     * Must be a power of 2 and small, typically around 32 entries. Increasing
     * sizes typically yield diminishing returns.
     */
    private final int cacheSize;
    private final int sizeMask;

    /*
     * Soft reference to array, so that the LogItems can be released when
     * under GC pressure.
     */
    private volatile SoftReference<AtomicReference<LogItem>[]>
        cacheReference =
        new SoftReference<AtomicReference<LogItem>[]>(null);

    private final LongStat nHits;
    private final LongStat nMisses;

    /**
     * Creates a log item size of the specified size.
     *
     * @param cacheSize it must be a power of two
     * @param statGroup the statsGroup to which this cache adds its stats
     * @throws IllegalArgumentException via ReplicatedEnvironment ctor.
     */
    LogItemCache(int cacheSize, StatGroup statGroup) {
        if (Integer.bitCount(cacheSize) != 1) {
            throw new IllegalArgumentException
                ("Bad cache size: " + cacheSize + "; it must be a power of 2");
        }
        this.cacheSize = cacheSize;
        sizeMask = cacheSize - 1;
        nHits = new LongStat(statGroup, VLSNIndexStatDefinition.N_HITS);
        nMisses = new LongStat(statGroup, VLSNIndexStatDefinition.N_MISSES);
    }

    void put(VLSN vlsn, LogItem logItem) {
        getArray()[(int)vlsn.getSequence() & sizeMask].set(logItem);
    }

    LogItem get(VLSN vlsn) {
        final LogItem item =
            getArray()[(int)vlsn.getSequence() & sizeMask].get();
        if ((item != null) && item.header.getVLSN().equals(vlsn)) {
            nHits.increment();
            return item;
        }

        nMisses.increment();
        return null;
    }

    /**
     * For explicit release of references.
     */
    void clear() {
        for (AtomicReference<LogItem> element : getArray()) {
            element.set(null);
        }
    }

    /**
     * Returns the cache array, creating a new one, if the GC had cleared the
     * reference to the earlier one.
     * <p>
     *
     * Note that there may be a slight inefficiency if getArray is called
     * concurrently, and it had been cleared earlier, since it would be
     * allocated twice and introduce a cache miss. This occurrence is
     * infrequent enough that it's not worth the overhead of a sync mechanism.
     *
     * @return the underlying array, allocating a new one, if the previous one
     * had been GC'd
     */
    @SuppressWarnings("unchecked")
    private final AtomicReference<LogItem>[] getArray() {
        AtomicReference<LogItem>[] array = cacheReference.get();
        if (array == null) {
            array = new AtomicReference[cacheSize];
            for (int i=0; i < array.length; i++) {
                array[i] = new AtomicReference<LogItem>();
            }
            cacheReference =
                new SoftReference<AtomicReference<LogItem>[]>(array);
        }
        return array;
    }
}
