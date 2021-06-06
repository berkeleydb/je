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

package com.sleepycat.je.rep.utilint;

import static com.sleepycat.je.rep.utilint.SizeAwaitMapStatDefinition.N_NO_WAITS;
import static com.sleepycat.je.rep.utilint.SizeAwaitMapStatDefinition.N_REAL_WAITS;
import static com.sleepycat.je.rep.utilint.SizeAwaitMapStatDefinition.N_WAIT_TIME;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.utilint.RepUtils.ExceptionAwareCountDownLatch;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Creates a Map that Threads can conveniently wait on to contain a specific
 * number of entries, where the values optionally match a predicate. The wait
 * functionality is provided by the sizeAwait() method defined by this
 * class. Map values must not be null.
 */
public class SizeAwaitMap<K, V> implements Map<K, V> {

    /* The environment to use for exception reporting. */
    private final EnvironmentImpl envImpl;

    /*
     * The predicate to apply to the value when counting entries or null to
     * match all entries.
     */
    private final Predicate<V> predicate;

    /*
     * The latch map. There is a latch for each threshold of interest to a
     * thread.
     */
    private final HashMap<Integer, ExceptionAwareCountDownLatch>
        thresholdLatches;

    /* The underlying map of interest to threads. */
    private final Map<K, V> map = new HashMap<K, V>();

    /*
     * The number of entries with values matching the predicate, or the total
     * number of entries if the predicate is null.
     */
    private int count = 0;

    private final StatGroup stats;
    private final LongStat nNoWaits;
    private final LongStat nRealWaits;
    private final LongStat nWaitTime;

    /**
     * Creates an instance of this class.
     *
     * @param envImpl the environment, used for exception handling
     * @param predicate the predicate for counting matching entries, or
     *        {@code null} to match all entries
     */
    public SizeAwaitMap(final EnvironmentImpl envImpl,
                        final Predicate<V> predicate) {
        this.envImpl = envImpl;
        this.predicate = predicate;
        thresholdLatches =
            new HashMap<Integer, ExceptionAwareCountDownLatch>();
        stats = new StatGroup(SizeAwaitMapStatDefinition.GROUP_NAME,
                              SizeAwaitMapStatDefinition.GROUP_DESC);
        nNoWaits = new LongStat(stats, N_NO_WAITS);
        nRealWaits = new LongStat(stats, N_REAL_WAITS);
        nWaitTime = new LongStat(stats, N_WAIT_TIME);
    }

    public StatGroup getStatistics() {
        return stats;
    }

    /**
     * Causes the requesting thread to wait until the map reaches the specified
     * size or the thread is interrupted.
     *
     * @param thresholdSize the size to wait for.
     *
     * @return true if the threshold was reached, false, if the wait timed out.
     *
     * @throws InterruptedException for the usual reasons, or if the map
     * was cleared and the size threshold was not actually reached.
     *
     */
    public boolean sizeAwait(int thresholdSize,
                             long timeout,
                             TimeUnit unit)
        throws InterruptedException {

        assert(thresholdSize >= 0);
        ExceptionAwareCountDownLatch l = null;
        synchronized (this) {
            if (thresholdSize <= count) {
                nNoWaits.increment();
                return true;
            }
            l = thresholdLatches.get(thresholdSize);
            if (l == null) {
                l = new ExceptionAwareCountDownLatch(envImpl, 1);
                thresholdLatches.put(thresholdSize, l);
            }
        }
        nRealWaits.increment();
        long startTime = System.currentTimeMillis();
        try {
            return l.awaitOrException(timeout, unit);
        } finally {
            nWaitTime.add((System.currentTimeMillis() - startTime));
        }
    }

    /**
     * Used for unit tests only
     * @return
     */
    synchronized int latchCount() {
        return thresholdLatches.size();
    }

    /**
     * Notes the addition of a new value and counts down any latches that were
     * assigned to that threshold.
     */
    @Override
    public synchronized V put(final K key, final V value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        int countDelta = checkPredicate(value) ? 1 : 0;
        final V oldValue = map.put(key, value);
        if ((oldValue != null) && checkPredicate(oldValue)) {
            countDelta--;
        }
        count += countDelta;
        if (countDelta > 0) {
            /* Incremented count */
            final CountDownLatch l = thresholdLatches.remove(count);
            if (l != null) {
                l.countDown();
            }
        }
        return oldValue;
    }

    /** Checks if the value matches the predicate. */
    private boolean checkPredicate(final V value) {
        return (predicate == null) || predicate.match(value);
    }

    @Override
    public synchronized V remove(Object key) {
        final V oldValue = map.remove(key);
        if ((oldValue != null) && checkPredicate(oldValue)) {
            count--;
        }
        return oldValue;
    }

    /**
     * @deprecated Use {@link #clear(Exception)} instead.
     */
    @Deprecated
    @Override
    public void clear() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Clears the underlying map and the latch map, after first counting them
     * down, thus permitting any waiting threads to make progress.
     *
     * @cause the value is non-null if the map is being cleared in response to
     * an exception and results in the exception being thrown in the waiting
     * threads. It's null if the map is being cleared as part of a normal
     * shutdown, in which case no exception is thrown.
     */
    public synchronized void clear(Exception cause) {
        for (ExceptionAwareCountDownLatch l : thresholdLatches.values()) {
            l.releaseAwait(cause);
        }
        thresholdLatches.clear();
        map.clear();
        count = 0;
    }

    /* The remaining methods below merely forward to the underlying map. */

    @Override
    public synchronized boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public synchronized boolean containsValue(Object value) {
        return map.containsKey(value);
    }

    /**
     * The caller should synchronize on the map while accessing the return
     * value.
     */
    @Override
    public synchronized Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public synchronized V get(Object key) {
        return map.get(key);
    }

    @Override
    public synchronized boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * The caller should synchronize on the map while accessing the return
     * value.
     */
    @Override
    public synchronized Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> t) {
        throw EnvironmentFailureException.unexpectedState
            ("putAll not supported");
    }

    @Override
    public synchronized int size() {
        return map.size();
    }

    /**
     * The caller should synchronize on the map while accessing the return
     * value.
     */
    @Override
    public synchronized Collection<V> values() {
        return map.values();
    }

    /**
     * Specifies which values should be counted.
     */
    public interface Predicate<V> {

        /**
         * Whether an entry with this value should included in the count of
         * entries being waited for.
         */
        boolean match(V value);
    }
}
