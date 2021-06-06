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

package com.sleepycat.je.utilint;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * A long JE stat component generated from an exponential moving average over a
 * specified time period of the rate of change in a long value over time.
 */
public class LongAvgRate extends MapStatComponent<Long, LongAvgRate> {
    private static final long serialVersionUID = 1L;

    /**
     * The minimum number of milliseconds for computing rate changes, to avoid
     * quantizing errors.
     */
    public static final long MIN_PERIOD = 200;

    /** The time unit for reporting the result. */
    private final TimeUnit reportTimeUnit;

    /** The average of the rate values. */
    private final DoubleExpMovingAvg avg;

    /**
     * The previous value, or 0.  Synchronize on this instance when accessing
     * this field.
     */
    private long prevValue;

    /**
     * The time in milliseconds of the previous value, or 0.  Synchronize on
     * this instance when accessing this field.
     */
    private long prevTime;

    /**
     * Creates an instance of this class.
     *
     * @param name the name of this stat
     * @param periodMillis the averaging period in milliseconds
     * @param reportTimeUnit the time unit for reporting the result
     */
    public LongAvgRate(String name,
                       long periodMillis,
                       TimeUnit reportTimeUnit) {
        avg = new DoubleExpMovingAvg(name, periodMillis);
        assert reportTimeUnit != null;
        this.reportTimeUnit = reportTimeUnit;
    }

    /**
     * Creates an instance of this class as a copy of another instance.
     *
     * @param other the other instance to copy
     */
    private LongAvgRate(LongAvgRate other) {
        avg = new DoubleExpMovingAvg(other.avg.copy());
        reportTimeUnit = other.reportTimeUnit;
        synchronized (this) {
            synchronized (other) {
                prevValue = other.prevValue;
                prevTime = other.prevTime;
            }
        }
    }

    /**
     * Returns the name of this stat.
     *
     * @return the name of this stat
     */
    public String getName() {
        return avg.getName();
    }

    /**
     * Adds a new value to the average, ignoring values that are less than
     * {@link #MIN_PERIOD} milliseconds older than the last entry.
     *
     * @param value the new value
     * @param time the current time in milliseconds
     */
    public synchronized void add(long value, long time) {
        assert time > 0;
        if (prevTime != 0) {
            final long deltaTime = time - prevTime;
            if (deltaTime < MIN_PERIOD) {
                return;
            }
            avg.add(((double) (value - prevValue)) / ((double) deltaTime),
                    time);
        }
        prevValue = value;
        prevTime = time;
    }

    /**
     * Update with more recent values from another stat.
     *
     * @param other the other stat
     */
    public void add(LongAvgRate other) {
        final LongAvgRate copyOther = other.copy();
        synchronized (this) {
            synchronized (copyOther) {
                addInternal(copyOther);
            }
        }
    }

    /**
     * Do an  add, letting the caller  arrange to synchronize on  this instance
     * and the argument safely.
     */
    private void addInternal(LongAvgRate other) {
        assert(Thread.holdsLock(this));
        assert(Thread.holdsLock(other));

        /*
         * Only use the other values if they are newer by more than the
         * minimum
         */
        if ((other.prevTime - prevTime) > MIN_PERIOD) {
            avg.add(other.avg);
            prevValue = other.prevValue;
            prevTime = other.prevTime;
        }
    }

    /**
     * Create and return a new stat that includes the most recent values from
     * this stat and another stat.
     *
     * @param other the other stat
     * @return a copy containing all new values
     */
    public LongAvgRate copyLatest(LongAvgRate other) {
        final LongAvgRate otherCopy = other.copy();
        synchronized (this) {
            synchronized (otherCopy) {
                if (prevTime > otherCopy.prevTime) {
                    otherCopy.addInternal(this);
                    return otherCopy;
                }
                final LongAvgRate result = copy();
                synchronized (result) {
                    result.addInternal(otherCopy);
                    return result;
                }
            }
        }
    }

    /**
     * Returns the time the last new value was added, or 0 if no values have
     * been added.
     *
     * @return the time or 0
     */
    synchronized long getPrevTime() {
        return prevTime;
    }

    /**
     * Returns the current average rate, or 0 if no rate has been computed.
     */
    @Override
    public Long get() {
        return getPrimitive();
    }

    /** Returns the current average rate as a primitive value. */
    private long getPrimitive() {
        final double inMillis = avg.getPrimitive();
        if (reportTimeUnit == MILLISECONDS) {
            return Math.round(inMillis);
        } else if (reportTimeUnit.compareTo(MILLISECONDS) < 0) {
            return Math.round(
                inMillis / reportTimeUnit.convert(1, MILLISECONDS));
        } else {
            return Math.round(inMillis * reportTimeUnit.toMillis(1));
        }
    }

    @Override
    public synchronized void clear() {
        avg.clear();
        prevValue = 0;
        prevTime = 0;
    }

    @Override
    public LongAvgRate copy() {
        return new LongAvgRate(this);
    }

    @Override
    protected String getFormattedValue(boolean useCommas) {
        if (isNotSet()) {
            return "unknown";
        }
        final long val = getPrimitive();
        if (useCommas) {
            return Stat.FORMAT.format(val);
        } else {
            return Long.toString(val);
        }
    }

    @Override
    public boolean isNotSet() {
        return avg.isNotSet();
    }

    @Override
    public synchronized String toString() {
        return "LongAvgRate[" + avg + ", prevValue=" + prevValue +
            ", prevTime=" + prevTime + "]";
    }

    /** Synchronize access to fields. */
    private synchronized void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();
    }

    /** Synchronize access to fields. */
    private synchronized void writeObject(ObjectOutputStream out)
        throws IOException {

        out.defaultWriteObject();
    }
}
