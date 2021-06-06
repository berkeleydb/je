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

import java.text.DecimalFormat;

import com.sleepycat.je.EnvironmentFailureException;

/**
 * Base class for all JE statistics. A single Stat embodies a value and
 * definition. See StatGroup for a description of how to create and display
 * statistics.
 *
 * Note that Stat intentionally does not contain the statistics value itself.
 * Instead, the concrete subclass will implement the value as the appropriate
 * primitive type. That's done to avoid wrapper classes like Integer and Long,
 * and to  keep the overhead of statistics low.
 */
public abstract class Stat<T> extends BaseStat<T> implements Cloneable {
    private static final long serialVersionUID = 1L;

    public static final DecimalFormat FORMAT =
        new DecimalFormat("###,###,###,###,###,###,###");

    protected final StatDefinition definition;

    /**
     * A stat registers itself with an owning group.
     */
    Stat(StatGroup group, StatDefinition definition) {
        this.definition = definition;
        group.register(this);
    }

    /**
     * Creates an instance without registering it with the owning group, for
     * creating copies without using clone. For constructing an unregistered
     * instance.
     */
    Stat(StatDefinition definition) {
        this.definition = definition;
    }

    /**
     * Set the stat value.
     */
    public abstract void set(T newValue);

    /**
     * Add the value of "other" to this stat.
     */
    public abstract void add(Stat<T> other);

    /**
     * Compute interval value with respect to the base value.
     */
    public abstract Stat<T> computeInterval(Stat<T> base);

    /**
     * Negate the value.
     */
    public abstract void negate();

    @Override
    public Stat<T> copy() {
        @SuppressWarnings("unchecked")
        final Stat<T> copy;
        try {
            copy = (Stat<T>) super.clone();
        } catch (CloneNotSupportedException unexpected) {
            throw EnvironmentFailureException.unexpectedException(unexpected);
        }
        return copy;
    }

    /**
     * Return a copy of this statistic and add to group.
     */
    public Stat<T> copyAndAdd(StatGroup group) {
        Stat<T> newCopy = copy();
        group.register(newCopy);
        return newCopy;
    }

    /**
     * Return a copy of this stat, and clear the stat's value.
     */
    public Stat<T> copyAndClear() {
        Stat<T> newCopy = copy();
        clear();
        return newCopy;
    }

    public StatDefinition getDefinition() {
        return definition;
    }

    @Override
    public String toString() {
        return definition.getName() + "=" + getFormattedValue();
    }

    /**
     * Includes the per-stat description in the output string.
     */
    public String toStringVerbose() {
        return definition.getName() + "=" + getFormattedValue() +
            "\n\t\t" + definition.getDescription();
    }
}
