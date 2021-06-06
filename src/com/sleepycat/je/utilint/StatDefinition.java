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

import java.io.Serializable;

/**
 * Per-stat Metadata for JE statistics. The name and description are meant to
 * available in a verbose display of stats, and should be meaningful for users.
 */
public class StatDefinition implements Comparable, Serializable {
    private static final long serialVersionUID = 1L;

    /*
     * A CUMULATIVE statistic is a statistic that is never cleared
     * (represents totals) or whose value is computed from the system
     * state at the time the statistic is acquired.
     * An INCREMENTAL statistic is cleared when StatConfig.getClear
     * is true. The value of the statistic represent an incremental
     * value since the last clear.
     */
    public enum StatType {
        INCREMENTAL,
        CUMULATIVE
    }

    private final String name;
    private final String description;
    private final StatType type;

    /**
     * Convenience constructor used for INCREMENTAL stats.
     * @param name
     * @param description
     */
    public StatDefinition(String name, String description) {
        this.name = name;
        this.description = description;
        this.type = StatType.INCREMENTAL;
    }

    /**
     * Constructor
     * @param name
     * @param description
     * @param type
     */
    public StatDefinition(String name, String description, StatType type) {
        this.name = name;
        this.description = description;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public StatType getType() {
        return type;
    }

    @Override
    public String toString() {
        return name + ": " + description;
    }

    @Override
    public int compareTo(Object other) {
        return toString().compareTo(other.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof StatDefinition)) {
            return false;
        }

        StatDefinition other = (StatDefinition) obj;
        return (name.equals(other.name));
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
