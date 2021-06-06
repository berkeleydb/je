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

import java.io.PrintStream;

/**
 * Specifies the attributes of a statistics retrieval operation.
 */
public class StatsConfig implements Cloneable {

    /**
     * A convenience instance embodying the default configuration.
     */
    public static final StatsConfig DEFAULT = new StatsConfig();

    /**
     * A convenience instance for which setClear(true) has been called, and
     * all other properties have default values.
     */
    public static final StatsConfig CLEAR = new StatsConfig().setClear(true);

    private boolean fast = false;
    private boolean clear = false;
    private PrintStream showProgressStream = null;
    private int showProgressInterval = 0;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public StatsConfig() {
    }

    /**
     * Configures the statistics operation to return only the values which do
     * not incur some performance penalty.
     *
     * <p>The default value is false.</p>
     *
     * <p>For example, skip stats that require a traversal of the database or
     * in-memory tree, or which lock down the lock table for a period of
     * time.</p>
     *
     * @param fast If set to true, configure the statistics operation to return
     * only the values which do not incur some performance penalty.
     *
     * @return this
     */
    public StatsConfig setFast(boolean fast) {
        setFastVoid(fast);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setFastVoid(boolean fast) {
        this.fast = fast;
    }

    /**
     * Returns true if the statistics operation is configured to return only
     * the values which do not require expensive actions.
     *
     * @return true if the statistics operation is configured to return only
     * the values which do not require expensive actions.
     */
    public boolean getFast() {
        return fast;
    }

    /**
     * Configures the statistics operation to reset statistics after they are
     * returned. The default value is false.
     *
     * @param clear If set to true, configure the statistics operation to
     * reset statistics after they are returned.
     *
     * @return this
     */
    public StatsConfig setClear(boolean clear) {
        setClearVoid(clear);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setClearVoid(boolean clear) {
        this.clear = clear;
    }

    /**
     * Returns true if the statistics operation is configured to reset
     * statistics after they are returned.
     *
     * @return true if the statistics operation is configured to reset
     * statistics after they are returned.
     */
    public boolean getClear() {
        return clear;
    }

    /**
     * Configures the statistics operation to display progress to the
     * PrintStream argument.  The accumulated statistics will be displayed
     * every N records, where N is the value of showProgressInterval.
     *
     * @return this
     */
    public StatsConfig setShowProgressStream(PrintStream showProgressStream) {
        setShowProgressStreamVoid(showProgressStream);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setShowProgressStreamVoid(PrintStream showProgressStream) {
        this.showProgressStream = showProgressStream;
    }

    /**
     * Returns the PrintStream on which the progress messages will be displayed
     * during long running statistics gathering operations.
     */
    public PrintStream getShowProgressStream() {
        return showProgressStream;
    }

    /**
     * When the statistics operation is configured to display progress the
     * showProgressInterval is the number of LNs between each progress report.
     *
     * @return this
     */
    public StatsConfig setShowProgressInterval(int showProgressInterval) {
        setShowProgressIntervalVoid(showProgressInterval);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setShowProgressIntervalVoid(int showProgressInterval) {
        this.showProgressInterval = showProgressInterval;
    }

    /**
     * Returns the showProgressInterval value, if set.
     */
    public int getShowProgressInterval() {
        return showProgressInterval;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public StatsConfig clone() {
        try {
            return (StatsConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "fast=" + fast +
            "\nclear=" + clear +
            "\nshowProgressStream=" + showProgressStream +
            "\nshowProgressInterval=" + showProgressInterval +
            "\n";
    }
}
