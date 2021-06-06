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

/**
 * Specifies the attributes of an application invoked preload operation.
 */
public class PreloadConfig implements Cloneable {

    private long maxBytes;
    private long maxMillisecs;
    private boolean loadLNs;
    private ProgressListener<Phases> progressListener;
    private long lsnBatchSize = Long.MAX_VALUE;
    private long internalMemoryLimit = Long.MAX_VALUE;

    /**
     * Default configuration used if null is passed to {@link
     * com.sleepycat.je.Database#preload Database.preload}.
     */
    public PreloadConfig() {
    }

    /**
     * Configure the maximum number of bytes to preload.
     *
     * <p>The default is 0 for this class.</p>
     *
     * @param maxBytes If the maxBytes parameter is non-zero, a preload will
     * stop when the cache contains this number of bytes.
     *
     * @return this
     */
    public PreloadConfig setMaxBytes(final long maxBytes) {
        setMaxBytesVoid(maxBytes);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxBytesVoid(final long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /**
     * Return the number of bytes in the cache to stop the preload at.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return The number of bytes in the cache to stop the preload at.
     */
    public long getMaxBytes() {
        return maxBytes;
    }

    /**
     * Configure the maximum number of milliseconds to execute preload.
     *
     * <p>The default is 0 for this class.</p>
     *
     * @param maxMillisecs If the maxMillisecs parameter is non-zero, a preload
     * will stop when this amount of time has passed.
     *
     * @return this
     */
    public PreloadConfig setMaxMillisecs(final long maxMillisecs) {
        setMaxMillisecsVoid(maxMillisecs);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxMillisecsVoid(final long maxMillisecs) {
        this.maxMillisecs = maxMillisecs;
    }

    /**
     * Return the number of millisecs to stop the preload after.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return The number of millisecs to stop the preload after.
     */
    public long getMaxMillisecs() {
        return maxMillisecs;
    }

    /**
     * Configure the preload load LNs option.
     *
     * <p>The default is false for this class.</p>
     *
     * @param loadLNs If set to true, the preload will load Leaf Nodes (LNs)
     * containing the data values.
     *
     * @return this
     */
    public PreloadConfig setLoadLNs(final boolean loadLNs) {
        setLoadLNsVoid(loadLNs);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setLoadLNsVoid(final boolean loadLNs) {
        this.loadLNs = loadLNs;
    }

    /**
     * Return the configuration of the preload load LNs option.
     *
     * @return The configuration of the preload load LNs option.
     */
    public boolean getLoadLNs() {
        return loadLNs;
    }

    /**
     * Preload progress listeners report this phase value, along with a
     * count of the number if times that the preload has fetched from disk.
     */
    public static enum Phases { 
        /**
         * Preload is in progress and resulted in a fetch from disk.
         */
        PRELOAD };

    /**
     * Configure the preload operation to make periodic calls to a {@link
     * ProgressListener} to provide feedback on preload progress.
     * The ProgressListener.progress() method is called each time the preload
     * mush fetch a btree node or data record from disk. 
     * <p>
     * When using progress listeners, review the information at {@link
     * ProgressListener#progress} to avoid any unintended disruption to
     * replication stream syncup.
     * @param progressListener The ProgressListener to callback during 
     * preload.
     */
    public PreloadConfig
        setProgressListener(final ProgressListener<Phases> progressListener) {
        setProgressListenerVoid(progressListener);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setProgressListenerVoid
        (final ProgressListener<Phases> progressListener) {
        this.progressListener = progressListener;
    }

    /**
     * Return the ProgressListener for this PreloadConfig.
     *
     * @return the ProgressListener for this PreloadConfig.
     */
    public ProgressListener<Phases> getProgressListener() {
        return progressListener;
    }

    /**
     * Set the maximum number of LSNs to gather and sort at any one time.  The
     * default is an unlimited number of LSNs.  Setting this lower causes the
     * preload to use less memory, but it sorts and processes LSNs more
     * frequently thereby causing slower performance.  Setting this higher will
     * in general improve performance at the expense of memory.  Each LSN uses
     * 16 bytes of memory.
     *
     * @param lsnBatchSize the maximum number of LSNs to accumulate and sort
     * per batch.
     *
     * @return this
     */
    public PreloadConfig setLSNBatchSize(final long lsnBatchSize) {
        setLSNBatchSizeVoid(lsnBatchSize);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setLSNBatchSizeVoid(final long lsnBatchSize) {
        this.lsnBatchSize = lsnBatchSize;
    }

    /**
     * Preload is implemented to optimize I/O cost by fetching the records of 
     * a Database by disk order, so that disk access is are sequential rather 
     * than random. LSNs (log sequence numbers) are the disk addresses of
     * database records. Setting this value causes the preload to process
     * batches of LSNs rather than all in-memory LSNs at one time, 
     * which bounds the memory usage of 
     * the preload processing, at the expense of preload performance.
     *
     * @return the maximum number of LSNs to be sorted that this
     * preload is configured for.
     */
    public long getLSNBatchSize() {
        return lsnBatchSize;
    }

    /**
     * Set the maximum amount of non JE Cache Memory that preload can use at
     * one time.  The default is an unlimited amount of memory.  Setting this
     * lower causes the preload to use less memory, but generally results in 
     * slower performance. Setting this higher will often improve performance 
     * at the expense of higher memory utilization.
     *
     * @param internalMemoryLimit the maximum number of non JE Cache bytes to
     * use.
     *
     * @return this
     */
    public PreloadConfig
        setInternalMemoryLimit(final long internalMemoryLimit) {
        setInternalMemoryLimitVoid(internalMemoryLimit);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setInternalMemoryLimitVoid(final long internalMemoryLimit) {
        this.internalMemoryLimit = internalMemoryLimit;
    }

    /**
     * Returns the maximum amount of non JE Cache Memory that preload can use at
     * one time.
     *
     * @return the maximum amount of non JE Cache Memory that preload can use at
     * one time.
     */
    public long getInternalMemoryLimit() {
        return internalMemoryLimit;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public PreloadConfig clone() {
        try {
            return (PreloadConfig) super.clone();
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
        return "maxBytes=" + maxBytes +
            "\nmaxMillisecs=" + maxMillisecs +
            "\nloadLNs=" + loadLNs +
            "\nlsnBatchSize=" + lsnBatchSize +
            "\ninternalMemoryLimit=" + internalMemoryLimit +
            "\n";
    }
}
