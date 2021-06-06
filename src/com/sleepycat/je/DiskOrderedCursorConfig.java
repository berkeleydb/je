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
 * Specifies the attributes of a DiskOrderedCursor.
 * @since 5.0
 */
public class DiskOrderedCursorConfig implements Cloneable {

    /**
     * Default configuration used if null is passed to methods that create a
     * cursor.
     */
    public static final DiskOrderedCursorConfig DEFAULT =
        new DiskOrderedCursorConfig();

    private boolean binsOnly = false;

    private boolean keysOnly = false;

    private boolean countOnly = false;

    private long lsnBatchSize = Long.MAX_VALUE;

    private long internalMemoryLimit = Long.MAX_VALUE;

    private int queueSize = 1000;

    private boolean serialDBScan = false;

    private boolean debug = false;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public DiskOrderedCursorConfig() {
    }

    /**
     * Specify whether the DiskOrderedCursor should return only the key or key
     * + data.  The default value is false (key + data).  If keyOnly is true,
     * the performance of the disk ordered scan will be better, because the
     * Cursor only descends to the BIN level. 
     *
     * @param keysOnly If true, return only keys from this cursor.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setKeysOnly(final boolean keysOnly) {
        setKeysOnlyVoid(keysOnly);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setKeysOnlyVoid(final boolean keysOnly) {
        this.keysOnly = keysOnly;
    }

    /**
     * Returns true if the DiskOrderedCursor is configured to return only
     * keys.  Returns false if it is configured to return keys + data.
     *
     * @return true if the DiskOrderedCursor is configured to return keys only.
     */
    public boolean getKeysOnly() {
        return keysOnly;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public DiskOrderedCursorConfig setCountOnly(boolean val) {
        setBINsOnlyVoid(val);
        return this;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public void setCountOnlyVoid(boolean val) {
        countOnly = val;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public boolean getCountOnly() {
        return countOnly;
    }

    /**
     * Specify whether the DiskOrderedCursor should scan the BINs only. If
     * true, the performance of the disk ordered scan will be better, because
     * LNs are not read from disk. However, in this case, the data portion
     * of a record will be returned only if it is embedded in the BIN;
     * otherwise only the key will be returned.
     *
     * @param binsOnly If true, return keys and, if available, the associated
     * embedded data.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setBINsOnly(final boolean binsOnly) {
        setBINsOnlyVoid(binsOnly);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setBINsOnlyVoid(final boolean binsOnly) {
        this.binsOnly = binsOnly;
    }

    /**
     * Returns true if the DiskOrderedCursor is configured to scan BINs only,
     * returning all record keys and only those record data that are embedded
     * in the BINs.
     *
     * @return true if the DiskOrderedCursor is configured to scan BINs only.
     */
    public boolean getBINsOnly() {
        return binsOnly;
    }

    /**
     * Set the maximum number of LSNs to gather and sort at any one time.  The
     * default is an unlimited number of LSNs.  Setting this lower causes the
     * DiskOrderedScan to use less memory, but it sorts and processes LSNs more
     * frequently thereby causing slower performance.  Setting this higher will
     * in general improve performance at the expense of memory.  Each LSN uses
     * 16 bytes of memory.
     *
     * @param lsnBatchSize the maximum number of LSNs to accumulate and sort
     * per batch.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setLSNBatchSize(final long lsnBatchSize) {
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
     * Returns the maximum number of LSNs to be sorted that this
     * DiskOrderedCursor is configured for.
     *
     * @return the maximum number of LSNs to be sorted that this
     * DiskOrderedCursor is configured for.
     */
    public long getLSNBatchSize() {
        return lsnBatchSize;
    }

    /**
     * Set the maximum amount of JE Cache Memory that the DiskOrderedScan
     * can use at one time.  The default is an unlimited amount of memory.
     * Setting this lower causes the DiskOrderedScan to use less memory, but it
     * sorts and processes LSNs more frequently thereby generally causing slower
     * performance.  Setting this higher will in general improve performance at
     * the expense of JE cache memory.
     *
     * @param internalMemoryLimit the maximum number of non JE Cache bytes to
     * use.
     *
     * @return this
     *
     * @see <a href="EnvironmentStats.html#cacheUnexpectedSizes">Cache
     * Statistics: Unexpected Sizes</a>
     */
    public DiskOrderedCursorConfig setInternalMemoryLimit(
        final long internalMemoryLimit) {
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
     * Returns the maximum amount of JE Cache Memory that the
     * DiskOrderedScan can use at one time.
     *
     * @return the maximum amount of non JE Cache Memory that preload can use at
     * one time.
     */
    public long getInternalMemoryLimit() {
        return internalMemoryLimit;
    }

    /**
     * Set the queue size for entries being passed between the
     * DiskOrderedCursor producer thread and the application's consumer
     * thread. If the queue size reaches this number of entries, the producer
     * thread will block until the application thread removes one or more
     * entries (by calling ForwardCursor.getNext().  The default is 1000.
     *
     * @param queueSize the maximum number of entries the queue can hold before
     * the producer thread blocks.
     *
     * @return this
     */
    public DiskOrderedCursorConfig setQueueSize(final int queueSize) {
        setQueueSizeVoid(queueSize);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setQueueSizeVoid(final int queueSize) {
        this.queueSize = queueSize;
    }

    /**
     * Returns the maximum number of entries in the queue before the
     * DiskOrderedCursor producer thread blocks.
     *
     * @return the maximum number of entries in the queue before the
     * DiskOrderedCursor producer thread blocks.
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * @deprecated this method has no effect and will be removed in a future
     * release.
     */
    public DiskOrderedCursorConfig setMaxSeedMillisecs(
        final long maxSeedMillisecs) {
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxSeedMillisecsVoid(final long maxSeedMillisecs) {
    }

    /**
     * @deprecated this method returns zero and will be removed in a future
     * release.
     */
    public long getMaxSeedMillisecs() {
        return 0;
    }

    /**
     * @deprecated this method has no effect and will be removed in a future
     * release.
     */
    public DiskOrderedCursorConfig setMaxSeedNodes(final long maxSeedNodes) {
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxSeedNodesVoid(final long maxSeedNodes) {
    }

    /**
     * @deprecated this method returns zero and will be removed in a future
     * release.
     */
    public long getMaxSeedNodes() {
        return 0;
    }

    /**
     * @hidden
     */
    public DiskOrderedCursorConfig setSerialDBScan(final boolean v) {
        setSerialDBScanVoid(v);
        return this;
    }

    /**
     * @hidden
     */
    public void setSerialDBScanVoid(final boolean v) {
        this.serialDBScan = v;
    }

    /**
     * @hidden
     */
    public boolean getSerialDBScan() {
        return this.serialDBScan;
    }

    /**
     * @hidden
     */
    public DiskOrderedCursorConfig setDebug(final boolean v) {
        setDebugVoid(v);
        return this;
    }

    /**
     * @hidden
     */
    public void setDebugVoid(final boolean v) {
        this.debug = v;
    }

    /**
     * @hidden
     */
    public boolean getDebug() {
        return this.debug;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public DiskOrderedCursorConfig clone() {
        try {
            return (DiskOrderedCursorConfig) super.clone();
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
        return "keysOnly=" + keysOnly +
            "\nlsnBatchSize=" + lsnBatchSize +
            "\ninternalMemoryLimit=" + internalMemoryLimit +
            "\nqueueSize=" + queueSize;
    }
}
