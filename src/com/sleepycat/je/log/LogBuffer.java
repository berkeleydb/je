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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchFactory;
import com.sleepycat.je.utilint.DbLsn;

/**
 * LogBuffers hold outgoing, newly written log entries.
 * Space is allocated via the allocate() method that
 * returns a LogBufferSegment object. The LogBuffer.writePinCount
 * is incremented each time space is allocated. Once the
 * caller copies data into the log buffer, the
 * pin count is decremented via the free() method.
 * Readers of a log buffer wait until the pin count
 * is zero.
 *
 * The pin count is incremented under the readLatch. The
 * pin count is decremented without holding the latch.
 * Holding the readLatch will prevent the pin count from
 * being incremented.
 *
 * Apart from the pin count, access to the buffer is protected by the
 * readLatch and the LWL:
 * - Write access requires holding both the LWL and the readLatch.
 * - Read access requires holding either the LWL or the readLatch.
 *
 * Of course, for buffers outside the buffer pool, or in the process of being
 * constructed, these rules do not apply and no latching is necessary.
 *
 * TODO:
 * Although the above statement about latching reflects the current
 * implementation, it would be better if we can remove the reliance on the LWL
 * and protect all access to the buffer using the readLatch. To do this, the
 * callers of getFirstLsn and hasRoom will have to acquire the readLatch.
 *
 * @see LogBufferPool
 */
public class LogBuffer implements LogSource {

    private static final String DEBUG_NAME = LogBuffer.class.getName();

    /* Storage */
    private final ByteBuffer buffer;

    /* Information about what log entries are held here. */
    private long firstLsn;
    private long lastLsn;

    /*
     * The read latch protects all modifications to the buffer, and protects
     * read access to the buffer when the LWL is not held. Decrementing the pin
     * count is the only exception, and this can be done with no latching.
     */
    private Latch readLatch;

    /*
     * Buffer may be rewritten because an IOException previously occurred.
     */
    private boolean rewriteAllowed;

    private AtomicInteger writePinCount = new AtomicInteger();
    private byte[] data;
    private EnvironmentImpl env;

    LogBuffer(int capacity, EnvironmentImpl env)
        throws DatabaseException {

        data = new byte[capacity];
        buffer = ByteBuffer.wrap(data);
        readLatch = LatchFactory.createExclusiveLatch(
            env, DEBUG_NAME, false /*collectStats*/);
        this.env = env;
        reinit();
    }

    /*
     * Used by LogManager for the case when we have a temporary buffer in hand
     * and no LogBuffers in the LogBufferPool are large enough to hold the
     * current entry being written.  We just wrap the temporary ByteBuffer
     * in a LogBuffer and pass it to FileManager. [#12674].
     */
    LogBuffer(ByteBuffer buffer, long firstLsn) {
        this.buffer = buffer;
        this.firstLsn = firstLsn;
        this.lastLsn = firstLsn;
        rewriteAllowed = false;
    }

    /**
     * The LWL and buffer pool latch must be held.
     */
    void reinit()
        throws DatabaseException {

        readLatch.acquireExclusive();
        buffer.clear();
        firstLsn = DbLsn.NULL_LSN;
        lastLsn = DbLsn.NULL_LSN;
        rewriteAllowed = false;
        writePinCount.set(0);
        readLatch.release();
    }

    /*
     * Write support
     */

    /**
     * Return first LSN held in this buffer.
     *
     * The LWL or readLatch must be held.
     */
    public long getFirstLsn() {
        return firstLsn;
    }

    /**
     * Register the LSN for a buffer segment that has been allocated in this
     * buffer.
     *
     * The LWL and readLatch must be held.
     */
    void registerLsn(long lsn) {
        assert readLatch.isExclusiveOwner();

        if (lastLsn != DbLsn.NULL_LSN) {
            assert (DbLsn.compareTo(lsn, lastLsn) > 0):
                "lsn=" + lsn + " lastlsn=" + lastLsn;
        }

        lastLsn = lsn;

        if (firstLsn == DbLsn.NULL_LSN) {
            firstLsn = lsn;
        }
    }

    /**
     * Check capacity of buffer.
     *
     * The LWL or readLatch must be held.
     *
     * @return true if this buffer can hold this many more bytes.
     */
    boolean hasRoom(int numBytes) {
        return (numBytes <= (buffer.capacity() - buffer.position()));
    }

    /**
     * Returns the buffer for read access (although some tests may write to the
     * buffer).
     *
     * The LWL or readLatch must be held.
     *
     * @return the actual data buffer.
     */
    public ByteBuffer getDataBuffer() {
        return buffer;
    }

    /**
     * The LWL or readLatch must be held.
     *
     * @return capacity in bytes
     */
    int getCapacity() {
        return buffer.capacity();
    }

    /*
     * Read support
     */

    /**
     * Support for reading out of a still-in-memory log.  Can be used to
     * determine if a log entry with a given LSN is contained in this buffer,
     * or whether an arbitrary LSN location is present in the buffer.
     *
     * No latches need be held. The buffer is latched for read if true is
     * returned.
     *
     * This method must wait until the buffer's pin count goes to zero. When
     * writing is active and this is the currentWriteBuffer, it may have to
     * wait until the buffer is full.
     *
     * @return true if this buffer holds the data at this LSN location. If true
     * is returned, the buffer will be latched for read. Returns false if LSN
     * is not here, and releases the read latch.
     */
    boolean containsLsn(long lsn) {
        assert lsn != DbLsn.NULL_LSN;

        /*
         * Latch before we look at the LSNs. We do not have to wait
         * for zero to check the LSN field but need to have the count
         * zero for a reader to read the buffer.
         */
        waitForZeroAndLatch();
        boolean found = false;

        if ((firstLsn != DbLsn.NULL_LSN) &&
            (DbLsn.getFileNumber(firstLsn) == DbLsn.getFileNumber(lsn))) {

            final long fileOffset = DbLsn.getFileOffset(lsn);
            final int contentSize;
            if (buffer.position() == 0) {
                /* Buffer was flipped for reading. */
                contentSize = buffer.limit();
            } else {
                /* Buffer is still being written into. */
                contentSize = buffer.position();
            }
            final long firstLsnOffset = DbLsn.getFileOffset(firstLsn);
            final long lastContentOffset = firstLsnOffset + contentSize;

            if ((firstLsnOffset <= fileOffset) &&
                (lastContentOffset > fileOffset)) {
                found = true;
            }
        }

        if (found) {
            return true;
        } else {
            readLatch.release();
            return false;
        }
    }

    /**
     * Acquires the readLatch, providing exclusive access to the buffer.
     * When modifying the buffer, both the LWL and buffer latch must be held.
     *
     * Note that containsLsn() acquires the latch for reading.
     *
     * Call release() to release the latch.
     *
     * TODO:
     * It would be possible to use a shared buffer latch to allow concurrent
     * access by multiple readers. The access rules for would then be:
     * - Write access requires holding both the LWL and the buffer latch EX.
     * - Read access requires holding either the LWL or the buffer latch SH.
     * Note that LogBufferPool.bumpCurrent calls latchForWrite, but it may
     * actually only need read access.
     */
    public void latchForWrite()
        throws DatabaseException {

        readLatch.acquireExclusive();
    }

    /*
     * LogSource support
     */

    /**
     * Releases the readLatch.
     *
     * @see LogSource#release
     */
    public void release() {
        readLatch.releaseIfOwner();
    }

    boolean getRewriteAllowed() {
        return rewriteAllowed;
    }

    void setRewriteAllowed() {
        rewriteAllowed = true;
    }

    /**
     * Allocate a segment out of the buffer.
     *
     * The LWL and readLatch must be held.
     *
     * @param size of buffer to allocate
     *
     * @return null if not enough room, otherwise a
     *         LogBufferSegment for the data.
     */
    public LogBufferSegment allocate(int size) {
        assert readLatch.isExclusiveOwner();

        if (hasRoom(size)) {
            ByteBuffer buf =
                ByteBuffer.wrap(data, buffer.position(), size);
            buffer.position(buffer.position() + size);
            writePinCount.incrementAndGet();
            return new LogBufferSegment(this, buf);
        }
        return null;
    }

    /**
     * Called with the buffer not latched.
     */
    public void free() {
        writePinCount.decrementAndGet();
    }

    /**
     * Acquire the buffer latched and with the buffer pin count equal to zero.
     */
    public void waitForZeroAndLatch() {
        boolean done = false;
        while (!done) {
            if (writePinCount.get() > 0) {
                LockSupport.parkNanos(this, 100);
                /*
                 * This may be overkill to check if a thread was
                 * interrupted. There should be no interrupt of the
                 * thread pinning and unpinning the buffer.
                 */
                if (Thread.interrupted()) {
                    throw new ThreadInterruptedException(
                        env, "Interrupt during read operation");
                }
            } else {
                readLatch.acquireExclusive();
                if (writePinCount.get() == 0) {
                   done = true;
                } else {
                    readLatch.release();
                }
            }
        }
    }

    /**
     * Make a copy of this buffer (doesn't copy data, only buffer state)
     * and position it to read the requested data.
     *
     * The LWL or readLatch must be held.
     *
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset) {
        ByteBuffer copy = buffer.duplicate();
        copy.position((int) (fileOffset - DbLsn.getFileOffset(firstLsn)));
        return copy;
    }

    /**
     * Same as getBytes(long fileOffset) since buffer should always hold a
     * whole entry.
     *
     * The LWL or readLatch must be held.
     *
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset, int numBytes) {
        return getBytes(fileOffset);
    }

    /**
     * Entries in write buffers are always the current version.
     */
    public int getLogVersion() {
        return LogEntryType.LOG_VERSION;
    }

    @Override
    public String toString() {
        return
            "[LogBuffer firstLsn=" + DbLsn.getNoFormatString(firstLsn) + "]";
    }
}
