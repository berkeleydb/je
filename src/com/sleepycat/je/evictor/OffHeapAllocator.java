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

/**
 * Implemented by off-heap memory allocators.
 *
 * The allocator is responsible for allocating and freeing a block of memory
 * efficiently, maintaining the size of a block, identifying a block by a long
 * integer value (ID), and looking up a memory ID efficiently (in constant
 * time) in order to copy bytes, return the size, or free the block.
 * <p>
 * The allocator is also responsible for compacting memory when necessary to
 * perform allocations efficiently. A special case is when the off-heap cache
 * is reduced in size, and memory should be compacted to make memory available
 * to the OS; the implementation should account for this case.
 * <p>
 * Another responsibility of the allocator is to estimate the RAM usage for all
 * blocks currently allocated, including overhead and space taken by
 * fragmentation. It is recognized that this may only be a rough estimate in
 * some implementations (the default allocator, for example). See
 * {@link #getUsedBytes}.
 * <p>
 * Note that with the default allocator, the size is not a built-in property
 * of each block, and the {@link #size} method will be implemented by storing
 * the size at the front of the block. The {@code size} method is included in
 * the interface to allow for implementations where a block size property is
 * naturally available.
 * <p>
 * This interface requires that memory is copied in and out of the Java address
 * space to make use of the off-heap cache. A future enhancement might involve
 * adding a way to obtain a ByteBuffer for direct access to the off-heap memory
 * block, to avoid the copy if this is possible in some implementations. In the
 * default implementation, this is not practical without using non-public JVM
 * internals and risking incompatibilities.
 * <p>
 * All methods in the allocator must be thread safe, and contention among
 * threads should be minimized.
 * <p>
 * The memory blocks are not assumed to be fast access RAM and in particular
 * might be NVRAM. JE makes an effort to only copy memory to/from the block
 * when necessary, and to use single larger copy operations rather than
 * multiple smaller copy operations.
 */
public interface OffHeapAllocator {

    class OffHeapOverflowException extends Exception {}

    /**
     * Sets the maximum size of the off-heap cache, to be used as a hint for
     * the creation of implementation specific data structures.
     *
     * The maximum cache size is the amount of RAM that the app would like to
     * use for the off-heap cache, at the gross level of dividing up the RAM on
     * a machine among processes, the off-heap cache, the file system cache,
     * etc. Because there is overhead with any allocation scheme, less bytes
     * will actually be available for memory blocks created with the {@link
     * #allocate) method. In other words, JE will not assume that it can
     * allocate blocks totaling the specified maximum size. See {@link
     * #getUsedBytes }.
     * <p>
     * This method is always called once before any other method is called. It
     * may be called more than once if the off-heap cache is resized by the
     * app.
     *
     * @see #allocate(int)
     */
    void setMaxBytes(long maxBytes);

    /**
     * Returns an estimate of the amount of RAM used by the cache, including
     * the metadata and block overhead used by the implementation, as well as
     * any free space needed for performing compaction.
     *
     * This method should not cause thread contention when called frequently
     * from multiple threads. A volatile long field is the suggested
     * implementation.
     *
     * @see #allocate(int)
     */
    long getUsedBytes();

    /**
     * Allocates a block of a given size and returns its ID.
     *
     * The bytes of the memory block must be initialized to zero.
     * <p>
     * Note that because the used cache size is only an estimate, and in fact
     * the maximum size might not actually be available (due to memory use by
     * other processes when using the default allocator, for example), then the
     * {@link #allocate} method may fail (thrown an exception) even when the
     * used size is less than the maximum bytes (the value passed to {@link
     * #setMaxBytes}). JE handles this situation as follows.
     * <p>
     * JE uses an internal off-heap cache size limit to determine when to
     * perform eviction (which frees off-heap blocks). The limit is
     * initialized to the value passed to {@link #setMaxBytes}. JE calls
     * {@link #getUsedBytes()} to determine when to evict memory from the
     * cache. If the used bytes by grows very close to the limit or exceeds
     * it, JE will perform off-heap cache eviction. JE will make a best effort
     * not to call the {@link #allocate} method when the used size exceeds
     * the limit.
     * <p>
     * If an allocation failure occurs (i.e., this method throws an
     * exception), JE adjusts the limit downward to account for the
     * inaccuracies discussed above. When a RuntimeException is thrown, the
     * limit is set to the used size; when an OutOfMemoryError is thrown, the
     * limit is set to the used size minus the {@link
     * com.sleepycat.je.EnvironmentConfig#OFFHEAP_EVICT_BYTES}. This adjustment
     * should ensure that JE eviction occurs and prevent frequent allocation
     * failures and associated exception handling.
     * TODO: This never happens because Linux kills the process
     * <p>
     *
     * @return non-zero memory ID, or zero when the memory cannot be allocated.
     *
     * @throws OffHeapOverflowException if the block cannot be allocated
     * because the max size has been reached. The internal off-heap cache
     * size limit will be set as described above.
     *
     * @throws OutOfMemoryError if the block cannot be allocated because no
     * system memory is available. The internal off-heap cache size limit will
     * be set as described above. In addition, a SEVERE message for the
     * exception will be logged.
     *
     * @see #getUsedBytes
     */
    long allocate(int size)
        throws OutOfMemoryError, OffHeapOverflowException;

    /**
     * Frees a block previously allocated and returns the size freed, including
     * any overhead for the block that is now free.
     */
    int free(long memId);

    /**
     * Returns the size of an allocated block.
     */
    int size(long memId);

    /**
     * Returns the size of an allocated block plus any overhead for the block.
     */
    int totalSize(long memId);

    /**
     * Copies bytes from an allocated block to a Java byte array.
     */
    void copy(long memId, int memOff, byte[] buf, int bufOff, int len);

    /**
     * Copies bytes from a Java byte array to an allocated block.
     */
    void copy(byte[] buf, int bufOff, long memId, int memOff, int len);

    /**
     * Copies bytes from one allocated block to another.
     */
    void copy(long fromMemId,
              int fromMemOff,
              long toMemId,
              int toMemOff,
              int len);
}
