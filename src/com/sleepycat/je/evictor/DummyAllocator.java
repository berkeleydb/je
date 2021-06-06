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
 * Allocator that always fails to allocate.
 */
class DummyAllocator implements OffHeapAllocator {

    static final DummyAllocator INSTANCE = new DummyAllocator();

    private DummyAllocator() {
    }

    @Override
    public void setMaxBytes(long maxBytes) {
    }

    @Override
    public long getUsedBytes() {
        return 0;
    }

    @Override
    public long allocate(int size) {
        return 0;
    }

    @Override
    public int free(long memId) {
        return 0;
    }

    @Override
    public int size(long memId) {
        return 0;
    }

    @Override
    public int totalSize(long memId) {
        return 0;
    }

    @Override
    public void copy(long memId, int memOff, byte[] buf, int bufOff, int len) {
    }

    @Override
    public void copy(byte[] buf, int bufOff, long memId, int memOff, int len) {
    }

    @Override
    public void copy(
        long fromMemId, int fromMemOff, long toMemId, int toMemOff, int len) {
    }
}
