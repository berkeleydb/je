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

package com.sleepycat.je.latch;

/**
 * Extends Latch to provide a reader-writer/shared-exclusive latch.  This is
 * implemented with Java's ReentrantReadWriteLock, which is extended for a
 * few reasons (see Latch).
 *
 * This interface may be also be implemented using an underlying exclusive
 * latch.  This is done so that a single interface can be used for for all INs,
 * even though BIN latches are exclusive-only.  See method javadoc for their
 * behavior in exclusive-only mode.
 */
public interface SharedLatch extends Latch {

    /** Returns whether this latch is exclusive-only. */
    boolean isExclusiveOnly();

    /**
     * Acquires a latch for shared/read access.
     *
     * In exclusive-only mode, calling this method is equivalent to calling
     * {@link #acquireExclusive()}.
     */
    void acquireShared();
}
