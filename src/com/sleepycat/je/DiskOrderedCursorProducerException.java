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
 * Thrown by {@link ForwardCursor#getNext ForwardCursor.getNext} when a
 * {@link DiskOrderedCursor} producer thread throws an exception.
 * This exception wraps that thrown exception;
 *
 * @since 5.0
 */
public class DiskOrderedCursorProducerException
    extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public DiskOrderedCursorProducerException(String message, Throwable cause) {
        super(null /*locker*/, false /*abortOnly*/, message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new DiskOrderedCursorProducerException(msg, this);
    }
}
