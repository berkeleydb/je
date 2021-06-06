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

package com.sleepycat.util;

/**
 * A RuntimeException that can contain nested exceptions.
 *
 * @author Mark Hayes
 */
public class RuntimeExceptionWrapper extends RuntimeException
    implements ExceptionWrapper {

    /**
     * Wraps the given exception if it is not a {@code RuntimeException}.
     *
     * @param e any exception.
     *
     * @return {@code e} if it is a {@code RuntimeException}, otherwise a
     * {@code RuntimeExceptionWrapper} for {@code e}.
     */
    public static RuntimeException wrapIfNeeded(Throwable e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeExceptionWrapper(e);
    }

    private static final long serialVersionUID = 1106961350L;

    public RuntimeExceptionWrapper(Throwable e) {

        super(e);
    }

    /**
     * @deprecated replaced by {@link #getCause}.
     */
    public Throwable getDetail() {

        return getCause();
    }
}
