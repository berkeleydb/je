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


/*
 * Singleton class to indicate that something needs to be relatched for
 * exclusive access due to a fetch occurring.
 */
@SuppressWarnings("serial")
public class RelatchRequiredException extends Exception {
    public static RelatchRequiredException relatchRequiredException =
        new RelatchRequiredException();

    private RelatchRequiredException() {
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
