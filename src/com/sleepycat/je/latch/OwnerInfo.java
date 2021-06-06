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

import com.sleepycat.je.utilint.LoggerUtils;

class OwnerInfo {

    private final Thread thread;
    private final long acquireTime;
    private final Throwable acquireStack;

    OwnerInfo(final LatchContext context) {
        thread = Thread.currentThread();
        acquireTime = System.currentTimeMillis();
        acquireStack =
            new Exception("Latch Acquired: " + context.getLatchName());
    }

    void toString(StringBuilder builder) {
        builder.append(" captureThread: ");
        builder.append(thread);
        builder.append(" acquireTime: ");
        builder.append(acquireTime);
        if (acquireStack != null) {
            builder.append("\n");
            builder.append(LoggerUtils.getStackTrace(acquireStack));
        } else {
            builder.append(" -no stack-");
        }
    }
}
