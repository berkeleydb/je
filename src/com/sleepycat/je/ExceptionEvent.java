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

import com.sleepycat.je.utilint.LoggerUtils;

/**
 * A class representing an exception event.  Contains an exception and the name
 * of the daemon thread that it was thrown from.
 */
public class ExceptionEvent {

    private Exception exception;
    private String threadName;

    public ExceptionEvent(Exception exception, String threadName) {
        this.exception = exception;
        this.threadName = threadName;
    }

    public ExceptionEvent(Exception exception) {
        this.exception = exception;
        this.threadName = Thread.currentThread().toString();
    }

    /**
     * Returns the exception in the event.
     */
    public Exception getException() {
        return exception;
    }

    /**
     * Returns the name of the daemon thread that threw the exception.
     */
    public String getThreadName() {
        return threadName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("<ExceptionEvent exception=\"");
        sb.append(exception);
        sb.append("\" threadName=\"");
        sb.append(threadName);
        sb.append("\">");
        sb.append(LoggerUtils.getStackTrace(exception));
        return sb.toString();
    }
}
