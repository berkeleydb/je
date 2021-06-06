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
 * Unwraps nested exceptions by calling the {@link
 * ExceptionWrapper#getCause()} method for exceptions that implement the
 * {@link ExceptionWrapper} interface.  Does not currently support the Java 1.4
 * <code>Throwable.getCause()</code> method.
 *
 * @author Mark Hayes
 */
public class ExceptionUnwrapper {

    /**
     * Unwraps an Exception and returns the underlying Exception, or throws an
     * Error if the underlying Throwable is an Error.
     *
     * @param e is the Exception to unwrap.
     *
     * @return the underlying Exception.
     *
     * @throws Error if the underlying Throwable is an Error.
     *
     * @throws IllegalArgumentException if the underlying Throwable is not an
     * Exception or an Error.
     */
    public static Exception unwrap(Exception e) {

        Throwable t = unwrapAny(e);
        if (t instanceof Exception) {
            return (Exception) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new IllegalArgumentException("Not Exception or Error: " + t);
        }
    }

    /**
     * Unwraps an Exception and returns the underlying Throwable.
     *
     * @param e is the Exception to unwrap.
     *
     * @return the underlying Throwable.
     */
    public static Throwable unwrapAny(Throwable e) {

        while (true) {
            if (e instanceof ExceptionWrapper) {
                Throwable e2 = ((ExceptionWrapper) e).getCause();
                if (e2 == null) {
                    return e;
                } else {
                    e = e2;
                }
            } else {
                return e;
            }
        }
    }
}
