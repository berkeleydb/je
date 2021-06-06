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
 * This base class of {@link EnvironmentFailureException} is deprecated but
 * exists for API backward compatibility.
 *
 * <p>Prior to JE 4.0, {@code RunRecoveryException} is thrown to indicate that
 * the JE environment is invalid and cannot continue on safely.  Applications
 * catching {@code RunRecoveryException} prior to JE 4.0 were required to close
 * and re-open the {@code Environment}.</p>
 *
 * <p>When using JE 4.0 or later, the application should catch {@link
 * EnvironmentFailureException}. The application should then call {@link
 * Environment#isValid} to determine whether the {@code Environment} must be
 * closed and re-opened, or can continue operating without being closed.  See
 * {@link EnvironmentFailureException}.</p>
 *
 * @deprecated replaced by {@link EnvironmentFailureException} and {@link
 * Environment#isValid}.
 */
@Deprecated
public abstract class RunRecoveryException extends DatabaseException {

    private static final long serialVersionUID = 1913208269L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public RunRecoveryException(String message) {
        super(message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    public RunRecoveryException(String message, Throwable e) {
        super(message, e);
    }
}
