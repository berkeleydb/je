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

import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Thrown when an {@code IOException} or other failure occurs when writing to
 * the JE log.  This exception may be indicative of a full disk, although an
 * {@code IOException} does not contain enough information to determine this
 * definitively.
 * 
 * <p>This exception may be thrown as the result of any write operation,
 * including record writes, checkpoints, etc.</p>
 *
 * <p>Existing {@link Environment} handles are invalidated as a result of this
 * exception.</p>
 *
 * @since 4.0
 */
public class LogWriteException extends EnvironmentFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public LogWriteException(EnvironmentImpl envImpl, String message) {
        super(envImpl, EnvironmentFailureReason.LOG_WRITE, message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    public LogWriteException(EnvironmentImpl envImpl, Throwable t) {
        super(envImpl, EnvironmentFailureReason.LOG_WRITE, t);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    public LogWriteException(EnvironmentImpl envImpl,
                             String message,
                             Throwable t) {
        super(envImpl, EnvironmentFailureReason.LOG_WRITE, message, t);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private LogWriteException(String message,
                              LogWriteException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public EnvironmentFailureException wrapSelf(String msg) {
        return new LogWriteException(msg, this);
    }
}
