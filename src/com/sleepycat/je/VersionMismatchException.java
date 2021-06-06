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
 * Thrown by the Environment constructor when an environment cannot be
 * opened because the version of the existing log is not compatible with the
 * version of JE that is running.  This occurs when a later version of JE was
 * used to create the log.
 *
 * <p><strong>Warning:</strong> This exception should be handled when more than
 * one version of JE may be used to access an environment.</p>
 *
 * @since 4.0
 */
public class VersionMismatchException extends EnvironmentFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public VersionMismatchException(EnvironmentImpl envImpl, String message) {
        super(envImpl, EnvironmentFailureReason.VERSION_MISMATCH, message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private VersionMismatchException(String message,
                                     VersionMismatchException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public EnvironmentFailureException wrapSelf(String msg) {
        return new VersionMismatchException(msg, this);
    }
}
