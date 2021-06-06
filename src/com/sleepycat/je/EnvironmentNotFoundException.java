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
 * Thrown by the {@link Environment} constructor when {@code EnvironmentConfig
 * AllowCreate} property is false (environment creation is not permitted), but
 * there are no log files in the environment directory.
 *
 * @since 4.0
 */
public class EnvironmentNotFoundException extends EnvironmentFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public EnvironmentNotFoundException(EnvironmentImpl envImpl,
                                        String message) {
        super(envImpl, EnvironmentFailureReason.ENV_NOT_FOUND, message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private EnvironmentNotFoundException(String message,
                                         EnvironmentNotFoundException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public EnvironmentFailureException wrapSelf(String msg) {
        return new EnvironmentNotFoundException(msg, this);
    }
}
