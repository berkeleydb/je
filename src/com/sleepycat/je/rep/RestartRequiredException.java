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

package com.sleepycat.je.rep;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * RestartRequiredException serves as the base class for all exceptions which
 * makes it impossible for HA to proceed without some form of corrective action
 * on the part of the user, followed by a restart of the application. The
 * corrective action may involve an increase in resources used by the
 * application, a JE configurations change, discarding cached state, etc. The
 * error message details the nature of the problem.
 */
public abstract class RestartRequiredException 
    extends EnvironmentFailureException {

    /*
     * Classes that extend RestartRequiredException should be aware that their
     * constructors should not be seen as atomic. If the failure reason
     * mandates it, the environment may be invalidated by the super class
     * constructor, EnvironmentFailureException. At invalidation time, the
     * exception is saved within the environment as the precipitating failure,
     * and may be seen and used by other threads, and the sub class instance
     * may be seen before construction is complete. The subclass should take
     * care if it has any fields that are initialized in the constructor, after
     * the call to super().
     *
     * Any overloadings of getMessage() should also assume that they may be
     * called asynchronously before the subclass is fully initialized.
     */

    private static final long serialVersionUID = 1;

    public RestartRequiredException(EnvironmentImpl envImpl,
                                    EnvironmentFailureReason reason) {
        super(envImpl, reason);
    }

    public RestartRequiredException(EnvironmentImpl envImpl,
                                    EnvironmentFailureReason reason,
                                    Exception cause) {
        super(envImpl, reason, cause);
    }

    public RestartRequiredException(EnvironmentImpl envImpl,
                                    EnvironmentFailureReason reason,
                                    String msg) {
        super(envImpl, reason, msg);
    }
    
    /**
     * For internal use only.
     */
    protected RestartRequiredException(String message,
                                       RestartRequiredException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public abstract EnvironmentFailureException wrapSelf(String msg) ;
}
