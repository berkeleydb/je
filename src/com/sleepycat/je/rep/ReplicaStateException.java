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


/**
 * This exception indicates that the application attempted an operation that is
 * not permitted when it is in the {@link ReplicatedEnvironment.State#REPLICA}
 * state.
 */
public class ReplicaStateException extends StateChangeException {
    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public ReplicaStateException(String message) {
        super(message, null);
    }

    private ReplicaStateException(String message,
                                  ReplicaStateException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public ReplicaStateException wrapSelf(String msg) {
        return new ReplicaStateException(msg, this);
    }
}
