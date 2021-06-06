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

import com.sleepycat.je.OperationFailureException;

/**
 * Thrown when an operation requires a replication group member and that member
 * is not present in the replication group.
 */
public class MemberNotFoundException extends OperationFailureException {
    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public MemberNotFoundException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     */
    private MemberNotFoundException(String message,
                                      MemberNotFoundException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new MemberNotFoundException(msg, this);
    }
}
