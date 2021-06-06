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

package com.sleepycat.je.rep.util.ldiff;

import com.sleepycat.je.OperationFailureException;

/**
 * Thrown when LDiff requesting records on remote database fails.
 */
public class LDiffRecordRequestException extends OperationFailureException {

    private static final long serialVersionUID = 1925430616L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public LDiffRecordRequestException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    private LDiffRecordRequestException(String message,
                                        LDiffRecordRequestException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new LDiffRecordRequestException(msg, this);
    }
}
