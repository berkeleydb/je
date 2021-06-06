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
 * Thrown by {@link Database#openSequence Database.openSequence} if the
 * sequence record already exists and the {@code SequenceConfig
 * ExclusiveCreate} parameter is true.
 *
 * <p>The {@link Transaction} handle is <em>not</em> invalidated as a result of
 * this exception.</p>
 *
 * @since 4.0
 */
public class SequenceExistsException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public SequenceExistsException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private SequenceExistsException(String message,
                                    SequenceExistsException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new SequenceExistsException(msg, this);
    }
}
