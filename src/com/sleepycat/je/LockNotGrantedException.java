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

import com.sleepycat.je.txn.Locker;

/**
 * Thrown when a non-blocking operation fails to get a lock, and {@link
 * EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} is set to true.  Non-blocking
 * transactions are configured using {@link TransactionConfig#setNoWait}.
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * <p>For compatibility with JE 3.3 and earlier, {@link
 * LockNotGrantedException} is thrown instead of {@link
 * LockNotAvailableException} when {@link
 * EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} is set to true.  This
 * configuration parameter is false by default.  See {@link
 * EnvironmentConfig#LOCK_OLD_LOCK_EXCEPTIONS} for information on the changes
 * that should be made to all applications that upgrade from JE 3.3 or
 * earlier.</p>
 *
 * <p>Normally, applications should catch the base class {@link
 * LockConflictException} rather than catching one of its subclasses.  All lock
 * conflicts are typically handled in the same way, which is normally to abort
 * and retry the transaction.  See {@link LockConflictException} for more
 * information.</p>
 *
 * @deprecated replaced by {@link LockNotAvailableException}
 */
public class LockNotGrantedException extends DeadlockException {

    private static final long serialVersionUID = 646414701L;

    /*
     * LockNotGrantedException extends DeadlockException in order to support
     * the approach that all application need only handle
     * DeadlockException. The idea is that we don't want an application to fail
     * because a new type of exception is thrown when an operation is changed
     * to non-blocking.
     *
     * Applications that care about LockNotGrantedExceptions can add another
     * catch block to handle it, but otherwise they can be handled the same way
     * as deadlocks.  See SR [#10672]
     */

    /** 
     * For internal use only.
     * @hidden 
     */
    public LockNotGrantedException(Locker locker, String message) {
        /* Do not set abort-only for a no-wait lock failure. */
        super(message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private LockNotGrantedException(String message,
                                    LockNotGrantedException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new LockNotGrantedException(msg, this);
    }
}
