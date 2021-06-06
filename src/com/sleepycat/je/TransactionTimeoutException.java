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
 * Thrown when the transaction timeout interval is exceeded. This is normally
 * because the cumulative operation time for the transaction exceeds the
 * timeout, or another transaction or cursor holds a lock for longer than the
 * timeout interval. It may also occur if the application fails to close a
 * cursor, or fails to commit or abort a transaction, since any locks held by
 * the cursor or transaction will be held indefinitely.
 *
 * <p>This exception is not thrown if a deadlock is detected, even if the
 * timeout elapses before the deadlock is broken. If a deadlock is detected,
 * {@link DeadlockException} is always thrown instead.</p>
 *
 * <p>The transaction timeout interval may be set using
 * {@link Transaction#setTxnTimeout}.</p>
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * <p>Normally, applications should catch the base class {@link
 * LockConflictException} rather than catching one of its subclasses.  All lock
 * conflicts are typically handled in the same way, which is normally to abort
 * and retry the transaction.  See {@link LockConflictException} for more
 * information.</p>
 *
 * @since 4.0
 */
public class TransactionTimeoutException extends LockConflictException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public TransactionTimeoutException(Locker locker, String message) {
        super(locker, message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    private TransactionTimeoutException(String message,
                                        TransactionTimeoutException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new TransactionTimeoutException(msg, this);
    }
}
