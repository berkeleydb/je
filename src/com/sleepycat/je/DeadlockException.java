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
 * Thrown when a deadlock is detected. When this exception is thrown, JE
 * detected a deadlock and chose one transaction (or non-transactional
 * operation), the "victim", to invalidate in order to break the deadlock.
 * Note that this is different than a {@link LockTimeoutException lock
 * timeout} or {@link TransactionTimeoutException}, which occur for other
 * reasons.
 *
 * <p>For more information on deadlock detection, see
 * {@link EnvironmentConfig#LOCK_DEADLOCK_DETECT}. As described there, a
 * {@code DeadlockException} is normally thrown when a random victim is
 * selected; in this case the exception message will contain the string:
 * {@code This locker was chosen randomly as the victim}. If the deadlock
 * exception is thrown in a non-victim thread, due to live lock or an
 * unresponsive thread, the message will contain the string:
 * {@code Unable to break deadlock using random victim selection within the
 * timeout interval}.</p>
 *
 * <p>TODO: describe how to debug using info included with the exception.</p>
 *
 * <p>Normally, applications should catch the base class {@link
 * LockConflictException} rather than catching one of its subclasses.  All lock
 * conflicts are typically handled in the same way, which is normally to abort
 * and retry the transaction.  See {@link LockConflictException} for more
 * information.</p>
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 */
public class DeadlockException extends LockConflictException {

    private static final long serialVersionUID = 729943514L;

    /** 
     * For internal use only.
     * @hidden 
     */
    DeadlockException(String message) {
        super(message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    public DeadlockException(Locker locker, String message) {
        super(locker, message);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    DeadlockException(String message,
                      DeadlockException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new DeadlockException(msg, this);
    }
}
