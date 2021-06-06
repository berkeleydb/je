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
 * Base class for exceptions thrown when a write operation fails because of a
 * secondary constraint.  See subclasses for more information.
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * @see <a href="SecondaryDatabase.html#transactions">Special considerations
 * for using Secondary Databases with and without Transactions</a>
 *
 * @since 4.0
 */
public abstract class SecondaryConstraintException
    extends SecondaryReferenceException {

    private static final long serialVersionUID = 1L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public SecondaryConstraintException(Locker locker,
                                        String message,
                                        String secDbName,
                                        DatabaseEntry secKey,
                                        DatabaseEntry priKey,
                                        long expirationTime) {
        super(locker, message, secDbName, secKey, priKey, expirationTime);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    SecondaryConstraintException(String message,
                                 SecondaryReferenceException cause) {
        super(message, cause);
    }
}
