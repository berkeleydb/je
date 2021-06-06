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

package com.sleepycat.persist;

import com.sleepycat.je.OperationFailureException;

/**
 * Thrown by the {@link EntityStore} constructor when the {@link
 * StoreConfig#setAllowCreate AllowCreate} configuration parameter is false and
 * the store's internal catalog database does not exist.
 *
 * @author Mark Hayes
 */
public class StoreNotFoundException extends OperationFailureException {

    private static final long serialVersionUID = 1895430616L;

    /** 
     * For internal use only.
     * <!-- begin JE only -->
     * @hidden 
     * <!-- end JE only -->
     */
    public StoreNotFoundException(String message) {
        super(message);
    }

    /* <!-- begin JE only --> */

    /** 
     * For internal use only.
     * <!-- begin JE only -->
     * @hidden 
     * <!-- end JE only -->
     */
    private StoreNotFoundException(String message,
                                   OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * <!-- begin JE only -->
     * @hidden 
     * <!-- end JE only -->
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new StoreNotFoundException(msg, this);
    }

    /* <!-- end JE only --> */
}
