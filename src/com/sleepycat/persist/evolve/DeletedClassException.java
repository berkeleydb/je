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

package com.sleepycat.persist.evolve;

import com.sleepycat.je.OperationFailureException;

/**
 * While reading from an index, an instance of a deleted class version was
 * encountered.
 *
 * @see com.sleepycat.persist.evolve Class Evolution
 * @author Mark Hayes
 */
public class DeletedClassException extends OperationFailureException {

    private static final long serialVersionUID = 518500929L;

    /** 
     * For internal use only.
     * <!-- begin JE only -->
     * @hidden 
     * <!-- end JE only -->
     */
    public DeletedClassException(String message) {
        super(message);
    }

    /* <!-- begin JE only --> */

    /** 
     * For internal use only.
     * @hidden 
     */
    private DeletedClassException(String message,
                                  OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new DeletedClassException(msg, this);
    }

    /* <!-- end JE only --> */
}
