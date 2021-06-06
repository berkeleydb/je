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
 * Thrown by the {@link EntityStore#getPrimaryIndex getPrimaryIndex}, {@link
 * EntityStore#getSecondaryIndex getSecondaryIndex} and {@link
 * EntityStore#getSubclassIndex getSubclassIndex} when an index has not yet
 * been created.
 *
 * <!-- begin JE only -->
 * This exception can be thrown in two circumstances.
 * <ol>
 * <li>It can be thrown in a replicated environment when the Replica has been
 * upgraded to contain new persistent classes that define a new primary or
 * secondary index, but the Master has not yet been upgraded.  The index does
 * not exist because the Master has not yet been upgraded with the new classes.
 * If the application is aware of when the Master is upgraded, it can wait for
 * that to occur and then open the index. Or, the application may repeatedly
 * try to open the index until it becomes available.</li>
 * <li>
 * <!-- end JE only -->
 * <p>It can be thrown when opening an environment read-only with new
 * persistent classes that define a new primary or secondary index.  The index
 * does not exist because the environment has not yet been opened read-write
 * with the new classes.  When the index is created by a read-write
 * application, the read-only application must close and re-open the
 * environment in order to open the new index.</p>
 * <!-- begin JE only -->
 * </li>
 * </ol>
 * <!-- end JE only -->
 *
 * @author Mark Hayes
 */
public class IndexNotAvailableException extends OperationFailureException {

    private static final long serialVersionUID = 1L;

    /** 
     * For internal use only.
     * <!-- begin JE only -->
     * @hidden
     * <!-- end JE only -->
     *
     * @param message the message.
     */
    public IndexNotAvailableException(String message) {
        super(message);
    }

    /* <!-- begin JE only --> */

    /** 
     * For internal use only.
     * @hidden 
     */
    private IndexNotAvailableException(String message,
                                   OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new IndexNotAvailableException(msg, this);
    }

    /* <!-- end JE only --> */
}
