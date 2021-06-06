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
 * A class has been changed incompatibly and no mutation has been configured to
 * handle the change or a new class version number has not been assigned.
 *
 * <!-- begin JE only -->
 * <p>In a replicated environment, this exception is also thrown when upgrading
 * an application (persistent classes have been changed) and an upgraded node
 * is elected Master before all of the Replica nodes have been upgraded.  See
 * <a href="package-summary.html#repUpgrade">Upgrading a Replication Group</a>
 * for more information.</p>
 * <!-- end JE only -->
 *
 * @see com.sleepycat.persist.EntityStore#EntityStore EntityStore.EntityStore
 * @see com.sleepycat.persist.model.Entity#version
 * @see com.sleepycat.persist.model.Persistent#version
 *
 * @see com.sleepycat.persist.evolve Class Evolution
 * @author Mark Hayes
 */
public class IncompatibleClassException extends OperationFailureException {

    private static final long serialVersionUID = 2103957824L;

    public IncompatibleClassException(String message) {
        super(message);
    }

    /* <!-- begin JE only --> */

    /** 
     * For internal use only.
     * @hidden 
     */
    private IncompatibleClassException(String message,
                                       OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(String msg) {
        return new IncompatibleClassException(msg, this);
    }

    /* <!-- end JE only --> */
}
