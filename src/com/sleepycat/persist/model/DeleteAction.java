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

package com.sleepycat.persist.model;


/**
 * Specifies the action to take when a related entity is deleted having a
 * primary key value that exists as a secondary key value for this entity.
 * This can be specified using a {@link SecondaryKey#onRelatedEntityDelete}
 * annotation.
 *
 * @author Mark Hayes
 */
public enum DeleteAction {

    /**
     * The default action, {@code ABORT}, means that an exception is thrown in 
     * order to abort the current transaction. 
     * <!-- begin JE only -->
     * On BDB JE, a {@link com.sleepycat.je.DeleteConstraintException} is 
     * thrown.
     * <!-- end JE only -->
     */
    ABORT,

    /**
     * If {@code CASCADE} is specified, then this entity will be deleted also,
     * which could in turn trigger further deletions, causing a cascading
     * effect.
     */
    CASCADE,

    /**
     * If {@code NULLIFY} is specified, then the secondary key in this entity
     * is set to null and this entity is updated.  For a secondary key field
     * that has an array or collection type, the array or collection element
     * will be removed by this action.  The secondary key field must have a
     * reference (not a primitive) type in order to specify this action.
     */
    NULLIFY;
}
