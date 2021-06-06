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
 * The action taken when a referenced record in the foreign key database is
 * deleted.
 *
 * <p>The delete action applies to a secondary database that is configured to
 * have a foreign key integrity constraint.  The delete action is specified by
 * calling {@link SecondaryConfig#setForeignKeyDeleteAction}.</p>
 *
 * <p>When a record in the foreign key database is deleted, it is checked to
 * see if it is referenced by any record in the associated secondary database.
 * If the key is referenced, the delete action is applied.  By default, the
 * delete action is {@link #ABORT}.</p>
 *
 * @see SecondaryConfig
 */
public enum ForeignKeyDeleteAction {

    /**
     * When a referenced record in the foreign key database is deleted, abort
     * the transaction by throwing a {@link DeleteConstraintException}.
     */
    ABORT,

    /**
     * When a referenced record in the foreign key database is deleted, delete
     * the primary database record that references it.
     */
    CASCADE,

    /**
     * When a referenced record in the foreign key database is deleted, set the
     * reference to null in the primary database record that references it,
     * thereby deleting the secondary key. @see ForeignKeyNullifier @see
     * ForeignMultiKeyNullifier
     */
    NULLIFY;

    @Override
    public String toString() {
        return "ForeignKeyDeleteAction." + name();
    }
}
