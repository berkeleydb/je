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

package com.sleepycat.persist.impl;

/**
 * Base class for EntityInput implementations.  RecordInput cannot use this
 * base class because it extends TupleInput, so it repeats the code here.
 *
 * @author Mark Hayes
 */
abstract class AbstractInput implements EntityInput {

    Catalog catalog;
    boolean rawAccess;

    AbstractInput(Catalog catalog, boolean rawAccess) {
        this.catalog = catalog;
        this.rawAccess = rawAccess;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public boolean isRawAccess() {
        return rawAccess;
    }

    public boolean setRawAccess(boolean rawAccessParam) {
        boolean original = rawAccess;
        rawAccess = rawAccessParam;
        return original;
    }
}
