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

import java.util.IdentityHashMap;

/**
 * Extends RawAbstractInput to convert array (ObjectArrayFormat and
 * PrimitiveArrayteKeyFormat) RawObject instances.
 *
 * @author Mark Hayes
 */
class RawSingleInput extends RawAbstractInput {

    private Object singleValue;
    private Format declaredFormat;

    RawSingleInput(Catalog catalog,
                   boolean rawAccess,
                   IdentityHashMap converted,
                   Object singleValue,
                   Format declaredFormat) {
        super(catalog, rawAccess, converted);
        this.singleValue = singleValue;
        this.declaredFormat = declaredFormat;
    }

    @Override
    Object readNext()
        throws RefreshException {

        return checkAndConvert(singleValue, declaredFormat);
    }
}
