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

import com.sleepycat.persist.raw.RawObject;

import java.util.IdentityHashMap;

/**
 * Extends RawAbstractInput to convert complex (ComplexFormat and
 * CompositeKeyFormat) RawObject instances.
 *
 * @author Mark Hayes
 */
class RawComplexInput extends RawAbstractInput {

    private FieldInfo[] fields;
    private RawObject[] objects;
    private int index;

    RawComplexInput(Catalog catalog,
                    boolean rawAccess,
                    IdentityHashMap converted,
                    FieldInfo[] fields,
                    RawObject[] objects) {
        super(catalog, rawAccess, converted);
        this.fields = fields;
        this.objects = objects;
    }

    @Override
    Object readNext()
        throws RefreshException {

        RawObject raw = objects[index];
        FieldInfo field = fields[index];
        index += 1;
        Format format = field.getType();
        Object o = raw.getValues().get(field.getName());
        return checkAndConvert(o, format);
    }
}
