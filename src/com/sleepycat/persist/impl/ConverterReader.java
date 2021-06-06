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

import com.sleepycat.persist.model.EntityModel;
import com.sleepycat.persist.evolve.Converter;
import com.sleepycat.persist.raw.RawObject;

/**
 * Reader for invoking a class Converter mutation.
 *
 * @author Mark Hayes
 */
public class ConverterReader implements Reader {

    private static final long serialVersionUID = -305788321064984348L;

    private Converter converter;
    private transient Format oldFormat;

    ConverterReader(Converter converter) {
        this.converter = converter;
    }

    public void initializeReader(Catalog catalog,
                                 EntityModel model,
                                 int initVersion,
                                 Format oldFormat) {
        this.oldFormat = oldFormat;
    }

    public Object newInstance(EntityInput input, boolean rawAccess)
        throws RefreshException {

        /* Create the old format RawObject. */
        return oldFormat.newInstance(input, true);
    }

    public void readPriKey(Object o, EntityInput input, boolean rawAccess)
        throws RefreshException {
        
        /* Read the old format RawObject's primary key. */
        oldFormat.readPriKey(o, input, true);
    }

    public Object readObject(Object o, EntityInput input, boolean rawAccess)
        throws RefreshException {

        Catalog catalog = input.getCatalog();

        /* Read the old format RawObject and convert it. */
        boolean currentRawMode = input.setRawAccess(true);
        try {
            o = oldFormat.readObject(o, input, true);
        } finally {
            input.setRawAccess(currentRawMode);
        }
        o = converter.getConversion().convert(o);

        /* Convert the current format RawObject to a live Object. */
        if (!rawAccess && o instanceof RawObject) {
            o = catalog.convertRawObject((RawObject) o, null);
        }
        return o;
    }
    
    public Accessor getAccessor(boolean rawAccess) {
        return oldFormat.getAccessor(rawAccess);
    }
}
