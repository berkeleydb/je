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

import com.sleepycat.persist.impl.PersistCatalog;

/**
 * <!-- begin JE only -->
 * @hidden
 * <!-- end JE only -->
 * Internal access class that should not be used by applications.
 *
 * @author Mark Hayes
 */
public class ModelInternal {

    /**
     * Internal access method that should not be used by applications.
     *
     * @param model the EntityModel.
     * @param catalog the PersistCatalog.
     */
    public static void setCatalog(EntityModel model, PersistCatalog catalog) {
        model.setCatalog(catalog);
    }

    /**
     * Internal access method that should not be used by applications.
     *
     * @param model the EntityModel.
     * @param loader the ClassLoader.
     */
    public static void setClassLoader(EntityModel model, ClassLoader loader) {
        /* Do not overwrite loader with null value. */
        if (loader != null) {
            model.setClassLoader(loader);
        }
    }

    /**
     * Internal access method that should not be used by applications.
     *
     * @param model the EntityModel.
     * @return the ClassLoader.
     */
    public static ClassLoader getClassLoader(EntityModel model) {
        return model.getClassLoader();
    }
}
