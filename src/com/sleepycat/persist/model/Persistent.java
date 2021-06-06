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

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Identifies a persistent class that is not an {@link Entity} class or a
 * <a href="{@docRoot}/com/sleepycat/persist/model/Entity.html#simpleTypes">simple type</a>.
 *
 * @author Mark Hayes
 */
@Documented @Retention(RUNTIME) @Target(TYPE)
public @interface Persistent {

    /**
     * Identifies a new version of a class when an incompatible class change
     * has been made.
     *
     * @return the version.
     *
     * @see Entity#version
     */
    int version() default 0;

    /**
     * Specifies the class that is proxied by this {@link PersistentProxy}
     * instance.
     *
     * @return the Class.
     *
     * @see PersistentProxy
     */
    Class proxyFor() default void.class;
}
