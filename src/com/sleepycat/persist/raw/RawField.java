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

package com.sleepycat.persist.raw;

/**
 * The definition of a field in a {@link RawType}.
 *
 * <p>{@code RawField} objects are thread-safe.  Multiple threads may safely
 * call the methods of a shared {@code RawField} object.</p>
 *
 * @author Mark Hayes
 */
public interface RawField {

    /**
     * Returns the name of the field.
     *
     * @return the name of the field.
     */
    String getName();

    /**
     * Returns the type of the field, without expanding parameterized types,
     * or null if the type is an interface type or the Object class.
     *
     * @return the type of the field.
     */
    RawType getType();
}
