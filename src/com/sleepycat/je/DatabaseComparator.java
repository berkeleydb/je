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

import java.io.Serializable;
import java.util.Comparator;

/**
 * Implemented by btree and duplicate comparators that need to be initialized
 * before they are used or need access to the environment's ClassLoader
 * property.
 * @since 5.0
 */
public interface DatabaseComparator extends Comparator<byte[]>, Serializable {

    /**
     * Called to initialize a comparator object after it is instantiated or
     * deserialized, and before it is used.
     *
     * @param loader is the environment's ClassLoader property.
     */
    public void initialize(ClassLoader loader);
}
