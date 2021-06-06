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

package com.sleepycat.je.rep.net;

/**
 * The LoggerFactory interface provides a mechanism for obtaining logging
 * objects for use with dynamically constructed network objects.  Instances
 * of this interface are provided during object instantiation.
 */
public interface LoggerFactory {

    /**
     * Obtains an InstanceLogger for the specified class.
     *
     * @param clazz the class for which a logger instance is to be obtained.
     * @return a logging object
     */
    public InstanceLogger getLogger(Class<?> clazz);
}
