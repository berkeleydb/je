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
 * A callback to notify the application program when an exception occurs in a
 * JE Daemon thread.
 */
public interface ExceptionListener {

    /**
     * This method is called if an exception is seen in a JE Daemon thread.
     *
     * @param event the ExceptionEvent representing the exception that was
     * thrown.
     */        
    void exceptionThrown(ExceptionEvent event);
}
