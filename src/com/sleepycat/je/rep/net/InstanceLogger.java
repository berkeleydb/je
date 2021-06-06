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

import java.util.logging.Level;

/**
 * The InstanceLogger interface provides a basic logging interface.
 */
public interface InstanceLogger {

    /**
     * Logs a message at the specified logging level. The message is prefixed
     * with an instance-dependent identifier.
     *
     * @param logLevel the logging level at which the message should be logged.
     * @param msg a string to be logged.
     */
    public void log(Level logLevel, String msg);
}
