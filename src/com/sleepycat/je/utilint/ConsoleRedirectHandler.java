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

package com.sleepycat.je.utilint;

import java.util.logging.LogRecord;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Redirects logging messages to the owning environment's console handler, so
 * that messages can be prefixed with an environment name. See LoggerUtils.java
 * for an explanation of why loggers must be instantiated per-class rather than
 * per-class-instance.
 * 
 * In rare cases, this ConsoleHandler may be used to actually publish on its
 * own.
 */
public class ConsoleRedirectHandler extends java.util.logging.ConsoleHandler {

    public ConsoleRedirectHandler() {
        super();
    }

    @Override
    public void publish(LogRecord record) {
        EnvironmentImpl envImpl = 
            LoggerUtils.envMap.get(Thread.currentThread());

        /* 
         * If the caller forgets to set and release the envImpl so there is no
         * envImpl, or if we are logging before the envImpl is completely set,
         * log to the generic ConsoleHandler without an identifying
         * prefix. That way, we get a message, but don't risk a
         * NullPointerException.
         */
        if (envImpl == null){
            super.publish(record);
            return;
        }

        if (envImpl.getConsoleHandler() == null){
            super.publish(record);
            return;
        }

        envImpl.getConsoleHandler().publish(record);
    }
}
