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

import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Redirects logging messages to the owning environment's application
 * configured handler, if one was specified through
 * EnvironmentConfig.setLoggingHandler(). Handlers for JE logging can be
 * configured through EnvironmentConfig, to support handlers which:
 * - require a constructor with arguments
 * - is specific to this environment, and multiple environments exist in the
 *   same process.
 */
public class ConfiguredRedirectHandler extends Handler {

    public ConfiguredRedirectHandler() {
        /* No need to call super, this handler is not truly publishing. */
    }

    @Override
    public void publish(LogRecord record) {
        Handler h = getEnvSpecificConfiguredHandler();
        if ((h != null) && (h.isLoggable(record))) {
            h.publish(record);
        }
    }

    private Handler getEnvSpecificConfiguredHandler() {
        EnvironmentImpl envImpl =
            LoggerUtils.envMap.get(Thread.currentThread());

        /*
         * Prefer to lose logging output, rather than risk a
         * NullPointerException if the caller forgets to set and release the
         * environmentImpl.
         */
        if (envImpl == null) {
            return null;
        }

        return envImpl.getConfiguredHandler();
    }

    @Override
    public void close()
        throws SecurityException {
        Handler h = getEnvSpecificConfiguredHandler();
        if (h != null) {
            h.close();
        }
    }

    @Override
    public void flush() {
        Handler h = getEnvSpecificConfiguredHandler();
        if (h != null) {
            h.flush();
        }
    }
}
