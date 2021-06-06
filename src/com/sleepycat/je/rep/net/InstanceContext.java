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

import com.sleepycat.je.rep.ReplicationNetworkConfig;

/**
 * The InstanceContext class captures contextual information for object
 * instantiation by DataChannelFactory implementations.
 */
public class InstanceContext {
    private final ReplicationNetworkConfig repNetConfig;
    private LoggerFactory loggerFactory;

    /**
     * Creates an InstanceContext instance.
     *
     * @param repNetConfig the configuration from which an instantiation
     * is being generated.
     * @param logger a logger that can be used for logging errors or other
     * information
     */
    public InstanceContext(ReplicationNetworkConfig repNetConfig,
                           LoggerFactory loggerFactory) {
        this.repNetConfig = repNetConfig;
        this.loggerFactory = loggerFactory;
    }

    /**
     * Gets configuration information for this context.
     *
     * @return the configuration from which this context was created
     */
    final public ReplicationNetworkConfig getRepNetConfig() {
        return repNetConfig;
    }

    /**
     * Gets the LoggerFactory that is usable by an instantiation for creation
     * of a JE HA-friendly logging object.
     * @return a LoggerFactory object.
     */
    final public LoggerFactory getLoggerFactory() {
        return loggerFactory;
    }
}
