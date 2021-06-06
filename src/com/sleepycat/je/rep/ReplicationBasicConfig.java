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

package com.sleepycat.je.rep;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @hidden SSL deferred
 * Specifies the parameters for unencrypted communication within a
 * replicated environment. The parameters contained here are immutable.
 */
public class ReplicationBasicConfig extends ReplicationNetworkConfig {

    private static final long serialVersionUID = 1L;

    /* The set of Replication properties specific to this class */
    private static Set<String> repBasicProperties;
    static {
        repBasicProperties = new HashSet<String>();
        /* Nail the set down */
        repBasicProperties = Collections.unmodifiableSet(repBasicProperties);
    }

    /**
     * Constructs a ReplicationBasicConfig initialized with the system
     * default settings.
     */
    public ReplicationBasicConfig() {
    }

    /**
     * Constructs a ReplicationBasicConfig initialized with the
     * provided propeties.
     * @param properties a set of properties which which to initialize the
     * instance properties
     */
    public ReplicationBasicConfig(Properties properties) {
        super(properties);
    }

    /**
     * Get the channel type setting for the replication service.
     * This configuration specifies a "basic" channel type.
     *
     * @return the channel type
     */
    @Override
    public String getChannelType() {
        return "basic";
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public ReplicationBasicConfig clone() {
        return (ReplicationBasicConfig) super.clone();
    }

    /**
     * @hidden
     * Enumerate the subset of configuration properties that are intended to
     * control network access.
     */
    static Set<String> getRepBasicPropertySet() {

        return repBasicProperties;
    }
}
