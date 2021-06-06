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
 * The InstanceParams class captures configuration information for object
 * instantiation by DataChannelFactory implementations.
 */
public class InstanceParams {
    private final InstanceContext context;
    private final String classParams;

    /**
     * Creates an InstanceParams instance.
     * @param context the configuration context from which an instantiation
     * is being generated.
     * @param classParams a class-specific parameter argument, which may
     * be null
     */
    public InstanceParams(InstanceContext context, String classParams) {
        this.context = context;
        this.classParams = classParams;
    }

    final public InstanceContext getContext() {
        return context;
    }

    final public String getClassParams() {
        return classParams;
    }
}
