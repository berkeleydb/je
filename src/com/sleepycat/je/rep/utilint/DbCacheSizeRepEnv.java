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

package com.sleepycat.je.rep.utilint;

import java.io.File;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;

/**
 * Class for opening a ReplicatedEnvironment from a JE standalone utility,
 * DbCacheSize.  Must be instantiated from standalone JE using Class.forName.
 */
public class DbCacheSizeRepEnv
    implements com.sleepycat.je.utilint.DbCacheSizeRepEnv {

    private static final int START_PORT = 30100;
    private static final int PORT_RANGE = 100;

    @Override
    public Environment open(File envHome,
                            EnvironmentConfig envConfig,
                            Map<String, String> repParams) {
        final String host = "localhost";
        final FreePortLocator locator = new FreePortLocator
            (host, START_PORT, START_PORT + PORT_RANGE);
        final int port = locator.next();
        final String hostPort = host + ':' + port;
        final ReplicationConfig repConfig = new ReplicationConfig
            ("DbCacheSizeGroup", "DbCacheSizeNode", hostPort);
        repConfig.setHelperHosts(hostPort);
        for (Map.Entry<String, String> entry : repParams.entrySet()) {
            repConfig.setConfigParam(entry.getKey(), entry.getValue());
        }
        return new ReplicatedEnvironment(envHome, repConfig, envConfig);
    }
}
