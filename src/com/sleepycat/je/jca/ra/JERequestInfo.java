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

package com.sleepycat.je.jca.ra;

import java.io.File;

import javax.resource.spi.ConnectionRequestInfo;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.TransactionConfig;

public class JERequestInfo implements ConnectionRequestInfo {
    private File rootDir;
    private EnvironmentConfig envConfig;
    private TransactionConfig transConfig;

    public JERequestInfo(File rootDir,
                         EnvironmentConfig envConfig,
                         TransactionConfig transConfig) {
        this.rootDir = rootDir;
        this.envConfig = envConfig;
        this.transConfig = transConfig;
    }

    File getJERootDir() {
        return rootDir;
    }

    EnvironmentConfig getEnvConfig() {
        return envConfig;
    }

    TransactionConfig getTransactionConfig() {
        return transConfig;
    }

    public boolean equals(Object obj) {
        JERequestInfo info = (JERequestInfo) obj;
        return rootDir.equals(info.rootDir);
    }

    public int hashCode() {
        return rootDir.hashCode();
    }

    public String toString() {
        return "</JERequestInfo rootDir=" + rootDir.getAbsolutePath() + "/>";
    }
}
