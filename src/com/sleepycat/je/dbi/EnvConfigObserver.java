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

package com.sleepycat.je.dbi;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentMutableConfig;

/**
 * Implemented by observers of mutable config changes.
 */
public interface EnvConfigObserver {

    /**
     * Notifies the observer that one or more mutable properties have been
     * changed.
     */
    void envConfigUpdate(DbConfigManager configMgr,
                         EnvironmentMutableConfig newConfig)
        throws DatabaseException;
}
