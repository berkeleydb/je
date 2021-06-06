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

package com.sleepycat.je.latch;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Provides information about a latch, to avoid requiring this information to
 * be stored with every latch object.  This is implemented by the IN class to
 * reduce memory usage.  LatchFactory provides a default implementation for
 * cases where creating an extra object is not an issue.
 */
public interface LatchContext {

    /** Returns EnvironmentParams.ENV_LATCH_TIMEOUT */
    int getLatchTimeoutMs();

    /** Returns the latch name for debugging. */
    String getLatchName();

    /** Returns LatchTable for debug/test tracking. */
    LatchTable getLatchTable();

    /** Returns envImpl, or may throw another exception in unit tests. */
    EnvironmentImpl getEnvImplForFatalException();
}
