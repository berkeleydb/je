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

package com.sleepycat.je.rep.subscription;

/**
 * Subscription status returned to client
 */
public enum SubscriptionStatus {
    /* subscription not yet started */
    INIT,
    /* subscription start successfully and start consume stream */
    SUCCESS,
    /* requested vlsn is not available */
    VLSN_NOT_AVAILABLE,
    /* rep group shutdown */
    GRP_SHUTDOWN,
    /* connection error */
    CONNECTION_ERROR,
    /* timeout error */
    TIMEOUT_ERROR,
    /* unknown error */
    UNKNOWN_ERROR,
    /* security check error: authentication or authorization failure */
    SECURITY_CHECK_ERROR
}
