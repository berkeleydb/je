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

import com.sleepycat.je.rep.ReplicationSecurityException;

/**
 * Object represents an interface of subscriber authenticator, used by
 * subscriber to track token expiration and refresh token proactively.
 */
public interface SubscriptionAuthHandler {

    /**
     * Returns if subscriber has new token to update during subscription.
     * Subscriber need to update an existing token when 1) token will
     * expire soon and need to to be renewed, or 2) token is unable to be
     * renewed further and re-authenticate is needed to get a new token.
     * Implementation of the interface shall check the two conditions above
     * and refresh token if necessary.
     *
     * @throws ReplicationSecurityException if implementation of the
     * interface fails to renew or re-authenticate to get a new token
     *
     * @return true if the subscriber has a new token to update, false
     * otherwise.
     */
    boolean hasNewToken() throws ReplicationSecurityException;

    /**
     * Returns the login token in bytes. It returns null if no identity
     * information is available.
     *
     * @return login token as byte array, null if token is not available at
     * the time of calling.
     */
    byte[] getToken();
}
