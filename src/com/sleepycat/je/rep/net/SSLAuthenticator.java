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

import javax.net.ssl.SSLSession;

/**
 * Interface to check the identity of a client based on its certificate.
 * Implementations of this interface can be configured for use by the HA
 * service to determine whether a client connection should be treated as
 * authenticated.
 */
public interface SSLAuthenticator {
    /*
     * Based on the information in the SSLSession object, should the client peer
     * be trusted as an internal entity?  This method is called only in server
     * mode.
     *
     * @param sslSession an SSL session object
     * @return true if the SSL peer should be treated as "trusted"
     */
    public boolean isTrusted(SSLSession sslSession);
}

