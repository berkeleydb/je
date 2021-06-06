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

package com.sleepycat.je.rep.utilint.net;

import javax.net.ssl.SSLSession;

import com.sleepycat.je.rep.net.SSLAuthenticator;
import com.sleepycat.je.rep.net.InstanceParams;

/**
 * This is an implementation of SSLAuthenticator that authenticates based on
 * the certificate of the client matching the certificate that we would use when
 * operating as a client.
 */

public class SSLMirrorAuthenticator
    extends SSLMirrorMatcher
    implements SSLAuthenticator {

    /**
     * Construct an SSLMirrorAuthenticator
     *
     * @param params the instantiation parameters.
     * @throws IllegalArgumentException if the instance cannot be created due
     * to a problem related to the input parameters
     */
    public SSLMirrorAuthenticator(InstanceParams params)
        throws IllegalArgumentException {

        super(params, false);
    }

    /*
     * Checks whether the peer should be trusted based on the information in
     * the SSLSession object.  This should be called only after the SSL
     * handshake has completed.
     */
    @Override
    public boolean isTrusted(SSLSession sslSession) {
        return peerMatches(sslSession);
    }
}
