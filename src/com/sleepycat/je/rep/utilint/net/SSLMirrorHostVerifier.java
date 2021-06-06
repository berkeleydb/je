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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

import com.sleepycat.je.rep.net.InstanceParams;

/**
 * This is an implementation of HostnameVerifier, which is intended to verify
 * that the host to which we are connected is valid.  This implementation
 * authenticates based on the Distinguished Name (DN) in the certificate of
 * the server matching the DN in the certificate that we would use when
 * operating as a server.  This is useful if deploying with a common SSL key
 * for all hosts.
 */

public class SSLMirrorHostVerifier
    extends SSLMirrorMatcher
    implements HostnameVerifier {

    /**
     * Construct an SSLMirrorHostVerifier
     *
     * @param params the instantiation parameters.
     * @throws IllegalArgumentException if the instance cannot be created due
     * to a problem related to the input parameters
     */
    public SSLMirrorHostVerifier(InstanceParams params)
        throws IllegalArgumentException {

        super(params, true);
    }

    /**
     * Checks whether an SSL connection has been made to the intended target.
     * This should be called only after the SSL handshake has completed.
     *
     * @param targetHost the host to which a connection is being established.
     *   This parameter is not used by this implementation.
     * @param sslSession the established SSL session
     * @return true if the sslSession is set up with the correct host
     */
    @Override
    public boolean verify(String targetHost, SSLSession sslSession) {
        return peerMatches(sslSession);
    }
}
