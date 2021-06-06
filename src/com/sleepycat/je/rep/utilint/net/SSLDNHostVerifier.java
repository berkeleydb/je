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
 * that the host to which we are connected is valid.  This implementation is
 * designed for the case where it is expected that the server's certificate
 * does not match the host name, but instead, contains a well-known
 * distinguished name (DN).  This check verifies that the DN matches
 * expectations.
 * <p>
 * Matching is done using Java regular expressions against the RFC1779
 * normalized DN.  The regular expression is applied against the entire DN
 * string, but the regular expression could be constructed to treat only a
 * portion of it as relevant.
 */

public class SSLDNHostVerifier
    extends SSLDNMatcher
    implements HostnameVerifier {

    /**
     * Construct an SSLDNHostVerifier
     *
     * @param params The parameter for authentication creation.  This class
     *        requires a Java regular expression to be applied to the subject
     *        common name.
     */
    public SSLDNHostVerifier(InstanceParams params) {
        super(params);
    }

    /**
     * Checks whether an SSL connection has been made to the intended target.
     * This should be called only after the SSL handshake has completed.
     *
     * @param targetHost the intended target of a network connection
     *   This parameter is not used by this implementation.
     * @param sslSession the SSLSession that has been set up for the connection
     * @return true if sslSession indicates that the connection has been made
     * to the correct host
     */
    @Override
    public boolean verify(String targetHost, SSLSession sslSession) {
        return peerMatches(sslSession);
    }

    /**
     * Verify that the string is a valid pattern.
     * @throws IllegalArgumentException if not a valid pattern.
     */
    public static void validate(String regex)
        throws IllegalArgumentException {

        validateRegex(regex);
    }
}
