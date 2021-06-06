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
 * This is an implementation of SSLAuthenticator which authenticates based
 * on the Distinguished Name (DN) in the SSL peer's certificate.  Matching
 * is done using Java regular expressions against the RFC1779 normalized
 * DN.  This may be used to match against the complete DN or just a portion,
 * such as the CN portion.
 */

public class SSLDNAuthenticator
    extends SSLDNMatcher
    implements SSLAuthenticator {

    /**
     * Construct an SSLDNAuthenticator
     *
     * @param params The parameter for authentication creation.  This class
     *        requires a Java regular expression to be applied to the subject
     *        common name.
     */
    public SSLDNAuthenticator(InstanceParams params) {
        super(params);
    }

    /*
     * Based on the information in the SSLSession object, should the peer
     * be trusted as an internal entity?  This should be called only after
     * The SSL handshake has completed.
     */
    @Override
    public boolean isTrusted(SSLSession sslSession) {
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
