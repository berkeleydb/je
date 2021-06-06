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

import static java.util.logging.Level.INFO;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateParsingException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.InstanceLogger;

/**
 * This is an implementation of HostnameVerifier which verifies that the
 * host to which we are connected is valid using the standard SSL matching
 * rules.  That is, the host string that we are using to connect with
 * must have a match to the common name or a subject alternative name.
 */
public class SSLStdHostVerifier implements HostnameVerifier {

    private final InstanceLogger logger;

    private final static int ALTNAME_DNS = 2;
    private final static int ALTNAME_IP  = 7;

    /**
     * Construct an SSLStdHostVerifier
     */
    public SSLStdHostVerifier(InstanceParams params) {
        logger = params.getContext().getLoggerFactory().getLogger(getClass());
    }

    @Override
    public boolean verify(String targetHost, SSLSession sslSession) {
        if (targetHost == null) {
            return false;
        }

        Principal principal = null;
        Certificate[] peerCerts = null;
        try {
            principal = sslSession.getPeerPrincipal();
            peerCerts = sslSession.getPeerCertificates();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (principal != null && principal instanceof X500Principal) {
            final X500Principal x500Principal = (X500Principal) principal;
            final String name = x500Principal.getName(X500Principal.RFC1779);
            if (targetHost.equalsIgnoreCase(name)) {
                return true;
            }
        }

        /* Check for SubjectAlternativeNames */
        if (peerCerts[0] instanceof X509Certificate) {

            final X509Certificate peerCert = (X509Certificate)peerCerts[0];

            Collection<List<?>> altNames = null;
            try {
                altNames = peerCert.getSubjectAlternativeNames();
            } catch (CertificateParsingException cpe) {
                final Principal issuerPrinc = peerCert.getIssuerX500Principal();
                final BigInteger serNo = peerCert.getSerialNumber();

                logger.log(INFO, "Unable to parse peer certificate: " +
                           "issuer = " + issuerPrinc +
                           ", serialNumber = " + serNo);
                
            }

            if (altNames == null) {
                return false;
            }

            for (List<?> altName : altNames) {
                /*
                 * altName will be a 2-element list, with the first being
                 * the name type and the second being the "name".  For
                 * DNS and IP entries, the "name" will be a string.
                 */
                final int nameType = ((Integer)altName.get(0)).intValue();
                if (nameType == ALTNAME_IP || nameType == ALTNAME_DNS) {
                    final String nameValue = (String)altName.get(1);
                    if (targetHost.equals(nameValue)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}


