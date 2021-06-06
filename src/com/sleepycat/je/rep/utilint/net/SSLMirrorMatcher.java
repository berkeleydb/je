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

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.InstanceContext;
import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.InstanceParams;

/**
 * Common base class for mirror comparisons.  Supports both authenticator and
 * host verifier implementations.
 */

class SSLMirrorMatcher {

    /*
     * The Principal that represents us when in the expected peer's ssl mode.
     */
    final private Principal ourPrincipal;
    final private InstanceLogger logger;

    /**
     * Construct an SSLMirrorMatcher
     *
     * @param params The instantiation parameters.
     * @param clientMode set to true if the matcher will be evaluated
     * as a client that has a server as a peer, or false if it will be
     * evaluated as a server that has received a connection from a client.
     * @throws IllegalArgumentException if the instance cannot be created due
     * to a problem related to the input parameters
     */
    public SSLMirrorMatcher(InstanceParams params, boolean clientMode)
        throws IllegalArgumentException {

        ourPrincipal = determinePrincipal(params.getContext(), clientMode);
        if (ourPrincipal == null) {
            throw new IllegalArgumentException(
                "Unable to determine a local principal for comparison " +
                "with peer principals");
        }
        logger = params.getContext().getLoggerFactory().getLogger(getClass());
    }

    /**
     * Checks whether the SSL session peer's certificate DN matches our own.
     *
     * @param sslSession the SSL session that has been established with a peer
     * @return true if the peer's certificate DN matches ours
     */
    public boolean peerMatches(SSLSession sslSession) {

        if (ourPrincipal == null) {
            return false;
        }

        /*
         * Get the peer principal, which should also be an X500Principal.
         * We validate that here.
         */
        Principal peerPrincipal = null;
        try {
            peerPrincipal = sslSession.getPeerPrincipal();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (peerPrincipal == null ||
            ! (peerPrincipal instanceof X500Principal)) {
            logger.log(
                INFO,
                "Unable to attempt peer validation - peer Principal is: " +
                peerPrincipal);
            return false;
        }

        return ourPrincipal.equals(peerPrincipal);
    }

    /**
     * Attempt to determine the Principal that we take on when connecting
     * in client or server context based on the ReplicationNetworkConfig.
     * If we are unable to determine that principal, return null.
     */
    private Principal determinePrincipal(
        InstanceContext context, boolean clientMode)
        throws IllegalArgumentException {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        /*
         * Determine what alias would be used.  It is allowable for this to be
         * null.
         */
        String aliasProp = clientMode ?
            config.getSSLClientKeyAlias() :
            config.getSSLServerKeyAlias();

        final KeyStore keyStore = SSLChannelFactory.readKeyStore(context);

        if (aliasProp == null || aliasProp.isEmpty()) {
            /* Since we weren't told which one to use, there better be
             * only one option, or this might behave unexpectedly. */
            try {
                if (keyStore.size() < 1) {
                    logger.log(INFO, "KeyStore is empty");
                    return null;
                } else if (keyStore.size() > 1) {
                    logger.log(INFO, "KeyStore has multiple entries but no " +
                               "alias was specified.  Using the first one " +
                               "available.");
                }
                final Enumeration<String> e = keyStore.aliases();
                aliasProp = e.nextElement();
            } catch (KeyStoreException kse) {
                throw new IllegalArgumentException(
                    "Error accessing aliases from the keystore", kse);
            }
        }

        Certificate cert = null;
        try {
            cert = keyStore.getCertificate(aliasProp);
        } catch (KeyStoreException kse) {
            /* Shouldn't be possible */
            throw new IllegalArgumentException(
                "Error accessing certificate with alias " + aliasProp +
                " from the keystore", kse);
        }

        if (cert == null) {
            logger.log(INFO, "No certificate for alias " + aliasProp +
                       " found in KeyStore");
            throw new IllegalArgumentException(
                "Unable to find a certificate in the keystore");
        }

        if (!(cert instanceof X509Certificate)) {
            logger.log(INFO, "The certificate for alias " + aliasProp +
                       " is not an X509Certificate.");
            throw new IllegalArgumentException(
                "Unable to find a valid certificate in the keystore");
        }

        final X509Certificate x509Cert = (X509Certificate) cert;
        return x509Cert.getSubjectX500Principal();
    }
}
