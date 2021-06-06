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

import java.security.PrivateKey;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.net.Socket;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

/**
 * An implementation of X509ExtendedKeyManager which delegates most operations
 * to an underlying implementation, but which supports explicit selection of
 * alias.
 */
public class AliasKeyManager extends X509ExtendedKeyManager {

    private final X509ExtendedKeyManager delegateKeyManager;
    private final String serverAlias;
    private final String clientAlias;

    /**
     * Constructor.
     * @param delegateKeyManager the underlying key manager to fulfill key
     * retrieval requests
     * @param serverAlias the alias to return for server context requests
     * @param clientAlias the alias to return for client context requests
     */
    public AliasKeyManager(X509ExtendedKeyManager delegateKeyManager,
                           String serverAlias,
                           String clientAlias) {
        this.delegateKeyManager = delegateKeyManager;
        this.serverAlias = serverAlias;
        this.clientAlias = clientAlias;
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
    	return delegateKeyManager.getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(
        String[] keyType, Principal[] issuers, Socket socket) {
        if (clientAlias != null) {
            return clientAlias;
        }

        return delegateKeyManager.chooseClientAlias(keyType, issuers, socket);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return delegateKeyManager.getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(
        String keyType, Principal[] issuers, Socket socket) {

        if (serverAlias != null) {
            return serverAlias;
        }

        return delegateKeyManager.chooseServerAlias(keyType, issuers, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        return delegateKeyManager.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        return delegateKeyManager.getPrivateKey(alias);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType,
                                          Principal[] issuers,
                                          SSLEngine engine) {
        if (clientAlias != null) {
            return clientAlias;
        }
        return delegateKeyManager.
            chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public String chooseEngineServerAlias(String keyType,
                                          Principal[] issuers,
                                          SSLEngine engine) {
        if (serverAlias != null) {
            return serverAlias;
        }
        return delegateKeyManager.
            chooseEngineServerAlias(keyType, issuers, engine);
    }
}
