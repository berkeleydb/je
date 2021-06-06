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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceHandshake;

/**
 * Object represents a subscription authentication method used in service
 * handshake at server side
 */
public class ServerAuthMethod implements ServiceHandshake.AuthenticationMethod {

    private final StreamAuthenticator serverAuth;

    public ServerAuthMethod(StreamAuthenticator serverAuth) {
        this.serverAuth = serverAuth;
    }

    @Override
    public String getMechanismName() {
        return SubscriptionConfig.SERVICE_HANDSHAKE_AUTH_METHOD;
    }

    @Override
    public ServiceHandshake.ClientInitOp getClientOp(
        ServiceHandshake.ClientHandshake initState, String ignoredParams) {
        return new ClientTokenOp(initState);
    }

    @Override
    public ServiceHandshake.ServerInitOp getServerOp(
        ServiceHandshake.ServerHandshake initState) {
        return new ServerTokenOp(initState, serverAuth);
    }

    @Override
    public String getServerParams() {
        return "";
    }

    /**
     * Server side authentication
     */
    static class ServerTokenOp extends ServiceHandshake.ServerInitOp {

        /* start with tokenBuf length */
        private final static int BUFFER_TOKEN_SIZE = 4;
        private final ByteBuffer
            tokenSzBuf = ByteBuffer.allocate(BUFFER_TOKEN_SIZE);
        private ByteBuffer tokenBuf = null;
        private int tokenSz = 0;

        private final StreamAuthenticator auth;
        ServerTokenOp(ServiceHandshake.ServerHandshake initState,
                      StreamAuthenticator auth) {
            super(initState);
            this.auth = auth;
        }

        @Override
        public ServiceHandshake.InitResult processOp(DataChannel channel)
            throws IOException {

            ServiceHandshake.InitResult readResult;

            /* processOp() might be called multiple times? */
            if (tokenBuf == null) {
                readResult = fillBuffer(channel, tokenSzBuf);
                if (readResult != ServiceHandshake.InitResult.DONE) {
                    return readResult;
                }

                /* allocate buffer for token */
                tokenSzBuf.flip();
                tokenSz = LogUtils.readInt(tokenSzBuf);

                if (tokenSz <= 0) {
                    /* just in case a client put a bad value here */
                    return ServiceHandshake.InitResult.REJECT;
                }

                tokenBuf = ByteBuffer.allocate(tokenSz);
            }

            /* continue read token */
            readResult = fillBuffer(channel, tokenBuf);
            if (readResult != ServiceHandshake.InitResult.DONE) {
                return readResult;
            }

            tokenBuf.flip();
            final byte[] token = LogUtils.readBytesNoLength(tokenBuf, tokenSz);
            auth.setToken(token);
            if (!auth.authenticate()) {
                return ServiceHandshake.InitResult.REJECT;
            }
            return ServiceHandshake.InitResult.DONE;
        }
    }

    /**
     * Client side authentication, effectively no-op except rejecting
     * handshake and it is not supposed to be called at client-side.
     */
    class ClientTokenOp extends ServiceHandshake.ClientInitOp {

        ClientTokenOp(ServiceHandshake.ClientHandshake initState) {
            super(initState);
        }

        @Override
        public ServiceHandshake.InitResult processOp(
            ServiceHandshake.IOAdapter ioAdapter) throws IOException {
            return ServiceHandshake.InitResult.REJECT;
        }
    }
}
