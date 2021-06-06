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
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceHandshake;

/**
 * Object represents a subscription authentication method used in service
 * handshake at client side
 */
public class ClientAuthMethod implements ServiceHandshake.AuthenticationMethod {

    private final SubscriptionAuthHandler clientAuthHandler;

    ClientAuthMethod(SubscriptionAuthHandler clientAuthHandler) {
        this.clientAuthHandler = clientAuthHandler;
    }

    @Override
    public String getMechanismName() {
        return SubscriptionConfig.SERVICE_HANDSHAKE_AUTH_METHOD;
    }

    @Override
    public ServiceHandshake.ClientInitOp
    getClientOp(ServiceHandshake.ClientHandshake initState,
                String ignoredParams) {
        return new ClientTokenOp(initState, clientAuthHandler);
    }

    @Override
    public ServiceHandshake.ServerInitOp
    getServerOp(ServiceHandshake.ServerHandshake initState) {
        return new ServerTokenOp(initState);
    }

    @Override
    public String getServerParams() {
        return "";
    }

    /**
     * Client side authentication
     */
    static class ClientTokenOp extends ServiceHandshake.ClientInitOp {

        private final SubscriptionAuthHandler auth;
        ClientTokenOp(ServiceHandshake.ClientHandshake initState,
                      SubscriptionAuthHandler auth) {
            super(initState);
            this.auth = auth;
        }

        @Override
        public ServiceHandshake.InitResult processOp(
            ServiceHandshake.IOAdapter ioAdapter) throws IOException {

            final byte[] token = auth.getToken();
            if (token == null || token.length == 0) {
                throw new IOException("Token cannot be null or empty");
            }

            /* write size of token */
            final ByteBuffer szBuf = ByteBuffer.allocate(4);
            LogUtils.writeInt(szBuf, token.length);
            ioAdapter.write(szBuf.array());
            /* write token */
            final ByteBuffer tokenBuf = ByteBuffer.allocate(token.length);
            LogUtils.writeBytesNoLength(tokenBuf, token);
            ioAdapter.write(tokenBuf.array());

            final byte[] responseByte = new byte[1];
            final int result = ioAdapter.read(responseByte);
            if (result < 0) {
                throw new IOException(
                    "No service authenticate response byte: " + result);
            }
            final ServiceDispatcher.Response
                response = ServiceDispatcher.Response.get(responseByte[0]);
            if (response == null) {
                throw new IOException("Unexpected read response byte: " +
                                      responseByte[0]);
            }
            setResponse(response);
            return ServiceHandshake.InitResult.DONE;
        }
    }

    /**
     * Server side authentication, effectively no-op except rejecting
     * handshake and it is not supposed to be called at server-side.
     */
    class ServerTokenOp extends ServiceHandshake.ServerInitOp {

        ServerTokenOp(ServiceHandshake.ServerHandshake initState) {
            super(initState);
        }

        @Override
        public ServiceHandshake.InitResult processOp(DataChannel channel)
            throws IOException {
            return ServiceHandshake.InitResult.FAIL;
        }
    }
}
