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

import java.util.Arrays;

import com.sleepycat.je.rep.utilint.ServiceHandshake;

/**
 * Helper of test subscription authentication in handshake test
 * {@link com.sleepycat.je.rep.utilint.HandshakeTest}
 */
public class SubscriptionAuthTestHelper implements
    ServiceHandshake.AuthenticationMethod  {

    private final static byte[] goodToken = ("GoodToken").getBytes();
    private final static byte[] badToken = ("BadToken").getBytes();
    private final static String[] tableIds = new String[]{"1", "2", "3"};

    private final StreamAuthenticator serverAuth;
    private final SubscriptionAuthHandler clientAuthHandler;

    public SubscriptionAuthTestHelper(TokenType type) {

        serverAuth = new TestAuthenticator(goodToken, tableIds);

        if (type == TokenType.GOOD) {
            clientAuthHandler = new TestSubscriptionAuth(goodToken, true);
        } else if (type == TokenType.BAD) {
            clientAuthHandler = new TestSubscriptionAuth(badToken, true);
        } else if (type == TokenType.EMPTY) {
            clientAuthHandler = new TestSubscriptionAuth(new byte[0], true);
        } else {
            clientAuthHandler = new TestSubscriptionAuth(null, true);
        }
    }

    @Override
    public String getMechanismName() {
        return "SubscriptionAuthTest";
    }

    @Override
    public String getServerParams() {
        return null;
    }

    @Override
    public ServiceHandshake.ServerInitOp getServerOp(
        ServiceHandshake.ServerHandshake initState) {
        return new ServerTestOp(initState, serverAuth);
    }

    @Override
    public ServiceHandshake.ClientInitOp getClientOp(
        ServiceHandshake.ClientHandshake initState, String params) {
        return new ClientTestOp(initState, clientAuthHandler);
    }

    static class ServerTestOp extends ServerAuthMethod.ServerTokenOp {

        ServerTestOp(ServiceHandshake.ServerHandshake initState,
                     StreamAuthenticator serverAuth) {
            super(initState, serverAuth);
        }
    }

    static class ClientTestOp extends ClientAuthMethod.ClientTokenOp {

        ClientTestOp(ServiceHandshake.ClientHandshake initState,
                     SubscriptionAuthHandler clientAuthHandler) {
            super(initState, clientAuthHandler);
        }
    }

    /*
     * This method actually checks the contents of the expected token and
     * received token to ensure the data is being transmitted properly. So
     * does it for table ids.
     */
    static class TestAuthenticator implements StreamAuthenticator {

        private byte[] expectedToken;
        private byte[] receivedToken;

        private String[] expectedTableIds;
        private String[] receivedTableIds;

        private long ts;

        TestAuthenticator(byte[] expectedToken,
                          String[] expectedTableIds) {

            this.expectedToken = expectedToken;
            this.expectedTableIds = expectedTableIds;
            receivedToken = null;
            receivedTableIds = null;
            ts = 0;
        }

        @Override
        public void setToken(byte[] token) {
            receivedToken = token;
        }

        @Override
        public void setTableIds(String[] tableIdStr) {
            receivedTableIds = tableIdStr;
        }

        @Override
        public boolean authenticate() {
            ts = System.currentTimeMillis();
            return Arrays.equals(expectedToken, receivedToken);
        }

        @Override
        public boolean checkAccess() {
            ts = System.currentTimeMillis();
            return authenticate() &&
                   Arrays.equals(expectedTableIds, receivedTableIds);
        }

        @Override
        public long getLastCheckTimeMs() {
            return ts;
        }

    }

    static class TestSubscriptionAuth implements SubscriptionAuthHandler {

        private final byte[] token;
        private final boolean reauth;

        TestSubscriptionAuth(byte[] token, boolean reauth) {
            this.token = token;
            this.reauth = reauth;
        }

        @Override
        public boolean hasNewToken() {
            return reauth;
        }

        @Override
        public byte[] getToken() {
            return token;
        }
    }

    public enum TokenType {
        GOOD,
        BAD,
        EMPTY,
        NONE
    }
}
