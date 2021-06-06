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

package com.sleepycat.je.rep.utilint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.subscription.SubscriptionAuthTestHelper;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.utilint.ServiceHandshake.AuthenticationMethod;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

/**
 * Check various service handshake cases
 */
public class HandshakeTest extends TestBase {

    private final static String SERVICE_NAME = "testing";

    private InetSocketAddress dispAddr;
    private ServiceDispatcher dispatcher;

    @Override
    @After
    public void tearDown()
        throws Exception {

        if (dispatcher != null) {
            dispatcher.shutdown();
            dispatcher = null;
        }
    }

    /**
     * Sanity check that no authentication works.
     */
    @Test
    public void testBasicConfig()
        throws Exception {

        DataChannelFactory dcFactory =
            DataChannelFactoryBuilder.construct(
                ReplicationNetworkConfig.createDefault());

        initDispatcher(dcFactory, null);

        DataChannel channel =
            dcFactory.connect(dispAddr, new ConnectOptions());

        ServiceDispatcher.doServiceHandshake(channel, SERVICE_NAME);
    }

    /**
     * Try authentication with our test password authentication implemenation.
     */
    @Test
    public void testPwAuth()
        throws Exception {

        AuthenticationMethod[] authInfo =
            new AuthenticationMethod[] { new TestPasswordAuthentication(
                new TestPasswordSource("hello")) };

        DataChannelFactory dcFactory =
            DataChannelFactoryBuilder.construct(
                ReplicationNetworkConfig.createDefault());

        initDispatcher(dcFactory, authInfo);

        DataChannel channel = dcFactory.connect(dispAddr, new ConnectOptions());

        ServiceDispatcher.doServiceHandshake(channel, SERVICE_NAME, authInfo);
    }

    /**
     * Test no authentication provided when authentication is required.
     */
    @Test
    public void testNoAuthProvided()
        throws Exception {

        ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.createDefault();

        AuthenticationMethod[] authInfo =
            new AuthenticationMethod[] { new TestPasswordAuthentication(
                new TestPasswordSource("hello")) };

        DataChannelFactory dcFactory =
            DataChannelFactoryBuilder.construct(repNetConfig);

        initDispatcher(dcFactory, authInfo);

        DataChannel channel = dcFactory.connect(dispAddr, new ConnectOptions());

        try {
            ServiceDispatcher.doServiceHandshake(channel, SERVICE_NAME);
            fail("expected exception");
        } catch (ServiceConnectFailedException e) {
            /* expected exception */
        }
    }

    /**
     * Test no common authentication provided when authentication is required.
     */
    @Test
    public void testNoCommonAuth()
        throws Exception {

        ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.createDefault();

        AuthenticationMethod[] authInfo =
            new AuthenticationMethod[] { new TestPasswordAuthentication(
                new TestPasswordSource("hello")) };

        AuthenticationMethod privateAuth =
            new TestPasswordAuthentication(new TestPasswordSource("hello")) {

                @Override
                public String getMechanismName() {
                    return "private";
                }
            };

        AuthenticationMethod[] privateAuthInfo =
            new AuthenticationMethod[] { privateAuth };

        DataChannelFactory dcFactory =
            DataChannelFactoryBuilder.construct(repNetConfig);

        initDispatcher(dcFactory, authInfo);

        DataChannel channel = dcFactory.connect(dispAddr, new ConnectOptions());

        try {
            ServiceDispatcher.doServiceHandshake(channel, SERVICE_NAME,
                                                 privateAuthInfo);
            fail("expected exception");
        } catch (ServiceConnectFailedException e) {
            /* expected exception */
        }
    }

    /**
     * Test failed authentication
     */
    @Test
    public void testfailedAuth()
        throws Exception {

        ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.createDefault();

        AuthenticationMethod[] authInfo =
            new AuthenticationMethod[] { new TestPasswordAuthentication(
                new TestPasswordSource("hello")) };

        AuthenticationMethod[] badAuthInfo =
            new AuthenticationMethod[] { new TestPasswordAuthentication(
                new TestPasswordSource("xhello")) };

        DataChannelFactory dcFactory =
            DataChannelFactoryBuilder.construct(repNetConfig);

        initDispatcher(dcFactory, authInfo);

        DataChannel channel = dcFactory.connect(dispAddr, new ConnectOptions());

        try {
            ServiceDispatcher.doServiceHandshake(channel, SERVICE_NAME,
                                                 badAuthInfo);
            fail("expected exception");
        } catch (ServiceConnectFailedException e) {
            /* expected exception */
        }
    }

    /**
     * Test a successful subscription authentication
     */
    @Test
    public void testSubscriptionAuthSucc() throws Exception {
        /* use a good token */
        testSubscriptionAuth(SubscriptionAuthTestHelper.TokenType.GOOD);
    }

    /**
     * Test a failed subscription authentication
     */
    @Test
    public void testSubscriptionAuthFail() throws Exception {
        /* use a bad token */
        testSubscriptionAuth(SubscriptionAuthTestHelper.TokenType.BAD);
    }

    /**
     * Test a subscription authentication with empty token
     */
    @Test
    public void testSubscriptionAuthEmptyToken() throws Exception {
        testSubscriptionAuth(SubscriptionAuthTestHelper.TokenType.EMPTY);
    }

    /**
     * Test a subscription authentication without any token
     */
    @Test
    public void testSubscriptionAuthWithoutToken() throws Exception {
        testSubscriptionAuth(SubscriptionAuthTestHelper.TokenType.NONE);
    }

    private void testSubscriptionAuth(SubscriptionAuthTestHelper.TokenType type)
        throws Exception {

        /* create auth method with specified type of token */
        final AuthenticationMethod[] authInfo = new AuthenticationMethod[]{
            new SubscriptionAuthTestHelper(type)};
        final DataChannelFactory dcFactory =
            DataChannelFactoryBuilder.construct(
                ReplicationNetworkConfig.createDefault());

        initDispatcher(dcFactory, authInfo);

        final DataChannel channel = dcFactory.connect(dispAddr,
                                                      new ConnectOptions());

        if (type == SubscriptionAuthTestHelper.TokenType.GOOD) {
            /* expect successful authentication */
            ServiceDispatcher
                .doServiceHandshake(channel, SERVICE_NAME, authInfo);
            return;
        }

        if (type == SubscriptionAuthTestHelper.TokenType.BAD) {
            /* expect failed authentication */
            try {
                ServiceDispatcher
                    .doServiceHandshake(channel, SERVICE_NAME, authInfo);
                fail("Authentication should fail");
            } catch (ServiceConnectFailedException scfe) {
                assertEquals("Unexpected response",
                             ServiceDispatcher.Response.INVALID,
                             scfe.getResponse());
            }
            return;
        }

        if (type == SubscriptionAuthTestHelper.TokenType.EMPTY ||
            type == SubscriptionAuthTestHelper.TokenType.NONE) {

            /* expect failed authentication */
            try {
                ServiceDispatcher
                    .doServiceHandshake(channel, SERVICE_NAME, authInfo);
                fail("Authentication should fail");
            } catch (IOException ioe) {
                /* expected ioe due to empty or null token */
            }
        }
    }

    private void initDispatcher(DataChannelFactory channelFactory,
                                AuthenticationMethod[] authInfo)
        throws Exception {

        FreePortLocator locator = new FreePortLocator("localhost", 5000, 6000);
        int freePort = locator.next();

        dispAddr = new InetSocketAddress("localhost", freePort);
        dispatcher = new ServiceDispatcher(dispAddr, channelFactory);
        dispatcher.addTestAuthentication(authInfo);
        final BlockingQueue<DataChannel> serviceQueue = new
            LinkedBlockingQueue<>();
        dispatcher.register(SERVICE_NAME, serviceQueue);
        dispatcher.start();
    }

    private class TestPasswordSource implements PasswordSource {
        private final String password;

        private TestPasswordSource(String password) {
            this.password = password;
        }

        @Override
        public char[] getPassword() {
            return password.toCharArray();
        }
    }
}
