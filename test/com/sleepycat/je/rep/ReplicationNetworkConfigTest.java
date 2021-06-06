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

package com.sleepycat.je.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_FILE;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_CLASS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_PARAMS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_TYPE;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_CLIENT_KEY_ALIAS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_SERVER_KEY_ALIAS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_TRUSTSTORE_FILE;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_TRUSTSTORE_TYPE;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_CIPHER_SUITES;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_PROTOCOLS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_AUTHENTICATOR;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_AUTHENTICATOR_CLASS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_AUTHENTICATOR_PARAMS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_HOST_VERIFIER;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_HOST_VERIFIER_PARAMS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.impl.RepParams.ChannelTypeConfigParam;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.rep.utilint.net.SSLChannelFactory;
import com.sleepycat.je.rep.utilint.net.SSLMirrorAuthenticator;
import com.sleepycat.je.rep.utilint.net.SSLMirrorHostVerifier;
import com.sleepycat.je.rep.utilint.net.SimpleChannelFactory;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class ReplicationNetworkConfigTest extends TestBase {

    private Properties stdProps;

    @Before
    public void setup() {
        stdProps = new Properties();
        RepTestUtils.setUnitTestSSLProperties(stdProps);
    }

    @Test
    public void testChannelType() {
        /* default constructor initializes to "basic" */
        ReplicationNetworkConfig defRnc =
            ReplicationNetworkConfig.createDefault();
        assertEquals(ChannelTypeConfigParam.BASIC, defRnc.getChannelType());

        /* property constructor initializes to "basic" */
        Properties props = new Properties();
        ReplicationNetworkConfig empRnc =
            ReplicationNetworkConfig.create(props);
        assertEquals(ChannelTypeConfigParam.BASIC, empRnc.getChannelType());

        /* Use property constructor to set to a value */
        props.setProperty(ReplicationNetworkConfig.CHANNEL_TYPE,
                          ChannelTypeConfigParam.BASIC);
        ReplicationNetworkConfig rnc =
            ReplicationNetworkConfig.create(props);
        assertEquals(rnc.getChannelType(), ChannelTypeConfigParam.BASIC);

        /* Make sure other valid types work */
        ReplicationNetworkConfig rsc = new ReplicationSSLConfig();
        assertEquals(rsc.getChannelType(), ChannelTypeConfigParam.SSL);

        /* Make sure invalid values are rejected */
        props.setProperty(ReplicationNetworkConfig.CHANNEL_TYPE, "xyz");
        try {
            ReplicationNetworkConfig.create(props);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testLogName() {
        final String testLogName = "RNC";

        /* default constructor initializes to empty */
        ReplicationNetworkConfig defRnc =
            ReplicationNetworkConfig.createDefault();
        assertEmpty(defRnc.getLogName());

        /* property constructor initializes to empty */
        Properties props = new Properties();
        ReplicationNetworkConfig empRnc =
            ReplicationNetworkConfig.create(props);
        assertEmpty(empRnc.getLogName());

        /* Use property constructor to set to a value */
        props.setProperty(ReplicationNetworkConfig.CHANNEL_LOG_NAME,
                          testLogName);
        ReplicationNetworkConfig rnc =
            ReplicationNetworkConfig.create(props);
        assertEquals(rnc.getLogName(), testLogName);

        /* Make sure we can clear it */
        rnc.setLogName("");
        assertEmpty(rnc.getLogName());

        /* Make sure we can set it */
        rnc.setLogName(testLogName);
        assertEquals(rnc.getLogName(), testLogName);
    }

    @Test
    public void testDCFactoryClass() {

        /* default constructor initializes to "" */
        ReplicationNetworkConfig defRnc =
            ReplicationNetworkConfig.createDefault();
        assertEmpty(defRnc.getChannelFactoryClass());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationNetworkConfig empRnc =
            ReplicationNetworkConfig.create(props);
        assertEmpty(empRnc.getChannelFactoryClass());

        final String dummyClass = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(ReplicationNetworkConfig.CHANNEL_FACTORY_CLASS,
                          dummyClass);
        ReplicationNetworkConfig rnc =
            ReplicationNetworkConfig.create(props);
        assertEquals(rnc.getChannelFactoryClass(), dummyClass);

        /* Make sure we can clear it */
        rnc.setChannelFactoryClass("");
        assertEmpty(rnc.getChannelFactoryClass());

        /* Make sure we can set it */
        rnc.setChannelFactoryClass(dummyClass);
        assertEquals(rnc.getChannelFactoryClass(), dummyClass);

    }

    @Test
    public void testDCFactoryParams() {

        /* default constructor initializes to "" */
        ReplicationNetworkConfig defRnc =
            ReplicationNetworkConfig.createDefault();
        assertEmpty(defRnc.getChannelFactoryParams());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationNetworkConfig empRnc =
            ReplicationNetworkConfig.create(props);
        assertEmpty(empRnc.getChannelFactoryParams());

        final String dummyParams = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(ReplicationNetworkConfig.CHANNEL_FACTORY_PARAMS,
                          dummyParams);
        ReplicationNetworkConfig rnc = ReplicationNetworkConfig.create(props);
        assertEquals(rnc.getChannelFactoryParams(), dummyParams);

        /* Make sure we can clear it */
        rnc.setChannelFactoryParams("");
        assertEmpty(rnc.getChannelFactoryParams());

        /* Make sure we can set it */
        rnc.setChannelFactoryParams(dummyParams);
        assertEquals(rnc.getChannelFactoryParams(), dummyParams);

    }

    @Test
    public void testSSLKeyStore() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLKeyStore());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLKeyStore());

        final String dummyKS = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_KEYSTORE_FILE, dummyKS);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLKeyStore(), dummyKS);

        /* Make sure we can clear it */
        rsc.setSSLKeyStore("");
        assertEmpty(rsc.getSSLKeyStore());

        /* Make sure we can set it */
        rsc.setSSLKeyStore(dummyKS);
        assertEquals(rsc.getSSLKeyStore(), dummyKS);

    }

    @Test
    public void testSSLKeyStoreType() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLKeyStoreType());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLKeyStoreType());

        final String dummyType = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_KEYSTORE_TYPE, dummyType);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLKeyStoreType(), dummyType);

        /* Make sure we can clear it */
        rsc.setSSLKeyStoreType("");
        assertEmpty(rsc.getSSLKeyStoreType());

        /* Make sure we can set it */
        rsc.setSSLKeyStoreType(dummyType);
        assertEquals(rsc.getSSLKeyStoreType(), dummyType);

    }

    @Test
    public void testSSLKeyStorePassword() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLKeyStorePassword());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLKeyStorePassword());

        final String dummyPassword = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_KEYSTORE_PASSWORD, dummyPassword);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLKeyStorePassword(), dummyPassword);

        /* Make sure we can clear it */
        rsc.setSSLKeyStorePassword("");
        assertEmpty(rsc.getSSLKeyStorePassword());

        /* Make sure we can set it */
        rsc.setSSLKeyStorePassword(dummyPassword);
        assertEquals(rsc.getSSLKeyStorePassword(), dummyPassword);

    }

    @Test
    public void testSSLKeyStorePasswordClass() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLKeyStorePasswordClass());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLKeyStorePasswordClass());

        final String dummyPasswordClass = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_KEYSTORE_PASSWORD_CLASS, dummyPasswordClass);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLKeyStorePasswordClass(), dummyPasswordClass);

        /* Make sure we can clear it */
        rsc.setSSLKeyStorePasswordClass("");
        assertEmpty(rsc.getSSLKeyStorePasswordClass());

        /* Make sure we can set it */
        rsc.setSSLKeyStorePasswordClass(dummyPasswordClass);
        assertEquals(rsc.getSSLKeyStorePasswordClass(), dummyPasswordClass);

    }

    @Test
    public void testSSLKeyStorePasswordParams() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLKeyStorePasswordParams());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLKeyStorePasswordParams());

        final String dummyPasswordParams = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_KEYSTORE_PASSWORD_PARAMS, dummyPasswordParams);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLKeyStorePasswordParams(), dummyPasswordParams);

        /* Make sure we can clear it */
        rsc.setSSLKeyStorePasswordParams("");
        assertEmpty(rsc.getSSLKeyStorePasswordParams());

        /* Make sure we can set it */
        rsc.setSSLKeyStorePasswordParams(dummyPasswordParams);
        assertEquals(rsc.getSSLKeyStorePasswordParams(), dummyPasswordParams);

    }

    @Test
    public void testSSLServerKeyAlias() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLServerKeyAlias());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLServerKeyAlias());

        final String dummyAlias = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_SERVER_KEY_ALIAS, dummyAlias);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLServerKeyAlias(), dummyAlias);

        /* Make sure we can clear it */
        rsc.setSSLServerKeyAlias("");
        assertEmpty(rsc.getSSLServerKeyAlias());

        /* Make sure we can set it */
        rsc.setSSLServerKeyAlias(dummyAlias);
        assertEquals(rsc.getSSLServerKeyAlias(), dummyAlias);

    }

    @Test
    public void testSSLClientKeyAlias() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLClientKeyAlias());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLClientKeyAlias());

        final String dummyAlias = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_CLIENT_KEY_ALIAS, dummyAlias);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLClientKeyAlias(), dummyAlias);

        /* Make sure we can clear it */
        rsc.setSSLClientKeyAlias("");
        assertEmpty(rsc.getSSLClientKeyAlias());

        /* Make sure we can set it */
        rsc.setSSLClientKeyAlias(dummyAlias);
        assertEquals(rsc.getSSLClientKeyAlias(), dummyAlias);

    }

    @Test
    public void testSSLTrustStore() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLTrustStore());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLTrustStore());

        final String dummyTS = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_TRUSTSTORE_FILE, dummyTS);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLTrustStore(), dummyTS);

        /* Make sure we can clear it */
        rsc.setSSLTrustStore("");
        assertEmpty(rsc.getSSLTrustStore());

        /* Make sure we can set it */
        rsc.setSSLTrustStore(dummyTS);
        assertEquals(rsc.getSSLTrustStore(), dummyTS);

    }

    @Test
    public void testSSLTrustStoreType() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLTrustStoreType());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLTrustStoreType());

        final String dummyType = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_TRUSTSTORE_TYPE, dummyType);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLTrustStoreType(), dummyType);

        /* Make sure we can clear it */
        rsc.setSSLTrustStoreType("");
        assertEmpty(rsc.getSSLTrustStoreType());

        /* Make sure we can set it */
        rsc.setSSLTrustStoreType(dummyType);
        assertEquals(rsc.getSSLTrustStoreType(), dummyType);

    }

    @Test
    public void testSSLCipherSuites() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLCipherSuites());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLCipherSuites());

        final String dummySuites = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_CIPHER_SUITES, dummySuites);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLCipherSuites(), dummySuites);

        /* Make sure we can clear it */
        rsc.setSSLCipherSuites("");
        assertEmpty(rsc.getSSLCipherSuites());

        /* Make sure we can set it */
        rsc.setSSLCipherSuites(dummySuites);
        assertEquals(rsc.getSSLCipherSuites(), dummySuites);

    }

    @Test
    public void testSSLProtocols() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLProtocols());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLProtocols());

        final String dummyProtocols = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_PROTOCOLS, dummyProtocols);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLProtocols(), dummyProtocols);

        /* Make sure we can clear it */
        rsc.setSSLProtocols("");
        assertEmpty(rsc.getSSLProtocols());

        /* Make sure we can set it */
        rsc.setSSLProtocols(dummyProtocols);
        assertEquals(rsc.getSSLProtocols(), dummyProtocols);

    }

    @Test
    public void testSSLAuthenticator() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLAuthenticator());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLAuthenticator());

        final String mirrorAuthenticator = "mirror";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_AUTHENTICATOR, mirrorAuthenticator);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLAuthenticator(), mirrorAuthenticator);

        /* Make sure we can clear it */
        rsc.setSSLAuthenticator("");
        assertEmpty(rsc.getSSLAuthenticator());

        /* Make sure we can set it */
        rsc.setSSLAuthenticator(mirrorAuthenticator);
        assertEquals(rsc.getSSLAuthenticator(), mirrorAuthenticator);

        /* Check that dnmatch works */
        final String dnmatchAuthenticator = "dnmatch(foo)";
        rsc.setSSLAuthenticator(dnmatchAuthenticator);
        assertEquals(rsc.getSSLAuthenticator(), dnmatchAuthenticator);

        /* Check that invalid dnmatch is signaled */
        try {
            rsc.setSSLAuthenticator("dnmatch(foo");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }

        /* Check that invalid name is signaled */
        try {
            rsc.setSSLAuthenticator("foo");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLAuthenticatorClass() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLAuthenticatorClass());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLAuthenticatorClass());

        final String dummyAuthenticatorClass = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_AUTHENTICATOR_CLASS, dummyAuthenticatorClass);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLAuthenticatorClass(), dummyAuthenticatorClass);

        /* Make sure we can clear it */
        rsc.setSSLAuthenticatorClass("");
        assertEmpty(rsc.getSSLAuthenticatorClass());

        /* Make sure we can set it */
        rsc.setSSLAuthenticatorClass(dummyAuthenticatorClass);
        assertEquals(rsc.getSSLAuthenticatorClass(), dummyAuthenticatorClass);

    }

    @Test
    public void testSSLAuthenticatorParams() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLAuthenticatorParams());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLAuthenticatorParams());

        final String dummyParams = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_AUTHENTICATOR_PARAMS, dummyParams);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLAuthenticatorParams(), dummyParams);

        /* Make sure we can clear it */
        rsc.setSSLAuthenticatorParams("");
        assertEmpty(rsc.getSSLAuthenticatorParams());

        /* Make sure we can set it */
        rsc.setSSLAuthenticatorParams(dummyParams);
        assertEquals(rsc.getSSLAuthenticatorParams(), dummyParams);

    }

    @Test
    public void testSSLAuthenticatorConflict() {

        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));
        rsc.setSSLAuthenticator("dnmatch(foo)");
        rsc.setSSLHostVerifierClass(SSLMirrorAuthenticator.class.getName());

        /* Make sure that conflict is detected */
        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLHostVerifier() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLHostVerifier());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLHostVerifier());

        final String mirrorVerifier = "mirror";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_HOST_VERIFIER, mirrorVerifier);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLHostVerifier(), mirrorVerifier);

        /* Make sure we can clear it */
        rsc.setSSLHostVerifier("");
        assertEmpty(rsc.getSSLHostVerifier());

        /* Make sure we can set it */
        rsc.setSSLHostVerifier(mirrorVerifier);
        assertEquals(rsc.getSSLHostVerifier(), mirrorVerifier);

        /* Check that other options work */
        final String dnmatchVerifier = "dnmatch(foo)";
        rsc.setSSLHostVerifier(dnmatchVerifier);
        assertEquals(rsc.getSSLHostVerifier(), dnmatchVerifier);

        final String hostnameVerifier = "hostname";
        rsc.setSSLHostVerifier(hostnameVerifier);
        assertEquals(rsc.getSSLHostVerifier(), hostnameVerifier);

        /* Make sure that invalid choices are detected */
        try {
            rsc.setSSLHostVerifier("foo");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }

        /* Make sure that invalid dnmatch syntax is detected */
        try {
            rsc.setSSLHostVerifier("dnmatch(foo");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLHostVerifierClass() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLHostVerifierClass());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLHostVerifierClass());

        final String dummyClass = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_HOST_VERIFIER_CLASS, dummyClass);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLHostVerifierClass(), dummyClass);

        /* Make sure we can clear it */
        rsc.setSSLHostVerifierClass("");
        assertEmpty(rsc.getSSLHostVerifierClass());

        /* Make sure we can set it */
        rsc.setSSLHostVerifierClass(dummyClass);
        assertEquals(rsc.getSSLHostVerifierClass(), dummyClass);

    }

    @Test
    public void testSSLHostVerifierParams() {

        /* default constructor initializes to "" */
        ReplicationSSLConfig defRsc = new ReplicationSSLConfig();
        assertEmpty(defRsc.getSSLHostVerifierParams());

        /* property constructor initializes to "" */
        Properties props = new Properties();
        ReplicationSSLConfig empRsc = new ReplicationSSLConfig(props);
        assertEmpty(empRsc.getSSLHostVerifierParams());

        final String dummyParams = "xyz";

        /* Use property constructor to set to a value */
        props.setProperty(SSL_HOST_VERIFIER_PARAMS, dummyParams);
        ReplicationSSLConfig rsc = new ReplicationSSLConfig(props);
        assertEquals(rsc.getSSLHostVerifierParams(), dummyParams);

        /* Make sure we can clear it */
        rsc.setSSLHostVerifierParams("");
        assertEmpty(rsc.getSSLHostVerifierParams());

        /* Make sure we can set it */
        rsc.setSSLHostVerifierParams(dummyParams);
        assertEquals(rsc.getSSLHostVerifierParams(), dummyParams);

    }

    @Test
    public void testSSLHostVerifierConflict() {

        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));
        rsc.setSSLHostVerifier("dnmatch(foo)");
        rsc.setSSLHostVerifierClass(SSLMirrorHostVerifier.class.getName());

        /* Make sure that conflict is detected */
        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSetConfigParam() {

        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        assertEmpty(rsc.getSSLHostVerifierParams());

        final String dummyParams = "xyz";
        rsc.setConfigParam(SSL_HOST_VERIFIER_PARAMS, dummyParams);
        assertEquals(rsc.getSSLHostVerifierParams(), dummyParams);

        rsc.setConfigParam(SSL_HOST_VERIFIER_PARAMS, "");
        assertEmpty(rsc.getSSLHostVerifierParams());
    }

    @Test
    public void testBasicFactory() {

        ReplicationNetworkConfig rnc = new ReplicationBasicConfig();
        DataChannelFactory factory =
            DataChannelFactoryBuilder.construct(rnc);
        assertTrue(factory instanceof SimpleChannelFactory);
    }

    @Test
    public void testBasicFactoryDefault() {

        ReplicationNetworkConfig rnc = ReplicationNetworkConfig.createDefault();
        DataChannelFactory factory =
            DataChannelFactoryBuilder.construct(rnc);
        assertTrue(factory instanceof SimpleChannelFactory);
    }

    @Test
    public void testSSLFactory() {

        ReplicationNetworkConfig rnc = new ReplicationSSLConfig();
        DataChannelFactory factory =
            DataChannelFactoryBuilder.construct(rnc);
        assertTrue(factory instanceof SSLChannelFactory);
    }

    @Test
    public void testSSLConfigNoKSPW() {

        /* Keystore without a password */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadKSPW() {

        /* Keystore with the wrong password */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(
                                       SSL_KEYSTORE_PASSWORD) + "XXX");

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigKSPWSourceNoCtor() {

        /*
         * password source class doesn't have a ctor with the expected
         * signature.
         */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePasswordClass(String.class.getName());

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigKSPWSourceNotImplemented() {

        /*
         * password source class doesn't implement PasswordSource
         */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePasswordClass(DummyFactory.class.getName());

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadKSNotExist() {

        /* Keystore does not exist */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore("/tmp/ThisFileShouldNotExist");
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadKSNotKS() throws IOException {

        /* "Keystore" is not a keystore */
        
        File bogusKS = makeBogusKeyStore();
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(bogusKS.getPath());
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadTSNotExist() {

        /* Keystore does not exist */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));
        rsc.setSSLTrustStore("/tmp/ThisFileShouldNotExist");

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadTSNotTS() throws IOException {

        /* "truststore" is not a truststore */
        
        File bogusTS = makeBogusKeyStore();
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));
        rsc.setSSLTrustStore(bogusTS.getPath());

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadCiphers() {

        /* No valid cipher suites */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));
        rsc.setSSLCipherSuites("BlackMagic");

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testSSLConfigBadProtocols() {

        /* No valid protocols */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        rsc.setSSLKeyStore(stdProps.getProperty(SSL_KEYSTORE_FILE));
        rsc.setSSLKeyStorePassword(stdProps.getProperty(SSL_KEYSTORE_PASSWORD));
        rsc.setSSLCipherSuites("TLSv9");

        try {
            DataChannelFactoryBuilder.construct(rsc);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testCustomFactory() {

        ReplicationBasicConfig rbc = new ReplicationBasicConfig();
        final String constructParams = "abc";

        rbc.setChannelFactoryClass(DummyFactory.class.getName());
        rbc.setChannelFactoryParams(constructParams);

        DataChannelFactory factory = DataChannelFactoryBuilder.construct(rbc);
        DummyFactory constructFactory = (DummyFactory) factory;
        assertEquals(constructFactory.getParams(), constructParams);
    }

    @Test
    public void testBadCustomFactoryNoCtor() {

        ReplicationBasicConfig rbc = new ReplicationBasicConfig();

        /*
         * factory class doesn't have a ctor with the expected signature.
         */
        rbc.setChannelFactoryClass(String.class.getName());

        try {
            DataChannelFactoryBuilder.construct(rbc);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testBadCustomFactoryNotImplemented() {

        ReplicationBasicConfig rbc = new ReplicationBasicConfig();

        /*
         * factory class doesn't implement DataChannelFactory
         */
        rbc.setChannelFactoryClass(DummySource.class.getName());

        try {
            DataChannelFactoryBuilder.construct(rbc);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    public void testBasicClone() {

        /* build the initial config */
        ReplicationBasicConfig rbc = new ReplicationBasicConfig();
        final String constructParams = "abc";

        /* set a representative sample of properties */
        rbc.setChannelFactoryClass(DummyFactory.class.getName());
        rbc.setChannelFactoryParams(constructParams);
        DataChannelFactory factory = DataChannelFactoryBuilder.construct(rbc);

        /* make a clone */
        ReplicationBasicConfig copyRbc = rbc.clone();

        /* clone should produce a distinct object */
        assertFalse(copyRbc == rbc);

        /* Check one of the properties */
        assertEquals(constructParams, copyRbc.getChannelFactoryParams());
    }

    public void testSSLClone() {

        /* build the initial config */
        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        final String constructParams = "abc";
        final String pass = "hello";
        PasswordSource pwSource = new DummySource(pass);

        /* set a representative sample of properties */
        rsc.setChannelFactoryClass(DummyFactory.class.getName());
        rsc.setChannelFactoryParams(constructParams);
        rsc.setSSLKeyStorePasswordSource(pwSource);
        DataChannelFactory factory = DataChannelFactoryBuilder.construct(rsc);

        /* make a clone */
        ReplicationSSLConfig copyRsc = rsc.clone();

        /* clone should produce a distinct object */
        assertFalse(copyRsc == rsc);

        /* Check one of the properties */
        assertEquals(constructParams, copyRsc.getChannelFactoryParams());

        /* The password source should be kept */
        assertTrue(copyRsc.getSSLKeyStorePasswordSource() == pwSource);
    }

    public void testApplyRNP() {

        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        Properties props = new Properties();
        final String inputParams = "abc";

        props.setProperty(SSL_HOST_VERIFIER_PARAMS, inputParams);
        rsc.applyRepNetProperties(props);
        String outputParams = rsc.getSSLHostVerifierParams();
        assertEquals(inputParams, outputParams);
    }

    public void testApplyRNPReject() {

        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        Properties props = new Properties();
        final String inputParams = "abc";
        final String badProp = "not.a.property";
        final String repProp = ReplicationConfig.NODE_NAME;

        props.setProperty(SSL_HOST_VERIFIER_PARAMS, inputParams);
        props.setProperty(badProp, badProp);
        props.setProperty(repProp, repProp);
        rsc.applyRepNetProperties(props);

        Properties rscProps = rsc.getProps();

        String outputParams = rsc.getSSLHostVerifierParams();
        assertEquals(inputParams, outputParams);
        assertNull(rscProps.getProperty(badProp));
        assertNull(rscProps.getProperty(repProp));
    }

    @Test
    public void testSerializeBasic()
        throws Throwable {

        ReplicationBasicConfig rbc = new ReplicationBasicConfig();
        final String constructParams = "abc";

        /* set a representative sample of properties */
        rbc.setChannelFactoryClass(DummyFactory.class.getName());
        rbc.setChannelFactoryParams(constructParams);

        File envHome = SharedTestUtils.getTestDir();
        ReplicationBasicConfig newRbc = (ReplicationBasicConfig)
            TestUtils.serializeAndReadObject(envHome, rbc);

        /* clone should produce a distinct object */
        assertFalse(newRbc == rbc);

        /* Check one of the properties */
        assertEquals(constructParams, newRbc.getChannelFactoryParams());
    }

    @Test
    public void testSerializeSSL()
        throws Throwable {

        ReplicationSSLConfig rsc = new ReplicationSSLConfig();
        final String constructParams = "abc";
        final String pass = "hello";
        PasswordSource pwSource = new DummySource(pass);

        /* set a representative sample of properties */
        rsc.setChannelFactoryClass(DummyFactory.class.getName());
        rsc.setChannelFactoryParams(constructParams);
        rsc.setSSLKeyStorePasswordSource(pwSource);

        File envHome = SharedTestUtils.getTestDir();
        ReplicationSSLConfig newRsc = (ReplicationSSLConfig)
            TestUtils.serializeAndReadObject(envHome, rsc);

        /* clone should produce a distinct object */
        assertFalse(newRsc == rsc);

        /* Check one of the properties */
        assertEquals(constructParams, newRsc.getChannelFactoryParams());

        /* The password source should be discarded */
        assertNull(newRsc.getSSLKeyStorePasswordSource());
    }

    File makeBogusKeyStore() throws IOException {
        final File testDir = SharedTestUtils.getTestDir();
        final File bogusKS = new File(testDir.getPath(), "NotAKeyStore");
        final FileOutputStream fos = new FileOutputStream(bogusKS);
        final byte[] someData = new byte[1000];
        fos.write(someData);
        fos.close();
        return bogusKS;
    }


    public static class DummyFactory implements DataChannelFactory {
        private final String params;

        public DummyFactory(String param) {
            this.params = param;
        }

        public DummyFactory(InstanceParams params) {
            this.params = params.getClassParams();
        }

        public String getParams() {
            return params;
        }

        @Override
        public DataChannel acceptChannel(SocketChannel socketChannel) {
            return null;
        }

        @Override
        public DataChannel connect(InetSocketAddress addr,
                                   ConnectOptions connectOptions) {
            return null;
        }
    }

    public static class DummySource implements PasswordSource {
        private final String password;

        public DummySource(String pass) {
            password = pass;
        }

        public DummySource(InstanceParams params) {
            password = params.getClassParams();
        }

        public String getPasswordString() {
            return password;
        }

        @Override
        public char[] getPassword() {
            return password.toCharArray();
        }
    }

    private void assertEmpty(String value) {
        assertTrue("".equals(value));
    }
}
