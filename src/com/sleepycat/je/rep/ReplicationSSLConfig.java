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


import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * @hidden SSL deferred
 * Specifies the parameters that control replication network communication
 * within a replicated environment using SSL. The parameters contained here are
 * immutable.
 * <p>
 * To change the default settings for a replicated environment, an application
 * creates a configuration object, customizes settings and uses it for {@link
 * ReplicatedEnvironment} construction. Except as noted, the set methods of
 * this class perform only minimal validation of configuration values when the
 * method is called, and value checking is deferred until the time a
 * DataChannel factory is constructed. An IllegalArgumentException is thrown
 * if the value is not valid for that attribute.
 * <p>
 * ReplicationSSLkConfig follows precedence rules similar to those of
 * {@link EnvironmentConfig}.
 * <ol>
 * <li>Configuration parameters specified
 * in {@literal <environmentHome>/je.properties} take first precedence.</li>
 * <li>Configuration parameters set in the ReplicationSSLConfig object used
 * at {@code ReplicatedEnvironment} construction are next.</li>
 * <li>Any configuration parameters not set by the application are set to
 * system defaults, described along with the parameter name String constants
 * in this class.</li>
 *</ol>
 * <p>
 *
 */
public class ReplicationSSLConfig extends ReplicationNetworkConfig {

    private static final long serialVersionUID = 1L;

    /*
     * Note: all replicated parameters should start with
     * EnvironmentParams.REP_PARAMS_PREFIX, which is "je.rep.",
     * see SR [#19080].
     */

    /**
     * Configures the type of communication channel to use.  Valid values
     * for this parameter are:
     * <ul>
     *    <li><code>ssl</code></li>
     * </ul>
     *
     * <code>ssl</code> indicates that SSL is to be used for service
     * communication.  Using SSL normally provides both encryption and
     * authentication. This option supports numerous associated configuration
     * parameters. It requires, at a minimum, that a Java keystore and
     * associated keystore password be supplied. The keystore password can be
     * supplied using multiple methods, considered in the following order:
     *
     *   {@link #setSSLKeyStorePasswordSource}
     *   {@link #SSL_KEYSTORE_PASSWORD_CLASS je.rep.ssl.keyStorePasswordClass}
     *   {@link #setSSLKeyStorePassword}
     *   {@link #SSL_KEYSTORE_PASSWORD je.rep.ssl.keyStorePassword}
     *   The <code>javax.net.ssl.keyStorePassword</code> system property
     *
     * The properties supported by the supplied SSL channel factory are:
     * <pre>
     *   {@link #SSL_KEYSTORE_FILE je.rep.ssl.keyStoreFile}
     *   {@link #SSL_KEYSTORE_PASSWORD_CLASS je.rep.ssl.keyStorePasswordClass}
     *   {@link #SSL_KEYSTORE_PASSWORD_PARAMS je.rep.ssl.keyStorePasswordParams}
     *   {@link #SSL_KEYSTORE_PASSWORD je.rep.ssl.keyStorePassword}
     *   {@link #SSL_KEYSTORE_TYPE je.rep.ssl.keyStoreType}
     *   {@link #SSL_CLIENT_KEY_ALIAS je.rep.ssl.clientKeyAlias}
     *   {@link #SSL_SERVER_KEY_ALIAS je.rep.ssl.serverKeyAlias}
     *   {@link #SSL_TRUSTSTORE_FILE je.rep.ssl.trustStoreFile}
     *   {@link #SSL_TRUSTSTORE_TYPE je.rep.ssl.trustStoreType}
     *   {@link #SSL_CIPHER_SUITES je.rep.ssl.cipherSuites}
     *   {@link #SSL_PROTOCOLS je.rep.ssl.protocols}
     *   {@link #SSL_AUTHENTICATOR je.rep.ssl.authenticator}
     *   {@link #SSL_AUTHENTICATOR_CLASS je.rep.ssl.authenticatorClass}
     *   {@link #SSL_AUTHENTICATOR_PARAMS je.rep.ssl.authenticatorParams}
     *   {@link #SSL_HOST_VERIFIER je.rep.ssl.hostVerifier}
     *   {@link #SSL_HOST_VERIFIER_CLASS je.rep.ssl.hostVerifierClass}
     *   {@link #SSL_HOST_VERIFIER_PARAMS je.rep.ssl.hostVerifierParams}
     * </pre>
     */

    /**
     * The path to the Java keystore file for SSL data channnel factories.
     * The specified path must be absolute.
     * If this parameter is not set or has an empty value, the Java system
     * property <code>javax.net.ssl.keyStore</code> is used.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_KEYSTORE_FILE =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.keyStoreFile";

    /**
     * The password for accessing the Java keystore file for SSL data channnel
     * factories. If this parameter is not set or has an empty value, the Java
     * system property <code>javax.net.ssl.keyStorePassword</code> is used.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_KEYSTORE_PASSWORD =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.keyStorePassword";

    /**
     * A class that will be instantiated in order to retrieve a password that
     * allows access to the keystore file. The class must implement the
     * <code>com.sleepycat.je.rep.net.PasswordSource</code> interface.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_KEYSTORE_PASSWORD_CLASS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.keyStorePasswordClass";

    /**
     * A string encoding the parameters for configuring the password class.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_KEYSTORE_PASSWORD_PARAMS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.keyStorePasswordParams";

    /**
     * The type of the Java keystore file. This is used to determine what
     * keystore implementation should be used to manipulate the named
     * keystore file. If set to a non-empty value, the value must be a valid
     * keystore type for the Java environment. If this parameter is not set to
     * a non-empty value, the default Java keystore type is assumed.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_KEYSTORE_TYPE =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.keyStoreType";

    /**
     * The alias name of the preferred key for use by the service dispatcher
     * acting in SSL server mode.  When not set to a non-empty value and the
     * keystore contains multiple key options, the key selection algorithm is
     * unspecified.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_SERVER_KEY_ALIAS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.serverKeyAlias";

    /**
     * The alias name of the preferred key for use by a client connecting
     * to the service dispatcher.  When not set to a non-empty value and the
     * keystore contains multiple key options, the key selection algorithm is
     * unspecified.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_CLIENT_KEY_ALIAS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.clientKeyAlias";

    /**
     * The path to the Java truststore file for SSL data channel factories.
     * The specified path must be absolute.
     * If this parameter is not set to a non-empty value, the Java system
     * property <code>javax.net.ssl.trustStore</code> is used.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_TRUSTSTORE_FILE =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.trustStoreFile";

    /**
     * The type of the Java truststore file. This is used to determine what
     * keystore implementation should be used to manipulate the named
     * keystore file. If set to a non-empty value, the value must be a valid
     * keystore type for the Java environment. If this parameter is not set to
     * a non-empty value, the default Java keystore type is assumed.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"JKS"</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_TRUSTSTORE_TYPE =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.trustStoreType";

    /**
     * The list of SSL cipher suites that are acceptable for SSL data channel
     * factories.  The cipher suite list must be in comma-delimited form.
     * If this parameter is not set to a non-empty value, the Java default
     * set of enabled cipher suites is allowed.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_CIPHER_SUITES =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.cipherSuites";

    /**
     * The list of SSL protocols that are acceptable for SSL data channel
     * factories.  The protocol list must be in comma-delimited form.
     * If not specified, the default type selected is TBD: TLSv1.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_PROTOCOLS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.protocols";

    /**
     * The specification for an SSL authenicator.
     * The authenticator can be configured in one of the following ways:
     * <ul>
     *   <li><code>mirror</code></li>
     *   <li><code>dnmatch(</code>&lt;Regular Expression&gt;<code>)</code></li>
     * </ul>
     *
     * The <code>mirror</code> option causes the authenticator to check that the
     * Distinguished Name(DN) in the certificate of the incoming client
     * connection matches the DN of the certificate that this server presents
     * when connecting as a client to another server.
     *
     * The <code>dnmatch()</code> option causes the authenticator to check that
     * the DN in the certificate of the incoming client connection matches the
     * regular expression provided in the dnmatch() specification.
     *
     * Do not configure both the SSL authenticator and the SSL authenticator
     * class, or an exception will be thrown during DataChannelFactory
     * instantiation.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_AUTHENTICATOR =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.authenticator";

    /**
     * The string identifying a class to be instantiated to check whether
     * incoming client SSL connections are to be trusted. If specified, the
     * string must be a fully qualified Java class name for a class that
     * implements the {@link SSLAuthenticator}
     * interface and provides a public constructor with an argument list of
     * the form
     *   ( {@link InstanceParams} ).
     * <p>
     * Do not configure both the SSL authenticator and the SSL authenticator
     * class, or an exception will be thrown during DataChannelFactory
     * instantiation.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_AUTHENTICATOR_CLASS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.authenticatorClass";

    /**
     * A string encoding the parameters for configuring the authenticator class.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_AUTHENTICATOR_PARAMS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.authenticatorParams";

    /**
     * The configuration to be used for verifying the certificate of
     * a server when a connection is made.
     *
     * The verifier can be configured in one of the following ways:
     * <ul>
     *   <li><code>hostname</code></li>
     *   <li><code>mirror</code></li>
     *   <li><code>dnmatch(</code>&lt;Regular Expression&gt;<code>)</code></li>
     * </ul>
     * <p>
     * The <code>hostname</code> option causes the verifier to check that the
     * Distinguished Name(DN) or one of the Subject Alternative Names in the
     * certificate presented by the server contains the hostname that
     * was the target of the connection attempt.  This assumes that server
     * certificates are unique per server.
     *  <p>
     * The <code>mirror</code> option causes the verifier to check that the
     * Distinguished Name(DN) in the certificate of the server matches the DN
     * of the certificate that this server presents to incoming client
     * connections.  This assumes that all servers have equivalent certificates.
     *  <p>
     * The <code>dnmatch()</code> option causes the verifier to check that
     * the DN in the certificate of the server matches the regular expression
     * string provided in the dnmatch() specification.
     *  <p>
     * Do not configure both the SSL host verifier and the SSL host verifier
     * class, or an exception will be thrown during DataChannelFactory
     * instantiation.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_HOST_VERIFIER =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.hostVerifier";

    /**
     * The class to be instantiated to check whether the target host of a
     * connection initiated by a client is to be trusted. If specified, the
     * string must be a fully qualified Java class name for a class that
     * implements the <code>javax.net.ssl.HostnameVerifier</code> interface
     * and provides a public constructor with an argument list of the form
     *   ({@link InstanceParams}).
     * <p>
     * Do not configure both the SSL host verifier and the SSL host verifier
     * class, or an exception will be thrown during DataChannelFactory
     * instantiation.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_HOST_VERIFIER_CLASS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.hostVerifierClass";

    /**
     * A string encoding the parameters for configuring the host verifier
     * class, if needed.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table></p>
     */
    public static final String SSL_HOST_VERIFIER_PARAMS =
        EnvironmentParams.REP_PARAM_PREFIX + "ssl.hostVerifierParams";

    /* The set of Replication properties specific to this class */
    private static Set<String> repSSLProperties;
    static {
        repSSLProperties = new HashSet<String>();
        repSSLProperties.add(SSL_KEYSTORE_FILE);
        repSSLProperties.add(SSL_KEYSTORE_PASSWORD);
        repSSLProperties.add(SSL_KEYSTORE_PASSWORD_CLASS);
        repSSLProperties.add(SSL_KEYSTORE_PASSWORD_PARAMS);
        repSSLProperties.add(SSL_KEYSTORE_TYPE);
        repSSLProperties.add(SSL_SERVER_KEY_ALIAS);
        repSSLProperties.add(SSL_CLIENT_KEY_ALIAS);
        repSSLProperties.add(SSL_TRUSTSTORE_FILE);
        repSSLProperties.add(SSL_TRUSTSTORE_TYPE);
        repSSLProperties.add(SSL_CIPHER_SUITES);
        repSSLProperties.add(SSL_PROTOCOLS);
        repSSLProperties.add(SSL_AUTHENTICATOR);
        repSSLProperties.add(SSL_AUTHENTICATOR_CLASS);
        repSSLProperties.add(SSL_AUTHENTICATOR_PARAMS);
        repSSLProperties.add(SSL_HOST_VERIFIER);
        repSSLProperties.add(SSL_HOST_VERIFIER_CLASS);
        repSSLProperties.add(SSL_HOST_VERIFIER_PARAMS);
        /* Nail the set down */
        repSSLProperties = Collections.unmodifiableSet(repSSLProperties);
    }

    static {

        /*
         * Force loading when a ReplicationNetworkConfig is used and an
         * environment has not been created.
         */
        @SuppressWarnings("unused")
        final ConfigParam forceLoad = RepParams.CHANNEL_TYPE;
    }

    /* The possibly null password source for keystore access */
    private transient PasswordSource sslKeyStorePasswordSource;

    /**
     * Constructs a ReplicationSSLConfig initialized with the system default
     * settings. Defaults are documented with the string constants in this
     * class.
     */
    public ReplicationSSLConfig() {
    }

    /**
     * Creates an ReplicationSSLConfig which includes the properties
     * specified in the properties parameter.
     *
     * @param properties Supported properties are described as the string
     * constants in this class.
     *
     * @throws IllegalArgumentException If any properties read from the
     * properties parameter are invalid.
     */
    public ReplicationSSLConfig(Properties properties)
        throws IllegalArgumentException {

        super(properties);
    }

    /**
     * Get the channel type setting for the replication service.
     *
     * @return the channel type
     */
    @Override
    public String getChannelType() {
        return "ssl";
    }

    /**
     * Returns the name of the Java KeyStore file to be used for SSL key pair
     * retrieval.
     *
     * @return the KeyStore file name
     */
    public String getSSLKeyStore() {
        return DbConfigManager.getVal(props, RepParams.SSL_KEYSTORE_FILE);
    }

    /**
     * Sets the name of the Java KeyStore file to be used when creating
     * SSL connections.
     *
     * @param filename the KeyStore filename
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLKeyStore(String filename) {

        setSSLKeyStoreVoid(filename);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLKeyStoreVoid(String filename) {

        DbConfigManager.setVal(props, RepParams.SSL_KEYSTORE_FILE, filename,
                               validateParams);
    }

    /**
     * Returns the type of the Java Keystore file to be used for SSL key pair
     * retrieval.
     *
     * @return the KeyStore type
     */
    public String getSSLKeyStoreType() {
        return DbConfigManager.getVal(props, RepParams.SSL_KEYSTORE_TYPE);
    }

    /**
     * Sets the type of the Java KeyStore file to be used when creating
     * SSL connections.
     *
     * @param keyStoreType the Keystore type
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLKeyStoreType(String keyStoreType) {

        setSSLKeyStoreTypeVoid(keyStoreType);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLKeyStoreTypeVoid(String keyStoreType) {

        DbConfigManager.setVal(props, RepParams.SSL_KEYSTORE_TYPE,
                               keyStoreType, validateParams);
    }

    /**
     * Returns the password for the Java KeyStore file to be used for SSL key
     * pair retrieval.
     *
     * @return the KeyStore password
     */
    public String getSSLKeyStorePassword() {
        return DbConfigManager.getVal(props, RepParams.SSL_KEYSTORE_PASSWORD);
    }

    /**
     * Sets the password for the Java KeyStore file to be used when creating
     * SSL connections.
     *
     * @param password the KeyStore password
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLKeyStorePassword(String password) {

        setSSLKeyStorePasswordVoid(password);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLKeyStorePasswordVoid(String password) {

        DbConfigManager.setVal(props, RepParams.SSL_KEYSTORE_PASSWORD, password,
                               validateParams);
    }

    /**
     * Returns the name of a class that should be instantiated to retrieve the
     * password for the Java KeyStore file.
     *
     * @return the KeyStore password source class name
     */
    public String getSSLKeyStorePasswordClass() {

        return DbConfigManager.getVal(props,
                                      RepParams.SSL_KEYSTORE_PASSWORD_CLASS);
    }

    /**
     * Sets the name of a class that should be instantiated to retrieve the
     * password for the Java KeyStore file.
     *
     * @param className the name of the class
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLKeyStorePasswordClass(
        String className) {

        setSSLKeyStorePasswordClassVoid(className);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLKeyStorePasswordClassVoid(String className) {

        DbConfigManager.setVal(props, RepParams.SSL_KEYSTORE_PASSWORD_CLASS,
                               className, validateParams);
    }

    /**
     * Returns a string to be used in the constructor for a keystore password
     * source instance.
     *
     * @return the parameter values
     */
    public String getSSLKeyStorePasswordParams() {

        return DbConfigManager.getVal(props,
                                      RepParams.SSL_KEYSTORE_PASSWORD_PARAMS);
    }

    /**
     * Sets the string to be used in the constructor for a keystore password
     * source instance.
     *
     * @param params a string that is to be passed to the constructor
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLKeyStorePasswordParams(
        String params) {

        setSSLKeyStorePasswordParamsVoid(params);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLKeyStorePasswordParamsVoid(String params) {

        DbConfigManager.setVal(props, RepParams.SSL_KEYSTORE_PASSWORD_PARAMS,
                               params, validateParams);
    }

    /**
     * Returns the Java KeyStore alias associated with the key that should be
     * used to accept incoming SSL connections.
     *
     * @return the KeyStore alias
     */
    public String getSSLServerKeyAlias() {
        return DbConfigManager.getVal(props, RepParams.SSL_SERVER_KEY_ALIAS);
    }

    /**
     * Sets the alias associated with the key in the Java KeyStore file to be
     * used when accepting incoming SSL connections.
     *
     * @param alias the KeyStore alias
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLServerKeyAlias(String alias) {

        setSSLServerKeyAliasVoid(alias);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLServerKeyAliasVoid(String alias) {

        DbConfigManager.setVal(props, RepParams.SSL_SERVER_KEY_ALIAS, alias,
                               validateParams);
    }

    /**
     * Returns the Java KeyStore alias associated with the key that should be
     * used when initiating SSL connections .
     *
     * @return the KeyStore alias
     */
    public String getSSLClientKeyAlias() {
        return DbConfigManager.getVal(props, RepParams.SSL_CLIENT_KEY_ALIAS);
    }

    /**
     * Sets the alias associated with the key in the Java KeyStore file to be
     * used when initiating SSL connections.
     *
     * @param alias the KeyStore alias
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLClientKeyAlias(String alias) {

        setSSLClientKeyAliasVoid(alias);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLClientKeyAliasVoid(String alias) {

        DbConfigManager.setVal(props, RepParams.SSL_CLIENT_KEY_ALIAS, alias,
                               validateParams);
    }

    /**
     * Returns the name of the Java TrustStore file to be used for SSL
     * certificate validation.
     *
     * @return the TrustStore file name
     */
    public String getSSLTrustStore() {
        return DbConfigManager.getVal(props, RepParams.SSL_TRUSTSTORE_FILE);
    }

    /**
     * Sets the name of the Java TrustStore file to be used when validating
     * SSL certificates.
     *
     * @param filename the TrustStore filename
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLTrustStore(String filename) {

        setSSLTrustStoreVoid(filename);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLTrustStoreVoid(String filename) {

        DbConfigManager.setVal(props, RepParams.SSL_TRUSTSTORE_FILE, filename,
                               validateParams);
    }

    /**
     * Returns the type of the Java Truststore file to be used for SSL key pair
     * retrieval.
     *
     * @return the Truststore type
     */
    public String getSSLTrustStoreType() {
        return DbConfigManager.getVal(props, RepParams.SSL_TRUSTSTORE_TYPE);
    }

    /**
     * Sets the type of the Java Truststore file to be used when creating
     * SSL connections.
     *
     * @param trustStoreType the Truststore type
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLTrustStoreType(String trustStoreType) {

        setSSLTrustStoreTypeVoid(trustStoreType);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLTrustStoreTypeVoid(String trustStoreType) {

        DbConfigManager.setVal(props, RepParams.SSL_TRUSTSTORE_TYPE,
                               trustStoreType, validateParams);
    }

    /**
     * Returns the list of SSL cipher suites that are acceptable
     *
     * @return the list of SSL cipher suites in comma-delimited form
     */
    public String getSSLCipherSuites() {
        return DbConfigManager.getVal(props, RepParams.SSL_CIPHER_SUITES);
    }

    /**
     * Sets the list of SSL cipher suites that are acceptable
     *
     * @param cipherSuites a comma-delimited list of SSL cipher suites
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLCipherSuites(String cipherSuites) {

        setSSLCipherSuitesVoid(cipherSuites);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLCipherSuitesVoid(String cipherSuites) {

        DbConfigManager.setVal(props, RepParams.SSL_CIPHER_SUITES, cipherSuites,
                               validateParams);
    }

    /**
     * Returns the list of SSL protocols that are acceptable
     *
     * @return the list of SSL protocols in comma-delimited form
     */
    public String getSSLProtocols() {
        return DbConfigManager.getVal(props, RepParams.SSL_PROTOCOLS);
    }

    /**
     * Sets the list of SSL protocols that are acceptable
     *
     * @param protocols a comma-delimited list of SSL protocols
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLProtocols(String protocols) {

        setSSLProtocolsVoid(protocols);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLProtocolsVoid(String protocols) {

        DbConfigManager.setVal(props, RepParams.SSL_PROTOCOLS, protocols,
                               validateParams);
    }

    /**
     * Returns the SSLAuthenticator configuration to be used for authenticating
     * incoming client connections.
     *
     * @return the authentication configuration, if configured
     */
    public String getSSLAuthenticator() {

        return DbConfigManager.getVal(props, RepParams.SSL_AUTHENTICATOR);
    }

    /**
     * Sets the authenticator configuration to be used for authenticating
     * incoming client connections.
     *
     * See {@link #SSL_AUTHENTICATOR} for a complete description of this
     * parameter.
     *
     * @param authenticator the authentication configuration to use
     *
     * @return this
     *
     * @throws IllegalArgumentException if the authenticator specification
     * is not syntactically valid
     */
    public ReplicationNetworkConfig setSSLAuthenticator(String authenticator)
        throws IllegalArgumentException {

        setSSLAuthenticatorVoid(authenticator);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLAuthenticatorVoid(String authenticator)
        throws IllegalArgumentException {

        DbConfigManager.setVal(props, RepParams.SSL_AUTHENTICATOR,
                               authenticator, validateParams);
    }

    /**
     * Returns the SSLAuthenticator factory class to be used for creating
     * new Authenticator instances
     *
     * @return the SSLAuthenticator factory class name, if configured
     */
    public String getSSLAuthenticatorClass() {

        return DbConfigManager.getVal(
            props, RepParams.SSL_AUTHENTICATOR_CLASS);
    }

    /**
     * Sets the authenticator class to be instantiated for creation of
     * new SSL Authenticator instances.
     *
     * @param authenticatorClass the class name to use
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLAuthenticatorClass(
        String authenticatorClass) {

        setSSLAuthenticatorClassVoid(authenticatorClass);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLAuthenticatorClassVoid(String authenticatorClass)
        throws IllegalArgumentException {

        DbConfigManager.setVal(props, RepParams.SSL_AUTHENTICATOR_CLASS,
                               authenticatorClass, validateParams);
    }

    /**
     * Returns the SSLAuthenticator parameters to be used for creating
     * new Authenticator instances
     *
     * @return the SSLAuthenticator factory params name, if configured
     */
    public String getSSLAuthenticatorParams() {

        return DbConfigManager.getVal(
            props, RepParams.SSL_AUTHENTICATOR_PARAMS);
    }

    /**
     * Sets the Authenticator parameters to be passed to the
     * SSL server Authenticator class when instantiated.
     *
     * @param authenticatorParams the parameter value to use
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLAuthenticatorParams(
        String authenticatorParams) {

        setSSLAuthenticatorParamsVoid(authenticatorParams);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLAuthenticatorParamsVoid(String authenticatorParams) {

        DbConfigManager.setVal(props, RepParams.SSL_AUTHENTICATOR_PARAMS,
                               authenticatorParams, validateParams);
    }

    /**
     * Returns the HostnameVerifier factory class to be used for creating
     * new host verifier instances for client-mode operation
     *
     * @return the HostnameVerifier factory class name, if configured
     */
    public String getSSLHostVerifier() {

        return DbConfigManager.getVal(
            props, RepParams.SSL_HOST_VERIFIER);
    }

    /**
     * Sets the configuration to be used for verifying the certificate of
     * a server when a connection is made.
     *
     * See {@link #SSL_HOST_VERIFIER} for a complete description of this
     * parameter.
     *
     * @param hostVerifier the verifier configuration to use
     *
     * @return this
     *
     * @throws IllegalArgumentException if the authenticator specification
     * is not syntactically valid
     */
    public ReplicationNetworkConfig setSSLHostVerifier(String hostVerifier)
        throws IllegalArgumentException {

        setSSLHostVerifierVoid(hostVerifier);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLHostVerifierVoid(String hostVerifier)
        throws IllegalArgumentException {

        DbConfigManager.setVal(props, RepParams.SSL_HOST_VERIFIER,
                               hostVerifier, validateParams);
    }

    /**
     * Returns the HostnameVerifier factory class to be used for creating
     * new host verifier instances for client-mode operation
     *
     * @return the HostnameVerifier factory class name, if configured
     */
    public String getSSLHostVerifierClass() {

        return DbConfigManager.getVal(
            props, RepParams.SSL_HOST_VERIFIER_CLASS);
    }

    /**
     * Sets the host verifier class to be instantiated for creation of
     * new SSL host verifier instances.
     *
     * @param hostVerifierClass the class name to use
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLHostVerifierClass(
        String hostVerifierClass) {

        setSSLHostVerifierClassVoid(hostVerifierClass);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLHostVerifierClassVoid(String hostVerifierClass) {

        DbConfigManager.setVal(props, RepParams.SSL_HOST_VERIFIER_CLASS,
                               hostVerifierClass, validateParams);
    }

    /**
     * Returns the SSLHostVerifier parameters to be used for creating
     * new host verifier instances for operation in client mode, if needed.
     *
     * @return the SSLHostVerifier factory params name, if configured
     */
    public String getSSLHostVerifierParams() {

        return DbConfigManager.getVal(
            props, RepParams.SSL_HOST_VERIFIER_PARAMS);
    }

    /**
     * Sets the host verifier parameters to be passed to the SSL host verifier
     * class when instantiated.
     *
     * @param hostVerifierParams the parameter value to use
     *
     * @return this
     */
    public ReplicationNetworkConfig setSSLHostVerifierParams(
        String hostVerifierParams) {

        setSSLHostVerifierParamsVoid(hostVerifierParams);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSSLHostVerifierParamsVoid(String hostVerifierParams) {

        DbConfigManager.setVal(props, RepParams.SSL_HOST_VERIFIER_PARAMS,
                               hostVerifierParams, validateParams);
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public ReplicationSSLConfig clone() {
        return (ReplicationSSLConfig) super.clone();
    }

    /**
     * Gets the password source provided for KeyStore access by the SSL
     * implementation.
     */
    public PasswordSource getSSLKeyStorePasswordSource() {
        return sslKeyStorePasswordSource;
    }

    /**
     * Sets the password source for KeyStore access by the SSL implementation.
     * If not set to a non-empty value, the SSL implementation uses the
     * {@link #SSL_KEYSTORE_PASSWORD je.rep.ssl.keyStorePassword}
     * property instead.
     * This setting is not included in the serialized representation.
     */
    public ReplicationNetworkConfig
        setSSLKeyStorePasswordSource(PasswordSource passwordSource) {

        setSSLKeyStorePasswordSourceVoid(passwordSource);
        return this;
    }

    /**
     * @hidden
     * For bean editors.
     */
    public void setSSLKeyStorePasswordSourceVoid(
        PasswordSource passwordSource) {
        sslKeyStorePasswordSource = passwordSource;
    }

    /**
     * @hidden
     * Enumerate the subset of configuration properties that are intended to
     * control network access.
     */
    static Set<String> getRepSSLPropertySet() {

        return repSSLProperties;
    }

    /**
     * Checks whether the named parameter is valid for this configuration type.
     * @param paramName the configuration parameter name, one of the String
     * constants in this class
     * @return true if the named parameter is a valid parameter name
     */
    protected boolean isValidConfigParam(String paramName) {
        if (repSSLProperties.contains(paramName)) {
            return true;
        }
        return super.isValidConfigParam(paramName);
    }

    static {

        /*
         * Force loading when a ReplicationNetworkConfig is used and an
         * environment has not been created.
         */
        @SuppressWarnings("unused")
        final ConfigParam forceLoad = RepParams.CHANNEL_TYPE;
    }

}
