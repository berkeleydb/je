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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.utilint.RepUtils;

/**
 * @hidden SSL deferred
 * This is the root class for specifying the parameters that control
 * replication network communication within a a replicated environment. The
 * parameters contained here are immutable.
 * <p>
 * To change the default settings for a replicated environment, an application
 * creates a configuration object, customizes settings and uses it for {@link
 * ReplicatedEnvironment} construction. Except as noted, the set methods of
 * this class perform only minimal validation of configuration values when the
 * method is called, and value checking is deferred until the time a
 * DataChannel factory is constructed. An IllegalArgumentException is thrown
 * if the value is not valid for that attribute.
 * <p>
 * ReplicationNetworkConfig follows precedence rules similar to those of
 * {@link EnvironmentConfig}.
 * <ol>
 * <li>Configuration parameters specified
 * in {@literal <environmentHome>/je.properties} take first precedence.</li>
 * <li>Configuration parameters set in the ReplicationNetworkConfig object used
 * at {@code ReplicatedEnvironment} construction are next.</li>
 * <li>Any configuration parameters not set by the application are set to
 * system defaults, described along with the parameter name String constants
 * in this class.</li>
 *</ol>
 * <p>
 *
 */
public abstract class ReplicationNetworkConfig
    implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * Note: all replicated parameters should start with
     * EnvironmentParams.REP_PARAMS_PREFIX, which is "je.rep.",
     * see SR [#19080].
     */

    /*
     * The following is currently undocumented:
     * The channelType property may also take the value:
     *    <code>custom</code>
     * <code>custom</code> indicates that the channel implementation is to be
     * provided by the application.  This can be done through the use of the
     * combination of two configuration parameters
     * <pre>
     *   {@link #CHANNEL_FACTORY_CLASS je.rep.channelFactoryClass}
     *   {@link #CHANNEL_FACTORY_PARAMS je.rep.channelFactoryParams}
     * </pre>
     */

    /**
     * Configures the type of communication channel to use.  This property is
     * not directly setable.  It can be specified in a property file or
     * property set passed to a create() method, or a direct instantiation of
     * a class derived from this class may be used.  When set through one of
     * the create() methods or when read through the getChannelType() method,
     * the valid values for this parameter are:
     * <ul>
     *    <li><code>basic</code></li>
     *    <li><code>ssl</code></li>
     * </ul>
     *
     * <code>basic</code> is the standard implementation, which uses ordinary,
     * unencrypted communication, and is represented by this the
     * {@link ReplicationBasicConfig} class.
     * <p>
     * <code>ssl</code> indicates that SSL is to be used for service
     * communication.  When using SSL, an instance of 
     * {@link ReplicationSSLConfig} must be used.  
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"basic"</td>
     * </tr>
     * </table></p>
     */
    public static final String CHANNEL_TYPE =
        EnvironmentParams.REP_PARAM_PREFIX + "channelType";

    /**
     * @hidden
     * A string identifying a class to instantiate as the data channel
     * factory.  Typical product use does not require this configuration
     * parameter, but this allows a custom data channel factory to be supplied.
     * If supplied, it must be a fully qualified Java class name for a class
     * that implements the {@link DataChannelFactory} interface and
     * provides a public constructor with an argument list of the form
     *   ( {@link InstanceParams} )
     *<p>
     * Note: Setting this class instantiated from this parameter must
     * be of the same configuration type as indicated by channelType().
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
    public static final String CHANNEL_FACTORY_CLASS =
        EnvironmentParams.REP_PARAM_PREFIX + "channelFactoryClass";

    /**
     * @hidden
     * A string providing factory-specific data channel configuration
     * parameters.  The encoding of parameters within the string is determined
     * by the specific factory class implementation.  As examples, it may
     * choose to join multiple strings with a delimiter character or may
     * allow binary data to be hex-encoded.
     *
     * Note: Setting this parameter is ignored unless
     * {@link #CHANNEL_TYPE je.rep.channelFactoryClass} is set.
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
    public static final String CHANNEL_FACTORY_PARAMS =
        EnvironmentParams.REP_PARAM_PREFIX + "channelFactoryParams";

    /**
     * A string providing a logging context identification string. This string
     * is incorporated into log messages in order to help associate messages
     * with the configuration context.
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
    public static final String CHANNEL_LOG_NAME =
        EnvironmentParams.REP_PARAM_PREFIX + "channelLogName";

    /* The set of Replication properties specific to this class */
    private static Set<String> repNetLocalProperties;
    static {
        repNetLocalProperties = new HashSet<String>();
        repNetLocalProperties.add(CHANNEL_TYPE);
        repNetLocalProperties.add(CHANNEL_LOG_NAME);
        repNetLocalProperties.add(CHANNEL_FACTORY_CLASS);
        repNetLocalProperties.add(CHANNEL_FACTORY_PARAMS);
        /* Nail the set down */
        repNetLocalProperties =
            Collections.unmodifiableSet(repNetLocalProperties);
    }

    /*
     * The set of Replication properties for this class and derived classes.
     * It is created later, no demand, to deal with class loading ordering
     */
    private static Set<String> repNetAllProperties;

    static {

        /*
         * Force loading when a ReplicationNetworkConfig is used and an
         * environment has not been created.
         */
        @SuppressWarnings("unused")
        final ConfigParam forceLoad = RepParams.CHANNEL_TYPE;
    }

    /* The properties for this configuration */
    protected final Properties props;
    protected final boolean validateParams = true;

    /**
     * Creates an ReplicationNetworkConfig which includes the properties
     * specified in the named properties file.
     *
     * @param propFile a File from which the configuration properties will
     * be read.
     *
     * @return an instance of a class derived from ReplicationNetworkConfig
     * as indicated by the channelType property.
     *
     * @throws FileNotFoundException If the property file cannot be found
     * @throws IllegalArgumentException If any properties read from the
     * properties parameter are invalid.
     */
    public static ReplicationNetworkConfig create(File propFile)
        throws IllegalArgumentException, FileNotFoundException {

        return create(readProperties(propFile));
    }

    /**
     * Creates an ReplicationNetworkConfig which includes the properties
     * specified in the properties parameter.
     *
     * @param properties Supported properties are described as the string
     * constants in this class.
     *
     * @return an instance of a class derived from ReplicationNetworkConfig
     * as indicated by the channelType property.
     *
     * @throws IllegalArgumentException If any properties read from the
     * properties parameter are invalid.
     */
    public static ReplicationNetworkConfig create(Properties properties)
        throws IllegalArgumentException {

        final String channelType =
            DbConfigManager.getVal(properties, RepParams.CHANNEL_TYPE);

        if ("basic".equals(channelType)) {
            return new ReplicationBasicConfig(properties);
        }
        if ("ssl".equals(channelType)) {
            return new ReplicationSSLConfig(properties);
        }
        throw new IllegalArgumentException(
            "Unknown channel type: " + channelType);
    }

    /**
     * Creates a default ReplicationNetworkConfig instance.
     *
     * @return an instance of a class derived from ReplicationNetworkConfig
     * as indicated by the channelType property default.
     */
    public static ReplicationNetworkConfig createDefault() {

        return new ReplicationBasicConfig();
    }

    /**
     * Constructs a basic ReplicationNetworkConfig initialized with the system
     * default settings. Defaults are documented with the string constants in
     * this class.
     */
    public ReplicationNetworkConfig() {
        props = new Properties();
    }

    /**
     * Constructs a basic ReplicationNetworkConfig initialized with the
     * provided propeties.
     * @param properties a set of properties which which to initialize the
     * instance properties
     */
    public ReplicationNetworkConfig(Properties properties) {
        props = new Properties();
        applyRepNetProperties(properties);
    }

    /**
     * Get the channel type setting for the replication service.
     *
     * @return the channel type
     */
    public abstract String getChannelType();

    /**
     * Get the channel logging name setting for the replication service.
     *
     * @return the channel logging name
     */
    public String getLogName() {
        return DbConfigManager.getVal(props, RepParams.CHANNEL_LOG_NAME);
    }

    /**
     * Sets the channel logging name to be used for replication service access.
     *
     * @param logName the channel logging name to be used.
     *
     * @return this
     *
     * @throws IllegalArgumentException If the value of logName is invalid.
     */
    public ReplicationNetworkConfig setLogName(String logName)
        throws IllegalArgumentException {

        setLogNameVoid(logName);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setLogNameVoid(String logName)
        throws IllegalArgumentException {

        DbConfigManager.setVal(props, RepParams.CHANNEL_LOG_NAME, logName,
                               validateParams);
    }

    /**
     * @hidden
     * Returns name of the DataChannel factory class to be used for creating
     * new DataChannel instances
     *
     * @return the DataChannelFactory class name, if configured
     */
    public String getChannelFactoryClass() {

        return DbConfigManager.getVal(
            props, RepParams.CHANNEL_FACTORY_CLASS);
    }

    /**
     * @hidden
     * Sets the name of the DataChannelFactory class to be instantiated for
     * creation of new DataChannel instances.
     *
     * @param factoryClass the class name to use
     *
     * @return this
     */
    public ReplicationNetworkConfig setChannelFactoryClass(
        String factoryClass) {

        setChannelFactoryClassVoid(factoryClass);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setChannelFactoryClassVoid(String factoryClass) {

        DbConfigManager.setVal(props, RepParams.CHANNEL_FACTORY_CLASS,
                               factoryClass, validateParams);
    }

    /**
     * @hidden
     * Returns the DataChannelFactory class parameters to be used for when
     * instantiating the DataChannelFactoryClass
     *
     * @return the parameters argument, if configured
     */
    public String getChannelFactoryParams() {

        return DbConfigManager.getVal(props, RepParams.CHANNEL_FACTORY_PARAMS);
    }

    /**
     * @hidden
     * Sets the DataChannelFactory parameters to be passed when instantiating
     * the DataChannelFactoryClass.
     *
     * @param factoryParams a string encoding any parameters to be passed to
     * the class constructor.
     *
     * @return this
     */
    public ReplicationNetworkConfig setChannelFactoryParams(
        String factoryParams) {

        setChannelFactoryParamsVoid(factoryParams);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setChannelFactoryParamsVoid(String factoryParams) {

        DbConfigManager.setVal(props, RepParams.CHANNEL_FACTORY_PARAMS,
                               factoryParams, validateParams);
    }

    /**
     * Set this configuration parameter with this value. Values are validated
     * before setting the parameter.
     *
     * @param paramName the configuration parameter name, one of the String
     * constants in this class
     * @param value the configuration value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the paramName or value is invalid, or
     * if paramName is not a parameter that applies to ReplicationNetworkConfig.
     */
    public ReplicationNetworkConfig setConfigParam(
        String paramName, String value)
        throws IllegalArgumentException {

        if (isValidConfigParam(paramName)) {
            setConfigParam(props, paramName, value, validateParams);
        }
        return this;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public ReplicationNetworkConfig clone() {
        try {
            ReplicationNetworkConfig copy =
                (ReplicationNetworkConfig) super.clone();
            return copy;
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * @hidden
     * Enumerate the subset of configuration properties that are intended to
     * control network access that are specific to this class.
     */
    static Set<String> getRepNetLocalPropertySet() {

        return repNetLocalProperties;
    }

    /**
     * @hidden
     * Enumerate the subset of configuration properties that are intended to
     * control network access.
     */
    public static Set<String> getRepNetPropertySet() {

        synchronized(repNetLocalProperties) {
            if (repNetAllProperties == null) {
                Set<String> props = new HashSet<String>();
                props.addAll(repNetLocalProperties);
                props.addAll(ReplicationBasicConfig.getRepBasicPropertySet());
                props.addAll(ReplicationSSLConfig.getRepSSLPropertySet());
                /* Nail the set down */
                repNetAllProperties = Collections.unmodifiableSet(props);
            }
            return repNetAllProperties;
        }
    }

    /**
     * @hidden
     * Ensure that the parameters for this and all known derived classes are
     * registered.  This is called by testing code to ensure that parameter
     * registration happens when non-standard API access is used.
     */
    public static void registerParams() {
        /* Call for side effect */
        getRepNetPropertySet();
    }


    /**
     * @hidden
     * Apply the configurations specified in sourceProps to our properties.
     *
     * @throws IllegalArgumentException if any of the contained property
     * entries have invalid names or invalid values
     */
    void applyRepNetProperties(Properties sourceProps)
        throws IllegalArgumentException {

        for (Map.Entry<Object, Object> propPair : sourceProps.entrySet()) {
            final String name = (String) propPair.getKey();
            if (!isValidConfigParam(name)) {
                continue;
            }
            final String value = (String) propPair.getValue();
            setConfigParam(props, name, value,
                           true /* validateParams */);
        }
    }

    /**
     * @hidden
     * Apply the configurations specified in sourceProps to updateProps,
     * effectively copying the replication service access properties from the
     * sourceProps hash. Only the source properties that are applicable to
     * a ReplicationNetworkConfig are used.
     *
     * @throws IllegalArgumentException if any of the contained property
     * entries have invalid values
     */
    public static void applyRepNetProperties(Properties sourceProps,
                                             Properties updateProps)
        throws IllegalArgumentException {

        final Set<String> repNetProps = getRepNetPropertySet();
        for (Map.Entry<Object, Object> propPair : sourceProps.entrySet()) {
            final String name = (String) propPair.getKey();
            if (!repNetProps.contains(name)) {
                continue;
            }
            final String value = (String) propPair.getValue();
            setConfigParam(updateProps, name, value,
            		       true /* validateParams */);
        }
    }

    /**
     * @hidden
     * For access by ReplicationConfig and testing only
     */
    Properties getProps() {
        Properties returnProps = new Properties(props);
        returnProps.setProperty(CHANNEL_TYPE, getChannelType());
        return returnProps;
    }

    /**
     * Set this configuration parameter with this value in the specified
     * Properties object, which is assumed to represent the properties
     * that are applicable to this class. Values are validated
     * before setting the parameter.
     *
     * @param props the Properties object to update
     * @param paramName the configuration parameter name, one of the String
     * constants in this class
     * @param value the configuration value.
     *
     * @throws IllegalArgumentException if the paramName or value is invalid
     */
    private static void setConfigParam(
        Properties props, String paramName, String value,
        boolean validateParams)
        throws IllegalArgumentException {

        DbConfigManager.setConfigParam(props,
                                       paramName,
                                       value,
                                       false,   /* require mutability. */
                                       validateParams,
                                       true,   /* forReplication */
                                       false);  /* verifyForReplication */
    }

    /**
     * Checks whether the named parameter is valid for this configuration type.
     * @param paramName the configuration parameter name, one of the String
     * constants in this class
     * @return true if the named parameter is a valid parameter name
     */
    protected boolean isValidConfigParam(String paramName) {
        return repNetLocalProperties.contains(paramName);
    }

    /**
     * Read a properties file into a Properties object.
     * @param propFile a file containing property settings
     * @return a Properties object containing the property settings
     * @throws FileNotFoundException if the file does not exist
     */
    private static Properties readProperties(File propFile)
        throws FileNotFoundException {

        if (!propFile.exists()) {
            throw new FileNotFoundException(
                "The properties file " + propFile + " does not exist.");
        }

        Properties props = new Properties();
        RepUtils.populateNetProps(props, propFile);
        return props;
    }
}
