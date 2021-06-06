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

package com.sleepycat.je.rep.arbiter;

import java.util.Properties;
import java.util.logging.Level;

import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.impl.RepParams;

/**
 * The mutable configuration parameters for an {@link Arbiter}.
 *
 * @see Arbiter#setArbiterMutableConfig(ArbiterMutableConfig)
 */
public class ArbiterMutableConfig implements Cloneable {

    Properties props;

    boolean validateParams = true;

    ArbiterMutableConfig() {
       props = new Properties();
    }

    ArbiterMutableConfig(Properties properties) {
        props = (Properties)properties.clone();
    }

    /**
     * Identify one or more helpers nodes by their host and port pairs in this
     * format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     *
     * @param helperHosts the string representing the host and port pairs.
     */
    public ArbiterMutableConfig setHelperHosts(String helperHosts) {
        DbConfigManager.setVal(
            props, RepParams.HELPER_HOSTS, helperHosts, validateParams);
        return this;
    }

    /**
     * Returns the string identifying one or more helper host and port pairs in
     * this format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     *
     * @return the string representing the host port pairs.
     */
    public String getHelperHosts() {
        return DbConfigManager.getVal(props, RepParams.HELPER_HOSTS);
    }

    /**
     * Trace messages equal and above this level will be logged to the je.info
     * file, which is in the Arbiter home directory.  Value should
     * be one of the predefined java.util.logging.Level values.
     * <p>
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>com.sleepycat.je.util.FileHandler.level</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"INFO"</td>
     * </tr>
     * </table></p>
     * @see <a href="{@docRoot}/../GettingStartedGuide/managelogging.html"
     * target="_top">Chapter 12. Logging</a>
     *
     * @param val value of the logging level.
     * @return ArbiterConfig.
     */
    public ArbiterMutableConfig setFileLoggingLevel(String val) {
        Level.parse(val);
        DbConfigManager.setVal(
            props, EnvironmentParams.JE_FILE_LEVEL, val, false);
        return this;
    }

    /**
     * Gets the file logging level.
     * @return logging level
     */
    public String getFileLoggingLevel() {
        return DbConfigManager.getVal(props, EnvironmentParams.JE_FILE_LEVEL);
    }

    /**
     * Trace messages equal and above this level will be logged to the
     * console. Value should be one of the predefined
     * java.util.logging.Level values.
     *
     * <p><table border="1">
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>com.sleepycat.je.util.ConsoleHandler.level</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"OFF"</td>
     * </tr>
     * </table></p>
     * @see <a href="{@docRoot}/../GettingStartedGuide/managelogging.html"
     * target="_top">Chapter 12. Logging</a>
     *
     * @param val Logging level.
     * @return this.
     */
    public ArbiterMutableConfig setConsoleLoggingLevel(String val) {
        Level.parse(val);
        DbConfigManager.setVal(
            props, EnvironmentParams.JE_CONSOLE_LEVEL, val, false);
        return this;
    }

    /**
     * Gets the console logging level.
     * @return logging level
     */
    public String getConsoleLoggingLevel() {
        return DbConfigManager.getVal(props, EnvironmentParams.JE_CONSOLE_LEVEL);
    }

    /**
     * @hidden
     * Set this configuration parameter. First validate the value specified for
     * the configuration parameter; if it is valid, the value is set in the
     * configuration. Hidden could be used to set parameters internally.
     *
     * @param paramName the configuration parameter name, one of the String
     * constants in this class
     *
     * @param value The configuration value
     *
     * @return this
     *
     * @throws IllegalArgumentException if the paramName or value is invalid.
     */
    public ArbiterMutableConfig setConfigParam(String paramName,
                                                   String value)
        throws IllegalArgumentException {

        boolean forReplication = false;
        ConfigParam param =
            EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
        if (param != null) {
            forReplication = param.isForReplication();
        }

        DbConfigManager.setConfigParam(props,
                                       paramName,
                                       value,
                                       true, /* require mutability. */
                                       true,
                                       forReplication, /* forReplication */
                                       true  /* verifyForReplication */);
        return this;
    }

    /**
     * @hidden
     * Returns the value for this configuration parameter.
     *
     * @param paramName a valid configuration parameter, one of the String
     * constants in this class.
     * @return the configuration value.
     * @throws IllegalArgumentException if the paramName is invalid.
     */
    public String getConfigParam(String paramName)
        throws IllegalArgumentException {

       return DbConfigManager.getConfigParam(props, paramName);
    }

    protected ArbiterMutableConfig copy() {
        return new ArbiterMutableConfig(props);
    }

    /**
     * @hidden
     * For internal use only.
     */
    public boolean isConfigParamSet(String paramName) {
        return props.containsKey(paramName);
    }

    public ArbiterMutableConfig clone() {
        try {
            ArbiterMutableConfig copy =
                (ArbiterMutableConfig) super.clone();
            copy.props = (Properties) props.clone();
            return copy;
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    public Properties getProps() {
        return (Properties) props.clone();
    }

    /**
     * Display configuration values.
     */
    @Override
    public String toString() {
        return (props.toString() + "\n");
    }

}
