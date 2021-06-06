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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.InstanceContext;
import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.LoggerFactory;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TracerFormatter;

/**
 * Class for creating DataChannel instances.
 */
public class DataChannelFactoryBuilder {

    /**
     * A count of the number of factories for which construction was attempted.
     */
    private static final AtomicInteger factoryCount = new AtomicInteger(0);

    /**
     * Construct the "default" DataChannelFactory that arises from an empty
     * DataChannelFactory configuration.
     */
    public static DataChannelFactory constructDefault() {
        return new SimpleChannelFactory();
    }

    /**
     * Construct a DataChannelFactory from the specified network
     * configuration.
     * The choice of DataChannelFactory type is determined by the setting
     * of {@link ReplicationNetworkConfig#CHANNEL_TYPE je.rep.channelType}.
     *
     * If set to <code>ssl</code>then the internal SSL implementation is
     * is used.  If set to <code>custom</code> then a custom channel
     * factory is constructed based on the setting of
     *   {@link ReplicationNetworkConfig#CHANNEL_FACTORY_CLASS je.rep.dataChannelFactoryClass}
     *
     * If set to <code>basic</code> or not set, SimpleChannelFactory
     * is instantiated.
     *
     * @param repNetConfig The configuration to control factory building
     * @return a DataChannelFactory
     * @throws IllegalArgumentException if an invalid configuration
     * property value or combination of values was specified.
     */
    public static DataChannelFactory construct(
            ReplicationNetworkConfig repNetConfig)
        throws IllegalArgumentException {

        return construct(repNetConfig, (String) null);
    }

    /**
     * Construct a DataChannelFactory from the specified access
     * configuration.
     * The choice of DataChannelFactory type is determined by the setting
     * of {@link ReplicationNetworkConfig#CHANNEL_TYPE je.rep.channelType}.
     *
     * If set to <code>ssl</code>then the internal SSL implementation is
     * is used.  If set to <code>custom</code> then a custom channel
     * factory is constructed based on the setting of
     *   {@link ReplicationNetworkConfig#CHANNEL_FACTORY_CLASS je.rep.dataChannelFactoryClass}
     *
     * If set to <code>basic</code> or not set, SimpleChannelFactory
     * is instantiated.
     *
     * @param repNetConfig The configuration to control factory building
     * @param logContext A null-allowable String that contributes to the
     * logging identifier for the factory.
     * @return a DataChannelFactory
     * @throws IllegalArgumentException if an invalid configuration
     * property value or combination of values was specified.
     */
    public static DataChannelFactory construct(
            ReplicationNetworkConfig repNetConfig, String logContext)
        throws IllegalArgumentException {

        final String logName = repNetConfig.getLogName();
        if (logName.isEmpty() && (logContext == null || logContext.isEmpty())) {
            return construct(repNetConfig, (LoggerFactory) null);
        }

        final String logId;
        if (logName.isEmpty()) {
            logId = logContext;
        } else if (logContext == null || logContext.isEmpty()) {
            logId = logName;
        } else {
            logId = logName + ":" + logContext;
        }
        final LoggerFactory loggerFactory = makeLoggerFactory(logId);

        return construct(repNetConfig, loggerFactory);
    }

    /**
     * Construct a DataChannelFactory from the specified access
     * configuration.
     * The choice of DataChannelFactory type is determined by the setting
     * of {@link ReplicationNetworkConfig#CHANNEL_TYPE je.rep.channelType}.
     *
     * If set to <code>ssl</code>then the internal SSL implementation is
     * is used.  If set to <code>custom</code> then a custom channel
     * factory is constructed based on the setting of
     *   {@link ReplicationNetworkConfig#CHANNEL_FACTORY_CLASS je.rep.dataChannelFactoryClass}
     *
     * If set to <code>basic</code> or not set, SimpleChannelFactory
     * is instantiated.
     *
     * @param repNetConfig The configuration to control factory building
     * @param loggerFactory A null-allowable LoggerFactory for use in channel
     * factory construction
     * @return a DataChannelFactory
     * @throws IllegalArgumentException if an invalid configuration
     * property value or combination of values was specified.
     */
    public static DataChannelFactory construct(
            ReplicationNetworkConfig repNetConfig,
            LoggerFactory loggerFactory)
        throws IllegalArgumentException {

        final String channelType = repNetConfig.getChannelType();
        final int factoryIndex = factoryCount.getAndIncrement();

        /*
         * Build the LoggerFactory if not provided by the caller
         */
        if (loggerFactory == null) {
            String logName = repNetConfig.getLogName();
            if (logName.isEmpty()) {
                logName = Integer.toString(factoryIndex);
            }
            loggerFactory = makeLoggerFactory(logName);
        }

        final InstanceContext context =
            new InstanceContext(repNetConfig, loggerFactory);

        final String factoryClass = repNetConfig.getChannelFactoryClass();
        if (factoryClass == null || factoryClass.isEmpty()) {
            if (channelType.equalsIgnoreCase("basic")) {
                return new SimpleChannelFactory(
                    new InstanceParams(context, null));
            }

            if (channelType.equalsIgnoreCase("ssl")) {
                return new SSLChannelFactory(new InstanceParams(context, null));
            }

            throw new IllegalArgumentException(
                "The channelType setting '" + channelType + "' is not valid");
        }

        final String classParams = repNetConfig.getChannelFactoryParams();
        final InstanceParams factoryParams =
            new InstanceParams(context, classParams);
        return construct(factoryClass, factoryParams);
    }

    /**
     * Constructs a DataChannelFactory implementation.
     * @param factoryClassName the name of the class to instantiate,
     * which must implement DataChannelFactory
     * @param factoryParams the context and factory arguments
     * @return a newly constructed instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    private static DataChannelFactory construct(
        String factoryClassName, InstanceParams factoryParams)
        throws IllegalArgumentException {

        return (DataChannelFactory) constructObject(
            factoryClassName, DataChannelFactory.class,
            "data channel factory",
            new CtorArgSpec(new Class<?>[] { InstanceParams.class },
                            new Object[] { factoryParams }));
    }

    /**
     * Instantiates a class based on a configuration specification. This method
     * looks up a class of the specified name, then finds a constructor with
     * an argument list that matches the caller's specification, and constructs
     * an instance using that constructor and validates that the instance
     * extends or implements the mustImplement class specified.
     *
     * @param instClassName the name of the class to instantiate
     * @param mustImplement a class denoting a required base class or
     * required implemented interface of the class whose name is
     * specified by instClassName.
     * @param miDesc a descriptive term for the mustImplement class
     * @param ctorArgSpec specifies the required constructor signature and
     * the values to be passed
     * @return an instance of the specified class
     * @throws IllegalArgumentException if any of the input arguments are
     * invalid
     */
    static Object constructObject(String instClassName,
                                  Class<?> mustImplement,
                                  String miDesc,
                                  CtorArgSpec ctorArgSpec)
        throws IllegalArgumentException {

        /*
         * Resolve the class
         */
        Class<?> instClass = null;
        try {
            instClass = Class.forName(instClassName);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException(
                "Error resolving " + miDesc + " class " +
                instClassName, cnfe);
        }

        /*
         * Find an appropriate constructor for the class.
         */
        final Constructor<?> constructor;
        try {
            constructor = instClass.getConstructor(ctorArgSpec.argTypes);
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException(
                "Unable to find an appropriate constructor for " + miDesc +
                " class " + instClassName);
        }

        /*
         * Get an instance of the class.
         */
        final Object instObject;
        try {
            instObject = constructor.newInstance(ctorArgSpec.argValues);
        } catch (IllegalAccessException iae) {
            /* Constructor is not accessible */
            throw new IllegalArgumentException(
                "Error instantiating " + miDesc + " class " + instClassName +
                ".  Not accessible?",
                iae);
        } catch (IllegalArgumentException iae) {
            /* Wrong arguments - should not be possible here */
            throw new IllegalArgumentException(
                "Error instantiating " + miDesc + " class " + instClassName,
                iae);
        } catch (InstantiationException ie) {
            /* Class is abstract */
            throw new IllegalArgumentException(
                "Error instantiating " + miDesc + " class " + instClassName +
                ". Class is abstract?",
                ie);
        } catch (InvocationTargetException ite) {
            /* Exception thrown within constructor */
            throw new IllegalArgumentException(
                "Error instantiating " + miDesc + " class " + instClassName +
                ". Exception within constructor",
                ite);
        }

        /*
         * In this context, the class must implement the specified
         * interface.
         */
        if (! (mustImplement.isAssignableFrom(instObject.getClass()))) {
            throw new IllegalArgumentException(
                "The " + miDesc + " class " +  instClassName +
                " does not implement " + mustImplement.getName());
        }

        return instObject;
    }

    /**
     * Creates a logger factory based on an EnvironmentImpl
     *
     * @param envImpl a non-null EnvironmentImpl
     */
    public static LoggerFactory makeLoggerFactory(EnvironmentImpl envImpl) {
        if (envImpl == null) {
            throw new IllegalArgumentException("envImpl must not be null");
        }

        return new ChannelLoggerFactory(envImpl, null /* formatter */);
    }

    /**
     * Creates a logger factory based on a fixed string
     *
     * @param prefix a fixed string to be used as logger prefix
     */
    public static LoggerFactory makeLoggerFactory(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("prefix must not be null");
        }

        final Formatter formatter = new ChannelFormatter(prefix);

        return new ChannelLoggerFactory(null, /* envImpl */ formatter);
    }


    /**
     * A simple class that captures the proposed formal and actual argument
     * lists to match against possible constructors.
     */
    static class CtorArgSpec {
        private final Class<?>[] argTypes;
        private final Object[] argValues;

        CtorArgSpec(Class<?>[] argTypes, Object[] argValues) {
            this.argTypes = argTypes;
            this.argValues = argValues;
        }
    }

    /**
     * A simple implementation of LoggerFactory that encapsulates the 
     * necessary information to do JE environment-friendly logging without
     * needing to know JE HA internal logging.
     */
    static class ChannelLoggerFactory implements LoggerFactory {
        private final EnvironmentImpl envImpl;
        private final Formatter formatter;

        /**
         * Creates a LoggerFactory for use in construction of channel
         * objects. The caller should supply either an EnvironmentImpl or a
         * Formatter object.
         * 
         * @param envImpl a possibly-null EnvironmentImpl
         * @param formatter a possible null formatter
         */
        ChannelLoggerFactory(EnvironmentImpl envImpl,
                             Formatter formatter) {
            this.envImpl = envImpl;
            this.formatter = formatter;
        }

        /**
         * @see LoggerFactory#getLogger(Class)
         */
        @Override
        public InstanceLogger getLogger(Class<?> clazz) {
            final Logger logger;
            if (envImpl == null) {
                logger = LoggerUtils.getLoggerFormatterNeeded(clazz);
            } else {
                logger = LoggerUtils.getLogger(clazz);
            }
            return new ChannelInstanceLogger(envImpl, formatter, logger);
        }
    }

    /**
     * A simple implementation of InstanceLogger that encapuslates the 
     * necessary information to do JE environment-friendly logging without
     * needing to know JE logging rules.
     */
    static class ChannelInstanceLogger implements InstanceLogger {
        private final EnvironmentImpl envImpl;
        private final Formatter formatter;
        private final Logger logger;

        /**
         * Creates a ChannelInstanceLogger for use in construction of channel
         * objects. The caller should supply either an EnvironmentImpl or a
         * Formatter object.
         * 
         * @param envImpl a possibly-null EnvironmentImpl
         * @param formatter a possible null formatter
         * @param logger a logger created via LoggerUtils.getLogger()
         */
        ChannelInstanceLogger(EnvironmentImpl envImpl,
                              Formatter formatter,
                              Logger logger) {
            this.envImpl = envImpl;
            this.formatter = formatter;
            this.logger = logger;
        }

        /**
         * @see InstanceLogger#log(Level, String)
         */
        @Override
        public void log(Level logLevel, String msg) {
            LoggerUtils.logMsg(logger, envImpl, formatter, logLevel, msg);
        }
    }

    /**
     * Formatter for log messages
     */
    static class ChannelFormatter extends TracerFormatter {
        private final String id;

        ChannelFormatter(String id) {
            super();
            this.id = id;
        }

        @Override
        protected void appendEnvironmentName(StringBuilder sb) {
            sb.append(" [" + id + "]");
        }
    }
}
