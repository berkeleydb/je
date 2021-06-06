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

import java.io.File;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.arbiter.impl.ArbiterImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * Provides a mechanism to allow write availability for the Replication
 * group even when the number of replication nodes is less than majority.
 * The main use of an Arbiter is when the replication group consists of
 * two nodes. The addition of an Arbiter to the replication group
 * allows for one node to fail and provide write availability with ACK
 * durability of SIMPLE_MAJORITY. The Arbiter acknowledges the transaction,
 * but does not retain a copy of the data. The Arbiter persists a
 * small amount of state to insure that only the Replication nodes that
 * contain the Arbiter acknowledged transactions may become a Master.
 * <p>
 * The Arbiter node participates in elections and may acknowledge transaction
 * commits.
 * <p>
 * The Arbiter state is as follows:
 * UNKNOWN [ UNKNOWN | REPLICA]+ DETACHED
 */
public class Arbiter {

    private ArbiterImpl ai;
    private final ReplicatedEnvironment repEnv;
    private final ArbiterConfig ac;

    private final String ARB_CONFIG = "ArbiterConfig";
    private final String ARB_HOME = "ArbiterHome";

    /**
     * An Arbiter used in elections and transaction acknowledgments.
     * This method returns when a connection to the current master
     * replication node is made. The Arbiter.shutdown() method is
     * used to shutdown the threads that run as part of the Arbiter.
     *
     * @param arbiterConfig Configuration parameters for the Arbiter.
     *
     * @throws EnvironmentNotFoundException if the environment does not exist
     *
     * @throws EnvironmentLockedException when an environment cannot be opened
     * because another Arbiter has the environment open.
     *
     * @throws DatabaseException problem establishing connection to the master.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, an invalid {@code ArbiterConfig} parameter.
     */
    public Arbiter(ArbiterConfig arbiterConfig)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               DatabaseException,
               IllegalArgumentException {

        ac = arbiterConfig.clone();
        verifyParameters(ac);
        File envHome = new File(ac.getArbiterHome());
        if (!envHome.exists()) {
            throw new IllegalArgumentException(
                "The specified environment directory " +
                envHome.getAbsolutePath() +
                " does not exist.");
        }
        Properties allProps = ac.getProps();
        EnvironmentConfig envConfig =
            new EnvironmentConfig(getEnvProps(allProps));
        envConfig.setReadOnly(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam(
            EnvironmentParams.ENV_RECOVERY.getName(), "false");
        envConfig.setConfigParam(
            EnvironmentParams.ENV_SETUP_LOGGER.getName(), "true");
        envConfig.setConfigParam(
            EnvironmentParams.LOG_USE_WRITE_QUEUE.getName(), "false");
        envConfig.setConfigParam(
            EnvironmentParams.LOG_WRITE_QUEUE_SIZE.getName(), "4096");
        if (ac.getLoggingHandler() != null) {
            envConfig.setLoggingHandler(ac.getLoggingHandler());
        }

        ReplicationConfig repConfig =
            new ReplicationConfig(getRepEnvProps(allProps));
        repConfig.setConfigParam(RepParams.ARBITER_USE.getName(), "true");
        repConfig.setRepNetConfig(ac.getRepNetConfig());

        repEnv = RepInternal.createInternalEnvHandle(envHome,
                                                 repConfig,
                                                 envConfig);
        try {
            ai = new ArbiterImpl(
                envHome, RepInternal.getNonNullRepImpl(repEnv));
            ai.runArbiter();
        } catch (Throwable t) {
            shutdown();
            throw t;
        }
    }

    /**
     * Returns the Arbiter mutable attributes.
     *
     * @return Arbiter attributes.
     */
    public ArbiterMutableConfig getArbiterMutableConfig() {
        return ac.getArbiterMutableConfig();
    }

    /**
     * Sets the Arbiter mutable attributes.
     *
     * @param config Arbiter attributes.
     * @throws DatabaseException
     */
    public void setArbiterMutableConfig(ArbiterMutableConfig config)
        throws DatabaseException {
        ReplicationMutableConfig rmc = repEnv.getRepMutableConfig();
        Properties newProps = config.getProps();
        copyMutablePropsTo(newProps, rmc);
        repEnv.setRepMutableConfig(rmc);
        ai.refreshHelperHosts();

        EnvironmentMutableConfig emc = repEnv.getMutableConfig();
        copyMutablePropsTo(newProps, emc);
        repEnv.setMutableConfig(emc);
    }

    /**
     * Gets the Arbiter state.
     */
    public ReplicatedEnvironment.State getState() {
        return ai.getArbState();
    }
    /**
     * Gets the Arbiter statistics.
     *
     * @param config The general statistics attributes.  If null, default
     * attributes are used.
     *
     * @return Arbiter statistics.
     * @throws DatabaseException
     */
    public ArbiterStats getStats(StatsConfig config)
        throws DatabaseException {
        if (ai == null) {
            return null;
        }

        StatsConfig useConfig =
                (config == null) ? StatsConfig.DEFAULT : config;

        return new ArbiterStats(ai.loadStats(useConfig));
    }

    /**
     * Shutdown the Arbiter.
     * Threads are stopped and resources are released.
     * @throws DatabaseException
     */
    public void shutdown()
        throws DatabaseException {
        if (ai != null) {
            ai.shutdown();
            try {
                ai.join();
            } catch (InterruptedException ignore) {

            }
        }
        if (repEnv != null) {
            repEnv.close();
        }
    }

    private void verifyParameters(ArbiterConfig ac)
        throws IllegalArgumentException {
        DatabaseUtil.checkForNullParam(ac, ARB_CONFIG);
        DatabaseUtil.checkForNullParam(ac.getArbiterHome(), ARB_HOME);
        DatabaseUtil.checkForNullParam(ac.getGroupName(), ReplicationConfig.GROUP_NAME);
        DatabaseUtil.checkForNullParam(ac.getNodeHostPort(), ReplicationConfig.NODE_HOST_PORT);
        DatabaseUtil.checkForNullParam(ac.getHelperHosts(), ReplicationMutableConfig.HELPER_HOSTS);
    }

    private Properties getEnvProps(Properties props) {
        Properties envProps = new Properties();
        Iterator<Entry<Object, Object>> iter = props.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Object, Object> m = iter.next();
            String key = (String)m.getKey();
            if (!key.startsWith(EnvironmentParams.REP_PARAM_PREFIX)) {
                envProps.put(key, m.getValue());
            }
        }
        return envProps;
    }

    private Properties getRepEnvProps(Properties props) {
        Properties repEnvProps = new Properties();
        Iterator<Entry<Object, Object>> iter = props.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Object, Object> m = iter.next();
            String key = (String)m.getKey();
            if (key.startsWith(EnvironmentParams.REP_PARAM_PREFIX)) {
                repEnvProps.put(key, m.getValue());
            }
        }
        return repEnvProps;
    }

    private void copyMutablePropsTo(Properties from,
                                    ReplicationMutableConfig toConfig) {

        Enumeration<?> propNames = from.propertyNames();
        while (propNames.hasMoreElements()) {
            String paramName = (String) propNames.nextElement();
            ConfigParam param =
                EnvironmentParams.SUPPORTED_PARAMS.get(paramName);

            if (param != null && param.isForReplication() &&
                param.isMutable()) {
                toConfig.setConfigParam(paramName, from.getProperty(paramName));
            }
        }
    }

    private void copyMutablePropsTo(Properties from,
                                    EnvironmentMutableConfig toConfig) {

        Enumeration<?> propNames = from.propertyNames();
        while (propNames.hasMoreElements()) {
            String paramName = (String) propNames.nextElement();
            ConfigParam param =
                EnvironmentParams.SUPPORTED_PARAMS.get(paramName);

            if (param != null && !param.isForReplication() &&
                param.isMutable()) {
                toConfig.setConfigParam(paramName, from.getProperty(paramName));
            }
        }
    }

}
