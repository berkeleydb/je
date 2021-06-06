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
import java.util.Properties;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;

/**
 * @hidden
 * For internal use only. It serves to shelter methods that must be public to
 * be used by other BDBJE packages but that are not part of the public api
 * available to applications.
 */
public class RepInternal {

    /**
     * Proxy to ReplicatedEnvironment.getMaybeNullRepImpl.
     *
     * This method does not check whether the env is valid.
     *
     * WARNING: This method will be phased out over time and normally
     * getNonNullRepImpl should be called instead.
     *
     * @return the non-null underlying RepImpl, or null if the env has been
     * closed.
     */
    public static RepImpl getRepImpl(ReplicatedEnvironment rep) {
        return rep.getMaybeNullRepImpl();
    }

    /**
     * Proxy to ReplicatedEnvironment.getNonNullEnvImpl
     *
     * This method is called to access the underlying RepImpl when an env is
     * expected to be open, to guard against NPE when the env has been closed.
     *
     * This method does not check whether the env is valid.
     *
     * @return the non-null, underlying RepImpl.
     *
     * @throws IllegalStateException if the env has been closed.
     */
    public static RepImpl getNonNullRepImpl(ReplicatedEnvironment rep) {
        return rep.getNonNullRepImpl();
    }

    public static RepGroupImpl getRepGroupImpl(ReplicationGroup group) {
        return group.getRepGroupImpl();
    }

    public static ReplicationConfig
        makeReplicationConfig(Properties props, boolean validateParams)
        throws IllegalArgumentException {

        return new ReplicationConfig(props, validateParams);
    }

    public static int getNodeId(ReplicatedEnvironment rep) {
        return getNonNullRepImpl(rep).getNodeId();
    }

    /*
     * Create an environment handle but do not join the group as part of the
     * creation of this handle. This operation is only really meaningful in
     * the absence of existing handles that had already been used to join the
     * group.
     */
    public static ReplicatedEnvironment
        createDetachedEnv(File envHome,
                          ReplicationConfig repConfig,
                          EnvironmentConfig envConfig)
        throws DatabaseException {

        return new ReplicatedEnvironment(envHome, repConfig, envConfig,
                                         null,
                                         QuorumPolicy.SIMPLE_MAJORITY,
                                         false,
                                         null);
    }

    /*
     * Create an environment handle but do not join the group as part of the
     * creation of this handle.
     */
    public static ReplicatedEnvironment
        createInternalEnvHandle(File envHome,
                                ReplicationConfig repConfig,
                                EnvironmentConfig envConfig)
        throws DatabaseException {

        return new ReplicatedEnvironment(envHome, repConfig, envConfig,
                                         null,
                                         null,
                                         false,
                                         null);
    }

    /**
     * Proxy to ReplicationMutableConfig.validateParams.
     */
    public static void disableParameterValidation
        (ReplicationMutableConfig config) {
        config.setOverrideValidateParams(false);
    }

    public static
        ReplicatedEnvironmentStats makeReplicatedEnvironmentStats
        (RepImpl repImpl, StatsConfig config) {

        return new ReplicatedEnvironmentStats(repImpl, config);
    }


    public static void setAllowConvert(final ReplicationConfig repConfig,
                                       final boolean allowConvert) {
        repConfig.setAllowConvert(allowConvert);
    }

    public static boolean getAllowConvert(final ReplicationConfig repConfig) {
        return repConfig.getAllowConvert();
    }
}
