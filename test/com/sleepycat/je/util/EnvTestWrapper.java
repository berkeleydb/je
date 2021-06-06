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

package com.sleepycat.je.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * This wrapper encapsulates environment operations used while running unit
 * tests. The encapsulation permits creation of standalone or replicated
 * environments as needed behind the wrapper, so that the test does not have
 * to deal with the mechanics of environment management in each case and can
 * focus on just the test logic itself.
 *
 *  It provides the following API needed to:
 *
 * 1) create new environments
 *
 * 2) Close open environments.
 *
 * 3) Clear out the test directory used to create environments for a test
 *
 */
public abstract class EnvTestWrapper  {

    /**
     * Creates the environment to be used by the test case. If the environment
     * already exists on disk, it's reused. If not, it creates a new
     * environment and returns it.
     */
    public abstract Environment create(File envHome,
                                       EnvironmentConfig envConfig)
        throws DatabaseException;

    /**
     * Closes the environment.
     *
     * @param environment the environment to be closed.
     *
     * @throws DatabaseException
     */
    public abstract void close(Environment environment)
        throws DatabaseException;

    public abstract void closeNoCheckpoint(Environment environment)
        throws DatabaseException;

    public abstract void abnormalClose(Environment env);

    public abstract void resetNodeEqualityCheck();

    /**
     * Destroys the contents of the test directory used to hold the test
     * environments.
     *
     * @throws Exception
     */
    public abstract void destroy()
        throws Exception;

    /**
     * A wrapper for local tests.
     */
    public static class LocalEnvWrapper extends EnvTestWrapper {
        private File envDir;
        private final Map<File, Environment> dirEnvMap =
            new HashMap<File, Environment>();

        @Override
        public Environment create(File envHome,
                                  EnvironmentConfig envConfig)
            throws DatabaseException {

            this.envDir = envHome;
            Environment env = new Environment(envHome, envConfig);
            dirEnvMap.put(envHome, env);
            return env;
        }

        @Override
        public void close(Environment env)
            throws DatabaseException {
            
            env.close();
        }

        @Override
        public void resetNodeEqualityCheck() {
            throw new UnsupportedOperationException
                ("This opertaion is not supported by base environment.");
        }

        /* Provide the utility for closing without a checkpoint. */
        @Override
        public void closeNoCheckpoint(Environment env) 
            throws DatabaseException {

            DbInternal.getNonNullEnvImpl(env).close(false);
        }

        @Override
        public void abnormalClose(Environment env) {
            DbInternal.getNonNullEnvImpl(env).abnormalClose();
        }

        @Override
        public void destroy() {
            if (dirEnvMap == null) {
                return;
            }
            for (Environment env : dirEnvMap.values()) {
                try {
                    /* Close in case we hit an exception and didn't close */
                    env.close();
                } catch (RuntimeException e) {
                    /* OK if already closed */
                }
            }
            dirEnvMap.clear();
            if (envDir != null) {
                TestUtils.removeLogFiles("TearDown", envDir, false);
            }
            envDir = null;
        }
    }
}
