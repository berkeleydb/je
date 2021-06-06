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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Standalone environments can be converted once to be replicated environments.
 * Replicated environments can't be opened in standalone mode.
 */
public class ConversionTest extends TestBase {

    private final File envRoot;

    public ConversionTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Check that an environment which is opened for replication cannot be
     * re-opened as a standalone environment in r/w mode
     */
    @Test
    public void testNoStandaloneReopen()
        throws DatabaseException, IOException {

        RepEnvInfo[] repEnvInfo = initialOpenWithReplication();

        /* Try to re-open standalone r/w, should fail. */
        try {
            EnvironmentConfig reopenConfig = new EnvironmentConfig();
            reopenConfig.setTransactional(true);
            @SuppressWarnings("unused")
            Environment unused = new Environment(repEnvInfo[0].getEnvHome(),
                                                 reopenConfig);
            fail("Should have thrown an exception.");
        } catch (UnsupportedOperationException ignore) {
            /* throw a more specific exception? */
        }
    }

    /**
     * Check that an environment which is opened for replication can
     * also be opened as a standalone r/o environment.
     */
    @Test
    public void testStandaloneRO()
        throws DatabaseException, IOException {

        RepEnvInfo[] repEnvInfo = initialOpenWithReplication();

        /* Try to re-open standalone r/o, should succeed */
        try {
            EnvironmentConfig reopenConfig = new EnvironmentConfig();
            reopenConfig.setTransactional(true);
            reopenConfig.setReadOnly(true);
            Environment env = new Environment(repEnvInfo[0].getEnvHome(),
                                              reopenConfig);
            env.close();
        } catch (DatabaseException e) {
            fail("Should be successful" + e);
        }
    }

    @Test
    public void testStandaloneUtility()
        throws DatabaseException, IOException {

        RepEnvInfo[] repEnvInfo = initialOpenWithReplication();

        /* Try to re-open as a read/only utility, should succeed */
        try {
            EnvironmentConfig reopenConfig = new EnvironmentConfig();
            reopenConfig.setTransactional(true);
            reopenConfig.setReadOnly(true);
            EnvironmentImpl envImpl =
                CmdUtil.makeUtilityEnvironment(repEnvInfo[0].getEnvHome(),
                                           true /* readOnly */);
            envImpl.close();
        } catch (DatabaseException e) {
            fail("Should be successful" + e);
        }
    }

    private RepEnvInfo[] initialOpenWithReplication()
        throws DatabaseException, IOException {
     
        RepEnvInfo[] repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 2);
        RepTestUtils.joinGroup(repEnvInfo);
        for (RepEnvInfo repi : repEnvInfo) {
            repi.getEnv().close();
        }
        return repEnvInfo;
    }
}
