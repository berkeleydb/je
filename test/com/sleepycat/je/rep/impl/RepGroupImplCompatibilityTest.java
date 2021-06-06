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

package com.sleepycat.je.rep.impl;

import static com.sleepycat.je.rep.impl.RepParams.TEST_JE_VERSION;
import static org.junit.Assert.fail;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

import org.junit.Test;

/**
 * Test compatibility checks for RepGroupImpl versions.
 */
public class RepGroupImplCompatibilityTest extends RepTestBase {

    /**
     * Test using a master supporting a stream protocol 4, too old to support
     * RepGroupImpl version 3.
     */
    @Test
    public void testRepGroupImplCompatibilityOldMaster()
        throws DatabaseException {

        /* Create master with stream protocol version 4 */
        setJEVersion(Protocol.VERSION_4_JE_VERSION, repEnvInfo[0]);
        createGroup(1);

        /* Replica with RepGroupImpl minimum version is OK. */
        setJEVersion(RepGroupImpl.MIN_FORMAT_VERSION_JE_VERSION, repEnvInfo[1]);
        repEnvInfo[1].openEnv();

        /* Replica with RepGroupImpl version 3 is not. */
        setJEVersion(RepGroupImpl.FORMAT_VERSION_3_JE_VERSION, repEnvInfo[2]);
        try {
            repEnvInfo[2].openEnv();
            fail("Expected EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Replica version too new: " + e);
        }
    }

    /**
     * Test using a master requiring stream protocol 5, too new to support
     * RepGroupImpl version 2.
     */
    @Test
    public void testRepGroupImplCompatibilityNewMaster()
        throws DatabaseException {

        /* Prevent setting of minJEVersion for this test. */
        for (RepEnvInfo info : repEnvInfo) {
            info.getRepConfig().setConfigParam(
                RepParams.TEST_CBVLSN.getName(), "true");
        }

        /* Create master with RepGroupImpl version 3 */
        createGroup(1);

        /* Replica with stream protocol version 5 is OK */
        setJEVersion(Protocol.VERSION_5_JE_VERSION, repEnvInfo[1]);
        repEnvInfo[1].openEnv();

        /* Replica with stream protocol version 4 is not */
        setJEVersion(Protocol.VERSION_4_JE_VERSION, repEnvInfo[2]);
        try {
            repEnvInfo[2].openEnv();
            fail("Expected EnvironmentFailureException");
        } catch (EnvironmentFailureException e) {
            logger.info("Replica version too old: " + e);
        }
    }

    /** Set the JE version for the specified nodes. */
    private void setJEVersion(final JEVersion jeVersion,
                              final RepEnvInfo... nodes) {
        assert jeVersion != null;
        for (final RepEnvInfo node : nodes) {
            node.getRepConfig().setConfigParam(
                TEST_JE_VERSION.getName(), jeVersion.toString());
        }
    }
}
