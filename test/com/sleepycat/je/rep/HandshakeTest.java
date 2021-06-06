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
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.FeederReplicaHandshake;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshake;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class HandshakeTest extends TestBase {

    private final File envRoot;
    private final int groupSize = 4;

    private ReplicatedEnvironment master = null;
    private RepNode masterNode = null;

    RepEnvInfo[] repEnvInfo = null;
    RepEnvInfo replicaEnvInfo = null;

    public HandshakeTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, groupSize);

        /*
         * Disable stat collection to avoid NPE in testDup.  It constructs a
         * Feeder() with a null outputThread field, and this causes an NPE when
         * getProtocolStats is called from the stat collector, which depends on
         * timing.  Outside of this particular test the outputThread field is
         * non-null and final, so checking for null in Feeder is undesirable.
         */
        repEnvInfo[0].getEnvConfig().setConfigParam(
            EnvironmentConfig.STATS_COLLECT, "false");

        master = repEnvInfo[0].openEnv();
        replicaEnvInfo = repEnvInfo[1];
        State state = master.getState();
        masterNode = RepInternal.getNonNullRepImpl(master).getRepNode();
        assertEquals(ReplicatedEnvironment.State.MASTER, state);
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        RepTestUtils.shutdownRepEnvs(repEnvInfo);
    }

    /**
     * Test error handling on a version mismatch
     */
    @Test
    public void testProtocolVersionMismatch()
        throws Throwable {

        /* Hack the version number for the test */
        try {
            ReplicaFeederHandshake.setTestProtocolVersion(Integer.MIN_VALUE);
            checkForException
                (EnvironmentFailureReason.PROTOCOL_VERSION_MISMATCH);
        } finally {
            /* Restore the default version */
            ReplicaFeederHandshake.setTestProtocolVersion(0);
        }
    }

    /**
     * Test feeder older log version than replica. Version 13 introduces
     * dtvlsn commits and so changes the txn commit/abort record formats.
     */
    @Test
    public void testLogVersionFeeder12Replica13()
        throws Throwable {

        try {
            ReplicaFeederHandshake.setTestLogVersion(13);
            FeederReplicaHandshake.setTestLogVersion(12);
            checkSuccess();
        } finally {
            ReplicaFeederHandshake.setTestLogVersion(0);
        }
    }

    /**
     * Test feeder newer log version than replica. Version 13 introduces
     * dtvlsn commits and so changes the txn commit/abort record formats.
     */
    @Test
    public void testLogVersionFeeder13Replica12()
        throws Throwable {

        try {
            ReplicaFeederHandshake.setTestLogVersion(12);
            FeederReplicaHandshake.setTestLogVersion(13);
            checkSuccess();
        } finally {
            ReplicaFeederHandshake.setTestLogVersion(0);
        }
    }

    /**
     * Test feeder with log version 8 and replica one version older: older
     * replica is not supported until feeder version 9.
     */
    @Test
    public void testOneOlderLogVersion8()
        throws Throwable {

        try {
            ReplicaFeederHandshake.setTestLogVersion(7);
            FeederReplicaHandshake.setTestLogVersion(8);
            checkForException(EnvironmentFailureReason.HANDSHAKE_ERROR);
        } finally {
            ReplicaFeederHandshake.setTestLogVersion(0);
            FeederReplicaHandshake.setTestLogVersion(0);
        }
    }

    /**
     * Test feeder with log version 9 and replica log version 8: this downgrade
     * is supported.
     */
    @Test
    public void testOneOlderLogVersion9()
        throws Throwable {

        try {
            ReplicaFeederHandshake.setTestLogVersion(8);
            FeederReplicaHandshake.setTestLogVersion(9);
            checkSuccess();
        } finally {
            ReplicaFeederHandshake.setTestLogVersion(0);
            FeederReplicaHandshake.setTestLogVersion(0);
        }
    }

    /**
     * Test that the handshake accounts for the fact that the highest replica
     * log entry version may not be the current log version.
     */
    @Test
    public void testLogVersionFeederHighestReplicableVersion()
        throws Throwable {

        int highestFeederVersion = LogEntryType.LOG_VERSION_HIGHEST_REPLICABLE;
        int lowestReplicaVersion;
        if (highestFeederVersion >=
            LogEntryType.LOG_VERSION_REPLICATE_OLDER) {
            /* Feeder can downgrade by one or more versions */
            lowestReplicaVersion = highestFeederVersion - 1;
        } else {
            /* No downgrades */
            lowestReplicaVersion = highestFeederVersion;
        }
        try {
            ReplicaFeederHandshake.setTestLogVersion(lowestReplicaVersion);
            checkSuccess();
        } finally {
            ReplicaFeederHandshake.setTestLogVersion(0);
        }
    }

    /**
     * Test error handling when there is a duplicate replica node.
     */
    @Test
    public void testDup()
        throws Exception {

        // Introduce a fake feeder in the map
        masterNode.feederManager().putFeeder(replicaEnvInfo.getRepConfig().
                                             getNodeName(), new Feeder());
        checkForException(EnvironmentFailureReason.HANDSHAKE_ERROR);
    }

    @Test
    public void testReplicaLeadingClockSkew()
        throws Exception {

        int delta = (int) replicaEnvInfo.getRepConfig().getMaxClockDelta
            (TimeUnit.MILLISECONDS);
        try {
            RepImpl.setSkewMs(delta + 10);
            checkForException(EnvironmentFailureReason.HANDSHAKE_ERROR);
        } finally {
            RepImpl.setSkewMs(0);
        }
    }

    @Test
    public void testReplicaLaggingClockSkew()
        throws Exception {

        int delta = (int) replicaEnvInfo.getRepConfig().getMaxClockDelta
            (TimeUnit.MILLISECONDS);
        RepImpl.setSkewMs(-(delta + 10));
        checkForException(EnvironmentFailureReason.HANDSHAKE_ERROR);
        try {
            RepImpl.setSkewMs(0);
        } finally {
            RepImpl.setSkewMs(0);
        }
    }

    @Test
    public void testDuplicateSocket()
        throws Exception {

        ReplicatedEnvironment renv2 = repEnvInfo[1].openEnv();
        ReplicatedEnvironment renv3 = repEnvInfo[2].openEnv();
        renv3.close();
        try {
            ReplicationConfig config = repEnvInfo[3].getRepConfig();
            config.setNodeHostPort(repEnvInfo[2].getRepConfig().
                                   getNodeHostPort());
            ReplicatedEnvironment renv4 = repEnvInfo[3].openEnv();
            renv4.close();
            fail("Expected exception");
        } catch (EnvironmentFailureException e) {
            assertEquals(EnvironmentFailureReason.HANDSHAKE_ERROR,
                         e.getReason());
        } catch (Exception e) {
            fail ("Wrong exception type " + e);
        }
        renv2.close();
    }

    @Test
    public void testConflictingPort()
        throws Exception {

        /* Establish the node in the rep group db. */
        replicaEnvInfo.openEnv();
        replicaEnvInfo.closeEnv();

        ReplicationConfig config = replicaEnvInfo.getRepConfig();
        config.setNodeHostPort(config.getNodeHostname() + ":" + 8888 );

        checkForException(EnvironmentFailureReason.HANDSHAKE_ERROR);
    }

    @Test
    public void testConflictingType()
        throws Exception {

        /* Establish the node in the rep group db. */
        replicaEnvInfo.openEnv();
        replicaEnvInfo.closeEnv();

        ReplicationConfig config = replicaEnvInfo.getRepConfig();
        config.setNodeType(NodeType.MONITOR);

        checkForException(EnvironmentFailureReason.HANDSHAKE_ERROR);
    }

    @Test
    public void testBadGroupOnReopen()
        throws Exception {

        /* Establish the node in the rep group db. */
        replicaEnvInfo.openEnv();
        replicaEnvInfo.closeEnv();

        ReplicationConfig config = replicaEnvInfo.getRepConfig();
        config.setGroupName("BAD");
        checkForException(EnvironmentFailureReason.UNEXPECTED_STATE);
    }

    private void checkForException(EnvironmentFailureReason reason) {
        try {
            replicaEnvInfo.openEnv();
            replicaEnvInfo.closeEnv();
            fail("expected exception");
        } catch (EnvironmentFailureException e) {
            assertEquals(reason, e.getReason());
        }
    }

    private void checkSuccess() {
        replicaEnvInfo.openEnv();
        replicaEnvInfo.closeEnv();
    }
}
