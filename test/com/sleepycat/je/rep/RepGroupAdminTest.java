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

import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

public class RepGroupAdminTest extends RepTestBase {

    @Test
    public void testRemoveMember() {
        createGroup(groupSize);
        ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        assertTrue(master.getState().isMaster());

        RepEnvInfo rmMember = repEnvInfo[repEnvInfo.length-1];
        Set<InetSocketAddress> helperSockets =
            rmMember.getRepImpl().getHelperSockets();
        final String rmName = rmMember.getRepNode().getNodeName();
        rmMember.closeEnv();

        ReplicationGroupAdmin groupAdmin =
            new ReplicationGroupAdmin
            (RepTestUtils.TEST_REP_GROUP_NAME, helperSockets,
             RepTestUtils.readRepNetConfig());
        assertEquals(groupSize,
                     master.getGroup().getElectableNodes().size());
        groupAdmin.removeMember(rmName);
        assertEquals(groupSize-1,
                     master.getGroup().getElectableNodes().size());

        try {
            rmMember.openEnv();
            fail("Expected exception");
        } catch (EnvironmentFailureException e) {
            assertEquals(EnvironmentFailureReason.HANDSHAKE_ERROR,
                         e.getReason());
        }

        /* Exception tests.  We currently allow either IAE or EFE. */
        try {
            groupAdmin.removeMember("unknown node");
            fail("Expected exception");
        } catch (MemberNotFoundException e) {
            // Expected.
        }

        try {
            groupAdmin.removeMember(rmName);
            fail("Expected exception");
        } catch (MemberNotFoundException e) {
            // Expected.
        }

        try {
            groupAdmin.removeMember(master.getNodeName());
            fail("Expected exception");
        } catch (MasterStateException e) {
            // Expected.
        }
    }

    @Test
    public void testDeleteMember() {
        createGroup(groupSize);
        final ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        assertTrue(master.getState().isMaster());

        final RepEnvInfo delMember = repEnvInfo[repEnvInfo.length-1];
        final Set<InetSocketAddress> helperSockets =
            delMember.getRepImpl().getHelperSockets();
        final String delName = delMember.getRepNode().getNodeName();
        delMember.closeEnv();

        final ReplicationGroupAdmin groupAdmin = new ReplicationGroupAdmin(
            RepTestUtils.TEST_REP_GROUP_NAME, helperSockets,
            RepTestUtils.readRepNetConfig());
        assertEquals(groupSize,
                     master.getGroup().getElectableNodes().size());
        groupAdmin.deleteMember(delName);
        assertEquals(groupSize-1,
                     master.getGroup().getElectableNodes().size());

        /* The deleted member automatically rejoins when reopened */
        delMember.openEnv();

        /* Exception tests. */
        try {
            groupAdmin.deleteMember("unknown node");
            fail("Expected exception");
        } catch (MemberNotFoundException e) {
            // Expected.
        }

        try {
            groupAdmin.deleteMember(delName);
            fail("Expected exception");
        } catch (EnvironmentFailureException e) {
            // Expected.
        }

        delMember.closeEnv();
        groupAdmin.deleteMember(delName);

        try {
            groupAdmin.deleteMember(delName);
            fail("Expected exception");
        } catch (MemberNotFoundException e) {
            // Expected.
        }

        try {
            groupAdmin.deleteMember(master.getNodeName());
            fail("Expected exception");
        } catch (MasterStateException e) {
            // Expected.
        }
    }

    @Test
    public void testAddMonitor()
        throws DatabaseException, InterruptedException {

        ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
        RepImpl lastImpl = RepInternal.getNonNullRepImpl(
            repEnvInfo[repEnvInfo.length-1].getEnv());

        Set<InetSocketAddress> helperSockets =
            new HashSet<InetSocketAddress>();
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            helperSockets.add(RepInternal.getNonNullRepImpl(rep).getSocket());
        }

        DbConfigManager lastConfigMgr = lastImpl.getConfigManager();
        ReplicationGroupAdmin groupAdmin =
            new ReplicationGroupAdmin(lastConfigMgr.get(GROUP_NAME),
                                      helperSockets,
                                      RepTestUtils.readRepNetConfig());
        int lastId = lastImpl.getNodeId();
        final short monitorId = (short)(lastId+1);

        RepNodeImpl monitorNode =
            new RepNodeImpl(new NameIdPair("monitor" + monitorId,
                                           monitorId),
                            NodeType.MONITOR,
                            lastImpl.getHostName(),
                            lastImpl.getPort()+1,
                            null);
        groupAdmin.ensureMonitor(monitorNode);

        /* Second ensure should not result in errors. */
        groupAdmin.ensureMonitor(monitorNode);

        RepTestUtils.syncGroupToLastCommit(repEnvInfo, repEnvInfo.length);
        assertTrue(master.getState().isMaster());
        /* All nodes should know about the new monitor. */
        for (RepEnvInfo repi : repEnvInfo) {
            ReplicatedEnvironment rep = repi.getEnv();
            RepGroupImpl repGroup =
                RepInternal.getNonNullRepImpl(rep).getRepNode().getGroup();
            RepNodeImpl monitor = repGroup.getMember(monitorId);
            assertNotNull(monitor);
            assertTrue(monitorNode.equivalent(monitor));
        }

        /* Catch incorrect use of an existing non-monitor node name */
        RepNodeImpl badMonitorNode =
            new RepNodeImpl(
                new NameIdPair(repEnvInfo[1].getRepConfig().getNodeName()),
                NodeType.MONITOR,
                lastImpl.getHostName(),
                lastImpl.getPort(),
                null);
        try {
            groupAdmin.ensureMonitor(badMonitorNode);
            fail("expected exception");
        } catch (DatabaseException e) {
            assertTrue(true);
        }

        /* test exception from adding a non-monitor node. */
        badMonitorNode =
            new RepNodeImpl(new NameIdPair("monitor" + monitorId, monitorId),
                            NodeType.ELECTABLE,
                            lastImpl.getHostName(),
                            lastImpl.getPort(),
                            null);
        try {
            groupAdmin.ensureMonitor(badMonitorNode);
            fail("expected exception");
        } catch (EnvironmentFailureException e) {
            assertTrue(true);
        }
    }
}
