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

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;

import java.util.Set;

import org.junit.Test;

import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.monitor.Monitor;
import com.sleepycat.je.rep.monitor.MonitorConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;

public class ReplicationGroupTest extends RepTestBase {

    @SuppressWarnings("null")
    @Test
    public void testBasic()
        throws InterruptedException {

        final int dataNodeSize = groupSize - 1;
        final int electableNodeSize = groupSize - 2;
        final int persistentNodeSize = groupSize - 1;

        final ReplicationConfig sConfig =
            repEnvInfo[groupSize-2].getRepConfig();
        sConfig.setNodeType(NodeType.SECONDARY);

        createGroup(dataNodeSize);

        ReplicationConfig rConfig = repEnvInfo[groupSize-1].getRepConfig();
        /* RepNetConfig needs to come from an open environment */
        ReplicationConfig r0Config = repEnvInfo[0].getEnv().getRepConfig();
        rConfig.setNodeType(NodeType.MONITOR);
        MonitorConfig monConfig = new MonitorConfig();
        monConfig.setNodeName(rConfig.getNodeName());
        monConfig.setGroupName(rConfig.getGroupName());
        monConfig.setNodeHostPort(rConfig.getNodeHostPort());
        monConfig.setHelperHosts(rConfig.getHelperHosts());
        monConfig.setRepNetConfig(r0Config.getRepNetConfig());


        new Monitor(monConfig).register();

        for (int i=0; i < dataNodeSize; i++) {
            final ReplicatedEnvironment env = repEnvInfo[i].getEnv();
            final boolean isMaster = (env.getState() == MASTER);
            final int targetGroupSize =
                isMaster ? groupSize : persistentNodeSize;
            ReplicationGroup group = null;
            for (int j=0; j < 100; j++) {
                group = env.getGroup();
                if (group.getNodes().size() == targetGroupSize) {
                    break;
                }
                /* Wait for the replica to catch up. */
                Thread.sleep(1000);
            }
            assertEquals("Nodes", targetGroupSize, group.getNodes().size());
            assertEquals(RepTestUtils.TEST_REP_GROUP_NAME, group.getName());
            logger.info(group.toString());

            for (RepEnvInfo rinfo : repEnvInfo) {
                final ReplicationConfig repConfig = rinfo.getRepConfig();
                ReplicationNode member =
                    group.getMember(repConfig.getNodeName());
                if (!isMaster &&
                    repConfig.getNodeType().isSecondary()) {
                    assertNull("Member", member);
                } else {
                    assertNotNull("Member", member);
                    assertEquals(repConfig.getNodeName(), member.getName());
                    assertEquals(repConfig.getNodeType(), member.getType());
                    assertEquals(repConfig.getNodeSocketAddress(),
                                 member.getSocketAddress());
                }
            }

            final Set<ReplicationNode> electableNodes =
                group.getElectableNodes();
            for (final ReplicationNode n : electableNodes) {
                assertEquals(NodeType.ELECTABLE, n.getType());
            }
            assertEquals("Electable nodes",
                         electableNodeSize, electableNodes.size());

            final Set<ReplicationNode> monitorNodes = group.getMonitorNodes();
            for (final ReplicationNode n : monitorNodes) {
                assertEquals(NodeType.MONITOR, n.getType());
            }
            assertEquals("Monitor nodes", 1, monitorNodes.size());

            final Set<ReplicationNode> secondaryNodes =
                group.getSecondaryNodes();
            for (final ReplicationNode n : secondaryNodes) {
                assertEquals(NodeType.SECONDARY, n.getType());
            }
            assertEquals("Secondary nodes",
                         isMaster ? 1 : 0,
                         secondaryNodes.size());

            final Set<ReplicationNode> dataNodes = group.getDataNodes();
            for (final ReplicationNode n : dataNodes) {
                if (isMaster) {
                    assertThat(n.getType(),
                               anyOf(is(NodeType.ELECTABLE),
                                     is(NodeType.SECONDARY)));
                } else {
                    assertEquals(NodeType.ELECTABLE, n.getType());
                }
            }
            assertEquals("Data nodes",
                         isMaster ? dataNodeSize : electableNodeSize,
                         dataNodes.size());
        }
    }
}
