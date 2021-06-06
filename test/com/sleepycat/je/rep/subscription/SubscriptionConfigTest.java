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

package com.sleepycat.je.rep.subscription;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.TestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Test that SubscriptionConfig can be initialized, set, and read correctly.
 */
public class SubscriptionConfigTest extends TestBase {

    private final String home = "./test/subscription/";
    private final String feederHostPortPair = "localhost:6000";
    private final String subNodeName = "test-subscriber";
    private final String subHostPortPair = "localhost:6000";
    private final String groupName = "rg1";
    private final UUID   groupUUID =
        UUID.fromString("cb675927-433a-4ed6-8382-0403e9861619");
    private SubscriptionConfig config;

    private final Logger logger =
            LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        config = new SubscriptionConfig(subNodeName, home, subHostPortPair,
                                        feederHostPortPair, groupName,
                                        groupUUID);
    }

    @Test
    public void testInitialziedParameters() throws Exception {
        assertEquals(home, config.getSubscriberHome());
        assertEquals(subNodeName, config.getSubNodeName());
        assertEquals(subHostPortPair, config.getSubNodeHostPort());

        assertEquals(HostPortPair.getHostname(feederHostPortPair),
                     config.getFeederHost());
        assertEquals(HostPortPair.getPort(feederHostPortPair),
                     config.getFeederPort());

        assertEquals(groupName, config.getGroupName());
        assertEquals(groupUUID, config.getGroupUUID());
        assertEquals(NodeType.SECONDARY, config.getNodeType());

        assertEquals(EntryRequestType.DEFAULT, config.getStreamMode());
    }

    @Test
    public void testNodeType() throws Exception {
        SubscriptionConfig config1 =
            new SubscriptionConfig(subNodeName, home, subHostPortPair,
                                   feederHostPortPair, groupName, groupUUID,
                                   NodeType.EXTERNAL);
        assertEquals(NodeType.EXTERNAL, config1.getNodeType());

        /* types other than EXTERNAL and SECONDARY */
        NodeType[] types = new NodeType[]{NodeType.ARBITER,
            NodeType.ELECTABLE, NodeType.MONITOR};

        for (NodeType type : types) {
            try {
                new SubscriptionConfig(subNodeName, home, subHostPortPair,
                                       feederHostPortPair, groupName,
                                       groupUUID, type);
            } catch (IllegalArgumentException e) {
                /* expected exception due to non-supported node type */
                logger.info(
                    "Expected IllegalArgumentException " + e.getMessage());
            }
        }
    }

    @Test
    public void testSetParameters() {
        long timeout = 10000;
        TimeUnit unit = TimeUnit.MILLISECONDS;
        config.setChannelTimeout(timeout, unit);
        assertEquals(timeout, config.getChannelTimeout(unit));
        config.setPreHeartbeatTimeout(2 * timeout, unit);
        assertEquals(2 * timeout, config.getPreHeartbeatTimeout(unit));
        config.setStreamOpenTimeout(3 * timeout, unit);
        assertEquals(3 * timeout, config.getStreamOpenTimeout(unit));

        int interval = 2000;
        config.setHeartbeatInterval(interval);
        assertEquals(interval, config.getHeartbeatIntervalMs());

        int sz = 10240;
        config.setInputMessageQueueSize(sz);
        assertEquals(sz, config.getInputMessageQueueSize());
        config.setOutputMessageQueueSize(2 * sz);
        assertEquals(2 * sz, config.getOutputMessageQueueSize());

        config.setReceiveBufferSize(3 * sz);
        assertEquals(3 * sz, config.getReceiveBufferSize());
    }

    @Test
    public void testFeederFilter() throws Exception {

        /* filter cannot be null */
        try {
            config = new SubscriptionConfig(subNodeName,
                                            "./",
                                            subHostPortPair,
                                            feederHostPortPair,
                                            groupName);


            config.setFeederFilter(null);
        } catch (IllegalArgumentException e) {
            /* expected exception due to null feeder filter */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }

        /* filter is set correctly */
        config = new SubscriptionConfig(subNodeName,
                                        "./",
                                        subHostPortPair,
                                        feederHostPortPair,
                                        groupName);

        final String token = "test filter";
        config.setFeederFilter(new TestFeederFilter(token));
        assert(config.getFeederFilter().toString().equals(token));
    }

    @Test
    public void testCallback() throws Exception {

        /* callback cannot be null */
        try {
            config = new SubscriptionConfig(subNodeName,
                                            "./",
                                            subHostPortPair,
                                            feederHostPortPair,
                                            groupName);


            config.setCallback(null);
        } catch (IllegalArgumentException e) {
            /* expected exception due to null feeder filter */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }

        /* callback is set correctly */
        config = new SubscriptionConfig(subNodeName,
                                        "./",
                                        subHostPortPair,
                                        feederHostPortPair,
                                        groupName);

        final String token = "TestCallback";
        config.setCallback(new TestCallback(token));
        assert(config.getCallBack().toString().equals(token));
    }


    @Test
    public void testMissingParameters() throws Exception {

        /* making missing parameters */
        try {
            config = new SubscriptionConfig(null,
                                            "./",
                                            subHostPortPair,
                                            feederHostPortPair,
                                            groupName);

            /* should not be able to create a config with missing parameters */
            fail("Expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected exception */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }

        try {
            config = new SubscriptionConfig(subNodeName,
                                            null,
                                            subHostPortPair,
                                            feederHostPortPair,
                                            groupName);

            /* should not be able to create a config with missing parameters */
            fail("Expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected exception */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }

        try {
            config = new SubscriptionConfig(subNodeName,
                                            "./",
                                            null,
                                            feederHostPortPair,
                                            groupName);

            /* should not be able to create a config with missing parameters */
            fail("Expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected exception */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }

        try {
            config = new SubscriptionConfig(subNodeName,
                                            "./",
                                            subHostPortPair,
                                            null,
                                            groupName);

            /* should not be able to create a config with missing parameters */
            fail("Expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected exception */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }

        try {
            config = new SubscriptionConfig(subNodeName,
                                            "./",
                                            subHostPortPair,
                                            feederHostPortPair,
                                            null);

            /* should not be able to create a config with missing parameters */
            fail("Expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected exception */
            logger.info("Expected IllegalArgumentException " + e.getMessage());
        }
    }

    @Test
    public void testStreamMode() throws Exception {
        final SubscriptionConfig config =
            new SubscriptionConfig(subNodeName, home, subHostPortPair,
                                   feederHostPortPair, groupName, groupUUID,
                                   NodeType.EXTERNAL);

        for (EntryRequestType type : EntryRequestType.values()) {
            config.setStreamMode(type);
            assertEquals(type, config.getStreamMode());
        }
    }

    /* no-op test callback */
    private class TestCallback implements SubscriptionCallback {

        private final String id;

        TestCallback(String id) {
            this.id = id;
        }

        @Override
        public void processPut(VLSN vlsn, byte[] key, byte[] value,
                               long txnId) {

        }

        @Override
        public void processDel(VLSN vlsn, byte[] key, long txnId) {

        }

        @Override
        public void processCommit(VLSN vlsn, long txnid) {

        }

        @Override
        public void processAbort(VLSN vlsn, long txnid) {

        }

        @Override
        public void processException(final Exception exception) {

        }

        @Override
        public String toString() {
            return id;
        }
    }

    /* no-op test feeder filter */
    private class TestFeederFilter implements FeederFilter, Serializable {
        private static final long serialVersionUID = 1L;
        private final String id;

        TestFeederFilter(String id) {
            this.id = id;
        }

        @Override
        public OutputWireRecord execute(final OutputWireRecord record,
                                        final RepImpl repImpl) {
            return record;
        }

        @Override
        public String[] getTableIds() {
            return null;
        }

        @Override
        public String toString() {
            return id;
        }
    }
}
