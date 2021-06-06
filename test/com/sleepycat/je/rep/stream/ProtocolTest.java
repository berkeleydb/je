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

package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_ENTRIES_WRITTEN_OLD_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.sleepycat.je.Durability;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Trace;
import com.sleepycat.je.log.entry.TraceLogEntry;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.Protocol.FeederProtocolVersion;
import com.sleepycat.je.rep.stream.Protocol.ReplicaProtocolVersion;
import com.sleepycat.je.rep.stream.Protocol.SNTPRequest;
import com.sleepycat.je.rep.stream.Protocol.SNTPResponse;
import com.sleepycat.je.rep.util.TestChannel;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

/**
 * Test basic functionality of feeder protocol messages.
 */
public class ProtocolTest extends TestBase {

    @Test
    public void testBasic()
        throws IOException {

        /* init token and table ids used in some test */
        final byte[] token = new byte[8];
        for(int i = 0 ; i < token.length; i++) {
            token[i] = (new Integer(i)).byteValue();
        }
        final String[] tableIds = new String[8];
        for(int i = 0 ; i < token.length; i++) {
            tableIds[i] = String.valueOf(i);
        }

        /* Setup a collection of every type of message */
        List<Message> testMessages = new LinkedList<Message>();
        Protocol protocol =
            Protocol.get(new RepNode(), Protocol.MAX_VERSION,
                         Protocol.MIN_VERSION, Protocol.MAX_VERSION,
                         LogEntryType.LOG_VERSION);

        OutputWireRecord wireRecord = makeFakeLogEntry("Tom Brady");
        Message testMsg = protocol.new Entry(wireRecord);
        testMessages.add(testMsg);

        testMsg = protocol.new ReplicaProtocolVersion();
        testMessages.add(testMsg);

        testMsg = protocol.new FeederProtocolVersion(1);
        testMessages.add(testMsg);

        testMsg= protocol.new DuplicateNodeReject("1234");
        testMessages.add(testMsg);

        testMsg = protocol.new ReplicaJEVersions(JEVersion.CURRENT_VERSION,
                                                 LogEntryType.LOG_VERSION);
        testMessages.add(testMsg);

        testMsg = protocol.new FeederJEVersions(JEVersion.CURRENT_VERSION,
                                                LogEntryType.LOG_VERSION,
                                                JEVersion.CURRENT_VERSION);
        testMessages.add(testMsg);

        testMsg= protocol.new JEVersionsReject("1234");
        testMessages.add(testMsg);

        /* multiple StartStream msgs share the the same op id in protocol */
        int numStartStreamMsg = 0;
        testMsg = protocol.new StartStream(new VLSN(18));
        testMessages.add(testMsg);
        numStartStreamMsg++;
        testMsg = protocol.new StartStream(new VLSN(19), null);
        testMessages.add(testMsg);
        numStartStreamMsg++;
        testMsg = protocol.new StartStream(new VLSN(20),
                                           new TestFilter(token, tableIds));
        testMessages.add(testMsg);
        numStartStreamMsg++;

        testMsg = protocol.new Heartbeat(System.currentTimeMillis(),
                                         0xdeadbeefdeadbeefL);
        testMessages.add(testMsg);

        testMsg = protocol.new HeartbeatResponse(new VLSN(100),
                                                 new VLSN(200));
        testMessages.add(testMsg);

        wireRecord = makeFakeLogEntry("Randy Moss");
        testMsg = protocol.new Commit(true,
                                      Durability.SyncPolicy.SYNC,
                                      wireRecord);
        testMessages.add(testMsg);

        testMsg = protocol.new Ack(19);
        testMessages.add(testMsg);

        testMsg = protocol.new GroupAck(new long[]{100, 101, 102});
        testMessages.add(testMsg);

        testMsg = protocol.new NodeGroupInfo
        ("repGroup",
         UUID.randomUUID(),
         new NameIdPair("node1",(short)1),
         "oracle.com",
         7000,
         NodeType.ELECTABLE,
         true,
         null);
        testMessages.add(testMsg);

        testMsg = protocol.new NodeGroupInfoOK(UUID.randomUUID(),
                                               new NameIdPair("node1",(short)1));
        testMessages.add(testMsg);

        testMsg =
            protocol.new NodeGroupInfoReject("Patriots lost the Superbowl.");
        testMessages.add(testMsg);

        testMsg = protocol.new EntryRequest(new VLSN(80));
        testMessages.add(testMsg);

        testMsg = protocol.new EntryNotFound();
        testMessages.add(testMsg);

        testMessages.add(protocol.new RestoreRequest(new VLSN(50)));

        RepNodeImpl rn1 = new RepNodeImpl(new NameIdPair("n1",1),
                                          NodeType.ELECTABLE,
                                          "host1",
                                          1000,
                                          null);
        RepNodeImpl rn2 = new RepNodeImpl(new NameIdPair("n2",1),
                                          NodeType.ELECTABLE,
                                          "host2",
                                          2000,
                                          null);
        testMsg = protocol.new RestoreResponse
            (new VLSN(60), new RepNodeImpl[] {rn1, rn2});
        testMessages.add(testMsg);

        wireRecord = makeFakeLogEntry("Bruschi");
        testMsg = protocol.new AlternateMatchpoint(wireRecord);
        testMessages.add(testMsg);

        testMsg = protocol.new ShutdownRequest(System.currentTimeMillis());
        testMessages.add(testMsg);

        testMsg = protocol.new ShutdownResponse();
        testMessages.add(testMsg);

        testMsg = protocol.new ReAuthenticate(token);
        testMessages.add(testMsg);

        /*
         * For each type of message, make sure we can parse it, and that the
         * resulting new message is identical. Make sure we test all message
         * types but the SNTP messages, since they contain timestamp fields
         * that are initialized at serialization and deserialization.
         */
        assertEquals(protocol.messageCount() -
                        protocol.getPredefinedMessageCount() -
                        /* Excluded SNTP messages. */
                        2 +
                        /* Include duplicated StarStream messages */
                        (numStartStreamMsg - 1),
                testMessages.size());
        for (Message m : testMessages) {
            ByteBuffer testWireFormat = m.wireFormat().duplicate();
            Message newMessage =
                protocol.read(new TestChannel(testWireFormat));
            assertTrue(newMessage.getOp() + " new=" + newMessage +
                       " test=" + m,
                       newMessage.match(m));
        }
        /* Custom tests for sntp messages */
        testSNTPMessages(protocol);
    }

    public static class TestFilter implements FeederFilter, Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] token;
        private final String[] tableIds;

        public TestFilter(byte[] token, String[] tableIds) {
            this.token = token;
            this.tableIds = tableIds;
        }

        @Override
        public OutputWireRecord execute(final OutputWireRecord outputRecord,
                                        final RepImpl repImpl) {
            throw new UnsupportedOperationException("Method not implemented: " +
                                                    "execute");
        }

        @Override
        public String[] getTableIds() {
            return tableIds;
        }

    }

    private void testSNTPMessages(Protocol protocol)
        throws IOException {

        SNTPRequest m1s = protocol.new SNTPRequest(true);
        assertEquals(-1, m1s.getReceiveTimestamp());
        SNTPRequest m1r =
            (SNTPRequest) protocol.read(new TestChannel
                                        (m1s.wireFormat().duplicate()));
        assertFalse(-1 == m1r.getReceiveTimestamp());
        assertTrue(m1r.isLast());
        SNTPResponse m2s = protocol.new SNTPResponse(m1s);
        assertEquals(m1s.getOriginateTimestamp(), m2s.getOriginateTimestamp());
        assertEquals(m1s.getReceiveTimestamp(), m2s.getReceiveTimestamp());
        assertEquals(-1, m2s.getTransmitTimestamp());
        assertEquals(-1, m2s.getDestinationTimestamp());
        ByteBuffer wireFormat = m2s.wireFormat().duplicate();
        assertFalse(-1 == m2s.getTransmitTimestamp());
        assertEquals(-1, m2s.getDestinationTimestamp());
        SNTPResponse m2r =
            (SNTPResponse) protocol.read(new TestChannel(wireFormat));
        assertEquals(m1s.getOriginateTimestamp(), m2s.getOriginateTimestamp());
        assertEquals(m1s.getReceiveTimestamp(), m2r.getReceiveTimestamp());
        assertEquals(m2s.getTransmitTimestamp(), m2s.getTransmitTimestamp());
        assertFalse(-1 == m2r.getDestinationTimestamp());
    }

    private OutputWireRecord makeFakeLogEntry(String msg) {
        return makeFakeLogEntry(msg, LogEntryType.LOG_VERSION);
    }

    private OutputWireRecord makeFakeLogEntry(String msg, int logVersion) {
        final TraceLogEntry entry = new TraceLogEntry(new Trace(msg));
        final ByteBuffer entryBuffer = ByteBuffer.allocate(entry.getSize());
        entry.writeEntry(entryBuffer);
        entryBuffer.flip();
        final LogEntryHeader fakeHeader =
            new LogEntryHeader(LogEntryType.LOG_TRACE.getTypeNum(),
                               logVersion,
                               entry.getSize(),
                               new VLSN(33));
        return new OutputWireRecord(null, fakeHeader, entryBuffer);
    }

    @Test
    public void testVersion() {
        Protocol protocol100 =
            Protocol.get(new RepNode(), Protocol.MAX_VERSION,
                         Protocol.MIN_VERSION, Protocol.MAX_VERSION,
                         LogEntryType.LOG_VERSION);

        ReplicaProtocolVersion repVersion =
            protocol100.new ReplicaProtocolVersion();
        assertEquals(repVersion.getVersion(), protocol100.getVersion());
        assertEquals(repVersion.getVersion(), protocol100.getVersion());
        FeederProtocolVersion feederVersion =
            protocol100.new FeederProtocolVersion(protocol100.getVersion());
        assertEquals(feederVersion.getVersion(), protocol100.getVersion());

    }

    /**
     * Test that writing a message containing a log entry where the requested
     * log format version is less than both the current log version and the log
     * entry version results in converting the entry to the requested version.
     */
    @Test
    public void testWritePreviousVersionOlderConvert()
        throws IOException {

        /* Use this value when converting to the previous version */
        final Trace priorItem = new Trace("replacement");
        TraceLogEntry.setTestPriorItem(priorItem);
        try {
            final Protocol protocol =
                Protocol.get(new RepNode(), Protocol.MAX_VERSION,
                             Protocol.MIN_VERSION, Protocol.MAX_VERSION,
                             /* Request the previous version */
                             new TraceLogEntry().getLastFormatChange() - 1);
            final OutputWireRecord writeRecord = makeFakeLogEntry("original");
            final Message writeMessage = protocol.new Entry(writeRecord);
            final Protocol.Entry readMessage =
                (Protocol.Entry) protocol.read(
                    new TestChannel(writeMessage.wireFormat().duplicate()));
            final InputWireRecord readRecord = readMessage.getWireRecord();
            final TraceLogEntry readEntry =
                (TraceLogEntry) readRecord.getLogEntry();
            final Trace readItem = readEntry.getMainItem();
            /* Confirm that the entry was converted */
            assertEquals("Trace", priorItem, readItem);
            final StatGroup stats = protocol.getStats(new StatsConfig());
            assertEquals("N_ENTRIES_WRITTEN_OLD_VERSION",
                         1, stats.getLong(N_ENTRIES_WRITTEN_OLD_VERSION));
        } finally {
            TraceLogEntry.setTestPriorItem(null);
        }
    }

    /**
     * Test that writing a message containing a log entry where the requested
     * log format version is less than the current log version but is
     * compatible with the requested version results in the entry being copied,
     * not converted.
     */
    @Test
    public void testWritePreviousVersionCompatibleCopy()
        throws IOException {

        /* Use this value when converting to the previous version */
        final Trace priorItem = new Trace("replacement");
        TraceLogEntry.setTestPriorItem(priorItem);
        try {
            final Protocol protocol =
                Protocol.get(new RepNode(), Protocol.MAX_VERSION,
                             Protocol.MIN_VERSION, Protocol.MAX_VERSION,
                             /* Request the previous version */
                             new TraceLogEntry().getLastFormatChange() - 1);
            final OutputWireRecord writeRecord =
                makeFakeLogEntry("original",
                                 /* Create the entry in the previous version */
                                new TraceLogEntry().getLastFormatChange() - 1);
            final Message writeMessage = protocol.new Entry(writeRecord);
            final Protocol.Entry readMessage =
                (Protocol.Entry) protocol.read(
                    new TestChannel(writeMessage.wireFormat().duplicate()));
            final InputWireRecord readRecord = readMessage.getWireRecord();
            final TraceLogEntry readEntry =
                (TraceLogEntry) readRecord.getLogEntry();
            final Trace readItem = readEntry.getMainItem();
            /* Confirm that the entry was copied, not converted */
            assertEquals("Trace message", "original", readItem.getMessage());
            final StatGroup stats = protocol.getStats(new StatsConfig());
            assertEquals("N_ENTRIES_WRITTEN_OLD_VERSION",
                         0, stats.getLong(N_ENTRIES_WRITTEN_OLD_VERSION));
        } finally {
            TraceLogEntry.setTestPriorItem(null);
        }
    }

    /**
     * Test reading a NodeGroupInfo object using the format that does not
     * support the jeVersion field.
     */
    @Test
    public void testNodeGroupInfoNoJEVersion()
        throws IOException {

        final Protocol protocol =
            Protocol.get(new RepNode(), Protocol.VERSION_4,
                         Protocol.MIN_VERSION, Protocol.MAX_VERSION,
                         LogEntryType.LOG_VERSION);
        for (final JEVersion jeVersion :
                 new JEVersion[] { null, new JEVersion("1.2.3") }) {
            final Message msg = protocol.new NodeGroupInfo(
                "repGroup", UUID.randomUUID(), new NameIdPair("node7", 7),
                "example.com", 7000, NodeType.ELECTABLE, true, jeVersion);
            final ByteBuffer bytes = msg.wireFormat().duplicate();
            final Message newMsg = protocol.read(new TestChannel(bytes));
            assertTrue("Expected " + msg + ", found " + newMsg,
                       newMsg.match(msg));
        }
    }

    /**
     * Test sending a SECONDARY node to a master running a protocol version
     * that does not support them.  Note that protocol version 4 shouldn't get
     * picked for systems that can create secondary nodes, but just checking
     * that an internal exception is thrown in that case.
     */
    @Test
    public void testNodeGroupInfoSecondaryToOldMaster() {
        final Protocol protocol = Protocol.get(
            new RepNode(), Protocol.VERSION_4, Protocol.MIN_VERSION,
            Protocol.MAX_VERSION, LogEntryType.LOG_VERSION);
        final Message msg = protocol.new NodeGroupInfo(
            "repGroup", UUID.randomUUID(), new NameIdPair("node7", 7),
            "example.com", 7000, NodeType.SECONDARY, true,
            JEVersion.CURRENT_VERSION);
        try {
            msg.wireFormat();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
        }
    }
}
