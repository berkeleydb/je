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

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_ACK_MESSAGES;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_GROUPED_ACKS;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_GROUP_ACK_MESSAGES;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MAX_GROUPED_ACKS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.cbvlsn.GlobalCBVLSN;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.utilint.LongMaxStat;
import com.sleepycat.je.utilint.LongMaxZeroStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.VLSN;

/**
 * Defines the base protocol of messages used to set up a stream between
 * source and target.
 *
 * Note BaseProtocol represents a set of basic protocol operations intended to
 * be used by subclasses. For a complete description of message operations
 * used in JE HA protocol, please see the Protocol class in the same package.
 *
 * @see com.sleepycat.je.rep.stream.Protocol
 */
public abstract class BaseProtocol extends BinaryProtocol {

    /* --------------------------- */
    /* ---  protocol versions  --- */
    /* --------------------------- */

    /*
     * Note that the GROUP_ACK response message was introduced in version 5,
     * but is disabled by default via RepParams.REPLICA_ENABLE_GROUP_ACKS.
     *
     * It can be enabled when we can increase the protocol version number.
     */

    /* The default (highest) version supported by the Protocol code. */
    public static final int MAX_VERSION = 7;

    /* The minimum version we're willing to interact with. */
    static final int MIN_VERSION = 3;

    /*
     * Version added in JE 7.5.6 to support entry request type
     */
    public static final int VERSION_7 = 7;
    public static final JEVersion VERSION_7_JE_VERSION =
        new JEVersion("7.5.6");

    /*
     * Version added in JE 6.4.10 to support generic feeder filtering
     */
    public static final int VERSION_6 = 6;
    public static final JEVersion VERSION_6_JE_VERSION =
        new JEVersion("6.4.10");

    /* Version added in JE 6.0.1 to support RepGroupImpl version 3. */
    public static final int VERSION_5 = 5;
    public static final JEVersion VERSION_5_JE_VERSION =
        new JEVersion("6.0.1");

    /*
     * Version in which HEARTBEAT_RESPONSE added a second field.  We can manage
     * without this optional additional information if we have to, we we can
     * still interact with the previous protocol version.  (JE 5.0.58)
     */
    static final int VERSION_4 = 4;
    public static final JEVersion VERSION_4_JE_VERSION =
        new JEVersion("5.0.58");

    /* Version added in JE 4.0.50 to address byte order issues. */
    static final int VERSION_3 = 3;
    public static final JEVersion VERSION_3_JE_VERSION =
        new JEVersion("4.0.50");

    /* ------------------------------------------ */
    /* ---  messages defined in base protocol --- */
    /* ------------------------------------------ */

    /* range of op codes allowed in subclasses, inclusively. */
    protected final static short MIN_MESSAGE_OP_CODE_IN_SUBCLASS = 1024;
    protected final static short MAX_MESSAGE_OP_CODE_IN_SUBCLASS = 2047;

    /*
     * Following ops are core replication stream post-handshake messages
     * defined in streaming protocol and are intended to be used in subclasses.
     *
     * Note these msg op codes inherit from original implementation of stream
     * protocol. Due to backward compatibility requirement, we keep them
     * unchanged and directly copy them here.
     */
    public final static MessageOp ENTRY =
        new MessageOp((short) 101, Entry.class);

    public final static MessageOp START_STREAM =
        new MessageOp((short) 102, StartStream.class);

    public final static MessageOp HEARTBEAT =
        new MessageOp((short) 103, Heartbeat.class);

    public final static MessageOp HEARTBEAT_RESPONSE =
        new MessageOp((short) 104, HeartbeatResponse.class);

    public final static MessageOp COMMIT =
        new MessageOp((short) 105, Commit.class);

    public final static MessageOp ACK =
        new MessageOp((short) 106, Ack.class);

    public final static MessageOp ENTRY_REQUEST =
        new MessageOp((short) 107, EntryRequest.class);

    public final static MessageOp ENTRY_NOTFOUND =
        new MessageOp((short) 108, EntryNotFound.class);

    public final static MessageOp ALT_MATCHPOINT =
        new MessageOp((short) 109, AlternateMatchpoint.class);

    public final static MessageOp RESTORE_REQUEST =
        new MessageOp((short) 110, RestoreRequest.class);

    public final static MessageOp RESTORE_RESPONSE =
        new MessageOp((short) 111, RestoreResponse.class);

    public final static MessageOp SHUTDOWN_REQUEST =
        new MessageOp((short) 112, ShutdownRequest.class);

    public final static MessageOp SHUTDOWN_RESPONSE =
        new MessageOp((short) 113, ShutdownResponse.class);

    public final static MessageOp GROUP_ACK =
        new MessageOp((short) 114, GroupAck.class);

    /* --------------------------- */
    /* --------  fields  --------- */
    /* --------------------------- */

    /** The log version of the format used to write log entries to the stream. */
    protected int streamLogVersion;

    /* Count of all singleton ACK messages. */
    protected final LongStat nAckMessages;

    /* Count of all group ACK messages. */
    protected final LongStat nGroupAckMessages;

    /* Sum of all acks sent via group ACK messages. */
    protected final LongStat nGroupedAcks;

    /* Max number of acks sent via a single group ACK message. */
    protected final LongMaxStat nMaxGroupedAcks;

    protected final RepImpl repImpl;

    /**
     * Whether to fix the log version for log entries received from JE 7.0.x
     * feeders that use log version 12 format but are incorrectly marked with
     * later log versions due to a bug ([#25222]).  The problem is that the
     * feeder supplies an entry in log version 12 (LOG_VERSION_EXPIRE_INFO)
     * format, but says it has a later log version.
     *
     * <p>This field is only set to true by the replica, which only reads and
     * writes it from the main Replica thread, so no synchronization is needed.
     */
    private boolean fixLogVersion12Entries = false;

    /**
     * Returns a BaseProtocol object configured that implements the specified
     * (supported) protocol version.
     *
     * @param repImpl the node using the protocol
     *
     * @param nameIdPair name-id pair of the node using the protocol
     *
     * @param protocolVersion the version of the protocol that must be
     *        implemented by this object
     *
     * @param maxProtocolVersion the highest supported protocol version, which
     *        may be lower than the code version, for testing purposes
     *
     * @param streamLogVersion the log version of the format used to write log
     *        entries
     *
     * @param protocolOps the message operations that make up this protocol
     *
     * @param checkValidity whether to check the message operations for
     *        validity.  Checks should be performed for new protocols, but
     *        suppressed for legacy ones.
     */
    BaseProtocol(final RepImpl repImpl,
                 final NameIdPair nameIdPair,
                 final int protocolVersion,
                 final int maxProtocolVersion,
                 final int streamLogVersion,
                 final MessageOp[] protocolOps,
                 final boolean checkValidity) {
        super(nameIdPair, maxProtocolVersion, protocolVersion, repImpl);
        this.streamLogVersion = streamLogVersion;
        this.repImpl = repImpl;

        nAckMessages = new LongStat(stats, N_ACK_MESSAGES);
        nGroupAckMessages = new LongStat(stats, N_GROUP_ACK_MESSAGES);
        nGroupedAcks = new LongStat(stats, N_GROUPED_ACKS);
        nMaxGroupedAcks = new LongMaxZeroStat(stats, N_MAX_GROUPED_ACKS);
        initializeMessageOps(protocolOps, checkValidity);
    }

    /**
     * Returns a BaseProtocol object configured that implements the specified
     * (supported) protocol version, with enforced message operation
     * code validity check.
     *
     * This constructor enforces checking validity of message operation code.
     * It should be used in any subclass except the legacy HA protocol in
     * je.stream.Protocol.
     *
     * @param repImpl the node using the protocol
     *
     * @param nameIdPair name-id pair of the node using the protocol
     *
     * @param protocolVersion the version of the protocol that must be
     *        implemented by this object
     *
     * @param maxProtocolVersion the highest supported protocol version, which
     *        may be lower than the code version, for testing purposes
     *
     * @param streamLogVersion the log version of the format used to write log
     *        entries
     *
     * @param protocolOps the message operations that make up this protocol
     */
    protected BaseProtocol(final RepImpl repImpl,
                           final NameIdPair nameIdPair,
                           final int protocolVersion,
                           final int maxProtocolVersion,
                           final int streamLogVersion,
                           final MessageOp[] protocolOps) {

        this(repImpl, nameIdPair, protocolVersion, maxProtocolVersion,
             streamLogVersion, protocolOps, true);
    }

    public int getStreamLogVersion() {
        return streamLogVersion;
    }

    /**
     * Invoked in cases where the stream log version is not known at the time
     * the protocol object is created and stream log version negotiations are
     * required to determine the version that will be used for log records sent
     * over the HA stream.
     *
     * @param streamLogVersion the maximum log version associated with stream
     * records
     */
    public void setStreamLogVersion(int streamLogVersion) {
        this.streamLogVersion = streamLogVersion;
    }

    /**
     * Returns whether log entries need their log versions fixed to work around
     * [#25222].
     */
    public boolean getFixLogVersion12Entries() {
        return fixLogVersion12Entries;
    }

    /**
     * Sets whether log entries need their log versions fixed to work around
     * [#25222].
     */
    public void setFixLogVersion12Entries(boolean value) {
        fixLogVersion12Entries = value;
    }

    /* ------------------------------------------------- */
    /* ---  message classes defined in base protocol --- */
    /* ------------------------------------------------- */

    /**
     * A message containing a log entry in the replication stream.
     */
    public class Entry extends Message {

        /*
         * InputWireRecord is set when this Message had been received at this
         * node. OutputWireRecord is set when this message is created for
         * sending from this node.
         */
        final protected InputWireRecord inputWireRecord;
        protected OutputWireRecord outputWireRecord;

        public Entry(final OutputWireRecord outputWireRecord) {
            inputWireRecord = null;
            this.outputWireRecord = outputWireRecord;
        }

        @Override
        public MessageOp getOp() {
            return ENTRY;
        }

        @Override
        public ByteBuffer wireFormat() {
            final int bodySize = getWireSize();
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            writeOutputWireRecord(outputWireRecord, messageBuffer);
            messageBuffer.flip();
            return messageBuffer;
        }

        protected int getWireSize() {
            return outputWireRecord.getWireSize(streamLogVersion);
        }

        public Entry(final ByteBuffer buffer)
            throws DatabaseException {

            inputWireRecord =
                new InputWireRecord(repImpl, buffer, BaseProtocol.this);
        }

        public InputWireRecord getWireRecord() {
            return inputWireRecord;
        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder();
            sb.append(super.toString());

            if (inputWireRecord != null) {
                sb.append(" ");
                sb.append(inputWireRecord);
            }

            if (outputWireRecord != null) {
                sb.append(" ");
                sb.append(outputWireRecord);
            }

            return sb.toString();
        }

        /* For unit test support */
        @Override
        public boolean match(Message other) {

            /*
             * This message was read in, but we need to compare it to a message
             * that was sent out.
             */
            if (outputWireRecord == null) {
                outputWireRecord = new OutputWireRecord(repImpl,
                                                        inputWireRecord);
            }
            return super.match(other);
        }

        /* True if the log entry is a TxnAbort or TxnCommit. */
        public boolean isTxnEnd() {
            final byte entryType = getWireRecord().getEntryType();
            return LogEntryType.LOG_TXN_COMMIT.equalsType(entryType) ||
                   LogEntryType.LOG_TXN_ABORT.equalsType(entryType);

        }
    }

    /**
     * StartStream indicates that the replica would like the feeder to start
     * the replication stream at the proposed vlsn.
     */
    public class StartStream extends VLSNMessage {
        private final FeederFilter feederFilter;

        StartStream(VLSN startVLSN) {
            super(startVLSN);
            feederFilter = null;
        }

        StartStream(VLSN startVLSN, FeederFilter filter) {
            super(startVLSN);
            feederFilter = filter;
        }

        public StartStream(ByteBuffer buffer) {
            super(buffer);

            /* Feeder filtering not supported before protocol version 6 */
            if (getVersion() < VERSION_6) {
                feederFilter = null;
                return;
            }

            final int length = LogUtils.readInt(buffer);
            if (length == 0) {
                /* no filter is provided by client */
                feederFilter = null;
                return;
            }

            /* reconstruct filter from buffer */
            final byte filterBytes[] =
                LogUtils.readBytesNoLength(buffer, length);
            final ByteArrayInputStream bais =
                new ByteArrayInputStream(filterBytes);
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(bais);
                feederFilter = (FeederFilter) ois.readObject();
            } catch (ClassNotFoundException | IOException e) {
                logger.warning(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            } finally {
                if (ois != null) {
                    try {
                        ois.close();
                    } catch (IOException e) {
                        logger.finest("exception raised when closing the " +
                                      "object input stream object " +
                                      e.getLocalizedMessage());
                    }
                }
            }
        }

        public FeederFilter getFeederFilter() {
            return feederFilter;
        }

        @Override
        public ByteBuffer wireFormat() {
            /* Feeder filtering not supported before protocol version 6 */
            if (getVersion() < VERSION_6) {
                return super.wireFormat();
            }

            final int feederBufferSize;
            final byte[] filterBytes;

            if (feederFilter != null) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = null;
                try {
                    oos = new ObjectOutputStream(baos);
                    oos.writeObject(feederFilter);
                    oos.flush();
                } catch (IOException e) {
                    logger.warning(e.getLocalizedMessage());
                    throw new IllegalStateException(e);
                } finally {
                    if (oos != null) {
                        try {
                            oos.close();
                        } catch (IOException e) {
                            logger.finest("exception raised when closing the " +
                                          "object output stream object " +
                                          e.getLocalizedMessage());
                        }
                    }
                }
                filterBytes = baos.toByteArray();
                feederBufferSize = filterBytes.length;
            } else {
                filterBytes = null;
                feederBufferSize = 0;
            }

            /* build message buffer */
            final int bodySize = wireFormatSize() + 4 + feederBufferSize;
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            /* write 8 bytes of VLSN */
            LogUtils.writeLong(messageBuffer, vlsn.getSequence());
            /* write 4 bytes of feeder buf size */
            LogUtils.writeInt(messageBuffer, feederBufferSize);
            /* write feeder buffer */
            if (feederBufferSize > 0) {
                LogUtils.writeBytesNoLength(messageBuffer, filterBytes);
            }
            messageBuffer.flip();
            return messageBuffer;
        }

        @Override
        public MessageOp getOp() {
            return START_STREAM;
        }

        @Override
        public String toString() {
            String filterString = (feederFilter == null) ? "[no filtering]" :
                feederFilter.toString();

            return super.toString() + " " + filterString;
        }
    }

    public class Heartbeat extends Message {

        private final long masterNow;
        private final long currentTxnEndVLSN;

        public Heartbeat(long masterNow, long currentTxnEndVLSN) {
            this.masterNow = masterNow;
            this.currentTxnEndVLSN = currentTxnEndVLSN;
        }

        @Override
        public MessageOp getOp() {
            return HEARTBEAT;
        }

        @Override
        public ByteBuffer wireFormat() {
            int bodySize = 8 * 2 /* masterNow + currentTxnEndVLSN */;
            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, masterNow);
            LogUtils.writeLong(messageBuffer, currentTxnEndVLSN);
            messageBuffer.flip();
            return messageBuffer;
        }

        public Heartbeat(ByteBuffer buffer) {
            masterNow = LogUtils.readLong(buffer);
            currentTxnEndVLSN = LogUtils.readLong(buffer);
        }

        public long getMasterNow() {
            return masterNow;
        }

        public long getCurrentTxnEndVLSN() {
            return currentTxnEndVLSN;
        }

        @Override
        public String toString() {
            return super.toString() + " masterNow=" + masterNow +
                   " currentCommit=" + currentTxnEndVLSN;
        }
    }

    public class HeartbeatResponse extends Message {

        /*
         * The latest syncupVLSN. If the GlobalCBVLSN is defunct, this field
         * contains a null VLSN and is unused. If the GlobalCBVLSN is not
         * defunct:
         *  - When sent by an arbiter or subscriber, this field is null
         *    and unused.
         *  - When sent by a replica, this is the replica's local CBVLSN and
         *    is used for updating the GlobalCBVLSN on the master.
         */
        private final VLSN syncupVLSN;

        /* The latest commit/abort VLSN on the replica/arbiter/subscriber. */
        private final VLSN txnEndVLSN;

        public HeartbeatResponse(VLSN syncupVLSN, VLSN ackedVLSN) {
            super();
            this.syncupVLSN = syncupVLSN;
            this.txnEndVLSN = ackedVLSN;
        }

        public HeartbeatResponse(ByteBuffer buffer) {
            syncupVLSN = new VLSN(LogUtils.readLong(buffer));
            txnEndVLSN =
                getVersion() >= VERSION_4 ?
                    new VLSN(LogUtils.readLong(buffer)) :
                    null;
        }

        @Override
        public MessageOp getOp() {
            return HEARTBEAT_RESPONSE;
        }

        @Override
        public ByteBuffer wireFormat() {
            boolean includeTxnEndVLSN = getVersion() >= VERSION_4;
            int bodySize = includeTxnEndVLSN ?
                8 * 2 :
                8;
            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, syncupVLSN.getSequence());
            if (includeTxnEndVLSN) {
                LogUtils.writeLong(messageBuffer, txnEndVLSN.getSequence());
            }
            messageBuffer.flip();
            return messageBuffer;
        }

        public VLSN getSyncupVLSN() {
            return syncupVLSN;
        }

        public VLSN getTxnEndVLSN() {
            return txnEndVLSN;
        }

        @Override
        public String toString() {
            return super.toString() +
                " txnEndVLSN=" + txnEndVLSN +
                " syncupVLSN=" + syncupVLSN;
        }
    }

    /**
     * Message of a commit op
     */
    public class Commit extends Entry {
        private final boolean needsAck;
        private final SyncPolicy replicaSyncPolicy;

        public Commit(final boolean needsAck,
                      final SyncPolicy replicaSyncPolicy,
                      final OutputWireRecord wireRecord) {
            super(wireRecord);
            this.needsAck = needsAck;
            this.replicaSyncPolicy = replicaSyncPolicy;
        }

        @Override
        public MessageOp getOp() {
            return COMMIT;
        }

        @Override
        public ByteBuffer wireFormat() {
            final int bodySize = super.getWireSize() +
                                 1 /* needsAck */ +
                                 1 /* replica sync policy */;
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            messageBuffer.put((byte) (needsAck ? 1 : 0));
            messageBuffer.put((byte) replicaSyncPolicy.ordinal());
            writeOutputWireRecord(outputWireRecord, messageBuffer);
            messageBuffer.flip();
            return messageBuffer;
        }

        public Commit(final ByteBuffer buffer)
            throws DatabaseException {

            this(getByteNeedsAck(buffer.get()),
                 getByteReplicaSyncPolicy(buffer.get()),
                 buffer);
        }

        private Commit(final boolean needsAck,
                       final SyncPolicy replicaSyncPolicy,
                       final ByteBuffer buffer)
            throws DatabaseException {

            super(buffer);
            this.needsAck = needsAck;
            this.replicaSyncPolicy = replicaSyncPolicy;
        }

        public boolean getNeedsAck() {
            return needsAck;
        }

        public SyncPolicy getReplicaSyncPolicy() {
            return replicaSyncPolicy;
        }
    }

    /**
     * Message of an ack op
     */
    public class Ack extends Message {

        private final long txnId;

        public Ack(long txnId) {
            super();
            this.txnId = txnId;
            nAckMessages.increment();
        }

        @Override
        public MessageOp getOp() {
            return ACK;
        }

        @Override
        public ByteBuffer wireFormat() {
            int bodySize = 8;
            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, txnId);
            messageBuffer.flip();
            return messageBuffer;
        }

        public Ack(ByteBuffer buffer) {
            txnId = LogUtils.readLong(buffer);
        }

        public long getTxnId() {
            return txnId;
        }

        @Override
        public String toString() {
            return super.toString() + " txn " + txnId;
        }
    }

    /**
     * A replica node asks a feeder for the log entry at this VLSN.
     */
    public class EntryRequest extends VLSNMessage {

        private final EntryRequestType type;

        EntryRequest(VLSN matchpoint) {
            super(matchpoint);
            type = EntryRequestType.DEFAULT;
        }

        EntryRequest(VLSN matchpoint, EntryRequestType type) {
            super(matchpoint);
            this.type = type;
        }

        public EntryRequest(ByteBuffer buffer) {
            super(buffer);

            /* entry request type not supported before protocol version 7 */
            if (getVersion() < VERSION_7) {
                type = EntryRequestType.DEFAULT;
                return;
            }

            final int i = LogUtils.readInt(buffer);
            type = EntryRequestType.values()[i];
        }

        public EntryRequestType getType() {
            return type;
        }

        @Override
        public ByteBuffer wireFormat() {

            /* type not supported before protocol version 7 */
            if (getVersion() < VERSION_7) {
                return super.wireFormat();
            }

            /* build message buffer */
            final int bodySize = wireFormatSize();
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            /* write 8 bytes of VLSN */
            LogUtils.writeLong(messageBuffer, vlsn.getSequence());
            /* write 4 bytes of type */
            LogUtils.writeInt(messageBuffer, type.ordinal());
            messageBuffer.flip();
            return messageBuffer;
        }

        @Override
        public int wireFormatSize() {
            /* type not supported before protocol version 7 */
            if (getVersion() < VERSION_7) {
                return super.wireFormatSize();
            }

            return super.wireFormatSize() + 4;
        }

        @Override
        public MessageOp getOp() {
            return ENTRY_REQUEST;
        }

        @Override
        public String toString() {
            return "entry request vlsn: " + super.toString() +
                   ", type: " + type;
        }
    }

    /**
     * Type of entry request sent to feeder
     *
     * RV: VLSN requested by client
     * LOW: low end of available VLSN range in vlsn index
     * HIGH: high end of available VLSN range in vlsn index
     *
     * The DEFAULT mode is used by existing replication stream consumer e.g.
     * replica, arbiter, secondary nodes, etc, while the others are only used
     * in subscription (je.rep.subscription).
     *
     * -------------------------------------------------------------------
     *     MODE      | RV < LOW  |   RV in [LOW, HIGH] | RV > HIGH
     * -------------------------------------------------------------------
     *  DEFAULT      | NOT_FOUND |   REQUESTED ENTRY   | ALT MATCH POINT
     *  AVAILABLE    |   LOW     |   REQUESTED ENTRY   | HIGH
     *  NOW          |   HIGH    |   HIGH              | HIGH
     */
    public enum EntryRequestType {
        DEFAULT,
        AVAILABLE,
        NOW
    }

    /**
     * Response when the EntryRequest asks for a VLSN that is below the VLSN
     * range covered by the Feeder.
     */
    public class EntryNotFound extends Message {

        public EntryNotFound() {
        }

        public EntryNotFound(@SuppressWarnings("unused") ByteBuffer buffer) {
            super();
        }

        @Override
        public MessageOp getOp() {
            return ENTRY_NOTFOUND;
        }
    }

    public class AlternateMatchpoint extends Message {

        private final InputWireRecord alternateInput;
        private OutputWireRecord alternateOutput = null;

        AlternateMatchpoint(final OutputWireRecord alternate) {
            alternateInput = null;
            this.alternateOutput = alternate;
        }

        @Override
        public MessageOp getOp() {
            return ALT_MATCHPOINT;
        }

        @Override
        public ByteBuffer wireFormat() {
            final int bodySize = alternateOutput.getWireSize(streamLogVersion);
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);
            writeOutputWireRecord(alternateOutput, messageBuffer);
            messageBuffer.flip();
            return messageBuffer;
        }

        public AlternateMatchpoint(final ByteBuffer buffer)
            throws DatabaseException {
            alternateInput =
                new InputWireRecord(repImpl, buffer, BaseProtocol.this);
        }

        public InputWireRecord getAlternateWireRecord() {
            return alternateInput;
        }

        /* For unit test support */
        @Override
        public boolean match(Message other) {

            /*
             * This message was read in, but we need to compare it to a message
             * that was sent out.
             */
            if (alternateOutput == null) {
                alternateOutput =
                    new OutputWireRecord(repImpl, alternateInput);
            }
            return super.match(other);
        }
    }

    /**
     * Request from the replica to the feeder for sufficient information to
     * start a network restore.
     */
    public class RestoreRequest extends VLSNMessage {

        RestoreRequest(VLSN failedMatchpoint) {
            super(failedMatchpoint);
        }

        public RestoreRequest(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return RESTORE_REQUEST;
        }
    }

    /**
     * Response when the replica needs information to instigate a network
     * restore. The message contains a set of nodes that could be used as the
     * basis for a NetworkBackup so that the request node can become current
     * again.
     *
     * <p>In addition, when support is needed for older replica nodes that use
     * the GlobalCBVLSN, a vlsn is included. See
     * {@link GlobalCBVLSN#getRestoreResponseVLSN}.</p>
     */
    public class RestoreResponse extends SimpleMessage {
        /* Is null VLSN and unused if the GlobalCBVLSN is defunct. */
        private final VLSN cbvlsn;

        private final RepNodeImpl[] logProviders;

        public RestoreResponse(VLSN cbvlsn, RepNodeImpl[] logProviders) {
            this.cbvlsn = cbvlsn;
            this.logProviders = logProviders;
        }

        public RestoreResponse(ByteBuffer buffer) {
            long vlsnSequence = LogUtils.readLong(buffer);
            cbvlsn = new VLSN(vlsnSequence);
            logProviders = getRepNodeImplArray(buffer);
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(cbvlsn.getSequence(), logProviders);
        }

        /* Add support for RepNodeImpl arrays. */

        @Override
        protected void putWireFormat(final ByteBuffer buffer,
                                     final Object obj) {
            if (obj.getClass() == RepNodeImpl[].class) {
                putRepNodeImplArray(buffer, (RepNodeImpl[]) obj);
            } else {
                super.putWireFormat(buffer, obj);
            }
        }

        @Override
        protected int wireFormatSize(final Object obj) {
            if (obj.getClass() == RepNodeImpl[].class) {
                return getRepNodeImplArraySize((RepNodeImpl[]) obj);
            }
            return super.wireFormatSize(obj);
        }

        private void putRepNodeImplArray(final ByteBuffer buffer,
                                         final RepNodeImpl[] ra) {
            LogUtils.writeInt(buffer, ra.length);
            final int groupFormatVersion = getGroupFormatVersion();
            for (final RepNodeImpl node : ra) {
                putByteArray(
                    buffer,
                    RepGroupImpl.serializeBytes(node, groupFormatVersion));
            }
        }

        private RepNodeImpl[] getRepNodeImplArray(final ByteBuffer buffer) {
            final RepNodeImpl[] ra = new RepNodeImpl[LogUtils.readInt(buffer)];
            final int groupFormatVersion = getGroupFormatVersion();
            for (int i = 0; i < ra.length; i++) {
                ra[i] = RepGroupImpl.deserializeNode(
                    getByteArray(buffer), groupFormatVersion);
            }
            return ra;
        }

        private int getRepNodeImplArraySize(RepNodeImpl[] ra) {
            int size = 4; /* array length */
            final int groupFormatVersion = getGroupFormatVersion();
            for (final RepNodeImpl node : ra) {
                size += (4 /* Node size */ +
                         RepGroupImpl.serializeBytes(node, groupFormatVersion)
                             .length);
            }
            return size;
        }

        /**
         * Returns the RepGroupImpl version to use for the currently configured
         * protocol version.
         */
        private int getGroupFormatVersion() {
            return (getVersion() < VERSION_5) ?
                RepGroupImpl.FORMAT_VERSION_2 :
                RepGroupImpl.MAX_FORMAT_VERSION;
        }

        @Override
        public MessageOp getOp() {
            return RESTORE_RESPONSE;
        }

        RepNodeImpl[] getLogProviders() {
            return logProviders;
        }

        VLSN getCBVLSN() {
            return cbvlsn;
        }
    }

    /**
     * Message used to shutdown a node
     */
    public class ShutdownRequest extends SimpleMessage {
        /* The time that the shutdown was initiated on the master. */
        private final long shutdownTimeMs;

        public ShutdownRequest(long shutdownTimeMs) {
            super();
            this.shutdownTimeMs = shutdownTimeMs;
        }

        @Override
        public MessageOp getOp() {
            return SHUTDOWN_REQUEST;
        }

        public ShutdownRequest(ByteBuffer buffer) {
            shutdownTimeMs = LogUtils.readLong(buffer);
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(shutdownTimeMs);
        }

        public long getShutdownTimeMs() {
            return shutdownTimeMs;
        }
    }

    /**
     * Message in response to a shutdown request.
     */
    public class ShutdownResponse extends Message {

        public ShutdownResponse() {
            super();
        }

        @Override
        public MessageOp getOp() {
            return SHUTDOWN_RESPONSE;
        }

        public ShutdownResponse(@SuppressWarnings("unused") ByteBuffer buffer) {
        }
    }

    public class GroupAck extends Message {

        private final long txnIds[];

        public GroupAck(long txnIds[]) {
            super();
            this.txnIds = txnIds;
            nGroupAckMessages.increment();
            nGroupedAcks.add(txnIds.length);
            nMaxGroupedAcks.setMax(txnIds.length);
        }

        @Override
        public MessageOp getOp() {
            return GROUP_ACK;
        }

        @Override
        public ByteBuffer wireFormat() {

            final int bodySize = 4 + 8 * txnIds.length;
            final ByteBuffer messageBuffer =
                allocateInitializedBuffer(bodySize);

            putLongArray(messageBuffer, txnIds);
            messageBuffer.flip();

            return messageBuffer;
        }

        public GroupAck(ByteBuffer buffer) {
            txnIds = readLongArray(buffer);
        }

        public long[] getTxnIds() {
            return txnIds;
        }

        @Override
        public String toString() {
            return super.toString() + " txn " + Arrays.toString(txnIds);
        }
    }

    /**
     * Base class for messages which contain only a VLSN
     */
    protected abstract class VLSNMessage extends Message {
        protected final VLSN vlsn;

        VLSNMessage(VLSN vlsn) {
            super();
            this.vlsn = vlsn;
        }

        public VLSNMessage(ByteBuffer buffer) {
            long vlsnSequence = LogUtils.readLong(buffer);
            vlsn = new VLSN(vlsnSequence);
        }

        @Override
        public ByteBuffer wireFormat() {
            int bodySize = wireFormatSize();
            ByteBuffer messageBuffer = allocateInitializedBuffer(bodySize);
            LogUtils.writeLong(messageBuffer, vlsn.getSequence());
            messageBuffer.flip();
            return messageBuffer;
        }

        int wireFormatSize() {
            return 8;
        }

        VLSN getVLSN() {
            return vlsn;
        }

        @Override
        public String toString() {
            return super.toString() + " " + vlsn;
        }
    }

    /**
     * Base class for all protocol handshake messages.
     */
    protected abstract class HandshakeMessage extends SimpleMessage {
    }

    /**
     * Version broadcasts the sending node's protocol version.
     */
    protected abstract class ProtocolVersion extends HandshakeMessage {
        private final int version;

        @SuppressWarnings("hiding")
        private final NameIdPair nameIdPair;

        public ProtocolVersion(int version) {
            super();
            this.version = version;
            this.nameIdPair = BaseProtocol.this.nameIdPair;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(version, nameIdPair);
        }

        public ProtocolVersion(ByteBuffer buffer) {
            version = LogUtils.readInt(buffer);
            nameIdPair = getNameIdPair(buffer);
        }

        /**
         * @return the version
         */
        protected int getVersion() {
            return version;
        }

        /**
         * The nodeName of the sender
         *
         * @return nodeName
         */
        protected NameIdPair getNameIdPair() {
            return nameIdPair;
        }
    }

    /* ---------------------------------------- */
    /* ---  end of message class definition --- */
    /* ---------------------------------------- */

    /**
     * Write an entry output wire record to the message buffer using the write
     * log version format and increment nEntriesWrittenOldVersion if the entry
     * format was changed.
     */
    protected void writeOutputWireRecord(final OutputWireRecord record,
                                         final ByteBuffer messageBuffer) {
        final boolean changedFormat =
            record.writeToWire(messageBuffer, streamLogVersion);
        if (changedFormat) {
            nEntriesWrittenOldVersion.increment();
        }
    }

    /**
     * Initializes message ops and check if is valid within allocated range
     *
     * @param protocolOps  ops to be initialized
     * @param checkValidity true if check validity of op code
     */
    private void initializeMessageOps(MessageOp[] protocolOps,
                                      boolean checkValidity) {

        if (checkValidity) {
            /* Check if op code is valid before initialization */
            for (MessageOp op : protocolOps) {
                if (!isValidMsgOpCode(op.getOpId())) {
                    throw EnvironmentFailureException.unexpectedState
                        ("Op id: " + op.getOpId() +
                         " is out of allowed range inclusively [" +
                         MIN_MESSAGE_OP_CODE_IN_SUBCLASS + ", " +
                         MAX_MESSAGE_OP_CODE_IN_SUBCLASS + "]");
                }
            }
        }
        initializeMessageOps(protocolOps);
    }

    /**
     * Returns whether the byte value specifies that an acknowledgment is
     * needed.
     */
    private static boolean getByteNeedsAck(final byte needsAckByte) {
        switch (needsAckByte) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw EnvironmentFailureException.unexpectedState(
                    "Invalid bool ordinal: " + needsAckByte);
        }
    }

    /** Checks if op code defined in subclass fall in pre-allocated range */
    private static boolean isValidMsgOpCode(short opId) {

        return (opId <= MAX_MESSAGE_OP_CODE_IN_SUBCLASS) &&
               (opId >= MIN_MESSAGE_OP_CODE_IN_SUBCLASS);
    }

    /** Returns the sync policy specified by the argument. */
    private static SyncPolicy getByteReplicaSyncPolicy(
        final byte syncPolicyByte) {

        for (final SyncPolicy p : SyncPolicy.values()) {
            if (p.ordinal() == syncPolicyByte) {
                return p;
            }
        }
        throw EnvironmentFailureException.unexpectedState(
            "Invalid sync policy ordinal: " + syncPolicyByte);
    }

    /* Writes array of longs into buffer */
    private void putLongArray(ByteBuffer buffer, long[] la) {
        LogUtils.writeInt(buffer, la.length);

        for (long l : la) {
            LogUtils.writeLong(buffer, l);
        }
    }

    /* Reads array of longs from buffer */
    private long[] readLongArray(ByteBuffer buffer) {
        final long la[] = new long[LogUtils.readInt(buffer)];

        for (int i = 0; i < la.length; i++) {
            la[i] = LogUtils.readLong(buffer);
        }

        return la;
    }
}
