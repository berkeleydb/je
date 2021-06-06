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

package com.sleepycat.je.rep.utilint;

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_BYTES_READ;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_BYTES_WRITTEN;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_ENTRIES_WRITTEN_OLD_VERSION;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_BATCHED;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_READ;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_WRITTEN;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGE_BATCHES;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_READ_NANOS;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_WRITE_NANOS;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.StringUtils;

/**
 * Supplies the basic infrastructure for BinaryProtocols used to exchange
 * messages by the replications stream and during network based restore
 * operations.
 *
 * Note that this class and its subclasses are not synchronized. There must be
 * one instance of this class per thread of control.
 *
 * IMPORTANT: Please avoid all uses of ByteBuffer.get/put when serializing
 * message fields of types: long, int and short to avoid byte order issues.
 * Use LogUtils.read/write methods instead, since they use a single canonical
 * byte-independent representation.
 */
public abstract class BinaryProtocol {

    protected static final int MESSAGE_HEADER_SIZE =
        2 /* Message op id (short) */ +
        4 /* message size (int) */;

    /* Buffer reused to process the header of every message. */
    protected final ByteBuffer header =
        ByteBuffer.allocate((MESSAGE_HEADER_SIZE));

    /* The version as implemented by the actual code. */
    protected final int codeVersion;

    /* The version that this instance is actually configured to use. */
    /* It's not final to facilitate testing */
    protected int configuredVersion;

    /* Identifies the node using this protocol. */
    protected final NameIdPair nameIdPair;

    /* Maps the message op id to its canonical descriptor instance. */
    private final Map<Short, MessageOp> ops = new HashMap<Short, MessageOp>();
    private final int predefinedMessageCount;

    /* The max message size which will be accepted. */
    private final long maxMessageSize;

    /* Whether to use UTF8 or default encoding for Strings. */
    private final boolean useStringDefaultEncoding;

    /*
     * The predefined messages. IMPORTANT: Note that predefined message ops
     * start at 1000. to stay out of the way of subtype ops.
     */
    public final MessageOp CLIENT_VERSION =
        new MessageOp((short) 1001, ClientVersion.class);

    public final MessageOp SERVER_VERSION =
        new MessageOp((short) 1002, ServerVersion.class);

    public final MessageOp INCOMPATIBLE_VERSION =
        new MessageOp((short) 1003, IncompatibleVersion.class);

    public final MessageOp PROTOCOL_ERROR =
        new MessageOp((short) 1004, ProtocolError.class);

    /* Statistics definition. */
    protected final StatGroup stats;
    protected final LongStat nReadNanos;
    protected final LongStat nWriteNanos;
    protected final LongStat nBytesRead;
    protected final LongStat nMessagesRead;
    protected final LongStat nBytesWritten;
    protected final LongStat nMessagesWritten;
    protected final LongStat nMessagesBatched;
    protected final LongStat nMessageBatches;
    protected final LongStat nEntriesWrittenOldVersion;

    protected final Logger logger;
    protected final Formatter formatter;
    protected final EnvironmentImpl envImpl;

    /**
     * Returns a Protocol object configured that implements the specified
     * (supported) version.
     *
     * @param codeVersion the version actually implemented by the protocol.
     * @param configuredVersion the version of the protocol that must be
     * implemented/simulated by this protocol when communicating with the
     * recipient.
     */
    protected BinaryProtocol(NameIdPair nameIdPair,
                             int codeVersion,
                             int configuredVersion,
                             EnvironmentImpl envImpl) {
        this.nameIdPair = nameIdPair;
        this.codeVersion = codeVersion;
        this.configuredVersion = configuredVersion;
        this.envImpl = envImpl;

        if (envImpl != null) {
            this.logger = LoggerUtils.getLogger(getClass());
        } else {
            this.logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        }
        this.formatter = new ReplicationFormatter(nameIdPair);

        stats = new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                              BinaryProtocolStatDefinition.GROUP_DESC);
        nReadNanos = new LongStat(stats, N_READ_NANOS);
        nWriteNanos = new LongStat(stats, N_WRITE_NANOS);
        nBytesRead = new LongStat(stats, N_BYTES_READ);
        nMessagesRead = new LongStat(stats, N_MESSAGES_READ);
        nBytesWritten = new LongStat(stats, N_BYTES_WRITTEN);
        nMessagesWritten = new LongStat(stats, N_MESSAGES_WRITTEN);
        nMessagesBatched = new LongStat(stats, N_MESSAGES_BATCHED);
        nMessageBatches = new LongStat(stats, N_MESSAGE_BATCHES);
        nEntriesWrittenOldVersion =
            new LongStat(stats, N_ENTRIES_WRITTEN_OLD_VERSION);

        /* Initialize with the pre-defined protocol messages. */
        for (MessageOp op :
            new MessageOp[] { CLIENT_VERSION,
                              SERVER_VERSION,
                              INCOMPATIBLE_VERSION,
                              PROTOCOL_ERROR }) {

            if (ops.put(op.opId, op) != null) {
                throw EnvironmentFailureException.unexpectedState
                    ("Duplicate op: " + op.opId);
            }
        }
        predefinedMessageCount = ops.size();
        if (envImpl != null) {
            DbConfigManager configManager = envImpl.getConfigManager();
            long mMSz =
                configManager.getLong(RepParams.MAX_MESSAGE_SIZE);
            maxMessageSize = (mMSz == 0) ?
                (envImpl.getMemoryBudget().getMaxMemory() >> 1) :
                mMSz;
            useStringDefaultEncoding = configManager.getBoolean
                (RepParams.PROTOCOL_OLD_STRING_ENCODING);
        } else {
            /* Some unit tests pass in null EnvImpl. */
            maxMessageSize = 1 << 20;
            useStringDefaultEncoding = true;
        }
    }

    /*
     * Must be invoked after the constructor has completed, to get around
     * base/subclass initialization dependences; MessageOps are instances of
     * nested classes declared in the subclass.
     */
    protected void initializeMessageOps(MessageOp[] protocolOps) {

        for (MessageOp op : protocolOps) {
            if (ops.put(op.opId, op) != null) {
                throw EnvironmentFailureException.unexpectedState
                    ("Duplicate op: " + op.opId);
            }
        }
    }

    /*
     * The total number of messages defined by the protocol. Includes messages
     * defined by the subclass.
     */
    public int messageCount() {
        return ops.size();
    }

    /* The messages defined in this class. */
    final public int getPredefinedMessageCount() {
        return predefinedMessageCount;
    }

    /**
     * Returns the version associated with this protocol instance. Request
     * message generated by this instance conform to this version and responses
     * are expected to conform to this version as well.
     *
     * @return the version that is actually being used.
     */
    public int getVersion() {
        return configuredVersion;
    }

    public StatGroup getStats(StatsConfig config) {
        StatGroup ret = stats.cloneGroup(config.getClear());

        return ret;
    }

    public void resetStats() {
        stats.clear();
    }

    /* Messages <= this size will use the shared buffer. */
    private static int CACHED_BUFFER_SIZE = 0x4000;

    /*
     * The shared read and write buffers that are reused. There are
     * two buffers, so that reading and writing can proceed in parallel.
     */
    private final ByteBuffer cachedReadBuffer =
        ByteBuffer.allocate(CACHED_BUFFER_SIZE);

    private final ByteBuffer cachedWriteBuffer =
            ByteBuffer.allocate(CACHED_BUFFER_SIZE);

    /**
     * Returns a read buffer of the requested size.
     *
     * @param size the size of the requested buffer in bytes
     * @return the requested
     */
    private ByteBuffer allocateReadBuffer(int size) {
        if (size <= CACHED_BUFFER_SIZE ) {
            cachedReadBuffer.rewind();
            cachedReadBuffer.limit(size);
            return cachedReadBuffer;
        }
        return ByteBuffer.allocate(size);
    }

    /**
     * Returns a write buffer of the requested size.
     *
     * @param size the size of the requested buffer in bytes
     * @return the requested
     */
    private ByteBuffer allocateWriteBuffer(int size) {
        if(size <= CACHED_BUFFER_SIZE ) {
            cachedWriteBuffer.rewind();
            cachedWriteBuffer.limit(size);
            return cachedWriteBuffer;
        }
        return ByteBuffer.allocate(size);
    }

    /* Returns byte size of serialized string. */
    public int stringSize(String s) {
        return stringToBytes(s).length + 4;
    }

    /* Serialize the string into the buffer. */
    public void putString(String s, ByteBuffer buffer) {
        byte[] b = stringToBytes(s);
        LogUtils.writeInt(buffer, b.length);
        buffer.put(b);
    }

    /**
     * Reconstitutes the string serialized by the above method.
     *
     * @param buffer the buffer containing the string
     *
     * @return the de-serialized string
     */
    public String getString(ByteBuffer buffer) {
        int length = LogUtils.readInt(buffer);
        byte b[] = new byte[length];
        buffer.get(b);
        return bytesToString(b);
    }

    /**
     * Responsible for converting a String to an encoded value for all
     * protocols.
     * <p>
     * In JE 5.0.36 and earlier, only the default encoding was supported.  In
     * later releases, a config param was added to force UTF-8 to be used.  In
     * JE 5.1 and later, the default will be to use UTF-8.  [#20967]
     *
     * @see ReplicationConfig#PROTOCOL_OLD_STRING_ENCODING
     */
    private byte[] stringToBytes(String s) {
        if (useStringDefaultEncoding) {
            return s.getBytes();
        }
        return StringUtils.toUTF8(s);
    }

    /**
     * Responsible for converting an encoded value to a String for all
     * protocols.
     *
     * @see #stringToBytes
     */
    private String bytesToString(byte[] b) {
        if (useStringDefaultEncoding) {
            return new String(b);
        }
        return StringUtils.fromUTF8(b);
    }

    /**
     * The Operations that are part of the protocol.
     */
    public static class MessageOp {

        /* The string denoting the operation for the request message. */
        private final short opId;

        /* The class used to represent the message. */
        private final Class<? extends Message> messageClass;
        /* The constructor used to create message instances. */
        private Constructor<? extends Message> constructor;

        /* The label is used for debugging purposes. */
        private final String label;

        public MessageOp(short opId, Class<? extends Message> messageClass) {
            this.opId = opId;
            this.messageClass = messageClass;
            this.label = messageClass.getSimpleName();
            try {
                constructor = messageClass.getConstructor
                    (messageClass.getEnclosingClass(), ByteBuffer.class);
            } catch (SecurityException e) {
                throw EnvironmentFailureException.unexpectedException(e);
            } catch (NoSuchMethodException e) {
                throw EnvironmentFailureException.unexpectedException(e);
            }
        }

        public short getOpId() {
            return opId;
        }

        Class<? extends Message> getMessageClass() {
            return messageClass;
        }

        public Constructor<? extends Message> getConstructor() {
            return constructor;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * Returns the Op from a message buffer. It's always the first item in the
     * buffer. Leaves the message buffer positioned after the Op.
     *
     * @param messageBuffer a message buffer the the protocol
     * @return the OpId
     */
    private MessageOp getOp(ByteBuffer messageBuffer) {
        short opId = LogUtils.readShort(messageBuffer);
        final MessageOp messageOp = ops.get(opId);

        if (messageOp == null) {
            throw EnvironmentFailureException.unexpectedState
                (envImpl,
                 "Unknown message op id:" + opId +
                 " Known op ids:" + Arrays.toString(ops.keySet().toArray()));
        }
        return messageOp;
    }

    /*
     * Used to indicate that an entity is formatable and can be serialized and
     * de-serialized.
     */
    interface WireFormatable {

        /*
         * Returns a ByteBuffer holding the message in a representation
         * suitable for use in a network transmission. The buffer is flipped
         * and ready for relative reads.
         */
        abstract ByteBuffer wireFormat();
    }

    /**
     * Fills a dedicated empty buffer with bytes read from the channel. It
     * flips the buffer after it has been filled, so it's ready for reading.
     *
     * @param channel the channel to be read
     * @param buffer the buffer to be filled
     * @throws IOException if the errors were encountered while reading from
     * the channel or the buffer could not be filled with the expected number
     * of bytes
     */
    private void fillBuffer(ReadableByteChannel channel,
                            ByteBuffer buffer)
        throws IOException {

        final long start = System.nanoTime();
        assert(buffer.position() == 0);
        while (buffer.position() < buffer.limit()) {
            int numRead = channel.read(buffer);
            if (numRead <= 0) {
                throw new IOException("Expected bytes: " + buffer.limit() +
                                      " read bytes: " + buffer.position());
            }
        }
        nReadNanos.add(System.nanoTime() - start);
        buffer.flip();
    }

    /**
     * Read and parse an incoming message, specifying the incoming version.
     *
     * @param channel the channel to read from. Declared as a
     * ReadableByteChannel rather than the more obvious SocketChannel to
     * facilitate unit testing.
     *
     * @throws IOException
     */
    public Message read(ReadableByteChannel channel)
        throws IOException {

        /* Get the message header. */
        fillBuffer(channel, header);

        /* Use the type value to determine the message type. */
        MessageOp op = getOp(header);
        try {
            Constructor<? extends Message> cons = op.getConstructor();

            /* Read the size to determine the body of the message. */
            int messageBodySize = LogUtils.readInt(header);
            nBytesRead.add(MESSAGE_HEADER_SIZE + messageBodySize);
            nMessagesRead.increment();
            if (messageBodySize > 0) {
                if (messageBodySize > maxMessageSize) {
                    throw EnvironmentFailureException.unexpectedState
                        ("Message op: " + op + " Body size: " +
                         messageBodySize + " is too large.  maxSizeAllowed: " +
                         maxMessageSize +
                         "\nIf a larger value is needed, set the " +
                         "'je.rep.maxMessageSize' parameter.");
                }

                ByteBuffer body = allocateReadBuffer(messageBodySize);
                fillBuffer(channel, body);
                return cons.newInstance(this, body);
            }

            if (messageBodySize < 0) {
                throw EnvironmentFailureException.unexpectedState
                    ("Message op: " + op + " Body size: " + messageBodySize);
            }
            /* No body */
            return cons.newInstance(this, null);
        } catch (InstantiationException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (IllegalAccessException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (SecurityException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (InvocationTargetException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } finally {
            /* The header buffer will be reused, so clear it. */
            header.clear();
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T read(ReadableByteChannel channel, Class<T> cl)
        throws IOException, ProtocolException {

        Message message = read(channel);

        /*
         * Note: the subclassing check instead of an equality check makes it
         * convenient to deal with responses when there can be multiple
         * possible but related responses.
         */
        if (cl.isAssignableFrom(message.getClass())) {
            return (T)message;
        }
        throw new ProtocolException(message, cl);
    }

    /**
     * Write a message out to a channel.
     * @throws IOException
     */
    public void write(Message message, NamedChannel namedChannel)
        throws IOException {

        write(message, namedChannel, namedChannel.getNameIdPair());
    }

    /**
     * Write a message out to a channel.
     * @throws IOException
     */
    public void write(Message message, WritableByteChannel channel)
        throws IOException {

        write(message, channel, NameIdPair.NULL);
    }

    /**
     * Write a message out to a channel.
     * @throws IOException
     */
    private void write(Message message,
                       WritableByteChannel channel,
                       NameIdPair destinationNameIdPair)
        throws IOException {

        final ByteBuffer messageBuffer = message.wireFormat();

        nMessagesWritten.increment();

        flushBuffer(channel, messageBuffer);

        if (logger.isLoggable(Level.FINER)) {
            if (destinationNameIdPair == NameIdPair.NULL) {
                /* No nameIdPair was supplied, so use the channel. */
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINER,
                                   "Sent " + message + " to " + channel);
            } else {
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINER,
                                   "Sent to " +
                                   destinationNameIdPair.getName() +
                                   ": "+  message);
            }
        }

        /* Rewind the message buffer in case it's a reusable wire format */
        messageBuffer.rewind();
    }

    /**
     * Buffers the serialized form of the message (if possible) for later
     * writes to the network.
     *
     * If buffering the message would result in an overflow, the current
     * contents of the buffer are flushed before the message is added to the
     * buffer. If the size of the message exceeds the size of the buffer, the
     * message is flushed directly to the network.
     *
     * @param channel the channel to which the buffer is flushed on buffer
     * overflows.
     *
     * @param batchWriteBuffer the buffer accumulating the serialized messages
     * It's best for performance if this is a direct byte buffer, to avoid
     * another copy when the buffer is finally flushed.
     *
     * @param nMessages the number of messages currently in the buffer,
     * including this message, that is, nMessages is always > 0 upon entry.
     *
     * @param message the message to be buffered.
     *
     * @return the number of messages currently in the buffer after accounting
     * for potential buffer flushes.
     */
    public int bufferWrite(WritableByteChannel channel,
                           ByteBuffer batchWriteBuffer,
                           int nMessages,
                           Message message)
        throws IOException {

        assert nMessages > 0 ;

        final ByteBuffer messageBuffer = message.wireFormat();

        if (batchWriteBuffer.remaining() < messageBuffer.limit()) {

            flushBufferedWrites(channel, batchWriteBuffer, nMessages - 1);
            /* 1 for the message we add below. */
            nMessages = 1;

            if (batchWriteBuffer.remaining() < messageBuffer.limit()) {
                /*
                 * Buffer is too small for message, so write it directly.
                 * This write must always be preceded by a buffer flush.
                 */
                assert batchWriteBuffer.position() == 0 ;
                nMessagesWritten.increment();
                flushBuffer(channel, messageBuffer);
                nMessages = 0;
                return nMessages;
            }
        }

        batchWriteBuffer.put(messageBuffer);
        return nMessages;
    }

    /**
     * Flush all the messages accumulated by earlier calls to bufferWrite
     *
     * @param channel the channel to which the buffer is flushed.
     *
     * @param batchWriteBuffer the buffer containing the accumulated messages
     *
     * @param nMessages the number of messages in the batch
     */
    public void flushBufferedWrites(WritableByteChannel channel,
                                    ByteBuffer batchWriteBuffer,
                                    int nMessages)
        throws IOException {

        nMessagesWritten.add(nMessages);

        if (nMessages > 1) {
            nMessagesBatched.add(nMessages);
            nMessageBatches.increment();
        }

        batchWriteBuffer.flip();
        flushBuffer(channel, batchWriteBuffer);
        batchWriteBuffer.clear();

        return;
    }

    /**
     * Writes the entire contents of the buffer to the blocking channel.
     */
    private void flushBuffer(WritableByteChannel channel,
                             ByteBuffer bb)
        throws IOException {

        assert bb.position() == 0;

        if (bb.limit() == 0) {
            return;
        }

        final long start = System.nanoTime();

        /**
         * Even though the underlying channel is a blocking channel, there are
         * circumstances where the channel.write() returns without writing all
         * of the buffer, in seeming contradiction to the javadoc. One such
         * example is when the peer fails in the middle of a write; in this
         * case there is a "successful" partial write and the next write will
         * result in an I/O exception. There may be other such examples as
         * well, hence the defensive write loop below which tries to continue
         * writes to their logical conclusion or to an IOE.
         */
        while (bb.remaining() > 0) {
            int bytes = channel.write(bb);
            nBytesWritten.add(bytes);
            if (bytes == 0) {

                /*
                 * This should not happen since it's a blocking channel, but
                 * the java doc is vague on this subject, so yield control if
                 * we are not making progress.
                 */
                Thread.yield();
            }
        }

        nWriteNanos.add(System.nanoTime() - start);
    }

    /**
     * Base message class for all messages exchanged in the protocol.
     * Serialized layout of a message: - opType (short) - size of the body of
     * the message (int) - body of the message - message specific fields
     * <p>
     * All non-abstract subclasses must implement a constructor with a
     * ByteBuffer argument. This constructor is used during serialization to
     * recreate the Message instance. It's considered good practice to declare
     * all attributes of a message as final. It's a simple way to ensure that
     * the above constructor has initialized all the attributes of the message.
     */
    public abstract class Message implements WireFormatable {

        public abstract MessageOp getOp();

        /**
         * The default message consists of the operation enum and just a 0
         * length size.
         */
        @Override
        public ByteBuffer wireFormat() {
            ByteBuffer messageBuffer = allocateInitializedBuffer(0);
            messageBuffer.flip();
            return messageBuffer;
        }

        @Override
        public String toString() {
            return getOp().toString();
        }

        /*
         * For unit test support, so we can compare a message created for
         * sending against a message received. Some message types need to
         * override.
         */
        public boolean match(Message other) {
            return Arrays.equals(wireFormat().array().clone(),
                                 other.wireFormat().array().clone());
        }

        /**
         * Allocate a buffer for the message with the header initialized.
         *
         * @param size size of the message contents following the buffer
         *
         * @return the initialized buffer
         */
        public ByteBuffer allocateInitializedBuffer(int size) {
            ByteBuffer messageBuffer =
                allocateWriteBuffer(MESSAGE_HEADER_SIZE + size);
            LogUtils.writeShort(messageBuffer, getOp().getOpId());
            LogUtils.writeInt(messageBuffer, size);
            return messageBuffer;
        }
    }

    /**
     * Base class for simple messages. Ones where performance is not of the
     * utmost importance and reflection can be used to simplify message
     * serialization and de-serialization.
     */
    protected abstract class SimpleMessage extends Message {

        /**
         * Assembles a sequence of arguments into its byte based wire format.
         * The method does the serialization in two steps. In step 1, it
         * calculates the length of the buffer. In step 2, it assembles the
         * bytes into the allocated buffer. The interpretive approach used here
         * is suitable for the low performance requirements of the handshake
         * protocol, but not for the core data stream itself. It's for this
         * reason that the method is associated with the Handshake message
         * class and not the Message class.
         *
         * @param arguments the arguments to be passed in the message
         *
         * @return a byte buffer containing the serialized form of the message
         */
        protected ByteBuffer wireFormat(Object... arguments) {
            int size = 0;
            for (final Object obj : arguments) {
                size += wireFormatSize(obj);
            }

            /* Allocate the buffer and fill it up */
            final ByteBuffer buffer = allocateInitializedBuffer(size);

            for (final Object obj : arguments) {
                putWireFormat(buffer, obj);
            }
            buffer.flip();
            return buffer;
        }

        /**
         * Put the bytes of the wire format for the object into the current
         * position in the buffer.
         *
         * @param buffer the buffer
         * @param obj the object
         * @throws EnvironmentFailureException if the object is not supported
         */
        protected void putWireFormat(final ByteBuffer buffer,
                                     final Object obj) {
            final Class<?> cl = obj.getClass();
            if (cl == Long.class) {
                LogUtils.writeLong(buffer, ((Long)obj).longValue());
            } else if (cl == Integer.class) {
                LogUtils.writeInt(buffer, ((Integer)obj).intValue());
            } else if (cl == Short.class) {
                LogUtils.writeShort(buffer, ((Short)obj).shortValue());
            } else if (cl == Byte.class) {
                buffer.put(((Byte)obj).byteValue());
            }  else if (cl == Boolean.class) {
                buffer.put(((Boolean)obj).booleanValue() ?
                            (byte)1 :
                            (byte)0);
            } else if (cl == VLSN.class) {
                LogUtils.writeLong(buffer, ((VLSN)obj).getSequence());
            } else if (Enum.class.isAssignableFrom(cl)) {
                /* An enum is stored as it's identifier string. */
                Enum<?> e = (Enum<?>)obj;
                putString(e.name(), buffer);
            } else if (cl == String.class) {

                /*
                 * A string is stored with its length followed by its
                 * contents.
                 */
                putString((String)obj, buffer);
            } else if (cl == Double.class) {
                /* Treat a Double as a String. */
                putString(((Double) obj).toString(), buffer);
            } else if (cl == String[].class) {
                String sa[] = (String[])obj;
                LogUtils.writeInt(buffer, sa.length);
                for (String element : sa) {
                    putString(element, buffer);
                }
            } else if (cl == byte[].class) {
                putByteArray(buffer, (byte[])obj);
            } else if (obj instanceof NameIdPair) {
                /* instanceof used to accomodate ReadOnlyNameIdPair. */
                ((NameIdPair) obj).serialize(buffer, BinaryProtocol.this);
            } else {
                throw EnvironmentFailureException.unexpectedState(
                    "Unknown type: " + cl);
            }
        }

        /**
         * Put the bytes for the wire format of the specified byte array into
         * the current position in the buffer.
         *
         * @param buffer the buffer
         * @param ba the byte array
         */
        protected void putByteArray(ByteBuffer buffer, byte[] ba) {
            LogUtils.writeInt(buffer, ba.length);
            buffer.put(ba);
        }

        /**
         * Return the wire format size of the specified object.
         *
         * @param obj the object
         * @return the size
         * @throws EnvironmentFailureException if the object is not supported
         */
        protected int wireFormatSize(final Object obj) {
            final Class<?> cl = obj.getClass();
            if (cl == Long.class) {
                return 8;
            } else if (cl == Integer.class) {
                return 4;
            } else if (cl == Short.class) {
                return 2;
            } else if (cl == Byte.class) {
                return 1;
            } else if (cl == Boolean.class) {
                return 1;
            } else if (cl == VLSN.class) {
                return 8;
            } else if (Enum.class.isAssignableFrom(cl)) {
                return stringSize(((Enum<?>)obj).name());
            } else if (cl == String.class) {
               return stringSize((String)obj);
            } else if (cl == Double.class) {
               return stringSize(((Double) obj).toString());
            } else if (cl == String[].class) {
                int size = 4; /* array length */
                final String sa[] = (String[])obj;
                for (String element : sa) {
                    size += stringSize(element);
                }
                return size;
            } else if (cl == byte[].class) {
                return 4 + ((byte[])obj).length;
            } else if (obj instanceof NameIdPair) {
                /* instanceof used to accomodate ReadOnlyNameIdPair. */
                return ((NameIdPair) obj).serializedSize(BinaryProtocol.this);
            } else {
                throw EnvironmentFailureException.unexpectedState
                    ("Unknown type: " + cl);
            }
        }

        /**
         * Reconstitutes an array of strings.
         * @param buffer
         * @return
         */
        protected String[] getStringArray(ByteBuffer buffer) {
            String sa[] = new String[LogUtils.readInt(buffer)];
            for (int i=0; i < sa.length; i++) {
                sa[i] = getString(buffer);
            }
            return sa;
        }

        protected byte[] getByteArray(ByteBuffer buffer) {
            byte ba[] = new byte[LogUtils.readInt(buffer)];
            buffer.get(ba);
            return ba;
        }

        protected boolean getBoolean(ByteBuffer buffer) {
            byte b = buffer.get();
            if (b == 0) {
                return false;
            } else if (b == 1) {
                return true;
            } else {
                throw EnvironmentFailureException.unexpectedState
                    ("Unknown boolean value: " + b);
            }
        }

        protected VLSN getVLSN(ByteBuffer buffer) {
            long vlsn = LogUtils.readLong(buffer);
            return (vlsn == VLSN.NULL_VLSN_SEQUENCE) ?
                 VLSN.NULL_VLSN :
                 new VLSN(vlsn);
        }

        protected <T extends Enum<T>> T getEnum(Class<T> enumType,
                                                ByteBuffer buffer) {
            String enumName = getString(buffer);
            return Enum.valueOf(enumType, enumName);
        }

        protected Double getDouble(ByteBuffer buffer) {
            return new Double(getString(buffer));
        }

        protected NameIdPair getNameIdPair(ByteBuffer buffer) {
            return NameIdPair.deserialize(buffer, BinaryProtocol.this);
        }
    }

    /**
     * The base class for reject responses to requests
     */
    public abstract class RejectMessage extends SimpleMessage {
        /* Must not be null */
        protected String errorMessage;

        protected RejectMessage(String errorMessage) {
            super();

            /*
             * Replace the null message with an empty string since the Simple
             * Message assumes non-null contents.
             */
            if (errorMessage == null) {
                this.errorMessage = " ";
            } else {
                this.errorMessage = errorMessage;
            }
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(errorMessage);

        }

        public RejectMessage(ByteBuffer buffer) {
            errorMessage = getString(buffer);
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return errorMessage;
        }
    }

    public class ProtocolError extends RejectMessage {

        public ProtocolError(String errorMessage) {
            super(errorMessage);
        }

        public ProtocolError(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return PROTOCOL_ERROR;
        }
    }

    /**
     * Version broadcasts the sending node's protocol version.
     */
    abstract class ProtocolVersion extends SimpleMessage {

        private final int version;

        private final int nodeId;

        public ProtocolVersion(int version) {
            super();
            this.version = version;
            this.nodeId = BinaryProtocol.this.nameIdPair.getId();
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(version, nodeId);
        }

        public ProtocolVersion(ByteBuffer buffer) {
            version = LogUtils.readInt(buffer);
            nodeId = LogUtils.readInt(buffer);
        }

        /**
         * @return the version
         */
        public int getVersion() {
            return version;
        }

        /**
         * The nodeId of the sender
         *
         * @return nodeId
         */
        public int getNodeId() {
            return nodeId;
        }
    }

    public class ClientVersion extends ProtocolVersion {

        public ClientVersion() {
            super(codeVersion);
        }

        public ClientVersion(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return CLIENT_VERSION;
        }
    }

    public class ServerVersion extends ProtocolVersion {

        public ServerVersion() {
            super(codeVersion);
        }

        public ServerVersion(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return SERVER_VERSION;
        }
    }

    public class IncompatibleVersion extends RejectMessage {
        public IncompatibleVersion(String message) {
            super(message);
        }

        public IncompatibleVersion(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return INCOMPATIBLE_VERSION;
        }
    }

    /**
     * Thrown in response to an unexpected response to a request.
     */
    @SuppressWarnings("serial")
    static public class ProtocolException extends InternalException {
        private final Message unexpectedMessage;

        private final Class<? extends Message> cl;

        /**
         * Constructor used for message sequencing errors.
         */
        public ProtocolException(Message unexpectedMessage,
                                 Class<? extends Message> cl) {
            super();
            this.unexpectedMessage = unexpectedMessage;
            this.cl = cl;
        }

        public ProtocolException(String message) {
            super(message);
            this.unexpectedMessage = null;
            this.cl = null;
        }

        @Override
        public String getMessage() {
            return (unexpectedMessage != null) ?
                    ("Expected message type: " + cl + " but found: " +
                     unexpectedMessage.getClass() +

                     /*
                      * Include information about the message, which is
                      * particularly useful for a RejectMessage
                      */
                     ": " + unexpectedMessage) :

                    super.getMessage();
        }

        public Message getUnexpectedMessage() {
            return unexpectedMessage;
        }
    }
}
