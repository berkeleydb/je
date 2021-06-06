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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.DurationConfigParam;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.elections.Utils;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;

/**
 * TextProtocol provides the support for implementing simple low performance
 * protocols involving replication nodes. The protocol is primarily text based,
 * and checks group membership and version matches with every message favoring
 * flexibility over performance.
 *
 * The base class is primarily responsible for the message formatting and
 * message envelope validation. The subclasses define the specific messages
 * that constitute the protocol and the request/response semantics.
 *
 * Every message has the format:
 * <version>|<name>|<id>|<op>|<op-specific payload>
 *
 * <version> is the version of the protocol in use.
 * <name>    identifies a group participating in an election. It avoids
 *           accidental cross-talk across groups holding concurrent elections.
 * <id>      identifies the originator of the message within the group.
 * <op>      the operation identified by the specific message.
 * <op-specific payload> the payload associated with the particular operation.
 */

public abstract class TextProtocol {

    /* The name of the class associated with this protocol. */
    private final String name;

    /*
     * Protocol version string. Format: <major version>.<minor version>
     * It's used to ensure compatibility across versions.
     */
    private final String VERSION;

    /* The name of the group executing this protocol. */
    private final String groupName;

    /*
     * The set of ids of nodes that are permitted to communicate via this
     * protocol, or null if not restricted. It's updated as nodes enter and
     * leave the dynamic group.
     */
    private Set<Integer> memberIds;

    /* The id associated with this protocol participant. */
    private final NameIdPair nameIdPair;

    /*
     * The suffix of message prefix constituting the "fixed" part of the
     * message for this group and node, it does not include the version
     * information, which goes in front of this prefix.
     */
    protected final String messageNocheckSuffix;

    /*
     * Timeouts used for network communications. Use setTimeouts() to override
     * the defaults.
     */
    private int openTimeoutMs = 10000; // Default to 10 sec
    private int readTimeoutMs = 10000; // Default to 10 sec

    /* The token separator in messages */
    public static final String SEPARATOR = "|";
    public static final String SEPARATOR_REGEXP="\\" + SEPARATOR;

    /* A message defined by the base class to deal with all errors. */
    public final MessageOp PROTOCOL_ERROR =
                new MessageOp("PE", ProtocolError.class);
    public final MessageOp OK_RESP = new MessageOp("OK", OK.class);
    public final MessageOp FAIL_RESP = new MessageOp("FAIL", Fail.class);

    /* The number of message types defined by the subclass. */
    private int nonDefaultMessageCount;

    /* Maps request Ops to the corresponding enumerator. */
    private final Map<String,MessageOp> ops = new HashMap<>();

    protected final Logger logger;
    protected final Formatter formatter;
    protected final RepImpl repImpl;
    protected final DataChannelFactory channelFactory;

    /**
     * Hook used to modify messages in the serialized form. The hook is invoked
     * on a serialized Request immediately before it's written to the network
     * and immediately after a response is received and before it's
     * deserialized. The hook implementation must be re-entrant.
     */
    private static TestHook<String> serDeHook;

    /**
     * Creates an instance of the Protocol.
     *
     * @parameter version the protocol version number
     * @parameter groupName the name of the group executing this protocol
     * @param nameIdPair a unique identifier for this node
     * @param repImpl for logging, may be null
     * @param channelFactory the factory for channel creation
     */
    public TextProtocol(String version,
                        String  groupName,
                        NameIdPair nameIdPair,
                        RepImpl repImpl,
                        DataChannelFactory channelFactory) {
        this.VERSION = version;
        this.groupName = groupName;
        this.nameIdPair = nameIdPair;
        this.repImpl = repImpl;
        this.channelFactory = channelFactory;
        name = getClass().getName();

        messageNocheckSuffix =
            groupName + SEPARATOR + NameIdPair.NOCHECK_NODE_ID;

        if (repImpl != null) {
            this.logger = LoggerUtils.getLogger(getClass());
        } else {
            this.logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        }
        this.formatter = new ReplicationFormatter(nameIdPair);
    }

    /**
     * Sets the hook that is invoked post serialization on request messages and
     * pre deserialization on response messages.
     *
     *  The hook implementation must be re-entrant.
     */
    public static void setSerDeHook(TestHook<String> serDeHook) {
        TextProtocol.serDeHook = serDeHook;
    }

    /**
     * Set the network timeouts associated with uses of this protocol instance.
     */
    protected void setTimeouts(RepImpl repImpl,
                               DurationConfigParam openTimeoutConfig,
                               DurationConfigParam readTimeoutConfig) {
        if (repImpl == null) {
            return;
        }
        final DbConfigManager configManager = repImpl.getConfigManager();
        openTimeoutMs = configManager.getDuration(openTimeoutConfig);
        readTimeoutMs = configManager.getDuration(readTimeoutConfig);
    }

    /**
     * The messages as defined by the subclass. Note that PROTOCOL_ERROR is a
     * pre-defined message that is defined by this class. The initialization is
     * not considered until this method after been invoked typically in the
     * constructor itself. This two-step is unfortunately necessary since the
     * creation of MessageOps instances requires that this class be completely
     * initialized, otherwise the MessageOp list could have been passed in as a
     * constructor argument.
     *
     * @param protocolOps the message ops defined by the subclass.
     */
    protected void initializeMessageOps(MessageOp[] protocolOps) {
        for (MessageOp op : protocolOps) {
            ops.put(op.opId, op);
        }
        nonDefaultMessageCount = protocolOps.length;
        ops.put(PROTOCOL_ERROR.opId, PROTOCOL_ERROR);
        ops.put(OK_RESP.opId, OK_RESP);
        ops.put(FAIL_RESP.opId, FAIL_RESP);
    }

    /**
     * For testing only.
     */
    protected MessageOp replaceOp(String op, MessageOp message) {
        return ops.put(op, message);
    }

    /**
     * Returns the messages, of the specified type, used by the protocol
     */
    public Set<String> getOps(Class<? extends Message> messageType) {
        final Set<String> reqOps = new HashSet<>();
        for (Entry<String, MessageOp> e : ops.entrySet()) {
            if (messageType.
                isAssignableFrom(e.getValue().getMessageClass())) {
                reqOps.add(e.getKey());
            }
        }

        return reqOps;
    }

    public int getOpenTimeout() {
        return openTimeoutMs;
    }

    public int getReadTimeout() {
        return readTimeoutMs;
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    /* The total number of nonDefault messages defined by the protocol. */
    public int messageCount() {
        return nonDefaultMessageCount;
    }

    /**
     * Updates the current set of nodes that are permitted to communicate via
     * this protocol, or null for unrestricted.
     *
     * @param newMemberIds
     */
    public void updateNodeIds(Set<Integer> newMemberIds) {
        memberIds = newMemberIds;
    }

    /**
     * Returns the Integer number which represents a Protocol version.
     */
    public int getMajorVersionNumber(String version) {
        return Double.valueOf(version).intValue();
    }

    /**
     * The Operations that are part of the protocol.
     */
    public static class MessageOp {

        /* The string denoting the operation for the request message. */
        private final String opId;

        /* The class used to represent the message. */
        private final Class<? extends Message> messageClass;

        public MessageOp(String opId, Class<? extends Message> messageClass) {
            this.opId = opId;
            this.messageClass = messageClass;
        }

        String getOpId() {
            return opId;
        }

        Class<? extends Message> getMessageClass() {
            return messageClass;
        }

        @Override
        public String toString() {
            return opId;
        }
    }

    /**
     * Represents the tokens on a message line. The order of the enumerators
     * represents the order of the tokens in the wire format.
     */
    public enum TOKENS {
        VERSION_TOKEN,
        NAME_TOKEN,
        ID_TOKEN,
        OP_TOKEN,
        FIRST_PAYLOAD_TOKEN;
    }

    /* Used to indicate that an entity is formatable and can be serialized and
     * de-serialized.
     */
    protected interface WireFormatable {

        /*
         * Returns the string representation suitable for use in a network
         * request.
         */
        abstract String wireFormat();
    }

    /**
     * Parses a line into a Request/Response message.
     *
     * @param line containing the message
     * @return a message instance
     * @throws InvalidMessageException
     */
    public Message parse(String line)
        throws InvalidMessageException {

        String[] tokens = line.split(SEPARATOR_REGEXP);

        final int index = TOKENS.OP_TOKEN.ordinal();
        if (index >= tokens.length) {
            throw new InvalidMessageException(
                MessageError.BAD_FORMAT,
                "Missing message op in message: " + line);
        }
        final MessageOp op = ops.get(tokens[index]);
        if (op == null) {
            throw new InvalidMessageException(MessageError.BAD_FORMAT,
                                              "Text Protocol" +
                                              " unknown op:" + tokens[index] +
                                              " in message: " + line);
        }

        try {
            Class<? extends Message> c = op.getMessageClass();
            Constructor<? extends Message> cons =
                c.getConstructor(c.getEnclosingClass(),
                                 line.getClass(),
                                 tokens.getClass());
            Message message = cons.newInstance(this, line, tokens);
            return message;
        } catch (InstantiationException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (IllegalAccessException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (SecurityException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (NoSuchMethodException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (InvocationTargetException e) {
            /* Unwrap the exception. */
            Throwable target = e.getTargetException();
            if (target instanceof RuntimeException) {
                final String message = "message: " + line +
                  " exception:" + target.getClass().getName() +
                  " exception message:" + target.getMessage();
                throw new InvalidMessageException(MessageError.BAD_FORMAT,
                                                  message);

            } else if (target instanceof InvalidMessageException) {
                throw (InvalidMessageException) target;
            }
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Base message class for all messages exchanged in the protocol.
     */
    public abstract class Message implements WireFormatable {
        /* The sender of the message. */
        private int senderId = 0;

        /*
         * The version of this message, as it's deserialized and sent across
         * the network. The default is that messages are sent in the VERSION of
         * the current protocol, but in cases of mixed-version upgrades, the
         * message may be sent in an earlier version format.
         *
         * When this message is a RequestMessage, the sender will always
         * initially send it out its own native version, but may resend it in
         * an earlier version, if the recipient can't understand the native
         * version. When the message is a ResponseMessage, the sender can reply
         * either in its native version, or in an earlier version if the
         * requester is an older version of JE.
         */
        protected String sendVersion;

        /* The line representing the message. */
        private final String line;

        /* The tokenized form of the above line. */
        private final String[] tokens;

        /* The current variable arg token */
        private int currToken = TOKENS.FIRST_PAYLOAD_TOKEN.ordinal();

        protected String messagePrefixNocheck;

        /**
         * The constructor used for the original non-serialized instance of the
         * message, which does not use the line or tokens.
         */
        protected Message() {
            line = null;
            tokens = null;
            setSendVersion(VERSION);
        }

        /**
         * Every message must define a constructor of this form so that it can
         * be de-serialized. The constructor is invoked using reflection by the
         * parse() method.
         *
         * @param line the line constituting the message
         * @param tokens the line in token form
         * @throws InvalidMessageException
         * @throws EnvironmentFailureException on format errors
         */
        protected Message(String line, String[] tokens)
            throws InvalidMessageException {

            this.line = line;
            this.tokens = tokens;

            /* Validate the leading fixed fields. */
            final String version = getTokenString(TOKENS.VERSION_TOKEN);
            if (new Double(VERSION) < new Double(version)) {
                throw new InvalidMessageException
                    (MessageError.VERSION_MISMATCH,
                     "Version argument mismatch." +
                     " Expected: " + VERSION + ", found: " + version +
                     ", in message: " + line);
            }

            /*
             * Set the sender version of a request message. This version
             * information will be used by the receiver to determine what
             * version should be used for the response message.
             */
            setSendVersion(version);

            final String messageGroupName = getTokenString(TOKENS.NAME_TOKEN);
            if (!groupName.equals(messageGroupName)) {
                throw new InvalidMessageException
                (MessageError.GROUP_MISMATCH,
                 "Group name mismatch; this group name: " + groupName +
                 ", message group name: " + messageGroupName +
                 ", in message: " + line);
            }

            senderId =
                new Integer(getTokenString(TOKENS.ID_TOKEN)).intValue();
            if ((memberIds != null) &&
                (memberIds.size() > 0) &&
                (nameIdPair.getId() != NameIdPair.NOCHECK_NODE_ID) &&
                (senderId != NameIdPair.NOCHECK_NODE_ID) &&
                (senderId != nameIdPair.getId()) &&
                !memberIds.contains(senderId)) {
                throw new InvalidMessageException
                   (MessageError.NOT_A_MEMBER,
                    "Sender's member id: " + senderId +
                    ", message op: " + getTokenString(TOKENS.OP_TOKEN) +
                    ", was not a member of the group: " + memberIds +
                    ", in message: " + line);
            }
        }

        public int getSenderId() {
            return senderId;
        }

        /*
         * Set the version of the message that we have just received. This
         * version information will be used by the receiver to determine what
         * version should be used for the response message.
         */
        public void setSendVersion(String version) {
            if (new Double(VERSION) < new Double(version)) {
                throw new IllegalStateException
                    ("Send version: " + version + " shouldn't be larger " +
                     "than the current version: " + VERSION);
            }

            if (!version.equals(sendVersion)) {
                sendVersion = version;
                messagePrefixNocheck =
                    sendVersion + SEPARATOR + messageNocheckSuffix;
            }
        }

        /* Get the send version of a message. */
        public String getSendVersion() {
            return sendVersion;
        }

        protected String getMessagePrefix() {
            return sendVersion + SEPARATOR + groupName + SEPARATOR +
                   nameIdPair.getId();
        }

        public abstract MessageOp getOp();

        /**
         * Returns the protocol associated with this message
         */
        public TextProtocol getProtocol() {
            return TextProtocol.this;
        }

        /**
         * Returns the token value associated with the token type.
         *
         * @param tokenType identifies the token in the message
         * @return the associated token value
         */
        private String getTokenString(TOKENS tokenType) {
            final int index = tokenType.ordinal();
            if (index >= tokens.length) {
                throw EnvironmentFailureException.unexpectedState
                    ("Bad format; missing token: " + tokenType +
                     "at position: " + index + "in message: " + line);
            }
            return tokens[index];
        }

        /**
         * Returns the next token in the payload.
         *
         * @return the next payload token
         * @throws InvalidMessageException
         */
        protected String nextPayloadToken()
            throws InvalidMessageException {

            if (currToken >= tokens.length) {
                throw new InvalidMessageException
                (MessageError.BAD_FORMAT,
                 "Bad format; missing token at position: " + currToken +
                 ", in message: " + line);
            }
            return tokens[currToken++];
        }

        protected boolean hasMoreTokens() {
            return currToken < tokens.length;
        }

        /**
         * Returns the current token position in the payload.
         *
         * @return the current token position
         */
        protected int getCurrentTokenPosition() {
            return currToken;
        }
    }

    /**
     * Base classes for response messages.
     */
    public abstract class ResponseMessage extends Message {

        protected ResponseMessage() {
            super();
        }

        /**
         * Create an instance with the send version specified by the request.
         *
         * @param request the request
         */
        protected ResponseMessage(final RequestMessage request) {
            setSendVersion(request.getSendVersion());
        }

        protected ResponseMessage(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
        }

        /**
         * Returns the version id and Op concatenation that starts every
         * message.
         */
        protected String wireFormatPrefix() {
            return getMessagePrefix() + SEPARATOR + getOp().opId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ResponseMessage)) {
                return false;
            }
            return getOp().equals(((ResponseMessage)obj).getOp());
        }

        @Override
        public int hashCode() {
            return getOp().getOpId().hashCode();
        }
    }

    public class ProtocolError extends ResponseMessage {
        private final String message;
        private final MessageError errorType;

        public ProtocolError(InvalidMessageException messageException) {
            this(messageException.getErrorType(),
                 messageException.getMessage());
        }

        public ProtocolError(MessageError messageError, String message) {
            this.message = message;
            this.errorType = messageError;
        }

        public ProtocolError(String responseLine, String[] tokens)
            throws InvalidMessageException {

            super(responseLine, tokens);
            errorType = MessageError.valueOf(nextPayloadToken());
            message = nextPayloadToken();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((message == null) ? 0 : message.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof ProtocolError)) {
                return false;
            }
            final ProtocolError other = (ProtocolError) obj;
            if (message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!message.equals(other.message)) {
                return false;
            }

            return true;
        }

        @Override
        public MessageOp getOp() {
            return PROTOCOL_ERROR;
        }

        @Override
        public String wireFormat() {
            return wireFormatPrefix() + SEPARATOR +
                errorType.toString() + SEPARATOR + message;
        }

        public MessageError getErrorType() {
            return errorType;
        }

        public String getMessage() {
            return message;
        }
    }

    public class OK extends ResponseMessage {

        /**
         * Create an instance with the send version specified by the request.
         *
         * @param request the request
         */
        public OK(final RequestMessage request) {
            super(request);
        }

        public OK(String line, String[] tokens)
            throws InvalidMessageException {

            super(line, tokens);
        }

        @Override
        public MessageOp getOp() {
           return OK_RESP;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
           return wireFormatPrefix();
        }
    }

    public class Fail extends ResponseMessage {
        private final String message;

        public Fail(String message) {
            this.message = sanitize(message);
        }

        /**
         * Create an instance with the send version specified by the request.
         *
         * @param request the request
         * @param message the failure message
         */
        public Fail(final RequestMessage request, final String message) {
            super(request);
            this.message = sanitize(message);
        }

        public Fail(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);

            message = nextPayloadToken();
        }

        public String getMessage() {
            return message;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((message == null) ? 0 : message.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof Fail)) {
                return false;
            }
            Fail other = (Fail) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!message.equals(other.message)) {
                return false;
            }
            return true;
        }

        @Override
        public MessageOp getOp() {
           return FAIL_RESP;
        }

        @Override
        protected String getMessagePrefix() {
            return messagePrefixNocheck;
        }

        @Override
        public String wireFormat() {
           return wireFormatPrefix() + SEPARATOR + message;
        }

        private TextProtocol getOuterType() {
            return TextProtocol.this;
        }

        /**
         * Removes any newline characters.  Embedded newlines are not supported
         * by {@code TextProtocol}, but exception messages sometimes have them,
         * and the payload of a {@code Fail} response often comes from an
         * exception message.
         */
        private String sanitize(String msg) {
            return msg.replace("\n", "  ");
        }
    }

    /**
     * Base class for all Request messages
     */
    public abstract class RequestMessage extends Message {

        protected RequestMessage() {}

        protected RequestMessage(String line, String[] tokens)
            throws InvalidMessageException {
            super(line, tokens);
        }

        /**
         * Returns the version id and Op concatenation that form the prefix
         * for every message.
         */
        protected String wireFormatPrefix() {
            return getMessagePrefix() + SEPARATOR + getOp().opId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof RequestMessage)) {
                return false;
            }
            return getOp().equals(((RequestMessage) obj).getOp());
        }

        @Override
        public int hashCode() {
            return getOp().getOpId().hashCode();
        }
    }

    /**
     * Converts a response line into a ResponseMessage.
     *
     * @param responseLine
     * @return the response message
     * @throws InvalidMessageException
     */
    ResponseMessage parseResponse(String responseLine)
        throws InvalidMessageException {

        return (ResponseMessage) parse(responseLine);
    }

    /**
     * Converts a request line into a requestMessage.
     *
     * @param requestLine
     * @return the request message
     * @throws InvalidMessageException
     */
    public RequestMessage parseRequest(String requestLine)
        throws InvalidMessageException {

        return (RequestMessage) parse(requestLine);
    }

    /**
     * Reads the channel and returns a read request. If the message format
     * was bad, it sends a ProtocolError response back over the channel and
     * no further action is needed by the caller.
     *
     * @param channel the channel delivering the request
     * @return null if EOF was reached or the message format was bad
     * @throws IOException
     */
    public RequestMessage getRequestMessage(DataChannel channel)
        throws IOException {

        BufferedReader in = new BufferedReader
            (new InputStreamReader(Channels.newInputStream(channel)));

        String requestLine = in.readLine();
        if (requestLine == null) {
            /* EOF */
            return null;
        }
        try {
            return parseRequest(requestLine);
        } catch (InvalidMessageException e) {
            processIME(channel, e);
            return null;
        }
    }

    /**
     * Process an IME encountered during request processing by writing a
     * ProtocolError message as a response and logging it.
     *
     * @param channel the channel used to write the ProtocolError message
     * @param ime the exception
     */
    public void processIME(DataChannel channel,
                           InvalidMessageException ime) {
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                           name + " format error:" +
                           LoggerUtils.exceptionTypeAndMsg(ime));
        PrintWriter out =
            new PrintWriter(Channels.newOutputStream(channel), true);
        out.println(new ProtocolError(ime).wireFormat());
        out.close();
    }

    public ResponseMessage process(Object requestProcessor,
                                   RequestMessage requestMessage) {

        Class<? extends Object> cl = requestProcessor.getClass();
        try {
            Method method =
                cl.getMethod("process", requestMessage.getClass());
            return (ResponseMessage) method.invoke
                (requestProcessor, requestMessage);
        } catch (NoSuchMethodException e) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
                               name +
                               " Method: process(" +
                               requestMessage.getClass().getName() +
                               ") was missing");
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (Exception e) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
                               name +
                               " Unexpected exception: " +
                                LoggerUtils.exceptionTypeAndMsg(e));
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * A single request/response interaction, targetted at a given service
     * running at a particular remote socket address.  Since it implements
     * {@code Runnable} it can be used with thread pools, {@code Future}s,
     * etc.  But its {@code run()} method can also simply be called directly.
     */
    public class MessageExchange implements Runnable {

        public final InetSocketAddress target;
        private final RequestMessage requestMessage;
        private final String serviceName;
        private ResponseMessage responseMessage;
        public Exception exception;

        public MessageExchange(InetSocketAddress target,
                               String serviceName,
                               RequestMessage request) {
            this.target = target;
            this.serviceName = serviceName;
            this.requestMessage = request;
        }

        /*
         * Get the response message for a request message.
         *
         * If the response message is a ProtocolError message which caused by
         * protocol version mismatch, it resets the request message's
         * sendVersion as the ResponseMessage ProtocolError's version and send
         * again.
         */
        @Override
        public void run() {
            messageExchange();

            if (responseMessage != null &&
                responseMessage.getOp() == PROTOCOL_ERROR) {
                ProtocolError error = (ProtocolError) responseMessage;
                if (error.getErrorType() == MessageError.VERSION_MISMATCH) {
                    requestMessage.setSendVersion(error.getSendVersion());
                    messageExchange();
                    LoggerUtils.logMsg
                        (logger, repImpl, formatter, Level.INFO,
                          name +
                         " Resend message: " + requestMessage.toString() +
                         " in version: " + requestMessage.getSendVersion() +
                         " while protocol version is: " + VERSION +
                         " because of the version mismatch, the returned" +
                         " response message is: " + responseMessage);
                }
            }
        }

        /**
         * Run a message exchange. A successful exchange results in a response
         * message being set. All failures result in the response message being
         * null and an exception being set.
         */
        public void messageExchange() {

            DataChannel dataChannel = null;
            BufferedReader in = null;
            PrintWriter out = null;
            try {
                dataChannel =
                    channelFactory.connect(
                        target,
                        new ConnectOptions().
                        setTcpNoDelay(true).
                        setOpenTimeout(openTimeoutMs).
                        setReadTimeout(readTimeoutMs).
                        setBlocking(true).
                        setReuseAddr(true));

                ServiceDispatcher.doServiceHandshake(dataChannel,
                                                     serviceName);

                OutputStream ostream =
                    Channels.newOutputStream(dataChannel);
                out = new PrintWriter(ostream, true);
                String wireFormat = requestMessage.wireFormat();
                if (serDeHook != null) {
                    serDeHook.doHook(wireFormat);
                    wireFormat = serDeHook.getHookValue();
                }
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                                   name +
                                   " request: " + wireFormat+ " to " + target);
                out.println(wireFormat);
                out.flush();
                in = new BufferedReader
                    (new InputStreamReader(
                        Channels.newInputStream(dataChannel)));
                String line = in.readLine();
                if (serDeHook != null) {
                    serDeHook.doHook(line);
                    line = serDeHook.getHookValue();
                }

                LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                                   name +
                                   " response: " + line + " from " + target);
                if (line == null) {
                    setResponseMessage
                        (new ProtocolError(MessageError.BAD_FORMAT,
                                           "Premature EOF for request: " +
                                           wireFormat));
                } else {
                    setResponseMessage(parseResponse(line));
                }
            } catch (java.net.SocketTimeoutException e){
                this.exception = e;
            } catch (SocketException e) {
                this.exception = e;
            } catch (IOException e) {
                this.exception = e;
            } catch (TextProtocol.InvalidMessageException ime) {
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                                   name + " response format error:" +
                                   LoggerUtils.exceptionTypeAndMsg(ime) +
                                   " from:" + target);
                this.exception = ime;
            } catch (ServiceConnectFailedException e) {
                this.exception = e;
            } catch (Exception e) {
                this.exception = e;
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.SEVERE,
                                   name + " Unexpected exception:" +
                                   LoggerUtils.exceptionTypeAndMsg(e));
                throw EnvironmentFailureException.unexpectedException
                    ("Service: " + serviceName +
                     " failed; attempting request: " + requestMessage.getOp(),
                     e);
            } finally {
                Utils.cleanup(logger, repImpl, formatter, dataChannel, in, out);
            }
        }

        public void setResponseMessage(ResponseMessage responseMessage) {
            this.responseMessage = responseMessage;
        }

        /**
         * Returns the response message. The null may be returned as part of
         * the protocol exchange, or it may be null if an exception was
         * encountered say because of some IO problem. It's the caller's
         * responsibility to check for an exception in that circumstance.
         * <p>
         * Note: there may be some protocols (e.g., Monitor) that define null
         * to be a proper, expected response upon success.  It might be
         * preferable to redefine them to return an explicit OK response, if
         * possible.
         *
         * @return the response
         */
        public ResponseMessage getResponseMessage() {
            return responseMessage;
        }

        public RequestMessage getRequestMessage() {
            return requestMessage;
        }

        public Exception getException() {
            return exception;
        }
    }

    protected static class StringFormatable implements WireFormatable {
        protected String s;

        StringFormatable() {}

        protected StringFormatable(String s) {
            this.s = s;
        }

        public void init(String wireFormat) {
            s = wireFormat;
        }

        @Override
        public String wireFormat() {
            return s;
        }

        @Override
        public int hashCode() {
            return ((s == null) ? 0 : s.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof StringFormatable)) {
                return false;
            }

            final StringFormatable other = (StringFormatable) obj;
            if (s == null) {
                if (other.s != null) {
                    return false;
                }
            } else if (!s.equals(other.s)) {
                return false;
            }
            return true;
        }
    }

    /*
     * The type associated with an invalid Message. It's used by the exception
     * below and by ProtocolError.
     */
    static public enum MessageError
        {BAD_FORMAT, VERSION_MISMATCH, GROUP_MISMATCH, NOT_A_MEMBER}

    /**
     * Used to indicate a message format or invalid content exception.
     */
    @SuppressWarnings("serial")
    public static class InvalidMessageException extends Exception {
        private final MessageError errorType;

        public InvalidMessageException(MessageError errorType,
                                       String message) {
            super(message);

            this.errorType = errorType;
        }

        public MessageError getErrorType() {
            return errorType;
        }
    }
}
