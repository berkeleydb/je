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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channel;
import java.util.logging.Level;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.utilint.StringUtils;

/**
 * Provides Dispatcher service handshake logic, including identification of the
 * service to be accessed and authentication for access to the service.
 *
 * The service initialization protocol looks like one of these flows.  In the
 * flows, the notation byte(X) indicates that X is a 1-byte network field and
 * 2byte(X) indicates that X is a 2-byte network field.
 *
 * If No authentication is configured:
 *   Client                                         Dispatcher
 *       ->  "Service:" + byte(len) + <service name>
 *       <-  byte(OK|BUSY|UNKNOWN_SERVICE)
 *
 * If authentication is configured:
 *   Client                                         Dispatcher
 *       ->  "Service:" + byte(len) + <service name>
 *       <-  byte(AUTHENTICATE)
 *       ->  "Authenticate:" + byte(<len>) + <mechanism list>
 *       <-  "Mechanism:" + 2byte(<len>) + <server-selected mech name> +
 *                        ":" + <auth mech params>
 *       ->  <Mechanism-specific payload>
 *       <-  byte(OK|BUSY|UNKNOWN_SERVICE|INVALID)
 *
 * The code here is organized into client-side and server-side functions.
 * Client-side code is presumed to operated in blocking mode and server-side
 * code is presumed to operate in non-blocking mode.
 */
public class ServiceHandshake {

    /**
     * The maximum length of a service name
     */
    public static final int SERVICE_NAME_LIMIT = 127;

    /*
     * Maximum number of times to retry a write operation
     */
    private static final int CHANNEL_WRITE_ATTEMPT_LIMIT = 10;

    /**
     * Operation-level result indicator
     */
    public static enum InitResult {
        /* A failure has been detected */
        FAIL,
        /* Need to read additional input - only applicable server-side*/
        READ,
        /* Successful completion */
        DONE,
        /* Authentication failure, but no notification sent yet. */
        REJECT
    };

    /**
     * Server-side handshake state
     */
    public static class ServerHandshake {
        /* The communication channel on which we are operating */
        private final DataChannel channel;

        /* The dispatcher on whose behalf we are operating */
        private final ServiceDispatcher dispatcher;

        /* The set of valid authentication mechanisms available for use */
        private final AuthenticationMethod[] authInfo;

        /* The current handshake operation being processed */
        private ServerInitOp currentOp;

        /* Discovered as part of the handshake protocol */
        private String serviceName;

        ServerHandshake(DataChannel dataChannel,
                        ServiceDispatcher dispatcher,
                        AuthenticationMethod[] authInfo) {

            this.channel = dataChannel;
            this.dispatcher = dispatcher;
            this.authInfo = authInfo;
            currentOp = new ReceiveNameOp(this);
        }

        DataChannel getChannel() {
            return channel;
        }

        /**
         * Callback for underlying init operations upon successful discovery
         * of the requested service name.
         */
        void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        String getServiceName() {
            return serviceName;
        }

        /**
         * Process the handshake incrementally, as channel is presumed to be
         * in non-blocking mode.
         *
         * This function returns one of the following:
         *    InitResult.READ: handshake is not complete; more data needed
         *    InitResult.FAIL: handshake was rejected or otherwise failed
         *      the underlying data channel has been closed.
         *    InitResult.DONE: handshake is complete, and any authentication
         *      has been performed.  A Response still needs to be sent to
         *      the client.  Caller must do this after any additional validation
         *      that may be required.
         */
        InitResult process() throws IOException {
            final InitResult result = currentOp.processOp(channel);
            if (result != InitResult.DONE) {
                return result;
            }

            if (channel.isTrustCapable()) {
                if (channel.isTrusted()) {
                    /*
                     * Authentication of the requester has been handled at the
                     * DataChannel level, so no need to worry further about
                     * authentication.
                     */
                    return InitResult.DONE;
                }
                logMsg(Level.WARNING,
                       false, // noteError
                       "DataChannel is trust-capable but is not trusted");

                /*
                 * Defer rejecting the connection until the
                 * RequireAuthenticateOp step, in case there is an alternate
                 * authentication mechanism.
                 */
            }

            if (currentOp instanceof RequireAuthenticateOp ||
                authInfo == null) {
                /*
                 * Either we've just successfully completed authentication or
                 * no authentication is required.
                 */
                return InitResult.DONE;
            }

            /* Initiate the authentication step. */
            currentOp = new RequireAuthenticateOp(this, authInfo);
            return currentOp.processOp(channel);
        }

        void logMsg(Level level, boolean noteError, String msg) {
            dispatcher.logMsg(level, noteError, msg);
        }
    }

    /**
     * Client-side handshake state
     */
    public static class ClientHandshake {
        private String serviceName;
        private AuthenticationMethod[] authInfo;
        private IOAdapter ioAdapter;

        ClientHandshake(String serviceName,
                        AuthenticationMethod[] authInfo,
                        IOAdapter ioAdapter) {
            this.authInfo = authInfo;
            this.serviceName = serviceName;
            this.ioAdapter = ioAdapter;
        }

        /**
         * Process the entire handshake sequence and report the final
         * response code received from the service dispatcher.
         */
        Response process() throws IOException {
            final SendNameOp nameOp = new SendNameOp(this, serviceName);
            final InitResult nameResult = nameOp.processOp(ioAdapter);
            if (nameResult == InitResult.FAIL) {
                return Response.INVALID;
            }

            if (nameOp.getResponse() != Response.AUTHENTICATE) {
                return nameOp.getResponse();
            }

            final DoAuthenticateOp authOp =
                new DoAuthenticateOp(this, authInfo);
            final InitResult authResult = authOp.processOp(ioAdapter);
            if (authResult == InitResult.FAIL) {
                return Response.INVALID;
            }
            return authOp.getResponse();
        }
    }

    /**
     * The base class of elemental service initialization protocol operations
     * for the server side.
     */
    public abstract static class ServerInitOp {

        /* The handshake to which the operation belongs */
        protected final ServerHandshake initState;

        protected ServerInitOp(ServerHandshake initState) {
            this.initState = initState;
        }

        /**
         * Incrementally process the operation.  The operation may require
         * multiple passes since we are presumed to be in non-blocking mode.
         *
         * This function returns one of the following:
         *    InitResult.READ: operation is not complete; more data needed
         *    InitResult.FAIL: operation was rejected or otherwise failed
         *      the underlying data channel has been closed.
         *    InitResult.DONE: operation has completed
         */
        protected abstract InitResult processOp(DataChannel channel)
            throws IOException;

        /**
         * Helper function to read until the buffer is full.
         */
        protected InitResult fillBuffer(DataChannel channel, ByteBuffer buffer)
            throws IOException {

            while (buffer.remaining() > 0) {
                final int count = channel.read(buffer);
                if (count < 0) {
                    /* Premature EOF */
                    initState.logMsg(Level.WARNING,
                                     true, // noteError
                                     "Premature EOF on channel: " + channel +
                                     ", service: " +
                                     initState.getServiceName() +
                                     " - read() returned: " + count);
                    closeChannel(channel);
                    return InitResult.FAIL;
                }
                if (count == 0) {
                    return InitResult.READ;
                }
            }
            return InitResult.DONE;
        }

        /**
         * Helper function to write the contents of the buffer.
         * We make the simplifying assumption here that we will be able
         * to write as much as we want (within reason).
         */
        protected InitResult sendBuffer(DataChannel channel, ByteBuffer buffer)
            throws IOException {
            int tryCount = 0;
            while (buffer.remaining() > 0) {
                final int count = channel.write(buffer);
                if (count < 0) {
                    /* Premature EOF */
                    initState.logMsg(Level.WARNING,
                                     true, // noteError
                                     "Premature EOF on channel: " +
                                     channel + " write() returned: " +
                                     count);
                    closeChannel(channel);
                    return InitResult.FAIL;
                } else if (count == 0) {
                    tryCount++;
                    if (tryCount > CHANNEL_WRITE_ATTEMPT_LIMIT) {
                        initState.logMsg(Level.WARNING,
                                         true, // noteError
                                         "Failed to write to channel. " +
                                         "Send buffer size: " +
                                         channel.getSocketChannel().socket().
                                         getSendBufferSize());
                        throw EnvironmentFailureException.unexpectedState(
                            "Failed to write to channel");
                    }
                } else {
                    tryCount = 0;
                }
            }
            return InitResult.DONE;
        }

        /**
         * Closes the channel, logging any resulting exceptions.
         *
         * @param channel the channel being closed
         */
        void closeChannel(Channel channel) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e1) {
                    initState.logMsg(
                        Level.WARNING,
                        false, // noteError
                        "Exception during cleanup: " + e1.getMessage());
                }
            }
        }
    }

    /**
     * The base class of elemental service initialization protocol operations
     * for the client side.  The operation is assumed to be in blocking I/O
     * mode.
     */
    public abstract static class ClientInitOp {

        private Response response;

        protected final ClientHandshake initState;

        protected ClientInitOp(ClientHandshake initState) {
            this.initState = initState;
        }

        /**
         * Process the operation in support of the handshake.
         * This operation may consist of read and/or write operations.
         *
         * This function returns one of the following:
         *    InitResult.FAIL: operation was rejected or otherwise failed. The
         *      underlying communication channel needs to be closed by the
         *      caller.
         *    InitResult.DONE: operation has completed.  The most recent
         *      Response from the server received during this operation is
         *      available for inspection.  Note that this may return DONE
         *      with the response set to a failure response value (e.g. INVALID)
         */
        protected abstract InitResult processOp(IOAdapter ioAdapter)
            throws IOException;

        /**
         * Response is set after processOp returns InitResult.DONE.
         */
        Response getResponse() {
            return response;
        }

        protected void setResponse(Response response) {
            this.response = response;
        }
    }

    /**
     * Operations to communicate the requested service name.  This is the
     * initial portion of the message and if we have a connection from a
     * foreign entity, this is most likely to detect an error.
     */

    /* The prefix for a service request. */
    private static final String REQUEST_PREFIX = "Service:";
    private static final byte[] REQUEST_PREFIX_BYTES =
        StringUtils.toASCII(REQUEST_PREFIX);

    /**
     * Server-side: expect client to provide a service name.
     * Expected data format:
     *    Literal: "Service:" in ASCII encoding
     *    Length: 1 byte
     *    Service Name: <Length> bytes in ASCII encoding
     */
    static class ReceiveNameOp extends ServerInitOp {

        /*
         * The initial size is the prefix plus the byte that holds the
         * length of the service name.
         */
        private final int INITIAL_BUFFER_SIZE = REQUEST_PREFIX_BYTES.length+1;

        private ByteBuffer buffer;

        ReceiveNameOp(ServerHandshake initState) {
            super(initState);
            this.buffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        }

        @Override
        protected InitResult processOp(DataChannel channel) throws IOException {
            InitResult readResult = fillBuffer(channel, buffer);
            if (readResult != InitResult.DONE) {
                return readResult;
            }
            buffer.flip();
            if (buffer.capacity() == INITIAL_BUFFER_SIZE) {
                /*
                 * We've received exactly enough data to contain:
                 * "Service:"<Length>
                 * Make sure that it has the right format.
                 */
                final String prefix = StringUtils.fromASCII
                    (buffer.array(), 0, REQUEST_PREFIX.length());
                if (!prefix.equals(REQUEST_PREFIX)) {
                    initState.logMsg
                        (Level.WARNING,
                         true, // noteError
                         "Malformed service request: " + prefix);
                    channel.write(Response.FORMAT_ERROR.byteBuffer());
                    closeChannel(channel);
                    return InitResult.FAIL;
                }

                /* Enlarge the buffer to read the service name as well */
                final int nameLength = buffer.get(INITIAL_BUFFER_SIZE-1);
                if (nameLength <= 0) {
                    initState.logMsg
                        (Level.WARNING,
                         true, // noteError
                         "Bad service service name length: " + nameLength);
                    channel.write(Response.FORMAT_ERROR.byteBuffer());
                    closeChannel(channel);
                    return InitResult.FAIL;
                }
                final ByteBuffer newBuffer =
                    ByteBuffer.allocate(INITIAL_BUFFER_SIZE + nameLength);
                newBuffer.put(buffer);
                buffer = newBuffer;

                /*
                 * Recursive call to get the service name
                 */
                return processOp(channel);
            }

            /*
             * If we made it here, we have a complete service request
             * message. Extract the service name from the buffer
             */
            final String request = StringUtils.fromASCII(buffer.array());
            initState.setServiceName(
                request.substring(REQUEST_PREFIX.length()+1));
            return InitResult.DONE;
        }
    }

    /**
     * Client-side: Send the initial service request to the server and await
     * a Response.
     */
    static class SendNameOp extends ClientInitOp {

        String serviceName;

        SendNameOp(ClientHandshake initState, String serviceName) {
            super(initState);
            this.serviceName = serviceName;
        }

        @Override
        protected InitResult processOp(IOAdapter ioAdapter) throws IOException {
            byte[] message = null;
            try {
                message = serviceRequestMessage(serviceName);
            } catch (IllegalArgumentException iae) {
                throw new IOException(
                    "Unable to encode requested service name");
            }
            ioAdapter.write(message);
            final byte[] responseBytes = new byte[1];
            final int result = ioAdapter.read(responseBytes);
            if (result < 0) {
                throw new IOException("No service response byte: " + result);
            }
            final Response response = Response.get(responseBytes[0]);
            if (response == null) {
                throw new IOException("Unexpected read response byte: " +
                                      responseBytes[0]);
            }

            setResponse(response);
            return InitResult.DONE;
        }

        /**
         * Builds a service request suitable for sending over to a
         * ServiceDispatcher.
         *
         * @param serviceName the service that is being requested. The
         * service name must be less than SERVICE_NAME_LIMIT in size.
         *
         * @return the byte encoding of the service request message
         */
        private static byte[] serviceRequestMessage(String serviceName) {
            final byte[] serviceNameBytes = StringUtils.toASCII(serviceName);
            if (serviceNameBytes.length > SERVICE_NAME_LIMIT) {
                throw new IllegalArgumentException(
                    "The provided service name is too long: " +
                    serviceName);
            }

            final int length = REQUEST_PREFIX_BYTES.length + 1 +
                serviceNameBytes.length;
            final ByteBuffer buffer = ByteBuffer.allocate(length);
            buffer.put(REQUEST_PREFIX_BYTES).
                put((byte)serviceNameBytes.length).
                put(serviceNameBytes);
            return buffer.array();
        }
    }

    /*
     * Top-level operations for handling service authentication.
     * This class is used upon receipt of a service request which we have
     * responded to with an AUTHENTICATE.  The operation has two
     * sub-phases:
     *   1) negotiate an authentication methods,
     *   2) complete the negotiated method.
     */

    /* The prefix for an authentication request. */
    private static final String AUTH_PREFIX = "Authenticate:";
    private static final byte[] AUTH_PREFIX_BYTES;

    /* The prefix for an authentication mechanism response. */
    private static final String AUTH_MECH_PREFIX = "Mechanism:";
    private static final byte[] AUTH_MECH_PREFIX_BYTES;

    static {
        AUTH_PREFIX_BYTES = StringUtils.toASCII(AUTH_PREFIX);
        AUTH_MECH_PREFIX_BYTES = StringUtils.toASCII(AUTH_MECH_PREFIX);
    }

    /**
     * Server side operation to communicate the need for authentication
     * to the client, negotiate an authentication mechanism and then
     * complete the authentication.
     */
    static class RequireAuthenticateOp extends ServerInitOp {

        private final AuthenticationMethod[] authInfo;
        private ExpectAuthRequestOp expectRequestOp;
        private ServerInitOp authOp;

        RequireAuthenticateOp(ServerHandshake initState,
                              AuthenticationMethod[] authInfo) {
            super(initState);
            this.authInfo = authInfo;
        }

        @Override
        protected InitResult processOp(DataChannel channel) throws IOException {

            if (expectRequestOp == null) {
                /* Tell the other end that they need to authenticate */
                Response response = Response.AUTHENTICATE;
                InitResult writeResult = sendBuffer(channel,
                                                    response.byteBuffer());
                if (writeResult != InitResult.DONE) {
                    return writeResult;
                }
                expectRequestOp =
                    new ExpectAuthRequestOp(initState, authInfo);
            }

            if (authOp == null) {
                /* Waiting for the authentication initiation request */
                final InitResult readResult =
                    expectRequestOp.processOp(channel);
                if (readResult != InitResult.DONE) {
                    return readResult;
                }

                AuthenticationMethod selectedAuth =
                    expectRequestOp.getSelectedAuthentication();

                if (selectedAuth == null) {
                    selectedAuth = new NoMatchAuthentication();
                }
                authOp = selectedAuth.getServerOp(initState);

                /* Prepare the response message */
                final String authResponseStr = selectedAuth.getMechanismName() +
                    ":" + selectedAuth.getServerParams();
                final byte[] authResponseBytes =
                    StringUtils.toASCII(authResponseStr);
                final int length = 1 + AUTH_MECH_PREFIX_BYTES.length +
                    2 + authResponseBytes.length;

                final ByteBuffer buffer = ByteBuffer.allocate(length);
                buffer.put(Response.PROCEED.byteBuffer());
                buffer.put(AUTH_MECH_PREFIX_BYTES);
                putShort(buffer, (short) authResponseBytes.length);
                buffer.put(authResponseBytes);

                buffer.flip();

                /* Send the response message */
                final InitResult writeResult = sendBuffer(channel, buffer);
                if (writeResult != InitResult.DONE) {
                    return writeResult;
                }
            }

            return authOp.processOp(channel);
        }
    }

    /**
     * Client side operation to request authentication.  This is done
     * in response to a message from the server notifying us that authentication
     * is required.  The implementation of processOp() aggregates functionality
     * from SendRequestOp, which tells the server what authentication we
     * can provide, and then when we've agreed with the server on how to
     * communicate, another ClientOp is created that provides the actual
     * authentication implementation.
     */
    static class DoAuthenticateOp extends ClientInitOp {

        private final AuthenticationMethod[] authInfo;

        DoAuthenticateOp(ClientHandshake initState,
                         AuthenticationMethod[] authInfo) {
            super(initState);
            this.authInfo = authInfo;
        }

        @Override
        protected InitResult processOp(IOAdapter ioAdapter) throws IOException {

            /*
             * Tell the server that we want to authenticate ourselves, and
             * negotiate with the server how that will happen.
             */
            final SendRequestOp sendOp = new SendRequestOp(initState, authInfo);
            final InitResult sendResult = sendOp.processOp(ioAdapter);
            if (sendResult != InitResult.DONE) {
                return sendResult;
            }

            /*
             * Now use the authOp determined by sendOp() to perform the
             * actual authentication steps.
             */
            final ClientInitOp authOp = sendOp.getAuthOp();
            final InitResult authResult = authOp.processOp(ioAdapter);
            if (authResult == InitResult.DONE) {
                setResponse(authOp.getResponse());
            }
            return authResult;
        }
    }

    /**
     * Server side Authentication request setup.
     * Expect an authentication request that looks like:
     *     Authenticate:<len><mechanism list>
     * where len is a 1-byte length and mechanism list is a comma-separated
     * list of available authentication mechanisms.  Upon completion,
     * selectedMechanism is non-null if the client supports an authentication
     * mechanism in common with us.
     */
    static class ExpectAuthRequestOp extends ServerInitOp {

        /* Three pieces of information that we need to get */
        final static int WAIT_FOR_REQUEST = 1;
        final static int WAIT_FOR_LIST_SIZE = 2;
        final static int WAIT_FOR_LIST = 3;

        private int phase;
        private ByteBuffer buffer;
        private AuthenticationMethod[] availableAuth;
        private AuthenticationMethod selectedAuth;

        ExpectAuthRequestOp(ServerHandshake initState,
                            AuthenticationMethod[] authInfo) {
            super(initState);
            this.availableAuth = authInfo;
            this.phase = WAIT_FOR_REQUEST;
            this.buffer = ByteBuffer.allocate(AUTH_PREFIX_BYTES.length);
        }

        /**
         * Return the negotiated authentication mechanism.
         * Returns null if no common authentication mechanism found.
         */
        AuthenticationMethod getSelectedAuthentication() {
            return selectedAuth;
        }

        @Override
        protected InitResult processOp(DataChannel channel) throws IOException {
            InitResult readResult = fillBuffer(channel, buffer);
            if (readResult != InitResult.DONE) {
                return readResult;
            }

            if (phase == WAIT_FOR_REQUEST) {
                final String prefix = StringUtils.fromASCII
                    (buffer.array(), 0, AUTH_PREFIX.length());
                if (!prefix.equals(AUTH_PREFIX)) {
                    initState.logMsg(Level.WARNING,
                                     true, // noteError
                                     "Malformed authentication request: " +
                                     prefix);
                    sendBuffer(channel, Response.FORMAT_ERROR.byteBuffer());
                    closeChannel(channel);
                    return InitResult.FAIL;
                }

                /* Get the length of auth mechanism list */
                buffer.clear();
                buffer.limit(1);
                phase = WAIT_FOR_LIST_SIZE;

                readResult = fillBuffer(channel, buffer);
                if (readResult != InitResult.DONE) {
                    return readResult;
                }
            }

            if (phase == WAIT_FOR_LIST_SIZE) {
                buffer.flip();
                final int mechListSize = buffer.get();
                if (mechListSize < 0) {
                    initState.logMsg(Level.WARNING,
                                     true, // noteError
                                     "Negative mechanism list size received: " +
                                     mechListSize);
                    sendBuffer(channel, Response.FORMAT_ERROR.byteBuffer());
                    closeChannel(channel);
                    return InitResult.FAIL;
                }

                /* Now get the list itself */
                buffer = ByteBuffer.allocate(mechListSize);
                phase = WAIT_FOR_LIST;

                readResult = fillBuffer(channel, buffer);
                if (readResult != InitResult.DONE) {
                    return readResult;
                }
            }

            if (phase != WAIT_FOR_LIST) {
                throw EnvironmentFailureException.unexpectedState(
                    "Unexpected state: + phase");
            }

            /*
             * Get the mechanism list in string form and then split into
             * constituent components.
             */
            final String mechListStr = StringUtils.fromASCII
                (buffer.array(), 0, buffer.capacity());
            final String[] mechList = mechListStr.split(",");

            /* Find the first available match */
            selectedAuth = findMatch(mechList, availableAuth);

            if (selectedAuth == null) {
                /* No acceptable mechanism found */
                initState.logMsg
                    (Level.WARNING,
                     true, // noteError
                     "No acceptable authentication mechanism in list: " +
                     mechListStr);
                sendBuffer(channel, Response.INVALID.byteBuffer());
                closeChannel(channel);
                return InitResult.FAIL;
            }

            return InitResult.DONE;
        }
    }

    /**
     * Client side: Send authentication request to the server.
     */
    static class SendRequestOp extends ClientInitOp {

        private final AuthenticationMethod[] authInfo;
        private ClientInitOp authOp;

        SendRequestOp(ClientHandshake initState,
                      AuthenticationMethod[] authInfo) {
            super(initState);
            this.authInfo = authInfo;
        }

        @Override
        protected InitResult processOp(IOAdapter ioAdapter) throws IOException {

            final byte[] responseByte = new byte[1];

            /*
             * Send the authenticate message to the service dispatcher,
             * including reporting our available authentication mechanisms.
             */
            ioAdapter.write(serviceAuthenticateMessage());

            /*
             * Wait for a response indicating whether to proceed with
             * authentication
             */
            int result = ioAdapter.read(responseByte);
            if (result < 0) {
                throw new IOException(
                    "No service authenticate response byte: " + result);
            }

            final Response response = Response.get(responseByte[0]);
            setResponse(response);
            if (response == null) {
                throw new IOException("Unexpected read response byte: " +
                                      responseByte[0]);
            }
            if (response != Response.PROCEED) {
                return InitResult.FAIL;
            }

            /*
             * response is PROCEED, so find out what mechanism we are
             * to use.
             */
            final byte[] mechPrefix =
                new byte[AUTH_MECH_PREFIX_BYTES.length + 2];
            result = ioAdapter.read(mechPrefix);
            if (result < 0) {
                throw new IOException(
                    "EOF reading service authenticate response: " + result);
            }
            if (!arraysEqual(AUTH_MECH_PREFIX_BYTES, mechPrefix,
                             AUTH_MECH_PREFIX_BYTES.length)) {
                throw new IOException(
                    "Unexpected service authenticate response: " +
                    encodeBytes(mechPrefix));
            }
            final int mechLen = getShort(mechPrefix,
                                         AUTH_MECH_PREFIX_BYTES.length);
            if (mechLen < 0) {
                throw new IOException(
                    "Invalid mechanism length received: " + mechLen);
            }

            final byte[] mechBytes = new byte[mechLen];
            result = ioAdapter.read(mechBytes);

            if (result < 0) {
                throw new IOException(
                    "EOF reading service authenticate mechanism: " +
                    result);
            }

            /*
             * The mechStr is a concatenation of mechanism name and
             * mechanism parameters, if required, separated by a ':'.
             */
            final String mechStr =
                StringUtils.fromASCII(mechBytes, 0, mechBytes.length);
            final String[] mechList = mechStr.split(":");

            /*
             * Get the authentication mechanism object based on the name.
             */

            AuthenticationMethod selectedAuth =
                findMatch(new String[] { mechList[0] },
                          authInfo);
            if (selectedAuth == null) {
                throw new IOException(
                    "Requested authentication mechanism not supported; " +
                    mechList[0]);
            }

            /*
             * Then get the client-side operation from the authentication
             * mechanism object.
             */
            authOp = selectedAuth.getClientOp(
                initState,
                mechList.length > 1 ? mechList[1] : "");

            return InitResult.DONE;
        }

        ClientInitOp getAuthOp() {
            return authOp;
        }

        /**
         * Builds an authentication request suitable for sending over to a
         * ServiceDispatcher.
         * Looks like:   Authenticate:<len><mech list>
         *
         * @return the byte encoding of the authentication request message
         */
        private byte[] serviceAuthenticateMessage() {
            final byte[] mechListBytes =
                StringUtils.toASCII(mechanisms(authInfo));
            final int length =
                AUTH_PREFIX_BYTES.length + 1 + mechListBytes.length;
            final ByteBuffer buffer = ByteBuffer.allocate(length);
            buffer.put(AUTH_PREFIX_BYTES).
                put((byte)mechListBytes.length).
                put(mechListBytes);
            return buffer.array();
        }
    }

    /**
     * Returns a comma-delimited list of authentication mechanism
     * names from the authList argument
     */
    static String mechanisms(AuthenticationMethod[] authList) {
        StringBuilder mechList = new StringBuilder();
        if (authList != null) {
            for (AuthenticationMethod auth : authList) {
                if (mechList.length() > 0) {
                    mechList.append(",");
                }
                mechList.append(auth.getMechanismName());
            }
        }
        return mechList.toString();
    }

    /**
     * Find the first Authentication instance whose mechanism name
     * matches one of the entries in mechList.
     */
    static AuthenticationMethod findMatch(String[] mechList,
                                    AuthenticationMethod[] authList) {
        /* find the first match */
        for (AuthenticationMethod auth : authList) {
            for (String mech : mechList) {
                if (mech.equals(auth.getMechanismName())) {
                    return auth;
                }
            }
        }
        return null;
    }

    /**
     * The base of all authentication implementations.
     */
    public interface AuthenticationMethod {
        String getMechanismName();
        String getServerParams();
        ServerInitOp getServerOp(ServerHandshake initState);
        ClientInitOp getClientOp(ClientHandshake initState, String params);
    }

    /**
     * NoMatchAuthenticationOp communicates that none of the proposed
     * authentication mechanisms available on the client are available on
     * the dispatcher.
     */
    static class NoMatchAuthentication implements AuthenticationMethod {

        /* The indicator for the no available authentication method. */
        static final String MECHANISM = "NoMatch";

        @Override
        public String getMechanismName() {
            return MECHANISM;
        }

        @Override
        public ClientInitOp getClientOp(ClientHandshake initIgnored,
                                        String paramsIgnored) {
            /* This should never be called */
            return null;
        }

        @Override
        public String getServerParams() {
            return "";
        }

        @Override
        public ServerInitOp getServerOp(ServerHandshake initState) {
            return new NoMatchAuthenticateOp(initState);
        }

        static class NoMatchAuthenticateOp extends ServerInitOp {

            NoMatchAuthenticateOp(ServerHandshake initState) {
                super(initState);
            }

            @Override
            protected InitResult processOp(DataChannel channel) throws IOException {
                /* This always fails */
                sendBuffer(channel, Response.INVALID.byteBuffer());
                return InitResult.FAIL;
            }
        }
    }

    /**
     * A simple interface providing simple blocking I/O.
     */
    public interface IOAdapter {
        /**
         * Read fully into buf
         * @return the number of bytes read
         */
        int read(byte[] buf) throws IOException;

       /**
         * Write buf fully
         * @return the number of bytes wrtten
         */
        int write(byte[] buf) throws IOException;
    }

    /**
     * Implementation of IOAdapter based on a ByteChannel.
     */
    static class ByteChannelIOAdapter implements IOAdapter {
        private final ByteChannel channel;

        ByteChannelIOAdapter(ByteChannel channel) {
            this.channel = channel;
        }
        @Override
        public int read(byte[] buf) throws IOException {
            return channel.read(ByteBuffer.wrap(buf));
        }
        @Override
        public int write(byte[] buf) throws IOException {
            return channel.write(ByteBuffer.wrap(buf));
        }
    }

    /**
     * Implementation of IOAdapter based on a pair of streams.
     */
    static class IOStreamIOAdapter implements IOAdapter {
        private final DataInputStream dataInputStream;
        private final OutputStream outputStream;

        IOStreamIOAdapter(InputStream input, OutputStream output) {
            this.dataInputStream = new DataInputStream(input);
            this.outputStream = output;
        }

        @Override
        public int read(byte[] buf) throws IOException {
            dataInputStream.readFully(buf);
            return buf.length;
        }

        @Override
        public int write(byte[] buf) throws IOException {
            outputStream.write(buf);
            outputStream.flush();
            return buf.length;
        }
    }

    /**
     * Check whether the contents of two arrays are equal.
     *
     * @param array1 the first array to compare - must be non-null
     * @param array2 the second array to compare - must be non-null
     * @param len the number of bytes to compare - must be less than or
     *        equal to the length of the shorter of array1, array2
     * @return true if the first "len" bytes of the arrays are equal
     */
    private static boolean arraysEqual(byte[] array1, byte[] array2, int len) {
        for (int i = 0; i < len; i++) {
            if (array1[i] != array2[i]) {
                return false;
            }
        }
        return true;
    }

    private static String encodeBytes(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    /**
     * Write a short to a ByteBuffer, in network byte order.
     */
    private static void putShort(ByteBuffer buf, short i) {
        byte b = (byte) ((i >> 8) & 0xff);
        buf.put(b);
        b = (byte) ((i >> 0) & 0xff);
        buf.put(b);
    }

    /**
     * Read a short from a byte array, in network byte order.
     */
    private static short getShort(byte[] buf, int off) {
        return (short) (((buf[off] & 0xFF) << 8) +
                        ((buf[off+1] & 0xFF) << 0));
    }

}
