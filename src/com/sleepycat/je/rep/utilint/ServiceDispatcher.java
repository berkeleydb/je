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

import static com.sleepycat.je.rep.impl.RepParams.BIND_INADDR_ANY;
import static com.sleepycat.je.rep.impl.RepParams.SO_BIND_WAIT_MS;
import static com.sleepycat.je.rep.impl.RepParams.SO_REUSEADDR;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.TextProtocol;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.subscription.ServerAuthMethod;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.utilint.ServiceHandshake.AuthenticationMethod;
import com.sleepycat.je.rep.utilint.ServiceHandshake.ByteChannelIOAdapter;
import com.sleepycat.je.rep.utilint.ServiceHandshake.ClientHandshake;
import com.sleepycat.je.rep.utilint.ServiceHandshake.InitResult;
import com.sleepycat.je.rep.utilint.ServiceHandshake.ServerHandshake;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StoppableThreadFactory;

/**
 * ServiceDispatcher listens on a specific socket for service requests
 * and dispatches control to the service that is being requested. A service
 * request message has the format:
 *
 * Service:<one byte ServiceName.length><ServiceName>
 *
 * The format of the message is binary, with all text being encoded in ascii.
 *
 * Upon receipt of service request message, the new SocketChannel is queued for
 * processing by the service in the Queue associated with the service. The
 * SocketChannel is the responsibility of the service after this point. It can
 * configure the channel to best suit the requirements of the specific service.
 *
 * The dispatcher returns a single byte to indicate success or failure. The
 * byte value encodes a ServiceDispatcher.Response enumerator.
 *
 */
public class ServiceDispatcher extends StoppableThread {

    /* The socket on which the dispatcher is listening */
    private InetSocketAddress socketAddress;

    /*
     * The selector that watches for accept events on the server socket and
     * on subsequent read events.
     */
    private final Selector selector;
    private SelectionKey scKey;

    /* The server socket channel */
    private ServerSocketChannel serverChannel;

    /* Determines whether new connections should be accepted. */
    private boolean processAcceptRequests = true;

    /* Maintains the error count, used primarily for testing. */
    private int errorCount = 0;

    /*
     * Maps the service name to the queue of sockets processed by the
     * service.
     */
    private final Map<String, Service> serviceMap =
        new ConcurrentHashMap<String, Service>();

    /* The thread pool used to manage the threads used by services */
    private final ExecutorService pool;

    private final Logger logger;
    private final Formatter formatter;

    /*
     * A reference to a replicated environment, only used for error
     * propagation when this dispatcher has been created for a replicated
     * node.
     */
    private final RepImpl repImpl;

    private final DataChannelFactory channelFactory;

    private AuthenticationMethod[] authOptions;

    /**
     * The response to a service request.
     *
     * Do not rearrange the order of the enumerators, since their ordinal
     * values are currently used in messages.
     */
    public static enum Response {

        OK, BUSY, FORMAT_ERROR, UNKNOWN_SERVICE, PROCEED, INVALID, AUTHENTICATE;

        ByteBuffer byteBuffer() {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put((byte)ordinal());
            buffer.flip();
            return buffer;
        }

        public static Response get(int ordinal) {
            if (ordinal < values().length) {
                return values()[ordinal];
            }
            return null;
        }
    }

    /**
     * Create a ServiceDispatcher listening on a specific socket for service
     * requests. This service dispatcher has been created on behalf of a
     * replicated environment, and the node will be informed of any unexpected
     * failures seen by the dispatcher.
     *
     * @param socketAddress the socket on which it listens for service
     * requests. This address may be extended to cover all local addresses, if
     * {@link RepParams#BIND_INADDR_ANY} has been set to true.
     *
     * @throws IOException if the socket could not be bound.
     */
    public ServiceDispatcher(InetSocketAddress socketAddress,
                             RepImpl repImpl,
                             DataChannelFactory channelFactory)
        throws IOException {

        super(repImpl, "ServiceDispatcher-" + socketAddress.getHostName() +
                       ":" + socketAddress.getPort());

        this.repImpl = repImpl;
        this.socketAddress = socketAddress;
        this.channelFactory = channelFactory;
        selector = Selector.open();

        String poolName = "ServiceDispatcherPool";
        NameIdPair nameIdPair = NameIdPair.NULL;
        if (repImpl == null) {
            logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        } else {
            logger = LoggerUtils.getLogger(getClass());
            nameIdPair = repImpl.getNameIdPair();
            poolName += "_" + nameIdPair;
        }

        pool = Executors.newCachedThreadPool
            (new StoppableThreadFactory(poolName, logger));

        formatter = new ReplicationFormatter(nameIdPair);

        bindSocket();

        setAuthOptions();
    }

    private void bindSocket() throws IOException {

        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        scKey = serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        ServerSocket acceptSocket = serverChannel.socket();
        /* No timeout */
        acceptSocket.setSoTimeout(0);

        InetSocketAddress bindAddress = socketAddress;
        if (repImpl != null) {
            if (repImpl.getConfigManager().getBoolean(SO_REUSEADDR)) {

                /* Only turn it on if requested. Otherwise let it default. */

                 serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR,
                                         true);

                acceptSocket.setReuseAddress(true);
            }

            if (repImpl.getConfigManager().getBoolean(BIND_INADDR_ANY)) {
                bindAddress = new InetSocketAddress((InetAddress)null,
                                                    socketAddress.getPort());
            }
        }

        final int limitMs =  (repImpl != null) ?
            repImpl.getConfigManager().getInt(SO_BIND_WAIT_MS) : 0;

        /* Bind the socket */
        BindException bindException = null;
        final int retryWaitMs = 1000;
        int totalWaitMs;
        for (totalWaitMs = 0; totalWaitMs <= limitMs;
             totalWaitMs += retryWaitMs) {
            try {
                bindException = null;
                acceptSocket.bind(bindAddress);
                break;
            } catch (BindException be) {
                bindException = be;
                try {
                    Thread.sleep(retryWaitMs);
                } catch (InterruptedException e) {
                    throw bindException;
                }
            }
        }

        if (bindException != null) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                               "ServiceDispatcher HostPort=" +
                               socketAddress.getHostName() + ":" +
                               socketAddress.getPort() +
                               " bind failed despite waiting for " +
                               limitMs + "ms");

            if (limitMs > 0) {
                /*
                 * Print information to help identify the process currently
                 * binding the required port.
                 */
                /* Print all java processes and their args */
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                                    RepUtils.exec("jps", "-v"));
                /* Print all processes binding tcp ports. */
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                                   RepUtils.exec("netstat", "-lntp"));
            }

            /* Failed after retrying. */
            throw bindException;
        } else if (totalWaitMs != 0) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                               "ServiceDispatcher HostPort=" +
                               socketAddress.getHostName() + ":" +
                               socketAddress.getPort() +
                               " become available after: " +
                               totalWaitMs + "ms");
        }
    }

    /**
     * Convenience overloading for when the dispatcher is created without a
     * replicated environment, e.g. when used by the Monitor, and in unit test
     * situations.
     *
     * @see #ServiceDispatcher(InetSocketAddress, RepImpl, DataChannelFactory)
     */
    public ServiceDispatcher(InetSocketAddress socketAddress,
                             DataChannelFactory channelFactory)
        throws IOException {

        this(socketAddress, null /* repImpl */, channelFactory);
    }

    /**
     * Stop accepting new connections, while the individual services quiesce
     * and shut themselves down.
     */
    public void preShutdown() {
        processAcceptRequests = false;
    }

    /**
     * Shuts down the dispatcher, so that it's no longer listening for service
     * requests. The port is freed up upon return and the thread used to
     * listen on the port is shutdown.
     */
    public void shutdown() {
        if (shutdownDone(logger)) {
            return;
        }

        LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                           "ServiceDispatcher shutdown starting. HostPort=" +
                           socketAddress.getHostName() + ":" +
                           + socketAddress.getPort() +
                           " Registered services: " + serviceMap.keySet());

        shutdownThread(logger);

        for (String serviceName : serviceMap.keySet()) {
            cancel(serviceName);
        }

        /* Shutdown any executing and queued service requests. */
        pool.shutdownNow();
        try {
            serverChannel.socket().close();
            selector.close();
        } catch (IOException e) {
            LoggerUtils.logMsg
                (logger, repImpl, formatter, Level.WARNING,
                 "Ignoring I/O error during close: " +
                LoggerUtils.exceptionTypeAndMsg(e));
        }
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                           "ServiceDispatcher shutdown completed." +
                           " HostPort=" + socketAddress.getHostName() +
                           ":" + socketAddress.getPort());
    }

    @Override
    protected int initiateSoftShutdown() {
        selector.wakeup();
        return 0;
    }

    /**
     * @see StoppableThread#getLogger
     */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Logging interface for use by ServiceHandshake code.
     */
    void logMsg(Level level, boolean noteError, String msg) {
        if (noteError) {
            errorCount++;
        }
        LoggerUtils.logMsg(logger, repImpl, formatter, level, msg);
    }

    /**
     * Used by the client to set up a channel for the service. It performs the
     * initial handshake requesting the service and interprets the response to
     * determine if it was successful.
     *
     * @param channel the channel that is the basis for the service
     * @param serviceName the service running on the channel
     *
     * @throws IOException if the output stream could not be established
     * @throws ServiceConnectFailedException if the connection could not be
     * made.
     */
    static public void doServiceHandshake(DataChannel channel,
                                          String serviceName)
        throws IOException, ServiceConnectFailedException {

        doServiceHandshake(channel, serviceName, null);
    }

    /**
     * A variation on the method above. It's used by the client to setup a
     * channel for the service. It performs the initial handshake requesting
     * the service and interpreting the response to determine if it was
     * successful.
     *
     * @param channel the channel that is the basis for the service
     * @param serviceName the service running on the channel
     * @param authInfo a list of authentication methods supported by the
     * caller.
     * @throws ServiceConnectFailedException if the connection could not be
     * made.
     */
    static public void doServiceHandshake(DataChannel channel,
                                          String serviceName,
                                          AuthenticationMethod[] authInfo)
        throws IOException, ServiceConnectFailedException {

        final ClientHandshake handshake =
            new ClientHandshake(serviceName,
                                authInfo,
                                new ByteChannelIOAdapter(channel));

        final Response response = handshake.process();
        if (response != Response.OK) {
            throw new ServiceConnectFailedException(serviceName, response);
        }
    }

    /**
     * Returns the next socketChannel created in response to a request for the
     * service. The socketChannel and the associated socket is configured as
     * requested in the arguments.
     *
     * @param serviceName the service for which the channel must be created.
     * @param blocking true if the channel must be configured to block
     * @param soTimeout the timeout for the underlying socket
     * @return the configured channel or null if there are no more channels,
     * because the service has been shut down.
     * @throws InterruptedException
     */
    public DataChannel takeChannel(String serviceName,
                                   boolean blocking,
                                   int soTimeout)
        throws InterruptedException {

        while (true) {
            Service service = serviceMap.get(serviceName);
            if (service == null) {
                throw EnvironmentFailureException.unexpectedState
                ("Service: " + serviceName + " was not registered");
            }
            if (! (service instanceof QueuingService)) {
                throw EnvironmentFailureException.unexpectedState
                ("Service: " + serviceName + " is not a queuing service");
            }
            Socket socket = null;
            DataChannel channel = null;
            try {
                channel = ((QueuingService)service).take();
                assert channel != null;

                if (channel == RepUtils.CHANNEL_EOF_MARKER) {
                    /* A pseudo channel to indicate EOF, return null */
                    return null;
                }

                channel.getSocketChannel().configureBlocking(blocking);

                socket = channel.getSocketChannel().socket();
                socket.setSoTimeout(soTimeout);
                if (blocking) {

                    /*
                     * Ensure that writes have been flushed.  All message
                     * exchanges should be complete here, and we don't expect
                     * there to be any pending writes, but do this here in
                     * blocking mode to ensure that the writes complete without
                     * the need for a loop.
                     */
                    channel.flush();
                }

                return channel;
            } catch (IOException e) {
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                                   "Unable to configure channel " +
                                   "for '" + serviceName + "' service: " +
                                   LoggerUtils.exceptionTypeAndMsg(e));
                try {
                    channel.close();
                } catch (IOException e1) {
                    LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINEST,
                                       "Cleanup failed for service: " +
                                       serviceName + "\n" +
                                       LoggerUtils.exceptionTypeAndMsg(e1));
                }
                /* Wait for the next request. */
                continue;
            }
        }
    }

    /**
     * Returns the specific socket address associated with the dispatcher.
     * Unlike getSocketBoundAddress() it can never return the wild card address
     * INADDR_ANY. This is the address used by clients requesting
     * ServiceDispatcher services.
     *
     * If {@link RepParams#BIND_INADDR_ANY} has been set to true, this is one
     * of the addresses that the socket is associated with.
     *
     * @see #getSocketBoundAddress()
     */
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    /**
     * For testing only.
     *
     * Returns the server socket address that was actually used to bind the
     * socket. It's the wildcard address INADDR_ANY if the HA config {@link
     * RepParams#BIND_INADDR_ANY} has been set to true.
     */
    public InetAddress getSocketBoundAddress() {
        return serverChannel.socket().getInetAddress();
    }

    /**
     * Registers a service queue with the ServiceDispatcher. Requests for a
     * service result in a new SocketChannel being created on which the service
     * can communicate with the requester of the service.
     *
     * @param serviceName the name of the service being requested
     * @param serviceQueue the queue that will be used to hold channels
     * established for the service.
     */
    public void register(String serviceName,
                         BlockingQueue<DataChannel> serviceQueue) {
        if (serviceName == null) {
            throw EnvironmentFailureException.unexpectedState
                ("The serviceName argument must not be null");
        }
        if (serviceMap.containsKey(serviceName)) {
            throw EnvironmentFailureException.unexpectedState
                ("Service: " + serviceName + " is already registered");
        }
        if (serviceQueue == null) {
            throw EnvironmentFailureException.unexpectedState
                ("The serviceQueue argument must not be null");
        }
        serviceMap.put(serviceName,
                       new QueuingService(serviceName, serviceQueue));
    }

    public void register(Service service) {
        if (service == null) {
            throw EnvironmentFailureException.unexpectedState
                ("The service argument must not be null");
        }

        if (serviceMap.containsKey(service.name)) {
            throw EnvironmentFailureException.unexpectedState
                ("Service: " + service.name + " is already registered");
        }
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                           "Service: " + service.name + " registered.");
        serviceMap.put(service.name, service);
    }

    public boolean isRegistered(String serviceName) {
        if (serviceName == null) {
            throw EnvironmentFailureException.unexpectedState
                ("The serviceName argument must not be null");
        }
        return serviceMap.containsKey(serviceName);
    }

    public void setSimulateIOException(String serviceName,
                                       boolean simulateException) {

        Service service = serviceMap.get(serviceName);
        if (service == null) {
            throw new IllegalStateException
                ("Service: " + serviceName + " is not registered");
        }

        service.setSimulateIOException(simulateException);
    }

    /**
     * Cancels the registration of a service. Subsequent attempts to access the
     * service will be ignored and the channel will be closed and will not be
     * queued.
     *
     * @param serviceName the name of the service being cancelled
     */
    public void cancel(String serviceName) {
        if (serviceName == null) {
            throw EnvironmentFailureException.unexpectedState
                ("The serviceName argument must not be null.");
        }
        Service service = serviceMap.remove(serviceName);

        if (service == null) {
            throw EnvironmentFailureException.unexpectedState
                ("Service: " + serviceName + " was not registered.");
        }
        service.cancel();
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                           "Service: " + serviceName + " shut down.");
    }

    public DataChannelFactory getChannelFactory() {
        return channelFactory;
    }

    /**
     * For testing purposes
     */
    void addTestAuthentication(AuthenticationMethod[] authOpts) {
        authOptions = authOpts;
    }

    /**
     * Sets authentication methods to service dispatcher
     */
    private void setAuthOptions() {

        if (repImpl == null) {
            authOptions = null;
            return;
        }

        final StreamAuthenticator auth = repImpl.getAuthenticator();
        if (auth == null) {
            /* no authenticator, no auth methods */
            authOptions = null;
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                               "No server auth method");
        } else {
            final AuthenticationMethod method = new ServerAuthMethod(auth);
            authOptions = new AuthenticationMethod[]{method};
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                               "Server auth method: " +
                               method.getMechanismName());
        }
    }

    /**
     * Processes an accept event on the server socket. As a result of the
     * processing a new socketChannel is created, and the selector is
     * registered with the new channel so that it can process subsequent read
     * events.
     */
    private void processAccept() {

        SocketChannel socketChannel = null;
        try {
            socketChannel = serverChannel.accept();

            if (!processAcceptRequests) {
                closeChannel(socketChannel);
                return;
            }
            socketChannel.configureBlocking(false);
            final DataChannel dataChannel =
                getChannelFactory().acceptChannel(socketChannel);

            /*
             * If authenticationMethod is provided, use it. Otherwise if no
             * authenticationMethods are provided but the channel is capable
             * of determining trust, pass an empty Authentication
             * array to ServerHandshake in order to trigger the trust check.
             */
            final AuthenticationMethod[] authInfo =
                (dataChannel.isTrustCapable() && authOptions == null) ?
                new AuthenticationMethod[0] :
                authOptions;

            final ServerHandshake initState =
                new ServerHandshake(dataChannel, this, authInfo);

            /* Register the selector with the base SocketChannel */
            socketChannel.register(selector, SelectionKey.OP_READ, initState);
        } catch (IOException e) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                               "Server accept exception: " +
                                LoggerUtils.exceptionTypeAndMsg(e));
            closeChannel(socketChannel);
        }
    }

    /**
     * Processes read events on newly established socket channels. Input on the
     * channel is verified to ensure that it is a service request. The read is
     * accomplished in two parts, a read for the fixed size prefix and the name
     * length byte, followed by a read of the variable length name itself.
     *
     * Errors result in the channel being closed(with the key being canceled
     * as a result) and a null value being returned.
     *
     * If the service request is sane, we may require the connecting
     * entity to authenticate itself.
     *
     * @param initState the InitState object associated with the new channel
     *
     * @return the ServiceName or null if there was insufficient input, or an
     * error was encountered.
     */
    private String processRead(ServerHandshake initState) {
        try {
            final InitResult result = initState.process();
            if (result == InitResult.FAIL) {
                /* Probably already closed, but make sure */
                initState.getChannel().close();
                return null;
            }
            if (result == InitResult.REJECT) {
                initState.getChannel().write(Response.INVALID.byteBuffer());
                initState.getChannel().close();
                return null;
            }

            if (result == InitResult.DONE) {
                return initState.getServiceName();
            }
            /* Initial sequence not complete as yet, keep reading */
            return null;
        } catch (IOException e) {
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                               "Exception during read: " +
                                LoggerUtils.exceptionTypeAndMsg(e));
            closeChannel(initState.getChannel());
            return null;
        }
    }

    /**
     * Closes the channel, logging any resulting exceptions.
     *
     * @param channel the channel being closed
     */
    private void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e1) {
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                                   "Exception during cleanup: " +
                                   LoggerUtils.exceptionTypeAndMsg(e1));
            }
        }
    }

    /**
     * The central run method. It dispatches to the "accept" and "read" event
     * processing methods. Upon a completed read, it verifies the validity of
     * the service name and queues the channel for subsequent consumption
     * by the service.
     *
     */
    @Override
    public void run() {
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                           "Started ServiceDispatcher. HostPort=" +
                           socketAddress.getHostName() + ":" +
                           socketAddress.getPort());
        LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                           "DataChannel factory: " +
                           getChannelFactory().getClass().getName());
        try {
            while (true) {
                try {
                    /**
                     * To make the dispatcher resilient to IP address change,
                     * we periodically check for such change and rebind the
                     * socket if that occurs.
                     *
                     * Speculation and rational:
                     * New communications fail sliently which is possibly
                     * caused by that each TCP session use both IP and port
                     * number as the identifier. Thus it will drop packages
                     * after IP address change. Yet no exception is raised in
                     * that situation. Therefore, we cannot rely on exception
                     * for detecting such a change, but instead use an active
                     * approach.
                     */
                    boolean changed = false;
                    try {
                        changed = ipChanged();
                    } catch (Exception e) {
                        LoggerUtils.logMsg
                            (logger, repImpl, formatter, Level.INFO,
                             "Exception while check IP: " +
                             LoggerUtils.exceptionTypeAndMsg(e));
                    }
                    if (changed) {
                        rebindSocket();
                    }
                    final int result = selector.select(1000);
                    if (isShutdown()) {
                        return;
                    }
                    if (result == 0) {
                        continue;
                    }
                } catch (Exception e) {
                    LoggerUtils.logMsg
                        (logger, repImpl, formatter, Level.SEVERE,
                         "Server socket exception: " +
                        LoggerUtils.getStackTrace(e));
                    throw EnvironmentFailureException.unexpectedException(e);
                }
                Set<SelectionKey> skeys = selector.selectedKeys();
                for (SelectionKey key : skeys) {
                    switch (key.readyOps()) {

                        case SelectionKey.OP_ACCEPT:
                            processAccept();
                            break;

                        case SelectionKey.OP_READ:
                            final ServerHandshake initState =
                                (ServerHandshake) key.attachment();

                            final String serviceName = processRead(initState);
                            if (serviceName == null) {
                                break;
                            }
                            key.cancel();
                            processService(initState.getChannel(), serviceName);
                            break;

                        default:
                            throw EnvironmentFailureException.unexpectedState
                                ("Unexpected ops bit set: " + key.readyOps());
                    }
                }
                /* All keys have been processed clear them. */
                skeys.clear();
            }
        } finally {
            /*
             * Clean up any in-process connections that are still in the
             * handshake phase.
             */
            Iterator<SelectionKey> skIter = selector.keys().iterator();
            while (skIter.hasNext()) {
                SelectionKey key = skIter.next();
                final ServerHandshake initState =
                    (ServerHandshake) key.attachment();
                if (initState != null) {
                    LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                                       "Server closing in-process handshake");
                    closeChannel(initState.getChannel());
                    key.cancel();
                }
            }
            closeChannel(serverChannel);
            cleanup();
        }
    }

    private boolean ipChanged() throws Exception {
        if (repImpl == null) {
            return false;
        }
        InetAddress addr = InetAddress.getByName(repImpl.getHostName());
        String currentIP = addr.getHostAddress();
        String previousIP = socketAddress.getAddress().getHostAddress();
        boolean changed = !currentIP.equals(previousIP);
        if (changed) {
            LoggerUtils.logMsg
                (logger, repImpl, formatter, Level.INFO,
                 "ServiceDispatcher IP changed, from " + previousIP +
                 " to " + currentIP);
        }
        return changed;
    }

    private void rebindSocket() throws IOException {
        if (repImpl == null) {
            return;
        }
        scKey.cancel();
        serverChannel.close();
        socketAddress = repImpl.getSocket();

        bindSocket();

        LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                "Rebind ServiceDispatcher socket: " + serverChannel.socket());
    }

    /**
     * Performs the guts of the work underlying a service request. It validates
     * the service request and writes an appropriate response to the channel.
     * @param channel
     * @param serviceName
     */
    private void processService(DataChannel channel, String serviceName) {
        final Service service = serviceMap.get(serviceName);
        try {
            if (service == null) {
                errorCount++;
                channel.write(Response.UNKNOWN_SERVICE.byteBuffer());
                closeChannel(channel);
                /*
                 * Not unexpected in a distributed app due to calls being made
                 * before a service is actually registered.
                 */
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                                   "Request for unknown Service: " +
                                   serviceName + " Registered services: " +
                                   serviceMap.keySet());
                return;
            }
            Response response = Response.OK;
            if (service.isBusy()) {
                response = Response.BUSY;
            }
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                               "Service response: " + response +
                               " for service: " + service.name);

            if (channel.write(response.byteBuffer()) == 0) {
                throw EnvironmentFailureException.unexpectedState
                    ("Failed to write byte. Send buffer size: " +
                     channel.getSocketChannel().socket().getSendBufferSize());
            }
            if (response == Response.OK) {
                service.requestDispatch(channel);
            }
        } catch (IOException e) {
            closeChannel(channel);
            LoggerUtils.logMsg(logger, repImpl, formatter, Level.WARNING,
                               "IO error writing to channel for " +
                               "service: " +  serviceName +
                               LoggerUtils.exceptionTypeAndMsg(e));
        }
    }

    /**
     * The abstract class underlying all services.
     */
    static private abstract class Service {

        /* The name associated with the service. */
        final String name;

        private boolean simulateIOException = false;

        public Service(String name) {
            super();
            if (name == null) {
                throw EnvironmentFailureException.unexpectedState
                    ("Service name was null");
            }
            this.name = name;
        }

        /**
         * Informs the service of a new request. The implementation of the
         * method must not block.
         *
         * @param channel the channel on which the request was made
         */
        abstract void requestDispatch(DataChannel channel);

        /**
         * Used to limit a particular type of service to avoid excess load.
         */
        public boolean isBusy() {
            return false;
        }

        /**
         * Used during unit testing to simulate communications problems.
         */
        public boolean simulateIOException() {
            return simulateIOException;
        }

        public void setSimulateIOException(boolean simulateIOException) {
            this.simulateIOException = simulateIOException;
        }

        /**
         * Cancel the service as part of the registration being canceled.
         */
        abstract void cancel();
    }

    /**
     * A service where requests are simply added to the supplied queue. It's
     * the responsibility of the service creator to drain the queue. This
     * service is used when the service carries out a long-running dialog with
     * the service requester. For example, a Feeder service.
     */
    public class QueuingService extends Service {
        /* Holds the queue of pending requests, one per channel */
        private final BlockingQueue<DataChannel> queue;

        QueuingService(String serviceName,
                       BlockingQueue<DataChannel> queue) {
            super(serviceName);
            this.queue = queue;
        }

        DataChannel take() throws InterruptedException {
            return queue.take();
        }

        @Override
        void requestDispatch(DataChannel channel) {
            if (simulateIOException()) {
                LoggerUtils.logMsg(logger, repImpl, formatter, Level.INFO,
                                   "Simulated test IO exception");
                try {
                    /*
                     * This will provoke an IOException later when we try to
                     * use the channel in takeChannel().
                     */
                    channel.close();
                } catch (IOException e) {
                    LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINEST,
                                       "Close failure in '" + name +
                                       "' service: " +
                                       LoggerUtils.exceptionTypeAndMsg(e));
                }
            }
            if (!queue.add(channel)) {
                throw EnvironmentFailureException.unexpectedState
                    ("request queue overflow");
            }
        }

        @Override
        void cancel() {
            /*
             * Drain any existing pending requests. It's safe to just iterate
             * since the service dispatcher has already stopped accepting new
             * requests for the service.
             */
            for (DataChannel channel : queue) {
                try {
                    channel.close();
                } catch (IOException e) {
                    // Ignore it, it's only cleanup
                }
            }
            queue.add(RepUtils.CHANNEL_EOF_MARKER);
        }
    }

    /**
     * A queuing service that starts the thread that services the requests
     * lazily, upon first request and terminates the thread when the service is
     * unregistered. The thread must be "interrupt aware" and must exit when
     * it receives an interrupt.
     *
     * This type of service is suitable for services that are used
     * infrequently.
     */
    public class LazyQueuingService extends QueuingService {

        private final Thread serviceThread;

        public LazyQueuingService(String serviceName,
                                  BlockingQueue<DataChannel> queue,
                                  Thread serviceThread) {

            super(serviceName, queue);
            this.serviceThread = serviceThread;
        }

        @Override
        void requestDispatch(DataChannel channel) {

            switch (serviceThread.getState()) {

                case NEW:
                    serviceThread.start();
                    LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                                       "Thread started for service: " + name);
                    break;

                case RUNNABLE:
                case TIMED_WAITING:
                case WAITING:
                case BLOCKED:
                    /* Was previously activated. */
                    LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                                       "Thread started for service: " + name);
                    break;

                default:
                    RuntimeException e =
                        EnvironmentFailureException.unexpectedState
                            ("Thread for service:" + name +
                             "is in state:" + serviceThread.getState());
                    LoggerUtils.logMsg(logger, repImpl, formatter,
                                       Level.WARNING,
                                       LoggerUtils.exceptionTypeAndMsg(e));
                    throw e;
            }
            super.requestDispatch(channel);
        }

        @Override
        /**
         * Interrupts the thread to cause it to exit.
         */
        void cancel() {
            if (serviceThread.isAlive()) {
                serviceThread.interrupt();
                try {
                    serviceThread.join();
                } catch (InterruptedException e) {
                    /* Ignore it on shutdown. */
                }
            }
            super.cancel();
        }
    }

    /**
     * A service that is run immediately in a thread allocated to it. Subtypes
     * implement the getRunnable() method which provides the runnable object
     * for the service. This service frees up the caller from managing the the
     * threads associated with the service. The runnable must manage interrupts
     * so that it can be shut down by the underlying thread pool.
     */
    static public abstract class ExecutingService extends Service {
        final private ServiceDispatcher dispatcher;

        public ExecutingService(String serviceName,
                                ServiceDispatcher dispatcher) {
            super(serviceName);
            this.dispatcher = dispatcher;
        }

        public abstract Runnable getRunnable(DataChannel channel);

        @Override
        void requestDispatch(DataChannel channel) {
            dispatcher.pool.execute(getRunnable(channel));
        }

        @Override
        protected void cancel() {
            /* Nothing to do */
        }
    }

    @SuppressWarnings("serial")
    static public class ServiceConnectFailedException extends Exception {
        final Response response;
        final String serviceName;

        ServiceConnectFailedException(String serviceName,
                                      Response response) {
            assert(response != Response.OK);
            this.response = response;
            this.serviceName = serviceName;
        }

        public Response getResponse() {
            return response;
        }

        @Override
        public String getMessage() {
            switch (response) {
                case FORMAT_ERROR:
                    return "Bad message format, for service:" + serviceName;

                case UNKNOWN_SERVICE:
                    return "Unknown service request:" + serviceName;

                case BUSY:
                    return "Service was busy";

                case INVALID:
                    return "Invalid response supplied";

                case PROCEED:
                    return "Protocol continuation requested";

                case AUTHENTICATE:
                    return "Authentication required";

                case OK:
                    /*
                     * Don't expect an OK response to provoke an exception.
                     * Fall through.
                     */
                default:
                    throw EnvironmentFailureException.unexpectedState
                        ("Unexpected response:" + response +
                         " for service:" + serviceName);
            }
        }
    }

    abstract public static class ExecutingRunnable implements Runnable {
        protected final DataChannel channel;
        protected final TextProtocol protocol;
        protected final boolean expectResponse;

        public ExecutingRunnable(DataChannel channel,
                                 TextProtocol protocol,
                                 boolean expectResponse) {
            this.channel = channel;
            this.protocol = protocol;
            this.expectResponse = expectResponse;
        }

        /* Read request and send out response. */
        @Override
        public void run() {
            try {
                channel.getSocketChannel().configureBlocking(true);
                RequestMessage request = protocol.getRequestMessage(channel);
                if (request == null) {
                    return;
                }
                ResponseMessage response = getResponse(request);
                if (expectResponse && response != null) {
                    PrintWriter out = new PrintWriter
                        (Channels.newOutputStream(channel), true);
                    out.println(response.wireFormat());
                } else {
                    assert (response == null);
                }
            } catch (IOException e) {
                logMessage("IO error on socket: " +
                            LoggerUtils.exceptionTypeAndMsg(e));
                return;
            } finally {
                if (channel.isOpen()) {
                    try {
                        channel.close();
                    } catch (IOException e) {
                        logMessage("IO error on socket close: " +
                                   LoggerUtils.exceptionTypeAndMsg(e));
                         return;
                    }
                }
            }
        }

        /* Get the response for a request. */
        abstract protected ResponseMessage getResponse(RequestMessage request)
            throws IOException;

        /* Log the message. */
        abstract protected void logMessage(String message);
    }
}
