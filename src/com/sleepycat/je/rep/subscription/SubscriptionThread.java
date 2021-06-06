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

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.ChannelTimeoutTask;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThread;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshake;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshakeConfig;
import com.sleepycat.je.rep.stream.SubscriberFeederSyncup;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.NamedChannelWithTimeout;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * Main thread created by Subscription to stream log entries from feeder
 */
class SubscriptionThread extends StoppableThread {
    
    private final Logger logger;
    private final SubscriptionConfig config;
    private final SubscriptionStat stats;

    /* communication queues and working threads */
    private final BlockingQueue<Long> outputQueue;
    private final BlockingQueue<Object> inputQueue;
    private SubscriptionProcessMessageThread messageProcThread;

    /* communication channel between subscriber and feeder */
    private NamedChannelWithTimeout namedChannel;
    /* task to register channel with timeout */
    private ChannelTimeoutTask channelTimeoutTask;
    /* protocol used to communicate with feeder */
    private Protocol protocol;

    /* requested VLSN from which to stream log entries */
    private final VLSN reqVLSN;

    /*
     * volatile because it can be concurrently accessed by the subscription
     * thread itself in checkOutputThread(), and another thread trying to
     * shut down subscription by calling shutdown()
     */
    private volatile SubscriptionOutputThread outputThread;

    private volatile SubscriptionStatus status;

    /* stored exception */
    private volatile Exception storedException;

    /*
     * For unit test only. The hook will be called by unit test to inject an
     * exception into msg queue, which to be processed by the callback function
     * defined in unit test.
     */
    private TestHook<SubscriptionThread> exceptionHandlingTestHook;

    SubscriptionThread(ReplicatedEnvironment env,
                       VLSN reqVLSN,
                       SubscriptionConfig config,
                       SubscriptionStat stats,
                       Logger logger) {

        super(RepInternal.getNonNullRepImpl(env), "Subscription Main");
        setUncaughtExceptionHandler(new SubscriptionThreadExceptionHandler());
        
        this.reqVLSN = reqVLSN;
        this.config = config;
        this.stats = stats;
        this.logger = logger;
        protocol = null;
        namedChannel = null;
        /* init subscription input and output queue */
        inputQueue =
            new ArrayBlockingQueue<>(config.getInputMessageQueueSize());
        outputQueue =
            new ArrayBlockingQueue<>(config.getOutputMessageQueueSize());

        status = SubscriptionStatus.INIT;
        storedException = null;
        exceptionHandlingTestHook = null;
    }

    /**
     * Returns subscription status to client
     *
     * @return subscription status
     */
    public SubscriptionStatus getStatus() {
        return status;
    }

    /**
     * Returns stored exception
     *
     * @return stored exception
     */
    public Exception getStoredException() {
        return storedException;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void run() {

        LoggerUtils.info(logger, envImpl,
                         "Start subscription from VLSN " + reqVLSN +
                         " from feeder at " +
                         config.getFeederHost() + ":" + config.getFeederPort());

        try {
            final int maxRetry = config.getMaxConnectRetries();
            boolean auxThreadCreated = false;
            int numRetry = 0;

            while (!isShutdown()) {
                try {
                    initializeConnection();
                    if (!auxThreadCreated) {
                        LoggerUtils.fine(logger, envImpl,
                                         "Create auxiliary msg processing " +
                                         "and output threads");

                        auxThreadCreated = createAuxThread();
                        if (auxThreadCreated) {
                            /* subscription succeed, start streaming data */
                            status = SubscriptionStatus.SUCCESS;
                            loopInternal();
                        } else {
                            status = SubscriptionStatus.UNKNOWN_ERROR;
                        }
                    }
                    break;
                } catch (ConnectionException e) {
                    if (numRetry == maxRetry) {
                        LoggerUtils.info(logger, envImpl,
                                         "Reaching the max retry " + maxRetry +
                                         " to connect feeder " +
                                         config.getFeederHost() +
                                         ", shut down subscription" +
                                         "\n" + LoggerUtils.getStackTrace(e));
                        storedException = e;
                        status = SubscriptionStatus.CONNECTION_ERROR;
                        break;
                    } else {
                        numRetry++;
                        LoggerUtils.fine(logger, envImpl,
                                         "Fail to connect feeder at " +
                                         config.getFeederHost() +
                                         " sleep for " + e.getRetrySleepMs() +
                                         " ms and re-connect again");
                        Thread.sleep(e.getRetrySleepMs());
                    }
                }
            }
        } catch (ReplicationSecurityException ure) {
            storedException = ure;
            LoggerUtils.warning(logger, envImpl,
                                "Subscription exited due to security check " +
                                "failure: " + ure.getMessage());
            status = SubscriptionStatus.SECURITY_CHECK_ERROR;
        } catch (GroupShutdownException e) {
            if (messageProcThread.isAlive()) {
                try {
                    /* let message processing thread finish up */
                    messageProcThread.join();
                } catch (InterruptedException ie) {
                    /* ignore since we will shut down, just log */
                    LoggerUtils.fine(logger, envImpl,
                                     "exception in shutting down msg proc " +
                                     "thread " + ie.getMessage() +
                                     "\n" + LoggerUtils.getStackTrace(ie));
                }
            }
            storedException = e;
            LoggerUtils.info(logger, envImpl,
                             "received group shutdown " + e.getMessage() +
                             "\n" + LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.GRP_SHUTDOWN;
        } catch (InsufficientLogException e) {
            storedException = e;
            LoggerUtils.info(logger, envImpl,
                             "unable to subscribe from requested VLSN " +
                             reqVLSN +
                             "\n" + LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.VLSN_NOT_AVAILABLE;
        } catch (EnvironmentFailureException e) {
            storedException = e;
            LoggerUtils.warning(logger, envImpl,
                                "unable to sync up with feeder due to EFE " +
                                e.getMessage() +
                                "\n" + LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } catch (InterruptedException e) {
            storedException = e;
            LoggerUtils.warning(logger, envImpl,
                                "interrupted exception " + e.getMessage() +
                                "\n" + LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } catch (InternalException e) {
            storedException = e;
            LoggerUtils.warning(logger, envImpl,
                                "internal exception " + e.getMessage() +
                                "\n" + LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } finally {
            shutdown();
        }
    }

    /**
     * For unit test
     *
     * @param exceptionHandlingTestHook test hook
     */
    void setExceptionHandlingTestHook(
        TestHook<SubscriptionThread> exceptionHandlingTestHook) {
        this.exceptionHandlingTestHook = exceptionHandlingTestHook;
    }

    /**
     * Sets subscription status
     *
     * @param s subscription status
     */
    void setStatus(SubscriptionStatus s) {
        status = s;
    }

    /**
     * shutdown the subscriber and all auxiliary threads, close channel to
     * the Feeder.
     */
    void shutdown() {

        /* Note start of shutdown and return if already requested */
        if (shutdownDone(logger)) {
            return;
        }

        /* shutdown aux threads */
        if (messageProcThread != null) {
            try {
                messageProcThread.shutdownThread(logger);
                LoggerUtils.info(logger, envImpl,
                                 "message processing thread has shut down.");
            } catch (Exception e) {
                /* Ignore so shutdown can continue */
                LoggerUtils.warning(logger, envImpl,
                                    "error in shutdown msg proc thread: " +
                                    e.getMessage() + ", continue shutdown the" +
                                    " subscription thread.");
            } finally {
                messageProcThread = null;
            }
        }
        if (outputThread != null) {
            try {
                outputThread.shutdownThread(logger);
                LoggerUtils.info(logger, envImpl,
                                 "output thread has shut down.");

            } catch (Exception e) {
                /* Ignore we will clean up via killing IO channel anyway. */
                LoggerUtils.warning(logger, envImpl,
                                    "error in shutdown output thread: " +
                                    e.getMessage() + ", continue shutdown " +
                                    "subscription thread.");
            } finally {
                outputThread = null;
            }
        }

        inputQueue.clear();
        outputQueue.clear();
        RepUtils.shutdownChannel(namedChannel);
        if (channelTimeoutTask != null) {
            channelTimeoutTask.cancel();
        }

        shutdownThread(logger);

        LoggerUtils.info(logger, envImpl,
                         "queues cleared and channel closed, subscription " +
                         "thread has completely shut down");
    }

    /**
     * Enqueue message received from feeder into input queue
     *
     * @param message  message received from feeder
     *
     * @throws InterruptedException if enqueue is interrupted
     * @throws GroupShutdownException if receive shutdown message from feeder
     */
    void offer(Object message)
        throws InterruptedException, GroupShutdownException {

        RepImpl repImpl = (RepImpl)envImpl;

        while (!isShutdown() &&     /* stop enqueue msg if thread is shutdown */
               !inputQueue.offer(message,
                                 SubscriptionConfig.QUEUE_POLL_INTERVAL_MS,
                                 TimeUnit.MILLISECONDS)) {
            /* Offer timed out. */
            if (!messageProcThread.isAlive()) {
                LoggerUtils.info(logger, repImpl,
                                 "Thread consuming input queue is gone, start" +
                                 " shutdown process");
                throw new GroupShutdownException(logger, repImpl,
                                                 config.getFeederHost(),
                                                 stats.getHighVLSN(), 0);
            } else {
                /* count the overflow and retry */
                stats.getNumReplayQueueOverflow().increment();
            }
        }
    }

    /**
     * Create connection to feeder and execute handshake
     *
     * @throws InternalException if unable to connect to source node due to
     * protocol error
     * @throws EnvironmentFailureException if fail to handshake with source, or
     * source does not have enough log to start streaming
     * @throws ConnectionException if unable to connect to source node
     * @throws ReplicationSecurityException if authentication failure
     */
    private void initializeConnection() throws InternalException,
        EnvironmentFailureException, ConnectionException,
        ReplicationSecurityException {

        /* open a channel to feeder */
        LoggerUtils.fine(logger, envImpl,
                         "Subscription " + config.getSubNodeName() +
                         " start open channel and handshake with feeder");

        try {

            openChannel();
            ReplicaFeederHandshake handshake =
                new ReplicaFeederHandshake(new SubFeederHandshakeConfig
                                               (config.getNodeType()));

            protocol = handshake.execute();

            /* check if negociated protocol version is high enough */
            final int minReqVersion = config.getMinProtocolVersion();
            if (protocol.getVersion() < minReqVersion) {
                throw new BinaryProtocol.ProtocolException(
                    "HA protocol version (" + protocol.getVersion() + ") is " +
                    "lower than minimal required version (" + minReqVersion +
                    ")");
            }

            LoggerUtils.fine(logger, envImpl,
                             "subscription " + config.getSubNodeName() +
                             " sync-up with feeder at vlsn: " + reqVLSN);
            SubscriberFeederSyncup syncup =
                new SubscriberFeederSyncup(namedChannel, protocol,
                                           config.getFeederFilter(),
                                           (RepImpl) envImpl,
                                           config.getStreamMode(),
                                           logger);
            final VLSN startVLSN = syncup.execute(reqVLSN);
            LoggerUtils.fine(logger, envImpl,
                             "sync-up with feeder done, start vlsn: " +
                             startVLSN);

            if (!startVLSN.equals(VLSN.NULL_VLSN)) {

                stats.setStartVLSN(startVLSN);

                /* read heartbeat and respond */
                protocol.read(namedChannel.getChannel(),
                              Protocol.Heartbeat.class);
                queueAck(ReplicaOutputThread.HEARTBEAT_ACK);

                LoggerUtils.info(logger, envImpl,
                                 "Subscription " + config.getSubNodeName() +
                                 " successfully connect to feeder at " +
                                 config.getFeederHost() + ":" +
                                 config.getFeederPort() +
                                 ", reqVLSN: " + reqVLSN +
                                 ", start VLSN: " + startVLSN);
            } else {
                throw new InsufficientLogException((RepImpl) envImpl, reqVLSN);
            }
        } catch (IOException e) {
            throw new ConnectionException("Unable to connect due to " +
                                          e.getMessage() +
                                          ",  will retry later.",
                                          config.getSleepBeforeRetryMs(),
                                          e);
        } catch (EnvironmentFailureException e) {
            logger.warning("Fail to handshake with feeder: " +
                           e.getMessage());
            throw e;
        } catch (BinaryProtocol.ProtocolException e) {
            final String msg = ("Unable to connect to feeder " +
                                config.getFeederHost() +
                                " due to protocol exception " +
                                e.getMessage());
            LoggerUtils.warning(logger, envImpl, msg);
            throw new InternalException(msg, e);
        }
    }

    /**
     * Create auxiliary message processing and output thread
     */
    private boolean createAuxThread() {

        RepImpl repImpl = (RepImpl)envImpl;

        inputQueue.clear();
        outputQueue.clear();

        /* start output thread over data channel to send response to feeder */
        outputThread =
                new SubscriptionOutputThread(this,
                                             repImpl, outputQueue, protocol,
                                             namedChannel.getChannel(),
                                             config.getAuthenticator(), stats);
        /*
         * output thread can be shutdown and set to null anytime, thus
         * use a cached copy to ensure it is alive before start it
         */
        final SubscriptionOutputThread cachedOutputThread = outputThread;
        if (cachedOutputThread != null) {
            cachedOutputThread.start();
            LoggerUtils.fine(logger, envImpl,
                             "output thread created for subscription " +
                             config.getSubNodeName());
            /* start thread to consume data in input queue */
            messageProcThread =
                new SubscriptionProcessMessageThread(repImpl, inputQueue,
                                                     config,
                                                     stats, logger);
            messageProcThread.start();
            LoggerUtils.fine(logger, envImpl,
                             "message processing thread created for subscription " +
                             config.getSubNodeName());
            return true;
        } else {
            LoggerUtils.info(logger, envImpl,
                             "subscription " +  config.getSubNodeName() + " " +
                             "just shut down, no need to create auxiliary " +
                             "threads");
            return false;
        }
    }

    /**
     * Open a data channel to feeder
     *
     * @return created name channel
     *
     * @throws ConnectionException unable to connect due to error and need retry
     * @throws InternalException fail to handshake with feeder
     * @throws ReplicationSecurityException if unauthorized to stream
     * from feeder
     */
    private NamedChannel openChannel() throws ConnectionException,
        InternalException, ReplicationSecurityException {

        RepImpl repImpl = (RepImpl)envImpl;

        if (repImpl == null) {
            throw new IllegalStateException("Replication env is unavailable.");
        }

        try {
            DataChannelFactory.ConnectOptions connectOpts =
                new DataChannelFactory
                    .ConnectOptions()
                    .setTcpNoDelay(config.TCP_NO_DELAY)
                    .setReceiveBufferSize(config.getReceiveBufferSize())
                    .setOpenTimeout((int) config
                        .getStreamOpenTimeout(TimeUnit.MILLISECONDS))
                    .setBlocking(config.BLOCKING_MODE_CHANNEL);

            final DataChannel channel =
                repImpl.getChannelFactory()
                       .connect(config.getInetSocketAddress(), connectOpts);

            ServiceDispatcher.doServiceHandshake(channel,
                                                 FeederManager.FEEDER_SERVICE,
                                                 config.getAuthInfo());
            LoggerUtils.fine(logger, envImpl,
                             "channel opened to service " +
                             FeederManager.FEEDER_SERVICE + "@" +
                             config.getFeederHost() +
                             "[address: " + config.getFeederHostAddr() +
                             " port: " + config.getFeederPort() + "]");

            final int timeoutMs = repImpl.getConfigManager().
                getDuration(RepParams.PRE_HEARTBEAT_TIMEOUT);

            channelTimeoutTask = new ChannelTimeoutTask(new Timer(true));
            namedChannel =
                new NamedChannelWithTimeout(repImpl, logger, channelTimeoutTask,
                                            channel, timeoutMs);
        } catch (IOException cause) {
            /* retry if unable to connect to feeder */
            throw new ConnectionException("Fail to open channel to feeder " +
                                          "due to " + cause.getMessage() +
                                          ", will retry later",
                                          config.getSleepBeforeRetryMs(),
                                          cause);
        } catch (ServiceDispatcher.ServiceConnectFailedException cause) {

            /*
             * The feeder may not have established the Feeder Service
             * as yet. For example, the transition to the master may not have
             * been completed.
             */
            if (cause.getResponse() ==
                ServiceDispatcher.Response.UNKNOWN_SERVICE) {
                throw new ConnectionException("Service exception: " +
                                              cause.getMessage() +
                                              ", wait longer and will retry " +
                                              "later",
                                              config.getSleepBeforeRetryMs(),
                                              cause);
            }

            if (cause.getResponse() ==
                ServiceDispatcher.Response.INVALID) {
                throw new ReplicationSecurityException(
                    "Security check failure:"  + cause.getMessage(),
                    config.getSubNodeName(),
                    cause);
            }

            throw new InternalException("Subscription " +
                                        config.getSubNodeName() +
                                        "failed to handshake for service " +
                                        FeederManager.FEEDER_SERVICE +
                                        " with feeder " +
                                        config.getFeederHost(),
                                        cause);
        }
        LoggerUtils.info(logger, envImpl,
                         "Subscription " + config.getSubNodeName() +
                         " has successfully created a channel to feeder at " +
                         config.getFeederHost() + ":" + config.getFeederPort());

        return namedChannel;
    }

    /**
     * Internal loop to dequeue message from channel to the feeder,
     * process shutdown and heartbeat messages, and relay data operations to
     * the input queue to be consumed by input thread.
     *
     * @throws InternalException if error in reading messages from channel or
     *                           enqueue message into input queue
     * @throws GroupShutdownException if receive shutdown message from feeder
     * @throws ReplicationSecurityException if output thread exits due to
     * security check failure. In this case the main subscription need to
     * exit without retry.
     */
    private void loopInternal() throws InternalException,
        GroupShutdownException, ReplicationSecurityException {

        RepImpl repImpl = (RepImpl)envImpl;

        try {

            LoggerUtils.info(logger, envImpl,
                             "Start reading messages from feeder " +
                             config.getFeederHost() + ":" +
                             config.getFeederPort());
            while (!isShutdown()) {

                checkOutputThread();

                BinaryProtocol.Message message = protocol.read(namedChannel);

                if ((message == null)) {
                    LoggerUtils.info(logger, envImpl,
                                     "Subscription " + config.getSubNodeName() +
                                     " has nothing stream, exit loop.");
                    return;
                }

                assert TestHookExecute.doHookIfSet(exceptionHandlingTestHook,
                                                   this);

                stats.getNumMsgReceived().increment();

                BinaryProtocol.MessageOp messageOp = message.getOp();
                if (messageOp == Protocol.HEARTBEAT) {
                    LoggerUtils.finest(logger, envImpl,
                                       "receive heartbeat from " +
                                       namedChannel.getNameIdPair());
                    queueAck(ReplicaOutputThread.HEARTBEAT_ACK);
                } else if (messageOp == Protocol.SHUTDOWN_REQUEST) {

                    LoggerUtils.info(logger, envImpl,
                                     "Receive shutdown request from feeder " +
                                     config.getFeederHost() +
                                     ", shutdown subscriber");
                    
                    /*
                     * create a shutdown request, make it in the queue so
                     * client is able to see that in callback, and throw an
                     * exception.
                     *
                     * The message processing thread will exit when seeing a
                     * GroupShutdownException
                     */
                    Protocol.ShutdownRequest req =
                        (Protocol.ShutdownRequest) message;
                    Exception exp =
                        new GroupShutdownException(logger, repImpl,
                                                   config.getFeederHost(),
                                                   stats.getHighVLSN(),
                                                   req.getShutdownTimeMs());
                    offer(exp);
                    throw exp;
                } else {
                    /* a regular data entry message */
                    offer(message);

                    final long pending = inputQueue.size();
                    if (pending > stats.getMaxPendingInput().get()) {
                        stats.getMaxPendingInput().set(pending);
                        LoggerUtils.finest(logger, envImpl,
                                           "Max pending request log items:" +
                                           pending);
                    }
                }

            }
        } catch (GroupShutdownException | ReplicationSecurityException exp) {
            /* throw to caller, let caller deal with it */
            throw exp;
        } catch (Exception e) {
            /* other exception is thrown as IE */
            throw new InternalException(e.getMessage(), e);
        }
    }

    /**
     * Checks status of output thread and propagates RSE to main
     * loop. If output thread exited due to RSE, the main thread need to
     * capture it to set the subscription status correctly. For other
     * exceptions, output thread uses the traditional mechanism to notify the
     * main subscription thread: simply shut down channel.
     */
    private void checkOutputThread()
        throws InternalException, ReplicationSecurityException {

        /*
         * output thread can be shutdown and set to null anytime, thus
         * use a cached copy to avoid NPE after the first check
         */
        final SubscriptionOutputThread cachedOutputThread = outputThread;

        /* output thread already gone */
        if (cachedOutputThread == null) {
            /*
             * if output thread is set to null only when subscription thread
             * shut down. If we reach here, it means the subscription thread
             * is shut down right after isShutdown check in loopInternal().
             * We simply return and subscription thread would detect the shut
             * down in next check of isShutdown in loopInternal().
             */
            LoggerUtils.fine(logger, envImpl,
                             "output thread no longer exists");
            return;
        }

        if (cachedOutputThread.getException() instanceof
                ReplicationSecurityException) {
            final ReplicationSecurityException rse =
                (ReplicationSecurityException) cachedOutputThread.getException();
            LoggerUtils.warning(logger, envImpl,
                                "Output thread exited due to security check " +
                                "failure: " + rse.getMessage());
            throw rse;
        }
    }

    /**
     * Enqueue an ack message in output queue
     *
     * @param xid txn id to enqueue
     *
     * @throws IOException if fail to queue the msg
     */
    private void queueAck(Long xid) throws IOException {

        try {
            outputQueue.put(xid);
        } catch (InterruptedException ie) {

            /*
             * If interrupted while waiting, have the higher levels treat
             * it like an IOE and exit the thread.
             */
            throw new IOException("Ack I/O interrupted", ie);
        }
    }

    /*-----------------------------------*/
    /*-         Inner Classes           -*/
    /*-----------------------------------*/

    /**
     * Subscriber-Feeder handshake config
     */
    private class SubFeederHandshakeConfig
            implements ReplicaFeederHandshakeConfig {

        private final NodeType nodeType;
        private final RepImpl repImpl;
        SubFeederHandshakeConfig(NodeType nodeType) {
            this.nodeType = nodeType;
            repImpl = (RepImpl)envImpl;
        }

        public RepImpl getRepImpl() {
            return repImpl;
        }

        public NameIdPair getNameIdPair() {
            return getRepImpl().getNameIdPair();
        }

        public RepUtils.Clock getClock() {
            return new RepUtils.Clock(RepImpl.getClockSkewMs());
        }

        public NodeType getNodeType() {
            return nodeType;
        }

        public NamedChannel getNamedChannel() {
            return namedChannel;
        }

        /* create a group impl from group name and group uuid */
        public RepGroupImpl getGroup() {

            RepGroupImpl repGroupImpl = new RepGroupImpl(
                    config.getGroupName(),
                    true, /* unknown group uuid */
                    repImpl.getCurrentJEVersion());

            /* use uuid if specified, otherwise unknown uuid will be used */
            if (config.getGroupUUID() != null) {
                repGroupImpl.setUUID(config.getGroupUUID());
            }
            return repGroupImpl;
        }
    }

    /**
     * Thrown to indicate that the Subscriber must retry connecting to the same
     * master, after some period of time.
     */
    private class ConnectionException extends RuntimeException {

        private final long retrySleepMs;

        ConnectionException(String message,
                            long retrySleepMs,
                            Throwable cause) {
            super(message, cause);
            this.retrySleepMs = retrySleepMs;
        }

        /**
         * Get thread sleep time before retry
         *
         * @return sleep time in ms
         */
        long getRetrySleepMs() {
            return retrySleepMs;
        }

        @Override
        public String getMessage() {
            return "Failed to connect, will retry after sleeping " +
                   retrySleepMs + " ms";
        }
    }

    /**
     * Handle exceptions uncaught in SubscriptionThread
     */
    private class SubscriptionThreadExceptionHandler
        implements UncaughtExceptionHandler {

        public void uncaughtException(Thread t, Throwable e) {
            logger.severe("Error { " + e.getMessage() +
                          " } in SubscriptionThread {" +
                          t + " } was uncaught.\nstack trace:\n" +
                          LoggerUtils.getStackTrace(e));
        }
    }
}

