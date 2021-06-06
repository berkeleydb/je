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

package com.sleepycat.je.rep.elections;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.WinningProposal;
import com.sleepycat.je.rep.elections.Protocol.MasterQueryResponse;
import com.sleepycat.je.rep.elections.Protocol.Result;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.elections.Utils.FutureTrackingCompService;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.MessageError;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.TextProtocol.MessageOp;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StoppableThreadFactory;

/**
 * The Learner agent.  It runs in its own dedicated thread, listening for
 * messages announcing the results of elections and, in turn, invoking
 * Listeners within the process to propagate the result.  It also listens for
 * requests asking for elections results, and provides static methods for
 * requesting those results.
 */
public class Learner extends ElectionAgentThread {

    /* The service dispatcher used by the Learner */
    private final ServiceDispatcher serviceDispatcher;

    /* The listeners interested in Election outcomes. */
    private final List<Listener> listeners = new LinkedList<>();

    /* The latest winning proposal and value propagated to Listeners. */
    private Proposal currentProposal = null;
    private Value currentValue = null;

    /* Identifies the Learner Service. */
    public static final String SERVICE_NAME = "Learner";

    /**
     * Creates an instance of a Learner which will listen for election results
     * to propagate to local listeners, and for requests asking for election
     * results.
     *
     * <p>Note that this constructor does not take a repNode as an argument, so
     * that it can be used as the basis for the standalone Monitor.
     *
     * @param protocol the protocol used for message exchange
     * @param serviceDispatcher the service dispatcher used by the agent
     */
    public Learner(Protocol protocol,
                   ServiceDispatcher serviceDispatcher) {
       this(null, protocol, serviceDispatcher);
    }

    public Learner(RepImpl repImpl,
                    Protocol protocol,
                    ServiceDispatcher serviceDispatcher) {
        super(repImpl, protocol,
              "Learner Thread " + protocol.getNameIdPair().getName());
        this.serviceDispatcher = serviceDispatcher;

        /* Add a listener for logging. */
        addListener(new Listener() {
                @Override
                public void notify(Proposal proposal, Value value) {
                    LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                                       "Learner notified. Proposal:" +
                                       proposal + " Value: " + value);
                }
            });
    }

    /**
     * Adds a Listener to the existing set of listeners, so that it can be
     * informed of the outcome of election results.
     *
     * @param listener the new listener to be added
     */
    public void addListener(Listener listener) {
        synchronized (listeners) {
            if (!listeners.contains(listener)) {
                listeners.add(listener);
            }
        }
    }

    /**
     * Removes a Listeners from the existing set of listeners.
     *
     * @param listener the listener to be removed.
     */
    void removeListener(Listener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    /**
     * Processes a result message
     *
     * @param proposal the winning proposal
     * @param value the winning value
     */
    synchronized public void processResult(Proposal proposal, Value value) {
        if ((currentProposal != null) &&
            (proposal.compareTo(currentProposal) < 0)) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                               "Ignoring obsolete winner: " + proposal);
            return;
        }
        currentProposal = proposal;
        currentValue = value;

        /* We have a new winning proposal and value, inform the listeners */
        synchronized (listeners) {
            for (Listener listener : listeners) {
                try {
                    listener.notify(currentProposal, currentValue);
                } catch (Exception e) {
                    e.printStackTrace();
                    /* Report the exception and keep going. */
                    LoggerUtils.logMsg
                        (logger, envImpl, formatter, Level.SEVERE,
                         "Exception in Learner Listener: " + e.getMessage());
                    continue;
                }
            }
        }
    }

    /**
     * The main Learner loop. It accepts requests and propagates them to its
     * Listeners, if the proposal isn't out of date.
     */
    @Override
    public void run() {
        serviceDispatcher.register(SERVICE_NAME, channelQueue);
        LoggerUtils.logMsg
            (logger, envImpl, formatter, Level.FINE, "Learner started");
        DataChannel channel = null;
        try {
            while (true) {
                channel = serviceDispatcher.takeChannel
                    (SERVICE_NAME,
                     true /* blocking socket */,
                     protocol.getReadTimeout());

                if (channel == null) {
                    /* A soft shutdown. */
                   return;
                }

                BufferedReader in = null;
                PrintWriter out = null;
                try {
                    in = new BufferedReader
                        (new InputStreamReader(
                            Channels.newInputStream(channel)));
                    final String requestLine = in.readLine();
                    if (requestLine == null) {
                        continue;
                    }
                    final RequestMessage requestMessage;
                    try {
                        requestMessage = protocol.parseRequest(requestLine);
                    } catch (InvalidMessageException ime) {
                        protocol.processIME(channel, ime);
                        continue;
                    }

                    final MessageOp op = requestMessage.getOp();
                    LoggerUtils.logMsg(logger, envImpl, formatter,
                                       Level.FINEST,
                                       "learner request: " + op +
                                       " sender: " +
                                       requestMessage.getSenderId());
                    if (op == protocol.RESULT) {
                        Result result = (Result) requestMessage;
                        processResult(result.getProposal(), result.getValue());
                    } else if (op == protocol.MASTER_QUERY) {
                        processMasterQuery(channel, requestMessage);
                    } else if (op == protocol.SHUTDOWN) {
                        LoggerUtils.logMsg
                            (logger, envImpl, formatter, Level.FINE,
                             "Learner thread exiting");
                        break;
                    } else {
                        final String message =
                            "Malformed request: '" + requestLine + "'" +
                            " Unexpected op:" + op;
                        final InvalidMessageException ime =
                            new InvalidMessageException(MessageError.BAD_FORMAT,
                                                        message);
                        protocol.processIME(channel, ime);
                        continue;
                    }
                } catch (IOException e) {
                    LoggerUtils.logMsg
                        (logger, envImpl, formatter, Level.INFO,
                         "IO exception: " + e.getMessage());
                } catch (Exception e) {
                    throw EnvironmentFailureException.unexpectedException(e);
                } finally {
                    Utils.cleanup(logger, envImpl, formatter, channel, in, out);
                }
            }
        } catch (InterruptedException e) {
            if (isShutdown()) {
                /* Treat it like a shutdown, exit the thread. */
                return;
            }
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.WARNING,
                               "Learner unexpected interrupted");
            throw EnvironmentFailureException.unexpectedException(e);
       } finally {
            serviceDispatcher.cancel(SERVICE_NAME);
            cleanup();
       }
    }

    /**
     * Responds to a query for the current master. A response is only
     * generated if the node is currently in the Master or Replica state to
     * ensure that the information is reasonably current.
     */
    synchronized private void processMasterQuery(DataChannel channel,
                                                 RequestMessage requestMessage)
    {
        if ((currentProposal == null) || (currentValue == null)) {
            /* Don't have any election results to share. */
            return;
        }

        if ((envImpl == null) || !((RepImpl) envImpl).getState().isActive()) {
            /* Knowledge of master is potentially obsolete */
            return;
        }

        PrintWriter out = null;
        try {
            out = new PrintWriter(Channels.newOutputStream(channel), true);
            final MasterQueryResponse responseMessage = protocol.new
                MasterQueryResponse(currentProposal, currentValue);

            /*
             * The request message may be of an earlier version. If so, this
             * node transparently read the older version. JE only throws out
             * InvalidMessageException when the version of the request message
             * is newer than the current protocol. To avoid sending a response
             * that the requester cannot understand, we send a response in the
             * same version as that of the original request message.
             */
            responseMessage.setSendVersion(requestMessage.getSendVersion());
            out.println(responseMessage.wireFormat());
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * Queries other learners, in parallel, to determine whether they know of
     * an existing master in the group. If one is found, the result is
     * processed via {@link #processResult} as though it were an election
     * result that was sent to the Learner, resulting in the node transitioning
     * to the master or replica state as appropriate.
     * <p>
     * Note that this node itself is not allowed to become a master as a result
     * of such a query. It must only do so via an election.
     *
     * @param learnerSockets the sockets associated with learners at other
     * nodes. The nodes are queried on these sockets.
     */
    public void queryForMaster(Set<InetSocketAddress> learnerSockets) {
        if (learnerSockets.size() <= 0) {
                return;
        }
        int threadPoolSize = Math.min(learnerSockets.size(), 10);
        final ExecutorService pool =
            Executors.newFixedThreadPool
               (threadPoolSize, new StoppableThreadFactory("JE Learner",
                                                           logger));
        try {
            RequestMessage masterQuery = protocol.new MasterQuery();
            FutureTrackingCompService<MessageExchange> compService =
                Utils.broadcastMessage(learnerSockets,
                                       Learner.SERVICE_NAME,
                                       masterQuery,
                                       pool);
            /*
             * 2 * read timeout below to roughly cover the max time for a
             * message exchange.
             */
            new Utils.WithFutureExceptionHandler<MessageExchange>
            (compService, 2 * protocol.getReadTimeout(), TimeUnit.MILLISECONDS,
                logger, (RepImpl)envImpl, formatter) {

                @Override
                protected void processResponse(MessageExchange me) {

                    if (me.getResponseMessage().getOp() ==
                        protocol.MASTER_QUERY_RESPONSE){
                        MasterQueryResponse accept =
                            (MasterQueryResponse) me.getResponseMessage();
                        MasterValue masterValue =
                            (MasterValue) accept.getValue();
                        if ((masterValue != null) &&
                            masterValue.getNameId().
                            equals(protocol.getNameIdPair())) {

                            /*
                             * Should not transition to master as a result
                             * of a query it risks imposing a hard recovery
                             * on the replicas.
                             */
                            return;
                        }
                        processResult(accept.getProposal(), masterValue);
                    }
                }

                @Override
                protected boolean isShutdown() {
                    return Learner.this.isShutdown();
                }
            }.execute();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Returns the socket address for the current master, or null if one
     * could not be determined from the available set of learners. This API
     * is suitable for tools which need to contact the master for a specific
     * service, e.g. to delete a replication node, or to add a monitor. This
     * method could be used in principle to establish other types of nodes as
     * well via a tool, but that is currently done by the handshake process.
     *
     * @param protocol the protocol to be used when determining the master
     *
     * @param learnerSockets the learner to be queried for the master
     * @param logger for log messages
     * @return the MasterValue identifying the master
     * @throws UnknownMasterException if no master could be established
     */
    static public MasterValue findMaster
        (final Protocol protocol,
         Set<InetSocketAddress> learnerSockets,
         final Logger logger,
         final RepImpl repImpl,
         final Formatter formatter )
        throws UnknownMasterException {

        if (learnerSockets.size() <= 0) {
                return null;
        }
        int threadPoolSize = Math.min(learnerSockets.size(), 10);
        final ExecutorService pool =
            Executors.newFixedThreadPool(threadPoolSize);
        try {
            FutureTrackingCompService<MessageExchange> compService =
                Utils.broadcastMessage(learnerSockets,
                                       Learner.SERVICE_NAME,
                                       protocol.new MasterQuery(),
                                       pool);

            final List<MasterQueryResponse> results = new LinkedList<>();
            new Utils.WithFutureExceptionHandler<MessageExchange>
            (compService, 2 * protocol.getReadTimeout(), TimeUnit.MILLISECONDS,
             logger, repImpl, formatter) {

                @Override
                protected void processResponse(MessageExchange me) {

                    final ResponseMessage response = me.getResponseMessage();

                    if (response.getOp() ==
                        protocol.MASTER_QUERY_RESPONSE){
                        results.add((MasterQueryResponse)response);
                    } else {
                        LoggerUtils.logMsg(logger, repImpl, formatter,
                                           Level.WARNING,
                                           "Unexpected MasterQuery response:" +
                                           response.wireFormat());
                    }
                }

                @Override
                protected boolean isShutdown() {
                    return (repImpl != null) && !repImpl.isValid();
                }

            }.execute();

            MasterQueryResponse bestResponse = null;
            for (MasterQueryResponse result : results) {
                if ((bestResponse == null) ||
                    (result.getProposal().
                        compareTo(bestResponse.getProposal()) > 0)) {
                    bestResponse = result;
                }
            }
            if (bestResponse == null) {
                throw new UnknownMasterException
                ("Could not determine master from helpers at:" +
                    learnerSockets.toString());
            }
            return(MasterValue) bestResponse.getValue();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A method to re-broadcast this Learner's notion of the master. This
     * re-broadcast is done primarily to inform an obsolete master that it's no
     * longer the current master. Obsolete master situations arise in network
     * partition scenarios, where a current master is not able to participate
     * in an election, nor is it informed about the results. The re-broadcast
     * is the mechanism for rectifying such a situation. When the obsolete
     * master receives the new results after the network partition has been
     * fixed, it will revert to being a replica.
     *
     * @param learners the learners that must be informed
     * @param threadPool the pool used to dispatch broadcast requests in
     * in parallel
     */
    public void reinformLearners(Set<InetSocketAddress> learners,
                                 ExecutorService threadPool) {

        Proposer.WinningProposal winningProposal;
        synchronized (this) {
            if ((currentProposal == null) || (currentValue == null)) {
                return;
            }
            winningProposal =
                new WinningProposal(currentProposal, currentValue, null);
        }

        final RepImpl repImpl = (RepImpl)envImpl;
        if (repImpl == null) {
            return;
        }
        informLearners(learners, winningProposal, protocol, threadPool,
                       logger, repImpl, formatter);
    }

    /**
     * A utility method used to broadcast the results of an election to
     * Listeners.
     *
     * @param learners that need to be informed.
     * @param winningProposal the result that needs to be propagated
     * @param protocol to be used for communication
     * @param threadPool used to supply threads for the broadcast
     */
    public static void informLearners(Set<InetSocketAddress> learners,
                                      Proposer.WinningProposal winningProposal,
                                      Protocol protocol,
                                      ExecutorService threadPool,
                                      final Logger logger,
                                      final RepImpl repImpl,
                                      final Formatter formatter) {

        if ((learners == null) || (learners.size() == 0)) {
            throw EnvironmentFailureException.unexpectedState
                ("There must be at least one learner");
        }

        LoggerUtils.logMsg(logger, repImpl, formatter, Level.FINE,
                           "Informing " + learners.size() + " learners.");
        FutureTrackingCompService<MessageExchange> compService =
            Utils.broadcastMessage(learners,
                                   Learner.SERVICE_NAME,
                                   protocol.new Result
                                   (winningProposal.proposal,
                                    winningProposal.chosenValue),
                                   threadPool);

        /* Consume the futures. */

        /* Atomic to provide incrementable "final" to nested method. */
        final AtomicInteger count = new AtomicInteger(0);

        new Utils.WithFutureExceptionHandler<MessageExchange>
        (compService, 2 * protocol.getReadTimeout(), TimeUnit.MILLISECONDS,
         logger, repImpl, formatter) {

            @Override
            protected void processResponse(MessageExchange me) {
                /* Do nothing, just consume the futures. */
                count.incrementAndGet();
            }

            @Override
            protected void processNullResponse(MessageExchange me) {
                if (me.getException() == null) {
                    count.incrementAndGet();
                }
            }

            @Override
            protected boolean isShutdown() {
                return (repImpl != null) && !repImpl.isValid();
            }

        }.execute();
        LoggerUtils.logMsg
            (logger, repImpl, formatter, Level.FINE,
             "Informed learners: " + count.get());
    }

    /**
     * @see StoppableThread#getLogger
     */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    /*
     * Notifies the listener that a new proposal has been accepted. Note that
     * the value may be unchanged. The proposals may be out of sequence, it's
     * up to the listener to deal with it appropriately.
     */
    public static interface Listener {
        void notify(Proposal proposal, Value value);
    }
}
