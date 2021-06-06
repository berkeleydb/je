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

import static com.sleepycat.je.rep.impl.RepParams.ELECTIONS_REBROADCAST_PERIOD;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.IntConfigParam;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.elections.Proposer.MaxRetriesException;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.elections.Utils.FutureTrackingCompService;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.node.ElectionQuorum;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StoppableThreadFactory;

/**
 * Represents the environment in which elections are run on behalf of a node.
 * There is exactly one instance of an Elections for each node. Elections are
 * initiated via this class.
 *
 * One of the primary goals of this interface is to keep Elections as free
 * standing as possible, so that we can change how elections are held, or
 * aspects of the election infrastructure with minimal impact on replication.
 * For example, elections currently used tcp for communication of election
 * messages but may want to switch over to udp. Such a change should be
 * confined to just the Elections module. Other changes might include changes
 * to the strategy used to suggest Values and the weight associated with a
 * suggested Value.
 *
 * The following are the principal points of interaction between Elections and
 * Replication:
 *
 * 1) The initiation of elections via the initiateElections() method.
 *
 * 2) The suggestion of nodes as masters and the ranking of the
 * suggestion. This is done via the Acceptor.SuggestionGenerator interface. An
 * instance of this interface is supplied when the Elections class is
 * instantiated. Note that the implementation must also initiate a freeze of
 * VLSNs to ensure that the ranking does not change as the election
 * progresses. The VLSN can make progress when the node is informed via its
 * Listener that an election with a higher Proposal number (than the one in the
 * Propose request) has finished.
 *
 * 3) Obtaining the result of an election initiated in step 1. This is done via
 * the Learner.Listener interface. An instance of this class is supplied when
 * the Election class is first instantiated.
 *
 */

public class Elections {

    /* Describes all nodes of the group. */
    private RepGroupImpl repGroup;

    /*
     * A unique identifier for this election agent. It's used by all the
     * agents that comprise Elections.
     */
    private final NameIdPair nameIdPair;

    /*
     * A repNode is kept for error propagation if this election belongs to a
     * replicated environment. Elections are dependent on the RepNode to track
     * the number of members currently in a group and to deal with changing
     * quorum requirements when a node is acting as a Primary.
     * Note that repNode may be null if the creator of this Elections object
     * does not initiate an election and if the node can never be a master.
     * The Arbiter uses it this way.
     */
    private final RepNode repNode;

    private final ElectionsConfig config;

    private final RepImpl envImpl;

    /*
     * Shutdown can only be executed once. The shutdown field protects against
     * multiple invocations.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /* The three agents involved in the elections run by this class. */
    private Proposer proposer;
    private Acceptor acceptor;
    private Learner learner;

    /* The thread pool used to manage the threads used by the Proposer. */
    private final ExecutorService pool;

    /* Components of the agents. */
    final private Acceptor.SuggestionGenerator suggestionGenerator;
    final private Learner.Listener listener;

    /*
     * The protocol used to run the elections. All three agents use this
     * instance of the protocol
     */
    private final Protocol protocol;

    /*
     * The thread used to run the proposer during the current election. It's
     * volatile to ensure that shutdown can perform an unsynchronized access
     * to the iv even if an election is in progress.
     */
    private volatile ElectionThread electionThread = null;

    /* The listener used to indicate completion of an election. */
    private ElectionListener electionListener = null;

    /**
     * The timer task that re-broadcasts election results from a master. It's
     * null in unit tests.
     */
    private final RebroadcastTask rebroadcastTask;

    /* The number of elections that were held. */
    private int nElections = 0;

    private final Logger logger;
    private final Formatter formatter;

    /**
     * Creates an instance of Elections. There should be just one instance per
     * node. Note that the creation does not result in the election threads
     * being started, that is, the instance does not participate in elections.
     * This call is typically followed up with a call to startLearner that lets
     * it both learn about and supply elections results, and, if applicable, by
     * a subsequent call to participate to let it vote in elections.
     * The RepNode parameter is null when the Elections object is used by
     * the Arbiter. The Arbiter is a Learner and Acceptor. It will never
     * initiate an election (the RepNode must be non-null) and never
     * become Master.
     *
     * @param config elections configuration
     * @param listener the Listener invoked when results are available
     * @param suggestionGenerator used by the Acceptor
     */
    public Elections(ElectionsConfig config,
                     Learner.Listener listener,
                     Acceptor.SuggestionGenerator suggestionGenerator) {

        envImpl = config.getRepImpl();
        this.repNode = config.getRepNode();
        this.config = config;
        this.nameIdPair = config.getNameIdPair();
        DataChannelFactory channelFactory;

        if (repNode != null && repNode.getRepImpl() != null) {
            logger = LoggerUtils.getLogger(getClass());
            final DbConfigManager configManager = envImpl.getConfigManager();
            int rebroadcastPeriod = configManager.
                getDuration(ELECTIONS_REBROADCAST_PERIOD);
            rebroadcastTask = new RebroadcastTask(rebroadcastPeriod);
        } else {
            logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
            rebroadcastTask = null;
        }
        channelFactory = config.getServiceDispatcher().getChannelFactory();
        formatter = new ReplicationFormatter(nameIdPair);

        protocol = new Protocol(TimebasedProposalGenerator.getParser(),
                                MasterValue.getParser(),
                                config.getGroupName(),
                                nameIdPair,
                                config.getRepImpl(),
                                channelFactory);
        this.suggestionGenerator = suggestionGenerator;
        this.listener = listener;

        pool = Executors.newCachedThreadPool
            (new StoppableThreadFactory("JE Elections Factory " + nameIdPair,
                                        logger));
    }

    /* The thread pool used to allocate threads used during elections. */
    public ExecutorService getThreadPool() {
        return pool;
    }

    public ServiceDispatcher getServiceDispatcher() {
        return config.getServiceDispatcher();
    }

    public ElectionQuorum getElectionQuorum() {
        return repNode.getElectionQuorum();
    }

    public RepNode getRepNode() {
        return repNode;
    }

    public Logger getLogger() {
        return logger;
    }

    /* Get repImpl for Proposer to set up loggers. */
    public RepImpl getRepImpl() {
        return config.getRepImpl();
    }

    /**
     * Starts a Learner agent. Note that the underlying Protocol instance it
     * uses must have a current picture of the replication group otherwise it
     * will reject messages from nodes that it does not think are currently
     * part of the replication group.
     */
    public void startLearner() {
        // repNode used for thread name but can be null here
        learner = new Learner(config.getRepImpl(),
                              protocol,
                              config.getServiceDispatcher());
        learner.start();
        learner.addListener(listener);
        electionListener = new ElectionListener();
        learner.addListener(electionListener);
        if (rebroadcastTask != null) {
            repNode.getTimer().schedule(rebroadcastTask,
                                        rebroadcastTask.getPeriod(),
                                        rebroadcastTask.getPeriod());
        }
    }

    /**
     * Permits the Election agent to start participating in elections held
     * by the replication group, or initiate elections on behalf of this node.
     * Participation in elections is initiated only after a node has current
     * information about group membership.
     */
    public void participate() {
        proposer = new RankingProposer(this, nameIdPair);
        startAcceptor();
    }

    public void startAcceptor() {
        acceptor = new Acceptor(protocol, config, suggestionGenerator);
        acceptor.start();
    }

    /**
     * Returns the Acceptor associated with this node.
     * @return the Acceptor
     */
    public Acceptor getAcceptor() {
        return acceptor;
    }

    /**
     * Returns a current set of acceptor sockets.
     */
    public Set<InetSocketAddress> getAcceptorSockets() {
        if (repGroup == null) {
            throw EnvironmentFailureException.unexpectedState
                ("No rep group was configured");
        }
        return repGroup.getAllAcceptorSockets();
    }

    public Protocol getProtocol() {
        return protocol;
    }

    /**
     * Returns the Learner associated with this node
     * @return the Learner
     */
    public Learner getLearner() {
        return learner;
    }

    /**
     * The number of elections that have been held.  Used for testing.
     *
     * @return total elections initiated by this node.
     */
    public int getElectionCount() {
        return nElections;
    }

    /**
     * Initiates an election. Note that this may just be one of many possible
     * elections that are in progress in a replication group. The method does
     * not wait for this election to complete, but instead returns as soon as
     * any election result (including one initiated by some other Proposer)
     * becomes available via the Learner.
     *
     * A proposal submitted as part of this election may lose out to other
     * concurrent elections, or there may not be a sufficient number of
     * Acceptor agents active or reachable to reach a quorum. In such cases,
     * the election will not produce a result. That is, there will be no
     * notification to the Learners. Note that only one election can be
     * initiated at a time at a node If a new election is initiated while one
     * is already in progress, then the method will wait until it completes
     * before starting a new one.
     *
     * The results of this and any other elections that may have been initiated
     * concurrently by other nodes are made known to the Learner agents. Note
     * that this method does not return a result, since the concurrent arrival
     * of results could invalidate the result even before its returned.
     *
     * @param newGroup the definition of the group to be used for this election
     * @param quorumPolicy the policy to be used to reach a quorum.
     * @param maxRetries the max number of times a proposal may be retried
     * @throws InterruptedException
     */
    public synchronized void initiateElection(RepGroupImpl newGroup,
                                              QuorumPolicy quorumPolicy,
                                              int maxRetries)
        throws InterruptedException {

        updateRepGroup(newGroup);
        long startTime = System.currentTimeMillis();
        nElections++;
        LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                           "Election initiated; election #" + nElections);
        if (electionThread != null) {
            /*
             * The factor of four used below to arrive at a timeout value is a
             * heuristic: A factor of two to cover any pending message exchange
             * and another factor of two as a grace period. We really don't
             * expect to hit this timeout in the absence of networking issues,
             * hence the thread dump to understand the reason in case there's
             * some bug.
             */
            final int waitMs = protocol.getReadTimeout() * 4;
            // A past election request, wait until the election has quiesced
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                               "Election in progress. Waiting ... for " +
                               waitMs + "ms");

            electionThread.join(waitMs);
            if (electionThread.isAlive()) {
                /* Dump out threads for future analysis if it did not quit. */
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                                   "Election did not finish as expected." +
                                   " resorting to shutdown");
                LoggerUtils.fullThreadDump(logger, envImpl, Level.INFO);
                electionThread.shutdown();
            }

            final Exception exception =
                electionThread.getSavedShutdownException();
            if (exception != null) {
                throw new EnvironmentFailureException
                (envImpl,
                 EnvironmentFailureReason.UNEXPECTED_EXCEPTION,
                 exception);
            }
        }

        CountDownLatch countDownLatch = null;
        synchronized (electionListener) {
            // Watch for any election results from this point forward
            countDownLatch = electionListener.setLatch();
        }

        RetryPredicate retryPredicate =
            new RetryPredicate(repNode, maxRetries, countDownLatch);
        electionThread = new ElectionThread(quorumPolicy, retryPredicate,
                                            envImpl,
                                            (envImpl == null) ? null :
                                            envImpl.getName());

        electionThread.start();
        try {
            /* Wait until we hear of some "new" election result */
            countDownLatch.await();
            if (retryPredicate.pendingRetries <= 0) {
                /* Ran out of retries -- a test situation */
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                                   "Retry count exhausted: " +
                                   retryPredicate.maxRetries);
            }

            /*
             * Note that the election thread continues to run past this point
             * and may be active upon re-entry
             */
        } catch (InterruptedException e) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.WARNING,
                               "Election initiation interrupted");
            shutdown();
            throw e;
        }
        LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                           "Election finished. Elapsed time: " +
                           (System.currentTimeMillis() - startTime) + "ms");
    }

    /**
     * The standard method for requesting and election, we normally want to run
     * elections until we hear of an election result. Once initiated, elections
     * run until there is a successful conclusion, that is, a new master has
     * been elected. Since a successful conclusion requires the participation
     * of at least a simple majority, this may take a while if a sufficient
     * number of nodes are not available.
     *
     * The above method is used mainly for testing.
     *
     * @throws InterruptedException
     *
     * @see #initiateElection
     */
    public synchronized void initiateElection(RepGroupImpl newGroup,
                                              QuorumPolicy quorumPolicy)
            throws InterruptedException {

        initiateElection(newGroup, quorumPolicy, Integer.MAX_VALUE);
    }

    /**
     * Updates elections notion of the rep group, so that acceptors are aware
     * of the current state of the group, even in the absence of an election
     * conducted by the node itself.
     *
     * This method should be invoked each time a node becomes aware of a group
     * membership change.
     *
     * @param newRepGroup defines the new group
     */
    public void updateRepGroup(RepGroupImpl newRepGroup) {
        repGroup = newRepGroup;
        protocol.updateNodeIds(newRepGroup.getAllElectionMemberIds());
    }

    /**
     * Updates elections notion of the rep group, so that acceptors are aware
     * of the current state of the group, even in the absence of an election
     * conducted by the node itself. However this method does not update the
     * members in the protocol so checks are not made for the member id.
     *
     * This method should be invoked each time a node becomes aware of a group
     * membership change.
     *
     * @param newRepGroup defines the new group
     */
    public void updateRepGroupOnly(RepGroupImpl newRepGroup) {
        repGroup = newRepGroup;
    }

    /**
     * Predicate to determine whether an election is currently in progress.
     */
    public synchronized boolean electionInProgress() {
        return (electionThread != null) && electionThread.isAlive();
    }

    /**
     * Statistics used during testing.
     */
    public synchronized StatGroup getStats() {
        if (electionInProgress()) {
            throw EnvironmentFailureException.unexpectedState
                ("Election in progress");
        }
        return electionThread.getStats();
    }

    /**
     * For INTERNAL TESTING ONLY. Ensures that the initiated election has
     * reached a conclusion that can be tested.
     *
     * @throws InterruptedException
     */
    public synchronized void waitForElection()
        throws InterruptedException {

        assert(electionThread != null);
        electionThread.join();
    }

    /**
     * Shutdown all acceptor and learner agents by broadcasting a Shutdown
     * message. It waits until reachable agents have acknowledged the message
     * and the local learner and acceptor threads have exited.
     *
     * This is method is intended for use during testing only.
     *
     * @throws InterruptedException
     */
    public void shutdownAcceptorsLearners
            (Set<InetSocketAddress> acceptorSockets,
             Set<InetSocketAddress> learnerSockets)
        throws InterruptedException {

        LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                           "Elections being shutdown");
        FutureTrackingCompService<MessageExchange> compService =
            Utils.broadcastMessage(acceptorSockets,
                                   Acceptor.SERVICE_NAME,
                                   protocol.new Shutdown(),
                                   pool);
        /* The 60 seconds is just a reasonable timeout for use in tests */
        Utils.checkFutures(compService, 60, TimeUnit.SECONDS,
                           logger, envImpl, formatter);
        compService = Utils.broadcastMessage(learnerSockets,
                                         Learner.SERVICE_NAME,
                                         protocol.new Shutdown(),
                                         pool);
        Utils.checkFutures(compService, 60, TimeUnit.SECONDS,
                           logger, envImpl, formatter);
        if (learner != null) {
            learner.join();
        }
        if (acceptor != null) {
            acceptor.join();
        }
    }

    /**
     * Shuts down just the election support at this node. That is the Acceptor,
     * and Learner associated with this Elections as well as any pending
     * election running in its thread is terminated.
     *
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                           "Elections shutdown initiated");
        if (acceptor != null) {
            acceptor.shutdown();
        }

        if (learner != null) {
            learner.shutdown();
        }

        if (electionThread != null) {
            electionThread.shutdown();
        }

        if (proposer != null) {
            proposer.shutdown();
        }

        if (rebroadcastTask != null) {
            rebroadcastTask.cancel();
        }
        pool.shutdown();
        LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                           "Elections shutdown completed");
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * Used to short-circuit Proposal retries if a new election has completed
     * since the time this election was initiated.
     */
    static class ElectionListener implements Learner.Listener {

        /*
         * The election latch that is shared by the RetryPredicate. It's
         * counted down either when some election result becomes available or
         * when elections that are in progress need to be shutdown.
         */
        private CountDownLatch electionLatch = null;

        ElectionListener() {
            this.electionLatch = null;
        }

        /**
         * Returns a new latch to be associated with the RetryPredicate.
         */
        public synchronized CountDownLatch setLatch() {
            electionLatch = new CountDownLatch(1);
            return electionLatch;
        }

        /**
         * Used during shutdown only
         *
         * @return the latch on which elections wait
         */
        public CountDownLatch getElectionLatch() {
            return electionLatch;
        }

        /**
         * The Listener protocol announcing election results.
         */
        @Override
        public synchronized void notify(Proposal proposal, Value value) {
            // Free up the retry predicate if its waiting
            if (electionLatch != null) {
                electionLatch.countDown();
            }
        }
    }

    /**
     * Implements the retry policy
     */
    static class RetryPredicate implements Proposer.RetryPredicate {
        private final RepNode repNode;
        private final int maxRetries;
        private int pendingRetries;
        /* The latch that is activated by the Listener. */
        private final CountDownLatch electionLatch;

        /*
         * The number of time to retry an election before trying to activate
         * the primary.
         */
        private final int primaryRetries;

        private static final int BACKOFF_SLEEP_MIN = 1;
        private static final int BACKOFF_SLEEP_MAX = 32;

        private int backoffSleepInterval = BACKOFF_SLEEP_MIN;

        RetryPredicate(RepNode repNode,
                       int maxRetries,
                       CountDownLatch electionLatch) {
            this.repNode = repNode;
            this.maxRetries = maxRetries;
            pendingRetries = maxRetries;
            this.electionLatch = electionLatch;
            final RepImpl repImpl = repNode.getRepImpl();
            final IntConfigParam retriesParam =
                RepParams.ELECTIONS_PRIMARY_RETRIES;
            primaryRetries = (repImpl != null) ?
                 repImpl.getConfigManager().getInt(retriesParam) :
                 Integer.parseInt(retriesParam.getDefault());
        }

        /**
         * Returns the time to backoff before a retry. The backoff is
         * non-linear.
         *
         * @return the time to backoff in ms
         */
        private int backoffWaitTime() {
            backoffSleepInterval =
                Math.min(BACKOFF_SLEEP_MAX, backoffSleepInterval * 2);
            return backoffSleepInterval * 1000;
        }

        /**
         * Implements the protocol
         */
        @Override
        public boolean retry() throws InterruptedException {
            if ((maxRetries - pendingRetries) >= primaryRetries) {
                if ((repNode != null) &&
                     repNode.getArbiter().activateArbitration()) {
                    pendingRetries = maxRetries;
                    return true;
                }
            }
            if (pendingRetries-- <= 0) {
                /* Free up the main election thread */
                electionLatch.countDown();
                return false;
            }

            electionLatch.await(backoffWaitTime(), TimeUnit.MILLISECONDS);
            if (electionLatch.getCount() == 0) {
                /* An election completed, we can quit issuing proposals */
                return false;
            }
            /* Timed out and did not hear any election results. */
            return true;
        }

        /**
         * The number of times a retry was attempted
         */
        @Override
        public int retries() {
            return (maxRetries-pendingRetries);
        }
    }

    /**
     * The thread that actually runs an election. The thread exits either after
     * it has successfully had its proposal accepted and after it has informed
     * all learners, or if it gives up after some number of retries.
     */
    private class ElectionThread extends StoppableThread {

        final private QuorumPolicy quorumPolicy;

        /* Non-null on termination if a proposal was issued and accepted. */
        Proposer.WinningProposal winningProposal;

        /* Non-null at termination if no proposal was accepted. */
        MaxRetriesException maxRetriesException;

        final private RetryPredicate retryPredicate;

        private ElectionThread(QuorumPolicy quorumPolicy,
                               RetryPredicate retryPredicate,
                               EnvironmentImpl envImpl,
                               String envName) {
            super(envImpl, "ElectionThread_" + envName);
            this.quorumPolicy = quorumPolicy;
            this.retryPredicate = retryPredicate;
        }

        /**
         * Carries out an election and informs learners of the results. Any
         * uncaught exception will invalidate the environment if this is
         * being executed on behalf of a replicated node.
         */
        @Override
        public void run() {
            try {
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                                   "Started election thread " + new Date());
                winningProposal =
                    proposer.issueProposal(quorumPolicy, retryPredicate);

                /*
                 * TODO: Consider adding an optimization to inform SECONDARY
                 * nodes of election results, but continuing to only wait for
                 * the completion of notifications to ELECTABLE nodes.  That
                 * change would increase the chance that SECONDARY nodes have
                 * up-to-date information about the master, but would avoid
                 * adding sensitivity to potentially longer network delays in
                 * communicating with secondary nodes.
                 */
                Learner.informLearners(repGroup.getAllLearnerSockets(),
                                       winningProposal,
                                       protocol,
                                       pool,
                                       logger,
                                       config.getRepImpl(),
                                       null);
            } catch (MaxRetriesException mre) {
                maxRetriesException = mre;
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                                   "Exiting election after " +
                                   retryPredicate.retries() + " retries");
                return;
            } catch (InterruptedException e) {
                pool.shutdownNow();
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.INFO,
                                   "Election thread interrupted");
            } catch (Exception e) {
                saveShutdownException(e);
            } finally {
                cleanup();
                LoggerUtils.logMsg
                    (logger, envImpl, formatter, Level.INFO,
                     "Election thread exited. Group master: " +
                     ((repNode != null) ?
                      repNode.getMasterStatus().getGroupMasterNameId() :
                      Integer.MAX_VALUE));
            }
        }

        public void shutdown() {

            if (shutdownDone(logger)) {
                return;
            }
            shutdownThread(logger);
        }

        @Override
        protected int initiateSoftShutdown() {

            final CountDownLatch electionLatch =
                electionListener.getElectionLatch();

            if (electionLatch != null) {

                /*
                 * Unblock any initiated elections waiting for a result as
                 * well as this thread.
                 */
                electionLatch.countDown();
            }

            /*
             * Wait roughly for the time it would take for a read to timeout.
             * since the delay in testing the latch is probably related to
             * some slow network event
             */
            return protocol.getReadTimeout();
        }

        /**
         * Statistics from the election. Should only be invoked after the run()
         * method has exited.
         *
         * @return statistics generated by the proposer
         */
        StatGroup getStats() {
            return (winningProposal != null) ?
                winningProposal.proposerStats :
                maxRetriesException.proposerStats;
        }

        /**
         * @see StoppableThread#getLogger
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /**
     * Used to propagate the results of an election to any monitors. Note that
     * monitors are informed of results redundantly, both from the node that
     * concludes the election and via this re-propagation. The use of multiple
     * network paths increases the likelihood that the result will reach the
     * monitor via some functioning network path.
     *
     * The method returns immediately after queuing the operation in the
     * thread pool.
     */
    public void asyncInformMonitors(Proposal proposal, Value value) {
        final Set<InetSocketAddress> monitorSockets =
            repGroup.getAllMonitorSockets();
        if (monitorSockets.size() == 0) {
            return;
        }
        LoggerUtils.logMsg
            (logger, envImpl, formatter, Level.INFO,
             String.format("Propagating election results to %d monitors\n",
                           monitorSockets.size()));
        pool.execute(new InformLearners
                     (monitorSockets,
                      new Proposer.WinningProposal(proposal, value, null)));
    }

    /**
     * Provides the underpinnings of the async mechanism used to deliver
     * election results to the monitors.
     */
    private class InformLearners implements Runnable {
        final Set<InetSocketAddress> learners;
        final Proposer.WinningProposal winningProposal;

        InformLearners(Set<InetSocketAddress> learners,
                       Proposer.WinningProposal winningProposal) {
            this.learners = learners;
            this.winningProposal = winningProposal;
        }

        @Override
        public void run() {
            Learner.informLearners(learners,
                                   winningProposal,
                                   protocol,
                                   pool,
                                   logger,
                                   config.getRepImpl(),
                                   null);
        }
    }

    /**
     * Task to re-inform learners of election results by re-broadcasting the
     * results of an election from the master. This re-broadcast is intended to
     * help in network partition situations. See [#20220] for details.
     */
    private class RebroadcastTask extends TimerTask {

        /* Lock to ensure that async executions don't overlap. */
        private final ReentrantLock lock = new ReentrantLock();
        private int acquireFailCount = 0;
        private final int periodMs;

        public RebroadcastTask(int periodMs) {
            this.periodMs = periodMs;
        }

        public int getPeriod() {
           return periodMs;
        }

        /**
         * If the node is a master, it broadcasts election results to nodes
         * that are not currently connected to it via feeders.
         *
         * It's worth noting that since this is a timer task method it must be
         * be lightweight. So the actual broadcast is done in an asynchronous
         * method using a thread from the election thread pool.
         */
        @Override
        public void run() {
            try {
                if (!lock.tryLock()) {
                    if ((++acquireFailCount % 100) == 0) {
                        LoggerUtils.logMsg(logger, envImpl, formatter,
                                           Level.WARNING,
                                           "Failed to acquire lock after " +
                                           acquireFailCount + " retries");

                    }
                    return;
                }
                acquireFailCount = 0;
                if (!repNode.getMasterStatus().isGroupMaster()) {
                    return;
                }

                /*
                 * Re-informing when the node is a master is just an
                 * optimization, it does not impact correctness. Further
                 * minimize network traffic by trying just the nodes that are
                 * currently disconnected.
                 */
                final FeederManager feederManager = repNode.feederManager();
                final Set<String> active = feederManager.activeReplicas();
                active.add(repNode.getNodeName());

                final Set<InetSocketAddress> learners =
                    new HashSet<>();
                for (final RepNodeImpl rn : repGroup.getAllLearnerMembers()) {
                    if (!active.contains(rn.getName())) {
                        learners.add(rn.getSocketAddress());
                    }
                }

                if (learners.size() == 0) {
                    return;
                }

                LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE,
                                   "informing learners:" +
                                   Arrays.toString(learners.toArray()) +
                                   " active: " +
                                   Arrays.toString(active.toArray()));

                pool.execute(new Runnable() {
                        @Override
                        public void run() {
                            learner.reinformLearners(learners, pool);
                        }
                    });
            } catch (Exception e) {
                LoggerUtils.logMsg(logger, envImpl, formatter, Level.SEVERE,
                                   "Unexpected exception:" + e.getMessage());
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
    }
}
