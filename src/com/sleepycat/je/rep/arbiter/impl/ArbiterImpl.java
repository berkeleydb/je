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

package com.sleepycat.je.rep.arbiter.impl;

import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_STATE;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.Elections;
import com.sleepycat.je.rep.elections.ElectionsConfig;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol.BinaryNodeStateResponse;
import com.sleepycat.je.rep.impl.BinaryNodeStateService;
import com.sleepycat.je.rep.impl.NodeStateService;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.ChannelTimeoutTask;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.RepUtils.ExceptionAwareCountDownLatch;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StringStat;

/**
 * The implementation of the Arbiter. The Arbiter is a participant in
 * elections and may acknowledge transaction commits.
 * <p>
 * The Arbiter persists the Arbiter's replication group node identifier
 * and the highest commit VLSN that has been acknowledged.
 * Currently the Feeder sends commit acknowledgment requests
 * to the Arbiter if the replication factor is two and the other
 * Rep node is not available (RepImpl.preLogCommitHook).
 * <p>
 * The VLSN is used in response to an election promise request.
 * The priority of the response is lower than
 * a RepNode. This allows the RepNode to be selected if the VLSN is
 * equal. An Arbiter may not "win" an election. If the Arbiter's VLSN
 * is the highest in the election, the election result is ignored. A
 * NULL nodeid in a promise response is used to identify a promise
 * response is from an Arbiter.
 * <p>
 * Two pieces of information are persisted by the
 * Arbiter.
 * The replication group node identifier is persisted because
 * this information is located in the group database and the Arbiter
 * does not have a copy of the group database.
 * The other is the high VLSN of a Arbiter ACKed commit.
 * <p>
 * In the future, the algorithm could be changed to request commit
 * acknowledgments when the replication factor is greater than
 * two. This would allow for better write availability when
 * the replication factor is an even number.
 */

public class ArbiterImpl extends StoppableThread {

    private final static String DATA_FILE_NAME = "00000000.adb";

    /*
     * Amount of times to sleep between retries when a new node tries to locate
     * a master.
     */
    private static final int MASTER_QUERY_INTERVAL = 1000;

    private ServiceDispatcher serviceDispatcher;
    private DataChannelFactory channelFactory;
    private MasterStatus masterStatus;
    private MasterChangeListener changeListener;
    private Acceptor.SuggestionGenerator suggestionGenerator;
    private ReplicationGroupAdmin repGroupAdmin;
    private RepGroupImpl cachedRepGroupImpl;
    private Timer timer;
    private ChannelTimeoutTask channelTimeoutTask;
    private ArbiterVLSNTracker arbiterVLSNTracker;
    private ArbiterNodeStateService nodeStateService;
    private ArbBinaryStateService binaryStateService;

    /* The Arbiter's logger. */
    private Logger logger;
    private Formatter formatter;
    private final RepImpl repImpl;
    NameIdPair nameIdPair;
    private Elections elections;
    private final File arbiterHome;
    private String groupName;
    private ArbiterAcker arbiterAcker;
    private MonitorEventManager monitorEventManager;

    /*
     * Determines whether the Arbiter has been shutdown. Usually this is held
     * within the StoppableThread, but the Feeder's two child threads have
     * their shutdown coordinated by the parent Feeder.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /*
     * The latch used to wait for the ArbiterAcker to establish
     * a connection with the Master.
     */
    private volatile ExceptionAwareCountDownLatch readyLatch = null;
    private AtomicReference<ReplicatedEnvironment.State> currentState;
    private long joinGroupTime;
    private Set<InetSocketAddress> helperSockets;

    /**
     * The Arbiter implementation.
     * Uses the following replication parameters:
     * RepParams.GROUP_NAME The replication group name.
     * RepParams.ENV_UNKNOWN_STATE_TIMEOUT Timeout used for being in
     *                                     the unknown state.
     * RepParams.NODE_HOST_PORT The host name and port associated with this
     *                          node.
     *
     * @param arbiterHome - Arbiter home directory.
     * @param repImpl - RepImpl
     *
     * @throws EnvironmentNotFoundException
     * @throws EnvironmentLockedException
     * @throws DatabaseException
     */
    public ArbiterImpl(File arbiterHome, RepImpl repImpl)
        throws EnvironmentNotFoundException,
               EnvironmentLockedException,
               DatabaseException {
        super(repImpl, "ArbiterNode " + repImpl.getNameIdPair());
        this.repImpl = repImpl;
        this.arbiterHome = arbiterHome;
        try {
            initialize();
        } catch (IOException ioe) {
            throw EnvironmentFailureException.unexpectedException(
                repImpl, "Problem attempting to join on " + getSocket(), ioe);
        }
    }

    public StatGroup loadStats(StatsConfig config) {
        StatGroup arbStat;
        if (arbiterAcker == null) {
            arbStat =  new StatGroup(ArbiterStatDefinition.GROUP_NAME,
                                     ArbiterStatDefinition.GROUP_DESC);
        } else {
            arbStat = arbiterAcker.loadStats(config);
        }
        StringStat state = new StringStat(arbStat, ARB_STATE);
        state.set(currentState.toString());

        StatGroup trackerStats =
            arbiterVLSNTracker == null ? ArbiterVLSNTracker.loadEmptyStats() :
                arbiterVLSNTracker.loadStats(config);
        /* Add the tracker stats */
            arbStat.addAll(trackerStats);

        return arbStat;
    }

    private void initialize() throws IOException {
        nameIdPair = repImpl.getNameIdPair();
        currentState = new AtomicReference<ReplicatedEnvironment.State>
        (ReplicatedEnvironment.State.UNKNOWN);

        logger = LoggerUtils.getLogger(getClass());
        formatter = new ReplicationFormatter(nameIdPair);
        readyLatch = new ExceptionAwareCountDownLatch(repImpl, 1);

        channelFactory = repImpl.getChannelFactory();

        serviceDispatcher =
            new ServiceDispatcher(getSocket(), repImpl,
                                  channelFactory);
        serviceDispatcher.start();

        masterStatus = new MasterStatus(nameIdPair);
        changeListener = new MasterChangeListener();
        File dataFile =
            new File(arbiterHome.getAbsolutePath() +
                     File.separator + DATA_FILE_NAME);
        arbiterVLSNTracker = new ArbiterVLSNTracker(dataFile);
        suggestionGenerator = new MasterSuggestionGenerator();

        if (arbiterVLSNTracker.getCachedNodeId() != NameIdPair.NULL_NODE_ID) {
            nameIdPair.update(
                new NameIdPair(nameIdPair.getName(),
                               arbiterVLSNTracker.getCachedNodeId()));
        }
        groupName = repImpl.getConfigManager().get(RepParams.GROUP_NAME);
        helperSockets = repImpl.getHelperSockets();
        monitorEventManager = new MonitorEventManager(this);
    }

    public void runArbiter() {

        elections = new Elections(new ArbElectionsConfig(),
                                  changeListener,
                                  suggestionGenerator);

        elections.startLearner();
        elections.startAcceptor();

        repGroupAdmin =
                new ReplicationGroupAdmin(
                    groupName,
                    helperSockets,
                    channelFactory);
        timer = new Timer(true);
        channelTimeoutTask = new ChannelTimeoutTask(timer);

        utilityServicesStart();

        start();

        int timeout =
            repImpl.getConfigManager().getDuration(
                RepParams.ENV_UNKNOWN_STATE_TIMEOUT);
        if (timeout == 0) {
            timeout = Integer.MAX_VALUE;
        }

        try {
            /*
             * Wait for ArbiterAcker to establish connection to master if there
             * is one, or timeout and return, if we could not find one in the
             * ENV_UNKNOWN_STATE_TIMEOUT period.
             */
            getReadyLatch().awaitOrException(timeout,
                                             TimeUnit.MILLISECONDS);
            LoggerUtils.fine(logger, repImpl,
                             "Arbiter started in " + currentState + " state.");
        } catch (InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    @Override
    public void run() {
        /* Set to indicate an error-initiated shutdown. */
        Error repNodeError = null;
        try {
            while (!isShutdownOrInvalid()) {
                queryGroupForMembership();
                masterStatus.sync();
                arbiterAcker = new ArbiterAcker(this, repImpl);
                arbiterAcker.runArbiterAckLoop();
            }
        } catch (InterruptedException e) {
            LoggerUtils.fine(logger, repImpl,
                             "Arbiter main thread interrupted - " +
                             " forced shutdown.");
        } catch (GroupShutdownException e) {
            saveShutdownException(e);
            LoggerUtils.fine(logger, repImpl,
                             "Arbiter main thread sees group shutdown - " + e);
        } catch (InsufficientLogException e) {
            saveShutdownException(e);
        } catch (RuntimeException e) {
            LoggerUtils.fine(logger, repImpl,
                             "Arbiter main thread sees runtime ex - " + e);
            saveShutdownException(e);
            throw e;
        } catch (Error e) {
            LoggerUtils.fine(logger, repImpl, e +
                             " incurred during arbiter loop");
            repNodeError = e;
            repImpl.invalidate(e);
        } finally {
                LoggerUtils.info(logger, repImpl,
                                 "Arbiter main thread shutting down.");

                if (repNodeError != null) {
                    LoggerUtils.info(logger, repImpl,
                                     "Node state at shutdown:\n"+
                                     repImpl.dumpState());
                    throw repNodeError;
                }
                Throwable exception = getSavedShutdownException();

                if (exception == null) {
                    LoggerUtils.fine(logger, repImpl,
                                     "Node state at shutdown:\n"+
                                     repImpl.dumpState());
                } else {
                    LoggerUtils.info(logger, repImpl,
                                     "Arbiter shutdown exception:\n" +
                                     exception.getMessage() +
                                     repImpl.dumpState());
                }

                try {
                    shutdown();
                } catch (DatabaseException e) {
                    RepUtils.chainExceptionCause(e, exception);
                    LoggerUtils.severe(logger, repImpl,
                                       "Unexpected exception during shutdown" +
                                       e);
                    throw e;
                }
            setState(ReplicatedEnvironment.State.DETACHED);
            cleanup();
        }
    }

    public ReplicatedEnvironment.State getArbState() {
        return currentState.get();
    }

    /* Get the shut down reason for this node. */
    private LeaveReason getLeaveReason() {
        LeaveReason reason = null;

        Exception exception = getSavedShutdownException();
        if (exception == null) {
            reason = LeaveReason.NORMAL_SHUTDOWN;
        } else if (exception instanceof GroupShutdownException) {
            reason = LeaveReason.MASTER_SHUTDOWN_GROUP;
        } else {
            reason = LeaveReason.ABNORMAL_TERMINATION;
        }

        return reason;
    }

    /* Get the current master name if it exists. */
    String getMasterName() {
        if (masterStatus.getGroupMasterNameId().getId() ==
            NameIdPair.NULL_NODE_ID) {
            return null;
        }

        return masterStatus.getGroupMasterNameId().getName();
    }

    String getNodeName() {
        return nameIdPair.getName();
    }

    RepGroupImpl getGroup() {
        return cachedRepGroupImpl;
    }

    public Elections getElections() {
        return elections;
    }

    public void setState(ReplicatedEnvironment.State state) {
        currentState.set(state);
        repImpl.getNodeState().changeAndNotify(state, NameIdPair.NULL);
    }

    private void utilityServicesStart() {
        /* Register the node state querying service. */
        nodeStateService = new ArbiterNodeStateService(serviceDispatcher, this);
        serviceDispatcher.register(nodeStateService);

        binaryStateService =
            new ArbBinaryStateService(serviceDispatcher, this);
    }

    private void utilityServicesShutdown() {

        if (binaryStateService != null) {
            try {
                binaryStateService.shutdown();
            } catch (Exception e) {
                LoggerUtils.info(logger, repImpl,
                                 "Error shutting down binaryStateService " +
                                 e.getMessage());
            }
        }

        if (nodeStateService != null) {
            try {
                serviceDispatcher.cancel(NodeStateService.SERVICE_NAME);
            } catch (Exception e) {
                LoggerUtils.info(logger, repImpl,
                                 "Error canceling serviceDispatch " +
                                 e.getMessage());
            }
        }
    }

    public void shutdown() {
        boolean changed = shutdown.compareAndSet(false, true);
        if (!changed) {
            return;
        }

        try {
            monitorEventManager.notifyLeaveGroup(getLeaveReason());
        } catch (Exception e) {
            LoggerUtils.info(logger, repImpl,
                             "Error shutting down monitor event manager " +
                             e.getMessage());
        }

        utilityServicesShutdown();

        if (arbiterAcker != null) {
            try {
                arbiterAcker.shutdown();
            } catch (Exception e) {
                LoggerUtils.info(logger, repImpl,
                                 "Error shutting down ArbiterAcker " +
                                 e.getMessage());
            }
        }

        if (elections != null) {
            try {
                elections.shutdown();
            } catch (Exception e) {
                LoggerUtils.info(logger, repImpl,
                                 "Error shutting down elections " +
                                 e.getMessage());
            }
        }
        if (serviceDispatcher != null) {
            serviceDispatcher.shutdown();
        }
        LoggerUtils.info(logger, repImpl,
                         nameIdPair + " shutdown completed.");
        masterStatus.setGroupMaster(null, 0, NameIdPair.NULL);
        readyLatch.releaseAwait(getSavedShutdownException());
        arbiterVLSNTracker.close();
        /* Cancel the TimerTasks. */
        channelTimeoutTask.cancel();
        timer.cancel();
    }

    ReplicatedEnvironment.State getNodeState() {
        return currentState.get();
    }

    String getGroupName() {
        return groupName;
    }

    RepImpl getRepImpl() {
        return repImpl;
    }

    public void refreshHelperHosts() {
        final Set<InetSocketAddress> helpers =
            new HashSet<InetSocketAddress>(repImpl.getHelperSockets());
        if (cachedRepGroupImpl != null) {
            helpers.addAll(cachedRepGroupImpl.getAllHelperSockets());
        }
        helperSockets = helpers;
        if (repGroupAdmin != null) {
            repGroupAdmin.setHelperSockets(helperSockets);
        }
    }

    RepGroupImpl refreshCachedGroup()
        throws DatabaseException {
        RepGroupImpl repGroupImpl;
        repGroupImpl = repGroupAdmin.getGroup().getRepGroupImpl();
        elections.updateRepGroupOnly(repGroupImpl);
        if (nameIdPair.hasNullId()) {
            RepNodeImpl n = repGroupImpl.getMember(nameIdPair.getName());
            if (n != null) {
                nameIdPair.update(n.getNameIdPair());
                arbiterVLSNTracker.writeNodeId(n.getNameIdPair().getId());
            }
        }
        final Set<InetSocketAddress> helpers =
            new HashSet<InetSocketAddress>(repImpl.getHelperSockets());
        helpers.addAll(repGroupImpl.getAllHelperSockets());
        helperSockets = helpers;
        cachedRepGroupImpl = repGroupImpl;
        return cachedRepGroupImpl;
    }

    void updateNameIdPair(NameIdPair other) {
        nameIdPair.update(other);
    }

    void notifyJoinGroup() {
        this.joinGroupTime = System.currentTimeMillis();
        monitorEventManager.notifyJoinGroup();
    }

    long getJoinGroupTime() {
        return joinGroupTime;
    }

    /**
     * Communicates with existing nodes in the group in order figure out
     * who is the master.
     * In the case where the local node does not appear to be in
     * the (local copy of the) GroupDB, typically because the node is starting
     * up with an empty env directory.  It could be that this is a new node
     * (never before been part of the group).  Or it could be a pre-existing
     * group member that has lost its env dir contents and wants to be restored
     * via a Network Restore operation.
     * <p>
     * We query the designated helpers for the Master
     * information. The helpers are
     * the ones that were identified via the node's configuration.
     * <p>
     * Returns normally when the master is found.
     *
     * @throws InterruptedException if the current thread is interrupted,
     *         typically due to a shutdown
     */
    private void queryGroupForMembership()
        throws InterruptedException {

        checkLoopbackAddresses();

        if (helperSockets.isEmpty()) {
            throw EnvironmentFailureException.unexpectedState
                ("Need a helper to add a new node into the group");
        }

        NameIdPair groupMasterNameId;
        while (true) {
            elections.getLearner().queryForMaster(helperSockets);
            groupMasterNameId = masterStatus.getGroupMasterNameId();
            if (!groupMasterNameId.hasNullId()) {
                /* A new, or pre-query, group master. */
                if (nameIdPair.hasNullId() &&
                    groupMasterNameId.getName().equals(nameIdPair.getName())) {

                    /*
                     * Residual obsolete information in replicas, ignore it.
                     * Can't be master if we don't know our own id, but some
                     * other node does! This state means that the node was a
                     * master in the recent past, but has had its environment
                     * deleted since that time.
                     */
                    try {
                        Thread.sleep(MASTER_QUERY_INTERVAL);
                    } catch (InterruptedException e) {
                        throw EnvironmentFailureException.
                            unexpectedException(e);
                    }
                    continue;
                }
                if (checkGroupMasterIsAlive(groupMasterNameId)) {
                    /* Use the current group master if it's alive. */
                    break;
                }
            }
            if (isShutdownOrInvalid()) {
                throw new InterruptedException("Arbiter node shutting down.");
            }
            Thread.sleep(MASTER_QUERY_INTERVAL);
        }
        LoggerUtils.fine(logger, repImpl, "New node " + nameIdPair.getName() +
                         " located master: " + groupMasterNameId);
    }

    ArbiterVLSNTracker getArbiterVLSNTracker() {
        return arbiterVLSNTracker;
    }

    /**
     * Returns true if the node has been shutdown or if the underlying
     * environment has been invalidated. It's used as the basis for exiting
     * the FeederManager or the Replica.
     */
    boolean isShutdownOrInvalid() {
        if (isShutdown()) {
            return true;
        }
        if (repImpl.wasInvalidated()) {
            saveShutdownException(repImpl.getInvalidatingException());
            return true;
        }
        return false;
    }

    private void checkLoopbackAddresses() {

        final InetAddress myAddress = getSocket().getAddress();
        final boolean isLoopback = myAddress.isLoopbackAddress();

        for (InetSocketAddress socketAddress : helperSockets) {
            final InetAddress nodeAddress = socketAddress.getAddress();

            if (nodeAddress.isLoopbackAddress() == isLoopback) {
                continue;
            }
            String message = getSocket() +
                " the address associated with this node, " +
                (isLoopback? "is " : "is not ") +  "a loopback address." +
                " It conflicts with an existing use, by a different node " +
                " of the address:" +
                socketAddress +
                (!isLoopback ? " which is a loopback address." :
                 " which is not a loopback address.") +
                " Such mixing of addresses within a group is not allowed, " +
                "since the nodes will not be able to communicate with " +
                "each other.";
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Check that the master found by querying other group nodes is indeed
     * alive and that we are not dealing with obsolete cached information.
     *
     * @return true if the master node could be contacted and was truly alive
     *
     * TODO: handle protocol version mismatch here and in DbPing, also
     * consolidate code so that a single copy is shared.
     */
    private boolean checkGroupMasterIsAlive(NameIdPair groupMasterNameId) {

        DataChannel channel = null;

        try {
            final InetSocketAddress masterSocket =
                masterStatus.getGroupMaster();

            final BinaryNodeStateProtocol protocol =
                new BinaryNodeStateProtocol(NameIdPair.NOCHECK, null);

            /* Build the connection. Set the parameter connectTimeout.*/
            channel = repImpl.getChannelFactory().
                connect(masterSocket,
                        new ConnectOptions().
                        setTcpNoDelay(true).
                        setOpenTimeout(5000).
                        setReadTimeout(5000));
            ServiceDispatcher.doServiceHandshake
                (channel, BinaryNodeStateService.SERVICE_NAME);
            /* Send a NodeState request to the node. */
            protocol.write(
                protocol.new BinaryNodeStateRequest(
                    groupMasterNameId.getName(),
                    repImpl.getConfigManager().get(RepParams.GROUP_NAME)),
                 channel);
            /* Get the response and return the NodeState. */
            BinaryNodeStateResponse response =
                protocol.read(channel, BinaryNodeStateResponse.class);

            ReplicatedEnvironment.State state = response.getNodeState();
           return (state != null) && state.isMaster();
        } catch (Exception e) {
            LoggerUtils.info(logger, repImpl,
                             "Queried master:" + groupMasterNameId +
                             " unavailable. Reason:" + e);
            return false;
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ioe) {
                    /* Ignore it */
                }
            }
        }
    }

    private InetSocketAddress getSocket() {
        return new InetSocketAddress(getHostName(), getPort());
    }

    NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    MasterStatus getMasterStatus() {
        return masterStatus;
    }

    ChannelTimeoutTask getChannelTimeoutTask() {
        return channelTimeoutTask;
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }
    @Override
    public Logger getLogger() {
        return logger;
    }

    public ExceptionAwareCountDownLatch getReadyLatch() {
        return readyLatch;
    }

    public void resetReadyLatch(Exception exception) {
        ExceptionAwareCountDownLatch old = readyLatch;
        readyLatch = new ExceptionAwareCountDownLatch(repImpl, 1);
        if (old.getCount() != 0) {
            /* releasing latch in some error situation. */
            old.releaseAwait(exception);
        }
    }

    /**
     * Returns the hostname associated with this node.
     *
     * @return the hostname
     */
    public String getHostName() {
        String hostAndPort =
            repImpl.getConfigManager().get(RepParams.NODE_HOST_PORT);
        int colonToken = hostAndPort.indexOf(":");
        return (colonToken >= 0) ?
               hostAndPort.substring(0, colonToken) :
               hostAndPort;
    }

    /**
     * Returns the  port used by the replication node.
     *
     * @return the port number
     */
    public int getPort() {

        String hostAndPort =
            repImpl.getConfigManager().get(RepParams.NODE_HOST_PORT);
        int colonToken = hostAndPort.indexOf(":");

        return (colonToken >= 0) ?
                Integer.parseInt(hostAndPort.substring(colonToken + 1)) :
                    Integer.parseInt(RepParams.DEFAULT_PORT.getDefault());
    }

    /**
     * The Listener used to learn about new Masters
     */
    private class MasterChangeListener implements Learner.Listener {
        /* The current learned value. */
        private MasterValue currentValue = null;

        /**
         * Implements the Listener protocol.
         */
        @Override
        public void notify(Proposal proposal, Value value) {
            /* We have a winning new proposal, is it truly different? */
            if (value.equals(currentValue)) {
                return;
            }
            currentValue = (MasterValue) value;
            try {
                String currentMasterName = currentValue.getNodeName();
                LoggerUtils.logMsg(logger, formatter, Level.FINE,
                                   "Arbiter notified of new Master: " +
                                   currentMasterName);
                masterStatus.setGroupMaster
                    (currentValue.getHostName(),
                     currentValue.getPort(),
                     currentValue.getNameId());
            } catch (Exception e) {
                LoggerUtils.logMsg
                    (logger, formatter, Level.SEVERE,
                     "Arbiter change event processing exception: " +
                     e.getMessage());
            }
        }
    }

    private class ArbElectionsConfig implements ElectionsConfig {

        @Override
        public String getGroupName() {
            return groupName;
        }

        @Override
        public NameIdPair getNameIdPair() {
            return nameIdPair;
        }

        @Override
        public ServiceDispatcher getServiceDispatcher() {
            return serviceDispatcher;
        }

        @Override
        public int getElectionPriority() {
            return Integer.MIN_VALUE;
        }

        @Override
        public int getLogVersion() {
            return LogEntryType.LOG_VERSION;
        }

        @Override
        public RepImpl getRepImpl() {
            return repImpl;
        }

        @Override
        public RepNode getRepNode() {
            return null;
        }
    }

    private class MasterSuggestionGenerator
        implements Acceptor.SuggestionGenerator {
        @Override
        public Value get(Proposal proposal) {
            return new MasterValue(null,
                                   getPort(),
                                   NameIdPair.NULL);
        }

        @Override
        public Ranking getRanking(Proposal proposal) {
            return new Ranking(arbiterVLSNTracker.getDTVLSN().getSequence(),
                               arbiterVLSNTracker.get().getSequence());

        }
    }
}
