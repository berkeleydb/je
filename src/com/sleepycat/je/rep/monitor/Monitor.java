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

package com.sleepycat.je.rep.monitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.elections.TimebasedProposalGenerator;
import com.sleepycat.je.rep.impl.NodeStateProtocol;
import com.sleepycat.je.rep.impl.NodeStateProtocol.NodeStateResponse;
import com.sleepycat.je.rep.impl.NodeStateService;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.monitor.GroupChangeEvent.GroupChangeType;
import com.sleepycat.je.rep.monitor.LeaveGroupEvent.LeaveReason;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.util.DbPing;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Provides a lightweight mechanism to track the current master node and the
 * members of the replication group. The information provided by the monitor
 * can be used to route update requests to the node that is currently the
 * master and distribute read requests across the other members of the group.
 * <p>
 * The Monitor is typically run on a machine that participates in load
 * balancing, request routing or is simply serving as a basis for application
 * level monitoring and does not have a replicated environment. To avoid
 * creating a single point of failure, an application may need to create
 * multiple monitor instances, with each monitor running on a distinct machine.
 * <p>
 * Applications with direct access to a {@link
 * com.sleepycat.je.rep.ReplicatedEnvironment ReplicatedEnvironment} can use
 * its {@link <a href="{@docRoot}/../ReplicationGuide/replicawrites.html">
 * synchronous and asynchronous mechanisms</a>} for determining the master node
 * and group composition changes. The Monitor class is not needed by such
 * applications.
 * <p>
 * The Monitor generally learns about changes to group status through events
 * issued by replication group members. In addition, the Monitor maintains a
 * daemon thread which periodically pings members of the group so that the
 * Monitor can proactively discover group status changes that occur when it is
 * down or has lost network connectivity.
 * <p>
 * The following code excerpt illustrates the typical code sequence used to
 * initiate a Monitor. Exception handling has been omitted to simplify the
 * example.
 *
 * <pre class="code">
 * MonitorConfig monConfig = new MonitorConfig();
 * monConfig.setGroupName("PlanetaryRepGroup");
 * monConfig.setNodeName("mon1");
 * monConfig.setNodeHostPort("monhost1.acme.com:7000");
 * monConfig.setHelperHosts("mars.acme.com:5000,jupiter.acme.com:5000");
 *
 * Monitor monitor = new Monitor(monConfig);
 *
 * // If the monitor has not been registered as a member of the group,
 * // register it now. register() returns the current node that is the
 * // master.
 *
 * ReplicationNode currentMaster = monitor.register();
 *
 * // Start up the listener, so that it can be used to track changes
 * // in the master node, or group composition. It can also be used to help
 * // determine the electable nodes that are currently active and participating
 * // in the replication group.
 * monitor.startListener(new MyChangeListener());
 * </pre>
 *
 * @see MonitorChangeListener
 * @see <a href="{@docRoot}../ReplicationGuide/monitors.html">Writing Monitor
 * Nodes</a>
 * @see <a
 * href="{@docRoot}../examples/je/rep/quote/package-summary.html">je.rep.quote
 * Examples</a>
 */
public class Monitor {

    /* The Monitor Id */
    private final NameIdPair nameIdPair;

    /* The configuration in use by this Monitor. */
    private final MonitorConfig monitorConfig;

    /* Provides the admin functionality for the monitor. */
    private final ReplicationGroupAdmin repGroupAdmin;

    /* The underlying learner that drives the Monitor. */
    private Learner learner;

    /* The Master change listener used by the Learner agent */
    private MasterChangeListener masterChangeListener;

    /* The Monitor's logger. */
    private final Logger logger;
    private final Formatter formatter;

    /* The user designated monitor change listener to be invoked. */
    private MonitorChangeListener monitorChangeListener;

    /* The channel factory */
    private DataChannelFactory channelFactory;

    /* The service dispatcher used by the Learner Agent and the Monitor. */
    private ServiceDispatcher serviceDispatcher;

    /* Set to true to force a shutdown of this monitor. */
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    /*
     * The groupMembers set saves the names of all nodes known to be part of
     * the group.
     *
     * 1. If a node is not in the set, it means this node has not been added to
     *    the group or has been removed from the group already.  Note that a
     *    node not in the groupMembers set may have an entry in the joinEvents
     *    map if a join event arrived for the node without an associated group
     *    add event, possibly because of an event delivery failure.
     * 2. If a node is in the set, but does not have an entry in the joinEvents
     *    map, this node may have closed itself (due to either crash or normal
     *    close) or has issued an ADD GroupChangeEvent. However, it hasn't
     *    issued a JoinGroupChangeEvent yet (because ADD GroupChangeEvent and
     *    JoinGroupEvent are notified in two phases).
     * 3. If a node is in the set, and is also in the joinEvents map, the node
     *    has already issued a JoinGroupEvent.
     */
    private final Set<String> groupMembers = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());

    /*
     * This map records whether a JoinGroupEvent has been issued for this node.
     * Used when the ping thread is issuing a LeaveGroupEvent.
     */
    private final ConcurrentHashMap<String, JoinGroupEvent> joinEvents =
        new ConcurrentHashMap<String, JoinGroupEvent>();

    /*
     * A thread which proactively checks on group status. TODO: ideally
     * implementation would be changed to use an ExecutorService rather than
     * a Thread.
     */
    private PingThread pingThread;

    /*
     * A TestHook, used by unit tests. If it's true, no MonitorChangeEvents
     * except NewMasterEvent will be issued.
     */
    private boolean disableNotify = false;

    /**
     * Deprecated as of JE5. Creates a monitor instance using a {@link
     * ReplicationConfig}. Monitor-specific properties that are not available
     * in ReplicationConfig use default settings.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     *
     * @deprecated As of JE 5, replaced by
     * {@link Monitor#Monitor(MonitorConfig)}
     */
    @Deprecated
    public Monitor(ReplicationConfig monitorConfig) {
        this(new MonitorConfig(monitorConfig));
    }

    /**
     * Creates a monitor instance.
     * <p>
     * @param monitorConfig configuration used by a Monitor
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public Monitor(MonitorConfig monitorConfig) {
        String groupName = monitorConfig.getGroupName();
        if (groupName == null) {
            throw new IllegalArgumentException("Missing group name");
        }
        nameIdPair = new NameIdPair(monitorConfig.getNodeName());
        String nodeHost = monitorConfig.getNodeHostPort();
        if (nodeHost == null) {
            throw new IllegalArgumentException("Missing nodeHost");
        }
        this.monitorConfig = monitorConfig.clone();
        this.channelFactory =
            DataChannelFactoryBuilder.construct(
                monitorConfig.getRepNetConfig(), groupName);
        repGroupAdmin =
            new ReplicationGroupAdmin(groupName,
                                      monitorConfig.getHelperSockets(),
                                      channelFactory);
        logger = LoggerUtils.getLoggerFormatterNeeded(getClass());
        formatter = new ReplicationFormatter(nameIdPair);
    }

    /**
     * Returns the name of the group associated with the Monitor.
     *
     * @return the group name
     */
    public String getGroupName() {
        return monitorConfig.getGroupName();
    }

    /**
     * @hidden
     * Returns the group-wide unique id associated with the monitor
     *
     * @return the monitor id
     */
    public NameIdPair getMonitorNameIdPair() {
        return nameIdPair;
    }

    /**
     * Returns the group-wide unique name associated with the monitor
     *
     * @return the monitor name
     */
    public String getNodeName() {
        return nameIdPair.getName();
    }

    /**
     * Returns the socket used by this monitor to listen for group changes
     *
     * @return the monitor socket address
     */
    public InetSocketAddress getMonitorSocketAddress() {
        return monitorConfig.getNodeSocketAddress();
    }

    /**
     * Registers the monitor with the group so that it can be kept informed
     * of the outcome of elections and group membership changes. The
     * monitor, just like a replication node, is identified by its nodeName.
     * The Monitor uses the helper nodes to locate a master with which it can
     * register itself. If the helper nodes are not available the registration
     * will fail.
     * <p>
     * A monitor must be registered at least once in order to be informed of
     * ongoing election results and group changes. Attempts to re-register the
     * same monitor are ignored. Registration, once it has been completed
     * successfully, persists beyond the lifetime of the Monitor instance and
     * does not need to be repeated. Repeated registrations are benign and
     * merely confirm that the current monitor configuration is consistent with
     * earlier registrations of this monitor.
     *
     * @return the node that is the current master
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the monitor has been shutdown, or no
     * helper sockets were specified at Monitor initialization.
     */
    public ReplicationNode register()
        throws EnvironmentFailureException {

        if (shutdown.get()) {
            throw new IllegalStateException("The monitor has been shutdown");
        }

        if (repGroupAdmin.getHelperSockets().size() == 0) {
            throw new IllegalStateException
                ("No helper sockets were specified at Monitor initialization");
        }
        RepNodeImpl monitorNode =
            new RepNodeImpl(nameIdPair,
                            NodeType.MONITOR,
                            monitorConfig.getNodeHostname(),
                            monitorConfig.getNodePort(),
                            JEVersion.CURRENT_VERSION);
        /* Ensure that the monitor is part of the group. */
        return repGroupAdmin.ensureMonitor(monitorNode);
    }

    /**
     * Starts the listener so it's actively listening for election results and
     * broadcasts of replication group changes.
     * <p>
     * {@link Monitor#register} should be called before starting the listener.
     * If the monitor has not been registered, it will not be updated, and its
     * listener will not be invoked.
     * <p>
     * Once the registration has been completed, the Monitor can start
     * listening even if none of the other nodes in the group are available.
     * It will be contacted automatically by the other nodes as they come up.
     * <p>
     * If the group has a Master, invoking <code>startListener</code> results
     * in a synchronous callback to the application via the {@link
     * MonitorChangeListener#notify(NewMasterEvent)} method.  If there is no
     * Master at this time, the callback takes place asynchronously, after the
     * method returns, when a Master is eventually elected.
     * <p>
     * Starting the listener will start the underlying ping thread, which
     * proactively checks group status for changes that might have been
     * missed when this Monitor instance has lost network connectivity or
     * is down.
     *
     * @param newListener the listener used to monitor events of interest.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IOException if the monitor socket could not be set up
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     *
     * @throws IllegalStateException if the monitor has been shutdown, or a
     * listener has already been established.
     */
    public void startListener(MonitorChangeListener newListener)
        throws DatabaseException, IOException {

        if (shutdown.get()) {
            throw new IllegalStateException("The monitor has been shutdown");
        }
        if (newListener == null) {
            throw new IllegalArgumentException
                ("A MonitorChangeListener must be associated with "  +
                 " this Monitor when invoking this method");
        }
        if (this.monitorChangeListener != null) {
            throw new IllegalStateException
                ("A Listener has already been established");
        }

        this.monitorChangeListener = newListener;

        serviceDispatcher =
            new ServiceDispatcher(monitorConfig.getNodeSocketAddress(),
                                  channelFactory);
        serviceDispatcher.start();
        Protocol electionProtocol =
            new Protocol(TimebasedProposalGenerator.getParser(),
                         MasterValue.getParser(),
                         monitorConfig.getGroupName(),
                         nameIdPair,
                         null,
                         channelFactory);
        learner = new Learner(electionProtocol, serviceDispatcher);
        serviceDispatcher.register(new MonitorService(this,
                                                      serviceDispatcher));
        masterChangeListener = new MasterChangeListener();
        learner.addListener(masterChangeListener);
        learner.start();
        try {
            /* Notify the listener about the current master. */
            final ReplicationGroup repGroup = repGroupAdmin.getGroup();
            final RepGroupImpl group = RepInternal.getRepGroupImpl(repGroup);

            /*
             * In the absence of a network failure, the query should result in
             * a call to the notify method of MonitorChangeListener.
             */
            learner.queryForMaster(group.getAllLearnerSockets());

            /* Notify JoinGroupEvents for those current active nodes. */
            notifyJoinGroupEventsForActiveNodes(repGroup);

            /* Start an underlying ping thread. */
            pingThread = new PingThread(repGroup);
            pingThread.start();
        } catch (UnknownMasterException ume) {
            /* The Listener will be informed when a Master is elected. */
            LoggerUtils.logMsg
                (logger, formatter, Level.INFO, "No current master.");
        }
    }

    /*
     * Used by unit test, disable notifying any GroupChangeEvents or
     * JoinGroupEvents.
     */
    void disableNotify(@SuppressWarnings("hiding")
                       final boolean disableNotify) {
        this.disableNotify = disableNotify;
    }

    /**
     * Notify JoinGroupEvents for currently active nodes in replication group,
     * and update the list of group members.
     */
    private void notifyJoinGroupEventsForActiveNodes(ReplicationGroup group) {
        NodeStateProtocol stateProtocol =
            new NodeStateProtocol(group.getName(),
                                  NameIdPair.NOCHECK,
                                  null,
                                  channelFactory);
        for (ReplicationNode repNode : group.getNodes()) {

            /*
             * Note any existing nodes with persistent id as group
             * members. Nodes w/ transient id never send add or remove
             * events, so they are not included in groupMembers.
             */
            if (!repNode.getType().hasTransientId()) {
                groupMembers.add(repNode.getName());
            }

            /*
             * Send out a NodeState request message for this node, but only for
             * electable and secondary nodes, since monitor nodes don't support
             * the NodeStateService.
             */
            if (repNode.getType().isMonitor()) {
                continue;
            }
            MessageExchange me = stateProtocol.new MessageExchange
                (repNode.getSocketAddress(),
                 NodeStateService.SERVICE_NAME,
                 stateProtocol.new NodeStateRequest(repNode.getName()));
            me.run();
            ResponseMessage resp = me.getResponseMessage();
            if (resp instanceof NodeStateResponse) {
                NodeStateResponse response = (NodeStateResponse) resp;
                notifyJoin(new JoinGroupEvent(response.getNodeName(),
                                              response.getMasterName(),
                                              response.getJoinTime()));
            }
        }
    }

    /**
     * Identifies the master of the replication group, resulting from the last
     * successful election. This method relies on the helper nodes supplied
     * to the monitor and queries them for the master.
     *
     * This method is useful when a Monitor first starts up and the Master
     * needs to be determined. Once a Monitor is registered and the Listener
     * has been started, it's kept up to date via events that are delivered
     * to the Listener.
     *
     * @return the id associated with the master replication node.
     *
     * @throws UnknownMasterException if the master could not be determined
     * from the set of helpers made available to the Monitor.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the monitor has been shutdown.
     */
    public String getMasterNodeName()
        throws UnknownMasterException {

        if (shutdown.get()) {
            throw new IllegalStateException("The monitor has been shutdown");
        }
        return repGroupAdmin.getMasterNodeName();
    }

    /**
     * Returns the current composition of the group. It does so by first
     * querying the helpers to determine the master and then obtaining the
     * group information from the master.
     *
     * @return an instance of RepGroup denoting the current composition of the
     * group
     *
     * @throws UnknownMasterException if the master could not be determined
     * from the set of helpers made available to the Monitor.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the monitor has been shutdown.
     */
    public ReplicationGroup getGroup()
        throws UnknownMasterException, DatabaseException {

        if (shutdown.get()) {
            throw new IllegalStateException("The monitor has been shutdown");
        }

        /*
         * TODO: Should we use this information to update the helper set as an
         * optimization?
         */
        return repGroupAdmin.getGroup();
    }

    /**
     * Release monitor resources and shut down the monitor.
     * @throws InterruptedException
     */
    public synchronized void shutdown()
        throws InterruptedException {

        boolean changed = shutdown.compareAndSet(false, true);
        if (!changed) {
            return;
        }

        LoggerUtils.logMsg(logger, formatter, Level.INFO,
                           "Shutting down monitor " + nameIdPair);

        /* Shutdown the Ping thread. */
        if (pingThread != null) {
            pingThread.stopThread();
            pingThread = null;
        }

        if (learner != null) {
            learner.shutdown();
        }

        if (serviceDispatcher != null) {
            serviceDispatcher.shutdown();
        }
    }

    /**
     * Notify the MonitorChangeListener that a GroupChangeEvent occurred.
     */
    synchronized void notifyGroupChange(GroupChangeEvent event) {
        if (disableNotify) {
            return;
        }

        final String name = event.getNodeName();
        switch (event.getChangeType()) {
            case REMOVE:
                /* If the REMOVE event has been fired before, do nothing. */
                if (!groupMembers.contains(name)) {
                    return;
                }
                /* Remove the deleted node from the group members set. */
                groupMembers.remove(name);
                break;
            case ADD:
                /* If the ADD event has been fired before, do nothing. */
                if (groupMembers.contains(name)) {
                    return;
                }

                /* Add the node to the group members set. */
                groupMembers.add(name);
                break;
            default:
                throw new IllegalArgumentException
                    ("Unrecognized GroupChangeType: " + event.getChangeType());
        }

        monitorChangeListener.notify(event);
    }

    /**
     * Notify the MonitorChangeListener that a JoinGroupEvent happens.
     */
    synchronized void notifyJoin(JoinGroupEvent event) {
        if (disableNotify) {
            return;
        }

        final String name = event.getNodeName();
        /* If this JoinGroupEvent has been fired before, do nothing. */
        if (joinEvents.containsKey(name)) {
            return;
        }

        /*
         * Save the JoinGroupEvent for this node so that it can be used while
         * notifying an abnormal LeaveGroupEvent for this node.
         */
        joinEvents.put(name, event);
        monitorChangeListener.notify(event);
    }

    /**
     * Notify the MonitorChangeListener that a LeaveGroupEvent occurred.
     */
    synchronized void notifyLeave(LeaveGroupEvent event) {

        /* Only notify a leave event if the node is currently joined */
        if (joinEvents.remove(event.getNodeName()) != null) {
            monitorChangeListener.notify(event);
        }
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
                LoggerUtils.logMsg(logger, formatter, Level.INFO,
                                   "Monitor notified of new Master: " +
                                   currentMasterName);
                if (monitorChangeListener == null) {
                    /* No interest */
                    return;
                }
                monitorChangeListener.notify
                    (new NewMasterEvent(currentValue));
            } catch (Exception e) {
                LoggerUtils.logMsg
                    (logger, formatter, Level.SEVERE,
                     "Monitor change event processing exception: " +
                     e.getMessage());
            }
        }
    }

    /*
     * PingThread periodically queries the replication group state in order
     * to proactively find group changes that this monitor may have missed
     * if the monitor was down or had network connectivity problems. Any
     * missed changes are propagated as the appropriate type of events.
     *
     * PingThread takes these steps:
     * 1. Get the current group information. If not available, use the last
     *    valid group information. Get information for both removed and
     *    non-removed nodes.
     * 2. Walk through the removed nodes set. If there is a node which is in
     *    the removed nodes set but still contained in groupMembers, send a
     *    REMOVE GroupChangeEvent.
     * 2a. Also walk through the group members remembered by the monitor, and
     *     send REMOVE GroupChangeEvent for each one that is not recorded in
     *     the current group info for removed or unremoved nodes, to account
     *     for deleted nodes.
     * 3. Walk through all unremoved nodes. There are three cases:
     *    a. If a node is reachable (it acks state request), but it's not
     *       in groupMembers, emit an ADD GroupChangeEvent.  Only do this for
     *       electable and monitor nodes -- secondary nodes don't send these
     *       events.
     *    b. If a node is reachable, but joinEvents.containsKey(node name)
     *       returns false, send a JoinGroupEvent.
     *    c. If a node is unreachable, but joinEvents.containsKey(node name)
     *       returns true, we want to send a LeaveGroupEvent, but guard against
     *       this being a transient situation. We do some retries and if we get
     *       the same result for all retries, send a missed LeaveGroupEvent.
     * 3a. Also walk through all nodes removed nodes, and do cases 3.b. and
     *     3.c. for those nodes
     * 4. Walk through all nodes in joinEvents, and perform 3.c. for ones which
     *    are not present as removed or unremoved nodes in the group.  These
     *    nodes are secondary nodes, which are removed from the group when the
     *    node becomes unavailable.
     */
    private class PingThread extends Thread {
        private volatile boolean running = true;
        private ReplicationGroup group;
        private final int retries;
        private final long retryInterval;
        private final int socketConnectTimeout;

        /*
         * Track the missed LeaveEvents, mapping from the node name to the
         * frequency it is thought to be missed.
         */
        private final Map<String, Integer> missedLeaveEvents =
            new HashMap<String, Integer>();

        /* Construct an underlying PingThread. */
        public PingThread(ReplicationGroup group) {
            this.group = group;
            this.retries = monitorConfig.getNumRetries();
            this.retryInterval = monitorConfig.getRetryInterval();
            this.socketConnectTimeout =
                monitorConfig.getSocketConnectTimeout();
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (running) {
                    for (int i = 0; i < retries && running; i++) {

                        queryNodes();

                        /*
                         * Ensure that the monitor hasn't missed any election
                         * results due to network interruptions by explicitly
                         * querying for a new master on a periodic basis.
                         */
                        RepGroupImpl groupImpl =
                            RepInternal.getRepGroupImpl(group);
                        /*
                         * The query for a master will result in
                         * NewMasterEvents, if there are any changes in the
                         * master.
                         */
                        learner.queryForMaster(
                            groupImpl.getAllLearnerSockets());

                        /* Sleep a while after querying all the nodes. */
                        sleep(retryInterval);
                    }
                    missedLeaveEvents.clear();
                }
            } catch (InterruptedException e) {
                 LoggerUtils.logMsg(logger, formatter, Level.INFO,
                                    "The daemon PingThread is interrupted: " +
                                    e.getMessage());
            }
        }

        /**
         * Ping all nodes to find out about possible missed events.
         * Manufacture notifications for any missed events.
         */
        private void queryNodes() {
            /* Get the current valid group information. */
            final ReplicationGroup repGroup = getValidGroup();

            final Set<? extends ReplicationNode> removedNodes =
                repGroup.getRepGroupImpl().getRemovedNodes();

            /* Send missed REMOVE GroupChangeEvents for removed nodes. */
            for (final ReplicationNode repNode : removedNodes) {
                notifyGroupChange
                    (new GroupChangeEvent(repGroup,
                                          repNode.getName(),
                                          GroupChangeType.REMOVE));
            }

            /*
             * Also send REMOVE GroupChangeEvents for nodes in groupMembers
             * that are no longer in the replication group, to account for
             * deleted nodes, which will not be remembered as removed nodes
             */
            for (final String nodeName : groupMembers) {
                if (repGroup.getRepGroupImpl().getNode(nodeName) == null) {
                    notifyGroupChange(
                        new GroupChangeEvent(
                            repGroup, nodeName, GroupChangeType.REMOVE));
                }
            }

            /*
             * Ping nodes to send missed ADD GroupChangeEvents, JoinGroupEvent
             * and LeaveGroupEvent.
             */
            for (ReplicationNode repNode : repGroup.getNodes()) {
                pingNode(repNode, repGroup, false);
            }

            /* Also ping removed nodes for missed leave events */
            for (final ReplicationNode repNode : removedNodes) {
                pingNode(repNode, repGroup, true);
            }

            /*
             * Send leave events for secondary nodes that no longer appear in
             * the group, since those nodes are completely removed as soon as
             * they disconnect from the feeder, which, because they are not
             * removed from the feeder otherwise, should only happen due to a
             * shutdown or network failure.
             *
             * TODO: Provide for distinguishing secondary and deleted electable
             * nodes.
             */
            for (final String nodeName : joinEvents.keySet()) {
                final ReplicationNode repNode =
                    repGroup.getRepGroupImpl().getNode(nodeName);
                if (repNode == null) {
                    notifyMissedLeaveEvents(nodeName);
                }
            }
        }

        /** Ping node to issue missed events. */
        private void pingNode(ReplicationNode repNode,
                              ReplicationGroup repGroup,
                              boolean removed) {

            /* Monitor nodes don't respond to pings */
            if (repNode.getType().isMonitor()) {
                return;
            }

            final String name = repNode.getName();
            try {
                DbPing ping = new DbPing(repNode,
                                         getGroupName(),
                                         socketConnectTimeout,
                                         channelFactory);
                NodeState state = ping.getNodeState();

                /* Send ADD GroupChangeEvent if needed */
                if (!groupMembers.contains(name) &&
                    !repNode.getType().isSecondary() &&
                    !repNode.getType().isExternal() &&
                    !removed) {
                    notifyGroupChange(
                        new GroupChangeEvent(repGroup,
                                             name,
                                             GroupChangeType.ADD));
                }

                /* Send JoinGroupEvent if needed */
                if (!joinEvents.containsKey(name)) {
                    notifyJoin(new JoinGroupEvent(name,
                                                  state.getMasterName(),
                                                  state.getJoinTime()));
                }
            } catch (IOException e) {
                /* Increase the counter of this down node. */
                notifyMissedLeaveEvents(name);
            } catch (ServiceConnectFailedException e) {
                /* Increase the counter of this down node. */
                notifyMissedLeaveEvents(name);
            } catch (ProtocolException e) {

                /*
                 * Thrown if a node is restarted with a new name on a
                 * hostname/port combination used for an earlier node, meaning
                 * the old node is offline.  Increase the counter of this down
                 * node.
                 */
                notifyMissedLeaveEvents(name);
            }
        }

        /*
         * If the master is currently unknown, use the last valid group
         * information so that the ping thread can continue working.
         */
        private ReplicationGroup getValidGroup() {
            ReplicationGroup repGroup = null;
            try {
                repGroup = getGroup();
                group = repGroup;
            } catch (Exception e) {
                repGroup = group;
            }

            return repGroup;
        }

        /* Notify a missed LeaveGroupEvent. */
        private void notifyMissedLeaveEvents(String name) {
            final JoinGroupEvent event = joinEvents.get(name);
            if (event == null) {
                return;
            }

            int counter = (missedLeaveEvents.get(name) == null) ?
                1 : missedLeaveEvents.get(name) + 1;
            missedLeaveEvents.put(name, counter);

            if (missedLeaveEvents.get(name) == retries) {
                notifyLeave(new LeaveGroupEvent(
                                name, event.getMasterName(),
                                LeaveReason.ABNORMAL_TERMINATION,
                                event.getJoinTime().getTime(),
                                System.currentTimeMillis()));
            }

        }

        public void stopThread() {
            running = false;
        }
    }
}
