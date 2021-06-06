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

package com.sleepycat.je.rep;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackup;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackup.RejectedServerException;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;

/**
 * Obtains log files for a Replica from other members of the replication
 * group. A Replica may need to do so if it has been offline for some time, and
 * has fallen behind in its execution of the replication stream.
 * <p>
 * During that time, the connected nodes may have reduced their log files by
 * deleting files after doing log cleaning. When this node rejoins the group,
 * it is possible that the current Master's log files do not go back far enough
 * to adequately {@link <a
 * href="{@docRoot}/../ReplicationGuide/lifecycle.html#lifecycle-nodestartup">
 * sync * up</a>} this node. In that case, the node can use a {@code
 * NetworkRestore} object to copy the log files from one of the nodes in the
 * group. The system tries to avoid deleting log files that either would be
 * needed for replication by current nodes or where replication would be more
 * efficient than network restore.
 * <p>
 * A Replica discovers the need for a NetworkRestore operation when a call to
 * {@code ReplicatedEnvironment()} fails with a {@link
 * InsufficientLogException}.
 * <p>
 * A call to {@code NetworkRestore.execute()} will copy the required log
 * files from a member of the group who owns the files and seems to be the
 * least busy. For example:
 * <pre class=code>
 *  try {
 *     node = new ReplicatedEnvironment(envDir, envConfig, repConfig);
 * } catch (InsufficientLogException insufficientLogEx) {
 *
 *     NetworkRestore restore = new NetworkRestore();
 *     NetworkRestoreConfig config = new NetworkRestoreConfig();
 *     config.setRetainLogFiles(false); // delete obsolete log files.
 *
 *     // Use the members returned by insufficientLogEx.getLogProviders() to
 *     // select the desired subset of members and pass the resulting list
 *     // as the argument to config.setLogProviders(), if the default selection
 *     // of providers is not suitable.
 *
 *     restore.execute(insufficientLogEx, config);
 *
 *     // retry
 *     node = new ReplicatedEnvironment(envDir, envConfig, repConfig);
 * }
 * </pre>
 * @see <a href="{@docRoot}/../ReplicationGuide/logfile-restore.html">
 * Restoring Log Files</a>
 */
public class NetworkRestore {

    /* The node that needs to be restored. */
    private RepImpl repImpl;

    /* The server's VLSN range end must be GT this vlsn. */
    private VLSN minVLSN;

    /* See 'Algorithm'. */
    private int maxLag;

    /*
     * The log provider actually used to obtain the log files. It must be one
     * of the members from the logProviders list.
     */
    private ReplicationNode logProvider;

    /* The current backup attempt. */
    private volatile NetworkBackup backup;

    private Logger logger;

    /* For unit tests only */
    private TestHook<File> interruptHook;

    /**
     * Creates an instance of NetworkRestore suitable for restoring the logs at
     * this node. After the logs are restored, the node can create a new
     * {@link ReplicatedEnvironment} and join the group
     */
    public NetworkRestore() {
    }

    /**
     * Initializes this instance for an impending execute() operation.
     *
     * Algorithm
     * =========
     * If we simply choose the server with the highest maxVSLN, we would
     * always choose the master, which is typically the server with the
     * highest load. If we choose based on load alone, we may choose a
     * lagging replica, and this may result in syncup failing later on the
     * restored server. The compromise solution involves maxLag. We don't
     * select servers less than maxLag behind the master (the server with the
     * highest VLSN range end) to increase the chances of syncup working
     * later, and among the non-lagging servers we choose the lowest load.
     *
     * 1. Collect list of servers and get their load/rangeEnd using the
     *    first part of the restore protocol. For each server, its load is its
     *    number of feeders and rangeEnd is the upper end of its VLSN range.
     *    Remove unresponsive servers from the list.
     *
     * 2. At the beginning of each round, if the server list is empty, give up.
     *
     * 3. Sort list by load. Let minVLSN be max(all rangeEnds) minus maxLag.
     *
     * 4. Attempt to perform restore in list order, refreshing each server's
     *    load/rangeEnd as we go. Reject any server with a refreshed rangeEnd
     *    that is LT minVLSN or a refreshed load that is GT its prior known
     *    load. Remove unresponsive servers from the list.
     *
     * 5. If the restore is incomplete, goto 2 and do another round.
     *
     * Note that between rounds the load and minVLSN of each server can
     * change, which is why servers are not removed from the list unless
     * they are unresponsive. The idea is to choose the best server based on
     * the information we collected in the last round, but reject servers
     * with new load or rangeEnd values that invalidate the earlier decision,
     * and always get fresh load/rangeEnd values for each server.
     *
     * @param logException the exception packing information driving the
     * restore operation.
     * @param config may contain an explicit list of members.
     * @return the list of candidate Server instances
     * @throws IllegalArgumentException if the configured log providers are
     * invalid
     */
    private List<Server> init(InsufficientLogException logException,
                              NetworkRestoreConfig config)
        throws IllegalArgumentException {

        repImpl = logException.getRepImpl();

        logger = LoggerUtils.getLogger(getClass());

        maxLag = repImpl.getConfigManager().getInt(
            RepParams.NETWORKBACKUP_MAX_LAG);

        List<ReplicationNode> logProviders;

        if ((config.getLogProviders() != null) &&
            (config.getLogProviders().size() > 0)) {
            final Set<String> memberNames = new HashSet<>();
            for (ReplicationNode node : logException.getLogProviders()) {
                memberNames.add(node.getName());
            }
            for (ReplicationNode node : config.getLogProviders()) {
                if (!memberNames.contains(node.getName())) {
                    throw new  IllegalArgumentException
                        ("Node:" + node.getName() +
                         " is not a suitable member for NetworkRestore." +
                         " It's not a member of logException." +
                         "getLogProviders(): " +
                         Arrays.toString(memberNames.toArray()));
                }
            }
            logProviders = config.getLogProviders();
        } else {
            logProviders = new LinkedList<>(logException.getLogProviders());
        }

        LoggerUtils.info(logger, repImpl, "Started network restore");

        List<Server> serverList = new LinkedList<>();

        /*
         * Set minVLSN and loadThreshold such that all attempts in the initial
         * round will produce RejectedServerException. Real values will be used
         * as servers are contacted and added to the list for the next round.
         * No initial sort is needed because all servers have the same load.
         */
        VLSN maxVLSN = new VLSN(Long.MAX_VALUE);
        int loadThreshold = -1;

        for (ReplicationNode node : logProviders) {
            serverList.add(new Server(node, maxVLSN, loadThreshold));
        }

        minVLSN = maxVLSN;

        return serverList;
    }

    /**
     * Sorts the refreshed server list by load and computes minVLSN.
     */
    private void resetServerList(List<Server> serverList) {

        if (serverList.isEmpty()) {
            return;
        }

        /* Natural comparator sorts by Server.load. */
        Collections.sort(serverList);

        /* Get server with max VLSN range end. */
        Server maxVlsnServer = Collections.max(serverList,
            Comparator.comparingLong(s -> s.rangeEnd.getSequence()));

        /* Subtract lag and ensure that result is GTE 0. */
        minVLSN = new VLSN(
            Math.max(0, maxVlsnServer.rangeEnd.getSequence() - maxLag));
    }

    /**
     * Restores the log files from one of the members of the replication group.
     * <p>
     * If <code>config.getLogProviders()</code> returns null, or an empty list,
     * it uses the member that is least busy as the provider of the log files.
     * Otherwise it selects a member from the list, choosing the first member
     * that's available, to provide the log files. If the members in this list
     * are not present in <code>logException.getLogProviders()</code>, it will
     * result in an <code>IllegalArgumentException</code> being thrown.
     * Exceptions handlers for <code>InsufficientLogException</code> will
     * typically use {@link InsufficientLogException#getLogProviders()} as the
     * starting point to compute an appropriate list, with which to set up
     * the <code>config</code> argument.
     * <p>
     * Log files that are currently at the node will be retained if they are
     * part of a consistent set of log files. Obsolete log files are either
     * deleted, or are renamed based on the the configuration of
     * <code>config.getRetainLogFiles()</code>.
     *
     * @param logException the exception thrown by {@code
     * ReplicatedEnvironment()} that necessitated this log refresh operation
     *
     * @param config configures the execution of the network restore operation
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalArgumentException if the <code>config</code> is invalid
     *
     * @see NetworkRestoreConfig
     */
    public synchronized void execute(InsufficientLogException logException,
                                     NetworkRestoreConfig config)
        throws EnvironmentFailureException,
               IllegalArgumentException {

        try {
            List<Server> serverList = init(logException, config);
            boolean firstRound = true;
            /*
             * Loop trying busier servers. It sorts the servers by the number
             * of active feeders at each server and contacts each one in turn,
             * trying increasingly busy servers until it finds a suitable one
             * that will service its request for log files. The same server may
             * be contacted multiple times, since it may become busier between
             * the time it was first contacted and a subsequent attempt.
             */
            while (!serverList.isEmpty()) {
                final List<Server> newServerList = new LinkedList<>();
                File envHome = repImpl.getEnvironmentHome();

                for (Server server : serverList) {
                    InetSocketAddress serverSocket =
                        server.node.getSocketAddress();
                    if (serverSocket.equals(repImpl.getSocket())) {
                        /* Cannot restore from yourself. */
                        continue;
                    }
                    LoggerUtils.info(logger, repImpl,
                                     "Network restore candidate server: " +
                                     server.node);
                    logProvider = server.node;
                    final long startTime = System.currentTimeMillis();
                    try {
                        backup = new NetworkBackup
                            (serverSocket,
                                config.getReceiveBufferSize(),
                                envHome,
                                repImpl.getNameIdPair(),
                                config.getRetainLogFiles(),
                                server.load,
                                minVLSN,
                                repImpl,
                                repImpl.getFileManager(),
                                repImpl.getLogManager(),
                                repImpl.getChannelFactory(),
                                logException.getProperties());

                        backup.setInterruptHook(interruptHook);
                        backup.execute();
                        LoggerUtils.info(logger, repImpl, String.format(
                            "Network restore completed from: %s. " +
                                "Elapsed time: %,d s.",
                            server.node,
                            ((System.currentTimeMillis() - startTime) /
                                1000)));
                        return;
                    } catch (RestoreMarker.FileCreationException e) {
                        throw
                            EnvironmentFailureException.unexpectedException(e);
                    } catch (DatabaseException e) {
                        /* Likely A malfunctioning server. */
                        LoggerUtils.warning(logger, repImpl,
                                            "Backup failed from node: " +
                                            server.node + "\n" +
                                            e.getMessage());
                    } catch (ConnectException e) {
                        /* Move on if the network connection is troublesome. */
                        LoggerUtils.info(logger, repImpl,
                                         "Backup server node: " + server.node +
                                         " is not available: " +
                                         e.getMessage());

                    } catch (IOException | ServiceConnectFailedException e) {
                        /* Move on if the network connection is troublesome. */
                        LoggerUtils.warning(logger, repImpl,
                                            "Backup failed from node: " +
                                            server.node + "\n" +
                                            e.getMessage());
                    } catch (RejectedServerException e) {
                        /*
                         * This is for one of two reasons:
                         * 1. This is the initial round and we expect this
                         *    exception for every server. We should not log a
                         *    message. Add server to the new list now that we
                         *    have its true rangeEnd and load.
                         * 2. The server got busier or is lagging since the
                         *    prior round, based on its refreshed rangeEnd and
                         *    load. Add server to the list in case it qualifies
                         *    in subsequent rounds.
                         */
                        if (!firstRound) {
                            LoggerUtils.info(logger, repImpl, e.getMessage());
                        }

                        newServerList.add(
                            new Server(server.node, e.getRangeLast(),
                                e.getActiveServers()));

                    } catch (IllegalArgumentException e) {
                        throw EnvironmentFailureException.unexpectedException(e);
                    }
                }
                serverList = newServerList; /* New list for the next round. */
                resetServerList(serverList);
                firstRound = false;
            }
            throw EnvironmentFailureException.unexpectedState
                ("Tried and failed with every node");
        } finally {
            logException.releaseRepImpl();
        }
    }

    /**
     * @hidden
     *
     * for testing use only
     */
    public NetworkBackup getBackup() {
        return backup;
    }

    /**
     * @hidden
     *
     * for testing use only
     *
     * Returns the member that was used to provide the log files.
     */
    public ReplicationNode getLogProvider() {
        return logProvider;
    }

    /**
     * @hidden
     *
     * Returns the network backup statistics for the current network restore
     * attempt, or {@code null} if a network backup is not currently underway.
     *
     * @return the statistics or {@code null}
     */
    public NetworkBackupStats getNetworkBackupStats() {
        final NetworkBackup currentBackup = backup;
        return (currentBackup != null) ? currentBackup.getStats() : null;
    }

    /**
     * A convenience class to help aggregate server attributes that may be
     * relevant to ordering the servers in terms of their suitability.
     */
    private static class Server implements Comparable<Server> {
        private final ReplicationNode node;
        private final VLSN rangeEnd;
        private final int load;

        public Server(ReplicationNode node, VLSN rangeEnd, int load) {
            this.node = node;
            this.rangeEnd = rangeEnd;
            this.load = load;
        }

        /**
         * This method is used in the sort to prioritize servers.
         */
        @Override
        public int compareTo(Server o) {
            return load - o.load;
        }

        @Override
        public String toString() {
            return node.getName();
        }
    }

    /**
     * @hidden
     * For unit testing
     * 
     * @param hook
     */
    public void setInterruptHook(TestHook<File> hook) {
        interruptHook = hook;
    }
}
